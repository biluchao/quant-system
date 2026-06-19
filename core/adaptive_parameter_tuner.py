#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 自适应参数调谐器 (AdaptiveParameterTuner) v23.0.0 — 机构级终极版

核心职责：
1. 基于历史交易记录，使用 CMA-ES 进化策略在线优化策略超参数
2. 优化目标为最大化滚动 Calmar 比率（可配置），通过回放引擎精确评估
3. 严格的边界约束、变化限制、线程安全、超时保护与完整日志脱敏
4. 完整的可观测性：优化历史、收敛曲线、Prometheus 指标与审计事件

外部依赖：
- numpy : 数值计算（必要条件）
- cma (可选) : CMA-ES 库；若不可用降级为随机搜索
- core.trade_database.TradeDatabase : 获取历史交易记录
- core.backtest_engine.BacktestEngine : 回放引擎（必须可用）
- core.metrics.MetricsCollector : 指标暴露（可选）

接口契约：
- trigger_update() -> Tuple[bool, str]  执行一次优化，返回(是否有新推荐, 原因)
- get_recommended_params() -> Dict[str, float]
- apply_params(params: Dict) -> bool  管理员确认后应用
- reset() -> None  恢复到默认参数
- health_check() -> Dict[str, Any]

异常与降级：
- 若 numpy 不可用，优化功能完全禁用
- 若 cma 不可用，降级为随机搜索并记录 WARNING
- 所有异常均被捕获，优化失败不影响主策略运行
- 参数推荐值必须通过边界与变化限制双重校验，且需人工确认
"""

import copy
import hashlib
import logging
import math
import time
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError, as_completed
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger(__name__)

VERSION = "23.0.0"
SPDX_IDENTIFIER = "Apache-2.0"

# ── 可选依赖 ──────────────────────────────────────────────
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False
    np = None

try:
    import cma
    CMA_AVAILABLE = True
except ImportError:
    CMA_AVAILABLE = False
    cma = None

try:
    from core.trade_database import TradeDatabase
except ImportError:
    TradeDatabase = None

try:
    from core.backtest_engine import BacktestEngine
except ImportError:
    BacktestEngine = None

try:
    from core.metrics import MetricsCollector
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    MetricsCollector = None

# ── 常量 ──────────────────────────────────────────────────
DEFAULT_POPULATION_SIZE = 20
DEFAULT_SIGMA = 0.1
DEFAULT_MAX_ITERATIONS = 8
MIN_SAMPLES_FOR_UPDATE = 30
PARAM_CHANGE_LIMIT_RELATIVE = 0.15
PARAM_CHANGE_LIMIT_ABSOLUTE = 0.01
DEFAULT_OBJECTIVE = "calmar_ratio"
DEFAULT_LOOKBACK_DAYS = 30
MIN_OPTIMIZATION_COOLDOWN_SEC = 3600
MAX_EVALUATION_TRADES = 1000
EVALUATION_TIMEOUT_SEC = 60.0
OPTIMIZATION_TIMEOUT_SEC = 600.0
OPTIMIZATION_HISTORY_SIZE = 20
RANDOM_SEARCH_TRIALS = 200
MAX_CMA_ITERATIONS_HARD = 50
CMA_VERBOSE_LEVEL = -9
PENALTY_FACTOR = 1e4
MAX_LOOKBACK_DAYS = 365
MIN_COOLDOWN_SEC = 60
DEFAULT_EVAL_WORKERS = 4
MAX_EVAL_WORKERS = 16
INVALID_OBJECTIVE_SCORE = 1e12
PARAM_HASH_LENGTH = 8
MIN_SIGMA = 1e-6
MAX_POPULATION_SIZE = 200
HEALTH_CHECK_TIMEOUT_SEC = 0.5
NA_STRING = "N/A"
MIN_TASK_TIMEOUT_SEC = 5.0
MAX_TASK_TIMEOUT_SEC = 300.0
MAX_EVENT_IMPORT_RETRIES = 2
ENGINE_CREATION_FAIL_WARN_SUPPRESS_SEC = 60
MAX_ENGINE_FAILURES_PER_GENERATION = 5
MAX_CONSECUTIVE_SKIPPED_GENERATIONS = 3
OLD_EXECUTOR_SHUTDOWN_TIMEOUT_SEC = 10.0
DECIMAL_PRECISION = 18


class AdaptiveParameterTuner:
    """在线参数优化器（CMA-ES / 随机搜索），万亿级生产标准"""

    def __init__(self, trade_db=None, backtest_engine=None,
                 param_bounds: Optional[Dict[str, Tuple[float, float]]] = None,
                 config: Optional[Dict] = None):
        self.trade_db = trade_db or (TradeDatabase() if TradeDatabase else None)
        self._base_engine = backtest_engine or (BacktestEngine() if BacktestEngine else None)
        bounds_input = param_bounds or DEFAULT_PARAM_BOUNDS
        self.param_bounds = self._validate_bounds(copy.deepcopy(bounds_input))
        self.config = config or {}
        self.current_params = self._init_params()
        self.recommended_params = dict(self.current_params)
        self._last_optimization_time = 0.0
        self._optimization_count = 0
        self._best_objective = -float("inf")
        self._best_lock = threading.Lock()
        self._optimization_history: List[Dict[str, Any]] = []

        self._state_lock = threading.RLock()

        if NUMPY_AVAILABLE:
            seed = self.config.get("seed", int(time.time() * 1000) % 2**31)
            self.rng = np.random.RandomState(seed)
        else:
            self.rng = None

        # 事件总线
        self._event_bus = None
        self._event_bus_retries = 0
        self._event_bus_failed = False

        # 优化控制
        self._cancel_event = threading.Event()
        self._optimization_running = False
        self._run_lock = threading.Lock()

        # 线程池
        self._executor = None
        self._executor_lock = threading.Lock()
        self._init_executor()

        # 引擎创建失败抑制
        self._last_engine_fail_time = 0.0

        # 参数版本号（用于检测并发修改）
        self._params_version = 0
        self._params_version_lock = threading.Lock()

        import decimal
        decimal.getcontext().prec = DECIMAL_PRECISION

        logger.info("AdaptiveParameterTuner v%s 初始化，维度 %d", VERSION, len(self.param_bounds))

    # ── 公共接口 ──────────────────────────────────────────

    def trigger_update(self) -> Tuple[bool, str]:
        """执行一次优化，返回(是否有新推荐, 原因)"""
        if not self._dependencies_ready():
            return False, "依赖不可用"
        if not NUMPY_AVAILABLE:
            return False, "numpy 不可用"

        now = time.time()
        with self._state_lock:
            cooldown = max(MIN_COOLDOWN_SEC,
                           self.config.get("cooldown_seconds", MIN_OPTIMIZATION_COOLDOWN_SEC))
            if now - self._last_optimization_time < cooldown:
                return False, "冷却中"
            start_params = copy.deepcopy(self.current_params)
            start_version = self._params_version

        trades = self._get_recent_trades()
        if len(trades) < MIN_SAMPLES_FOR_UPDATE:
            return False, f"样本不足 ({len(trades)}/{MIN_SAMPLES_FOR_UPDATE})"

        with self._run_lock:
            if self._optimization_running:
                return False, "已有优化任务在执行"
            self._optimization_running = True
            self._cancel_event.clear()
        try:
            best_params = self._run_optimization(trades, start_params)
            if best_params is None:
                return False, "优化未找到有效解"

            with self._state_lock:
                # 检测并发修改：若参数在优化期间被 apply_params 修改，则放弃本次推荐
                if self._params_version != start_version:
                    logger.warning("参数在优化期间被修改，放弃本次推荐")
                    return False, "参数已被外部修改，放弃推荐"

                clipped = self._clip_changes(start_params, best_params)
                if clipped == start_params:
                    return False, "裁剪后无变化"
                self.recommended_params = clipped
                self._last_optimization_time = time.time()
                self._optimization_count += 1
                best_obj = self._get_best_objective()
                self._record_history(clipped, best_obj)
                self._emit_event("param_recommendation_ready",
                                 {"params_hash": self._params_hash(clipped)})
                logger.info("优化完成，推荐参数摘要: %s", self._params_summary(clipped))
            self._record_metrics("tuner_optimization", 1)
            self._record_metrics("tuner_best_objective", best_obj)
            return True, "优化成功"
        finally:
            with self._run_lock:
                self._optimization_running = False

    def get_recommended_params(self) -> Dict[str, float]:
        with self._state_lock:
            return copy.deepcopy(self.recommended_params)

    def apply_params(self, params: Dict[str, float]) -> bool:
        with self._state_lock:
            if not params:
                logger.warning("应用参数为空")
                return False
            validated = {}
            unknown_keys = []
            for k in params:
                if k in self.param_bounds:
                    validated[k] = params[k]
                else:
                    unknown_keys.append(k)
            if unknown_keys:
                logger.error("存在未知参数键: %s，拒绝应用", unknown_keys)
                return False
            if not validated:
                return False
            for key in validated:
                val = validated[key]
                low, high = self.param_bounds[key]
                if not isinstance(val, (int, float)):
                    logger.error("参数 %s 类型错误", key)
                    return False
                if not (low <= val <= high):
                    logger.error("参数 %s 越界", key)
                    return False
                validated[key] = float(val)
            if all(abs(validated.get(k, self.current_params.get(k, 0)) - self.current_params.get(k, 0)) < 1e-10
                   for k in self.param_bounds):
                logger.info("参数无实质变化，跳过应用")
                return True
            old_params = copy.deepcopy(self.current_params)
            clipped = self._clip_changes(self.current_params, validated)
            self.current_params.update(clipped)
            self.recommended_params = dict(self.current_params)
            # 递增版本号，使任何正在进行的优化感知到变化
            self._params_version += 1
            logger.info("参数已应用，旧摘要: %s, 新摘要: %s",
                        self._params_summary(old_params),
                        self._params_summary(self.current_params))
            self._emit_event("params_applied", {"old_hash": self._params_hash(old_params),
                                                "new_hash": self._params_hash(self.current_params)})
            self._record_metrics("tuner_params_applied", 1)
            return True

    def get_current_params(self) -> Dict[str, float]:
        with self._state_lock:
            return copy.deepcopy(self.current_params)

    def reset(self) -> None:
        with self._state_lock:
            with self._run_lock:
                if self._optimization_running:
                    self._cancel_event.set()
            self.current_params = self._init_params()
            self.recommended_params = dict(self.current_params)
            self._optimization_count = 0
            self._best_objective = -float("inf")
            self._last_optimization_time = 0.0
            self._optimization_history.clear()
            self._event_bus_retries = 0
            self._event_bus_failed = False
            self._params_version += 1
            logger.info("参数已重置")
        self._record_metrics("tuner_params_reset", 1)
        self._emit_event("params_reset", {})

    def cancel_optimization(self) -> None:
        with self._run_lock:
            if self._optimization_running:
                self._cancel_event.set()
                logger.info("优化取消请求已发出")
                self._emit_event("optimization_cancelled", {})

    def is_optimization_running(self) -> bool:
        with self._run_lock:
            return self._optimization_running

    def get_optimization_history(self) -> List[Dict]:
        with self._state_lock:
            return copy.deepcopy(self._optimization_history)

    def health_check(self) -> Dict[str, Any]:
        warnings = []
        if not self.trade_db:
            warnings.append("TradeDatabase 未配置")
        if not self._base_engine:
            warnings.append("BacktestEngine 未配置")
        if not NUMPY_AVAILABLE:
            warnings.append("numpy 缺失")
        elif not CMA_AVAILABLE:
            warnings.append("cma 库缺失，降级随机搜索")
        with self._run_lock:
            if self._optimization_running:
                warnings.append("优化任务进行中")
        if self._optimization_count == 0:
            warnings.append("尚无优化记录")
        best_obj = self._get_best_objective()
        best_str = f"{best_obj:.4f}" if math.isfinite(best_obj) else NA_STRING
        with self._executor_lock:
            executor = self._executor
        if executor is None:
            warnings.append("线程池未初始化")
        else:
            try:
                future = executor.submit(lambda: True)
                future.result(timeout=HEALTH_CHECK_TIMEOUT_SEC)
            except Exception:
                warnings.append("线程池不可用")
        status = "degraded" if warnings else "ok"
        return {"status": status, "reason": f"优化次数: {self._optimization_count}, 最佳目标: {best_str}",
                "warnings": warnings}

    def shutdown(self) -> None:
        with self._run_lock:
            if self._optimization_running:
                self._cancel_event.set()
        with self._executor_lock:
            if self._executor is not None:
                try:
                    self._executor.shutdown(wait=True, timeout=10.0)
                except Exception as e:
                    logger.warning("关闭线程池异常: %s", e)
                self._executor = None
        logger.info("AdaptiveParameterTuner 已关闭")

    # ── 内部方法 ──────────────────────────────────────────

    def _init_executor(self):
        with self._executor_lock:
            if self._executor is not None:
                try:
                    self._executor.shutdown(wait=True, timeout=OLD_EXECUTOR_SHUTDOWN_TIMEOUT_SEC)
                except Exception:
                    pass
            workers = max(1, min(int(self.config.get("eval_workers", DEFAULT_EVAL_WORKERS)), MAX_EVAL_WORKERS))
            try:
                self._executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="tuner")
                logger.debug("线程池初始化成功，workers=%d", workers)
            except Exception as e:
                logger.exception("无法创建线程池，回退串行模式")
                self._executor = None

    def _ensure_executor(self):
        with self._executor_lock:
            if self._executor is None:
                self._init_executor()

    def _dependencies_ready(self) -> bool:
        if not self.trade_db:
            logger.warning("TradeDatabase 不可用")
            return False
        if not self._base_engine:
            logger.warning("BacktestEngine 不可用")
            return False
        return True

    @staticmethod
    def _validate_bounds(bounds: Dict[str, Tuple[float, float]]) -> Dict[str, Tuple[float, float]]:
        for key, (low, high) in bounds.items():
            if not (math.isfinite(low) and math.isfinite(high)):
                raise ValueError(f"参数边界 {key} 非法")
            if low > high:
                raise ValueError(f"参数边界 {key} 无效")
        return bounds

    def _init_params(self) -> Dict[str, float]:
        return {k: round(max(low, min(high, (low + high) / 2)), 10) for k, (low, high) in self.param_bounds.items()}

    def _get_recent_trades(self) -> List[Dict]:
        lookback = max(1, min(self.config.get("lookback_days", DEFAULT_LOOKBACK_DAYS), MAX_LOOKBACK_DAYS))
        since = max(0, time.time() - lookback * 86400)
        try:
            trades = self.trade_db.get_closed_trades(since=since)
            if not isinstance(trades, list):
                logger.error("返回类型错误")
                return []
            trades = [t for t in trades if isinstance(t, dict)]
            def sort_key(t):
                val = t.get("exit_time")
                if isinstance(val, (int, float)) and val > 0:
                    return val
                return float('inf')
            trades.sort(key=sort_key)
            return trades
        except Exception as e:
            logger.error("获取交易失败: %s", e)
            return []

    def _run_optimization(self, trades: List[Dict], start_params: Dict[str, float]) -> Optional[Dict[str, float]]:
        if CMA_AVAILABLE:
            result = None
            try:
                result = self._optimize_cma(trades, start_params)
            except Exception as e:
                logger.warning("CMA-ES 抛出未捕获异常: %s，降级随机搜索", e)
            if result is None:
                logger.info("CMA-ES 未找到有效解，尝试随机搜索")
                return self._optimize_random(trades, start_params)
            return result
        else:
            logger.info("CMA-ES 不可用，使用随机搜索")
            return self._optimize_random(trades, start_params)

    def _create_engine(self) -> Optional[Any]:
        try:
            if hasattr(self._base_engine, 'clone'):
                return self._base_engine.clone()
            if BacktestEngine:
                new_engine = BacktestEngine()
                if hasattr(self._base_engine, 'get_config') and hasattr(new_engine, 'set_config'):
                    try:
                        cfg = copy.deepcopy(self._base_engine.get_config())
                        new_engine.set_config(cfg)
                    except Exception as e:
                        logger.warning("复制引擎配置失败，新引擎可能使用默认配置: %s", e)
                return new_engine
        except Exception as e:
            now = time.time()
            if now - self._last_engine_fail_time > ENGINE_CREATION_FAIL_WARN_SUPPRESS_SEC:
                logger.warning("创建独立引擎失败: %s", e)
                self._last_engine_fail_time = now
        return None

    def _optimize_cma(self, trades: List[Dict], start_params: Dict[str, float]) -> Optional[Dict[str, float]]:
        param_keys = sorted(self.param_bounds.keys())
        dim = len(param_keys)
        try:
            x0 = np.array([start_params[k] for k in param_keys], dtype=float)
        except KeyError as e:
            logger.error("起始参数缺失键: %s", e)
            return None
        if not np.isfinite(x0).all():
            logger.error("初始点包含非法值")
            return None
        lower = np.array([self.param_bounds[k][0] for k in param_keys])
        upper = np.array([self.param_bounds[k][1] for k in param_keys])
        x0 = np.clip(x0, lower, upper)

        max_trades = max(1, self.config.get("max_evaluation_trades", MAX_EVALUATION_TRADES))
        # 深拷贝一次，后续只读共享（引擎不得修改输入）
        try:
            sample = [copy.deepcopy(t) for t in trades[-max_trades:]]
        except Exception as e:
            logger.error("无法深拷贝交易样本: %s", e)
            return None
        if not sample:
            return None

        popsize = max(dim + 1, min(int(self.config.get("population_size", DEFAULT_POPULATION_SIZE)), MAX_POPULATION_SIZE))
        sigma_val = self.config.get("sigma", DEFAULT_SIGMA)
        if not isinstance(sigma_val, (int, float)) or sigma_val <= 0:
            sigma_val = DEFAULT_SIGMA
        sigma0 = max(MIN_SIGMA, min(float(sigma_val), float(np.max(upper - lower)) * 0.5))
        max_iter = min(int(self.config.get("max_iterations", DEFAULT_MAX_ITERATIONS)), MAX_CMA_ITERATIONS_HARD)
        if max_iter <= 0:
            logger.warning("最大迭代次数为0，无法优化")
            return None

        self._ensure_executor()

        # 评估单个解：返回 (目标值, 原始score) 以便记录真实性能
        def evaluate_single(x, engine, shared_sample):
            if engine is None:
                return (INVALID_OBJECTIVE_SCORE, -float("inf"))
            params = {k: float(x[i]) for i, k in enumerate(param_keys)}
            violation = np.sum(np.maximum(lower - x, 0) ** 2 + np.maximum(x - upper, 0) ** 2)
            penalty = violation * PENALTY_FACTOR
            score = self._evaluate_params_sync(engine, shared_sample, params)
            if not math.isfinite(score) or score >= INVALID_OBJECTIVE_SCORE:
                return (INVALID_OBJECTIVE_SCORE + penalty, -float("inf"))
            return (-score + penalty, score)  # CMA‑ES 目标值 , 真实目标值

        start_time = time.time()
        best_current_objective = -float("inf")
        best_current_real_score = -float("inf")
        generation_count = 0
        consecutive_skipped = 0
        try:
            es = cma.CMAEvolutionStrategy(
                x0, sigma0,
                {"popsize": popsize, "maxiter": max_iter,
                 "bounds": [lower.tolist(), upper.tolist()],
                 "seed": self.rng.randint(1, 99999),
                 "verbose": CMA_VERBOSE_LEVEL}
            )
            while not es.stop() and not self._cancel_event.is_set():
                generation_count += 1
                elapsed = time.time() - start_time
                if elapsed > OPTIMIZATION_TIMEOUT_SEC:
                    logger.warning("CMA-ES 整体超时，终止")
                    break
                solutions = es.ask()
                n = len(solutions)
                values = [INVALID_OBJECTIVE_SCORE] * n
                real_scores = [-float("inf")] * n

                with self._executor_lock:
                    executor = self._executor
                engines = [self._create_engine() for _ in range(n)]
                engine_failures = sum(1 for e in engines if e is None)
                if engine_failures > MAX_ENGINE_FAILURES_PER_GENERATION:
                    logger.error("引擎创建失败过多 (%d/%d)，跳过第 %d 代", engine_failures, n, generation_count)
                    consecutive_skipped += 1
                    if consecutive_skipped > MAX_CONSECUTIVE_SKIPPED_GENERATIONS:
                        logger.error("连续跳过代数超过上限，终止优化")
                        break
                    continue
                else:
                    consecutive_skipped = 0

                valid_indices = [i for i, eng in enumerate(engines) if eng is not None]
                if not valid_indices:
                    continue

                if executor is None:
                    for i in valid_indices:
                        if self._cancel_event.is_set():
                            break
                        values[i], real_scores[i] = evaluate_single(solutions[i], engines[i], sample)
                else:
                    futures = {}
                    for i in valid_indices:
                        if self._cancel_event.is_set():
                            break
                        try:
                            future = executor.submit(evaluate_single, solutions[i], engines[i], sample)
                            futures[future] = i
                        except RuntimeError as e:
                            logger.error("提交任务失败: %s，改为串行", e)
                            values[i], real_scores[i] = evaluate_single(solutions[i], engines[i], sample)
                    if futures:
                        remaining = OPTIMIZATION_TIMEOUT_SEC - elapsed
                        task_timeout = min(EVALUATION_TIMEOUT_SEC * len(futures),
                                           max(MIN_TASK_TIMEOUT_SEC, remaining * 0.8))
                        task_timeout = min(task_timeout, MAX_TASK_TIMEOUT_SEC)
                        try:
                            for future in as_completed(futures, timeout=task_timeout):
                                idx = futures[future]
                                try:
                                    values[idx], real_scores[idx] = future.result(timeout=0)
                                except Exception:
                                    values[idx] = INVALID_OBJECTIVE_SCORE
                                    real_scores[idx] = -float("inf")
                        except FutureTimeoutError:
                            logger.warning("种群评估超时，取消剩余任务")
                            for f in futures:
                                if not f.done():
                                    f.cancel()

                # 记录本代最佳真实目标值
                valid_real = [r for r in real_scores if math.isfinite(r) and r >= -INVALID_OBJECTIVE_SCORE]
                if valid_real:
                    gen_best = max(valid_real)
                    if gen_best > best_current_real_score:
                        best_current_real_score = gen_best
                        with self._best_lock:
                            if gen_best > self._best_objective:
                                self._best_objective = gen_best
                        logger.debug("第 %d 代最佳目标: %.4f", generation_count, gen_best)

                if self._cancel_event.is_set():
                    logger.info("优化被取消")
                    break
                if len(values) == n:
                    if all(v >= INVALID_OBJECTIVE_SCORE for v in values):
                        logger.warning("所有候选评估无效，跳过第 %d 代", generation_count)
                        consecutive_skipped += 1
                        if consecutive_skipped > MAX_CONSECUTIVE_SKIPPED_GENERATIONS:
                            logger.error("连续跳过代数超过上限，终止优化")
                            break
                        continue
                    else:
                        consecutive_skipped = 0
                    safe_values = [v if np.isfinite(v) else INVALID_OBJECTIVE_SCORE for v in values]
                    try:
                        es.tell(solutions, safe_values)
                    except Exception as e:
                        logger.error("CMA-ES tell 异常: %s，终止优化", e)
                        break
                else:
                    logger.error("结果数量不匹配，终止优化")
                    break

            result = es.result
            if result.xbest is not None and np.isfinite(result.fbest) and result.fbest < INVALID_OBJECTIVE_SCORE:
                optimized = {k: float(result.xbest[i]) for i, k in enumerate(param_keys)}
                logger.info("CMA-ES 完成，真实最佳目标: %.4f, 代数: %d", best_current_real_score, result.iterations)
                return optimized
            else:
                logger.info("CMA-ES 未收敛到有效解")
                return None
        except Exception as e:
            logger.exception("CMA-ES 异常: %s", e)
            return None

    def _optimize_random(self, trades: List[Dict], start_params: Dict[str, float]) -> Optional[Dict[str, float]]:
        param_keys = sorted(self.param_bounds.keys())
        best_params = None
        best_score = -float("inf")
        trials = self.config.get("random_search_trials", RANDOM_SEARCH_TRIALS)
        max_trades = max(1, self.config.get("max_evaluation_trades", MAX_EVALUATION_TRADES))
        try:
            sample = [copy.deepcopy(t) for t in trades[-max_trades:]]
        except Exception as e:
            logger.error("无法深拷贝样本: %s", e)
            return None
        if not sample:
            return None

        engine = self._create_engine()
        if engine is not None:
            score = self._evaluate_params_sync(engine, sample, start_params)
            if score > best_score:
                best_score = score
                best_params = start_params

        start_time = time.time()
        for i in range(trials):
            if self._cancel_event.is_set() or time.time() - start_time > OPTIMIZATION_TIMEOUT_SEC:
                break
            candidate = {}
            for k in param_keys:
                low, high = self.param_bounds[k]
                if best_params and self.rng and self.rng.random() < 0.3:
                    center = best_params.get(k, (low + high) / 2)
                    std = max((high - low) * 0.05, 1e-8)
                    val = self.rng.normal(center, std)
                    candidate[k] = max(low, min(high, val))
                else:
                    candidate[k] = self.rng.uniform(low, high) if self.rng else (low + high) / 2
            engine = self._create_engine()
            if engine is None:
                continue
            score = self._evaluate_params_sync(engine, sample, candidate)
            if score > best_score:
                best_score = score
                best_params = candidate
                with self._best_lock:
                    if best_score > self._best_objective:
                        self._best_objective = best_score
                logger.debug("随机搜索新最佳: %.4f (第 %d 次)", best_score, i)
            if (i + 1) % 50 == 0:
                logger.debug("随机搜索进度: %d/%d, 当前最佳: %.4f", i+1, trials, best_score)
        if best_params is None or best_score == -float("inf"):
            logger.warning("随机搜索未找到有效解")
            return None
        with self._best_lock:
            if best_score > self._best_objective:
                self._best_objective = best_score
        return best_params

    def _evaluate_params_sync(self, engine: Any, sample: List[Dict], params: Dict[str, float]) -> float:
        if engine is None:
            return -float("inf")
        try:
            result = engine.run_replay(sample, copy.copy(params))
            if result is None:
                return -float("inf")
            return self._parse_evaluation_result(result)
        except Exception as e:
            logger.debug("评估异常: %s", e)
            return -float("inf")

    def _parse_evaluation_result(self, result: Any) -> float:
        if not isinstance(result, dict):
            return -float("inf")
        metric = self.config.get("objective", DEFAULT_OBJECTIVE)
        value = result.get(metric)
        if value is None and metric == DEFAULT_OBJECTIVE:
            value = result.get("calmar") or result.get("calmar_ratio")
        if value is None:
            return -float("inf")
        try:
            if isinstance(value, Decimal):
                if value.is_nan() or value.is_infinite():
                    return -float("inf")
                fval = float(value)
            else:
                fval = float(value)
            if not math.isfinite(fval) or math.isnan(fval) or fval >= INVALID_OBJECTIVE_SCORE:
                return -float("inf")
            return fval
        except (TypeError, ValueError, InvalidOperation):
            return -float("inf")

    def _clip_changes(self, old_params: Dict[str, float], new_params: Dict[str, float]) -> Dict[str, float]:
        clipped = {}
        for key, (low, high) in self.param_bounds.items():
            old_val = old_params.get(key, (low + high) / 2)
            new_val = new_params.get(key, old_val)
            delta = new_val - old_val
            if not math.isfinite(delta):
                delta = 0.0
            abs_limit = max(abs(old_val) * PARAM_CHANGE_LIMIT_RELATIVE, PARAM_CHANGE_LIMIT_ABSOLUTE)
            range_size = high - low
            if range_size > 0:
                abs_limit = min(abs_limit, range_size * 0.3)
            abs_limit = max(abs_limit, 1e-8)
            if abs(delta) > abs_limit:
                delta = math.copysign(abs_limit, delta) if delta != 0 else 0.0
                new_val = old_val + delta
            new_val = max(low, min(high, new_val))
            if not math.isfinite(new_val):
                new_val = old_val
            clipped[key] = new_val
        return clipped

    @staticmethod
    def _params_hash(params: Dict[str, float]) -> str:
        raw = "|".join(f"{k}={v:.10f}" for k, v in sorted(params.items()))
        return hashlib.sha256(raw.encode()).hexdigest()[:PARAM_HASH_LENGTH]

    @staticmethod
    def _params_summary(params: Dict[str, float]) -> str:
        items = list(params.keys())
        if not items:
            return "{}"
        return f"{{{items[0]}, ... ({len(items)} keys)}}"

    def _record_history(self, params: Dict[str, float], objective: float):
        if not math.isfinite(objective):
            return
        entry = {
            "timestamp": time.time(),
            "params": copy.deepcopy(params),
            "objective": float(objective),
            "optimization_id": self._optimization_count,
        }
        self._optimization_history.append(entry)
        if len(self._optimization_history) > OPTIMIZATION_HISTORY_SIZE:
            self._optimization_history.pop(0)

    def _get_best_objective(self) -> float:
        with self._best_lock:
            return self._best_objective

    def _emit_event(self, event_type: str, data: Dict):
        if self._event_bus_failed:
            return
        try:
            if self._event_bus is None:
                from core.event_bus import EventBus
                self._event_bus = EventBus()
            from core.event_bus import EventTypes
            self._event_bus.publish(EventTypes.SYSTEM_ALERT, {"subtype": event_type, "data": data})
            self._event_bus_retries = 0
        except ImportError:
            self._event_bus_retries += 1
            if self._event_bus_retries >= MAX_EVENT_IMPORT_RETRIES:
                self._event_bus_failed = True
                logger.warning("事件总线导入失败已达最大重试次数，永久停止尝试")
            else:
                logger.warning("事件总线导入失败，将重试 (%d/%d)", self._event_bus_retries, MAX_EVENT_IMPORT_RETRIES)
        except Exception:
            logger.debug("事件发布临时失败")

    def _record_metrics(self, name: str, value: float):
        if METRICS_AVAILABLE and MetricsCollector:
            try:
                if name == "tuner_best_objective":
                    MetricsCollector.gauge(name, value, {"module": "adaptive_tuner"})
                else:
                    MetricsCollector.counter(name, value, {"module": "adaptive_tuner"})
            except Exception:
                pass
