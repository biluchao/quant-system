#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 趋势状态推断器 (TrendStateInference) v24.0.0

核心职责：
1. 基于粒子滤波后验估计，实时推断市场趋势状态（发散、回归、震荡、恢复）
2. 联合假突破检测器，识别趋势恢复信号并发出回补指令
3. 提供滞后机制与时间衰减，减少噪声切换，提高稳定性
4. 完整的可观测性：指标、脱敏审计日志与事件发布

外部依赖：
- core.particle_filter.ParticleFilter : 提供 μ, θ 后验估计
- core.false_breakout.FalseBreakoutDetector : 识别假突破恢复
- core.event_bus.EventBus : 发布状态变更事件（可选）
- core.metrics.MetricsCollector : 指标暴露（可选）

接口契约：
- evaluate(market_data: Dict) -> Dict[str, Any]
  返回 {"state": str, "confidence": float, "action": str, "reason": str, "timestamp_ns": int}
- set_thresholds(params: Dict) -> bool  热重载判定阈值，返回是否成功
- get_thresholds() -> Dict[str, Any]  获取当前阈值
- reset() -> None  重置内部状态（含滞后计数器）
- health_check() -> Dict[str, Any]
"""

import copy
import hashlib
import logging
import math
import queue
import threading
import time
from enum import IntEnum
from typing import Any, Dict, Optional, Tuple, Union

logger = logging.getLogger(__name__)

VERSION = "24.0.0"
SPDX_IDENTIFIER = "Apache-2.0"

# 可选依赖
try:
    from core.particle_filter import ParticleFilter
except ImportError:
    ParticleFilter = None

try:
    from core.false_breakout import FalseBreakoutDetector
except ImportError:
    FalseBreakoutDetector = None

try:
    from core.event_bus import EventBus, EventTypes
except ImportError:
    EventBus = None
    EventTypes = None

try:
    from core.metrics import MetricsCollector
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    MetricsCollector = None

# 状态枚举
class TrendState(IntEnum):
    """趋势状态枚举，值固定以保证序列化稳定性"""
    OSCILLATING = 0
    DIVERGING = 1
    RETRACING = 2
    RECOVERY = 3

    @classmethod
    def from_name(cls, name: Optional[str]) -> Optional['TrendState']:
        if name is None:
            return None
        try:
            return cls[name.upper()]
        except (KeyError, AttributeError):
            return None

# 默认阈值常量
DEFAULT_MU_DIVERGE_THRESHOLD = 0.2
DEFAULT_MU_RETRACE_THRESHOLD = -0.15
DEFAULT_PROB_DIVERGE_THRESHOLD = 0.6
DEFAULT_HYSTERESIS_COUNT = 2
DEFAULT_HYSTERESIS_TIME_SEC = 120.0
DEFAULT_RECOVERY_HYSTERESIS_COUNT = 2
DEFAULT_RECOVERY_CONFIDENCE_BASE = 0.75
DEFAULT_RECOVERY_CONFIDENCE_FLOOR = 0.5
MIN_ESS_RATIO = 0.1
MAX_HYSTERESIS_COUNT = 8
MAX_HYSTERESIS_TIME_SEC = 600.0
MIN_HYSTERESIS_TIME_SEC = 30.0
Z_SCORE_EXTREME_THRESHOLD = 5.0
MAX_MU_MEAN = 10.0
EVAL_TIMEOUT_SEC = 0.5
MIN_DELTA_EVAL_SEC = 1.0
MAX_Z_SCORE_EXTREME = 15.0
HASH_ALGORITHM = 'sha256'

CACHE_STATE = "state"
CACHE_CONFIDENCE = "confidence"
CACHE_ACTION = "action"
CACHE_REASON = "reason"
CACHE_MU = "mu_mean"
CACHE_PROB = "prob_divergence"
CACHE_ESS = "ess_ratio"
CACHE_TIMESTAMP = "timestamp_ns"

DEFAULT_DEDUP_FIELDS = ('z_score', 'phi', 'volume_ratio')

class TrendStateInference:
    """趋势状态推断器，融合粒子滤波与假突破检测，机构级生产就绪"""

    def __init__(self, particle_filter=None, false_breakout_detector=None,
                 event_bus=None, config: Optional[Dict] = None):
        self.particle_filter = particle_filter
        self.false_breakout = false_breakout_detector
        self.event_bus = event_bus or (EventBus() if EventBus else None)

        self._state_counter: Dict[TrendState, int] = {s: 0 for s in TrendState}
        self._last_state = TrendState.OSCILLATING
        self._last_state_change_time = time.monotonic()
        self._last_eval_hash: Optional[str] = None
        self._last_eval_time: float = 0.0
        self._cached: Dict[str, Any] = {}

        config = config or {}
        self._mu_div_thresh = self._validate_param(
            config.get('mu_divergence', DEFAULT_MU_DIVERGE_THRESHOLD), float,
            lambda v: 0 < v <= 1.0, DEFAULT_MU_DIVERGE_THRESHOLD, 'mu_divergence')
        self._mu_ret_thresh = self._validate_param(
            config.get('mu_retrace', DEFAULT_MU_RETRACE_THRESHOLD), float,
            lambda v: -1.0 <= v < 0, DEFAULT_MU_RETRACE_THRESHOLD, 'mu_retrace')
        self._prob_div_thresh = self._validate_param(
            config.get('prob_divergence', DEFAULT_PROB_DIVERGE_THRESHOLD), float,
            lambda v: 0.5 <= v <= 0.99, DEFAULT_PROB_DIVERGE_THRESHOLD, 'prob_divergence')
        self._hysteresis_count = self._validate_param(
            config.get('hysteresis_count', DEFAULT_HYSTERESIS_COUNT), int,
            lambda v: 1 <= v <= MAX_HYSTERESIS_COUNT, DEFAULT_HYSTERESIS_COUNT, 'hysteresis_count')
        self._hysteresis_time = self._validate_param(
            config.get('hysteresis_time', DEFAULT_HYSTERESIS_TIME_SEC), float,
            lambda v: MIN_HYSTERESIS_TIME_SEC <= v <= MAX_HYSTERESIS_TIME_SEC, DEFAULT_HYSTERESIS_TIME_SEC, 'hysteresis_time')
        self._recovery_hyst_count = self._validate_param(
            config.get('recovery_hysteresis_count', DEFAULT_RECOVERY_HYSTERESIS_COUNT), int,
            lambda v: 1 <= v <= 10, DEFAULT_RECOVERY_HYSTERESIS_COUNT, 'recovery_hysteresis_count')
        self._recovery_conf_base = self._validate_param(
            config.get('recovery_confidence_base', DEFAULT_RECOVERY_CONFIDENCE_BASE), float,
            lambda v: 0.5 <= v <= 1.0, DEFAULT_RECOVERY_CONFIDENCE_BASE, 'recovery_confidence_base')
        self._recovery_conf_floor = self._validate_param(
            config.get('recovery_confidence_floor', DEFAULT_RECOVERY_CONFIDENCE_FLOOR), float,
            lambda v: 0.2 <= v <= 0.8, DEFAULT_RECOVERY_CONFIDENCE_FLOOR, 'recovery_confidence_floor')
        self._min_ess_ratio = self._validate_param(
            config.get('min_ess_ratio', MIN_ESS_RATIO), float,
            lambda v: 0.0 <= v <= 1.0, MIN_ESS_RATIO, 'min_ess_ratio')
        self._z_extreme = self._validate_param(
            config.get('z_score_extreme_threshold', Z_SCORE_EXTREME_THRESHOLD), float,
            lambda v: 1.0 <= v <= MAX_Z_SCORE_EXTREME, Z_SCORE_EXTREME_THRESHOLD, 'z_score_extreme_threshold')
        self._eval_timeout_sec = self._validate_param(
            config.get('eval_timeout_sec', EVAL_TIMEOUT_SEC), float,
            lambda v: 0.1 <= v <= 5.0, EVAL_TIMEOUT_SEC, 'eval_timeout_sec')
        self._dedup_fields = self._load_dedup_fields(config.get('dedup_fields', DEFAULT_DEDUP_FIELDS))

        self._lock = threading.RLock()

        self._validate_and_repair_thresholds(log_prefix="初始化")

        known_keys = {'mu_divergence', 'mu_retrace', 'prob_divergence', 'hysteresis_count',
                      'hysteresis_time', 'recovery_hysteresis_count', 'recovery_confidence_base',
                      'recovery_confidence_floor', 'min_ess_ratio', 'z_score_extreme_threshold',
                      'eval_timeout_sec', 'dedup_fields'}
        unknown = set(config.keys()) - known_keys
        if unknown:
            logger.warning("忽略未知配置键: %s", unknown)

        logger.info("TrendStateInference v%s 初始化", VERSION)

    @staticmethod
    def _validate_param(value, cast, validator, default, name):
        try:
            val = cast(value)
            if validator(val):
                return val
        except (ValueError, TypeError):
            pass
        logger.error("参数 %s 非法 (%s)，使用默认值 %s", name, value, default)
        return default

    @staticmethod
    def _load_dedup_fields(raw_value) -> Tuple[str, ...]:
        """加载去重字段，若非法则返回默认值"""
        if isinstance(raw_value, (list, tuple)) and len(raw_value) > 0:
            if all(isinstance(f, str) for f in raw_value):
                return tuple(raw_value)
        logger.error("dedup_fields 非法 (%s)，使用默认值 %s", raw_value, DEFAULT_DEDUP_FIELDS)
        return DEFAULT_DEDUP_FIELDS

    def _validate_and_repair_thresholds(self, log_prefix: str = ""):
        """校验并修复阈值一致性"""
        repaired = []
        with self._lock:
            if not (0 < self._mu_div_thresh <= 1.0 and -1.0 <= self._mu_ret_thresh < 0):
                logger.error("%s mu_divergence 或 mu_retrace 非法，重置为默认", log_prefix)
                self._mu_div_thresh = DEFAULT_MU_DIVERGE_THRESHOLD
                self._mu_ret_thresh = DEFAULT_MU_RETRACE_THRESHOLD
                repaired.append('mu_div/ret')
            elif self._mu_div_thresh <= self._mu_ret_thresh:
                logger.error("%s mu_divergence 必须大于 mu_retrace，重置为默认", log_prefix)
                self._mu_div_thresh = DEFAULT_MU_DIVERGE_THRESHOLD
                self._mu_ret_thresh = DEFAULT_MU_RETRACE_THRESHOLD
                repaired.append('mu_div/ret')
            if self._recovery_conf_floor > self._recovery_conf_base:
                old_floor = self._recovery_conf_floor
                logger.error("%s recovery_confidence_floor (%.2f) > recovery_confidence_base (%.2f)，调整为相等",
                            log_prefix, old_floor, self._recovery_conf_base)
                self._recovery_conf_floor = self._recovery_conf_base
                repaired.append(f'recovery_floor: {old_floor}->{self._recovery_conf_base}')
        if repaired:
            logger.warning("阈值一致性修复: %s", repaired)

    def _reset_internal_state(self):
        """重置所有滞后和缓存状态（在锁内调用）"""
        self._state_counter = {s: 0 for s in TrendState}
        self._last_state = TrendState.OSCILLATING
        self._last_state_change_time = time.monotonic()
        self._last_eval_hash = None
        self._last_eval_time = 0.0
        self._cached = {}

    def evaluate(self, market_data: Dict) -> Dict[str, Any]:
        if not isinstance(market_data, dict):
            return self._fallback("输入类型错误")
        z_val = market_data.get('z_score')
        if z_val is None or not isinstance(z_val, (int, float)) or not math.isfinite(z_val):
            return self._fallback("z_score无效")

        try:
            market_copy = copy.deepcopy(market_data)
        except Exception:
            market_copy = market_data  # 降级使用原始数据

        # 在锁内获取配置快照，确保线程安全
        with self._lock:
            dedup_fields = self._dedup_fields
            pf = self.particle_filter
            fb = self.false_breakout
            thresholds = {
                'mu_div': self._mu_div_thresh,
                'mu_ret': self._mu_ret_thresh,
                'prob_div': self._prob_div_thresh,
                'hyst_count': self._hysteresis_count,
                'hyst_time': self._hysteresis_time,
                'recovery_hyst': self._recovery_hyst_count,
                'recovery_conf_base': self._recovery_conf_base,
                'recovery_conf_floor': self._recovery_conf_floor,
                'min_ess': self._min_ess_ratio,
                'z_extreme': self._z_extreme,
            }
            eval_timeout = self._eval_timeout_sec

        data_fp = self._compute_data_fingerprint(market_copy, dedup_fields)
        now_mono = time.monotonic()
        with self._lock:
            if (self._last_eval_hash is not None and
                data_fp == self._last_eval_hash and
                (now_mono - self._last_eval_time) < MIN_DELTA_EVAL_SEC):
                if self._cached:
                    return copy.deepcopy(self._cached)
                logger.warning("缓存丢失但指纹匹配，重新计算")
            self._last_eval_hash = data_fp
            self._last_eval_time = now_mono

        if abs(z_val) > thresholds['z_extreme']:
            logger.warning("z_score异常 (abs > %.2f)", thresholds['z_extreme'])
            self._emit_event("z_score_extreme", {"z_score": round(z_val, 2)})
            return self._fallback("z_score异常")

        if not pf:
            return self._fallback("粒子滤波不可用")

        if not hasattr(pf, 'num_particles') or getattr(pf, 'num_particles', 0) == 0:
            return self._fallback("粒子滤波未初始化")

        est = self._run_with_timeout(pf.get_estimates, eval_timeout, "粒子滤波评估")
        if est is None:
            return self._fallback("估计异常或超时")
        if not isinstance(est, dict):
            logger.error("粒子滤波返回非字典: %s", type(est))
            return self._fallback("估计异常")

        mu_mean = float(est.get("mu_mean", 0.0))       # 确保为 float
        prob_div = est.get("prob_divergence", 0.5)
        ess = est.get("ess", 0)
        n_particles = getattr(pf, 'num_particles', 1)
        if n_particles <= 0:
            n_particles = 1

        missing = [k for k in ("mu_mean", "prob_divergence", "ess") if k not in est]
        if missing:
            logger.warning("粒子滤波估计缺失字段: %s", missing)

        # 数值清洗与裁剪
        if not math.isfinite(mu_mean):
            mu_mean = 0.0
        if abs(mu_mean) > MAX_MU_MEAN:
            logger.debug("mu_mean 超出范围: %.4f, 裁剪", mu_mean)
            mu_mean = math.copysign(MAX_MU_MEAN, mu_mean) if mu_mean != 0 else 0.0
        prob_div = max(0.0, min(1.0, float(prob_div)))
        if not math.isfinite(prob_div):
            prob_div = 0.5
        if not math.isfinite(ess) or math.isnan(ess):
            ess = 0
        ess = max(0, min(ess, n_particles))
        ess_ratio = min(ess / n_particles, 1.0)

        if ess_ratio <= thresholds['min_ess']:
            logger.debug("粒子退化, ess_ratio=%.3f", ess_ratio)
            return self._fallback("粒子退化")

        candidate = self._determine_candidate(mu_mean, prob_div, thresholds)

        recovery_signal, raw_recovery_conf = self._detect_recovery(fb, market_copy, thresholds, eval_timeout)

        with self._lock:
            confirmed = self._apply_hysteresis(candidate, recovery_signal, raw_recovery_conf, thresholds)
            confidence = self._compute_confidence(confirmed, mu_mean, prob_div, ess_ratio,
                                                  recovery_signal, raw_recovery_conf, thresholds)
            action = self._state_to_action(confirmed)

            if confirmed != self._last_state:
                logger.info("状态切换: %s -> %s (置信度 %.4f)",
                            self._last_state.name, confirmed.name, confidence)
                self._emit_event("trend_state_change", {
                    "from": self._last_state.name,
                    "to": confirmed.name,
                })
            self._last_state = confirmed

            self._cached = {
                CACHE_STATE: confirmed.name.lower(),
                CACHE_CONFIDENCE: round(confidence, 6),
                CACHE_ACTION: action,
                CACHE_REASON: self._build_reason(confirmed),
                CACHE_MU: round(mu_mean, 6),
                CACHE_PROB: round(prob_div, 6),
                CACHE_ESS: round(ess_ratio, 6),
                CACHE_TIMESTAMP: time.time_ns(),
            }
            result = self._cached.copy()

        self._record_metrics("trend_state_current", confirmed.value, {"state": confirmed.name})
        return result

    def set_thresholds(self, params: Dict[str, Union[float, int]]) -> bool:
        with self._lock:
            old = self.get_thresholds()
            success = True
            updates = []

            # 特殊处理 dedup_fields
            if 'dedup_fields' in params:
                raw = params['dedup_fields']
                if isinstance(raw, (list, tuple)) and len(raw) > 0 and all(isinstance(f, str) for f in raw):
                    self._dedup_fields = tuple(raw)
                    updates.append('dedup_fields')
                else:
                    success = False
                    logger.error("dedup_fields 无效: %s", raw)

            mapping = {
                'mu_divergence': ('_mu_div_thresh', float, lambda v: 0 < v <= 1.0),
                'mu_retrace': ('_mu_ret_thresh', float, lambda v: -1.0 <= v < 0),
                'prob_divergence': ('_prob_div_thresh', float, lambda v: 0.5 <= v <= 0.99),
                'hysteresis_count': ('_hysteresis_count', int, lambda v: 1 <= v <= MAX_HYSTERESIS_COUNT),
                'hysteresis_time': ('_hysteresis_time', float, lambda v: MIN_HYSTERESIS_TIME_SEC <= v <= MAX_HYSTERESIS_TIME_SEC),
                'recovery_hysteresis_count': ('_recovery_hyst_count', int, lambda v: 1 <= v <= 10),
                'recovery_confidence_base': ('_recovery_conf_base', float, lambda v: 0.5 <= v <= 1.0),
                'recovery_confidence_floor': ('_recovery_conf_floor', float, lambda v: 0.2 <= v <= 0.8),
                'min_ess_ratio': ('_min_ess_ratio', float, lambda v: 0.0 <= v <= 1.0),
                'z_score_extreme_threshold': ('_z_extreme', float, lambda v: 1.0 <= v <= MAX_Z_SCORE_EXTREME),
                'eval_timeout_sec': ('_eval_timeout_sec', float, lambda v: 0.1 <= v <= 5.0),
            }

            for key, (attr, cast, validator) in mapping.items():
                if key in params:
                    try:
                        val = cast(params[key])
                        if validator(val):
                            setattr(self, attr, val)
                            updates.append(key)
                        else:
                            success = False
                    except (ValueError, TypeError):
                        success = False

            unknown = set(params.keys()) - set(mapping.keys()) - {'dedup_fields'}
            if unknown:
                logger.warning("忽略未知阈值键: %s", unknown)

            repaired = self._validate_and_repair_thresholds(log_prefix="阈值热重载")
            if repaired:
                logger.warning("阈值一致性修复: %s", repaired)
                success = False

            if updates:
                self._reset_internal_state()
                logger.info("阈值已更新并重置所有状态: %s, 成功=%s", ', '.join(updates), success)
                self._emit_event("thresholds_updated", {"old": old, "new": self.get_thresholds()})
            elif not success:
                self._emit_event("thresholds_update_failed", {"params": params})
            return success

    def get_thresholds(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "mu_divergence": self._mu_div_thresh,
                "mu_retrace": self._mu_ret_thresh,
                "prob_divergence": self._prob_div_thresh,
                "hysteresis_count": self._hysteresis_count,
                "hysteresis_time": self._hysteresis_time,
                "recovery_hysteresis_count": self._recovery_hyst_count,
                "recovery_confidence_base": self._recovery_conf_base,
                "recovery_confidence_floor": self._recovery_conf_floor,
                "min_ess_ratio": self._min_ess_ratio,
                "z_score_extreme_threshold": self._z_extreme,
                "eval_timeout_sec": self._eval_timeout_sec,
                "dedup_fields": self._dedup_fields,
            }

    def reset(self) -> None:
        with self._lock:
            self._reset_internal_state()
        logger.info("状态推断器已重置")
        self._emit_event("state_inference_reset", {})

    def get_last_state(self) -> str:
        with self._lock:
            return self._last_state.name.lower()

    def set_particle_filter(self, pf) -> None:
        if pf is None:
            logger.warning("尝试设置空的粒子滤波实例，忽略")
            return
        with self._lock:
            if pf is self.particle_filter:
                return
            self.particle_filter = pf
            self._reset_internal_state()
        logger.info("粒子滤波实例已更新并重置状态")
        self._emit_event("particle_filter_replaced", {})

    @classmethod
    def health_check(cls) -> Dict[str, Any]:
        warnings = []
        if ParticleFilter is None:
            warnings.append("ParticleFilter 不可用")
        else:
            try:
                pf = ParticleFilter(num_particles=10)
                est = pf.get_estimates()
                if not isinstance(est, dict) or "mu_mean" not in est:
                    warnings.append("ParticleFilter 功能异常")
            except Exception as e:
                warnings.append(f"ParticleFilter 自检失败: {e}")
        if FalseBreakoutDetector is None:
            warnings.append("FalseBreakoutDetector 不可用")
        else:
            try:
                fb = FalseBreakoutDetector()
                if not hasattr(fb, 'check_recovery_signal'):
                    warnings.append("FalseBreakoutDetector 缺少 check_recovery_signal 方法")
            except Exception as e:
                warnings.append(f"FalseBreakoutDetector 实例化失败: {e}")
        status = "degraded" if warnings else "ok"
        return {
            "status": status,
            "reason": f"TrendStateInference v{VERSION}",
            "warnings": warnings,
        }

    def _compute_data_fingerprint(self, market_data: Dict, dedup_fields: Tuple[str, ...]) -> str:
        """根据市场数据计算去重指纹，使用给定的去重字段"""
        try:
            fields = dedup_fields if dedup_fields else DEFAULT_DEDUP_FIELDS
            core_fields = {}
            for k in fields:
                if k in market_data:
                    v = market_data[k]
                    if isinstance(v, float):
                        if math.isfinite(v):
                            core_fields[k] = f"{v:.6f}"
                        else:
                            if math.isnan(v):
                                core_fields[k] = f"{k}_NaN"
                            elif v > 0:
                                core_fields[k] = f"{k}_+Inf"
                            else:
                                core_fields[k] = f"{k}_-Inf"
                    else:
                        if v is None:
                            core_fields[k] = f"{k}_None"
                        else:
                            core_fields[k] = str(v)
            formatted = sorted(core_fields.items(), key=lambda item: item[0])
            data_str = str(formatted)
            return hashlib.new(HASH_ALGORITHM, data_str.encode()).hexdigest()
        except Exception:
            return str(time.time_ns())

    @staticmethod
    def _run_with_timeout(func, timeout_sec: float, task_name: str = "任务") -> Optional[Any]:
        """通用超时执行函数，返回结果或 None，使用 queue.Queue 确保线程安全"""
        result_queue = queue.Queue(maxsize=1)
        exception_queue = queue.Queue(maxsize=1)

        def worker():
            try:
                res = func()
                try:
                    result_queue.put(res, block=False)
                except queue.Full:
                    exception_queue.put(RuntimeError("结果队列已满"), block=False)
            except Exception as e:
                try:
                    exception_queue.put(e, block=False)
                except queue.Full:
                    pass

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        thread.join(timeout=timeout_sec)
        if thread.is_alive():
            logger.error("%s 超时 (%.2fs)", task_name, timeout_sec)
            return None
        try:
            exc = exception_queue.get_nowait()
            logger.error("%s 异常: %s", task_name, exc)
            return None
        except queue.Empty:
            pass
        try:
            return result_queue.get_nowait()
        except queue.Empty:
            logger.error("%s 未返回结果", task_name)
            return None

    def _detect_recovery(self, fb, market_data: Dict, thr: Dict, timeout_sec: float) -> Tuple[bool, float]:
        """检测假突破恢复信号，返回 (信号, 置信度) —— 置信度已限制在 [0,1]"""
        if not fb or not hasattr(fb, 'check_recovery_signal'):
            return False, 0.0

        res = self._run_with_timeout(
            lambda: fb.check_recovery_signal(market_data),
            timeout_sec * 0.6,
            "假突破检测"
        )
        if res is None:
            return False, 0.0

        if isinstance(res, (tuple, list)) and len(res) >= 2:
            raw_signal = res[0]
            if isinstance(raw_signal, (bool, int, float)):
                signal = bool(raw_signal)
            elif isinstance(raw_signal, str):
                signal = raw_signal.strip().lower() in ('true', '1', 'yes')
            else:
                signal = False
            try:
                raw_conf = float(res[1]) if res[1] is not None else 0.0
                conf = raw_conf if math.isfinite(raw_conf) else 0.0
                conf = max(0.0, min(1.0, conf))
            except (ValueError, TypeError):
                conf = 0.0
            return signal, conf
        elif isinstance(res, bool):
            return res, thr['recovery_conf_base'] if res else 0.0
        else:
            logger.warning("假突破检测返回未知类型: %s", type(res))
            return False, 0.0

    def _determine_candidate(self, mu: float, prob_div: float, thr: Dict) -> TrendState:
        if mu >= thr['mu_div'] and prob_div >= thr['prob_div']:
            return TrendState.DIVERGING
        if mu <= thr['mu_ret'] and prob_div <= (1 - thr['prob_div']):
            return TrendState.RETRACING
        return TrendState.OSCILLATING

    def _apply_hysteresis(self, candidate: TrendState, recovery: bool,
                          raw_recovery_conf: float, thr: Dict) -> TrendState:
        now = time.monotonic()

        effective_recovery = recovery and raw_recovery_conf >= thr['recovery_conf_floor']

        if effective_recovery and self._last_state != TrendState.DIVERGING:
            target = TrendState.RECOVERY
            required = thr['recovery_hyst']
        elif effective_recovery and self._last_state == TrendState.DIVERGING and candidate != TrendState.DIVERGING:
            target = TrendState.RECOVERY
            required = thr['recovery_hyst']
        else:
            target = candidate
            required = thr['hyst_count']

        if required > MAX_HYSTERESIS_COUNT:
            logger.warning("滞后计数上限 %d 超出，裁剪至 %d", required, MAX_HYSTERESIS_COUNT)
            required = MAX_HYSTERESIS_COUNT
        required = max(1, required)

        if now < self._last_state_change_time:
            self._last_state_change_time = now

        for s in TrendState:
            if s == target:
                self._state_counter[s] = min(self._state_counter[s] + 1, MAX_HYSTERESIS_COUNT)
            else:
                self._state_counter[s] = 0

        if self._state_counter[target] >= required:
            time_since_last = now - self._last_state_change_time
            if time_since_last >= thr['hyst_time']:
                for s in TrendState:
                    self._state_counter[s] = 0
                self._last_state_change_time = now
                return target
        return self._last_state

    @staticmethod
    def _state_to_action(state: TrendState) -> str:
        mapping = {
            TrendState.DIVERGING: "hold_or_add",
            TrendState.RETRACING: "reduce_or_wait",
            TrendState.OSCILLATING: "wait",
            TrendState.RECOVERY: "hold",
        }
        return mapping.get(state, "wait")

    def _compute_confidence(self, state: TrendState, mu: float, prob_div: float,
                            ess_ratio: float, recovery: bool, raw_recovery_conf: float,
                            thr: Dict) -> float:
        if state == TrendState.DIVERGING:
            strength_factor = min(abs(mu) / (max(abs(thr['mu_div']), 1e-6) + 0.01), 1.0)
            raw = prob_div * ess_ratio * strength_factor
        elif state == TrendState.RETRACING:
            strength_factor = min(abs(mu) / (max(abs(thr['mu_ret']), 1e-6) + 0.01), 1.0)
            raw = (1 - prob_div) * ess_ratio * strength_factor
        elif state == TrendState.RECOVERY:
            if recovery:
                conf_val = max(min(raw_recovery_conf, thr['recovery_conf_base']), thr['recovery_conf_floor'])
            else:
                conf_val = thr['recovery_conf_base']
            raw = conf_val * ess_ratio
        else:
            raw = 0.5 * ess_ratio
        return max(0.0, min(1.0, raw))

    @staticmethod
    def _build_reason(state: TrendState) -> str:
        mapping = {
            TrendState.DIVERGING: "趋势发散",
            TrendState.RETRACING: "趋势回归",
            TrendState.OSCILLATING: "无明确方向",
            TrendState.RECOVERY: "假突破恢复",
        }
        return mapping.get(state, "未知")

    def _fallback(self, reason: str) -> Dict[str, Any]:
        logger.warning("趋势推断降级: %s", reason)
        return {
            CACHE_STATE: TrendState.OSCILLATING.name.lower(),
            CACHE_CONFIDENCE: 0.0,
            CACHE_ACTION: "wait",
            CACHE_REASON: "降级",
            CACHE_MU: 0.0,
            CACHE_PROB: 0.5,
            CACHE_ESS: 0.0,
            CACHE_TIMESTAMP: time.time_ns(),
        }

    def _emit_event(self, event_type: str, data: Dict):
        if not self.event_bus:
            return
        try:
            evt_type = getattr(EventTypes, 'STATE_CHANGE', "state_change")
            self.event_bus.publish(evt_type, {
                "subtype": event_type,
                "data": data,
                "timestamp_ns": time.time_ns(),
            })
        except Exception as e:
            logger.debug("事件发布失败: %s", e)

    def _record_metrics(self, name: str, value: float, labels: Optional[Dict] = None):
        if METRICS_AVAILABLE and MetricsCollector:
            try:
                MetricsCollector.gauge(name, value, labels=labels or {})
            except TypeError:
                try:
                    MetricsCollector.gauge(name, labels or {}, value)
                except Exception:
                    pass
            except Exception:
                pass
