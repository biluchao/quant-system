#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 贝叶斯多因子得分融合器 (BayesianScoreCalculator) v8.0.0

核心职责：
1. 基于朴素贝叶斯快速计算开仓倾向后验概率，融合多个标准化因子
2. 支持可配置的离散化阈值、在线似然表更新（EMA）、线程安全
3. 提供不确定性估计（1 - 确信度）及完整的可观测性与审计

外部依赖：
- core.metrics.MetricsCollector : 可选，用于指标上报
- core.event_bus.EventBus : 可选，发布状态变更事件

接口契约：
- compute_score(factors: List[float]) -> Dict[str, float]
  返回 {"probability": float, "uncertainty": float}
- update_likelihood_table(factors: List[float], label: int) -> None
- get_likelihood_table() -> Dict
- reset_likelihood() -> None
- set_prior(prior: float) -> None
- get_prior() -> float
- set_update_alpha(alpha: float) -> None
- get_update_alpha() -> float
- set_discretize_thresholds(thresholds: List[float]) -> bool
- health_check() -> Dict[str, Any]
"""

import copy
import logging
import math
import threading
import time
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

VERSION = "8.0.0"
SPDX_IDENTIFIER = "Apache-2.0"

# 可选依赖
try:
    from core.metrics import MetricsCollector
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    MetricsCollector = None

try:
    from core.event_bus import EventBus, EventTypes
    EVENT_BUS_AVAILABLE = True
except ImportError:
    EVENT_BUS_AVAILABLE = False
    EventBus = None
    EventTypes = None

# 常量
DEFAULT_FACTOR_COUNT = 14
DEFAULT_LEVELS = 3
PRIOR_PROB_DEFAULT = 0.5
MIN_PROB = 0.001
MAX_PROB = 0.999
LOG_ODDS_CLAMP = 20.0
MIN_LIKELIHOOD_VALUE = 1e-9
KNOWN_CONFIG_KEYS = {
    'num_factors', 'num_levels', 'prior_prob', 'update_alpha',
    'discretize_thresholds', 'likelihood_table', 'factor_range'
}
DEFAULT_FACTOR_RANGE = (-1.0, 1.0)


def _safe_float(value: Any) -> Optional[float]:
    """安全转换为 float，兼容 numpy/Decimal/bool，排除 bool，返回 None 表示非法"""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class BayesianScoreCalculator:
    """朴素贝叶斯得分计算器，支持在线学习与线程安全"""

    def __init__(self,
                 likelihood_table: Optional[Dict] = None,
                 config: Optional[Dict] = None,
                 event_bus: Optional[Any] = None):
        config = config or {}
        # 因子数与级别数
        self.num_factors = self._parse_int(config.get('num_factors'), DEFAULT_FACTOR_COUNT, 1, "num_factors")
        self.num_levels = self._parse_int(config.get('num_levels'), DEFAULT_LEVELS, 2, "num_levels")

        # 加载似然表（支持从 config 或直接传入）
        loaded_table = likelihood_table
        if loaded_table is None and 'likelihood_table' in config:
            loaded_table = config['likelihood_table']
        if loaded_table is not None and self._validate_likelihood(loaded_table):
            self.likelihood = copy.deepcopy(loaded_table)
            self._normalize_likelihood()
        else:
            if loaded_table is not None:
                logger.warning("似然表无效，使用均匀分布")
            self.likelihood = {1: self._uniform_likelihood(), 0: self._uniform_likelihood()}
        self._enforce_table_size()

        # 先验概率
        prior_raw = _safe_float(config.get('prior_prob'))
        if prior_raw is not None and 0 < prior_raw < 1:
            self.prior = prior_raw
        else:
            if prior_raw is not None:
                logger.warning("prior_prob 非法 (%.4f)，使用默认", prior_raw)
            self.prior = PRIOR_PROB_DEFAULT

        # 学习率
        alpha_raw = _safe_float(config.get('update_alpha'))
        self._update_alpha = max(0.0, min(1.0, alpha_raw)) if alpha_raw is not None else 0.05

        # 离散化阈值（可自动生成或使用配置，适配因子范围）
        self._factor_range = self._parse_factor_range(config.get('factor_range', DEFAULT_FACTOR_RANGE))
        raw_thresholds = config.get('discretize_thresholds')
        if (raw_thresholds and isinstance(raw_thresholds, list)
                and len(raw_thresholds) == self.num_levels - 1
                and self._is_strictly_increasing(raw_thresholds)):
            self.discretize_thresholds = [float(t) for t in raw_thresholds]
        else:
            if raw_thresholds:
                logger.warning("离散化阈值无效，使用自动生成")
            self.discretize_thresholds = self._auto_thresholds()

        self._lock = threading.RLock()
        self.event_bus = event_bus if event_bus and EVENT_BUS_AVAILABLE else None

        # 告警未知配置键
        unknown = set(config.keys()) - KNOWN_CONFIG_KEYS
        if unknown:
            logger.warning("忽略未知配置键: %s", unknown)

        logger.info("BayesianScoreCalculator v%s 初始化: 因子=%d, 级别=%d",
                     VERSION, self.num_factors, self.num_levels)

    # ── 公共接口 ──────────────────────────────────────────

    def compute_score(self, factors: List[float]) -> Dict[str, float]:
        """计算开仓倾向概率与不确定性"""
        if not isinstance(factors, list) or len(factors) != self.num_factors:
            logger.warning("因子数量不匹配: 期望 %d", self.num_factors)
            return {"probability": self.prior, "uncertainty": 0.5}

        cleaned = []
        nan_count = 0
        for v in factors:
            fval = _safe_float(v)
            if fval is not None and math.isfinite(fval):
                cleaned.append(fval)
            else:
                cleaned.append(float('nan'))
                nan_count += 1
        if nan_count == self.num_factors:
            logger.warning("所有因子均为非法值，返回先验")
            return {"probability": self.prior, "uncertainty": 0.5}

        # 读取离散化阈值（锁内快照）
        with self._lock:
            thresholds = list(self.discretize_thresholds)
        discrete = self._discretize_factors(cleaned, thresholds)

        with self._lock:
            likelihood_1 = list(self.likelihood[1])
            likelihood_0 = list(self.likelihood[0])
            prior = self.prior

        # 对数几率计算
        try:
            log_odds = math.log(prior / (1.0 - prior))
        except (ValueError, ZeroDivisionError):
            log_odds = 0.0
        if not math.isfinite(log_odds):
            log_odds = 0.0

        for i, level in enumerate(discrete):
            idx = i * self.num_levels + level
            if 0 <= idx < len(likelihood_1):
                p1 = max(likelihood_1[idx], MIN_LIKELIHOOD_VALUE)
                p0 = max(likelihood_0[idx], MIN_LIKELIHOOD_VALUE)
                try:
                    log_odds += math.log(p1 / p0)
                except (ValueError, ZeroDivisionError):
                    continue
                if not math.isfinite(log_odds):
                    log_odds = math.copysign(LOG_ODDS_CLAMP, log_odds)
                log_odds = max(-LOG_ODDS_CLAMP, min(LOG_ODDS_CLAMP, log_odds))
            else:
                logger.warning("似然表索引越界: i=%d level=%d", i, level)
                return {"probability": prior, "uncertainty": 0.5}

        prob = 1.0 / (1.0 + math.exp(-log_odds))
        prob = max(MIN_PROB, min(MAX_PROB, prob))
        certainty = abs(prob - 0.5) * 2.0
        uncertainty = 1.0 - certainty

        self._record_metrics("bayesian_probability", prob, labels={"type": "score"})
        return {
            "probability": round(prob, 6),
            "uncertainty": round(uncertainty, 6)
        }

    def update_likelihood_table(self, factors: List[float], label: int) -> None:
        """在线更新似然表（EMA）"""
        if label not in (0, 1):
            logger.warning("标签必须为 0 或 1")
            return
        if not isinstance(factors, list) or len(factors) != self.num_factors:
            logger.warning("因子数量不匹配")
            return

        cleaned = []
        for v in factors:
            fval = _safe_float(v)
            if fval is not None and math.isfinite(fval):
                cleaned.append(fval)
            else:
                cleaned.append(float('nan'))
        with self._lock:
            thresholds = list(self.discretize_thresholds)
        discrete = self._discretize_factors(cleaned, thresholds)

        with self._lock:
            for i, level in enumerate(discrete):
                base_idx = i * self.num_levels
                indices = [base_idx + l for l in range(self.num_levels)]
                arr = self.likelihood[label]
                for j, idx in enumerate(indices):
                    target = 1.0 if j == level else 0.0
                    arr[idx] = (1 - self._update_alpha) * arr[idx] + self._update_alpha * target
                total = sum(arr[idx] for idx in indices)
                if total > 0:
                    for idx in indices:
                        arr[idx] /= total
                else:
                    logger.warning("更新时因子 %d 总和为0，重置", i)
                    for idx in indices:
                        arr[idx] = 1.0 / self.num_levels
        self._record_metrics("bayesian_update", 1, labels={"label": str(label)})
        self._emit_event("likelihood_updated", {"label": label})

    def get_likelihood_table(self) -> Dict:
        """返回当前似然表的深拷贝"""
        with self._lock:
            return copy.deepcopy(self.likelihood)

    def reset_likelihood(self) -> None:
        """重置似然表为均匀分布"""
        with self._lock:
            self.likelihood = {1: self._uniform_likelihood(), 0: self._uniform_likelihood()}
        logger.info("似然表已重置")
        self._emit_event("likelihood_reset", {})

    def set_prior(self, prior: float) -> None:
        """设置先验概率（0<prior<1）"""
        fprior = _safe_float(prior)
        if fprior is None or not (0 < fprior < 1):
            logger.warning("先验概率非法: %s", prior)
            return
        with self._lock:
            old = self.prior
            self.prior = fprior
        logger.info("先验概率更新: %.4f -> %.4f", old, fprior)
        self._emit_event("prior_changed", {"old": old, "new": fprior})

    def get_prior(self) -> float:
        with self._lock:
            return self.prior

    def set_update_alpha(self, alpha: float) -> None:
        """设置在线学习率（[0,1]）"""
        falpha = _safe_float(alpha)
        if falpha is None:
            logger.warning("学习率非法: %s", alpha)
            return
        falpha = max(0.0, min(1.0, falpha))
        with self._lock:
            old = self._update_alpha
            self._update_alpha = falpha
        logger.info("学习率更新: %.4f -> %.4f", old, falpha)
        self._emit_event("update_alpha_changed", {"old": old, "new": falpha})

    def get_update_alpha(self) -> float:
        with self._lock:
            return self._update_alpha

    def set_discretize_thresholds(self, thresholds: List[float]) -> bool:
        """动态设置离散化阈值，成功返回 True"""
        if not isinstance(thresholds, list) or len(thresholds) != self.num_levels - 1:
            logger.warning("阈值数量不匹配: 期望 %d", self.num_levels - 1)
            return False
        try:
            typed = [float(t) for t in thresholds]
        except (TypeError, ValueError):
            logger.warning("阈值类型转换失败")
            return False
        if not self._is_strictly_increasing(typed):
            logger.warning("阈值未严格递增")
            return False
        with self._lock:
            self.discretize_thresholds = typed
        logger.info("离散化阈值已更新")
        self._emit_event("discretize_thresholds_changed", {"threshold_count": len(typed)})
        return True

    def validate_normalization(self) -> bool:
        """公开的归一化验证接口"""
        with self._lock:
            for cls in (0, 1):
                for i in range(self.num_factors):
                    start = i * self.num_levels
                    total = sum(self.likelihood[cls][start:start + self.num_levels])
                    if abs(total - 1.0) > 1e-9:
                        return False
        return True

    # ── 内部方法 ──────────────────────────────────────────

    def _discretize_factors(self, factors: List[float], thresholds: List[float]) -> List[int]:
        if len(thresholds) != self.num_levels - 1:
            logger.error("阈值数量与级别数不匹配: %d vs %d", len(thresholds), self.num_levels - 1)
            return [self.num_levels // 2] * len(factors)
        levels = []
        for val in factors:
            if not math.isfinite(val):
                level = self.num_levels // 2
            else:
                level = 0
                for t in thresholds:
                    if val <= t:
                        break
                    level += 1
                level = min(level, self.num_levels - 1)
            levels.append(level)
        return levels

    def _uniform_likelihood(self) -> List[float]:
        return [1.0 / self.num_levels] * (self.num_factors * self.num_levels)

    def _validate_likelihood(self, table: Dict) -> bool:
        if not isinstance(table, dict) or 1 not in table or 0 not in table:
            logger.error("似然表格式错误：缺少键")
            return False
        expected = self.num_factors * self.num_levels
        for cls in (0, 1):
            arr = table[cls]
            if not isinstance(arr, list) or len(arr) != expected:
                logger.error("似然表大小错误 (cls=%d): 期望 %d, 实际 %d",
                           cls, expected, len(arr) if isinstance(arr, list) else -1)
                return False
            for idx, val in enumerate(arr):
                fval = _safe_float(val)
                if fval is None or not (0.0 <= fval <= 1.0):
                    logger.warning("似然值非法: cls=%d idx=%d val=%s", cls, idx, val)
                    return False
        return True

    def _normalize_likelihood(self) -> None:
        for i in range(self.num_factors):
            base = i * self.num_levels
            for cls in (0, 1):
                arr = self.likelihood[cls]
                total = sum(arr[base + l] for l in range(self.num_levels))
                if total > 0:
                    for l in range(self.num_levels):
                        arr[base + l] /= total
                else:
                    logger.warning("归一化时因子 %d 类别 %d 总和为0，重置", i, cls)
                    for l in range(self.num_levels):
                        arr[base + l] = 1.0 / self.num_levels

    def _enforce_table_size(self) -> None:
        expected = self.num_factors * self.num_levels
        for cls in (0, 1):
            if len(self.likelihood[cls]) != expected:
                logger.warning("似然表大小不匹配 (cls=%d)，重建", cls)
                self.likelihood[cls] = self._uniform_likelihood()

    def _auto_thresholds(self) -> List[float]:
        low, high = self._factor_range
        span = high - low
        step = span / self.num_levels
        return [low + step * (i + 1) for i in range(self.num_levels - 1)]

    @staticmethod
    def _parse_factor_range(value) -> tuple:
        if isinstance(value, (list, tuple)) and len(value) == 2:
            try:
                low, high = float(value[0]), float(value[1])
                if low < high:
                    return (low, high)
            except (TypeError, ValueError):
                pass
        logger.warning("factor_range 无效，使用默认")
        return DEFAULT_FACTOR_RANGE

    @staticmethod
    def _is_strictly_increasing(seq) -> bool:
        for i in range(1, len(seq)):
            try:
                if float(seq[i]) <= float(seq[i-1]):
                    return False
            except (TypeError, ValueError):
                return False
        return True

    @staticmethod
    def _parse_int(value, default, minimum, name):
        if value is None:
            return default
        if isinstance(value, bool):
            logger.warning("配置 %s 为布尔值，使用默认", name)
            return default
        fval = _safe_float(value)
        if fval is not None:
            try:
                ival = int(fval)
                if ival >= minimum:
                    return ival
            except (ValueError, OverflowError):
                pass
        logger.warning("配置 %s 非法 (%s)，使用默认 %d", name, value, default)
        return default

    def _emit_event(self, event_type: str, data: Dict):
        if self.event_bus and EVENT_BUS_AVAILABLE:
            try:
                evt = getattr(EventTypes, 'SYSTEM_ALERT', "system_alert")
                self.event_bus.publish(evt, {
                    "subtype": event_type,
                    "data": data,
                    "timestamp_ns": time.time_ns(),
                })
            except Exception:
                pass

    def _record_metrics(self, name: str, value: float, labels: Optional[Dict] = None):
        if METRICS_AVAILABLE and MetricsCollector:
            try:
                if labels:
                    MetricsCollector.gauge(name, value, labels=labels)
                else:
                    MetricsCollector.gauge(name, value)
            except Exception:
                pass

    # ── 健康检查 ──────────────────────────────────────────

    @classmethod
    def health_check(cls) -> Dict[str, Any]:
        try:
            calc = cls()
            test = [0.0] * calc.num_factors
            res = calc.compute_score(test)
            if not (0 <= res['probability'] <= 1):
                return {"status": "error", "reason": "概率越界"}
            calc.update_likelihood_table(test, 1)
            if not calc.validate_normalization():
                return {"status": "error", "reason": "归一化失败"}
            # 测试阈值设置
            calc.set_discretize_thresholds([-0.5, 0.5])
            return {"status": "ok", "reason": "自检通过"}
        except Exception as e:
            return {"status": "error", "reason": str(e)}
