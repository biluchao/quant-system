#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 假突破检测与恢复信号 (FalseBreakout) v14.0.0

核心职责：
1. 双向检测价格短暂穿越MA26容忍带后快速恢复的“假突破”形态
2. 维护潜在假突破状态，支持基于K线序列的实时判断与超时自动清理
3. 在满足方向感知的恢复条件时发出一次性带置信度的恢复信号
4. 完整的可观测性：事件发布（脱敏）、Prometheus指标与结构化日志

外部依赖：
- 无（纯逻辑模块，依赖传入的市场数据快照）
- core.event_bus.EventBus (可选) : 发布假突破/恢复事件
- core.metrics.MetricsCollector (可选) : 指标暴露

接口契约：
- update(market_data: Dict) -> None  每根K线喂入数据
- check_recovery_signal(market_data: Dict) -> Tuple[bool, float, str]
  返回 (信号是否有效, 置信度0-1, 方向 'long'/'short'/'')，信号为一次性消费
- set_thresholds(params: Dict) -> None  热重载参数
- reset() -> None  重置内部状态
- health_check() -> Dict[str, Any]

异常与降级：
- 传入数据缺失或非法时静默返回安全值
- 任何异常均被捕获并记录，不影响调用方
"""

import logging
import math
import threading
import time
from collections import deque
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

VERSION = "14.0.0"
SPDX_IDENTIFIER = "Apache-2.0"

# ── 可选依赖 ──────────────────────────────────────────────
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

# ── 常量 ──────────────────────────────────────────────────
DEFAULT_TOLERANCE_ATR_MULT = 0.3
DEFAULT_RECOVERY_BARS = 2
DEFAULT_RECOVERY_CONFIRM_COUNT = 3
DEFAULT_VOLUME_RATIO_THRESHOLD = 1.2
DEFAULT_LARGE_Z_LONG_THRESHOLD = 1.0
DEFAULT_LARGE_Z_SHORT_THRESHOLD = -1.0
DEFAULT_ANGLE_DEG_LONG_THRESHOLD = 5.0
DEFAULT_ANGLE_DEG_SHORT_THRESHOLD = 5.0
DEFAULT_BAYESIAN_SCORE_LONG_THRESHOLD = 0.60
DEFAULT_BAYESIAN_SCORE_SHORT_THRESHOLD = 0.40
MIN_RECOVERY_CONFIDENCE = 0.3
DEFAULT_CONFIDENCE_RANGE = 0.7
TOTAL_CONDITIONS = 4
MAX_HISTORY_BARS = 100
MAX_ATR_MULT = 10.0
MAX_RECOVERY_BARS = 20
MAX_RECOVERY_CONFIRM_COUNT = 4
MIN_VALID_ATR = 1e-12
MIN_TOLERANCE_ATR_MULT = 1e-6
MIN_CLOSE_PRICE = 1e-12
MAX_REASONABLE_PRICE = 1e9
HISTORY_EXTRA_BARS = 5                     # recovery_bars 的附加容量

# 方向常量
LONG = "long"
SHORT = "short"

# 内部ID键
ID_KEY = "__fb_id"

# 指标名称
METRIC_RECOVERY_TOTAL = "fb_recovery_total"


class FalseBreakoutDetector:
    """假突破检测器，识别短暂穿越MA26后恢复的形态（双向）"""

    def __init__(self, config: Optional[Dict] = None, event_bus=None):
        self.config = config or {}
        self.event_bus = event_bus

        # 通用参数（带严格边界和默认值）
        self.tolerance_atr_mult = self._clamp(
            self._safe_float(self.config.get('tolerance_atr_mult'), DEFAULT_TOLERANCE_ATR_MULT),
            MIN_TOLERANCE_ATR_MULT, MAX_ATR_MULT
        )
        self.recovery_bars = max(1, min(
            self._safe_int(self.config.get('recovery_bars'), DEFAULT_RECOVERY_BARS),
            MAX_RECOVERY_BARS
        ))
        self.recovery_confirm_count = max(1, min(
            self._safe_int(self.config.get('recovery_confirm_count'), DEFAULT_RECOVERY_CONFIRM_COUNT),
            MAX_RECOVERY_CONFIRM_COUNT
        ))
        # 成交量阈值必须为正，防止误触
        self.volume_ratio_threshold = max(0.0, self._safe_float(
            self.config.get('volume_ratio_threshold'), DEFAULT_VOLUME_RATIO_THRESHOLD
        ))
        # 多头恢复条件，强制取值范围
        self.large_z_long_threshold = max(0.0, self._safe_float(
            self.config.get('large_z_long_threshold'), DEFAULT_LARGE_Z_LONG_THRESHOLD
        ))
        self.angle_deg_long_threshold = max(0.0, self._safe_float(
            self.config.get('angle_deg_long_threshold'), DEFAULT_ANGLE_DEG_LONG_THRESHOLD
        ))
        self.bayesian_score_long_threshold = self._clamp(
            self._safe_float(self.config.get('bayesian_score_long_threshold'), DEFAULT_BAYESIAN_SCORE_LONG_THRESHOLD),
            0.5, 1.0
        )
        # 空头恢复条件
        self.large_z_short_threshold = min(0.0, self._safe_float(
            self.config.get('large_z_short_threshold'), DEFAULT_LARGE_Z_SHORT_THRESHOLD
        ))
        self.angle_deg_short_threshold = max(0.0, self._safe_float(
            self.config.get('angle_deg_short_threshold'), DEFAULT_ANGLE_DEG_SHORT_THRESHOLD
        ))
        self.bayesian_score_short_threshold = self._clamp(
            self._safe_float(self.config.get('bayesian_score_short_threshold'), DEFAULT_BAYESIAN_SCORE_SHORT_THRESHOLD),
            0.0, 0.5
        )
        # 置信度参数
        self.min_recovery_confidence = self._clamp(
            self._safe_float(self.config.get('min_recovery_confidence'), MIN_RECOVERY_CONFIDENCE),
            0.0, 1.0
        )
        self.confidence_range = self._clamp(
            self._safe_float(self.config.get('confidence_range'), DEFAULT_CONFIDENCE_RANGE),
            0.0, 1.0 - self.min_recovery_confidence
        )

        max_history = max(self.recovery_bars + HISTORY_EXTRA_BARS,
                          self._safe_int(self.config.get('max_history_bars'), MAX_HISTORY_BARS))

        # 内部状态
        self._lock = threading.RLock()
        self._history: deque = deque(maxlen=max_history)
        self._potential_long: Optional[Dict] = None
        self._potential_short: Optional[Dict] = None
        self._bar_id_counter: int = 0

        # 未知配置告警
        known = {'tolerance_atr_mult', 'recovery_bars', 'recovery_confirm_count',
                 'volume_ratio_threshold', 'large_z_long_threshold', 'angle_deg_long_threshold',
                 'bayesian_score_long_threshold', 'large_z_short_threshold', 'angle_deg_short_threshold',
                 'bayesian_score_short_threshold', 'min_recovery_confidence', 'confidence_range',
                 'max_history_bars'}
        unknown = set(self.config.keys()) - known
        if unknown:
            logger.warning("忽略未知配置键: %s", unknown)

        # 启动时参数合理性告警
        self._warn_on_suspicious_params()

        logger.info("FalseBreakoutDetector v%s 初始化, tol=%.2f, bars=%d, confirm=%d",
                    VERSION, self.tolerance_atr_mult, self.recovery_bars, self.recovery_confirm_count)

    def _warn_on_suspicious_params(self):
        if self.large_z_long_threshold < 0:
            logger.warning("large_z_long_threshold 应为正，当前值 %.2f", self.large_z_long_threshold)
        if self.large_z_short_threshold > 0:
            logger.warning("large_z_short_threshold 应为负，当前值 %.2f", self.large_z_short_threshold)
        if self.angle_deg_long_threshold == 0:
            logger.warning("angle_deg_long_threshold 为 0，可能导致频繁假恢复")
        if self.angle_deg_short_threshold == 0:
            logger.warning("angle_deg_short_threshold 为 0，可能导致频繁假恢复")
        if self.volume_ratio_threshold <= 0:
            logger.warning("volume_ratio_threshold 非正，成交量条件将无效")
        if self.recovery_confirm_count > TOTAL_CONDITIONS:
            logger.warning("recovery_confirm_count (%d) 超过总条件数 (%d)，信号将永不可能触发",
                           self.recovery_confirm_count, TOTAL_CONDITIONS)
        if self.min_recovery_confidence == 0 and self.confidence_range == 0:
            logger.warning("置信度将恒为 0，恢复信号无区分度")

    # ── 公共接口 ──────────────────────────────────────────

    def update(self, market_data: Dict) -> None:
        """喂入最新K线数据"""
        if not isinstance(market_data, dict):
            logger.debug("update 收到非字典数据，忽略")
            return
        close, ma26, atr = self._extract_required_fields(market_data)
        if close is None:
            return

        with self._lock:
            self._bar_id_counter += 1
            # 存储 bar 副本，保留关键字段（兼容 Decimal/numpy 等类型）
            bar = {k: v for k, v in market_data.items()}
            bar[ID_KEY] = self._bar_id_counter
            self._history.append(bar)

            self._expire_potential_long()
            self._expire_potential_short()

            if self._potential_long is None:
                self._check_potential_breakout(bar, LONG, close, ma26, atr)
            if self._potential_short is None:
                self._check_potential_breakout(bar, SHORT, close, ma26, atr)

    def check_recovery_signal(self, market_data: Dict) -> Tuple[bool, float, str]:
        """返回一次性恢复信号 (是否有效, 置信度, 方向)"""
        close, ma26, atr = self._extract_required_fields(market_data)
        if close is None:
            return False, 0.0, ""

        with self._lock:
            tolerance = self.tolerance_atr_mult * atr

            # 多头恢复
            if self._potential_long is not None and not self._is_breakout_expired(self._potential_long):
                if close > ma26 + tolerance:
                    conditions = self._count_recovery_conditions(market_data, LONG)
                    if conditions >= self.recovery_confirm_count:
                        conf = self._calc_confidence(conditions)
                        self._potential_long = None
                        self._emit_event("false_breakout_recovery", {"direction": LONG})
                        self._record_metrics(METRIC_RECOVERY_TOTAL, 1, {"direction": LONG})
                        return True, conf, LONG

            # 空头恢复
            if self._potential_short is not None and not self._is_breakout_expired(self._potential_short):
                if close < ma26 - tolerance:
                    conditions = self._count_recovery_conditions(market_data, SHORT)
                    if conditions >= self.recovery_confirm_count:
                        conf = self._calc_confidence(conditions)
                        self._potential_short = None
                        self._emit_event("false_breakout_recovery", {"direction": SHORT})
                        self._record_metrics(METRIC_RECOVERY_TOTAL, 1, {"direction": SHORT})
                        return True, conf, SHORT

            return False, 0.0, ""

    def set_thresholds(self, params: Dict[str, Any]) -> None:
        """热重载参数，自动校验合理性并告警"""
        with self._lock:
            updated_fields = []
            if 'tolerance_atr_mult' in params:
                self.tolerance_atr_mult = self._clamp(
                    self._safe_float(params['tolerance_atr_mult'], self.tolerance_atr_mult),
                    MIN_TOLERANCE_ATR_MULT, MAX_ATR_MULT
                )
                updated_fields.append('tolerance_atr_mult')
            if 'recovery_bars' in params:
                self.recovery_bars = max(1, min(
                    self._safe_int(params['recovery_bars'], self.recovery_bars),
                    MAX_RECOVERY_BARS
                ))
                self._history = deque(self._history, maxlen=max(self._history.maxlen, self.recovery_bars + HISTORY_EXTRA_BARS))
                updated_fields.append('recovery_bars')
            if 'recovery_confirm_count' in params:
                self.recovery_confirm_count = max(1, min(
                    self._safe_int(params['recovery_confirm_count'], self.recovery_confirm_count),
                    MAX_RECOVERY_CONFIRM_COUNT
                ))
                if self.recovery_confirm_count > TOTAL_CONDITIONS:
                    logger.warning("recovery_confirm_count (%d) 超过总条件数 (%d)，信号将永远无法触发",
                                   self.recovery_confirm_count, TOTAL_CONDITIONS)
                updated_fields.append('recovery_confirm_count')
            if 'volume_ratio_threshold' in params:
                val = self._safe_float(params['volume_ratio_threshold'], self.volume_ratio_threshold)
                self.volume_ratio_threshold = max(0.0, val)
                updated_fields.append('volume_ratio_threshold')
            if 'large_z_long_threshold' in params:
                val = self._safe_float(params['large_z_long_threshold'], self.large_z_long_threshold)
                self.large_z_long_threshold = max(0.0, val)
                if val < 0:
                    logger.warning("large_z_long_threshold 被强制设为 0")
                updated_fields.append('large_z_long_threshold')
            if 'angle_deg_long_threshold' in params:
                val = self._safe_float(params['angle_deg_long_threshold'], self.angle_deg_long_threshold)
                self.angle_deg_long_threshold = max(0.0, val)
                if val < 0:
                    logger.warning("angle_deg_long_threshold 被强制设为 0")
                updated_fields.append('angle_deg_long_threshold')
            if 'bayesian_score_long_threshold' in params:
                val = self._safe_float(params['bayesian_score_long_threshold'], self.bayesian_score_long_threshold)
                self.bayesian_score_long_threshold = self._clamp(val, 0.5, 1.0)
                updated_fields.append('bayesian_score_long_threshold')
            if 'large_z_short_threshold' in params:
                val = self._safe_float(params['large_z_short_threshold'], self.large_z_short_threshold)
                self.large_z_short_threshold = min(0.0, val)
                if val > 0:
                    logger.warning("large_z_short_threshold 被强制设为 0")
                updated_fields.append('large_z_short_threshold')
            if 'angle_deg_short_threshold' in params:
                val = self._safe_float(params['angle_deg_short_threshold'], self.angle_deg_short_threshold)
                self.angle_deg_short_threshold = max(0.0, val)
                if val < 0:
                    logger.warning("angle_deg_short_threshold 被强制设为 0")
                updated_fields.append('angle_deg_short_threshold')
            if 'bayesian_score_short_threshold' in params:
                val = self._safe_float(params['bayesian_score_short_threshold'], self.bayesian_score_short_threshold)
                self.bayesian_score_short_threshold = self._clamp(val, 0.0, 0.5)
                updated_fields.append('bayesian_score_short_threshold')
            if 'min_recovery_confidence' in params:
                new_min = self._clamp(
                    self._safe_float(params['min_recovery_confidence'], self.min_recovery_confidence),
                    0.0, 1.0
                )
                if new_min != self.min_recovery_confidence:
                    self.min_recovery_confidence = new_min
                    if self.confidence_range > 1.0 - self.min_recovery_confidence:
                        old_range = self.confidence_range
                        self.confidence_range = 1.0 - self.min_recovery_confidence
                        logger.info("confidence_range 由 %.4f 自动调整为 %.4f", old_range, self.confidence_range)
                    updated_fields.append('min_recovery_confidence')
            if 'confidence_range' in params:
                new_range = self._clamp(
                    self._safe_float(params['confidence_range'], self.confidence_range),
                    0.0, 1.0 - self.min_recovery_confidence
                )
                if new_range != self.confidence_range:
                    self.confidence_range = new_range
                    updated_fields.append('confidence_range')
            if 'max_history_bars' in params:
                new_max = max(self.recovery_bars + HISTORY_EXTRA_BARS,
                              self._safe_int(params['max_history_bars'], self._history.maxlen))
                self._history = deque(self._history, maxlen=new_max)
                updated_fields.append('max_history_bars')
            # 检查未知键
            known = {'tolerance_atr_mult', 'recovery_bars', 'recovery_confirm_count',
                     'volume_ratio_threshold', 'large_z_long_threshold', 'angle_deg_long_threshold',
                     'bayesian_score_long_threshold', 'large_z_short_threshold', 'angle_deg_short_threshold',
                     'bayesian_score_short_threshold', 'min_recovery_confidence', 'confidence_range',
                     'max_history_bars'}
            unknown = set(params.keys()) - known
            if unknown:
                logger.warning("忽略未知配置键: %s", unknown)
            if updated_fields:
                logger.info("假突破检测器参数已更新: %s", ', '.join(updated_fields))

    def reset(self) -> None:
        with self._lock:
            self._history.clear()
            self._potential_long = None
            self._potential_short = None
            self._bar_id_counter = 0
        logger.debug("假突破检测器已重置")

    @classmethod
    def health_check(cls) -> Dict[str, Any]:
        warnings = []
        try:
            detector = cls(event_bus=None)
            # 多头假突破测试
            detector.update({'close': 98.0, 'ma26': 100.0, 'atr': 2.0,
                             'volume_ratio': 0.8, 'large_z': 0.5,
                             'ma_angle_deg': -1.0, 'bayesian_score': 0.5})
            detector.update({'close': 101.0, 'ma26': 100.0, 'atr': 2.0,
                             'volume_ratio': 1.5, 'large_z': 1.2,
                             'ma_angle_deg': 6.0, 'bayesian_score': 0.65})
            s1, c1, d1 = detector.check_recovery_signal({'close': 101.0, 'ma26': 100.0, 'atr': 2.0,
                                                         'volume_ratio': 1.5, 'large_z': 1.2,
                                                         'ma_angle_deg': 6.0, 'bayesian_score': 0.65})
            if not s1 or d1 != LONG or not (0 <= c1 <= 1):
                warnings.append("多头恢复未触发信号或置信度异常")
            s2, _, _ = detector.check_recovery_signal({'close': 101.0, 'ma26': 100.0, 'atr': 2.0,
                                                        'volume_ratio': 1.5, 'large_z': 1.2,
                                                        'ma_angle_deg': 6.0, 'bayesian_score': 0.65})
            if s2:
                warnings.append("多头信号未一次性消费")

            # 空头假突破测试
            detector.reset()
            detector.update({'close': 102.0, 'ma26': 100.0, 'atr': 2.0,
                             'volume_ratio': 0.8, 'large_z': -0.5,
                             'ma_angle_deg': 1.0, 'bayesian_score': 0.5})
            detector.update({'close': 99.0, 'ma26': 100.0, 'atr': 2.0,
                             'volume_ratio': 1.5, 'large_z': -1.5,
                             'ma_angle_deg': -6.0, 'bayesian_score': 0.3})
            s3, c3, d3 = detector.check_recovery_signal({'close': 99.0, 'ma26': 100.0, 'atr': 2.0,
                                                         'volume_ratio': 1.5, 'large_z': -1.5,
                                                         'ma_angle_deg': -6.0, 'bayesian_score': 0.3})
            if not s3 or d3 != SHORT or not (0 <= c3 <= 1):
                warnings.append("空头恢复未触发信号或置信度异常")
            s4, _, _ = detector.check_recovery_signal({'close': 99.0, 'ma26': 100.0, 'atr': 2.0,
                                                        'volume_ratio': 1.5, 'large_z': -1.5,
                                                        'ma_angle_deg': -6.0, 'bayesian_score': 0.3})
            if s4:
                warnings.append("空头信号未一次性消费")

            # 测试非法值
            s5, _, _ = detector.check_recovery_signal({'close': float('nan'), 'ma26': 100.0, 'atr': 2.0})
            if s5:
                warnings.append("NaN值未正确处理")
            s6, _, _ = detector.check_recovery_signal({'close': 101.0, 'ma26': 100.0, 'atr': 0.0})
            if s6:
                warnings.append("ATR=0未正确处理")
            s7, _, _ = detector.check_recovery_signal({'close': -1.0, 'ma26': 100.0, 'atr': 2.0})
            if s7:
                warnings.append("负价格未正确处理")
            # 测试极大值
            s8, _, _ = detector.check_recovery_signal({'close': 1e12, 'ma26': 100.0, 'atr': 2.0})
            if s8:
                warnings.append("极大价格未正确处理")
            # 测试参数热重载
            detector.set_thresholds({'recovery_confirm_count': 4, 'min_recovery_confidence': 0.5,
                                     'volume_ratio_threshold': 2.0})
            if detector.recovery_confirm_count != 4 or detector.min_recovery_confidence != 0.5 or detector.volume_ratio_threshold != 2.0:
                warnings.append("热重载失败")
        except Exception as e:
            warnings.append(f"自检异常: {e}")
        status = "degraded" if warnings else "ok"
        return {"status": status, "reason": f"FalseBreakoutDetector v{VERSION}", "warnings": warnings}

    # ── 内部方法 ──────────────────────────────────────────

    @staticmethod
    def _extract_required_fields(data: Dict) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        close = FalseBreakoutDetector._safe_float(data.get('close'), None)
        ma26 = FalseBreakoutDetector._safe_float(data.get('ma26'), None)
        atr = FalseBreakoutDetector._safe_float(data.get('atr'), None)
        if close is None or ma26 is None or atr is None or atr <= MIN_VALID_ATR:
            return None, None, None
        if not (MIN_CLOSE_PRICE <= close <= MAX_REASONABLE_PRICE):
            return None, None, None
        if not (MIN_CLOSE_PRICE <= ma26 <= MAX_REASONABLE_PRICE):
            return None, None, None
        return close, ma26, atr

    @staticmethod
    def _safe_float(value: Any, default: Optional[float] = 0.0) -> Optional[float]:
        """安全转换为有限浮点数，兼容 Decimal/numpy 等类型，排除布尔型"""
        if value is None:
            return default
        # 排除 Python 布尔
        if isinstance(value, bool):
            return default
        # 排除 numpy 布尔 (dtype == bool)
        if hasattr(value, 'dtype') and str(getattr(value, 'dtype', '')) == 'bool':
            return default
        try:
            fval = float(value)
            if math.isfinite(fval):
                return fval
        except (ValueError, TypeError, OverflowError):
            pass
        return default

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, float) and not math.isfinite(value):
                return default
            return int(float(value))
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _clamp(value: float, low: float, high: float) -> float:
        return max(low, min(high, value))

    def _get_bar_id(self, bar: Dict) -> Optional[int]:
        return bar.get(ID_KEY)

    def _expire_potential_long(self):
        self._expire_potential(self._potential_long, '_potential_long')

    def _expire_potential_short(self):
        self._expire_potential(self._potential_short, '_potential_short')

    def _expire_potential(self, potential: Optional[Dict], attr: str):
        if potential is None:
            return
        trigger_id = self._get_bar_id(potential.get('trigger_bar', {}))
        if trigger_id is None:
            setattr(self, attr, None)
            return
        trigger_idx = self._find_bar_index_by_id(trigger_id)
        if trigger_idx == -1:
            setattr(self, attr, None)
            return
        bars_passed = len(self._history) - trigger_idx - 1
        if bars_passed > self.recovery_bars:
            setattr(self, attr, None)

    def _is_breakout_expired(self, potential: Dict) -> bool:
        if potential is None:
            return True
        trigger_id = self._get_bar_id(potential.get('trigger_bar', {}))
        if trigger_id is None:
            return True
        trigger_idx = self._find_bar_index_by_id(trigger_id)
        if trigger_idx == -1:
            return True
        bars_passed = len(self._history) - trigger_idx - 1
        return bars_passed > self.recovery_bars

    def _find_bar_index_by_id(self, bar_id: int) -> int:
        for i, b in enumerate(self._history):
            if self._get_bar_id(b) == bar_id:
                return i
        return -1

    def _check_potential_breakout(self, bar: Dict, direction: str,
                                  close: float, ma26: float, atr: float):
        """使用已验证的close/ma26/atr检测潜在假突破"""
        tolerance = self.tolerance_atr_mult * atr
        if direction == LONG:
            if close < ma26 - tolerance:
                self._potential_long = {'trigger_bar': bar}
                logger.debug("潜在假突破[long]")
                self._emit_event("potential_false_breakout", {"direction": LONG})
        elif direction == SHORT:
            if close > ma26 + tolerance:
                self._potential_short = {'trigger_bar': bar}
                logger.debug("潜在假突破[short]")
                self._emit_event("potential_false_breakout", {"direction": SHORT})

    def _count_recovery_conditions(self, bar: Dict, direction: str) -> int:
        """根据方向统计恢复条件满足数（总共4个）"""
        count = 0
        vol = self._safe_float(bar.get('volume_ratio'))
        if vol > self.volume_ratio_threshold:
            count += 1
        lz = self._safe_float(bar.get('large_z'))
        if direction == LONG and lz > self.large_z_long_threshold:
            count += 1
        elif direction == SHORT and lz < self.large_z_short_threshold:
            count += 1
        ang = self._safe_float(bar.get('ma_angle_deg'))
        if direction == LONG and ang > self.angle_deg_long_threshold:
            count += 1
        elif direction == SHORT and ang < -self.angle_deg_short_threshold:
            count += 1
        bayes = self._safe_float(bar.get('bayesian_score'))
        if direction == LONG and bayes > self.bayesian_score_long_threshold:
            count += 1
        elif direction == SHORT and bayes < self.bayesian_score_short_threshold:
            count += 1
        return count

    def _calc_confidence(self, conditions_met: int) -> float:
        ratio = conditions_met / float(TOTAL_CONDITIONS)
        conf = self.min_recovery_confidence + ratio * self.confidence_range
        return self._clamp(conf, 0.0, 1.0)

    def _emit_event(self, event_type: str, data: Dict):
        if self.event_bus:
            try:
                evt_type = getattr(EventTypes, 'SYSTEM_ALERT', 'system_alert')
                self.event_bus.publish(
                    evt_type,
                    {"subtype": event_type, "data": data, "timestamp_ns": time.time_ns()}
                )
            except Exception as e:
                logger.debug("事件发布失败: %s", e)

    def _record_metrics(self, name: str, value: float, labels: Optional[Dict] = None):
        if METRICS_AVAILABLE and MetricsCollector:
            try:
                if labels:
                    MetricsCollector.counter(name, value, labels)
                else:
                    MetricsCollector.counter(name, value)
            except Exception:
                pass
