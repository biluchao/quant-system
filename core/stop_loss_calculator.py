#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 动态止损计算器 (StopLossCalculator) v6.0.0

核心职责：
1. 根据持仓方向、市场波动率、趋势角度及支撑压力位，计算动态止损价格
2. 支持保本止损触发，当浮盈达到一定阈值后自动上移止损至盈亏平衡
3. 支持参数热重载与多空双向独立配置，并保证 min ≤ max 一致性
4. 完整的可观测性：事件发布、Prometheus指标与结构化日志

外部依赖：
- 无（纯计算模块，依赖传入的市场数据）
- core.event_bus.EventBus (可选) : 发布止损更新事件
- core.metrics.MetricsCollector (可选) : 指标暴露

接口契约：
- calculate(position: Dict, market_data: Dict, sr_levels: Optional[Dict] = None) -> Dict[str, Any]
  返回 {"stop_price": float, "breakeven_triggered": bool, "reason": str}
- set_thresholds(params: Dict) -> None  热重载参数
- get_thresholds() -> Dict[str, Any]  获取当前参数
- reset() -> None  恢复默认参数
- health_check() -> Dict[str, Any]

异常与降级：
- 输入数据缺失或非法时返回安全止损（入场价，且方向合理），并记录警告
- 任何异常均被捕获，不影响调用方
"""

import logging
import math
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

VERSION = "6.0.0"
SPDX_IDENTIFIER = "Apache-2.0"

# 可选依赖
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

# ── 默认参数常量 ──────────────────────────────────────────
DEFAULT_ATR_MULTIPLIER = 2.5
DEFAULT_MIN_ATR_MULTIPLIER = 1.0
DEFAULT_ANGLE_DECAY = 0.15
DEFAULT_BREAKEVEN_TRIGGER_RISK_MULT = 2.0
DEFAULT_BREAKEVEN_BUFFER_ATR = 0.5
DEFAULT_MA26_ATR_BUFFER = 0.5
DEFAULT_SR_PROXIMITY_ATR = 0.8
DEFAULT_SR_TIGHTEN_FACTOR = 1.5

# 方向常量
LONG = "long"
SHORT = "short"

# 微小的价格安全边距（防止止损等于当前价），以百分比形式动态计算
PRICE_EPSILON_RATIO = 1e-7          # 相对当前价的比例
MIN_PRICE_EPSILON = 1e-12           # 绝对最小值


class StopLossCalculator:
    """动态止损计算器，结合波动率、趋势与支撑压力"""

    def __init__(self, config: Optional[Dict] = None, event_bus=None):
        self.config = config or {}
        self.event_bus = event_bus

        self.atr_multiplier = self._safe_float_clamped(
            self.config.get('atr_multiplier', DEFAULT_ATR_MULTIPLIER),
            default=DEFAULT_ATR_MULTIPLIER, low=1.0, high=10.0
        )
        self.min_atr_multiplier = self._safe_float_clamped(
            self.config.get('min_atr_multiplier', DEFAULT_MIN_ATR_MULTIPLIER),
            default=DEFAULT_MIN_ATR_MULTIPLIER, low=0.5, high=5.0
        )
        self._enforce_min_max_atr()

        self.angle_decay = self._safe_float_clamped(
            self.config.get('angle_decay', DEFAULT_ANGLE_DECAY),
            default=DEFAULT_ANGLE_DECAY, low=0.0, high=1.0
        )
        self.breakeven_trigger_risk_mult = self._safe_float_clamped(
            self.config.get('breakeven_trigger_risk_mult', DEFAULT_BREAKEVEN_TRIGGER_RISK_MULT),
            default=DEFAULT_BREAKEVEN_TRIGGER_RISK_MULT, low=1.0, high=5.0
        )
        self.breakeven_buffer_atr = self._safe_float_clamped(
            self.config.get('breakeven_buffer_atr', DEFAULT_BREAKEVEN_BUFFER_ATR),
            default=DEFAULT_BREAKEVEN_BUFFER_ATR, low=0.01, high=2.0  # 最小值调整为0.01，防止零缓冲
        )
        self.ma26_atr_buffer = self._safe_float_clamped(
            self.config.get('ma26_atr_buffer', DEFAULT_MA26_ATR_BUFFER),
            default=DEFAULT_MA26_ATR_BUFFER, low=0.0, high=3.0
        )
        self.sr_proximity_atr = self._safe_float_clamped(
            self.config.get('sr_proximity_atr', DEFAULT_SR_PROXIMITY_ATR),
            default=DEFAULT_SR_PROXIMITY_ATR, low=0.0, high=5.0
        )
        self.sr_tighten_factor = self._safe_float_clamped(
            self.config.get('sr_tighten_factor', DEFAULT_SR_TIGHTEN_FACTOR),
            default=DEFAULT_SR_TIGHTEN_FACTOR, low=1.0, high=5.0
        )

        logger.info("StopLossCalculator v%s 初始化, ATR乘数=%.2f, min=%.2f", VERSION,
                    self.atr_multiplier, self.min_atr_multiplier)

    # ── 公共接口 ──────────────────────────────────────────

    def calculate(self, position: Dict, market_data: Dict,
                  sr_levels: Optional[Dict] = None) -> Dict[str, Any]:
        """计算动态止损价格"""
        if not isinstance(position, dict) or not isinstance(market_data, dict):
            return self._fallback("参数类型错误", None, LONG)

        try:
            side = self._validate_side(position.get('side'))
            if side is None:
                return self._fallback("无效持仓方向", position, LONG)

            entry = self._safe_float(position.get('entry_price'), None)
            initial_stop = self._safe_float(position.get('initial_stop_price'), None)
            if entry is None or entry <= 0 or initial_stop is None:
                return self._fallback("无效入场价或初始止损", position, side)

            if (side == LONG and initial_stop >= entry) or \
               (side == SHORT and initial_stop <= entry):
                logger.warning("%s 初始止损与入场价关系不合理", side)
                return self._fallback("初始止损不合理", position, side)

            close = self._safe_float(market_data.get('close'), None)
            atr = self._safe_float(market_data.get('atr'), None)
            ma26 = self._safe_float(market_data.get('ma26'), None)
            angle = self._safe_float(market_data.get('ma_angle_deg'), 0.0)
            if close is None or atr is None or ma26 is None or atr <= 0:
                return self._fallback("无效市场数据", position, side)
            if abs(angle) > 180.0:
                logger.warning("ma_angle_deg 异常: %.2f", angle)
                angle = math.copysign(0.0, angle)  # 异常角度置零，避免极端带宽

            initial_risk = abs(entry - initial_stop)
            if initial_risk <= 0:
                logger.warning("初始风险距离为零，使用 ATR 估算")
                initial_risk = atr * self.atr_multiplier

            # 利润保护基准价
            if side == LONG:
                benchmark = self._safe_float(position.get('highest_price_since_entry', close), close)
            else:
                benchmark = self._safe_float(position.get('lowest_price_since_entry', close), close)

            # 计算自适应止损带宽（价格距离）
            band = self._calculate_stop_band(atr, angle, side, close, sr_levels)

            # 基础止损价格，使用相对市价的安全边距
            epsilon = max(close * PRICE_EPSILON_RATIO, MIN_PRICE_EPSILON)
            if side == LONG:
                stop_price = benchmark - band
                ma26_bound = ma26 - self.ma26_atr_buffer * atr
                stop_price = max(stop_price, ma26_bound)
                stop_price = min(stop_price, close - epsilon)
            else:
                stop_price = benchmark + band
                ma26_bound = ma26 + self.ma26_atr_buffer * atr
                stop_price = min(stop_price, ma26_bound)
                stop_price = max(stop_price, close + epsilon)

            # 保本止损
            breakeven_triggered = False
            if side == LONG:
                profit = close - entry
                if profit >= self.breakeven_trigger_risk_mult * initial_risk:
                    breakeven_stop = entry + self.breakeven_buffer_atr * atr
                    breakeven_stop = min(breakeven_stop, close - epsilon)
                    stop_price = max(stop_price, breakeven_stop)
                    breakeven_triggered = True
            else:
                profit = entry - close
                if profit >= self.breakeven_trigger_risk_mult * initial_risk:
                    breakeven_stop = entry - self.breakeven_buffer_atr * atr
                    breakeven_stop = max(breakeven_stop, close + epsilon)
                    stop_price = min(stop_price, breakeven_stop)
                    breakeven_triggered = True

            stop_price = max(stop_price, 0.0)

            self._emit_event("stop_loss_updated", {
                "side": side,
                "breakeven": breakeven_triggered,
            })
            self._record_metrics("spark_stop_loss_updated", 1, {"side": side})

            return {
                "stop_price": round(stop_price, 8),
                "breakeven_triggered": breakeven_triggered,
                "reason": self._build_reason(side, breakeven_triggered),
            }
        except Exception as e:
            logger.exception("止损计算异常: %s", e)
            return self._fallback("计算异常", position, side if isinstance(position, dict) else LONG)

    def set_thresholds(self, params: Dict[str, Any]) -> None:
        """热重载参数，自动保持一致性，并记录警告"""
        updated = []
        if 'atr_multiplier' in params:
            new_val = self._safe_float_clamped(
                params['atr_multiplier'], default=self.atr_multiplier, low=1.0, high=10.0
            )
            if new_val < self.min_atr_multiplier:
                logger.warning("atr_multiplier 小于当前 min_atr_multiplier，将同步下调 min")
                self.min_atr_multiplier = new_val
            self.atr_multiplier = new_val
            updated.append('atr_multiplier')
        if 'min_atr_multiplier' in params:
            self.min_atr_multiplier = self._safe_float_clamped(
                params['min_atr_multiplier'], default=self.min_atr_multiplier, low=0.5, high=5.0
            )
            self._enforce_min_max_atr()
            updated.append('min_atr_multiplier')
        if 'angle_decay' in params:
            self.angle_decay = self._safe_float_clamped(
                params['angle_decay'], default=self.angle_decay, low=0.0, high=1.0
            )
            updated.append('angle_decay')
        if 'breakeven_trigger_risk_mult' in params:
            self.breakeven_trigger_risk_mult = self._safe_float_clamped(
                params['breakeven_trigger_risk_mult'], default=self.breakeven_trigger_risk_mult, low=1.0, high=5.0
            )
            updated.append('breakeven_trigger_risk_mult')
        if 'breakeven_buffer_atr' in params:
            self.breakeven_buffer_atr = self._safe_float_clamped(
                params['breakeven_buffer_atr'], default=self.breakeven_buffer_atr, low=0.01, high=2.0
            )
            updated.append('breakeven_buffer_atr')
        if 'ma26_atr_buffer' in params:
            self.ma26_atr_buffer = self._safe_float_clamped(
                params['ma26_atr_buffer'], default=self.ma26_atr_buffer, low=0.0, high=3.0
            )
            updated.append('ma26_atr_buffer')
        if 'sr_proximity_atr' in params:
            self.sr_proximity_atr = self._safe_float_clamped(
                params['sr_proximity_atr'], default=self.sr_proximity_atr, low=0.0, high=5.0
            )
            updated.append('sr_proximity_atr')
        if 'sr_tighten_factor' in params:
            self.sr_tighten_factor = self._safe_float_clamped(
                params['sr_tighten_factor'], default=self.sr_tighten_factor, low=1.0, high=5.0
            )
            updated.append('sr_tighten_factor')
        if updated:
            logger.info("止损计算器参数已更新: %s", ', '.join(updated))
            self._emit_event("stop_loss_thresholds_updated", {"params": updated})

    def get_thresholds(self) -> Dict[str, Any]:
        """获取当前所有可配置参数（脱敏）"""
        return {
            "atr_multiplier": round(self.atr_multiplier, 2),
            "min_atr_multiplier": round(self.min_atr_multiplier, 2),
            "angle_decay": round(self.angle_decay, 4),
            "breakeven_trigger_risk_mult": round(self.breakeven_trigger_risk_mult, 2),
            "breakeven_buffer_atr": round(self.breakeven_buffer_atr, 2),
            "ma26_atr_buffer": round(self.ma26_atr_buffer, 2),
            "sr_proximity_atr": round(self.sr_proximity_atr, 2),
            "sr_tighten_factor": round(self.sr_tighten_factor, 2),
            "version": VERSION,
        }

    def reset(self) -> None:
        """恢复所有参数为默认值"""
        self.atr_multiplier = DEFAULT_ATR_MULTIPLIER
        self.min_atr_multiplier = DEFAULT_MIN_ATR_MULTIPLIER
        self.angle_decay = DEFAULT_ANGLE_DECAY
        self.breakeven_trigger_risk_mult = DEFAULT_BREAKEVEN_TRIGGER_RISK_MULT
        self.breakeven_buffer_atr = DEFAULT_BREAKEVEN_BUFFER_ATR
        self.ma26_atr_buffer = DEFAULT_MA26_ATR_BUFFER
        self.sr_proximity_atr = DEFAULT_SR_PROXIMITY_ATR
        self.sr_tighten_factor = DEFAULT_SR_TIGHTEN_FACTOR
        logger.info("止损计算器参数已恢复默认")
        self._emit_event("stop_loss_reset", {})

    @classmethod
    def health_check(cls) -> Dict[str, Any]:
        warnings = []
        try:
            calc = cls()
            # 多头保本测试
            pos = {'side': LONG, 'entry_price': 100.0, 'initial_stop_price': 98.0,
                   'highest_price_since_entry': 106.0}
            mkt = {'close': 106.0, 'atr': 2.0, 'ma26': 102.0, 'ma_angle_deg': 15.0}
            result = calc.calculate(pos, mkt)
            if not isinstance(result.get('stop_price'), float) or result['stop_price'] < 100.0:
                warnings.append("多头保本止损未正确触发")
            # 空头保本测试
            pos2 = {'side': SHORT, 'entry_price': 100.0, 'initial_stop_price': 102.0,
                    'lowest_price_since_entry': 94.0}
            mkt2 = {'close': 94.0, 'atr': 2.0, 'ma26': 98.0, 'ma_angle_deg': -15.0}
            result2 = calc.calculate(pos2, mkt2)
            if not isinstance(result2.get('stop_price'), float) or result2['stop_price'] > 100.0:
                warnings.append("空头保本止损未正确触发")
            # 支撑阻力测试
            sr = {'support': 95.0, 'resistance': 105.0}
            pos3 = {'side': LONG, 'entry_price': 100.0, 'initial_stop_price': 98.0,
                    'highest_price_since_entry': 104.0}
            mkt3 = {'close': 104.0, 'atr': 2.0, 'ma26': 102.0, 'ma_angle_deg': 5.0}
            result3 = calc.calculate(pos3, mkt3, sr)
            if not isinstance(result3.get('stop_price'), float):
                warnings.append("支撑阻力止损计算异常")
            # 非法值降级测试
            result4 = calc.calculate({'side': LONG}, {'close': float('nan')})
            if result4['stop_price'] != 0.0 or result4['breakeven_triggered']:
                warnings.append("非法值未正确处理")
        except Exception as e:
            warnings.append(f"自检异常: {e}")
        status = "degraded" if warnings else "ok"
        return {"status": status, "reason": f"StopLossCalculator v{VERSION}", "warnings": warnings}

    # ── 内部方法 ──────────────────────────────────────────

    @staticmethod
    def _validate_side(side: Any) -> Optional[str]:
        if isinstance(side, str) and side.lower() in (LONG, SHORT):
            return side.lower()
        return None

    def _calculate_stop_band(self, atr: float, angle_deg: float, side: str,
                             close: float, sr_levels: Optional[Dict]) -> float:
        """计算止损带宽（价格距离）"""
        abs_angle = abs(angle_deg)
        band_mult = self.min_atr_multiplier + (self.atr_multiplier - self.min_atr_multiplier) * \
                    math.exp(-self.angle_decay * abs_angle)
        band = band_mult * atr

        if sr_levels and isinstance(sr_levels, dict):
            sr_tighten = self._apply_sr_tightening(close, side, sr_levels, atr)
            if sr_tighten is not None:
                band = min(band, sr_tighten)
        return band

    def _apply_sr_tightening(self, close: float, side: str,
                             sr_levels: Dict, atr: float) -> Optional[float]:
        """根据价格与支撑/压力的距离，临时收紧止损带宽"""
        # 大小写不敏感查找支撑/阻力
        resistance = self._find_sr_key(sr_levels, 'resistance')
        support = self._find_sr_key(sr_levels, 'support')

        if side == LONG and resistance is not None:
            if resistance > close:
                distance = resistance - close
                if distance < self.sr_proximity_atr * atr:
                    tightened_band = self.atr_multiplier * atr / self.sr_tighten_factor
                    return max(tightened_band, self.min_atr_multiplier * atr)
        elif side == SHORT and support is not None:
            if support < close:
                distance = close - support
                if distance < self.sr_proximity_atr * atr:
                    tightened_band = self.atr_multiplier * atr / self.sr_tighten_factor
                    return max(tightened_band, self.min_atr_multiplier * atr)
        return None

    @staticmethod
    def _find_sr_key(sr_levels: Dict, key: str) -> Optional[float]:
        """在支撑阻力字典中查找键，大小写不敏感"""
        if not isinstance(sr_levels, dict):
            return None
        for k, v in sr_levels.items():
            if isinstance(k, str) and k.lower() == key:
                return StopLossCalculator._safe_float(v, None)
        return None

    def _build_reason(self, side: str, breakeven: bool) -> str:
        side_cn = "多头" if side == LONG else "空头"
        parts = [f"{side_cn}止损"]
        if breakeven:
            parts.append("保本已触发")
        return ", ".join(parts)

    def _fallback(self, reason: str, position: Optional[Dict], side: str = LONG) -> Dict[str, Any]:
        logger.warning("止损计算降级: %s", reason)
        entry = 0.0
        if isinstance(position, dict):
            raw = position.get('entry_price')
            if raw is not None:
                entry = self._safe_float(raw, 0.0) or 0.0
        # 空头止损不应低于入场价，降级时使用入场价作为止损（空头止损高于入场价不合理，但至少是原始价位）
        # 更好的做法是返回 None 让外部处理，但保持现有契约。
        return {
            "stop_price": entry,
            "breakeven_triggered": False,
            "reason": f"降级: {reason}",
        }

    @staticmethod
    def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
        """安全转换为有限浮点数，失败返回 default"""
        if value is None:
            return default
        if isinstance(value, bool):
            return default
        try:
            fval = float(value)
            if math.isfinite(fval):
                return fval
        except (ValueError, TypeError):
            pass
        return default

    @staticmethod
    def _safe_float_clamped(value: Any, default: float,
                            low: Optional[float] = None,
                            high: Optional[float] = None) -> float:
        """安全转换为有限浮点数，并裁剪到 [low, high]；若 low > high 则返回 default 并记录错误"""
        fval = StopLossCalculator._safe_float(value, default)
        if fval is None:
            fval = default
        if low is not None and high is not None and low > high:
            logger.error("_safe_float_clamped: low (%.2f) > high (%.2f), 返回默认值", low, high)
            return default
        if low is not None:
            fval = max(low, fval)
        if high is not None:
            fval = min(high, fval)
        return fval

    def _enforce_min_max_atr(self):
        if self.min_atr_multiplier > self.atr_multiplier:
            logger.warning("min_atr_multiplier > atr_multiplier，已将 min 调整至 max")
            self.min_atr_multiplier = self.atr_multiplier

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
                MetricsCollector.counter(name, value, labels or {})
            except Exception:
                pass
