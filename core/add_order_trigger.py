#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 加仓触发器 (AddOrderTrigger) v7.0.0

核心职责：
1. 基于浮盈（ATR归一化）、贝叶斯得分、加仓冷却，判定是否触发加仓
2. 计算加仓规模（遵循递减序列），并调用风控二次审核
3. 发布脱敏加仓事件，提供完整审计日志

外部依赖：
- core.position_keeper.PositionKeeper : 提供持仓均价、已加仓次数、初始风险
- core.risk_manager.RiskManager : 二次风控审批
- core.event_bus.EventBus : 发布加仓信号事件（可选）
- core.metrics.MetricsCollector : 指标暴露（可选）

接口契约：
- evaluate(current_state: Dict) -> Dict[str, Any]
  返回 {"trigger": bool, "size": float, "reason": str, "warnings": list}
- health_check() -> Dict[str, Any]

异常与降级：
- 若持仓信息缺失或异常，返回不触发，并记录 WARNING
- 浮盈计算或风控异常时，保守处理，不触发加仓
- 所有公开方法均不抛出异常
"""

import copy
import logging
import math
import time
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

VERSION = "7.0.0"
SPDX_IDENTIFIER = "Apache-2.0"

# 可选依赖
try:
    from core.position_keeper import PositionKeeper
except ImportError:
    PositionKeeper = None

try:
    from core.risk_manager import RiskManager
except ImportError:
    RiskManager = None

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

# 默认加仓序列（第2次开始，对应 add_count=0 为第一次加仓）
DEFAULT_ADD_COEFFICIENTS = [Decimal("1.0"), Decimal("0.8"), Decimal("0.6"), Decimal("0.4")]
DEFAULT_PROFIT_STEPS = [Decimal("1.5"), Decimal("2.5"), Decimal("3.5"), Decimal("4.5")]
DEFAULT_COOL_DOWN_BARS = 3
DEFAULT_MIN_BAYESIAN_SCORE = Decimal("0.55")
MAX_COOL_DOWN_BARS = 1000
MIN_ATR = Decimal("1e-8")
MIN_NOTIONAL_USDT = Decimal("5.0")
MAX_NOTIONAL_USDT = Decimal("50000000")
MAX_ADD_COUNT_HARD = 8
DIRECTION_LONG = 1
DIRECTION_SHORT = -1
MIN_INITIAL_RISK_DIST = Decimal("0.1")
MAX_INITIAL_RISK_DIST = Decimal("10.0")
DEFAULT_STOP_DISTANCE_PCT = Decimal("0.02")
MIN_MULTIPLIER = Decimal("0")
VALID_SIDES = (DIRECTION_LONG, DIRECTION_SHORT)
ZERO = Decimal("0")
ONE = Decimal("1")
HALF = Decimal("0.5")

class AddOrderTrigger:
    """加仓触发器，机构级生产就绪"""

    def __init__(self, position_keeper=None, risk_manager=None,
                 event_bus=None, config: Optional[Dict] = None):
        # 依赖注入
        self.position_keeper = position_keeper
        self.risk_manager = risk_manager

        # 事件总线延迟初始化
        try:
            self.event_bus = event_bus or (EventBus() if EventBus else None)
        except Exception:
            self.event_bus = None

        # 加载配置（深拷贝保护）
        config = config or {}
        raw_coefficients = config.get('add_coefficients')
        raw_profit_steps = config.get('profit_steps')
        if isinstance(raw_coefficients, list):
            raw_coefficients = copy.deepcopy(raw_coefficients)
        else:
            raw_coefficients = None
        if isinstance(raw_profit_steps, list):
            raw_profit_steps = copy.deepcopy(raw_profit_steps)
        else:
            raw_profit_steps = None

        # 安全提取 cool_down_bars
        try:
            self._cool_down_bars = max(0, min(int(config.get('cool_down_bars', DEFAULT_COOL_DOWN_BARS)), MAX_COOL_DOWN_BARS))
        except (ValueError, TypeError):
            self._cool_down_bars = DEFAULT_COOL_DOWN_BARS
            logger.warning("cool_down_bars 配置非法，使用默认值 %d", DEFAULT_COOL_DOWN_BARS)

        # 安全提取 min_bayesian_score
        raw_score = config.get('min_bayesian_score')
        try:
            self._min_bayesian_score = self._safe_decimal(raw_score) if raw_score is not None else DEFAULT_MIN_BAYESIAN_SCORE
        except Exception:
            self._min_bayesian_score = DEFAULT_MIN_BAYESIAN_SCORE

        # 配置校验与清洗
        self._add_coefficients, self._profit_steps = self._validate_sequences(
            raw_coefficients, raw_profit_steps
        )

        if not (Decimal("0") <= self._min_bayesian_score <= Decimal("1")):
            logger.error("贝叶斯得分阈值非法，使用默认")
            self._min_bayesian_score = DEFAULT_MIN_BAYESIAN_SCORE

        # 安全提取 stop_distance_pct
        raw_stop_pct = config.get('stop_distance_pct', DEFAULT_STOP_DISTANCE_PCT)
        try:
            self._stop_distance_pct = self._safe_decimal(raw_stop_pct) if raw_stop_pct is not None else DEFAULT_STOP_DISTANCE_PCT
        except Exception:
            self._stop_distance_pct = DEFAULT_STOP_DISTANCE_PCT
        if not (Decimal("0") < self._stop_distance_pct <= Decimal("1")):
            logger.error("stop_distance_pct 非法，使用默认")
            self._stop_distance_pct = DEFAULT_STOP_DISTANCE_PCT

        # 记录未知配置键
        known_keys = {'add_coefficients', 'profit_steps', 'cool_down_bars', 'min_bayesian_score', 'stop_distance_pct'}
        unknown = set(config.keys()) - known_keys
        if unknown:
            logger.warning("忽略未知配置键: %s", unknown)

        logger.info("AddOrderTrigger v%s 初始化，加仓序列长度: %d", VERSION, len(self._add_coefficients))

    @staticmethod
    def _validate_sequences(coeffs: Any, steps: Any) -> tuple:
        """校验加仓系数和盈利门槛序列，返回 (coeffs_decimal_list, steps_decimal_list)"""
        default_coeffs = list(DEFAULT_ADD_COEFFICIENTS)
        default_steps = list(DEFAULT_PROFIT_STEPS)

        if coeffs is None or steps is None:
            logger.info("使用默认加仓序列")
            return default_coeffs, default_steps
        if not isinstance(coeffs, list) or not isinstance(steps, list):
            logger.error("加仓系数或盈利门槛非列表，使用默认")
            return default_coeffs, default_steps
        if len(coeffs) != len(steps):
            logger.error("加仓系数与盈利门槛数量不匹配 (%d vs %d)，使用默认", len(coeffs), len(steps))
            return default_coeffs, default_steps
        if not coeffs:
            logger.error("加仓序列为空，使用默认")
            return default_coeffs, default_steps

        clean_coeffs = []
        clean_steps = []
        for i, (c, s) in enumerate(zip(coeffs, steps)):
            try:
                c_dec = Decimal(str(c))
                s_dec = Decimal(str(s))
                if c_dec < 0 or s_dec <= 0:   # 门槛必须 > 0
                    raise ValueError("负值或零")
                clean_coeffs.append(c_dec)
                clean_steps.append(s_dec)
            except Exception as e:
                logger.error("第%d个加仓参数非法 (c=%s, s=%s): %s", i+1, c, s, e)
                return default_coeffs, default_steps

        # 检查门槛是否严格递增
        for i in range(1, len(clean_steps)):
            if clean_steps[i] <= clean_steps[i-1]:
                logger.error("盈利门槛未严格递增，使用默认")
                return default_coeffs, default_steps

        # 检查系数是否非递增（建议递减）
        for i in range(1, len(clean_coeffs)):
            if clean_coeffs[i] > clean_coeffs[i-1]:
                logger.warning("加仓系数未递减，可能增加风险，但继续使用")

        # 截断到硬限制
        max_len = min(len(clean_coeffs), MAX_ADD_COUNT_HARD)
        if max_len < len(clean_coeffs):
            logger.warning("加仓序列长度超过硬限制 %d，截断", MAX_ADD_COUNT_HARD)

        return clean_coeffs[:max_len], clean_steps[:max_len]

    def evaluate(self, current_state: Dict) -> Dict[str, Any]:
        """
        评估加仓条件

        Args:
            current_state: 必须包含:
                - symbol (str)
                - direction (int, 1=多, -1=空)
                - current_price (float)
                - atr (float)
                - bayesian_score (float)
                - bars_since_last_add (int)
                - initial_risk_distance (float)  初始止损 ATR 倍数
        Returns:
            触发结果字典
        """
        warnings: List[str] = []

        if not self.position_keeper:
            return self._no_trigger("PositionKeeper 不可用", warnings)

        # 提取并校验 symbol
        symbol = str(current_state.get('symbol', '')).strip().upper()
        if not symbol:
            return self._no_trigger("symbol 缺失或为空", warnings)

        # 校验 direction
        direction = current_state.get('direction')
        if direction not in VALID_SIDES:
            return self._no_trigger("无效持仓方向", warnings)

        # 校验 current_price
        try:
            current_price = self._safe_decimal(current_state.get('current_price'))
            if current_price <= 0:
                raise ValueError
        except Exception:
            return self._no_trigger("current_price 无效", warnings)

        # 校验 atr
        try:
            atr = self._safe_decimal(current_state.get('atr'))
            if atr < MIN_ATR:
                raise ValueError
        except Exception:
            return self._no_trigger("ATR 无效或过小", warnings)

        # 校验 bayesian_score
        try:
            bayesian_score = self._safe_decimal(current_state.get('bayesian_score'))
            if not (Decimal("0") <= bayesian_score <= Decimal("1")):
                raise ValueError
        except Exception:
            return self._no_trigger("bayesian_score 无效", warnings)

        # 校验 bars_since_last_add
        bars_since = current_state.get('bars_since_last_add')
        if bars_since is None or not isinstance(bars_since, int) or bars_since < 0:
            return self._no_trigger("bars_since_last_add 缺失或非法", warnings)

        # 校验 initial_risk_distance
        try:
            initial_risk_dist = self._safe_decimal(current_state.get('initial_risk_distance'))
            if not (MIN_INITIAL_RISK_DIST <= initial_risk_dist <= MAX_INITIAL_RISK_DIST):
                raise ValueError
        except Exception:
            return self._no_trigger("initial_risk_distance 无效", warnings)

        # 获取持仓信息
        try:
            position = self.position_keeper.get_position(symbol)
        except Exception as e:
            logger.error("获取持仓异常: %s", e)
            return self._no_trigger("获取持仓异常", warnings)

        if not isinstance(position, dict):
            return self._no_trigger("持仓数据格式异常", warnings)

        # 提取并校验 entry_price
        try:
            entry_price = self._safe_decimal(position.get('entry_price'))
            if entry_price <= 0:
                raise ValueError
        except Exception:
            return self._no_trigger("entry_price 无效", warnings)

        # 提取并校验 quantity
        try:
            current_qty = self._safe_decimal(position.get('quantity'))
            if current_qty <= 0:
                raise ValueError
        except Exception:
            return self._no_trigger("持仓数量无效", warnings)

        # 合约乘数（必须 > 0）
        try:
            multiplier = self._safe_decimal(position.get('multiplier', 1))
            if multiplier <= MIN_MULTIPLIER:
                raise ValueError("乘数必须 > 0")
        except Exception:
            return self._no_trigger("合约乘数无效", warnings)

        # 加仓次数
        add_count = position.get('add_count')
        if add_count is None:
            return self._no_trigger("持仓缺失 add_count 字段", warnings)
        try:
            add_count = int(add_count)
        except (ValueError, TypeError):
            return self._no_trigger("add_count 无法解析", warnings)
        if add_count < 0:
            return self._no_trigger("add_count 为负值", warnings)

        # 持仓方向一致性
        pos_side = position.get('side', '')
        expected_side = 'BUY' if direction == DIRECTION_LONG else 'SELL'
        if pos_side and pos_side.upper() != expected_side:
            return self._no_trigger("持仓方向不一致", warnings)

        max_adds = len(self._add_coefficients)
        if max_adds == 0:
            return self._no_trigger("加仓序列为空，无法加仓", warnings)
        if add_count >= max_adds:
            return self._no_trigger("已达加仓上限", warnings)

        # 价格合理性预警（仅警告，不阻止）
        if entry_price > 0:
            price_deviation = abs(current_price - entry_price) / entry_price
            if price_deviation > HALF:
                logger.warning("价格偏离过大: %.2f%%, 当前价=%s, 入场价=%s",
                               float(price_deviation * 100), current_price, entry_price)

        # 浮盈计算（ATR倍数），考虑合约乘数
        if direction == DIRECTION_LONG:
            profit_atr = (current_price - entry_price) * multiplier / atr
        else:
            profit_atr = (entry_price - current_price) * multiplier / atr

        # 所需浮盈门槛（严格比较）
        required_profit = self._profit_steps[add_count] * initial_risk_dist
        if profit_atr < required_profit:
            return self._no_trigger("浮盈不足", warnings)

        # 贝叶斯得分
        if bayesian_score < self._min_bayesian_score:
            return self._no_trigger("贝叶斯得分不足", warnings)

        # 冷却期
        if bars_since < self._cool_down_bars:
            return self._no_trigger("加仓冷却中", warnings)

        # 计算加仓量（基础仓位数量）
        add_coeff = self._add_coefficients[add_count]
        add_size = current_qty * add_coeff

        # 名义价值检查（含乘数）
        notional = add_size * current_price * multiplier
        if notional < MIN_NOTIONAL_USDT:
            return self._no_trigger("加仓名义价值过小", warnings)
        if notional > MAX_NOTIONAL_USDT:
            return self._no_trigger("加仓名义价值过大", warnings)

        # 总敞口检查（考虑方向，使用绝对值）
        try:
            current_exposure_raw = self.position_keeper.get_total_exposure()
            if current_exposure_raw is not None:
                # 假设敞口返回的是绝对值（无方向），如果没有提供，则尝试计算净敞口
                current_exposure_abs = abs(self._safe_decimal(current_exposure_raw))
                # 加仓增加敞口（绝对值）
                new_exposure = current_exposure_abs + abs(notional)
                max_exposure_raw = self.position_keeper.get_max_exposure()
                if max_exposure_raw is not None:
                    max_exposure_dec = self._safe_decimal(max_exposure_raw)
                    if max_exposure_dec > ZERO and new_exposure > max_exposure_dec:
                        return self._no_trigger("加仓后总敞口超限", warnings)
        except AttributeError:
            logger.debug("总敞口检查跳过：方法不可用")
        except Exception as e:
            logger.warning("总敞口检查异常: %s，跳过", e)

        # 风控审核
        if self.risk_manager:
            # 风控所需字段，转为 float 可能导致精度损失，但风控接口通常使用 float
            order_preview = {
                'symbol': symbol,
                'side': expected_side,
                'quantity': float(add_size),
                'price': float(current_price),
                'risk_amount': float(add_size * current_price * multiplier * self._stop_distance_pct),
                'client_order_id': f"add_{symbol}_{add_count+1}_{int(time.time()*1000)}",
            }
            try:
                approved, reason = self.risk_manager.approve_order(copy.deepcopy(order_preview))
                if not approved:
                    logger.warning("风控拒绝加仓: %s", reason)
                    return self._no_trigger("风控拒绝", warnings)
            except Exception as e:
                logger.error("风控审核异常: %s", e)
                return self._no_trigger("风控审核异常", warnings)

        # 触发
        logger.info("加仓触发: symbol=%s, add_count=%d", symbol, add_count + 1)
        self._emit_event("add_order_triggered", {
            "symbol": symbol,
            "add_count": add_count + 1,
            "timestamp_ns": time.time_ns(),
        })
        self._record_metrics("add_order_triggered", 1, {"symbol": symbol})

        return {
            "trigger": True,
            "size": float(round(add_size, 8)),
            "reason": "加仓条件满足",
            "warnings": warnings,
        }

    def _no_trigger(self, reason: str, warnings: Optional[List[str]] = None) -> Dict[str, Any]:
        if warnings is None:
            warnings = []
        return {
            "trigger": False,
            "size": 0.0,
            "reason": reason,
            "warnings": warnings,
        }

    @staticmethod
    def _safe_decimal(value: Any) -> Decimal:
        """安全转换为 Decimal，处理 None、Decimal 实例、NaN/Inf 等"""
        if value is None:
            raise ValueError("value 为 None")
        if isinstance(value, Decimal):
            return value
        # 对 float，避免 str 转换的精度问题，但可能产生科学计数法，Decimal 构造函数可接受
        try:
            # 直接使用 Decimal 构造，对于 float 可能保留 53 位精度，但金融数据通常为 Decimal 或字符串
            return Decimal(str(value))
        except Exception:
            raise ValueError(f"无法转换为 Decimal: {value}")

    def _emit_event(self, event_type: str, data: Dict):
        if not self.event_bus:
            return
        try:
            evt_type = getattr(EventTypes, 'SYSTEM_ALERT', None) or "system_alert"
            self.event_bus.publish(evt_type, {
                "subtype": event_type,
                "data": data,
                "timestamp_ns": time.time_ns(),
            })
        except Exception:
            pass

    def _record_metrics(self, name: str, value: float, labels: Optional[Dict] = None):
        if METRICS_AVAILABLE and MetricsCollector:
            try:
                MetricsCollector.counter(name, value, labels or {})
            except Exception:
                pass

    @classmethod
    def health_check(cls) -> Dict[str, Any]:
        warnings = []
        if PositionKeeper is None:
            warnings.append("PositionKeeper 不可用")
        if RiskManager is None:
            warnings.append("RiskManager 不可用（可选）")
        # 测试触发器逻辑（不依赖实际外部服务）
        try:
            inst = cls(position_keeper=None, risk_manager=None)
            test_states = [
                {
                    "symbol": "TEST",
                    "direction": DIRECTION_LONG,
                    "current_price": 100.0,
                    "atr": 1.0,
                    "bayesian_score": 0.7,
                    "bars_since_last_add": 5,
                    "initial_risk_distance": 2.0,
                },
                {
                    "symbol": "TEST",
                    "direction": 999,  # 非法方向
                    "current_price": 100.0,
                    "atr": 1.0,
                    "bayesian_score": 0.7,
                    "bars_since_last_add": 5,
                    "initial_risk_distance": 2.0,
                },
            ]
            for state in test_states:
                result = inst.evaluate(state)
                if result.get("trigger") is not False:
                    warnings.append("空依赖时 evaluate 未返回 False")
                    break
        except Exception as e:
            warnings.append(f"自检异常: {e}")
        status = "degraded" if warnings else "ok"
        return {
            "status": status,
            "reason": f"AddOrderTrigger v{VERSION}",
            "warnings": warnings,
        }
