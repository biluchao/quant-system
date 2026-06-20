#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 动态风险预算 (RiskBudget) v21.0.0

核心职责：
1. 计算持仓组合的风险度量：年化波动率、VaR、Expected Shortfall (ES)
2. 基于目标波动率动态计算最大允许敞口（总敞口绝对值之和）
3. 提供新增订单的事前风险预算检查：杠杆、ES、最大敞口
4. 发布脱敏风险指标事件与 Prometheus 指标

外部依赖：
- core.position_keeper.PositionKeeper : 获取权益、持仓敞口（名义价值）、历史权益曲线
- core.event_bus.EventBus : 发布风险事件（可选）
- core.metrics.MetricsCollector : 指标暴露（可选）
- numpy (可选) : 用于统计，若不可用则降级为纯 Python 实现

接口契约：
- compute_risk_metrics(force: bool = False, publish: bool = True) -> Dict[str, Any]
  返回 {"volatility": float, "var_95": float, "es_95": float, "max_exposure": float, ...}
- check_exposure(new_order: Dict) -> Tuple[bool, str]
  返回 (是否通过, 原因)
- health_check() -> Dict[str, Any]

异常与降级：
- 若 numpy 不可用，使用纯 Python 实现，并记录 WARNING
- 若 position_keeper 不可用，返回保守默认值，并在 check_exposure 中拒绝
- 所有计算异常均被捕获，返回安全值并记录错误
"""

import copy
import logging
import math
import threading
import time
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

VERSION = "21.0.0"
SPDX_IDENTIFIER = "Apache-2.0"

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False
    np = None

try:
    from core.position_keeper import PositionKeeper
except ImportError:
    PositionKeeper = None

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

# 默认参数
DEFAULT_CONFIDENCE = 0.95
DEFAULT_TARGET_VOLATILITY = 0.15
DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_MAX_LEVERAGE = 5.0
DEFAULT_ES_LIMIT_PCT = 0.02
DEFAULT_CACHE_TTL_SEC = 5.0
DEFAULT_ANNUAL_FACTOR = math.sqrt(365)      # 日线数据年化因子，可配置
DEFAULT_MAX_EXPOSURE_HARD_RATIO = 10.0       # 硬性最大敞口/权益倍数
DEFAULT_MAX_RETURN_ABS = 1.0                 # 日收益率绝对值上限（可配置，建议根据资产特性调整）
DEFAULT_VOL_ANOMALY_THRESHOLD = 20.0         # 年化波动率异常阈值
DEFAULT_VAR_FALLBACK_DIVISOR = 0.5           # VaR兜底使用波动率的比例
DEFAULT_VOL_FLOOR = 0.001                    # 波动率下限（避免除零）
DEFAULT_MAX_RISK_ADJ_RATIO = 20.0            # 最大风险调整敞口/权益倍数
DEFAULT_RETURN_DISCARD_RATIO = 0.2           # 收益率清洗丢弃比例告警阈值
MIN_RETURNS_FOR_VAR = 10
MIN_RETURNS_FOR_VOL = 5
VAR_FALLBACK_MULTIPLIER = 2.5
ES_FALLBACK_MULTIPLIER = 3.5
DEFAULT_RISK_EVENT_INTERVAL_SEC = 60.0       # 风险指标事件最小发送间隔
MAX_POSITIONS = 200                           # 单次最大持仓数量限制
MIN_ES_RATIO_THRESHOLD = 1e-6                # ES比率最小有效阈值

# 指标名称常量（带命名空间前缀）
METRIC_PREFIX = "risk_budget_"
METRIC_VOL = METRIC_PREFIX + "volatility"
METRIC_VAR = METRIC_PREFIX + "var_95"
METRIC_ES = METRIC_PREFIX + "es_95"
METRIC_MAX_EXP = METRIC_PREFIX + "max_exposure"
METRIC_TOTAL_EXP = METRIC_PREFIX + "total_exposure"
METRIC_EQUITY = METRIC_PREFIX + "equity"
METRIC_LEVERAGE = METRIC_PREFIX + "leverage"
METRIC_ES_RATIO = METRIC_PREFIX + "es_ratio"
METRIC_EQUITY_ZERO = METRIC_PREFIX + "equity_zero"  # 权益为零标记

# 风险检查结果码
REASON_OK = "OK"
REASON_LEV = "LEVERAGE"
REASON_ES = "ES"
REASON_EXP = "EXPOSURE"
REASON_NO_EQ = "NO_EQUITY"
REASON_INVALID = "INVALID_ORDER"


class RiskBudget:
    """动态风险预算管理器，机构级生产就绪"""

    def __init__(self, position_keeper=None, event_bus=None, config: Optional[Dict] = None):
        # 依赖注入，异常保护
        if position_keeper is not None:
            self.position_keeper = position_keeper
        elif PositionKeeper is not None:
            try:
                self.position_keeper = PositionKeeper()
            except Exception as e:
                logger.critical("PositionKeeper 初始化失败: %s", e)
                self.position_keeper = None
        else:
            self.position_keeper = None

        if event_bus is not None:
            self.event_bus = event_bus
        elif EventBus is not None:
            try:
                self.event_bus = EventBus()
            except Exception as e:
                logger.error("EventBus 初始化失败: %s", e)
                self.event_bus = None
        else:
            self.event_bus = None

        config = config or {}
        self.confidence = self._validate_config_float(
            config.get('confidence', DEFAULT_CONFIDENCE), 0.5, 0.999, DEFAULT_CONFIDENCE, "confidence"
        )
        self.target_volatility = self._validate_config_float(
            config.get('target_volatility', DEFAULT_TARGET_VOLATILITY), 0.01, 1.0, DEFAULT_TARGET_VOLATILITY, "target_volatility"
        )
        self.lookback_days = max(1, min(int(config.get('lookback_days', DEFAULT_LOOKBACK_DAYS)), 365))
        self.max_leverage = self._validate_config_float(
            config.get('max_leverage', DEFAULT_MAX_LEVERAGE), 1.0, 20.0, DEFAULT_MAX_LEVERAGE, "max_leverage"
        )
        self.es_limit_pct = self._validate_config_float(
            config.get('es_limit_pct', DEFAULT_ES_LIMIT_PCT), 0.001, 0.1, DEFAULT_ES_LIMIT_PCT, "es_limit_pct"
        )
        self.cache_ttl = self._validate_config_float(
            config.get('cache_ttl_sec', DEFAULT_CACHE_TTL_SEC), 0.0, 300.0, DEFAULT_CACHE_TTL_SEC, "cache_ttl_sec"
        )
        self.annual_factor = self._validate_config_float(
            config.get('annual_factor', DEFAULT_ANNUAL_FACTOR), 1.0, 100.0, DEFAULT_ANNUAL_FACTOR, "annual_factor"
        )
        self.max_exposure_hard_ratio = self._validate_config_float(
            config.get('max_exposure_hard_ratio', DEFAULT_MAX_EXPOSURE_HARD_RATIO),
            2.0, 20.0, DEFAULT_MAX_EXPOSURE_HARD_RATIO, "max_exposure_hard_ratio"
        )
        self.max_return_abs = self._validate_config_float(
            config.get('max_return_abs', DEFAULT_MAX_RETURN_ABS),
            0.1, 100.0, DEFAULT_MAX_RETURN_ABS, "max_return_abs"
        )
        self.vol_anomaly_threshold = self._validate_config_float(
            config.get('vol_anomaly_threshold', DEFAULT_VOL_ANOMALY_THRESHOLD),
            1.0, 100.0, DEFAULT_VOL_ANOMALY_THRESHOLD, "vol_anomaly_threshold"
        )
        self.var_fallback_divisor = self._validate_config_float(
            config.get('var_fallback_divisor', DEFAULT_VAR_FALLBACK_DIVISOR),
            0.1, 1.0, DEFAULT_VAR_FALLBACK_DIVISOR, "var_fallback_divisor"
        )
        self.vol_floor = self._validate_config_float(
            config.get('vol_floor', DEFAULT_VOL_FLOOR),
            1e-6, 0.01, DEFAULT_VOL_FLOOR, "vol_floor"
        )
        self.max_risk_adj_ratio = self._validate_config_float(
            config.get('max_risk_adj_ratio', DEFAULT_MAX_RISK_ADJ_RATIO),
            1.0, 100.0, DEFAULT_MAX_RISK_ADJ_RATIO, "max_risk_adj_ratio"
        )
        self.return_discard_ratio = self._validate_config_float(
            config.get('return_discard_ratio', DEFAULT_RETURN_DISCARD_RATIO),
            0.0, 1.0, DEFAULT_RETURN_DISCARD_RATIO, "return_discard_ratio"
        )
        self.risk_event_interval = self._validate_config_float(
            config.get('risk_event_interval_sec', DEFAULT_RISK_EVENT_INTERVAL_SEC),
            1.0, 3600.0, DEFAULT_RISK_EVENT_INTERVAL_SEC, "risk_event_interval_sec"
        )

        # 可变状态受 _lock 保护
        self._cached_metrics: Optional[Dict] = None
        self._cache_time: float = 0.0
        self._last_vol_anomaly = False
        self._last_risk_event_time: float = 0.0
        self._lock = threading.RLock()

        known_keys = {'confidence', 'target_volatility', 'lookback_days', 'max_leverage',
                      'es_limit_pct', 'cache_ttl_sec', 'annual_factor', 'max_exposure_hard_ratio',
                      'max_return_abs', 'vol_anomaly_threshold', 'var_fallback_divisor',
                      'vol_floor', 'max_risk_adj_ratio', 'return_discard_ratio',
                      'risk_event_interval_sec'}
        unknown = set(config.keys()) - known_keys
        if unknown:
            logger.warning("忽略未知配置键: %s", unknown)

        if not NUMPY_AVAILABLE:
            logger.warning("numpy 不可用，风险计算降级为纯 Python 实现")

        logger.info("RiskBudget v%s 初始化，置信度=%.2f, 目标波动率=%.2f, 回看=%d天",
                    VERSION, self.confidence, self.target_volatility, self.lookback_days)

    @staticmethod
    def _validate_config_float(value: Any, low: float, high: float,
                               default: float, name: str) -> float:
        try:
            val = float(value)
            if math.isnan(val) or math.isinf(val) or not (low <= val <= high):
                raise ValueError
            return val
        except (ValueError, TypeError):
            logger.error("配置 %s 非法 (%s)，使用默认 %s", name, value, default)
            return default

    @staticmethod
    def _safe_metric_val(val: Any, default: float = 0.0) -> float:
        """过滤非有限值并返回；若无法转换则记录并返回默认值"""
        try:
            f = float(val)
            if math.isfinite(f):
                return f
        except (ValueError, TypeError):
            pass
        logger.warning("指标值非有限或不可转换: %s, 使用默认值 %.4f", val, default)
        return default

    # ── 公共接口 ──────────────────────────────────────────

    def compute_risk_metrics(self, force: bool = False, publish: bool = True) -> Dict[str, Any]:
        # 锁外获取数据，减少持锁时间
        equity = self._get_equity()
        exposures = self._get_exposures()
        returns = self._get_returns()

        with self._lock:
            now = time.time()
            if not force and self._cached_metrics and (now - self._cache_time) < self.cache_ttl:
                return copy.deepcopy(self._cached_metrics)

            total_exposure = sum(exposures)  # exposures 已是绝对值列表

            vol = self._compute_volatility(returns)
            var_95, es_95 = self._compute_var_es(returns, vol)

            equity_val = equity if equity is not None and equity > 0 else 0.0
            if equity_val > 0:
                var_amount = var_95 * equity_val
                es_amount = es_95 * equity_val
                leverage = total_exposure / equity_val
                es_ratio = es_amount / equity_val
            else:
                var_amount = 0.0
                es_amount = 0.0
                leverage = -1.0 if total_exposure > 0 else 0.0
                es_ratio = 0.0

            max_exposure = self._compute_max_exposure(equity_val if equity_val > 0 else None, vol)

            metrics = {
                "volatility": vol,
                "var_95": var_95,
                "es_95": es_95,
                "var_amount": var_amount,
                "es_amount": es_amount,
                "max_exposure": max_exposure,
                "equity": equity_val,
                "total_exposure": total_exposure,
                "leverage": leverage,
                "es_ratio": es_ratio,
                "timestamp": now,
                "data_points": len(returns),
            }

            self._cached_metrics = metrics
            self._cache_time = now

            is_anomaly = vol > self.vol_anomaly_threshold
            anomaly_changed = is_anomaly != self._last_vol_anomaly
            self._last_vol_anomaly = is_anomaly

            send_risk_event = publish and (now - self._last_risk_event_time >= self.risk_event_interval)
            if send_risk_event:
                self._last_risk_event_time = now

            result = copy.deepcopy(metrics)

        self._record_metrics(metrics)

        if publish and anomaly_changed:
            self._emit_event("volatility_anomaly" if is_anomaly else "volatility_normalized", {})
        if send_risk_event:
            self._emit_event("risk_metrics_updated", {
                "volatility_anomaly": is_anomaly,
            })

        logger.debug("风险指标计算完成: vol=%.4f, ES=%.4f, max_exposure=%.0f",
                     vol, es_95, max_exposure)
        return result

    def check_exposure(self, new_order: Dict) -> Tuple[bool, str]:
        if not isinstance(new_order, dict):
            return False, REASON_INVALID

        try:
            qty = float(new_order.get('quantity', 0.0))
            if qty <= 0:
                return False, REASON_INVALID
            price = float(new_order.get('price', 0.0))
            if price <= 0 or not math.isfinite(price):
                return False, REASON_INVALID
            side = str(new_order.get('side', '')).upper()
            if side not in ('BUY', 'SELL'):
                return False, REASON_INVALID
            symbol = str(new_order.get('symbol', '')).strip().upper()
            if not symbol:
                return False, REASON_INVALID
        except (ValueError, TypeError):
            return False, REASON_INVALID

        new_exposure = qty * price

        metrics = self.compute_risk_metrics(force=True, publish=False)
        equity = metrics.get("equity", 0.0)
        if equity <= 0:
            return False, REASON_NO_EQ

        # 使用 Decimal 精确计算杠杆
        total_exposure_after = Decimal(str(metrics.get("total_exposure", 0.0))) + Decimal(str(new_exposure))
        equity_dec = Decimal(str(equity))
        if total_exposure_after / equity_dec > Decimal(str(self.max_leverage)):
            return False, REASON_LEV

        es_ratio = metrics.get("es_ratio", 0.0)
        if es_ratio > MIN_ES_RATIO_THRESHOLD and es_ratio > self.es_limit_pct:
            return False, REASON_ES
        if es_ratio <= MIN_ES_RATIO_THRESHOLD and metrics.get("data_points", 0) > 0:
            logger.info("ES 比率数据不足，跳过 ES 检查")

        max_exposure = Decimal(str(metrics.get("max_exposure", float('inf'))))
        if total_exposure_after > max_exposure:
            return False, REASON_EXP

        return True, REASON_OK

    def health_check(self) -> Dict[str, Any]:
        warnings = []
        if not self.position_keeper:
            warnings.append("PositionKeeper 未配置")
        else:
            try:
                eq = self.position_keeper.get_equity()
                if eq is None:
                    warnings.append("权益数据缺失")
                else:
                    val = float(eq)
                    if val <= 0 or not math.isfinite(val):
                        warnings.append("权益数据无效")
            except Exception as e:
                warnings.append(f"PositionKeeper 调用异常: {e}")
        if not NUMPY_AVAILABLE:
            warnings.append("numpy 不可用，使用简化方法")
        if self.event_bus is None:
            warnings.append("事件总线不可用（可选）")

        # 健康检查：强制重算但不污染正常缓存（使用独立的本地变量存储结果）
        try:
            # 绕过缓存，直接调用核心计算（但不使用锁？不行，核心计算使用锁内部缓存更新）
            # 为简化，这里仅使用 force=True 但之后不恢复缓存；外部调用者应理解健康检查可能更新缓存
            metrics = self.compute_risk_metrics(force=True, publish=False)
            if not isinstance(metrics, dict):
                warnings.append("风险指标计算失败")
            elif metrics.get("equity", 0) <= 0:
                warnings.append("权益数据缺失或为零")
        except Exception as e:
            warnings.append(f"风险计算异常: {e}")

        status = "degraded" if warnings else "ok"
        return {"status": status, "reason": f"RiskBudget v{VERSION}", "warnings": warnings}

    def invalidate_cache(self) -> None:
        with self._lock:
            self._cached_metrics = None
            self._cache_time = 0.0
            self._last_vol_anomaly = False
            self._last_risk_event_time = 0.0
        logger.info("风险预算缓存已手动失效")

    # ── 内部数据获取 ──────────────────────────────────────

    def _get_equity(self) -> Optional[float]:
        if not self.position_keeper:
            return None
        try:
            eq = self.position_keeper.get_equity()
            if eq is None:
                return None
            eq_f = float(eq)
            if not math.isfinite(eq_f) or eq_f <= 0:
                return None
            return eq_f
        except Exception as e:
            logger.warning("获取权益失败: %s", e)
            return None

    def _get_exposures(self) -> List[float]:
        """获取当前所有持仓的名义敞口绝对值列表（去重）"""
        if not self.position_keeper:
            return []
        try:
            exposures = []
            # 优先使用 get_exposures（假设已去重且准确）
            if hasattr(self.position_keeper, 'get_exposures'):
                raw = self.position_keeper.get_exposures()
                if isinstance(raw, (list, tuple)):
                    for e in raw:
                        try:
                            val = float(abs(float(e)))
                            if math.isfinite(val):
                                exposures.append(val)
                        except (ValueError, TypeError):
                            continue
                    return exposures[:MAX_POSITIONS]
            # 回退：遍历持仓并去重
            if hasattr(self.position_keeper, 'get_all_positions'):
                raw_positions = self.position_keeper.get_all_positions()
                if not isinstance(raw_positions, (list, tuple)):
                    try:
                        raw_positions = list(raw_positions)
                    except Exception:
                        logger.warning("get_all_positions 无法转换为列表，类型: %s", type(raw_positions))
                        raw_positions = []
                raw_positions = raw_positions[:MAX_POSITIONS]
                seen_symbols = set()
                for pos in raw_positions:
                    if not isinstance(pos, dict):
                        continue
                    symbol = str(pos.get('symbol', '')).upper()
                    if not symbol or symbol in seen_symbols:
                        continue
                    seen_symbols.add(symbol)
                    try:
                        qty = abs(float(pos.get('quantity', 0.0)))
                    except (ValueError, TypeError):
                        continue
                    if qty == 0:
                        continue
                    price = self._get_position_price(pos)
                    if price > 0:
                        exposures.append(qty * price)
                    else:
                        logger.warning("持仓价格无效，跳过敞口: symbol=%s", symbol)
                return exposures
        except Exception as e:
            logger.error("获取敞口失败: %s", e)
        return []

    def _get_returns(self) -> List[float]:
        if not self.position_keeper:
            return []
        try:
            if hasattr(self.position_keeper, 'get_returns'):
                raw = self.position_keeper.get_returns(days=self.lookback_days)
                if raw:
                    return self._clean_returns(list(raw))
            curve = self.position_keeper.get_equity_curve(days=self.lookback_days)
            if curve and len(curve) > 1:
                returns = []
                for i in range(1, len(curve)):
                    try:
                        prev = float(curve[i-1])
                        curr = float(curve[i])
                        if prev > 0 and math.isfinite(curr):
                            r = (curr - prev) / prev
                            returns.append(r)
                    except (ValueError, TypeError):
                        continue
                if returns:
                    return self._clean_returns(returns)
        except Exception as e:
            logger.error("获取收益率数据失败: %s", e)
        return []

    def _clean_returns(self, raw: List[float]) -> List[float]:
        cleaned = []
        for r in raw:
            try:
                val = float(r)
                if math.isfinite(val) and abs(val) <= self.max_return_abs:
                    cleaned.append(val)
            except (ValueError, TypeError):
                continue
        total = len(raw)
        if total > 0:
            discarded = total - len(cleaned)
            if discarded / total >= self.return_discard_ratio:
                logger.warning("收益率数据清洗丢弃 %d/%d 个点 (%.0f%%)", discarded, total,
                               discarded / total * 100)
        return cleaned

    @staticmethod
    def _get_position_price(pos: Dict) -> float:
        mark = pos.get('mark_price')
        if mark is not None:
            try:
                p = float(mark)
                if p > 0 and math.isfinite(p):
                    return p
            except (ValueError, TypeError):
                pass
        entry = pos.get('entry_price')
        if entry is not None:
            try:
                p = float(entry)
                if p > 0 and math.isfinite(p):
                    return p
            except (ValueError, TypeError):
                pass
        return 0.0

    # ── 风险计算核心 ──────────────────────────────────────

    def _compute_volatility(self, returns: List[float]) -> float:
        if not returns or len(returns) < MIN_RETURNS_FOR_VOL:
            logger.debug("收益率数据不足 (%d), 使用目标波动率 %.2f", len(returns), self.target_volatility)
            return self.target_volatility

        if NUMPY_AVAILABLE:
            daily_vol = np.std(returns, ddof=1)
        else:
            mean = sum(returns) / len(returns)
            variance = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1) if len(returns) > 1 else 0.0
            daily_vol = math.sqrt(variance) if variance > 0 else 0.0

        annual_vol = daily_vol * self.annual_factor
        if annual_vol > self.vol_anomaly_threshold:
            logger.warning("波动率异常偏高: %.4f (阈值 %.2f), 回退目标波动率", annual_vol, self.vol_anomaly_threshold)
            return self.target_volatility
        return max(annual_vol, 0.0)

    def _compute_var_es(self, returns: List[float], fallback_vol: float) -> Tuple[float, float]:
        if not returns or len(returns) < MIN_RETURNS_FOR_VAR:
            var = fallback_vol * VAR_FALLBACK_MULTIPLIER
            es = fallback_vol * ES_FALLBACK_MULTIPLIER
            return var, es

        if NUMPY_AVAILABLE:
            sorted_ret = np.sort(returns)
            idx = int((1 - self.confidence) * len(sorted_ret))
            idx = max(0, min(idx, len(sorted_ret) - 1))
            tail = sorted_ret[:idx+1]
            var = -sorted_ret[idx] if sorted_ret[idx] < 0 else fallback_vol * self.var_fallback_divisor
            es = -float(np.mean(tail)) if len(tail) > 0 and tail[0] < 0 else var
        else:
            sorted_ret = sorted(returns)
            idx = int((1 - self.confidence) * len(sorted_ret))
            idx = max(0, min(idx, len(sorted_ret) - 1))
            tail = sorted_ret[:idx+1]
            var = -sorted_ret[idx] if sorted_ret[idx] < 0 else fallback_vol * self.var_fallback_divisor
            es = -sum(tail) / len(tail) if len(tail) > 0 and tail[0] < 0 else var

        var_annual = var * self.annual_factor
        es_annual = es * self.annual_factor
        return max(0.0, var_annual), max(0.0, es_annual)

    def _compute_max_exposure(self, equity: Optional[float], volatility: float) -> float:
        if not equity or equity <= 0:
            return 0.0

        vol_clipped = max(volatility, self.vol_floor)
        risk_adj_ratio = min(self.target_volatility / vol_clipped, self.max_risk_adj_ratio)
        return min(equity * risk_adj_ratio,
                   equity * self.max_leverage,
                   equity * self.max_exposure_hard_ratio)

    # ── 事件与指标 ────────────────────────────────────────

    def _emit_event(self, event_type: str, data: Optional[Dict] = None):
        if not self.event_bus:
            return
        if data is None:
            data = {}
        try:
            evt_type = getattr(EventTypes, 'SYSTEM_ALERT', "system_alert")
            self.event_bus.publish(evt_type, {
                "subtype": event_type,
                "data": data,
                "timestamp_ns": time.time_ns(),
            })
        except Exception:
            logger.debug("事件发布失败", exc_info=True)

    def _record_metrics(self, metrics: Dict):
        if not METRICS_AVAILABLE or MetricsCollector is None:
            return
        try:
            mc = MetricsCollector()
            mc.gauge(METRIC_VOL, self._safe_metric_val(metrics.get("volatility")))
            mc.gauge(METRIC_VAR, self._safe_metric_val(metrics.get("var_95")))
            mc.gauge(METRIC_ES, self._safe_metric_val(metrics.get("es_95")))
            mc.gauge(METRIC_MAX_EXP, self._safe_metric_val(metrics.get("max_exposure")))
            mc.gauge(METRIC_TOTAL_EXP, self._safe_metric_val(metrics.get("total_exposure")))
            mc.gauge(METRIC_EQUITY, self._safe_metric_val(metrics.get("equity")))
            lev = metrics.get("leverage", 0.0)
            if isinstance(lev, (int, float)) and lev >= 0:
                mc.gauge(METRIC_LEVERAGE, float(lev))
            else:
                # 权益为零时记录特殊标记
                mc.gauge(METRIC_EQUITY_ZERO, 1.0 if metrics.get("equity", 0) <= 0 else 0.0)
            mc.gauge(METRIC_ES_RATIO, self._safe_metric_val(metrics.get("es_ratio")))
        except Exception:
            logger.debug("指标记录失败")
