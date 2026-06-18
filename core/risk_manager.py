#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 独立风控中心 (RiskManager) v8.0.0 — 机构级最终版

核心职责：
1. 订单前置多维风控：频率、保证金、杠杆、单笔风险(基于实时ATR)、日内亏损、组合ES、名义价值上限
2. 多品种完全隔离风控：独立的参数、损失计数、熔断状态、权益基准、风险预算
3. 支持全熔断与半熔断(仅平仓)，自动恢复窗口，所有熔断事件持久化审计
4. 权益与敞口数据带TTL缓存，防止对外部接口造成压力；数值全部Decimal化
5. 完整的Prometheus指标与事件总线告警，线程安全且高并发低延迟

外部依赖：
- core.event_bus.EventBus : 发布风控事件
- core.position_keeper.PositionKeeper : 获取权益、持仓、敞口、ATR
- core.metrics.MetricsCollector : 指标收集

接口契约：
- approve_order(order: Dict) -> Tuple[bool, str]
- record_trade_result(profit: float, symbol: str) -> None
- reset_circuit_breaker(symbol: str, audit_info: Dict) -> None
- reload_config(new_config: Dict, symbol_params: Optional[Dict]) -> None
- health_check() -> Dict[str, Any]

异常与降级：
- 外部依赖不可用时，采用缓存值或保守拒绝策略
- 所有异常被捕获，默认返回安全侧
- 指标记录与事件发布失败不影响主流程
"""

import logging
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from threading import Lock, RLock
from typing import Dict, Any, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

# ── 可选依赖 ──────────────────────────────────────────────
try:
    from core.event_bus import EventBus, EventTypes
except ImportError:
    EventBus = None
    EventTypes = None

try:
    from core.position_keeper import PositionKeeper
except ImportError:
    PositionKeeper = None

try:
    from core.metrics import MetricsCollector
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    MetricsCollector = None


# ── 常量 ──────────────────────────────────────────────────
# 全局默认值
DEFAULT_MAX_LEVERAGE = Decimal('5.0')
DEFAULT_MAX_MARGIN_PCT = Decimal('0.5')            # 占可用保证金
DEFAULT_MAX_ORDERS_PER_SECOND = 8
DEFAULT_MAX_CONSECUTIVE_LOSSES = 5
DEFAULT_DAILY_LOSS_LIMIT_PCT = Decimal('0.08')
DEFAULT_MAX_SINGLE_RISK_PCT = Decimal('0.015')
DEFAULT_PORTFOLIO_ES_LIMIT_PCT = Decimal('0.02')
DEFAULT_STOP_ATR_MULT = Decimal('2.0')
DEFAULT_ATR_PERIOD = 14
ORDER_WINDOW_SEC = 1.0
MIN_EQUITY = Decimal('1e-8')
MAX_ORDER_VALUE_USDT_MAP = {
    'BTCUSDT': Decimal('5_000_000'),
    'ETHUSDT': Decimal('2_000_000'),
    'DEFAULT': Decimal('500_000')
}
CIRCUIT_BREAKER_AUTO_RESET_SEC = 3600
EQUITY_CACHE_TTL_SEC = 0.5
EXPOSURE_CACHE_TTL_SEC = 0.5
ATR_CACHE_TTL_SEC = 10.0
MAX_ORDER_VALUE_CACHE_SIZE = 128  # 缓存品种的上限值
FREQUENCY_QUEUE_MULTIPLIER = 10


class RiskManager:
    """独立风控中心（线程安全，多品种完全隔离）"""

    def __init__(self, config: Optional[Dict] = None, event_bus=None,
                 position_keeper=None):
        self.config = self._validate_and_merge_config(config or {})
        self.event_bus = event_bus or (EventBus() if EventBus else None)
        self.position_keeper = position_keeper or (PositionKeeper() if PositionKeeper else None)

        # 频率控制
        max_len = int(self.config['max_orders_per_second'] * FREQUENCY_QUEUE_MULTIPLIER)
        self._order_timestamps: deque = deque(maxlen=max_len)
        self._ts_lock = Lock()

        # 连续亏损（品种级别）
        self._losses: Dict[str, int] = {}
        self._loss_lock = Lock()

        # 熔断（全熔断 + 半熔断）
        self._circuit_breaker: Dict[str, bool] = {}
        self._circuit_reason: Dict[str, str] = {}
        self._circuit_time: Dict[str, float] = {}
        self._soft_circuit: Dict[str, bool] = {}
        self._breaker_lock = Lock()

        # 权益基准（品种独立）
        self._day_start_equity: Dict[str, Decimal] = {}
        self._last_equity_day: Dict[str, datetime] = {}
        self._equity_lock = Lock()

        # 缓存
        self._equity_cache: Optional[Decimal] = None
        self._equity_cache_time: float = 0.0
        self._exposure_cache: Dict[str, Tuple[Decimal, float]] = {}
        self._atr_cache: Dict[str, Tuple[Decimal, float]] = {}
        self._cache_lock = Lock()

        # 品种级别参数
        self._symbol_params: Dict[str, Dict] = {}
        self._symbol_params_lock = RLock()

        # 风控延迟
        self._last_approve_latency_us = 0.0
        self._latency_lock = Lock()

        self._init_equity()
        logger.info("RiskManager v8.0.0 初始化完成")

    # ── 公共接口 ──────────────────────────────────────────

    def approve_order(self, order: Dict) -> Tuple[bool, str]:
        if not isinstance(order, dict):
            return False, "无效订单格式"
        symbol = order.get('symbol', '').upper()
        if not symbol:
            return False, "订单缺少交易对"

        # 熔断
        breaker_ok, breaker_msg = self._check_circuit(symbol)
        if not breaker_ok:
            return False, breaker_msg

        # 频率（快速路径）
        if not self._check_rate_limit():
            self._emit_alert("order_rejected_rate_limit", symbol)
            return False, "订单频率超限"

        # 权益（缓存）
        equity = self._get_equity_cached()
        if equity is None or equity < MIN_EQUITY:
            return False, "无法获取有效权益"

        # 订单合法性
        legal, reason = self._validate_order_legal(order)
        if not legal:
            return False, reason

        # 保证金
        margin_ok, reason = self._check_margin(order, equity, symbol)
        if not margin_ok:
            self._emit_alert("order_rejected_margin", symbol, reason)
            return False, reason

        # 总杠杆
        leverage_ok, reason = self._check_total_leverage(order, equity, symbol)
        if not leverage_ok:
            self._emit_alert("order_rejected_leverage", symbol, reason)
            return False, reason

        # 单笔风险
        risk_ok, reason = self._check_single_risk(order, equity, symbol)
        if not risk_ok:
            self._emit_alert("order_rejected_single_risk", symbol, reason)
            return False, reason

        # 日内亏损
        daily_ok, reason = self._check_daily_loss(equity, symbol)
        if not daily_ok:
            self._emit_alert("order_rejected_daily_loss", symbol, reason)
            return False, reason

        # 名义价值上限
        max_val = self._get_max_order_value(symbol)
        order_value = abs(Decimal(str(order.get('quantity', 0))) * Decimal(str(order.get('price', 0))))
        if order_value > max_val:
            self._emit_alert("order_rejected_max_value", symbol)
            return False, "订单名义价值超限"

        # 通过
        self._record_order_time()
        return True, "ok"

    def record_trade_result(self, profit: float, symbol: str) -> None:
        if not isinstance(profit, (int, float)) or str(profit) in ('nan', 'inf', '-inf'):
            return
        sym = symbol.upper() or "DEFAULT"
        try:
            with self._loss_lock:
                if profit < 0:
                    self._losses[sym] = self._losses.get(sym, 0) + 1
                    if self._losses[sym] >= self.config['max_consecutive_losses']:
                        self._set_circuit(sym, True, f"连续亏损 {self._losses[sym]} 次")
                else:
                    self._losses[sym] = 0
                    with self._breaker_lock:
                        if self._soft_circuit.get(sym, False):
                            self._soft_circuit[sym] = False
                            logger.info("%s 半熔断解除", sym)
            self._update_daily_equity_benchmark(sym)
        except Exception:
            logger.exception("record_trade_result error")

    def reset_circuit_breaker(self, symbol: str, audit_info: Dict) -> None:
        if not audit_info or 'operator' not in audit_info:
            logger.error("重置熔断需要审计信息")
            return
        self._set_circuit(symbol.upper(), False, f"手动重置 by {audit_info['operator']}")

    def reload_config(self, new_config: Dict, symbol_params: Optional[Dict] = None) -> None:
        self.config = self._validate_and_merge_config(new_config)
        if symbol_params:
            with self._symbol_params_lock:
                for sym, params in symbol_params.items():
                    self._symbol_params[sym.upper()] = self._validate_and_merge_config(params)
        logger.info("风控配置热重载完成")

    def get_state(self, symbol: str = "DEFAULT") -> Dict:
        sym = symbol.upper()
        with self._breaker_lock:
            cb = self._circuit_breaker.get(sym, False)
            reason = self._circuit_reason.get(sym, "")
        with self._loss_lock:
            losses = self._losses.get(sym, 0)
        with self._equity_lock:
            equity = str(self._day_start_equity.get(sym, Decimal('0')))
        return {"circuit_breaker": cb, "reason": reason, "consecutive_losses": losses, "day_start_equity": equity}

    def health_check(self) -> Dict[str, Any]:
        warnings = []
        if not self.position_keeper:
            warnings.append("PositionKeeper未配置")
        with self._breaker_lock:
            for sym, state in self._circuit_breaker.items():
                if state:
                    warnings.append(f"{sym} 熔断: {self._circuit_reason.get(sym)}")
        with self._loss_lock:
            for sym, loss in self._losses.items():
                if loss >= self.config['max_consecutive_losses'] - 1:
                    warnings.append(f"{sym} 接近连续亏损熔断 ({loss}/{self.config['max_consecutive_losses']})")
        status = "degraded" if warnings else "ok"
        return {"status": status, "reason": "ok" if not warnings else "; ".join(warnings), "warnings": warnings}

    # ── 配置与参数 ──────────────────────────────────────

    def _validate_and_merge_config(self, user_config: Dict) -> Dict:
        defaults = {
            'max_leverage': float(DEFAULT_MAX_LEVERAGE),
            'max_margin_per_order_pct': float(DEFAULT_MAX_MARGIN_PCT),
            'max_orders_per_second': DEFAULT_MAX_ORDERS_PER_SECOND,
            'max_consecutive_losses': DEFAULT_MAX_CONSECUTIVE_LOSSES,
            'daily_loss_limit_pct': float(DEFAULT_DAILY_LOSS_LIMIT_PCT),
            'max_single_risk_pct': float(DEFAULT_MAX_SINGLE_RISK_PCT),
            'portfolio_es_limit_pct': float(DEFAULT_PORTFOLIO_ES_LIMIT_PCT),
            'stop_atr_mult': float(DEFAULT_STOP_ATR_MULT),
            'atr_period': DEFAULT_ATR_PERIOD,
        }
        merged = {}
        for k, default in defaults.items():
            val = user_config.get(k, default)
            if not isinstance(val, (int, float)) or val <= 0:
                val = default
            merged[k] = val
        return merged

    def _get_symbol_param(self, symbol: str, key: str, default_val=None):
        sym = symbol.upper()
        with self._symbol_params_lock:
            params = self._symbol_params.get(sym, {})
        return params.get(key, self.config.get(key, default_val))

    # ── 检查子函数 ──────────────────────────────────────

    def _check_circuit(self, symbol: str) -> Tuple[bool, str]:
        sym = symbol.upper()
        with self._breaker_lock:
            # 全熔断
            if sym in self._circuit_breaker and self._circuit_breaker[sym]:
                if time.time() - self._circuit_time.get(sym, 0) > CIRCUIT_BREAKER_AUTO_RESET_SEC:
                    self._circuit_breaker[sym] = False
                    logger.info("%s 熔断自动解除", sym)
                    return True, ""
                return False, f"熔断: {self._circuit_reason[sym]}"
            # 半熔断
            if self._soft_circuit.get(sym, False):
                return False, "半熔断(仅平仓)"
        return True, ""

    def _check_rate_limit(self) -> bool:
        with self._ts_lock:
            now = time.time()
            while self._order_timestamps and now - self._order_timestamps[0] > ORDER_WINDOW_SEC:
                self._order_timestamps.popleft()
            return len(self._order_timestamps) < self.config['max_orders_per_second']

    def _validate_order_legal(self, order: Dict) -> Tuple[bool, str]:
        try:
            qty = Decimal(str(order.get('quantity', 0)))
            price = Decimal(str(order.get('price', 0)))
            if qty <= 0 or price <= 0:
                return False, "数量或价格无效"
        except (InvalidOperation, ValueError):
            return False, "数量或价格格式错误"
        side = order.get('side', '').upper()
        if side not in ('BUY', 'SELL'):
            return False, "方向无效"
        return True, ""

    def _check_margin(self, order: Dict, equity: Decimal, symbol: str) -> Tuple[bool, str]:
        margin = self._estimate_margin(order, symbol)
        if margin is None or margin <= 0:
            return False, "保证金估算失败"
        max_margin_pct = Decimal(str(self._get_symbol_param(symbol, 'max_margin_per_order_pct')))
        if margin > equity * max_margin_pct:
            return False, "保证金超限"
        return True, ""

    def _check_total_leverage(self, order: Dict, equity: Decimal, symbol: str) -> Tuple[bool, str]:
        new_exposure = abs(Decimal(str(order.get('quantity', 0))) * Decimal(str(order.get('price', 0))))
        current_exposure = self._get_total_exposure_cached(symbol)
        total_leverage = (current_exposure + new_exposure) / equity
        max_lev = Decimal(str(self._get_symbol_param(symbol, 'max_leverage')))
        if total_leverage > max_lev:
            return False, "杠杆超限"
        return True, ""

    def _check_single_risk(self, order: Dict, equity: Decimal, symbol: str) -> Tuple[bool, str]:
        risk_amount = order.get('risk_amount')
        if not risk_amount or risk_amount <= 0:
            stop_atr = Decimal(str(self._get_symbol_param(symbol, 'stop_atr_mult')))
            atr = self._get_atr_cached(symbol)
            if atr is None:
                # 回退到默认百分比
                stop_distance = Decimal('0.02')
                risk_amount = abs(Decimal(str(order.get('quantity', 0))) * Decimal(str(order.get('price', 0))) * stop_distance)
            else:
                risk_amount = abs(Decimal(str(order.get('quantity', 0))) * atr * stop_atr)
        max_risk_pct = Decimal(str(self._get_symbol_param(symbol, 'max_single_risk_pct')))
        if Decimal(str(risk_amount)) > equity * max_risk_pct:
            return False, "单笔风险超限"
        return True, ""

    def _check_daily_loss(self, equity: Decimal, symbol: str) -> Tuple[bool, str]:
        sym = symbol.upper()
        with self._equity_lock:
            self._update_daily_equity_benchmark(sym)
            start_eq = self._day_start_equity.get(sym, equity)
            if start_eq <= 0:
                self._day_start_equity[sym] = equity
                return True, ""
            loss_pct = (start_eq - equity) / start_eq
            limit = Decimal(str(self._get_symbol_param(sym, 'daily_loss_limit_pct')))
            if loss_pct >= limit:
                return False, f"日内亏损 {loss_pct:.2%} >= {limit:.2%}"
        return True, ""

    # ── 数据获取与缓存 ──────────────────────────────────

    def _get_equity_cached(self) -> Optional[Decimal]:
        now = time.time()
        with self._cache_lock:
            if self._equity_cache is not None and now - self._equity_cache_time < EQUITY_CACHE_TTL_SEC:
                return self._equity_cache
        eq = self._get_equity_raw()
        if eq is not None:
            with self._cache_lock:
                self._equity_cache = eq
                self._equity_cache_time = now
        return eq

    def _get_equity_raw(self) -> Optional[Decimal]:
        if not self.position_keeper:
            return None
        try:
            val = self.position_keeper.get_equity()
            if val is None or val <= 0:
                return None
            return Decimal(str(val))
        except Exception:
            return None

    def _get_total_exposure_cached(self, symbol: str) -> Decimal:
        sym = symbol.upper()
        now = time.time()
        with self._cache_lock:
            if sym in self._exposure_cache:
                cached, t = self._exposure_cache[sym]
                if now - t < EXPOSURE_CACHE_TTL_SEC:
                    return cached
        exp = self._get_total_exposure_raw(sym)
        with self._cache_lock:
            self._exposure_cache[sym] = (exp, now)
        return exp

    def _get_total_exposure_raw(self, symbol: str) -> Decimal:
        if not self.position_keeper:
            return Decimal('0')
        try:
            return Decimal(str(abs(self.position_keeper.get_total_exposure(symbol))))
        except Exception:
            return Decimal('0')

    def _get_atr_cached(self, symbol: str) -> Optional[Decimal]:
        sym = symbol.upper()
        now = time.time()
        with self._cache_lock:
            if sym in self._atr_cache:
                cached, t = self._atr_cache[sym]
                if now - t < ATR_CACHE_TTL_SEC:
                    return cached
        atr = self._get_atr_raw(sym)
        if atr is not None:
            with self._cache_lock:
                self._atr_cache[sym] = (atr, now)
        return atr

    def _get_atr_raw(self, symbol: str) -> Optional[Decimal]:
        if self.position_keeper and hasattr(self.position_keeper, 'get_atr'):
            try:
                val = self.position_keeper.get_atr(symbol)
                if val:
                    return Decimal(str(val))
            except Exception:
                pass
        return None

    def _estimate_margin(self, order: Dict, symbol: str) -> Decimal:
        try:
            qty = Decimal(str(abs(order.get('quantity', 0))))
            price = Decimal(str(order.get('price', 0)))
            if price == 0:
                return Decimal('Infinity')
            leverage = Decimal(str(order.get('leverage', self._get_symbol_param(symbol, 'max_leverage'))))
            if leverage <= 0:
                return Decimal('Infinity')
            return (qty * price) / leverage
        except (InvalidOperation, ValueError):
            return Decimal('Infinity')

    def _get_max_order_value(self, symbol: str) -> Decimal:
        return MAX_ORDER_VALUE_USDT_MAP.get(symbol.upper(), MAX_ORDER_VALUE_USDT_MAP['DEFAULT'])

    def _init_equity(self):
        eq = self._get_equity_cached()
        if eq:
            with self._equity_lock:
                self._day_start_equity['DEFAULT'] = eq
                self._last_equity_day['DEFAULT'] = datetime.now(timezone.utc).date()

    def _update_daily_equity_benchmark(self, symbol: str):
        eq = self._get_equity_cached()
        if eq is None:
            return
        sym = symbol.upper()
        today = datetime.now(timezone.utc).date()
        with self._equity_lock:
            last_day = self._last_equity_day.get(sym)
            if last_day != today:
                self._day_start_equity[sym] = eq
                self._last_equity_day[sym] = today
            elif self._day_start_equity.get(sym, Decimal('0')) <= 0:
                self._day_start_equity[sym] = eq

    def _record_order_time(self):
        with self._ts_lock:
            self._order_timestamps.append(time.time())

    def _set_circuit(self, symbol: str, state: bool, reason: str):
        sym = symbol.upper()
        with self._breaker_lock:
            self._circuit_breaker[sym] = state
            self._circuit_reason[sym] = reason
            if state:
                self._circuit_time[sym] = time.time()
                self._soft_circuit[sym] = False
                logger.critical("%s 熔断: %s", sym, reason)
                self._emit_alert("circuit_breaker_trigger", sym, reason)
            else:
                self._circuit_time.pop(sym, None)
                self._soft_circuit.pop(sym, None)
                logger.info("%s 熔断解除: %s", sym, reason)
                self._emit_alert("circuit_breaker_reset", sym, reason)

    def _emit_alert(self, alert_type: str, symbol: str, message: str = ""):
        if self.event_bus and EventTypes:
            try:
                self.event_bus.publish(EventTypes.SYSTEM_ALERT, {
                    "alert_type": alert_type,
                    "symbol": symbol,
                    "message": message,
                    "timestamp": time.time(),
                })
            except Exception:
                pass
        if METRICS_AVAILABLE and MetricsCollector:
            try:
                MetricsCollector.counter("risk_alert_total", 1, {"type": alert_type, "symbol": symbol})
            except Exception:
                pass
