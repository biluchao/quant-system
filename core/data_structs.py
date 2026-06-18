#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 金融级基础数据结构 (DataStructures) v5.0.0 — 机构级终极版

核心职责：
1. 定义系统内所有标准化金融数据结构（K线、逐笔、深度、订单、持仓、成交等）
2. 强制使用 Decimal 存储价格与数量，严禁浮点误差；所有数值输出规范化字符串
3. 提供安全的构造器、严格校验器（含详细原因）、精确序列化/反序列化
4. 数据结构均为不可变 (frozen dataclass)，确保事件流传递安全
5. 内置输入清洗、日志告警、模块自检，符合万亿级账户合规审计要求

外部依赖：
- dataclasses : 标准库
- decimal : 高精度数值
- typing : 类型注解
- math : 数学函数

接口契约：
- 每个数据类提供 from_dict() 和 to_dict()
- validate() 返回 bool，validate_detail() 返回 (bool, str)
- DataStructValidator.health_check() -> Dict[str, Any] 模块自检

异常与降级：
- 构造失败返回 None，记录 WARNING
- 数值转换失败使用安全回退并告警
- 序列化 Decimal 为规范化字符串，防止科学计数法
"""

import logging
import math
from dataclasses import dataclass, asdict
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext, getcontext
from typing import Dict, Any, Optional, Tuple, ClassVar, Final

logger = logging.getLogger(__name__)

VERSION: Final[str] = "5.0.0"
SPDX_IDENTIFIER: Final[str] = "Apache-2.0"

# ── Decimal 配置 ──────────────────────────────────────────
DECIMAL_CONTEXT_PREC = 28
DECIMAL_OUTPUT_PREC = Decimal("0.00000001")
DECIMAL_ZERO = Decimal("0")
MAX_SYMBOL_LENGTH = 20
MAX_CLIENT_ORDER_ID_LENGTH = 64
MAX_DEPTH_LEVELS = 100
MAX_PRICE = Decimal("1000000000")      # 10亿，防止异常大数值
MIN_PRICE = Decimal("0.00000001")

# 设置默认全局精度（模块内使用 localcontext 保护）
getcontext().prec = DECIMAL_CONTEXT_PREC


def _safe_decimal(value, default: Decimal = DECIMAL_ZERO, field_name: str = "unknown") -> Decimal:
    """安全转换为 Decimal，支持 float/int/str/Decimal。NaN/Inf 返回默认并告警。"""
    if isinstance(value, Decimal):
        if value.is_nan() or value.is_infinite():
            logger.warning("[%s] Decimal 为 NaN 或 Infinity，使用默认值", field_name)
            return default
        return value
    if value is None:
        return default
    try:
        if isinstance(value, float):
            if not math.isfinite(value):
                logger.warning("[%s] 非有限浮点数: %s，使用默认值", field_name, value)
                return default
            s = repr(value)
        else:
            s = str(value).strip()
            if not s:
                return default
        with localcontext() as ctx:
            ctx.prec = DECIMAL_CONTEXT_PREC
            d = Decimal(s)
            if d.is_nan() or d.is_infinite():
                logger.warning("[%s] 转换结果非法: %s，使用默认值", field_name, s)
                return default
            return d
    except (InvalidOperation, ValueError, TypeError) as e:
        logger.warning("[%s] Decimal 转换失败: value=%s, error=%s", field_name, value, e)
        return default


def _safe_int(value, default: int = 0, field_name: str = "unknown") -> int:
    """安全转换为 int，对 float 取整并告警"""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if value is None:
        return default
    try:
        if isinstance(value, float):
            if not math.isfinite(value):
                logger.warning("[%s] 非有限浮点数: %s", field_name, value)
                return default
            logger.debug("[%s] 浮点数转为 int: %s -> %d", field_name, value, int(value))
        if isinstance(value, bool):
            logger.warning("[%s] 收到 bool 值: %s，转换为 int", field_name, value)
        return int(value)
    except (ValueError, TypeError) as e:
        logger.warning("[%s] int 转换失败: value=%s, error=%s", field_name, value, e)
        return default


def _format_decimal(value: Decimal, prec: Decimal = DECIMAL_OUTPUT_PREC) -> str:
    """将 Decimal 规范化为字符串，避免科学计数法；失败则回退 str(value)"""
    if not isinstance(value, Decimal):
        return str(value)
    try:
        return str(value.quantize(prec, rounding=ROUND_HALF_UP))
    except InvalidOperation:
        return str(value)


# ── 数据结构定义 ──────────────────────────────────────────

@dataclass(frozen=True)
class Kline:
    """标准化 K 线数据"""
    symbol: str = ""
    interval: str = "3m"
    open: Decimal = DECIMAL_ZERO
    high: Decimal = DECIMAL_ZERO
    low: Decimal = DECIMAL_ZERO
    close: Decimal = DECIMAL_ZERO
    volume: Decimal = DECIMAL_ZERO
    quote_volume: Decimal = DECIMAL_ZERO
    taker_buy_volume: Decimal = DECIMAL_ZERO
    trades_count: int = 0
    is_closed: bool = False
    start_time_ms: int = 0
    end_time_ms: int = 0
    local_timestamp_ns: int = 0

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional['Kline']:
        if not isinstance(data, dict) or not data:
            return None
        symbol = str(data.get('symbol', '')).strip().upper()
        if not symbol or len(symbol) > MAX_SYMBOL_LENGTH:
            logger.warning("Kline symbol 无效: '%s'", symbol)
            return None
        try:
            return cls(
                symbol=symbol,
                interval=str(data.get('interval', '3m')).strip() or "3m",
                open=_safe_decimal(data.get('open'), DECIMAL_ZERO, 'Kline.open'),
                high=_safe_decimal(data.get('high'), DECIMAL_ZERO, 'Kline.high'),
                low=_safe_decimal(data.get('low'), DECIMAL_ZERO, 'Kline.low'),
                close=_safe_decimal(data.get('close'), DECIMAL_ZERO, 'Kline.close'),
                volume=_safe_decimal(data.get('volume'), DECIMAL_ZERO, 'Kline.volume'),
                quote_volume=_safe_decimal(data.get('quote_volume'), DECIMAL_ZERO, 'Kline.quote'),
                taker_buy_volume=_safe_decimal(data.get('taker_buy_volume'), DECIMAL_ZERO, 'Kline.tbv'),
                trades_count=_safe_int(data.get('trades_count'), 0, 'Kline.trades'),
                is_closed=bool(data.get('is_closed', False)),
                start_time_ms=_safe_int(data.get('start_time_ms'), 0, 'Kline.start_ms'),
                end_time_ms=_safe_int(data.get('end_time_ms'), 0, 'Kline.end_ms'),
                local_timestamp_ns=_safe_int(data.get('local_timestamp_ns'), 0, 'Kline.local_ts'),
            )
        except Exception as e:
            logger.error("Kline 构造失败: %s", e)
            return None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        for field_name in ('open', 'high', 'low', 'close', 'volume', 'quote_volume', 'taker_buy_volume'):
            val = d[field_name]
            d[field_name] = _format_decimal(val) if isinstance(val, Decimal) else str(val)
        return d

    def validate(self) -> bool:
        valid, _ = self.validate_detail()
        return valid

    def validate_detail(self) -> Tuple[bool, str]:
        if not self.symbol:
            return False, "symbol为空"
        if self.high < self.low:
            return False, f"high({self.high}) < low({self.low})"
        if any(p <= 0 for p in (self.open, self.high, self.low, self.close)):
            return False, "价格非正"
        if not (self.low <= self.close <= self.high):
            return False, f"close({self.close}) 不在 [{self.low}, {self.high}] 内"
        if self.volume < 0:
            return False, "volume为负"
        if self.taker_buy_volume > self.volume:
            return False, "taker_buy_volume > volume"
        if self.start_time_ms <= 0 or self.end_time_ms <= 0:
            return False, "时间戳非正"
        if self.start_time_ms > self.end_time_ms:
            return False, "start > end"
        if self.open > MAX_PRICE or self.high > MAX_PRICE or self.low > MAX_PRICE or self.close > MAX_PRICE:
            return False, f"价格超出上限 {MAX_PRICE}"
        return True, ""


@dataclass(frozen=True)
class Tick:
    """标准化逐笔成交数据"""
    symbol: str = ""
    price: Decimal = DECIMAL_ZERO
    quantity: Decimal = DECIMAL_ZERO
    timestamp_ms: int = 0
    trade_id: int = 0
    is_buyer_maker: bool = False
    local_timestamp_ns: int = 0

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional['Tick']:
        if not isinstance(data, dict) or not data:
            return None
        symbol = str(data.get('symbol', '')).strip().upper()
        if not symbol or len(symbol) > MAX_SYMBOL_LENGTH:
            return None
        try:
            return cls(
                symbol=symbol,
                price=_safe_decimal(data.get('price'), DECIMAL_ZERO, 'Tick.price'),
                quantity=_safe_decimal(data.get('quantity'), DECIMAL_ZERO, 'Tick.qty'),
                timestamp_ms=_safe_int(data.get('timestamp_ms'), 0, 'Tick.ts'),
                trade_id=_safe_int(data.get('trade_id'), 0, 'Tick.trade_id'),
                is_buyer_maker=bool(data.get('is_buyer_maker', False)),
                local_timestamp_ns=_safe_int(data.get('local_timestamp_ns'), 0, 'Tick.local_ts'),
            )
        except Exception:
            return None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['price'] = _format_decimal(d['price'])
        d['quantity'] = _format_decimal(d['quantity'])
        return d

    def validate(self) -> bool:
        valid, _ = self.validate_detail()
        return valid

    def validate_detail(self) -> Tuple[bool, str]:
        if not self.symbol:
            return False, "symbol为空"
        if self.price <= 0:
            return False, "price非正"
        if self.quantity <= 0:
            return False, "quantity非正"
        if self.timestamp_ms <= 0:
            return False, "timestamp_ms非正"
        if self.price > MAX_PRICE:
            return False, f"price超出上限 {MAX_PRICE}"
        return True, ""


@dataclass(frozen=True)
class DepthSnapshot:
    """标准化深度快照"""
    symbol: str = ""
    last_update_id: int = 0
    first_update_id: int = 0
    bids: Tuple[Tuple[Decimal, Decimal], ...] = ()
    asks: Tuple[Tuple[Decimal, Decimal], ...] = ()
    timestamp_ms: int = 0
    local_timestamp_ns: int = 0

    @classmethod
    def from_dict(cls, data: Dict[str, Any], max_levels: int = MAX_DEPTH_LEVELS) -> Optional['DepthSnapshot']:
        if not isinstance(data, dict) or not data:
            return None
        symbol = str(data.get('symbol', '')).strip().upper()
        if not symbol or len(symbol) > MAX_SYMBOL_LENGTH:
            return None
        try:
            bids_raw = data.get('bids') or []
            asks_raw = data.get('asks') or []
            bids = tuple(
                (_safe_decimal(p, DECIMAL_ZERO, 'Depth.bid_p'), _safe_decimal(q, DECIMAL_ZERO, 'Depth.bid_q'))
                for p, q in bids_raw
                if _safe_decimal(p, DECIMAL_ZERO) > 0 and _safe_decimal(q, DECIMAL_ZERO) > 0
            )[:max_levels]
            asks = tuple(
                (_safe_decimal(p, DECIMAL_ZERO, 'Depth.ask_p'), _safe_decimal(q, DECIMAL_ZERO, 'Depth.ask_q'))
                for p, q in asks_raw
                if _safe_decimal(p, DECIMAL_ZERO) > 0 and _safe_decimal(q, DECIMAL_ZERO) > 0
            )[:max_levels]
            return cls(
                symbol=symbol,
                last_update_id=_safe_int(data.get('last_update_id'), 0, 'Depth.last_id'),
                first_update_id=_safe_int(data.get('first_update_id'), 0, 'Depth.first_id'),
                bids=bids,
                asks=asks,
                timestamp_ms=_safe_int(data.get('timestamp_ms'), 0, 'Depth.ts'),
                local_timestamp_ns=_safe_int(data.get('local_timestamp_ns'), 0, 'Depth.local_ts'),
            )
        except Exception:
            return None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['bids'] = [[_format_decimal(p), _format_decimal(q)] for p, q in self.bids]
        d['asks'] = [[_format_decimal(p), _format_decimal(q)] for p, q in self.asks]
        return d

    def validate(self) -> bool:
        valid, _ = self.validate_detail()
        return valid

    def validate_detail(self) -> Tuple[bool, str]:
        if not self.symbol:
            return False, "symbol为空"
        if not self.bids or not self.asks:
            return False, "买卖盘为空"
        if self.first_update_id > self.last_update_id:
            return False, "first_update_id > last_update_id"
        best_bid = max(p for p, _ in self.bids)
        best_ask = min(p for p, _ in self.asks)
        if best_bid >= best_ask:
            return False, f"买卖价交叉: bid={best_bid}, ask={best_ask}"
        if best_bid > MAX_PRICE or best_ask > MAX_PRICE:
            return False, f"价格超出上限 {MAX_PRICE}"
        return True, ""


@dataclass(frozen=True)
class Order:
    """标准化订单"""
    symbol: str = ""
    side: str = ""            # BUY / SELL
    quantity: Decimal = DECIMAL_ZERO
    price: Optional[Decimal] = None   # None 表示市价
    order_type: str = "LIMIT"         # LIMIT / MARKET
    client_order_id: str = ""
    parent_plan_id: str = ""
    slice_index: int = 0
    created_at_ns: int = 0

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional['Order']:
        if not isinstance(data, dict) or not data:
            return None
        symbol = str(data.get('symbol', '')).strip().upper()
        if not symbol or len(symbol) > MAX_SYMBOL_LENGTH:
            return None
        try:
            price = None
            raw_price = data.get('price')
            if raw_price is not None:
                price = _safe_decimal(raw_price, None, 'Order.price')
                if price is not None and (price <= 0 or price > MAX_PRICE):
                    logger.warning("Order price 无效 (%s)，视为市价单", raw_price)
                    price = None
            order_type = str(data.get('order_type', 'LIMIT')).upper().strip()
            if order_type not in ('LIMIT', 'MARKET'):
                logger.warning("未知 order_type '%s'，回退为 LIMIT", order_type)
                order_type = 'LIMIT'
            side = str(data.get('side', '')).upper().strip()
            if side not in ('BUY', 'SELL'):
                logger.warning("无效 side '%s'，拒绝订单", side)
                return None
            return cls(
                symbol=symbol,
                side=side,
                quantity=_safe_decimal(data.get('quantity'), DECIMAL_ZERO, 'Order.qty'),
                price=price,
                order_type=order_type,
                client_order_id=str(data.get('client_order_id', ''))[:MAX_CLIENT_ORDER_ID_LENGTH],
                parent_plan_id=str(data.get('parent_plan_id', ''))[:MAX_CLIENT_ORDER_ID_LENGTH],
                slice_index=_safe_int(data.get('slice_index'), 0, 'Order.slice'),
                created_at_ns=_safe_int(data.get('created_at_ns'), 0, 'Order.created'),
            )
        except Exception:
            return None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['quantity'] = _format_decimal(d['quantity'])
        if d['price'] is not None:
            d['price'] = _format_decimal(d['price'])
        return d

    def validate(self) -> bool:
        valid, _ = self.validate_detail()
        return valid

    def validate_detail(self) -> Tuple[bool, str]:
        if not self.symbol:
            return False, "symbol为空"
        if self.side not in ('BUY', 'SELL'):
            return False, f"非法 side: {self.side}"
        if self.quantity <= 0:
            return False, "quantity非正"
        if self.order_type == 'LIMIT':
            if self.price is None or self.price <= 0:
                return False, "限价单缺少有效价格"
        elif self.order_type == 'MARKET':
            if self.price is not None:
                return False, "市价单不应包含价格"
        else:
            return False, f"非法 order_type: {self.order_type}"
        if self.quantity > Decimal("1000000000"):
            return False, "quantity过大"
        return True, ""


@dataclass(frozen=True)
class Position:
    """标准化持仓"""
    symbol: str = ""
    side: str = "BUY"         # BUY 或 SELL (已统一)
    quantity: Decimal = DECIMAL_ZERO
    entry_price: Decimal = DECIMAL_ZERO
    mark_price: Decimal = DECIMAL_ZERO
    multiplier: Decimal = Decimal("1")
    unrealized_pnl: Decimal = DECIMAL_ZERO
    realized_pnl: Decimal = DECIMAL_ZERO
    funding_fees: Decimal = DECIMAL_ZERO

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional['Position']:
        if not isinstance(data, dict) or not data:
            return None
        symbol = str(data.get('symbol', '')).strip().upper()
        if not symbol or len(symbol) > MAX_SYMBOL_LENGTH:
            return None
        try:
            side = str(data.get('side', 'BUY')).upper()
            if side in ('LONG',):
                side = 'BUY'
            elif side in ('SHORT',):
                side = 'SELL'
            if side not in ('BUY', 'SELL'):
                return None
            return cls(
                symbol=symbol,
                side=side,
                quantity=_safe_decimal(data.get('quantity'), DECIMAL_ZERO, 'Pos.qty'),
                entry_price=_safe_decimal(data.get('entry_price'), DECIMAL_ZERO, 'Pos.entry'),
                mark_price=_safe_decimal(data.get('mark_price'), DECIMAL_ZERO, 'Pos.mark'),
                multiplier=_safe_decimal(data.get('multiplier'), Decimal("1"), 'Pos.mult'),
                unrealized_pnl=_safe_decimal(data.get('unrealized_pnl'), DECIMAL_ZERO, 'Pos.upnl'),
                realized_pnl=_safe_decimal(data.get('realized_pnl'), DECIMAL_ZERO, 'Pos.rpnl'),
                funding_fees=_safe_decimal(data.get('funding_fees'), DECIMAL_ZERO, 'Pos.fees'),
            )
        except Exception:
            return None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        for field_name in ('quantity', 'entry_price', 'mark_price', 'multiplier',
                           'unrealized_pnl', 'realized_pnl', 'funding_fees'):
            d[field_name] = _format_decimal(d[field_name])
        return d

    def validate(self) -> bool:
        valid, _ = self.validate_detail()
        return valid

    def validate_detail(self) -> Tuple[bool, str]:
        if not self.symbol:
            return False, "symbol为空"
        if self.side not in ('BUY', 'SELL'):
            return False, f"非法 side: {self.side}"
        if self.quantity <= 0:
            return False, "quantity非正"
        if self.entry_price <= 0:
            return False, "entry_price非正"
        if self.entry_price > MAX_PRICE:
            return False, "entry_price过大"
        return True, ""


@dataclass(frozen=True)
class Trade:
    """标准化成交记录"""
    symbol: str = ""
    order_id: str = ""
    side: str = "BUY"
    quantity: Decimal = DECIMAL_ZERO
    price: Decimal = DECIMAL_ZERO
    fee: Decimal = DECIMAL_ZERO
    fee_currency: str = ""
    realized_pnl: Decimal = DECIMAL_ZERO
    timestamp_ms: int = 0

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional['Trade']:
        if not isinstance(data, dict) or not data:
            return None
        symbol = str(data.get('symbol', '')).strip().upper()
        if not symbol:
            return None
        try:
            return cls(
                symbol=symbol,
                order_id=str(data.get('order_id', ''))[:64],
                side=str(data.get('side', 'BUY')).upper()[:4],
                quantity=_safe_decimal(data.get('quantity'), DECIMAL_ZERO, 'Trade.qty'),
                price=_safe_decimal(data.get('price'), DECIMAL_ZERO, 'Trade.price'),
                fee=_safe_decimal(data.get('fee'), DECIMAL_ZERO, 'Trade.fee'),
                fee_currency=str(data.get('fee_currency', ''))[:10],
                realized_pnl=_safe_decimal(data.get('realized_pnl'), DECIMAL_ZERO, 'Trade.pnl'),
                timestamp_ms=_safe_int(data.get('timestamp_ms'), 0, 'Trade.ts'),
            )
        except Exception:
            return None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        for key in ('quantity', 'price', 'fee', 'realized_pnl'):
            d[key] = _format_decimal(d[key])
        return d

    def validate(self) -> bool:
        valid, _ = self.validate_detail()
        return valid

    def validate_detail(self) -> Tuple[bool, str]:
        if not self.symbol:
            return False, "symbol为空"
        if self.side not in ('BUY', 'SELL'):
            return False, f"非法 side: {self.side}"
        if self.quantity <= 0:
            return False, "quantity非正"
        if self.price <= 0:
            return False, "price非正"
        if self.price > MAX_PRICE:
            return False, "price过大"
        if self.timestamp_ms <= 0:
            return False, "timestamp_ms非正"
        return True, ""


# ── 模块健康检查器 ────────────────────────────────────────

class DataStructValidator:
    """统一数据结构自检与验证"""

    @classmethod
    def health_check(cls) -> Dict[str, Any]:
        warnings = []
        try:
            # Kline
            kline = Kline(symbol="BTCUSDT", open=Decimal("50000"), high=Decimal("51000"),
                          low=Decimal("49000"), close=Decimal("50500"), volume=Decimal("10"),
                          taker_buy_volume=Decimal("5"), start_time_ms=100, end_time_ms=200)
            valid, msg = kline.validate_detail()
            if not valid:
                warnings.append(f"Kline.validate 失败: {msg}")
            d = kline.to_dict()
            k2 = Kline.from_dict(d)
            if k2 != kline:
                warnings.append("Kline 序列化往返不一致")

            # Tick
            tick = Tick(symbol="ETHUSDT", price=Decimal("2000.5"), quantity=Decimal("1.5"), timestamp_ms=123456)
            if not tick.validate():
                warnings.append("Tick.validate 失败")

            # DepthSnapshot
            depth = DepthSnapshot(symbol="BTCUSDT",
                                  bids=((Decimal("50000"), Decimal("1")),),
                                  asks=((Decimal("50001"), Decimal("2")),),
                                  first_update_id=1, last_update_id=2)
            if not depth.validate():
                warnings.append("DepthSnapshot.validate 失败")

            # Order LIMIT / MARKET
            limit_order = Order(symbol="BTCUSDT", side="BUY", quantity=Decimal("1"),
                                price=Decimal("50000"), order_type="LIMIT")
            if not limit_order.validate():
                warnings.append("限价单 validate 失败")
            market_order = Order(symbol="BTCUSDT", side="SELL", quantity=Decimal("0.5"),
                                 price=None, order_type="MARKET")
            if not market_order.validate():
                warnings.append("市价单 validate 失败")

            # Position
            pos = Position(symbol="BTCUSDT", quantity=Decimal("1.5"), entry_price=Decimal("50000"))
            if not pos.validate():
                warnings.append("Position.validate 失败")

            # Trade
            trade = Trade(symbol="BTCUSDT", side="BUY", quantity=Decimal("1"), price=Decimal("50000"),
                          fee=Decimal("0.01"), realized_pnl=Decimal("100"), timestamp_ms=123456)
            if not trade.validate():
                warnings.append("Trade.validate 失败")

            # 边界测试
            if Kline.from_dict({}) is not None:
                warnings.append("空字典应返回 None")
            if Kline.from_dict({"symbol": ""}) is not None:
                warnings.append("空 symbol 应返回 None")

            bad_tick = Tick.from_dict({"symbol": "X", "price": -1, "quantity": 1})
            if bad_tick is not None and bad_tick.validate():
                warnings.append("负价格不应通过验证")

            # NaN 测试
            nan_kline = Kline.from_dict({"symbol": "BTCUSDT", "open": float('nan')})
            if nan_kline is not None:
                warnings.append("NaN 价格应返回 None")

        except Exception as e:
            return {"status": "error", "reason": str(e), "warnings": [str(e)]}

        status = "degraded" if warnings else "ok"
        return {
            "status": status,
            "reason": f"DataStructures v{VERSION} 自检完成",
            "warnings": warnings,
        }


# 快速自检入口
if __name__ == "__main__":
    import json
    result = DataStructValidator.health_check()
    print(json.dumps(result, indent=2, ensure_ascii=False))
