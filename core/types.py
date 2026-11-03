"""core/types.py — 全局类型定义（全系统唯一真相源）

铁律声明：本文件遵守
- 类型统一：所有金额用 Decimal，时间用 int(Unix ms)，枚举用 Enum
- 命名统一：类型 PascalCase，常量 UPPER_SNAKE，枚举成员大写
- 异常统一：构造函数校验失败抛精确异常（ValidationError）
"""
from __future__ import annotations

from decimal import Decimal
from enum import Enum, IntEnum, auto
from dataclasses import dataclass, field
from typing import NewType, Optional, Union, Dict, Any, List, Tuple, FrozenSet
from uuid import uuid4


# ═══════════════════════════════════════════════════════════════
# 基础类型别名（NewType 防止语义混用）
# ═══════════════════════════════════════════════════════════════

Symbol = NewType("Symbol", str)
OrderId = NewType("OrderId", str)
ClientOrderId = NewType("ClientOrderId", str)
StrategyId = NewType("StrategyId", str)
TraceId = NewType("TraceId", str)
SessionId = NewType("SessionId", str)

# 金额与数量统一使用 Decimal，禁止 float 参与计算
Money = Decimal
Quantity = Decimal
Price = Decimal
Timestamp = int  # Unix 毫秒时间戳


def new_trace_id() -> TraceId:
    """生成新的追踪 ID"""
    return TraceId(str(uuid4()))


def new_order_id(prefix: str = "ORD") -> OrderId:
    """生成新的订单 ID"""
    return OrderId(f"{prefix}-{uuid4().hex[:12]}")


def new_client_order_id(strategy_id: Optional[StrategyId] = None) -> ClientOrderId:
    """生成新的客户端订单 ID（格式：策略前缀 + 时间戳 + 随机）"""
    import time
    ts = int(time.time() * 1000)
    rand = uuid4().hex[:6]
    if strategy_id:
        return ClientOrderId(f"{strategy_id}-{ts}-{rand}")
    return ClientOrderId(f"CLI-{ts}-{rand}")


# ═══════════════════════════════════════════════════════════════
# 枚举定义
# ═══════════════════════════════════════════════════════════════

class OrderSide(str, Enum):
    """订单方向"""
    BUY = "BUY"
    SELL = "SELL"

    def opposite(self) -> "OrderSide":
        """获取反方向"""
        return OrderSide.SELL if self == OrderSide.BUY else OrderSide.BUY

    def sign(self) -> int:
        """方向符号（BUY=1, SELL=-1），用于盈亏计算"""
        return 1 if self == OrderSide.BUY else -1


class OrderType(str, Enum):
    """订单类型"""
    MARKET = "MARKET"          # 市价单
    LIMIT = "LIMIT"            # 限价单
    STOP = "STOP"              # 止损单
    STOP_MARKET = "STOP_MARKET"
    TAKE_PROFIT = "TAKE_PROFIT"
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"
    TRAILING_STOP_MARKET = "TRAILING_STOP_MARKET"


class OrderStatus(IntEnum):
    """订单状态机（严格单向流转）

    PENDING → ACKED → PARTIALLY_FILLED → FILLED
                                    ↘ CANCELLED
              → REJECTED
              → EXPIRED
    """
    PENDING = 0           # 已创建，未提交
    ACKED = 1             # 交易所已确认接收
    PARTIALLY_FILLED = 2  # 部分成交
    FILLED = 3            # 完全成交
    CANCELLED = 4         # 已取消
    REJECTED = 5          # 被拒绝
    EXPIRED = 6           # 已过期


class TimeInForce(str, Enum):
    """订单有效期"""
    GTC = "GTC"   # Good Till Cancelled
    IOC = "IOC"   # Immediate Or Cancel
    FOK = "FOK"   # Fill Or Kill
    POST_ONLY = "POST_ONLY"


class PositionSide(str, Enum):
    """持仓方向"""
    LONG = "LONG"
    SHORT = "SHORT"
    FLAT = "FLAT"


class MarginType(str, Enum):
    """保证金模式"""
    ISOLATED = "ISOLATED"   # 逐仓
    CROSSED = "CROSSED"     # 全仓


class Exchange(str, Enum):
    """交易所"""
    BINANCE = "BINANCE"
    OKX = "OKX"
    DERIBIT = "DERIBIT"
    BYBIT = "BYBIT"


class SystemMode(str, Enum):
    """系统运行模式"""
    BACKTEST = "BACKTEST"   # 回测
    PAPER = "PAPER"         # 模拟盘
    DRY_RUN = "DRY_RUN"     # 干跑（真实行情，预检但不提交）
    LIVE = "LIVE"           # 实盘


class CircuitLevel(IntEnum):
    """熔断级别（从低到高）"""
    STRATEGY = 1   # 策略级
    SYMBOL = 2     # 品种级
    GLOBAL = 3     # 全局


class AlertLevel(IntEnum):
    """告警级别"""
    P0 = 0   # 致命，立即响应
    P1 = 1   # 严重，5分钟内
    P2 = 2   # 警告，30分钟内
    P3 = 3   # 信息，不主动推送


class ComponentStatus(str, Enum):
    """组件状态"""
    UNINITIALIZED = "UNINITIALIZED"
    INITIALIZING = "INITIALIZING"
    RUNNING = "RUNNING"
    DEGRADED = "DEGRADED"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


# ═══════════════════════════════════════════════════════════════
# 不可变数据对象（frozen=True 保证线程安全 + 可哈希）
# ═══════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class PriceLevel:
    """盘口价位"""
    price: Price
    quantity: Quantity

    def __post_init__(self) -> None:
        if self.price <= 0:
            raise ValidationError(f"价格必须为正数，当前值: {self.price}")
        if self.quantity <= 0:
            raise ValidationError(f"数量必须为正数，当前值: {self.quantity}")


@dataclass(frozen=True)
class TradeTick:
    """逐笔成交"""
    symbol: Symbol
    price: Price
    quantity: Quantity
    side: OrderSide
    timestamp: Timestamp
    exchange: Exchange
    trade_id: str

    def __post_init__(self) -> None:
        if self.price <= 0:
            raise ValidationError(f"价格必须为正数: {self.price}")
        if self.quantity <= 0:
            raise ValidationError(f"数量必须为正数: {self.quantity}")
        if self.timestamp <= 0:
            raise ValidationError(f"时间戳必须为正: {self.timestamp}")

    def notional(self) -> Money:
        """成交额"""
        return self.price * self.quantity


@dataclass(frozen=True)
class Kline:
    """K线数据"""
    symbol: Symbol
    interval: str
    open_time: Timestamp
    close_time: Timestamp
    open: Price
    high: Price
    low: Price
    close: Price
    volume: Quantity
    is_closed: bool = False

    def __post_init__(self) -> None:
        if self.high < self.low:
            raise ValidationError(f"最高价({self.high})不能低于最低价({self.low})")
        if self.close_time <= self.open_time:
            raise ValidationError("收盘时间必须晚于开盘时间")


@dataclass(frozen=True)
class OrderBookSnapshot:
    """订单簿快照"""
    symbol: Symbol
    bids: Tuple[PriceLevel, ...]   # 不可变元组
    asks: Tuple[PriceLevel, ...]
    timestamp: Timestamp
    last_update_id: Optional[int] = None

    def best_bid(self) -> Optional[Price]:
        """最优买价"""
        return self.bids[0].price if self.bids else None

    def best_ask(self) -> Optional[Price]:
        """最优卖价"""
        return self.asks[0].price if self.asks else None

    def mid_price(self) -> Optional[Price]:
        """中间价"""
        bid, ask = self.best_bid(), self.best_ask()
        if bid is not None and ask is not None:
            return (bid + ask) / 2
        return None

    def spread(self) -> Optional[Price]:
        """买卖价差"""
        bid, ask = self.best_bid(), self.best_ask()
        if bid is not None and ask is not None:
            return ask - bid
        return None


@dataclass(frozen=True)
class MarkPrice:
    """标记价格"""
    symbol: Symbol
    mark_price: Price
    index_price: Price
    funding_rate: Decimal
    next_funding_time: Timestamp
    timestamp: Timestamp


@dataclass(frozen=True)
class Balance:
    """账户余额"""
    asset: str
    free: Money
    locked: Money
    total: Money = field(init=False)

    def __post_init__(self) -> None:
        if self.free < 0 or self.locked < 0:
            raise ValidationError("余额不能为负")
        # 计算 total（frozen 对象需通过 object.__setattr__）
        object.__setattr__(self, 'total', self.free + self.locked)


@dataclass(frozen=True)
class Position:
    """持仓"""
    symbol: Symbol
    side: PositionSide
    quantity: Quantity
    entry_price: Price
    mark_price: Optional[Price] = None
    unrealized_pnl: Money = Money("0")
    leverage: int = 1
    margin_type: MarginType = MarginType.CROSSED

    def __post_init__(self) -> None:
        if self.quantity < 0:
            raise ValidationError("持仓数量不能为负")
        if self.leverage < 1:
            raise ValidationError("杠杆倍数必须 >= 1")
        if self.side == PositionSide.FLAT and self.quantity != 0:
            raise ValidationError("FLAT 状态数量必须为 0")

    def notional_value(self) -> Money:
        """名义价值"""
        price = self.mark_price or self.entry_price
        return price * self.quantity

    def margin_used(self) -> Money:
        """占用保证金"""
        if self.notional_value() == 0:
            return Money("0")
        return self.notional_value() / self.leverage

    def is_open(self) -> bool:
        """是否有持仓"""
        return self.side != PositionSide.FLAT and self.quantity > 0


@dataclass(frozen=True)
class Fill:
    """成交记录"""
    order_id: OrderId
    client_order_id: ClientOrderId
    symbol: Symbol
    side: OrderSide
    filled_qty: Quantity
    fill_price: Price
    fee: Money
    fee_asset: str
    is_maker: bool
    timestamp: Timestamp
    trade_id: str

    def notional(self) -> Money:
        """成交额"""
        return self.fill_price * self.filled_qty


@dataclass
class Order:
    """订单（可变，因为状态会变）

    注意：这里不用 frozen，因为订单状态会流转。
    状态变更通过方法控制，外部不直接修改。
    """
    order_id: OrderId
    client_order_id: ClientOrderId
    symbol: Symbol
    side: OrderSide
    order_type: OrderType
    quantity: Quantity
    price: Optional[Price] = None
    stop_price: Optional[Price] = None
    status: OrderStatus = OrderStatus.PENDING
    time_in_force: TimeInForce = TimeInForce.GTC
    strategy_id: Optional[StrategyId] = None
    exchange: Exchange = Exchange.BINANCE
    margin_type: MarginType = MarginType.CROSSED
    leverage: int = 1

    # 运行时字段
    filled_qty: Quantity = Decimal("0")
    avg_fill_price: Price = Decimal("0")
    fee: Money = Decimal("0")
    created_at: Timestamp = 0
    updated_at: Timestamp = 0
    trace_id: Optional[TraceId] = None
    idempotency_key: Optional[str] = None
    error_code: Optional[int] = None
    error_message: Optional[str] = None

    def __post_init__(self) -> None:
        if self.quantity <= 0:
            raise ValidationError(f"订单数量必须为正数: {self.quantity}")
        if self.order_type == OrderType.LIMIT and self.price is None:
            raise ValidationError("限价单必须指定价格")
        if self.price is not None and self.price <= 0:
            raise ValidationError(f"价格必须为正数: {self.price}")
        if self.leverage < 1:
            raise ValidationError("杠杆倍数必须 >= 1")

    def remaining_qty(self) -> Quantity:
        """剩余未成交量"""
        return self.quantity - self.filled_qty

    def is_active(self) -> bool:
        """是否处于活跃状态（可继续成交/取消）"""
        return self.status in (
            OrderStatus.PENDING,
            OrderStatus.ACKED,
            OrderStatus.PARTIALLY_FILLED,
        )

    def can_transition_to(self, new_status: OrderStatus) -> bool:
        """检查状态转换是否合法"""
        valid_transitions: Dict[OrderStatus, FrozenSet[OrderStatus]] = {
            OrderStatus.PENDING: frozenset({OrderStatus.ACKED, OrderStatus.REJECTED}),
            OrderStatus.ACKED: frozenset({
                OrderStatus.PARTIALLY_FILLED,
                OrderStatus.FILLED,
                OrderStatus.CANCELLED,
                OrderStatus.EXPIRED,
            }),
            OrderStatus.PARTIALLY_FILLED: frozenset({
                OrderStatus.FILLED,
                OrderStatus.CANCELLED,
                OrderStatus.EXPIRED,
            }),
            # 终态不可转换
            OrderStatus.FILLED: frozenset(),
            OrderStatus.CANCELLED: frozenset(),
            OrderStatus.REJECTED: frozenset(),
            OrderStatus.EXPIRED: frozenset(),
        }
        return new_status in valid_transitions.get(self.status, frozenset())


# ═══════════════════════════════════════════════════════════════
# 结果类型
# ═══════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class RiskCheckResult:
    """风控检查结果"""
    passed: bool
    reason: Optional[str] = None
    code: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OrderResult:
    """下单结果"""
    success: bool
    order_id: Optional[OrderId] = None
    error_code: Optional[int] = None
    error_message: Optional[str] = None


# ═══════════════════════════════════════════════════════════════
# 异常定义（精确子类，禁止裸 Exception）
# ═══════════════════════════════════════════════════════════════

class QuantError(Exception):
    """所有量化系统异常的基类"""
    code: int = 0
    severity: str = "error"

    def __init__(self, message: str, **context: Any) -> None:
        super().__init__(message)
        self.context = context


class ValidationError(QuantError):
    """数据校验失败"""
    code = 1000
    severity = "error"


class StateTransitionError(QuantError):
    """状态转换非法"""
    code = 1001


class NotFoundError(QuantError):
    """资源不存在"""
    code = 1002


class DuplicateError(QuantError):
    """重复操作"""
    code = 1003


class TimeoutError_(QuantError):
    """操作超时"""
    code = 1004
    severity = "warning"


# 别名，避免与 builtins.TimeoutError 冲突
TimeoutError = TimeoutError_

