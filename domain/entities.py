# -*- coding: utf-8 -*-
"""
domain/entities.py — 核心领域实体

铁律声明：本文件严格执行八项铁律
实体具有唯一标识，生命周期内标识不变。
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Set

from .value_objects import (
    Money, Price, Quantity, Symbol, Instrument,
    TradeDirection, Timestamp,
)


class OrderStatus(str, Enum):
    """订单状态（有限状态机）"""
    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"

    @property
    def is_terminal(self) -> bool:
        return self in (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED)

    @property
    def is_active(self) -> bool:
        return self in (OrderStatus.PENDING, OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED)


class OrderSide(str, Enum):
    """订单方向"""
    BUY = "buy"
    SELL = "sell"

    @property
    def opposite(self) -> "OrderSide":
        return OrderSide.SELL if self == OrderSide.BUY else OrderSide.BUY


class OrderType(str, Enum):
    """订单类型"""
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"
    TWAP = "twap"
    VWAP = "vwap"


@dataclass(frozen=True)
class Order:
    """
    订单实体 — 不可变（通过 replace 产生新实例）

    业务规则：
    1. 限价单必须有价格
    2. 止损单必须有止损价
    3. 数量必须为正
    4. 状态转换必须符合 FSM
    """

    order_id: str
    symbol: Symbol
    side: OrderSide
    order_type: OrderType
    quantity: Quantity
    price: Optional[Price] = None
    stop_price: Optional[Price] = None
    status: OrderStatus = OrderStatus.PENDING
    filled_quantity: Quantity = field(default_factory=lambda: Quantity(Decimal("0")))
    average_fill_price: Optional[Price] = None
    remaining_quantity: Quantity = field(init=False)
    strategy_id: Optional[str] = None
    account_id: str = ""
    exchange: str = ""
    time_in_force: str = "GTC"  # GTC/IOC/FOK
    created_at: int = field(default_factory=lambda: int(datetime.now(timezone.utc).timestamp() * 1000))
    updated_at: int = field(default_factory=lambda: int(datetime.now(timezone.utc).timestamp() * 1000))
    tags: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        # 业务规则校验
        if self.quantity.value <= 0:
            raise OrderEntityError(f"Order quantity must be positive, got {self.quantity.value}")

        if self.order_type == OrderType.LIMIT and self.price is None:
            raise OrderEntityError("Limit order must have a price")

        if self.order_type in (OrderType.STOP, OrderType.STOP_LIMIT) and self.stop_price is None:
            raise OrderEntityError(f"{self.order_type.value} order must have stop_price")

        # 计算剩余数量
        object.__setattr__(self, 'remaining_quantity', self.quantity - self.filled_quantity)

    def replace(self, **changes) -> "Order":
        """创建新实例（不可变模式）"""
        return dataclass.replace(self, **changes)

    def fill(self, fill_qty: Quantity, fill_price: Price) -> "Order":
        """执行成交"""
        if self.status.is_terminal:
            raise OrderEntityError(f"Cannot fill order in status {self.status.value}")

        if fill_qty.value <= 0:
            raise OrderEntityError("Fill quantity must be positive")

        new_filled = self.filled_quantity + fill_qty
        if new_filled.value > self.quantity.value:
            raise OrderEntityError(
                f"Fill quantity {new_filled.value} exceeds order quantity {self.quantity.value}"
            )

        # 计算加权平均成交价
        if self.average_fill_price:
            total_value = (self.filled_quantity.value * self.average_fill_price.value + fill_qty.value * fill_price.value)
            new_avg = Price(total_value / new_filled.value)
        else:
            new_avg = fill_price

        new_status = OrderStatus.FILLED if new_filled.value == self.quantity.value else OrderStatus.PARTIALLY_FILLED

        return self.replace(
            filled_quantity=new_filled,
            average_fill_price=new_avg,
            status=new_status,
            updated_at=int(datetime.now(timezone.utc).timestamp() * 1000),
        )

    def cancel(self, reason: str = "") -> "Order":
        """取消订单"""
        if self.status.is_terminal:
            raise OrderEntityError(f"Cannot cancel order in status {self.status.value}")
        return self.replace(
            status=OrderStatus.CANCELLED,
            updated_at=int(datetime.now(timezone.utc).timestamp() * 1000),
            metadata={**self.metadata, "cancel_reason": reason},
        )

    def reject(self, reason: str = "") -> "Order":
        """拒绝订单"""
        return self.replace(
            status=OrderStatus.REJECTED,
            updated_at=int(datetime.now(timezone.utc).timestamp() * 1000),
            metadata={**self.metadata, "reject_reason": reason},
        )

    def submit(self) -> "Order":
        """提交订单"""
        if self.status != OrderStatus.PENDING:
            raise OrderEntityError(f"Cannot submit order in status {self.status.value}")
        return self.replace(
            status=OrderStatus.SUBMITTED,
            updated_at=int(datetime.now(timezone.utc).timestamp() * 1000),
        )

    @property
    def is_filled(self) -> bool:
        return self.status == OrderStatus.FILLED

    @property
    def is_active(self) -> bool:
        return self.status.is_active

    @property
    def fill_ratio(self) -> Decimal:
        if self.quantity.value == 0:
            return Decimal("0")
        return self.filled_quantity.value / self.quantity.value


class PositionSide(str, Enum):
    """持仓方向"""
    LONG = "long"
    SHORT = "short"
    FLAT = "flat"


@dataclass(frozen=True)
class Position:
    """
    持仓实体

    业务规则：
    1. 多仓数量为正，空仓数量为负
    2. 平仓时数量不能超过当前持仓
    3. 已实现盈亏 = Σ(平仓价 - 开仓均价) × 平仓量
    """

    position_id: str
    symbol: Symbol
    side: PositionSide
    quantity: Quantity
    entry_price: Price
    current_price: Optional[Price] = None
    unrealized_pnl: Optional[Money] = None
    realized_pnl: Money = field(default_factory=lambda: Money(Decimal("0")))
    margin_used: Optional[Money] = None
    account_id: str = ""
    strategy_id: Optional[str] = None
    opened_at: int = field(default_factory=lambda: int(datetime.now(timezone.utc).timestamp() * 1000))
    updated_at: int = field(default_factory=lambda: int(datetime.now(timezone.utc).timestamp() * 1000))
    orders: List[str] = field(default_factory=list)  # 关联订单ID
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.side == PositionSide.LONG and self.quantity.value < 0:
            raise PositionEntityError("Long position quantity must be non-negative")
        if self.side == PositionSide.SHORT and self.quantity.value > 0:
            raise PositionEntityError("Short position quantity must be non-positive")

    def replace(self, **changes) -> "Position":
        return dataclass.replace(self, **changes)

    def update_price(self, new_price: Price) -> "Position":
        """更新当前价格，重算未实现盈亏"""
        if self.side == PositionSide.FLAT:
            return self

        if self.side == PositionSide.LONG:
            pnl_value = (new_price.value - self.entry_price.value) * self.quantity.value
        else:  # SHORT
            pnl_value = (self.entry_price.value - new_price.value) * abs(self.quantity.value)

        return self.replace(
            current_price=new_price,
            unrealized_pnl=Money(pnl_value),
            updated_at=int(datetime.now(timezone.utc).timestamp() * 1000),
        )

    def add_quantity(self, qty: Quantity, price: Price) -> "Position":
        """加仓"""
        if self.side == PositionSide.FLAT:
            # 新开仓
            new_side = PositionSide.LONG if qty.value > 0 else PositionSide.SHORT
            return self.replace(
                side=new_side,
                quantity=qty if qty.value > 0 else Quantity(abs(qty.value)),
                entry_price=price,
                current_price=price,
                opened_at=int(datetime.now(timezone.utc).timestamp() * 1000),
            )

        # 同方向加仓，加权平均
        total_qty = self.quantity.value + (qty.value if self.side == PositionSide.LONG else -qty.value)
        if (self.side == PositionSide.LONG and total_qty < 0) or (self.side == PositionSide.SHORT and total_qty > 0):
            raise PositionEntityError("Adding opposite side requires closing existing position first")

        new_qty = Quantity(abs(total_qty))
        weighted_price = Price(
            (self.entry_price.value * self.quantity.value + price.value * abs(qty.value)) / new_qty.value
        )

        return self.replace(
            quantity=new_qty,
            entry_price=weighted_price,
            current_price=price,
            updated_at=int(datetime.now(timezone.utc).timestamp() * 1000),
        )

    def reduce_quantity(self, qty: Quantity, exit_price: Price) -> "Position":
        """减仓"""
        if self.side == PositionSide.FLAT:
            raise PositionEntityError("Cannot reduce flat position")

        reduce_abs = abs(qty.value)
        current_abs = self.quantity.value

        if reduce_abs > current_abs:
            raise PositionEntityError(f"Reduce quantity {reduce_abs} exceeds position {current_abs}")

        # 计算已实现盈亏
        if self.side == PositionSide.LONG:
            realized = (exit_price.value - self.entry_price.value) * reduce_abs
        else:
            realized = (self.entry_price.value - exit_price.value) * reduce_abs

        new_realized = self.realized_pnl + Money(realized)
        new_qty = Quantity(current_abs - reduce_abs)

        if new_qty.value == 0:
            new_side = PositionSide.FLAT
            new_entry = self.entry_price
        else:
            new_side = self.side
            new_entry = self.entry_price

        return self.replace(
            quantity=new_qty,
            side=new_side,
            entry_price=new_entry,
            realized_pnl=new_realized,
            current_price=exit_price,
            updated_at=int(datetime.now(timezone.utc).timestamp() * 1000),
        )

    @property
    def market_value(self) -> Money:
        if not self.current_price:
            return Money(Decimal("0"))
        return Money(self.current_price.value * self.quantity.value)

    @property
    def total_pnl(self) -> Money:
        unrealized = self.unrealized_pnl.value if self.unrealized_pnl else Decimal("0")
        return Money(unrealized + self.realized_pnl.value)


class AccountStatus(str, Enum):
    """账户状态"""
    ACTIVE = "active"
    SUSPENDED = "suspended"
    LIQUIDATING = "liquidating"
    CLOSED = "closed"


@dataclass(frozen=True)
class Account:
    """
    账户实体

    业务规则：
    1. 余额不能为负（除非允许信用）
    2. 保证金使用率不能超过阈值
    3. 出入金需要审计
    """

    account_id: str
    name: str
    currency: str = "USDT"
    balance: Money = field(default_factory=lambda: Money(Decimal("0")))
    frozen_balance: Money = field(default_factory=lambda: Money(Decimal("0")))
    available_balance: Money = field(init=False)
    margin_balance: Money = field(default_factory=lambda: Money(Decimal("0")))
    unrealized_pnl: Money = field(default_factory=lambda: Money(Decimal("0")))
    realized_pnl_today: Money = field(default_factory=lambda: Money(Decimal("0")))
    status: AccountStatus = AccountStatus.ACTIVE
    max_drawdown_pct: Decimal = Decimal("0.10")
    created_at: int = field(default_factory=lambda: int(datetime.now(timezone.utc).timestamp() * 1000))
    updated_at: int = field(default_factory=lambda: int(datetime.now(timezone.utc).timestamp() * 1000))
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        avail = self.balance.value - self.frozen_balance.value
        if avail < 0:
            raise AccountEntityError(f"Available balance cannot be negative: {avail}")
        object.__setattr__(self, 'available_balance', Money(avail))

    def replace(self, **changes) -> "Account":
        return dataclass.replace(self, **changes)

    def debit(self, amount: Money, reason: str = "") -> "Account":
        """扣款"""
        if amount.value <= 0:
            raise AccountEntityError("Debit amount must be positive")

        if self.available_balance.value < amount.value:
            raise AccountEntityError(
                f"Insufficient balance: available={self.available_balance.value}, requested={amount.value}"
            )

        return self.replace(
            balance=Money(self.balance.value - amount.value),
            updated_at=int(datetime.now(timezone.utc).timestamp() * 1000),
            metadata={**self.metadata, "last_debit": reason},
        )

    def credit(self, amount: Money, reason: str = "") -> "Account":
        """入账"""
        if amount.value <= 0:
            raise AccountEntityError("Credit amount must be positive")

        return self.replace(
            balance=Money(self.balance.value + amount.value),
            updated_at=int(datetime.now(timezone.utc).timestamp() * 1000),
            metadata={**self.metadata, "last_credit": reason},
        )

    def freeze(self, amount: Money) -> "Account":
        """冻结资金"""
        if self.available_balance.value < amount.value:
            raise AccountEntityError("Insufficient available balance to freeze")

        return self.replace(
            frozen_balance=Money(self.frozen_balance.value + amount.value),
        )

    def unfreeze(self, amount: Money) -> "Account":
        """解冻资金"""
        if self.frozen_balance.value < amount.value:
            raise AccountEntityError("Unfreeze amount exceeds frozen balance")

        return self.replace(
            frozen_balance=Money(self.frozen_balance.value - amount.value),
        )

    @property
    def total_equity(self) -> Money:
        return Money(self.balance.value + self.unrealized_pnl.value)

    @property
    def margin_ratio(self) -> Decimal:
        if self.margin_balance.value == 0:
            return Decimal("0")
        return self.margin_balance.value / self.total_equity().value


class StrategyStatus(str, Enum):
    """策略状态"""
    IDLE = "idle"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass(frozen=True)
class Strategy:
    """
    策略实体

    业务规则：
    1. 策略必须有唯一ID
    2. 状态转换必须符合 FSM
    3. 参数不可为空
    """

    strategy_id: str
    name: str
    strategy_type: str
    status: StrategyStatus = StrategyStatus.IDLE
    parameters: Dict[str, Any] = field(default_factory=dict)
    symbols: List[Symbol] = field(default_factory=list)
    account_id: Optional[str] = None
    max_positions: int = 10
    max_order_size: Optional[Quantity] = None
    pnl: Money = field(default_factory=lambda: Money(Decimal("0")))
    total_trades: int = 0
    winning_trades: int = 0
    created_at: int = field(default_factory=lambda: int(datetime.now(timezone.utc).timestamp() * 1000))
    started_at: Optional[int] = None
    stopped_at: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def replace(self, **changes) -> "Strategy":
        return dataclass.replace(self, **changes)

    def start(self) -> "Strategy":
        if self.status not in (StrategyStatus.IDLE, StrategyStatus.STOPPED):
            raise StrategyEntityError(f"Cannot start strategy in status {self.status.value}")
        return self.replace(
            status=StrategyStatus.RUNNING,
            started_at=int(datetime.now(timezone.utc).timestamp() * 1000),
        )

    def stop(self) -> "Strategy":
        if self.status != StrategyStatus.RUNNING:
            raise StrategyEntityError(f"Cannot stop strategy in status {self.status.value}")
        return self.replace(
            status=StrategyStatus.STOPPED,
            stopped_at=int(datetime.now(timezone.utc).timestamp() * 1000),
        )

    def record_trade(self, pnl: Money) -> "Strategy":
        """记录交易结果"""
        return self.replace(
            total_trades=self.total_trades + 1,
            winning_trades=self.winning_trades + (1 if pnl.value > 0 else 0),
            pnl=self.pnl + pnl,
        )

    @property
    def win_rate(self) -> Decimal:
        if self.total_trades == 0:
            return Decimal("0")
        return Decimal(self.winning_trades) / Decimal(self.total_trades)


# ─── 异常 ────────────────────────────────────────────────────

class OrderEntityError(Exception):
    pass


class PositionEntityError(Exception):
    pass


class AccountEntityError(Exception):
    pass


class StrategyEntityError(Exception):
    pass
