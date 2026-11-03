# -*- coding: utf-8 -*-
"""
domain/aggregates.py — 聚合根

铁律声明：本文件严格执行八项铁律
聚合根是事务边界，保证内部一致性。
"""

from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set

from .entities import Order, Position, Account, OrderStatus, PositionSide
from .value_objects import Money, Price, Quantity, Symbol
from .domain_events import DomainEvent


class AggregateError(Exception):
    """聚合异常"""
    pass


class AggregateRoot(ABC):
    """
    聚合根基类

    特性：
    1. 维护内部一致性
    2. 收集领域事件
    3. 版本号（乐观锁）
    """

    def __init__(self):
        self._domain_events: List[DomainEvent] = []
        self._version: int = 0
        self._id: str = ""

    @property
    def id(self) -> str:
        return self._id

    @property
    def version(self) -> int:
        return self._version

    def add_domain_event(self, event: DomainEvent) -> None:
        """添加领域事件"""
        self._domain_events.append(event)

    def clear_domain_events(self) -> List[DomainEvent]:
        """清除并返回所有领域事件"""
        events = list(self._domain_events)
        self._domain_events.clear()
        return events

    def get_domain_events(self) -> List[DomainEvent]:
        """获取领域事件（不清除）"""
        return list(self._domain_events)

    def increment_version(self) -> None:
        self._version += 1


@dataclass
class OrderAggregate(AggregateRoot):
    """
    订单聚合根

    包含：
    - 主订单（Order）
    - 关联的子订单（如 TWAP 拆单）
    - 执行记录

    一致性边界：
    - 订单状态变更必须同步更新子订单
    - 成交记录必须和订单数量一致
    """

    order: Order
    child_orders: List[Order] = field(default_factory=list)
    executions: List[Dict[str, Any]] = field(default_factory=list)

    def __post_init__(self):
        self._id = self.order.order_id

    def submit(self) -> None:
        """提交订单"""
        self.order = self.order.submit()
        self.add_domain_event(
            OrderCreatedEvent(
                aggregate_id=self._id,
                order_id=self.order.order_id,
                symbol=str(self.order.symbol),
                side=self.order.side.value,
                quantity=str(self.order.quantity.value),
                price=str(self.order.price.value) if self.order.price else None,
            )
        )
        self.increment_version()

    def fill(self, fill_qty: Quantity, fill_price: Price, execution_id: str = "") -> None:
        """成交"""
        old_status = self.order.status
        self.order = self.order.fill(fill_qty, fill_price)

        execution = {
            "execution_id": execution_id or f"exec_{uuid.uuid4().hex[:12]}",
            "quantity": fill_qty.value,
            "price": fill_price.value,
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        }
        self.executions.append(execution)

        self.add_domain_event(
            OrderFilledEvent(
                aggregate_id=self._id,
                order_id=self.order.order_id,
                fill_quantity=str(fill_qty.value),
                fill_price=str(fill_price.value),
                remaining_quantity=str(self.order.remaining_quantity.value),
                is_complete=self.order.is_filled,
            )
        )

        if self.order.is_filled and old_status != OrderStatus.FILLED:
            # 完全成交事件
            pass

        self.increment_version()

    def cancel(self, reason: str = "") -> None:
        """取消订单"""
        if self.order.status.is_terminal:
            raise AggregateError(f"Cannot cancel order in status {self.order.status.value}")

        self.order = self.order.cancel(reason)
        self.add_domain_event(
            OrderCancelledEvent(
                aggregate_id=self._id,
                order_id=self.order.order_id,
                reason=reason,
                remaining_quantity=str(self.order.remaining_quantity.value),
            )
        )
        self.increment_version()

    def add_child_order(self, child: Order) -> None:
        """添加子订单"""
        if child.strategy_id != self.order.strategy_id:
            raise AggregateError("Child order must belong to same strategy")
        self.child_orders.append(child)

    def total_filled_quantity(self) -> Quantity:
        """总成交数量"""
        return self.order.filled_quantity

    def is_complete(self) -> bool:
        """是否完成"""
        return self.order.status.is_terminal

    def consistency_check(self) -> List[str]:
        """一致性检查"""
        errors: List[str] = []

        # 检查执行记录总量是否匹配
        total_exec_qty = sum(Decimal(str(e["quantity"])) for e in self.executions)
        if total_exec_qty != self.order.filled_quantity.value:
            errors.append(
                f"Execution total {total_exec_qty} != order filled {self.order.filled_quantity.value}"
            )

        # 检查子订单总量
        child_total = sum(c.quantity.value for c in self.child_orders)
        if child_total > self.order.quantity.value:
            errors.append(f"Child orders total {child_total} exceeds parent {self.order.quantity.value}")

        return errors


@dataclass
class PortfolioAggregate(AggregateRoot):
    """
    投资组合聚合根

    包含：
    - 账户（Account）
    - 持仓列表（Position）
    - 策略关联

    一致性边界：
    - 开仓必须检查保证金
    - 平仓必须更新已实现盈亏
    - 账户余额和持仓价值必须一致
    """

    account: Account
    positions: Dict[str, Position] = field(default_factory=dict)  # symbol -> Position
    strategy_allocations: Dict[str, Money] = field(default_factory=dict)

    def __post_init__(self):
        self._id = self.account.account_id

    def open_position(
        self,
        symbol: Symbol,
        side: PositionSide,
        quantity: Quantity,
        entry_price: Price,
        order_id: str = "",
    ) -> None:
        """开仓"""
        key = str(symbol)

        # 保证金检查
        required_margin = Money(
            quantity.value * entry_price.value * Decimal("0.1")  # 假设 10% 保证金
        )
        if self.account.available_balance < required_margin:
            raise AggregateError(
                f"Insufficient margin: required {required_margin.value}, "
                f"available {self.account.available_balance.value}"
            )

        if key in self.positions and self.positions[key].side != PositionSide.FLAT:
            # 加仓
            self.positions[key] = self.positions[key].add_quantity(quantity, entry_price)
        else:
            # 新仓
            from .entities import Position
            pos = Position(
                position_id=f"pos_{uuid.uuid4().hex[:12]}",
                symbol=symbol,
                side=side,
                quantity=quantity,
                entry_price=entry_price,
                current_price=entry_price,
                account_id=self.account.account_id,
            )
            self.positions[key] = pos

        # 冻结保证金
        self.account = self.account.freeze(required_margin)

        self.add_domain_event(
            PositionOpenedEvent(
                aggregate_id=self._id,
                position_id=self.positions[key].position_id,
                symbol=str(symbol),
                side=side.value,
                quantity=str(quantity.value),
                entry_price=str(entry_price.value),
            )
        )
        self.increment_version()

    def close_position(
        self,
        symbol: Symbol,
        exit_price: Price,
        close_quantity: Optional[Quantity] = None,
    ) -> Money:
        """平仓"""
        key = str(symbol)
        if key not in self.positions or self.positions[key].side == PositionSide.FLAT:
            raise AggregateError(f"No open position for {symbol}")

        position = self.positions[key]
        qty_to_close = close_quantity or position.quantity

        # 计算已实现盈亏
        realized = position.reduce_quantity(qty_to_close, exit_price)
        self.positions[key] = realized

        # 解冻保证金并结算
        margin_to_free = Money(
            qty_to_close.value * position.entry_price.value * Decimal("0.1")
        )
        self.account = self.account.unfreeze(margin_to_free)

        # 将盈亏计入账户
        pnl = realized.realized_pnl - position.realized_pnl
        if pnl.value > 0:
            self.account = self.account.credit(pnl)
        else:
            self.account = self.account.debit(Money(abs(pnl.value)))

        self.add_domain_event(
            PositionClosedEvent(
                aggregate_id=self._id,
                position_id=position.position_id,
                symbol=str(symbol),
                closed_quantity=str(qty_to_close.value),
                exit_price=str(exit_price.value),
                realized_pnl=str(pnl.value),
            )
        )
        self.increment_version()
        return pnl

    def update_all_prices(self, prices: Dict[str, Price]) -> None:
        """更新所有持仓价格"""
        for key, price in prices.items():
            if key in self.positions:
                self.positions[key] = self.positions[key].update_price(price)

        # 更新账户未实现盈亏
        total_unrealized = Money(Decimal("0"))
        for pos in self.positions.values():
            if pos.unrealized_pnl:
                total_unrealized = Money(total_unrealized.value + pos.unrealized_pnl.value)

        self.account = self.account.replace(unrealized_pnl=total_unrealized)

    def total_equity(self) -> Money:
        """总权益"""
        return self.account.total_equity

    def consistency_check(self) -> List[str]:
        """一致性检查"""
        errors: List[str] = []

        # 持仓数量非负
        for key, pos in self.positions.items():
            if pos.side != PositionSide.FLAT and pos.quantity.value <= 0:
                errors.append(f"Position {key} has zero/negative quantity")

        # 保证金一致性
        total_margin = sum(
            p.quantity.value * p.entry_price.value * Decimal("0.1")
            for p in self.positions.values()
            if p.side != PositionSide.FLAT
        )
        if total_margin != self.account.margin_balance.value:
            errors.append(
                f"Margin mismatch: positions={total_margin}, account={self.account.margin_balance.value}"
            )

        return errors
