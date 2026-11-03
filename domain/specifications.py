# -*- coding: utf-8 -*-
"""
domain/specifications.py — 规约模式

铁律声明：本文件严格执行八项铁律
规约封装业务规则，可组合、可复用。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, List, Optional, TypeVar

from .entities import Order, Position, OrderStatus, OrderSide, PositionSide
from .value_objects import Money, Price, Quantity, Symbol

T = TypeVar("T")


class Specification(ABC):
    """规约基类"""

    @abstractmethod
    def is_satisfied_by(self, candidate: T) -> bool:
        pass

    def and_(self, other: "Specification[T]") -> "AndSpecification[T]":
        return AndSpecification(self, other)

    def or_(self, other: "Specification[T]") -> "OrSpecification[T]":
        return OrSpecification(self, other)

    def not_(self) -> "NotSpecification[T]":
        return NotSpecification(self)


class CompositeSpecification(Specification[T]):
    """组合规约基类"""
    pass


@dataclass
class AndSpecification(CompositeSpecification[T]):
    left: Specification[T]
    right: Specification[T]

    def is_satisfied_by(self, candidate: T) -> bool:
        return self.left.is_satisfied_by(candidate) and self.right.is_satisfied_by(candidate)


@dataclass
class OrSpecification(CompositeSpecification[T]):
    left: Specification[T]
    right: Specification[T]

    def is_satisfied_by(self, candidate: T) -> bool:
        return self.left.is_satisfied_by(candidate) or self.right.is_satisfied_by(candidate)


@dataclass
class NotSpecification(CompositeSpecification[T]):
    spec: Specification[T]

    def is_satisfied_by(self, candidate: T) -> bool:
        return not self.spec.is_satisfied_by(candidate)


# ─── 订单规约 ────────────────────────────────────────────────

class OrderSpecifications:
    """订单规约集合"""

    class ActiveOrder(Specification[Order]):
        def is_satisfied_by(self, order: Order) -> bool:
            return order.is_active

    class TerminalOrder(Specification[Order]):
        def is_satisfied_by(self, order: Order) -> bool:
            return order.status.is_terminal

    class BuyOrder(Specification[Order]):
        def is_satisfied_by(self, order: Order) -> bool:
            return order.side == OrderSide.BUY

    class SellOrder(Specification[Order]):
        def is_satisfied_by(self, order: Order) -> bool:
            return order.side == OrderSide.SELL

    class LimitOrder(Specification[Order]):
        def is_satisfied_by(self, order: Order) -> bool:
            return order.order_type == OrderType.LIMIT

    class MarketOrder(Specification[Order]):
        def is_satisfied_by(self, order: Order) -> bool:
            return order.order_type == OrderType.MARKET

    class MinQuantity(Specification[Order]):
        def __init__(self, min_qty: Decimal):
            self.min_qty = min_qty

        def is_satisfied_by(self, order: Order) -> bool:
            return order.quantity.value >= self.min_qty

    class SymbolMatch(Specification[Order]):
        def __init__(self, symbol: Symbol):
            self.symbol = symbol

        def is_satisfied_by(self, order: Order) -> bool:
            return order.symbol == self.symbol

    class StrategyMatch(Specification[Order]):
        def __init__(self, strategy_id: str):
            self.strategy_id = strategy_id

        def is_satisfied_by(self, order: Order) -> bool:
            return order.strategy_id == self.strategy_id

    class FillRatioAbove(Specification[Order]):
        def __init__(self, threshold: Decimal):
            self.threshold = threshold

        def is_satisfied_by(self, order: Order) -> bool:
            return order.fill_ratio >= self.threshold

    class CreatedAfter(Specification[Order]):
        def __init__(self, timestamp: int):
            self.timestamp = timestamp

        def is_satisfied_by(self, order: Order) -> bool:
            return order.created_at >= self.timestamp


# ─── 持仓规约 ────────────────────────────────────────────────

class PositionSpecifications:
    """持仓规约集合"""

    class OpenPosition(Specification[Position]):
        def is_satisfied_by(self, position: Position) -> bool:
            return position.side != PositionSide.FLAT

    class LongPosition(Specification[Position]):
        def is_satisfied_by(self, position: Position) -> bool:
            return position.side == PositionSide.LONG

    class ShortPosition(Specification[Position]):
        def is_satisfied_by(self, position: Position) -> bool:
            return position.side == PositionSide.SHORT

    class ProfitablePosition(Specification[Position]):
        def is_satisfied_by(self, position: Position) -> bool:
            return position.total_pnl.value > 0

    class LosingPosition(Specification[Position]):
        def is_satisfied_by(self, position: Position) -> bool:
            return position.total_pnl.value < 0

    class MinPositionSize(Specification[Position]):
        def __init__(self, min_qty: Decimal):
            self.min_qty = min_qty

        def is_satisfied_by(self, position: Position) -> bool:
            return position.quantity.value >= self.min_qty

    class SymbolMatch(Specification[Position]):
        def __init__(self, symbol: Symbol):
            self.symbol = symbol

        def is_satisfied_by(self, position: Position) -> bool:
            return position.symbol == self.symbol

    class UnrealizedPnlAbove(Specification[Position]):
        def __init__(self, threshold: Money):
            self.threshold = threshold

        def is_satisfied_by(self, position: Position) -> bool:
            if not position.unrealized_pnl:
                return False
            return position.unrealized_pnl.value >= self.threshold.value

    class DrawdownExceeds(Specification[Position]):
        def __init__(self, threshold_pct: Decimal):
            self.threshold_pct = threshold_pct

        def is_satisfied_by(self, position: Position) -> bool:
            if not position.current_price or position.entry_price.value == 0:
                return False
            if position.side == PositionSide.LONG:
                drawdown = (position.entry_price.value - position.current_price.value) / position.entry_price.value
            else:
                drawdown = (position.current_price.value - position.entry_price.value) / position.entry_price.value
            return drawdown > self.threshold_pct


# 需要导入 OrderType
from .entities import OrderType
