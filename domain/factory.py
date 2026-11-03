# -*- coding: utf-8 -*-
"""
domain/factory.py — 领域工厂

铁律声明：本文件严格执行八项铁律
工厂封装复杂对象的创建逻辑，保证不变量。
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from .entities import (
    Order, OrderStatus, OrderSide, OrderType,
    Position, PositionSide,
    Account, AccountStatus,
    Strategy, StrategyStatus,
)
from .value_objects import Money, Price, Quantity, Symbol, Exchange, Currency
from .aggregates import OrderAggregate, PortfolioAggregate


class DomainFactoryError(Exception):
    """领域工厂异常"""
    pass


class OrderFactory:
    """
    订单工厂

    封装订单创建逻辑，保证所有业务规则。
    """

    @staticmethod
    def create_market_order(
        symbol: Symbol,
        side: OrderSide,
        quantity: Quantity,
        *,
        strategy_id: Optional[str] = None,
        account_id: str = "",
        exchange: str = "",
        tags: Optional[Dict[str, str]] = None,
    ) -> Order:
        """创建市价单"""
        return Order(
            order_id=f"ord_{uuid.uuid4().hex[:16]}",
            symbol=symbol,
            side=side,
            order_type=OrderType.MARKET,
            quantity=quantity,
            strategy_id=strategy_id,
            account_id=account_id,
            exchange=exchange,
            tags=tags or {},
        )

    @staticmethod
    def create_limit_order(
        symbol: Symbol,
        side: OrderSide,
        quantity: Quantity,
        price: Price,
        *,
        strategy_id: Optional[str] = None,
        account_id: str = "",
        exchange: str = "",
        time_in_force: str = "GTC",
        stop_price: Optional[Price] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> Order:
        """创建限价单"""
        if price.value <= 0:
            raise DomainFactoryError(f"Limit price must be positive: {price.value}")

        return Order(
            order_id=f"ord_{uuid.uuid4().hex[:16]}",
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            quantity=quantity,
            price=price,
            stop_price=stop_price,
            strategy_id=strategy_id,
            account_id=account_id,
            exchange=exchange,
            time_in_force=time_in_force,
            tags=tags or {},
        )

    @staticmethod
    def create_stop_order(
        symbol: Symbol,
        side: OrderSide,
        quantity: Quantity,
        stop_price: Price,
        *,
        limit_price: Optional[Price] = None,
        strategy_id: Optional[str] = None,
        account_id: str = "",
        exchange: str = "",
        tags: Optional[Dict[str, str]] = None,
    ) -> Order:
        """创建止损单"""
        if stop_price.value <= 0:
            raise DomainFactoryError(f"Stop price must be positive: {stop_price.value}")

        order_type = OrderType.STOP_LIMIT if limit_price else OrderType.STOP

        return Order(
            order_id=f"ord_{uuid.uuid4().hex[:16]}",
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=limit_price,
            stop_price=stop_price,
            strategy_id=strategy_id,
            account_id=account_id,
            exchange=exchange,
            tags=tags or {},
        )

    @staticmethod
    def create_twap_order(
        symbol: Symbol,
        side: OrderSide,
        total_quantity: Quantity,
        duration_sec: int,
        slices: int,
        *,
        strategy_id: Optional[str] = None,
        account_id: str = "",
        exchange: str = "",
        tags: Optional[Dict[str, str]] = None,
    ) -> List[Order]:
        """创建 TWAP 拆单（返回子订单列表）"""
        if duration_sec <= 0 or slices <= 0:
            raise DomainFactoryError("Duration and slices must be positive")

        slice_qty = Quantity(total_quantity.value / Decimal(str(slices)))
        orders = []

        for i in range(slices):
            order = Order(
                order_id=f"ord_{uuid.uuid4().hex[:16]}",
                symbol=symbol,
                side=side,
                order_type=OrderType.TWAP,
                quantity=slice_qty,
                strategy_id=strategy_id,
                account_id=account_id,
                exchange=exchange,
                tags={**(tags or {}), "twap_slice": str(i), "twap_total": str(slices)},
            )
            orders.append(order)

        return orders

    @staticmethod
    def create_order_aggregate(order: Order) -> OrderAggregate:
        """创建订单聚合"""
        return OrderAggregate(order=order)


class PositionFactory:
    """
    持仓工厂
    """

    @staticmethod
    def create_position(
        symbol: Symbol,
        side: PositionSide,
        quantity: Quantity,
        entry_price: Price,
        *,
        account_id: str = "",
        strategy_id: Optional[str] = None,
    ) -> Position:
        """创建持仓"""
        if side == PositionSide.FLAT:
            raise DomainFactoryError("Cannot create flat position")

        return Position(
            position_id=f"pos_{uuid.uuid4().hex[:16]}",
            symbol=symbol,
            side=side,
            quantity=quantity,
            entry_price=entry_price,
            current_price=entry_price,
            account_id=account_id,
            strategy_id=strategy_id,
        )


class AccountFactory:
    """
    账户工厂
    """

    @staticmethod
    def create_account(
        name: str,
        *,
        currency: str = "USDT",
        initial_balance: Money = None,
        max_drawdown_pct: Decimal = Decimal("0.10"),
    ) -> Account:
        """创建账户"""
        if not name:
            raise DomainFactoryError("Account name cannot be empty")

        return Account(
            account_id=f"acc_{uuid.uuid4().hex[:16]}",
            name=name,
            currency=currency,
            balance=initial_balance or Money(Decimal("0")),
            max_drawdown_pct=max_drawdown_pct,
        )

    @staticmethod
    def create_portfolio_aggregate(account: Account) -> PortfolioAggregate:
        """创建投资组合聚合"""
        return PortfolioAggregate(account=account)


class StrategyFactory:
    """
    策略工厂
    """

    @staticmethod
    def create_strategy(
        name: str,
        strategy_type: str,
        *,
        parameters: Optional[Dict[str, Any]] = None,
        symbols: Optional[List[Symbol]] = None,
        account_id: Optional[str] = None,
        max_positions: int = 10,
    ) -> Strategy:
        """创建策略"""
        if not name:
            raise DomainFactoryError("Strategy name cannot be empty")
        if not strategy_type:
            raise DomainFactoryError("Strategy type cannot be empty")

        return Strategy(
            strategy_id=f"strat_{uuid.uuid4().hex[:16]}",
            name=name,
            strategy_type=strategy_type,
            parameters=parameters or {},
            symbols=symbols or [],
            account_id=account_id,
            max_positions=max_positions,
        )
