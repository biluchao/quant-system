# -*- coding: utf-8 -*-
"""
domain/services.py — 领域服务

铁律声明：本文件严格执行八项铁律
领域服务封装跨实体的领域逻辑，无状态。
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from .entities import Order, Position, Account, OrderSide, OrderType, OrderStatus
from .value_objects import Money, Price, Quantity, Symbol
from .aggregates import OrderAggregate, PortfolioAggregate


class DomainServiceError(Exception):
    """领域服务异常"""
    pass


class MatchingService:
    """
    撮合领域服务

    职责：
    1. 订单匹配逻辑
    2. 价格优先级
    3. 成交生成
    """

    @staticmethod
    def can_match(buy_order: Order, sell_order: Order, current_price: Price) -> bool:
        """判断两笔订单是否能撮合"""
        if buy_order.symbol != sell_order.symbol:
            return False
        if buy_order.side == sell_order.side:
            return False

        # 价格检查
        if buy_order.order_type == OrderType.MARKET:
            return True
        if sell_order.order_type == OrderType.MARKET:
            return True

        if buy_order.price and sell_order.price:
            return buy_order.price.value >= sell_order.price.value

        return False

    @staticmethod
    def calculate_match_price(buy_order: Order, sell_order: Order, market_price: Price) -> Price:
        """计算撮合价格（按价格时间优先）"""
        if buy_order.order_type == OrderType.MARKET:
            return sell_order.price or market_price
        if sell_order.order_type == OrderType.MARKET:
            return buy_order.price or market_price

        # 取买方价格（价格优先）
        return buy_order.price or market_price

    @staticmethod
    def calculate_match_quantity(buy_order: Order, sell_order: Order) -> Quantity:
        """计算撮合数量（取较小值）"""
        buy_remaining = buy_order.remaining_quantity
        sell_remaining = sell_order.remaining_quantity
        return Quantity(min(buy_remaining.value, sell_remaining.value))

    @staticmethod
    def execute_match(
        buy_aggregate: OrderAggregate,
        sell_aggregate: OrderAggregate,
        market_price: Price,
    ) -> Tuple[Quantity, Price]:
        """执行撮合"""
        if not MatchingService.can_match(buy_aggregate.order, sell_aggregate.order, market_price):
            raise DomainServiceError("Orders cannot be matched")

        match_qty = MatchingService.calculate_match_quantity(
            buy_aggregate.order, sell_aggregate.order
        )
        match_price = MatchingService.calculate_match_price(
            buy_aggregate.order, sell_aggregate.order, market_price
        )

        # 执行成交
        buy_aggregate.fill(match_qty, match_price)
        sell_aggregate.fill(match_qty, match_price)

        return match_qty, match_price


class PricingService:
    """
    定价领域服务

    职责：
    1. 计算保证金
    2. 计算盈亏
    3. 计算手续费
    """

    @staticmethod
    def calculate_margin(
        quantity: Quantity,
        price: Price,
        leverage: Decimal = Decimal("1"),
        margin_rate: Decimal = Decimal("0.1"),
    ) -> Money:
        """计算保证金"""
        if leverage <= 0:
            raise DomainServiceError("Leverage must be positive")
        notional = quantity.value * price.value
        margin = notional * margin_rate / leverage
        return Money(margin)

    @staticmethod
    def calculate_pnl(
        entry_price: Price,
        exit_price: Price,
        quantity: Quantity,
        side: str,  # "long" or "short"
    ) -> Money:
        """计算盈亏"""
        if side == "long":
            pnl = (exit_price.value - entry_price.value) * quantity.value
        elif side == "short":
            pnl = (entry_price.value - exit_price.value) * quantity.value
        else:
            raise DomainServiceError(f"Invalid side: {side}")

        return Money(pnl)

    @staticmethod
    def calculate_fee(
        quantity: Quantity,
        price: Price,
        fee_rate: Decimal,
    ) -> Money:
        """计算手续费"""
        notional = quantity.value * price.value
        return Money(notional * fee_rate)

    @staticmethod
    def calculate_liquidation_price(
        entry_price: Price,
        quantity: Quantity,
        margin: Money,
        side: str,
        maintenance_margin_rate: Decimal = Decimal("0.005"),
    ) -> Price:
        """计算强平价"""
        notional = quantity.value * entry_price.value

        if side == "long":
            # 多仓强平：价格下跌到保证金不足
            liq_price = entry_price.value - (margin.value / quantity.value) * (Decimal("1") - maintenance_margin_rate)
        else:
            # 空仓强平：价格上涨到保证金不足
            liq_price = entry_price.value + (margin.value / quantity.value) * (Decimal("1") - maintenance_margin_rate)

        return Price(max(Decimal("0"), liq_price))


class RiskDomainService:
    """
    风控领域服务

    职责：
    1. 持仓风险检查
    2. 账户风险检查
    3. 订单风险预检
    """

    @staticmethod
    def check_order_risk(
        order: Order,
        account: Account,
        existing_positions: Dict[str, Position],
    ) -> List[str]:
        """订单风险预检"""
        risks: List[str] = []

        # 资金检查
        if order.price and order.quantity:
            notional = order.quantity.value * order.price.value
            if notional > account.available_balance.value:
                risks.append(f"Order notional {notional} exceeds available balance {account.available_balance.value}")

        # 持仓集中度检查
        symbol_key = str(order.symbol)
        if symbol_key in existing_positions:
            pos = existing_positions[symbol_key]
            if pos.side != PositionSide.FLAT:
                total_exposure = pos.quantity.value * (pos.current_price or pos.entry_price).value
                if total_exposure > account.total_equity().value * Decimal("0.3"):
                    risks.append(f"Position concentration exceeds 30% of equity")

        # 订单大小检查
        if order.quantity.value > Decimal("1000"):
            risks.append("Order size exceeds single order limit")

        return risks

    @staticmethod
    def check_portfolio_risk(portfolio: PortfolioAggregate) -> List[str]:
        """组合风险检查"""
        risks: List[str] = []

        # 回撤检查
        equity = portfolio.total_equity()
        if equity.value < portfolio.account.balance.value * Decimal("0.9"):
            risks.append("Portfolio drawdown exceeds 10%")

        # 保证金率检查
        if portfolio.account.margin_ratio > Decimal("0.8"):
            risks.append(f"Margin ratio {portfolio.account.margin_ratio} exceeds 80%")

        # 一致性检查
        consistency_errors = portfolio.consistency_check()
        risks.extend(consistency_errors)

        return risks

    @staticmethod
    def calculate_var(
        positions: Dict[str, Position],
        confidence: Decimal = Decimal("0.95"),
        horizon_days: int = 1,
    ) -> Money:
        """简化 VaR 计算（基于持仓规模）"""
        total_notional = Decimal("0")
        for pos in positions.values():
            if pos.side != PositionSide.FLAT and pos.current_price:
                total_notional += pos.quantity.value * pos.current_price.value

        # 简化假设：日波动率 2%
        daily_vol = Decimal("0.02")
        var = total_notional * daily_vol * Decimal(str(confidence))

        return Money(var)
