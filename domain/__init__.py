# -*- coding: utf-8 -*-
"""
domain 包初始化 — 领域层

铁律声明：本文件严格执行八项铁律（领域纯净 / 无框架依赖 / 不可变值对象）
"""

from .entities import (
    Order, OrderStatus, OrderSide, OrderType,
    Position, PositionSide,
    Account, AccountStatus,
    Strategy, StrategyStatus,
)
from .value_objects import (
    Money, Price, Quantity, Symbol, Instrument,
    Exchange, Currency, Timestamp, TradeDirection,
)
from .aggregates import (
    OrderAggregate, PortfolioAggregate,
    AggregateRoot, AggregateError,
)
from .domain_events import (
    OrderCreatedEvent, OrderFilledEvent, OrderCancelledEvent,
    PositionOpenedEvent, PositionClosedEvent,
    AccountDebitedEvent, AccountCreditedEvent,
    StrategyStartedEvent, StrategyStoppedEvent,
    DomainEvent,
)
from .services import (
    MatchingService, PricingService, RiskDomainService,
    DomainServiceError,
)
from .repositories import (
    OrderRepository, PositionRepository, AccountRepository,
    RepositoryError, Repository,
)
from .specifications import (
    OrderSpecifications, PositionSpecifications,
    Specification, CompositeSpecification,
)
from .factory import (
    OrderFactory, PositionFactory, AccountFactory,
    DomainFactoryError,
)

__all__ = [
    # entities
    "Order", "OrderStatus", "OrderSide", "OrderType",
    "Position", "PositionSide",
    "Account", "AccountStatus",
    "Strategy", "StrategyStatus",
    # value_objects
    "Money", "Price", "Quantity", "Symbol", "Instrument",
    "Exchange", "Currency", "Timestamp", "TradeDirection",
    # aggregates
    "OrderAggregate", "PortfolioAggregate", "AggregateRoot", "AggregateError",
    # domain_events
    "OrderCreatedEvent", "OrderFilledEvent", "OrderCancelledEvent",
    "PositionOpenedEvent", "PositionClosedEvent",
    "AccountDebitedEvent", "AccountCreditedEvent",
    "StrategyStartedEvent", "StrategyStoppedEvent",
    "DomainEvent",
    # services
    "MatchingService", "PricingService", "RiskDomainService", "DomainServiceError",
    # repositories
    "OrderRepository", "PositionRepository", "AccountRepository", "Repository", "RepositoryError",
    # specifications
    "OrderSpecifications", "PositionSpecifications", "Specification", "CompositeSpecification",
    # factory
    "OrderFactory", "PositionFactory", "AccountFactory", "DomainFactoryError",
]

__version__ = "5.0.0-production"
