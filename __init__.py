# -*- coding: utf-8 -*-
"""
core 包初始化 — 量化系统基础设施层

铁律声明：本文件严格执行八项铁律（类型/接口/事件/异常/配置/命名统一 + 三层防线 + 幂等审计）
"""

from .types import (
    QuantError,
    SystemError,
    ModuleError,
    BusinessError,
    ValidationError,
    ErrorCode,
    EventType,
    EventPriority,
    OrderSide,
    OrderType,
    OrderStatus,
    PositionSide,
    TimeInForce,
    SystemMode,
    ModuleStatus,
    Decimal,
    Timestamp,
    EventId,
    OrderId,
    StrategyId,
    AccountId,
    ExchangeId,
    Symbol,
    Quantity,
    Price,
    Money,
    ImmutableBase,
    SystemEvent,
    OrderEvent,
    RiskEvent,
    AccountEvent,
    StrategyEvent,
    AlertEvent,
    EventEnvelope,
    SystemConfig,
)
from .circuit_breaker import CircuitBreaker, CircuitState, CircuitOpenError
from .idempotency import IdempotencyEngine, DuplicateRequestError
from .audit import AuditEngine, AuditEntry
from .resource_pool import ResourcePool, PoolExhaustedError
from .memory_watchdog import MemoryWatchdog
from .bulkhead import Bulkhead, BulkheadRejectedError
from .rate_limiter_hierarchy import RateLimiterHierarchy, RateLimitExceededError
from .dependency_injection import DIContainer, DIError
from .lazy_init import LazyInitializer
from .bootstrap import Bootstrap, BootstrapError

__all__ = [
    # types
    "QuantError", "SystemError", "ModuleError", "BusinessError", "ValidationError",
    "ErrorCode", "EventType", "EventPriority", "OrderSide", "OrderType", "OrderStatus",
    "PositionSide", "TimeInForce", "SystemMode", "ModuleStatus",
    "Decimal", "Timestamp", "EventId", "OrderId", "StrategyId", "AccountId", "ExchangeId",
    "Symbol", "Quantity", "Price", "Money", "ImmutableBase",
    "SystemEvent", "OrderEvent", "RiskEvent", "AccountEvent", "StrategyEvent", "AlertEvent",
    "EventEnvelope", "SystemConfig",
    # circuit_breaker
    "CircuitBreaker", "CircuitState", "CircuitOpenError",
    # idempotency
    "IdempotencyEngine", "DuplicateRequestError",
    # audit
    "AuditEngine", "AuditEntry",
    # resource_pool
    "ResourcePool", "PoolExhaustedError",
    # memory_watchdog
    "MemoryWatchdog",
    # bulkhead
    "Bulkhead", "BulkheadRejectedError",
    # rate_limiter
    "RateLimiterHierarchy", "RateLimitExceededError",
    # dependency_injection
    "DIContainer", "DIError",
    # lazy_init
    "LazyInitializer",
    # bootstrap
    "Bootstrap", "BootstrapError",
]

__version__ = "5.0.0-production"
