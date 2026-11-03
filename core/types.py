# -*- coding: utf-8 -*-
"""
core/types.py — 全局类型定义（八项铁律·类型统一）

铁律声明：本文件严格执行八项铁律
所有金额/价格/数量使用 Decimal，禁止 float 参与业务计算。
所有事件/数据对象不可变（frozen dataclass）。
前后端类型通过 TypeScript 生成工具从本文件提取对齐。
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal as _Decimal, ROUND_HALF_UP, ROUND_FLOOR, ROUND_CEILING
from enum import Enum, IntEnum
from typing import (
    Any, Callable, Dict, List, Optional, Set, Tuple, Type, TypeVar, Union,
    Generic, Protocol, runtime_checkable, NewType, ClassVar,
)
import hashlib
import json

# ─────────────────────────────────────────────────────────────
# 基础类型别名（NewType 保证类型安全）
# ─────────────────────────────────────────────────────────────

Decimal = _Decimal
Timestamp = NewType("Timestamp", int)       # Unix毫秒时间戳
EventId = NewType("EventId", str)           # 事件唯一ID
OrderId = NewType("OrderId", str)           # 订单ID
StrategyId = NewType("StrategyId", str)     # 策略ID
AccountId = NewType("AccountId", str)       # 账户ID
ExchangeId = NewType("ExchangeId", str)     # 交易所ID
Symbol = NewType("Symbol", str)             # 交易对
Quantity = NewType("Quantity", Decimal)     # 数量
Price = NewType("Price", Decimal)           # 价格
Money = NewType("Money", Decimal)           # 金额（本币）

T = TypeVar("T")
TKey = TypeVar("TKey")
TValue = TypeVar("TValue")


# ─────────────────────────────────────────────────────────────
# 枚举定义（命名统一：PascalCase）
# ─────────────────────────────────────────────────────────────

class EventType(str, Enum):
    """事件类型枚举 — 命名格式: {domain}.{entity}.{action}"""
    # 系统
    SYSTEM_STARTUP = "system.startup"
    SYSTEM_SHUTDOWN = "system.shutdown"
    SYSTEM_MODULE_STATUS_CHANGED = "system.module.status_changed"
    SYSTEM_HEALTH_CHECK = "system.health_check"
    SYSTEM_CONFIG_CHANGED = "system.config.changed"
    SYSTEM_CLOCK_DRIFT = "system.clock.drift"
    # 交易
    EXECUTION_ORDER_SUBMITTED = "execution.order.submitted"
    EXECUTION_ORDER_FILLED = "execution.order.filled"
    EXECUTION_ORDER_PARTIALLY_FILLED = "execution.order.partially_filled"
    EXECUTION_ORDER_CANCELED = "execution.order.canceled"
    EXECUTION_ORDER_REJECTED = "execution.order.rejected"
    EXECUTION_ORDER_EXPIRED = "execution.order.expired"
    # 风控
    RISK_VIOLATION_DETECTED = "risk.violation.detected"
    RISK_CIRCUIT_BREAKER_TRIGGERED = "risk.circuit_breaker.triggered"
    RISK_CIRCUIT_BREAKER_RESET = "risk.circuit_breaker.reset"
    RISK_MARGIN_CALL = "risk.margin_call"
    # 账户
    ACCOUNT_BALANCE_UPDATED = "account.balance.updated"
    ACCOUNT_POSITION_OPENED = "account.position.opened"
    ACCOUNT_POSITION_CLOSED = "account.position.closed"
    ACCOUNT_POSITION_UPDATED = "account.position.updated"
    ACCOUNT_PNL_REALIZED = "account.pnl.realized"
    # 策略
    STRATEGY_STARTED = "strategy.started"
    STRATEGY_STOPPED = "strategy.stopped"
    STRATEGY_PAUSED = "strategy.paused"
    STRATEGY_RESUMED = "strategy.resumed"
    STRATEGY_ERROR = "strategy.error"
    STRATEGY_SIGNAL_GENERATED = "strategy.signal.generated"
    # 告警
    ALERT_TRIGGERED = "alert.triggered"
    ALERT_ACKNOWLEDGED = "alert.acknowledged"
    ALERT_ESCALATED = "alert.escalated"
    ALERT_RESOLVED = "alert.resolved"


class EventPriority(IntEnum):
    """事件优先级"""
    P0_CRITICAL = 0
    P1_HIGH = 1
    P2_MEDIUM = 2
    P3_LOW = 3


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"


class OrderStatus(str, Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"

    def can_transition_to(self, target: "OrderStatus") -> bool:
        """订单状态机 — 严格单向流转"""
        transitions: Dict[OrderStatus, Set[OrderStatus]] = {
            OrderStatus.PENDING: {OrderStatus.SUBMITTED, OrderStatus.REJECTED, OrderStatus.CANCELED},
            OrderStatus.SUBMITTED: {
                OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED,
                OrderStatus.CANCELED, OrderStatus.REJECTED, OrderStatus.EXPIRED,
            },
            OrderStatus.PARTIALLY_FILLED: {
                OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.EXPIRED,
            },
            OrderStatus.FILLED: set(),          # 终态
            OrderStatus.CANCELED: set(),         # 终态
            OrderStatus.REJECTED: set(),         # 终态
            OrderStatus.EXPIRED: set(),          # 终态
        }
        return target in transitions.get(self, set())


class PositionSide(str, Enum):
    LONG = "long"
    SHORT = "short"
    FLAT = "flat"


class TimeInForce(str, Enum):
    GTC = "gtc"          # Good Till Cancel
    IOC = "ioc"          # Immediate Or Cancel
    FOK = "fok"          # Fill Or Kill
    GTD = "gtd"          # Good Till Date


class SystemMode(str, Enum):
    LIVE = "live"
    PAPER = "paper"
    BACKTEST = "backtest"
    DRY_RUN = "dry_run"


class ModuleStatus(str, Enum):
    INITIALIZING = "initializing"
    RUNNING = "running"
    DEGRADED = "degraded"
    STOPPED = "stopped"
    FAILED = "failed"


# ─────────────────────────────────────────────────────────────
# 异常层级（八项铁律·异常统一）
# ─────────────────────────────────────────────────────────────

class QuantError(Exception):
    """量化系统基础异常"""
    def __init__(self, code: str, message: str, details: Optional[Dict[str, Any]] = None):
        self.code = code
        self.message = message
        self.details = details or {}
        self.timestamp = _utc_now_ms()
        super().__init__(f"[{code}] {message}")


class SystemError(QuantError):
    """系统级异常（L1）"""
    pass


class ModuleError(QuantError):
    """模块级异常（L2）"""
    pass


class BusinessError(QuantError):
    """业务级异常（L3）"""
    pass


class ValidationError(QuantError):
    """校验级异常（L4）"""
    pass


class ErrorCode:
    """错误码常量 — 格式: {模块}_{子模块}_{序号}"""
    # core
    CORE_BOOTSTRAP_001 = "CORE_BOOTSTRAP_001"
    CORE_BOOTSTRAP_002 = "CORE_BOOTSTRAP_002"
    CORE_DI_001 = "CORE_DI_001"
    CORE_LAZY_INIT_001 = "CORE_LAZY_INIT_001"
    CORE_RESOURCE_POOL_001 = "CORE_RESOURCE_POOL_001"
    CORE_MEMORY_WATCHDOG_001 = "CORE_MEMORY_WATCHDOG_001"
    CORE_BULKHEAD_001 = "CORE_BULKHEAD_001"
    CORE_RATE_LIMIT_001 = "CORE_RATE_LIMIT_001"
    CORE_CIRCUIT_001 = "CORE_CIRCUIT_001"
    CORE_IDEMPOTENCY_001 = "CORE_IDEMPOTENCY_001"
    CORE_AUDIT_001 = "CORE_AUDIT_001"
    # risk
    RISK_ENGINE_001 = "RISK_ENGINE_001"
    RISK_ENGINE_002 = "RISK_ENGINE_002"
    # execution
    EXECUTION_ORDER_001 = "EXECUTION_ORDER_001"
    EXECUTION_ORDER_002 = "EXECUTION_ORDER_002"
    # gateway
    GATEWAY_BINANCE_001 = "GATEWAY_BINANCE_001"
    GATEWAY_OKX_001 = "GATEWAY_OKX_001"


# ─────────────────────────────────────────────────────────────
# 不可变基础数据类（八项铁律·幂等审计）
# ─────────────────────────────────────────────────────────────

def _utc_now_ms() -> Timestamp:
    """当前UTC毫秒时间戳"""
    return Timestamp(int(datetime.now(timezone.utc).timestamp() * 1000))


def _gen_event_id() -> EventId:
    """生成事件唯一ID"""
    return EventId(f"evt_{uuid.uuid4().hex}")


def _hash_dict(data: Dict[str, Any]) -> str:
    """字典哈希（用于幂等键生成）"""
    canonical = json.dumps(data, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:32]


@dataclass(frozen=True)
class ImmutableBase:
    """不可变对象基类"""
    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if not k.startswith("_")}

    def replace(self: T, **changes: Any) -> T:
        """创建替换字段后的新实例"""
        # frozen dataclass 不能直接修改，用 dataclasses.replace
        from dataclasses import replace
        return replace(self, **changes)


@dataclass(frozen=True)
class SystemEvent(ImmutableBase):
    """系统事件"""
    event_id: EventId = field(default_factory=_gen_event_id)
    event_type: EventType = EventType.SYSTEM_HEALTH_CHECK
    timestamp: Timestamp = field(default_factory=_utc_now_ms)
    source: str = "system"
    priority: EventPriority = EventPriority.P3_LOW
    payload: Dict[str, Any] = field(default_factory=dict)
    version: str = "1.0"


@dataclass(frozen=True)
class OrderEvent(ImmutableBase):
    """订单事件"""
    event_id: EventId = field(default_factory=_gen_event_id)
    event_type: EventType = EventType.EXECUTION_ORDER_SUBMITTED
    timestamp: Timestamp = field(default_factory=_utc_now_ms)
    source: str = "execution"
    priority: EventPriority = EventPriority.P1_HIGH
    order_id: OrderId = field(default_factory=lambda: OrderId(""))
    symbol: Symbol = field(default_factory=lambda: Symbol(""))
    side: OrderSide = OrderSide.BUY
    order_type: OrderType = OrderType.LIMIT
    quantity: Quantity = field(default_factory=lambda: Quantity(Decimal("0")))
    price: Optional[Price] = None
    status: OrderStatus = OrderStatus.PENDING
    filled_quantity: Quantity = field(default_factory=lambda: Quantity(Decimal("0")))
    filled_price: Optional[Price] = None
    exchange: ExchangeId = field(default_factory=lambda: ExchangeId(""))
    strategy_id: Optional[StrategyId] = None
    payload: Dict[str, Any] = field(default_factory=dict)
    version: str = "1.0"


@dataclass(frozen=True)
class RiskEvent(ImmutableBase):
    """风控事件"""
    event_id: EventId = field(default_factory=_gen_event_id)
    event_type: EventType = EventType.RISK_VIOLATION_DETECTED
    timestamp: Timestamp = field(default_factory=_utc_now_ms)
    source: str = "risk"
    priority: EventPriority = EventPriority.P0_CRITICAL
    rule_name: str = ""
    violation_level: str = "warning"  # warning / critical / fatal
    current_value: Optional[Decimal] = None
    threshold_value: Optional[Decimal] = None
    payload: Dict[str, Any] = field(default_factory=dict)
    version: str = "1.0"


@dataclass(frozen=True)
class AccountEvent(ImmutableBase):
    """账户事件"""
    event_id: EventId = field(default_factory=_gen_event_id)
    event_type: EventType = EventType.ACCOUNT_BALANCE_UPDATED
    timestamp: Timestamp = field(default_factory=_utc_now_ms)
    source: str = "account"
    priority: EventPriority = EventPriority.P2_MEDIUM
    account_id: AccountId = field(default_factory=lambda: AccountId(""))
    exchange: ExchangeId = field(default_factory=lambda: ExchangeId(""))
    symbol: Optional[Symbol] = None
    balance: Optional[Money] = None
    position_side: Optional[PositionSide] = None
    payload: Dict[str, Any] = field(default_factory=dict)
    version: str = "1.0"


@dataclass(frozen=True)
class StrategyEvent(ImmutableBase):
    """策略事件"""
    event_id: EventId = field(default_factory=_gen_event_id)
    event_type: EventType = EventType.STRATEGY_STARTED
    timestamp: Timestamp = field(default_factory=_utc_now_ms)
    source: str = "strategy"
    priority: EventPriority = EventPriority.P2_MEDIUM
    strategy_id: StrategyId = field(default_factory=lambda: StrategyId(""))
    payload: Dict[str, Any] = field(default_factory=dict)
    version: str = "1.0"


@dataclass(frozen=True)
class AlertEvent(ImmutableBase):
    """告警事件"""
    event_id: EventId = field(default_factory=_gen_event_id)
    event_type: EventType = EventType.ALERT_TRIGGERED
    timestamp: Timestamp = field(default_factory=_utc_now_ms)
    source: str = "alerting"
    priority: EventPriority = EventPriority.P1_HIGH
    alert_id: str = ""
    alert_level: str = "P2"
    message: str = ""
    acknowledged: bool = False
    escalated: bool = False
    payload: Dict[str, Any] = field(default_factory=dict)
    version: str = "1.0"


@dataclass(frozen=True)
class EventEnvelope(ImmutableBase):
    """
    事件信封 — 所有事件通过此信封包装传输
    八项铁律：事件统一、幂等审计
    """
    envelope_id: EventId = field(default_factory=_gen_event_id)
    created_at: Timestamp = field(default_factory=_utc_now_ms)
    event: Any = None  # SystemEvent | OrderEvent | RiskEvent | AccountEvent | StrategyEvent | AlertEvent
    producer: str = ""
    idempotency_key: Optional[str] = None
    ttl_ms: Optional[int] = None
    retry_count: int = 0
    trace_id: Optional[str] = None

    def is_expired(self) -> bool:
        if self.ttl_ms is None:
            return False
        now = _utc_now_ms()
        return (now - self.created_at) > self.ttl_ms


# ─────────────────────────────────────────────────────────────
# 配置类型
# ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class SystemConfig(ImmutableBase):
    """系统全局配置（版本化 + 校验 + 热重载）"""
    version: str = "1.0.0"
    mode: SystemMode = SystemMode.PAPER
    max_strategies: int = 50
    max_orders_per_second: int = 100
    max_positions_per_strategy: int = 20
    default_exchange: ExchangeId = field(default_factory=lambda: ExchangeId("binance"))
    risk_enabled: bool = True
    audit_enabled: bool = True
    reconciliation_interval_sec: int = 60
    websocket_reconnect_interval_sec: int = 5
    websocket_max_retries: int = 10
    db_connection_pool_size: int = 20
    redis_connection_pool_size: int = 50
    memory_limit_mb: int = 2048
    cpu_limit_percent: int = 80
    enable_chaos_testing: bool = False
    enable_deterministic_replay: bool = False
    log_level: str = "INFO"
    created_at: Timestamp = field(default_factory=_utc_now_ms)

    def validate(self) -> None:
        """三层防线：配置校验"""
        if self.max_strategies <= 0:
            raise ValidationError(ErrorCode.CORE_BOOTSTRAP_001, "max_strategies must be positive")
        if self.max_orders_per_second <= 0:
            raise ValidationError(ErrorCode.CORE_BOOTSTRAP_001, "max_orders_per_second must be positive")
        if self.memory_limit_mb <= 0:
            raise ValidationError(ErrorCode.CORE_BOOTSTRAP_001, "memory_limit_mb must be positive")
        if self.cpu_limit_percent <= 0 or self.cpu_limit_percent > 100:
            raise ValidationError(ErrorCode.CORE_BOOTSTRAP_001, "cpu_limit_percent must be 1-100")


# ─────────────────────────────────────────────────────────────
# Protocol 定义（八项铁律·接口统一）
# ─────────────────────────────────────────────────────────────

@runtime_checkable
class EventProducer(Protocol):
    """事件生产者接口"""
    def produce(self, event: EventEnvelope) -> None: ...


@runtime_checkable
class EventConsumer(Protocol):
    """事件消费者接口"""
    def consume(self, event: EventEnvelope) -> None: ...
    def can_consume(self, event_type: EventType) -> bool: ...


@runtime_checkable
class HealthCheckable(Protocol):
    """健康检查接口"""
    def health_check(self) -> Dict[str, Any]: ...
    def status(self) -> ModuleStatus: ...


@runtime_checkable
class LifecycleManaged(Protocol):
    """生命周期管理接口"""
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def restart(self) -> None: ...
