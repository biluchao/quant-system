# -*- coding: utf-8 -*-
"""
domain/domain_events.py — 领域事件

铁律声明：本文件严格执行八项铁律
领域事件描述已经发生的事实，不可变。
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .value_objects import Timestamp


@dataclass(frozen=True)
class DomainEvent:
    """
    领域事件基类

    所有领域事件必须：
    1. 不可变
    2. 携带时间戳
    3. 关联聚合根ID
    """

    event_id: str = field(default_factory=lambda: f"dom_{uuid.uuid4().hex[:16]}")
    aggregate_id: str = ""
    occurred_at: int = field(default_factory=lambda: int(datetime.now(timezone.utc).timestamp() * 1000))
    event_type: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "aggregate_id": self.aggregate_id,
            "occurred_at": self.occurred_at,
            "event_type": self.event_type or self.__class__.__name__,
        }


@dataclass(frozen=True)
class OrderCreatedEvent(DomainEvent):
    """订单创建事件"""
    order_id: str = ""
    symbol: str = ""
    side: str = ""
    quantity: str = ""
    price: Optional[str] = None

    def __post_init__(self):
        object.__setattr__(self, 'event_type', 'OrderCreated')


@dataclass(frozen=True)
class OrderFilledEvent(DomainEvent):
    """订单成交事件"""
    order_id: str = ""
    fill_quantity: str = ""
    fill_price: str = ""
    remaining_quantity: str = ""
    is_complete: bool = False

    def __post_init__(self):
        object.__setattr__(self, 'event_type', 'OrderFilled')


@dataclass(frozen=True)
class OrderCancelledEvent(DomainEvent):
    """订单取消事件"""
    order_id: str = ""
    reason: str = ""
    remaining_quantity: str = ""

    def __post_init__(self):
        object.__setattr__(self, 'event_type', 'OrderCancelled')


@dataclass(frozen=True)
class PositionOpenedEvent(DomainEvent):
    """持仓开仓事件"""
    position_id: str = ""
    symbol: str = ""
    side: str = ""
    quantity: str = ""
    entry_price: str = ""

    def __post_init__(self):
        object.__setattr__(self, 'event_type', 'PositionOpened')


@dataclass(frozen=True)
class PositionClosedEvent(DomainEvent):
    """持仓平仓事件"""
    position_id: str = ""
    symbol: str = ""
    closed_quantity: str = ""
    exit_price: str = ""
    realized_pnl: str = ""

    def __post_init__(self):
        object.__setattr__(self, 'event_type', 'PositionClosed')


@dataclass(frozen=True)
class AccountDebitedEvent(DomainEvent):
    """账户扣款事件"""
    account_id: str = ""
    amount: str = ""
    currency: str = ""
    reason: str = ""

    def __post_init__(self):
        object.__setattr__(self, 'event_type', 'AccountDebited')


@dataclass(frozen=True)
class AccountCreditedEvent(DomainEvent):
    """账户入账事件"""
    account_id: str = ""
    amount: str = ""
    currency: str = ""
    reason: str = ""

    def __post_init__(self):
        object.__setattr__(self, 'event_type', 'AccountCredited')


@dataclass(frozen=True)
class StrategyStartedEvent(DomainEvent):
    """策略启动事件"""
    strategy_id: str = ""
    name: str = ""
    symbols: List[str] = field(default_factory=list)

    def __post_init__(self):
        object.__setattr__(self, 'event_type', 'StrategyStarted')


@dataclass(frozen=True)
class StrategyStoppedEvent(DomainEvent):
    """策略停止事件"""
    strategy_id: str = ""
    name: str = ""
    total_trades: int = 0
    final_pnl: str = ""

    def __post_init__(self):
        object.__setattr__(self, 'event_type', 'StrategyStopped')
