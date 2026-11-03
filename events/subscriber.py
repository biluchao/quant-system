# -*- coding: utf-8 -*-
"""
events/subscriber.py — 订阅者管理（分组 + 负载均衡）

铁律声明：本文件严格执行八项铁律
订阅者按组管理，支持负载均衡和故障转移。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

from core.types import EventEnvelope, EventType, EventPriority, QuantError, ErrorCode


class SubscriberError(QuantError):
    """订阅者异常"""
    pass


class LoadBalanceStrategy(str, Enum):
    """负载均衡策略"""
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    PRIORITY = "priority"
    STICKY = "sticky"  # 同一事件类型总是路由到同一订阅者


@dataclass
class Subscriber:
    """订阅者"""
    name: str
    handler: Callable[[EventEnvelope], Any]
    group: Optional[str] = None
    priority: EventPriority = EventPriority.P3_LOW
    max_concurrent: int = 1
    active_tasks: int = 0
    total_processed: int = 0
    total_errors: int = 0
    last_error: Optional[str] = None
    last_processed_at: Optional[int] = None
    enabled: bool = True

    def can_accept(self) -> bool:
        return self.enabled and self.active_tasks < self.max_concurrent


@dataclass
class SubscriberGroup:
    """订阅者组"""
    name: str
    subscribers: List[Subscriber] = field(default_factory=list)
    strategy: LoadBalanceStrategy = LoadBalanceStrategy.ROUND_ROBIN
    _round_robin_index: int = 0

    def add_subscriber(self, subscriber: Subscriber) -> None:
        subscriber.group = self.name
        self.subscribers.append(subscriber)

    def remove_subscriber(self, name: str) -> bool:
        for i, sub in enumerate(self.subscribers):
            if sub.name == name:
                self.subscribers.pop(i)
                return True
        return False

    def select_subscriber(self, envelope: EventEnvelope) -> Optional[Subscriber]:
        """选择订阅者（负载均衡）"""
        available = [s for s in self.subscribers if s.can_accept()]
        if not available:
            return None

        if self.strategy == LoadBalanceStrategy.ROUND_ROBIN:
            sub = available[self._round_robin_index % len(available)]
            self._round_robin_index += 1
            return sub

        elif self.strategy == LoadBalanceStrategy.RANDOM:
            import random
            return random.choice(available)

        elif self.strategy == LoadBalanceStrategy.PRIORITY:
            return min(available, key=lambda s: s.priority.value)

        elif self.strategy == LoadBalanceStrategy.STICKY:
            # 基于事件类型哈希
            event_type = ""
            if hasattr(envelope.event, 'event_type'):
                event_type = str(envelope.event.event_type.value)
            idx = hash(event_type) % len(available)
            return available[idx]

        return available[0]


class SubscriberManager:
    """
    订阅者管理器

    职责：
    1. 订阅者注册/注销
    2. 分组管理
    3. 负载均衡
    4. 故障转移
    5. 并发控制
    """

    def __init__(self):
        self._subscribers: Dict[str, Subscriber] = {}
        self._groups: Dict[str, SubscriberGroup] = {}
        self._event_type_routes: Dict[EventType, List[str]] = defaultdict(list)
        self._lock = asyncio.Lock()

    async def register(
        self,
        name: str,
        handler: Callable[[EventEnvelope], Any],
        *,
        group: Optional[str] = None,
        priority: EventPriority = EventPriority.P3_LOW,
        max_concurrent: int = 1,
        event_types: Optional[List[EventType]] = None,
    ) -> Subscriber:
        """注册订阅者"""
        async with self._lock:
            if name in self._subscribers:
                raise SubscriberError(
                    ErrorCode.CORE_SUBSCRIBER_001,
                    f"Subscriber '{name}' already registered"
                )

            subscriber = Subscriber(
                name=name,
                handler=handler,
                group=group,
                priority=priority,
                max_concurrent=max_concurrent,
            )

            self._subscribers[name] = subscriber

            # 加入组
            if group:
                if group not in self._groups:
                    self._groups[group] = SubscriberGroup(name=group)
                self._groups[group].add_subscriber(subscriber)

            # 路由注册
            if event_types:
                for et in event_types:
                    self._event_type_routes[et].append(name)

            return subscriber

    async def unregister(self, name: str) -> bool:
        """注销订阅者"""
        async with self._lock:
            if name not in self._subscribers:
                return False

            subscriber = self._subscribers[name]

            # 从组移除
            if subscriber.group and subscriber.group in self._groups:
                self._groups[subscriber.group].remove_subscriber(name)

            # 从路由移除
            for names in self._event_type_routes.values():
                if name in names:
                    names.remove(name)

            del self._subscribers[name]
            return True

    async def dispatch(self, envelope: EventEnvelope) -> List[Any]:
        """分发事件到匹配的订阅者"""
        results = []
        event_type = None
        if hasattr(envelope.event, 'event_type'):
            event_type = envelope.event.event_type

        # 获取匹配的订阅者名称
        matched_names: List[str] = []
        if event_type:
            matched_names.extend(self._event_type_routes.get(event_type, []))

        # 如果没有路由配置，广播给所有订阅者
        if not matched_names:
            matched_names = list(self._subscribers.keys())

        # 执行分发
        for name in matched_names:
            if name not in self._subscribers:
                continue
            subscriber = self._subscribers[name]

            if not subscriber.can_accept():
                continue

            subscriber.active_tasks += 1
            try:
                if asyncio.iscoroutinefunction(subscriber.handler):
                    result = await subscriber.handler(envelope)
                else:
                    result = subscriber.handler(envelope)

                subscriber.total_processed += 1
                subscriber.last_processed_at = _utc_now_ms()
                results.append(result)

            except Exception as e:
                subscriber.total_errors += 1
                subscriber.last_error = str(e)
                results.append(e)

            finally:
                subscriber.active_tasks -= 1

        return results

    def get_group(self, name: str) -> Optional[SubscriberGroup]:
        return self._groups.get(name)

    def create_group(self, name: str, strategy: LoadBalanceStrategy = LoadBalanceStrategy.ROUND_ROBIN) -> SubscriberGroup:
        if name in self._groups:
            raise SubscriberError(
                ErrorCode.CORE_SUBSCRIBER_001,
                f"Group '{name}' already exists"
            )
        group = SubscriberGroup(name=name, strategy=strategy)
        self._groups[name] = group
        return group

    def list_subscribers(self) -> List[Dict[str, Any]]:
        return [
            {
                "name": s.name,
                "group": s.group,
                "priority": s.priority.value,
                "active_tasks": s.active_tasks,
                "total_processed": s.total_processed,
                "total_errors": s.total_errors,
                "enabled": s.enabled,
            }
            for s in self._subscribers.values()
        ]

    def stats(self) -> Dict[str, Any]:
        return {
            "total_subscribers": len(self._subscribers),
            "total_groups": len(self._groups),
            "total_routes": sum(len(v) for v in self._event_type_routes.values()),
            "active_tasks": sum(s.active_tasks for s in self._subscribers.values()),
        }


# 需要导入
from core.types import _utc_now_ms
