# -*- coding: utf-8 -*-
"""
events/bus.py — 事件总线（核心中枢）

铁律声明：本文件严格执行八项铁律
事件统一 / 幂等审计 / 命名统一 / 异常统一

更新：集成 EventRegistry、EventVersionManager、FilterChain
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any, Callable, Coroutine, Dict, List, Optional, Set, Tuple, Type, Union,
)

from core.types import (
    EventEnvelope, EventType, EventPriority, Timestamp, EventId,
    QuantError, ErrorCode, SystemEvent, _utc_now_ms,
    ImmutableBase,
)


class EventBusError(QuantError):
    """事件总线异常"""
    pass


@dataclass
class SubscriptionHandle:
    """订阅句柄（用于取消订阅）"""
    subscription_id: str
    event_types: List[EventType]
    subscriber_name: str
    created_at: Timestamp = field(default_factory=_utc_now_ms)

    def __hash__(self) -> int:
        return hash(self.subscription_id)


class EventBus:
    """
    事件总线 — 系统核心中枢

    职责：
    1. 发布事件（同步/异步）
    2. 订阅管理（精确匹配 + 通配符）
    3. 事件去重（幂等键）
    4. 优先级路由
    5. 死信队列
    6. 事件过滤链
    7. 版本兼容
    8. 审计集成
    """

    def __init__(
        self,
        *,
        max_queue_size: int = 100000,
        dlq_max_size: int = 10000,
        enable_dedup: bool = True,
        dedup_window_sec: int = 300,
        enable_audit: bool = True,
        enable_versioning: bool = True,
        enable_filters: bool = True,
    ):
        self._subscribers: Dict[EventType, List["_SubscriberEntry"]] = defaultdict(list)
        self._wildcard_subscribers: List["_SubscriberEntry"] = []
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue_size)
        self._dlq: deque = deque(maxlen=dlq_max_size)
        self._dedup_cache: Dict[str, float] = {}
        self._dedup_window = dedup_window_sec
        self._enable_dedup = enable_dedup
        self._enable_audit = enable_audit
        self._enable_versioning = enable_versioning
        self._enable_filters = enable_filters
        self._running = False
        self._dispatch_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

        # 统计
        self._stats = _BusStats()

        # 依赖（延迟注入）
        self._registry = None
        self._version_manager = None
        self._filter_chain = None
        self._audit_engine = None

    def set_registry(self, registry) -> None:
        self._registry = registry

    def set_version_manager(self, vm) -> None:
        self._version_manager = vm

    def set_filter_chain(self, fc) -> None:
        self._filter_chain = fc

    def set_audit_engine(self, ae) -> None:
        self._audit_engine = ae

    # ─── 发布 ────────────────────────────────────────────────

    async def publish(
        self,
        event: Union[EventEnvelope, SystemEvent, Any],
        *,
        producer: str = "",
        idempotency_key: Optional[str] = None,
        priority: Optional[EventPriority] = None,
        ttl_ms: Optional[int] = None,
    ) -> EventId:
        """发布事件"""
        # 包装为信封
        if isinstance(event, EventEnvelope):
            envelope = event
        else:
            envelope = EventEnvelope(
                event=event,
                producer=producer,
                idempotency_key=idempotency_key,
                ttl_ms=ttl_ms,
            )

        if priority:
            envelope = envelope.replace(priority=priority)

        # 幂等检查
        if self._enable_dedup and envelope.idempotency_key:
            if await self._is_duplicate(envelope.idempotency_key):
                raise EventBusError(
                    ErrorCode.CORE_IDEMPOTENCY_001,
                    f"Duplicate event: {envelope.idempotency_key}"
                )

        # 版本检查
        if self._enable_versioning and self._version_manager:
            envelope = await self._version_manager.wrap_for_publish(envelope)

        # 审计
        if self._enable_audit and self._audit_engine:
            await self._audit_engine.log(
                event_type="event.published",
                actor=producer or "system",
                action="publish",
                target=str(envelope.event_type if hasattr(envelope, 'event_type') else envelope.event.__class__.__name__),
                payload={"envelope_id": envelope.envelope_id},
            )

        # 入队
        try:
            self._queue.put_nowait(envelope)
            self._stats.published += 1
        except asyncio.QueueFull:
            self._dlq.append(envelope)
            self._stats.dlq_dropped += 1
            raise EventBusError(
                ErrorCode.CORE_EVENT_BUS_001,
                "Event bus queue full, event moved to DLQ"
            )

        return envelope.envelope_id

    async def publish_sync(self, event: Union[EventEnvelope, Any], **kwargs) -> EventId:
        """同步发布（直接分发，不入队）"""
        envelope = self._wrap_envelope(event, **kwargs)

        # 幂等
        if self._enable_dedup and envelope.idempotency_key:
            if await self._is_duplicate(envelope.idempotency_key):
                raise EventBusError(
                    ErrorCode.CORE_IDEMPOTENCY_001,
                    f"Duplicate event: {envelope.idempotency_key}"
                )

        # 直接分发
        await self._dispatch(envelope)
        return envelope.envelope_id

    # ─── 订阅 ────────────────────────────────────────────────

    def subscribe(
        self,
        event_type: Union[EventType, str],
        handler: Callable[[EventEnvelope], Any],
        *,
        subscriber_name: str = "",
        priority: Optional[EventPriority] = None,
        filter_fn: Optional[Callable[[EventEnvelope], bool]] = None,
    ) -> SubscriptionHandle:
        """订阅事件"""
        sub_id = f"sub_{uuid.uuid4().hex[:12]}"
        entry = _SubscriberEntry(
            subscription_id=sub_id,
            handler=handler,
            subscriber_name=subscriber_name or handler.__name__,
            priority=priority or EventPriority.P3_LOW,
            filter_fn=filter_fn,
        )

        if isinstance(event_type, str) and event_type.endswith(".*"):
            # 通配符订阅
            self._wildcard_subscribers.append(entry)
        else:
            if isinstance(event_type, str):
                # 尝试从注册表解析
                if self._registry:
                    resolved = self._registry.resolve_event_type(event_type)
                    if resolved:
                        event_type = resolved
                else:
                    raise EventBusError(
                        ErrorCode.CORE_EVENT_BUS_001,
                        f"Cannot resolve event type string '{event_type}' without registry"
                    )
            self._subscribers[event_type].append(entry)

        self._stats.total_subscriptions += 1

        return SubscriptionHandle(
            subscription_id=sub_id,
            event_types=[event_type] if isinstance(event_type, EventType) else [],
            subscriber_name=entry.subscriber_name,
        )

    def unsubscribe(self, handle: SubscriptionHandle) -> bool:
        """取消订阅"""
        removed = False
        for entries in self._subscribers.values():
            for i, entry in enumerate(entries):
                if entry.subscription_id == handle.subscription_id:
                    entries.pop(i)
                    removed = True
                    break

        for i, entry in enumerate(self._wildcard_subscribers):
            if entry.subscription_id == handle.subscription_id:
                self._wildcard_subscribers.pop(i)
                removed = True
                break

        if removed:
            self._stats.total_subscriptions -= 1
        return removed

    # ─── 启动/停止 ──────────────────────────────────────────

    async def start(self) -> None:
        """启动事件总线"""
        if self._running:
            return
        self._running = True
        self._dispatch_task = asyncio.create_task(self._dispatch_loop())
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def stop(self) -> None:
        """停止事件总线"""
        self._running = False
        if self._dispatch_task:
            self._dispatch_task.cancel()
            try:
                await self._dispatch_task
            except asyncio.CancelledError:
                pass
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

    # ─── 内部方法 ────────────────────────────────────────────

    async def _dispatch_loop(self) -> None:
        """分发循环"""
        while self._running:
            try:
                envelope = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                await self._dispatch(envelope)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                print(f"[EventBus] Dispatch error: {e}")

    async def _dispatch(self, envelope: EventEnvelope) -> None:
        """分发事件到所有匹配的订阅者"""
        # 过期检查
        if envelope.is_expired():
            self._stats.expired += 1
            return

        # 过滤链
        if self._enable_filters and self._filter_chain:
            if not self._filter_chain.evaluate(envelope):
                self._stats.filtered += 1
                return

        # 确定事件类型
        event_type = None
        if hasattr(envelope.event, 'event_type'):
            event_type = envelope.event.event_type

        if not event_type:
            self._stats.no_type += 1
            return

        # 收集匹配的订阅者
        matched: List[_SubscriberEntry] = []

        # 精确匹配
        if event_type in self._subscribers:
            matched.extend(self._subscribers[event_type])

        # 通配符匹配
        for entry in self._wildcard_subscribers:
            if entry.filter_fn and not entry.filter_fn(envelope):
                continue
            matched.append(entry)

        if not matched:
            self._stats.no_subscribers += 1
            return

        # 按优先级排序
        matched.sort(key=lambda e: e.priority.value)

        # 分发
        for entry in matched:
            try:
                if entry.filter_fn and not entry.filter_fn(envelope):
                    continue

                if asyncio.iscoroutinefunction(entry.handler):
                    await entry.handler(envelope)
                else:
                    entry.handler(envelope)

                self._stats.dispatched += 1

            except Exception as e:
                self._stats.handler_errors += 1
                # 放入死信队列
                self._dlq.append((envelope, str(e)))

    async def _is_duplicate(self, key: str) -> bool:
        """检查幂等键"""
        now = time.monotonic()
        # 清理过期
        expired = [k for k, t in self._dedup_cache.items() if now - t > self._dedup_window]
        for k in expired:
            del self._dedup_cache[k]

        if key in self._dedup_cache:
            return True

        self._dedup_cache[key] = now
        return False

    async def _cleanup_loop(self) -> None:
        """定期清理"""
        while self._running:
            await asyncio.sleep(60)
            now = time.monotonic()
            expired = [k for k, t in self._dedup_cache.items() if now - t > self._dedup_window]
            for k in expired:
                del self._dedup_cache[k]

    def _wrap_envelope(self, event: Any, **kwargs) -> EventEnvelope:
        if isinstance(event, EventEnvelope):
            return event
        return EventEnvelope(
            event=event,
            producer=kwargs.get("producer", ""),
            idempotency_key=kwargs.get("idempotency_key"),
            ttl_ms=kwargs.get("ttl_ms"),
        )

    def stats(self) -> Dict[str, Any]:
        """统计信息"""
        return {
            "published": self._stats.published,
            "dispatched": self._stats.dispatched,
            "dlq_dropped": self._stats.dlq_dropped,
            "expired": self._stats.expired,
            "filtered": self._stats.filtered,
            "handler_errors": self._stats.handler_errors,
            "no_subscribers": self._stats.no_subscribers,
            "queue_size": self._queue.qsize(),
            "dlq_size": len(self._dlq),
            "total_subscriptions": self._stats.total_subscriptions,
        }


@dataclass
class _SubscriberEntry:
    subscription_id: str
    handler: Callable
    subscriber_name: str
    priority: EventPriority
    filter_fn: Optional[Callable[[EventEnvelope], bool]] = None


@dataclass
class _BusStats:
    published: int = 0
    dispatched: int = 0
    dlq_dropped: int = 0
    expired: int = 0
    filtered: int = 0
    handler_errors: int = 0
    no_subscribers: int = 0
    no_type: int = 0
    total_subscriptions: int = 0
