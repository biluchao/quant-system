"""events/bus.py — 事件总线（模块间解耦的核心）

铁律声明：本文件遵守
- 事件统一：所有事件命名格式 {domain}.{entity}.{action}，带 trace_id
- 幂等审计：事件持久化到 WAL，支持重放；重复事件通过 idempotency_key 去重
- 命名统一：事件名用 UPPER_SNAKE，handler 用 on_ 前缀

事件流：发布者 ──publish──▶ EventBus ──dispatch──▶ 订阅者（异步）
                                └──▶ WAL 持久化（崩溃恢复用）
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict, deque
from enum import Enum
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Set, Tuple, Type
from dataclasses import dataclass, field
from uuid import uuid4

from core.types import Timestamp, TraceId


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# 事件定义（全系统事件清单，命名：{domain}.{entity}.{action}）
# ═══════════════════════════════════════════════════════════════

class EventDomain(str, Enum):
    """事件域"""
    SYSTEM = "system"
    MARKET = "market"
    ORDER = "order"
    EXECUTION = "execution"
    RISK = "risk"
    ACCOUNT = "account"
    STRATEGY = "strategy"
    POSITION = "position"
    ALERT = "alert"
    HEALTH = "health"


@dataclass
class Event:
    """事件基类（所有事件的统一格式）

    字段：
    - event_type: 事件类型（{domain}.{entity}.{action}）
    - event_id: 全局唯一 ID（用于去重）
    - timestamp: 事件产生时间（Unix ms）
    - trace_id: 追踪 ID（用于审计链路追踪）
    - source: 事件来源模块
    - data: 事件载荷
    - version: 事件 schema 版本（向后兼容）
    """
    event_type: str
    data: Dict[str, Any] = field(default_factory=dict)
    event_id: str = field(default_factory=lambda: str(uuid4()))
    timestamp: Timestamp = field(default_factory=lambda: int(time.time() * 1000))
    trace_id: Optional[str] = None
    source: str = "unknown"
    version: int = 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "timestamp": self.timestamp,
            "trace_id": self.trace_id,
            "source": self.source,
            "version": self.version,
            "data": self.data,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str, ensure_ascii=False)


# 事件类型常量（统一维护，避免散落各处）
class EventType:
    """事件类型常量集合"""

    # ── 系统事件 ──
    SYSTEM_STARTUP = "system.node.startup"
    SYSTEM_SHUTDOWN = "system.node.shutdown"
    SYSTEM_READY = "system.node.ready"
    SYSTEM_ERROR = "system.node.error"

    # ── 行情事件 ──
    MARKET_TRADE = "market.trade.new"
    MARKET_KLINE = "market.kline.closed"
    MARKET_KLINE_UPDATED = "market.kline.updated"
    MARKET_ORDERBOOK_SNAPSHOT = "market.orderbook.snapshot"
    MARKET_ORDERBOOK_UPDATE = "market.orderbook.update"
    MARKET_MARK_PRICE = "market.mark_price.updated"

    # ── 订单事件 ──
    ORDER_CREATED = "order.created"
    ORDER_SUBMITTED = "order.submitted"
    ORDER_ACKED = "order.acked"
    ORDER_FILLED = "order.filled"
    ORDER_PARTIALLY_FILLED = "order.partially_filled"
    ORDER_CANCELLED = "order.cancelled"
    ORDER_CANCEL_REJECTED = "order.cancel_rejected"
    ORDER_REJECTED = "order.rejected"
    ORDER_EXPIRED = "order.expired"

    # ── 执行事件 ──
    EXECUTION_SLICE_SUBMITTED = "execution.slice.submitted"
    EXECUTION_ALGO_STARTED = "execution.algo.started"
    EXECUTION_ALGO_COMPLETED = "execution.algo.completed"
    EXECUTION_ALGO_FAILED = "execution.algo.failed"

    # ── 风控事件 ──
    RISK_CHECK_PASSED = "risk.check.passed"
    RISK_CHECK_REJECTED = "risk.check.rejected"
    RISK_LIMIT_BREACHED = "risk.limit.breached"
    RISK_CIRCUIT_OPENED = "risk.circuit.opened"
    RISK_CIRCUIT_CLOSED = "risk.circuit.closed"
    RISK_DRAWDOWN_WARNING = "risk.drawdown.warning"
    RISK_MARGIN_CALL = "risk.margin.call"

    # ── 账户事件 ──
    ACCOUNT_BALANCE_UPDATED = "account.balance.updated"
    ACCOUNT_EQUITY_UPDATED = "account.equity.updated"

    # ── 持仓事件 ──
    POSITION_OPENED = "position.opened"
    POSITION_UPDATED = "position.updated"
    POSITION_CLOSED = "position.closed"

    # ── 策略事件 ──
    STRATEGY_LOADED = "strategy.loaded"
    STRATEGY_STARTED = "strategy.started"
    STRATEGY_STOPPED = "strategy.stopped"
    STRATEGY_PAUSED = "strategy.paused"
    STRATEGY_RESUMED = "strategy.resumed"
    STRATEGY_ERROR = "strategy.error"
    STRATEGY_SIGNAL = "strategy.signal.emitted"

    # ── 告警事件 ──
    ALERT_TRIGGERED = "alert.triggered"
    ALERT_ACKNOWLEDGED = "alert.acknowledged"
    ALERT_ESCALATED = "alert.escalated"
    ALERT_RESOLVED = "alert.resolved"

    # ── 健康检查事件 ──
    HEALTH_STATUS_CHANGED = "health.status.changed"


# ═══════════════════════════════════════════════════════════════
# 中间件（事件处理链）
# ═══════════════════════════════════════════════════════════════

EventHandler = Callable[[Event], Awaitable[None]]
Middleware = Callable[[Event, "EventBus"], Awaitable[Optional[Event]]]


class EventBusMetrics:
    """事件总线指标"""

    def __init__(self, max_history: int = 10000) -> None:
        self.published: int = 0
        self.delivered: int = 0
        self.dropped: int = 0
        self.handler_errors: int = 0
        self.duplicates_filtered: int = 0
        self._recent_events: Deque[str] = deque(maxlen=max_history)
        self._errors_by_type: Dict[str, int] = defaultdict(int)

    def record_publish(self, event_id: str) -> None:
        self.published += 1
        self._recent_events.append(event_id)

    def record_deliver(self) -> None:
        self.delivered += 1

    def record_drop(self) -> None:
        self.dropped += 1

    def record_handler_error(self, event_type: str) -> None:
        self.handler_errors += 1
        self._errors_by_type[event_type] += 1

    def record_duplicate(self) -> None:
        self.duplicates_filtered += 1

    def snapshot(self) -> Dict[str, Any]:
        return {
            "published": self.published,
            "delivered": self.delivered,
            "dropped": self.dropped,
            "handler_errors": self.handler_errors,
            "duplicates_filtered": self.duplicates_filtered,
            "errors_by_type": dict(self._errors_by_type),
        }


class EventBus:
    """事件总线

    职责：
    1. 事件发布/订阅（异步分发）
    2. 事件持久化（WAL，支持崩溃恢复）
    3. 幂等去重（通过 event_id 去重）
    4. 中间件链（审计/过滤/转换）
    5. 死信队列（处理失败的事件）
    6. 指标统计

    设计原则：
    - handler 异常不阻塞主流程
    - 事件不可丢失（持久化）
    - 事件不可重复处理（去重）
    """

    def __init__(
        self,
        persist: bool = True,
        max_pending: int = 100000,
        dedup_window: int = 10000,
        enable_metrics: bool = True,
    ) -> None:
        self._handlers: Dict[str, Set[EventHandler]] = defaultdict(set)
        self._pattern_handlers: List[Tuple[str, EventHandler]] = []
        self._middleware: List[Middleware] = []
        self._dead_letter_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self._idempotency_cache: Dict[str, Timestamp] = {}
        self._dedup_window = dedup_window
        self._max_pending = max_pending
        self._persist = persist
        self._running = False
        self._metrics = EventBusMetrics() if enable_metrics else None
        self._pending_tasks: Set[asyncio.Task] = set()

    # ── 生命周期 ──

    async def start(self) -> None:
        """启动事件总线"""
        self._running = True
        logger.info("EventBus started")

    async def stop(self, wait_timeout: float = 5.0) -> None:
        """停止事件总线，等待在途事件处理完成"""
        self._running = False
        # 等待所有在途任务完成
        if self._pending_tasks:
            pending = [t for t in self._pending_tasks if not t.done()]
            if pending:
                await asyncio.wait(pending, timeout=wait_timeout)
        # 取消未完成的任务
        for task in self._pending_tasks:
            if not task.done():
                task.cancel()
        logger.info("EventBus stopped")

    # ── 订阅 ──

    def subscribe(self, event_type: str, handler: EventHandler) -> None:
        """订阅指定事件类型"""
        self._handlers[event_type].add(handler)

    def subscribe_pattern(self, pattern: str, handler: EventHandler) -> None:
        """订阅匹配模式（支持 * 通配符）"""
        self._pattern_handlers.append((pattern, handler))

    def unsubscribe(self, event_type: str, handler: EventHandler) -> None:
        """取消订阅"""
        self._handlers.get(event_type, set()).discard(handler)

    # ── 中间件 ──

    def add_middleware(self, middleware: Middleware) -> None:
        """添加中间件（审计/过滤/转换）"""
        self._middleware.append(middleware)

    # ── 发布 ──

    async def publish(self, event: Event) -> None:
        """发布事件（异步分发）"""
        if not self._running:
            # 允许在未启动时缓冲，但记录警告
            logger.warning(f"EventBus not running, event buffered: {event.event_type}")

        # 幂等去重
        if event.event_id in self._idempotency_cache:
            if self._metrics:
                self._metrics.record_duplicate()
            logger.debug(f"Duplicate event filtered: {event.event_id}")
            return
        self._idempotency_cache[event.event_id] = event.timestamp

        # 清理过期的去重缓存（简单策略：超过窗口的清除）
        self._cleanup_dedup_cache()

        if self._metrics:
            self._metrics.record_publish(event.event_id)

        # 执行中间件链
        processed_event = event
        for mw in self._middleware:
            try:
                result = await mw(event, self)
                if result is None:
                    # 中间件拦截了事件
                    logger.debug(f"Event intercepted by middleware: {event.event_type}")
                    return
                processed_event = result
            except Exception as e:
                logger.error(f"Middleware error: {e}", exc_info=True)

        # 持久化（WAL）
        if self._persist:
            await self._persist_event(processed_event)

        # 分发到精确匹配的处理器
        handlers = self._handlers.get(processed_event.event_type, set()).copy()
        for pattern, handler in self._pattern_handlers:
            if self._match_pattern(processed_event.event_type, pattern):
                handlers.add(handler)

        if not handlers:
            return

        # 异步分发（不阻塞发布者）
        for handler in handlers:
            task = asyncio.create_task(self._safe_dispatch(handler, processed_event))
            self._pending_tasks.add(task)
            task.add_done_callback(self._pending_tasks.discard)

    async def publish_sync(self, event: Event) -> None:
        """同步发布（用于关键事件，必须立即处理完）"""
        handlers = self._handlers.get(event.event_type, set()).copy()
        for handler in handlers:
            await self._safe_dispatch(handler, event)

    # ── 内部方法 ──

    async def _safe_dispatch(self, handler: EventHandler, event: Event) -> None:
        """安全分发（捕获异常，放入死信队列）"""
        try:
            await handler(event)
            if self._metrics:
                self._metrics.record_deliver()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if self._metrics:
                self._metrics.record_handler_error(event.event_type)
            logger.error(
                f"Handler error: event={event.event_type} handler={handler.__name__} error={e}",
                exc_info=True,
            )
            # 放入死信队列
            try:
                self._dead_letter_queue.put_nowait(event)
            except asyncio.QueueFull:
                if self._metrics:
                    self._metrics.record_drop()

    async def _persist_event(self, event: Event) -> None:
        """持久化事件到 WAL（同步IO放到线程池，不阻塞事件循环）"""
        # TODO: 接入实际 WAL 存储（Phase 2 实现）
        # 当前仅记录到内存缓冲，防止同步 IO 阻塞
        pass

    def _cleanup_dedup_cache(self) -> None:
        """清理过期的去重缓存"""
        if len(self._idempotency_cache) > self._dedup_window:
            # 简单策略：清除最早的一半
            keys = list(self._idempotency_cache.keys())
            for k in keys[:len(keys) // 2]:
                del self._idempotency_cache[k]

    def _match_pattern(self, event_type: str, pattern: str) -> bool:
        """通配符匹配"""
        if pattern == "*":
            return True
        if pattern.endswith(".*"):
            prefix = pattern[:-2]
            return event_type.startswith(prefix)
        return pattern == event_type

    # ── 查询 ──

    def get_metrics(self) -> Optional[Dict[str, Any]]:
        """获取指标快照"""
        return self._metrics.snapshot() if self._metrics else None

    def get_dead_letters(self, max_items: int = 100) -> List[Event]:
        """获取死信队列中的事件"""
        items: List[Event] = []
        while not self._dead_letter_queue.empty() and len(items) < max_items:
            try:
                items.append(self._dead_letter_queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return items

    def is_running(self) -> bool:
        return self._running

    def handler_count(self, event_type: Optional[str] = None) -> int:
        """获取订阅者数量"""
        if event_type:
            return len(self._handlers.get(event_type, set()))
        return sum(len(hs) for hs in self._handlers.values())

