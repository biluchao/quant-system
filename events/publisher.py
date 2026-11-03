# -*- coding: utf-8 -*-
"""
events/publisher.py — 发布者（封装 + 批量 + 重试）

铁律声明：本文件严格执行八项铁律
发布者封装发布逻辑，支持批量发布和失败重试。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Union

from core.types import (
    EventEnvelope, EventType, EventPriority, Timestamp, EventId,
    QuantError, ErrorCode, _utc_now_ms, SystemEvent,
)


class PublisherError(QuantError):
    """发布者异常"""
    pass


@dataclass
class PublishConfig:
    """发布配置"""
    producer: str = "default"
    default_priority: EventPriority = EventPriority.P3_LOW
    default_ttl_ms: Optional[int] = None
    max_retries: int = 3
    retry_delay_sec: float = 1.0
    batch_size: int = 1
    batch_timeout_sec: float = 0.1
    enable_dedup: bool = True
    idempotency_key_prefix: str = ""


class Publisher:
    """
    发布者接口
    """
    async def publish(self, event: Any, **kwargs) -> EventId:
        raise NotImplementedError

    async def publish_batch(self, events: List[Any], **kwargs) -> List[EventId]:
        raise NotImplementedError


class DefaultPublisher(Publisher):
    """
    默认发布者

    特性：
    1. 自动包装信封
    2. 批量发布（攒批）
    3. 失败重试（指数退避）
    4. 幂等键自动生成
    """

    def __init__(self, event_bus, config: Optional[PublishConfig] = None):
        self._bus = event_bus
        self._config = config or PublishConfig()
        self._batch_queue: asyncio.Queue = asyncio.Queue()
        self._batch_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        """启动批量发布器"""
        if self._running:
            return
        self._running = True
        if self._config.batch_size > 1:
            self._batch_task = asyncio.create_task(self._batch_loop())

    async def stop(self) -> None:
        """停止批量发布器"""
        self._running = False
        if self._batch_task:
            self._batch_task.cancel()
            try:
                await self._batch_task
            except asyncio.CancelledError:
                pass

    async def publish(self, event: Any, **kwargs) -> EventId:
        """发布单个事件"""
        config = self._config

        # 生成幂等键
        idempotency_key = kwargs.get("idempotency_key")
        if not idempotency_key and config.enable_dedup:
            event_id = ""
            if hasattr(event, 'event_id'):
                event_id = event.event_id
            elif hasattr(event, 'order_id'):
                event_id = event.order_id
            idempotency_key = f"{config.idempotency_key_prefix}{event_id or _utc_now_ms()}"

        # 重试逻辑
        last_error = None
        for attempt in range(config.max_retries + 1):
            try:
                return await self._bus.publish(
                    event,
                    producer=kwargs.get("producer", config.producer),
                    idempotency_key=idempotency_key,
                    priority=kwargs.get("priority", config.default_priority),
                    ttl_ms=kwargs.get("ttl_ms", config.default_ttl_ms),
                )
            except Exception as e:
                last_error = e
                if attempt < config.max_retries:
                    delay = config.retry_delay_sec * (2 ** attempt)
                    await asyncio.sleep(min(delay, 30.0))

        raise PublisherError(
            ErrorCode.CORE_PUBLISHER_001,
            f"Publish failed after {config.max_retries} retries: {last_error}"
        )

    async def publish_batch(self, events: List[Any], **kwargs) -> List[EventId]:
        """批量发布"""
        if self._config.batch_size <= 1:
            # 直接逐个发布
            results = []
            for event in events:
                result = await self.publish(event, **kwargs)
                results.append(result)
            return results

        # 放入批量队列
        futures: List[asyncio.Future] = []
        for event in events:
            future: asyncio.Future = asyncio.Future()
            await self._batch_queue.put((event, kwargs, future))
            futures.append(future)

        # 等待结果
        results = []
        for future in futures:
            results.append(await future)

        return results

    async def _batch_loop(self) -> None:
        """批量发布循环"""
        batch: List[tuple] = []
        last_flush = _utc_now_ms()

        while self._running:
            try:
                item = await asyncio.wait_for(
                    self._batch_queue.get(),
                    timeout=self._config.batch_timeout_sec,
                )
                batch.append(item)

                now = _utc_now_ms()
                if len(batch) >= self._config.batch_size or (now - last_flush) >= (self._config.batch_timeout_sec * 1000):
                    await self._flush_batch(batch)
                    batch.clear()
                    last_flush = now

            except asyncio.TimeoutError:
                if batch:
                    await self._flush_batch(batch)
                    batch.clear()
                    last_flush = _utc_now_ms()

    async def _flush_batch(self, batch: List[tuple]) -> None:
        """刷新批量"""
        for event, kwargs, future in batch:
            try:
                result = await self._bus.publish(event, **kwargs)
                if not future.done():
                    future.set_result(result)
            except Exception as e:
                if not future.done():
                    future.set_exception(e)
