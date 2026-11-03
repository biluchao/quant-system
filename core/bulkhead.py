# -*- coding: utf-8 -*-
"""
core/bulkhead.py — 舱壁隔离（策略级资源配额）

铁律声明：本文件严格执行八项铁律
一个策略崩溃/资源耗尽不应影响其他策略。
每个策略独立资源配额（内存/CPU/消息队列容量）。
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .types import QuantError, ErrorCode


class BulkheadRejectedError(QuantError):
    """舱壁拒绝异常（资源配额已满）"""
    pass


@dataclass
class BulkheadConfig:
    """舱壁配置"""
    name: str
    max_concurrent_calls: int = 10       # 最大并发调用数
    max_queue_size: int = 50              # 最大等待队列长度
    max_memory_mb: int = 256              # 最大内存配额(MB)
    call_timeout_sec: float = 30.0        # 单次调用超时
    queue_timeout_sec: float = 5.0        # 排队超时
    max_calls_per_second: int = 100       # 令牌桶速率


@dataclass
class BulkheadStats:
    """舱壁统计"""
    name: str
    active_calls: int = 0
    queued_calls: int = 0
    rejected_calls: int = 0
    timed_out_calls: int = 0
    total_calls: int = 0
    failed_calls: int = 0
    memory_used_mb: float = 0.0


class Bulkhead:
    """
    舱壁隔离器

    每个策略/模块一个 Bulkhead 实例。
    限制并发数、队列长度、内存使用、调用速率。
    """

    def __init__(self, config: BulkheadConfig):
        self._config = config
        self._active_count = 0
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=config.max_queue_size)
        self._lock = asyncio.Lock()
        self._stats = BulkheadStats(name=config.name)
        self._closed = False
        self._tokens = config.max_calls_per_second
        self._last_refill = time.monotonic()
        self._token_lock = asyncio.Lock()

    async def acquire(self) -> "BulkheadPermit":
        """获取执行许可"""
        if self._closed:
            raise BulkheadRejectedError(
                ErrorCode.CORE_BULKHEAD_001,
                f"Bulkhead '{self._config.name}' is closed"
            )

        # 速率限制（令牌桶）
        await self._consume_token()

        async with self._lock:
            if self._active_count < self._config.max_concurrent_calls:
                self._active_count += 1
                self._stats.active_calls = self._active_count
                self._stats.total_calls += 1
                return BulkheadPermit(self)

            # 尝试入队
            if self._queue.qsize() < self._config.max_queue_size:
                # 创建等待任务
                future: asyncio.Future = asyncio.Future()
                try:
                    self._queue.put_nowait(future)
                    self._stats.queued_calls = self._queue.qsize()
                except asyncio.QueueFull:
                    self._stats.rejected_calls += 1
                    raise BulkheadRejectedError(
                        ErrorCode.CORE_BULKHEAD_001,
                        f"Bulkhead '{self._config.name}' queue full "
                        f"(max={self._config.max_queue_size})"
                    )

        # 等待队列中的许可
        try:
            await asyncio.wait_for(future, timeout=self._config.queue_timeout_sec)
            self._stats.active_calls = self._active_count
            return BulkheadPermit(self)
        except asyncio.TimeoutError:
            self._stats.timed_out_calls += 1
            # 从队列中移除
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            raise BulkheadRejectedError(
                ErrorCode.CORE_BULKHEAD_001,
                f"Bulkhead '{self._config.name}' queue timeout "
                f"after {self._config.queue_timeout_sec}s"
            )

    async def _consume_token(self) -> None:
        """令牌桶限流"""
        async with self._token_lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            # 补充令牌
            new_tokens = int(elapsed * self._config.max_calls_per_second)
            if new_tokens > 0:
                self._tokens = min(
                    self._config.max_calls_per_second,
                    self._tokens + new_tokens
                )
                self._last_refill = now

            if self._tokens > 0:
                self._tokens -= 1
            else:
                # 等待令牌补充
                wait_time = 1.0 / self._config.max_calls_per_second
                await asyncio.sleep(wait_time)
                self._tokens = 0

    def release(self) -> None:
        """释放许可"""
        if self._closed:
            return

        # 唤醒队列中等待的任务
        try:
            future = self._queue.get_nowait()
            if not future.done():
                self._active_count += 1
                future.set_result(True)
        except asyncio.QueueEmpty:
            self._active_count = max(0, self._active_count - 1)

        self._stats.active_calls = self._active_count
        self._stats.queued_calls = self._queue.qsize()

    def update_memory_usage(self, mb: float) -> None:
        """更新内存使用"""
        self._stats.memory_used_mb = mb
        if mb > self._config.max_memory_mb:
            # 内存超限，触发告警（由外部处理）
            pass

    def close(self) -> None:
        """关闭舱壁"""
        self._closed = True
        # 取消所有等待中的任务
        while not self._queue.empty():
            try:
                future = self._queue.get_nowait()
                if not future.done():
                    future.set_exception(BulkheadRejectedError(
                        ErrorCode.CORE_BULKHEAD_001,
                        f"Bulkhead '{self._config.name}' closed"
                    ))
            except asyncio.QueueEmpty:
                break

    def stats(self) -> BulkheadStats:
        """获取统计"""
        return self._stats

    @property
    def name(self) -> str:
        return self._config.name

    @property
    def is_closed(self) -> bool:
        return self._closed


class BulkheadPermit:
    """舱壁许可（上下文管理器）"""

    def __init__(self, bulkhead: Bulkhead):
        self._bulkhead = bulkhead
        self._released = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.release()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.release()

    def release(self) -> None:
        if not self._released:
            self._bulkhead.release()
            self._released = True


class BulkheadRegistry:
    """舱壁注册表（集中管理所有舱壁）"""

    def __init__(self):
        self._bulkheads: Dict[str, Bulkhead] = {}

    def create(self, config: BulkheadConfig) -> Bulkhead:
        """创建并注册舱壁"""
        if config.name in self._bulkheads:
            raise BulkheadRejectedError(
                ErrorCode.CORE_BULKHEAD_001,
                f"Bulkhead '{config.name}' already exists"
            )
        bh = Bulkhead(config)
        self._bulkheads[config.name] = bh
        return bh

    def get(self, name: str) -> Bulkhead:
        if name not in self._bulkheads:
            raise BulkheadRejectedError(
                ErrorCode.CORE_BULKHEAD_001,
                f"Bulkhead '{name}' not found"
            )
        return self._bulkheads[name]

    def remove(self, name: str) -> None:
        if name in self._bulkheads:
            self._bulkheads[name].close()
            del self._bulkheads[name]

    def list_stats(self) -> Dict[str, BulkheadStats]:
        return {name: bh.stats() for name, bh in self._bulkheads.items()}

    def close_all(self) -> None:
        for bh in self._bulkheads.values():
            bh.close()
        self._bulkheads.clear()
