# -*- coding: utf-8 -*-
"""
core/resource_pool.py — 连接池/线程池/协程池统一管理

铁律声明：本文件严格执行八项铁律
所有 I/O 资源通过池化管理，禁止无限制创建。
async 函数内禁止同步阻塞 I/O（代码审查红线）。
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from threading import Lock, Thread
from typing import Any, Callable, Dict, List, Optional, TypeVar

from .types import QuantError, ErrorCode

T = TypeVar("T")


class PoolExhaustedError(QuantError):
    """资源池耗尽异常"""
    pass


class ResourceFactory(Protocol):
    """资源工厂接口"""
    def create(self) -> Any: ...
    def destroy(self, resource: Any) -> None: ...
    def is_valid(self, resource: Any) -> bool: ...


@dataclass
class PoolStats:
    """池统计信息"""
    created: int = 0
    destroyed: int = 0
    acquired: int = 0
    released: int = 0
    timed_out: int = 0
    current_size: int = 0
    max_size: int = 0
    waiting_count: int = 0


class ResourcePool:
    """
    通用资源池

    支持：
    - 最大/最小池大小
    - 获取超时
    - 资源有效性检查
    - 空闲回收
    - 统计信息
    """

    def __init__(
        self,
        factory: ResourceFactory,
        *,
        min_size: int = 1,
        max_size: int = 10,
        acquire_timeout: float = 5.0,
        idle_timeout: float = 300.0,
        max_lifetime: float = 3600.0,
        name: str = "default",
    ):
        if min_size < 0:
            raise ValueError("min_size must be >= 0")
        if max_size < min_size:
            raise ValueError("max_size must be >= min_size")

        self._factory = factory
        self._min_size = min_size
        self._max_size = max_size
        self._acquire_timeout = acquire_timeout
        self._idle_timeout = idle_timeout
        self._max_lifetime = max_lifetime
        self._name = name

        self._pool: deque = deque()
        self._all_resources: Dict[int, _ResourceWrapper] = {}
        self._lock = Lock()
        self._stats = PoolStats(max_size=max_size)
        self._closed = False

        # 预创建最小数量
        for _ in range(min_size):
            self._create_resource()

    def acquire(self) -> Any:
        """获取资源（阻塞直到超时）"""
        if self._closed:
            raise PoolExhaustedError(ErrorCode.CORE_RESOURCE_POOL_001, f"Pool '{self._name}' is closed")

        deadline = time.monotonic() + self._acquire_timeout

        while True:
            with self._lock:
                self._stats.waiting_count += 1
                try:
                    # 尝试从池中获取
                    while self._pool:
                        wrapper = self._pool.popleft()
                        if self._is_resource_valid(wrapper):
                            self._stats.acquired += 1
                            self._stats.waiting_count -= 1
                            wrapper.last_acquired_at = time.monotonic()
                            return wrapper.resource
                        else:
                            self._destroy_resource(wrapper)

                    # 池空，尝试创建新资源
                    if len(self._all_resources) < self._max_size:
                        wrapper = self._create_resource()
                        self._stats.acquired += 1
                        self._stats.waiting_count -= 1
                        wrapper.last_acquired_at = time.monotonic()
                        return wrapper.resource

                    # 池满，等待
                    self._stats.waiting_count -= 1
                except Exception:
                    self._stats.waiting_count -= 1
                    raise

            # 等待后重试
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._stats.timed_out += 1
                raise PoolExhaustedError(
                    ErrorCode.CORE_RESOURCE_POOL_001,
                    f"Pool '{self._name}' exhausted, timeout after {self._acquire_timeout}s"
                )
            time.sleep(min(0.1, remaining))

    def release(self, resource: Any) -> None:
        """释放资源回池"""
        if self._closed:
            self._factory.destroy(resource)
            return

        resource_id = id(resource)
        with self._lock:
            wrapper = self._all_resources.get(resource_id)
            if wrapper is None:
                # 不是池中的资源，直接销毁
                self._factory.destroy(resource)
                return

            if not self._factory.is_valid(resource):
                self._destroy_resource(wrapper)
                return

            wrapper.last_released_at = time.monotonic()
            self._pool.append(wrapper)
            self._stats.released += 1

    def _create_resource(self) -> "_ResourceWrapper":
        """创建新资源"""
        resource = self._factory.create()
        wrapper = _ResourceWrapper(resource=resource)
        self._pool.append(wrapper)
        self._all_resources[id(resource)] = wrapper
        self._stats.created += 1
        self._stats.current_size = len(self._all_resources)
        return wrapper

    def _destroy_resource(self, wrapper: "_ResourceWrapper") -> None:
        """销毁资源"""
        self._factory.destroy(wrapper.resource)
        self._all_resources.pop(id(wrapper.resource), None)
        self._stats.destroyed += 1
        self._stats.current_size = len(self._all_resources)

    def _is_resource_valid(self, wrapper: "_ResourceWrapper") -> bool:
        """检查资源是否有效"""
        if not self._factory.is_valid(wrapper.resource):
            return False
        # 检查生命周期
        if time.monotonic() - wrapper.created_at > self._max_lifetime:
            return False
        return True

    def close(self) -> None:
        """关闭池，销毁所有资源"""
        with self._lock:
            self._closed = True
            for wrapper in list(self._all_resources.values()):
                self._factory.destroy(wrapper.resource)
            self._pool.clear()
            self._all_resources.clear()
            self._stats.current_size = 0

    def stats(self) -> PoolStats:
        """获取统计信息"""
        with self._lock:
            return PoolStats(
                created=self._stats.created,
                destroyed=self._stats.destroyed,
                acquired=self._stats.acquired,
                released=self._stats.released,
                timed_out=self._stats.timed_out,
                current_size=self._stats.current_size,
                max_size=self._stats.max_size,
                waiting_count=self._stats.waiting_count,
            )


@dataclass
class _ResourceWrapper:
    """资源包装器"""
    resource: Any
    created_at: float = field(default_factory=time.monotonic)
    last_acquired_at: float = 0.0
    last_released_at: float = 0.0


class AsyncResourcePool:
    """
    异步资源池（基于 asyncio.Queue）
    """

    def __init__(
        self,
        factory: Callable[[], Any],
        destroyer: Optional[Callable[[Any], None]] = None,
        *,
        max_size: int = 10,
        acquire_timeout: float = 5.0,
        name: str = "async_default",
    ):
        self._factory = factory
        self._destroyer = destroyer or (lambda r: None)
        self._max_size = max_size
        self._acquire_timeout = acquire_timeout
        self._name = name
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_size)
        self._created_count = 0
        self._closed = False

    async def acquire(self) -> Any:
        """获取资源"""
        if self._closed:
            raise PoolExhaustedError(ErrorCode.CORE_RESOURCE_POOL_001, f"AsyncPool '{self._name}' is closed")

        try:
            if self._created_count < self._max_size:
                resource = self._factory()
                self._created_count += 1
                return resource
            else:
                return await asyncio.wait_for(
                    self._queue.get(), timeout=self._acquire_timeout
                )
        except asyncio.TimeoutError:
            raise PoolExhaustedError(
                ErrorCode.CORE_RESOURCE_POOL_001,
                f"AsyncPool '{self._name}' exhausted, timeout after {self._acquire_timeout}s"
            )

    async def release(self, resource: Any) -> None:
        """释放资源"""
        if self._closed:
            self._destroyer(resource)
            return
        if not self._queue.full():
            await self._queue.put(resource)
        else:
            self._destroyer(resource)

    async def close(self) -> None:
        """关闭池"""
        self._closed = True
        while not self._queue.empty():
            try:
                resource = self._queue.get_nowait()
                self._destroyer(resource)
            except asyncio.QueueEmpty:
                break
