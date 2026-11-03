"""core/resource_pool.py — 连接池/线程池/协程池统一管理

铁律声明：本文件遵守
- 类型统一：所有接口参数有类型注解
- 命名统一：类 PascalCase，方法动词开头，私有 _ 前缀
- 异常统一：池耗尽/创建失败抛精确异常
- 幂等审计：所有分配/释放可追踪

设计要点：
1. 统一管理三类资源：线程池、进程池、异步信号量（限并发）
2. 所有资源可限制上限（防资源耗尽）
3. 支持资源泄漏检测（分配后未释放的追踪）
4. 关闭时等待在途任务完成
5. 绝不阻塞事件循环：CPU密集任务走线程池/进程池
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import time
import threading
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, TypeVar
from contextlib import asynccontextmanager

from core.types import TraceId, new_trace_id


logger = logging.getLogger(__name__)

T = TypeVar("T")


# ═══════════════════════════════════════════════════════════════
# 异常
# ═══════════════════════════════════════════════════════════════

class ResourcePoolError(Exception):
    """资源池异常基类"""
    code = 3000


class PoolExhaustedError(ResourcePoolError):
    """资源池耗尽"""
    code = 3001


class PoolClosedError(ResourcePoolError):
    """资源池已关闭"""
    code = 3002


# ═══════════════════════════════════════════════════════════════
# 资源池指标
# ═══════════════════════════════════════════════════════════════

@dataclass
class PoolMetrics:
    """资源池指标"""

    name: str
    max_size: int
    in_use: int = 0
    total_created: int = 0
    total_acquired: int = 0
    total_released: int = 0
    total_errors: int = 0
    created_at: float = field(default_factory=time.monotonic)

    @property
    def available(self) -> int:
        return self.max_size - self.in_use

    @property
    def utilization(self) -> float:
        return self.in_use / max(self.max_size, 1)

    def snapshot(self) -> dict:
        return {
            "name": self.name,
            "max_size": self.max_size,
            "in_use": self.in_use,
            "available": self.available,
            "utilization": round(self.utilization, 4),
            "total_created": self.total_created,
            "total_acquired": self.total_acquired,
            "total_released": self.total_released,
            "total_errors": self.total_errors,
            "uptime_sec": round(time.monotonic() - self.created_at, 1),
        }


# ═══════════════════════════════════════════════════════════════
# 异步并发信号量池
# ═══════════════════════════════════════════════════════════════

class ConcurrencyLimiter:
    """并发限制器（基于 Semaphore）

    用于限制某类操作的并发数，例如：
    - 同时最多 10 个 REST 请求
    - 同时最多 5 个数据库查询
    """

    def __init__(self, name: str, max_concurrency: int = 10) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency 必须 >= 1")
        self._name = name
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._metrics = PoolMetrics(name=name, max_size=max_concurrency)

    @property
    def name(self) -> str:
        return self._name

    @asynccontextmanager
    async def acquire(self, timeout: Optional[float] = None):
        """获取一个并发槽位（上下文管理器）

        Usage:
            async with limiter.acquire(timeout=5.0):
                await do_work()
        """
        if timeout is not None:
            try:
                await asyncio.wait_for(self._semaphore.acquire(), timeout=timeout)
            except asyncio.TimeoutError:
                self._metrics.total_errors += 1
                raise PoolExhaustedError(
                    f"获取并发槽位超时（{timeout}s）: {self._name}"
                ) from None
        else:
            await self._semaphore.acquire()

        self._metrics.in_use += 1
        self._metrics.total_acquired += 1
        acquired_at = time.monotonic()
        try:
            yield
        finally:
            self._metrics.in_use -= 1
            self._metrics.total_released += 1
            self._semaphore.release()
            elapsed_ms = (time.monotonic() - acquired_at) * 1000
            if elapsed_ms > 1000:
                logger.warning(
                    f"ConcurrencyLimiter 持有时间过长: {self._name} ({elapsed_ms:.0f}ms)"
                )

    def get_metrics(self) -> dict:
        return self._metrics.snapshot()


# ═══════════════════════════════════════════════════════════════
# 线程池包装器
# ═══════════════════════════════════════════════════════════════

class ManagedThreadPool:
    """托管线程池

    所有同步阻塞操作（DB IO、文件 IO、CPU 密集）必须通过此池执行，
    绝对不能在 async 函数中直接调用同步阻塞函数。
    """

    def __init__(
        self,
        name: str,
        max_workers: int = 4,
        max_queue_size: int = 1000,
    ) -> None:
        if max_workers < 1:
            raise ValueError("max_workers 必须 >= 1")
        self._name = name
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=f"quant-{name}",
        )
        self._metrics = PoolMetrics(name=name, max_size=max_workers)
        self._closed = False
        self._lock = threading.Lock()

    @property
    def name(self) -> str:
        return self._name

    async def submit(self, func: Callable[..., T], *args, **kwargs) -> T:
        """提交任务到线程池（不阻塞事件循环）

        Args:
            func: 要执行的函数
            *args, **kwargs: 参数

        Returns:
            函数返回值

        Raises:
            PoolClosedError: 池已关闭
        """
        if self._closed:
            raise PoolClosedError(f"线程池已关闭: {self._name}")

        loop = asyncio.get_event_loop()
        self._metrics.total_acquired += 1

        try:
            return await loop.run_in_executor(
                self._executor,
                lambda: func(*args, **kwargs),
            )
        except Exception:
            self._metrics.total_errors += 1
            raise
        finally:
            self._metrics.total_released += 1

    def get_metrics(self) -> dict:
        return self._metrics.snapshot()

    def shutdown(self, wait: bool = True) -> None:
        """关闭线程池"""
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._executor.shutdown(wait=wait)
        logger.info(f"线程池已关闭: {self._name}")


# ═══════════════════════════════════════════════════════════════
# 进程池包装器
# ═══════════════════════════════════════════════════════════════

class ManagedProcessPool:
    """托管进程池（用于 CPU 密集型计算）

    例如：回测引擎的重计算、大量历史数据压缩。
    """

    def __init__(self, name: str, max_workers: Optional[int] = None) -> None:
        self._name = name
        self._executor = concurrent.futures.ProcessPoolExecutor(
            max_workers=max_workers,
        )
        self._metrics = PoolMetrics(
            name=name,
            max_size=max_workers or (os_cpu_count() or 1),
        )
        self._closed = False

    async def submit(self, func: Callable[..., T], *args, **kwargs) -> T:
        """提交 CPU 密集任务"""
        if self._closed:
            raise PoolClosedError(f"进程池已关闭: {self._name}")

        loop = asyncio.get_event_loop()
        self._metrics.total_acquired += 1
        try:
            return await loop.run_in_executor(
                self._executor,
                lambda: func(*args, **kwargs),
            )
        except Exception:
            self._metrics.total_errors += 1
            raise
        finally:
            self._metrics.total_released += 1

    def shutdown(self, wait: bool = True) -> None:
        if self._closed:
            return
        self._closed = True
        self._executor.shutdown(wait=wait)

    def get_metrics(self) -> dict:
        return self._metrics.snapshot()


def os_cpu_count() -> Optional[int]:
    """安全获取 CPU 核心数"""
    try:
        import os
        return os.cpu_count()
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════
# 资源池总管理器
# ═══════════════════════════════════════════════════════════════

class ResourcePoolManager:
    """资源池总管理器

    统一管理所有线程池/进程池/并发限制器。
    提供：
    - 注册与查询
    - 批量关闭
    - 全局指标汇总
    - 资源泄漏检测
    """

    def __init__(self) -> None:
        self._thread_pools: Dict[str, ManagedThreadPool] = {}
        self._process_pools: Dict[str, ManagedProcessPool] = {}
        self._limiters: Dict[str, ConcurrencyLimiter] = {}
        self._closed = False
        self._allocation_log: List[Tuple[str, str, float, TraceId]] = []

    # ── 线程池 ──

    def create_thread_pool(
        self,
        name: str,
        max_workers: int = 4,
        max_queue_size: int = 1000,
    ) -> ManagedThreadPool:
        """创建并注册线程池"""
        if self._closed:
            raise PoolClosedError("ResourcePoolManager 已关闭")
        if name in self._thread_pools:
            raise DuplicateError(f"线程池已存在: {name}")

        pool = ManagedThreadPool(name, max_workers, max_queue_size)
        self._thread_pools[name] = pool
        self._log_allocation("thread_pool", name)
        return pool

    def get_thread_pool(self, name: str) -> ManagedThreadPool:
        if name not in self._thread_pools:
            raise NotFoundError(f"线程池不存在: {name}")
        return self._thread_pools[name]

    # ── 进程池 ──

    def create_process_pool(
        self,
        name: str,
        max_workers: Optional[int] = None,
    ) -> ManagedProcessPool:
        """创建并注册进程池"""
        if self._closed:
            raise PoolClosedError("ResourcePoolManager 已关闭")
        if name in self._process_pools:
            raise DuplicateError(f"进程池已存在: {name}")

        pool = ManagedProcessPool(name, max_workers)
        self._process_pools[name] = pool
        self._log_allocation("process_pool", name)
        return pool

    def get_process_pool(self, name: str) -> ManagedProcessPool:
        if name not in self._process_pools:
            raise NotFoundError(f"进程池不存在: {name}")
        return self._process_pools[name]

    # ── 并发限制器 ──

    def create_limiter(
        self,
        name: str,
        max_concurrency: int = 10,
    ) -> ConcurrencyLimiter:
        """创建并注册并发限制器"""
        if self._closed:
            raise PoolClosedError("ResourcePoolManager 已关闭")
        if name in self._limiters:
            raise DuplicateError(f"并发限制器已存在: {name}")

        limiter = ConcurrencyLimiter(name, max_concurrency)
        self._limiters[name] = limiter
        self._log_allocation("limiter", name)
        return limiter

    def get_limiter(self, name: str) -> ConcurrencyLimiter:
        if name not in self._limiters:
            raise NotFoundError(f"并发限制器不存在: {name}")
        return self._limiters[name]

    # ── 关闭 ──

    async def shutdown_all(self, wait_timeout: float = 10.0) -> None:
        """关闭所有资源池"""
        self._closed = True
        logger.info("开始关闭所有资源池...")

        # 线程池
        for name, pool in self._thread_pools.items():
            try:
                await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None, pool.shutdown, True
                    ),
                    timeout=wait_timeout,
                )
                logger.info(f"  线程池 [{name}] 已关闭")
            except asyncio.TimeoutError:
                logger.error(f"  线程池 [{name}] 关闭超时")
                pool.shutdown(wait=False)
            except Exception as e:
                logger.error(f"  线程池 [{name}] 关闭异常: {e}")

        # 进程池
        for name, pool in self._process_pools.items():
            try:
                await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None, pool.shutdown, True
                    ),
                    timeout=wait_timeout,
                )
                logger.info(f"  进程池 [{name}] 已关闭")
            except asyncio.TimeoutError:
                logger.error(f"  进程池 [{name}] 关闭超时")
                pool.shutdown(wait=False)
            except Exception as e:
                logger.error(f"  进程池 [{name}] 关闭异常: {e}")

        logger.info("✅ 所有资源池已关闭")

    # ── 指标 ──

    def get_all_metrics(self) -> Dict[str, Any]:
        """获取所有资源池指标"""
        result: Dict[str, Any] = {
            "thread_pools": {
                n: p.get_metrics() for n, p in self._thread_pools.items()
            },
            "process_pools": {
                n: p.get_metrics() for n, p in self._process_pools.items()
            },
            "limiters": {
                n: l.get_metrics() for n, l in self._limiters.items()
            },
            "summary": {
                "total_pools": (
                    len(self._thread_pools)
                    + len(self._process_pools)
                    + len(self._limiters)
                ),
                "allocation_count": len(self._allocation_log),
            },
        }
        return result

    def check_leaks(self) -> List[Dict[str, Any]]:
        """检测资源泄漏

        检查所有池中 in_use > 0 的资源。
        """
        leaks: List[Dict[str, Any]] = []

        for name, pool in self._thread_pools.items():
            m = pool.get_metrics()
            if m["in_use"] > 0:
                leaks.append({
                    "type": "thread_pool",
                    "name": name,
                    "in_use": m["in_use"],
                })

        for name, pool in self._process_pools.items():
            m = pool.get_metrics()
            if m["in_use"] > 0:
                leaks.append({
                    "type": "process_pool",
                    "name": name,
                    "in_use": m["in_use"],
                })

        return leaks

    # ── 内部 ──

    def _log_allocation(self, pool_type: str, name: str) -> None:
        self._allocation_log.append(
            (pool_type, name, time.monotonic(), new_trace_id())
        )


# ═══════════════════════════════════════════════════════════════
# 便捷装饰器
# ═══════════════════════════════════════════════════════════════

def run_in_thread(pool_name: str = "default"):
    """装饰器：将同步函数放到指定线程池执行

    Usage:
        @run_in_thread("db")
        async def query_db(sql):
            ...
    """
    def decorator(func: Callable[..., T]) -> Callable[..., Awaitable[T]]:
        async def wrapper(*args, **kwargs) -> T:
            from core.bootstrap import get_service
            pool = get_service(f"thread_pool:{pool_name}")
            return await pool.submit(func, *args, **kwargs)
        return wrapper
    return decorator

