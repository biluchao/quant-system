"""core/bulkhead.py — 舱壁隔离（资源隔离）

铁律声明：本文件遵守
- 类型统一：所有配额/用量使用明确类型
- 命名统一：类 PascalCase，方法动词开头
- 异常统一：资源耗尽抛精确异常
- 幂等审计：配额分配可追踪

设计问题：
    一个疯狂下单的策略不应该拖垮整个系统。
    舱壁隔离（Bulkhead）模式将系统资源按逻辑单元（策略/品种/模块）隔离。

设计要点：
1. 每个隔离单元有独立的并发限制和队列容量
2. 单元之间互不影响（一个满了不影响其他）
3. 支持优先级（关键操作可插队）
4. 超额请求快速失败（fail-fast），不阻塞
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Tuple

from core.types import AlertLevel, StrategyId, TraceId, new_trace_id


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# 异常
# ═══════════════════════════════════════════════════════════════

class BulkheadError(Exception):
    """舱壁异常基类"""
    code = 5000


class BulkheadFullError(BulkheadError):
    """舱壁已满（资源配额耗尽）"""
    code = 5001


class BulkheadNotFoundError(BulkheadError):
    """舱壁不存在"""
    code = 5002


# ═══════════════════════════════════════════════════════════════
# 舱壁配置
# ═══════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class BulkheadConfig:
    """舱壁配置（不可变）"""

    name: str
    # 最大并发执行数
    max_concurrency: int = 10
    # 等待队列最大长度（0 = 不排队，直接拒绝）
    max_queue_size: int = 100
    # 单个任务最大执行时间（秒），超时强制取消
    task_timeout: float = 30.0
    # 队列满时的策略：reject（拒绝）/ wait（等待）
    overflow_strategy: str = "reject"
    # 是否关键舱壁（关键舱壁耗尽时升级告警级别）
    critical: bool = False


# ═══════════════════════════════════════════════════════════════
# 舱壁统计
# ═══════════════════════════════════════════════════════════════

@dataclass
class BulkheadStats:
    """舱壁运行时统计"""

    name: str
    max_concurrency: int
    max_queue_size: int
    current_concurrency: int = 0
    current_queue_size: int = 0
    total_submitted: int = 0
    total_rejected: int = 0
    total_completed: int = 0
    total_failed: int = 0
    total_timeouts: int = 0
    total_cancelled: int = 0
    # 任务耗时统计（毫秒）
    min_latency_ms: float = 0.0
    max_latency_ms: float = 0.0
    avg_latency_ms: float = 0.0
    _latency_sum_ms: float = 0.0
    created_at: float = field(default_factory=time.monotonic)

    @property
    def available(self) -> int:
        return self.max_concurrency - self.current_concurrency

    @property
    def queue_available(self) -> int:
        return self.max_queue_size - self.current_queue_size

    @property
    def utilization(self) -> float:
        return self.current_concurrency / max(self.max_concurrency, 1)

    @property
    def queue_utilization(self) -> float:
        if self.max_queue_size == 0:
            return 0.0
        return self.current_queue_size / self.max_queue_size

    @property
    def is_saturated(self) -> bool:
        return self.current_concurrency >= self.max_concurrency

    @property
    def is_queue_full(self) -> bool:
        return self.current_queue_size >= self.max_queue_size

    def record_latency(self, latency_ms: float) -> None:
        if self.total_completed == 0:
            self.min_latency_ms = latency_ms
            self.max_latency_ms = latency_ms
        else:
            self.min_latency_ms = min(self.min_latency_ms, latency_ms)
            self.max_latency_ms = max(self.max_latency_ms, latency_ms)
        self._latency_sum_ms += latency_ms
        self.avg_latency_ms = self._latency_sum_ms / max(self.total_completed, 1)

    def snapshot(self) -> dict:
        return {
            "name": self.name,
            "max_concurrency": self.max_concurrency,
            "max_queue_size": self.max_queue_size,
            "current_concurrency": self.current_concurrency,
            "current_queue_size": self.current_queue_size,
            "available": self.available,
            "queue_available": self.queue_available,
            "utilization": round(self.utilization, 4),
            "queue_utilization": round(self.queue_utilization, 4),
            "is_saturated": self.is_saturated,
            "is_queue_full": self.is_queue_full,
            "total_submitted": self.total_submitted,
            "total_rejected": self.total_rejected,
            "total_completed": self.total_completed,
            "total_failed": self.total_failed,
            "total_timeouts": self.total_timeouts,
            "total_cancelled": self.total_cancelled,
            "min_latency_ms": round(self.min_latency_ms, 2),
            "max_latency_ms": round(self.max_latency_ms, 2),
            "avg_latency_ms": round(self.avg_latency_ms, 2),
            "uptime_sec": round(time.monotonic() - self.created_at, 1),
        }


# ═══════════════════════════════════════════════════════════════
# 单个舱壁
# ═══════════════════════════════════════════════════════════════

class Bulkhead:
    """单个舱壁隔离单元

    一个舱壁 = 一个隔离的资源池。
    例如：
    - "strategy:arbitrage_v1" — 套利策略的舱壁
    - "strategy:market_maker" — 做市策略的舱壁
    - "exchange:binance" — Binance 网关的舱壁
    - "internal:reconciliation" — 对账任务的舱壁
    """

    def __init__(self, config: BulkheadConfig) -> None:
        if config.max_concurrency < 1:
            raise ValueError("max_concurrency 必须 >= 1")

        self._config = config
        self._semaphore = asyncio.Semaphore(config.max_concurrency)
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max(config.max_queue_size, 0))
        self._stats = BulkheadStats(
            name=config.name,
            max_concurrency=config.max_concurrency,
            max_queue_size=config.max_queue_size,
        )
        self._running = False

    @property
    def name(self) -> str:
        return self._config.name

    @property
    def stats(self) -> BulkheadStats:
        return self._stats

    @property
    def config(self) -> BulkheadConfig:
        return self._config

    def acquire_sync(self) -> "_BulkheadPermit":
        """同步获取许可（上下文管理器）

        Usage:
            async with bulkhead.acquire() as permit:
                await do_work()
        """
        return _BulkheadPermit(self)

    async def acquire(self, timeout: Optional[float] = None) -> "_BulkheadPermit":
        """异步获取许可"""
        return await _BulkheadPermit(self).acquire(timeout)

    async def submit(
        self,
        func: Callable[..., Awaitable[T]],
        *args,
        timeout: Optional[float] = None,
        trace_id: Optional[TraceId] = None,
        **kwargs,
    ) -> asyncio.Task[T]:
        """提交任务到舱壁

        如果舱壁已满：
        - overflow_strategy=reject → 立即抛出 BulkheadFullError
        - overflow_strategy=wait → 进入队列等待
        """
        if not self._running and timeout != 0:
            # 舱壁未启动，尝试快速执行
            pass

        # 检查是否可以立即执行
        if self._stats.current_concurrency < self._config.max_concurrency:
            self._stats.total_submitted += 1
            task = asyncio.create_task(
                self._execute(func, args, kwargs, trace_id)
            )
            return task

        # 舱壁已满
        if self._config.overflow_strategy == "reject":
            self._stats.total_rejected += 1
            alert_level = AlertLevel.P0 if self._config.critical else AlertLevel.P2
            raise BulkheadFullError(
                f"舱壁 [{self._config.name}] 已满 "
                f"({self._stats.current_concurrency}/{self._config.max_concurrency})"
            )

        # 进入等待队列
        if self._stats.current_queue_size >= self._config.max_queue_size:
            self._stats.total_rejected += 1
            raise BulkheadFullError(
                f"舱壁 [{self._config.name}] 队列已满 "
                f"({self._stats.current_queue_size}/{self._config.max_queue_size})"
            )

        self._stats.current_queue_size += 1
        self._stats.total_submitted += 1

        # 排队等待
        future: asyncio.Future = asyncio.get_event_loop().create_future()

        async def queued_executor():
            try:
                await self._semaphore.acquire()
                self._stats.current_queue_size -= 1
                self._stats.current_concurrency += 1
                start = time.monotonic()
                try:
                    if timeout and timeout > 0:
                        result = await asyncio.wait_for(
                            func(*args, **kwargs),
                            timeout=timeout,
                        )
                    else:
                        result = await func(*args, **kwargs)
                    self._stats.total_completed += 1
                    self._stats.record_latency((time.monotonic() - start) * 1000)
                    return result
                except asyncio.TimeoutError:
                    self._stats.total_timeouts += 1
                    raise
                except asyncio.CancelledError:
                    self._stats.total_cancelled += 1
                    raise
                except Exception:
                    self._stats.total_failed += 1
                    raise
                finally:
                    self._stats.current_concurrency -= 1
                    self._semaphore.release()
            finally:
                if not future.done():
                    future.set_result(None)

        asyncio.create_task(queued_executor())
        return future  # type: ignore

    async def _execute(
        self,
        func: Callable[..., Awaitable[T]],
        args: tuple,
        kwargs: dict,
        trace_id: Optional[TraceId],
    ) -> T:
        """执行任务"""
        await self._semaphore.acquire()
        self._stats.current_concurrency += 1
        start = time.monotonic()

        try:
            if self._config.task_timeout > 0:
                result = await asyncio.wait_for(
                    func(*args, **kwargs),
                    timeout=self._config.task_timeout,
                )
            else:
                result = await func(*args, **kwargs)

            self._stats.total_completed += 1
            self._stats.record_latency((time.monotonic() - start) * 1000)
            return result

        except asyncio.TimeoutError:
            self._stats.total_timeouts += 1
            raise BulkheadFullError(
                f"舱壁 [{self._config.name}] 任务超时 "
                f"({self._config.task_timeout}s)"
            )
        except asyncio.CancelledError:
            self._stats.total_cancelled += 1
            raise
        except Exception:
            self._stats.total_failed += 1
            raise
        finally:
            self._stats.current_concurrency -= 1
            self._semaphore.release()

    async def drain(self) -> None:
        """排空舱壁（等待所有在途任务完成）"""
        while self._stats.current_concurrency > 0 or self._stats.current_queue_size > 0:
            await asyncio.sleep(0.1)

    def get_status(self) -> dict:
        return self._stats.snapshot()


# ═══════════════════════════════════════════════════════════════
# 许可令牌
# ═══════════════════════════════════════════════════════════════

class _BulkheadPermit:
    """舱壁许可（上下文管理器）"""

    def __init__(self, bulkhead: Bulkhead) -> None:
        self._bulkhead = bulkhead
        self._acquired = False

    async def __aenter__(self) -> "_BulkheadPermit":
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()

    async def acquire(self, timeout: Optional[float] = None) -> "_BulkheadPermit":
        bh = self._bulkhead
        try:
            if timeout is not None:
                await asyncio.wait_for(
                    bh._semaphore.acquire(),
                    timeout=timeout,
                )
            else:
                await bh._semaphore.acquire()
        except asyncio.TimeoutError:
            raise BulkheadFullError(
                f"获取舱壁许可超时: {bh.name}"
            )

        bh._stats.current_concurrency += 1
        self._acquired = True
        return self

    def release(self) -> None:
        if self._acquired:
            self._bulkhead._stats.current_concurrency -= 1
            self._bulkhead._semaphore.release()
            self._acquired = False


# ═══════════════════════════════════════════════════════════════
# 舱壁管理器
# ═══════════════════════════════════════════════════════════════

class BulkheadManager:
    """舱壁管理器

    统一管理所有舱壁隔离单元。
    预设策略级/品种级/模块级的资源配额。
    """

    def __init__(self) -> None:
        self._bulkheads: Dict[str, Bulkhead] = {}
        self._default_config: Optional[BulkheadConfig] = None

    def set_default_config(self, config: BulkheadConfig) -> None:
        """设置默认配置（未显式创建的舱壁使用此配置）"""
        self._default_config = config

    def create(self, config: BulkheadConfig) -> Bulkhead:
        """创建舱壁"""
        if config.name in self._bulkheads:
            raise DuplicateError(f"舱壁已存在: {config.name}")

        bh = Bulkhead(config)
        self._bulkheads[config.name] = bh
        logger.info(
            f"舱壁已创建: {config.name} "
            f"(max_concurrency={config.max_concurrency}, "
            f"queue={config.max_queue_size})"
        )
        return bh

    def get(self, name: str) -> Bulkhead:
        """获取舱壁（不存在则按默认配置创建）"""
        if name in self._bulkheads:
            return self._bulkheads[name]

        if self._default_config is not None:
            config = BulkheadConfig(
                name=name,
                max_concurrency=self._default_config.max_concurrency,
                max_queue_size=self._default_config.max_queue_size,
                task_timeout=self._default_config.task_timeout,
                overflow_strategy=self._default_config.overflow_strategy,
            )
            return self.create(config)

        raise BulkheadNotFoundError(f"舱壁不存在: {name}")

    def get_or_create(
        self,
        name: str,
        config: Optional[BulkheadConfig] = None,
    ) -> Bulkhead:
        """获取或创建"""
        if name in self._bulkheads:
            return self._bulkheads[name]

        if config is None:
            if self._default_config is None:
                raise ValueError("必须提供 config 或设置 default_config")
            config = BulkheadConfig(
                name=name,
                max_concurrency=self._default_config.max_concurrency,
                max_queue_size=self._default_config.max_queue_size,
                task_timeout=self._default_config.task_timeout,
                overflow_strategy=self._default_config.overflow_strategy,
            )

        return self.create(config)

    def remove(self, name: str) -> bool:
        """移除舱壁"""
        if name in self._bulkheads:
            del self._bulkheads[name]
            return True
        return False

    def list_all(self) -> List[str]:
        """列出所有舱壁名"""
        return list(self._bulkheads.keys())

    def get_all_stats(self) -> Dict[str, dict]:
        """获取所有舱壁统计"""
        return {
            name: bh.get_status()
            for name, bh in self._bulkheads.items()
        }

    def get_saturated(self) -> List[str]:
        """获取所有饱和的舱壁"""
        return [
            name for name, bh in self._bulkheads.items()
            if bh.stats.is_saturated
        ]

    def get_overloaded(self) -> List[str]:
        """获取队列满的舱壁"""
        return [
            name for name, bh in self._bulkheads.items()
            if bh.stats.is_queue_full
        ]

    async def drain_all(self) -> None:
        """排空所有舱壁"""
        for bh in self._bulkheads.values():
            await bh.drain()

    def shutdown(self) -> None:
        """关闭管理器"""
        self._bulkheads.clear()
        logger.info("BulkheadManager 已关闭")


# ═══════════════════════════════════════════════════════════════
# 预设配置（按策略/模块级别）
# ═══════════════════════════════════════════════════════════════

def default_strategy_bulkhead(strategy_id: str) -> BulkheadConfig:
    """策略默认舱壁配置"""
    return BulkheadConfig(
        name=f"strategy:{strategy_id}",
        max_concurrency=5,
        max_queue_size=50,
        task_timeout=10.0,
        overflow_strategy="reject",
        critical=False,
    )


def default_gateway_bulkhead(exchange: str) -> BulkheadConfig:
    """交易所网关默认舱壁配置"""
    return BulkheadConfig(
        name=f"gateway:{exchange}",
        max_concurrency=20,
        max_queue_size=200,
        task_timeout=5.0,
        overflow_strategy="wait",
        critical=True,
    )


def default_internal_bulkhead(name: str) -> BulkheadConfig:
    """内部任务默认舱壁配置"""
    return BulkheadConfig(
        name=f"internal:{name}",
        max_concurrency=3,
        max_queue_size=20,
        task_timeout=60.0,
        overflow_strategy="wait",
        critical=False,
    )

