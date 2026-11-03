"""core/memory_watchdog.py — 内存监控与自动清理

铁律声明：本文件遵守
- 类型统一：所有阈值/用量使用明确类型
- 命名统一：类 PascalCase，回调动词开头
- 异常统一：清理失败不阻断主流程
- 幂等审计：每次清理动作可追踪

设计要点：
1. 定期采样内存使用（tracemalloc + psutil）
2. 超过阈值触发清理回调（LRU 缓存、事件队列、历史数据）
3. 清理后再次采样，记录释放量
4. 提供健康状态（正常/警告/危险）
5. 注册自定义清理器（各模块自行注册释放逻辑）
"""
from __future__ import annotations

import asyncio
import gc
import logging
import time
import tracemalloc
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from core.types import AlertLevel, Timestamp


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# 异常
# ═══════════════════════════════════════════════════════════════

class MemoryWatchdogError(Exception):
    """内存看门狗异常"""
    code = 4000


class CleanupHookError(MemoryWatchdogError):
    """清理钩子执行失败"""
    code = 4001


# ═══════════════════════════════════════════════════════════════
# 清理钩子协议
# ═══════════════════════════════════════════════════════════════

CleanupHook = Callable[[], Awaitable[int]]  # 返回释放的字节数


# ═══════════════════════════════════════════════════════════════
# 内存快照
# ═══════════════════════════════════════════════════════════════

@dataclass
class MemorySnapshot:
    """内存快照"""

    timestamp: Timestamp = field(default_factory=lambda: int(time.time() * 1000))
    # RSS（Resident Set Size，进程实际占用物理内存）
    rss_bytes: int = 0
    # 虚拟内存
    vms_bytes: int = 0
    # Python 对象分配（tracemalloc）
    python_allocated: int = 0
    # 堆对象数量
    object_count: int = 0
    # GC 统计
    gc_collected: int = 0

    @property
    def rss_mb(self) -> float:
        return self.rss_bytes / (1024 * 1024)

    @property
    def vms_mb(self) -> float:
        return self.vms_bytes / (1024 * 1024)


# ═══════════════════════════════════════════════════════════════
# 健康状态
# ═══════════════════════════════════════════════════════════════

class MemoryHealth(str):
    """内存健康状态"""

    NORMAL = "NORMAL"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


# ═══════════════════════════════════════════════════════════════
# 内存看门狗
# ═══════════════════════════════════════════════════════════════

class MemoryWatchdog:
    """内存看门狗

    定期监控内存使用，超过阈值自动触发清理。
    可注册自定义清理钩子，由各模块自行释放缓存/队列。

    设计原则：
    - 采样频率自适应（压力小时低频，压力大时高频）
    - 清理动作幂等（多次调用不会出问题）
    - 清理失败不阻断（记录日志，继续下一个钩子）
    - GC 强制回收作为最后手段
    """

    def __init__(
        self,
        warning_threshold_mb: float = 512.0,
        critical_threshold_mb: float = 1024.0,
        sample_interval_sec: float = 30.0,
        enable_tracemalloc: bool = True,
    ) -> None:
        if warning_threshold_mb <= 0:
            raise ValueError("warning_threshold_mb 必须 > 0")
        if critical_threshold_mb <= warning_threshold_mb:
            raise ValueError("critical 阈值必须 > warning 阈值")

        self._warning_threshold = warning_threshold_mb * 1024 * 1024
        self._critical_threshold = critical_threshold_mb * 1024 * 1024
        self._sample_interval = sample_interval_sec
        self._enable_tracemalloc = enable_tracemalloc

        self._cleanup_hooks: List[Tuple[str, CleanupHook]] = []
        self._running = False
        self._task: Optional[asyncio.Task] = None

        # 统计
        self._samples: List[MemorySnapshot] = []
        self._max_samples = 100  # 保留最近 100 次采样
        self._cleanup_count = 0
        self._total_reclaimed_bytes = 0
        self._last_health: MemoryHealth = MemoryHealth.NORMAL
        self._started_at: Optional[float] = None

        # 启动 tracemalloc
        if self._enable_tracemalloc and not tracemalloc.is_tracing():
            tracemalloc.start(25)  # 25 层栈帧

    # ── 生命周期 ──

    async def start(self) -> None:
        """启动看门狗"""
        if self._running:
            return
        self._running = True
        self._started_at = time.monotonic()
        self._task = asyncio.create_task(self._watch_loop())
        logger.info(
            f"MemoryWatchdog started "
            f"(warning={self._warning_threshold / 1024 / 1024:.0f}MB, "
            f"critical={self._critical_threshold / 1024 / 1024:.0f}MB)"
        )

    async def stop(self) -> None:
        """停止看门狗"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._enable_tracemalloc and tracemalloc.is_tracing():
            tracemalloc.stop()
        logger.info("MemoryWatchdog stopped")

    def health_check(self) -> bool:
        """健康检查（Lifecycle 接口要求）"""
        return self._running

    # ── 注册清理钩子 ──

    def register_cleanup_hook(self, name: str, hook: CleanupHook) -> None:
        """注册清理钩子

        钩子返回释放的字节数（估算）。
        多个钩子按注册顺序执行。

        Args:
            name: 钩子标识（用于日志）
            hook: 异步函数，返回释放字节数
        """
        if not callable(hook):
            raise TypeError("hook 必须是可调用对象")
        self._cleanup_hooks.append((name, hook))

    def unregister_cleanup_hook(self, name: str) -> bool:
        """注销清理钩子"""
        for i, (n, _) in enumerate(self._cleanup_hooks):
            if n == name:
                del self._cleanup_hooks[i]
                return True
        return False

    # ── 采样 ──

    def sample(self) -> MemorySnapshot:
        """采样当前内存使用"""
        snapshot = MemorySnapshot()

        # psutil 获取进程内存
        try:
            import psutil
            process = psutil.Process()
            mem_info = process.memory_info()
            snapshot.rss_bytes = mem_info.rss
            snapshot.vms_bytes = mem_info.vms
        except ImportError:
            logger.debug("psutil 未安装，跳过 RSS 采样")
        except Exception as e:
            logger.warning(f"内存采样异常: {e}")

        # tracemalloc 获取 Python 分配
        if self._enable_tracemalloc and tracemalloc.is_tracing():
            current, peak = tracemalloc.get_traced_memory()
            snapshot.python_allocated = current

        # 对象数量
        snapshot.object_count = len(gc.get_objects())

        # 保留采样历史
        self._samples.append(snapshot)
        if len(self._samples) > self._max_samples:
            self._samples.pop(0)

        return snapshot

    def get_health(self, snapshot: Optional[MemorySnapshot] = None) -> MemoryHealth:
        """判断当前健康状态"""
        snap = snapshot or self.sample()
        rss = snap.rss_bytes

        if rss >= self._critical_threshold:
            return MemoryHealth.CRITICAL
        elif rss >= self._warning_threshold:
            return MemoryHealth.WARNING
        else:
            return MemoryHealth.NORMAL

    # ── 清理 ──

    async def trigger_cleanup(self, force_gc: bool = True) -> int:
        """手动触发清理

        Returns:
            释放的字节数
        """
        before = self.sample()
        reclaimed = 0

        # 执行所有清理钩子
        for name, hook in self._cleanup_hooks:
            try:
                released = await hook()
                if released > 0:
                    reclaimed += released
                    logger.info(f"  清理钩子 [{name}] 释放 {released / 1024 / 1024:.1f}MB")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"  清理钩子 [{name}] 执行失败: {e}")

        # 强制 GC
        if force_gc:
            collected = gc.collect()
            if collected > 0:
                logger.info(f"  GC 回收 {collected} 个对象")

        after = self.sample()
        # 估算实际释放量（RSS 差值）
        actual_reclaimed = max(0, before.rss_bytes - after.rss_bytes)
        reclaimed = max(reclaimed, actual_reclaimed)

        self._cleanup_count += 1
        self._total_reclaimed_bytes += reclaimed

        return reclaimed

    # ── 监控循环 ──

    async def _watch_loop(self) -> None:
        """监控循环"""
        # 初始采样
        last_snapshot = self.sample()

        while self._running:
            try:
                await asyncio.sleep(self._sample_interval)
                current = self.sample()

                health = self.get_health(current)
                self._last_health = health

                # 自适应采样间隔
                if health == MemoryHealth.CRITICAL:
                    # 危险：提高采样频率
                    interval = max(2.0, self._sample_interval / 4)
                elif health == MemoryHealth.WARNING:
                    interval = max(5.0, self._sample_interval / 2)
                else:
                    interval = self._sample_interval

                # 状态变化日志
                if health != self.get_health(last_snapshot):
                    level = logging.WARNING if health == MemoryHealth.WARNING else logging.ERROR
                    logger.log(
                        level,
                        f"内存状态变化: {last_snapshot.rss_mb:.1f}MB -> "
                        f"{current.rss_mb:.1f}MB ({health})"
                    )

                # 触发清理
                if health in (MemoryHealth.WARNING, MemoryHealth.CRITICAL):
                    logger.warning(
                        f"内存使用 {current.rss_mb:.1f}MB 超过阈值 "
                        f"({self._warning_threshold / 1024 / 1024:.0f}MB)，触发清理"
                    )
                    reclaimed = await self.trigger_cleanup()
                    if reclaimed > 0:
                        logger.info(f"  本次清理释放 {reclaimed / 1024 / 1024:.1f}MB")

                last_snapshot = current

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"内存监控循环异常: {e}", exc_info=True)
                await asyncio.sleep(self._sample_interval)

    # ── 查询 ──

    def get_metrics(self) -> Dict[str, Any]:
        """获取看门狗指标"""
        latest = self._samples[-1] if self._samples else None
        return {
            "running": self._running,
            "health": self._last_health,
            "warning_threshold_mb": self._warning_threshold / 1024 / 1024,
            "critical_threshold_mb": self._critical_threshold / 1024 / 1024,
            "current_rss_mb": latest.rss_mb if latest else 0,
            "current_python_allocated_mb": (
                latest.python_allocated / 1024 / 1024 if latest else 0
            ),
            "cleanup_count": self._cleanup_count,
            "total_reclaimed_mb": self._total_reclaimed_bytes / 1024 / 1024,
            "registered_hooks": len(self._cleanup_hooks),
            "uptime_sec": (
                round(time.monotonic() - self._started_at, 1)
                if self._started_at else 0
            ),
        }

    def get_samples(self, limit: int = 20) -> List[Dict[str, Any]]:
        """获取最近采样记录"""
        samples = self._samples[-limit:] if limit > 0 else self._samples
        return [
            {
                "timestamp": s.timestamp,
                "rss_mb": round(s.rss_mb, 2),
                "vms_mb": round(s.vms_mb, 2),
                "python_allocated_mb": round(s.python_allocated / 1024 / 1024, 2),
                "object_count": s.object_count,
            }
            for s in samples
        ]


# ═══════════════════════════════════════════════════════════════
# 全局实例
# ═══════════════════════════════════════════════════════════════

_watchdog: Optional[MemoryWatchdog] = None


def get_watchdog() -> Optional[MemoryWatchdog]:
    """获取全局看门狗实例"""
    return _watchdog


def set_watchdog(wd: MemoryWatchdog) -> None:
    """设置全局看门狗实例"""
    global _watchdog
    _watchdog = wd

