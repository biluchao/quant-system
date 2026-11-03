# -*- coding: utf-8 -*-
"""
core/memory_watchdog.py — 内存监控 + 自动清理

铁律声明：本文件严格执行八项铁律
所有队列/缓存/字典必须有上限，内存超限时自动触发清理。
"""

from __future__ import annotations

import asyncio
import gc
import psutil
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from .types import QuantError, ErrorCode, SystemConfig

# DEVIATION: psutil 使用 float 而非 Decimal，因为 psutil API 返回 float，
# 且内存值不需要业务级精度


class MemoryWatchdogError(QuantError):
    """内存监控异常"""
    pass


@dataclass
class MemorySnapshot:
    """内存快照"""
    timestamp: int
    rss_mb: float           # 物理内存
    vms_mb: float           # 虚拟内存
    percent: float          # 系统内存占比
    available_mb: float     # 系统可用内存
    gc_objects: int         # GC 跟踪对象数
    process_fd_count: int   # 文件描述符数


@dataclass
class WatchdogConfig:
    """看门狗配置"""
    warning_threshold_mb: int = 1024
    critical_threshold_mb: int = 2048
    fatal_threshold_mb: int = 4096
    check_interval_sec: float = 10.0
    auto_gc_on_warning: bool = True
    auto_gc_on_critical: bool = True
    max_fd_count: int = 4096
    cleanup_callbacks: List[Callable[[], int]] = field(default_factory=list)


class MemoryWatchdog:
    """
    内存看门狗

    职责：
    1. 定期采样内存使用
    2. 超过阈值触发告警/清理
    3. 注册清理回调（缓存/队列/字典的 trim 方法）
    4. 文件描述符监控
    """

    def __init__(self, config: Optional[WatchdogConfig] = None):
        self._config = config or WatchdogConfig()
        self._process = psutil.Process(os.getpid())
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._snapshots: List[MemorySnapshot] = []
        self._max_snapshots = 100  # 防止快照本身内存泄漏
        self._cleanup_callbacks: List[Callable[[], int]] = list(self._config.cleanup_callbacks)
        self._alert_callbacks: List[Callable[[str, MemorySnapshot], None]] = []
        self._lock = asyncio.Lock()

    def register_cleanup(self, callback: Callable[[], int]) -> None:
        """注册清理回调，返回清理了多少项"""
        self._cleanup_callbacks.append(callback)

    def register_alert_callback(self, callback: Callable[[str, MemorySnapshot], None]) -> None:
        """注册告警回调"""
        self._alert_callbacks.append(callback)

    async def start(self) -> None:
        """启动看门狗"""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._monitor_loop())

    async def stop(self) -> None:
        """停止看门狗"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _monitor_loop(self) -> None:
        """监控循环"""
        while self._running:
            try:
                snapshot = self._take_snapshot()
                await self._check_thresholds(snapshot)
                await self._trim_snapshots()
            except Exception as e:
                # 看门狗自身不能崩
                print(f"[MemoryWatchdog] Error in monitor loop: {e}")

            await asyncio.sleep(self._config.check_interval_sec)

    def _take_snapshot(self) -> MemorySnapshot:
        """采集内存快照"""
        mem_info = self._process.memory_info()
        sys_mem = psutil.virtual_memory()
        snapshot = MemorySnapshot(
            timestamp=int(asyncio.get_event_loop().time() * 1000),
            rss_mb=mem_info.rss / 1024 / 1024,
            vms_mb=mem_info.vms / 1024 / 1024,
            percent=self._process.memory_percent(),
            available_mb=sys_mem.available / 1024 / 1024,
            gc_objects=len(gc.get_objects()),
            process_fd_count=self._process.num_fds() if hasattr(self._process, 'num_fds') else 0,
        )
        return snapshot

    async def _check_thresholds(self, snapshot: MemorySnapshot) -> None:
        """检查阈值并触发动作"""
        rss_mb = snapshot.rss_mb

        if rss_mb >= self._config.fatal_threshold_mb:
            await self._handle_fatal(snapshot)
        elif rss_mb >= self._config.critical_threshold_mb:
            await self._handle_critical(snapshot)
        elif rss_mb >= self._config.warning_threshold_mb:
            await self._handle_warning(snapshot)

        # 文件描述符检查
        if snapshot.process_fd_count > self._config.max_fd_count:
            await self._handle_fd_exhaustion(snapshot)

    async def _handle_warning(self, snapshot: MemorySnapshot) -> None:
        """警告级别：触发 GC"""
        for callback in self._alert_callbacks:
            callback("WARNING", snapshot)

        if self._config.auto_gc_on_warning:
            collected = gc.collect()
            snapshot.gc_objects = len(gc.get_objects())

    async def _handle_critical(self, snapshot: MemorySnapshot) -> None:
        """严重级别：GC + 触发所有清理回调"""
        for callback in self._alert_callbacks:
            callback("CRITICAL", snapshot)

        if self._config.auto_gc_on_critical:
            gc.collect()

        # 执行所有清理回调
        total_cleaned = 0
        for callback in self._cleanup_callbacks:
            try:
                cleaned = callback()
                total_cleaned += cleaned
            except Exception as e:
                print(f"[MemoryWatchdog] Cleanup callback error: {e}")

        # 再次 GC
        gc.collect()

    async def _handle_fatal(self, snapshot: MemorySnapshot) -> None:
        """致命级别：强制清理 + 告警"""
        for callback in self._alert_callbacks:
            callback("FATAL", snapshot)

        # 强制清理
        for callback in self._cleanup_callbacks:
            try:
                callback()
            except Exception:
                pass

        # 强制 GC（全代）
        gc.collect(0)
        gc.collect(1)
        gc.collect(2)

        # 裁剪快照
        self._snapshots.clear()

    async def _handle_fd_exhaustion(self, snapshot: MemorySnapshot) -> None:
        """文件描述符耗尽处理"""
        for callback in self._alert_callbacks:
            callback("FD_EXHAUSTION", snapshot)

    async def _trim_snapshots(self) -> None:
        """限制快照数量"""
        if len(self._snapshots) > self._max_snapshots:
            self._snapshots = self._snapshots[-self._max_snapshots:]

    def get_latest_snapshot(self) -> Optional[MemorySnapshot]:
        """获取最新快照"""
        return self._snapshots[-1] if self._snapshots else None

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        latest = self.get_latest_snapshot()
        return {
            "running": self._running,
            "latest_rss_mb": latest.rss_mb if latest else 0,
            "latest_percent": latest.percent if latest else 0,
            "snapshot_count": len(self._snapshots),
            "cleanup_callbacks_count": len(self._cleanup_callbacks),
            "config": {
                "warning_mb": self._config.warning_threshold_mb,
                "critical_mb": self._config.critical_threshold_mb,
                "fatal_mb": self._config.fatal_threshold_mb,
                "check_interval": self._config.check_interval_sec,
            },
        }
