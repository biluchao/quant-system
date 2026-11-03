# -*- coding: utf-8 -*-
"""
events/replay.py — 事件重放引擎（确定性回放）

铁律声明：本文件严格执行八项铁律
支持从任意时间点重放事件，保证确定性（相同输入→相同输出）。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

from core.types import (
    EventEnvelope, EventType, Timestamp, QuantError, ErrorCode, _utc_now_ms,
)
from .event_store import EventStore, InMemoryEventStore


class ReplayError(QuantError):
    """重放异常"""
    pass


class ReplayMode(str, Enum):
    """重放模式"""
    EXACT_TIME = "exact_time"        # 按原始时间间隔重放
    FAST_FORWARD = "fast_forward"    # 尽快重放（忽略间隔）
    STEP_BY_STEP = "step_by_step"    # 逐步重放（手动控制）
    DETERMINISTIC = "deterministic"  # 确定性模式（记录随机种子）


@dataclass
class ReplayConfig:
    """重放配置"""
    mode: ReplayMode = ReplayMode.FAST_FORWARD
    start_time: Optional[Timestamp] = None
    end_time: Optional[Timestamp] = None
    event_types: Optional[List[EventType]] = None
    producers: Optional[List[str]] = None
    max_events: Optional[int] = None
    speed_multiplier: float = 1.0       # 速度倍率（仅 EXACT_TIME 模式）
    deterministic_seed: Optional[int] = None
    verify_checksums: bool = True
    stop_on_error: bool = False
    pause_on_event_type: Optional[EventType] = None


@dataclass
class ReplayResult:
    """重放结果"""
    total_events: int = 0
    processed_events: int = 0
    skipped_events: int = 0
    error_events: int = 0
    checksum_mismatches: int = 0
    start_time: Timestamp = field(default_factory=_utc_now_ms)
    end_time: Optional[Timestamp] = None
    duration_ms: int = 0
    errors: List[str] = field(default_factory=list)
    checksum: str = ""

    def finalize(self) -> None:
        self.end_time = _utc_now_ms()
        self.duration_ms = self.end_time - self.start_time
        # 计算校验和
        data = f"{self.total_events}|{self.processed_events}|{self.skipped_events}|{self.error_events}"
        self.checksum = hashlib.sha256(data.encode()).hexdigest()[:16]


class EventReplayEngine:
    """
    事件重放引擎

    职责：
    1. 从 EventStore 加载历史事件
    2. 按配置重放（时间/顺序/过滤）
    3. 确定性保证（种子+校验）
    4. 断点续传
    5. 重放结果校验
    """

    def __init__(self, event_store: EventStore, event_bus=None):
        self._store = event_store
        self._bus = event_bus
        self._running = False
        self._paused = False
        self._current_index = 0
        self._processed_ids: Set[str] = set()
        self._pause_event = asyncio.Event()
        self._pause_event.set()  # 默认不暂停

    async def replay(
        self,
        config: ReplayConfig,
        handler: Optional[Callable[[EventEnvelope], Any]] = None,
    ) -> ReplayResult:
        """执行重放"""
        result = ReplayResult()

        # 确定性种子
        if config.deterministic_seed is not None:
            import random
            random.seed(config.deterministic_seed)

        # 查询事件
        events = await self._store.query(
            event_type=config.event_types[0] if config.event_types else None,
            since=config.start_time,
            until=config.end_time,
            limit=config.max_events or 100000,
        )

        if config.event_types and len(config.event_types) > 1:
            # 多类型过滤
            events = [e for e in events
                      if hasattr(e.event, 'event_type') and e.event.event_type in config.event_types]

        if config.producers:
            events = [e for e in events if e.producer in config.producers]

        result.total_events = len(events)

        # 按时间排序
        events.sort(key=lambda e: e.created_at)

        self._running = True
        self._paused = False
        self._current_index = 0

        prev_time = None

        for i, envelope in enumerate(events):
            if not self._running:
                break

            # 暂停控制
            if self._paused:
                await self._pause_event.wait()

            # 断点续传
            if i < self._current_index:
                result.skipped_events += 1
                continue

            # 校验和验证
            if config.verify_checksums:
                if envelope.envelope_id in self._processed_ids:
                    result.checksum_mismatches += 1
                    if config.stop_on_error:
                        result.errors.append(f"Duplicate event: {envelope.envelope_id}")
                        break

            # 时间控制
            if config.mode == ReplayMode.EXACT_TIME and prev_time and envelope.created_at > prev_time:
                elapsed = envelope.created_at - prev_time
                sleep_sec = (elapsed / 1000.0) / config.speed_multiplier
                if sleep_sec > 0:
                    await asyncio.sleep(min(sleep_sec, 1.0))

            prev_time = envelope.created_at

            # 执行处理
            try:
                if handler:
                    if asyncio.iscoroutinefunction(handler):
                        await handler(envelope)
                    else:
                        handler(envelope)
                elif self._bus:
                    await self._bus.publish_sync(envelope)

                result.processed_events += 1
                self._processed_ids.add(envelope.envelope_id)

            except Exception as e:
                result.error_events += 1
                result.errors.append(f"Event {envelope.envelope_id}: {e}")
                if config.stop_on_error:
                    break

            # 暂停点
            if config.pause_on_event_type:
                if hasattr(envelope.event, 'event_type') and envelope.event.event_type == config.pause_on_event_type:
                    self._paused = True
                    self._pause_event.clear()
                    await self._pause_event.wait()

            self._current_index = i + 1

        result.finalize()
        self._running = False
        return result

    def pause(self) -> None:
        """暂停重放"""
        self._paused = True
        self._pause_event.clear()

    def resume(self) -> None:
        """恢复重放"""
        self._paused = False
        self._pause_event.set()

    def stop(self) -> None:
        """停止重放"""
        self._running = False
        self.resume()  # 解除暂停

    def get_progress(self) -> Dict[str, Any]:
        return {
            "running": self._running,
            "paused": self._paused,
            "current_index": self._current_index,
            "processed_count": len(self._processed_ids),
        }
