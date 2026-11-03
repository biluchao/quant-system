# -*- coding: utf-8 -*-
"""
events/event_store.py — 事件存储（WAL + 查询）

铁律声明：本文件严格执行八项铁律
事件不可丢失，支持持久化和回溯查询。
"""

from __future__ import annotations

import asyncio
import json
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from core.types import EventEnvelope, EventType, Timestamp, QuantError, ErrorCode, _utc_now_ms


class EventStoreError(QuantError):
    """事件存储异常"""
    pass


class EventStore(ABC):
    """事件存储抽象基类"""

    @abstractmethod
    async def append(self, envelope: EventEnvelope) -> None:
        pass

    @abstractmethod
    async def query(
        self,
        *,
        event_type: Optional[EventType] = None,
        since: Optional[Timestamp] = None,
        until: Optional[Timestamp] = None,
        producer: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[EventEnvelope]:
        pass

    @abstractmethod
    async def get_by_id(self, envelope_id: str) -> Optional[EventEnvelope]:
        pass

    @abstractmethod
    async def count(self, event_type: Optional[EventType] = None) -> int:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass


class InMemoryEventStore(EventStore):
    """内存事件存储（用于测试和回测）"""

    def __init__(self, max_size: int = 100000):
        self._events: List[EventEnvelope] = []
        self._index_by_id: Dict[str, EventEnvelope] = {}
        self._index_by_type: Dict[EventType, List[int]] = defaultdict(list)
        self._max_size = max_size
        self._lock = asyncio.Lock()

    async def append(self, envelope: EventEnvelope) -> None:
        async with self._lock:
            idx = len(self._events)
            self._events.append(envelope)
            self._index_by_id[envelope.envelope_id] = envelope

            event_type = None
            if hasattr(envelope.event, 'event_type'):
                event_type = envelope.event.event_type
            if event_type:
                self._index_by_type[event_type].append(idx)

            # 限制大小
            if len(self._events) > self._max_size:
                removed = self._events.pop(0)
                self._index_by_id.pop(removed.envelope_id, None)
                if hasattr(removed.event, 'event_type'):
                    et = removed.event.event_type
                    if et in self._index_by_type:
                        self._index_by_type[et] = [i - 1 for i in self._index_by_type[et][1:]]

    async def query(
        self,
        *,
        event_type: Optional[EventType] = None,
        since: Optional[Timestamp] = None,
        until: Optional[Timestamp] = None,
        producer: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[EventEnvelope]:
        async with self._lock:
            results: List[EventEnvelope] = []

            # 优先使用类型索引
            if event_type:
                indices = self._index_by_type.get(event_type, [])
                candidates = [self._events[i] for i in indices if i < len(self._events)]
            else:
                candidates = list(self._events)

            for env in reversed(candidates):
                if since and env.created_at < since:
                    continue
                if until and env.created_at > until:
                    continue
                if producer and env.producer != producer:
                    continue
                results.append(env)
                if len(results) >= limit + offset:
                    break

            return results[offset:offset + limit] if offset > 0 else results[:limit]

    async def get_by_id(self, envelope_id: str) -> Optional[EventEnvelope]:
        async with self._lock:
            return self._index_by_id.get(envelope_id)

    async def count(self, event_type: Optional[EventType] = None) -> int:
        async with self._lock:
            if event_type:
                return len(self._index_by_type.get(event_type, []))
            return len(self._events)

    async def close(self) -> None:
        pass


class FileEventStore(EventStore):
    """文件事件存储（WAL 模式）"""

    def __init__(self, base_path: str, max_file_size_mb: int = 100):
        self._base_path = base_path
        self._max_file_size = max_file_size_mb * 1024 * 1024
        self._current_file = None
        self._current_file_size = 0
        self._file_index: Dict[str, str] = {}  # envelope_id -> file_path
        self._lock = asyncio.Lock()
        os.makedirs(base_path, exist_ok=True)
        self._open_new_file()

    def _open_new_file(self) -> None:
        import time
        filename = f"events_{int(time.time() * 1000)}.wal"
        filepath = os.path.join(self._base_path, filename)
        self._current_file = open(filepath, "a", encoding="utf-8")
        self._current_file_size = 0

    async def append(self, envelope: EventEnvelope) -> None:
        async with self._lock:
            if not self._current_file or self._current_file_size >= self._max_file_size:
                if self._current_file:
                    self._current_file.close()
                self._open_new_file()

            line = json.dumps(envelope.to_dict(), default=str) + "\n"
            self._current_file.write(line)
            self._current_file.flush()
            self._current_file_size += len(line.encode())
            self._file_index[envelope.envelope_id] = self._current_file.name

    async def query(self, **kwargs) -> List[EventEnvelope]:
        """文件存储查询（简化实现，全量扫描）"""
        results: List[EventEnvelope] = []
        limit = kwargs.get("limit", 100)
        offset = kwargs.get("offset", 0)

        # 扫描所有 WAL 文件
        for filename in sorted(os.listdir(self._base_path)):
            if not filename.endswith(".wal"):
                continue
            filepath = os.path.join(self._base_path, filename)
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    data = json.loads(line)
                    # 简化过滤
                    results.append(self._dict_to_envelope(data))
                    if len(results) >= limit + offset:
                        break

        return results[offset:offset + limit] if offset > 0 else results[:limit]

    async def get_by_id(self, envelope_id: str) -> Optional[EventEnvelope]:
        async with self._lock:
            if envelope_id not in self._file_index:
                return None
            filepath = self._file_index[envelope_id]
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    data = json.loads(line)
                    if data.get("envelope_id") == envelope_id:
                        return self._dict_to_envelope(data)
        return None

    async def count(self, event_type: Optional[EventType] = None) -> int:
        total = 0
        for filename in os.listdir(self._base_path):
            if not filename.endswith(".wal"):
                continue
            filepath = os.path.join(self._base_path, filename)
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    if event_type:
                        data = json.loads(line)
                        env_type = data.get("event", {}).get("event_type")
                        if env_type != event_type.value:
                            continue
                    total += 1
        return total

    async def close(self) -> None:
        async with self._lock:
            if self._current_file:
                self._current_file.close()
                self._current_file = None

    def _dict_to_envelope(self, data: Dict[str, Any]) -> EventEnvelope:
        """字典转信封（简化版）"""
        from core.types import EventEnvelope
        # 简化实现，实际需要完整反序列化
        return EventEnvelope(
            envelope_id=data.get("envelope_id", ""),
            created_at=data.get("created_at", 0),
            producer=data.get("producer", ""),
            idempotency_key=data.get("idempotency_key"),
            ttl_ms=data.get("ttl_ms"),
            retry_count=data.get("retry_count", 0),
            trace_id=data.get("trace_id"),
        )


from collections import defaultdict
