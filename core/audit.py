# -*- coding: utf-8 -*-
"""
core/audit.py — 审计日志引擎（不可篡改）

铁律声明：本文件严格执行八项铁律（幂等审计）
所有写操作留审计痕迹，审计日志不可删除、不可修改。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from .types import QuantError, ErrorCode, Timestamp, _utc_now_ms


class AuditError(QuantError):
    """审计异常"""
    pass


@dataclass(frozen=True)
class AuditEntry(ImmutableBase):
    """审计条目（不可变）"""
    entry_id: str = ""
    timestamp: Timestamp = field(default_factory=_utc_now_ms)
    event_type: str = ""
    actor: str = ""              # 操作者（用户/策略/系统）
    action: str = ""             # 操作类型
    target: str = ""             # 操作目标
    payload: Dict[str, Any] = field(default_factory=dict)
    idempotency_key: Optional[str] = None
    previous_hash: str = ""      # 前一条审计条目的哈希
    entry_hash: str = ""        # 本条目的哈希
    signature: str = ""         # 可选签名

    def __post_init__(self):
        # 计算哈希（如果有 entry_id 但 hash 为空）
        if not self.entry_hash and self.entry_id:
            object.__setattr__(self, 'entry_hash', self._compute_hash())

    def _compute_hash(self) -> str:
        """计算条目哈希"""
        data = f"{self.entry_id}|{self.timestamp}|{self.event_type}|{self.actor}|{self.action}|{self.target}|{json.dumps(self.payload, sort_keys=True)}|{self.previous_hash}"
        return hashlib.sha256(data.encode()).hexdigest()

    def verify_integrity(self) -> bool:
        """验证完整性"""
        if not self.entry_hash:
            return False
        computed = self._compute_hash()
        return computed == self.entry_hash


class AuditEngine:
    """
    审计日志引擎

    特性：
    1. 链式哈希（每条记录包含前一条的哈希，防止篡改）
    2. 不可变（frozen dataclass）
    3. 异步写入（不阻塞业务）
    4. 可持久化到文件/WAL
    5. 完整性校验
    """

    def __init__(
        self,
        *,
        storage_path: Optional[str] = None,
        enable_persistence: bool = True,
        max_memory_entries: int = 10000,
        hash_chain_enabled: bool = True,
    ):
        self._entries: List[AuditEntry] = []
        self._storage_path = storage_path
        self._enable_persistence = enable_persistence
        self._max_memory_entries = max_memory_entries
        self._hash_chain_enabled = hash_chain_enabled
        self._last_hash = "genesis"
        self._lock = asyncio.Lock()
        self._write_queue: asyncio.Queue = asyncio.Queue()
        self._flush_task: Optional[asyncio.Task] = None
        self._running = False

        if storage_path and enable_persistence:
            os.makedirs(os.path.dirname(storage_path), exist_ok=True)

    async def start(self) -> None:
        """启动审计引擎"""
        self._running = True
        if self._enable_persistence:
            self._flush_task = asyncio.create_task(self._flush_loop())

    async def stop(self) -> None:
        """停止审计引擎"""
        self._running = False
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        # 最后刷新
        await self._flush_to_storage()

    async def log(
        self,
        event_type: str,
        actor: str,
        action: str,
        target: str,
        payload: Optional[Dict[str, Any]] = None,
        idempotency_key: Optional[str] = None,
    ) -> AuditEntry:
        """记录审计日志"""
        entry_id = f"audit_{len(self._entries) + 1:010d}"

        previous_hash = self._last_hash if self._hash_chain_enabled else ""

        entry = AuditEntry(
            entry_id=entry_id,
            event_type=event_type,
            actor=actor,
            action=action,
            target=target,
            payload=payload or {},
            idempotency_key=idempotency_key,
            previous_hash=previous_hash,
        )

        # 计算哈希
        entry_hash = entry._compute_hash()
        entry = entry.replace(entry_hash=entry_hash)

        async with self._lock:
            self._entries.append(entry)
            if self._hash_chain_enabled:
                self._last_hash = entry_hash

            # 限制内存条目数
            if len(self._entries) > self._max_memory_entries:
                # 保留最新的
                self._entries = self._entries[-self._max_memory_entries:]

        # 写入队列（异步持久化）
        if self._enable_persistence:
            await self._write_queue.put(entry)

        return entry

    async def query(
        self,
        *,
        event_type: Optional[str] = None,
        actor: Optional[str] = None,
        action: Optional[str] = None,
        target: Optional[str] = None,
        since: Optional[Timestamp] = None,
        until: Optional[Timestamp] = None,
        limit: int = 100,
    ) -> List[AuditEntry]:
        """查询审计日志"""
        results: List[AuditEntry] = []
        async with self._lock:
            for entry in reversed(self._entries):
                if event_type and entry.event_type != event_type:
                    continue
                if actor and entry.actor != actor:
                    continue
                if action and entry.action != action:
                    continue
                if target and entry.target != target:
                    continue
                if since and entry.timestamp < since:
                    continue
                if until and entry.timestamp > until:
                    continue
                results.append(entry)
                if len(results) >= limit:
                    break
        return results

    async def verify_chain_integrity(self) -> bool:
        """验证哈希链完整性"""
        async with self._lock:
            previous_hash = "genesis"
            for entry in self._entries:
                if entry.previous_hash != previous_hash:
                    return False
                if not entry.verify_integrity():
                    return False
                previous_hash = entry.entry_hash
        return True

    async def _flush_loop(self) -> None:
        """异步刷新到存储"""
        batch: List[AuditEntry] = []
        last_flush = time.monotonic()

        while self._running:
            try:
                # 收集一批
                try:
                    entry = await asyncio.wait_for(
                        self._write_queue.get(), timeout=1.0
                    )
                    batch.append(entry)
                except asyncio.TimeoutError:
                    pass

                now = time.monotonic()
                if len(batch) >= 100 or (batch and now - last_flush >= 5.0):
                    await self._flush_batch(batch)
                    batch.clear()
                    last_flush = now

            except Exception as e:
                print(f"[AuditEngine] Flush error: {e}")

        # 刷新剩余
        if batch:
            await self._flush_batch(batch)

    async def _flush_batch(self, batch: List[AuditEntry]) -> None:
        """刷新一批到存储"""
        if not self._storage_path:
            return
        try:
            with open(self._storage_path, "a", encoding="utf-8") as f:
                for entry in batch:
                    f.write(json.dumps(entry.to_dict(), default=str) + "\n")
        except Exception as e:
            raise AuditError(
                ErrorCode.CORE_AUDIT_001,
                f"Failed to flush audit entries: {e}"
            )

    async def _flush_to_storage(self) -> None:
        """强制刷新所有到存储"""
        if not self._storage_path or not self._entries:
            return
        try:
            with open(self._storage_path, "a", encoding="utf-8") as f:
                for entry in self._entries:
                    f.write(json.dumps(entry.to_dict(), default=str) + "\n")
        except Exception as e:
            raise AuditError(
                ErrorCode.CORE_AUDIT_001,
                f"Failed to flush audit entries: {e}"
            )

    def stats(self) -> Dict[str, Any]:
        """统计信息"""
        return {
            "total_entries": len(self._entries),
            "hash_chain_enabled": self._hash_chain_enabled,
            "last_hash": self._last_hash,
            "storage_path": self._storage_path,
            "running": self._running,
        }
