# -*- coding: utf-8 -*-
"""
core/idempotency.py — 幂等性引擎（幂等键 + 去重）

铁律声明：本文件严格执行八项铁律（幂等审计）
所有写操作必须携带 idempotency_key，重复请求返回缓存结果。
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Tuple

from .types import QuantError, ErrorCode, Timestamp, _utc_now_ms


class DuplicateRequestError(QuantError):
    """重复请求异常"""
    pass


@dataclass
class IdempotencyRecord:
    """幂等记录"""
    idempotency_key: str
    result: Any = None
    error: Optional[str] = None
    created_at: Timestamp = field(default_factory=_utc_now_ms)
    completed_at: Optional[Timestamp] = None
    ttl_sec: int = 3600  # 默认1小时
    status: str = "processing"  # processing | completed | failed

    def is_expired(self) -> bool:
        now = _utc_now_ms()
        return (now - self.created_at) > (self.ttl_sec * 1000)

    def is_completed(self) -> bool:
        return self.status == "completed"


class IdempotencyEngine:
    """
    幂等性引擎

    功能：
    1. 检查请求是否已处理过（通过 idempotency_key）
    2. 处理中请求去重（防止并发重复提交）
    3. 已完成请求返回缓存结果
    4. 过期记录自动清理
    """

    def __init__(self, default_ttl_sec: int = 3600, cleanup_interval_sec: int = 300):
        self._records: Dict[str, IdempotencyRecord] = {}
        self._default_ttl = default_ttl_sec
        self._cleanup_interval = cleanup_interval_sec
        self._last_cleanup = time.monotonic()
        self._lock = asyncio.Lock()

    async def check_or_execute(
        self,
        idempotency_key: str,
        func: Callable[..., Any],
        *args,
        ttl_sec: Optional[int] = None,
        **kwargs,
    ) -> Any:
        """
        幂等执行：如果 key 已存在且完成，返回缓存结果；
        如果正在处理，等待结果；
        否则执行函数并缓存结果。
        """
        ttl = ttl_sec or self._default_ttl

        async with self._lock:
            self._maybe_cleanup()

            record = self._records.get(idempotency_key)

            if record is not None:
                if record.is_expired():
                    # 过期，删除后重新执行
                    del self._records[idempotency_key]
                elif record.is_completed():
                    # 已完成，返回缓存结果
                    if record.error:
                        raise DuplicateRequestError(
                            ErrorCode.CORE_IDEMPOTENCY_001,
                            f"Request '{idempotency_key}' previously failed: {record.error}"
                        )
                    return record.result
                else:
                    # 处理中，等待
                    pass

            # 创建新记录
            record = IdempotencyRecord(
                idempotency_key=idempotency_key,
                ttl_sec=ttl,
                status="processing",
            )
            self._records[idempotency_key] = record

        # 执行函数（在锁外，避免长时间持锁）
        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)

            # 更新记录
            async with self._lock:
                record.result = result
                record.status = "completed"
                record.completed_at = _utc_now_ms()

            return result

        except Exception as e:
            # 记录失败
            async with self._lock:
                record.error = str(e)
                record.status = "failed"
                record.completed_at = _utc_now_ms()
            raise

    async def check(self, idempotency_key: str) -> Optional[IdempotencyRecord]:
        """检查 key 是否存在"""
        async with self._lock:
            self._maybe_cleanup()
            record = self._records.get(idempotency_key)
            if record and record.is_expired():
                del self._records[idempotency_key]
                return None
            return record

    async def invalidate(self, idempotency_key: str) -> bool:
        """使 key 失效"""
        async with self._lock:
            if idempotency_key in self._records:
                del self._records[idempotency_key]
                return True
            return False

    async def clear_expired(self) -> int:
        """清理所有过期记录"""
        async with self._lock:
            expired_keys = [
                k for k, v in self._records.items()
                if v.is_expired()
            ]
            for k in expired_keys:
                del self._records[k]
            return len(expired_keys)

    def _maybe_cleanup(self) -> None:
        """定期清理"""
        now = time.monotonic()
        if now - self._last_cleanup >= self._cleanup_interval:
            expired_keys = [k for k, v in self._records.items() if v.is_expired()]
            for k in expired_keys:
                del self._records[k]
            self._last_cleanup = now

    def stats(self) -> Dict[str, Any]:
        """统计信息"""
        return {
            "total_records": len(self._records),
            "processing": sum(1 for r in self._records.values() if r.status == "processing"),
            "completed": sum(1 for r in self._records.values() if r.status == "completed"),
            "failed": sum(1 for r in self._records.values() if r.status == "failed"),
        }
