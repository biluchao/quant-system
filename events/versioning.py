# -*- coding: utf-8 -*-
"""
events/versioning.py — 事件版本管理

铁律声明：本文件严格执行八项铁律
事件格式变更时保证向后兼容，支持 schema 迁移。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Type

from core.types import EventEnvelope, EventType, QuantError, ErrorCode, _utc_now_ms


class EventVersionError(QuantError):
    """事件版本异常"""
    pass


@dataclass(frozen=True)
class VersionedEvent:
    """版本化事件"""
    envelope: EventEnvelope
    schema_version: str = "1.0"
    compatible_versions: List[str] = field(default_factory=lambda: ["1.0"])
    migration_path: List[str] = field(default_factory=list)


@dataclass
class SchemaMigration:
    """Schema 迁移定义"""
    from_version: str
    to_version: str
    migrate_fn: Callable[[Dict[str, Any]], Dict[str, Any]]
    description: str = ""


class EventVersionManager:
    """
    事件版本管理器

    职责：
    1. 管理事件 schema 版本
    2. 自动迁移旧版本事件到当前版本
    3. 验证版本兼容性
    4. 支持多版本并行
    """

    def __init__(self, current_version: str = "1.0"):
        self._current_version = current_version
        self._migrations: Dict[str, SchemaMigration] = {}  # key: "from->to"
        self._version_registry: Dict[EventType, List[str]] = defaultdict(list)
        self._compatibility_matrix: Dict[str, List[str]] = {}

    def register_migration(self, migration: SchemaMigration) -> None:
        """注册迁移路径"""
        key = f"{migration.from_version}->{migration.to_version}"
        self._migrations[key] = migration

    def register_event_version(self, event_type: EventType, version: str) -> None:
        """注册事件类型支持的版本"""
        if version not in self._version_registry[event_type]:
            self._version_registry[event_type].append(version)

    def set_compatibility(self, version: str, compatible_with: List[str]) -> None:
        """设置版本兼容性"""
        self._compatibility_matrix[version] = compatible_with

    async def wrap_for_publish(self, envelope: EventEnvelope) -> EventEnvelope:
        """发布前包装版本信息"""
        # 检查事件类型是否注册
        event_type = None
        if hasattr(envelope.event, 'event_type'):
            event_type = envelope.event.event_type

        if event_type and event_type in self._version_registry:
            versions = self._version_registry[event_type]
            if self._current_version not in versions:
                # 需要迁移
                envelope = await self._migrate_envelope(envelope, self._current_version)

        # 注入版本信息到 payload
        if hasattr(envelope, 'payload') and envelope.payload is not None:
            envelope.payload["_schema_version"] = self._current_version

        return envelope

    async def unwrap_for_consume(self, envelope: EventEnvelope) -> EventEnvelope:
        """消费前解包和版本适配"""
        payload_version = None
        if envelope.payload and "_schema_version" in envelope.payload:
            payload_version = envelope.payload["_schema_version"]

        if payload_version and payload_version != self._current_version:
            # 需要迁移
            envelope = await self._migrate_envelope(envelope, self._current_version)

        return envelope

    async def _migrate_envelope(self, envelope: EventEnvelope, target_version: str) -> EventEnvelope:
        """迁移信封到目标版本"""
        if not envelope.payload:
            return envelope

        current_version = envelope.payload.get("_schema_version", "1.0")

        # 查找迁移路径
        migration_key = f"{current_version}->{target_version}"
        if migration_key in self._migrations:
            migration = self._migrations[migration_key]
            new_payload = migration.migrate_fn(envelope.payload)
            new_payload["_schema_version"] = target_version
            return envelope.replace(payload=new_payload)

        # 尝试多步迁移
        if current_version in self._compatibility_matrix:
            compat_versions = self._compatibility_matrix[current_version]
            if target_version in compat_versions:
                # 兼容版本，直接升级标记
                new_payload = dict(envelope.payload)
                new_payload["_schema_version"] = target_version
                return envelope.replace(payload=new_payload)

        # 无法迁移，返回原样（不阻塞）
        return envelope

    def is_compatible(self, version: str) -> bool:
        """检查版本是否兼容当前版本"""
        if version == self._current_version:
            return True
        compat = self._compatibility_matrix.get(self._current_version, [])
        return version in compat

    def get_current_version(self) -> str:
        return self._current_version

    def list_migrations(self) -> List[str]:
        return list(self._migrations.keys())

    def stats(self) -> Dict[str, Any]:
        return {
            "current_version": self._current_version,
            "registered_events": len(self._version_registry),
            "migrations": len(self._migrations),
            "compatibility_entries": len(self._compatibility_matrix),
        }
