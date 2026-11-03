# -*- coding: utf-8 -*-
"""
events/event_registry.py — 事件注册表（事件目录 + Schema 校验）

铁律声明：本文件严格执行八项铁律
所有事件类型必须注册，未注册事件禁止发布。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Type, Callable

from core.types import EventType, EventPriority, QuantError, ErrorCode, _utc_now_ms


class EventRegistryError(QuantError):
    """事件注册表异常"""
    pass


@dataclass(frozen=True)
class EventSchema:
    """事件 Schema 定义"""
    event_type: EventType
    version: str = "1.0"
    required_fields: List[str] = field(default_factory=list)
    optional_fields: List[str] = field(default_factory=list)
    field_types: Dict[str, str] = field(default_factory=dict)
    validator: Optional[Callable[[Any], bool]] = None
    description: str = ""


@dataclass
class EventMetadata:
    """事件元数据"""
    event_type: EventType
    producer_modules: List[str] = field(default_factory=list)
    consumer_modules: List[str] = field(default_factory=list)
    priority: EventPriority = EventPriority.P3_LOW
    ttl_ms: Optional[int] = None
    max_rate_per_sec: Optional[int] = None
    schema: Optional[EventSchema] = None
    tags: List[str] = field(default_factory=list)
    registered_at: int = field(default_factory=_utc_now_ms)


class EventRegistry:
    """
    事件注册表

    职责：
    1. 事件类型注册与发现
    2. Schema 校验
    3. 生产者/消费者追踪
    4. 事件目录（支持前端 EVENT_CATALOG 生成）
    5. 防止未注册事件发布
    """

    def __init__(self, strict_mode: bool = True):
        self._strict_mode = strict_mode
        self._events: Dict[EventType, EventMetadata] = {}
        self._schemas: Dict[EventType, List[EventSchema]] = defaultdict(list)
        self._aliases: Dict[str, EventType] = {}
        self._lock = asyncio.Lock() if False else None  # 同步版本

    def register_event(
        self,
        event_type: EventType,
        *,
        producer_modules: Optional[List[str]] = None,
        consumer_modules: Optional[List[str]] = None,
        priority: EventPriority = EventPriority.P3_LOW,
        ttl_ms: Optional[int] = None,
        max_rate_per_sec: Optional[int] = None,
        schema: Optional[EventSchema] = None,
        tags: Optional[List[str]] = None,
        description: str = "",
    ) -> None:
        """注册事件类型"""
        if event_type in self._events:
            raise EventRegistryError(
                ErrorCode.CORE_EVENT_REGISTRY_001,
                f"Event type '{event_type.value}' already registered"
            )

        metadata = EventMetadata(
            event_type=event_type,
            producer_modules=producer_modules or [],
            consumer_modules=consumer_modules or [],
            priority=priority,
            ttl_ms=ttl_ms,
            max_rate_per_sec=max_rate_per_sec,
            schema=schema,
            tags=tags or [],
        )

        self._events[event_type] = metadata

        if schema:
            self._schemas[event_type].append(schema)

        # 注册别名（短名称）
        short_name = event_type.value.split(".")[-1] if "." in event_type.value else event_type.value
        self._aliases[short_name] = event_type

    def register_schema(self, event_type: EventType, schema: EventSchema) -> None:
        """为已注册事件添加 schema"""
        if event_type not in self._events:
            if self._strict_mode:
                raise EventRegistryError(
                    ErrorCode.CORE_EVENT_REGISTRY_001,
                    f"Event type '{event_type.value}' not registered"
                )
        self._schemas[event_type].append(schema)

    def resolve_event_type(self, name: str) -> Optional[EventType]:
        """通过名称/别名解析事件类型"""
        # 直接匹配
        for et in self._events:
            if et.value == name:
                return et

        # 别名匹配
        if name in self._aliases:
            return self._aliases[name]

        return None

    def validate_event(self, event: Any) -> List[str]:
        """校验事件（返回错误列表，空列表表示通过）"""
        errors: List[str] = []

        if not hasattr(event, 'event_type'):
            errors.append("Event missing 'event_type' attribute")
            return errors

        event_type = event.event_type

        if event_type not in self._events:
            if self._strict_mode:
                errors.append(f"Event type '{event_type.value}' not registered")
            return errors

        metadata = self._events[event_type]

        # Schema 校验
        schemas = self._schemas.get(event_type, [])
        if schemas:
            schema = schemas[-1]  # 使用最新 schema
            for field_name in schema.required_fields:
                if not hasattr(event, field_name) or getattr(event, field_name) is None:
                    errors.append(f"Required field '{field_name}' missing")

            if schema.validator:
                try:
                    if not schema.validator(event):
                        errors.append("Custom schema validation failed")
                except Exception as e:
                    errors.append(f"Schema validator error: {e}")

        return errors

    def get_metadata(self, event_type: EventType) -> Optional[EventMetadata]:
        return self._events.get(event_type)

    def list_all_events(self) -> List[EventMetadata]:
        return list(self._events.values())

    def list_by_tag(self, tag: str) -> List[EventMetadata]:
        return [m for m in self._events.values() if tag in m.tags]

    def list_by_producer(self, module: str) -> List[EventMetadata]:
        return [m for m in self._events.values() if module in m.producer_modules]

    def list_by_consumer(self, module: str) -> List[EventMetadata]:
        return [m for m in self._events.values() if module in m.consumer_modules]

    def export_catalog(self) -> Dict[str, Any]:
        """导出事件目录（用于前端 EVENT_CATALOG）"""
        catalog = {
            "version": "1.0",
            "total_events": len(self._events),
            "events": [],
        }

        for et, metadata in self._events.items():
            entry = {
                "type": et.value,
                "priority": metadata.priority.value,
                "producers": metadata.producer_modules,
                "consumers": metadata.consumer_modules,
                "ttl_ms": metadata.ttl_ms,
                "max_rate": metadata.max_rate_per_sec,
                "tags": metadata.tags,
            }
            schemas = self._schemas.get(et, [])
            if schemas:
                latest = schemas[-1]
                entry["schema"] = {
                    "version": latest.version,
                    "required_fields": latest.required_fields,
                    "optional_fields": latest.optional_fields,
                    "field_types": latest.field_types,
                }
            catalog["events"].append(entry)

        return catalog

    def is_registered(self, event_type: EventType) -> bool:
        return event_type in self._events

    def stats(self) -> Dict[str, Any]:
        return {
            "total_events": len(self._events),
            "total_schemas": sum(len(s) for s in self._schemas.values()),
            "aliases": len(self._aliases),
            "strict_mode": self._strict_mode,
        }


# 需要导入 asyncio
import asyncio
