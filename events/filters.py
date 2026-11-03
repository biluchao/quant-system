# -*- coding: utf-8 -*-
"""
events/filters.py — 事件过滤器（链式过滤）

铁律声明：本文件严格执行八项铁律
事件在分发前经过过滤链，支持组合条件。
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Union

from core.types import EventEnvelope, EventType, EventPriority, Timestamp


class EventFilter(ABC):
    """事件过滤器基类"""

    @abstractmethod
    def evaluate(self, envelope: EventEnvelope) -> bool:
        pass

    def __and__(self, other: "EventFilter") -> "CompositeFilter":
        return CompositeFilter([self, other], mode="AND")

    def __or__(self, other: "EventFilter") -> "CompositeFilter":
        return CompositeFilter([self, other], mode="OR")


class FilterChain:
    """
    过滤链

    多个过滤器按序执行，全部通过才放行。
    """

    def __init__(self, filters: Optional[List[EventFilter]] = None):
        self._filters: List[EventFilter] = filters or []

    def add_filter(self, filter: EventFilter) -> None:
        self._filters.append(filter)

    def remove_filter(self, filter: EventFilter) -> None:
        self._filters.remove(filter)

    def evaluate(self, envelope: EventEnvelope) -> bool:
        """执行过滤链"""
        for f in self._filters:
            if not f.evaluate(envelope):
                return False
        return True

    def clear(self) -> None:
        self._filters.clear()

    def count(self) -> int:
        return len(self._filters)


class EventTypeFilter(EventFilter):
    """事件类型过滤器"""

    def __init__(self, allowed_types: List[Union[EventType, str]]):
        self._allowed = allowed_types

    def evaluate(self, envelope: EventEnvelope) -> bool:
        event_type = None
        if hasattr(envelope.event, 'event_type'):
            event_type = envelope.event.event_type

        if not event_type:
            return False

        for allowed in self._allowed:
            if isinstance(allowed, str):
                if allowed == str(event_type.value) or allowed.endswith("*") and str(event_type.value).startswith(allowed[:-1]):
                    return True
            else:
                if event_type == allowed:
                    return True

        return False


class SourceFilter(EventFilter):
    """事件源过滤器"""

    def __init__(self, allowed_sources: List[str], mode: str = "allow"):
        self._allowed = set(allowed_sources)
        self._mode = mode  # allow | deny

    def evaluate(self, envelope: EventEnvelope) -> bool:
        source = ""
        if hasattr(envelope.event, 'source'):
            source = envelope.event.source

        if self._mode == "allow":
            return source in self._allowed
        else:
            return source not in self._allowed


class PriorityFilter(EventFilter):
    """优先级过滤器"""

    def __init__(self, min_priority: EventPriority, max_priority: Optional[EventPriority] = None):
        self._min = min_priority
        self._max = max_priority or EventPriority.P0_CRITICAL

    def evaluate(self, envelope: EventEnvelope) -> bool:
        priority = EventPriority.P3_LOW
        if hasattr(envelope.event, 'priority'):
            priority = envelope.event.priority

        return self._min.value <= priority.value <= self._max.value


class TimeRangeFilter(EventFilter):
    """时间范围过滤器"""

    def __init__(self, start: Optional[Timestamp] = None, end: Optional[Timestamp] = None):
        self._start = start
        self._end = end

    def evaluate(self, envelope: EventEnvelope) -> bool:
        ts = envelope.created_at

        if self._start and ts < self._start:
            return False
        if self._end and ts > self._end:
            return False

        return True


class PayloadFilter(EventFilter):
    """Payload 字段过滤器"""

    def __init__(self, conditions: Dict[str, Any], match_mode: str = "exact"):
        self._conditions = conditions
        self._match_mode = match_mode  # exact | contains | regex | gt | lt

    def evaluate(self, envelope: EventEnvelope) -> bool:
        payload = envelope.payload or {}

        for key, expected in self._conditions.items():
            actual = payload.get(key)

            if self._match_mode == "exact":
                if actual != expected:
                    return False
            elif self._match_mode == "contains":
                if expected not in str(actual):
                    return False
            elif self._match_mode == "regex":
                if not re.search(expected, str(actual)):
                    return False
            elif self._match_mode == "gt":
                if actual is None or float(actual) <= float(expected):
                    return False
            elif self._match_mode == "lt":
                if actual is None or float(actual) >= float(expected):
                    return False

        return True


class CompositeFilter(EventFilter):
    """组合过滤器"""

    def __init__(self, filters: List[EventFilter], mode: str = "AND"):
        self._filters = filters
        self._mode = mode  # AND | OR

    def evaluate(self, envelope: EventEnvelope) -> bool:
        if not self._filters:
            return True

        if self._mode == "AND":
            for f in self._filters:
                if not f.evaluate(envelope):
                    return False
            return True

        elif self._mode == "OR":
            for f in self._filters:
                if f.evaluate(envelope):
                    return True
            return False

        return False
