# -*- coding: utf-8 -*-
"""
events 包初始化 — 事件驱动架构层

铁律声明：本文件严格执行八项铁律（事件统一 / 幂等审计 / 命名统一）
"""

from .bus import EventBus, EventBusError, SubscriptionHandle
from .versioning import (
    EventVersionManager, EventVersionError,
    VersionedEvent, SchemaMigration,
)
from .event_registry import (
    EventRegistry, EventRegistryError,
    EventMetadata, EventSchema,
)
from .event_store import (
    EventStore, EventStoreError,
    InMemoryEventStore, FileEventStore,
)
from .replay import (
    EventReplayEngine, ReplayError,
    ReplayConfig, ReplayMode, ReplayResult,
)
from .subscriber import (
    SubscriberManager, Subscriber,
    SubscriberGroup, SubscriberError,
)
from .publisher import (
    Publisher, PublisherError,
    DefaultPublisher,
)
from .filters import (
    EventFilter, FilterChain,
    EventTypeFilter, SourceFilter, PriorityFilter,
    TimeRangeFilter, PayloadFilter, CompositeFilter,
)

__all__ = [
    # bus
    "EventBus", "EventBusError", "SubscriptionHandle",
    # versioning
    "EventVersionManager", "EventVersionError",
    "VersionedEvent", "SchemaMigration",
    # event_registry
    "EventRegistry", "EventRegistryError",
    "EventMetadata", "EventSchema",
    # event_store
    "EventStore", "EventStoreError",
    "InMemoryEventStore", "FileEventStore",
    # replay
    "EventReplayEngine", "ReplayError",
    "ReplayConfig", "ReplayMode", "ReplayResult",
    # subscriber
    "SubscriberManager", "Subscriber",
    "SubscriberGroup", "SubscriberError",
    # publisher
    "Publisher", "PublisherError", "DefaultPublisher",
    # filters
    "EventFilter", "FilterChain",
    "EventTypeFilter", "SourceFilter", "PriorityFilter",
    "TimeRangeFilter", "PayloadFilter", "CompositeFilter",
]

__version__ = "5.0.0-production"
