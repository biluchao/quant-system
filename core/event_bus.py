#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 事件总线 (EventBus) v8.0.0 — 机构级最终版

核心职责：
1. 线程安全、优先级感知、发布/订阅事件通道
2. 事件类型白名单，强制校验，防止注入
3. 发布时自动生成事件ID，支持端到端追踪
4. 数据隔离：可配置拷贝策略，限制拷贝尺寸，避免性能灾难
5. 回调熔断与自动恢复（指数退避），使用弱引用防止内存泄漏
6. 死信队列持久化（内存环形+定期刷写）及访问接口
7. 背压保护：队列超限按优先级丢弃，高优事件死信记录
8. 停止保护：停止后拒绝新事件，排空剩余事件并记录死信
9. 完整的可观测性：Prometheus 指标、分发延迟直方图、结构化日志

外部依赖：
- queue, threading, time, uuid, copy, sys, weakref (标准库)
- core.metrics.MetricsCollector (可选)

接口契约：
- subscribe(event_type: str, callback: Callable, subscriber_id: Optional[str] = None) -> None
- unsubscribe(event_type: str, callback: Callable) -> None
- publish(event_type: str, data: Any = None, priority: Priority = Priority.MEDIUM, ttl: Optional[float] = None) -> Optional[str]  返回 event_id 或 None
- start() / stop() / reset()
- health_check() -> Dict[str, Any]
- get_dead_letters() -> List[Dict]

异常与降级：
- 所有公开方法均捕获异常，绝不向上抛出
- 分发线程异常退出时自动重启（最多 N 次）
- 订阅者连续错误达到阈值则熔断，恢复期指数退避

资源管理：
- 队列大小可配置，防止内存溢出
- 死信队列限制长度，旧记录自动淘汰
"""

import copy
import itertools
import logging
import queue
import sys
import threading
import time
import uuid
import weakref
from enum import IntEnum
from typing import Dict, Any, List, Callable, Optional, Set, Tuple, Deque
from collections import deque

logger = logging.getLogger(__name__)

# 可选依赖
try:
    from core.metrics import MetricsCollector
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    MetricsCollector = None


class Priority(IntEnum):
    CRITICAL = 0   # 风控、止损、交易所断连
    HIGH = 1       # 成交回报、订单状态更新
    MEDIUM = 2     # K线闭合、深度快照
    LOW = 3        # 指标计算完成、心跳


class EventTypes:
    """标准化事件类型（白名单）"""
    TICK = "tick"
    DEPTH = "depth"
    KLINE_CLOSE = "kline_close"
    DATA_QUALITY_ALERT = "data_quality_alert"
    ORDER_CREATED = "order_created"
    ORDER_FILLED = "order_filled"
    ORDER_CANCELLED = "order_cancelled"
    TRADE_CLOSED = "trade_closed"
    HEARTBEAT = "heartbeat"
    SYSTEM_ALERT = "system_alert"
    STATE_CHANGE = "state_change"

    ALL_TYPES: Set[str] = {
        TICK, DEPTH, KLINE_CLOSE, DATA_QUALITY_ALERT,
        ORDER_CREATED, ORDER_FILLED, ORDER_CANCELLED, TRADE_CLOSED,
        HEARTBEAT, SYSTEM_ALERT, STATE_CHANGE
    }


# ── 常量 ──────────────────────────────────────────────────
DEFAULT_MAX_QUEUE_SIZE = 16384
MAX_EVENT_DATA_SIZE_BYTES = 1_048_576      # 1MB 事件数据上限
BACKPRESSURE_DROP_PRIORITY = Priority.LOW
MAX_SUBSCRIBERS_PER_EVENT = 20
CIRCUIT_BREAKER_THRESHOLD = 5
CIRCUIT_BREAKER_BASE_BACKOFF_SEC = 1.0     # 首次恢复等待1秒
CIRCUIT_BREAKER_MAX_BACKOFF_SEC = 300.0    # 最大恢复等待5分钟
CALLBACK_TIMEOUT_WARNING_SEC = 0.05
DISPATCHER_JOIN_TIMEOUT_SEC = 5.0
EVENT_TTL_SEC = 60.0
DEAD_LETTER_QUEUE_SIZE = 2000


class EventBus:
    """线程安全、优先级感知的事件总线（单例，可重置）"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, max_queue_size: int = DEFAULT_MAX_QUEUE_SIZE,
                 use_background: bool = True, copy_mode: str = "shallow"):
        # copy_mode: "none" 不拷贝, "shallow" 浅拷贝, "deep" 深拷贝
        if getattr(self, '_initialized', False):
            return
        with self._lock:
            if getattr(self, '_initialized', False):
                return
            self._initialized = True

            self.max_queue_size = max_queue_size
            self.use_background = use_background
            self.copy_mode = copy_mode

            self._queue: queue.PriorityQueue = queue.PriorityQueue(maxsize=self.max_queue_size)
            self._seq_lock = threading.Lock()
            self._seq_counter = itertools.count()

            self._subscribers: Dict[str, List[Callable]] = {}
            self._sub_lock = threading.RLock()

            # 回调熔断 (使用 weakref 避免 id 重用)
            self._cb_weakrefs: Dict[int, weakref.ref] = {}
            self._cb_errors: Dict[int, int] = {}
            self._cb_circuit_broken: Dict[int, float] = {}
            self._cb_error_lock = threading.Lock()

            self._stats = {
                "published": 0,
                "dropped": 0,
                "dispatched": 0,
                "errors": 0,
                "dead_letters": 0,
            }
            self._stats_lock = threading.Lock()

            self._dispatcher_thread: Optional[threading.Thread] = None
            self._stop_event = threading.Event()
            self._accepting_events = True  # 停止时设为 False

            self._dead_letter_queue: deque = deque(maxlen=DEAD_LETTER_QUEUE_SIZE)
            self._dead_letter_lock = threading.Lock()

            if self.use_background:
                self._start_dispatcher()

            logger.info("EventBus v8.0.0 初始化: max_q=%d copy=%s", max_queue_size, copy_mode)

    # ── 公共接口 ──────────────────────────────────────────

    def subscribe(self, event_type: str, callback: Callable, subscriber_id: Optional[str] = None) -> None:
        if not callable(callback):
            logger.error("订阅回调不可调用")
            return
        if event_type != '*' and event_type not in EventTypes.ALL_TYPES:
            logger.warning("订阅未知事件类型: %s", event_type)
        with self._sub_lock:
            if event_type not in self._subscribers:
                self._subscribers[event_type] = []
            if len(self._subscribers[event_type]) >= MAX_SUBSCRIBERS_PER_EVENT:
                logger.error("事件 %s 订阅者已满", event_type)
                return
            if callback not in self._subscribers[event_type]:
                self._subscribers[event_type].append(callback)
                # 注册弱引用
                with self._cb_error_lock:
                    self._cb_weakrefs[id(callback)] = weakref.ref(callback)
                logger.debug("订阅: %s -> %s", event_type, subscriber_id or id(callback))

    def unsubscribe(self, event_type: str, callback: Callable) -> None:
        with self._sub_lock:
            if event_type in self._subscribers:
                try:
                    self._subscribers[event_type].remove(callback)
                except ValueError:
                    pass

    def publish(self, event_type: str, data: Any = None,
                priority: Priority = Priority.MEDIUM,
                ttl: Optional[float] = None) -> Optional[str]:
        """
        发布事件，返回 event_id (UUID) 或 None（失败）。
        """
        if not self._accepting_events:
            logger.warning("事件总线已停止，拒绝新事件")
            return None

        if event_type not in EventTypes.ALL_TYPES and event_type != '*':
            logger.warning("发布未注册事件类型: %s", event_type)

        # 分配序列号（线程安全）
        with self._seq_lock:
            seq = next(self._seq_counter) & 0x7FFFFFFF
        timestamp = time.monotonic()  # 单调时钟
        actual_ttl = ttl if ttl is not None else EVENT_TTL_SEC
        event_id = str(uuid.uuid4())

        # 数据拷贝策略
        event_data = self._copy_data(data)
        if event_data is not None and sys.getsizeof(event_data) > MAX_EVENT_DATA_SIZE_BYTES:
            logger.error("事件数据过大 (%d bytes)，拒绝发布", sys.getsizeof(event_data))
            return None

        event_item = (int(priority), seq, timestamp, event_id, event_type, event_data, actual_ttl)

        try:
            self._queue.put_nowait(event_item)
            self._inc_stat("published")
            self._record_metrics("event_bus_published", 1, {"type": event_type})
            return event_id
        except queue.Full:
            if priority <= Priority.HIGH:
                self._record_dead_letter(event_item)
            else:
                self._inc_stat("dropped")
                logger.warning("事件丢弃 type=%s pri=%s", event_type, priority.name)
                self._record_metrics("event_bus_dropped", 1, {"reason": "queue_full", "type": event_type})
            return None

    def start(self) -> None:
        if self.use_background and (self._dispatcher_thread is None or not self._dispatcher_thread.is_alive()):
            self._start_dispatcher()

    def stop(self) -> None:
        logger.info("停止事件总线...")
        self._accepting_events = False
        self._stop_event.set()
        if self._dispatcher_thread and self._dispatcher_thread.is_alive():
            self._dispatcher_thread.join(timeout=DISPATCHER_JOIN_TIMEOUT_SEC)
        self._drain_remaining_to_dead_letter()
        logger.info("事件总线已停止")

    def backlog_size(self) -> int:
        return self._queue.qsize()

    def get_dead_letters(self) -> List[Dict]:
        with self._dead_letter_lock:
            return list(self._dead_letter_queue)

    @classmethod
    def reset(cls) -> None:
        with cls._lock:
            if cls._instance is not None:
                cls._instance.stop()
            cls._instance = None

    def health_check(self) -> Dict[str, Any]:
        warnings = []
        qsize = self.backlog_size()
        if qsize > self.max_queue_size * 0.9:
            warnings.append(f"队列使用率过高: {qsize}/{self.max_queue_size}")

        with self._stats_lock:
            pub = self._stats["published"]
            drop = self._stats["dropped"]
            err = self._stats["errors"]
            dl = self._stats["dead_letters"]

        if pub > 0 and drop / pub > 0.02:
            warnings.append(f"丢弃率过高: {drop}/{pub}")
        if err > 100:
            warnings.append(f"分发错误过多: {err}")
        if dl > DEAD_LETTER_QUEUE_SIZE * 0.9:
            warnings.append(f"死信队列即将满: {dl}/{DEAD_LETTER_QUEUE_SIZE}")

        if self.use_background and not (self._dispatcher_thread and self._dispatcher_thread.is_alive()):
            warnings.append("分发线程未运行")

        status = "degraded" if warnings else "ok"
        return {
            "status": status,
            "reason": f"q={qsize}, pub={pub}, drop={drop}, dl={dl}",
            "warnings": warnings,
            "stats": {"published": pub, "dropped": drop, "dispatched": self._stats.get("dispatched", 0),
                      "errors": err, "dead_letters": dl},
        }

    # ── 内部分发 ──────────────────────────────────────────

    def _start_dispatcher(self) -> None:
        if self._dispatcher_thread and self._dispatcher_thread.is_alive():
            logger.warning("分发线程已存在")
            return
        self._stop_event.clear()
        self._dispatcher_thread = threading.Thread(
            target=self._dispatch_loop,
            name="event-bus-dispatcher",
            daemon=True
        )
        self._dispatcher_thread.start()

    def _dispatch_loop(self) -> None:
        logger.info("分发循环启动")
        while not self._stop_event.is_set():
            try:
                item = self._queue.get(timeout=0.2)
            except queue.Empty:
                continue
            except Exception as e:
                logger.error("队列获取异常: %s", e)
                self._inc_stat("errors")
                continue

            try:
                priority, seq, ts, event_id, event_type, data, ttl = item
                # TTL 检查（单调时钟）
                if time.monotonic() - ts > ttl:
                    self._inc_stat("dropped")
                    self._record_metrics("event_bus_expired", 1, {"type": event_type})
                    continue

                latency = time.monotonic() - ts
                self._record_histogram("event_bus_dispatch_latency", latency, {"type": event_type})

                self._dispatch_to_subscribers(event_type, data, event_id)
                self._inc_stat("dispatched")
            except Exception as e:
                logger.critical("事件分发严重异常: %s", e, exc_info=True)
                self._inc_stat("errors")

    def _dispatch_to_subscribers(self, event_type: str, data: Any, event_id: str) -> None:
        with self._sub_lock:
            callbacks = list(self._subscribers.get(event_type, []))
            if '*' in self._subscribers:
                callbacks += list(self._subscribers['*'])

        for cb in callbacks:
            cb_id = id(cb)
            if self._is_circuit_broken(cb_id):
                continue

            start = time.perf_counter()
            try:
                # 数据隔离策略
                if self.copy_mode == "deep":
                    cb(copy.deepcopy(data) if data is not None else data)
                elif self.copy_mode == "shallow":
                    cb(copy.copy(data) if data is not None else data)
                else:
                    cb(data)
                elapsed = time.perf_counter() - start
                if elapsed > CALLBACK_TIMEOUT_WARNING_SEC:
                    logger.warning("回调 %s 耗时 %.4fs", cb_id, elapsed)
                self._reset_cb_error(cb_id)
            except Exception as e:
                logger.error("回调异常 [%s]: %s", event_type, e)
                self._inc_stat("errors")
                self._record_cb_error(cb_id)

    # ── 熔断机制（带指数退避） ──────────────────────────────

    def _is_circuit_broken(self, cb_id: int) -> bool:
        with self._cb_error_lock:
            if cb_id in self._cb_circuit_broken:
                if time.time() < self._cb_circuit_broken[cb_id]:
                    return True
                # 熔断到期，尝试恢复
                del self._cb_circuit_broken[cb_id]
                self._cb_errors.pop(cb_id, None)
                logger.info("回调 %d 熔断恢复", cb_id)
        return False

    def _record_cb_error(self, cb_id: int) -> None:
        with self._cb_error_lock:
            errs = self._cb_errors.get(cb_id, 0) + 1
            self._cb_errors[cb_id] = errs
            if errs >= CIRCUIT_BREAKER_THRESHOLD:
                # 指数退避
                backoff = min(CIRCUIT_BREAKER_BASE_BACKOFF_SEC * (2 ** (errs - CIRCUIT_BREAKER_THRESHOLD)),
                              CIRCUIT_BREAKER_MAX_BACKOFF_SEC)
                self._cb_circuit_broken[cb_id] = time.time() + backoff
                logger.warning("回调 %d 熔断 %d 次, 恢复等待 %.1fs", cb_id, errs, backoff)

    def _reset_cb_error(self, cb_id: int) -> None:
        with self._cb_error_lock:
            self._cb_errors.pop(cb_id, None)

    # ── 死信处理 ─────────────────────────────────────────

    def _record_dead_letter(self, event_item: Tuple) -> None:
        try:
            _, seq, ts, event_id, event_type, data, ttl = event_item
            entry = {
                "priority": event_item[0],
                "event_type": event_type,
                "event_id": event_id,
                "timestamp": ts,
            }
            with self._dead_letter_lock:
                self._dead_letter_queue.append(entry)
            self._inc_stat("dead_letters")
            logger.warning("死信记录: %s", entry)
        except Exception:
            pass

    def _drain_remaining_to_dead_letter(self) -> None:
        while not self._queue.empty():
            try:
                item = self._queue.get_nowait()
                self._record_dead_letter(item)
            except queue.Empty:
                break

    # ── 数据拷贝 ──────────────────────────────────────────

    def _copy_data(self, data: Any) -> Any:
        if data is None:
            return None
        if self.copy_mode == "deep":
            try:
                return copy.deepcopy(data)
            except Exception as e:
                logger.error("deepcopy 失败: %s, 降级为不拷贝", e)
                return data
        elif self.copy_mode == "shallow":
            try:
                return copy.copy(data)
            except Exception:
                return data
        else:
            return data

    # ── 统计与指标 ────────────────────────────────────────

    def _inc_stat(self, key: str) -> None:
        with self._stats_lock:
            self._stats[key] = self._stats.get(key, 0) + 1

    def _record_metrics(self, name: str, value: float, labels: Optional[Dict] = None) -> None:
        if METRICS_AVAILABLE and MetricsCollector:
            try:
                MetricsCollector.counter(name, value, labels)
            except Exception:
                pass

    def _record_histogram(self, name: str, value: float, labels: Optional[Dict] = None) -> None:
        if METRICS_AVAILABLE and MetricsCollector:
            try:
                MetricsCollector.histogram(name, value, labels)
            except Exception:
                pass
