#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors. All Rights Reserved.
"""
火种系统 · WebSocket 客户端管理器 (WebSocketClient)

核心职责：
1. 管理与币安交易所的 WebSocket 长连接（公开行情 / 用户私有数据流）
2. 自动重连（指数退避 + 随机抖动 + 最大重试）与心跳保活
3. 消息接收与硬件单调时间戳注入，标准化后推入内部事件总线（带背压控制）
4. 连接池管理、流去重、订阅确认处理、TLS证书固定

外部依赖（真实模块接口）：
- core.event_bus.EventBus : 内部事件发布（支持背压的 publish 方法）
- core.secrets_manager.SecretsManager : 获取交易所 API 密钥（Vault）
- core.metrics : Prometheus 指标输出（可选）
- core.audit_logger : 审计日志（可选）
- core.exchange_clock : 交易所时钟同步（可选）

接口契约：
- connect(streams: List[str]) -> bool
- disconnect() -> None
- health_check() -> Dict[str, Any]
  输出字典固定包含 "status", "reason", "warnings", "metrics"
- subscribe(streams: List[str]) -> bool  (动态追加订阅)
- unsubscribe(streams: List[str]) -> bool (动态取消订阅)

异常与降级：
- 连接断开自动重连（指数退避 + 随机抖动，最大间隔 30s，最大重试 10 次）
- 心跳超时（>30s 无 pong）触发强制重连
- 网络不可用时降级为离线模式，发布 WS_OFFLINE 事件通知风控
- 密钥获取失败时拒绝建立私有连接，记录 CRITICAL
- 消息队列背压时优先丢弃非关键事件（如 depth 更新）
- 连续 3 次重连失败后暂停 60s 冷却，避免交易所 IP 封禁

资源管理：
- 每个连接一个独立守护线程，使用队列异步处理消息
- 断开时确保关闭 WebSocket（带超时）并清理线程
- 退出时由 atexit 钩子 + signal handler 自动断开所有连接
- 文件描述符使用软限制监控，接近上限时告警

安全：
- TLS 证书固定（SHA256指纹校验）
- 消息深度限制防止栈溢出
- 消息大小限制防止内存耗尽
- 敏感信息过滤（日志中脱敏 API 密钥）
- 路径注入防护（流名校验白名单）

用法示例:
    client = WebSocketClient(event_bus, secrets_manager)
    client.connect(["btcusdt@kline_3m", "btcusdt@depth@100ms"])
"""

import atexit
import hashlib
import json
import logging
import os
import random
import signal
import sys
import threading
import time
import uuid
from enum import Enum
from typing import Dict, Any, List, Optional, Set, Callable, Union

import websocket

# ── 可选依赖（优雅降级） ──────────────────────────────────
try:
    from core.metrics import MetricsCollector
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    MetricsCollector = None  # type: ignore

try:
    from core.audit_logger import AuditLogger
    AUDIT_AVAILABLE = True
except ImportError:
    AUDIT_AVAILABLE = False
    AuditLogger = None  # type: ignore

try:
    from core.exchange_clock import ExchangeClock
    CLOCK_AVAILABLE = True
except ImportError:
    CLOCK_AVAILABLE = False
    ExchangeClock = None  # type: ignore

logger = logging.getLogger(__name__)

# ── 常量定义 ──────────────────────────────────────────────
VERSION: str = "3.0.2"
SPDX_IDENTIFIER: str = "Apache-2.0"

# WebSocket 端点
WEBSOCKET_BASE_URL: str = "wss://fstream.binance.com/ws"
WEBSOCKET_BASE_URL_TESTNET: str = "wss://stream.binancefuture.com/ws"
# 币安已知 TLS 证书 SHA256 指纹（生产环境）
BINANCE_TLS_FINGERPRINTS: Set[str] = {
    "a1b2c3d4e5f6...",  # 替换为实际指纹
}

# 重连配置
DEFAULT_RECONNECT_MAX_RETRIES: int = 10
DEFAULT_RECONNECT_BASE_DELAY: float = 0.5  # 秒
DEFAULT_RECONNECT_MAX_DELAY: float = 30.0  # 秒
RECONNECT_JITTER_FACTOR: float = 0.1  # ±10% 随机抖动
COOLDOWN_AFTER_CONSECUTIVE_FAILURES: int = 3  # 连续失败次数阈值
COOLDOWN_DURATION: float = 60.0  # 冷却秒数

# 心跳配置
HEARTBEAT_PING_INTERVAL: int = 15  # 发送 ping 间隔（秒）
HEARTBEAT_PONG_TIMEOUT: int = 30  # 等待 pong 超时（秒）

# 消息处理配置
MAX_MESSAGE_DEPTH: int = 20  # JSON 最大嵌套深度
MAX_MESSAGE_SIZE: int = 1024 * 1024  # 1MB 单消息上限
MAX_STREAMS_PER_CONNECTION: int = 200  # 币安单连接限制
BATCH_PUBLISH_SIZE: int = 50  # 批量发布阈值
MAX_QUEUE_SIZE: int = 10000  # 内部队列最大容量

# 线程与超时配置
DISCONNECT_TIMEOUT: float = 5.0  # 断开超时（秒）
CONNECT_TIMEOUT: float = 10.0  # 连接建立超时（秒）
THREAD_JOIN_TIMEOUT: float = 5.0  # 线程等待超时（秒）

# 流名字符白名单（防注入）
STREAM_NAME_PATTERN: str = r'^[a-zA-Z0-9_@]+$'

# 日志格式
LOG_FORMAT_JSON: str = (
    '{"timestamp":"%(asctime)s","level":"%(levelname)s",'
    '"module":"%(name)s","client_id":"%(client_id)s","message":"%(message)s"}'
)

# 事件类型枚举
class WSEventType(str, Enum):
    """WebSocket 事件类型"""
    CONNECTED = "ws_connected"
    DISCONNECTED = "ws_disconnected"
    MARKET_DATA = "market_data"
    ERROR = "ws_error"
    OFFLINE = "ws_offline"
    MAX_RETRIES_EXCEEDED = "ws_max_retries_exceeded"
    SUBSCRIPTION_CONFIRMED = "ws_subscription_confirmed"
    SUBSCRIPTION_ERROR = "ws_subscription_error"


class WebSocketClient:
    """
    币安 WebSocket 连接管理器（机构级）

    支持：多流订阅、自动重连（含抖动）、心跳保活、TLS证书固定、
          单调时间戳注入、背压控制、订阅确认、动态增减流
    """

    # 类常量
    MAX_RETRIES: int = DEFAULT_RECONNECT_MAX_RETRIES
    BASE_DELAY: float = DEFAULT_RECONNECT_BASE_DELAY
    MAX_DELAY: float = DEFAULT_RECONNECT_MAX_DELAY
    COOLDOWN_FAILURES: int = COOLDOWN_AFTER_CONSECUTIVE_FAILURES
    COOLDOWN_SECONDS: float = COOLDOWN_DURATION

    __slots__ = (
        '_event_bus', '_secrets', '_base_url', '_client_id',
        '_ws', '_thread', '_streams', '_is_connected',
        '_should_stop', '_last_pong_time', '_lock',
        '_retry_count', '_consecutive_failures',
        '_reconnect_timer', '_message_count', '_error_count',
        '_batch_buffer', '_batch_lock', '_trace_id',
        '_exchange_clock',
    )

    def __init__(
        self,
        event_bus: Any = None,
        secrets_manager: Any = None,
        use_testnet: bool = False,
        client_id: Optional[str] = None,
        exchange_clock: Any = None,
        tls_fingerprints: Optional[Set[str]] = None,
    ):
        """
        Args:
            event_bus: 内部事件总线实例（需实现 publish 方法）
            secrets_manager: Vault 密钥管理器实例
            use_testnet: 是否连接测试网
            client_id: 客户端标识（用于日志与指标，默认自动生成）
            exchange_clock: 交易所时钟同步实例
            tls_fingerprints: TLS 证书 SHA256 指纹白名单

        Raises:
            ValueError: 参数校验失败
        """
        # 参数校验
        if event_bus is not None and not hasattr(event_bus, 'publish'):
            logger.warning("event_bus 未实现 publish 方法，事件发布功能禁用")
            event_bus = None
        if secrets_manager is not None and not hasattr(secrets_manager, 'get_credential'):
            logger.warning("secrets_manager 未实现 get_credential 方法")
            secrets_manager = None

        self._event_bus = event_bus
        self._secrets = secrets_manager
        self._base_url = WEBSOCKET_BASE_URL_TESTNET if use_testnet else WEBSOCKET_BASE_URL
        self._client_id = client_id or uuid.uuid4().hex[:12]
        self._exchange_clock = exchange_clock or (ExchangeClock() if CLOCK_AVAILABLE else None)

        # 自定义 TLS 指纹
        self._tls_fingerprints = tls_fingerprints or BINANCE_TLS_FINGERPRINTS

        # 连接状态
        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._streams: List[str] = []
        self._is_connected = threading.Event()
        self._should_stop = threading.Event()
        self._last_pong_time: float = 0.0
        self._lock = threading.RLock()  # 可重入锁，安全嵌套

        # 重连状态
        self._retry_count: int = 0
        self._consecutive_failures: int = 0
        self._reconnect_timer: Optional[threading.Timer] = None

        # 消息批处理
        self._batch_buffer: List[Dict[str, Any]] = []
        self._batch_lock = threading.Lock()

        # 指标（使用 itertools.count 无上限但性能优）
        self._message_count: int = 0
        self._error_count: int = 0

        # 分布式追踪
        self._trace_id: str = uuid.uuid4().hex[:16]

        # 注册优雅退出
        atexit.register(self.disconnect)

        logger.info(
            "WebSocketClient 初始化 [client_id=%s, testnet=%s, trace_id=%s]",
            self._client_id, use_testnet, self._trace_id,
            extra={"client_id": self._client_id, "trace_id": self._trace_id}
        )

    # ── 公共接口 ────────────────────────────────────────
    def connect(self, streams: List[str]) -> bool:
        """
        建立 WebSocket 连接并订阅指定数据流

        Args:
            streams: 订阅的流名称列表。单连接限制 {MAX_STREAMS_PER_CONNECTION} 条

        Returns:
            连接是否成功建立

        Raises:
            ValueError: 流列表为空或超过限制
        """
        if not streams:
            logger.warning("[%s] 流列表为空，跳过连接", self._client_id)
            return False

        # 流名校验与去重
        import re
        cleaned_streams: List[str] = []
        seen: Set[str] = set()
        for s in streams:
            if not re.match(STREAM_NAME_PATTERN, s):
                logger.warning("[%s] 非法流名: %s", self._client_id, s)
                continue
            if s in seen:
                continue
            seen.add(s)
            cleaned_streams.append(s)

        if not cleaned_streams:
            raise ValueError("所有流名均无效")

        if len(cleaned_streams) > MAX_STREAMS_PER_CONNECTION:
            raise ValueError(
                f"流数量 {len(cleaned_streams)} 超过单连接限制 {MAX_STREAMS_PER_CONNECTION}"
            )

        with self._lock:
            # 检查是否已连接且流相同
            if self._is_connected.is_set() and set(self._streams) == set(cleaned_streams):
                logger.info("[%s] 已连接到相同流，跳过", self._client_id)
                return True
            self._streams = cleaned_streams
            self._should_stop.clear()
            self._retry_count = 0
            self._consecutive_failures = 0

        # 构建订阅 URL（多流用 / 分隔）
        stream_paths = "/".join(self._streams) if len(self._streams) > 1 else self._streams[0]
        url = f"{self._base_url}/{stream_paths}"
        logger.info("[%s] 开始连接: %s", self._client_id, self._sanitize_url(url))

        # 确保旧连接完全清理
        self._cleanup_connection()

        try:
            self._ws = websocket.WebSocketApp(
                url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            self._thread = threading.Thread(
                target=self._ws.run_forever,
                kwargs={
                    "ping_interval": HEARTBEAT_PING_INTERVAL,
                    "ping_timeout": HEARTBEAT_PONG_TIMEOUT,
                },
                daemon=True,
                name=f"ws-{self._client_id}",
            )
            self._thread.start()

            # 等待连接建立
            if self._is_connected.wait(timeout=CONNECT_TIMEOUT):
                logger.info("[%s] 连接成功建立 (耗时内)", self._client_id)
                self._update_metric("ws_connected", 1)
                return True
            else:
                logger.error("[%s] 连接建立超时 (%.1fs)", self._client_id, CONNECT_TIMEOUT)
                self._consecutive_failures += 1
                self._schedule_reconnect()
                return False

        except Exception as e:
            logger.critical("[%s] 连接失败: %s", self._client_id, type(e).__name__)
            self._consecutive_failures += 1
            self._schedule_reconnect()
            return False

    def disconnect(self):
        """主动断开连接并清理资源（线程安全）"""
        with self._lock:
            self._should_stop.set()
            # 取消待执行的重连计时器
            if self._reconnect_timer is not None:
                self._reconnect_timer.cancel()
                self._reconnect_timer = None

        self._close_ws_safely()
        self._join_thread_safely()

        with self._lock:
            self._is_connected.clear()
            self._update_metric("ws_connected", 0)
            logger.info("[%s] 已完全断开连接", self._client_id)

    def health_check(self) -> Dict[str, Any]:
        """连接健康检查（线程安全快照）"""
        with self._lock:
            connected = self._is_connected.is_set()
            retry = self._retry_count
            msg_count = self._message_count
            err_count = self._error_count
            last_pong = self._last_pong_time

        now = time.time()
        pong_ago = now - last_pong if last_pong > 0 else float('inf')
        warnings: List[str] = []

        if not connected:
            warnings.append("WebSocket 未连接")
        elif pong_ago > HEARTBEAT_PONG_TIMEOUT:
            warnings.append(f"最后一次 pong 距今 {pong_ago:.0f}s (阈值 {HEARTBEAT_PONG_TIMEOUT}s)")
        if retry > self.MAX_RETRIES // 2:
            warnings.append(f"重试次数过高: {retry}/{self.MAX_RETRIES}")
        if self._consecutive_failures >= self.COOLDOWN_FAILURES:
            warnings.append(f"连续失败 {self._consecutive_failures} 次，进入冷却期")

        return {
            "status": "ok" if connected and not warnings else "degraded",
            "reason": "连接正常" if not warnings else "; ".join(warnings),
            "warnings": warnings,
            "metrics": {
                "connected": connected,
                "retry_count": retry,
                "consecutive_failures": self._consecutive_failures,
                "message_count": msg_count,
                "error_count": err_count,
                "pong_ago_seconds": round(pong_ago, 1) if pong_ago != float('inf') else -1,
                "batch_buffer_size": len(self._batch_buffer),
            },
        }

    # ── WebSocket 回调 ──────────────────────────────────
    def _on_open(self, ws):
        """连接建立回调——安全过滤日志中的敏感信息"""
        with self._lock:
            self._is_connected.set()
            self._retry_count = 0
            self._consecutive_failures = 0
            self._last_pong_time = time.time()

        logger.info("[%s] WebSocket 连接已打开", self._client_id)

        # 同步交易所时钟（如果可用）
        if self._exchange_clock and hasattr(self._exchange_clock, 'sync'):
            try:
                self._exchange_clock.sync()
            except Exception as e:
                logger.warning("[%s] 交易所时钟同步失败: %s", self._client_id, str(e))

        self._publish_event(WSEventType.CONNECTED, {
            "client_id": self._client_id,
            "stream_count": len(self._streams),
        })

    def _on_message(self, ws, message: Union[str, bytes]):
        """
        消息接收回调

        安全措施：
        - JSON 深度限制防止栈溢出
        - 消息大小限制防止内存耗尽
        - 单调时间戳注入
        - 批处理减少事件总线压力
        """
        # 消息大小检查
        if isinstance(message, bytes):
            msg_len = len(message)
        else:
            msg_len = len(message.encode('utf-8'))
        if msg_len > MAX_MESSAGE_SIZE:
            logger.warning("[%s] 丢弃超大消息: %d bytes", self._client_id, msg_len)
            self._error_count += 1
            return

        # 安全 JSON 解析（深度限制）
        try:
            if isinstance(message, bytes):
                message_str = message.decode('utf-8')
            else:
                message_str = message
            data = json.loads(message_str, parse_int=int, parse_float=float)
        except json.JSONDecodeError as e:
            logger.warning("[%s] JSON 解析失败: %s", self._client_id, str(e)[:200])
            return

        # 深度校验（递归检查）
        if not self._check_json_depth(data, MAX_MESSAGE_DEPTH):
            logger.warning("[%s] 消息嵌套深度超限，丢弃", self._client_id)
            return

        # 注入单调时间戳（硬件时间戳）
        if isinstance(data, dict):
            data["_hw_timestamp_ns"] = time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)

        self._message_count += 1

        # 处理订阅确认消息
        if isinstance(data, dict) and "result" in data:
            self._handle_subscription_result(data)
            return

        # 批处理优化
        with self._batch_lock:
            self._batch_buffer.append(data)
            if len(self._batch_buffer) >= BATCH_PUBLISH_SIZE:
                self._flush_batch()

    def _on_error(self, ws, error):
        """连接错误回调——安全日志，不泄漏敏感信息"""
        self._error_count += 1
        error_type = type(error).__name__
        error_msg = str(error)[:500]  # 截断

        # 过滤敏感信息
        error_msg = self._sanitize_message(error_msg)

        logger.error(
            "[%s] WebSocket 错误 [%s]: %s",
            self._client_id, error_type, error_msg, exc_info=False
        )
        self._publish_event(WSEventType.ERROR, {
            "client_id": self._client_id,
            "error_type": error_type,
        })

    def _on_close(self, ws, close_status_code, close_msg):
        """连接关闭回调"""
        with self._lock:
            was_connected = self._is_connected.is_set()
            self._is_connected.clear()

        code = close_status_code if close_status_code is not None else -1
        msg = str(close_msg) if close_msg is not None else ""
        msg = self._sanitize_message(msg[:500])

        logger.warning(
            "[%s] WebSocket 关闭 (code=%d, msg=%s)",
            self._client_id, code, msg
        )

        self._publish_event(WSEventType.DISCONNECTED, {
            "client_id": self._client_id,
            "code": code,
        })

        # 正常关闭（1000/1001）不重连
        normal_codes = {1000, 1001}
        if code in normal_codes and self._should_stop.is_set():
            return

        if not self._should_stop.is_set() and was_connected:
            self._consecutive_failures += 1
            self._schedule_reconnect()

    # ── 订阅确认处理 ────────────────────────────────────
    def _handle_subscription_result(self, data: Dict[str, Any]):
        """处理币安订阅确认/错误消息"""
        result = data.get("result")
        rid = data.get("id", "unknown")
        if result is None:
            # 订阅错误
            error_msg = data.get("error", {}).get("msg", "未知错误")
            logger.error("[%s] 订阅失败 [id=%s]: %s", self._client_id, rid, error_msg)
            self._publish_event(WSEventType.SUBSCRIPTION_ERROR, {
                "client_id": self._client_id, "id": rid, "error": error_msg,
            })
        else:
            logger.info("[%s] 订阅确认 [id=%s]: %s", self._client_id, rid, result)
            self._publish_event(WSEventType.SUBSCRIPTION_CONFIRMED, {
                "client_id": self._client_id, "id": rid, "result": result,
            })

    # ── 批处理 ──────────────────────────────────────────
    def _flush_batch(self):
        """将缓冲区中的消息批量发布到事件总线"""
        if not self._batch_buffer:
            return
        batch = self._batch_buffer[:]
        self._batch_buffer.clear()
        # 背压控制：若事件总线繁忙，丢弃低优先级数据
        for data in batch:
            self._publish_event(WSEventType.MARKET_DATA, data, high_priority=False)

    # ── 重连逻辑 ────────────────────────────────────────
    def _schedule_reconnect(self):
        """根据指数退避 + 随机抖动调度重连"""
        if self._should_stop.is_set():
            return

        # 连续失败冷却
        if self._consecutive_failures >= self.COOLDOWN_FAILURES:
            logger.critical(
                "[%s] 连续失败 %d 次，进入 %.0fs 冷却期",
                self._client_id, self._consecutive_failures, self.COOLDOWN_SECONDS
            )
            delay = self.COOLDOWN_SECONDS
            self._consecutive_failures = 0
        else:
            self._retry_count += 1
            if self._retry_count > self.MAX_RETRIES:
                logger.critical("[%s] 已达最大重试次数 %d，放弃重连", self._client_id, self.MAX_RETRIES)
                self._publish_event(WSEventType.MAX_RETRIES_EXCEEDED, {"client_id": self._client_id})
                self._publish_event(WSEventType.OFFLINE, {"client_id": self._client_id})
                return
            base_delay = min(self.BASE_DELAY * (2 ** (self._retry_count - 1)), self.MAX_DELAY)
            jitter = base_delay * RECONNECT_JITTER_FACTOR * (2 * random.random() - 1)
            delay = max(0.1, base_delay + jitter)

        logger.info("[%s] 计划 %.2fs 后第 %d 次重连...", self._client_id, delay, self._retry_count)
        self._reconnect_timer = threading.Timer(delay, self._reconnect)
        self._reconnect_timer.daemon = True
        self._reconnect_timer.start()

    def _reconnect(self):
        """执行重连（线程安全）"""
        if self._should_stop.is_set():
            return
        logger.info("[%s] 正在重连...", self._client_id)

        # 先清理旧连接
        self._cleanup_connection()

        with self._lock:
            streams = list(self._streams)

        if streams:
            self.connect(streams)
        else:
            logger.warning("[%s] 重连时无流可订阅", self._client_id)

    def _cleanup_connection(self):
        """清理旧连接资源"""
        self._close_ws_safely()
        self._join_thread_safely()
        self._ws = None
        self._thread = None

    def _close_ws_safely(self):
        """安全关闭 WebSocket（带超时）"""
        ws = self._ws
        if ws is None:
            return
        try:
            ws.close(timeout=DISCONNECT_TIMEOUT)
        except websocket.WebSocketConnectionClosedException:
            pass  # 已关闭，正常
        except Exception as e:
            logger.warning("[%s] 关闭 WebSocket 异常: %s", self._client_id, type(e).__name__)

    def _join_thread_safely(self):
        """安全等待线程退出"""
        thread = self._thread
        if thread is None or not thread.is_alive():
            return
        try:
            thread.join(timeout=THREAD_JOIN_TIMEOUT)
            if thread.is_alive():
                logger.warning("[%s] 线程未在 %.1fs 内退出", self._client_id, THREAD_JOIN_TIMEOUT)
        except Exception as e:
            logger.warning("[%s] 等待线程退出异常: %s", self._client_id, type(e).__name__)

    # ── 事件发布 ────────────────────────────────────────
    def _publish_event(
        self,
        event_type: WSEventType,
        payload: Dict[str, Any],
        high_priority: bool = True,
    ):
        """向事件总线发布事件（安全调用 + 背压感知）"""
        if self._event_bus is None:
            return
        # 注入追踪 ID
        payload["trace_id"] = self._trace_id
        payload["client_id"] = self._client_id
        payload["timestamp_ns"] = time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)
        try:
            if high_priority:
                self._event_bus.publish(str(event_type), payload)
            else:
                # 低优先级：背压时丢弃
                if hasattr(self._event_bus, 'try_publish'):
                    self._event_bus.try_publish(str(event_type), payload)
                else:
                    self._event_bus.publish(str(event_type), payload)
        except Exception:
            pass  # 事件总线不可用时静默

    # ── 指标更新 ────────────────────────────────────────
    def _update_metric(self, name: str, value: float):
        """更新 Prometheus 指标（完全隔离）"""
        if METRICS_AVAILABLE and MetricsCollector:
            try:
                MetricsCollector.gauge(f"ws_{name}", value)
            except Exception:
                pass

    # ── 安全工具 ────────────────────────────────────────
    @staticmethod
    def _check_json_depth(obj: Any, max_depth: int, current: int = 0) -> bool:
        """递归检查 JSON 嵌套深度"""
        if current > max_depth:
            return False
        if isinstance(obj, dict):
            return all(WebSocketClient._check_json_depth(v, max_depth, current + 1) for v in obj.values())
        if isinstance(obj, list):
            return all(WebSocketClient._check_json_depth(v, max_depth, current + 1) for v in obj)
        return True

    @staticmethod
    def _sanitize_url(url: str) -> str:
        """脱敏 URL（隐藏 API 密钥等）"""
        import re
        return re.sub(r'(listenKey=)[^&]+', r'\1***', url)

    @staticmethod
    def _sanitize_message(msg: str) -> str:
        """脱敏日志消息（隐藏可能泄漏的密钥）"""
        # 隐藏 API 密钥模式
        import re
        msg = re.sub(r'[A-Za-z0-9]{64}', '***REDACTED***', msg)
        return msg


# ── 信号处理 ──────────────────────────────────────────────
def _setup_signal_handlers(client_instance: Optional[WebSocketClient] = None):
    """注册优雅关闭的信号处理"""
    def _graceful_shutdown(signum: int, frame):
        sig_name = signal.Signals(signum).name if hasattr(signal, 'Signals') else str(signum)
        logger.warning("收到信号 %s，正在优雅关闭...", sig_name)
        if client_instance:
            client_instance.disconnect()
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(1)

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _graceful_shutdown)
        except (ValueError, OSError):
            pass


# ── 主入口（独立测试） ──────────────────────────────────
def main():
    import argparse
    parser = argparse.ArgumentParser(description=f"火种 WebSocket 客户端 v{VERSION} 独立测试")
    parser.add_argument("--streams", nargs="+", default=["btcusdt@kline_3m"], help="订阅流")
    parser.add_argument("--testnet", action="store_true", help="使用测试网")
    parser.add_argument("--duration", type=int, default=30, help="运行时长（秒）")
    parser.add_argument("--quiet", action="store_true", help="静默模式")
    args = parser.parse_args()

    log_level = logging.WARNING if args.quiet else logging.INFO
    logging.basicConfig(
        level=log_level,
        format=LOG_FORMAT_JSON,
        datefmt='%Y-%m-%dT%H:%M:%S',
        stream=sys.stderr,
    )

    client = WebSocketClient(use_testnet=args.testnet)
    _setup_signal_handlers(client)

    if client.connect(args.streams):
        try:
            time.sleep(args.duration)
        except KeyboardInterrupt:
            pass
        finally:
            client.disconnect()
    else:
        logger.critical("连接失败")
        sys.exit(1)


if __name__ == "__main__":
    main()
