#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
# ------------------------------------------------------------------
# 火种系统 · 市场数据网关入站处理器 (MarketDataIngestor)
# 符合 Citadel/Renaissance 级微秒数据接入规范
# ------------------------------------------------------------------

from __future__ import annotations

import json
import logging
import math
import signal
import sys
import time
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from decimal import Decimal, InvalidOperation
from typing import (Any, Callable, Dict, Final, List, Optional, Set, Tuple,
                    Type, Union)

# 安全导入可选依赖
try:
    from gateway.ws_client import WsClient, ConnectionState as WsConnState
except ImportError:
    WsClient = None
    WsConnState = None
try:
    from core.event_bus import EventBus, EventTypes
except ImportError:
    EventBus = None
    EventTypes = None
try:
    from core.data_quality import DataQuality, DataQualityReport
except ImportError:
    DataQuality = None
    DataQualityReport = None
try:
    from core.clock import Clock
except ImportError:
    Clock = None
try:
    from core.metrics import MetricsCollector
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    MetricsCollector = None

# 平台特定检查
try:
    from threading import Lock as ThreadingLock
except ImportError:
    ThreadingLock = None

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 数据结构 (不可变、类型安全)
# ──────────────────────────────────────────────


@dataclass(frozen=True)
class TickEvent:
    """标准化逐笔成交事件"""
    event_type: str = field(default="tick", init=False)
    symbol: str = ""
    price: Decimal = Decimal("0")
    quantity: Decimal = Decimal("0")
    timestamp_ms: int = 0
    trade_id: int = 0
    is_buyer_maker: bool = False
    local_timestamp_ns: int = 0


@dataclass(frozen=True)
class DepthEvent:
    """标准化深度快照事件 (深度数据使用嵌套不可变元组)"""
    event_type: str = field(default="depth", init=False)
    symbol: str = ""
    last_update_id: int = 0
    first_update_id: int = 0
    # 冻结列表的实现：存储为元组，元组内为(float, float)对的Decimal
    bids: Tuple[Tuple[Decimal, Decimal], ...] = ()
    asks: Tuple[Tuple[Decimal, Decimal], ...] = ()
    timestamp_ms: int = 0
    local_timestamp_ns: int = 0


@dataclass(frozen=True)
class KlineEvent:
    """标准化K线事件"""
    event_type: str = field(default="kline", init=False)
    symbol: str = ""
    interval: str = "3m"
    open: Decimal = Decimal("0")
    high: Decimal = Decimal("0")
    low: Decimal = Decimal("0")
    close: Decimal = Decimal("0")
    volume: Decimal = Decimal("0")
    quote_volume: Decimal = Decimal("0")
    taker_buy_volume: Decimal = Decimal("0")
    trades_count: int = 0
    is_closed: bool = False
    start_time_ms: int = 0
    end_time_ms: int = 0
    local_timestamp_ns: int = 0


# ──────────────────────────────────────────────
# 常量与类型别名
# ──────────────────────────────────────────────

class _Const:
    """不可变常量容器"""
    RECONNECT_BASE_MS: Final = 500
    MAX_RECONNECT_WAIT_MS: Final = 30_000
    HEARTBEAT_TIMEOUT_MS: Final = 3_000
    MAX_EVENT_BACKLOG: Final = 2000
    DROP_RATE_HIGH_THRESHOLD: Final = 0.02
    BACKOFF_MULTIPLIER: Final = 2.0
    MAX_SUBSCRIPTION_RETRIES: Final = 5
    METRICS_PREFIX: Final = "spark_market_data"
    DECIMAL_PREC: Final = 8
    GRACE_SHUTDOWN_SEC: Final = 2.0

# 类型别名
StreamName = str
SubscriptionHandle = Any


# ──────────────────────────────────────────────
# 主类
# ──────────────────────────────────────────────

class MarketDataIngestor:
    """
    超机构级市场数据入站处理器
    - 完全异步非阻塞（依赖外部事件循环）
    - 数值精度无损失（Decimal）
    - 背压保护与优雅降级
    - 完整可观测性（指标、结构化日志、心跳）
    """

    CONST = _Const

    def __init__(self,
                 instruments: List[str],
                 event_bus: Optional[EventBus] = None,
                 ws_client: Optional[WsClient] = None,
                 data_quality: Optional[DataQuality] = None,
                 clock: Optional[Clock] = None):
        # ── 参数校验 ──
        if not isinstance(instruments, list) or not instruments:
            raise ValueError("instruments 必须为非空列表")
        sanitized: List[str] = []
        for s in instruments:
            if not isinstance(s, str) or not s.isalnum():
                raise ValueError(f"非法交易品种名称: {s!r}")
            sanitized.append(s.lower())
        self._instruments = sanitized

        # ── 依赖注入 ──
        self._event_bus = event_bus or EventBus()
        self._ws = ws_client or WsClient()
        self._dq = data_quality or DataQuality()
        self._clock = clock or Clock()

        # ── 内部状态 (所有可变状态加锁保护) ──
        self._lock = ThreadingLock() if ThreadingLock else _DummyLock()
        self._active_streams: Dict[StreamName, SubscriptionHandle] = {}
        self._running = False
        self._stats = self._init_stats()
        self._last_heartbeat_ns: int = 0
        self._sub_attempts: Dict[StreamName, int] = defaultdict(int)
        self._event_types_available = self._check_event_types()

    # ── 公共 API ──────────────────────────────

    def start(self) -> None:
        """启动数据流，非阻塞；所有订阅在后台连接建立后自动开始"""
        with self._lock:
            if self._running:
                logger.warning("ingestor 已在运行")
                return
            self._running = True
        self._install_signal_handlers()
        logger.info("启动市场数据入站 (品种=%s)", self._instruments)
        self._subscribe_all()

    def stop(self) -> None:
        """优雅关闭，释放所有资源"""
        with self._lock:
            if not self._running:
                return
            self._running = False
        logger.info("停止 ingestor，清理 %d 个流", len(self._active_streams))
        # 先取消所有订阅
        for name in list(self._active_streams):
            self._safe_unsubscribe(name)
        # 关闭WebSocket
        try:
            self._ws.close()
        except Exception as e:
            logger.error("关闭 WebSocket 异常: %s", e)
        # 等待事件总线排空
        time.sleep(0.3)
        logger.info("ingestor 已停止")

    def get_stats(self) -> Dict[str, Any]:
        """获取统计快照"""
        with self._lock:
            stats = self._stats.copy()
        stats["active_streams"] = len(self._active_streams)
        stats["heartbeat_ago_ms"] = self._heartbeat_ago_ms()
        return stats

    def health_check(self) -> Dict[str, Any]:
        """模块健康检查，返回标准结构"""
        warnings: List[str] = []
        # 心跳检查
        hb_ago = self._heartbeat_ago_ms()
        if hb_ago > self.CONST.HEARTBEAT_TIMEOUT_MS:
            warnings.append(f"心跳超时 ({hb_ago:.0f}ms)")
        # 流检查
        if not self._active_streams:
            warnings.append("无活跃数据流")
        # 丢包率
        total_recv = self._stats.get("messages_received", 0)
        if total_recv > 100:
            drop_rate = self._stats["messages_dropped"] / total_recv
            if drop_rate > self.CONST.DROP_RATE_HIGH_THRESHOLD:
                warnings.append(f"高丢弃率 {drop_rate:.2%}")
        # WebSocket 连接状态
        ws_state = self._get_ws_state()
        if ws_state != "connected" and self._running:
            warnings.append(f"WebSocket 状态异常: {ws_state}")
        return {
            "status": "degraded" if warnings else "ok",
            "reason": f"流数:{len(self._active_streams)}, 已处理:{self._stats['messages_processed']}",
            "warnings": warnings,
            "stats": self.get_stats()
        }

    # ── 内部方法 ──────────────────────────────

    def _init_stats(self) -> Dict[str, int]:
        return {
            "messages_received": 0,
            "messages_processed": 0,
            "messages_dropped": 0,
            "data_quality_rejects": 0,
            "backpressure_drops": 0,
            "reconnect_attempts": 0,
        }

    def _subscribe_all(self) -> None:
        for sym in self._instruments:
            self._safe_subscribe(f"{sym}@aggTrade", self._on_trade)
            self._safe_subscribe(f"{sym}@depth@100ms", self._on_depth)
            self._safe_subscribe(f"{sym}@kline_3m", self._on_kline)

    def _safe_subscribe(self, stream: StreamName, callback: Callable) -> None:
        with self._lock:
            if stream in self._active_streams:
                return
            if not self._running:
                logger.warning("已停止，拒绝订阅 %s", stream)
                return
            attempts = self._sub_attempts.get(stream, 0)
            if attempts >= self.CONST.MAX_SUBSCRIPTION_RETRIES:
                logger.error("流 %s 已达最大重试次数 %d", stream, attempts)
                return
        try:
            handle = self._ws.subscribe(stream, callback)
            if handle is None:
                raise RuntimeError("subscribe 返回空句柄")
            with self._lock:
                self._active_streams[stream] = handle
                self._sub_attempts[stream] = 0
            logger.info("订阅成功: %s", stream)
        except Exception as e:
            with self._lock:
                self._sub_attempts[stream] = attempts + 1
                self._stats["reconnect_attempts"] += 1
            logger.error("订阅失败 %s: %s", stream, e)
            if self._running:
                self._schedule_reconnect(stream, callback, attempts + 1)

    def _safe_unsubscribe(self, stream: StreamName) -> None:
        with self._lock:
            handle = self._active_streams.pop(stream, None)
            self._sub_attempts.pop(stream, None)
        if handle:
            try:
                self._ws.unsubscribe(handle)
            except Exception as e:
                logger.warning("取消订阅 %s 异常: %s", stream, e)

    def _schedule_reconnect(self, stream: StreamName, callback: Callable, attempt: int) -> None:
        # 生产环境应使用 reactor.call_later，此处为示例使用线程
        import threading
        delay = min(self.CONST.RECONNECT_BASE_MS * (self.CONST.BACKOFF_MULTIPLIER ** attempt),
                    self.CONST.MAX_RECONNECT_WAIT_MS) / 1000.0
        logger.info("流 %s 将在 %.1fs 后重连 (第%d次)", stream, delay, attempt)

        def _reconnect():
            time.sleep(delay)
            if self._running:
                self._safe_subscribe(stream, callback)

        t = threading.Thread(target=_reconnect, daemon=True, name=f"reconnect-{stream}")
        t.start()

    # ── 消息回调 ──────────────────────────────

    def _on_trade(self, raw_msg: str) -> None:
        self._inc_stat("messages_received")
        event = self._parse_and_convert(raw_msg, self._std_trade, TickEvent)
        if event:
            self._publish(self._event_types_available.get("tick", "tick"), event)
            self._heartbeat()

    def _on_depth(self, raw_msg: str) -> None:
        self._inc_stat("messages_received")
        if self._backpressure():
            self._inc_stat("backpressure_drops")
            return
        event = self._parse_and_convert(raw_msg, self._std_depth, DepthEvent)
        if event:
            self._publish(self._event_types_available.get("depth", "depth"), event)
            self._heartbeat()

    def _on_kline(self, raw_msg: str) -> None:
        self._inc_stat("messages_received")
        event = self._parse_and_convert(raw_msg, self._std_kline, KlineEvent)
        if event:
            self._publish(self._event_types_available.get("kline_close", "kline"), event)
            self._heartbeat()

    # ── 解析与标准化 ───────────────────────────

    def _parse_and_convert(self, raw: str, converter: Callable, expected_type: Type) -> Optional[Any]:
        if not isinstance(raw, str):
            return None
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            logger.debug("JSON解析失败")
            self._inc_stat("messages_dropped")
            return None

        # 数据质量验证
        dq_result = self._dq.validate(msg)
        if not dq_result.is_valid:
            self._inc_stat("data_quality_rejects")
            logger.debug("数据质量拒绝: %s", dq_result.reason)
            return None

        try:
            event = converter(msg)
            if not isinstance(event, expected_type):
                raise TypeError(f"类型错误: 期望 {expected_type}, 实际 {type(event)}")
            # 字段合法性校验
            if not self._validate_event(event):
                self._inc_stat("messages_dropped")
                return None
            self._inc_stat("messages_processed")
            return event
        except Exception as e:
            logger.critical("标准化异常: %s, 原始: %.100s", e, raw, exc_info=True)
            self._inc_stat("messages_dropped")
            return None

    def _validate_event(self, event) -> bool:
        try:
            if hasattr(event, 'price') and event.price < 0:
                return False
            if hasattr(event, 'quantity') and event.quantity < 0:
                return False
            if hasattr(event, 'volume') and event.volume < 0:
                return False
        except TypeError:
            return False
        return True

    # ── 标准化实现 ─────────────────────────────

    def _std_trade(self, raw: Dict) -> TickEvent:
        s = raw.get("s")
        if not isinstance(s, str):
            s = ""
        return TickEvent(
            symbol=s.lower(),
            price=self._dec(raw.get("p", "0")),
            quantity=self._dec(raw.get("q", "0")),
            timestamp_ms=int(raw.get("T", 0)),
            trade_id=int(raw.get("a", 0)),
            is_buyer_maker=bool(raw.get("m", False)),
            local_timestamp_ns=self._now_ns(),
        )

    def _std_depth(self, raw: Dict) -> DepthEvent:
        def parse_levels(data):
            if not isinstance(data, list):
                return ()
            levels = []
            for entry in data:
                if len(entry) >= 2:
                    levels.append((self._dec(entry[0]), self._dec(entry[1])))
            return tuple(levels)

        s = raw.get("s", "")
        if not isinstance(s, str):
            s = ""
        return DepthEvent(
            symbol=s.lower(),
            last_update_id=int(raw.get("u", 0)),
            first_update_id=int(raw.get("U", 0)),
            bids=parse_levels(raw.get("b", [])),
            asks=parse_levels(raw.get("a", [])),
            timestamp_ms=int(raw.get("E", 0)),
            local_timestamp_ns=self._now_ns(),
        )

    def _std_kline(self, raw: Dict) -> KlineEvent:
        k = raw.get("k", {}) if isinstance(raw.get("k"), dict) else {}
        s = k.get("s", "")
        if not isinstance(s, str):
            s = ""
        return KlineEvent(
            symbol=s.lower(),
            interval=k.get("i", "3m"),
            open=self._dec(k.get("o", "0")),
            high=self._dec(k.get("h", "0")),
            low=self._dec(k.get("l", "0")),
            close=self._dec(k.get("c", "0")),
            volume=self._dec(k.get("v", "0")),
            quote_volume=self._dec(k.get("q", "0")),
            taker_buy_volume=self._dec(k.get("V", "0")),
            trades_count=int(k.get("n", 0)),
            is_closed=bool(k.get("x", False)),
            start_time_ms=int(k.get("t", 0)),
            end_time_ms=int(k.get("T", 0)),
            local_timestamp_ns=self._now_ns(),
        )

    # ── 数值安全 ───────────────────────────────

    def _dec(self, value: Any) -> Decimal:
        """安全转换为Decimal，拒绝NaN/Inf"""
        if isinstance(value, Decimal):
            return self._truncate(value)
        try:
            s = str(value).strip()
            # 拒绝特殊浮点值
            if s.lower() in ("nan", "inf", "-inf", "infinity", "-infinity"):
                logger.warning("拒绝非法数值: %s", s)
                return Decimal("0")
            d = Decimal(s)
        except (InvalidOperation, ValueError, TypeError):
            logger.debug("Decimal转换失败: %s", value)
            return Decimal("0")
        return self._truncate(d)

    def _truncate(self, d: Decimal) -> Decimal:
        """按配置精度截断"""
        return d.quantize(Decimal('1e-{}'.format(self.CONST.DECIMAL_PREC)))

    # ── 事件发布 ───────────────────────────────

    def _publish(self, event_type: str, event: Any) -> None:
        try:
            # 为 dataclass 提供安全的序列化 (Decimal->str 等)
            payload = self._serialize_event(event)
            self._event_bus.publish(event_type, payload)
        except Exception as e:
            logger.error("发布失败 type=%s: %s", event_type, e)

    def _serialize_event(self, event: Any) -> Dict:
        """将不可变 dataclass 转为字典，Decimal 转为字符串以保持精度"""
        if hasattr(event, '__dataclass_fields__'):
            result = {}
            for field_name in event.__dataclass_fields__:
                value = getattr(event, field_name)
                if isinstance(value, Decimal):
                    result[field_name] = str(value)
                elif isinstance(value, tuple):
                    # 递归处理深度嵌套
                    result[field_name] = self._serialize_tuple(value)
                else:
                    result[field_name] = value
            return result
        return dict(event) if isinstance(event, dict) else {}

    def _serialize_tuple(self, tpl: Tuple) -> Tuple:
        return tuple(
            (str(a), str(b)) if isinstance(a, Decimal) and isinstance(b, Decimal)
            else self._serialize_tuple(item) if isinstance(item, tuple)
            else str(item)
            for item in tpl
        ) if isinstance(tpl, tuple) else ()

    # ── 背压检测 ───────────────────────────────

    def _backpressure(self) -> bool:
        try:
            backlog = self._event_bus.backlog_size()
            return backlog > self.CONST.MAX_EVENT_BACKLOG
        except AttributeError:
            return False

    # ── 线程安全统计 ───────────────────────────

    def _inc_stat(self, key: str) -> None:
        with self._lock:
            self._stats[key] = self._stats.get(key, 0) + 1

    def _heartbeat(self) -> None:
        self._last_heartbeat_ns = self._now_ns()

    def _heartbeat_ago_ms(self) -> float:
        if self._last_heartbeat_ns == 0:
            return -1.0
        return (self._now_ns() - self._last_heartbeat_ns) / 1e6

    def _now_ns(self) -> int:
        """获取纳秒时间戳，出错时返回0"""
        try:
            return self._clock.now_ns()
        except Exception:
            return int(time.time() * 1e9)

    def _get_ws_state(self) -> str:
        try:
            return self._ws.connection_state()
        except Exception:
            return "unknown"

    def _check_event_types(self) -> Dict[str, str]:
        """安全获取事件类型常量"""
        if EventTypes is None:
            return {}
        try:
            return {
                "tick": EventTypes.EVENT_TICK,
                "depth": EventTypes.EVENT_DEPTH,
                "kline_close": EventTypes.EVENT_KLINE_CLOSE,
            }
        except AttributeError:
            logger.warning("EventTypes 缺少必要事件常量")
            return {}

    def _install_signal_handlers(self) -> None:
        if sys.platform == "win32":
            return
        def _shutdown(sig, frame):
            logger.warning("收到信号 %d，关闭 ingestor", sig)
            self.stop()
        for s in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(s, _shutdown)
            except Exception:
                pass


# ── 辅助锁 ───────────────────────────────────

class _DummyLock:
    def __enter__(self): pass
    def __exit__(self, *args): pass
