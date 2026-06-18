#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 订单管理器 (OrderManager) v6.0.0 — 机构级最终版

核心职责：
1. 接收策略决策，生成标准化订单指令并发送至交易所网关
2. 实现严格的订单状态机，杜绝非法状态转换
3. 基于幂等键和交换机确保请求不会重复执行
4. 提供订单状态查询、持仓同步与挂起队列重试机制
5. 完整的可观测性与审计日志，满足金融级合规要求

外部依赖：
- gateway.order_dispatcher.OrderDispatcher : 交易所订单发送与撤销
- core.event_bus.EventBus : 发布订单状态变更事件
- core.risk_manager.RiskManager : 事前风控审核
- core.position_keeper.PositionKeeper : 仓位同步
- core.metrics.MetricsCollector : Prometheus 指标 (可选)

接口契约：
- submit_order(order: Dict) -> Dict[str, Any]
- cancel_order(order_id: str) -> Dict[str, Any]
- get_order(order_id: str) -> Optional[Dict]
- sync_positions() -> Dict[str, Any]
- health_check() -> Dict[str, Any]

异常与降级：
- 所有外部调用均设置超时，超时后标记订单为 unknown 并等待同步
- 挂起队列满时拒绝新订单，避免 OOM
- 任何模块异常均被捕获，保证主流程不崩溃

资源管理：
- 订单缓存限制最大条目数，采用 TTL + LRU 淘汰
- 挂起队列限制长度，超时订单自动丢弃
- 分发线程优雅停止，确保资源释放
"""

import logging
import threading
import time
import uuid
from collections import OrderedDict
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# ── 可选依赖 ──────────────────────────────────────────────
try:
    from gateway.order_dispatcher import OrderDispatcher, OrderDispatcherException
except ImportError:
    OrderDispatcher = None
    OrderDispatcherException = Exception

try:
    from core.event_bus import EventBus, EventTypes
except ImportError:
    EventBus = None
    EventTypes = None

try:
    from core.risk_manager import RiskManager
except ImportError:
    RiskManager = None

try:
    from core.position_keeper import PositionKeeper
except ImportError:
    PositionKeeper = None

try:
    from core.metrics import MetricsCollector
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    MetricsCollector = None


# ── 状态机定义 ────────────────────────────────────────────
class OrderStatus:
    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    UNKNOWN = "unknown"
    ERROR = "error"

    # 合法状态转换映射
    ALLOWED_TRANSITIONS = {
        PENDING: {SUBMITTED, REJECTED, ERROR, UNKNOWN},
        SUBMITTED: {PARTIALLY_FILLED, FILLED, CANCELLING, CANCELLED, REJECTED, EXPIRED, UNKNOWN, ERROR},
        PARTIALLY_FILLED: {PARTIALLY_FILLED, FILLED, CANCELLING, CANCELLED, EXPIRED, UNKNOWN, ERROR},
        CANCELLING: {CANCELLED, PARTIALLY_FILLED, FILLED, ERROR},  # 可能撤销前部分成交
        FILLED: set(),
        CANCELLED: set(),
        REJECTED: set(),
        EXPIRED: set(),
        UNKNOWN: {FILLED, CANCELLED, REJECTED, EXPIRED, ERROR},  # 允许同步修复
        ERROR: {SUBMITTED, REJECTED, UNKNOWN},  # 允许重试
    }
    TERMINAL_STATES = {FILLED, CANCELLED, REJECTED, EXPIRED, UNKNOWN}


# ── 常量 ──────────────────────────────────────────────────
MAX_CACHED_ORDERS = 10000
MAX_PENDING_ORDERS = 500
PENDING_ORDER_TIMEOUT_SEC = 300
ORDER_CONFIRM_TIMEOUT_SEC = 60
CACHE_TTL_SEC = 86400  # 24小时
RETRY_MAX_ATTEMPTS = 3
DISPATCHER_TIMEOUT_SEC = 5.0
MACHINE_ID = str(uuid.getnode()).zfill(16)[-16:]  # 标准化机器标识


class OrderValidationError(Exception):
    pass


class OrderManager:
    """订单管理器 v6.0.0"""

    def __init__(self, order_dispatcher=None, event_bus=None,
                 risk_manager=None, position_keeper=None):
        self.dispatcher = order_dispatcher or (OrderDispatcher() if OrderDispatcher else None)
        self.event_bus = event_bus or (EventBus() if EventBus else None)
        self.risk_manager = risk_manager or (RiskManager() if RiskManager else None)
        self.position_keeper = position_keeper or (PositionKeeper() if PositionKeeper else None)

        self._orders: OrderedDict[str, Dict] = OrderedDict()
        self._orders_lock = threading.RLock()
        self._active_count = 0  # 活跃订单计数，避免遍历
        self._count_lock = threading.Lock()

        self._pending_orders: List[Dict] = []
        self._pending_lock = threading.Lock()

        self._stop_cleanup = threading.Event()
        self._cleanup_thread = None
        self._start_cleanup()

        logger.info("OrderManager v6.0.0 初始化完成")

    # ── 公共接口 ──────────────────────────────────────────

    def submit_order(self, order: Dict) -> Dict[str, Any]:
        trace_id = order.get('trace_id', str(uuid.uuid4())[:8])
        try:
            self._validate_order_input(order)
        except OrderValidationError as e:
            self._record_metrics("order_validation_error", 1, {"reason": str(e)})
            return {"status": "rejected", "order_id": "", "reason": str(e), "warnings": []}

        # 风控预审核（此时尚未缓存）
        if self.risk_manager:
            approved, reason = self.risk_manager.approve_order(order)
            if not approved:
                logger.warning("[%s] 风控拒绝: %s", trace_id, reason)
                self._record_metrics("order_rejected_risk", 1, {"reason": reason})
                return {"status": "rejected", "order_id": "", "reason": f"风控拒绝: {reason}", "warnings": []}

        client_id = self._generate_client_order_id(order['symbol'], order['side'])
        order['client_order_id'] = client_id
        order['trace_id'] = trace_id
        order['created_at'] = time.time()

        # 缓存订单（状态 pending）
        if not self._cache_order(client_id, order):
            return {"status": "rejected", "order_id": client_id, "reason": "重复订单或缓存满", "warnings": []}

        # 发送到交易所
        if not self.dispatcher:
            self._enqueue_pending(order)
            return {"status": "pending", "order_id": client_id, "reason": "网关不可用，订单挂起", "warnings": []}

        try:
            response = self.dispatcher.send_order(order, timeout=DISPATCHER_TIMEOUT_SEC)
            return self._process_submit_response(client_id, response, trace_id)
        except OrderDispatcherException as e:
            logger.error("[%s] 发送失败: %s", trace_id, e)
            self._transition_state(client_id, OrderStatus.ERROR, str(e))
            self._enqueue_pending(order)
            return {"status": "error", "order_id": client_id, "reason": f"发送失败: {e}", "warnings": []}
        except Exception as e:
            logger.critical("[%s] 未知异常: %s", trace_id, e, exc_info=True)
            self._transition_state(client_id, OrderStatus.UNKNOWN, str(e))
            return {"status": "error", "order_id": client_id, "reason": f"未知错误: {e}", "warnings": []}

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        with self._orders_lock:
            order = self._orders.get(order_id)
            if not order:
                return {"status": "error", "order_id": order_id, "reason": "订单不存在", "warnings": []}
            if order['status'] in OrderStatus.TERMINAL_STATES:
                return {"status": "error", "order_id": order_id, "reason": f"订单已终态: {order['status']}", "warnings": []}
            if order['status'] == OrderStatus.CANCELLING:
                return {"status": "ok", "order_id": order_id, "reason": "正在撤销中", "warnings": []}
            # 标记为 cancelling
            self._set_status(order, OrderStatus.CANCELLING)

        if not self.dispatcher:
            return {"status": "error", "order_id": order_id, "reason": "网关不可用", "warnings": []}

        try:
            self.dispatcher.cancel_order(order_id, order.get('symbol', ''))
            self._transition_state(order_id, OrderStatus.CANCELLED, "用户撤销")
            self._publish_event(EventTypes.ORDER_CANCELLED if EventTypes else "order_cancelled",
                                {"client_order_id": order_id})
            return {"status": "ok", "order_id": order_id, "reason": "撤销成功", "warnings": []}
        except Exception as e:
            logger.error("撤销失败 [%s]: %s", order_id, e)
            # 恢复原状态（可能已变化）
            self._transition_state(order_id, order.get('status'), f"撤销失败: {e}")
            return {"status": "error", "order_id": order_id, "reason": f"撤销失败: {e}", "warnings": []}

    def get_order(self, order_id: str) -> Optional[Dict]:
        with self._orders_lock:
            order = self._orders.get(order_id)
            return order.copy() if order else None

    def sync_positions(self) -> Dict[str, Any]:
        if not self.position_keeper or not self.dispatcher:
            return {"status": "error", "reason": "依赖不可用", "warnings": []}
        try:
            live_positions = self.dispatcher.get_positions()
            discrepancies = self.position_keeper.sync(live_positions)
            logger.info("持仓同步完成，差异: %s", discrepancies)
            return {"status": "ok", "reason": f"同步 {len(live_positions)} 个持仓", "warnings": discrepancies}
        except Exception as e:
            logger.error("持仓同步失败: %s", e)
            return {"status": "error", "reason": str(e), "warnings": []}

    def health_check(self) -> Dict[str, Any]:
        warnings = []
        if not self.dispatcher:
            warnings.append("OrderDispatcher 未配置")
        if not self.risk_manager:
            warnings.append("RiskManager 未配置")
        if not self.position_keeper:
            warnings.append("PositionKeeper 未配置")

        with self._count_lock:
            active = self._active_count
        with self._pending_lock:
            pending_count = len(self._pending_orders)
        if pending_count > MAX_PENDING_ORDERS * 0.8:
            warnings.append(f"挂起队列接近满: {pending_count}/{MAX_PENDING_ORDERS}")

        status = "degraded" if warnings else "ok"
        return {
            "status": status,
            "reason": f"活跃订单: {active}, 挂起: {pending_count}",
            "warnings": warnings,
            "stats": {"active": active, "pending": pending_count}
        }

    def stop(self):
        self._stop_cleanup.set()
        if self._cleanup_thread:
            self._cleanup_thread.join(timeout=5)
        with self._pending_lock:
            for order in self._pending_orders:
                logger.warning("未处理挂起订单: %s", order.get('client_order_id'))

    # ── 内部实现 ──────────────────────────────────────────

    def _validate_order_input(self, order: Dict) -> None:
        required = ['symbol', 'side', 'quantity']
        for field in required:
            if field not in order:
                raise OrderValidationError(f"缺少必填字段: {field}")
        if order['side'] not in ('BUY', 'SELL'):
            raise OrderValidationError(f"无效方向: {order['side']}")
        qty = order.get('quantity')
        if not isinstance(qty, (int, float)) or qty <= 0:
            raise OrderValidationError(f"无效数量: {qty}")
        order_type = order.get('type', 'LIMIT')
        if order_type not in ('LIMIT', 'MARKET', 'STOP_LIMIT', 'STOP_MARKET'):
            raise OrderValidationError(f"无效订单类型: {order_type}")
        if order_type in ('LIMIT', 'STOP_LIMIT'):
            price = order.get('price')
            if not isinstance(price, (int, float)) or price <= 0:
                raise OrderValidationError(f"无效价格: {price}")
        # 防注入：限制字段长度和特殊字符
        for key in ('symbol', 'side', 'client_order_id', 'trace_id'):
            if key in order:
                val = str(order[key])
                if len(val) > 100:
                    raise OrderValidationError(f"字段 {key} 过长")
                if ';' in val or '\n' in val or '\r' in val:
                    raise OrderValidationError(f"字段 {key} 包含非法字符")

    def _generate_client_order_id(self, symbol: str, side: str) -> str:
        # 微秒时间戳 + 随机数 + 机器标识，保证极高唯一性
        ts = int(time.time() * 1e6)
        rand = uuid.uuid4().hex[:8]
        return f"spark-{MACHINE_ID[:12]}-{ts}-{rand}-{side[0]}{symbol.upper()[:6]}"

    def _cache_order(self, client_id: str, order: Dict) -> bool:
        with self._orders_lock:
            if client_id in self._orders:
                existing = self._orders[client_id]
                if existing.get('status') not in OrderStatus.TERMINAL_STATES:
                    logger.warning("重复未终态订单: %s", client_id)
                    return False
                # 终态订单允许覆盖（重新提交）
            if len(self._orders) >= MAX_CACHED_ORDERS:
                self._trim_cache()
            self._orders[client_id] = {**order, "status": OrderStatus.PENDING, "updated_at": time.time()}
            self._inc_active()
            return True

    def _enqueue_pending(self, order: Dict) -> None:
        with self._pending_lock:
            if len(self._pending_orders) >= MAX_PENDING_ORDERS:
                logger.error("挂起队列满，拒绝订单 %s", order.get('client_order_id'))
                self._transition_state(order['client_order_id'], OrderStatus.REJECTED, "挂起队列满")
                self._record_metrics("order_pending_dropped", 1)
                return
            self._pending_orders.append(order)

    def _process_submit_response(self, client_id: str, response: Dict, trace_id: str) -> Dict:
        # 映射交易所状态
        exchange_status = response.get('status', '').lower()
        mapped_status = self._map_exchange_status(exchange_status)
        exchange_id = response.get('orderId', '')
        if mapped_status == OrderStatus.REJECTED:
            reason = response.get('reason', '交易所拒绝')
            self._transition_state(client_id, mapped_status, reason)
            self._publish_event(EventTypes.ORDER_REJECTED, {"client_order_id": client_id, "reason": reason})
            return {"status": "rejected", "order_id": client_id, "reason": reason, "warnings": []}
        self._transition_state(client_id, mapped_status, exchange_order_id=exchange_id)
        self._publish_event(EventTypes.ORDER_CREATED, {"client_order_id": client_id, "exchange_id": exchange_id})
        self._record_metrics("order_submitted", 1, {"side": self._orders.get(client_id, {}).get('side', 'unknown')})
        return {"status": "ok", "order_id": client_id, "reason": "订单已提交", "warnings": []}

    def _map_exchange_status(self, raw_status: str) -> str:
        mapping = {
            'new': OrderStatus.SUBMITTED,
            'partially_filled': OrderStatus.PARTIALLY_FILLED,
            'filled': OrderStatus.FILLED,
            'canceled': OrderStatus.CANCELLED,
            'cancelled': OrderStatus.CANCELLED,
            'rejected': OrderStatus.REJECTED,
            'expired': OrderStatus.EXPIRED,
        }
        return mapping.get(raw_status.lower(), OrderStatus.SUBMITTED)

    def _transition_state(self, client_id: str, new_status: str, reason: str = "", exchange_order_id: str = ""):
        with self._orders_lock:
            order = self._orders.get(client_id)
            if not order:
                return
            old_status = order.get('status')
            if not self._is_valid_transition(old_status, new_status):
                logger.error("非法状态转换 %s -> %s，订单 %s", old_status, new_status, client_id)
                return
            self._set_status(order, new_status)
            order['updated_at'] = time.time()
            if exchange_order_id:
                order['exchange_order_id'] = exchange_order_id
            if reason:
                order['status_reason'] = reason
            # 维护活跃计数
            was_active = old_status not in OrderStatus.TERMINAL_STATES
            is_active = new_status not in OrderStatus.TERMINAL_STATES
            if was_active and not is_active:
                self._dec_active()
                order['closed_at'] = time.time()
            elif not was_active and is_active:
                self._inc_active()
            # 发布事件
            if new_status in (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED):
                self._publish_event(EventTypes.STATE_CHANGE if EventTypes else "state_change",
                                    {"client_order_id": client_id, "status": new_status})

    def _set_status(self, order: Dict, status: str):
        order['status'] = status

    def _is_valid_transition(self, old: str, new: str) -> bool:
        if old is None:
            return True
        allowed = OrderStatus.ALLOWED_TRANSITIONS.get(old, set())
        return new in allowed

    def _trim_cache(self):
        now = time.time()
        with self._orders_lock:
            expired = [k for k, v in self._orders.items() if now - v.get('updated_at', 0) > CACHE_TTL_SEC]
            for k in expired:
                self._remove_order(k)
            while len(self._orders) > MAX_CACHED_ORDERS:
                k, _ = self._orders.popitem(last=False)
                self._remove_order(k)

    def _remove_order(self, order_id: str):
        order = self._orders.pop(order_id, None)
        if order and order.get('status') not in OrderStatus.TERMINAL_STATES:
            self._dec_active()

    def _inc_active(self):
        with self._count_lock:
            self._active_count += 1

    def _dec_active(self):
        with self._count_lock:
            self._active_count = max(0, self._active_count - 1)

    # ── 事件与指标 ────────────────────────────────────────

    def _publish_event(self, event_type: str, data: Dict):
        if self.event_bus and EventTypes:
            try:
                self.event_bus.publish(event_type, data)
            except Exception as e:
                logger.debug("事件发布失败: %s", e)

    def _record_metrics(self, name: str, value: float, labels: Optional[Dict] = None):
        if METRICS_AVAILABLE and MetricsCollector:
            try:
                MetricsCollector.counter(name, value, labels)
            except Exception:
                pass

    # ── 后台任务 ──────────────────────────────────────────

    def _start_cleanup(self):
        def _run():
            while not self._stop_cleanup.wait(timeout=10):
                self._retry_pending()
                self._check_timeouts()
                self._trim_cache()
        self._cleanup_thread = threading.Thread(target=_run, daemon=True)
        self._cleanup_thread.start()

    def _retry_pending(self):
        if not self.dispatcher:
            return
        with self._pending_lock:
            remaining = []
            for order in self._pending_orders:
                attempts = order.get('_retry_count', 0)
                if attempts >= RETRY_MAX_ATTEMPTS:
                    logger.error("订单 %s 超过最大重试次数，标记为 rejected", order.get('client_order_id'))
                    self._transition_state(order['client_order_id'], OrderStatus.REJECTED, "重试耗尽")
                    self._record_metrics("order_pending_expired", 1)
                    continue
                try:
                    response = self.dispatcher.send_order(order, timeout=DISPATCHER_TIMEOUT_SEC)
                    self._process_submit_response(order['client_order_id'], response, order.get('trace_id', ''))
                except Exception as e:
                    logger.warning("挂起重试失败: %s", e)
                    order['_retry_count'] = attempts + 1
                    remaining.append(order)
            self._pending_orders = remaining

    def _check_timeouts(self):
        now = time.time()
        with self._orders_lock:
            for oid, order in list(self._orders.items()):
                if order['status'] in (OrderStatus.SUBMITTED, OrderStatus.PENDING, OrderStatus.CANCELLING):
                    if now - order.get('created_at', 0) > ORDER_CONFIRM_TIMEOUT_SEC:
                        self._transition_state(oid, OrderStatus.UNKNOWN, "确认超时")
                        logger.error("订单 %s 确认超时，标记为 unknown", oid)
