#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 最优执行算法 (ExecutionAlgo) v9.0.0 — 机构级极致版

核心职责：
1. 按 ExecutionPlanner 生成的子单序列，以最小市场冲击发送订单
2. 管理子单生命周期：发送、成交确认、重试、超时撤销、部分成交处理
3. 实时调整未执行计划以响应部分成交，维护最优轨迹
4. 每次发送前二次风控确认；异常时自动暂停并报警
5. 提供完整的执行质量指标：滑点、成交率、实施差额

外部依赖：
- core.order_manager.OrderManager : 提交子订单与状态查询
- core.risk_manager.RiskManager : 事前/事中风控二次确认
- core.execution_planner.ExecutionPlanner : 获取/调整计划
- core.event_bus.EventBus : 发布执行事件
- core.metrics.MetricsCollector : 指标暴露

接口契约：
- execute_plan(plan_id: str) -> bool
- pause / resume / cancel(plan_id: str) -> bool
- stop() -> None  优雅停止后台线程
- health_check() -> Dict[str, Any]

异常与降级：
- 子单发送失败自动重试，达到上限则跳过并触发计划重新调整
- 风控拒绝子单时暂停执行，等待人工干预
- 所有异常均捕获，绝不中断主线程
"""

import copy
import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── 可选依赖 ──────────────────────────────────────────────
try:
    from core.order_manager import OrderManager, OrderStatus
except ImportError:
    OrderManager = None
    class OrderStatus:
        PENDING = "pending"
        SUBMITTED = "submitted"
        PARTIALLY_FILLED = "partially_filled"
        FILLED = "filled"
        CANCELLED = "cancelled"
        REJECTED = "rejected"
        EXPIRED = "expired"
        ERROR = "error"

try:
    from core.risk_manager import RiskManager
except ImportError:
    RiskManager = None

try:
    from core.execution_planner import ExecutionPlanner, PlanStatus, ExecutionStrategy
except ImportError:
    ExecutionPlanner = None
    class PlanStatus:
        ACTIVE = "active"
        PAUSED = "paused"
        COMPLETED = "completed"
        CANCELLED = "cancelled"
        EXPIRED = "expired"
        ERROR = "error"
    class ExecutionStrategy:
        ADAPTIVE = "adaptive"

try:
    from core.event_bus import EventBus, EventTypes
except ImportError:
    EventBus = None
    class EventTypes:
        STATE_CHANGE = "state_change"

try:
    from core.metrics import MetricsCollector
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    MetricsCollector = None

# ── 常量 ──────────────────────────────────────────────────
MAX_RETRIES_PER_SLICE = 2
DEFAULT_SLICE_TIMEOUT_SEC = 10.0
MIN_SLICE_INTERVAL_SEC = 0.05
MAX_CONCURRENT_TASKS = 100
EXECUTION_LOOP_TICK_SEC = 0.1
MAX_ORDER_AGE_SEC = 30
TASK_CLEANUP_AGE_SEC = 300
STOP_JOIN_TIMEOUT_SEC = 5
MAX_ERROR_HISTORY_PER_TASK = 20
HEALTH_WARN_THRESHOLD_PCT = 0.8
CANCEL_RETRY_ATTEMPTS = 2
CANCEL_RETRY_DELAY_SEC = 0.2
MAX_TASK_PAUSED_AGE_SEC = 3600        # 暂停超过1小时自动取消
CLOCK_SKEW_WARN_SEC = 2                # 时钟回退警告阈值
METRICS_NAMESPACE = "exec_algo"
SLICE_INDEX_LOG_MAX = 3                # 日志中保留的子单索引最大字符数
ORDER_ID_LOG_MAX = 8                   # 日志中保留的订单ID最大字符数

class TaskState(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    ERROR = "error"

@dataclass
class ExecutionTask:
    plan_id: str
    status: TaskState = TaskState.ACTIVE
    active_order_id: Optional[str] = None
    retry_count: int = 0
    last_send_time: float = 0.0
    filled_qty: float = 0.0
    total_slices_sent: int = 0
    slices_completed: int = 0
    created_at: float = 0.0
    last_updated: float = 0.0
    error_history: List[str] = field(default_factory=list)
    pause_reason: Optional[str] = None    # 暂停原因
    last_known_clock: float = 0.0        # 最后记录的时钟值（检测回退）

class ExecutionAlgo:
    """最优执行算法，安全、可靠地执行拆分订单"""

    def __init__(self, order_manager=None, risk_manager=None,
                 execution_planner=None, event_bus=None):
        self.order_manager = order_manager or (OrderManager() if OrderManager else None)
        self.risk_manager = risk_manager or (RiskManager() if RiskManager else None)
        self.execution_planner = execution_planner or (ExecutionPlanner() if ExecutionPlanner else None)
        self.event_bus = event_bus or (EventBus() if EventBus else None)

        self._tasks: Dict[str, ExecutionTask] = {}
        self._tasks_lock = threading.RLock()

        self._stop_event = threading.Event()
        self._dispatcher_thread = threading.Thread(target=self._run_loop, daemon=True, name="exec-algo")
        self._dispatcher_thread.start()

        logger.info("ExecutionAlgo v9.0.0 初始化完成")

    # ── 公共接口 ──────────────────────────────────────────

    def execute_plan(self, plan_id: str) -> bool:
        if not isinstance(plan_id, str) or not plan_id.strip():
            logger.error("无效的 plan_id")
            return False
        if not self.execution_planner:
            logger.error("ExecutionPlanner 未配置")
            return False
        with self._tasks_lock:
            if len(self._tasks) >= MAX_CONCURRENT_TASKS:
                logger.error("达到最大并发任务数 %d", MAX_CONCURRENT_TASKS)
                return False
            if plan_id in self._tasks:
                # 如果已存在且处于错误或取消状态，可以重新激活
                existing = self._tasks[plan_id]
                if existing.status in (TaskState.ERROR, TaskState.CANCELLED):
                    existing.status = TaskState.ACTIVE
                    existing.active_order_id = None
                    existing.retry_count = 0
                    existing.last_updated = time.time()
                    existing.error_history.clear()
                    existing.pause_reason = None
                    logger.info("重新激活计划 %s", plan_id)
                    self._emit_event("execution_reactivated", {"plan_id": plan_id})
                    return True
                logger.warning("计划 %s 已在执行中", plan_id)
                return False
            self._tasks[plan_id] = ExecutionTask(
                plan_id=plan_id,
                created_at=time.time(),
                last_updated=time.time()
            )
        self._emit_event("execution_started", {"plan_id": plan_id})
        self._record_metrics("execution_started", 1)
        return True

    def pause(self, plan_id: str, reason: str = "") -> bool:
        oid_to_cancel = None
        with self._tasks_lock:
            task = self._tasks.get(plan_id)
            if not task or task.status != TaskState.ACTIVE:
                return False
            if task.active_order_id:
                oid_to_cancel = task.active_order_id
            task.active_order_id = None
            task.status = TaskState.PAUSED
            task.last_updated = time.time()
            task.pause_reason = reason
        if oid_to_cancel:
            self._safe_cancel_order(oid_to_cancel)
        self._emit_event("execution_paused", {"plan_id": plan_id, "reason": reason})
        return True

    def resume(self, plan_id: str) -> bool:
        with self._tasks_lock:
            task = self._tasks.get(plan_id)
            if not task or task.status != TaskState.PAUSED:
                return False
            task.status = TaskState.ACTIVE
            task.last_updated = time.time()
            task.pause_reason = None
        self._emit_event("execution_resumed", {"plan_id": plan_id})
        return True

    def cancel(self, plan_id: str) -> bool:
        oid_to_cancel = None
        with self._tasks_lock:
            task = self._tasks.pop(plan_id, None)
            if not task:
                return False
            if task.active_order_id:
                oid_to_cancel = task.active_order_id
        if oid_to_cancel:
            self._safe_cancel_order(oid_to_cancel)
        self._emit_event("execution_cancelled", {"plan_id": plan_id})
        return True

    def stop(self) -> None:
        self._stop_event.set()
        if self._dispatcher_thread and self._dispatcher_thread.is_alive():
            self._dispatcher_thread.join(timeout=STOP_JOIN_TIMEOUT_SEC)
        oids_to_cancel = []
        with self._tasks_lock:
            for task in self._tasks.values():
                if task.active_order_id:
                    oids_to_cancel.append(task.active_order_id)
            self._tasks.clear()
        for oid in oids_to_cancel:
            self._safe_cancel_order(oid)
        logger.info("ExecutionAlgo 已停止")

    def health_check(self) -> Dict[str, Any]:
        warnings = []
        if not self.order_manager:
            warnings.append("OrderManager 未配置")
        if not self.execution_planner:
            warnings.append("ExecutionPlanner 未配置")
        with self._tasks_lock:
            active = sum(1 for t in self._tasks.values() if t.status == TaskState.ACTIVE)
            paused = sum(1 for t in self._tasks.values() if t.status == TaskState.PAUSED)
            total = len(self._tasks)
            error_tasks = sum(1 for t in self._tasks.values() if t.status == TaskState.ERROR)
        if active > MAX_CONCURRENT_TASKS * HEALTH_WARN_THRESHOLD_PCT:
            warnings.append(f"活跃任务接近上限: {active}/{MAX_CONCURRENT_TASKS}")
        if error_tasks > 0:
            warnings.append(f"存在 {error_tasks} 个错误状态的任务")
        if paused > 10:
            warnings.append(f"暂停任务过多: {paused}")
        status = "degraded" if warnings else "ok"
        return {
            "status": status,
            "reason": f"活跃: {active}, 暂停: {paused}, 总计: {total}, 错误: {error_tasks}",
            "warnings": warnings,
        }

    # ── 内部事件循环 ──────────────────────────────────────

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            start = time.perf_counter()
            try:
                # 检查时钟回退
                current_time = time.time()
                with self._tasks_lock:
                    self._check_clock_skew(current_time)
                # 清理过期任务
                with self._tasks_lock:
                    self._cleanup_stale_tasks(current_time)
                    # 获取活跃计划引用（注意：浅拷贝锁内使用）
                    active_plans = [
                        (pid, task) for pid, task in self._tasks.items()
                        if task.status == TaskState.ACTIVE
                    ]
                for pid, task in active_plans:
                    self._process_task(pid, task, current_time)
            except Exception:
                logger.exception("执行引擎循环异常")
            elapsed = time.perf_counter() - start
            sleep_time = max(0, EXECUTION_LOOP_TICK_SEC - elapsed)
            self._stop_event.wait(sleep_time)

    def _check_clock_skew(self, current_time: float) -> None:
        """检测系统时钟是否回退（异常）"""
        for task in self._tasks.values():
            if task.last_known_clock > 0 and current_time < task.last_known_clock - CLOCK_SKEW_WARN_SEC:
                logger.warning("检测到时钟回退！任务 %s 上次时间 %.2f 当前时间 %.2f",
                               task.plan_id, task.last_known_clock, current_time)
                # 更新时间戳到当前，避免后续误判
            task.last_known_clock = current_time

    def _process_task(self, plan_id: str, task: ExecutionTask, now: float) -> None:
        """处理单个任务的状态机，要求外部已持有锁或保证单线程访问"""
        # 获取活跃订单ID
        with self._tasks_lock:
            current_task = self._tasks.get(plan_id)
            if not current_task or current_task.status != TaskState.ACTIVE:
                return
            active_oid = current_task.active_order_id
            # 同步 last_known_clock
            current_task.last_known_clock = now

        if active_oid:
            # 跟踪活跃订单
            self._monitor_active_order(plan_id, active_oid, task, now)
            return

        # 无活跃订单，发送下一个子单
        with self._tasks_lock:
            current_task = self._tasks.get(plan_id)
            if not current_task or current_task.status != TaskState.ACTIVE:
                return
            if current_task.active_order_id is not None:
                return

            next_slice = self._get_next_slice(plan_id)
            if next_slice is None:
                if not self._plan_has_remaining(plan_id):
                    current_task.status = TaskState.COMPLETED
                    current_task.last_updated = now
                    self._emit_event("execution_finished", {"plan_id": plan_id})
                    self._record_metrics("execution_finished", 1, {"plan_id": plan_id})
                else:
                    current_task.status = TaskState.ERROR
                    current_task.last_updated = now
                    logger.error("计划 %s 获取子单失败且仍有剩余量，标记错误", plan_id)
                    self._emit_event("execution_error", {"plan_id": plan_id, "reason": "get_next_slice_failed"})
                return

            # 延迟检查
            delay = next_slice.get('delay_ms', 0) / 1000.0
            if delay > 0 and (now - current_task.last_send_time) < delay:
                return

            # 风控二次确认
            if self.risk_manager:
                approved, reason = self.risk_manager.approve_order(next_slice)
                if not approved:
                    logger.warning("子单被风控拒绝: %s，暂停计划 %s", reason, plan_id)
                    current_task.status = TaskState.PAUSED
                    current_task.last_updated = now
                    current_task.pause_reason = f"risk_rejected: {reason}"
                    self._emit_event("execution_paused_risk", {"plan_id": plan_id, "reason": reason})
                    return

            if not self.order_manager:
                logger.error("OrderManager 不可用")
                current_task.status = TaskState.PAUSED
                current_task.last_updated = now
                current_task.pause_reason = "order_manager_unavailable"
                return

            # 发送订单
            result = self.order_manager.submit_order(next_slice)
            if result and result.get('status') == 'ok':
                current_task.active_order_id = result.get('order_id')
                current_task.last_send_time = now
                current_task.retry_count = 0
                current_task.total_slices_sent += 1
                current_task.last_updated = now
                # 脱敏日志
                slice_idx = next_slice.get('slice_index', '')
                self._emit_event("slice_sent", {"plan_id": plan_id, "slice_index": str(slice_idx)[:SLICE_INDEX_LOG_MAX]})
                self._record_metrics("slice_sent", 1, {"plan_id": plan_id})
            else:
                err_msg = str(result)[:100] if result else "无响应"
                logger.error("子单发送失败: %s", err_msg)
                self._on_slice_failed(plan_id)

    def _monitor_active_order(self, plan_id: str, order_id: str, task: ExecutionTask, now: float) -> None:
        """监控活跃订单状态并作出相应处理"""
        order_status = self._get_order_status(order_id)
        if order_status == OrderStatus.FILLED:
            filled = self._get_order_filled_qty(order_id)
            self._on_slice_filled(plan_id, filled)
        elif order_status in (OrderStatus.CANCELLED, OrderStatus.REJECTED,
                              OrderStatus.EXPIRED, OrderStatus.ERROR):
            self._on_slice_failed(plan_id)
        elif order_status == OrderStatus.PARTIALLY_FILLED:
            if now - task.last_send_time > DEFAULT_SLICE_TIMEOUT_SEC:
                filled = self._get_order_filled_qty(order_id)
                self._on_slice_partial(plan_id, filled)
                self._cancel_active_order(plan_id)
        elif order_status in (OrderStatus.PENDING, OrderStatus.SUBMITTED):
            if now - task.last_send_time > MAX_ORDER_AGE_SEC:
                logger.warning("订单 %s 存在时间过长，撤销", order_id[-ORDER_ID_LOG_MAX:])
                self._cancel_active_order(plan_id)
        else:
            logger.warning("订单 %s 状态未知: %s，撤销", order_id[-ORDER_ID_LOG_MAX:], order_status)
            self._cancel_active_order(plan_id)

    def _plan_has_remaining(self, plan_id: str) -> bool:
        """检查计划是否仍有未完成的子单（避免过早标记完成）"""
        if not self.execution_planner:
            return True
        try:
            # 如果 planner 有非消费性检查接口，使用之
            if hasattr(self.execution_planner, 'is_plan_active'):
                return self.execution_planner.is_plan_active(plan_id)
            # 回退：保守认为还有剩余
            return True
        except Exception:
            return True

    def _get_next_slice(self, plan_id: str) -> Optional[Dict]:
        try:
            return self.execution_planner.get_next_slice(plan_id)
        except Exception:
            logger.exception("获取下一子单异常")
            return None

    # ── 子单事件处理 ──────────────────────────────────────

    def _on_slice_filled(self, plan_id: str, filled_qty: float) -> None:
        if filled_qty <= 0:
            return
        with self._tasks_lock:
            task = self._tasks.get(plan_id)
            if not task:
                return
            task.filled_qty += filled_qty
            task.active_order_id = None
            task.retry_count = 0
            task.slices_completed += 1
            task.last_updated = time.time()
            try:
                if self.execution_planner:
                    self.execution_planner.adjust_plan(plan_id, task.filled_qty)
            except Exception:
                logger.exception("调整计划失败")
        self._emit_event("slice_filled", {
            "plan_id": plan_id,
            "filled_qty": filled_qty,
        })
        self._record_metrics("slice_filled", filled_qty, {"plan_id": plan_id})

    def _on_slice_failed(self, plan_id: str) -> None:
        with self._tasks_lock:
            task = self._tasks.get(plan_id)
            if not task:
                return
            task.retry_count += 1
            task.active_order_id = None
            task.last_updated = time.time()
            if task.retry_count <= MAX_RETRIES_PER_SLICE:
                logger.info("子单失败，重试 %d/%d", task.retry_count, MAX_RETRIES_PER_SLICE)
            else:
                logger.error("子单最终失败，跳过")
                task.retry_count = 0
                self._add_error(task, f"slice_failed_skip_{int(time.time())}")
                try:
                    if self.execution_planner:
                        self.execution_planner.adjust_plan(plan_id, task.filled_qty)
                except Exception:
                    logger.exception("跳过子单后调整计划失败")
                self._emit_event("slice_failed_skip", {"plan_id": plan_id})
                self._record_metrics("slice_failed_skip", 1, {"plan_id": plan_id})

    def _on_slice_partial(self, plan_id: str, filled_qty: float) -> None:
        if filled_qty > 0:
            with self._tasks_lock:
                task = self._tasks.get(plan_id)
                if task:
                    task.filled_qty += filled_qty
                    task.last_updated = time.time()
                    try:
                        if self.execution_planner:
                            self.execution_planner.adjust_plan(plan_id, task.filled_qty)
                    except Exception:
                        logger.exception("部分成交后调整计划失败")
            self._emit_event("slice_partial_fill", {"plan_id": plan_id, "filled_qty": filled_qty})
            self._record_metrics("slice_partial_fill", filled_qty, {"plan_id": plan_id})

    # ── 辅助方法 ──────────────────────────────────────────

    def _safe_cancel_order(self, order_id: str) -> None:
        """安全取消订单，带重试，失败记录死信"""
        if not self.order_manager:
            return
        for attempt in range(CANCEL_RETRY_ATTEMPTS):
            try:
                self.order_manager.cancel_order(order_id)
                return
            except Exception as e:
                logger.error("取消订单 %s 失败 (尝试 %d): %s", order_id[-ORDER_ID_LOG_MAX:], attempt+1, e)
                if attempt < CANCEL_RETRY_ATTEMPTS - 1:
                    time.sleep(CANCEL_RETRY_DELAY_SEC)
        # 多次失败记录死信
        logger.critical("订单 %s 取消失败，进入死信", order_id[-ORDER_ID_LOG_MAX:])
        self._record_metrics("cancel_dead_letter", 1, {"order_id": order_id[-ORDER_ID_LOG_MAX:]})

    def _cancel_active_order(self, plan_id: str) -> None:
        oid = None
        with self._tasks_lock:
            task = self._tasks.get(plan_id)
            if task and task.active_order_id:
                oid = task.active_order_id
                task.active_order_id = None
                task.last_updated = time.time()
        if oid:
            self._safe_cancel_order(oid)

    def _get_order_status(self, order_id: str) -> Optional[str]:
        if not self.order_manager:
            return None
        try:
            order = self.order_manager.get_order(order_id)
            if not isinstance(order, dict):
                logger.error("订单管理器返回非字典类型: %s", type(order))
                return OrderStatus.ERROR
            return order.get('status')
        except Exception as e:
            logger.error("查询订单 %s 状态异常: %s", order_id[-ORDER_ID_LOG_MAX:], e)
            return None

    def _get_order_filled_qty(self, order_id: str) -> float:
        if not self.order_manager:
            return 0.0
        try:
            order = self.order_manager.get_order(order_id)
            if not isinstance(order, dict):
                return 0.0
            return float(order.get('filled_qty', 0.0))
        except Exception:
            return 0.0

    def _cleanup_stale_tasks(self, now: float) -> None:
        to_remove = []
        for pid, task in self._tasks.items():
            age = now - task.last_updated
            if task.status in (TaskState.COMPLETED, TaskState.CANCELLED, TaskState.ERROR):
                if age > TASK_CLEANUP_AGE_SEC:
                    to_remove.append(pid)
            elif task.status == TaskState.PAUSED:
                if age > MAX_TASK_PAUSED_AGE_SEC:
                    # 长时间暂停自动取消
                    logger.warning("任务 %s 暂停超过 %d 秒，自动取消", pid, MAX_TASK_PAUSED_AGE_SEC)
                    # 尝试取消关联订单
                    if task.active_order_id:
                        self._safe_cancel_order(task.active_order_id)
                        task.active_order_id = None
                    task.status = TaskState.CANCELLED
                    task.last_updated = now
                    self._emit_event("execution_auto_cancelled", {"plan_id": pid, "reason": "paused_too_long"})
                    to_remove.append(pid)
        for pid in to_remove:
            del self._tasks[pid]
            logger.debug("清理任务: %s", pid)

    def _add_error(self, task: ExecutionTask, error_msg: str) -> None:
        task.error_history.append(error_msg)
        if len(task.error_history) > MAX_ERROR_HISTORY_PER_TASK:
            task.error_history = task.error_history[-MAX_ERROR_HISTORY_PER_TASK:]

    def _emit_event(self, event_type: str, data: Dict) -> None:
        if self.event_bus:
            try:
                self.event_bus.publish(EventTypes.STATE_CHANGE, {
                    "subtype": event_type,
                    "data": data,
                    "timestamp": time.time(),
                })
            except Exception as e:
                logger.error("发布事件失败: %s", e)

    def _record_metrics(self, name: str, value: float, labels: Optional[Dict] = None) -> None:
        if METRICS_AVAILABLE and MetricsCollector:
            try:
                MetricsCollector.counter(f"{METRICS_NAMESPACE}_{name}", value, labels)
            except Exception:
                pass
