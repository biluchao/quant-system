#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 服务降级控制器 (DegradationController) v4.0.0 — 机构级终极版

核心职责：
1. 根据系统健康报告自动决策降级级别，实现资源保护与核心业务连续性
2. 支持手动覆盖与恢复自动模式，所有操作需审计（操作人、时间、原因）
3. 线程安全的状态转移与模块可用性查询，支持震荡保护与历史追溯
4. 发布降级变更事件与 Prometheus 指标，满足万亿级账户合规要求

外部依赖：
- core.event_bus.EventBus : 订阅健康事件，发布降级事件
- core.metrics.MetricsCollector : 指标暴露
- threading : 线程安全

接口契约：
- handle_health_event(report: Dict) -> None
- manual_override(level: str, operator: str) -> bool
- reset_manual_override(operator: str) -> None
- get_current_level() -> str
- is_module_enabled(module_name: str) -> bool
- get_history(limit: int) -> List[Dict]
- health_check() -> Dict[str, Any]
"""

import logging
import time
import threading
from collections import deque
from typing import Dict, Any, Optional, Set, List, Tuple

logger = logging.getLogger(__name__)

VERSION = "4.0.0"
SPDX_IDENTIFIER = "Apache-2.0"

# 可选依赖
try:
    from core.event_bus import EventBus, EventTypes
except ImportError:
    EventBus = None
    EventTypes = None
try:
    from core.metrics import MetricsCollector
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    MetricsCollector = None


class DegradationLevel:
    """降级级别常量"""
    NORMAL = "normal"
    REDUCED = "reduced"
    MINIMAL = "minimal"
    HALT = "halt"

    ALL = frozenset({NORMAL, REDUCED, MINIMAL, HALT})
    ORDER = (NORMAL, REDUCED, MINIMAL, HALT)  # 从宽松到严格

    @classmethod
    def compare(cls, a: str, b: str) -> int:
        """比较两个级别：a < b 表示 a 更宽松"""
        try:
            return cls.ORDER.index(a) - cls.ORDER.index(b)
        except ValueError:
            return 0


# 降级配置：每个级别禁用的模块集合
DISABLED_MODULES: Dict[str, frozenset] = {
    DegradationLevel.NORMAL: frozenset(),
    DegradationLevel.REDUCED: frozenset({
        "rl_meta_controller", "ai_sandbox", "advanced_analytics",
        "performance_reporting", "frontend_streaming"
    }),
    DegradationLevel.MINIMAL: frozenset({
        "rl_meta_controller", "ai_sandbox", "advanced_analytics",
        "performance_reporting", "frontend_streaming", "execution_planner_non_urgent",
        "health_monitor_non_critical", "data_archiver"
    }),
    DegradationLevel.HALT: frozenset({"__ALL_EXCEPT__"}),
}

# HALT 白名单（仅这些模块在 HALT 级别运行）
HALT_WHITELIST = frozenset({
    "risk_manager", "order_manager", "event_bus", "position_keeper"
})

# 默认阈值
DEFAULT_CPU_CRITICAL = 98.0
DEFAULT_MEMORY_CRITICAL = 95.0
DEFAULT_WARNINGS_ESCALATE = 3
MAX_HISTORY_SIZE = 200
MAX_REASON_LENGTH = 256
MIN_TRANSITION_INTERVAL_SEC = 5.0  # 震荡保护：最小转换间隔
HEALTH_EVENT_SUBTYPE = "health_check"


class DegradationController:
    """服务降级控制器（线程安全，震荡保护）"""

    def __init__(self, event_bus=None, thresholds: Optional[Dict] = None):
        self.event_bus = event_bus or (EventBus() if EventBus else None)
        thresholds = thresholds or {}
        self.cpu_critical = self._safe_float(thresholds.get("cpu_critical", DEFAULT_CPU_CRITICAL), DEFAULT_CPU_CRITICAL)
        self.memory_critical = self._safe_float(thresholds.get("memory_critical", DEFAULT_MEMORY_CRITICAL), DEFAULT_MEMORY_CRITICAL)
        self.warnings_escalate = int(thresholds.get("warnings_escalate", DEFAULT_WARNINGS_ESCALATE))

        self._lock = threading.RLock()
        self._current_level = DegradationLevel.NORMAL
        self._manual_override = False
        self._last_transition_time = 0.0
        self._last_health_report: Optional[Dict] = None

        # 历史记录
        self._history: deque = deque(maxlen=MAX_HISTORY_SIZE)

        # 订阅事件
        if self.event_bus and EventTypes:
            try:
                self.event_bus.subscribe(EventTypes.SYSTEM_ALERT, self._on_health_event)
                logger.info("已订阅系统告警事件")
            except Exception as e:
                logger.error("订阅事件失败: %s", e)

        logger.info("DegradationController v%s 初始化，当前级别: %s", VERSION, self._current_level)
        self._update_metrics()

    # ── 公共接口 ──────────────────────────────────────────

    def handle_health_event(self, report: Dict) -> None:
        """处理健康报告，线程安全，支持震荡保护"""
        if not isinstance(report, dict):
            logger.warning("无效的健康报告类型: %s", type(report))
            return
        try:
            with self._lock:
                # 缓存最近报告（供手动恢复后使用）
                self._last_health_report = report

                if self._manual_override:
                    logger.debug("手动覆盖模式，跳过自动健康评估")
                    return

                status = str(report.get("status", "unknown")).lower()
                resources = report.get("resources", {}) or {}
                services = report.get("services", {}) or {}
                warnings = report.get("warnings", [])
                if not isinstance(warnings, list):
                    warnings = []

                cpu = self._safe_float(resources.get("cpu_percent"), -1.0)
                mem = self._safe_float(resources.get("memory_percent"), -1.0)
                redis_ok = bool(services.get("redis_ok", True))
                engine_ok = bool(services.get("engine_heartbeat_ok", True))

                new_level = self._decide_level(
                    status, cpu, mem, redis_ok, engine_ok, warnings
                )
                self._transition_to(new_level, reason=f"健康评估:{status}")
        except Exception as e:
            logger.error("处理健康事件异常: %s", e)

    def manual_override(self, level: str, operator: str = "unknown") -> bool:
        """手动设置降级级别"""
        if level not in DegradationLevel.ALL:
            logger.error("无效的降级级别: %s", level)
            return False
        operator = str(operator)[:64]  # 限制长度
        with self._lock:
            old = self._current_level
            self._manual_override = True
            self._transition_to(level, reason=f"手动覆盖 by {operator}")
            self._add_history("manual_override", old, level, operator, True)
            logger.warning("管理员 %s 设置降级: %s -> %s", operator, old, level)
        return True

    def reset_manual_override(self, operator: str = "unknown") -> None:
        """恢复自动模式，并立即基于最近报告评估"""
        operator = str(operator)[:64]
        with self._lock:
            if not self._manual_override:
                return
            old = self._current_level
            self._manual_override = False
            self._add_history("reset_manual", old, old, operator, False)
            logger.info("管理员 %s 解除手动覆盖", operator)

            # 立即基于最近报告重新评估
            if self._last_health_report:
                self.handle_health_event(self._last_health_report)
            else:
                # 无历史报告，保持当前级别，等待下次健康事件
                logger.info("无历史健康报告，保持当前级别")

    def get_current_level(self) -> str:
        with self._lock:
            return self._current_level

    def is_module_enabled(self, module_name: str) -> bool:
        """查询模块是否可用"""
        if not isinstance(module_name, str) or not module_name:
            return False
        with self._lock:
            level = self._current_level
        if level == DegradationLevel.HALT:
            return module_name in HALT_WHITELIST
        disabled = DISABLED_MODULES.get(level, frozenset())
        return module_name not in disabled

    def get_history(self, limit: int = 20) -> List[Dict]:
        """获取降级历史（副本）"""
        with self._lock:
            return [dict(entry) for entry in list(self._history)[-limit:]]

    def health_check(self) -> Dict[str, Any]:
        with self._lock:
            level = self._current_level
            manual = self._manual_override
        return {
            "status": "degraded" if manual else "ok",
            "reason": f"当前级别: {level}",
            "warnings": ["手动覆盖模式"] if manual else [],
            "manual_override": manual,
        }

    # ── 决策逻辑 ──────────────────────────────────────────

    def _decide_level(self, status: str, cpu: float, mem: float,
                      redis_ok: bool, engine_ok: bool, warnings: List) -> str:
        """根据指标计算目标降级级别"""
        # HALT
        if not engine_ok or status in ("critical", "halt"):
            return DegradationLevel.HALT

        # MINIMAL
        if (cpu >= 0 and cpu > self.cpu_critical) or (mem >= 0 and mem > self.memory_critical):
            return DegradationLevel.MINIMAL
        if not redis_ok:
            return DegradationLevel.MINIMAL

        # REDUCED
        if status in ("degraded", "error") or len(warnings) >= self.warnings_escalate:
            return DegradationLevel.REDUCED

        return DegradationLevel.NORMAL

    def _transition_to(self, new_level: str, reason: str = "") -> None:
        """执行级别转换（必须在锁内调用），含震荡保护"""
        if new_level == self._current_level:
            return
        if new_level not in DegradationLevel.ALL:
            logger.error("无效的目标级别: %s", new_level)
            return

        # 震荡保护
        now = time.time()
        if now - self._last_transition_time < MIN_TRANSITION_INTERVAL_SEC:
            logger.debug("震荡保护：距离上次转换仅 %.2fs", now - self._last_transition_time)
            return

        old = self._current_level
        self._current_level = new_level
        self._last_transition_time = now

        direction = "降级" if DegradationLevel.compare(new_level, old) > 0 else "恢复"
        reason = str(reason)[:MAX_REASON_LENGTH]

        # 历史记录
        self._add_history("auto_transition", old, new_level, "system", False, reason)

        # 执行级别动作
        self._apply_level_actions(new_level)

        # 事件发布
        self._emit_state_event(old, new_level, direction, reason)

        # 指标
        self._update_metrics()

        logger.warning("降级变更: %s -> %s (%s), 原因: %s", old, new_level, direction, reason)

    def _apply_level_actions(self, level: str) -> None:
        """根据级别执行实际控制动作"""
        if level == DegradationLevel.HALT:
            logger.critical("进入 HALT 级别，仅白名单模块运行")
            # 通知风控停止新交易（通过事件总线）
            self._emit_alert("degradation_halt", "系统进入 HALT 模式")
        else:
            disabled = DISABLED_MODULES.get(level, frozenset()) - {"__ALL_EXCEPT__"}
            if disabled:
                logger.info("降级到 %s，禁用模块: %s", level, sorted(disabled))

    # ── 事件回调 ──────────────────────────────────────────

    def _on_health_event(self, event_data: Any) -> None:
        try:
            if not isinstance(event_data, dict):
                return
            if event_data.get("subtype") != HEALTH_EVENT_SUBTYPE:
                return
            report = event_data.get("report")
            if isinstance(report, dict):
                self.handle_health_event(report)
        except Exception as e:
            logger.error("健康事件回调异常: %s", e)

    # ── 辅助方法 ──────────────────────────────────────────

    @staticmethod
    def _safe_float(value: Any, default: float = -1.0) -> float:
        """安全转换 float，拒绝 NaN/Inf"""
        try:
            v = float(value)
            if v != v or v in (float('inf'), float('-inf')):  # NaN or Inf
                return default
            return v
        except (ValueError, TypeError):
            return default

    def _add_history(self, action: str, old_level: str, new_level: str,
                     operator: str, manual: bool, reason: str = "") -> None:
        self._history.append({
            "timestamp": time.time(),
            "action": action,
            "old_level": old_level,
            "new_level": new_level,
            "operator": str(operator)[:64],
            "manual": manual,
            "reason": str(reason)[:MAX_REASON_LENGTH],
        })

    def _emit_state_event(self, old_level: str, new_level: str,
                          direction: str, reason: str) -> None:
        if self.event_bus and EventTypes:
            try:
                self.event_bus.publish(EventTypes.SYSTEM_ALERT, {
                    "subtype": "degradation_level_changed",
                    "old_level": old_level,
                    "new_level": new_level,
                    "direction": direction,
                    "reason": reason,
                    "timestamp": time.time(),
                })
            except Exception as e:
                logger.error("发布降级事件失败: %s", e)

    def _emit_alert(self, alert_type: str, message: str) -> None:
        if self.event_bus and EventTypes:
            try:
                self.event_bus.publish(EventTypes.SYSTEM_ALERT, {
                    "alert_type": alert_type,
                    "message": message,
                    "timestamp": time.time(),
                })
            except Exception:
                pass

    def _update_metrics(self) -> None:
        if METRICS_AVAILABLE and MetricsCollector:
            try:
                level_map = {
                    DegradationLevel.NORMAL: 0, DegradationLevel.REDUCED: 1,
                    DegradationLevel.MINIMAL: 2, DegradationLevel.HALT: 3
                }
                MetricsCollector.gauge("degradation_level", level_map.get(self._current_level, -1))
            except Exception:
                pass
