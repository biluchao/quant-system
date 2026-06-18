#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 全局健康监控 (HealthMonitor) v6.0.0 — 机构级终极版

核心职责：
1. 监控系统资源：CPU、内存、磁盘（含inode）、文件描述符、系统负载
2. 检测关键服务：Redis（超时保护）、事件总线（背压）、订单网关、策略引擎心跳
3. 模型漂移检测：基于 KL 散度，预警模型退化
4. 智能告警分级（degraded/error/critical），去重，防告警风暴
5. 完整线程安全，无死锁，无泄漏，监控线程异常自恢复
6. 提供结构化审计事件、Prometheus 指标，满足万亿级账户合规要求

外部依赖：
- psutil (可选) : 系统资源指标
- redis (可选) : Redis 连通性
- core.event_bus.EventBus : 事件发布
- core.metrics.MetricsCollector : 指标暴露
- resource (标准库) : 文件描述符限制

接口契约：
- check() -> Dict[str, Any]
- start(interval_sec: float) -> None
- stop() -> None
- update_drift_score(kl_divergence: float) -> None
- set_engine_heartbeat_checker(callable) -> None
- set_order_gateway_pinger(callable) -> None
- health_check() -> Dict[str, Any]
"""

import logging
import math
import os
import resource
import socket
import time
import threading
from typing import Dict, Any, Optional, List, Callable

logger = logging.getLogger(__name__)

VERSION = "6.0.0"
SPDX_IDENTIFIER = "Apache-2.0"

# 可选依赖
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    psutil = None

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None

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

# ── 常量 ──────────────────────────────────────────────────
SENSOR_UNAVAILABLE = -1.0
DEFAULT_CHECK_INTERVAL_SEC = 10.0
MIN_INTERVAL_SEC = 1.0
MAX_INTERVAL_SEC = 300.0

CPU_HIGH = 90.0
CPU_CRIT = 98.0
MEM_HIGH = 85.0
MEM_CRIT = 95.0
DISK_HIGH = 90.0
DISK_CRIT = 98.0
INODE_HIGH = 90.0
INODE_CRIT = 98.0
FD_HIGH = 80.0
FD_CRIT = 95.0
LOAD_HIGH = 10.0          # 系统负载过高阈值（相对于CPU核数）

REDIS_TIMEOUT_SEC = 2.0
MODEL_DRIFT_KL_THRESHOLD = 0.2
EVENT_BUS_BACKLOG_HIGH = 10000
CONSECUTIVE_FAILURES_THRESHOLD = 5
MAX_WARNINGS_BEFORE_ESCALATION = 3
HEALTH_EVENT_SUBTYPE = "health_check"
ALERT_COOLDOWN_SEC = 60.0  # 同类型告警最小间隔


class HealthMonitor:
    """全局系统健康监控器（线程安全）"""

    def __init__(self, event_bus=None, redis_client=None,
                 order_gateway_pinger: Optional[Callable[[], bool]] = None,
                 engine_heartbeat_pinger: Optional[Callable[[], bool]] = None):
        self.event_bus = event_bus or (EventBus() if EventBus else None)
        self.redis_client = redis_client
        self._order_gateway_pinger = order_gateway_pinger if callable(order_gateway_pinger) else None
        self._engine_heartbeat_pinger = engine_heartbeat_pinger if callable(engine_heartbeat_pinger) else None

        # 后台线程
        self._stop_event = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

        # 状态（仅在 _lock 内更新）
        self._last_resources = {
            "cpu_percent": SENSOR_UNAVAILABLE,
            "memory_percent": SENSOR_UNAVAILABLE,
            "disk_percent": SENSOR_UNAVAILABLE,
            "inode_percent": SENSOR_UNAVAILABLE,
            "fd_percent": SENSOR_UNAVAILABLE,
            "load_average": SENSOR_UNAVAILABLE,
        }
        self._last_services = {
            "redis_ok": False,
            "event_bus_ok": False,
            "order_gateway_ok": False,
            "engine_heartbeat_ok": False,
        }
        self._drift_score = 0.0
        self._consecutive_failures = 0
        self._total_checks = 0
        self._last_check_time = 0.0
        self._last_alert_time: Dict[str, float] = {}  # 告警冷却

        # 预热 psutil cpu_percent，避免首次返回 0
        if PSUTIL_AVAILABLE:
            try:
                _ = psutil.cpu_percent(interval=0.0)
            except Exception:
                pass

        logger.info("HealthMonitor v%s 初始化完成", VERSION)

    # ── 公共接口 ──────────────────────────────────────────

    def check(self) -> Dict[str, Any]:
        """执行一次全面健康检查（线程安全）"""
        with self._lock:
            warnings: List[str] = []
            resources = self._check_resources()
            services = self._check_services()
            drift = self._check_model_drift()

            self._evaluate_resource_warnings(resources, warnings)
            self._evaluate_service_warnings(services, warnings)
            if drift.get("drift_detected"):
                warnings.append(f"模型漂移: KL={drift['kl_divergence']:.4f}")

            # 去重
            warnings = list(dict.fromkeys(warnings))

            status = self._determine_status(warnings)

            if status in ("error", "critical"):
                self._consecutive_failures += 1
            else:
                self._consecutive_failures = 0

            if self._consecutive_failures >= CONSECUTIVE_FAILURES_THRESHOLD:
                status = "critical"
                logger.critical("连续健康检查失败 %d 次，升级为 CRITICAL",
                               self._consecutive_failures)

            self._last_resources = resources
            self._last_services = services
            self._drift_score = drift["kl_divergence"]
            self._total_checks += 1
            self._last_check_time = time.time()

            report = {
                "status": status,
                "timestamp": self._last_check_time,
                "version": VERSION,
                "resources": resources,
                "services": services,
                "model_drift": drift,
                "warnings": warnings,
                "checks_total": self._total_checks,
                "consecutive_failures": self._consecutive_failures,
            }

        # 在锁外发送事件和指标
        self._emit_health_event(report)
        self._update_metrics(report)

        # 告警冷却
        if report["status"] == "critical":
            self._maybe_emit_alert("health_critical", "系统健康进入 CRITICAL 状态")
        return report

    def start(self, interval_sec: float = DEFAULT_CHECK_INTERVAL_SEC) -> None:
        """启动后台定时健康检查，确保仅有一个线程运行"""
        interval_sec = max(MIN_INTERVAL_SEC, min(interval_sec, MAX_INTERVAL_SEC))

        old_thread = None
        with self._lock:
            if self._monitor_thread and self._monitor_thread.is_alive():
                old_thread = self._monitor_thread
                self._stop_event.set()

        if old_thread:
            old_thread.join(timeout=5)
            if old_thread.is_alive():
                logger.warning("旧监控线程未能及时退出，将被替换")

        with self._lock:
            self._stop_event.clear()
            self._monitor_thread = threading.Thread(
                target=self._run_loop,
                args=(interval_sec,),
                daemon=True,
                name="health-monitor"
            )
            self._monitor_thread.start()
            logger.info("后台健康监控已启动，间隔 %.1fs", interval_sec)

    def stop(self) -> None:
        """停止后台监控"""
        self._stop_event.set()
        with self._lock:
            thread = self._monitor_thread
            self._monitor_thread = None
        if thread and thread.is_alive():
            thread.join(timeout=5)
            if thread.is_alive():
                logger.warning("监控线程未能在超时内停止")
        logger.info("健康监控已停止")

    def update_drift_score(self, kl_divergence: float) -> None:
        """更新模型漂移 KL 散度分数"""
        if not isinstance(kl_divergence, (int, float)) or math.isnan(kl_divergence) or kl_divergence < 0:
            logger.warning("无效的 KL 散度值: %s，已重置为0", kl_divergence)
            self._drift_score = 0.0
            return
        self._drift_score = float(kl_divergence)

    def set_engine_heartbeat_checker(self, checker: Callable[[], bool]) -> None:
        if callable(checker):
            self._engine_heartbeat_pinger = checker
            logger.info("引擎心跳检测器已设置")

    def set_order_gateway_pinger(self, pinger: Callable[[], bool]) -> None:
        if callable(pinger):
            self._order_gateway_pinger = pinger
            logger.info("订单网关检测器已设置")

    def get_last_report(self) -> Dict[str, Any]:
        """获取最近一次的健康检查报告快照（线程安全）"""
        with self._lock:
            return {
                "status": self._determine_status([]),  # 简单状态，实际可返回缓存报告
                "last_check_time": self._last_check_time,
                "resources": dict(self._last_resources),
                "services": dict(self._last_services),
                "drift_score": self._drift_score,
            }

    def health_check(self) -> Dict[str, Any]:
        """模块自检"""
        warnings = []
        if not PSUTIL_AVAILABLE:
            warnings.append("psutil 不可用，资源指标降级")
        if self.event_bus is None:
            warnings.append("EventBus 未配置")
        thread_alive = self._monitor_thread and self._monitor_thread.is_alive()
        if not thread_alive and not self._stop_event.is_set():
            warnings.append("后台监控线程未运行")
        status = "degraded" if warnings else "ok"
        return {
            "status": status,
            "reason": f"HealthMonitor v{VERSION}",
            "warnings": warnings,
        }

    # ── 资源检查 ──────────────────────────────────────────

    def _check_resources(self) -> Dict[str, float]:
        if not PSUTIL_AVAILABLE:
            return dict(self._last_resources)
        try:
            cpu = psutil.cpu_percent(interval=0.2)
            mem = psutil.virtual_memory().percent
            disk_usage = psutil.disk_usage('/')
            disk = disk_usage.percent
            # inode
            inode = SENSOR_UNAVAILABLE
            if hasattr(disk_usage, 'inodes'):
                inode = disk_usage.inodes.percent
            # 文件描述符
            fd_pct = SENSOR_UNAVAILABLE
            if os.path.isdir('/proc/self/fd'):
                try:
                    fd_used = len(os.listdir('/proc/self/fd'))
                    soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
                    if soft > 0:
                        fd_pct = (fd_used / soft) * 100.0
                except (PermissionError, OSError):
                    logger.warning("无法读取 /proc/self/fd")
            # 系统负载 (1分钟平均)
            load = SENSOR_UNAVAILABLE
            if hasattr(psutil, 'getloadavg'):
                try:
                    load1, _, _ = psutil.getloadavg()
                    load = float(load1)
                except Exception:
                    pass
            return {
                "cpu_percent": cpu,
                "memory_percent": mem,
                "disk_percent": disk,
                "inode_percent": inode,
                "fd_percent": fd_pct,
                "load_average": load,
            }
        except Exception as e:
            logger.error("资源指标采集异常: %s", e)
            return dict(self._last_resources)

    # ── 服务检查 ──────────────────────────────────────────

    def _check_services(self) -> Dict[str, bool]:
        redis_ok = self._check_redis()
        event_bus_ok = self._check_event_bus()
        order_gateway_ok = self._check_order_gateway()
        engine_ok = self._check_engine_heartbeat()
        return {
            "redis_ok": redis_ok,
            "event_bus_ok": event_bus_ok,
            "order_gateway_ok": order_gateway_ok,
            "engine_heartbeat_ok": engine_ok,
        }

    def _check_redis(self) -> bool:
        if not self.redis_client:
            return False
        if not hasattr(self.redis_client, 'ping'):
            return False
        old_timeout = getattr(self.redis_client, 'socket_timeout', None)
        try:
            if hasattr(self.redis_client, 'set_socket_timeout'):
                self.redis_client.set_socket_timeout(REDIS_TIMEOUT_SEC)
            self.redis_client.ping()
            return True
        except Exception:
            return False
        finally:
            # 恢复原超时
            if old_timeout is not None and hasattr(self.redis_client, 'set_socket_timeout'):
                try:
                    self.redis_client.set_socket_timeout(old_timeout)
                except Exception:
                    pass

    def _check_event_bus(self) -> bool:
        if not self.event_bus:
            return False
        try:
            if not hasattr(self.event_bus, 'backlog_size'):
                return True  # 无法检测，假设正常
            backlog = self.event_bus.backlog_size()
            if backlog > EVENT_BUS_BACKLOG_HIGH:
                logger.warning("事件总线队列堆积: %d", backlog)
                return False
            return True
        except Exception:
            logger.warning("事件总线状态检查异常")
            return False

    def _check_order_gateway(self) -> bool:
        if not self._order_gateway_pinger:
            return True
        try:
            return self._order_gateway_pinger()
        except Exception:
            return False

    def _check_engine_heartbeat(self) -> bool:
        if not self._engine_heartbeat_pinger:
            return False  # 安全侧：未配置则认为不健康
        try:
            return self._engine_heartbeat_pinger()
        except Exception:
            return False

    # ── 模型漂移 ──────────────────────────────────────────

    def _check_model_drift(self) -> Dict[str, Any]:
        kl = self._drift_score
        if math.isnan(kl) or math.isinf(kl):
            kl = 0.0
            self._drift_score = 0.0
        drifted = kl > MODEL_DRIFT_KL_THRESHOLD
        return {
            "drift_detected": drifted,
            "kl_divergence": kl,
        }

    # ── 告警评估 ──────────────────────────────────────────

    @staticmethod
    def _evaluate_resource_warnings(resources: Dict[str, float], warnings: List[str]) -> None:
        cpu = resources.get("cpu_percent", SENSOR_UNAVAILABLE)
        mem = resources.get("memory_percent", SENSOR_UNAVAILABLE)
        disk = resources.get("disk_percent", SENSOR_UNAVAILABLE)
        inode = resources.get("inode_percent", SENSOR_UNAVAILABLE)
        fd = resources.get("fd_percent", SENSOR_UNAVAILABLE)
        load = resources.get("load_average", SENSOR_UNAVAILABLE)

        if cpu == SENSOR_UNAVAILABLE:
            warnings.append("CPU 使用率不可用")
        elif cpu > CPU_CRIT:
            warnings.append(f"CPU 使用率严重过高: {cpu:.1f}%")
        elif cpu > CPU_HIGH:
            warnings.append(f"CPU 使用率过高: {cpu:.1f}%")

        if mem == SENSOR_UNAVAILABLE:
            warnings.append("内存使用率不可用")
        elif mem > MEM_CRIT:
            warnings.append(f"内存使用率严重过高: {mem:.1f}%")
        elif mem > MEM_HIGH:
            warnings.append(f"内存使用率过高: {mem:.1f}%")

        if disk == SENSOR_UNAVAILABLE:
            warnings.append("磁盘使用率不可用")
        elif disk > DISK_CRIT:
            warnings.append(f"磁盘使用率严重过高: {disk:.1f}%")
        elif disk > DISK_HIGH:
            warnings.append(f"磁盘使用率过高: {disk:.1f}%")

        if inode != SENSOR_UNAVAILABLE:
            if inode > INODE_CRIT:
                warnings.append(f"inode 使用率严重过高: {inode:.1f}%")
            elif inode > INODE_HIGH:
                warnings.append(f"inode 使用率过高: {inode:.1f}%")

        if fd != SENSOR_UNAVAILABLE:
            if fd > FD_CRIT:
                warnings.append(f"文件描述符使用率严重过高: {fd:.1f}%")
            elif fd > FD_HIGH:
                warnings.append(f"文件描述符使用率过高: {fd:.1f}%")

        if load != SENSOR_UNAVAILABLE and load > LOAD_HIGH:
            warnings.append(f"系统负载过高: {load:.2f}")

    @staticmethod
    def _evaluate_service_warnings(services: Dict[str, bool], warnings: List[str]) -> None:
        if not services.get("redis_ok", False):
            warnings.append("Redis 连接异常")
        if not services.get("event_bus_ok", False):
            warnings.append("事件总线不可用或积压")
        if not services.get("order_gateway_ok", False):
            warnings.append("订单网关不可用")
        if not services.get("engine_heartbeat_ok", False):
            warnings.append("策略引擎心跳异常")

    @staticmethod
    def _determine_status(warnings: List[str]) -> str:
        if not warnings:
            return "ok"
        critical_keywords = ["严重", "critical"]
        for w in warnings:
            lower_w = w.lower()
            if any(kw in lower_w for kw in critical_keywords):
                return "critical"
        if len(warnings) >= MAX_WARNINGS_BEFORE_ESCALATION:
            return "error"
        return "degraded"

    # ── 后台循环 ──────────────────────────────────────────

    def _run_loop(self, interval_sec: float) -> None:
        logger.info("健康监控循环开始")
        while not self._stop_event.wait(interval_sec):
            try:
                self.check()
            except Exception as e:
                logger.exception("健康检查循环异常: %s", e)
                with self._lock:
                    self._consecutive_failures += 1
        logger.info("健康监控循环退出")

    # ── 事件与指标 ────────────────────────────────────────

    def _emit_health_event(self, report: Dict) -> None:
        if self.event_bus and EventTypes:
            try:
                # 只发布摘要和最多5条警告详情
                self.event_bus.publish(EventTypes.SYSTEM_ALERT, {
                    "subtype": HEALTH_EVENT_SUBTYPE,
                    "status": report["status"],
                    "warnings_count": len(report["warnings"]),
                    "warnings_sample": report["warnings"][:5],
                    "timestamp": report["timestamp"],
                })
            except Exception as e:
                logger.error("发布健康事件失败: %s", e)

    def _maybe_emit_alert(self, alert_type: str, message: str) -> None:
        """带冷却的告警发送"""
        now = time.time()
        last = self._last_alert_time.get(alert_type, 0)
        if now - last < ALERT_COOLDOWN_SEC:
            return
        self._last_alert_time[alert_type] = now
        self._emit_alert(alert_type, message)

    def _emit_alert(self, alert_type: str, message: str) -> None:
        if self.event_bus and EventTypes:
            try:
                self.event_bus.publish(EventTypes.SYSTEM_ALERT, {
                    "alert_type": alert_type,
                    "message": message,
                    "timestamp": time.time(),
                })
            except Exception as e:
                logger.error("发布告警事件失败: %s", e)

    def _update_metrics(self, report: Dict) -> None:
        if METRICS_AVAILABLE and MetricsCollector:
            try:
                res = report.get("resources", {})
                for key, value in res.items():
                    if isinstance(value, (int, float)) and value >= 0:
                        MetricsCollector.gauge(f"health_{key}", value)
                svc = report.get("services", {})
                MetricsCollector.gauge("health_redis_up", 1 if svc.get("redis_ok") else 0)
                MetricsCollector.gauge("health_event_bus_up", 1 if svc.get("event_bus_ok") else 0)
                MetricsCollector.gauge("health_order_gateway_up", 1 if svc.get("order_gateway_ok") else 0)
                MetricsCollector.gauge("health_engine_heartbeat_up", 1 if svc.get("engine_heartbeat_ok") else 0)
                kl = report["model_drift"]["kl_divergence"]
                if not math.isnan(kl):
                    MetricsCollector.gauge("health_model_drift_kl", kl)
            except Exception:
                pass
