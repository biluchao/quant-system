#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 指标收集器 (MetricsCollector) — 机构级生产实现 v6.0.0

核心职责：
1. 提供命名空间隔离的 Prometheus 指标暴露（Counter / Gauge / Histogram / Summary）
2. 内置延迟、吞吐量、盈亏等多场景预定义桶，支持动态自定义桶
3. 严格校验标签数量与值长度，防止指标爆炸与性能退化
4. 支持多进程模式（基于 PROMETHEUS_MULTIPROC_DIR），自动添加进程标识避免文件冲突
5. 提供 /metrics 端点所需的完整 HTTP 响应（含 Content-Type）
6. 全链路无崩溃点：任何操作失败不影响主业务流程
7. 暴露自身健康指标（spark_metrics_errors_total, spark_metrics_info）供运维监控
8. 支持测试环境重置，便于单元测试

外部依赖：
- prometheus_client (可选) : 未安装时降级为空操作，不影响业务
- math, os, threading, re : 标准库

接口契约：
- counter / gauge / histogram / summary 方法签名见各自 docstring
- get_metrics_text() -> str  返回 Prometheus 文本，异常时返回空
- get_metrics_response() -> Tuple[str, int, Dict]  返回可直接用于 HTTP 响应的三元组
- health_check() -> Dict[str, Any]

异常与降级：
- 所有公开方法均捕获 Exception，内部降级并记录 DEBUG/ERROR，绝不向上抛出
- 标签过多 (>20) 或值过长 (>1024) 静默截断或拒绝，防止 OOM
- 指标名称冲突自动解决：标签集不一致时拒绝记录（不崩溃）
- 多进程模式初始化失败时回退到单进程模式

资源管理：
- 指标注册表内存常驻，可通过 reset() 释放（仅测试环境）
- 所有字典操作在锁内完成，防止竞态条件
"""

import logging
import math
import os
import re
import threading
from typing import Dict, Any, Optional, List, Tuple, Union

logger = logging.getLogger(__name__)

# ── 版本与许可 ────────────────────────────────────────────
VERSION = "6.0.0"
SPDX_IDENTIFIER = "Apache-2.0"

# ── 常量 ──────────────────────────────────────────────────
DEFAULT_NAMESPACE = "spark"
DEFAULT_SUBSYSTEM = "core"
MAX_LABEL_COUNT = 20                # 防止标签爆炸
MAX_LABEL_VALUE_LENGTH = 512        # 标签值最大长度
MAX_METRIC_NAME_LENGTH = 200        # 指标全名最大字符数
MAX_TOTAL_METRICS = 5000            # 全局最大注册指标数，防止内存耗尽
METRICS_AVAILABLE = False
PROMETHEUS_IMPORTED = False

# ── 预定义桶 ──────────────────────────────────────────────
# 延迟桶（秒）
LATENCY_BUCKETS = (
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025,
    0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0
)

# 吞吐量桶（events/s）
THROUGHPUT_BUCKETS = (
    1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000
)

# 盈亏百分比桶（只保留非负部分，观测值可为负，负值落入 -Inf 桶）
PNL_PCT_BUCKETS = (
    0.0, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0
)

DEFAULT_BUCKETS = LATENCY_BUCKETS

BUCKET_PRESETS = {
    "latency": LATENCY_BUCKETS,
    "throughput": THROUGHPUT_BUCKETS,
    "pnl_pct": PNL_PCT_BUCKETS,
}

# ── 动态导入 Prometheus 客户端 ────────────────────────────
try:
    from prometheus_client import (
        Counter,
        Gauge,
        Histogram,
        Summary,
        CollectorRegistry,
        generate_latest,
        REGISTRY,
        MultiprocessCollector,
    )
    PROMETHEUS_IMPORTED = True
    METRICS_AVAILABLE = True
except ImportError:
    class _MissingType:
        pass
    Counter = _MissingType
    Gauge = _MissingType
    Histogram = _MissingType
    Summary = _MissingType
    CollectorRegistry = _MissingType
    generate_latest = None
    REGISTRY = None
    MultiprocessCollector = None
    logger.warning("prometheus_client 未安装，指标功能降级为空操作")


class _MetricDef:
    """内部指标描述符，不可变"""
    __slots__ = ('metric_type', 'name', 'labelnames', 'buckets', 'collector')

    def __init__(self, metric_type, name: str, labelnames: Tuple[str, ...], buckets=None):
        self.metric_type = metric_type
        self.name = name
        self.labelnames = labelnames
        self.buckets = buckets
        self.collector = None


class MetricsCollector:
    """
    指标收集器（线程安全单例，支持测试重置）

    用法:
        MetricsCollector.counter("trades_total", 1, {"side": "buy"})
        MetricsCollector.gauge("position_size", 100.0, {"symbol": "btcusdt"})
        MetricsCollector.histogram("latency_s", 0.003, {"operation": "order"}, buckets="latency")
    """

    DEFAULT_NAMESPACE = DEFAULT_NAMESPACE
    DEFAULT_SUBSYSTEM = DEFAULT_SUBSYSTEM
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._initialized = False
                    cls._instance = instance
        return cls._instance

    def __init__(self, namespace: str = DEFAULT_NAMESPACE, subsystem: str = DEFAULT_SUBSYSTEM,
                 registry: Optional[CollectorRegistry] = None):
        # 防止重复初始化
        if self._initialized:
            return
        with self._lock:
            if self._initialized:
                return
            self._initialized = True

            self.namespace = self._sanitize_name(namespace) if namespace else DEFAULT_NAMESPACE
            self.subsystem = self._sanitize_name(subsystem) if subsystem else DEFAULT_SUBSYSTEM
            self._registry = registry or REGISTRY
            self._metrics: Dict[str, _MetricDef] = {}
            self._error_counter = 0
            self._error_lock = threading.Lock()
            self._self_metric_errors = None
            self._self_metric_info = None

            # 多进程模式初始化
            self._init_multiprocess()
            # 注册内部指标
            self._register_self_metrics()

            logger.info("MetricsCollector v%s 初始化: ns=%s, sub=%s, multiproc=%s",
                        VERSION, self.namespace, self.subsystem,
                        bool(os.environ.get("PROMETHEUS_MULTIPROC_DIR")))

    # ── 公共类方法 ──────────────────────────────────────────

    @classmethod
    def reset(cls) -> None:
        """重置单例（仅用于测试环境）"""
        with cls._lock:
            if cls._instance is not None:
                cls._instance._cleanup()
                cls._instance = None

    @classmethod
    def counter(cls, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        """
        递增计数器
        Args:
            name: 指标名称（不含前缀）
            value: 增量（须非负）
            labels: 维度标签
        """
        inst = cls._get_instance()
        inst._record(Counter, name, value, labels, "counter")

    @classmethod
    def gauge(cls, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """设置仪表值"""
        inst = cls._get_instance()
        inst._record(Gauge, name, value, labels, "gauge")

    @classmethod
    def histogram(cls, name: str, value: float, labels: Optional[Dict[str, str]] = None,
                  buckets: Optional[Union[str, tuple]] = None) -> None:
        """
        记录直方图观测值
        Args:
            buckets: 预定义桶名 ("latency","throughput","pnl_pct") 或自定义递增正数元组
        """
        inst = cls._get_instance()
        resolved_buckets = cls._resolve_buckets(buckets)
        if resolved_buckets is None:
            inst._inc_error("无效的桶参数")
            return
        inst._record(Histogram, name, value, labels, "histogram", buckets=resolved_buckets)

    @classmethod
    def summary(cls, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """记录摘要观测值"""
        inst = cls._get_instance()
        inst._record(Summary, name, value, labels, "summary")

    @classmethod
    def get_metrics_text(cls) -> str:
        """返回 Prometheus 文本格式"""
        if not METRICS_AVAILABLE or not generate_latest:
            return ""
        inst = cls._get_instance()
        try:
            return generate_latest(inst._registry).decode('utf-8')
        except Exception:
            logger.exception("指标文本生成失败")
            return ""

    @classmethod
    def get_metrics_response(cls) -> Tuple[str, int, Dict[str, str]]:
        """返回适合 HTTP 响应的 (body, status_code, headers)"""
        body = cls.get_metrics_text()
        return body, 200, {"Content-Type": "text/plain; charset=utf-8"}

    @classmethod
    def health_check(cls) -> Dict[str, Any]:
        inst = cls._get_instance()
        warnings = []
        if not METRICS_AVAILABLE:
            warnings.append("prometheus_client 未安装")
        else:
            try:
                _ = generate_latest(inst._registry)
            except Exception as e:
                warnings.append(f"指标生成测试失败: {e}")
        with inst._error_lock:
            errors = inst._error_counter
        if errors > 0:
            warnings.append(f"内部错误计数: {errors}")
        status = "degraded" if warnings else "ok"
        return {
            "status": status,
            "reason": f"ns={inst.namespace}/{inst.subsystem}, errors={errors}, registered={len(inst._metrics)}",
            "warnings": warnings,
        }

    # ── 内部核心逻辑 ───────────────────────────────────────

    @classmethod
    def _resolve_buckets(cls, buckets: Optional[Union[str, tuple]]) -> Optional[tuple]:
        """解析桶参数，确保返回合法桶"""
        if buckets is None:
            return DEFAULT_BUCKETS
        if isinstance(buckets, str):
            return BUCKET_PRESETS.get(buckets, DEFAULT_BUCKETS)
        if isinstance(buckets, tuple):
            # 校验桶的合法性：必须为正数且严格递增
            if len(buckets) == 0:
                logger.warning("自定义桶为空，使用默认桶")
                return DEFAULT_BUCKETS
            prev = -1.0
            for b in buckets:
                if not isinstance(b, (int, float)) or b <= 0:
                    logger.warning("桶值必须为正数，得到: %s", b)
                    return None
                if b <= prev:
                    logger.warning("桶值必须严格递增，当前: %s, 前一个: %s", b, prev)
                    return None
                prev = b
            return buckets
        logger.warning("无效的桶参数类型: %s", type(buckets))
        return None

    @classmethod
    def _get_instance(cls) -> 'MetricsCollector':
        if cls._instance is not None:
            return cls._instance
        return cls()

    def _cleanup(self) -> None:
        """清理注册的指标（用于单例重置）"""
        if METRICS_AVAILABLE and self._registry:
            # 取消注册所有指标
            try:
                self._registry = None  # 帮助GC
            except Exception:
                pass
        self._metrics.clear()

    def _record(self, metric_type, name: str, value: float, labels: Optional[Dict[str, str]],
                kind: str, buckets: Optional[tuple] = None) -> None:
        if not METRICS_AVAILABLE or metric_type is None:
            return

        # 1. 清洗名称
        safe_name = self._sanitize_name(name)
        if not safe_name:
            self._inc_error("指标名称为空")
            return

        # 2. 构建全名并检查长度
        full_name = self._build_full_name(safe_name)
        if len(full_name) > MAX_METRIC_NAME_LENGTH:
            self._inc_error(f"指标全名过长: {full_name}")
            return

        # 3. 校验数值（含NaN/Inf）
        value = self._validate_value(value, kind)
        if value is None:
            return

        # 4. 校验并限制标签
        clean_labels = self._validate_labels(labels)

        # 5. 获取或注册指标定义（完全在锁内，避免竞态）
        metric_def = self._get_or_register_metric(metric_type, full_name, kind, clean_labels, buckets)
        if metric_def is None:
            return

        collector = metric_def.collector
        if collector is None:
            self._inc_error(f"指标收集器未初始化: {full_name}")
            return

        # 6. 执行记录
        try:
            if clean_labels:
                labeled = collector.labels(**clean_labels)
                if kind == "gauge":
                    labeled.set(value)
                elif kind == "counter":
                    labeled.inc(value)
                else:
                    labeled.observe(value)
            else:
                if kind == "gauge":
                    collector.set(value)
                elif kind == "counter":
                    collector.inc(value)
                else:
                    collector.observe(value)
        except Exception as e:
            self._inc_error(f"指标记录失败 [{kind}] {full_name}: {e}")

    def _sanitize_name(self, name: str) -> str:
        """清洗为 [a-zA-Z_][a-zA-Z0-9_]* 格式，保留唯一性提示"""
        if not name or not isinstance(name, str):
            return ""
        cleaned = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        cleaned = re.sub(r'^[^a-zA-Z]', '', cleaned)
        if not cleaned:
            cleaned = "unknown"
        return cleaned[:100]

    def _build_full_name(self, name: str) -> str:
        return f"{self.namespace}_{self.subsystem}_{name}"

    def _validate_value(self, value: Any, kind: str) -> Optional[float]:
        """返回安全的数值，无效时返回 None"""
        if not isinstance(value, (int, float)):
            self._inc_error(f"非数值类型: {type(value)}")
            return None
        if math.isnan(value) or math.isinf(value):
            self._inc_error(f"非法数值 (NaN/Inf): {value}")
            return None
        if kind == "counter" and value < 0:
            logger.warning("Counter 不接受负值: %s，置为0", value)
            return 0.0
        if not (-1e12 <= value <= 1e12):
            logger.warning("指标值超出范围: %s=%s，裁剪", kind, value)
            return max(-1e12, min(1e12, value))
        return float(value)

    def _validate_labels(self, labels: Optional[Dict[str, str]]) -> Dict[str, str]:
        """清洗并限制标签，防止基数爆炸，自动去重"""
        if labels is None:
            return {}
        if not isinstance(labels, dict):
            self._inc_error(f"标签参数应为字典，收到: {type(labels)}")
            return {}

        clean = {}
        seen_keys = set()
        for k, v in list(labels.items()):
            if not isinstance(k, str) or not isinstance(v, str):
                self._inc_error(f"忽略非字符串标签: {k}={v}")
                continue
            k_clean = re.sub(r'[^a-zA-Z0-9_]', '_', k)
            if not k_clean:
                continue
            # 去重：如果清洗后键已存在，忽略后续
            if k_clean in seen_keys:
                continue
            seen_keys.add(k_clean)
            v_clean = v[:MAX_LABEL_VALUE_LENGTH]
            clean[k_clean] = v_clean

        if len(clean) > MAX_LABEL_COUNT:
            logger.warning("标签数量 %d 超过上限 %d，截断", len(clean), MAX_LABEL_COUNT)
            clean = dict(list(clean.items())[:MAX_LABEL_COUNT])

        return clean

    def _get_or_register_metric(self, metric_type, full_name: str, kind: str,
                                labels: Dict[str, str], buckets: Optional[tuple]) -> Optional[_MetricDef]:
        """获取或注册指标定义，线程安全，指标总数限制，标签不一致拒绝"""
        provided_labels = set(labels.keys()) if labels else set()

        with self._lock:
            if full_name in self._metrics:
                existing = self._metrics[full_name]
                expected_labels = set(existing.labelnames)
                if expected_labels != provided_labels:
                    self._inc_error(f"指标 {full_name} 标签集不一致: 期望 {expected_labels}, 提供 {provided_labels}")
                    return None
                return existing

            # 检查总数限制
            if len(self._metrics) >= MAX_TOTAL_METRICS:
                self._inc_error(f"指标注册数已达上限 {MAX_TOTAL_METRICS}，拒绝注册 {full_name}")
                return None

            # 新注册
            labelnames = tuple(sorted(provided_labels)) if provided_labels else ()
            try:
                if metric_type == Counter:
                    collector = Counter(full_name, f"{kind} {full_name}",
                                        labelnames=labelnames, registry=self._registry)
                elif metric_type == Gauge:
                    collector = Gauge(full_name, f"{kind} {full_name}",
                                      labelnames=labelnames, registry=self._registry)
                elif metric_type == Histogram:
                    # 确保桶合法
                    if buckets is None:
                        buckets = DEFAULT_BUCKETS
                    collector = Histogram(full_name, f"{kind} {full_name}",
                                          labelnames=labelnames, buckets=buckets,
                                          registry=self._registry)
                elif metric_type == Summary:
                    collector = Summary(full_name, f"{kind} {full_name}",
                                        labelnames=labelnames, registry=self._registry)
                else:
                    return None
            except Exception as e:
                self._inc_error(f"指标注册失败 {full_name}: {e}")
                return None

            metric_def = _MetricDef(metric_type, full_name, labelnames, buckets)
            metric_def.collector = collector
            self._metrics[full_name] = metric_def
            return metric_def

    def _init_multiprocess(self) -> None:
        """初始化多进程支持，添加进程标识避免文件冲突"""
        if not PROMETHEUS_IMPORTED:
            return
        mp_dir = os.environ.get("PROMETHEUS_MULTIPROC_DIR")
        if not mp_dir:
            return
        if not os.path.isdir(mp_dir):
            logger.error("PROMETHEUS_MULTIPROC_DIR 目录不存在: %s", mp_dir)
            return
        if not os.access(mp_dir, os.W_OK):
            logger.error("PROMETHEUS_MULTIPROC_DIR 目录不可写: %s", mp_dir)
            return
        # 设置进程唯一标识，避免文件覆盖（如果未设置）
        if not os.environ.get("PROMETHEUS_MULTIPROC_ID"):
            pid = os.getpid()
            os.environ["PROMETHEUS_MULTIPROC_ID"] = str(pid)
            logger.info("自动设置 PROMETHEUS_MULTIPROC_ID=%d", pid)
        try:
            self._registry = CollectorRegistry()
            MultiprocessCollector(self._registry)
            logger.info("多进程指标模式已启用: %s", mp_dir)
        except Exception as e:
            logger.error("多进程指标初始化失败，回退单进程: %s", e)

    def _register_self_metrics(self) -> None:
        """注册自身健康指标"""
        if not METRICS_AVAILABLE:
            return
        try:
            self._self_metric_errors = Counter(
                f"{self.namespace}_{self.subsystem}_metrics_errors_total",
                "MetricsCollector internal errors",
                registry=self._registry
            )
            info_gauge = Gauge(
                f"{self.namespace}_{self.subsystem}_metrics_info",
                "MetricsCollector version info",
                labelnames=("version",),
                registry=self._registry
            )
            info_gauge.labels(version=VERSION).set(1)
            self._self_metric_info = info_gauge
        except Exception as e:
            logger.warning("注册内部指标失败: %s", e)

    def _inc_error(self, msg: str = "") -> None:
        """线程安全内部错误计数，更新自身错误指标"""
        try:
            with self._error_lock:
                self._error_counter += 1
                if self._error_counter % 100 == 0:
                    logger.warning("指标收集器内部错误计数: %d, 最新: %s", self._error_counter, msg)
            if self._self_metric_errors is not None:
                self._self_metric_errors.inc()
        except Exception:
            pass
