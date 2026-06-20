#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 执行模块性能基准测试 (BenchmarkExecution) v6.0.0

核心职责：
1. 对 ExecutionPlanner / ExecutionAlgo 进行微基准及吞吐量测试
2. 测量延迟分布（P50/P95/P99）、吞吐量、内存占用，含置信区间
3. 输出标准化 JSON 报告，可被 CI/CD 直接消费
4. 严格的资源隔离、异常保护、可配置性与环境降级

外部依赖：
- core.execution_planner.ExecutionPlanner
- core.execution_algo.ExecutionAlgo
- 内建轻量 Mock 替代 order_manager / risk_manager / event_bus

接口契约：
- run(config: Optional[Dict] = None) -> Dict[str, Any]
  返回包含 status、timestamp、planner_latency 等字段的报告

异常与降级：
- 任何子测试异常均被捕获，标记为 error 并继续
- 依赖缺失时整体状态为 error
- 若 tracemalloc 不可用，内存测试自动跳过

资源管理：
- 每个子测试使用 try-finally 确保创建的计划被清理
- 内存测试前后控制 GC，避免干扰；always 在 finally 停止 tracemalloc
- 最后统一停止 planner/algo 后台线程
"""

import copy
import gc
import json
import logging
import math
import os
import platform
import statistics
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

VERSION = "6.0.0"
SPDX_IDENTIFIER = "Apache-2.0"

# ── 可选依赖 ──────────────────────────────────────────────
try:
    from core.execution_planner import ExecutionPlanner
except ImportError:
    ExecutionPlanner = None
try:
    from core.execution_algo import ExecutionAlgo
except ImportError:
    ExecutionAlgo = None

TRAcemalloc_AVAILABLE = True
try:
    import tracemalloc
except ImportError:
    TRAcemalloc_AVAILABLE = False

# ── 默认测试参数（均可在 config 中覆盖）──────────────────────
DEFAULT_NUM_WARMUP = 100
DEFAULT_NUM_SAMPLES = 500
DEFAULT_BULK_SIZE = 100
DEFAULT_ORDER_QUANTITY = 1.0
DEFAULT_ORDER_PRICE = 50000.0
DEFAULT_TOTAL_TIME_SEC = 30.0
DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_SIDE = "BUY"
DEFAULT_MAX_PLANS_MEM = 200
DEFAULT_QUIET = False
DEFAULT_CONFIDENCE_LEVEL = 0.95
DEFAULT_MAX_MEMORY_MB = 500
MIN_SAMPLES_FOR_CONFIDENCE = 10
MIN_SAMPLES_FOR_STDEV = 2

# ── 正态分位数近似常量（常见置信度） ─────────────────────────
_Z_VALUES = {
    0.80: 1.2815515655446004,
    0.85: 1.4395314709384563,
    0.90: 1.6448536269514722,
    0.95: 1.959963984540054,
    0.975: 2.241402727604947,
    0.99: 2.5758293035489004,
    0.995: 2.8070337683438043,
    0.999: 3.2905267314918916,
}


class BenchmarkExecution:
    """执行模块性能基准测试（机构级）"""

    def __init__(self):
        self.planner = None
        self.algo = None
        self._created_plan_ids: List[str] = []
        self._config: Dict[str, Any] = {}
        self._lock = threading.Lock()

    # ── 公共接口 ──────────────────────────────────────────

    def run(self, config: Optional[Dict] = None) -> Dict[str, Any]:
        if config is not None and not isinstance(config, dict):
            return {"status": "error", "reason": "config must be a dict or None",
                    "timestamp": time.time()}

        self._config = copy.deepcopy(config) if config else {}
        logger.info("Execution benchmark started")

        if not ExecutionPlanner or not ExecutionAlgo:
            return {
                "status": "error",
                "reason": "ExecutionPlanner or ExecutionAlgo not available",
                "timestamp": time.time(),
            }

        try:
            self._init_components()
        except Exception as e:
            logger.exception("Component initialization failed")
            return {"status": "error", "reason": str(e), "timestamp": time.time()}

        report: Dict[str, Any] = {
            "status": "ok",
            "timestamp": time.time(),
            "version": VERSION,
            "system_info": self._collect_system_info(),
            "config": self._export_config(),
        }

        sub_tests = [
            ("planner_latency", self._benchmark_planner, "planner"),
            ("algo_throughput", self._benchmark_algo_throughput, "throughput"),
            ("memory_usage", self._benchmark_memory, "memory"),
            ("combined_latency", self._benchmark_combined, "combined"),
        ]
        for key, func, name in sub_tests:
            start = time.perf_counter()
            result = self._run_sub_test(func, name)
            elapsed = time.perf_counter() - start
            result["elapsed_sec"] = round(elapsed, 6)
            report[key] = result

        peak_kb = self._safe_get(report, "memory_usage.peak_memory_kb", 0.0)
        if isinstance(peak_kb, (int, float)) and peak_kb > self._cfg("max_memory_mb", DEFAULT_MAX_MEMORY_MB) * 1024:
            report.setdefault("warnings", []).append(
                f"Memory peak ({peak_kb / 1024:.1f} MB) exceeded threshold"
            )

        self._shutdown_services()
        self._output_report(report)
        return report

    # ── 配置辅助 ──────────────────────────────────────────

    def _cfg(self, key: str, default: Any) -> Any:
        return self._config.get(key, default)

    def _export_config(self) -> Dict[str, Any]:
        """导出完整配置（脱敏）"""
        return {
            "num_warmup": self._cfg("num_warmup", DEFAULT_NUM_WARMUP),
            "num_samples": self._cfg("num_samples", DEFAULT_NUM_SAMPLES),
            "bulk_size": self._cfg("bulk_size", DEFAULT_BULK_SIZE),
            "order_quantity": self._cfg("order_quantity", DEFAULT_ORDER_QUANTITY),
            "order_price": self._cfg("order_price", DEFAULT_ORDER_PRICE),
            "total_time_sec": self._cfg("total_time_sec", DEFAULT_TOTAL_TIME_SEC),
            "symbol": self._cfg("symbol", DEFAULT_SYMBOL),
            "side": self._cfg("side", DEFAULT_SIDE),
            "mem_plans": self._cfg("mem_plans", DEFAULT_MAX_PLANS_MEM),
            "confidence_level": self._cfg("confidence_level", DEFAULT_CONFIDENCE_LEVEL),
            "max_memory_mb": self._cfg("max_memory_mb", DEFAULT_MAX_MEMORY_MB),
            "quiet": self._cfg("quiet", DEFAULT_QUIET),
            "json_output": self._cfg("json_output", None),
        }

    @staticmethod
    def _safe_get(report: Dict, dotted_key: str, default: Any) -> Any:
        keys = dotted_key.split('.')
        obj = report
        for k in keys:
            if isinstance(obj, dict):
                obj = obj.get(k, default)
            else:
                return default
        return obj

    # ── 系统信息收集 ──────────────────────────────────────

    @staticmethod
    def _collect_system_info() -> Dict[str, Any]:
        info = {
            "python_version": sys.version,
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
        }
        try:
            import psutil
            freq = psutil.cpu_freq()
            if freq:
                info["cpu_freq_mhz"] = freq.current
            mem = psutil.virtual_memory()
            info["total_memory_mb"] = round(mem.total / (1024 * 1024), 1)
        except Exception:
            pass
        return info

    # ── 初始化 ────────────────────────────────────────────

    def _init_components(self) -> None:
        class _MockRiskManager:
            def approve_order(self, order):
                return True, "mock_approved"

        class _MockOrderManager:
            def submit_order(self, order):
                return {"status": "ok", "order_id": f"mock_{time.time_ns()}"}
            def get_order(self, oid):
                return {"status": "filled", "filled_qty": DEFAULT_ORDER_QUANTITY}
            def cancel_order(self, oid, symbol=""):
                return {"status": "ok"}

        class _MockEventBus:
            def publish(self, *args, **kwargs):
                pass

        risk_mgr = _MockRiskManager()
        order_mgr = _MockOrderManager()
        event_bus = _MockEventBus()

        self.planner = ExecutionPlanner(
            risk_manager=risk_mgr,
            order_manager=order_mgr,
            event_bus=event_bus,
        )
        self.algo = ExecutionAlgo(
            order_manager=order_mgr,
            risk_manager=risk_mgr,
            execution_planner=self.planner,
            event_bus=event_bus,
        )

    # ── 统一子测试包装 ────────────────────────────────────

    def _run_sub_test(self, func: Callable, name: str) -> Dict[str, Any]:
        logger.debug("Running sub-test: %s", name)
        try:
            return func()
        except Exception as e:
            logger.exception("Sub-test [%s] failed: %s", name, e)
            return {"status": "error", "reason": str(e)}
        finally:
            self._cleanup_plans()

    # ── 计划生成与跟踪（线程安全）──────────────────────────

    def _make_order(self) -> Dict:
        return {
            "symbol": self._cfg("symbol", DEFAULT_SYMBOL),
            "side": self._cfg("side", DEFAULT_SIDE),
            "quantity": self._cfg("order_quantity", DEFAULT_ORDER_QUANTITY),
            "price": self._cfg("order_price", DEFAULT_ORDER_PRICE),
        }

    def _plan_and_track(self) -> Optional[str]:
        res = self.planner.plan(
            self._make_order(),
            total_time_sec=self._cfg("total_time_sec", DEFAULT_TOTAL_TIME_SEC),
        )
        if isinstance(res, dict) and res.get("status") == "ok":
            pid = res["plan_id"]
            with self._lock:
                self._created_plan_ids.append(pid)
            return pid
        return None

    def _cleanup_plans(self) -> None:
        with self._lock:
            pids_to_clean = self._created_plan_ids[:]
            self._created_plan_ids.clear()
        for pid in pids_to_clean:
            try:
                if self.planner is not None:
                    self.planner.cancel_plan(pid)
            except Exception:
                pass

    def _shutdown_services(self) -> None:
        for service in (self.algo, self.planner):
            if service and hasattr(service, 'stop'):
                try:
                    service.stop()
                except Exception:
                    pass

    # ── 子测试实现 ────────────────────────────────────────

    def _benchmark_planner(self) -> Dict[str, Any]:
        warmup = self._cfg("num_warmup", DEFAULT_NUM_WARMUP)
        samples = self._cfg("num_samples", DEFAULT_NUM_SAMPLES)

        # 预热
        for _ in range(warmup):
            self._plan_and_track()
        self._cleanup_plans()

        latencies = []
        failed = 0
        for _ in range(samples):
            gc.collect()
            start = time.perf_counter_ns()
            pid = self._plan_and_track()
            end = time.perf_counter_ns()
            if pid:
                latencies.append((end - start) / 1e3)
            else:
                failed += 1

        result = self._calc_percentiles(latencies)
        result["plan_failures"] = failed
        return result

    def _benchmark_algo_throughput(self) -> Dict[str, Any]:
        bulk = self._cfg("bulk_size", DEFAULT_BULK_SIZE)

        plan_ids = []
        for _ in range(bulk):
            pid = self._plan_and_track()
            if pid:
                plan_ids.append(pid)

        if not plan_ids:
            return {"status": "error", "reason": "no valid plans"}

        gc.collect()
        start = time.perf_counter_ns()
        for pid in plan_ids:
            try:
                self.algo.execute_plan(pid)
            except Exception:
                pass
            finally:
                try:
                    self.algo.cancel(pid)
                except Exception:
                    pass
        elapsed_ns = time.perf_counter_ns() - start
        elapsed_sec = elapsed_ns / 1e9

        throughput = len(plan_ids) / elapsed_sec if elapsed_sec > 0 else 0.0
        return {
            "throughput_plans_per_sec": round(throughput, 2),
            "total_plans": len(plan_ids),
            "elapsed_sec": round(elapsed_sec, 6),
        }

    def _benchmark_memory(self) -> Dict[str, Any]:
        if not TRAcemalloc_AVAILABLE:
            return {"status": "skipped", "reason": "tracemalloc not available"}

        gc.collect()
        gc_enabled = gc.isenabled()
        if gc_enabled:
            gc.disable()
        try:
            if not tracemalloc.is_tracing():
                tracemalloc.start()
            _ = tracemalloc.take_snapshot()

            num_plans = self._cfg("mem_plans", DEFAULT_MAX_PLANS_MEM)
            for _ in range(num_plans):
                res = self.planner.plan(
                    self._make_order(),
                    total_time_sec=self._cfg("total_time_sec", DEFAULT_TOTAL_TIME_SEC),
                )
                if isinstance(res, dict) and res.get("status") == "ok":
                    with self._lock:
                        self._created_plan_ids.append(res["plan_id"])

            current, peak = tracemalloc.get_traced_memory()
        finally:
            try:
                if tracemalloc.is_tracing():
                    tracemalloc.stop()
            except Exception:
                pass
            if gc_enabled:
                gc.enable()

        return {
            "current_memory_kb": round(current / 1024, 2),
            "peak_memory_kb": round(peak / 1024, 2),
        }

    def _benchmark_combined(self) -> Dict[str, Any]:
        samples = self._cfg("num_samples", DEFAULT_NUM_SAMPLES)
        latencies = []
        failed = 0
        for _ in range(samples):
            gc.collect()
            start = time.perf_counter_ns()
            pid = self._plan_and_track()
            if pid:
                try:
                    self.algo.execute_plan(pid)
                except Exception:
                    pass
                finally:
                    try:
                        self.algo.cancel(pid)
                    except Exception:
                        pass
            end = time.perf_counter_ns()
            if pid:
                latencies.append((end - start) / 1e3)
            else:
                failed += 1

        result = self._calc_percentiles(latencies)
        result["plan_failures"] = failed
        return result

    # ── 统计工具 ──────────────────────────────────────────

    def _calc_percentiles(self, latencies: List[float]) -> Dict[str, Any]:
        if not latencies:
            return {"status": "error", "reason": "no data", "samples": 0}

        n = len(latencies)
        sorted_lat = sorted(latencies)
        p50 = sorted_lat[max(0, min(n - 1, int(n * 0.5)))]
        p95 = sorted_lat[max(0, min(n - 1, int(n * 0.95)))]
        p99 = sorted_lat[max(0, min(n - 1, int(n * 0.99)))]

        mean_val = statistics.mean(latencies) if n > 0 else 0.0
        stdev_val = statistics.stdev(latencies) if n >= MIN_SAMPLES_FOR_STDEV else 0.0

        conf_level = self._cfg("confidence_level", DEFAULT_CONFIDENCE_LEVEL)
        if not (0 < conf_level < 1):
            conf_level = DEFAULT_CONFIDENCE_LEVEL
        ci_key = f"ci_{int(conf_level*100)}"

        z = self._get_z(conf_level)
        if n >= MIN_SAMPLES_FOR_CONFIDENCE and stdev_val > 0:
            ci_half_width = z * stdev_val / math.sqrt(n)
            ci_lower = mean_val - ci_half_width
            ci_upper = mean_val + ci_half_width
        else:
            ci_lower = mean_val
            ci_upper = mean_val

        result = {
            "p50_us": round(p50, 2),
            "p95_us": round(p95, 2),
            "p99_us": round(p99, 2),
            "mean_us": round(mean_val, 2),
            "std_us": round(stdev_val, 2),
            ci_key + "_lower_us": round(ci_lower, 2),
            ci_key + "_upper_us": round(ci_upper, 2),
            "max_us": round(max(latencies), 2),
            "min_us": round(min(latencies), 2),
            "samples": n,
        }
        return result

    @staticmethod
    def _get_z(conf_level: float) -> float:
        """获取标准正态分布的 (1+conf_level)/2 分位数"""
        p = (1 + conf_level) / 2
        # 查找最近的已知值
        best = 1.96  # 默认
        best_diff = float('inf')
        for key, val in _Z_VALUES.items():
            diff = abs((1 + key) / 2 - p)
            if diff < best_diff:
                best_diff = diff
                best = val
        # 如果非常接近某个已知值，直接返回；否则通过近似公式
        if best_diff < 0.005:
            return best
        # 使用通用的逆误差函数近似
        try:
            from math import erfcinv
            return -math.sqrt(2) * erfcinv(2 * p)
        except ImportError:
            # 回退到最佳猜测
            return best

    # ── 报告输出 ──────────────────────────────────────────

    def _output_report(self, report: Dict) -> None:
        quiet = self._cfg("quiet", DEFAULT_QUIET)
        if not quiet:
            lines = [
                "=== Execution Benchmark Summary ===",
                f"  Planner  P50: {self._safe_get(report, 'planner_latency.p50_us', 'N/A')} us",
                f"           P99: {self._safe_get(report, 'planner_latency.p99_us', 'N/A')} us",
                f"  Algo throughput: {self._safe_get(report, 'algo_throughput.throughput_plans_per_sec', 'N/A')} plans/s",
                f"  Memory peak: {self._safe_get(report, 'memory_usage.peak_memory_kb', 'N/A')} KB",
                f"  Combined P99: {self._safe_get(report, 'combined_latency.p99_us', 'N/A')} us",
            ]
            logger.info("\n".join(lines))

        json_path = self._cfg("json_output", None)
        if json_path:
            dir_name = os.path.dirname(json_path)
            if dir_name:
                os.makedirs(dir_name, exist_ok=True)
            try:
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(report, f, indent=2, default=str)
                logger.info("Benchmark report written to %s", json_path)
            except Exception as e:
                logger.error("Failed to write JSON report: %s", e)


# ── 独立运行 ──────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    bench = BenchmarkExecution()
    result = bench.run()
    if result.get("status") != "ok":
        sys.exit(1)
