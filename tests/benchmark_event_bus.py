#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 事件总线性能基准测试 (BenchmarkEventBus) v13.0.0

核心职责：
1. 测量 EventBus 端到端延迟、吞吐量、并发性能
2. 覆盖背压、多订阅者、高负载等边界场景
3. 提供详尽、可复现的基准报告，包含精确的系统环境信息

运行方式:
    python tests/benchmark_event_bus.py [--json] [--iterations N] [--warmup N] [--duration S]
"""

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

VERSION = "13.0.0"
SPDX_IDENTIFIER = "Apache-2.0"

# 尝试导入被测模块
try:
    from core.event_bus import EventBus, EventTypes
    EVENT_BUS_AVAILABLE = True
except ImportError:
    EventBus = None
    EventTypes = None
    EVENT_BUS_AVAILABLE = False

# 可选环境信息模块
try:
    import psutil as _psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    _psutil = None
    PSUTIL_AVAILABLE = False

# 基准测试可配置参数
DEFAULT_WARMUP_ITERATIONS = 1000
DEFAULT_MEASURE_ITERATIONS = 10000
MAX_MEASURE_ITERATIONS = 1000000
DEFAULT_THROUGHPUT_DURATION_SEC = 2.0
DEFAULT_MAX_SUBSCRIBERS = 10
DEFAULT_THREAD_POOL_SIZE = 4
DEFAULT_EVENTS_PER_THREAD = 5000
MAX_EVENTS_PER_THREAD = 100000
MAX_THREAD_POOL_SIZE = 256
LATENCY_PERCENTILES = (50, 90, 95, 99)

# 操作超时与等待参数
MAX_QUEUE_DRAIN_WAIT_SEC = 10.0
BACKLOG_POLL_INTERVAL_SEC = 0.001
SUBSYSTEM_STARTUP_GRACE_SEC = 0.1
CALLBACK_TIMEOUT_SEC = 5.0
BARRIER_TIMEOUT_SEC = 10.0
MIN_QUEUE_DRAIN_FALLBACK_SEC = 0.5
BUS_STOP_TIMEOUT_SEC = 3.0
TEST_ISOLATION_SLEEP_SEC = 0.2

# 测试负载
EVENT_PAYLOAD_SMALL = {"price": 10000.0, "qty": 1.0}
EVENT_PAYLOAD_LARGE = {
    "depth": {
        "bids": [[10000.0, 1.0]] * 50,
        "asks": [[10001.0, 1.0]] * 50
    }
}


class BenchmarkEventBus:
    """事件总线性能基准测试套件（机构级）"""

    def __init__(self,
                 warmup: int = DEFAULT_WARMUP_ITERATIONS,
                 measure: int = DEFAULT_MEASURE_ITERATIONS,
                 throughput_duration: float = DEFAULT_THROUGHPUT_DURATION_SEC):
        if not EVENT_BUS_AVAILABLE:
            raise RuntimeError("EventBus 不可用，无法运行基准测试")
        if not hasattr(EventBus, 'subscribe') or not hasattr(EventBus, 'publish') or not hasattr(EventBus, 'unsubscribe'):
            raise RuntimeError("EventBus 接口不兼容，缺少必要方法")
        if EventTypes is None or not hasattr(EventTypes, 'HEARTBEAT'):
            raise RuntimeError("EventTypes.HEARTBEAT 不存在")
        self.warmup = max(0, warmup)
        self.measure = max(1, min(measure, MAX_MEASURE_ITERATIONS))
        self.throughput_duration = max(0.1, throughput_duration)
        self._create_bus = lambda: EventBus(use_background=True, max_queue_size=200000)

    # ── 辅助方法 ──────────────────────────────────────────

    @staticmethod
    def _drain_queue(bus: EventBus, timeout: float = MAX_QUEUE_DRAIN_WAIT_SEC) -> bool:
        """等待事件总线队列排空"""
        if not hasattr(bus, 'backlog_size'):
            time.sleep(MIN_QUEUE_DRAIN_FALLBACK_SEC)
            return True  # 无法精确检测，等待后返回 True
        deadline = time.perf_counter() + timeout
        while time.perf_counter() < deadline:
            try:
                if bus.backlog_size() == 0:
                    return True
            except Exception:
                pass
            time.sleep(BACKLOG_POLL_INTERVAL_SEC)
        return False

    @classmethod
    def _validate_bus_functionality(cls, bus: EventBus) -> bool:
        """验证事件总线基本功能：发布、分发、回调执行、取消订阅"""
        if not hasattr(bus, 'subscribe') or not hasattr(bus, 'publish') or not hasattr(bus, 'unsubscribe'):
            return False
        received = []
        event = threading.Event()

        def callback(data):
            received.append(data)
            event.set()

        bus.subscribe(EventTypes.HEARTBEAT, callback)
        ok = bus.publish(EventTypes.HEARTBEAT, {"test": 1})
        if not ok:
            bus.unsubscribe(EventTypes.HEARTBEAT, callback)
            return False
        if not event.wait(timeout=CALLBACK_TIMEOUT_SEC):
            bus.unsubscribe(EventTypes.HEARTBEAT, callback)
            return False
        bus.unsubscribe(EventTypes.HEARTBEAT, callback)
        # 等待残余事件并验证取消订阅
        cls._drain_queue(bus, timeout=2.0)
        received_before = len(received)
        bus.publish(EventTypes.HEARTBEAT, {"test": 2})
        cls._drain_queue(bus, timeout=2.0)
        return len(received) == received_before and received[0] == {"test": 1}

    @staticmethod
    def _compute_stats(latencies: List[float]) -> Dict[str, float]:
        if not latencies:
            return {"min": 0.0, "max": 0.0, "mean": 0.0, "median": 0.0,
                    "stdev": 0.0, "count": 0}
        sorted_lat = sorted(latencies)
        n = len(sorted_lat)
        median_val = statistics.median(sorted_lat)
        stats = {
            "min": sorted_lat[0],
            "max": sorted_lat[-1],
            "mean": statistics.mean(sorted_lat),
            "median": median_val,
            "stdev": statistics.stdev(sorted_lat) if n >= 2 else 0.0,
            "count": n,
        }
        for p in LATENCY_PERCENTILES:
            if p == 50:
                stats[f"p{p}"] = median_val
                continue
            if n == 1:
                stats[f"p{p}"] = sorted_lat[0]
                continue
            k = (n - 1) * p / 100.0
            f = int(math.floor(k))
            c = int(math.ceil(k))
            if f == c:
                stats[f"p{p}"] = sorted_lat[f]
            else:
                stats[f"p{p}"] = sorted_lat[f] * (c - k) + sorted_lat[c] * (k - f)
        return stats

    @staticmethod
    def _collect_system_info() -> Dict[str, Any]:
        info: Dict[str, Any] = {
            "platform": platform.platform(),
            "python_version": platform.python_version(),
            "cpu_count_logical": os.cpu_count() or 0,
            "event_bus_version": getattr(EventBus, 'VERSION', 'N/A') if EVENT_BUS_AVAILABLE else 'N/A',
        }
        if PSUTIL_AVAILABLE and _psutil is not None:
            try:
                info["cpu_count_physical"] = _psutil.cpu_count(logical=False) or 0
                mem = _psutil.virtual_memory()
                info["total_memory_gb"] = round(mem.total / (1024**3), 2)
                info["available_memory_gb"] = round(mem.available / (1024**3), 2)
            except Exception:
                pass
        if sys.platform == "linux":
            try:
                with open("/proc/cpuinfo") as f:
                    for line in f:
                        if "model name" in line:
                            info["cpu_model"] = line.split(":")[-1].strip()
                            break
            except Exception:
                pass
        elif sys.platform == "darwin":
            try:
                import subprocess
                result = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"],
                                        capture_output=True, text=True)
                if result.returncode == 0:
                    info["cpu_model"] = result.stdout.strip()
            except Exception:
                pass
        return info

    def _new_bus(self) -> EventBus:
        bus = self._create_bus()
        bus.start()
        time.sleep(SUBSYSTEM_STARTUP_GRACE_SEC)
        return bus

    def _wait_drain_or_timeout(self, bus: EventBus, timeout: float = MAX_QUEUE_DRAIN_WAIT_SEC) -> Tuple[bool, int]:
        ok = self._drain_queue(bus, timeout)
        final_size = bus.backlog_size() if hasattr(bus, 'backlog_size') else -1
        if not ok:
            logger.warning("队列排空超时 (%.1fs), 最终队列大小: %d", timeout, final_size)
        return ok, final_size

    @staticmethod
    def _safe_unsubscribe(bus: EventBus, event_type: str, callback: Callable) -> None:
        try:
            if bus is not None and hasattr(bus, 'unsubscribe'):
                bus.unsubscribe(event_type, callback)
        except ValueError:
            pass
        except Exception as e:
            logger.debug("取消订阅异常: %s", e)

    def _safe_stop(self, bus: Optional[EventBus]) -> None:
        if bus is None:
            return
        try:
            if hasattr(bus, 'stop'):
                stopped = threading.Event()
                def _stop():
                    try:
                        bus.stop()
                    finally:
                        stopped.set()
                t = threading.Thread(target=_stop, daemon=False)
                t.start()
                if not stopped.wait(timeout=BUS_STOP_TIMEOUT_SEC):
                    logger.warning("总线停止超时")
        except Exception as e:
            logger.warning("总线停止异常: %s", e)

    # ── 基准测试方法 ──────────────────────────────────────

    def benchmark_publish_latency(self,
                                  payload: Optional[Dict] = None,
                                  iterations: Optional[int] = None) -> Dict[str, Any]:
        if payload is None:
            payload = EVENT_PAYLOAD_SMALL
        if iterations is None:
            iterations = self.measure
        test_start = time.perf_counter()
        bus = self._new_bus()
        received_flag = threading.Event()

        def callback(data):
            received_flag.set()

        bus.subscribe(EventTypes.HEARTBEAT, callback)

        warmup_failed = 0
        for _ in range(self.warmup):
            received_flag.clear()
            if not bus.publish(EventTypes.HEARTBEAT, payload):
                warmup_failed += 1
                continue
            received_flag.wait(timeout=CALLBACK_TIMEOUT_SEC)
        self._wait_drain_or_timeout(bus)

        latencies = []
        failed = 0
        timeouts = 0
        for _ in range(iterations):
            received_flag.clear()
            start = time.perf_counter()
            ok = bus.publish(EventTypes.HEARTBEAT, payload)
            if not ok:
                failed += 1
                continue
            if not received_flag.wait(timeout=CALLBACK_TIMEOUT_SEC):
                timeouts += 1
                continue
            latencies.append(time.perf_counter() - start)

        self._safe_unsubscribe(bus, EventTypes.HEARTBEAT, callback)
        self._safe_stop(bus)
        elapsed = time.perf_counter() - test_start
        stats = self._compute_stats(latencies)
        total_attempts = iterations
        success = total_attempts - failed - timeouts
        stats.update({
            "test_name": "publish_latency",
            "failed_publishes": failed,
            "callback_timeouts": timeouts,
            "warmup_failed": warmup_failed,
            "successful_measurements": success,
            "reliability_pct": success / max(1, total_attempts) * 100,
            "test_duration_sec": elapsed,
            "status": "ok" if (failed + timeouts) / max(1, total_attempts) < 0.01 else "degraded",
        })
        return stats

    def benchmark_publish_throughput(self,
                                     duration: Optional[float] = None,
                                     payload: Optional[Dict] = None) -> Dict[str, Any]:
        if duration is None:
            duration = self.throughput_duration
        if payload is None:
            payload = EVENT_PAYLOAD_SMALL
        test_start = time.perf_counter()
        bus = self._new_bus()
        processed = [0]
        lock = threading.Lock()

        def callback(data):
            with lock:
                processed[0] += 1

        bus.subscribe(EventTypes.HEARTBEAT, callback)

        warmup_failed = 0
        for _ in range(self.warmup):
            if not bus.publish(EventTypes.HEARTBEAT, payload):
                warmup_failed += 1
        self._wait_drain_or_timeout(bus)
        with lock:
            processed[0] = 0

        start = time.perf_counter()
        published = 0
        failed = 0
        while time.perf_counter() - start < duration:
            if bus.publish(EventTypes.HEARTBEAT, payload):
                published += 1
            else:
                failed += 1
            if published % 1000 == 0:
                time.sleep(0)
        drained, final_queue = self._wait_drain_or_timeout(bus)
        elapsed_time = time.perf_counter() - start
        with lock:
            processed_count = processed[0]

        self._safe_unsubscribe(bus, EventTypes.HEARTBEAT, callback)
        self._safe_stop(bus)
        test_elapsed = time.perf_counter() - test_start
        # 可靠标志：若无法检测 backlog，则仅依赖 drained 和失败数
        if final_queue == -1:
            reliable = drained and failed == 0
        else:
            reliable = drained and final_queue == 0 and failed == 0
        return {
            "test_name": "publish_throughput",
            "duration_sec": elapsed_time,
            "published": published,
            "failed_publishes": failed,
            "processed_by_subscriber": processed_count,
            "queue_fully_drained": drained,
            "final_queue_size": final_queue,
            "warmup_failed": warmup_failed,
            "reliable": reliable,
            "throughput_events_per_sec": processed_count / elapsed_time if elapsed_time > 0 else 0,
            "test_duration_sec": test_elapsed,
            "status": "ok" if reliable else "degraded",
        }

    def benchmark_multi_subscriber_latency(self,
                                           num_subscribers: int = DEFAULT_MAX_SUBSCRIBERS) -> Dict[str, Any]:
        if num_subscribers <= 0:
            num_subscribers = 1
        test_start = time.perf_counter()
        bus = self._new_bus()
        received_flag = threading.Event()
        counter = [0]
        lock = threading.Lock()

        def create_callback():
            def cb(data):
                with lock:
                    counter[0] += 1
                    if counter[0] >= num_subscribers:
                        received_flag.set()
            return cb

        callbacks = [create_callback() for _ in range(num_subscribers)]
        for cb in callbacks:
            bus.subscribe(EventTypes.HEARTBEAT, cb)

        warmup_failed = 0
        warmup_rounds = max(1, self.warmup // 10)
        for _ in range(warmup_rounds):
            counter[0] = 0
            received_flag.clear()
            if not bus.publish(EventTypes.HEARTBEAT, {"x": 1}):
                warmup_failed += 1
                continue
            received_flag.wait(timeout=CALLBACK_TIMEOUT_SEC)
        self._wait_drain_or_timeout(bus)

        latencies = []
        timeouts = 0
        failed = 0
        iter_count = max(1, self.measure // 10)
        if self.measure < 10:
            logger.warning("测量迭代次数 (%d) 小于 10，多订阅者测试结果可能不可靠", self.measure)
        for _ in range(iter_count):
            counter[0] = 0
            received_flag.clear()
            start = time.perf_counter()
            ok = bus.publish(EventTypes.HEARTBEAT, {"x": 1})
            if not ok:
                failed += 1
                continue
            if not received_flag.wait(timeout=CALLBACK_TIMEOUT_SEC):
                timeouts += 1
                continue
            latencies.append(time.perf_counter() - start)

        for cb in callbacks:
            self._safe_unsubscribe(bus, EventTypes.HEARTBEAT, cb)
        self._safe_stop(bus)
        elapsed = time.perf_counter() - test_start
        stats = self._compute_stats(latencies)
        total_attempts = iter_count
        success = total_attempts - failed - timeouts
        stats.update({
            "test_name": "multi_subscriber_latency",
            "timeout_count": timeouts,
            "failed_publishes": failed,
            "warmup_failed": warmup_failed,
            "successful_measurements": success,
            "reliability_pct": success / max(1, total_attempts) * 100,
            "test_duration_sec": elapsed,
            "status": "ok" if (failed + timeouts) / max(1, total_attempts) < 0.01 else "degraded",
        })
        return stats

    def benchmark_concurrent_publish(self,
                                     num_threads: int = DEFAULT_THREAD_POOL_SIZE,
                                     events_per_thread: int = DEFAULT_EVENTS_PER_THREAD) -> Dict[str, Any]:
        if num_threads < 2:
            num_threads = 2
        test_start = time.perf_counter()
        bus = self._new_bus()
        processed = [0]
        lock = threading.Lock()

        def callback(data):
            with lock:
                processed[0] += 1

        bus.subscribe(EventTypes.HEARTBEAT, callback)

        results: List[int] = []
        failures: List[int] = []
        lock_pub = threading.Lock()
        barrier = threading.Barrier(num_threads, timeout=BARRIER_TIMEOUT_SEC)
        barrier_ok = threading.Event()
        barrier_failures = 0
        barrier_lock = threading.Lock()

        def worker(events: int):
            nonlocal barrier_failures
            try:
                barrier.wait()
                barrier_ok.set()
            except threading.BrokenBarrierError:
                with barrier_lock:
                    barrier_failures += 1
                # 同步失败也继续执行，但数据可能受干扰
            local_pub = 0
            local_fail = 0
            for _ in range(events):
                if bus.publish(EventTypes.HEARTBEAT, {"x": 1}):
                    local_pub += 1
                else:
                    local_fail += 1
            with lock_pub:
                results.append(local_pub)
                failures.append(local_fail)

        threads = []
        start_time = time.perf_counter()
        for _ in range(num_threads):
            t = threading.Thread(target=worker, args=(events_per_thread,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()
        elapsed_time = time.perf_counter() - start_time

        self._wait_drain_or_timeout(bus)
        total_published = sum(results)
        total_failed = sum(failures)
        with lock:
            processed_count = processed[0]

        self._safe_unsubscribe(bus, EventTypes.HEARTBEAT, callback)
        self._safe_stop(bus)
        test_elapsed = time.perf_counter() - test_start
        return {
            "test_name": "concurrent_publish",
            "num_threads": num_threads,
            "threads_completed": len(results),
            "barrier_success": barrier_ok.is_set(),
            "barrier_failures": barrier_failures,
            "total_events_attempted": num_threads * events_per_thread,
            "total_published": total_published,
            "total_failed": total_failed,
            "processed_by_subscriber": processed_count,
            "duration_sec": elapsed_time,
            "throughput_events_per_sec": processed_count / elapsed_time if elapsed_time > 0 else 0,
            "test_duration_sec": test_elapsed,
            "status": "ok" if barrier_failures == 0 else "degraded",
        }

    def benchmark_backpressure(self,
                               max_queue: int = 1000,
                               events_to_push: int = 5000) -> Dict[str, Any]:
        if max_queue <= 0:
            max_queue = 1
        test_start = time.perf_counter()
        bus = EventBus(use_background=True, max_queue_size=max_queue)
        bus.start()
        time.sleep(SUBSYSTEM_STARTUP_GRACE_SEC)

        received = [0]
        lock = threading.Lock()

        def callback(data):
            with lock:
                received[0] += 1

        bus.subscribe(EventTypes.HEARTBEAT, callback)

        published = 0
        failed = 0
        for _ in range(events_to_push):
            if bus.publish(EventTypes.HEARTBEAT, {"x": 1}):
                published += 1
            else:
                failed += 1
        self._wait_drain_or_timeout(bus)
        self._safe_unsubscribe(bus, EventTypes.HEARTBEAT, callback)
        self._safe_stop(bus)
        elapsed = time.perf_counter() - test_start
        with lock:
            received_count = received[0]
        return {
            "test_name": "backpressure",
            "max_queue_size": max_queue,
            "events_pushed": events_to_push,
            "published": published,
            "failed_publishes": failed,
            "received_by_callback": received_count,
            "drop_rate": failed / events_to_push if events_to_push > 0 else 0,
            "test_duration_sec": elapsed,
            "status": "ok",
        }

    # ── 运行所有基准测试 ─────────────────────────────────

    def run_benchmarks(self) -> Dict[str, Any]:
        logger.info("开始事件总线基准测试...")
        gc.disable()
        try:
            bus_check = self._new_bus()
            if not self._validate_bus_functionality(bus_check):
                logger.error("EventBus 功能验证失败，基准测试中止")
                self._safe_stop(bus_check)
                return {"status": "error", "reason": "EventBus 功能异常", "results": {}}
            self._safe_stop(bus_check)

            system_info = self._collect_system_info()
            overall_start = time.perf_counter()
            report: Dict[str, Any] = {
                "benchmark_version": VERSION,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
                "system": system_info,
                "test_config": {
                    "warmup_iterations": self.warmup,
                    "measure_iterations": self.measure,
                    "throughput_duration_sec": self.throughput_duration,
                },
                "results": {},
            }

            report["results"]["publish_latency_small"] = self.benchmark_publish_latency(EVENT_PAYLOAD_SMALL)
            time.sleep(TEST_ISOLATION_SLEEP_SEC)
            report["results"]["publish_latency_large"] = self.benchmark_publish_latency(EVENT_PAYLOAD_LARGE)
            time.sleep(TEST_ISOLATION_SLEEP_SEC)
            report["results"]["publish_throughput"] = self.benchmark_publish_throughput()
            time.sleep(TEST_ISOLATION_SLEEP_SEC)
            report["results"]["multi_subscriber_latency"] = self.benchmark_multi_subscriber_latency()
            time.sleep(TEST_ISOLATION_SLEEP_SEC)
            report["results"]["concurrent_publish"] = self.benchmark_concurrent_publish()
            time.sleep(TEST_ISOLATION_SLEEP_SEC)
            report["results"]["backpressure"] = self.benchmark_backpressure()

            overall_elapsed = time.perf_counter() - overall_start
            report["total_benchmark_duration_sec"] = overall_elapsed
            logger.info("基准测试完成，总耗时 %.2fs", overall_elapsed)
            return report
        finally:
            gc.enable()

    # ── 健康检查 ─────────────────────────────────────────

    @classmethod
    def health_check(cls) -> Dict[str, Any]:
        if not EVENT_BUS_AVAILABLE:
            return {"status": "error", "reason": "EventBus 不可用", "warnings": ["无法导入"]}
        return {"status": "ok", "reason": "基准测试就绪", "warnings": []}


def main():
    import argparse
    parser = argparse.ArgumentParser(description="火种事件总线性能基准测试")
    parser.add_argument("--json", action="store_true", help="以 JSON 格式输出")
    parser.add_argument("--warmup", type=int, default=DEFAULT_WARMUP_ITERATIONS,
                        help=f"预热迭代次数 (默认: {DEFAULT_WARMUP_ITERATIONS})")
    parser.add_argument("--measure", type=int, default=DEFAULT_MEASURE_ITERATIONS,
                        help=f"测量迭代次数 (默认: {DEFAULT_MEASURE_ITERATIONS}, 上限: {MAX_MEASURE_ITERATIONS})")
    parser.add_argument("--duration", type=float, default=DEFAULT_THROUGHPUT_DURATION_SEC,
                        help=f"吞吐量测试持续时间秒 (默认: {DEFAULT_THROUGHPUT_DURATION_SEC})")
    parser.add_argument("--threads", type=int, default=DEFAULT_THREAD_POOL_SIZE,
                        help=f"并发测试线程数 (默认: {DEFAULT_THREAD_POOL_SIZE}, 上限: {MAX_THREAD_POOL_SIZE})")
    parser.add_argument("--events-per-thread", type=int, default=DEFAULT_EVENTS_PER_THREAD,
                        help=f"每线程事件数 (默认: {DEFAULT_EVENTS_PER_THREAD}, 上限: {MAX_EVENTS_PER_THREAD})")
    args = parser.parse_args()

    if args.warmup < 0:
        parser.error("预热次数不能为负数")
    if args.measure <= 0:
        parser.error("测量次数必须为正数")
    if args.duration <= 0:
        parser.error("吞吐量持续时间必须为正数")
    if args.threads < 1 or args.threads > MAX_THREAD_POOL_SIZE:
        parser.error(f"线程数必须在 1 到 {MAX_THREAD_POOL_SIZE} 之间")
    if args.events_per_thread <= 0 or args.events_per_thread > MAX_EVENTS_PER_THREAD:
        parser.error(f"每线程事件数必须在 1 到 {MAX_EVENTS_PER_THREAD} 之间")

    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')

    try:
        benchmark = BenchmarkEventBus(
            warmup=args.warmup,
            measure=args.measure,
            throughput_duration=args.duration
        )
        report = benchmark.run_benchmarks()
        if args.json:
            print(json.dumps(report, indent=2, default=str))
        else:
            print("\n========== 事件总线基准测试结果 ==========")
            print(f"版本: {report.get('benchmark_version', 'N/A')}")
            sys_info = report.get('system', {})
            print(f"系统: {sys_info.get('platform', 'N/A')}")
            print(f"Python: {sys_info.get('python_version', 'N/A')}")
            print(f"EventBus 版本: {sys_info.get('event_bus_version', 'N/A')}")
            cpu = sys_info.get('cpu_model') or f"逻辑核数: {sys_info.get('cpu_count_logical', 'N/A')}"
            print(f"CPU: {cpu}")
            if 'total_memory_gb' in sys_info:
                print(f"内存: {sys_info['total_memory_gb']} GB 总量, {sys_info['available_memory_gb']} GB 可用")
            print("\n--- 配置 ---")
            for k, v in report['test_config'].items():
                print(f"  {k}: {v}")
            print("\n--- 性能结果 ---")
            for test_name, result in report['results'].items():
                print(f"\n{test_name}:")
                if isinstance(result, dict):
                    for k, v in result.items():
                        if isinstance(v, float):
                            print(f"  {k}: {v:.6f}")
                        else:
                            print(f"  {k}: {v}")
            print(f"\n总耗时: {report.get('total_benchmark_duration_sec', 0):.2f}s")
            print("==========================================")
    except Exception as e:
        logger.error("基准测试运行失败: %s", e)
        return 1
    return 0


if __name__ == "__main__":
    exit(main())
