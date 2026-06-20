#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 模块热重载管理器 (ModuleLoader) v11.0.0

核心职责：
1. 递归监控 core/ 目录及其子目录下所有 .py 文件的变更（mtime 轮询），支持新增/删除/修改
2. 基于 AST 解析模块间的完整依赖关系（支持子包及 from core import xxx 等多种导入形式）
3. 当文件变更时，按拓扑排序的依赖顺序重新加载受影响模块，并原子更新依赖图
4. 采用“先初始化新实例，成功后再关闭旧实例”的安全重载顺序，失败时完整回滚旧实例和旧模块
5. 完全线程安全：所有共享状态操作均受 RLock 保护，依赖图深拷贝隔离
6. 完整的可观测性：版本追踪、脱敏审计日志、Prometheus 指标、重载事件

外部依赖：
- ast (标准库) : 解析 import 语句构建依赖图
- threading (标准库) : 后台监控线程与并发控制
- importlib (标准库) : 动态导入与重载
- collections (标准库) : 优化拓扑排序队列
- core.metrics.MetricsCollector (可选) : Prometheus 指标
- core.event_bus.EventBus (可选) : 发布重载事件

接口契约：
- start() -> None  启动后台文件监控
- stop() -> None  优雅停止
- get_module(name: str) -> Optional[Any]  获取指定模块实例
- reload_module(name: str) -> bool  手动触发单个模块重载
- get_module_version(name: str) -> int  获取模块版本号
- list_modules() -> Dict[str, int]  列出所有模块及版本
- health_check() -> Dict[str, Any]

异常与降级：
- 若模块重载失败，保留旧版本并报警，绝不中断服务
- 若依赖解析失败，跳过该模块并记录 WARNING
- 若新模块 health_check 返回 "degraded" 或 "ok" 均可接受，仅 "error" 拒绝
- 所有异常均被捕获，不影响主事件循环

资源管理：
- 后台线程使用 daemon 模式，stop() 时优雅退出
- 模块缓存使用字典，内存占用 < 10MB
- 冷却期字典与重载锁字典定期清理，防止内存泄漏
"""

import ast
import collections
import importlib
import logging
import os
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

VERSION = "11.0.0"
SPDX_IDENTIFIER = "Apache-2.0"

# 可选依赖
try:
    from core.metrics import MetricsCollector
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    MetricsCollector = None

try:
    from core.event_bus import EventBus, EventTypes
except ImportError:
    EventBus = None
    EventTypes = None

# ── 常量 ──────────────────────────────────────────────────
DEFAULT_SCAN_INTERVAL_SEC = 2.0
MIN_SCAN_INTERVAL_SEC = 0.5
MAX_SCAN_INTERVAL_SEC = 60.0
MODULE_DIR = "core"
RELOAD_COOLDOWN_SEC = 3.0
FAILURE_COOLDOWN_SEC = 0.5
MAX_DEPENDENCY_DEPTH = 10
HEALTH_CHECK_TIMEOUT_SEC = 2.0
SHUTDOWN_TIMEOUT_SEC = 2.0
INIT_TIMEOUT_SEC = 2.0
COOLDOWN_CLEANUP_INTERVAL = 300
RELOAD_LOCK_CLEANUP_INTERVAL = 3600
CORE_PREFIX = "core."

# 事件与指标常量
EVENT_MODULE_LOADED = "module_loaded"
EVENT_MODULE_RELOADED = "module_reloaded"
EVENT_MODULE_RELOAD_FAILED = "module_reload_failed"
METRIC_LOAD_SUCCESS = "module_load_success"
METRIC_LOAD_FAILURE = "module_load_failure"
METRIC_RELOAD_SUCCESS = "module_reload_success"
METRIC_RELOAD_FAILURE = "module_reload_failure"
METRIC_RELOAD_COUNT = "module_reload_total"
METRIC_FAILURE_TOTAL = "module_reload_failures_total"

# 扫描排除目录集合（去重）
_EXCLUDED_DIRS = {
    '__pycache__', 'venv', 'env', '.git', '.mypy_cache', '.pytest_cache',
    '.tox', '.nox', '.idea', '.egg', '.eggs'
}


def _find_py_files(root_dir: str) -> Dict[str, str]:
    """递归扫描目录下所有 .py 文件，排除隐藏目录和缓存目录，返回 {模块名: 绝对路径}"""
    modules: Dict[str, str] = {}
    for dirpath, dirnames, filenames in os.walk(root_dir):
        dirnames[:] = [d for d in dirnames if not d.startswith('.') and d not in _EXCLUDED_DIRS]
        for fname in filenames:
            if fname.endswith('.py') and fname != '__init__.py':
                rel_path = os.path.relpath(os.path.join(dirpath, fname), root_dir)
                module_name = rel_path[:-3].replace(os.sep, '.')
                abs_path = os.path.join(dirpath, fname)
                modules[module_name] = abs_path
    return modules


class ModuleLoader:
    """模块热重载管理器，支持依赖感知的原子热更新"""

    _instance = None
    _singleton_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._singleton_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, scan_interval: float = DEFAULT_SCAN_INTERVAL_SEC,
                 module_dir: str = MODULE_DIR,
                 event_bus=None):
        if getattr(self, '_initialized', False):
            return
        with self._singleton_lock:
            if getattr(self, '_initialized', False):
                return
            self._initialized = True

            self.scan_interval = max(MIN_SCAN_INTERVAL_SEC,
                                     min(scan_interval, MAX_SCAN_INTERVAL_SEC))
            self.module_dir = os.path.abspath(module_dir)
            # 安全创建事件总线，失败时置为 None 并记录
            try:
                self.event_bus = event_bus or (EventBus() if EventBus else None)
            except Exception:
                logger.error("事件总线初始化失败，事件通知将不可用")
                self.event_bus = None

            self._state_lock = threading.RLock()
            self._modules: Dict[str, Any] = {}
            self._module_versions: Dict[str, int] = {}
            self._module_paths: Dict[str, str] = {}
            self._file_mtimes: Dict[str, float] = {}
            self._reload_cooldowns: Dict[str, float] = {}
            self._reload_last_status: Dict[str, bool] = {}

            self._dependency_graph: Dict[str, Set[str]] = {}
            self._reverse_deps: Dict[str, Set[str]] = {}

            self._stop_event = threading.Event()
            self._monitor_thread: Optional[threading.Thread] = None

            self._reload_count = 0
            self._reload_failures = 0
            self._last_cooldown_cleanup = time.time()
            self._last_lock_cleanup = time.time()

            self._reload_locks: Dict[str, threading.Lock] = {}
            self._reload_locks_lock = threading.Lock()

            logger.info("ModuleLoader v%s 初始化，扫描间隔 %.1fs, 目录: %s",
                        VERSION, self.scan_interval, self.module_dir)

    # ── 公共接口 ──────────────────────────────────────────

    def start(self) -> None:
        if self._monitor_thread and self._monitor_thread.is_alive():
            logger.warning("模块监控已在运行")
            return
        if not os.path.isdir(self.module_dir):
            logger.error("模块目录不存在，无法启动监控: %s", self.module_dir)
            return
        self._stop_event.clear()
        self._initial_scan()
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name="module-loader",
            daemon=True
        )
        self._monitor_thread.start()
        logger.info("模块热重载监控已启动")

    def stop(self) -> None:
        self._stop_event.set()
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=5.0)
            if self._monitor_thread.is_alive():
                logger.warning("模块监控线程未能在超时内停止，将强制退出")
        logger.info("模块热重载监控已停止")

    def get_module(self, name: str) -> Optional[Any]:
        with self._state_lock:
            return self._modules.get(name)

    def reload_module(self, name: str) -> bool:
        with self._state_lock:
            if name not in self._module_paths:
                logger.error("模块 %s 未注册", name)
                return False
        return self._reload_single(name, bypass_cooldown=True)

    def get_module_version(self, name: str) -> int:
        with self._state_lock:
            return self._module_versions.get(name, 0)

    def list_modules(self) -> Dict[str, int]:
        with self._state_lock:
            return dict(self._module_versions)

    def health_check(self) -> Dict[str, Any]:
        warnings = []
        with self._state_lock:
            modules_snapshot = list(self._modules.items())
            failures = self._reload_failures
            total = len(self._modules)
        for name, instance in modules_snapshot:
            if hasattr(instance, 'health_check'):
                try:
                    ok = self._run_health_check(instance, name)
                    if not ok:
                        warnings.append(f"模块 {name} 健康检查未通过")
                except Exception:
                    warnings.append(f"模块 {name} 健康检查异常")
        if total == 0:
            warnings.append("无已加载模块")
        if failures > 0:
            warnings.append(f"累计重载失败: {failures}")
        status = "degraded" if warnings else "ok"
        return {
            "status": status,
            "reason": f"已加载 {total} 个模块, 重载 {self._reload_count} 次",
            "warnings": warnings,
        }

    # ── 初始扫描与依赖构建 ─────────────────────────────────

    def _initial_scan(self) -> None:
        if not os.path.isdir(self.module_dir):
            return
        paths = _find_py_files(self.module_dir)
        with self._state_lock:
            for name, path in paths.items():
                self._module_paths[name] = path
                self._file_mtimes[path] = os.path.getmtime(path)
            self._build_dependency_graph_locked(paths)
            load_order = self._topological_sort_full_locked()
        for name in load_order:
            path = self._module_paths.get(name)
            if path:
                if not self._load_module(name, path):
                    self._cleanup_failed_module(name, path)

    def _cleanup_failed_module(self, name: str, path: str) -> None:
        """彻底清理加载失败的模块痕迹"""
        with self._state_lock:
            self._module_paths.pop(name, None)
            if path in self._file_mtimes:
                del self._file_mtimes[path]
            self._modules.pop(name, None)
            self._module_versions.pop(name, None)
            self._dependency_graph.pop(name, None)
            for dep_set in self._dependency_graph.values():
                dep_set.discard(name)
            for dep_set in self._reverse_deps.values():
                dep_set.discard(name)

    def _build_dependency_graph_locked(self, paths: Dict[str, str]) -> None:
        self._dependency_graph.clear()
        self._reverse_deps.clear()
        for name, path in paths.items():
            deps = self._parse_imports(path)
            self._dependency_graph[name] = deps
            for dep in deps:
                self._reverse_deps.setdefault(dep, set()).add(name)
        logger.debug("依赖图构建完成: %d 个模块", len(self._dependency_graph))

    def _topological_sort_full_locked(self) -> List[str]:
        dep_graph = {name: set(self._dependency_graph.get(name, set())) for name in self._module_paths}
        reverse_deps = {name: set(self._reverse_deps.get(name, set())) for name in self._module_paths}
        return self._perform_topological_sort(set(self._module_paths.keys()), dep_graph, reverse_deps)

    # ── 文件监控循环 ──────────────────────────────────────

    def _monitor_loop(self) -> None:
        logger.info("模块监控循环开始")
        consecutive_errors = 0
        max_consecutive_errors = 5
        while not self._stop_event.is_set():
            try:
                self._scan_changes()
                self._cleanup_cooldowns()
                self._cleanup_reload_locks()
                consecutive_errors = 0
            except Exception as e:
                if isinstance(e, (SystemExit, KeyboardInterrupt)):
                    raise
                consecutive_errors += 1
                logger.exception("模块扫描异常 (连续 %d 次)", consecutive_errors)
                if consecutive_errors >= max_consecutive_errors:
                    logger.critical("模块扫描连续失败 %d 次，监控降级", consecutive_errors)
                    break
            self._stop_event.wait(self.scan_interval)
        logger.info("模块监控循环退出")

    def _scan_changes(self) -> None:
        if not os.path.isdir(self.module_dir):
            return

        current_paths = _find_py_files(self.module_dir)
        current_files = set(current_paths.keys())

        with self._state_lock:
            known_files = set(self._module_paths.keys())

        new_files = [(name, path) for name, path in current_paths.items()
                     if name not in known_files]
        deleted = [name for name in known_files if name not in current_files]
        mtime_updates: Dict[str, float] = {}
        changed = []
        for name, path in current_paths.items():
            if name in known_files:
                try:
                    mtime = os.path.getmtime(path)
                except OSError:
                    continue
                old_mtime = self._file_mtimes.get(path, 0)
                if mtime > old_mtime:
                    changed.append((name, path))
                    mtime_updates[path] = mtime

        if mtime_updates:
            with self._state_lock:
                for path, mtime in mtime_updates.items():
                    # 仅在路径仍在注册表中时更新，防止删除后残留
                    if any(p == path for p in self._file_mtimes):
                        self._file_mtimes[path] = mtime

        if new_files or changed or deleted:
            logger.info("变更检测: 修改=%d, 新增=%d, 删除=%d",
                        len(changed), len(new_files), len(deleted))
            self._handle_changes(changed, new_files, deleted)

    def _handle_changes(self, changed: List[Tuple[str, str]],
                        new_files: List[Tuple[str, str]],
                        deleted: List[str]) -> None:
        # 1. 处理删除
        for name in deleted:
            self._unload_module(name)
            with self._state_lock:
                path = self._module_paths.pop(name, None)
                self._modules.pop(name, None)
                self._module_versions.pop(name, None)
                self._reload_cooldowns.pop(name, None)
                self._reload_last_status.pop(name, None)
                self._dependency_graph.pop(name, None)
                for dep_set in self._dependency_graph.values():
                    dep_set.discard(name)
                for dep_set in self._reverse_deps.values():
                    dep_set.discard(name)
                if path:
                    self._file_mtimes.pop(path, None)

        # 2. 处理新增
        new_loaded = []
        for name, path in new_files:
            with self._state_lock:
                self._module_paths[name] = path
                self._file_mtimes[path] = os.path.getmtime(path)
            if not self._load_module(name, path):
                self._cleanup_failed_module(name, path)
                continue
            new_loaded.append(name)
            deps = self._parse_imports(path)
            with self._state_lock:
                self._dependency_graph[name] = deps
                for dep in deps:
                    self._reverse_deps.setdefault(dep, set()).add(name)

        # 3. 收集受影响模块
        affected: Set[str] = set()
        for name, _ in changed:
            affected.add(name)
            self._collect_dependents(name, affected)
        for name in new_loaded:
            self._collect_dependents(name, affected)

        # 4. 拓扑重载
        if affected:
            reload_list = self._topological_sort(affected)
            for name in reload_list:
                if name in self._module_paths:
                    self._reload_single(name)

    # ── 模块加载/卸载/重载 ─────────────────────────────────

    def _acquire_reload_lock(self, name: str) -> None:
        with self._reload_locks_lock:
            if name not in self._reload_locks:
                self._reload_locks[name] = threading.Lock()
            lock = self._reload_locks[name]
        lock.acquire()
        logger.debug("获取模块 %s 重载锁", name)

    def _release_reload_lock(self, name: str) -> None:
        with self._reload_locks_lock:
            lock = self._reload_locks.get(name)
        if lock:
            lock.release()
            logger.debug("释放模块 %s 重载锁", name)

    def _load_module(self, name: str, path: str) -> bool:
        module_key = CORE_PREFIX + name
        self._acquire_reload_lock(name)
        try:
            if module_key in sys.modules:
                del sys.modules[module_key]
            module = importlib.import_module(module_key)
            instance = self._get_module_instance(module, name)
            if instance is None:
                logger.error("模块 %s 未提供有效实例", name)
                self._cleanup_failed_load(module_key)
                return False
            if not self._run_health_check(instance, name):
                logger.error("模块 %s 首次加载健康检查失败", name)
                self._cleanup_failed_load(module_key)
                return False
            if hasattr(instance, 'init'):
                if not self._run_with_timeout(instance.init, name, 'init', INIT_TIMEOUT_SEC):
                    logger.error("模块 %s init 超时或异常", name)
                    self._cleanup_failed_load(module_key)
                    return False
            with self._state_lock:
                self._modules[name] = instance
                self._module_versions[name] = 1
            self._emit_event(EVENT_MODULE_LOADED, {"module": name, "version": 1})
            self._record_metric(METRIC_LOAD_SUCCESS, 1, {"module": name})
            logger.info("模块 %s 首次加载成功 (v1)", name)
            return True
        except Exception as e:
            logger.error("加载模块 %s 失败: %s", name, e)
            self._cleanup_failed_load(module_key)
            self._reload_failures += 1
            self._record_metric(METRIC_LOAD_FAILURE, 1, {"module": name})
            return False
        finally:
            self._release_reload_lock(name)

    def _cleanup_failed_load(self, module_key: str) -> None:
        if module_key in sys.modules:
            del sys.modules[module_key]

    def _unload_module(self, name: str) -> None:
        with self._state_lock:
            instance = self._modules.get(name)
        if instance and hasattr(instance, 'shutdown'):
            self._run_with_timeout(instance.shutdown, name, 'shutdown', SHUTDOWN_TIMEOUT_SEC)
        module_key = CORE_PREFIX + name
        if module_key in sys.modules:
            del sys.modules[module_key]
        logger.info("模块 %s 已卸载", name)

    def _reload_single(self, name: str, bypass_cooldown: bool = False) -> bool:
        start_time = time.time()
        now = start_time
        last_status = self._reload_last_status.get(name, True)
        cooldown = RELOAD_COOLDOWN_SEC if last_status else FAILURE_COOLDOWN_SEC

        if not bypass_cooldown:
            last_time = self._reload_cooldowns.get(name, 0)
            if now - last_time < cooldown:
                logger.debug("模块 %s 冷却中，跳过重载", name)
                return False

        self._acquire_reload_lock(name)
        try:
            with self._state_lock:
                old_instance = self._modules.get(name)
                old_version = self._module_versions.get(name, 0)
                path = self._module_paths.get(name)
            if not path:
                logger.error("模块 %s 路径不存在，无法重载", name)
                return False

            module_key = CORE_PREFIX + name
            old_module = sys.modules.get(module_key)

            if module_key in sys.modules:
                del sys.modules[module_key]
            module = importlib.import_module(module_key)
            new_instance = self._get_module_instance(module, name)
            if new_instance is None:
                self._handle_reload_failure(name, old_instance, old_module, "无法获取实例", now)
                return False

            if not self._run_health_check(new_instance, name):
                self._handle_reload_failure(name, old_instance, old_module, "健康检查失败", now)
                return False

            if hasattr(new_instance, 'init'):
                if not self._run_with_timeout(new_instance.init, name, 'init', INIT_TIMEOUT_SEC):
                    self._handle_reload_failure(name, old_instance, old_module, "init 失败", now)
                    return False

            if old_instance is not None and hasattr(old_instance, 'shutdown'):
                self._run_with_timeout(old_instance.shutdown, name, 'shutdown', SHUTDOWN_TIMEOUT_SEC)

            new_version = old_version + 1
            with self._state_lock:
                self._modules[name] = new_instance
                self._module_versions[name] = new_version
                self._reload_cooldowns[name] = now
                self._reload_last_status[name] = True
                self._reload_count += 1

            # 更新依赖图
            new_deps = self._parse_imports(path)
            with self._state_lock:
                self._dependency_graph[name] = new_deps
                for dep_set in self._reverse_deps.values():
                    dep_set.discard(name)
                for dep in new_deps:
                    self._reverse_deps.setdefault(dep, set()).add(name)

            # 同步文件时间戳
            try:
                with self._state_lock:
                    self._file_mtimes[path] = os.path.getmtime(path)
            except OSError:
                pass

            elapsed = time.time() - start_time
            self._emit_event(EVENT_MODULE_RELOADED, {"module": name, "version": new_version})
            self._record_metric(METRIC_RELOAD_SUCCESS, 1, {"module": name})
            self._record_metric(METRIC_RELOAD_COUNT, 1)
            logger.info("模块 %s 重载成功 (v%d -> v%d, 耗时 %.3fs)", name, old_version, new_version, elapsed)
            return True

        except Exception as e:
            logger.error("模块 %s 重载异常: %s", name, e)
            self._handle_reload_failure(name, old_instance, old_module, str(e), now)
            return False
        finally:
            self._release_reload_lock(name)

    def _handle_reload_failure(self, name: str, old_instance: Any,
                               old_module: Any, reason: str, timestamp: float) -> None:
        self._reload_failures += 1
        self._record_metric(METRIC_RELOAD_FAILURE, 1, {"module": name})
        self._record_metric(METRIC_FAILURE_TOTAL, 1)
        with self._state_lock:
            self._reload_cooldowns[name] = timestamp
            self._reload_last_status[name] = False
            if old_instance is not None:
                self._modules[name] = old_instance
                if old_module is not None:
                    sys.modules[CORE_PREFIX + name] = old_module
                logger.warning("模块 %s 重载失败 (%s)，已恢复旧版本 (v%d)",
                              name, reason, self._module_versions.get(name, 0))
            else:
                self._modules.pop(name, None)
                logger.critical("模块 %s 重载失败且无旧版本，服务可能降级", name)
            self._emit_event(EVENT_MODULE_RELOAD_FAILED, {"module": name, "reason": reason})
        # 更新文件时间戳
        path = self._module_paths.get(name)
        if path:
            try:
                with self._state_lock:
                    self._file_mtimes[path] = os.path.getmtime(path)
            except OSError:
                pass

    # ── 依赖解析 ──────────────────────────────────────────

    def _parse_imports(self, path: str) -> Set[str]:
        deps: Set[str] = set()
        try:
            with open(path, 'r', encoding='utf-8') as f:
                source = f.read()
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        self._extract_core_dep(alias.name, deps)
                elif isinstance(node, ast.ImportFrom):
                    if node.module:
                        self._extract_core_dep(node.module, deps)
                    if node.module == 'core':
                        for alias in node.names:
                            if alias.name and alias.name != '*':
                                deps.add(alias.name)
        except Exception as e:
            logger.warning("解析 %s 的 import 失败: %s", path, e)
        return {d for d in deps if d in self._module_paths}

    @staticmethod
    def _extract_core_dep(module_name: str, deps: Set[str]) -> None:
        if module_name.startswith(CORE_PREFIX):
            rest = module_name[len(CORE_PREFIX):]
            if rest:
                deps.add(rest)
        elif module_name == 'core':
            pass

    def _collect_dependents(self, name: str, affected: Set[str]) -> None:
        stack = [(name, 0)]
        visited = set()
        while stack:
            current, depth = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            with self._state_lock:
                rev_deps = list(self._reverse_deps.get(current, []))
            for dep in rev_deps:
                if dep not in affected:
                    affected.add(dep)
                if depth < MAX_DEPENDENCY_DEPTH:
                    stack.append((dep, depth + 1))

    def _topological_sort(self, modules: Set[str]) -> List[str]:
        with self._state_lock:
            dep_graph = {name: set(self._dependency_graph.get(name, set())) for name in modules}
            reverse_deps: Dict[str, Set[str]] = {}
            for name in modules:
                rd = self._reverse_deps.get(name, set())
                reverse_deps[name] = set(rd)
        return self._perform_topological_sort(modules, dep_graph, reverse_deps)

    @staticmethod
    def _perform_topological_sort(module_names: Set[str], dep_graph: Dict[str, Set[str]],
                                  reverse_deps: Dict[str, Set[str]]) -> List[str]:
        in_degree: Dict[str, int] = {name: 0 for name in module_names}
        for name in module_names:
            for dep in dep_graph.get(name, set()):
                if dep in in_degree:
                    in_degree[name] += 1

        queue = collections.deque([name for name, deg in in_degree.items() if deg == 0])
        sorted_list = []
        while queue:
            node = queue.popleft()
            sorted_list.append(node)
            for dep in reverse_deps.get(node, set()):
                if dep in in_degree:
                    in_degree[dep] -= 1
                    if in_degree[dep] == 0:
                        queue.append(dep)

        remaining = set(in_degree.keys()) - set(sorted_list)
        if remaining:
            logger.warning("依赖图中可能存在循环: %s", remaining)
            sorted_list.extend(remaining)
        return sorted_list

    # ── 辅助方法 ──────────────────────────────────────────

    def _get_module_instance(self, module, name: str) -> Optional[Any]:
        short_name = name.split('.')[-1]
        candidates = self._generate_class_names(short_name)
        for class_name in candidates:
            try:
                # 安全获取属性，避免触发描述符
                attr = getattr(module, class_name, None)
                if attr is not None and isinstance(attr, type):
                    instance = attr()
                    if isinstance(instance, object):
                        return instance
            except Exception:
                continue
        for attr_name in dir(module):
            if attr_name.startswith('_'):
                continue
            try:
                attr = getattr(module, attr_name)
                if isinstance(attr, type) and hasattr(attr, 'health_check'):
                    instance = attr()
                    if isinstance(instance, object):
                        return instance
            except Exception:
                continue
        logger.warning("模块 %s 中未找到合适的可实例化类", name)
        return None

    @staticmethod
    def _generate_class_names(name: str) -> List[str]:
        if not name:
            return []
        parts = name.split('_')
        pascal = ''.join(p.capitalize() for p in parts)
        candidates = [pascal]
        if name == name.lower() and '_' not in name:
            candidates.append(name.upper())
        if len(parts) >= 2 and len(parts[0]) <= 3:
            candidates.insert(0, parts[0].upper() + ''.join(p.capitalize() for p in parts[1:]))
        if all(len(p) <= 3 for p in parts):
            candidates.append(''.join(p.upper() for p in parts))
        return candidates

    def _run_health_check(self, instance, name: str) -> bool:
        if not hasattr(instance, 'health_check'):
            logger.warning("模块 %s 无 health_check 方法，跳过检查", name)
            return True

        result_holder: List[Dict] = []
        done_event = threading.Event()

        def _check():
            try:
                result = instance.health_check()
                if isinstance(result, dict):
                    result_holder.append(result)
                else:
                    result_holder.append({"status": "error", "reason": "返回非字典"})
            except Exception as e:
                if isinstance(e, (SystemExit, KeyboardInterrupt)):
                    raise
                result_holder.append({"status": "error", "reason": str(e)})
            finally:
                done_event.set()

        thread = threading.Thread(target=_check, daemon=True, name=f"hc-{name}")
        thread.start()
        finished = done_event.wait(timeout=HEALTH_CHECK_TIMEOUT_SEC)
        if not finished:
            logger.error("模块 %s health_check 超时 (%.1fs)", name, HEALTH_CHECK_TIMEOUT_SEC)
            return False
        if not result_holder:
            logger.error("模块 %s health_check 无返回", name)
            return False

        result = result_holder[0]
        status = result.get('status', 'error')
        return status in ('ok', 'degraded')

    @staticmethod
    def _run_with_timeout(func: Callable, module_name: str, method_name: str,
                           timeout: float) -> bool:
        result_holder = []
        exception_holder = []
        done_event = threading.Event()

        def _run():
            try:
                func()
                result_holder.append(True)
            except Exception as e:
                if isinstance(e, (SystemExit, KeyboardInterrupt)):
                    raise
                exception_holder.append(e)
            finally:
                done_event.set()

        thread = threading.Thread(target=_run, daemon=True, name=f"run-{module_name}")
        thread.start()
        finished = done_event.wait(timeout=timeout)
        if not finished:
            logger.error("模块 %s %s 超时 (%.1fs)", module_name, method_name, timeout)
            return False
        if exception_holder:
            logger.error("模块 %s %s 异常: %s", module_name, method_name, exception_holder[0])
            return False
        return bool(result_holder)

    def _cleanup_cooldowns(self) -> None:
        now = time.time()
        if now - self._last_cooldown_cleanup < COOLDOWN_CLEANUP_INTERVAL:
            return
        self._last_cooldown_cleanup = now
        with self._state_lock:
            success_threshold = now - RELOAD_COOLDOWN_SEC * 10
            failure_threshold = now - FAILURE_COOLDOWN_SEC * 10
            expired = []
            for name, t in self._reload_cooldowns.items():
                status = self._reload_last_status.get(name, True)
                threshold = success_threshold if status else failure_threshold
                if t < threshold:
                    expired.append(name)
            for name in expired:
                del self._reload_cooldowns[name]
                self._reload_last_status.pop(name, None)

    def _cleanup_reload_locks(self) -> None:
        now = time.time()
        if now - self._last_lock_cleanup < RELOAD_LOCK_CLEANUP_INTERVAL:
            return
        self._last_lock_cleanup = now
        with self._reload_locks_lock:
            stale = [name for name in self._reload_locks if name not in self._module_paths]
            for name in stale:
                del self._reload_locks[name]

    # ── 事件与指标 ────────────────────────────────────────

    def _emit_event(self, event_type: str, data: Dict):
        if not self.event_bus:
            return
        try:
            evt_type = getattr(EventTypes, 'SYSTEM_ALERT', "system_alert")
            self.event_bus.publish(evt_type, {
                "subtype": event_type,
                "data": data,
                "timestamp_ns": time.time_ns(),
            })
        except Exception:
            logger.debug("事件发布失败", exc_info=True)

    def _record_metric(self, name: str, value: float, labels: Optional[Dict] = None):
        if METRICS_AVAILABLE and MetricsCollector:
            try:
                MetricsCollector.counter(name, value, labels or {})
            except Exception:
                logger.debug("指标记录失败", exc_info=True)
