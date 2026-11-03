# -*- coding: utf-8 -*-
"""
core/bootstrap.py — 系统启动引导（依赖顺序编排 + 健康检查）

铁律声明：本文件严格执行八项铁律
启动顺序不可乱：config → types → events → core_infra → domain → application → presentation
"""

from __future__ import annotations

import asyncio
import signal
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Type

from .types import (
    SystemConfig, ModuleStatus, QuantError, SystemError, ErrorCode,
    Timestamp, _utc_now_ms,
)


class BootstrapError(SystemError):
    """启动异常"""
    pass


class ShutdownError(SystemError):
    """关闭异常"""
    pass


@dataclass
class ModuleRegistration:
    """模块注册信息"""
    name: str
    module_type: str  # infrastructure | domain | application | presentation
    dependencies: List[str] = field(default_factory=list)
    init_fn: Optional[Callable] = None
    start_fn: Optional[Callable] = None
    stop_fn: Optional[Callable] = None
    status: ModuleStatus = ModuleStatus.INITIALIZING
    started_at: Optional[Timestamp] = None
    error: Optional[str] = None


class Bootstrap:
    """
    系统启动引导器

    职责：
    1. 加载并校验配置（三层防线第1层）
    2. 按依赖顺序初始化模块
    3. 启动所有模块
    4. 注册信号处理（graceful shutdown）
    5. 健康检查编排
    """

    def __init__(self, config: Optional[SystemConfig] = None):
        self._config = config or SystemConfig()
        self._modules: Dict[str, ModuleRegistration] = {}
        self._startup_order: List[str] = []
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._start_time: Optional[Timestamp] = None
        self._errors: List[BootstrapError] = []

        # 三层防线：配置校验
        self._config.validate()

    def register_module(
        self,
        name: str,
        module_type: str,
        dependencies: Optional[List[str]] = None,
        init_fn: Optional[Callable] = None,
        start_fn: Optional[Callable] = None,
        stop_fn: Optional[Callable] = None,
    ) -> None:
        """注册模块"""
        if name in self._modules:
            raise BootstrapError(
                ErrorCode.CORE_BOOTSTRAP_001,
                f"Module '{name}' already registered"
            )
        self._modules[name] = ModuleRegistration(
            name=name,
            module_type=module_type,
            dependencies=dependencies or [],
            init_fn=init_fn,
            start_fn=start_fn,
            stop_fn=stop_fn,
        )

    def _resolve_dependencies(self) -> List[str]:
        """
        拓扑排序解析启动顺序
        检测循环依赖
        """
        visited: Set[str] = set()
        visiting: Set[str] = set()
        order: List[str] = []

        def dfs(module_name: str) -> None:
            if module_name in visiting:
                cycle = " -> ".join(list(visiting) + [module_name])
                raise BootstrapError(
                    ErrorCode.CORE_BOOTSTRAP_002,
                    f"Circular dependency detected: {cycle}"
                )
            if module_name in visited:
                return

            if module_name not in self._modules:
                raise BootstrapError(
                    ErrorCode.CORE_BOOTSTRAP_001,
                    f"Dependency '{module_name}' not found in registered modules"
                )

            visiting.add(module_name)
            mod = self._modules[module_name]
            for dep in mod.dependencies:
                dfs(dep)

            visiting.remove(module_name)
            visited.add(module_name)
            order.append(module_name)

        # 按层级排序：infrastructure -> domain -> application -> presentation
        type_order = {"infrastructure": 0, "domain": 1, "application": 2, "presentation": 3}
        sorted_modules = sorted(
            self._modules.keys(),
            key=lambda n: type_order.get(self._modules[n].module_type, 99)
        )

        for name in sorted_modules:
            dfs(name)

        return order

    async def start(self) -> None:
        """启动系统"""
        self._start_time = _utc_now_ms()
        self._running = True

        print(f"[{self._start_time}] 🚀 QuantSystem v5.0.0-production bootstrap starting...")

        # 1. 解析依赖顺序
        try:
            self._startup_order = self._resolve_dependencies()
        except BootstrapError as e:
            self._errors.append(e)
            raise

        print(f"  📋 Startup order: {' -> '.join(self._startup_order)}")

        # 2. 按序初始化并启动模块
        for name in self._startup_order:
            mod = self._modules[name]
            try:
                print(f"  🔧 Initializing [{name}]...", end="", flush=True)
                if mod.init_fn:
                    if asyncio.iscoroutinefunction(mod.init_fn):
                        await mod.init_fn()
                    else:
                        mod.init_fn()
                mod.status = ModuleStatus.RUNNING
                mod.started_at = _utc_now_ms()
                print(" ✅")
            except Exception as e:
                mod.status = ModuleStatus.FAILED
                mod.error = str(e)
                err = BootstrapError(
                    ErrorCode.CORE_BOOTSTRAP_001,
                    f"Module '{name}' initialization failed: {e}"
                )
                self._errors.append(err)
                print(f" ❌ {e}")
                # 三层防线：启动失败不静默，抛出
                raise err from e

        # 3. 注册信号处理
        self._register_signal_handlers()

        print(f"✅ Bootstrap complete. {len(self._startup_order)} modules started.")

    async def stop(self) -> None:
        """停止系统（优雅关闭）"""
        if not self._running:
            return

        print(f"\n🛑 Shutting down QuantSystem...")

        # 逆序停止
        for name in reversed(self._startup_order):
            mod = self._modules[name]
            if mod.status != ModuleStatus.RUNNING:
                continue
            try:
                print(f"  🔧 Stopping [{name}]...", end="", flush=True)
                if mod.stop_fn:
                    if asyncio.iscoroutinefunction(mod.stop_fn):
                        await mod.stop_fn()
                    else:
                        mod.stop_fn()
                mod.status = ModuleStatus.STOPPED
                print(" ✅")
            except Exception as e:
                mod.status = ModuleStatus.FAILED
                mod.error = str(e)
                print(f" ❌ {e}")

        self._running = False
        self._shutdown_event.set()
        print("✅ Shutdown complete.")

    def _register_signal_handlers(self) -> None:
        """注册操作系统信号处理器"""
        loop = asyncio.get_event_loop()

        def signal_handler(sig: signal.Signals) -> None:
            print(f"\n⚠️  Received signal {sig.name}, initiating graceful shutdown...")
            asyncio.create_task(self.stop())

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda s=sig: signal_handler(s))
            except NotImplementedError:
                # Windows 不支持 add_signal_handler
                signal.signal(sig, lambda s, f: signal_handler(sig))

    def register_shutdown_callback(self, callback: Callable) -> None:
        """注册关闭回调"""
        self._shutdown_callbacks.append(callback)

    async def wait_for_shutdown(self) -> None:
        """等待关闭信号"""
        await self._shutdown_event.wait()

    def health_check(self) -> Dict[str, Any]:
        """全系统健康检查"""
        return {
            "running": self._running,
            "start_time": self._start_time,
            "uptime_ms": (_utc_now_ms() - self._start_time) if self._start_time else 0,
            "modules": {
                name: {
                    "status": mod.status.value,
                    "type": mod.module_type,
                    "started_at": mod.started_at,
                    "error": mod.error,
                }
                for name, mod in self._modules.items()
            },
            "errors": [str(e) for e in self._errors],
        }

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def config(self) -> SystemConfig:
        return self._config
