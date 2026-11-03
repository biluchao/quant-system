# -*- coding: utf-8 -*-
# 铁律遵守声明：本文件严格执行八项铁律（类型/接口/事件/异常/配置/命名统一 + 三层防线 + 幂等审计）
"""
延迟初始化容器 —— 打破循环依赖

问题：risk/account/strategy 互相引用导致 import 循环
解决：模块注册时只存工厂函数，首次访问时才实例化
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import threading
from typing import Any, Callable, Dict, Optional, Type, TypeVar, cast

from core.types import ErrorCode, QuantError, SystemMode, SystemStatus

logger = logging.getLogger(__name__)

T = TypeVar("T")


class LazyInitError(QuantError):
    """延迟初始化异常"""

    def __init__(self, module_name: str, reason: str):
        super().__init__(
            code=ErrorCode.SYSTEM_ERROR,
            message=f"LazyInit failed for '{module_name}': {reason}",
            context={"module": module_name, "reason": reason},
        )


class LazyContainer:
    """
    延迟初始化容器

    用法：
        container = LazyContainer()
        container.register("risk_engine", RiskEngine, dependencies=["config", "event_bus"])
        engine = container.resolve("risk_engine")  # 首次访问时实例化
    """

    def __init__(self) -> None:
        self._factories: Dict[str, Callable[..., Any]] = {}
        self._dependencies: Dict[str, list[str]] = {}
        self._singletons: Dict[str, Any] = {}
        self._instantiating: set[str] = set()  # 检测循环依赖
        self._lock = threading.RLock()

    def register(
        self,
        name: str,
        factory: Callable[..., T],
        dependencies: Optional[list[str]] = None,
    ) -> None:
        """
        注册模块工厂

        三层防线第1层：输入校验
        """
        if not name or not isinstance(name, str):
            raise LazyInitError(name or "<empty>", "Module name must be non-empty string")
        if not callable(factory):
            raise LazyInitError(name, "Factory must be callable")

        with self._lock:
            self._factories[name] = factory
            self._dependencies[name] = dependencies or []

    def resolve(self, name: str) -> Any:
        """
        解析模块实例（单例，线程安全）

        首次调用时实例化，后续返回缓存实例
        """
        # 已实例化，直接返回
        if name in self._singletons:
            return self._singletons[name]

        with self._lock:
            # 双重检查
            if name in self._singletons:
                return self._singletons[name]

            if name not in self._factories:
                raise LazyInitError(name, "Module not registered")

            # 检测循环依赖
            if name in self._instantiating:
                raise LazyInitError(
                    name,
                    f"Circular dependency detected. Chain: {list(self._instantiating)} -> {name}",
                )

            self._instantiating.add(name)

            try:
                # 解析依赖
                deps = self._dependencies.get(name, [])
                resolved_deps: Dict[str, Any] = {}
                for dep_name in deps:
                    if dep_name == name:
                        continue
                    resolved_deps[dep_name] = self.resolve(dep_name)

                # 实例化
                factory = self._factories[name]
                sig = inspect.signature(factory)
                kwargs: Dict[str, Any] = {}

                # 尝试按参数名匹配依赖
                for param_name, param in sig.parameters.items():
                    if param_name in resolved_deps:
                        kwargs[param_name] = resolved_deps[param_name]
                    elif param.annotation and hasattr(param.annotation, "__name__"):
                        # 按类型匹配
                        for dep in resolved_deps.values():
                            if isinstance(dep, param.annotation):
                                kwargs[param_name] = dep
                                break

                instance = factory(**kwargs)
                self._singletons[name] = instance
                logger.info(f"LazyInit: module '{name}' instantiated successfully")

                # 三层防线第2层：业务校验
                if instance is None:
                    raise LazyInitError(name, "Factory returned None")

                return instance

            finally:
                self._instantiating.discard(name)

    def has(self, name: str) -> bool:
        """检查模块是否已注册"""
        return name in self._factories

    def is_instantiated(self, name: str) -> bool:
        """检查模块是否已实例化"""
        return name in self._singletons

    def reset(self, name: str) -> None:
        """重置模块实例（用于测试或热重载）"""
        with self._lock:
            self._singletons.pop(name, None)
            logger.info(f"LazyInit: module '{name}' reset")

    def shutdown_all(self) -> None:
        """关闭所有模块（调用 close()/shutdown() 方法）"""
        with self._lock:
            for name, instance in list(self._singletons.items()):
                try:
                    if hasattr(instance, "close"):
                        instance.close()
                    elif hasattr(instance, "shutdown"):
                        instance.shutdown()
                    logger.info(f"LazyInit: module '{name}' shut down")
                except Exception as e:
                    logger.error(f"LazyInit: error shutting down '{name}': {e}")
            self._singletons.clear()


# 全局延迟初始化容器
_global_container: Optional[LazyContainer] = None
_container_lock = threading.Lock()


def get_container() -> LazyContainer:
    """获取全局延迟初始化容器（单例）"""
    global _global_container
    if _global_container is None:
        with _container_lock:
            if _global_container is None:
                _global_container = LazyContainer()
    return _global_container


def register_module(
    name: str,
    factory: Callable[..., Any],
    dependencies: Optional[list[str]] = None,
) -> None:
    """快捷注册模块"""
    get_container().register(name, factory, dependencies)


def resolve_module(name: str) -> Any:
    """快捷解析模块"""
    return get_container().resolve(name)
