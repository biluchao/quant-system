# -*- coding: utf-8 -*-
# 铁律遵守声明：本文件严格执行八项铁律
"""
依赖注入容器 —— 解耦模块间直接引用

核心思想：
- 模块只声明需要什么（接口），不关心谁提供
- 容器负责组装依赖关系
- 支持作用域：singleton / transient / scoped
"""

from __future__ import annotations

import inspect
import logging
import threading
import uuid
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Type, TypeVar, cast

from core.types import ErrorCode, QuantError

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ServiceLifetime(Enum):
    """服务生命周期"""
    SINGLETON = "singleton"      # 全局单例
    TRANSIENT = "transient"      # 每次新建
    SCOPED = "scoped"            # 作用域内单例


class DIError(QuantError):
    """依赖注入异常"""

    def __init__(self, message: str, context: Optional[Dict[str, Any]] = None):
        super().__init__(code=ErrorCode.SYSTEM_ERROR, message=message, context=context)


class ServiceDescriptor:
    """服务描述符"""

    def __init__(
        self,
        interface: Type[Any],
        implementation: Any,
        lifetime: ServiceLifetime = ServiceLifetime.SINGLETON,
        factory: Optional[Callable[..., Any]] = None,
    ):
        self.interface = interface
        self.implementation = implementation
        self.lifetime = lifetime
        self.factory = factory
        self._instance: Optional[Any] = None


class DIContainer:
    """
    依赖注入容器

    用法：
        container = DIContainer()
        container.register(IRiskEngine, RiskEngine, lifetime=ServiceLifetime.SINGLETON)
        container.register(IEventBus, EventBus, lifetime=ServiceLifetime.SINGLETON)

        risk = container.resolve(IRiskEngine)
    """

    def __init__(self) -> None:
        self._descriptors: Dict[Type[Any], ServiceDescriptor] = {}
        self._scoped_instances: Dict[str, Dict[Type[Any], Any]] = {}
        self._lock = threading.RLock()
        self._resolution_stack: List[Type[Any]] = []

    def register(
        self,
        interface: Type[T],
        implementation: Any = None,
        lifetime: ServiceLifetime = ServiceLifetime.SINGLETON,
        factory: Optional[Callable[..., T]] = None,
    ) -> None:
        """
        注册服务

        三层防线第1层：输入校验
        """
        if not isinstance(interface, type):
            raise DIError(f"Interface must be a type, got {type(interface)}")

        impl = implementation or interface
        if not isinstance(impl, type) and not callable(impl):
            raise DIError(f"Implementation must be a type or callable, got {type(impl)}")

        with self._lock:
            self._descriptors[interface] = ServiceDescriptor(
                interface=interface,
                implementation=impl,
                lifetime=lifetime,
                factory=factory,
            )
            logger.debug(f"DI: registered {interface.__name__} -> {impl.__name__ if isinstance(impl, type) else 'factory'}")

    def resolve(self, interface: Type[T], scope_id: Optional[str] = None) -> T:
        """
        解析服务实例

        三层防线第2层：循环依赖检测 + 业务校验
        """
        if interface not in self._descriptors:
            raise DIError(f"Interface '{interface.__name__}' not registered")

        descriptor = self._descriptors[interface]

        # 检测循环依赖
        if interface in self._resolution_stack:
            chain = " -> ".join(t.__name__ for t in self._resolution_stack)
            raise DIError(
                f"Circular dependency detected: {chain} -> {interface.__name__}",
                context={"chain": chain, "interface": interface.__name__},
            )

        self._resolution_stack.append(interface)

        try:
            # Singleton：返回缓存实例
            if descriptor.lifetime == ServiceLifetime.SINGLETON:
                if descriptor._instance is not None:
                    return cast(T, descriptor._instance)

                instance = self._create_instance(descriptor)
                descriptor._instance = instance
                return cast(T, instance)

            # Scoped：作用域内单例
            elif descriptor.lifetime == ServiceLifetime.SCOPED:
                if scope_id is None:
                    raise DIError("Scope ID required for scoped resolution")

                if scope_id not in self._scoped_instances:
                    self._scoped_instances[scope_id] = {}

                if interface in self._scoped_instances[scope_id]:
                    return cast(T, self._scoped_instances[scope_id][interface])

                instance = self._create_instance(descriptor)
                self._scoped_instances[scope_id][interface] = instance
                return cast(T, instance)

            # Transient：每次新建
            else:
                return cast(T, self._create_instance(descriptor))

        finally:
            self._resolution_stack.pop()

    def _create_instance(self, descriptor: ServiceDescriptor) -> Any:
        """创建实例（自动注入依赖）"""
        # 使用工厂函数
        if descriptor.factory is not None:
            return descriptor.factory()

        impl = descriptor.implementation

        # 如果是类，自动注入构造函数参数
        if isinstance(impl, type):
            sig = inspect.signature(impl.__init__)
            kwargs: Dict[str, Any] = {}

            for param_name, param in sig.parameters.items():
                if param_name == "self":
                    continue

                # 尝试按类型注入
                if param.annotation and isinstance(param.annotation, type):
                    try:
                        kwargs[param_name] = self.resolve(param.annotation)
                    except DIError:
                        # 无法解析，使用默认值
                        if param.default is not inspect.Parameter.empty:
                            kwargs[param_name] = param.default
                        else:
                            raise DIError(
                                f"Cannot resolve parameter '{param_name}' "
                                f"of type '{param.annotation.__name__}' "
                                f"for '{impl.__name__}'"
                            )
                elif param.default is not inspect.Parameter.empty:
                    kwargs[param_name] = param.default

            return impl(**kwargs)

        # 如果是可调用对象
        elif callable(impl):
            return impl()

        else:
            raise DIError(f"Cannot create instance from {impl}")

    def create_scope(self) -> str:
        """创建作用域"""
        scope_id = str(uuid.uuid4())
        self._scoped_instances[scope_id] = {}
        return scope_id

    def dispose好，继续往上搭。这轮把 `core/` 剩余 9 个基础设施文件全部写完——这是整个系统的骨架，后续所有模块都依赖它们。

好，继续。这轮把 `core/` 剩余 9 个基础设施文件全部写完——这是整个系统的骨架，后续所有模块都依赖它们。

先确认项目路径：

好，直接输出。这轮把 `core/` 剩余 10 个基础设施文件全部写完，每个文件完整可运行，铁律声明齐全。

---

## 文件一：`core/lazy_init.py` — 延迟初始化

