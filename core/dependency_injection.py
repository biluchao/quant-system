# -*- coding: utf-8 -*-
"""
core/dependency_injection.py — 依赖注入容器

铁律声明：本文件严格执行八项铁律
模块间依赖通过容器注入，禁止硬编码依赖关系。
支持作用域：singleton / transient / scoped
"""

from __future__ import annotations

import inspect
from enum import Enum
from typing import Any, Callable, Dict, Optional, Type, TypeVar, cast

from .types import QuantError, ErrorCode

T = TypeVar("T")


class DIScope(str, Enum):
    SINGLETON = "singleton"      # 全局单例
    TRANSIENT = "transient"      # 每次新建
    SCOPED = "scoped"           # 作用域内单例


class DIError(QuantError):
    """依赖注入异常"""
    pass


class DIContainer:
    """
    轻量级依赖注入容器

    特性：
    - 构造函数自动注入（通过类型注解）
    - 支持工厂函数
    - 支持作用域
    - 循环依赖检测
    """

    def __init__(self, parent: Optional["DIContainer"] = None):
        self._parent = parent
        self._registrations: Dict[Type, "_Registration"] = {}
        self._singletons: Dict[Type, Any] = {}
        self._scoped_instances: Dict[Type, Any] = {}
        self._resolving: set[Type] = set()

    def register(
        self,
        interface: Type[T],
        implementation: Optional[Type[T]] = None,
        *,
        factory: Optional[Callable[..., T]] = None,
        scope: DIScope = DIScope.SINGLETON,
        name: str = "",
    ) -> None:
        """注册依赖"""
        impl = implementation or interface
        self._registrations[interface] = _Registration(
            interface=interface,
            implementation=impl,
            factory=factory,
            scope=scope,
            name=name,
        )

    def register_instance(self, interface: Type[T], instance: T) -> None:
        """注册已有实例（作为 singleton）"""
        self._registrations[interface] = _Registration(
            interface=interface,
            implementation=type(instance),
            factory=None,
            scope=DIScope.SINGLETON,
        )
        self._singletons[interface] = instance

    def resolve(self, interface: Type[T]) -> T:
        """解析依赖"""
        # 循环依赖检测
        if interface in self._resolving:
            raise DIError(
                ErrorCode.CORE_DI_001,
                f"Circular dependency detected while resolving {interface.__name__}"
            )

        # 检查当前容器
        if interface in self._singletons:
            return cast(T, self._singletons[interface])

        if interface in self._scoped_instances:
            return cast(T, self._scoped_instances[interface])

        # 查找注册
        reg = self._registrations.get(interface)
        if reg is None and self._parent:
            return self._parent.resolve(interface)
        if reg is None:
            raise DIError(
                ErrorCode.CORE_DI_001,
                f"Interface '{interface.__name__}' not registered"
            )

        self._resolving.add(interface)

        try:
            if reg.scope == DIScope.SINGLETON and interface in self._singletons:
                return cast(T, self._singletons[interface])

            # 创建实例
            if reg.factory:
                instance = reg.factory(self)
            else:
                instance = self._create_instance(reg.implementation)

            # 存储
            if reg.scope == DIScope.SINGLETON:
                self._singletons[interface] = instance
            elif reg.scope == DIScope.SCOPED:
                self._scoped_instances[interface] = instance

            return cast(T, instance)

        finally:
            self._resolving.discard(interface)

    def _create_instance(self, cls: Type[T]) -> T:
        """通过构造函数自动注入创建实例"""
        sig = inspect.signature(cls.__init__)
        kwargs: Dict[str, Any] = {}

        for param_name, param in sig.parameters.items():
            if param_name == "self":
                continue
            if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                continue

            if param.annotation is not inspect.Parameter.empty:
                annotation = param.annotation
                try:
                    kwargs[param_name] = self.resolve(annotation)
                except DIError:
                    if param.default is not inspect.Parameter.empty:
                        kwargs[param_name] = param.default
                    else:
                        raise DIError(
                            ErrorCode.CORE_DI_001,
                            f"Cannot resolve parameter '{param_name}' "
                            f"of type '{annotation.__name__}' for {cls.__name__}"
                        )
            else:
                if param.default is not inspect.Parameter.empty:
                    kwargs[param_name] = param.default
                else:
                    raise DIError(
                        ErrorCode.CORE_DI_001,
                        f"Parameter '{param_name}' in {cls.__name__} has no type annotation and no default"
                    )

        return cls(**kwargs)

    def create_scope(self) -> "DIContainer":
        """创建子容器（新的 scoped 作用域）"""
        return DIContainer(parent=self)

    def clear_scope(self) -> None:
        """清除 scoped 实例"""
        self._scoped_instances.clear()

    def has_registration(self, interface: Type) -> bool:
        """检查是否已注册"""
        return interface in self._registrations

    def list_registrations(self) -> list[str]:
        """列出所有注册"""
        return [f"{r.interface.__name__} -> {r.implementation.__name__} ({r.scope.value})"
                for r in self._registrations.values()]


@dataclass
class _Registration:
    interface: Type
    implementation: Type
    factory: Optional[Callable]
    scope: DIScope
    name: str = ""
