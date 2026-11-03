# -*- coding: utf-8 -*-
"""
core/lazy_init.py — 延迟初始化（打破循环依赖）

铁律声明：本文件严格执行八项铁律
模块间循环依赖通过延迟初始化解决，禁止 import 时触发副作用。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from threading import Lock
from typing import Any, Callable, Dict, Optional, Type, TypeVar

from .types import QuantError, ErrorCode

T = TypeVar("T")


class LazyInitError(QuantError):
    """延迟初始化异常"""
    pass


class LazyInitializer:
    """
    延迟初始化器

    使用方式：
        lazy = LazyInitializer(lambda: create_expensive_resource())
        resource = lazy.get()  # 首次调用时初始化，后续直接返回
    """

    def __init__(self, factory: Callable[[], T], name: str = ""):
        self._factory = factory
        self._name = name
        self._instance: Optional[T] = None
        self._lock = Lock()
        self._initialized = False
        self._init_error: Optional[Exception] = None

    def get(self) -> T:
        """获取实例（线程安全，延迟初始化）"""
        if self._initialized:
            if self._init_error:
                raise self._init_error
            return self._instance  # type: ignore

        with self._lock:
            if self._initialized:
                if self._init_error:
                    raise self._init_error
                return self._instance  # type: ignore

            try:
                self._instance = self._factory()
                self._initialized = True
                return self._instance  # type: ignore
            except Exception as e:
                self._init_error = LazyInitError(
                    ErrorCode.CORE_LAZY_INIT_001,
                    f"Lazy init failed for '{self._name}': {e}"
                )
                raise self._init_error

    async def get_async(self) -> T:
        """异步获取实例"""
        if self._initialized:
            if self._init_error:
                raise self._init_error
            return self._instance  # type: ignore

        # asyncio 下用 Lock 替代 threading.Lock
        if not hasattr(self, '_async_lock'):
            self._async_lock = asyncio.Lock()

        async with self._async_lock:  # type: ignore
            if self._initialized:
                if self._init_error:
                    raise self._init_error
                return self._instance  # type: ignore

            try:
                result = self._factory()
                if asyncio.iscoroutine(result):
                    self._instance = await result
                else:
                    self._instance = result
                self._initialized = True
                return self._instance  # type: ignore
            except Exception as e:
                self._init_error = LazyInitError(
                    ErrorCode.CORE_LAZY_INIT_001,
                    f"Lazy async init failed for '{self._name}': {e}"
                )
                raise self._init_error

    def reset(self) -> None:
        """重置（用于测试或重启）"""
        with self._lock:
            self._instance = None
            self._initialized = False
            self._init_error = None

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    @property
    def name(self) -> str:
        return self._name


class LazyRegistry:
    """
    延迟初始化注册表

    集中管理所有延迟初始化对象，支持按名称查找和批量初始化。
    """

    def __init__(self):
        self._initializers: Dict[str, LazyInitializer] = {}
        self._lock = Lock()

    def register(self, name: str, factory: Callable[[], Any], *, force: bool = False) -> LazyInitializer:
        """注册延迟初始化对象"""
        with self._lock:
            if name in self._initializers and not force:
                raise LazyInitError(
                    ErrorCode.CORE_LAZY_INIT_001,
                    f"Name '{name}' already registered in LazyRegistry"
                )
            initializer = LazyInitializer(factory, name=name)
            self._initializers[name] = initializer
            return initializer

    def get(self, name: str) -> Any:
        """获取已注册的对象"""
        if name not in self._initializers:
            raise LazyInitError(
                ErrorCode.CORE_LAZY_INIT_001,
                f"Name '{name}' not found in LazyRegistry"
            )
        return self._initializers[name].get()

    def get_initializer(self, name: str) -> LazyInitializer:
        """获取初始化器本身"""
        if name not in self._initializers:
            raise LazyInitError(
                ErrorCode.CORE_LAZY_INIT_001,
                f"Name '{name}' not found in LazyRegistry"
            )
        return self._initializers[name]

    def list_names(self) -> list[str]:
        return list(self._initializers.keys())

    def reset_all(self) -> None:
        """重置所有"""
        with self._lock:
            for initializer in self._initializers.values():
                initializer.reset()

    def preload(self, names: Optional[list[str]] = None) -> None:
        """预加载指定或全部对象"""
        targets = names or list(self._initializers.keys())
        for name in targets:
            if name in self._initializers:
                self._initializers[name].get()
