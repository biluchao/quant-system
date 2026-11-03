"""core/lazy_init.py — 延迟初始化（打破循环依赖）

铁律声明：本文件遵守
- 类型统一：所有接口参数有类型注解
- 命名统一：类 PascalCase，方法动词开头
- 异常统一：初始化失败抛精确异常
- 幂等审计：初始化结果可查询

设计问题：
    risk → account → risk 这类循环依赖在量化系统中很常见。
    直接 import 会导致 ImportError 或初始化死锁。

解决方案：
    1. LazyRef：持有对尚未初始化的组件的弱引用占位符
    2. LazyInitializer：延迟工厂，首次访问时才创建实例
    3. 通过 setter 注入打破循环（init 时传 LazyRef，start 时解析）

使用模式：
    # 模块A 需要模块B，但B也可能需要A
    class ModuleA:
        def __init__(self, bus, b_ref: LazyRef):
            self._b = b_ref  # 持有引用，不立即访问

        async def start(self):
            b = await self._b.resolve()  # 此时B已初始化
            self._b_real = b
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, Optional, TypeVar

from core.types import TraceId, new_trace_id

logger = logging.getLogger(__name__)

T = TypeVar("T")


# ═══════════════════════════════════════════════════════════════
# 异常
# ═══════════════════════════════════════════════════════════════

class LazyInitError(Exception):
    """延迟初始化异常"""
    code = 2100


class CircularResolutionError(LazyInitError):
    """循环解析异常（解析链形成环）"""
    code = 2101


# ═══════════════════════════════════════════════════════════════
# 延迟引用
# ═══════════════════════════════════════════════════════════════

class LazyRef:
    """延迟引用占位符

    持有对目标组件的引用，但不立即解析。
    首次 resolve() 时才获取真实实例。

    特性：
    - 可追踪解析链（防止循环）
    - 可缓存解析结果（幂等）
    - 支持同步/异步工厂
    """

    def __init__(
        self,
        name: str,
        factory: Optional[Callable[..., Any]] = None,
        registry: Optional[Any] = None,
    ) -> None:
        """
        Args:
            name: 目标组件名
            factory: 创建组件的工厂函数（可选）
            registry: 服务注册表（用于按名查找，如 ServiceLocator）
        """
        if not name:
            raise ValueError("name 不能为空")
        self._name = name
        self._factory = factory
        self._registry = registry
        self._instance: Optional[Any] = None
        self._resolved = False
        self._resolving = False
        self._trace_id: TraceId = new_trace_id()

    @property
    def name(self) -> str:
        return self._name

    @property
    def is_resolved(self) -> bool:
        return self._resolved

    async def resolve(self) -> Any:
        """解析真实实例

        优先级：
        1. 已缓存的实例
        2. registry 中查找
        3. factory 创建

        检测循环解析：如果解析链中再次出现当前 ref，则报循环错误。
        """
        if self._resolved and self._instance is not None:
            return self._instance

        # 检测循环
        if self._resolving:
            raise CircularResolutionError(
                f"检测到循环解析: {self._name} (trace: {self._trace_id})"
            )

        self._resolving = True
        start_time = time.monotonic()

        try:
            # 1. 从注册表查找
            if self._registry is not None:
                try:
                    instance = self._registry.resolve(self._name)
                    if instance is not None:
                        self._instance = instance
                        self._resolved = True
                        return instance
                except (NotFoundError, AttributeError):
                    pass

            # 2. 从工厂创建
            if self._factory is not None:
                result = self._factory()
                # 支持异步工厂
                if asyncio.iscoroutine(result):
                    result = await result
                if result is None:
                    raise LazyInitError(
                        f"工厂返回 None: {self._name}"
                    )
                self._instance = result
                self._resolved = True
                return result

            raise LazyInitError(
                f"无法解析组件 {self._name}（无 registry 且无 factory）"
            )

        finally:
            self._resolving = False
            elapsed_ms = (time.monotonic() - start_time) * 1000
            if elapsed_ms > 100:
                logger.warning(
                    f"LazyRef 解析耗时 {elapsed_ms:.1f}ms: {self._name}"
                )

    def set_instance(self, instance: Any) -> None:
        """手动设置实例（用于依赖注入）"""
        self._instance = instance
        self._resolved = True

    def reset(self) -> None:
        """重置（仅测试用）"""
        self._instance = None
        self._resolved = False
        self._resolving = False


# ═══════════════════════════════════════════════════════════════
# 延迟初始化管理器
# ═══════════════════════════════════════════════════════════════

class LazyInitializer:
    """延迟初始化管理器

    管理一批 LazyRef，支持批量解析与状态查询。
    """

    def __init__(self, registry: Optional[Any] = None) -> None:
        self._refs: Dict[str, LazyRef] = {}
        self._registry = registry

    def register(
        self,
        name: str,
        factory: Optional[Callable[..., Any]] = None,
    ) -> LazyRef:
        """注册一个延迟引用"""
        if name in self._refs:
            return self._refs[name]

        ref = LazyRef(name, factory=factory, registry=self._registry)
        self._refs[name] = ref
        return ref

    def get(self, name: str) -> LazyRef:
        """获取已注册的引用"""
        if name not in self._refs:
            raise NotFoundError(f"未注册的 LazyRef: {name}")
        return self._refs[name]

    async def resolve_all(self, names: Optional[List[str]] = None) -> Dict[str, Any]:
        """批量解析

        Args:
            names: 要解析的名称列表，None 则解析全部

        Returns:
            名称 -> 实例 的映射
        """
        targets = names or list(self._refs.keys())
        results: Dict[str, Any] = {}

        for name in targets:
            if name not in self._refs:
                raise NotFoundError(f"未注册的 LazyRef: {name}")
            results[name] = await self._refs[name].resolve()

        return results

    def get_unresolved(self) -> List[str]:
        """获取尚未解析的引用名"""
        return [n for n, ref in self._refs.items() if not ref.is_resolved]

    def get_status(self) -> Dict[str, Any]:
        """获取所有引用的状态"""
        return {
            name: {
                "resolved": ref.is_resolved,
            }
            for name, ref in self._refs.items()
        }

    def reset_all(self) -> None:
        """重置全部（仅测试用）"""
        for ref in self._refs.values():
            ref.reset()


# ═══════════════════════════════════════════════════════════════
# 装饰器：标记需要延迟解析的字段
# ═══════════════════════════════════════════════════════════════

def lazy_property(func: Callable[[Any], LazyRef]):
    """延迟属性装饰器

    用法：
        class Service:
            @lazy_property
            def db(self) -> LazyRef:
                return LazyRef("database", registry=locator)

            async def use_db(self):
                db = await self.db.resolve()
                return db.query(...)
    """
    attr_name = f"_lazy_{func.__name__}"

    @property
    def wrapper(self):
        if not hasattr(self, attr_name):
            ref = func(self)
            setattr(self, attr_name, ref)
        return getattr(self, attr_name)

    return wrapper

