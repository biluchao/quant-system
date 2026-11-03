"""core/bootstrap.py — 模块初始化编排与依赖注入容器

铁律声明：本文件遵守
- 类型统一：所有接口参数/返回值有类型注解
- 命名统一：类名 PascalCase，方法动词开头，私有 _ 前缀
- 异常统一：启动失败抛精确异常，不裸 except
- 三层防线：注册校验 → 依赖校验 → 启动后校验

设计要点：
1. ServiceLocator 是轻量依赖注入容器（单例），按接口名注册/获取组件
2. Bootstrap 负责按依赖顺序启动/关闭组件
3. 所有组件实现 Lifecycle 协议（start/stop/health_check）
4. 启动顺序由依赖关系自动推导（拓扑排序）
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Type, TypeVar

from core.types import ComponentStatus


logger = logging.getLogger(__name__)

T = TypeVar("T")


# ═══════════════════════════════════════════════════════════════
# 组件生命周期协议
# ═══════════════════════════════════════════════════════════════

class Lifecycle(ABC):
    """所有可启动组件的协议

    任何需要被 Bootstrap 管理的组件必须实现此接口。
    """

    @abstractmethod
    async def start(self) -> None:
        """启动组件"""
        ...

    @abstractmethod
    async def stop(self) -> None:
        """停止组件"""
        ...

    @abstractmethod
    def health_check(self) -> bool:
        """健康检查，返回是否健康"""
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """组件名称"""
        ...

    @property
    def status(self) -> ComponentStatus:
        """组件当前状态"""
        return ComponentStatus.RUNNING


# ═══════════════════════════════════════════════════════════════
# 启动阶段枚举
# ═══════════════════════════════════════════════════════════════

class BootstrapPhase(str, Enum):
    """启动阶段"""

    UNINITIALIZED = "UNINITIALIZED"
    REGISTERING = "REGISTERING"
    RESOLVING_DEPS = "RESOLVING_DEPS"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


class StartupError(Exception):
    """启动失败异常"""
    code = 2000

    def __init__(self, message: str, component: Optional[str] = None) -> None:
        super().__init__(message)
        self.component = component


class DependencyError(StartupError):
    """依赖解析失败（循环依赖 / 缺失依赖）"""
    code = 2001


class ShutdownError(Exception):
    """关闭失败异常"""
    code = 2100


# ═══════════════════════════════════════════════════════════════
# 依赖注入容器
# ═══════════════════════════════════════════════════════════════

class ServiceLocator:
    """轻量依赖注入容器（单例）

    功能：
    - 按接口名/类型注册组件实例
    - 延迟解析（Lazy Resolution）
    - 工厂函数支持
    - 线程安全（asyncio 单线程模型下无需锁）
    """

    _instance: Optional["ServiceLocator"] = None

    def __init__(self) -> None:
        # interface_name -> instance
        self._instances: Dict[str, Any] = {}
        # interface_name -> factory function
        self._factories: Dict[str, Callable[..., Any]] = {}
        # 注册顺序（用于调试）
        self._registration_order: List[str] = []

    @classmethod
    def instance(cls) -> "ServiceLocator":
        """获取单例"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """重置容器（仅测试用）"""
        cls._instance = None

    def register(self, interface: str, instance: Any) -> None:
        """注册组件实例

        Args:
            interface: 接口名（如 "EventBus", "RiskEngine"）
            instance: 组件实例
        """
        if not interface:
            raise ValueError("interface 不能为空")
        if interface in self._instances:
            logger.warning(f"覆盖已注册的组件: {interface}")
        self._instances[interface] = instance
        if interface not in self._registration_order:
            self._registration_order.append(interface)

    def register_factory(self, interface: str, factory: Callable[..., Any]) -> None:
        """注册工厂函数（延迟创建）

        factory 在首次 resolve 时调用。
        """
        if not interface:
            raise ValueError("interface 不能为空")
        if not callable(factory):
            raise TypeError("factory 必须是可调用对象")
        self._factories[interface] = factory

    def resolve(self, interface: str) -> Any:
        """获取组件实例

        优先返回已注册实例，否则尝试工厂创建。
        """
        if interface in self._instances:
            return self._instances[interface]

        if interface in self._factories:
            instance = self._factories[interface]()
            self._instances[interface] = instance
            if interface not in self._registration_order:
                self._registration_order.append(interface)
            return instance

        raise NotFoundError(f"未注册的组件: {interface}")

    def has(self, interface: str) -> bool:
        """检查是否已注册"""
        return interface in self._instances or interface in self._factories

    def unregister(self, interface: str) -> bool:
        """注销组件（返回是否成功）"""
        removed = False
        if interface in self._instances:
            del self._instances[interface]
            removed = True
        if interface in self._factories:
            del self._factories[interface]
            removed = True
        if interface in self._registration_order:
            self._registration_order.remove(interface)
        return removed

    def list_registered(self) -> List[str]:
        """列出所有已注册组件名"""
        return list(self._registration_order)

    def clear(self) -> None:
        """清空容器"""
        self._instances.clear()
        self._factories.clear()
        self._registration_order.clear()


# 全局便捷函数
def get_service(interface: str) -> Any:
    """获取服务（便捷函数）"""
    return ServiceLocator.instance().resolve(interface)


def register_service(interface: str, instance: Any) -> None:
    """注册服务（便捷函数）"""
    ServiceLocator.instance().register(interface, instance)


# ═══════════════════════════════════════════════════════════════
# 组件元数据
# ═══════════════════════════════════════════════════════════════

@dataclass
class ComponentMeta:
    """组件元数据（用于启动编排）"""

    name: str
    instance: Lifecycle
    dependencies: List[str] = field(default_factory=list)
    # 启动超时（秒）
    start_timeout: float = 30.0
    # 关闭超时（秒）
    stop_timeout: float = 10.0
    # 是否关键组件（关键组件失败则系统启动中止）
    critical: bool = True
    # 启动阶段分组（数字越小越先启动）
    phase_group: int = 100


# ═══════════════════════════════════════════════════════════════
# 启动编排器
# ═══════════════════════════════════════════════════════════════

class Bootstrap:
    """模块启动编排器

    职责：
    1. 注册组件及其依赖关系
    2. 拓扑排序确定启动顺序
    3. 按序启动组件（带超时）
    4. 启动失败时自动回滚已启动的组件
    5. 按逆序关闭所有组件
    """

    def __init__(self) -> None:
        self._components: Dict[str, ComponentMeta] = {}
        self._started: List[str] = []
        self._phase: BootstrapPhase = BootstrapPhase.UNINITIALIZED
        self._locator = ServiceLocator.instance()

    # ── 注册 ──

    def register_component(
        self,
        name: str,
        instance: Lifecycle,
        dependencies: Optional[List[str]] = None,
        start_timeout: float = 30.0,
        stop_timeout: float = 10.0,
        critical: bool = True,
        phase_group: int = 100,
    ) -> None:
        """注册组件

        Args:
            name: 组件唯一名
            instance: 组件实例（实现 Lifecycle）
            dependencies: 依赖的组件名列表
            start_timeout: 启动超时（秒）
            stop_timeout: 关闭超时（秒）
            critical: 是否关键组件
            phase_group: 启动阶段分组
        """
        if not name:
            raise ValueError("组件名不能为空")
        if not isinstance(instance, Lifecycle):
            raise TypeError(f"组件 {name} 必须实现 Lifecycle 接口")
        if name in self._components:
            raise DuplicateError(f"组件已注册: {name}")

        self._components[name] = ComponentMeta(
            name=name,
            instance=instance,
            dependencies=dependencies or [],
            start_timeout=start_timeout,
            stop_timeout=stop_timeout,
            critical=critical,
            phase_group=phase_group,
        )

        # 同步注册到 ServiceLocator
        self._locator.register(name, instance)

    # ── 依赖解析 ──

    def resolve_order(self) -> List[str]:
        """拓扑排序，返回启动顺序

        使用 Kahn 算法。
        检测循环依赖。
        """
        # 构建入度表和邻接表
        in_degree: Dict[str, int] = {name: 0 for name in self._components}
        graph: Dict[str, List[str]] = defaultdict(list)

        for name, meta in self._components.items():
            for dep in meta.dependencies:
                if dep not in self._components:
                    raise DependencyError(
                        f"组件 {name} 的依赖 {dep} 未注册",
                        component=name,
                    )
                graph[dep].append(name)
                in_degree[name] += 1

        # Kahn 算法
        queue: deque = deque([n for n, d in in_degree.items() if d == 0])
        result: List[str] = []
        visited_count = 0

        while queue:
            # 同层级按 phase_group 排序
            queue = deque(sorted(queue, key=lambda n: self._components[n].phase_group))
            node = queue.popleft()
            result.append(node)
            visited_count += 1

            for neighbor in sorted(graph[node], key=lambda n: self._components[n].phase_group):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        # 检测循环依赖
        if visited_count != len(self._components):
            remaining = [n for n, d in in_degree.items() if d > 0]
            raise DependencyError(
                f"检测到循环依赖: {remaining}",
                component=None,
            )

        return result

    # ── 启动 ──

    async def start_all(self) -> None:
        """启动所有组件"""
        self._phase = BootstrapPhase.RESOLVING_DEPS

        try:
            order = self.resolve_order()
        except DependencyError:
            self._phase = BootstrapPhase.FAILED
            raise

        self._phase = BootstrapPhase.STARTING
        logger.info(f"启动顺序: {' -> '.join(order)}")

        for name in order:
            meta = self._components[name]
            logger.info(f"  [{name}] 启动中...")

            try:
                await asyncio.wait_for(
                    meta.instance.start(),
                    timeout=meta.start_timeout,
                )
                self._started.append(name)
                logger.info(f"  [{name}] ✅ 已启动")
            except asyncio.TimeoutError:
                logger.error(f"  [{name}] ❌ 启动超时（{meta.start_timeout}s）")
                await self._rollback()
                raise StartupError(
                    f"组件 {name} 启动超时",
                    component=name,
                )
            except Exception as e:
                logger.error(f"  [{name}] ❌ 启动失败: {e}")
                if meta.critical:
                    await self._rollback()
                    raise StartupError(
                        f"关键组件 {name} 启动失败: {e}",
                        component=name,
                    ) from e
                # 非关键组件失败不阻断
                logger.warning(f"  非关键组件 {name} 跳过")

        self._phase = BootstrapPhase.RUNNING
        logger.info(f"✅ 所有组件启动完成（{len(self._started)} 个）")

    async def _rollback(self) -> None:
        """回滚：关闭已启动的组件"""
        logger.warning("启动失败，开始回滚...")
        await self._stop_in_reverse(self._started)

    # ── 关闭 ──

    async def stop_all(self) -> None:
        """关闭所有组件（逆序）"""
        self._phase = BootstrapPhase.STOPPING
        await self._stop_in_reverse(list(self._started))
        self._started.clear()
        self._phase = BootstrapPhase.STOPPED
        logger.info("✅ 所有组件已关闭")

    async def _stop_in_reverse(self, names: List[str]) -> None:
        """按逆序关闭"""
        for name in reversed(names):
            if name not in self._components:
                continue
            meta = self._components[name]
            logger.info(f"  [{name}] 关闭中...")

            try:
                await asyncio.wait_for(
                    meta.instance.stop(),
                    timeout=meta.stop_timeout,
                )
                logger.info(f"  [{name}] ✅ 已关闭")
            except asyncio.TimeoutError:
                logger.error(f"  [{name}] 关闭超时，强制跳过")
            except Exception as e:
                logger.error(f"  [{name}] 关闭异常: {e}")
                # 关闭阶段不抛出，尽量关闭所有组件

    # ── 健康检查 ──

    def health_check_all(self) -> Dict[str, bool]:
        """对所有组件做健康检查"""
        results: Dict[str, bool] = {}
        for name, meta in self._components.items():
            try:
                results[name] = meta.instance.health_check()
            except Exception as e:
                logger.error(f"健康检查异常 [{name}]: {e}")
                results[name] = False
        return results

    def get_status(self) -> Dict[str, Any]:
        """获取启动器状态"""
        return {
            "phase": self._phase.value,
            "registered": list(self._components.keys()),
            "started": list(self._started),
            "health": self.health_check_all() if self._phase == BootstrapPhase.RUNNING else {},
        }

