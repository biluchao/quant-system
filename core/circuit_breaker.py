"""core/circuit_breaker.py — 熔断器（分级熔断 + 状态机）

铁律声明：本文件遵守
- 类型统一：状态/统计使用明确类型
- 命名统一：类 PascalCase，方法动词开头
- 异常统一：熔断打开时抛精确异常
- 幂等审计：每次状态转换可追踪

设计要点：
1. 三级熔断：策略级 / 品种级 / 全局级
2. 状态机：CLOSED → OPEN → HALF_OPEN → CLOSED
3. 失败率阈值触发（不是失败次数）
4. 半开状态试探性放行（探测是否恢复）
5. 熔断触发后只接受人工重置（防抖动）
6. 支持回调通知（告警/日志/事件）
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Tuple

from core.types import AlertLevel, CircuitLevel, StrategyId, Symbol, Timestamp


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# 异常
# ═══════════════════════════════════════════════════════════════

class CircuitBreakerError(Exception):
    """熔断器异常基类"""
    code = 7000


class CircuitOpenError(CircuitBreakerError):
    """熔断器处于打开状态（请求被拒绝）"""
    code = 7001

    def __init__(
        self,
        name: str,
        level: CircuitLevel,
        failure_rate: float,
        threshold: float,
    ) -> None:
        super().__init__(
            f"熔断器打开 [{name}] level={level.name} "
            f"failure_rate={failure_rate:.2%} threshold={threshold:.2%}"
        )
        self.name = name
        self.level = level
        self.failure_rate = failure_rate
        self.threshold = threshold


# ═══════════════════════════════════════════════════════════════
# 熔断状态
# ═══════════════════════════════════════════════════════════════

class CircuitState(str):
    """熔断状态"""

    CLOSED = "CLOSED"       # 正常，请求放行
    OPEN = "OPEN"           # 熔断，请求拒绝
    HALF_OPEN = "HALF_OPEN" # 半开，试探性放行


# ═══════════════════════════════════════════════════════════════
# 熔断配置
# ═══════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class CircuitBreakerConfig:
    """熔断器配置"""

    name: str
    level: CircuitLevel = CircuitLevel.STRATEGY
    # 失败率阈值（0.0 ~ 1.0）
    failure_threshold: float = 0.5
    # 统计窗口大小（请求数）
    rolling_window_size: int = 100
    # 熔断后等待多久进入半开（秒）
    open_timeout_sec: float = 30.0
    # 半开状态允许的试探请求数
    half_open_max_calls: int = 3
    # 最小请求数（低于此数不触发熔断，防止低流量误判）
    min_requests: int = 10
    # 是否允许自动重置（False = 必须人工重置）
    auto_reset: bool = False


# ═══════════════════════════════════════════════════════════════
# 熔断统计
# ═══════════════════════════════════════════════════════════════

@dataclass
class CircuitStats:
    """熔断统计"""

    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    state_changes: int = 0
    last_failure_time: float = 0.0
    last_success_time: float = 0.0
    last_opened_time: float = 0.0
    last_closed_time: float = 0.0

    @property
    def failure_rate(self) -> float:
        """当前失败率"""
        if self.total_calls == 0:
            return 0.0
        return self.failed_calls / self.total_calls

    @property
    def success_rate(self) -> float:
        if self.total_calls == 0:
            return 0.0
        return self.successful_calls / self.total_calls

    def snapshot(self) -> dict:
        return {
            "total_calls": self.total_calls,
            "successful_calls": self.successful_calls,
            "failed_calls": self.failed_calls,
            "rejected_calls": self.rejected_calls,
            "failure_rate": round(self.failure_rate, 4),
            "success_rate": round(self.success_rate, 4),
            "state_changes": self.state_changes,
        }


# ═══════════════════════════════════════════════════════════════
# 单个熔断器
# ═══════════════════════════════════════════════════════════════

class CircuitBreaker:
    """熔断器（状态机）

    状态流转：
        CLOSED ──失败率超阈值──▶ OPEN
        OPEN ──超时──▶ HALF_OPEN
        HALF_OPEN ──成功──▶ CLOSED
        HALF_OPEN ──失败──▶ OPEN
        OPEN ──人工/自动重置──▶ CLOSED
    """

    def __init__(self, config: CircuitBreakerConfig) -> None:
        if not 0 < config.failure_threshold <= 1:
            raise ValueError("failure_threshold 必须在 (0, 1] 范围内")
        if config.rolling_window_size < 1:
            raise ValueError("rolling_window_size 必须 >= 1")

        self._config = config
        self._state = CircuitState.CLOSED
        self._stats = CircuitStats()

        # 滚动窗口（记录每次调用的成功/失败）
        self._calls: Deque[bool] = deque(maxlen=config.rolling_window_size)

        # 半开状态计数器
        self._half_open_calls = 0

        # 锁（防止并发状态竞争）
        self._lock = asyncio.Lock()

        # 回调
        self._on_open: Optional[Callable[[str], Awaitable[None]]] = None
        self._on_close: Optional[Callable[[str], Awaitable[None]]] = None
        self._on_half_open: Optional[Callable[[str], Awaitable[None]]] = None

    @property
    def name(self) -> str:
        return self._config.name

    @property
    def level(self) -> CircuitLevel:
        return self._config.level

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def stats(self) -> CircuitStats:
        return self._stats

    @property
    def is_open(self) -> bool:
        return self._state == CircuitState.OPEN

    @property
    def is_closed(self) -> bool:
        return self._state == CircuitState.CLOSED

    @property
    def is_half_open(self) -> bool:
        return self._state == CircuitState.HALF_OPEN

    # ── 回调注册 ──

    def on_open(self, callback: Callable[[str], Awaitable[None]]) -> None:
        self._on_open = callback

    def on_close(self, callback: Callable[[str], Awaitable[None]]) -> None:
        self._on_close = callback

    def on_half_open(self, callback: Callable[[str], Awaitable[None]]) -> None:
        self._on_half_open = callback

    # ── 核心逻辑 ──

    async def call(self, func: Callable[..., Awaitable[T]], *args, **kwargs) -> T:
        """执行受保护的调用

        根据当前状态决定放行/拒绝。
        """
        await self._check_state()

        if self._state == CircuitState.OPEN:
            self._stats.rejected_calls += 1
            raise CircuitOpenError(
                self._config.name,
                self._config.level,
                self._stats.failure_rate,
                self._config.failure_threshold,
            )

        # 半开状态：限制试探请求数
        if self._state == CircuitState.HALF_OPEN:
            async with self._lock:
                if self._half_open_calls >= self._config.half_open_max_calls:
                    self._stats.rejected_calls += 1
                    raise CircuitOpenError(
                        self._config.name,
                        self._config.level,
                        self._stats.failure_rate,
                        self._config.failure_threshold,
                    )
                self._half_open_calls += 1

        # 执行调用
        start = time.monotonic()
        try:
            result = await func(*args, **kwargs)
            await self._record_success()
            return result
        except Exception:
            await self._record_failure()
            raise

    async def _check_state(self) -> None:
        """检查是否需要从 OPEN 转换到 HALF_OPEN"""
        if self._state != CircuitState.OPEN:
            return

        elapsed = time.monotonic() - self._stats.last_opened_time
        if elapsed >= self._config.open_timeout_sec:
            await self._transition_to(CircuitState.HALF_OPEN)
            self._half_open_calls = 0

    async def _record_success(self) -> None:
        """记录成功调用"""
        async with self._lock:
            self._calls.append(True)
            self._stats.successful_calls += 1
            self._stats.total_calls += 1
            self._stats.last_success_time = time.monotonic()

            # 半开状态下成功 → 恢复
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_calls -= 1
                if self._half_open_calls <= 0:
                    await self._transition_to(CircuitState.CLOSED)

    async def _record_failure(self) -> None:
        """记录失败调用"""
        async with self._lock:
            self._calls.append(False)
            self._stats.failed_calls += 1
            self._stats.total_calls += 1
            self._stats.last_failure_time = time.monotonic()

            # 检查是否需要熔断
            if self._state == CircuitState.CLOSED:
                if self._should_trip():
                    await self._transition_to(CircuitState.OPEN)
            elif self._state == CircuitState.HALF_OPEN:
                # 半开状态任何失败都重新打开
                await self._transition_to(CircuitState.OPEN)

    def _should_trip(self) -> bool:
        """是否应该触发熔断"""
        # 请求数不够，不判断
        if len(self._calls) < self._config.min_requests:
            return False

        # 计算窗口内失败率
        failures = sum(1 for c in self._calls if not c)
        rate = failures / len(self._calls)
        return rate >= self._config.failure_threshold

    async def _transition_to(self, new_state: CircuitState) -> None:
        """状态转换"""
        if self._state == new_state:
            return

        old_state = self._state
        self._state = new_state
        self._stats.state_changes += 1

        logger.warning(
            f"熔断器状态转换 [{self._config.name}] "
            f"{old_state} → {new_state}"
        )

        if new_state == CircuitState.OPEN:
            self._stats.last_opened_time = time.monotonic()
            if self._on_open:
                try:
                    await self._on_open(self._config.name)
                except Exception as e:
                    logger.error(f"on_open 回调失败: {e}")

        elif new_state == CircuitState.HALF_OPEN:
            self._half_open_calls = 0
            if self._on_half_open:
                try:
                    await self._on_half_open(self._config.name)
                except Exception as e:
                    logger.error(f"on_half_open 回调失败: {e}")

        elif new_state == CircuitState.CLOSED:
            self._stats.last_closed_time = time.monotonic()
            # 清空统计，重新开始
            self._calls.clear()
            if self._on_close:
                try:
                    await self._on_close(self._config.name)
                except Exception as e:
                    logger.error(f"on_close 回调失败: {e}")

    # ── 手动控制 ──

    async def reset(self) -> None:
        """手动重置（关闭熔断）

        仅人工干预或自动重置配置下可用。
        """
        if self._state == CircuitState.OPEN and not self._config.auto_reset:
            logger.warning(
                f"⚠️ 熔断器 [{self._config.name}] 被人工重置 "
                f"(level={self._config.level.name})"
            )
        await self._transition_to(CircuitState.CLOSED)

    async def force_open(self) -> None:
        """手动强制打开（紧急停止某策略/品种）"""
        logger.warning(
            f"⚠️ 熔断器 [{self._config.name}] 被强制打开 "
            f"(level={self._config.level.name})"
        )
        await self._transition_to(CircuitState.OPEN)

    def get_status(self) -> dict:
        return {
            "name": self._config.name,
            "level": self._config.level.name,
            "state": self._state,
            "is_open": self.is_open,
            "failure_threshold": self._config.failure_threshold,
            "current_failure_rate": round(self._stats.failure_rate, 4),
            "calls_window_size": len(self._calls),
            "min_requests": self._config.min_requests,
            "auto_reset": self._config.auto_reset,
            "open_timeout_sec": self._config.open_timeout_sec,
            **self._stats.snapshot(),
        }


# ═══════════════════════════════════════════════════════════════
# 分级熔断管理器
# ═══════════════════════════════════════════════════════════════

class CircuitBreakerManager:
    """分级熔断管理器

    管理三级熔断器：策略级 / 品种级 / 全局级。
    全局熔断时，所有请求被拒绝。
    品种熔断时，该品种的所有操作被拒绝。
    策略熔断时，该策略的所有操作被拒绝。
    """

    def __init__(self) -> None:
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._level_index: Dict[CircuitLevel, List[str]] = {
            CircuitLevel.STRATEGY: [],
            CircuitLevel.SYMBOL: [],
            CircuitLevel.GLOBAL: [],
        }

    def create(self, config: CircuitBreakerConfig) -> CircuitBreaker:
        """创建熔断器"""
        if config.name in self._breakers:
            raise DuplicateError(f"熔断器已存在: {config.name}")

        breaker = CircuitBreaker(config)
        self._breakers[config.name] = breaker
        self._level_index[config.level].append(config.name)
        return breaker

    def get(self, name: str) -> CircuitBreaker:
        if name not in self._breakers:
            raise NotFoundError(f"熔断器不存在: {name}")
        return self._breakers[name]

    def get_or_create(self, config: CircuitBreakerConfig) -> CircuitBreaker:
        """获取或创建"""
        if config.name in self._breakers:
            return self._breakers[config.name]
        return self.create(config)

    # ── 分级查询 ──

    def get_by_level(self, level: CircuitLevel) -> List[CircuitBreaker]:
        """获取某级别的所有熔断器"""
        names = self._level_index.get(level, [])
        return [self._breakers[n] for n in names if n in self._breakers]

    def is_strategy_open(self, strategy_id: str) -> bool:
        """策略级是否熔断"""
        name = f"strategy:{strategy_id}"
        return name in self._breakers and self._breakers[name].is_open

    def is_symbol_open(self, symbol: str) -> bool:
        """品种级是否熔断"""
        name = f"symbol:{symbol}"
        return name in self._breakers and self._breakers[name].is_open

    def is_global_open(self) -> bool:
        """全局是否熔断"""
        # 查找名为 "global" 的熔断器
        for name, breaker in self._breakers.items():
            if breaker.level == CircuitLevel.GLOBAL and breaker.is_open:
                return True
        return False

    def can_trade(
        self,
        strategy_id: Optional[str] = None,
        symbol: Optional[str] = None,
    ) -> Tuple[bool, Optional[str]]:
        """综合判断是否可以交易

        Returns:
            (是否可以, 拒绝原因)
        """
        # 全局优先
        if self.is_global_open():
            return False, "全局熔断已触发"

        if symbol:
            name = f"symbol:{symbol}"
            if name in self._breakers and self._breakers[name].is_open:
                return False, f"品种 {symbol} 熔断"

        if strategy_id:
            name = f"strategy:{strategy_id}"
            if name in self._breakers and self._breakers[name].is_open:
                return False, f"策略 {strategy_id} 熔断"

        return True, None

    # ── 手动控制 ──

    async def reset_all(self) -> None:
        """重置所有熔断器"""
        for breaker in self._breakers.values():
            if breaker.is_open:
                await breaker.reset()
        logger.warning("⚠️ 所有熔断器已重置（人工操作）")

    async def emergency_stop_all(self) -> None:
        """紧急熔断所有（全局停止）"""
        for breaker in self._breakers.values():
            if not breaker.is_open:
                await breaker.force_open()
        logger.error("🚨 所有熔断器已强制打开（紧急停止）")

    # ── 查询 ──

    def list_all(self) -> List[str]:
        return list(self._breakers.keys())

    def get_all_status(self) -> Dict[str, dict]:
        return {
            name: breaker.get_status()
            for name, breaker in self._breakers.items()
        }

    def get_open_breakers(self) -> List[str]:
        """获取所有处于打开状态的熔断器"""
        return [
            name for name, b in self._breakers.items()
            if b.is_open
        ]

    def shutdown(self) -> None:
        self._breakers.clear()
        self._level_index.clear()


# ═══════════════════════════════════════════════════════════════
# 装饰器
# ═══════════════════════════════════════════════════════════════

def with_circuit_breaker(name: str):
    """熔断器装饰器

    Usage:
        @with_circuit_breaker("strategy:arbitrage_v1")
        async def execute_order(order):
            ...
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            from core.bootstrap import get_service
            manager = get_service("circuit_breaker_manager")
            breaker = manager.get(name)
            return await breaker.call(func, *args, **kwargs)
        return wrapper
    return decorator

