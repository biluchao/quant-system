# -*- coding: utf-8 -*-
"""
core/circuit_breaker.py — 熔断器

铁律声明：本文件严格执行八项铁律
依赖服务故障时快速失败，防止级联崩溃。
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from .types import QuantError, ErrorCode


class CircuitState(str, Enum):
    CLOSED = "closed"        # 正常，允许请求
    OPEN = "open"            # 熔断，拒绝请求
    HALF_OPEN = "half_open"  # 试探，允许有限请求


class CircuitOpenError(QuantError):
    """熔断器开启异常"""
    pass


@dataclass
class CircuitBreakerConfig:
    """熔断器配置"""
    name: str
    failure_threshold: int = 5           # 连续失败次数触发熔断
    success_threshold: int = 2           # 半开状态下成功次数恢复
    open_duration_sec: float = 60.0      # 熔断持续时间
    half_open_max_calls: int = 3         # 半开状态最大试探请求数
    expected_exception: tuple = (Exception,)  # 哪些异常算失败


@dataclass
class CircuitBreakerStats:
    """熔断器统计"""
    name: str
    state: CircuitState = CircuitState.CLOSED
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    total_calls: int = 0
    total_failures: int = 0
    total_successes: int = 0
    total_rejected: int = 0
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None
    state_changed_at: float = field(default_factory=time.monotonic)


class CircuitBreaker:
    """
    熔断器

    状态流转：
    CLOSED → (连续失败达到阈值) → OPEN
    OPEN → (超时后) → HALF_OPEN
    HALF_OPEN → (成功达到阈值) → CLOSED
    HALF_OPEN → (失败) → OPEN
    """

    def __init__(self, config: CircuitBreakerConfig):
        self._config = config
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._consecutive_successes = 0
        self._half_open_calls = 0
        self._stats = CircuitBreakerStats(
            name=config.name,
            state=self._state,
            state_changed_at=time.monotonic(),
        )
        self._lock = asyncio.Lock()

    async def call(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        """通过熔断器调用函数"""
        async with self._lock:
            await self._check_state()

        if self._state == CircuitState.OPEN:
            self._stats.total_rejected += 1
            raise CircuitOpenError(
                ErrorCode.CORE_CIRCUIT_001,
                f"Circuit breaker '{self._config.name}' is OPEN "
                f"(consecutive failures: {self._consecutive_failures})"
            )

        # 执行调用
        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)

            await self._on_success()
            return result

        except self._config.expected_exception as e:
            await self._on_failure()
            raise

    async def _check_state(self) -> None:
        """检查并可能转换状态"""
        now = time.monotonic()

        if self._state == CircuitState.OPEN:
            if now - self._stats.state_changed_at >= self._config.open_duration_sec:
                self._state = CircuitState.HALF_OPEN
                self._half_open_calls = 0
                self._consecutive_successes = 0
                self._stats.state = self._state
                self._stats.state_changed_at = now

    async def _on_success(self) -> None:
        """成功回调"""
        async with self._lock:
            self._stats.total_calls += 1
            self._stats.total_successes += 1
            self._stats.last_success_time = time.monotonic()

            if self._state == CircuitState.HALF_OPEN:
                self._consecutive_successes += 1
                self._half_open_calls += 1
                if self._consecutive_successes >= self._config.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._consecutive_failures = 0
                    self._consecutive_successes = 0
                    self._stats.state = self._state
                    self._stats.state_changed_at = time.monotonic()
            else:
                self._consecutive_failures = 0

    async def _on_failure(self) -> None:
        """失败回调"""
        async with self._lock:
            self._stats.total_calls += 1
            self._stats.total_failures += 1
            self._stats.last_failure_time = time.monotonic()
            self._consecutive_failures += 1

            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                self._stats.state = self._state
                self._stats.state_changed_at = time.monotonic()
            elif self._consecutive_failures >= self._config.failure_threshold:
                self._state = CircuitState.OPEN
                self._stats.state = self._state
                self._stats.state_changed_at = time.monotonic()

    def force_open(self) -> None:
        """强制打开熔断器"""
        self._state = CircuitState.OPEN
        self._stats.state = self._state
        self._stats.state_changed_at = time.monotonic()

    def force_close(self) -> None:
        """强制关闭熔断器"""
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._consecutive_successes = 0
        self._stats.state = self._state
        self._stats.state_changed_at = time.monotonic()

    def reset(self) -> None:
        """重置熔断器"""
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._consecutive_successes = 0
        self._half_open_calls = 0
        self._stats = CircuitBreakerStats(
            name=self._config.name,
            state=self._state,
            state_changed_at=time.monotonic(),
        )

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def is_open(self) -> bool:
        return self._state == CircuitState.OPEN

    @property
    def is_closed(self) -> bool:
        return self._state == CircuitState.CLOSED

    def stats(self) -> CircuitBreakerStats:
        return self._stats


class CircuitBreakerRegistry:
    """熔断器注册表"""

    def __init__(self):
        self._breakers: Dict[str, CircuitBreaker] = {}

    def create(self, config: CircuitBreakerConfig) -> CircuitBreaker:
        if config.name in self._breakers:
            raise CircuitOpenError(
                ErrorCode.CORE_CIRCUIT_001,
                f"CircuitBreaker '{config.name}' already exists"
            )
        cb = CircuitBreaker(config)
        self._breakers[config.name] = cb
        return cb

    def get(self, name: str) -> CircuitBreaker:
        if name not in self._breakers:
            raise CircuitOpenError(
                ErrorCode.CORE_CIRCUIT_001,
                f"CircuitBreaker '{name}' not found"
            )
        return self._breakers[name]

    def list_stats(self) -> Dict[str, CircuitBreakerStats]:
        return {name: cb.stats() for name, cb in self._breakers.items()}

    def force_open_all(self) -> None:
        for cb in self._breakers.values():
            cb.force_open()

    def reset_all(self) -> None:
        for cb in self._breakers.values():
            cb.reset()
