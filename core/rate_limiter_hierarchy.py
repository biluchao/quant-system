# -*- coding: utf-8 -*-
"""
core/rate_limiter_hierarchy.py — 四层限流（API / 策略 / 全局 / 交易所）

铁律声明：本文件严格执行八项铁律
限流是系统稳定的第一道防线，必须在请求进入业务层之前拦截。
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .types import QuantError, ErrorCode


class RateLimitExceededError(QuantError):
    """限流异常"""
    pass


@dataclass
class RateLimitConfig:
    """限流配置"""
    name: str
    max_requests: int       # 时间窗口内最大请求数
    window_sec: float      # 时间窗口大小(秒)
    burst_size: int = 0    # 突发容量（0=与max_requests相同）


@dataclass
class RateLimitStats:
    """限流统计"""
    name: str
    total_requests: int = 0
    allowed_requests: int = 0
    rejected_requests: int = 0
    current_tokens: float = 0.0
    last_refill_time: float = 0.0


class TokenBucket:
    """
    令牌桶限流器（单实例）

    线程安全 + 协程安全
    """

    def __init__(self, config: RateLimitConfig):
        self._config = config
        self._capacity = config.burst_size if config.burst_size > 0 else config.max_requests
        self._tokens = float(self._capacity)
        self._last_refill = time.monotonic()
        self._stats = RateLimitStats(
            name=config.name,
            current_tokens=float(self._capacity),
            last_refill_time=self._last_refill,
        )
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> None:
        """获取令牌"""
        async with self._lock:
            self._refill()
            self._stats.total_requests += 1

            if self._tokens >= tokens:
                self._tokens -= tokens
                self._stats.allowed_requests += 1
                self._stats.current_tokens = self._tokens
            else:
                self._stats.rejected_requests += 1
                raise RateLimitExceededError(
                    ErrorCode.CORE_RATE_LIMIT_001,
                    f"Rate limit '{self._config.name}' exceeded: "
                    f"{self._tokens:.1f}/{self._capacity} tokens available, "
                    f"need {tokens}"
                )

    def try_acquire(self, tokens: int = 1) -> bool:
        """非阻塞尝试获取令牌"""
        now = time.monotonic()
        elapsed = now - self._last_refill
        refill_rate = self._config.max_requests / self._config.window_sec
        new_tokens = elapsed * refill_rate

        self._tokens = min(self._capacity, self._tokens + new_tokens)
        self._last_refill = now

        self._stats.total_requests += 1

        if self._tokens >= tokens:
            self._tokens -= tokens
            self._stats.allowed_requests += 1
            self._stats.current_tokens = self._tokens
            return True
        else:
            self._stats.rejected_requests += 1
            return False

    def _refill(self) -> None:
        """补充令牌"""
        now = time.monotonic()
        elapsed = now - self._last_refill
        refill_rate = self._config.max_requests / self._config.window_sec
        new_tokens = elapsed * refill_rate

        self._tokens = min(self._capacity, self._tokens + new_tokens)
        self._last_refill = now
        self._stats.current_tokens = self._tokens
        self._stats.last_refill_time = now

    def stats(self) -> RateLimitStats:
        return self._stats


class RateLimiterHierarchy:
    """
    四层限流器

    层级（从外到内）：
    1. 交易所层 — 按交易所API限制
    2. 全局层 — 系统总请求数限制
    3. 策略层 — 按策略限制
    4. API层 — 按接口端点限制

    请求必须通过所有层才能放行。
    """

    def __init__(self):
        self._exchange_limiters: Dict[str, TokenBucket] = {}
        self._global_limiters: Dict[str, TokenBucket] = {}
        self._strategy_limiters: Dict[str, TokenBucket] = {}
        self._api_limiters: Dict[str, TokenBucket] = {}
        self._lock = asyncio.Lock()

    # ─── 交易所层 ───────────────────────────────────────────

    def add_exchange_limiter(self, exchange: str, config: RateLimitConfig) -> None:
        """添加交易所限流"""
        self._exchange_limiters[exchange] = TokenBucket(config)

    async def check_exchange(self, exchange: str, tokens: int = 1) -> None:
        """检查交易所限流"""
        if exchange not in self._exchange_limiters:
            return  # 未配置则不限流
        await self._exchange_limiters[exchange].acquire(tokens)

    # ─── 全局层 ────────────────────────────────────────────

    def add_global_limiter(self, name: str, config: RateLimitConfig) -> None:
        self._global_limiters[name] = TokenBucket(config)

    async def check_global(self, name: str = "default", tokens: int = 1) -> None:
        if name not in self._global_limiters:
            return
        await self._global_limiters[name].acquire(tokens)

    # ─── 策略层 ────────────────────────────────────────────

    def add_strategy_limiter(self, strategy_id: str, config: RateLimitConfig) -> None:
        self._strategy_limiters[strategy_id] = TokenBucket(config)

    async def check_strategy(self, strategy_id: str, tokens: int = 1) -> None:
        if strategy_id not in self._strategy_limiters:
            return
        await self._strategy_limiters[strategy_id].acquire(tokens)

    # ─── API层 ─────────────────────────────────────────────

    def add_api_limiter(self, endpoint: str, config: RateLimitConfig) -> None:
        self._api_limiters[endpoint] = TokenBucket(config)

    async def check_api(self, endpoint: str, tokens: int = 1) -> None:
        if endpoint not in self._api_limiters:
            return
        await self._api_limiters[endpoint].acquire(tokens)

    # ─── 组合检查 ──────────────────────────────────────────

    async def check_all(
        self,
        *,
        exchange: Optional[str] = None,
        global_name: str = "default",
        strategy_id: Optional[str] = None,
        endpoint: Optional[str] = None,
        tokens: int = 1,
    ) -> None:
        """
        依次检查所有层级，任一失败则抛出
        顺序：全局 → 交易所 → 策略 → API
        """
        # 全局（最先检查，防止雪崩）
        await self.check_global(global_name, tokens)

        # 交易所
        if exchange:
            await self.check_exchange(exchange, tokens)

        # 策略
        if strategy_id:
            await self.check_strategy(strategy_id, tokens)

        # API
        if endpoint:
            await self.check_api(endpoint, tokens)

    def get_all_stats(self) -> Dict[str, Dict[str, RateLimitStats]]:
        """获取所有限流器统计"""
        return {
            "exchange": {k: v.stats() for k, v in self._exchange_limiters.items()},
            "global": {k: v.stats() for k, v in self._global_limiters.items()},
            "strategy": {k: v.stats() for k, v in self._strategy_limiters.items()},
            "api": {k: v.stats() for k, v in self._api_limiters.items()},
        }

    def reset_all(self) -> None:
        """重置所有限流器"""
        for bucket in self._exchange_limiters.values():
            bucket._tokens = float(bucket._capacity)
        for bucket in self._global_limiters.values():
            bucket._tokens = float(bucket._capacity)
        for bucket in self._strategy_limiters.values():
            bucket._tokens = float(bucket._capacity)
        for bucket in self._api_limiters.values():
            bucket._tokens = float(bucket._capacity)
