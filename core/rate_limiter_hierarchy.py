"""core/rate_limiter_hierarchy.py — 四层限流

铁律声明：本文件遵守
- 类型统一：所有限流参数有类型注解
- 命名统一：类 PascalCase，方法动词开头
- 异常统一：限流拒绝抛精确异常
- 幂等审计：每次限流决策可追踪

设计要点：
四层限流（由外到内）：
1. API 限流 — 外部 API 请求（按 IP/用户/接口）
2. 策略限流 — 单个策略的下单频率
3. 全局限流 — 全系统总下单频率
4. 交易所限流 — 单个交易所的 API 权重/频率限制

决策顺序：从最外层到最内层，任何一层拒绝则整体拒绝。
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional, Tuple

from core.types import StrategyId, Timestamp


logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# 异常
# ═══════════════════════════════════════════════════════════════

class RateLimitError(Exception):
    """限流异常基类"""
    code = 6000


class RateLimitExceeded(RateLimitError):
    """触发限流"""
    code = 6001

    def __init__(
        self,
        message: str,
        layer: str,
        limit: float,
        current: float,
        retry_after_ms: Optional[int] = None,
    ) -> None:
        super().__init__(message)
        self.layer = layer
        self.limit = limit
        self.current = current
        self.retry_after_ms = retry_after_ms


# ═══════════════════════════════════════════════════════════════
# 限流层
# ═══════════════════════════════════════════════════════════════

class RateLimitLayer(str):
    """限流层标识"""

    API = "api"
    STRATEGY = "strategy"
    GLOBAL = "global"
    EXCHANGE = "exchange"


# ═══════════════════════════════════════════════════════════════
# 限流配置
# ═══════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class RateLimitConfig:
    """限流配置

    rate: 每秒允许的最大请求数
    burst: 突发容量（令牌桶容量）
    window_sec: 滑动窗口大小（秒），用于精确计数

    实现：令牌桶算法（Token Bucket）
    """

    rate: float           # 每秒令牌生成速率
    burst: int = 0        # 令牌桶容量（0 = 自动取 max(rate, 1)）
    window_sec: float = 1.0  # 统计窗口

    def __post_init__(self) -> None:
        if self.rate <= 0:
            raise ValueError("rate 必须 > 0")
        if self.window_sec <= 0:
            raise ValueError("window_sec 必须 > 0")


# ═══════════════════════════════════════════════════════════════
# 令牌桶
# ═══════════════════════════════════════════════════════════════

class TokenBucket:
    """令牌桶限流器

    算法：
    - 桶容量为 burst
    - 每秒补充 rate 个令牌
    - 每次请求消耗 1 个令牌（或 weight 个）
    - 桶空则拒绝
    """

    def __init__(self, config: RateLimitConfig) -> None:
        self._rate = config.rate
        self._burst = config.burst if config.burst > 0 else max(int(config.rate), 1)
        self._tokens = float(self._burst)  # 初始满桶
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

        # 统计
        self._total_requests = 0
        self._total_allowed = 0
        self._total_rejected = 0
        self._window_requests: Deque[float] = deque()

    async def try_acquire(self, weight: int = 1) -> Tuple[bool, Optional[int]]:
        """尝试获取令牌

        Returns:
            (是否允许, 建议重试等待毫秒)
        """
        async with self._lock:
            self._total_requests += 1
            now = time.monotonic()

            # 补充令牌
            elapsed = now - self._last_refill
            self._tokens = min(
                self._burst,
                self._tokens + elapsed * self._rate,
            )
            self._last_refill = now

            if self._tokens >= weight:
                self._tokens -= weight
                self._total_allowed += 1
                self._record_request(now)
                return True, None
            else:
                self._total_rejected += 1
                # 计算多久后令牌足够
                deficit = weight - self._tokens
                wait_sec = deficit / self._rate
                retry_after_ms = int(wait_sec * 1000) + 1
                return False, retry_after_ms

    def get_stats(self) -> dict:
        """获取统计"""
        return {
            "total_requests": self._total_requests,
            "total_allowed": self._total_allowed,
            "total_rejected": self._total_rejected,
            "rejection_rate": (
                self._total_rejected / max(self._total_requests, 1)
            ),
            "available_tokens": round(self._tokens, 2),
            "capacity": self._burst,
        }

    def _record_request(self, timestamp: float) -> None:
        """记录请求时间（用于滑动窗口统计）"""
        self._window_requests.append(timestamp)
        # 清理窗口外的记录
        cutoff = timestamp - 60.0  # 保留最近 60 秒
        while self._window_requests and self._window_requests[0] < cutoff:
            self._window_requests.popleft()

    def current_rate(self) -> float:
        """当前窗口内的请求速率（每秒）"""
        if not self._window_requests:
            return 0.0
        now = time.monotonic()
        count = len(self._window_requests)
        return count / 60.0  # 60秒窗口


# ═══════════════════════════════════════════════════════════════
# 单桶限流器
# ═══════════════════════════════════════════════════════════════

class RateLimiter:
    """单桶限流器（带标签）

    用于单维度的限流，例如：
    - 某策略的限流
    - 某用户的 API 限流
    """

    def __init__(self, name: str, config: RateLimitConfig) -> None:
        if not name:
            raise ValueError("name 不能为空")
        self._name = name
        self._bucket = TokenBucket(config)
        self._config = config

    @property
    def name(self) -> str:
        return self._name

    async def allow(self, weight: int = 1) -> bool:
        """是否允许请求（不抛异常）"""
        allowed, _ = await self._bucket.try_acquire(weight)
        return allowed

    async def acquire(self, weight: int = 1) -> None:
        """获取令牌（超限抛异常）"""
        allowed, retry_after = await self._bucket.try_acquire(weight)
        if not allowed:
            raise RateLimitExceeded(
                f"限流触发 [{self._name}]: rate={self._config.rate}/s, "
                f"retry_after={retry_after}ms",
                layer=self._name,
                limit=self._config.rate,
                current=self._bucket.current_rate(),
                retry_after_ms=retry_after,
            )

    def get_stats(self) -> dict:
        return {
            "name": self._name,
            "rate": self._config.rate,
            "burst": self._config.burst,
            **self._bucket.get_stats(),
        }


# ═══════════════════════════════════════════════════════════════
# 分层限流器
# ═══════════════════════════════════════════════════════════════

class HierarchicalRateLimiter:
    """四层限流器

    按以下顺序检查，任一层拒绝则整体拒绝：
    1. API 层（外部请求）
    2. 策略层（单策略频率）
    3. 全局层（系统总量）
    4. 交易所层（交易所限制）

    每层内部支持多桶（按 key 区分，如不同用户、不同策略）。
    """

    def __init__(self) -> None:
        # 四层配置
        self._api_config: Optional[RateLimitConfig] = None
        self._global_config: Optional[RateLimitConfig] = None
        self._exchange_configs: Dict[str, RateLimitConfig] = {}

        # 桶实例缓存
        self._api_buckets: Dict[str, TokenBucket] = {}          # key -> bucket
        self._strategy_buckets: Dict[str, TokenBucket] = {}    # strategy_id -> bucket
        self._global_bucket: Optional[TokenBucket] = None
        self._exchange_buckets: Dict[str, TokenBucket] = {}    # exchange -> bucket

        # 全局开关
        self._enabled = True

    # ── 配置 ──

    def configure_api(self, config: RateLimitConfig) -> None:
        self._api_config = config

    def configure_global(self, config: RateLimitConfig) -> None:
        self._global_config = config
        self._global_bucket = TokenBucket(config)

    def configure_exchange(self, exchange: str, config: RateLimitConfig) -> None:
        self._exchange_configs[exchange] = config
        self._exchange_buckets[exchange] = TokenBucket(config)

    def configure_strategy(self, strategy_id: str, config: RateLimitConfig) -> None:
        self._strategy_buckets[strategy_id] = TokenBucket(config)

    def enable(self) -> None:
        self._enabled = True

    def disable(self) -> None:
        """禁用限流（仅紧急场景，需审计）"""
        self._enabled = False
        logger.warning("⚠️ 限流已禁用（紧急模式）")

    # ── 检查 ──

    async def check(
        self,
        api_key: Optional[str] = None,
        strategy_id: Optional[str] = None,
        exchange: Optional[str] = None,
        weight: int = 1,
    ) -> None:
        """执行四层限流检查

        任一層拒绝则抛 RateLimitExceeded。
        顺序：API → 策略 → 全局 → 交易所
        """
        if not self._enabled:
            return

        # 第1层：API 限流
        if self._api_config is not None and api_key is not None:
            bucket = self._api_buckets.get(api_key)
            if bucket is None:
                bucket = TokenBucket(self._api_config)
                self._api_buckets[api_key] = bucket
            allowed, retry_after = await bucket.try_acquire(weight)
            if not allowed:
                raise RateLimitExceeded(
                    f"API 层限流触发 [key={api_key}]",
                    layer=RateLimitLayer.API,
                    limit=self._api_config.rate,
                    current=bucket.current_rate(),
                    retry_after_ms=retry_after,
                )

        # 第2层：策略限流
        if strategy_id is not None and strategy_id in self._strategy_buckets:
            bucket = self._strategy_buckets[strategy_id]
            allowed, retry_after = await bucket.try_acquire(weight)
            if not allowed:
                raise RateLimitExceeded(
                    f"策略层限流触发 [strategy={strategy_id}]",
                    layer=RateLimitLayer.STRATEGY,
                    limit=self._strategy_buckets[strategy_id]._rate if hasattr(bucket, '_rate') else 0,
                    current=bucket.current_rate(),
                    retry_after_ms=retry_after,
                )

        # 第3层：全局限流
        if self._global_bucket is not None:
            allowed, retry_after = await self._global_bucket.try_acquire(weight)
            if not allowed:
                raise RateLimitExceeded(
                    f"全局限流触发",
                    layer=RateLimitLayer.GLOBAL,
                    limit=self._global_config.rate if self._global_config else 0,
                    current=self._global_bucket.current_rate(),
                    retry_after_ms=retry_after,
                )

        # 第4层：交易所限流
        if exchange is not None and exchange in self._exchange_buckets:
            bucket = self._exchange_buckets[exchange]
            allowed, retry_after = await bucket.try_acquire(weight)
            if not allowed:
                raise RateLimitExceeded(
                    f"交易所层限流触发 [exchange={exchange}]",
                    layer=RateLimitLayer.EXCHANGE,
                    limit=self._exchange_configs[exchange].rate,
                    current=bucket.current_rate(),
                    retry_after_ms=retry_after,
                )

    async def check_all_layers(
        self,
        api_key: Optional[str] = None,
        strategy_id: Optional[str] = None,
        exchange: Optional[str] = None,
        weight: int = 1,
    ) -> Dict[str, Any]:
        """检查所有层，返回每层的状态（不抛异常）

        Returns:
            {layer: {allowed: bool, rate: float, ...}}
        """
        results: Dict[str, Any] = {}

        # API
        if self._api_config and api_key:
            bucket = self._api_buckets.get(api_key)
            if bucket:
                allowed, retry = await bucket.try_acquire(weight)
                results[RateLimitLayer.API] = {
                    "allowed": allowed,
                    "retry_after_ms": retry,
                    "current_rate": bucket.current_rate(),
                    "limit": self._api_config.rate,
                }

        # 策略
        if strategy_id and strategy_id in self._strategy_buckets:
            bucket = self._strategy_buckets[strategy_id]
            allowed, retry = await bucket.try_acquire(weight)
            results[RateLimitLayer.STRATEGY] = {
                "allowed": allowed,
                "retry_after_ms": retry,
                "current_rate": bucket.current_rate(),
                "limit": bucket._rate if hasattr(bucket, '_rate') else 0,
            }

        # 全局
        if self._global_bucket:
            allowed, retry = await self._global_bucket.try_acquire(weight)
            results[RateLimitLayer.GLOBAL] = {
                "allowed": allowed,
                "retry_after_ms": retry,
                "current_rate": self._global_bucket.current_rate(),
                "limit": self._global_config.rate if self._global_config else 0,
            }

        # 交易所
        if exchange and exchange in self._exchange_buckets:
            bucket = self._exchange_buckets[exchange]
            allowed, retry = await bucket.try_acquire(weight)
            results[RateLimitLayer.EXCHANGE] = {
                "allowed": allowed,
                "retry_after_ms": retry,
                "current_rate": bucket.current_rate(),
                "limit": self._exchange_configs[exchange].rate,
            }

        return results

    # ── 便捷方法 ──

    async def acquire(
        self,
        api_key: Optional[str] = None,
        strategy_id: Optional[str] = None,
        exchange: Optional[str] = None,
        weight: int = 1,
    ) -> None:
        """便捷方法：检查并获取（超限抛异常）"""
        await self.check(
            api_key=api_key,
            strategy_id=strategy_id,
            exchange=exchange,
            weight=weight,
        )

    # ── 统计 ──

    def get_stats(self) -> Dict[str, Any]:
        """获取所有层的统计"""
        return {
            "enabled": self._enabled,
            "api_buckets": len(self._api_buckets),
            "strategy_buckets": {
                k: self._bucket_stats(v)
                for k, v in self._strategy_buckets.items()
            },
            "global": (
                self._bucket_stats(self._global_bucket)
                if self._global_bucket else None
            ),
            "exchanges": {
                k: self._bucket_stats(v)
                for k, v in self._exchange_buckets.items()
            },
        }

    def _bucket_stats(self, bucket: Optional[TokenBucket]) -> Optional[dict]:
        if bucket is None:
            return None
        return bucket.get_stats()


# ═══════════════════════════════════════════════════════════════
# 装饰器
# ═══════════════════════════════════════════════════════════════

def rate_limit(
    layer: str = RateLimitLayer.GLOBAL,
    weight: int = 1,
):
    """限流装饰器

    Usage:
        @rate_limit(layer="strategy", weight=1)
        async def place_order(order):
            ...
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            limiter = get_limiter()
            if limiter:
                await limiter.acquire(layer=layer, weight=weight)
            return await func(*args, **kwargs)
        return wrapper
    return decorator


def get_limiter() -> Optional[HierarchicalRateLimiter]:
    """从全局获取限流器（便捷函数，需先注册到 ServiceLocator）"""
    try:
        from core.bootstrap import get_service
        return get_service("rate_limiter")
    except (NotFoundError, RuntimeError):
        return None

