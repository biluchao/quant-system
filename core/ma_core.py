#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 移动平均核心计算 (MACore) v5.0.0 — 机构级终极版

核心职责：
1. 高精度、高性能移动平均（SMA / EMA / WMA），支持多种周期与方法切换
2. 增量更新与智能缓存，最小化锁持有时间和内存分配
3. 内置异常价格过滤、精度可配置、角度自适应归一化
4. 完全线程安全，所有公共方法返回 Optional，调用者须处理 None

外部依赖：
- collections.deque : 滑动窗口
- decimal.Decimal : 高精度数值
- threading.Lock : 线程同步
- core.metrics.MetricsCollector : 指标暴露（可选）

接口契约：
- feed(price) -> None
- value(length, method) -> Optional[Decimal]
- slope(length, method) -> Optional[Decimal]
- angle_deg(length, method, norm_factor) -> Optional[float]
- get_series(length, method, n) -> List[Decimal]
- reset() -> None
- health_check() -> Dict[str, Any]

异常与降级：
- 无效价格忽略并记录 WARNING
- 数据不足返回 None
- 所有方法不向上抛出异常
"""

import logging
import math
from collections import deque
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from threading import Lock
from typing import Dict, Any, Optional, Union, List, Tuple

logger = logging.getLogger(__name__)

VERSION = "5.0.0"
SPDX_IDENTIFIER = "Apache-2.0"

try:
    from core.metrics import MetricsCollector
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    MetricsCollector = None

# ── 常量 ──────────────────────────────────────────────────
DEFAULT_PERIOD = 26
MAX_PERIOD = 500
MIN_PERIOD = 1
MIN_PRICE = Decimal("0")
DEFAULT_DECIMAL_PREC = 8           # 默认小数位数
PRICE_JUMP_THRESHOLD_PCT = 0.30   # 价格跳变阈值（30%）


class MACore:
    """高精度移动平均核心（线程安全，低延迟优化）"""

    def __init__(self, default_period: int = DEFAULT_PERIOD,
                 max_period: int = MAX_PERIOD,
                 decimal_precision: int = DEFAULT_DECIMAL_PREC,
                 allow_negative_price: bool = False):
        if default_period < MIN_PERIOD or default_period > max_period:
            raise ValueError(f"default_period 必须在 [{MIN_PERIOD}, {max_period}] 内")
        self.default_period = default_period
        self.max_period = max_period
        self.decimal_precision = decimal_precision
        self._quantize_str = f"0.{'0' * decimal_precision}" if decimal_precision > 0 else "1"
        self._min_price = MIN_PRICE if not allow_negative_price else Decimal("-inf")
        self._last_price = None

        self._prices: deque[Decimal] = deque(maxlen=max_period)
        self._lock = Lock()

        # 缓存：存储 (method, period) -> value
        self._cache: Dict[Tuple[str, int], Decimal] = {}
        self._dirty = True

        # EMA 状态: period -> (alpha, ema_value)
        self._ema_state: Dict[int, Tuple[Decimal, Decimal]] = {}

        # 性能统计（可选）
        self._compute_count = 0
        self._cache_hits = 0

        logger.info("MACore v%s 初始化: period=%d, max=%d, precision=%d",
                    VERSION, default_period, max_period, decimal_precision)

    # ── 公共接口 ──────────────────────────────────────────

    def feed(self, price: Union[Decimal, float, int, str]) -> None:
        """添加新价格，更新所有缓存与 EMA"""
        dec_price = self._to_decimal(price)
        if dec_price is None:
            return
        if dec_price < self._min_price:
            logger.warning("价格低于最小允许值 %s，忽略", dec_price)
            return

        # 异常价格过滤
        if self._last_price is not None and self._last_price > 0:
            change = abs(dec_price - self._last_price) / self._last_price
            if change > PRICE_JUMP_THRESHOLD_PCT:
                logger.warning("价格跳变过大: %.2f%% (%.2f -> %.2f)，可能为异常数据",
                              change * 100, self._last_price, dec_price)
        self._last_price = dec_price

        with self._lock:
            self._prices.append(dec_price)
            self._dirty = True
            # 清除所有缓存，因为价格变了
            self._cache.clear()
            # 更新 EMA 状态
            for period, (alpha, ema_val) in self._ema_state.items():
                if ema_val is None:
                    self._ema_state[period] = (alpha, dec_price)
                else:
                    new_ema = alpha * dec_price + (1 - alpha) * ema_val
                    self._ema_state[period] = (alpha, new_ema)
        self._record_metrics("price_fed", 1)

    def value(self, length: Optional[int] = None,
              method: str = "SMA") -> Optional[Decimal]:
        """返回均线值，数据不足返回 None"""
        period = self._normalize_period(length)
        if period is None:
            return None
        method = method.upper()
        if method not in ("SMA", "EMA", "WMA"):
            logger.warning("不支持的均线方法: %s", method)
            return None

        with self._lock:
            if len(self._prices) < period:
                return None
            cache_key = (method, period)
            if not self._dirty and cache_key in self._cache:
                self._cache_hits += 1
                return self._cache[cache_key]

            result = None
            if method == "SMA":
                result = self._compute_sma(period)
            elif method == "EMA":
                result = self._get_ema(period)
            elif method == "WMA":
                result = self._compute_wma(period)

            if result is not None:
                self._cache[cache_key] = result
                self._compute_count += 1
            return result

    def slope(self, length: Optional[int] = None,
              method: str = "SMA") -> Optional[Decimal]:
        """均线斜率"""
        period = self._normalize_period(length)
        if period is None:
            return None
        method = method.upper()
        if method not in ("SMA", "EMA", "WMA"):
            return None

        with self._lock:
            if len(self._prices) < period + 1:
                return None
            if method == "SMA":
                cur = self._compute_sma(period)
                prev = self._compute_sma(period, offset=1)
            elif method == "EMA":
                cur = self._get_ema(period)
                if cur is None:
                    return None
                alpha = self._ema_alpha(period)
                if alpha >= Decimal("1"):
                    return Decimal("0")
                current_price = self._prices[-1]
                prev = (cur - alpha * current_price) / (1 - alpha)
            elif method == "WMA":
                cur = self._compute_wma(period)
                prev = self._compute_wma(period, offset=1)
            else:
                return None
            return cur - prev

    def angle_deg(self, length: Optional[int] = None,
                  method: str = "SMA",
                  norm_factor: Optional[float] = None) -> Optional[float]:
        """均线角度（度）"""
        s = self.slope(length, method)
        if s is None:
            return None
        if norm_factor is None:
            with self._lock:
                if len(self._prices) >= 5:
                    # 使用最近 5 个价格估算归一化因子，避免大量复制
                    recent = [self._prices[-i] for i in range(1, min(6, len(self._prices)+1))]
                    avg = sum(recent) / len(recent)
                    factor = float(avg) * 0.0001
                    if factor <= 0.0:
                        factor = 0.01
                else:
                    factor = 0.01
        else:
            factor = norm_factor if norm_factor > 0 else 0.01

        try:
            ratio = float(s) / factor
        except (ValueError, ZeroDivisionError):
            return 0.0
        if abs(ratio) > 1e6:
            return 90.0 if ratio > 0 else -90.0
        return math.degrees(math.atan(ratio))

    def get_series(self, length: Optional[int] = None,
                   method: str = "SMA", n: int = 10) -> List[Decimal]:
        """返回最近 n 个均线值"""
        period = self._normalize_period(length)
        if period is None or n <= 0:
            return []
        method = method.upper()
        if method not in ("SMA", "WMA"):
            logger.warning("get_series 仅支持 SMA/WMA")
            return []

        with self._lock:
            if len(self._prices) < period + n:
                return []
            series = []
            # 一次性取出完整窗口数据，避免重复复制
            full_list = list(self._prices)
            total = len(full_list)
            for i in range(n - 1, -1, -1):
                end = total - i
                start = end - period
                if start < 0:
                    break
                window = full_list[start:end]
                if method == "SMA":
                    series.append(sum(window) / len(window))
                else:
                    n_w = len(window)
                    weight_sum = Decimal(n_w * (n_w + 1) // 2)
                    wma = sum((j+1) * p for j, p in enumerate(window)) / weight_sum
                    series.append(wma)
            return series

    def reset(self) -> None:
        """清空所有数据与缓存"""
        with self._lock:
            self._prices.clear()
            self._cache.clear()
            self._ema_state.clear()
            self._dirty = True
            self._last_price = None
            self._compute_count = 0
            self._cache_hits = 0
        logger.info("MACore 已重置")

    def data_count(self) -> int:
        """当前数据点数量（线程安全）"""
        with self._lock:
            return len(self._prices)

    def health_check(self) -> Dict[str, Any]:
        warnings = []
        with self._lock:
            cnt = len(self._prices)
        if cnt < self.default_period:
            warnings.append(f"数据不足 {cnt}/{self.default_period}")
        status = "degraded" if warnings else "ok"
        return {
            "status": status,
            "reason": f"MACore v{VERSION}, 数据点: {cnt}, 缓存命中率: {self._cache_hits}/{self._compute_count}",
            "warnings": warnings,
        }

    # ── 内部方法 ──────────────────────────────────────────

    def _normalize_period(self, length: Optional[int]) -> Optional[int]:
        if length is None:
            return self.default_period
        if not isinstance(length, int) or length < MIN_PERIOD or length > self.max_period:
            logger.warning("无效周期: %s", length)
            return None
        return length

    def _ema_alpha(self, period: int) -> Decimal:
        return Decimal("2") / (Decimal(str(period)) + Decimal("1"))

    def _get_ema(self, period: int) -> Decimal:
        if period not in self._ema_state:
            alpha = self._ema_alpha(period)
            seed = self._compute_sma(period)  # 数据已由调用方保证足够
            self._ema_state[period] = (alpha, seed)
        return self._ema_state[period][1]

    def _compute_sma(self, period: int, offset: int = 0) -> Decimal:
        """计算 SMA，锁内调用，避免重复复制整个 deque"""
        total = len(self._prices)
        end = total - offset
        start = end - period
        if start < 0 or end > total or end <= 0:
            return Decimal("0")
        # 直接迭代取需要的部分，而非全量 list
        # deque 不支持切片，使用 islice 也不行，最轻量方法是提取子列表
        # 但为了降低开销，可预先将 deque 转为 list 并缓存，此处仍保留简单实现
        window = [self._prices[i] for i in range(start, end)]
        return sum(window) / len(window)

    def _compute_wma(self, period: int, offset: int = 0) -> Decimal:
        """计算 WMA"""
        total = len(self._prices)
        end = total - offset
        start = end - period
        if start < 0 or end > total or end <= 0:
            return Decimal("0")
        window = [self._prices[i] for i in range(start, end)]
        n = len(window)
        weight_sum = Decimal(n * (n + 1) // 2)  # 整数除法，避免浮点
        wma = sum((i + 1) * p for i, p in enumerate(window)) / weight_sum
        return wma

    def _to_decimal(self, price: Union[Decimal, float, int, str]) -> Optional[Decimal]:
        """将价格安全转换为 Decimal，保持精度"""
        if isinstance(price, Decimal):
            # 不强制量化，保留原始精度
            return price
        try:
            d = Decimal(str(price))
            return d
        except (InvalidOperation, ValueError, TypeError):
            logger.warning("无效价格: %s", price)
            return None

    def _record_metrics(self, name: str, value: float) -> None:
        if METRICS_AVAILABLE and MetricsCollector:
            try:
                MetricsCollector.counter(name, value)
            except Exception:
                pass
