# -*- coding: utf-8 -*-
"""
domain/value_objects.py — 值对象

铁律声明：本文件严格执行八项铁律
值对象不可变，通过属性值判断相等性，无唯一标识。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from decimal import Decimal, getcontext
from enum import Enum
from typing import Any, Optional, Tuple

# 高精度计算
getcontext().prec = 28


class Exchange(str, Enum):
    """交易所"""
    BINANCE = "binance"
    OKX = "okx"
    DERIBIT = "deribit"
    BYBIT = "bybit"
    CUSTOM = "custom"


class Currency(str, Enum):
    """货币"""
    USDT = "USDT"
    USDC = "USDC"
    BTC = "BTC"
    ETH = "ETH"
    BNB = "BNB"
    SOL = "SOL"


class TradeDirection(str, Enum):
    """交易方向"""
    LONG = "long"
    SHORT = "short"
    FLAT = "flat"


@dataclass(frozen=True)
class Money:
    """
    金额值对象

    不可变，精度固定为 8 位小数。
    所有货币运算返回新实例。
    """

    value: Decimal = Decimal("0")
    currency: Currency = Currency.USDT

    def __post_init__(self):
        if not isinstance(self.value, Decimal):
            object.__setattr__(self, 'value', Decimal(str(self.value)))
        # 精度截断
        object.__setattr__(self, 'value', self.value.quantize(Decimal("0.00000000")))

    def __add__(self, other: "Money") -> "Money":
        if self.currency != other.currency:
            raise ValueError(f"Currency mismatch: {self.currency} != {other.currency}")
        return Money(self.value + other.value, self.currency)

    def __sub__(self, other: "Money") -> "Money":
        if self.currency != other.currency:
            raise ValueError(f"Currency mismatch: {self.currency} != {other.currency}")
        return Money(self.value - other.value, self.currency)

    def __mul__(self, scalar: Decimal | int | float) -> "Money":
        if not isinstance(scalar, (Decimal, int, float)):
            return NotImplemented
        return Money(self.value * Decimal(str(scalar)), self.currency)

    def __truediv__(self, scalar: Decimal | int | float) -> "Money":
        if not isinstance(scalar, (Decimal, int, float)):
            return NotImplemented
        if Decimal(str(scalar)) == 0:
            raise ZeroDivisionError("Cannot divide Money by zero")
        return Money(self.value / Decimal(str(scalar)), self.currency)

    def __neg__(self) -> "Money":
        return Money(-self.value, self.currency)

    def __lt__(self, other: "Money") -> bool:
        if self.currency != other.currency:
            raise ValueError(f"Currency mismatch: {self.currency} != {other.currency}")
        return self.value < other.value

    def __le__(self, other: "Money") -> bool:
        if self.currency != other.currency:
            raise ValueError(f"Currency mismatch: {self.currency} != {other.currency}")
        return self.value <= other.value

    def __gt__(self, other: "Money") -> bool:
        if self.currency != other.currency:
            raise ValueError(f"Currency mismatch: {self.currency} != {other.currency}")
        return self.value > other.value

    def __ge__(self, other: "Money") -> bool:
        if self.currency != other.currency:
            raise ValueError(f"Currency mismatch: {self.currency} != {other.currency}")
        return self.value >= other.value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Money):
            return NotImplemented
        return self.value == other.value and self.currency == other.currency

    def is_zero(self) -> bool:
        return self.value == 0

    def is_positive(self) -> bool:
        return self.value > 0

    def is_negative(self) -> bool:
        return self.value < 0

    def abs(self) -> "Money":
        return Money(abs(self.value), self.currency)

    def to_dict(self) -> dict:
        return {"value": str(self.value), "currency": self.currency.value}


@dataclass(frozen=True)
class Price:
    """
    价格值对象
    """

    value: Decimal
    currency: Currency = Currency.USDT

    def __post_init__(self):
        if not isinstance(self.value, Decimal):
            object.__setattr__(self, 'value', Decimal(str(self.value)))
        object.__setattr__(self, 'value', self.value.quantize(Decimal("0.00000000")))
        if self.value < 0:
            raise ValueError("Price cannot be negative")

    def __add__(self, other: "Price") -> "Price":
        if self.currency != other.currency:
            raise ValueError(f"Currency mismatch: {self.currency} != {other.currency}")
        return Price(self.value + other.value, self.currency)

    def __sub__(self, other: "Price") -> "Price":
        if self.currency != other.currency:
            raise ValueError(f"Currency mismatch: {self.currency} != {other.currency}")
        return Price(self.value - other.value, self.currency)

    def __mul__(self, scalar: Decimal | int | float) -> "Price":
        return Price(self.value * Decimal(str(scalar)), self.currency)

    def __truediv__(self, scalar: Decimal | int | float) -> "Price":
        if Decimal(str(scalar)) == 0:
            raise ZeroDivisionError("Cannot divide Price by zero")
        return Price(self.value / Decimal(str(scalar)), self.currency)

    def __lt__(self, other: "Price") -> bool:
        return self.value < other.value

    def __le__(self, other: "Price") -> bool:
        return self.value <= other.value

    def __gt__(self, other: "Price") -> bool:
        return self.value > other.value

    def __ge__(self, other: "Price") -> bool:
        return self.value >= other.value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Price):
            return NotImplemented
        return self.value == other.value and self.currency == other.currency

    def pct_change(self, other: "Price") -> Decimal:
        """计算价格变化百分比"""
        if self.value == 0:
            raise ZeroDivisionError("Cannot calculate pct_change from zero price")
        return ((other.value - self.value) / self.value * 100).quantize(Decimal("0.0001"))


@dataclass(frozen=True)
class Quantity:
    """
    数量值对象
    """

    value: Decimal

    def __post_init__(self):
        if not isinstance(self.value, Decimal):
            object.__setattr__(self, 'value', Decimal(str(self.value)))
        object.__setattr__(self, 'value', self.value.quantize(Decimal("0.00000000")))

    def __add__(self, other: "Quantity") -> "Quantity":
        return Quantity(self.value + other.value)

    def __sub__(self, other: "Quantity") -> "Quantity":
        return Quantity(self.value - other.value)

    def __mul__(self, scalar: Decimal | int | float) -> "Quantity":
        return Quantity(self.value * Decimal(str(scalar)))

    def __truediv__(self, scalar: Decimal | int | float) -> "Quantity":
        if Decimal(str(scalar)) == 0:
            raise ZeroDivisionError("Cannot divide Quantity by zero")
        return Quantity(self.value / Decimal(str(scalar)))

    def __lt__(self, other: "Quantity") -> bool:
        return self.value < other.value

    def __le__(self, other: "Quantity") -> bool:
        return self.value <= other.value

    def __gt__(self, other: "Quantity") -> bool:
        return self.value > other.value

    def __ge__(self, other: "Quantity") -> bool:
        return self.value >= other.value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Quantity):
            return NotImplemented
        return self.value == other.value

    def __neg__(self) -> "Quantity":
        return Quantity(-self.value)

    def abs(self) -> "Quantity":
        return Quantity(abs(self.value))

    def is_zero(self) -> bool:
        return self.value == 0


@dataclass(frozen=True)
class Symbol:
    """
    交易对值对象

    格式：BASE/QUOTE[:EXCHANGE]
    例：BTC/USDT:BINANCE
    """

    base: str
    quote: str
    exchange: Exchange = Exchange.BINANCE

    def __post_init__(self):
        object.__setattr__(self, 'base', self.base.upper())
        object.__setattr__(self, 'quote', self.quote.upper())

    def __str__(self) -> str:
        return f"{self.base}/{self.quote}:{self.exchange.value}"

    @classmethod
    def parse(cls, s: str) -> "Symbol":
        """解析字符串为 Symbol"""
        parts = s.split(":")
        pair = parts[0]
        exchange = Exchange(parts[1]) if len(parts) > 1 else Exchange.BINANCE

        if "/" not in pair:
            raise ValueError(f"Invalid symbol format: {s}, expected BASE/QUOTE[:EXCHANGE]")

        base, quote = pair.split("/", 1)
        return cls(base=base, quote=quote, exchange=exchange)

    def to_dict(self) -> dict:
        return {
            "base": self.base,
            "quote": self.quote,
            "exchange": self.exchange.value,
        }

    def __hash__(self) -> int:
        return hash((self.base, self.quote, self.exchange.value))


@dataclass(frozen=True)
class Instrument:
    """
    交易工具值对象（扩展 Symbol，包含合约规格）
    """

    symbol: Symbol
    instrument_type: str = "spot"  # spot / future / option / swap
    contract_size: Quantity = field(default_factory=lambda: Quantity(Decimal("1")))
    tick_size: Price = field(default_factory=lambda: Price(Decimal("0.01")))
    min_order_size: Quantity = field(default_factory=lambda: Quantity(Decimal("0.001")))
    max_order_size: Quantity = field(default_factory=lambda: Quantity(Decimal("1000000")))
    maker_fee: Decimal = Decimal("0.001")
    taker_fee: Decimal = Decimal("0.001")
    margin_requirement: Decimal = Decimal("0.1")
    is_active: bool = True

    def __post_init__(self):
        # 精度
        object.__setattr__(self, 'maker_fee', self.maker_fee.quantize(Decimal("0.000001")))
        object.__setattr__(self, 'taker_fee', self.taker_fee.quantize(Decimal("0.000001")))
        object.__setattr__(self, 'margin_requirement', self.margin_requirement.quantize(Decimal("0.0001")))

    def validate_order_size(self, qty: Quantity) -> bool:
        return self.min_order_size.value <= qty.value <= self.max_order_size.value

    def validate_order_price(self, price: Price) -> bool:
        if self.tick_size.value <= 0:
            return True
        remainder = price.value % self.tick_size.value
        return remainder == 0 or remainder == price.value


@dataclass(frozen=True)
class Timestamp:
    """
    时间戳值对象
    """

    value: int  # milliseconds since epoch

    @classmethod
    def now(cls) -> "Timestamp":
        from datetime import datetime, timezone
        return cls(int(datetime.now(timezone.utc).timestamp() * 1000))

    def __lt__(self, other: "Timestamp") -> bool:
        return self.value < other.value

    def __le__(self, other: "Timestamp") -> bool:
        return self.value <= other.value

    def __gt__(self, other: "Timestamp") -> bool:
        return self.value > other.value

    def __ge__(self, other: "Timestamp") -> bool:
        return self.value >= other.value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Timestamp):
            return NotImplemented
        return self.value == other.value

    def to_iso8601(self) -> str:
        from datetime import datetime, timezone
        return datetime.fromtimestamp(self.value / 1000, tz=timezone.utc).isoformat()

    def elapsed_ms(self) -> int:
        return Timestamp.now().value - self.value
