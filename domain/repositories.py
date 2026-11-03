# -*- coding: utf-8 -*-
"""
domain/repositories.py — 仓储接口

铁律声明：本文件严格执行八项铁律
仓储接口定义在领域层，实现在基础设施层。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from .entities import Order, Position, Account, OrderStatus
from .value_objects import Symbol, Money, Timestamp


class RepositoryError(Exception):
    """仓储异常"""
    pass


class Repository(ABC):
    """仓储基类"""

    @abstractmethod
    async def save(self, entity: Any) -> None:
        pass

    @abstractmethod
    async def get_by_id(self, entity_id: str) -> Optional[Any]:
        pass

    @abstractmethod
    async def delete(self, entity_id: str) -> bool:
        pass

    @abstractmethod
    async def list_all(self, limit: int = 100, offset: int = 0) -> List[Any]:
        pass


class OrderRepository(Repository):
    """订单仓储"""

    @abstractmethod
    async def save(self, order: Order) -> None:
        pass

    @abstractmethod
    async def get_by_id(self, order_id: str) -> Optional[Order]:
        pass

    @abstractmethod
    async def get_by_symbol(self, symbol: Symbol, status: Optional[OrderStatus] = None) -> List[Order]:
        pass

    @abstractmethod
    async def get_by_account(self, account_id: str, status: Optional[OrderStatus] = None) -> List[Order]:
        pass

    @abstractmethod
    async def get_by_strategy(self, strategy_id: str, status: Optional[OrderStatus] = None) -> List[Order]:
        pass

    @abstractmethod
    async def get_active_orders(self, symbol: Optional[Symbol] = None) -> List[Order]:
        pass

    @abstractmethod
    async def delete(self, order_id: str) -> bool:
        pass

    @abstractmethod
    async def list_all(self, limit: int = 100, offset: int = 0) -> List[Order]:
        pass


class PositionRepository(Repository):
    """持仓仓储"""

    @abstractmethod
    async def save(self, position: Position) -> None:
        pass

    @abstractmethod
    async def get_by_id(self, position_id: str) -> Optional[Position]:
        pass

    @abstractmethod
    async def get_by_symbol(self, symbol: Symbol) -> Optional[Position]:
        pass

    @abstractmethod
    async def get_by_account(self, account_id: str) -> List[Position]:
        pass

    @abstractmethod
    async def get_open_positions(self, account_id: Optional[str] = None) -> List[Position]:
        pass

    @abstractmethod
    async def delete(self, position_id: str) -> bool:
        pass

    @abstractmethod
    async def list_all(self, limit: int = 100, offset: int = 0) -> List[Position]:
        pass


class AccountRepository(Repository):
    """账户仓储"""

    @abstractmethod
    async def save(self, account: Account) -> None:
        pass

    @abstractmethod
    async def get_by_id(self, account_id: str) -> Optional[Account]:
        pass

    @abstractmethod
    async def get_by_name(self, name: str) -> Optional[Account]:
        pass

    @abstractmethod
    async def list_active(self) -> List[Account]:
        pass

    @abstractmethod
    async def delete(self, account_id: str) -> bool:
        pass

    @abstractmethod
    async def list_all(self, limit: int = 100, offset: int = 0) -> List[Account]:
        pass


# ─── 内存实现（用于测试和回测） ──────────────────────────────

class InMemoryOrderRepository(OrderRepository):
    """内存订单仓储"""

    def __init__(self):
        self._orders: Dict[str, Order] = {}
        self._by_symbol: Dict[str, List[str]] = {}
        self._by_account: Dict[str, List[str]] = {}
        self._by_strategy: Dict[str, List[str]] = {}

    async def save(self, order: Order) -> None:
        self._orders[order.order_id] = order
        key = str(order.symbol)
        if key not in self._by_symbol:
            self._by_symbol[key] = []
        if order.order_id not in self._by_symbol[key]:
            self._by_symbol[key].append(order.order_id)
        if order.account_id:
            if order.account_id not in self._by_account:
                self._by_account[order.account_id] = []
            if order.order_id not in self._by_account[order.account_id]:
                self._by_account[order.account_id].append(order.order_id)
        if order.strategy_id:
            if order.strategy_id not in self._by_strategy:
                self._by_strategy[order.strategy_id] = []
            if order.order_id not in self._by_strategy[order.strategy_id]:
                self._by_strategy[order.strategy_id].append(order.order_id)

    async def get_by_id(self, order_id: str) -> Optional[Order]:
        return self._orders.get(order_id)

    async def get_by_symbol(self, symbol: Symbol, status: Optional[OrderStatus] = None) -> List[Order]:
        key = str(symbol)
        ids = self._by_symbol.get(key, [])
        orders = [self._orders[oid] for oid in ids if oid in self._orders]
        if status:
            orders = [o for o in orders if o.status == status]
        return orders

    async def get_by_account(self, account_id: str, status: Optional[OrderStatus] = None) -> List[Order]:
        ids = self._by_account.get(account_id, [])
        orders = [self._orders[oid] for oid in ids if oid in self._orders]
        if status:
            orders = [o for o in orders if o.status == status]
        return orders

    async def get_by_strategy(self, strategy_id: str, status: Optional[OrderStatus] = None) -> List[Order]:
        ids = self._by_strategy.get(strategy_id, [])
        orders = [self._orders[oid] for oid in ids if oid in self._orders]
        if status:
            orders = [o for o in orders if o.status == status]
        return orders

    async def get_active_orders(self, symbol: Optional[Symbol] = None) -> List[Order]:
        if symbol:
            orders = await self.get_by_symbol(symbol)
        else:
            orders = list(self._orders.values())
        return [o for o in orders if o.is_active]

    async def delete(self, order_id: str) -> bool:
        if order_id not in self._orders:
            return False
        order = self._orders[order_id]
        del self._orders[order_id]
        key = str(order.symbol)
        if key in self._by_symbol and order_id in self._by_symbol[key]:
            self._by_symbol[key].remove(order_id)
        return True

    async def list_all(self, limit: int = 100, offset: int = 0) -> List[Order]:
        all_orders = list(self._orders.values())
        return all_orders[offset:offset + limit]


class InMemoryPositionRepository(PositionRepository):
    """内存持仓仓储"""

    def __init__(self):
        self._positions: Dict[str, Position] = {}
        self._by_symbol: Dict[str, str] = {}
        self._by_account: Dict[str, List[str]] = {}

    async def save(self, position: Position) -> None:
        self._positions[position.position_id] = position
        self._by_symbol[str(position.symbol)] = position.position_id
        if position.account_id:
            if position.account_id not in self._by_account:
                self._by_account[position.account_id] = []
            if position.position_id not in self._by_account[position.account_id]:
                self._by_account[position.account_id].append(position.position_id)

    async def get_by_id(self, position_id: str) -> Optional[Position]:
        return self._positions.get(position_id)

    async def get_by_symbol(self, symbol: Symbol) -> Optional[Position]:
        key = str(symbol)
        pid = self._by_symbol.get(key)
        if pid:
            return self._positions.get(pid)
        return None

    async def get_by_account(self, account_id: str) -> List[Position]:
        ids = self._by_account.get(account_id, [])
        return [self._positions[pid] for pid in ids if pid in self._positions]

    async def get_open_positions(self, account_id: Optional[str] = None) -> List[Position]:
        if account_id:
            positions = await self.get_by_account(account_id)
        else:
            positions = list(self._positions.values())
        from .entities import PositionSide
        return [p for p in positions if p.side != PositionSide.FLAT]

    async def delete(self, position_id: str) -> bool:
        if position_id not in self._positions:
            return False
        pos = self._positions[position_id]
        del self._positions[position_id]
        key = str(pos.symbol)
        if key in self._by_symbol and self._by_symbol[key] == position_id:
            del self._by_symbol[key]
        return True

    async def list_all(self, limit: int = 100, offset: int = 0) -> List[Position]:
        all_pos = list(self._positions.values())
        return all_pos[offset:offset + limit]


class InMemoryAccountRepository(AccountRepository):
    """内存账户仓储"""

    def __init__(self):
        self._accounts: Dict[str, Account] = {}
        self._by_name: Dict[str, str] = {}

    async def save(self, account: Account) -> None:
        self._accounts[account.account_id] = account
        self._by_name[account.name] = account.account_id

    async def get_by_id(self, account_id: str) -> Optional[Account]:
        return self._accounts.get(account_id)

    async def get_by_name(self, name: str) -> Optional[Account]:
        aid = self._by_name.get(name)
        if aid:
            return self._accounts.get(aid)
        return None

    async def list_active(self) -> List[Account]:
        return [a for a in self._accounts.values() if a.status.value == "active"]

    async def delete(self, account_id: str) -> bool:
        if account_id not in self._accounts:
            return False
        account = self._accounts[account_id]
        del self._accounts[account_id]
        if account.name in self._by_name:
            del self._by_name[account.name]
        return True

    async def list_all(self, limit: int = 100, offset: int = 0) -> List[Account]:
        all_accounts = list(self._accounts.values())
        return all_accounts[offset:offset + limit]
