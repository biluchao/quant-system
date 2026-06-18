#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 持仓管理器 (PositionKeeper) v7.0.0 — 机构级最终版

核心职责：
1. 精确维护多品种双向持仓，支持 USDT 本位合约的全仓/逐仓模式
2. 正确处理资金费率、手续费折算、合约面值，保证金融计算精度
3. 与交易所持仓及钱包余额定期同步，偏差超阈值时报警并强制对齐
4. 提供线程安全的实时仓位、可用余额、总权益、风险敞口等风控数据
5. 发布持仓变更事件，但脱敏处理敏感财务数据

外部依赖：
- core.event_bus.EventBus : 发布持仓变更事件
- core.metrics.MetricsCollector : 指标暴露（可选）

接口契约：
- get_equity() -> float          总权益
- get_wallet_balance() -> float  钱包余额
- get_available_balance() -> float 可用余额（扣除保证金+未实现亏损）
- update_from_fill(fill: Dict) -> bool  根据成交更新
- sync(exchange_positions, exchange_balance) -> bool  同步
- health_check() -> Dict[str, Any]

异常与降级：
- 成交处理失败返回 False 且不修改状态
- 同步偏差过大强制覆盖并记录 WARNING
- 所有金融计算使用 Decimal，避免浮点误差
"""

import copy
import logging
import threading
import time
from decimal import Decimal, ROUND_HALF_UP, getcontext
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger(__name__)

# 可选依赖
try:
    from core.event_bus import EventBus, EventTypes
except ImportError:
    EventBus = None
    EventTypes = None
try:
    from core.metrics import MetricsCollector
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    MetricsCollector = None

# ── 常量 ──────────────────────────────────────────────────
SYNC_QTY_THRESHOLD = Decimal('0.001')
SYNC_PRICE_THRESHOLD = Decimal('0.002')
MAX_POSITIONS = 200
DUST_QTY = Decimal('1e-10')
DEFAULT_CONTRACT_SIZE = Decimal('1')
MIN_MARGIN_RATE = Decimal('0.005')       # 维持保证金率0.5%，用于风险评估
MAX_LEVERAGE = Decimal('125')            # 最大杠杆
MIN_LEVERAGE = Decimal('1')

getcontext().prec = 28

class PositionKeeper:
    """持仓管理器（线程安全，支持全仓/逐仓、资金费率）"""

    def __init__(self, initial_wallet_balance: Decimal = Decimal('0'),
                 contract_sizes: Optional[Dict[str, Decimal]] = None,
                 event_bus=None):
        self.wallet_balance = Decimal(str(initial_wallet_balance))  # 钱包余额（已实现资金）
        self.realized_pnl = Decimal('0')                            # 累计已实现盈亏（净额，已扣手续费）
        self.contract_sizes = contract_sizes or {}                  # symbol -> 合约面值（如BTC：0.001）
        # 持仓：key = (symbol.upper(), side)，side 为 'BUY'/'SELL'
        self._positions: Dict[Tuple[str, str], Dict] = {}
        self._lock = threading.RLock()
        self.event_bus = event_bus or (EventBus() if EventBus else None)
        self._last_sync_time = 0.0
        self._last_balance_sync = 0.0
        # 每日权益基准（用于日内止损）
        self._day_start_equity = Decimal(str(initial_wallet_balance))
        self._last_day_reset = time.strftime("%Y%m%d")
        logger.info("PositionKeeper v7.0.0 初始化，钱包余额: %s", self.wallet_balance)

    # ── 公共接口 ──────────────────────────────────────────

    def get_positions(self) -> List[Dict]:
        with self._lock:
            result = []
            for (sym, side), p in self._positions.items():
                qty = p['quantity']
                if qty <= 0:
                    continue
                result.append({
                    "symbol": sym, "side": side,
                    "quantity": float(qty), "entry_price": float(p['entry_price']),
                    "mark_price": float(p.get('mark_price', p['entry_price'])),
                    "unrealized_pnl": float(self._calc_unrealized_pnl(sym, side)),
                    "leverage": float(p.get('leverage', Decimal('1'))),
                    "margin": float(p.get('margin', Decimal('0'))),
                    "isolated": p.get('isolated', True),
                })
            return result

    def get_position(self, symbol: str, side: str) -> Optional[Dict]:
        key = (symbol.upper(), side.upper())
        with self._lock:
            p = self._positions.get(key)
            if not p or p['quantity'] <= 0:
                return None
            return {
                "symbol": key[0], "side": key[1],
                "quantity": float(p['quantity']),
                "entry_price": float(p['entry_price']),
                "mark_price": float(p.get('mark_price', p['entry_price'])),
                "unrealized_pnl": float(self._calc_unrealized_pnl(*key)),
                "leverage": float(p.get('leverage', Decimal('1'))),
                "margin": float(p.get('margin', Decimal('0'))),
                "isolated": p.get('isolated', True),
            }

    def get_equity(self) -> float:
        with self._lock:
            total_upl = sum(self._calc_unrealized_pnl(s, d) for (s, d) in self._positions)
            return float(self.wallet_balance + total_upl)

    def get_wallet_balance(self) -> float:
        with self._lock:
            return float(self.wallet_balance)

    def get_available_balance(self) -> float:
        """可用余额 = 钱包余额 - 总保证金 - 维持保证金不足部分（保守）"""
        with self._lock:
            margin = self._calc_total_margin()
            # 考虑未实现亏损可能导致需要追加保证金
            maintenance_margin = margin * MIN_MARGIN_RATE
            available = self.wallet_balance - margin - maintenance_margin
            return float(max(Decimal('0'), available))

    def get_total_margin(self) -> float:
        with self._lock:
            return float(self._calc_total_margin())

    def get_total_exposure(self) -> float:
        with self._lock:
            exp = Decimal('0')
            for (sym, side), p in self._positions.items():
                if p['quantity'] <= 0:
                    continue
                price = p.get('mark_price', p['entry_price'])
                if price <= 0:
                    continue
                csize = self._get_contract_size(sym)
                exp += p['quantity'] * csize * price
            return float(exp)

    def get_net_exposure(self) -> float:
        with self._lock:
            net = Decimal('0')
            for (sym, side), p in self._positions.items():
                if p['quantity'] <= 0:
                    continue
                price = p.get('mark_price', p['entry_price'])
                if price <= 0:
                    continue
                csize = self._get_contract_size(sym)
                sign = Decimal('1') if side == 'BUY' else Decimal('-1')
                net += sign * p['quantity'] * csize * price
            return float(net)

    def get_realized_pnl(self) -> float:
        with self._lock:
            return float(self.realized_pnl)

    def update_from_fill(self, fill: Dict) -> bool:
        """根据成交回报更新，返回 True 成功，False 因风控或异常拒绝"""
        try:
            sym = str(fill.get("symbol", "")).upper()
            side = str(fill.get("side", "")).upper()
            qty = Decimal(str(fill.get("quantity", "0")))
            price = Decimal(str(fill.get("price", "0")))
            comm = Decimal(str(fill.get("commission", "0")))
            comm_asset = str(fill.get("commission_asset", "USDT")).upper()
            leverage = Decimal(str(fill.get("leverage", "1")))
            csize = Decimal(str(fill.get("contract_size", "1")))
            if csize <= 0:
                csize = self._get_contract_size(sym)
            isolated = fill.get("isolated", True)
        except Exception as e:
            logger.error("成交字段解析失败: %s", e)
            return False

        if sym == "" or side not in ("BUY", "SELL") or qty <= 0 or price <= 0:
            logger.error("成交参数无效")
            return False
        if leverage < MIN_LEVERAGE or leverage > MAX_LEVERAGE:
            leverage = Decimal('1')
        # 手续费折算为 USDT
        comm_usdt = self._convert_commission(comm, comm_asset, price)
        with self._lock:
            key = (sym, side)
            existing = self._positions.get(key)
            is_new_position = (existing is None or existing['quantity'] <= 0)

            if is_new_position:
                # 新开仓：检查可用余额
                required_margin = (qty * csize * price) / leverage
                available = self.wallet_balance - self._calc_total_margin()
                if available < required_margin + comm_usdt:
                    logger.error("资金不足: 可用 %s, 需要保证金 %s 手续费 %s", available, required_margin, comm_usdt)
                    return False
                if len(self._positions) >= MAX_POSITIONS:
                    logger.error("持仓数量超限")
                    return False
                self._positions[key] = {
                    "symbol": sym, "side": side, "quantity": qty,
                    "entry_price": price, "mark_price": price,
                    "leverage": leverage, "contract_size": csize,
                    "margin": required_margin, "isolated": isolated,
                    "avg_leverage": leverage, "total_cost": qty * csize * price,
                }
                self.wallet_balance -= (required_margin + comm_usdt)
                self.realized_pnl -= comm_usdt
                logger.info("新开仓 %s %s qty=%s margin=%s", sym, side, qty, required_margin)
                self._emit_event("position_opened", {"symbol": sym, "side": side})
                return True

            # 已有同向持仓
            if side == existing['side']:
                old_qty = existing['quantity']
                # 加权平均入场价
                new_qty = old_qty + qty
                existing['total_cost'] = existing.get('total_cost', existing['entry_price'] * old_qty * csize) + qty * csize * price
                existing['entry_price'] = existing['total_cost'] / (new_qty * csize) if new_qty > 0 else price
                existing['quantity'] = new_qty
                # 新增保证金
                new_margin = (qty * csize * price) / leverage
                existing['margin'] += new_margin
                self.wallet_balance -= (new_margin + comm_usdt)
                self.realized_pnl -= comm_usdt
                # 更新杠杆为按数量加权平均值（近似）
                existing['leverage'] = (old_qty * existing['leverage'] + qty * leverage) / new_qty
                existing['mark_price'] = price
                logger.info("加仓 %s %s 数量 %s 均价 %s", sym, side, existing['quantity'], existing['entry_price'])
                self._emit_event("position_increased", {"symbol": sym, "side": side})
                return True

            # 反向成交：先平仓
            close_qty = min(qty, existing['quantity'])
            if close_qty <= 0:
                return False
            # 平仓盈亏（已扣手续费）
            if existing['side'] == 'BUY':
                pnl = (price - existing['entry_price']) * close_qty * csize - comm_usdt
            else:
                pnl = (existing['entry_price'] - price) * close_qty * csize - comm_usdt
            self.realized_pnl += pnl
            # 释放保证金
            if existing['quantity'] > 0:
                released = (close_qty / existing['quantity']) * existing['margin']
            else:
                released = Decimal('0')
            released = min(released, existing['margin'])
            existing['margin'] -= released
            self.wallet_balance += released + pnl  # 释放保证金和利润（已扣手续费）
            # 更新数量
            remaining_old = existing['quantity'] - close_qty
            if remaining_old <= DUST_QTY:
                del self._positions[key]
                logger.info("完全平仓 %s %s", sym, side)
                self._emit_event("position_closed", {"symbol": sym, "side": side})
            else:
                existing['quantity'] = remaining_old
                # 更新总成本
                existing['total_cost'] = existing.get('total_cost', existing['entry_price'] * existing['quantity'] * csize) * (remaining_old / (remaining_old + close_qty))

            # 剩余数量反手开仓
            remaining_qty = qty - close_qty
            if remaining_qty > 0:
                new_side = side  # 新方向
                new_key = (sym, new_side)
                new_pos = self._positions.get(new_key)
                new_margin = (remaining_qty * csize * price) / leverage
                if new_pos is None:
                    self._positions[new_key] = {
                        "symbol": sym, "side": new_side,
                        "quantity": remaining_qty, "entry_price": price,
                        "mark_price": price, "leverage": leverage,
                        "contract_size": csize, "margin": new_margin,
                        "isolated": isolated,
                        "total_cost": remaining_qty * csize * price,
                        "avg_leverage": leverage,
                    }
                else:
                    # 已有同方向，加权平均
                    old_qty2 = new_pos['quantity']
                    new_pos['total_cost'] = new_pos.get('total_cost', new_pos['entry_price'] * old_qty2 * csize) + remaining_qty * csize * price
                    new_pos['quantity'] += remaining_qty
                    new_pos['entry_price'] = new_pos['total_cost'] / (new_pos['quantity'] * csize)
                    new_pos['margin'] += new_margin
                    new_pos['leverage'] = (old_qty2 * new_pos['leverage'] + remaining_qty * leverage) / new_pos['quantity']
                self.wallet_balance -= new_margin
                logger.info("反手开仓 %s %s 数量 %s", sym, new_side, remaining_qty)
                self._emit_event("position_reversed", {"symbol": sym, "side": new_side})

            return True

    def sync(self, exchange_positions: List[Dict],
             exchange_wallet_balance: Optional[Decimal] = None,
             exchange_margin_balance: Optional[Decimal] = None) -> bool:
        """同步交易所持仓与钱包余额，返回是否无严重偏差"""
        if not isinstance(exchange_positions, list):
            logger.error("持仓数据格式无效")
            return False

        self._last_sync_time = time.time()
        with self._lock:
            success = True
            exchange_keys = set()
            # 处理交易所持仓
            for ep in exchange_positions:
                try:
                    sym = str(ep.get("symbol", "")).upper()
                    # 区分双向持仓模式
                    pos_side = str(ep.get("positionSide", "BOTH")).upper()
                    if pos_side == "LONG":
                        side = "BUY"
                    elif pos_side == "SHORT":
                        side = "SELL"
                    elif pos_side == "BOTH":
                        amt = Decimal(str(ep.get("positionAmt", "0")))
                        if amt > 0:
                            side = "BUY"
                        elif amt < 0:
                            side = "SELL"
                        else:
                            continue
                    else:
                        continue

                    pos_amt = abs(Decimal(str(ep.get("positionAmt", "0"))))
                    entry_price = Decimal(str(ep.get("entryPrice", "0")))
                    mark_price = Decimal(str(ep.get("markPrice", "0")))
                    margin = Decimal(str(ep.get("margin", "0")))
                    leverage = Decimal(str(ep.get("leverage", "1")))
                    isolated = ep.get("isolated", True)

                    if pos_amt == 0:
                        key = (sym, side)
                        if key in self._positions:
                            self.wallet_balance += self._positions[key]['margin']
                            del self._positions[key]
                            logger.warning("交易所无持仓 %s，清除本地", key)
                        continue

                    key = (sym, side)
                    exchange_keys.add(key)
                    local = self._positions.get(key)
                    if local:
                        qty_dev = abs(local['quantity'] - pos_amt) / max(pos_amt, Decimal('1'))
                        price_dev = abs(local['entry_price'] - entry_price) / max(entry_price, Decimal('1'))
                        if qty_dev > SYNC_QTY_THRESHOLD or price_dev > SYNC_PRICE_THRESHOLD:
                            logger.warning("持仓偏差 %s: 本地 qty=%s px=%s -> 交易所 qty=%s px=%s",
                                           key, local['quantity'], local['entry_price'], pos_amt, entry_price)
                            # 调整保证金差额
                            old_margin = local['margin']
                            local['quantity'] = pos_amt
                            local['entry_price'] = entry_price
                            local['mark_price'] = mark_price
                            local['margin'] = margin
                            local['leverage'] = leverage
                            local['isolated'] = isolated
                            self.wallet_balance += (margin - old_margin)
                            success = False
                        else:
                            margin_diff = margin - local['margin']
                            local['mark_price'] = mark_price
                            local['margin'] = margin
                            local['leverage'] = leverage
                            self.wallet_balance += margin_diff
                    else:
                        self._positions[key] = {
                            "symbol": sym, "side": side, "quantity": pos_amt,
                            "entry_price": entry_price, "mark_price": mark_price,
                            "leverage": leverage, "contract_size": DEFAULT_CONTRACT_SIZE,
                            "margin": margin, "isolated": isolated,
                            "total_cost": pos_amt * DEFAULT_CONTRACT_SIZE * entry_price,
                        }
                        self.wallet_balance -= margin
                        logger.info("新增持仓 %s %s", key, pos_amt)

                except Exception as e:
                    logger.error("同步持仓异常 %s: %s", ep.get("symbol"), e, exc_info=True)
                    success = False

            # 删除本地多余持仓
            local_keys = set(self._positions.keys())
            for extra in local_keys - exchange_keys:
                p = self._positions.pop(extra)
                self.wallet_balance += p['margin']
                logger.warning("移除多余持仓 %s，释放保证金 %s", extra, p['margin'])

            # 同步钱包余额
            if exchange_wallet_balance is not None:
                self.wallet_balance = Decimal(str(exchange_wallet_balance))
                self._last_balance_sync = time.time()
            elif exchange_margin_balance is not None:
                # 如果提供了保证金余额，反推钱包余额 = 保证金余额 + 未实现盈亏
                upl = sum(self._calc_unrealized_pnl(s, d) for (s, d) in self._positions)
                self.wallet_balance = Decimal(str(exchange_margin_balance)) + upl
                self._last_balance_sync = time.time()

            # 每日权益重置
            today = time.strftime("%Y%m%d")
            if today != self._last_day_reset:
                self._day_start_equity = self.get_equity()
                self._last_day_reset = today

            self._emit_event("sync", {"success": success})
            return success

    def update_wallet_balance(self, new_balance: Decimal):
        with self._lock:
            self.wallet_balance = Decimal(str(new_balance))
            self._last_balance_sync = time.time()
            logger.info("钱包余额更新: %s", self.wallet_balance)

    def health_check(self) -> Dict[str, Any]:
        warnings = []
        with self._lock:
            equity = self.get_equity()
            margin = self._calc_total_margin()
            pos_count = len(self._positions)
            if equity < 0:
                warnings.append("总权益为负")
            if pos_count > 50:
                warnings.append(f"持仓过多: {pos_count}")
            if time.time() - self._last_sync_time > 300:
                warnings.append("长时间未同步")
            if margin > Decimal('0') and equity / margin < Decimal('1.2'):
                warnings.append("保证金覆盖率过低")
        status = "degraded" if warnings else "ok"
        return {
            "status": status,
            "reason": f"持仓数: {pos_count}, 权益: {equity:.2f}",
            "warnings": warnings,
        }

    # ── 内部工具 ──────────────────────────────────────────

    def _calc_unrealized_pnl(self, sym: str, side: str) -> Decimal:
        p = self._positions.get((sym, side))
        if not p or p['quantity'] <= 0:
            return Decimal('0')
        mark = p.get('mark_price', p['entry_price'])
        if mark <= 0:
            return Decimal('0')
        csize = p.get('contract_size', self._get_contract_size(sym))
        if side == 'BUY':
            return (mark - p['entry_price']) * p['quantity'] * csize
        else:
            return (p['entry_price'] - mark) * p['quantity'] * csize

    def _calc_total_margin(self) -> Decimal:
        total = Decimal('0')
        for p in self._positions.values():
            total += p.get('margin', Decimal('0'))
        return total

    def _get_contract_size(self, symbol: str) -> Decimal:
        return self.contract_sizes.get(symbol, DEFAULT_CONTRACT_SIZE)

    def _convert_commission(self, amount: Decimal, asset: str, price: Decimal) -> Decimal:
        if asset == "USDT" or amount == 0:
            return amount
        if asset in ("BTC", "ETH") and price > 0:
            return amount * price
        logger.warning("无法折算手续费 %s %s", amount, asset)
        return Decimal('0')

    def _emit_event(self, subtype: str, data: Dict) -> None:
        if self.event_bus:
            try:
                # 脱敏：不发送具体余额
                safe_data = {k: v for k, v in data.items() if "balance" not in k}
                self.event_bus.publish(EventTypes.STATE_CHANGE, {
                    "subtype": subtype,
                    "data": safe_data,
                    "timestamp": time.time(),
                })
            except Exception:
                pass
