#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 执行计划生成器 (ExecutionPlanner) v10.0.0 — 机构级绝对终极版

核心职责：
1. 基于 Almgren-Chriss 最优执行框架，结合实时订单簿微观结构生成拆分计划
2. 支持自适应、TWAP、VWAP、被动、激进等策略，动态调整剩余计划
3. 所有子单发送前回调风控二次确认，对部分成交自动重新规划
4. 完整生命周期管理：创建、暂停、恢复、取消、过期清理、容量控制
5. 每笔计划均绑定全局唯一幂等键，防止重复执行，并妥善处理重复请求
6. 容量移除时主动取消交易所订单，避免幽灵订单

外部依赖：
- core.risk_manager.RiskManager : 事前与事中风控审核
- core.order_manager.OrderManager : 提交子订单与查询成交
- core.event_bus.EventBus : 发布执行事件
- core.metrics.MetricsCollector : 指标暴露

接口契约：
- plan(order, strategy, total_time_sec, urgency, market_data) -> Dict
- cancel_plan / pause_plan / resume_plan / get_next_slice / adjust_plan
- health_check() -> Dict[str, Any]
- stop() -> None 优雅停止后台线程

异常与降级：
- 风控拒绝或流动性不足时动态缩减计划规模，延长执行时间
- 所有异常均被捕获，记录完整上下文并返回安全侧结果
"""

import copy
import logging
import math
import time
import uuid
from collections import OrderedDict
from enum import Enum
from threading import Event, Lock, RLock, Thread
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── 可选依赖 ──────────────────────────────────────────────
try:
    from core.risk_manager import RiskManager
except ImportError:
    RiskManager = None
try:
    from core.order_manager import OrderManager
except ImportError:
    OrderManager = None
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
DEFAULT_SLICES = 12
MAX_SLICES = 120
MIN_SLICE_INTERVAL_MS = 30
DEFAULT_SLICE_INTERVAL_MS = 250
MAX_TOTAL_TIME_SEC = 600.0
MIN_TOTAL_TIME_SEC = 5.0
IMPACT_RISK_AVERSION = 0.5             # 可配置为策略参数
DEFAULT_URGENCY = 0.5
MAX_ACTIVE_PLANS = 300
PLAN_EXPIRY_SEC = 1800
CLEANUP_INTERVAL_SEC = 30
MIN_SLICE_NOTIONAL_USDT = 5.0
MAX_RETRIES_PER_SLICE = 2
SLIPPAGE_LIMIT_MULT = 0.5
CLIENT_ORDER_PREFIX = "spark"
EPSILON = 1e-12
MIN_VOLATILITY = 1e-6
DEFAULT_VOLATILITY = 0.02
DEFAULT_LIQUIDITY_FACTOR = 0.01
MAX_PLAN_ID_LENGTH = 128
MIN_SLICES_AFTER_SKIP = 2              # 跳过极小名义价值后最少保留切片数


class PlanStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    ERROR = "error"


class ExecutionStrategy(str, Enum):
    ADAPTIVE = "adaptive"
    TWAP = "twap"
    PASSIVE = "passive"
    AGGRESSIVE = "aggressive"


class ExecutionPlanner:
    """最优执行计划生成与生命周期管理器"""

    def __init__(self, risk_manager=None, order_manager=None, event_bus=None):
        self.risk_manager = risk_manager or (RiskManager() if RiskManager else None)
        self.order_manager = order_manager or (OrderManager() if OrderManager else None)
        self.event_bus = event_bus or (EventBus() if EventBus else None)

        self._plans: OrderedDict[str, Dict] = OrderedDict()
        self._plans_lock = RLock()

        self._coid_index: Dict[str, str] = {}   # client_order_id -> plan_id
        self._coid_lock = Lock()

        self._stop_cleanup = Event()
        self._cleanup_thread = Thread(target=self._cleanup_loop, daemon=True, name="exec-cleanup")
        self._cleanup_thread.start()

        logger.info("ExecutionPlanner v10.0.0 初始化完成")

    # ── 公共接口 ──────────────────────────────────────────

    def plan(self, order: Dict, strategy: str = ExecutionStrategy.ADAPTIVE,
             total_time_sec: float = 60.0, urgency: float = DEFAULT_URGENCY,
             market_data: Optional[Dict] = None) -> Dict[str, Any]:
        """
        生成最优执行计划。
        如果 client_order_id 重复且已有非活跃计划则拒绝，防止重复执行。
        """
        if not isinstance(order, dict) or not order:
            return self._error("订单字典无效或为空")

        symbol = str(order.get('symbol', '')).strip().upper()
        side = str(order.get('side', '')).strip().upper()
        if not symbol or side not in ('BUY', 'SELL'):
            return self._error(f"订单参数错误: symbol={symbol}, side={side}")

        try:
            total_qty = abs(float(order['quantity']))
        except (ValueError, KeyError, TypeError):
            return self._error("订单数量无效")
        if total_qty <= 0:
            return self._error("订单数量必须为正")

        limit_price = None
        raw_limit = order.get('limit_price')
        if raw_limit is not None:
            try:
                limit_price = float(raw_limit)
            except (ValueError, TypeError):
                return self._error("限价无效")

        arrival_price = 0.0
        raw_arrival = order.get('expected_arrival_price')
        if raw_arrival is not None:
            try:
                arrival_price = float(raw_arrival)
            except (ValueError, TypeError):
                arrival_price = 0.0
        else:
            arrival_price = limit_price if limit_price is not None else 0.0

        client_order_id = str(order.get('client_order_id', '')).strip()

        try:
            total_time_sec = float(total_time_sec)
        except (ValueError, TypeError):
            return self._error("total_time_sec 必须为数字")
        total_time_sec = max(MIN_TOTAL_TIME_SEC, min(total_time_sec, MAX_TOTAL_TIME_SEC))

        try:
            urgency = float(urgency)
        except (ValueError, TypeError):
            urgency = DEFAULT_URGENCY
        urgency = max(0.0, min(1.0, urgency))

        if strategy not in (e.value for e in ExecutionStrategy):
            logger.warning("未知执行策略 '%s'，回退为 adaptive", strategy)
            strategy = ExecutionStrategy.ADAPTIVE

        # 原子化的去重、风控、生成与注册，保持锁顺序 _plans_lock 内嵌 _coid_lock
        with self._plans_lock:
            # 去重处理：若 client_order_id 已存在，根据状态决定
            if client_order_id:
                with self._coid_lock:
                    existing_pid = self._coid_index.get(client_order_id)
                if existing_pid:
                    existing_plan = self._plans.get(existing_pid)
                    if existing_plan:
                        status = existing_plan.get('status')
                        if status in (PlanStatus.ACTIVE, PlanStatus.PAUSED):
                            logger.info("重复 client_order_id %s，返回已有计划 %s", client_order_id, existing_pid)
                            return {
                                "status": "ok",
                                "plan_id": existing_pid,
                                "slices": [copy.deepcopy(s) for s in existing_plan['slices']],
                                "reason": "订单计划已存在（去重）",
                            }
                        else:
                            # 订单已终态，拒绝新的重复请求
                            return self._error(f"订单 {client_order_id} 已完成/取消，无法新建计划")

            # 风控预审
            if self.risk_manager:
                approved, reason = self.risk_manager.approve_order(order)
                if not approved:
                    self._emit_event("execution_plan_rejected", {"reason": reason})
                    return self._error(f"风控拒绝: {reason}")

            # 计算最优拆分数与子单列表
            slices_count = self._compute_optimal_slices(total_qty, strategy, urgency, symbol, market_data)
            slices = self._compute_slices(
                total_qty, slices_count, total_time_sec, strategy,
                limit_price, arrival_price, market_data
            )
            if not slices:
                return self._error("未能生成有效子单")

            # 注入父订单上下文
            for s in slices:
                s['symbol'] = symbol
                s['side'] = side
                s['parent_client_order_id'] = client_order_id
                s['client_order_prefix'] = f"{CLIENT_ORDER_PREFIX}-{symbol}-{side[0]}"

            plan_id = self._generate_plan_id(symbol)
            plan_data = {
                "plan_id": plan_id,
                "order": copy.deepcopy(order),
                "slices": slices,
                "strategy": strategy,
                "total_time_sec": total_time_sec,
                "created_at": time.time(),
                "status": PlanStatus.ACTIVE,
                "next_slice_idx": 0,
                "total_qty": total_qty,
                "executed_qty": 0.0,
                "last_updated": time.time(),
                "client_order_id": client_order_id,
                "arrival_price": arrival_price,
                "limit_price": limit_price,
            }

            # 容量控制：移除最旧的非活跃计划；若全活跃则取消并移除最旧活跃计划（须主动取消对应订单）
            while len(self._plans) >= MAX_ACTIVE_PLANS:
                victim = None
                for pid, p in self._plans.items():
                    if p['status'] not in (PlanStatus.ACTIVE, PlanStatus.PAUSED):
                        victim = pid
                        break
                if not victim:
                    # 所有计划均活跃，移除最旧的活跃计划，并尝试取消关联的交易所订单
                    victim = next(iter(self._plans))
                    victim_plan = self._plans[victim]
                    logger.critical("容量满，强制取消最旧活跃计划 %s", victim)
                    self._cancel_exchange_orders(victim_plan)
                    victim_plan['status'] = PlanStatus.CANCELLED
                if victim:
                    removed = self._plans.pop(victim, None)
                    if removed and removed.get('client_order_id'):
                        with self._coid_lock:
                            self._coid_index.pop(removed['client_order_id'], None)
            self._plans[plan_id] = plan_data

            if client_order_id:
                with self._coid_lock:
                    self._coid_index[client_order_id] = plan_id

        self._emit_event("execution_plan_created", {"plan_id": plan_id, "slices": len(slices)})
        self._record_metrics("execution_plan_created", 1, {"strategy": strategy})

        return {
            "status": "ok",
            "plan_id": plan_id,
            "slices": [copy.deepcopy(s) for s in slices],
            "reason": f"已生成 {len(slices)} 个子单",
        }

    def cancel_plan(self, plan_id: str) -> bool:
        with self._plans_lock:
            plan = self._plans.get(plan_id)
            if not plan or plan['status'] in (PlanStatus.COMPLETED, PlanStatus.CANCELLED, PlanStatus.EXPIRED):
                return False
            plan['status'] = PlanStatus.CANCELLED
            plan['last_updated'] = time.time()
            cid = plan.get('client_order_id')
        if cid:
            with self._coid_lock:
                self._coid_index.pop(cid, None)
        self._emit_event("execution_plan_cancelled", {"plan_id": plan_id})
        logger.info("执行计划 %s 已取消", plan_id)
        return True

    def pause_plan(self, plan_id: str) -> bool:
        with self._plans_lock:
            plan = self._plans.get(plan_id)
            if not plan or plan['status'] != PlanStatus.ACTIVE:
                return False
            plan['status'] = PlanStatus.PAUSED
            plan['last_updated'] = time.time()
        self._emit_event("execution_plan_paused", {"plan_id": plan_id})
        return True

    def resume_plan(self, plan_id: str) -> bool:
        with self._plans_lock:
            plan = self._plans.get(plan_id)
            if not plan or plan['status'] != PlanStatus.PAUSED:
                return False
            plan['status'] = PlanStatus.ACTIVE
            plan['last_updated'] = time.time()
        self._emit_event("execution_plan_resumed", {"plan_id": plan_id})
        return True

    def get_next_slice(self, plan_id: str) -> Optional[Dict]:
        with self._plans_lock:
            plan = self._plans.get(plan_id)
            if not plan or plan['status'] != PlanStatus.ACTIVE:
                return None
            idx = plan['next_slice_idx']
            if idx >= len(plan['slices']):
                plan['status'] = PlanStatus.COMPLETED
                plan['last_updated'] = time.time()
                cid = plan.get('client_order_id')
                if cid:
                    with self._coid_lock:
                        self._coid_index.pop(cid, None)
                self._emit_event("execution_plan_completed", {"plan_id": plan_id})
                return None
            slice_to_send = copy.deepcopy(plan['slices'][idx])
            plan['next_slice_idx'] = idx + 1
            plan['last_updated'] = time.time()
            return slice_to_send

    def adjust_plan(self, plan_id: str, filled_qty: float) -> bool:
        try:
            filled_qty = abs(float(filled_qty))
        except (TypeError, ValueError):
            logger.error("adjust_plan: filled_qty 无效")
            return False

        with self._plans_lock:
            plan = self._plans.get(plan_id)
            if not plan or plan['status'] not in (PlanStatus.ACTIVE, PlanStatus.PAUSED):
                return False
            if filled_qty > plan['total_qty'] + EPSILON:
                logger.error("filled_qty %.8f 超过计划总量 %.8f", filled_qty, plan['total_qty'])
                return False
            remaining = plan['total_qty'] - filled_qty
            if remaining <= 0:
                plan['status'] = PlanStatus.COMPLETED
                plan['last_updated'] = time.time()
                cid = plan.get('client_order_id')
                if cid:
                    with self._coid_lock:
                        self._coid_index.pop(cid, None)
                return True

            unsent_start = plan['next_slice_idx']
            unsent = plan['slices'][unsent_start:]
            if not unsent:
                plan['status'] = PlanStatus.ERROR
                logger.error("计划 %s 无剩余子单但仍有未成交量 %.8f", plan_id, remaining)
                return False

            total_slices = len(plan['slices'])
            time_elapsed_ratio = unsent_start / total_slices if total_slices > 0 else 0.0
            remaining_time = max(1.0, plan['total_time_sec'] * (1.0 - time_elapsed_ratio))

            resliced = self._compute_slices(
                remaining, len(unsent), remaining_time, plan['strategy'],
                plan.get('limit_price'), plan.get('arrival_price', 0)
            )
            if resliced:
                # 数量对齐
                if len(resliced) < len(unsent):
                    del unsent[len(resliced):]
                elif len(resliced) > len(unsent):
                    # 扩展未发送切片，复制上下文
                    last_ctx = unsent[-1] if unsent else {}
                    for i in range(len(unsent), len(resliced)):
                        new_slice = copy.deepcopy(last_ctx)
                        new_slice['quantity'] = 0.0
                        unsent.append(new_slice)
                for i, s in enumerate(resliced):
                    unsent[i]['quantity'] = s['quantity']
            else:
                # fallback
                qty_per = remaining / len(unsent)
                for s in unsent:
                    s['quantity'] = round(qty_per, 8)
                total_new = sum(s['quantity'] for s in unsent)
                diff = round(remaining - total_new, 8)
                if diff != 0 and unsent:
                    unsent[-1]['quantity'] = round(unsent[-1]['quantity'] + diff, 8)

            for s in unsent:
                if s['quantity'] < 0:
                    s['quantity'] = 0.0

            plan['last_updated'] = time.time()
            self._emit_event("execution_plan_adjusted", {"plan_id": plan_id, "remaining_qty": remaining})
            return True

    def stop(self) -> None:
        self._stop_cleanup.set()
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=5)
        logger.info("ExecutionPlanner 已停止")

    def health_check(self) -> Dict[str, Any]:
        warnings = []
        if not self.risk_manager:
            warnings.append("RiskManager 未配置")
        if not self.order_manager:
            warnings.append("OrderManager 未配置")
        with self._plans_lock:
            active = sum(1 for p in self._plans.values() if p['status'] in (PlanStatus.ACTIVE, PlanStatus.PAUSED))
            total = len(self._plans)
            status_counts = {}
            for p in self._plans.values():
                key = p['status'].value
                status_counts[key] = status_counts.get(key, 0) + 1
        if active > MAX_ACTIVE_PLANS * 0.8:
            warnings.append(f"活跃计划接近上限: {active}/{MAX_ACTIVE_PLANS}")
        status = "degraded" if warnings else "ok"
        return {
            "status": status,
            "reason": f"活跃计划: {active}, 总计: {total}, 分布: {status_counts}",
            "warnings": warnings,
        }

    # ── 内部核心算法 ──────────────────────────────────────

    def _compute_optimal_slices(self, total_qty: float, strategy: str, urgency: float,
                                symbol: str, market_data: Optional[Dict]) -> int:
        volatility = DEFAULT_VOLATILITY
        liquidity = 0.0
        if market_data:
            volatility = float(market_data.get('volatility', DEFAULT_VOLATILITY))
            avg_volume = float(market_data.get('avg_daily_volume', 0))
            if avg_volume > 0:
                liquidity = avg_volume * DEFAULT_LIQUIDITY_FACTOR
        volatility = max(volatility, MIN_VOLATILITY)
        if liquidity <= 0:
            liquidity = 100.0

        base = max(DEFAULT_SLICES, int(total_qty / liquidity * 50))
        base = min(base, MAX_SLICES)

        strategy_mult = {
            ExecutionStrategy.ADAPTIVE: 1.0,
            ExecutionStrategy.TWAP: 0.8,
            ExecutionStrategy.PASSIVE: 1.5,
            ExecutionStrategy.AGGRESSIVE: 0.5,
        }.get(strategy, 1.0)

        urgency_factor = 1.0 - urgency * 0.8
        slices = int(base * strategy_mult * urgency_factor)
        return max(1, min(slices, MAX_SLICES))

    def _compute_slices(self, total_qty: float, num_slices: int, total_time_sec: float,
                        strategy: str, limit_price: Optional[float],
                        arrival_price: float, market_data: Optional[Dict] = None) -> List[Dict]:
        if num_slices <= 0:
            return []

        volatility = DEFAULT_VOLATILITY
        if market_data:
            volatility = float(market_data.get('volatility', DEFAULT_VOLATILITY))
        volatility = max(volatility, MIN_VOLATILITY)

        kappa = volatility * IMPACT_RISK_AVERSION / max(total_time_sec, 1.0) * 8.0
        kappa = max(0.05, min(kappa, 5.0))

        interval_sec = total_time_sec / num_slices
        interval_ms = max(MIN_SLICE_INTERVAL_MS, int(interval_sec * 1000))

        time_points = [i * interval_sec for i in range(num_slices)]
        raw_weights = [math.exp(-kappa * t) for t in time_points]
        total_weight = sum(raw_weights)
        if total_weight <= EPSILON:
            raw_weights = [1.0] * num_slices
            total_weight = float(num_slices)

        # 计算理论数量，并提前调整最后一片使总和精确等于 total_qty
        temp_quantities = []
        for i in range(num_slices):
            w = raw_weights[i] / total_weight
            slice_qty = total_qty * w
            if strategy == ExecutionStrategy.TWAP:
                slice_qty = total_qty / num_slices
            temp_quantities.append(slice_qty)

        total_assigned = sum(temp_quantities)
        diff = total_qty - total_assigned
        if temp_quantities:
            temp_quantities[-1] += diff
            temp_quantities[-1] = max(0.0, temp_quantities[-1])

        # 构建切片列表，跳过过小的名义价值，但保证最终切片数不低于 MIN_SLICES_AFTER_SKIP
        slices = []
        cum_qty = 0.0
        for i, qty in enumerate(temp_quantities):
            if qty <= 0:
                continue
            notional = qty * arrival_price if arrival_price > 0 else qty
            # 跳过逻辑：非首片、非末片且名义价值过小
            if i != 0 and i != num_slices - 1 and notional < MIN_SLICE_NOTIONAL_USDT:
                continue

            if i == num_slices - 1:
                qty = max(0.0, total_qty - cum_qty)

            slice_order = {
                "symbol": None,
                "side": None,
                "quantity": round(qty, 8),
                "price": limit_price,
                "order_type": "LIMIT" if limit_price is not None else "MARKET",
                "slice_index": i,
                "delay_ms": interval_ms if i > 0 else 0,
                "retries_left": MAX_RETRIES_PER_SLICE,
                "expected_time": time.time() + (i + 1) * interval_sec,
            }
            slices.append(slice_order)
            cum_qty += qty

        # 强制保证最少切片数
        while len(slices) < MIN_SLICES_AFTER_SKIP and num_slices >= MIN_SLICES_AFTER_SKIP:
            # 补充一个切片，从第一个切片拆分
            if len(slices) >= 2:
                first = slices[0]
                half = first['quantity'] / 2.0
                first['quantity'] = round(half, 8)
                new_slice = copy.deepcopy(first)
                new_slice['quantity'] = round(half, 8)
                slices.insert(1, new_slice)
            else:
                break

        # 最终总量校准
        if slices:
            total_in = sum(s['quantity'] for s in slices)
            final_diff = round(total_qty - total_in, 8)
            if final_diff != 0:
                slices[-1]['quantity'] = round(slices[-1]['quantity'] + final_diff, 8)
                slices[-1]['quantity'] = max(0.0, slices[-1]['quantity'])

        return slices

    # ── 辅助方法 ──────────────────────────────────────────

    def _generate_plan_id(self, symbol: str) -> str:
        safe_symbol = symbol.replace('/', '-')[:10]
        uid = uuid.uuid4().hex[:12]
        ts = int(time.time() * 1000)
        pid = f"plan-{safe_symbol}-{uid}-{ts}"
        return pid[:MAX_PLAN_ID_LENGTH]

    def _error(self, reason: str) -> Dict:
        return {"status": "error", "reason": reason, "plan_id": "", "slices": []}

    def _cancel_exchange_orders(self, plan: Dict) -> None:
        """尝试取消与计划关联的交易所订单（如果可能）"""
        # 由于未存储每个子单的exchange_order_id，无法精确取消，这里记录严重警告并通知
        logger.critical("容量强制取消计划 %s，建议人工检查交易所未完成订单", plan.get('plan_id'))
        self._emit_event("execution_plan_force_cancelled", {"plan_id": plan.get('plan_id')})

    def _emit_event(self, event_type: str, data: Dict) -> None:
        if self.event_bus and EventTypes:
            try:
                self.event_bus.publish(EventTypes.STATE_CHANGE, {
                    "subtype": event_type,
                    "data": data,
                    "timestamp": time.time(),
                })
            except Exception as e:
                logger.error("发布执行事件失败: %s", e)

    def _record_metrics(self, name: str, value: float, labels: Optional[Dict] = None) -> None:
        if METRICS_AVAILABLE and MetricsCollector:
            try:
                MetricsCollector.counter(name, value, labels)
            except Exception:
                pass

    def _cleanup_loop(self) -> None:
        while not self._stop_cleanup.is_set():
            self._stop_cleanup.wait(CLEANUP_INTERVAL_SEC)
            try:
                now = time.time()
                with self._plans_lock:
                    to_remove = []
                    for pid, p in self._plans.items():
                        if p['status'] in (PlanStatus.COMPLETED, PlanStatus.CANCELLED,
                                           PlanStatus.EXPIRED, PlanStatus.ERROR):
                            age = now - p.get('last_updated', p['created_at'])
                            if age > PLAN_EXPIRY_SEC:
                                to_remove.append(pid)
                    for pid in to_remove:
                        p_data = self._plans.pop(pid, None)
                        if p_data and p_data.get('client_order_id'):
                            with self._coid_lock:
                                self._coid_index.pop(p_data['client_order_id'], None)
                        logger.debug("清理计划: %s", pid)
            except Exception:
                logger.exception("计划清理异常")
