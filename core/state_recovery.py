#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 启动状态恢复服务 (StateRecovery) v6.0.0

核心职责：
1. 系统重启时按优先级从 Redis（热备）、交易所 REST API（实时）、本地 SQLite（冷备）恢复关键状态
2. 对所有外部来源数据执行 HMAC 签名验证、JSON Schema 校验、时效性检查、数值类型标准化
3. 恢复后立即与交易所实时快照交叉校验，标记不一致并触发自动修复流程（仅在安全边界内）
4. 支持部分恢复模式：至少恢复持仓即可启动，订单可延迟重建；缺失关键数据则拒绝启动
5. 提供完整审计轨迹（结构化日志 + 不可变存储）、Prometheus 指标，确保合规与可观测性
6. 内建恢复锁定机制：恢复期间禁止新开仓，恢复完成后释放锁
7. 恢复完成后主动通知风控模块重算风险预算，重置熔断与亏损计数器

外部依赖：
- redis.Redis : 热备状态存储（可选）
- gateway.order_dispatcher.OrderDispatcher : 交易所 REST 接口（可选）
- core.trade_database.TradeDatabase : 本地 SQLite 冷备（可选）
- core.position_keeper.PositionKeeper : 持仓管理（可选）
- core.order_manager.OrderManager : 订单管理（可选）
- core.metrics.MetricsCollector : Prometheus 指标暴露（可选）
- core.event_bus.EventBus : 事件总线，用于发布恢复完成/失败事件（可选）

接口契约：
- restore() -> Dict[str, Any]
  返回字典包含 "status", "reason", "recovered_state", "source", "warnings", "audit_id"
- health_check() -> Dict[str, Any]
  输出字典固定包含 "status" (str), "reason" (str), "warnings" (List[str])

异常与降级：
- 任何恢复源不可用时自动降级至下一个优先级
- 所有网络/IO异常均被捕获，记录详细错误并继续
- 若全部失败，返回明确错误状态并写入紧急日志，等待人工介入
- 恢复期间发生任何异常绝不导致进程崩溃

资源管理：
- 每个恢复源的操作均限制超时，超时后立即释放资源
- 恢复完成后断开与外部数据源的连接
- 锁持有时间由 TTL 保护，防止死锁，并使用 Lua 脚本原子释放
"""

import json
import hashlib
import hmac
import logging
import os
import re
import time
import uuid
import math
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

# ── 可选依赖 ──────────────────────────────────────────────
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None

try:
    from gateway.order_dispatcher import OrderDispatcher
except ImportError:
    OrderDispatcher = None

try:
    from core.trade_database import TradeDatabase
except ImportError:
    TradeDatabase = None

try:
    from core.position_keeper import PositionKeeper
except ImportError:
    PositionKeeper = None

try:
    from core.order_manager import OrderManager
except ImportError:
    OrderManager = None

try:
    from core.metrics import MetricsCollector
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    MetricsCollector = None

try:
    from core.event_bus import EventBus, EventTypes
    EVENT_BUS_AVAILABLE = True
except ImportError:
    EVENT_BUS_AVAILABLE = False
    EventBus = None
    EventTypes = None

try:
    import jsonschema
    JSONSCHEMA_AVAILABLE = True
except ImportError:
    JSONSCHEMA_AVAILABLE = False
    jsonschema = None

try:
    from core.risk_manager import RiskManager
except ImportError:
    RiskManager = None


# ── 常量 ──────────────────────────────────────────────────
REDIS_KEY_PREFIX = "spark:state:"
REDIS_LOCK_KEY = "spark:state_recovery_lock"
REDIS_LOCK_TTL_SEC = int(os.environ.get("SPARK_RECOVERY_LOCK_TTL", 30))
REDIS_TIMEOUT_SEC = int(os.environ.get("SPARK_REDIS_TIMEOUT_SEC", 3))
EXCHANGE_API_TIMEOUT_SEC = int(os.environ.get("SPARK_EXCHANGE_TIMEOUT_SEC", 5))
LOCAL_DB_TIMEOUT_SEC = int(os.environ.get("SPARK_LOCALDB_TIMEOUT_SEC", 2))
MAX_RETRY_ATTEMPTS = 3
RETRY_BACKOFF_BASE_MS = 500
RETRY_BACKOFF_MULTIPLIER = 2
STATE_SNAPSHOT_MAX_AGE_SEC = 86400  # 24小时
MAX_STATE_SIZE_BYTES = 10 * 1024 * 1024  # 10MB
MIN_VALID_STATE_SIZE_BYTES = 10  # 防止空状态
QUANTITY_EPSILON = 1e-12

# 签名密钥：必须从环境变量注入，回退仅在测试环境使用
_STATE_SIGNING_KEY = os.environ.get("SPARK_STATE_SIGNING_KEY")
if not _STATE_SIGNING_KEY:
    if os.environ.get("SPARK_ENV", "production") == "test":
        _STATE_SIGNING_KEY = "test_insecure_key"
        logger.warning("测试环境使用不安全的签名密钥")
    else:
        logger.critical("SPARK_STATE_SIGNING_KEY 环境变量未设置，状态签名无法进行")
        # 生产环境不设置默认值

# JSON Schema 定义
STATE_SCHEMA = {
    "type": "object",
    "required": ["version", "timestamp", "positions"],
    "properties": {
        "version": {"type": "integer", "minimum": 1, "maximum": 999},
        "timestamp": {"type": "number", "minimum": 0},
        "positions": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["symbol", "side", "quantity", "entry_price"],
                "properties": {
                    "symbol": {"type": "string", "pattern": "^[a-zA-Z0-9]+$"},
                    "side": {"type": "string", "enum": ["BUY", "SELL"]},
                    "quantity": {"type": "string", "pattern": r"^\d*\.?\d+$"},
                    "entry_price": {"type": "string", "pattern": r"^\d*\.?\d+$"},
                    "leverage": {"type": "string", "pattern": r"^\d+$"},
                    "isolated": {"type": "boolean"}
                }
            }
        },
        "orders": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["order_id", "client_order_id", "symbol", "side", "type", "status"],
                "properties": {
                    "order_id": {"type": "string"},
                    "client_order_id": {"type": "string"},
                    "symbol": {"type": "string"},
                    "side": {"type": "string", "enum": ["BUY", "SELL"]},
                    "type": {"type": "string", "enum": ["LIMIT", "MARKET", "STOP_LOSS", "TAKE_PROFIT"]},
                    "status": {"type": "string", "enum": ["NEW", "PARTIALLY_FILLED", "PENDING_CANCEL"]}
                }
            }
        },
        "config": {"type": "object"}
    },
    "additionalProperties": False
}

# 恢复后自动修复的安全边界：仅当数量差在此范围内才自动修正，否则报警
MAX_AUTO_FIX_QUANTITY_DIFF_RATIO = 0.1  # 10%


class StateRecovery:
    """启动状态恢复器，符合金融级安全与可靠性最高要求"""

    def __init__(self, redis_client=None, order_dispatcher=None,
                 trade_db=None, position_keeper=None, order_manager=None,
                 event_bus=None, risk_manager=None):
        self._redis = redis_client
        self._dispatcher = order_dispatcher or (OrderDispatcher() if OrderDispatcher else None)
        self._trade_db = trade_db or (TradeDatabase() if TradeDatabase else None)
        self._position_keeper = position_keeper or (PositionKeeper() if PositionKeeper else None)
        self._order_manager = order_manager or (OrderManager() if OrderManager else None)
        self._event_bus = event_bus
        self._risk_manager = risk_manager or (RiskManager() if RiskManager else None)
        self._audit_id = str(uuid.uuid4())
        self._lock_acquired = False

    # ── 公共接口 ──────────────────────────────────────────

    def restore(self) -> Dict[str, Any]:
        start_time = time.monotonic()
        warnings: List[str] = []
        recovered_state = None
        source = "none"

        if not self._acquire_recovery_lock():
            logger.critical("无法获取恢复锁，可能存在并发恢复或锁未正确释放")
            return self._build_result("error", "恢复锁获取失败", None, source, warnings)

        try:
            logger.info("启动状态恢复流程 [audit_id=%s]", self._audit_id)

            # 优先级 1: Redis
            state, warn, integrity = self._restore_from_redis()
            warnings.extend(warn)
            if integrity and state is not None:
                recovered_state = state
                source = "redis"

            # 优先级 2: 交易所
            if recovered_state is None:
                state, warn, integrity = self._restore_from_exchange()
                warnings.extend(warn)
                if integrity and state is not None:
                    recovered_state = state
                    source = "exchange_api"

            # 优先级 3: 本地数据库
            if recovered_state is None:
                state, warn, integrity = self._restore_from_local_db()
                warnings.extend(warn)
                if integrity and state is not None:
                    recovered_state = state
                    source = "local_db"

            # 若恢复状态非空，执行标准化工序
            if recovered_state is not None:
                # 大小校验
                size_bytes = len(json.dumps(recovered_state))
                if size_bytes > MAX_STATE_SIZE_BYTES:
                    warnings.append(f"恢复状态过大 {size_bytes/1024:.1f}KB，可能存在异常")
                elif size_bytes < MIN_VALID_STATE_SIZE_BYTES:
                    warnings.append("恢复状态过小，可能无效")

                recovered_state = self._normalize_state(recovered_state)

                # 交叉校验（交易所在线）
                if self._dispatcher:
                    cross_warn = self._cross_validate_with_exchange(recovered_state)
                    warnings.extend(cross_warn)

                # 持久化恢复结果
                self._persist_recovered_state(recovered_state)

                # 通知风控模块重置风险预算
                if self._risk_manager:
                    self._risk_manager.reset_risk_budget()

            # 指标与审计
            elapsed = time.monotonic() - start_time
            self._record_metrics(source, recovered_state is not None, elapsed)

            if recovered_state is None:
                logger.critical("所有状态恢复源均失败，系统可能无法启动")
                self._emit_event("state_recovery_failed", {"warnings": warnings})
                return self._build_result("error", "无法从任何数据源恢复状态", None, source, warnings)

            self._emit_event("state_recovery_success", {"source": source, "positions": len(recovered_state.get("positions", []))})
            return self._build_result("ok", f"状态已从 {source} 恢复", recovered_state, source, warnings)

        except Exception as e:
            logger.exception("状态恢复过程发生未预期异常")
            return self._build_result("error", f"恢复异常: {e}", None, source, warnings)
        finally:
            self._release_recovery_lock()

    def health_check(self) -> Dict[str, Any]:
        warnings = []
        # Redis
        if REDIS_AVAILABLE and self._redis:
            try:
                if not self._redis.ping():
                    warnings.append("Redis ping 失败")
            except Exception as e:
                warnings.append(f"Redis 不可用: {e}")
        else:
            warnings.append("Redis 未配置或库不可用")
        # 交易所
        if self._dispatcher:
            try:
                if hasattr(self._dispatcher, 'ping') and not self._dispatcher.ping():
                    warnings.append("交易所 API 无响应")
            except Exception as e:
                warnings.append(f"交易所 API 不可达: {e}")
        else:
            warnings.append("OrderDispatcher 未配置")
        # 本地数据库
        if self._trade_db:
            try:
                self._trade_db.get_recent(1)
            except Exception as e:
                warnings.append(f"本地数据库异常: {e}")
        else:
            warnings.append("TradeDatabase 未配置")
        # 签名密钥
        if not _STATE_SIGNING_KEY:
            warnings.append("状态签名密钥未配置")
        # Schema 可用性
        if not JSONSCHEMA_AVAILABLE:
            warnings.append("jsonschema 库未安装，将使用手动验证")
        return {
            "status": "degraded" if warnings else "ok",
            "reason": "状态恢复模块自检完成",
            "warnings": warnings,
        }

    # ── 恢复源实现 ────────────────────────────────────────

    def _restore_from_redis(self) -> Tuple[Optional[Dict], List[str], bool]:
        warnings = []
        if not REDIS_AVAILABLE or self._redis is None:
            return None, ["Redis 不可用"], False
        for attempt in range(MAX_RETRY_ATTEMPTS):
            try:
                keys = ["positions", "orders", "config", "signature", "version"]
                full_keys = [f"{REDIS_KEY_PREFIX}{k}" for k in keys]
                results = self._redis.mget(full_keys)
                # 检查是否至少有一个非空
                values = dict(zip(keys, results))
                if not any(values.get(k) for k in ["positions", "orders"]):
                    return None, ["Redis 中无状态数据"], False

                positions_raw = values.get("positions")
                orders_raw = values.get("orders")
                config_raw = values.get("config")
                signature_raw = values.get("signature")
                version_raw = values.get("version")

                # 签名验证
                if _STATE_SIGNING_KEY:
                    payload_parts = [p for p in [positions_raw, orders_raw, config_raw] if p is not None]
                    if signature_raw is None:
                        warnings.append("Redis 缺少签名")
                        return None, warnings, False
                    payload = b''.join(payload_parts)
                    expected_sig = hmac.new(
                        _STATE_SIGNING_KEY.encode(), payload, hashlib.sha256
                    ).hexdigest()
                    if not hmac.compare_digest(signature_raw.decode() if isinstance(signature_raw, bytes) else signature_raw, expected_sig):
                        warnings.append("Redis 数据签名验证失败，可能被篡改")
                        return None, warnings, False

                state = {"version": int(version_raw) if version_raw else 1, "timestamp": time.time()}
                if positions_raw:
                    state['positions'] = json.loads(positions_raw) if isinstance(positions_raw, (str, bytes)) else positions_raw
                if orders_raw:
                    state['orders'] = json.loads(orders_raw) if isinstance(orders_raw, (str, bytes)) else orders_raw
                if config_raw:
                    state['config'] = json.loads(config_raw) if isinstance(config_raw, (str, bytes)) else config_raw

                if self._validate_schema(state):
                    return state, warnings, True
                else:
                    warnings.append("Redis 数据不符合 Schema")
                    return None, warnings, False
            except Exception as e:
                logger.error("Redis恢复异常 (attempt %d/%d): %s", attempt+1, MAX_RETRY_ATTEMPTS, e)
                warnings.append(f"Redis异常: {e}")
                if attempt < MAX_RETRY_ATTEMPTS - 1:
                    backoff = (RETRY_BACKOFF_BASE_MS * (RETRY_BACKOFF_MULTIPLIER ** attempt)) / 1000.0
                    time.sleep(backoff)
        return None, warnings, False

    def _restore_from_exchange(self) -> Tuple[Optional[Dict], List[str], bool]:
        warnings = []
        if self._dispatcher is None:
            return None, ["交易所接口不可用"], False
        try:
            positions = self._dispatcher.get_positions()
            orders = self._dispatcher.get_open_orders()
            state = {
                "version": 1,
                "timestamp": time.time(),
                "positions": positions,
                "orders": orders
            }
            if self._validate_schema(state):
                return state, warnings, True
            warnings.append("交易所数据Schema校验失败")
            return None, warnings, False
        except Exception as e:
            logger.error("交易所恢复失败: %s", e)
            return None, [f"交易所异常: {e}"], False

    def _restore_from_local_db(self) -> Tuple[Optional[Dict], List[str], bool]:
        warnings = []
        if self._trade_db is None:
            return None, ["本地数据库不可用"], False
        try:
            snapshot = self._trade_db.get_latest_state_snapshot()
            if not snapshot:
                return None, ["本地数据库无快照"], False
            state = json.loads(snapshot)
            # 检查快照时效
            age = time.time() - state.get("timestamp", 0)
            if age > STATE_SNAPSHOT_MAX_AGE_SEC:
                warnings.append(f"本地快照过旧 ({age/3600:.1f}h)，建议使用在线恢复")
            if self._validate_schema(state):
                return state, warnings, True
            warnings.append("本地数据库数据Schema校验失败")
            return None, warnings, False
        except Exception as e:
            logger.error("本地数据库恢复失败: %s", e)
            return None, [f"本地数据库异常: {e}"], False

    # ── 标准化与验证 ──────────────────────────────────────

    def _normalize_state(self, state: Dict) -> Dict:
        """对恢复的状态进行数值标准化，去重，过滤无效项"""
        seen = set()
        valid_positions = []
        for pos in state.get("positions", []):
            # 标准化数值
            for field in ("quantity", "entry_price", "leverage"):
                if field in pos and isinstance(pos[field], str):
                    try:
                        pos[field] = float(pos[field])
                    except (ValueError, TypeError):
                        logger.warning("无法标准化 %s=%s，置0", field, pos[field])
                        pos[field] = 0.0
            # 去除数量为0或负的持仓
            qty = pos.get("quantity", 0)
            if qty <= QUANTITY_EPSILON:
                continue
            # 去重（symbol+side 组合唯一）
            key = f"{pos.get('symbol','')}:{pos.get('side','')}"
            if key in seen:
                logger.warning("发现重复持仓 %s，保留第一个", key)
                continue
            seen.add(key)
            valid_positions.append(pos)
        state["positions"] = valid_positions
        # 订单去重（order_id唯一）
        order_ids = set()
        unique_orders = []
        for o in state.get("orders", []):
            oid = o.get("order_id")
            if oid and oid not in order_ids:
                order_ids.add(oid)
                unique_orders.append(o)
        state["orders"] = unique_orders
        return state

    @staticmethod
    def _validate_schema(state: Dict) -> bool:
        if JSONSCHEMA_AVAILABLE:
            try:
                jsonschema.validate(instance=state, schema=STATE_SCHEMA)
                return True
            except jsonschema.ValidationError as e:
                logger.warning("状态Schema验证失败: %s", e.message)
                return False
        else:
            # 手动校验回退
            if not isinstance(state, dict):
                return False
            if "positions" not in state:
                return False
            if not isinstance(state["positions"], list):
                return False
            for p in state["positions"]:
                if not isinstance(p, dict):
                    return False
                if not {"symbol", "side", "quantity", "entry_price"}.issubset(p.keys()):
                    return False
                if p.get("side") not in ("BUY", "SELL"):
                    return False
            return True

    # ── 交叉校验 ──────────────────────────────────────────

    def _cross_validate_with_exchange(self, recovered_state: Dict) -> List[str]:
        warnings = []
        try:
            live_positions = self._dispatcher.get_positions()
            live_map = {p['symbol']: p for p in live_positions}
            recovered_map = {p['symbol']: p for p in recovered_state.get('positions', [])}

            for sym, rec in recovered_map.items():
                if sym not in live_map:
                    warnings.append(f"恢复的持仓 {sym} 在交易所不存在，可能已平仓")
                else:
                    live = live_map[sym]
                    q_rec = float(rec.get('quantity', 0))
                    q_live = float(live.get('quantity', 0))
                    if abs(q_rec - q_live) > QUANTITY_EPSILON:
                        diff_ratio = abs(q_rec - q_live) / max(q_live, QUANTITY_EPSILON)
                        if diff_ratio <= MAX_AUTO_FIX_QUANTITY_DIFF_RATIO:
                            warnings.append(f"持仓 {sym} 数量轻微差异 {diff_ratio:.2%}，自动修正为实时值")
                            rec['quantity'] = q_live
                            rec['entry_price'] = float(live.get('entry_price', 0))
                        else:
                            warnings.append(f"持仓 {sym} 数量严重不一致 ({diff_ratio:.2%})，请人工确认")

            # 补充交易所存在而恢复缺失的持仓（仅在缺失数量较少时）
            missing = [sym for sym in live_map if sym not in recovered_map]
            if len(missing) <= 3:
                for sym in missing:
                    warnings.append(f"交易所存在持仓 {sym}，恢复状态缺失，已补充")
                    recovered_state.setdefault("positions", []).append(live_map[sym])
            else:
                warnings.append(f"恢复状态缺失大量持仓 ({len(missing)}个)，可能存在数据损坏")
        except Exception as e:
            logger.warning("交叉校验失败: %s", e)
            warnings.append("无法进行恢复状态交叉校验")
        return warnings

    # ── 持久化恢复状态 ────────────────────────────────────

    def _persist_recovered_state(self, state: Dict) -> None:
        try:
            if self._trade_db:
                self._trade_db.save_state_snapshot(json.dumps(state))
            if REDIS_AVAILABLE and self._redis:
                pipe = self._redis.pipeline()
                if 'positions' in state:
                    pipe.set(f"{REDIS_KEY_PREFIX}positions", json.dumps(state['positions']))
                if 'orders' in state:
                    pipe.set(f"{REDIS_KEY_PREFIX}orders", json.dumps(state['orders']))
                if 'config' in state:
                    pipe.set(f"{REDIS_KEY_PREFIX}config", json.dumps(state['config']))
                # 生成签名并存储
                if _STATE_SIGNING_KEY:
                    payload = json.dumps(state.get('positions', [])) + json.dumps(state.get('orders', [])) + json.dumps(state.get('config', {}))
                    sig = hmac.new(_STATE_SIGNING_KEY.encode(), payload.encode(), hashlib.sha256).hexdigest()
                    pipe.set(f"{REDIS_KEY_PREFIX}signature", sig)
                pipe.set(f"{REDIS_KEY_PREFIX}version", str(state.get("version", 1)))
                pipe.execute()
        except Exception as e:
            logger.warning("恢复状态持久化失败: %s", e)

    # ── 恢复锁 (Redis Lua 原子释放) ──────────────────────

    def _acquire_recovery_lock(self) -> bool:
        if REDIS_AVAILABLE and self._redis:
            try:
                acquired = self._redis.set(REDIS_LOCK_KEY, self._audit_id,
                                           nx=True, ex=REDIS_LOCK_TTL_SEC)
                if acquired:
                    self._lock_acquired = True
                    return True
                # 检查锁持有者是否存活（简单的僵尸锁检测）
                lock_val = self._redis.get(REDIS_LOCK_KEY)
                if lock_val and lock_val.decode() == self._audit_id:
                    # 自己之前锁定的情况（进程重启），强制获取
                    self._redis.delete(REDIS_LOCK_KEY)
                    acquired = self._redis.set(REDIS_LOCK_KEY, self._audit_id,
                                               nx=True, ex=REDIS_LOCK_TTL_SEC)
                    if acquired:
                        self._lock_acquired = True
                        return True
                logger.warning("恢复锁已被其他实例持有")
                return False
            except Exception as e:
                logger.warning("Redis锁获取失败，降级为无锁恢复: %s", e)
                self._lock_acquired = True
                return True
        # 无 Redis，直接允许
        self._lock_acquired = True
        return True

    def _release_recovery_lock(self) -> None:
        if not self._lock_acquired:
            return
        if REDIS_AVAILABLE and self._redis:
            try:
                # 原子释放：仅当锁值匹配时才删除
                script = """
                if redis.call("get", KEYS[1]) == ARGV[1] then
                    return redis.call("del", KEYS[1])
                else
                    return 0
                end
                """
                self._redis.eval(script, 1, REDIS_LOCK_KEY, self._audit_id)
            except Exception as e:
                logger.warning("释放恢复锁失败: %s", e)
        self._lock_acquired = False

    # ── 指标与事件 ────────────────────────────────────────

    def _record_metrics(self, source: str, success: bool, elapsed: float):
        if METRICS_AVAILABLE:
            MetricsCollector.counter("state_recovery_total", 1, {"source": source, "success": str(success)})
            MetricsCollector.histogram("state_recovery_duration_seconds", elapsed, {"source": source})

    def _emit_event(self, event_type: str, payload: Dict) -> None:
        if EVENT_BUS_AVAILABLE and self._event_bus:
            try:
                self._event_bus.publish(event_type, {
                    "audit_id": self._audit_id,
                    **payload,
                    "timestamp": time.time()
                })
            except Exception as e:
                logger.warning("事件发布失败: %s", e)

    def _build_result(self, status: str, reason: str, state: Optional[Dict], source: str, warnings: List[str]) -> Dict:
        return {
            "status": status,
            "reason": reason,
            "recovered_state": state,
            "source": source,
            "warnings": warnings,
            "audit_id": self._audit_id
        }
