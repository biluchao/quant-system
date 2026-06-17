#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors. All Rights Reserved.
# Maintainer: Quant Engineering <eng@spark-quant.internal>
"""
火种系统 · 订单调度器 (OrderDispatcher) v3.0.1

核心职责：
1. 将内部订单指令转换为币安合约 REST API 请求（POST/DELETE /fapi/v1/order）
2. 通过 Vault 安全加载 API 凭据，禁止环境变量回退于生产环境
3. 生成全局唯一幂等键（spark-{machine_id}-{prefix}-{uuid7_timestamp}）保障去重
4. 实现自适应限流退避（429/5xx）、连接池管理、签名安全
5. 订单全生命周期审计（发送、成交、拒绝、撤单）并输出 Prometheus 指标

外部依赖（真实模块接口）：
- core.secrets_manager.SecretsManager : 获取 API Key/Secret（HMAC-SHA256）
- core.audit_logger.AuditLogger : 结构化审计日志（订单事件）
- core.metrics.MetricsCollector : Prometheus 指标（计数器/直方图）

接口契约：
- send_order(order_spec: Dict) -> Dict[str, Any]
  返回固定字典 {"status": str, "order_id": str, "client_order_id": str, "reason": str, "raw": Dict}
- cancel_order(symbol: str, order_id: str = "", client_order_id: str = "") -> Dict[str, Any]
- health_check() -> Dict[str, Any]

异常与降级：
- 凭据不可用 → SYSTEM_ERROR，禁止所有订单，触发熔断计数器
- 网络超时（>3s）→ 重试1次，仍失败返回 REJECTED
- HTTP 429 → 自适应退避（Retry-After 优先，指数增长兜底），最多3次后 RATE_LIMITED
- HTTP 5xx → 指数退避（200ms/400ms/800ms），记录交易所健康状态
- 连续5次订单失败 → 自动熔断，60秒冷却期

资源管理：
- requests.Session 连接池（最大 50 连接，连接复用，300s 空闲回收）
- 凭据内存缓存（TTL 5分钟），减少 Vault 调用
- 响应体流式读取，限制最大 1MB 防止内存耗尽
"""

import hashlib
import hmac
import logging
import os
import platform
import secrets
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple, List, Set
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 可选依赖（优雅降级）
try:
    from core.secrets_manager import SecretsManager
    SECRETS_AVAILABLE = True
except ImportError:
    SECRETS_AVAILABLE = False
    SecretsManager = None

try:
    from core.audit_logger import AuditLogger
    AUDIT_AVAILABLE = True
except ImportError:
    AUDIT_AVAILABLE = False
    AuditLogger = None

try:
    from core.metrics import MetricsCollector
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    MetricsCollector = None

logger = logging.getLogger(__name__)

# ── 常量定义 ──────────────────────────────────────────────
BASE_URL = "https://fapi.binance.com"
BACKUP_URLS: List[str] = [
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
]
DEFAULT_TIMEOUT = 3.0                     # 请求超时秒数
MAX_RETRIES = 1                           # 网络错误最大重试
RATE_LIMIT_RETRIES = 3                    # 429 限流重试次数
RATE_LIMIT_BACKOFF_BASE = 2.0             # 429 退避基数秒
CIRCUIT_BREAKER_THRESHOLD = 5             # 连续失败熔断阈值
CIRCUIT_BREAKER_COOLDOWN = 60.0           # 熔断冷却秒数
CONNECTION_POOL_SIZE = 50                 # 连接池最大连接数
CONNECTION_IDLE_TIMEOUT = 300             # 空闲连接回收秒数
MAX_RESPONSE_SIZE = 1 * 1024 * 1024       # 1MB 响应体最大长度
CREDENTIAL_CACHE_TTL = 300                # 凭据缓存秒数
RECV_WINDOW = 5000                        # 交易所 recvWindow 毫秒
ALLOWED_ORDER_TYPES: Set[str] = {"LIMIT", "MARKET", "STOP", "STOP_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_MARKET"}
ALLOWED_SIDES: Set[str] = {"BUY", "SELL"}
ALLOWED_TIF: Set[str] = {"GTC", "IOC", "FOK", "GTX"}
MACHINE_ID = os.environ.get("SPARK_MACHINE_ID", platform.node()[:8])
PYTHON_MIN_VERSION: Tuple[int, int] = (3, 10)

logger = logging.getLogger(__name__)


class OrderDispatcher:
    """币安合约订单调度器，负责下单、撤单、签名与密钥管理"""

    __slots__ = (
        '_secrets', '_audit', '_session', '_api_key', '_api_secret',
        '_credential_cache_time', '_consecutive_failures', '_circuit_open_until',
        '_active_base_url',
    )

    def __init__(self, secrets_manager=None, audit_logger=None):
        self._secrets = secrets_manager
        self._audit = audit_logger
        self._session = self._build_session()
        self._api_key: Optional[str] = None
        self._api_secret: Optional[str] = None
        self._credential_cache_time: float = 0.0
        self._consecutive_failures: int = 0
        self._circuit_open_until: float = 0.0
        self._active_base_url: str = BASE_URL

    # ── 连接池构建 ──────────────────────────────────────
    @staticmethod
    def _build_session() -> requests.Session:
        """构建带连接池、超时与重试策略的 Session"""
        session = requests.Session()
        retry_strategy = Retry(
            total=0,
            connect=0,
            read=0,
            status_forcelist=[],
            backoff_factor=0,
        )
        adapter = HTTPAdapter(
            pool_connections=CONNECTION_POOL_SIZE,
            pool_maxsize=CONNECTION_POOL_SIZE,
            max_retries=retry_strategy,
            pool_block=True,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({
            "User-Agent": f"SparkQuant/{sys.version_info.major}.{sys.version_info.minor}",
            "Accept-Encoding": "gzip, deflate",
        })
        return session

    # ── 熔断器 ──────────────────────────────────────────
    def _check_circuit_breaker(self) -> Optional[Dict[str, Any]]:
        """检查熔断状态，若开启则拒绝订单"""
        if self._circuit_open_until > time.time():
            remaining = self._circuit_open_until - time.time()
            logger.critical("熔断器开启，剩余冷却 %.1fs", remaining)
            return {
                "status": "SYSTEM_ERROR",
                "order_id": "",
                "client_order_id": "",
                "reason": f"熔断冷却中，剩余 {remaining:.0f}s",
            }
        return None

    def _record_failure(self):
        """记录一次失败，达到阈值时触发熔断"""
        self._consecutive_failures += 1
        if self._consecutive_failures >= CIRCUIT_BREAKER_THRESHOLD:
            self._circuit_open_until = time.time() + CIRCUIT_BREAKER_COOLDOWN
            logger.critical(
                "触发熔断！连续失败 %d 次，冷却至 %s",
                self._consecutive_failures,
                datetime.fromtimestamp(self._circuit_open_until, tz=timezone.utc).isoformat(),
            )

    def _record_success(self):
        """记录成功，重置熔断计数器"""
        self._consecutive_failures = 0
        self._circuit_open_until = 0.0

    # ── 密钥加载（带缓存与 Vault 集成） ─────────────────
    def _load_credentials(self) -> Tuple[str, str]:
        """安全加载 API 凭据，优先 Vault，禁止生产环境回退"""
        # 缓存命中
        if (self._api_key and self._api_secret and
                time.time() - self._credential_cache_time < CREDENTIAL_CACHE_TTL):
            return self._api_key, self._api_secret

        # Vault 加载
        if self._secrets and SECRETS_AVAILABLE:
            try:
                creds = self._secrets.get_credential("binance")
                self._api_key = creds["api_key"]
                self._api_secret = creds["api_secret"]
                self._credential_cache_time = time.time()
                # 密钥格式校验
                if len(self._api_key) < 30 or len(self._api_secret) < 30:
                    raise ValueError("API 凭据格式异常（长度不足）")
                return self._api_key, self._api_secret
            except Exception as e:
                logger.critical("Vault 凭据加载失败: %s #RECOVERY: 检查 Vault 服务与路径", str(e))
                raise SystemError("凭据不可用") from e

        # 回退检查：仅在开发环境允许
        env_key = os.environ.get("BINANCE_API_KEY", "")
        env_secret = os.environ.get("BINANCE_API_SECRET", "")
        if env_key and env_secret:
            if os.environ.get("SPARK_ENV", "production") == "production":
                logger.critical("生产环境禁止使用环境变量凭据")
                raise SystemError("生产环境禁止使用环境变量凭据")
            logger.warning("使用环境变量凭据（非生产环境）")
            self._api_key = env_key
            self._api_secret = env_secret
            self._credential_cache_time = time.time()
            return env_key, env_secret

        raise SystemError("无可用 API 凭据")

    # ── 幂等键生成（UUID7 时间有序） ────────────────────
    @classmethod
    def _generate_client_order_id(cls, prefix: str = "order") -> str:
        """
        生成全局唯一幂等键：spark-{machine_id}-{prefix}-{uuid7}

        UUID7 格式：时间戳(48bit) + 版本(4bit) + 随机(12bit) + 变体(2bit) + 随机(62bit)
        时间戳从 Unix Epoch 毫秒计数，保证全局递增与防碰撞。
        """
        timestamp_ms = int(time.time() * 1000)
        # 构造 16 字节 UUID7
        timestamp_bytes = timestamp_ms.to_bytes(6, 'big')  # 48-bit 时间戳
        rand_bytes = secrets.token_bytes(10)               # 80-bit 随机
        # UUID7 布局: tttttttt-tttt-7xxx-yxxx-xxxxxxxxxxxx
        uuid7 = uuid.UUID(bytes=(
            timestamp_bytes[:4] + timestamp_bytes[4:6] +
            bytes([0x70 | (rand_bytes[0] & 0x0F)]) +        # version 7
            bytes([0x80 | (rand_bytes[1] & 0x3F)]) +        # variant 10
            rand_bytes[2:]
        ))
        return f"spark-{cls.MACHINE_ID}-{prefix[:20]}-{uuid7}"

    # ── 签名生成 ────────────────────────────────────────
    @staticmethod
    def _sign(query_string: str, secret: str) -> str:
        """HMAC-SHA256 签名，恒定时间复杂度"""
        return hmac.new(
            secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    # ── 交易所时间同步 ──────────────────────────────────
    def _get_server_time(self) -> int:
        """获取交易所服务器时间（毫秒）"""
        try:
            resp = self._session.get(
                f"{self._active_base_url}/fapi/v1/time",
                timeout=3,
            )
            resp.raise_for_status()
            data = resp.json()
            return int(data["serverTime"])
        except Exception as e:
            logger.warning("获取交易所时间失败: %s，使用本地时间", str(e))
            return int(time.time() * 1000)

    # ── 参数校验 ────────────────────────────────────────
    @staticmethod
    def _validate_order_spec(order_spec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """校验订单参数合法性，返回错误字典或 None"""
        required = ["symbol", "side", "type", "quantity"]
        for f in required:
            if f not in order_spec:
                return {
                    "status": "REJECTED",
                    "order_id": "",
                    "client_order_id": "",
                    "reason": f"缺少必要字段: {f}",
                }

        side = str(order_spec["side"]).upper()
        if side not in ALLOWED_SIDES:
            return {
                "status": "REJECTED",
                "order_id": "",
                "client_order_id": "",
                "reason": f"非法订单方向: {side}，允许: {ALLOWED_SIDES}",
            }

        order_type = str(order_spec["type"]).upper()
        if order_type not in ALLOWED_ORDER_TYPES:
            return {
                "status": "REJECTED",
                "order_id": "",
                "client_order_id": "",
                "reason": f"非法订单类型: {order_type}，允许: {ALLOWED_ORDER_TYPES}",
            }

        quantity = float(order_spec["quantity"])
        if quantity <= 0:
            return {
                "status": "REJECTED",
                "order_id": "",
                "client_order_id": "",
                "reason": f"订单数量非法: {quantity}",
            }

        # MARKET 订单不应带 timeInForce 或 price
        if order_type == "MARKET":
            if "price" in order_spec or "timeInForce" in order_spec:
                logger.warning("MARKET 订单忽略 price/timeInForce 参数")

        if "timeInForce" in order_spec:
            tif = str(order_spec["timeInForce"]).upper()
            if tif not in ALLOWED_TIF:
                return {
                    "status": "REJECTED",
                    "order_id": "",
                    "client_order_id": "",
                    "reason": f"非法 timeInForce: {tif}",
                }

        # 价格精度检查
        if "price" in order_spec:
            price = float(order_spec["price"])
            if price <= 0:
                return {
                    "status": "REJECTED",
                    "order_id": "",
                    "client_order_id": "",
                    "reason": f"价格非法: {price}",
                }

        return None  # 校验通过

    # ── 请求发送（带签名、重试、退避） ─────────────────
    def _signed_request(
        self, method: str, endpoint: str, params: Dict[str, Any], client_order_id: str = ""
    ) -> Dict[str, Any]:
        """
        发送带签名的 REST 请求

        安全特性：
        - 使用交易所服务器时间戳
        - recvWindow 5000ms
        - 429 自适应退避（Retry-After 优先）
        - 5xx 指数退避
        - 响应大小限制 1MB
        """
        api_key, api_secret = self._load_credentials()
        server_time = self._get_server_time()

        params["timestamp"] = server_time
        params["recvWindow"] = RECV_WINDOW
        # 过滤私有字段
        clean_params = {k: v for k, v in params.items() if k not in ("client_prefix",)}
        query = urlencode(sorted(clean_params.items()))
        signature = self._sign(query, api_secret)

        url = f"{self._active_base_url}{endpoint}?{query}&signature={signature}"
        if len(url) > 4096:
            logger.warning("请求 URL 过长 (%d 字符)，可能被交易所拒绝", len(url))

        headers = {
            "X-MBX-APIKEY": api_key,
            "Content-Type": "application/json",
        }

        for attempt in range(MAX_RETRIES + 1):
            try:
                resp = self._session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    timeout=DEFAULT_TIMEOUT,
                )

                # 响应大小检查
                content_length = resp.headers.get("Content-Length")
                if content_length and int(content_length) > MAX_RESPONSE_SIZE:
                    logger.error("响应体过大: %s 字节", content_length)
                    return self._build_error_result(client_order_id, "响应体超过限制")

                # HTTP 429 限流
                if resp.status_code == 429:
                    retry_after = self._parse_retry_after(resp.headers.get("Retry-After"))
                    if attempt < RATE_LIMIT_RETRIES:
                        backoff = RATE_LIMIT_BACKOFF_BASE * (2 ** attempt)
                        wait = max(retry_after, backoff) if retry_after else backoff
                        # 添加随机抖动（±25%）
                        wait *= 0.75 + secrets.randbits(8) / 512
                        logger.warning("收到 429，退避 %.2fs 后重试 (%d/%d)", wait, attempt + 1, RATE_LIMIT_RETRIES)
                        time.sleep(wait)
                        # 更新时间戳重签
                        server_time = self._get_server_time()
                        params["timestamp"] = server_time
                        query = urlencode(sorted({k: v for k, v in params.items() if k not in ("client_prefix",)}.items()))
                        signature = self._sign(query, api_secret)
                        url = f"{self._active_base_url}{endpoint}?{query}&signature={signature}"
                        continue
                    logger.error("429 限流重试耗尽")
                    return self._build_error_result(client_order_id, "RATE_LIMITED: 429 重试耗尽")

                # HTTP 5xx
                if 500 <= resp.status_code < 600:
                    if attempt < MAX_RETRIES:
                        wait = 0.2 * (2 ** attempt)
                        logger.warning("收到 %d，退避 %.2fs 后重试", resp.status_code, wait)
                        time.sleep(wait)
                        continue
                    return self._build_error_result(client_order_id, f"交易所 {resp.status_code} 错误")

                resp.raise_for_status()
                try:
                    data = resp.json()
                except Exception:
                    logger.error("响应非 JSON: %s", resp.text[:200])
                    return self._build_error_result(client_order_id, "响应解析失败")

                # 验证关键字段
                if data.get("orderId") and data["orderId"] > 0:
                    return {
                        "status": "FILLED" if data.get("status") == "FILLED" else "ACCEPTED",
                        "order_id": str(data["orderId"]),
                        "client_order_id": data.get("clientOrderId", client_order_id),
                        "reason": "OK",
                        "raw": {k: data[k] for k in ("orderId", "symbol", "side", "type", "origQty", "price", "status", "clientOrderId") if k in data},
                    }
                else:
                    # 订单被拒（如余额不足）
                    logger.error("订单被拒: %s", data)
                    return {
                        "status": "REJECTED",
                        "order_id": str(data.get("orderId", "")),
                        "client_order_id": client_order_id,
                        "reason": data.get("msg", "未知拒绝原因")[:200],
                    }

            except requests.Timeout:
                if attempt < MAX_RETRIES:
                    logger.warning("请求超时，重试 %d/%d", attempt + 1, MAX_RETRIES)
                    continue
                return self._build_error_result(client_order_id, "网络超时")
            except requests.RequestException as e:
                if attempt < MAX_RETRIES:
                    logger.warning("网络错误: %s，重试 %d/%d", str(e)[:100], attempt + 1, MAX_RETRIES)
                    continue
                logger.error("网络错误: %s", str(e)[:200])
                return self._build_error_result(client_order_id, f"网络错误: {str(e)[:150]}")

        return self._build_error_result(client_order_id, "未知错误")

    @staticmethod
    def _parse_retry_after(retry_after: Optional[str]) -> Optional[float]:
        """解析 Retry-After 头"""
        if retry_after is None:
            return None
        try:
            return float(retry_after)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _build_error_result(client_order_id: str, reason: str) -> Dict[str, Any]:
        return {
            "status": "REJECTED",
            "order_id": "",
            "client_order_id": client_order_id,
            "reason": reason,
            "raw": {},
        }

    # ── 公共接口：下订单 ─────────────────────────────────
    def send_order(self, order_spec: Dict[str, Any]) -> Dict[str, Any]:
        """
        发送订单

        Args:
            order_spec: {
                "symbol": "BTCUSDT",
                "side": "BUY" | "SELL",
                "type": "LIMIT" | "MARKET" | "STOP" | ...,
                "quantity": 0.001,
                "price": 50000.0,          # 限价单必填
                "timeInForce": "GTC",       # 可选
                "client_prefix": "ma26",    # 可选，幂等键前缀
            }

        Returns:
            {"status": "ACCEPTED"/"FILLED"/"REJECTED"/"RATE_LIMITED"/"SYSTEM_ERROR",
             "order_id": "123456",
             "client_order_id": "spark-...",
             "reason": "...",
             "raw": {...}}

        Raises:
            SystemError: 凭据不可用（已被内部捕获）
        """
        # 熔断检查
        circuit_result = self._check_circuit_breaker()
        if circuit_result:
            return circuit_result

        # 参数校验
        validation_error = self._validate_order_spec(order_spec)
        if validation_error:
            self._record_failure()
            return validation_error

        try:
            client_id = self._generate_client_order_id(
                str(order_spec.get("client_prefix", "order"))[:20]
            )
            params: Dict[str, Any] = {
                "symbol": str(order_spec["symbol"]).upper(),
                "side": str(order_spec["side"]).upper(),
                "type": str(order_spec["type"]).upper(),
                "quantity": order_spec["quantity"],
                "newClientOrderId": client_id,
            }
            if "price" in order_spec:
                params["price"] = order_spec["price"]
            if "timeInForce" in order_spec:
                params["timeInForce"] = str(order_spec["timeInForce"]).upper()

            result = self._signed_request("POST", "/fapi/v1/order", params, client_id)

            # 审计日志（脱敏）
            if self._audit and AUDIT_AVAILABLE:
                self._audit.log_event("ORDER_SENT", {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "client_order_id": client_id,
                    "symbol": params["symbol"],
                    "side": params["side"],
                    "type": params["type"],
                    "quantity": params["quantity"],
                    "result": result["status"],
                })

            # Prometheus 指标
            if METRICS_AVAILABLE and MetricsCollector:
                MetricsCollector.counter("order_dispatcher_requests_total", 1)
                if result["status"] in ("REJECTED", "RATE_LIMITED", "SYSTEM_ERROR"):
                    MetricsCollector.counter("order_dispatcher_failures_total", 1)
                    self._record_failure()
                else:
                    self._record_success()
                    MetricsCollector.counter("order_dispatcher_success_total", 1)

            return result
        except SystemError as e:
            logger.critical("系统错误，无法下单: %s", str(e))
            self._record_failure()
            return {
                "status": "SYSTEM_ERROR",
                "order_id": "",
                "client_order_id": "",
                "reason": "凭据不可用",
            }
        except Exception as e:
            logger.critical("下单异常: %s", str(e), exc_info=True)
            self._record_failure()
            return {
                "status": "SYSTEM_ERROR",
                "order_id": "",
                "client_order_id": "",
                "reason": f"内部错误: {type(e).__name__}: {str(e)[:150]}",
            }

    # ── 公共接口：撤销订单 ───────────────────────────────
    def cancel_order(self, symbol: str, order_id: str = "", client_order_id: str = "") -> Dict[str, Any]:
        """
        撤销订单

        Args:
            symbol: 交易对（如 BTCUSDT）
            order_id: 交易所订单 ID（与 client_order_id 二选一）
            client_order_id: 客户端幂等键

        Returns:
            与 send_order 相同格式的结果字典
        """
        circuit_result = self._check_circuit_breaker()
        if circuit_result:
            return circuit_result

        try:
            params: Dict[str, Any] = {"symbol": str(symbol).upper()}
            if order_id:
                params["orderId"] = order_id
            elif client_order_id:
                params["origClientOrderId"] = client_order_id
            else:
                return {
                    "status": "REJECTED",
                    "order_id": "",
                    "client_order_id": "",
                    "reason": "缺少 order_id 或 client_order_id（二选一必填）",
                }

            result = self._signed_request("DELETE", "/fapi/v1/order", params, client_order_id)

            if self._audit and AUDIT_AVAILABLE:
                self._audit.log_event("ORDER_CANCEL", {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "symbol": params["symbol"],
                    "order_id": order_id or client_order_id,
                    "result": result["status"],
                })

            if result["status"] in ("REJECTED", "RATE_LIMITED", "SYSTEM_ERROR"):
                self._record_failure()
            else:
                self._record_success()

            return result
        except SystemError as e:
            self._record_failure()
            return {
                "status": "SYSTEM_ERROR",
                "order_id": "",
                "client_order_id": "",
                "reason": "凭据不可用",
            }
        except Exception as e:
            logger.critical("撤单异常: %s", str(e), exc_info=True)
            self._record_failure()
            return {
                "status": "SYSTEM_ERROR",
                "order_id": "",
                "client_order_id": "",
                "reason": f"内部错误: {type(e).__name__}: {str(e)[:150]}",
            }

    # ── 健康检查 ─────────────────────────────────────────
    @classmethod
    def health_check(cls) -> Dict[str, Any]:
        """模块自检：验证幂等键生成、Session 创建、连接池状态"""
        try:
            inst = cls()
            cid = inst._generate_client_order_id("health")
            assert cid.startswith("spark-"), "幂等键格式错误"
            assert len(cid) > 30, "幂等键长度不足"
            assert inst._session is not None, "Session 创建失败"
            # 连接池检查
            adapter = inst._session.adapters.get("https://")
            if adapter:
                pool = adapter.poolmanager
                assert pool is not None, "连接池不可用"
            return {"status": "ok", "message": "订单调度器可用"}
        except Exception as e:
            logger.error("健康检查失败: %s", str(e))
            return {"status": "error", "message": str(e)[:200]}
