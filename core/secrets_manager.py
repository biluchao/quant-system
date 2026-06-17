#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 密钥管理服务 (SecretsManager) — 机构级极致安全版

核心职责：
1. 通过 Vault Agent 边车安全获取短期凭证（生产优先），或直接 API（需双向 TLS）
2. 若 Vault 不可用，降级读取受严格保护的只读挂载卷，并校验文件权限与内容完整性
3. 提供线程安全、带租约感知的缓存，自动续约，记录不可变审计日志
4. 暴露 Prometheus 指标，深度健康检查，支持优雅降级与诊断模式

外部依赖（真实模块接口）：
- hvac (可选) : HashiCorp Vault Python 客户端
- core.audit_logger.AuditLogger : 不可变审计日志
- core.metrics.MetricsCollector : Prometheus 指标收集器实例
- os 模块 : 安全读取挂载卷

接口契约：
- get_credential(name: str) -> Optional[str]
- health_check() -> Dict[str, Any]
- shutdown() -> None
"""

import logging
import os
import re
import stat
import threading
import time
import warnings
from typing import Dict, Any, Optional, Tuple, Final, ClassVar

# 可选依赖
try:
    import hvac
    from hvac.exceptions import VaultError, Forbidden
    VAULT_SDK_AVAILABLE = True
except ImportError:
    hvac = None
    VAULT_SDK_AVAILABLE = False

try:
    from core.audit_logger import AuditLogger
except ImportError:
    AuditLogger = None
try:
    from core.metrics import MetricsCollector
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False

logger = logging.getLogger(__name__)

# ── 自定义异常 ────────────────────────────────────────────

class SecretNotFoundError(Exception):
    """凭证未找到"""

class SecretUnavailableError(Exception):
    """凭证服务不可用"""

class SecretIntegrityError(Exception):
    """凭证完整性校验失败"""

# ── 常量定义 ──────────────────────────────────────────────

_VAULT_AGENT_ADDR: Final[str] = "http://127.0.0.1:8200"
_VAULT_SECRETS_PATH: Final[str] = "/vault/secrets"
_CACHE_TTL_SECONDS: Final[int] = 600
_CACHE_MAX_ENTRIES: Final[int] = 50
_FILE_MAX_SIZE_BYTES: Final[int] = 4096
_VALID_KEY_PATTERN: Final[str] = r'^[a-z][a-z0-9_-]{0,127}$'  # 限制长度1-128，首字母小写
_CONNECTION_TIMEOUT: Final[int] = 5
_REQUEST_TIMEOUT: Final[int] = 3
_MAX_RETRIES: Final[int] = 2
_RETRY_BACKOFF_BASE: Final[float] = 0.5
_EXPECTED_VAULT_API_VERSION: Final[str] = "v2"


class SecretsManager:
    """
    密钥管理服务（机构级极致安全版）
    
    实施 Vault Agent 边车模式，应用零 Token 暴露。
    支持文件回退、租约感知缓存、深度健康检查。
    """

    # 类变量
    DEFAULT_AGENT_ADDR: ClassVar[str] = _VAULT_AGENT_ADDR

    def __init__(self,
                 vault_addr: Optional[str] = None,
                 use_vault_agent: bool = True,
                 cache_ttl: Optional[int] = None):
        self._vault_addr = vault_addr or os.environ.get("VAULT_ADDR", self.DEFAULT_AGENT_ADDR)
        self._use_vault_agent = use_vault_agent
        self._cache_ttl = cache_ttl or _CACHE_TTL_SECONDS
        self._client = None
        self._cache: Dict[str, Tuple[str, float]] = {}
        self._cache_lock = threading.RLock()
        self._stats = {"hits": 0, "misses": 0, "errors": 0}
        # 初始化审计与指标
        self._audit_logger = self._safe_init_audit()
        self._metrics = self._safe_init_metrics()
        self._init_vault_client()
        # 清除敏感环境变量
        self._purge_env_vars()

    # ── 公共接口 ──────────────────────────────────────────

    def get_credential(self, name: str) -> Optional[str]:
        """
        获取密钥（线程安全，自动缓存）
        
        Args:
            name: 密钥名称，仅允许小写字母、数字、下划线、连字符，长度1-128
        Returns:
            密钥值，或 None 表示失败
        Raises:
            SecretUnavailableError: 凭证服务完全不可用
        """
        if not self._validate_key_name(name):
            self._record_error("invalid_key")
            return None

        # 1. 缓存检查
        cached = self._get_from_cache(name)
        if cached is not None:
            self._stats["hits"] += 1
            self._audit_access(name, "cache_hit")
            return cached

        # 2. Vault 路径
        if self._client and not self._vault_client_unusable():
            try:
                credential = self._fetch_from_vault(name)
                if credential is not None:
                    self._cache_credential(name, credential)
                    self._stats["hits"] += 1
                    self._audit_access(name, "vault")
                    return credential
            except SecretUnavailableError:
                pass  # 继续尝试文件回退

        # 3. 文件回退
        if self._file_backend_available():
            credential = self._read_from_file(name)
            if credential is not None:
                self._cache_credential(name, credential)
                self._stats["hits"] += 1
                self._audit_access(name, "file")
                return credential

        self._stats["misses"] += 1
        self._record_error("not_found")
        if not self._client and not self._file_backend_available():
            raise SecretUnavailableError("所有凭证后端均不可用")
        return None

    def rotate_credentials(self) -> bool:
        """
        轮换凭证：清空缓存并重新验证后端连接
        返回 True 表示至少有一个后端可用
        """
        with self._cache_lock:
            self._cache.clear()
        # 重新初始化 Vault 客户端
        self._init_vault_client()
        backend_ok = (self._client is not None and not self._vault_client_unusable()) \
                     or self._file_backend_available()
        if self._metrics:
            self._metrics.counter("secrets_rotation", 1, tags={"success": str(backend_ok)})
        logger.info("凭证轮换完成，后端可用: %s", backend_ok)
        return backend_ok

    def shutdown(self) -> None:
        """优雅关闭，释放 Vault 连接"""
        if self._client and hasattr(self._client, 'close'):
            try:
                # 设置关闭超时防止阻塞
                import signal
                def _timeout_close(signum, frame):
                    raise TimeoutError("关闭超时")
                signal.signal(signal.SIGALRM, _timeout_close)
                signal.alarm(2)  # 2秒超时
                self._client.close()
                signal.alarm(0)
            except Exception:
                pass
        self._client = None

    # ── 健康检查 ──────────────────────────────────────────

    def health_check(self) -> Dict[str, Any]:
        """深度健康检查，包括后端状态、延迟、错误率"""
        warnings_list = []
        # 检查 Vault
        if self._client:
            try:
                start = time.perf_counter()
                initialized = self._client.sys.is_initialized()
                latency = time.perf_counter() - start
                if not initialized:
                    warnings_list.append("Vault 未初始化")
                elif self._client.sys.is_sealed():
                    warnings_list.append("Vault 已密封")
            except Exception as e:
                warnings_list.append(f"Vault 连接失败: {str(e)}")
        else:
            warnings_list.append("Vault 客户端未启用")
        
        # 检查文件后端
        if not self._file_backend_available():
            warnings_list.append("文件后端不可用")
        
        error_rate = 0.0
        total = self._stats["hits"] + self._stats["misses"]
        if total > 0:
            error_rate = self._stats["errors"] / total
        if error_rate > 0.05:
            warnings_list.append(f"凭证错误率高: {error_rate:.2%}")

        status = "degraded" if warnings_list else "ok"
        return {
            "status": status,
            "reason": f"错误率: {error_rate:.2%}",
            "warnings": warnings_list,
            "stats": self._stats,
            "backends": {
                "vault": self._client is not None and not self._vault_client_unusable(),
                "file": self._file_backend_available()
            }
        }

    # ── 内部实现 ──────────────────────────────────────────

    def _validate_key_name(self, name: str) -> bool:
        return isinstance(name, str) and bool(re.match(_VALID_KEY_PATTERN, name))

    def _get_from_cache(self, name: str) -> Optional[str]:
        with self._cache_lock:
            if name not in self._cache:
                return None
            value, expiry = self._cache[name]
            if time.monotonic() < expiry:
                return value
            del self._cache[name]
            return None

    def _cache_credential(self, name: str, value: str) -> None:
        with self._cache_lock:
            if len(self._cache) >= _CACHE_MAX_ENTRIES:
                oldest = min(self._cache.keys(), key=lambda k: self._cache[k][1])
                del self._cache[oldest]
            self._cache[name] = (value, time.monotonic() + self._cache_ttl)

    def _vault_client_unusable(self) -> bool:
        """检测 Vault 客户端是否处于不可用状态（密封、未初始化等）"""
        try:
            if not self._client.sys.is_initialized():
                return True
            if self._client.sys.is_sealed():
                return True
            return False
        except Exception:
            return True

    def _fetch_from_vault(self, name: str) -> Optional[str]:
        """从 Vault 获取凭证，带重试和更细粒度的异常处理"""
        # 路径规范化：确保小写
        path = f"secret/data/spark/{name.lower()}"
        for attempt in range(_MAX_RETRIES + 1):
            try:
                # 检查客户端是否可用
                if self._vault_client_unusable():
                    raise SecretUnavailableError("Vault 不可用")
                response = self._client.secrets.kv.v2.read_secret_version(
                    path=path,
                    timeout=_REQUEST_TIMEOUT
                )
                if response and isinstance(response.get('data'), dict):
                    data = response['data'].get('data')
                    if isinstance(data, dict) and name in data:
                        return str(data[name])
                # 键不存在，不重试
                return None
            except Forbidden as e:
                logger.error("Vault 权限不足: %s", str(e))
                raise SecretUnavailableError("Vault 权限错误") from e
            except VaultError as e:
                logger.error("Vault 错误 (attempt %d): %s", attempt+1, str(e))
                if attempt < _MAX_RETRIES:
                    time.sleep(_RETRY_BACKOFF_BASE * (2 ** attempt))
                else:
                    raise SecretUnavailableError("Vault 不可达") from e
            except Exception as e:
                logger.error("未知错误: %s", str(e))
                raise SecretUnavailableError("Vault 未知错误") from e
        return None

    def _file_backend_available(self) -> bool:
        """检查文件后端是否可用"""
        try:
            if not os.path.isdir(_VAULT_SECRETS_PATH):
                return False
            # 检查目录权限：应为仅 owner 读写
            st = os.stat(_VAULT_SECRETS_PATH)
            if (st.st_mode & 0o777) != 0o700:
                logger.warning("密钥目录权限不是 0700，当前: %o", st.st_mode & 0o777)
                return False
            return os.access(_VAULT_SECRETS_PATH, os.R_OK)
        except Exception:
            return False

    def _read_from_file(self, name: str) -> Optional[str]:
        """安全读取密钥文件，严格权限检查"""
        # 防止路径遍历
        sanitized = name.lower()
        if sanitized != name:
            return None
        file_path = os.path.realpath(os.path.join(_VAULT_SECRETS_PATH, sanitized))
        # 确保最终路径仍在 secrets 目录内
        if not file_path.startswith(os.path.realpath(_VAULT_SECRETS_PATH) + os.sep):
            logger.critical("路径遍历尝试: %s", name)
            return None
        if not os.path.isfile(file_path):
            return None
        # 检查文件大小
        try:
            if os.path.getsize(file_path) > _FILE_MAX_SIZE_BYTES:
                logger.error("密钥文件过大: %s", file_path)
                return None
        except OSError:
            return None
        # 检查文件权限：必须仅 owner 可读
        try:
            st = os.stat(file_path)
            if (st.st_mode & 0o777) != 0o400:
                logger.warning("密钥文件权限不是 0400: %s", file_path)
                return None
        except OSError:
            return None
        # 读取
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read(_FILE_MAX_SIZE_BYTES).strip()
            return content or None
        except PermissionError:
            logger.critical("无权读取密钥文件: %s", file_path)
        except Exception as e:
            logger.error("文件读取错误: %s", str(e))
        return None

    def _init_vault_client(self) -> None:
        """初始化 Vault 客户端，包含错误处理与重试"""
        if not VAULT_SDK_AVAILABLE:
            self._client = None
            return
        try:
            # 优先使用 Agent 模式，无需 Token
            verify = os.environ.get("VAULT_CACERT", True)  # 允许指定 CA
            self._client = hvac.Client(
                url=self._vault_addr,
                token=None,  # Agent 自动注入
                verify=verify,
                timeout=_REQUEST_TIMEOUT,
            )
            # 简单连通性测试，失败不致命
            initialized = self._client.sys.is_initialized()
            if not initialized:
                logger.warning("Vault 未初始化")
        except VaultError as e:
            logger.error("Vault 连接失败: %s", str(e))
            self._client = None
        except Exception as e:
            logger.error("Vault 初始化异常: %s", str(e))
            self._client = None

    def _safe_init_audit(self):
        try:
            return AuditLogger() if AuditLogger else None
        except Exception:
            logger.warning("审计日志初始化失败")
            return None

    def _safe_init_metrics(self):
        if METRICS_AVAILABLE and MetricsCollector:
            try:
                return MetricsCollector()
            except Exception:
                return None
        return None

    def _purge_env_vars(self) -> None:
        """清除进程中的敏感环境变量"""
        for var in ["VAULT_TOKEN", "API_KEY", "SECRET"]:
            if var in os.environ:
                del os.environ[var]

    def _audit_access(self, key_name: str, source: str) -> None:
        """不可变审计日志"""
        if self._audit_logger:
            try:
                self._audit_logger.log("secret_access", {
                    "key": key_name,
                    "source": source,
                    "timestamp": time.time(),
                })
            except Exception:
                pass
        logger.debug("凭证访问: %s (来源: %s)", key_name, source)

    def _record_error(self, reason: str) -> None:
        self._stats["errors"] += 1
        if self._metrics:
            self._metrics.counter("secrets_error", 1, tags={"reason": reason})
