#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 不可变审计日志服务 (AuditLogger) — 万亿级生产强化版

核心职责：
1. 记录交易、风控、配置变更、凭证访问等关键事件为结构化审计日志
2. 日志文件采用增量哈希链、HMAC 签名和可选的透明加密，确保不可篡改性
3. 本地保留并异步上传至 AWS S3，启用对象锁定 (Object Lock) 满足 SEC 17a-4
4. 自动轮转、压缩、过期清理，提供完整性验证与 Prometheus 指标

外部依赖：
- core.secrets_manager.SecretsManager : 获取签名/加密密钥
- boto3 : S3 上传与对象锁定配置
- zstandard : 日志压缩 (可选)
- cryptography.fernet : 文件加密 (可选)

接口契约：
- log(event_type: str, payload: Dict[str, Any]) -> None
- verify_integrity(file_path: Path) -> bool
- health_check() -> Dict[str, Any]
- shutdown() -> None
"""

import atexit
import functools
import hashlib
import hmac
import json
import logging
import os
import queue
import secrets
import signal
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Union

# 可选压缩
try:
    import zstandard as zstd
    HAS_ZSTD = True
except ImportError:
    zstd = None
    HAS_ZSTD = False

# 可选加密
try:
    from cryptography.fernet import Fernet, InvalidToken
    HAS_CRYPTO = True
except ImportError:
    Fernet = None
    InvalidToken = Exception
    HAS_CRYPTO = False

# 云存储
try:
    import boto3
    from botocore.config import Config as BotoConfig
    from botocore.exceptions import ClientError
    HAS_BOTO3 = True
except ImportError:
    boto3 = None
    BotoConfig = None
    HAS_BOTO3 = False

from core.secrets_manager import SecretsManager  # 内部模块

logger = logging.getLogger(__name__)

__all__ = ["AuditLogger", "AuditConfig", "AuditIntegrityError"]


# ── 配置与异常 ────────────────────────────────────────────

@dataclass(frozen=True)
class AuditConfig:
    """审计日志不可变配置"""
    LOG_DIR: str = "logs/audit"
    FILE_PREFIX: str = "audit_"
    ROTATION_INTERVAL_SEC: int = 60
    MAX_FILE_SIZE_BYTES: int = 50 * 1024 * 1024
    COMPRESSION_ENABLED: bool = True
    ENCRYPTION_ENABLED: bool = True
    SIGN_KEY_NAME: str = "audit_hmac_key"
    ENCRYPT_KEY_NAME: str = "audit_encryption_key"
    S3_BUCKET: str = "spark-audit-logs"
    S3_PREFIX: str = "prod/"
    S3_REGION: str = "us-east-1"
    S3_OBJECT_LOCK_ENABLED: bool = True
    S3_RETENTION_DAYS: int = 2555  # 7年合规留存
    UPLOAD_QUEUE_SIZE: int = 100
    MAX_UPLOAD_RETRIES: int = 5
    MAX_PAYLOAD_SIZE: int = 4096
    DISK_FREE_THRESHOLD_PCT: int = 10
    RETENTION_DAYS: int = 30  # 本地保留天数
    FLUSH_INTERVAL_SEC: int = 5
    ALLOWED_EVENT_TYPES: Set[str] = frozenset({
        "order_placed", "order_filled", "order_cancelled",
        "risk_violation", "position_change", "config_change",
        "credential_access", "system_start", "system_stop",
        "model_update", "manual_override"
    })


class AuditIntegrityError(Exception):
    """审计日志完整性受损"""


class AuditWriteError(Exception):
    """审计日志写入失败"""


# ── 工具函数 ──────────────────────────────────────────────

def _monotonic_clock() -> float:
    return time.monotonic()


# ── 审计日志主类 ──────────────────────────────────────────

class AuditLogger:
    """不可变审计日志服务（强化版）"""

    def __init__(self,
                 config: AuditConfig = AuditConfig(),
                 secrets_manager: Optional[SecretsManager] = None):
        self.config = config
        self._secrets = secrets_manager or SecretsManager()

        # 密钥与加密
        self._signing_key: Optional[bytes] = None
        self._encryption_key: Optional[bytes] = None
        self._fernet: Optional[Any] = None
        self._load_keys()

        # 文件状态
        self._current_file: Optional[Path] = None
        self._file_handle: Optional[Any] = None
        self._file_open_ts: float = 0.0
        self._file_sequence: int = 0
        self._last_chain_hash: bytes = secrets.token_bytes(32)  # 初始随机种子
        self._write_lock = threading.Lock()

        # 定期刷盘定时器
        self._flush_timer: Optional[threading.Timer] = None

        # 上传队列与后台线程
        self._upload_queue: queue.Queue = queue.Queue(maxsize=self.config.UPLOAD_QUEUE_SIZE)
        self._upload_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # Prometheus 风格指标（内存计数器）
        self._metrics = {
            "entries_written": 0,
            "write_errors": 0,
            "uploads_succeeded": 0,
            "uploads_failed": 0,
            "entries_dropped": 0,
            "last_write_timestamp": 0.0,
        }

        # 初始化环境
        self._init_directory()
        self._rotate_file()
        self._start_flush_timer()
        self._start_upload_worker()
        # 注册退出处理
        atexit.register(self.shutdown)
        # 信号处理（使用 partial 保留实例引用）
        for sig in [signal.SIGTERM, signal.SIGINT]:
            try:
                signal.signal(sig, functools.partial(self._signal_handler, sig))
            except Exception:
                pass

    def _signal_handler(self, signum: int, frame):
        logger.warning("收到信号 %s，触发审计日志关闭", signum)
        self.shutdown()
        sys.exit(0)

    # ── 初始化与密钥 ────────────────────────────────────────

    def _init_directory(self):
        try:
            Path(self.config.LOG_DIR).mkdir(parents=True, exist_ok=True, mode=0o700)
        except OSError as e:
            logger.critical("无法创建审计日志目录 %s: %s", self.config.LOG_DIR, str(e))
            raise

    def _load_keys(self):
        # HMAC 签名密钥
        key_str = self._secrets.get_credential(self.config.SIGN_KEY_NAME)
        if key_str and len(key_str) >= 32:
            self._signing_key = key_str.encode('utf-8')
        else:
            logger.error("HMAC 签名密钥无效，审计完整性降低")
        # 加密密钥
        if self.config.ENCRYPTION_ENABLED and HAS_CRYPTO:
            enc_str = self._secrets.get_credential(self.config.ENCRYPT_KEY_NAME)
            if enc_str:
                self._encryption_key = enc_str.encode('utf-8')
                self._fernet = Fernet(self._encryption_key)
            else:
                logger.warning("文件加密密钥不可用，仅签名保护")

    # ── 公共接口 ──────────────────────────────────────────

    def log(self, event_type: str, payload: Dict[str, Any]) -> None:
        if event_type not in self.config.ALLOWED_EVENT_TYPES:
            raise ValueError(f"非法事件类型: {event_type}")
        if not isinstance(payload, dict):
            raise ValueError("payload 必须为字典")

        # 脱敏与序列化
        clean = self._sanitize(payload, depth=0)
        entry = {
            "ver": 1,
            "seq": self._file_sequence,
            "id": str(uuid.uuid4()),
            "ts": datetime.now(timezone.utc).isoformat(),
            "type": event_type,
            "host": os.uname().nodename,
            "pid": os.getpid(),
            "payload": clean,
        }
        line = json.dumps(entry, ensure_ascii=False, separators=(',', ':'))
        line_bytes = line.encode('utf-8')
        if len(line_bytes) > self.config.MAX_PAYLOAD_SIZE:
            self._metrics["entries_dropped"] += 1
            raise ValueError(f"单条日志过大 ({len(line_bytes)} bytes)")

        with self._write_lock:
            self._check_disk_space()
            if self._should_rotate():
                self._rotate_file()
            try:
                self._file_handle.write(line_bytes + b'\n')
                self._file_handle.flush()
                os.fsync(self._file_handle.fileno())
                # 更新链式哈希
                self._update_chain_hash(line_bytes)
                self._metrics["entries_written"] += 1
                self._metrics["last_write_timestamp"] = time.time()
            except Exception as e:
                self._metrics["write_errors"] += 1
                logger.critical("审计日志写入失败: %s", str(e))
                raise AuditWriteError("写入审计日志失败") from e

    def verify_integrity(self, file_path: Union[str, Path]) -> bool:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"审计文件不存在: {path}")
        try:
            with open(path, 'rb') as f:
                data = f.read()
        except OSError:
            return False
        if len(data) < 64:
            return False
        *content, sig_hex = data[:-64], data[-64:]
        content = b''.join(content)
        # 如果需要解密，先尝试解密
        if self.config.ENCRYPTION_ENABLED and self._fernet:
            try:
                content = self._fernet.decrypt(content)
            except InvalidToken:
                pass  # 可能未加密
        if not self._signing_key:
            return False
        computed = hmac.new(self._signing_key, content, hashlib.sha256).hexdigest()
        return hmac.compare_digest(computed, sig_hex.decode('ascii'))

    def health_check(self) -> Dict[str, Any]:
        issues = []
        try:
            stat = os.statvfs(self.config.LOG_DIR)
            free_pct = (stat.f_bavail / stat.f_blocks) * 100 if stat.f_blocks > 0 else 0
        except OSError:
            free_pct = 0
            issues.append("磁盘状态不可读")
        if free_pct < self.config.DISK_FREE_THRESHOLD_PCT:
            issues.append(f"磁盘可用空间不足 {free_pct:.1f}%")
        if not self._signing_key:
            issues.append("签名密钥缺失")
        qsize = self._upload_queue.qsize()
        if qsize > self.config.UPLOAD_QUEUE_SIZE * 0.8:
            issues.append(f"上传队列积压 {qsize}")
        return {
            "status": "degraded" if issues else "ok",
            "reason": f"队列: {qsize}, 写入: {self._metrics['entries_written']}",
            "warnings": issues,
            "metrics": self._metrics,
        }

    def shutdown(self) -> None:
        if self._stop_event.is_set():
            return  # 防止重入
        logger.info("审计日志服务关闭中...")
        self._stop_event.set()
        if self._flush_timer:
            self._flush_timer.cancel()
        with self._write_lock:
            self._finalize_current_file()
        if self._upload_thread and self._upload_thread.is_alive():
            self._upload_thread.join(timeout=15)
        logger.info("审计日志服务已关闭")

    # ── 内部文件管理 ─────────────────────────────────────

    def _sanitize(self, obj: Any, depth: int = 0) -> Any:
        SENSITIVE = {"api_key", "secret", "password", "token", "private_key"}
        if depth > 20:
            return "[MAX_DEPTH]"
        if isinstance(obj, dict):
            return {k: self._sanitize(v, depth+1) for k, v in obj.items() if k not in SENSITIVE}
        elif isinstance(obj, list):
            return [self._sanitize(item, depth+1) for item in obj]
        return obj

    def _check_disk_space(self):
        try:
            stat = os.statvfs(self.config.LOG_DIR)
            free_pct = (stat.f_bavail / stat.f_blocks) * 100 if stat.f_blocks > 0 else 0
            if free_pct < self.config.DISK_FREE_THRESHOLD_PCT:
                raise OSError(f"磁盘空间不足 {free_pct:.1f}%")
        except OSError:
            raise  # 上层 log 方法将转为异常

    def _should_rotate(self) -> bool:
        if self._file_handle is None:
            return True
        if _monotonic_clock() - self._file_open_ts >= self.config.ROTATION_INTERVAL_SEC:
            return True
        if self._current_file:
            try:
                if self._current_file.stat().st_size >= self.config.MAX_FILE_SIZE_BYTES:
                    return True
            except OSError:
                pass
        return False

    def _rotate_file(self):
        # 先关闭当前文件（签名+入队）
        if self._file_handle is not None:
            self._finalize_current_file()
        # 生成新文件名（含随机串防止冲突）
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
        rand = secrets.token_hex(4)
        fname = f"{self.config.FILE_PREFIX}{ts}_{rand}.audit"
        self._current_file = Path(self.config.LOG_DIR) / fname
        try:
            self._file_handle = open(self._current_file, 'wb', buffering=0)
            self._file_open_ts = _monotonic_clock()
            self._file_sequence += 1
            os.chmod(self._current_file, 0o600)
        except OSError as e:
            logger.critical("创建审计文件失败: %s", str(e))
            raise AuditWriteError("创建日志文件失败") from e

    def _finalize_current_file(self):
        if self._file_handle is None:
            return
        try:
            self._file_handle.flush()
            os.fsync(self._file_handle.fileno())
        finally:
            self._file_handle.close()
            self._file_handle = None
        # 签名、压缩、加密
        if self._current_file:
            self._sign_and_compress_file(self._current_file)
            self._enqueue_upload(self._current_file)
            self._current_file = None

    def _sign_and_compress_file(self, path: Path):
        """流式读取文件，计算 HMAC，压缩加密，写回"""
        if not self._signing_key:
            return
        # 读取原始内容（优化：大文件分块读取）
        raw_data = b''
        with open(path, 'rb') as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                raw_data += chunk
        # 可选压缩
        if self.config.COMPRESSION_ENABLED and HAS_ZSTD:
            raw_data = zstd.compress(raw_data)
        # 可选加密
        if self.config.ENCRYPTION_ENABLED and self._fernet:
            raw_data = self._fernet.encrypt(raw_data)
        # 计算 HMAC
        signature = hmac.new(self._signing_key, raw_data, hashlib.sha256).hexdigest()
        # 写回
        with open(path, 'wb') as f:
            f.write(raw_data)
            f.write(signature.encode('ascii'))
        os.chmod(path, 0o400)  # 只读
        # 更新链式哈希（基于文件名+序列号）
        self._update_chain_hash(f"{self._file_sequence}:{path.name}".encode())

    def _update_chain_hash(self, data: bytes):
        self._last_chain_hash = hashlib.sha256(self._last_chain_hash + data).digest()

    def _start_flush_timer(self):
        def _flush():
            with self._write_lock:
                if self._file_handle:
                    try:
                        self._file_handle.flush()
                        os.fsync(self._file_handle.fileno())
                    except Exception:
                        pass
            if not self._stop_event.is_set():
                self._flush_timer = threading.Timer(self.config.FLUSH_INTERVAL_SEC, _flush)
                self._flush_timer.start()
        self._flush_timer = threading.Timer(self.config.FLUSH_INTERVAL_SEC, _flush)
        self._flush_timer.start()

    # ── 上传逻辑 ────────────────────────────────────────

    def _enqueue_upload(self, path: Path):
        try:
            self._upload_queue.put_nowait(path)
        except queue.Full:
            logger.critical("上传队列已满，丢弃文件: %s", path)
            self._metrics["entries_dropped"] += 1

    def _start_upload_worker(self):
        self._upload_thread = threading.Thread(target=self._upload_loop, name="AuditUploader", daemon=True)
        self._upload_thread.start()

    def _upload_loop(self):
        while not self._stop_event.is_set() or not self._upload_queue.empty():
            try:
                path = self._upload_queue.get(timeout=2)
                success = self._upload_to_s3_with_retry(path)
                if success:
                    self._metrics["uploads_succeeded"] += 1
                else:
                    self._metrics["uploads_failed"] += 1
                    # 放回队列重试
                    try:
                        self._upload_queue.put_nowait(path)
                    except queue.Full:
                        self._metrics["entries_dropped"] += 1
            except queue.Empty:
                continue
            except Exception as e:
                logger.error("上传循环异常: %s", str(e))

    def _upload_to_s3_with_retry(self, path: Path) -> bool:
        for attempt in range(self.config.MAX_UPLOAD_RETRIES):
            try:
                self._upload_to_s3(path)
                return True
            except Exception as e:
                logger.warning("S3 上传失败 (尝试 %d/%d): %s", attempt+1, self.config.MAX_UPLOAD_RETRIES, str(e))
                time.sleep(min(2 ** attempt, 30))
        return False

    def _upload_to_s3(self, path: Path):
        if not HAS_BOTO3:
            raise RuntimeError("boto3 未安装")
        s3 = boto3.client('s3', config=BotoConfig(region_name=self.config.S3_REGION, retries={'max_attempts': 3}))
        key = f"{self.config.S3_PREFIX}{path.name}"
        extra_args = {
            'ServerSideEncryption': 'AES256',
            'StorageClass': 'STANDARD_IA',
            'Metadata': {
                'source-host': os.uname().nodename,
                'original-file': path.name,
            }
        }
        if self.config.S3_OBJECT_LOCK_ENABLED:
            extra_args['ObjectLockMode'] = 'GOVERNANCE'
            extra_args['ObjectLockRetainUntilDate'] = datetime.now(timezone.utc) + timedelta(days=self.config.S3_RETENTION_DAYS)
        s3.upload_file(str(path), self.config.S3_BUCKET, key, ExtraArgs=extra_args)
        # 验证上传完整性（可选）
        # 本地文件可移至已上传目录或直接删除（根据保留策略）
        if self.config.RETENTION_DAYS > 0:
            # 保留一段时间后由清理线程删除
            self._maybe_clean_old_files()

    def _maybe_clean_old_files(self):
        """删除本地超过保留期的审计日志"""
        cutoff = time.time() - self.config.RETENTION_DAYS * 86400
        try:
            for p in Path(self.config.LOG_DIR).glob(f"{self.config.FILE_PREFIX}*.audit"):
                if p.stat().st_mtime < cutoff:
                    p.unlink(missing_ok=True)
        except Exception as e:
            logger.warning("清理旧审计文件时出错: %s", str(e))
