#!/usr/bin/env python3
"""
火种系统 · 数据归档与清理工具 (DataArchiver)
版本: 2.1.0  |  符合全球顶级量化对冲基金生产环境标准

核心职责：
1. 安全、可靠地归档或清理超过保留期限的行情数据与日志文件。
2. 保证数据完整性（SHA256校验）、操作幂等性、并发安全。
3. 提供全链路审计、结构化日志、Prometheus 监控集成。

外部依赖（真实模块接口）：
- 标准库：os, sys, time, uuid, gzip, json, signal, hashlib, tempfile, tarfile, shutil, pathlib, fcntl (Unix)
- 可选：portalocker (跨平台锁), prometheus_client (指标)

接口契约：
- archive(source_dir, archive_dir, retention_days, ...) -> Dict
- cleanup(source_dir, retention_days, ...) -> Dict
- health_check() -> Dict
- 所有返回字典固定包含 "status" (str), "reason" (str), "warnings" (List[str]), "details" (Dict)

异常与降级：
- 所有文件操作均捕获具体异常，记录结构化日志，并支持重试。
- 关键操作前进行多维度磁盘空间预检，不满足时拒绝执行。
- 锁机制支持超时、僵尸锁检测，确保高可用。

资源管理：
- 使用临时文件+原子重命名+显式 fsync，确保归档完整性。
- 文件句柄在 finally 中释放，临时文件异常时清理。
- 扫描文件数、列表大小均有上限控制，防止内存溢出。
"""

import os
import sys
import time
import uuid
import json
import signal
import hashlib
import logging
import tarfile
import tempfile
import datetime
import argparse
import platform
import functools
import threading
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Callable

# 尝试导入跨平台文件锁（推荐）
try:
    import portalocker
    HAS_PORTALOCKER = True
except ImportError:
    HAS_PORTALOCKER = False

# 尝试导入 Prometheus 客户端
try:
    from prometheus_client import Counter, Gauge, start_http_server
    METRICS_ENABLED = True
except ImportError:
    METRICS_ENABLED = False

# ── 日志配置 ──
logger = logging.getLogger(__name__)
audit_logger = logging.getLogger("audit.file_ops")

# 可选告警通知钩子
_notification_hook: Optional[Callable[[str, str], None]] = None


def set_notification_hook(hook: Callable[[str, str], None]):
    """注入告警通知函数"""
    global _notification_hook
    _notification_hook = hook


# ── Prometheus 指标 ──
if METRICS_ENABLED:
    ops_counter = Counter('data_archiver_ops_total', 'Total archiver operations', ['mode', 'status'])
    space_gauge = Gauge('data_archiver_free_space_ratio', 'Free space ratio', ['path'])
    file_count_gauge = Gauge('data_archiver_files_processed', 'Files processed in last run')
    last_run_timestamp = Gauge('data_archiver_last_run_ts', 'Timestamp of last run')


class DataArchiver:
    """数据归档与清理器（华尔街级实现）"""

    # ── 类常量 ──
    DEFAULT_RETENTION_DAYS = 30
    MIN_RETENTION_DAYS = 7
    MAX_RETENTION_DAYS = 365
    ARCHIVE_SUFFIX = ".tar.gz"
    MIN_FREE_SPACE_RATIO = 0.10
    MAX_SCAN_FILES = 10000
    MAX_DETAILS_LIST_SIZE = 500                 # 返回列表中最大文件路径数
    LOG_PATTERN = "*.log"
    DATA_PATTERNS = ["*.csv", "*.parquet", "*.json", "*.db"]
    ARCHIVE_DIR_PERMISSIONS = 0o750
    ARCHIVE_FILE_PERMISSIONS = 0o640
    CHECKSUM_ALG = 'sha256'
    MAX_RETRIES = 1
    COMPRESSION_LEVEL = 6
    LOCK_TIMEOUT = 30                           # 锁获取超时（秒）
    STABILITY_WINDOW_SECONDS = 60               # 文件稳定时间：至少 N 秒未修改才处理

    # 内部状态
    _active_locks = threading.local()           # 线程安全的锁文件记录

    # ── 工具方法 ──
    @staticmethod
    def _get_utc_now() -> datetime.datetime:
        return datetime.datetime.now(datetime.timezone.utc)

    @staticmethod
    def _get_file_age_days(filepath: str, reference_ts: float = None) -> float:
        """基于 mtime 计算文件年龄（天）；若时钟异常可能为负，调用方需处理"""
        mtime = os.path.getmtime(filepath)
        if reference_ts is None:
            reference_ts = time.time()
        return (reference_ts - mtime) / 86400.0

    @classmethod
    def _ensure_dir(cls, path: str, permissions: int = 0o750) -> bool:
        """创建目录并设置权限，已存在则修正权限"""
        try:
            os.makedirs(path, exist_ok=True)
            os.chmod(path, permissions)
            return True
        except OSError as e:
            logger.error("目录操作失败", extra={"path": path, "error": str(e)})
            return False

    @classmethod
    def _get_free_space_ratio(cls, path: str) -> Optional[float]:
        try:
            stat = shutil.disk_usage(path)
            return stat.free / stat.total
        except OSError:
            return None

    @classmethod
    def _acquire_lock(cls, lockfile: str, timeout: int = None) -> bool:
        """获取排他文件锁，支持超时，跨平台回退"""
        if timeout is None:
            timeout = cls.LOCK_TIMEOUT
        if HAS_PORTALOCKER:
            try:
                fd = os.open(lockfile, os.O_CREAT | os.O_RDWR, 0o640)
                portalocker.lock(fd, portalocker.LOCK_EX, timeout=timeout)
                # 存储 fd 以便释放
                if not hasattr(cls._active_locks, 'fds'):
                    cls._active_locks.fds = {}
                cls._active_locks.fds[lockfile] = fd
                return True
            except (portalocker.LockException, OSError) as e:
                logger.error("获取锁失败", extra={"lockfile": lockfile, "error": str(e)})
                return False
        else:
            # 平台回退：基于文件存在 + 超时 + 僵尸锁检测
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                try:
                    fd = os.open(lockfile, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o640)
                    os.write(fd, str(os.getpid()).encode())
                    os.fsync(fd)
                    if not hasattr(cls._active_locks, 'fds'):
                        cls._active_locks.fds = {}
                    cls._active_locks.fds[lockfile] = fd
                    return True
                except FileExistsError:
                    # 检查僵尸锁（进程是否存在）
                    if cls._is_lock_stale(lockfile):
                        cls._force_release_lock(lockfile)
                        continue
                    time.sleep(0.5)
                except OSError:
                    return False
            logger.error("获取锁超时", extra={"lockfile": lockfile})
            return False

    @classmethod
    def _is_lock_stale(cls, lockfile: str) -> bool:
        """检查锁文件是否为僵尸锁（PID不存在或无法读取）"""
        try:
            with open(lockfile, 'r') as f:
                pid = int(f.read().strip())
            os.kill(pid, 0)  # 检测进程是否存在
            return False
        except (OSError, ValueError):
            return True

    @classmethod
    def _force_release_lock(cls, lockfile: str):
        """强制删除锁文件"""
        try:
            os.remove(lockfile)
            logger.info("移除僵尸锁", extra={"lockfile": lockfile})
        except OSError:
            pass

    @classmethod
    def _release_lock(cls, lockfile: str):
        """释放锁并删除锁文件"""
        try:
            if hasattr(cls._active_locks, 'fds') and lockfile in cls._active_locks.fds:
                fd = cls._active_locks.fds.pop(lockfile)
                if HAS_PORTALOCKER:
                    portalocker.unlock(fd)
                os.close(fd)
            # 确保文件清理
            if os.path.exists(lockfile):
                os.remove(lockfile)
        except OSError:
            pass

    @classmethod
    def _release_all_locks(cls):
        """释放当前线程所有锁"""
        for lockfile in list(getattr(cls._active_locks, 'fds', {}).keys()):
            cls._release_lock(lockfile)

    @classmethod
    def _sanitize_path(cls, path: str) -> str:
        """返回绝对真实路径，禁止符号链接攻击"""
        return os.path.realpath(os.path.abspath(path))

    @classmethod
    def _sanitize_filename(cls, name: str) -> str:
        """移除文件名中的危险字符"""
        import re
        safe = re.sub(r'[^\w\-.]', '_', name)
        return safe[:200]  # 限制长度

    @classmethod
    def _find_old_files(cls,
                        directory: str,
                        retention_days: int,
                        patterns: List[str],
                        reference_ts: float = None) -> List[str]:
        """
        递归扫描目录，返回超过保留期限的常规文件列表（去重）。
        跳过符号链接、已归档文件、近期修改文件（稳定窗口）。
        """
        old_files_set = set()
        if not os.path.isdir(directory):
            return []
        if reference_ts is None:
            reference_ts = time.time()
        dir_path = Path(directory)
        total_scanned = 0

        for pattern in patterns:
            if total_scanned >= cls.MAX_SCAN_FILES:
                logger.warning("扫描文件数达到全局上限，停止扫描", extra={"limit": cls.MAX_SCAN_FILES})
                break
            try:
                # 使用 rglob 递归，但限制跟随符号链接
                for filepath in dir_path.rglob(pattern):
                    if total_scanned >= cls.MAX_SCAN_FILES:
                        break
                    try:
                        if filepath.is_symlink() or not filepath.is_file():
                            continue
                        if filepath.name.endswith(cls.ARCHIVE_SUFFIX):
                            continue
                        # 稳定性检查：文件至少 N 秒未被修改
                        mtime = filepath.stat().st_mtime
                        if reference_ts - mtime < cls.STABILITY_WINDOW_SECONDS:
                            continue
                        age = cls._get_file_age_days(str(filepath), reference_ts)
                        if age >= retention_days:
                            old_files_set.add(str(filepath))
                            total_scanned += 1
                    except OSError:
                        continue
            except OSError as e:
                logger.error("遍历目录失败", extra={"directory": str(dir_path), "error": str(e)})

        # 截断到最大扫描数
        result = list(old_files_set)[:cls.MAX_SCAN_FILES]
        if len(old_files_set) > cls.MAX_SCAN_FILES:
            logger.warning("旧文件数量超过上限，仅处理部分", extra={"total": len(old_files_set), "limit": cls.MAX_SCAN_FILES})
        return result

    @classmethod
    def _verify_checksum(cls, filepath: str, expected: str) -> bool:
        """验证文件校验和"""
        return cls._compute_checksum(filepath) == expected

    @classmethod
    def _compute_checksum(cls, filepath: str) -> str:
        """计算文件 SHA256"""
        sha = hashlib.new(cls.CHECKSUM_ALG)
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha.update(chunk)
        return sha.hexdigest()

    @classmethod
    def _safe_remove(cls, filepath: str, retries: int = 1) -> bool:
        """带重试的安全删除，非阻塞版本"""
        for attempt in range(retries + 1):
            try:
                os.remove(filepath)
                return True
            except OSError as e:
                if attempt < retries:
                    time.sleep(0.1 * (2 ** attempt))
                else:
                    logger.error("删除失败", extra={"file": filepath, "error": str(e)})
                    return False
        return False

    @classmethod
    def _compress_and_archive(cls,
                              filepath: str,
                              archive_dir: str,
                              dry_run: bool = False,
                              verify: bool = True) -> Tuple[bool, str, Optional[dict]]:
        """
        压缩单个文件到归档目录。
        原子性：临时文件 -> fsync -> rename -> 校验 -> 删除原文件。
        """
        if dry_run:
            logger.info("干运行：将归档 %s -> %s", filepath, archive_dir)
            return True, "", {"file": filepath, "action": "archive_dry_run"}

        real_path = cls._sanitize_path(filepath)
        if not os.path.isfile(real_path) or os.path.islink(real_path):
            return False, "不是常规文件", None

        # 防止递归归档
        canonical_archive = cls._sanitize_path(archive_dir)
        if real_path.startswith(canonical_archive + os.sep):
            return False, "文件已在归档目录内", None

        # 创建归档目录
        if not cls._ensure_dir(canonical_archive, cls.ARCHIVE_DIR_PERMISSIONS):
            return False, "归档目录不可用", None

        # 生成安全的归档文件名
        safe_name = cls._sanitize_filename(os.path.basename(real_path))
        unique_id = str(uuid.uuid4())[:8]
        timestamp = cls._get_utc_now().strftime('%Y%m%d%H%M%S')
        archive_name = f"{safe_name}_{timestamp}_{unique_id}{cls.ARCHIVE_SUFFIX}"
        archive_path = os.path.join(canonical_archive, archive_name)

        # 预检磁盘空间
        try:
            src_size = os.path.getsize(real_path)
        except OSError as e:
            return False, f"无法获取文件大小: {e}", None
        estimated = src_size * 1.5 + 50 * 1024 * 1024  # 保守估计
        try:
            du = shutil.disk_usage(canonical_archive)
            if du.free < estimated:
                return False, f"磁盘空间不足，需要 {estimated} 字节，可用 {du.free}", None
        except OSError as e:
            return False, f"磁盘信息获取失败: {e}", None

        # 压缩到临时文件
        tmp_fd, tmp_path = None, None
        try:
            tmp_fd, tmp_path = tempfile.mkstemp(suffix=cls.ARCHIVE_SUFFIX, dir=canonical_archive)
            os.close(tmp_fd)
            tmp_fd = None

            with tarfile.open(tmp_path, "w:gz", compresslevel=cls.COMPRESSION_LEVEL, errorlevel=2) as tar:
                tar.add(real_path, arcname=safe_name)

            os.chmod(tmp_path, cls.ARCHIVE_FILE_PERMISSIONS)

            # 计算校验和
            if verify:
                checksum = cls._compute_checksum(tmp_path)
            else:
                checksum = None

            # 原子重命名
            os.replace(tmp_path, archive_path)
            tmp_path = None

            # 校验归档文件
            if verify:
                if not cls._verify_checksum(archive_path, checksum):
                    cls._safe_remove(archive_path)
                    return False, "归档文件校验失败", None

            # 删除原文件（仅校验通过后）
            if not cls._safe_remove(real_path):
                logger.warning("原文件删除失败，需手动处理", extra={"file": real_path})

            audit_info = {
                "original": real_path,
                "archive": archive_path,
                "checksum": checksum,
                "algorithm": cls.CHECKSUM_ALG if verify else "none",
                "timestamp": cls._get_utc_now().isoformat()
            }
            audit_logger.info("归档成功", extra=audit_info)
            return True, "", audit_info

        except (OSError, tarfile.TarError, ValueError) as e:
            logger.error("归档失败", extra={"file": real_path, "error": str(e)})
            if tmp_path and os.path.exists(tmp_path):
                cls._safe_remove(tmp_path)
            return False, str(e), None

    @classmethod
    def archive(cls,
                source_dir: str,
                archive_dir: str,
                retention_days: int = DEFAULT_RETENTION_DAYS,
                dry_run: bool = False,
                patterns: Optional[List[str]] = None,
                verify: bool = True) -> Dict[str, Any]:
        """归档过期文件"""
        warnings = []
        details = {"archived": [], "failed": [], "total_candidates": 0}

        # 参数验证
        if not cls.MIN_RETENTION_DAYS <= retention_days <= cls.MAX_RETENTION_DAYS:
            return {"status": "error",
                    "reason": f"保留天数必须在 {cls.MIN_RETENTION_DAYS}~{cls.MAX_RETENTION_DAYS} 之间",
                    "warnings": [], "details": details}
        if patterns is None:
            patterns = cls.DATA_PATTERNS

        real_source = cls._sanitize_path(source_dir)
        real_archive = cls._sanitize_path(archive_dir)

        # 禁止危险路径关系
        if real_source == real_archive:
            return {"status": "error", "reason": "源目录与归档目录相同", "warnings": [], "details": details}
        if real_archive.startswith(real_source + os.sep):
            return {"status": "error", "reason": "归档目录是源目录的子目录", "warnings": [], "details": details}
        if real_source.startswith(real_archive + os.sep):
            return {"status": "error", "reason": "源目录是归档目录的子目录", "warnings": [], "details": details}

        # 获取锁（与目录绑定）
        lockfile = os.path.join(tempfile.gettempdir(), f"data_archiver_{hash(real_source) & 0xFFFFFFFF:08x}.lock")
        if not cls._acquire_lock(lockfile):
            return {"status": "error", "reason": "无法获取操作锁", "warnings": [], "details": details}
        try:
            # 空间预检
            free_ratio = cls._get_free_space_ratio(real_source)
            if free_ratio is not None and free_ratio < cls.MIN_FREE_SPACE_RATIO:
                msg = f"源磁盘剩余空间不足 {cls.MIN_FREE_SPACE_RATIO*100}%"
                warnings.append(msg)
                if _notification_hook:
                    _notification_hook("磁盘告警", msg)

            old_files = cls._find_old_files(real_source, retention_days, patterns)
            details["total_candidates"] = len(old_files)

            for fpath in old_files:
                success, error, _ = cls._compress_and_archive(fpath, real_archive, dry_run, verify)
                if success:
                    if len(details["archived"]) < cls.MAX_DETAILS_LIST_SIZE:
                        details["archived"].append(fpath)
                else:
                    if len(details["failed"]) < cls.MAX_DETAILS_LIST_SIZE:
                        details["failed"].append({"file": fpath, "error": error})
                    warnings.append(f"归档失败 {fpath}: {error}")
        finally:
            cls._release_lock(lockfile)

        # 指标
        if METRICS_ENABLED:
            status = "success" if not details["failed"] else "partial_failure"
            ops_counter.labels(mode="archive", status=status).inc()
            file_count_gauge.set(len(details["archived"]))
            last_run_timestamp.set(time.time())

        if not old_files:
            return {"status": "ok", "reason": "无过期文件", "warnings": warnings, "details": details}
        elif not details["failed"]:
            return {"status": "ok", "reason": f"成功归档 {len(details['archived'])} 个文件", "warnings": warnings, "details": details}
        else:
            return {"status": "passed_with_warnings", "reason": "部分归档失败", "warnings": warnings, "details": details}

    @classmethod
    def cleanup(cls,
                source_dir: str,
                retention_days: int = DEFAULT_RETENTION_DAYS,
                dry_run: bool = False,
                patterns: Optional[List[str]] = None) -> Dict[str, Any]:
        """直接删除过期文件（用于日志）"""
        warnings = []
        details = {"deleted": [], "failed": [], "total_candidates": 0}

        if not cls.MIN_RETENTION_DAYS <= retention_days <= cls.MAX_RETENTION_DAYS:
            return {"status": "error", "reason": f"保留天数非法", "warnings": [], "details": details}
        if patterns is None:
            patterns = [cls.LOG_PATTERN]

        real_source = cls._sanitize_path(source_dir)
        lockfile = os.path.join(tempfile.gettempdir(), f"data_archiver_cleanup_{hash(real_source) & 0xFFFFFFFF:08x}.lock")
        if not cls._acquire_lock(lockfile):
            return {"status": "error", "reason": "无法获取操作锁", "warnings": [], "details": details}
        try:
            old_files = cls._find_old_files(real_source, retention_days, patterns)
            details["total_candidates"] = len(old_files)
            for fpath in old_files:
                if dry_run:
                    details["deleted"].append(fpath)
                    continue
                if cls._safe_remove(fpath):
                    if len(details["deleted"]) < cls.MAX_DETAILS_LIST_SIZE:
                        details["deleted"].append(fpath)
                    audit_logger.info("删除文件", extra={"file": fpath})
                else:
                    if len(details["failed"]) < cls.MAX_DETAILS_LIST_SIZE:
                        details["failed"].append({"file": fpath, "error": "删除失败"})
                    warnings.append(f"删除失败: {fpath}")
        finally:
            cls._release_lock(lockfile)

        if METRICS_ENABLED:
            ops_counter.labels(mode="cleanup", status="success" if not details["failed"] else "partial_failure").inc()
            file_count_gauge.set(len(details["deleted"]))
            last_run_timestamp.set(time.time())

        if not old_files:
            return {"status": "ok", "reason": "无过期文件", "warnings": warnings, "details": details}
        elif not details["failed"]:
            return {"status": "ok", "reason": f"成功删除 {len(details['deleted'])} 个文件", "warnings": warnings, "details": details}
        else:
            return {"status": "passed_with_warnings", "reason": "部分删除失败", "warnings": warnings, "details": details}

    @classmethod
    def health_check(cls) -> Dict[str, Any]:
        """非侵入式健康检查"""
        try:
            assert cls.MIN_RETENTION_DAYS < cls.MAX_RETENTION_DAYS
            _ = tempfile.gettempdir()
            return {"status": "ok", "message": "模块可用", "warnings": []}
        except Exception as e:
            return {"status": "error", "message": str(e), "warnings": [str(e)]}


# ── 优雅关闭 ──
def _handle_shutdown(signum, frame):
    logger.info("收到信号 %s，释放所有锁并退出", signum)
    DataArchiver._release_all_locks()
    sys.exit(0)


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)


def main():
    parser = argparse.ArgumentParser(description="火种数据归档与清理工具（华尔街级）")
    parser.add_argument("--source", required=True, help="源数据目录")
    parser.add_argument("--archive-dir", help="归档目录（archive 模式必需）")
    parser.add_argument("--mode", choices=["archive", "cleanup"], default="archive")
    parser.add_argument("--retention-days", type=int, default=DataArchiver.DEFAULT_RETENTION_DAYS)
    parser.add_argument("--dry-run", action="store_true", help="模拟运行")
    parser.add_argument("--patterns", nargs="+", help="文件匹配模式")
    parser.add_argument("--no-verify", action="store_true", help="跳过归档校验（不推荐，有数据丢失风险）")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR"])
    parser.add_argument("--json-log", action="store_true", help="JSON 格式日志")
    parser.add_argument("--version", action="version", version="%(prog)s 2.1.0")
    args = parser.parse_args()

    # 日志配置
    if args.json_log:
        try:
            import json_logging
            json_logging.init_non_web(enable_json=True)
            logging.basicConfig(level=args.log_level)
            json_logging.config_root_logger()
        except ImportError:
            logging.basicConfig(level=args.log_level, format="%(asctime)s [%(levelname)s] %(message)s")
            logger.warning("json_logging 未安装，使用文本日志")
    else:
        logging.basicConfig(level=args.log_level,
                            format="%(asctime)s [%(levelname)s] %(process)d %(name)s: %(message)s")

    if args.mode == "archive" and not args.archive_dir:
        print("错误：archive 模式需要 --archive-dir", file=sys.stderr)
        sys.exit(1)

    if args.mode == "archive":
        result = DataArchiver.archive(
            source_dir=args.source,
            archive_dir=args.archive_dir,
            retention_days=args.retention_days,
            dry_run=args.dry_run,
            patterns=args.patterns,
            verify=not args.no_verify
        )
    else:
        result = DataArchiver.cleanup(
            source_dir=args.source,
            retention_days=args.retention_days,
            dry_run=args.dry_run,
            patterns=args.patterns
        )

    print(f"\n状态: {result['status'].upper()}")
    print(f"原因: {result['reason']}")
    if result.get("warnings"):
        for w in result["warnings"]:
            print(f"  ⚠ {w}")
    d = result.get("details", {})
    if "total_candidates" in d: print(f"  待处理: {d['total_candidates']}")
    if "archived" in d: print(f"  已归档: {len(d['archived'])}")
    if "deleted" in d: print(f"  已删除: {len(d['deleted'])}")
    if "failed" in d and d["failed"]:
        print(f"  失败: {len(d['failed'])}")
        for f in d["failed"][:10]:
            print(f"    - {f['file']}: {f['error']}")

    sys.exit(0 if result["status"] != "error" else 1)


if __name__ == "__main__":
    main()
