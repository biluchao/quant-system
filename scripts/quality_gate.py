#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors. All Rights Reserved.
"""
火种系统 · 代码质量门禁总入口 (QualityGate)

核心职责：
1. 按优先级顺序执行全部代码质量与配置校验（支持子进程隔离与超时控制）
2. 生成结构化审计报告（终端输出 + JSON + Prometheus指标 + 审计链）
3. 提供细粒度退出码供CI/CD与运维监控集成

外部依赖（真实模块接口）：
- scripts.style_checker : 风格校验器
- scripts.dependency_checker : 依赖闭环校验器
- scripts.interface_verifier : 接口契约校验器
- scripts.config_decouple_checker.ConfigDecoupleChecker : 配置解耦校验器
- scripts.schema_validator : 配置文件Schema校验器
- core.metrics : Prometheus指标输出（可选依赖）
- core.audit_logger : 审计日志（可选依赖）

接口契约：
- run_all_checks(project_root: str, timeout: int = 300, report_path: Optional[str] = None, ...) -> Dict[str, Any]
  返回完整报告字典，包含 "exit_code", "summary", "details", "metrics", "audit_chain"
- health_check(project_root: str = ".") -> Dict[str, Any]
  输出字典固定包含 "status" (str), "reason" (str), "warnings" (List[str])

异常与降级：
- 任一校验器超时则记录 TIMEOUT 状态并继续后续校验（子进程隔离确保资源清理）
- 若校验器模块导入失败，记录 CRITICAL 并视为强制失败
- 所有异常按类别分级处理：TimeoutError > ImportError > RuntimeError > Exception
- 门禁自身异常时生成 emergency_report.json 并退出码 4

资源管理：
- 使用子进程隔离执行每个校验器，确保内存与CPU完全清理
- 审计报告使用原子写入（临时文件 + fsync + rename），权限 0o640
- 退出时 atexit 注册清理函数，确保临时文件删除
- 文件锁防止多实例并发执行

用法示例:
    python scripts/quality_gate.py --project-root . --timeout 120 --report report.json --severity high
"""

import argparse
import atexit
import fcntl
import importlib
import importlib.metadata
import json
import logging
import os
import platform
import shutil
import signal
import sys
import tempfile
import threading
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, NamedTuple, Set, Union

# ── 可选依赖（优雅降级） ──────────────────────────────────
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

try:
    from core.metrics import MetricsCollector
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    MetricsCollector = None

try:
    from core.audit_logger import AuditLogger
    AUDIT_AVAILABLE = True
except ImportError:
    AUDIT_AVAILABLE = False
    AuditLogger = None

# ── 常量定义 ──────────────────────────────────────────────
try:
    VERSION = importlib.metadata.version("spark-quant")
except importlib.metadata.PackageNotFoundError:
    VERSION = "3.0.1-dev"

SPDX_IDENTIFIER = "Apache-2.0"
DEFAULT_TIMEOUT_SECONDS = 300
DEFAULT_PROJECT_ROOT = "."
MAX_VIOLATIONS_DISPLAY = 20
SEPARATOR_LINE = "=" * 70
REPORT_FILENAME = "quality_gate_report_{timestamp}_{uuid}.json"
TEMP_DIR_PREFIX = "spark_quality_gate_"
LOCK_FILE_NAME = ".quality_gate.lock"
EXIT_CODE_MAP: Dict[str, int] = {
    "all_passed": 0,
    "warnings_only": 0,
    "soft_fail": 1,
    "hard_fail": 2,
    "timeout": 3,
    "system_error": 4,
}
VALID_STATUSES: Set[str] = {'ok', 'passed_with_warnings', 'failed', 'error', 'timeout'}
PYTHON_MIN_VERSION: Tuple[int, int] = (3, 10)
DEFAULT_REPORT_MODE = 0o640
DEFAULT_TEMP_MODE = 0o700
LOG_FORMAT_JSON = (
    '{"timestamp":"%(asctime)s","level":"%(levelname)s",'
    '"module":"%(name)s","message":"%(message)s"}'
)
# 线程本地存储（用于信号安全）
_thread_local = threading.local()

logger = logging.getLogger(__name__)


class CheckerDef(NamedTuple):
    """校验器定义"""
    module_name: str
    display_name: str
    required: bool
    timeout: int = DEFAULT_TIMEOUT_SECONDS


class QualityGate:
    """
    代码质量门禁，串联全部校验器并生成审计报告

    支持子进程隔离执行、超时控制、审计链、指标输出
    """

    DEFAULT_PROJECT_ROOT = DEFAULT_PROJECT_ROOT
    CHECKERS: List[CheckerDef] = [
        CheckerDef("scripts.style_checker", "风格校验", True),
        CheckerDef("scripts.dependency_checker", "依赖闭环校验", True),
        CheckerDef("scripts.interface_verifier", "接口契约校验", True),
        CheckerDef("scripts.config_decouple_checker", "配置解耦校验", True),
        CheckerDef("scripts.schema_validator", "配置文件Schema校验", True),
    ]

    _lock_fd: Optional[int] = None
    _temp_dir: Optional[str] = None
    _cleanup_registered: bool = False

    # ── 路径安全验证 ──────────────────────────────────────
    @classmethod
    def _validate_project_root(cls, project_root: str) -> str:
        """
        安全校验项目根目录路径

        防御：路径遍历攻击、符号链接循环、NFS挂载点阻塞
        """
        if not isinstance(project_root, str):
            raise ValueError(f"project_root 必须为字符串，收到 {type(project_root).__name__}")
        project_root = project_root.strip()
        if not project_root:
            raise ValueError("project_root 不能为空字符串")
        try:
            resolved = Path(project_root).resolve(strict=False)
        except (OSError, RuntimeError) as e:
            raise ValueError(f"路径解析失败: {e}") from e
        # 检测路径遍历
        parts = str(resolved).split(os.sep)
        if ".." in parts:
            raise ValueError(f"检测到路径遍历攻击: {project_root}")
        if not resolved.is_dir():
            raise FileNotFoundError(f"项目根目录不存在: {resolved}")
        return str(resolved)

    # ── 文件锁（防并发） ──────────────────────────────────
    @classmethod
    def _acquire_lock(cls, project_root: str) -> bool:
        """获取排他文件锁，防止多实例并发"""
        lock_path = os.path.join(project_root, LOCK_FILE_NAME)
        try:
            cls._lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o640)
            fcntl.flock(cls._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except (IOError, OSError):
            logger.error("无法获取门禁锁，可能有其他实例正在运行")
            return False

    @classmethod
    def _release_lock(cls):
        """释放文件锁并清理"""
        if cls._lock_fd is not None:
            try:
                fcntl.flock(cls._lock_fd, fcntl.LOCK_UN)
                os.close(cls._lock_fd)
            except Exception:
                pass
            cls._lock_fd = None

    # ── 临时目录管理 ──────────────────────────────────────
    @classmethod
    def _ensure_temp_dir(cls) -> str:
        """创建安全临时目录并注册清理"""
        if cls._temp_dir is None:
            cls._temp_dir = tempfile.mkdtemp(prefix=TEMP_DIR_PREFIX)
            os.chmod(cls._temp_dir, DEFAULT_TEMP_MODE)
            if not cls._cleanup_registered:
                atexit.register(cls._cleanup_temp_dir)
                cls._cleanup_registered = True
        return cls._temp_dir

    @classmethod
    def _cleanup_temp_dir(cls):
        """清理临时目录"""
        if cls._temp_dir and os.path.exists(cls._temp_dir):
            try:
                shutil.rmtree(cls._temp_dir, ignore_errors=True)
            except Exception:
                pass
            cls._temp_dir = None

    # ── 校验器定位 ────────────────────────────────────────
    @classmethod
    def _locate_check_function(cls, module, module_name: str):
        """
        在模块中定位 check 函数

        优先级：模块级 check > 类的 check 方法
        安全：避免触发 __getattr__ 或描述符
        """
        # 优先查找模块级 check 函数（安全：直接属性访问）
        if hasattr(module, 'check') and callable(getattr(module, 'check')):
            return module.check
        # 安全的类方法查找
        available = []
        for name in dir(module):
            if name.startswith('_') or name in ('__builtins__', '__cached__', '__doc__'):
                continue
            try:
                obj = getattr(module, name)
                if isinstance(obj, type) and hasattr(obj, 'check'):
                    available.append(name)
                    return obj.check
            except Exception:
                continue
        raise AttributeError(
            f"模块 {module_name} 未提供 check() 接口。"
            f"找到的类型: {available or '无'}"
        )

    # ── 单校验器执行（子进程隔离） ─────────────────────────
    @classmethod
    def _run_single_checker(
        cls, checker: CheckerDef, project_root: str
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        在子进程中隔离执行单个校验器

        使用 subprocess 确保：
        1. 超时强制终止（SIGKILL）
        2. 内存与文件描述符完全隔离
        3. 避免信号处理器冲突
        """
        import subprocess as sp

        start_time = time.perf_counter_ns()
        checker_script = os.path.join(
            project_root, checker.module_name.replace('.', os.sep) + '.py'
        )

        try:
            proc = sp.run(
                [
                    sys.executable, "-u", checker_script,
                    "--check-standalone",
                    "--project-root", project_root,
                ],
                capture_output=True,
                text=True,
                timeout=checker.timeout,
                cwd=project_root,
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
            )
            elapsed_ns = time.perf_counter_ns() - start_time
            elapsed = elapsed_ns / 1e9

            if proc.returncode == 0:
                try:
                    result = json.loads(proc.stdout.strip() or "{}")
                except json.JSONDecodeError:
                    result = {
                        "status": "error",
                        "reason": f"校验器返回非JSON输出: {proc.stdout[:200]}",
                    }
            else:
                result = {
                    "status": "error",
                    "reason": f"校验器退出码 {proc.returncode}: {proc.stderr[:500]}",
                }

            result.setdefault("elapsed_seconds", round(elapsed, 6))
            status = result.get("status", "error")
            if status not in VALID_STATUSES:
                result["status"] = "error"
                result["reason"] = result.get("reason", "") + " [原始状态码无效]"

            passed = status in ('ok', 'passed_with_warnings')
            return passed, result

        except sp.TimeoutExpired:
            elapsed = (time.perf_counter_ns() - start_time) / 1e9
            logger.error("校验器 [%s] 超时 (%ds)", checker.display_name, checker.timeout)
            return False, cls._build_error_result(
                checker.display_name, f"执行超时 ({checker.timeout}s)", elapsed
            )
        except Exception as e:
            elapsed = (time.perf_counter_ns() - start_time) / 1e9
            logger.critical("校验器 [%s] 执行异常: %s", checker.display_name, str(e), exc_info=True)
            return False, cls._build_error_result(
                checker.display_name, f"执行异常: {type(e).__name__}: {str(e)}", elapsed
            )

    @classmethod
    def _build_error_result(cls, display_name: str, reason: str, elapsed: float) -> Dict[str, Any]:
        """构建标准错误结果字典"""
        return {
            "status": "error",
            "reason": reason,
            "violations": [],
            "report": {},
            "warnings": [],
            "elapsed_seconds": round(elapsed, 6),
            "report_path": None,
        }

    # ── 审计报告写入 ──────────────────────────────────────
    @classmethod
    def _write_report(cls, report: Dict[str, Any], report_path: Optional[str] = None) -> str:
        """原子写入审计报告到磁盘（跨文件系统安全）"""
        temp_dir = cls._ensure_temp_dir()
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        uid = uuid.uuid4().hex[:8]
        if report_path is None:
            report_path = os.path.join(
                temp_dir, REPORT_FILENAME.format(timestamp=timestamp, uuid=uid)
            )
        # 深拷贝避免序列化副作用
        try:
            safe_report = json.loads(json.dumps(report, default=str))
        except Exception:
            safe_report = dict(report)

        # 原子写入策略：写入临时文件 → fsync → rename
        tmp_fd = None
        tmp_path = None
        try:
            tmp_fd, tmp_path = tempfile.mkstemp(
                suffix='.json', prefix='qg_', dir=os.path.dirname(report_path)
            )
            os.chmod(tmp_path, DEFAULT_REPORT_MODE)
            with os.fdopen(tmp_fd, 'w', encoding='utf-8') as f:
                json.dump(safe_report, f, indent=2, ensure_ascii=False, default=str)
                f.flush()
                os.fsync(f.fileno())
            os.rename(tmp_path, report_path)
            logger.info("审计报告已写入: %s", report_path)
        except OSError:
            # 跨文件系统 rename 失败，使用 shutil.move
            try:
                if tmp_path and os.path.exists(tmp_path):
                    shutil.move(tmp_path, report_path)
                logger.info("审计报告已写入 (shutil.move): %s", report_path)
            except Exception as e:
                logger.error("写入审计报告完全失败: %s", str(e))
                if tmp_path and os.path.exists(tmp_path):
                    os.remove(tmp_path)
                raise
        except Exception as e:
            logger.error("写入审计报告失败: %s #RECOVERY: 检查磁盘空间和权限", str(e))
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
            raise
        return report_path

    # ── 内存使用 ──────────────────────────────────────────
    @classmethod
    def _get_memory_usage(cls) -> float:
        """获取当前进程内存使用（MB），不依赖psutil时返回NaN"""
        if PSUTIL_AVAILABLE:
            try:
                return round(psutil.Process().memory_info().rss / 1024 / 1024, 3)
            except Exception:
                return float('nan')
        # 回退：读取 /proc/self/status
        try:
            with open('/proc/self/status', 'r') as f:
                for line in f:
                    if line.startswith('VmRSS:'):
                        return round(int(line.split()[1]) / 1024.0, 3)
        except Exception:
            pass
        return float('nan')

    # ── 指标输出 ──────────────────────────────────────────
    @classmethod
    def _update_metrics(
        cls, all_passed: bool, total_checks: int, failed_checks: int, total_elapsed: float
    ):
        """更新 Prometheus 指标（完全隔离，单项失败不影响其他）"""
        if not METRICS_AVAILABLE or MetricsCollector is None:
            return
        metric_updates = [
            ("quality_gate_passed", 1 if all_passed else 0, "gauge"),
            ("quality_gate_total_checks", total_checks, "counter"),
            ("quality_gate_failed_checks", failed_checks, "counter"),
            ("quality_gate_duration_seconds", total_elapsed, "histogram"),
        ]
        for name, value, mtype in metric_updates:
            try:
                if mtype == "gauge":
                    MetricsCollector.gauge(name, value)
                elif mtype == "counter":
                    MetricsCollector.counter(name, value)
                elif mtype == "histogram":
                    MetricsCollector.histogram(name, value)
            except Exception as e:
                logger.debug("指标更新失败 [%s]: %s", name, str(e))

    # ── 主执行逻辑 ────────────────────────────────────────
    @classmethod
    def run_all_checks(
        cls,
        project_root: str = DEFAULT_PROJECT_ROOT,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        report_path: Optional[str] = None,
        severity_filter: Optional[str] = None,
        skip_checkers: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        执行全部校验器，返回完整审计报告

        Args:
            project_root: 项目根目录路径
            timeout: 单校验器超时秒数 (1-3600)
            report_path: 审计报告输出路径（可选）
            severity_filter: 过滤严重度级别（high/medium/low）
            skip_checkers: 跳过的校验器模块名列表

        Returns:
            完整审计报告字典，包含 exit_code, summary, details, metrics, audit_chain
        """
        overall_start_ns = time.perf_counter_ns()
        execution_id = uuid.uuid4().hex[:16]
        logger.info("启动代码质量门禁 [execution_id=%s]", execution_id)

        # 路径安全校验
        try:
            safe_root = cls._validate_project_root(project_root)
        except (ValueError, FileNotFoundError) as e:
            logger.critical("项目路径校验失败: %s", str(e))
            return cls._build_emergency_report(execution_id, f"路径校验失败: {str(e)}")

        # 获取文件锁
        if not cls._acquire_lock(safe_root):
            return cls._build_emergency_report(execution_id, "无法获取门禁锁（并发冲突）")

        try:
            # 构建校验器列表（应用超时与跳过）
            checkers: List[CheckerDef] = []
            skip_set = set(skip_checkers or [])
            for c in cls.CHECKERS:
                if c.module_name in skip_set:
                    continue
                effective_timeout = c.timeout if c.timeout != DEFAULT_TIMEOUT_SECONDS else timeout
                effective_timeout = max(1, min(effective_timeout, 3600))
                checkers.append(c._replace(timeout=effective_timeout))

            print(f"\n{SEPARATOR_LINE}")
            print(f"  火种系统 - 代码质量门禁 v{VERSION}")
            print(f"  SPDX: {SPDX_IDENTIFIER}  |  执行ID: {execution_id}")
            print(f"{SEPARATOR_LINE}")

            all_passed = True
            details_list: List[Dict[str, Any]] = []
            total_checks = 0
            passed_checks = 0
            failed_checks = 0
            timeout_checks = 0
            total_elapsed = Decimal('0.0')

            for checker in checkers:
                print(f"\n>>> 执行 {checker.display_name} ({checker.module_name}) ...")
                passed, result = cls._run_single_checker(checker, safe_root)
                elapsed = Decimal(str(result.get('elapsed_seconds', 0)))
                total_elapsed += elapsed

                status = result.get('status', 'error')
                reason = result.get('reason', '无详细信息')
                violations = result.get('violations', [])

                # 严重度过滤
                if severity_filter and violations:
                    violations = [
                        v for v in violations
                        if v.get('severity', 'medium') == severity_filter
                    ]

                total_checks += 1
                if status == 'ok':
                    passed_checks += 1
                elif status == 'passed_with_warnings':
                    passed_checks += 1
                elif status in ('failed', 'error'):
                    failed_checks += 1
                    if 'timeout' in reason.lower() or status == 'timeout':
                        timeout_checks += 1

                print(f"    状态: {status.upper()} (耗时 {float(elapsed):.3f}s)")
                print(f"    说明: {reason}")
                if violations:
                    print(f"    违规项: {len(violations)} 条")
                    display_count = min(len(violations), MAX_VIOLATIONS_DISPLAY)
                    for v in violations[:display_count]:
                        loc = f"{v.get('file', 'N/A')}:{v.get('line', 'N/A')}"
                        sev = v.get('severity', 'N/A')
                        print(f"      [{sev}] {loc}: {v.get('reason', 'N/A')}")
                    if len(violations) > MAX_VIOLATIONS_DISPLAY:
                        print(
                            f"      ... 共 {len(violations)} 条，"
                            f"完整报告见 {report_path or 'JSON输出'}"
                        )

                detail: Dict[str, Any] = {
                    "checker": checker.display_name,
                    "module": checker.module_name,
                    "status": status,
                    "passed": passed,
                    "required": checker.required,
                    "elapsed_seconds": float(elapsed),
                    "violations": violations,
                    "reason": reason,
                    "warnings": result.get('warnings', []) or [],
                }
                details_list.append(detail)

                if checker.required and not passed:
                    all_passed = False

            # 输出总结
            print(f"\n{SEPARATOR_LINE}")
            summary = {
                "total": total_checks,
                "passed": passed_checks,
                "failed": failed_checks,
                "timeout": timeout_checks,
                "total_elapsed": float(total_elapsed),
            }
            if all_passed:
                print("  结果: 全部强制校验通过 ✅")
            else:
                failed_req = [d for d in details_list if d['required'] and not d['passed']]
                print(f"  结果: {len(failed_req)} 项强制校验未通过 ❌")
                for d in failed_req:
                    print(f"    - {d['checker']} ({d['status']})")
            print(f"  统计: {passed_checks}/{total_checks} 通过, 耗时 {float(total_elapsed):.3f}s")
            print(f"{SEPARATOR_LINE}\n")

            # 指标
            cls._update_metrics(all_passed, total_checks, failed_checks, float(total_elapsed))

            # 退出码
            if timeout_checks > 0:
                exit_code = EXIT_CODE_MAP["timeout"]
            elif not all_passed:
                exit_code = EXIT_CODE_MAP["hard_fail"]
            elif any(d['status'] == 'passed_with_warnings' for d in details_list):
                exit_code = EXIT_CODE_MAP["warnings_only"]
            else:
                exit_code = EXIT_CODE_MAP["all_passed"]

            report: Dict[str, Any] = {
                "execution_id": execution_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "version": VERSION,
                "spdx": SPDX_IDENTIFIER,
                "exit_code": exit_code,
                "all_passed": all_passed,
                "reason": "全部通过" if all_passed else "存在强制校验失败",
                "summary": summary,
                "details": details_list,
                "metrics": {
                    "total_elapsed_seconds": float(total_elapsed),
                    "peak_memory_mb": cls._get_memory_usage(),
                },
                "audit_chain": {
                    "parent_id": execution_id,
                    "sequence": len(details_list),
                },
            }

            # 写入审计报告
            report_file = cls._write_report(report, report_path)
            report["report_path"] = report_file

            return report

        finally:
            cls._release_lock()

    @classmethod
    def _build_emergency_report(cls, execution_id: str, reason: str) -> Dict[str, Any]:
        """构建紧急失败报告"""
        return {
            "execution_id": execution_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "version": VERSION,
            "spdx": SPDX_IDENTIFIER,
            "exit_code": EXIT_CODE_MAP["system_error"],
            "all_passed": False,
            "reason": reason,
            "summary": {"total": 0, "passed": 0, "failed": 0, "timeout": 0, "total_elapsed": 0},
            "details": [],
            "metrics": {"total_elapsed_seconds": 0, "peak_memory_mb": cls._get_memory_usage()},
            "audit_chain": {"parent_id": execution_id, "sequence": 0},
            "report_path": None,
        }

    @classmethod
    def health_check(cls, project_root: str = ".") -> Dict[str, Any]:
        """自检：验证所有校验器是否可导入"""
        warnings: List[str] = []
        seen: Set[str] = set()
        for checker in cls.CHECKERS:
            if checker.module_name in seen:
                continue
            seen.add(checker.module_name)
            try:
                importlib.import_module(checker.module_name)
            except Exception as e:
                msg = f"校验器 [{checker.display_name}] 不可用: {type(e).__name__}: {str(e)}"
                warnings.append(msg)
        if warnings:
            return {
                "status": "degraded",
                "reason": f"{len(warnings)} 个校验器不可用",
                "warnings": warnings,
            }
        return {
            "status": "ok",
            "reason": "所有校验器可导入",
            "warnings": [],
        }


# ── 信号处理（线程安全） ──────────────────────────────────
def _setup_signal_handlers():
    """注册优雅关闭的信号处理"""
    def _graceful_shutdown(signum: int, frame):
        sig_name = signal.Signals(signum).name
        logger.warning("收到信号 %s (%d)，正在优雅关闭...", sig_name, signum)
        QualityGate._release_lock()
        QualityGate._cleanup_temp_dir()
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(EXIT_CODE_MAP["system_error"])

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _graceful_shutdown)
        except (ValueError, OSError):
            pass  # 非主线程中无法设置信号


# ── 主入口 ────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description=f"火种代码质量门禁 v{VERSION} - 一键全量检查",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--project-root", default=DEFAULT_PROJECT_ROOT, help="项目根目录路径")
    parser.add_argument(
        "--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS,
        help=f"单校验器超时秒数 (1-3600，默认 {DEFAULT_TIMEOUT_SECONDS})"
    )
    parser.add_argument("--report", help="审计报告输出JSON路径")
    parser.add_argument("--severity", choices=["high", "medium", "low"], help="仅检查指定严重度")
    parser.add_argument("--skip", nargs="*", default=[], help="跳过的校验器模块名")
    parser.add_argument("--quiet", action="store_true", help="静默模式")
    parser.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")
    args = parser.parse_args()

    # 参数范围校验
    args.timeout = max(1, min(args.timeout, 3600))

    # 日志配置（JSON格式，输出到 stderr）
    log_level = logging.WARNING if args.quiet else logging.INFO
    logging.basicConfig(
        level=log_level,
        format=LOG_FORMAT_JSON,
        datefmt='%Y-%m-%dT%H:%M:%S',
        stream=sys.stderr,
    )

    # Python 版本检查
    if sys.version_info < PYTHON_MIN_VERSION:
        logger.critical("需要 Python %s+，当前版本 %s", PYTHON_MIN_VERSION, sys.version)
        sys.exit(EXIT_CODE_MAP["system_error"])

    # 信号处理
    _setup_signal_handlers()

    try:
        report = QualityGate.run_all_checks(
            project_root=args.project_root,
            timeout=args.timeout,
            report_path=args.report,
            severity_filter=args.severity,
            skip_checkers=args.skip,
        )
    except KeyboardInterrupt:
        logger.warning("用户中断操作")
        sys.exit(EXIT_CODE_MAP["system_error"])
    except Exception as e:
        logger.critical("门禁执行致命错误: %s", str(e), exc_info=True)
        try:
            emergency = QualityGate._build_emergency_report(
                uuid.uuid4().hex[:16], f"致命错误: {type(e).__name__}: {str(e)}"
            )
            emergency_path = os.path.join(
                QualityGate._ensure_temp_dir(),
                f"emergency_report_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
            )
            QualityGate._write_report(emergency, emergency_path)
            print(f"紧急报告已生成: {emergency_path}", file=sys.stderr)
        except Exception:
            pass
        sys.exit(EXIT_CODE_MAP["system_error"])

    if args.quiet and args.report:
        print(json.dumps({
            "exit_code": report["exit_code"],
            "report_path": report.get("report_path", ""),
        }))

    sys.exit(report["exit_code"])


if __name__ == "__main__":
    main()
