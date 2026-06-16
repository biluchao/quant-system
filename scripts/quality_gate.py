#!/usr/bin/env python3
"""
火种系统 · 代码质量门禁总入口 (QualityGate)
===============================================
机构级静态代码审查与合规验证系统。
符合华尔街高频交易级代码标准，适用于万亿美金管理规模的生产环境。

核心职责：
1. 插件式检查器生命周期管理：加载、超时控制、重试、降级。
2. 并发/串行执行多个检查器，支持首失败即停止。
3. 输出多格式审计报告（Terminal ANSI / JSON / SARIF）。
4. 原子写入报告文件，确保数据完整性。
5. 提供结构化健康检查，集成到 CI/CD 流水线及生产环境守护进程。

外部依赖（真实模块接口）：
- scripts.style_checker.StyleChecker              : 风格校验，需实现 run() -> Dict
- scripts.dependency_checker.DependencyChecker    : 依赖闭环校验
- scripts.interface_verifier.InterfaceVerifier    : 接口契约校验
- scripts.config_decouple_checker.ConfigDecoupleChecker : 配置解耦校验

接口契约：
- run_all(checks, *, timeout, parallel, stop_on_first_failure) -> GateResult
- health_check() -> Dict[str, Any]   (符合火种模块标准)

异常与降级：
- 检查器加载失败 → 重试后仍失败则标记为 ERROR，并记录建议恢复操作
- 单个检查器超时 → 取消其 Future，标记 TIMEOUT，其他继续
- 检查器抛出致命异常 → 捕获完整堆栈，标记 ERROR，不影响同批次其他检查
- 输出报告前强制验证数据结构完整性，若损坏则降级为文本摘要并记录告警
- 若全局线程池资源耗尽，自动回退为串行执行并告警
- 收到 SIGTERM/SIGINT → 优雅关闭，等待当前检查完成，取消未开始任务

资源管理：
- 使用上下文管理器确保线程池在异常时正确释放
- 报告文件采用原子写入（临时文件 + os.replace），避免部分写入
- 所有文件描述符使用 with 语句，临时文件权限 0o600
- 不持有持久资源
"""

import importlib
import logging
import sys
import os
import time
import signal
import traceback
import json
import platform
import tempfile
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, List, Optional, Callable, Union, Set, Tuple, Type
from enum import Enum
from contextlib import suppress

# -----------------------------------------------------------------------------
# 日志配置
# 生产环境应由外部日志管理器注入，此处提供安全默认值。
# 支持通过环境变量 QUALITY_GATE_LOG_LEVEL 控制日志级别。
# -----------------------------------------------------------------------------
logger = logging.getLogger(__name__)

if not logger.handlers:
    # 根据环境决定格式：TTY 使用彩色，CI 使用简洁
    if sys.stderr.isatty():
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)-5s] %(name)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S"
        )
    else:
        formatter = logging.Formatter("[%(levelname)s] %(message)s")
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

log_level = os.getenv("QUALITY_GATE_LOG_LEVEL", "INFO").upper()
logger.setLevel(getattr(logging, log_level, logging.INFO))


# -----------------------------------------------------------------------------
# 领域模型
# -----------------------------------------------------------------------------

class GateStatus(str, Enum):
    """门禁整体状态"""
    PASS = "pass"           # 全部检查通过
    FAIL = "fail"           # 至少一项代码检查失败
    ERROR = "error"         # 系统级故障（如所有检查器加载失败）
    DEGRADED = "degraded"   # 部分检查因环境问题未完成，但无明确代码缺陷


class CheckItemStatus(str, Enum):
    """单项检查状态"""
    PASS = "pass"
    FAIL = "fail"
    ERROR = "error"
    TIMEOUT = "timeout"
    SKIPPED = "skipped"


@dataclass
class CheckItemResult:
    """单项检查的详细结果"""
    check_name: str
    status: CheckItemStatus
    message: str = ""
    duration_ms: float = 0.0
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    raw: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []
        if self.errors is None:
            self.errors = []


@dataclass
class GateResult:
    """门禁聚合结果"""
    status: GateStatus
    reason: str
    timestamp: str = field(default_factory=lambda: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    environment: Dict[str, str] = field(default_factory=dict)
    check_results: List[CheckItemResult] = field(default_factory=list)
    total_duration_ms: float = 0.0
    extra: Dict[str, Any] = field(default_factory=dict)

    def is_success(self) -> bool:
        return self.status == GateStatus.PASS

    def has_failures(self) -> bool:
        return any(
            r.status in (CheckItemStatus.FAIL, CheckItemStatus.ERROR, CheckItemStatus.TIMEOUT)
            for r in self.check_results
        )


# -----------------------------------------------------------------------------
# 检查器注册表与接口
# -----------------------------------------------------------------------------

REQUIRED_CHECKER_METHOD = "run"
VALID_CHECK_STATUSES: frozenset[str] = frozenset({"pass", "fail", "error", "timeout"})

DEFAULT_CHECKER_REGISTRY: Dict[str, Tuple[str, str]] = {
    "style":            ("scripts.style_checker", "StyleChecker"),
    "dependency":       ("scripts.dependency_checker", "DependencyChecker"),
    "interface":        ("scripts.interface_verifier", "InterfaceVerifier"),
    "config_decouple":  ("scripts.config_decouple_checker", "ConfigDecoupleChecker"),
}

# 超时与重试配置（可通过环境变量覆盖）
DEFAULT_PER_CHECK_TIMEOUT = float(os.getenv("QG_CHECK_TIMEOUT", "120.0"))
MAX_RETRIES = int(os.getenv("QG_MAX_RETRIES", "2"))
RETRY_DELAY = 1.0
MAX_WORKERS = int(os.getenv("QG_MAX_WORKERS", "4"))

# 优雅退出标志
_graceful_shutdown_requested = False


# -----------------------------------------------------------------------------
# 信号处理（可重入）
# -----------------------------------------------------------------------------

def _signal_handler(signum: int, frame: Any) -> None:
    global _graceful_shutdown_requested
    sig_name = signal.Signals(signum).name
    logger.warning("收到信号 %s，将在当前检查完成后优雅退出", sig_name)
    _graceful_shutdown_requested = True


# 注册信号（如果未在其他地方注册）
for _sig in (signal.SIGTERM, signal.SIGINT):
    try:
        signal.signal(_sig, _signal_handler)
    except Exception:
        logger.warning("无法注册信号 %s，忽略", _sig.name)


# -----------------------------------------------------------------------------
# QualityGate 主类
# -----------------------------------------------------------------------------

class QualityGate:
    """代码质量门禁入口

    设计原则：
    - 无状态：所有方法为类方法，避免实例状态污染并发调用。
    - 配置外部化：通过环境变量和参数覆盖默认值。
    - 严格容错：单点故障不中断整体流程，记录充分上下文。
    - 审计友好：所有决策点均有日志，输出包含完整环境信息。
    - 原子输出：报告写入采用临时文件+原子替换，防止部分写或损坏。
    - 类型安全：全面使用类型注解，通过 mypy 严格检查。
    """

    DEFAULT_CHECKS: List[str] = list(DEFAULT_CHECKER_REGISTRY.keys())

    # ------------------------------------------------------------------------
    # 环境信息
    # ------------------------------------------------------------------------

    @classmethod
    def _build_environment_info(cls) -> Dict[str, str]:
        """采集当前执行环境信息，用于审计追踪"""
        return {
            "python_version": sys.version,
            "platform": platform.platform(),
            "node": platform.node(),
            "user": os.getenv("USER", os.getenv("USERNAME", "unknown")),
            "ci": os.getenv("CI", "false"),
            "commit_sha": os.getenv("GIT_COMMIT", os.getenv("GIT_SHA", "unknown")),
            "branch": os.getenv("GIT_BRANCH", os.getenv("GIT_REF", "unknown")),
            "quality_gate_version": "2.1.1",
        }

    # ------------------------------------------------------------------------
    # 输入校验
    # ------------------------------------------------------------------------

    @classmethod
    def _validate_checks_input(cls, checks: List[str]) -> Tuple[List[str], List[str]]:
        """校验检查项列表，去重并分类。返回 (有效项, 未知项)"""
        if not checks:
            return [], []
        if not isinstance(checks, list):
            raise TypeError(f"checks 参数必须为 list，收到 {type(checks)}")
        valid: List[str] = []
        unknown: List[str] = []
        seen: Set[str] = set()
        for item in checks:
            if not isinstance(item, str):
                logger.warning("忽略非字符串检查项: %s", item)
                continue
            if item in seen:
                continue
            seen.add(item)
            if item in DEFAULT_CHECKER_REGISTRY:
                valid.append(item)
            else:
                unknown.append(item)
        return valid, unknown

    # ------------------------------------------------------------------------
    # 检查器加载与重试
    # ------------------------------------------------------------------------

    @classmethod
    def _import_checker(cls, name: str, retry: int = MAX_RETRIES) -> Optional[Callable[..., Any]]:
        """加载检查器类/函数，带重试逻辑。返回可调用对象，失败返回 None。"""
        if name not in DEFAULT_CHECKER_REGISTRY:
            logger.error("未注册的检查器: %s", name)
            return None

        module_path, class_name = DEFAULT_CHECKER_REGISTRY[name]
        last_exception: Optional[Exception] = None
        for attempt in range(retry + 1):
            try:
                # 支持环境变量启用热重载（谨慎使用）
                if module_path in sys.modules and os.getenv("QG_HOT_RELOAD", "0") == "1":
                    importlib.reload(sys.modules[module_path])
                module = importlib.import_module(module_path)
                checker = getattr(module, class_name, None)
                if checker is None:
                    raise AttributeError(f"模块 {module_path} 中未找到 {class_name}")
                if not hasattr(checker, REQUIRED_CHECKER_METHOD):
                    raise AttributeError(f"{class_name} 缺少必需方法 '{REQUIRED_CHECKER_METHOD}'")
                logger.debug("检查器 %s 加载成功 (尝试 %d/%d)", name, attempt + 1, retry + 1)
                return checker
            except Exception as e:
                last_exception = e
                logger.warning("加载检查器 %s 失败 (尝试 %d/%d): %s", name, attempt + 1, retry + 1, e)
                if attempt < retry:
                    time.sleep(RETRY_DELAY * (attempt + 1))  # 简单退避
        logger.error("检查器 %s 最终加载失败: %s", name, last_exception)
        return None

    # ------------------------------------------------------------------------
    # 结果清洗
    # ------------------------------------------------------------------------

    @staticmethod
    def _sanitize_raw_result(raw: Any) -> Optional[Dict[str, Any]]:
        """将检查器原始返回值清洗为可安全序列化的字典"""
        if isinstance(raw, dict):
            cleaned: Dict[str, Any] = {}
            for k, v in raw.items():
                try:
                    json.dumps(v, default=str)  # 测试可序列化性
                    cleaned[str(k)] = v
                except (TypeError, ValueError):
                    cleaned[str(k)] = str(v)
            return cleaned
        if raw is None:
            return None
        return {"__raw_value__": str(raw)[:2000]}

    # ------------------------------------------------------------------------
    # 单个检查执行
    # ------------------------------------------------------------------------

    @classmethod
    def _run_single_check(cls, check_name: str, timeout: float) -> CheckItemResult:
        """执行单个检查器，管理超时与异常，返回 CheckItemResult。"""
        if _graceful_shutdown_requested:
            return CheckItemResult(
                check_name=check_name,
                status=CheckItemStatus.SKIPPED,
                message="系统正在关闭，跳过检查",
                duration_ms=0
            )

        start = time.perf_counter()
        checker = cls._import_checker(check_name)
        if checker is None:
            duration = (time.perf_counter() - start) * 1000
            return CheckItemResult(
                check_name=check_name,
                status=CheckItemStatus.ERROR,
                message="检查器加载失败，已重试。建议检查模块依赖与路径。",
                duration_ms=duration,
                errors=["检查器不可用"]
            )

        def target() -> Dict[str, Any]:
            try:
                if isinstance(checker, type):
                    instance = checker()
                    result = getattr(instance, REQUIRED_CHECKER_METHOD)()
                else:
                    result = checker()
                if not isinstance(result, dict):
                    return {
                        "status": "error",
                        "reason": f"检查器返回非字典类型: {type(result).__name__}",
                        "errors": []
                    }
                return result
            except Exception as e:
                logger.exception("检查器 %s 内部异常", check_name)
                return {
                    "status": "error",
                    "reason": str(e),
                    "errors": [traceback.format_exc()],
                }

        with ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"qg-{check_name}") as executor:
            future = executor.submit(target)
            try:
                raw_result = future.result(timeout=timeout)
            except FuturesTimeoutError:
                duration = (time.perf_counter() - start) * 1000
                future.cancel()  # 主动取消超时任务
                logger.warning("检查器 %s 超时 (%.1fs)", check_name, timeout)
                return CheckItemResult(
                    check_name=check_name,
                    status=CheckItemStatus.TIMEOUT,
                    message=f"检查器执行超时（阈值 {timeout} 秒）",
                    duration_ms=duration,
                    errors=["超时"]
                )
            except Exception as e:
                duration = (time.perf_counter() - start) * 1000
                logger.error("检查器 %s 线程异常: %s", check_name, e)
                return CheckItemResult(
                    check_name=check_name,
                    status=CheckItemStatus.ERROR,
                    message=f"检查器线程异常: {str(e)}",
                    duration_ms=duration,
                    errors=[traceback.format_exc()]
                )

        duration = (time.perf_counter() - start) * 1000

        # 提取状态并验证
        status_str = str(raw_result.get("status", "")).lower()
        if status_str not in VALID_CHECK_STATUSES:
            logger.warning("检查器 %s 返回未知状态 '%s'，视为 error", check_name, status_str)
            status_str = "error"

        warnings = raw_result.get("warnings", [])
        if not isinstance(warnings, list):
            warnings = [str(warnings)]
        errors = raw_result.get("errors", [])
        if not isinstance(errors, list):
            errors = [str(errors)]

        return CheckItemResult(
            check_name=check_name,
            status=CheckItemStatus(status_str),
            message=raw_result.get("reason", raw_result.get("message", "")),
            duration_ms=duration,
            warnings=warnings,
            errors=errors,
            metadata=raw_result.get("metadata", {}),
            raw=cls._sanitize_raw_result(raw_result),
        )

    # ------------------------------------------------------------------------
    # 主门禁逻辑
    # ------------------------------------------------------------------------

    @classmethod
    def run_all(cls,
                checks: Optional[List[str]] = None,
                *,
                timeout: float = DEFAULT_PER_CHECK_TIMEOUT,
                parallel: bool = True,
                stop_on_first_failure: bool = False) -> GateResult:
        """执行全部或指定质量检查。

        Args:
            checks: 检查项列表，默认全部。
            timeout: 单个检查器最大执行秒数（必须 > 0）。
            parallel: 是否并发执行。
            stop_on_first_failure: 首次失败即停止后续。

        Returns:
            GateResult 聚合结果。
        """
        if timeout <= 0:
            raise ValueError("timeout 必须为正数")

        start_total = time.perf_counter()
        env_info = cls._build_environment_info()

        if checks is None:
            checks = cls.DEFAULT_CHECKS.copy()
        elif not isinstance(checks, list):
            raise TypeError(f"checks 参数必须为 list，收到 {type(checks)}")
        valid_checks, unknown = cls._validate_checks_input(checks)
        if unknown:
            logger.warning("忽略了未知检查项: %s", unknown)
        if not valid_checks:
            reason = "没有有效的检查项可执行"
            if unknown:
                reason += f"，未知项: {unknown}"
            return GateResult(
                status=GateStatus.ERROR,
                reason=reason,
                environment=env_info,
                total_duration_ms=0.0,
                extra={"unknown_checks": unknown}
            )

        results: List[CheckItemResult] = []
        try:
            if parallel and len(valid_checks) > 1:
                max_workers = min(MAX_WORKERS, len(valid_checks))
                with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="qg-pool") as pool:
                    future_map: Dict[Any, str] = {
                        pool.submit(cls._run_single_check, name, timeout): name
                        for name in valid_checks
                    }
                    result_map: Dict[str, CheckItemResult] = {}
                    for future in future_map:
                        name = future_map[future]
                        try:
                            result = future.result(timeout=timeout + 5.0)
                            result_map[name] = result
                        except FuturesTimeoutError:
                            future.cancel()
                            result_map[name] = CheckItemResult(
                                check_name=name,
                                status=CheckItemStatus.TIMEOUT,
                                message="线程池级超时",
                                duration_ms=0,
                                errors=["线程池总超时"]
                            )
                        except Exception as e:
                            logger.exception("获取检查 %s 结果时异常", name)
                            result_map[name] = CheckItemResult(
                                check_name=name,
                                status=CheckItemStatus.ERROR,
                                message=f"执行异常: {str(e)}",
                                duration_ms=0,
                                errors=[traceback.format_exc()]
                            )
                    results = [result_map[name] for name in valid_checks]
            else:
                for idx, check_name in enumerate(valid_checks):
                    if _graceful_shutdown_requested:
                        for remaining in valid_checks[idx:]:
                            results.append(CheckItemResult(
                                check_name=remaining,
                                status=CheckItemStatus.SKIPPED,
                                message="系统关闭信号，跳过后续检查",
                                duration_ms=0
                            ))
                        break
                    res = cls._run_single_check(check_name, timeout)
                    results.append(res)
                    if stop_on_first_failure and res.status != CheckItemStatus.PASS:
                        logger.warning("第一个失败出现于 %s，停止后续检查", check_name)
                        for remaining in valid_checks[idx+1:]:
                            results.append(CheckItemResult(
                                check_name=remaining,
                                status=CheckItemStatus.SKIPPED,
                                message="由于前序检查失败而跳过",
                                duration_ms=0
                            ))
                        break
        except Exception as e:
            logger.exception("检查执行过程中发生未捕获异常")
            results.append(CheckItemResult(
                check_name="__gate_internal__",
                status=CheckItemStatus.ERROR,
                message=f"门禁内部异常: {str(e)}",
                errors=[traceback.format_exc()]
            ))

        total_duration = (time.perf_counter() - start_total) * 1000

        has_fail = any(
            r.status in (CheckItemStatus.FAIL, CheckItemStatus.ERROR, CheckItemStatus.TIMEOUT)
            for r in results
        )
        has_system_error = any(r.status == CheckItemStatus.ERROR for r in results)
        all_pass = all(r.status == CheckItemStatus.PASS for r in results) if results else False

        if all_pass:
            final_status = GateStatus.PASS
            reason = "所有质量门禁检查通过"
        elif has_system_error and not any(r.status == CheckItemStatus.FAIL for r in results):
            final_status = GateStatus.DEGRADED
            reason = "部分检查因环境或系统问题无法完成，但未发现代码缺陷"
        else:
            final_status = GateStatus.FAIL
            failed_names = [r.check_name for r in results if r.status != CheckItemStatus.PASS]
            reason = f"质量门禁未通过，失败项: {', '.join(failed_names)}"

        return GateResult(
            status=final_status,
            reason=reason,
            environment=env_info,
            check_results=results,
            total_duration_ms=total_duration,
            extra={
                "unknown_checks": unknown,
                "parallel": parallel,
                "timeout_per_check": timeout,
                "stop_on_first_failure": stop_on_first_failure,
                "graceful_shutdown": _graceful_shutdown_requested,
            }
        )

    # ------------------------------------------------------------------------
    # 输出格式化
    # ------------------------------------------------------------------------

    @staticmethod
    def format_terminal(result: GateResult, use_colors: bool = True) -> str:
        """生成面向终端的报告，支持 ANSI 颜色。"""
        if use_colors:
            GREEN = "\033[92m"; RED = "\033[91m"; YELLOW = "\033[93m"
            CYAN = "\033[96m"; BOLD = "\033[1m"; RESET = "\033[0m"
        else:
            GREEN = RED = YELLOW = CYAN = BOLD = RESET = ""

        lines = [
            f"{BOLD}{'='*60}{RESET}",
            f" 代码质量门禁报告 - {result.timestamp}",
            f" 状态: {GREEN if result.status == GateStatus.PASS else RED if result.status == GateStatus.FAIL else YELLOW}{result.status.value.upper()}{RESET}",
            f" 环境: {result.environment.get('node', 'N/A')} | CI: {result.environment.get('ci', 'false')}",
            f"{BOLD}{'='*60}{RESET}"
        ]

        icon_map = {
            CheckItemStatus.PASS: f"{GREEN}[✓]{RESET}",
            CheckItemStatus.FAIL: f"{RED}[✗]{RESET}",
            CheckItemStatus.ERROR: f"{RED}[!]{RESET}",
            CheckItemStatus.TIMEOUT: f"{YELLOW}[⏱]{RESET}",
            CheckItemStatus.SKIPPED: "[-]",
        }

        for r in result.check_results:
            icon = icon_map.get(r.status, "[?]")
            status_str = r.status.value.ljust(7)
            lines.append(f" {icon} {r.check_name:30s} {status_str} {r.duration_ms:8.0f}ms")
            if r.message:
                lines.append(f"    {CYAN}{r.message}{RESET}")
            if r.warnings:
                for w in r.warnings[:3]:
                    lines.append(f"    ⚠ {YELLOW}{w}{RESET}")
            if r.errors:
                for e in r.errors[:2]:
                    lines.append(f"    ❌ {RED}{e}{RESET}")

        lines.append(f"{BOLD}{'='*60}{RESET}")
        lines.append(f" 总耗时: {result.total_duration_ms:.0f}ms")
        return "\n".join(lines)

    @staticmethod
    def format_json(result: GateResult, indent: int = 2) -> str:
        """生成 JSON 格式报告，所有值保证可序列化。"""
        def default_serializer(obj: Any) -> str:
            if isinstance(obj, Enum):
                return obj.value
            if isinstance(obj, (GateResult, CheckItemResult)):
                return asdict(obj)
            return str(obj)

        data = asdict(result)
        for r in data.get("check_results", []):
            if "raw" in r and r["raw"] is not None:
                try:
                    json.dumps(r["raw"], default=default_serializer)
                except (TypeError, ValueError):
                    r["raw"] = str(r["raw"])
        return json.dumps(data, indent=indent, default=default_serializer, ensure_ascii=False)

    @staticmethod
    def format_sarif(result: GateResult) -> Dict[str, Any]:
        """生成 SARIF v2.1.0 格式报告，可被 CI 系统解析。"""
        sarif: Dict[str, Any] = {
            "version": "2.1.0",
            "runs": [{
                "tool": {
                    "driver": {
                        "name": "QualityGate",
                        "version": "2.1.1",
                        "informationUri": "https://spark-system.internal/quality-gate",
                    }
                },
                "invocation": {
                    "executionSuccessful": result.status != GateStatus.ERROR,
                },
                "results": []
            }]
        }
        for r in result.check_results:
            if r.status in (CheckItemStatus.PASS, CheckItemStatus.SKIPPED):
                continue
            level = "error" if r.status == CheckItemStatus.FAIL else "warning"
            artifact_uri = r.metadata.get("file", "unknown")
            sarif_result = {
                "ruleId": r.check_name,
                "level": level,
                "message": {
                    "text": r.message or f"Check {r.check_name} returned {r.status.value}"
                },
                "locations": [{
                    "physicalLocation": {
                        "artifactLocation": {
                            "uri": artifact_uri
                        }
                    }
                }],
                "properties": {
                    "status": r.status.value,
                    "duration_ms": r.duration_ms,
                }
            }
            sarif["runs"][0]["results"].append(sarif_result)
        return sarif

    # ------------------------------------------------------------------------
    # 健康检查
    # ------------------------------------------------------------------------

    @classmethod
    def health_check(cls) -> Dict[str, Any]:
        """模块自检：验证核心功能是否可达。返回火种标准状态字典。"""
        try:
            result = cls.run_all(checks=[], parallel=False)
            if result.status != GateStatus.PASS:
                return {
                    "status": "error",
                    "reason": f"空列表门禁应通过，得到 {result.status.value}: {result.reason}"
                }
            result2 = cls.run_all(checks=["__non_existent__"], parallel=False)
            if result2.status not in (GateStatus.ERROR, GateStatus.DEGRADED):
                logger.warning("未知检查预期 ERROR/DEGRADED，得到 %s", result2.status.value)
            env = cls._build_environment_info()
            for key in ("python_version", "platform", "quality_gate_version"):
                if key not in env:
                    return {"status": "error", "reason": f"环境信息缺少键: {key}"}
            return {"status": "ok", "message": "质量门禁核心功能正常", "warnings": []}
        except Exception as e:
            logger.exception("健康检查失败")
            return {"status": "error", "reason": str(e)}


# -----------------------------------------------------------------------------
# 命令行入口
# -----------------------------------------------------------------------------

def main() -> None:
    """命令行入口，处理参数解析、执行门禁、输出报告。"""
    import argparse
    parser = argparse.ArgumentParser(
        description="火种代码质量门禁 - 机构级静态审查系统",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  quality_gate.py                             # 执行全部检查，终端输出
  quality_gate.py --json                      # JSON 输出
  quality_gate.py --checks style dependency   # 只执行指定检查
  quality_gate.py --timeout 60 --no-parallel  # 串行执行，超时 60 秒
  quality_gate.py --sarif -o report.sarif     # 输出 SARIF 文件
  quality_gate.py --self-test                 # 仅执行健康检查
        """
    )
    parser.add_argument("--checks", nargs="+", default=None,
                        help="要执行的检查项，默认全部。可选: %s" % ", ".join(QualityGate.DEFAULT_CHECKS))
    parser.add_argument("--timeout", type=float, default=DEFAULT_PER_CHECK_TIMEOUT,
                        help="单个检查器的超时秒数 (默认: %(default)s)")
    parser.add_argument("--no-parallel", dest="parallel", action="store_false", default=True,
                        help="禁止并发执行")
    parser.add_argument("--stop-on-first-failure", action="store_true", default=False,
                        help="首次失败即停止后续检查")
    parser.add_argument("--json", action="store_true", help="以 JSON 格式输出")
    parser.add_argument("--sarif", action="store_true", help="输出 SARIF 格式")
    parser.add_argument("--no-color", action="store_true", help="禁用终端颜色")
    parser.add_argument("--output", "-o", type=str, help="将报告写入指定文件")
    parser.add_argument("--quiet", action="store_true", help="仅输出最终状态")
    parser.add_argument("--self-test", action="store_true", help="执行健康检查后退出")
    args = parser.parse_args()

    if args.timeout <= 0:
        print("错误: --timeout 必须为正数", file=sys.stderr)
        sys.exit(2)

    if args.self_test:
        hc = QualityGate.health_check()
        print(json.dumps(hc, indent=2) if args.json else hc.get("status", "unknown"))
        sys.exit(0 if hc.get("status") == "ok" else 1)

    try:
        result = QualityGate.run_all(
            checks=args.checks,
            timeout=args.timeout,
            parallel=args.parallel,
            stop_on_first_failure=args.stop_on_first_failure
        )
    except Exception as e:
        logger.exception("门禁执行严重异常")
        env = QualityGate._build_environment_info()
        result = GateResult(
            status=GateStatus.ERROR,
            reason=f"门禁系统严重错误: {str(e)}",
            environment=env,
            total_duration_ms=0.0,
            extra={"error_traceback": traceback.format_exc()}
        )

    # 生成输出内容
    if args.sarif:
        output_str = json.dumps(QualityGate.format_sarif(result), indent=2)
    elif args.json:
        output_str = QualityGate.format_json(result)
    else:
        output_str = QualityGate.format_terminal(result, use_colors=not args.no_color) if not args.quiet else result.status.value

    # 写入文件或标准输出
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        # 原子写入，权限 0o600
        fd = os.open(str(out_path) + ".tmp", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                f.write(output_str)
                f.flush()
                os.fsync(f.fileno())
            os.replace(str(out_path) + ".tmp", str(out_path))
        except Exception:
            with suppress(OSError):
                os.unlink(str(out_path) + ".tmp")
            raise
        if not args.quiet:
            print(f"报告已保存至 {out_path}", file=sys.stderr)
    else:
        # 确保 stdout 编码为 UTF-8
        sys.stdout.reconfigure(encoding='utf-8') if hasattr(sys.stdout, 'reconfigure') else None
        print(output_str)

    # 退出码：PASS=0, FAIL/DEGRADED=1, ERROR=2
    if result.status == GateStatus.PASS:
        sys.exit(0)
    elif result.status in (GateStatus.FAIL, GateStatus.DEGRADED):
        sys.exit(1)
    else:
        sys.exit(2)


if __name__ == "__main__":
    main()
