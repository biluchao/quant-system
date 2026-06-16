#!/usr/bin/env python3
"""
火种系统 · 模块骨架生成器 (generate_module.py)

核心职责：
1. 根据命令行参数安全地生成符合火种工程规范的Python模块骨架文件。
2. 生成的模块包含完整的文档头、类型注解、生命周期方法、健康检查、自定义异常以及标准化返回值结构。
3. 支持原子写入、路径安全校验、PascalCase强制、元信息注入（作者、版本、时间戳）。

外部依赖：无（仅标准库）
接口契约：
- main() -> None
  解析命令行参数，调用 generate_module()，输出结果到 stdout，错误到 stderr。
- generate_module(path, name, desc, ...) -> Path
  返回生成的文件的 Path 对象，失败抛出异常。

异常与降级：
- 参数验证失败 → 抛出 argparse.ArgumentError 或 ValueError，主程序捕获并打印用法。
- 路径不安全（目录遍历）→ 抛出 SecurityError。
- 磁盘空间不足或无写权限 → 抛出 IOError，并在清理临时文件后退出。
- 文件已存在且未指定 --force → 抛出 FileExistsError。

资源管理：
- 使用临时文件 + 原子重命名保证写入完整性。
- 生成的文件权限设置为 0o644。
- 所有文件句柄使用 with 语句管理。

审计与合规：
- 每次生成记录结构化日志（时间戳、用户、参数、生成路径）。
- 生成的模块头包含生成器版本和生成时间，便于 SBOM 追溯。
"""

import argparse
import logging
import os
import re
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional

# 配置脚本自身的日志（仅输出到控制台，不干扰模块）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("generate_module")

# 脚本版本，用于注入生成的模块头
SCRIPT_VERSION = "2.0.0"

# 允许生成的根目录（防止路径遍历）
ALLOWED_ROOTS = [
    Path("core").resolve(),
    Path("scripts").resolve(),
    Path("tests").resolve(),
    Path("ai_sandbox").resolve(),
    Path("gateway").resolve(),
]

# 模板字符串（使用双花括号转义，单花括号为占位符）
MODULE_TEMPLATE = '''# -*- coding: utf-8 -*-
"""
火种系统 · {desc} ({name})

核心职责：
1. {{职责一}}
2. {{职责二}}

外部依赖（真实模块接口）：
- {{依赖模块路径}}.{{类名}} : {{用途说明}}

接口契约：
- {{方法名}}({{参数}}) -> {return_type}
- 输出字典固定包含 "status" (str), "reason" (str), "warnings" (List[str])

异常与降级：
- {{异常处理策略，请替换为具体异常类}}

资源管理：
- {{资源释放说明，如实现 __enter__/__exit__ 或 startup/shutdown}}

生成信息：
- 生成器版本: {generator_version}
- 生成时间: {timestamp}
- 作者: {author}
"""

from __future__ import annotations

import logging
import sys
import warnings
from typing import Dict, Any, List, Optional, TypedDict

logger = logging.getLogger(__name__)


class ModuleResult(TypedDict, total=False):
    """模块标准化返回值"""
    status: str
    reason: str
    warnings: List[str]
    result: Any


class {name}Error(Exception):
    """模块自定义异常基类"""
    pass


class ConfigurationError({name}Error):
    """配置错误"""
    pass


class {name}:
    """{desc}

    生命周期：
    1. __init__ 或 startup() 进行初始化
    2. 调用业务方法
    3. shutdown() 释放资源
    """
    # ── 类常量区（默认配置，附带单位与取值范围注释）──────────────
    DEFAULT_THRESHOLD: float = 0.5        # 默认阈值，无量纲，取值范围 [0.0, 1.0]

    # ── 实例属性（在 __init__ 中初始化）─────────────────────────
    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self._config = config or {{}}
        self._is_initialized = False

    def startup(self) -> None:
        """初始化资源（建立连接、加载模型等）"""
        self._is_initialized = True
        logger.info("{name} 初始化完成")

    def shutdown(self) -> None:
        """释放资源（关闭连接、保存状态等）"""
        self._is_initialized = False
        logger.info("{name} 已安全关闭")

    @staticmethod
    def _validate_threshold(value: float) -> float:
        """阈值校验与裁剪"""
        if not isinstance(value, (int, float)):
            raise TypeError(f"阈值必须为数字，收到 {{type(value).__name__}}")
        if not (0.0 <= value <= 1.0):
            warnings.warn(f"阈值 {{value}} 超出 [0.0, 1.0]，已裁剪")
        return max(0.0, min(1.0, value))

    def public_method(self, param: float) -> ModuleResult:
        """公共接口方法（示例）

        参数:
            param: 输入参数，范围 [0.0, 1.0]

        返回:
            ModuleResult 字典，包含 status, reason, result, warnings
        """
        if not self._is_initialized:
            raise RuntimeError("模块未初始化，请先调用 startup()")
        param = self._validate_threshold(param)
        # 业务逻辑示例（请替换为真实算法）
        result = param * 2.0
        return ModuleResult(
            status="ok",
            result=result,
            reason=f"计算完成: {{param}} * 2 = {{result}}",
            warnings=[]
        )

    @classmethod
    def health_check(cls) -> ModuleResult:
        """模块自检（必须真实执行核心功能的最小冒烟测试）"""
        try:
            # 请在此处添加真实的功能验证，例如检查常量有效性、依赖服务可达等
            instance = cls()
            instance.startup()
            # 执行一个简单测试
            test_result = instance.public_method(cls.DEFAULT_THRESHOLD)
            instance.shutdown()
            if test_result.get("status") == "ok":
                return ModuleResult(status="ok", reason="所有自检测试通过", warnings=[])
            else:
                return ModuleResult(status="error", reason="功能测试返回异常", warnings=[])
        except Exception as e:
            logger.exception("健康检查失败")
            return ModuleResult(status="error", reason=str(e), warnings=[str(e)])

    def __repr__(self) -> str:
        return f"{name}(threshold={{self.DEFAULT_THRESHOLD}})"


# 模块独立自检入口
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    result = {name}.health_check()
    print(result)
    sys.exit(0 if result["status"] == "ok" else 1)
'''


class SecurityError(Exception):
    """路径安全违规"""
    pass


def validate_path_safety(target: Path) -> Path:
    """确保目标路径在允许的根目录内，防范目录遍历攻击"""
    resolved = target.resolve()
    for root in ALLOWED_ROOTS:
        # 允许文件在根目录的子目录下
        try:
            resolved.relative_to(root)
            return resolved
        except ValueError:
            continue
    raise SecurityError(
        f"路径 '{target}' 不在允许的生成范围内。允许的根目录: {[str(r) for r in ALLOWED_ROOTS]}"
    )


def validate_class_name(name: str) -> None:
    """验证类名符合 PascalCase 且为合法标识符"""
    if not name.isidentifier():
        raise ValueError(f"'{name}' 不是合法的 Python 标识符")
    if not re.match(r'^[A-Z][a-zA-Z0-9]*$', name):
        raise ValueError(f"类名 '{name}' 不符合 PascalCase 规范（首字母大写，仅字母数字）")


def generate_module(
    path: str,
    name: str,
    desc: str,
    author: str = "Quant Team",
    force: bool = False,
    dry_run: bool = False,
) -> Path:
    """
    生成模块骨架文件。
    返回生成的 Path 对象。
    """
    # 参数验证
    validate_class_name(name)
    target = Path(path).resolve()
    if target.suffix != ".py":
        raise ValueError(f"文件后缀必须为 .py，收到 '{target.suffix}'")

    # 安全路径检查
    safe_path = validate_path_safety(target)

    # 内容准备
    content = MODULE_TEMPLATE.format(
        desc=desc,
        name=name,
        return_type="ModuleResult",
        generator_version=SCRIPT_VERSION,
        timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        author=author,
    )

    if dry_run:
        print(f"[DRY RUN] 将生成文件: {safe_path}")
        print(content)
        return safe_path

    # 检查文件是否已存在
    if safe_path.exists() and not force:
        raise FileExistsError(f"文件 '{safe_path}' 已存在。使用 --force 覆盖。")

    # 确保目录存在
    safe_path.parent.mkdir(parents=True, exist_ok=True)

    # 原子写入：先写临时文件，再重命名
    try:
        fd, tmp_path = tempfile.mkstemp(
            suffix=".py",
            prefix="gen_",
            dir=str(safe_path.parent)
        )
        with os.fdopen(fd, 'w', encoding='utf-8') as tmp:
            tmp.write(content)
        # 设置文件权限 0o644 (所有者读写，组和其他只读)
        os.chmod(tmp_path, 0o644)
        # 原子重命名
        os.replace(tmp_path, str(safe_path))
    except Exception:
        # 清理临时文件
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise

    # 审计日志
    logger.info(
        f"模块骨架已生成",
        extra={
            "path": str(safe_path),
            "name": name,
            "author": author,
            "force": force,
        }
    )
    return safe_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description='火种系统模块骨架生成器 - 生成符合机构级规范的Python模块',
        epilog='示例: python scripts/generate_module.py --path core/new_module.py --name NewModule --desc "新模块" --author "Alice"',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--path', required=True, help='目标文件路径，相对或绝对（必须在允许的根目录内）')
    parser.add_argument('--name', required=True, help='类名（PascalCase），例如 NewModule')
    parser.add_argument('--desc', required=True, help='模块中文描述，例如 "自适应均线"')
    parser.add_argument('--author', default='Quant Team', help='作者标识')
    parser.add_argument('--force', action='store_true', help='强制覆盖已有文件')
    parser.add_argument('--dry-run', action='store_true', help='预览生成内容而不实际写入')
    parser.add_argument('--version', action='version', version=f'%(prog)s {SCRIPT_VERSION}')

    args = parser.parse_args()

    try:
        generated_path = generate_module(
            path=args.path,
            name=args.name,
            desc=args.desc,
            author=args.author,
            force=args.force,
            dry_run=args.dry_run,
        )
        if not args.dry_run:
            print(f"✅ 模块骨架已成功生成: {generated_path}")
    except SecurityError as e:
        logger.error(f"安全违规: {e}")
        sys.exit(3)
    except FileExistsError as e:
        logger.error(str(e))
        sys.exit(2)
    except ValueError as e:
        logger.error(f"参数错误: {e}")
        sys.exit(1)
    except Exception as e:
        logger.exception("未预期的生成错误")
        sys.exit(4)


if __name__ == '__main__':
    main()
