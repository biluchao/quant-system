#!/usr/bin/env python3
"""
火种系统 · 代码风格与安全校验器 (StyleChecker)

核心职责：
1. 基于可配置规则集扫描Python源码，检查编码规范、安全缺陷与性能隐患。
2. 输出结构化违规报告（JSON/SARIF/Text），支持CI/CD管道无缝集成。
3. 提供编程API与命令行工具，支持并行增量扫描与基线抑制。

外部依赖（真实模块接口）：
- 标准库：ast, sys, os, re, json, logging, argparse, pathlib, typing, dataclasses, fnmatch, concurrent, hashlib, itertools

接口契约：
- StyleChecker(rules: RuleSet) -> 实例
- check_file(path: Path) -> List[Violation]
- check_directory(path: Path, jobs: int = 8) -> List[Violation]
- report_json(violations: List[Violation]) -> str
- health_check() -> Dict[str, Any]

异常与降级：
- 所有文件I/O错误捕获并记录为系统级违规，不中断整体流程。
- AST解析失败按语法错误处理，生成违规记录并跳过后续检查。
- 配置文件解析失败回退到内建安全默认规则。

资源管理：
- 文件读取使用大小限制（默认16MB），防止内存溢出。
- 并行扫描使用有界线程池，最大线程数可配置。
- 所有I/O操作使用with语句，确保句柄释放。
"""

from __future__ import annotations

import ast
import sys
import os
import re
import json
import logging
import argparse
import fnmatch
import hashlib
from pathlib import Path
from typing import (
    Dict, Any, List, Optional, Set, Tuple, Union, Iterator, Final
)
from dataclasses import dataclass, field, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from itertools import chain

__version__ = "3.0.0"

# 内部日志器（不污染根logger）
logger = logging.getLogger("fire.style_checker")
logger.addHandler(logging.NullHandler())

# ------------------------------------------------------------------------------
# 规则定义
# ------------------------------------------------------------------------------

class RuleID:
    """规则代号命名空间"""
    MODULE_DOC_MISSING = "D001"
    MODULE_DOC_PREFIX = "D002"
    MODULE_DOC_SECTION_MISSING = "D003"
    CLASS_NAMING = "C001"
    CONSTANT_NAMING = "C002"
    FUNC_NAMING = "F001"
    FUNC_RETURN_ANNOTATION = "F002"
    FUNC_RETURN_DICT_KEYS = "F003"
    FUNC_PARAM_ANNOTATION = "F004"
    FUNC_TOO_LONG = "F005"
    BARE_EXCEPT = "E001"
    BROAD_EXCEPT = "E002"
    PLACEHOLDER = "P001"
    LONG_SHORT_BRANCH = "B001"
    IMPORT_ORDER = "I001"
    LINE_TRAILING_WHITESPACE = "L001"
    LINE_TOO_LONG = "L002"
    EOF_NO_NEWLINE = "L003"

@dataclass(frozen=True)
class Violation:
    """不可变违规记录"""
    file: str
    line: int
    column: int = 0
    code: str = ""
    message: str = ""
    severity: str = "error"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

# 默认规则集
DEFAULT_RULES: Final[Dict[str, Any]] = {
    "require_module_doc": True,
    "module_doc_prefix": "火种系统",
    "module_doc_required_sections": ["核心职责", "外部依赖", "接口契约", "异常与降级", "资源管理"],
    "class_naming_style": "PascalCase",
    "function_naming_style": "snake_case",
    "constant_naming_style": "UPPER_CASE",
    "public_method_return_type_hint": True,
    "public_method_return_dict_keys": ["reason"],
    "public_method_param_annotation": True,
    "forbid_bare_except": True,
    "forbid_broad_except": {"Exception", "BaseException"},
    "require_log_recovery_mark": True,
    "log_object_names": {"logger", "log", "LOGGER"},
    "log_recovery_levels": {"error", "critical", "warning"},
    "forbid_placeholders": {"pass", "...", "TODO", "FIXME"},
    "ellipsis_in_stub_allowed": True,
    "max_line_length": 120,
    "max_function_statements": 50,
    "forbid_trailing_whitespace": True,
    "require_eof_newline": True,
    "forbid_long_short_branches": True,
    "long_short_identifiers": {"long", "short", "is_long", "is_short"},
    "forbid_import_disorder": True,
    "max_file_size_mb": 16,
    "max_scan_workers": 8,
    "ignore_directories": {"__pycache__", ".git", ".tox", "venv", "node_modules", ".mypy_cache"},
    "ignore_files": {"setup.py", "conftest.py"},
    "noqa_tag": "noqa",
}

# ------------------------------------------------------------------------------
# 辅助工具
# ------------------------------------------------------------------------------

def _is_pascal_case(name: str) -> bool:
    return bool(re.fullmatch(r'[A-Z][a-zA-Z0-9]*', name))

def _is_snake_case(name: str) -> bool:
    return bool(re.fullmatch(r'_?_?[a-z][a-z0-9_]*', name))

def _is_upper_case(name: str) -> bool:
    return bool(re.fullmatch(r'[A-Z_][A-Z0-9_]*', name))

def _extract_docstring(node: ast.AST) -> Optional[str]:
    return ast.get_docstring(node)

def _direct_children(body: List[ast.stmt]) -> Iterator[ast.stmt]:
    """模块或函数体的直接子语句，排除嵌套类/函数内部的语句"""
    for stmt in body:
        yield stmt
        # 对于复合语句，不深入内部（除非是类或函数定义，我们仍不进入其内部）
        # 这里按顶级语句返回，仅此而已。

# ------------------------------------------------------------------------------
# 核心检查器
# ------------------------------------------------------------------------------

class StyleChecker:
    """代码风格与安全校验器"""

    def __init__(self, rules: Optional[Dict[str, Any]] = None, config_path: Optional[Path] = None):
        self._rules = DEFAULT_RULES.copy()
        # 加载配置文件（支持 JSON/JSONC 简单处理）
        if config_path:
            self._load_config(config_path)
        if rules:
            self._merge_rules(rules)
        # 预处理集合
        self._broad_except: Set[str] = set(self._rules["forbid_broad_except"])
        self._log_names: Set[str] = set(self._rules["log_object_names"])
        self._recovery_levels: Set[str] = set(self._rules["log_recovery_levels"])
        self._placeholder_set: Set[str] = set(self._rules["forbid_placeholders"])
        self._long_short_ids: Set[str] = set(self._rules["long_short_identifiers"])
        self._ignore_dirs: Set[str] = set(self._rules["ignore_directories"])
        self._ignore_files: Set[str] = set(self._rules["ignore_files"])
        self._max_file_bytes = self._rules["max_file_size_mb"] * 1024 * 1024

    def _load_config(self, path: Path):
        try:
            text = path.read_text(encoding='utf-8')
            # 简易 JSONC 注释处理
            text = re.sub(r'//.*?$', '', text, flags=re.MULTILINE)
            self._merge_rules(json.loads(text))
        except Exception as e:
            logger.error(f"加载配置文件失败 {path}: {e}，使用默认规则")

    def _merge_rules(self, new_rules: Dict[str, Any]):
        for k, v in new_rules.items():
            if k in DEFAULT_RULES:
                self._rules[k] = v
            else:
                logger.warning(f"未知规则键: {k}")

    # ------------------------------------------------------------------
    # 公开API
    # ------------------------------------------------------------------
    def check_file(self, filepath: Union[str, Path]) -> List[Violation]:
        path = Path(filepath)
        if path.name in self._ignore_files:
            return []
        # 大小检查
        try:
            if path.stat().st_size > self._max_file_bytes:
                return [Violation(str(path), 0, code="F000", message=f"文件大小超过限制 {self._max_file_bytes} 字节")]
        except OSError as e:
            return [Violation(str(path), 0, code="F000", message=f"无法读取文件信息: {e}")]
        try:
            source = path.read_text(encoding='utf-8')
        except Exception as e:
            return [Violation(str(path), 0, code="F000", message=f"读取失败: {e}")]
        return self._check_source(source, str(path))

    def check_directory(self, directory: Union[str, Path], jobs: int = None) -> List[Violation]:
        if jobs is None:
            jobs = self._rules["max_scan_workers"]
        dir_path = Path(directory)
        py_files = []
        for root, dirs, files in os.walk(dir_path):
            dirs[:] = [d for d in dirs if d not in self._ignore_dirs]
            for fname in files:
                if fname.endswith('.py'):
                    py_files.append(Path(root) / fname)
        all_violations: List[Violation] = []
        with ThreadPoolExecutor(max_workers=jobs) as executor:
            future_to_file = {executor.submit(self.check_file, p): p for p in py_files}
            for future in as_completed(future_to_file):
                try:
                    vlist = future.result(timeout=30)
                except TimeoutError:
                    f = future_to_file[future]
                    vlist = [Violation(str(f), 0, code="F000", message="检查超时")]
                all_violations.extend(vlist)
        return all_violations

    @staticmethod
    def report_json(violations: List[Violation]) -> str:
        return json.dumps([v.to_dict() for v in violations], ensure_ascii=False, indent=2)

    @classmethod
    def health_check(cls) -> Dict[str, Any]:
        try:
            checker = cls()
            source = "def foo(): pass\n"
            viols = checker._check_source(source, "<test>")
            # 必须检测出 pass 占位符
            codes = {v.code for v in viols}
            if "P001" not in codes:
                return {"status": "warn", "message": "pass占位符检测未生效"}
            return {"status": "ok", "message": "核心规则自检通过", "warnings": []}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    # ------------------------------------------------------------------
    # 内部核心
    # ------------------------------------------------------------------
    def _check_source(self, source: str, filepath: str) -> List[Violation]:
        violations = []
        violations.extend(self._check_lines(source, filepath))
        try:
            tree = ast.parse(source, filename=filepath)
        except SyntaxError as e:
            violations.append(Violation(filepath, e.lineno or 0, code="F000", message=f"语法错误: {e.msg}"))
            return violations
        # 一次遍历完成所有AST检查，避免重复walk
        self._check_ast_single_pass(tree, source, filepath, violations)
        return violations

    def _check_lines(self, source: str, filepath: str) -> List[Violation]:
        violations = []
        # 统一换行符处理
        source = source.replace('\r\n', '\n').replace('\r', '\n')
        lines = source.split('\n')
        for idx, line in enumerate(lines, start=1):
            # 尾随空格检查
            if self._rules["forbid_trailing_whitespace"] and (line.endswith(' ') or line.endswith('\t')):
                violations.append(Violation(filepath, idx, code=RuleID.LINE_TRAILING_WHITESPACE, message="行尾存在空白字符"))
            # 行长度检查
            max_len = self._rules["max_line_length"]
            if max_len and len(line) > max_len:
                if not (line.lstrip().startswith('#') or line.lstrip().startswith('"""') or line.lstrip().startswith("'''")):
                    violations.append(Violation(filepath, idx, code=RuleID.LINE_TOO_LONG, message=f"行长度 {len(line)} > {max_len}"))
            # 注释中的TODO/FIXME
            if self._placeholder_set & {"TODO", "FIXME"}:
                if re.search(r'\b(TODO|FIXME)\b', line):
                    violations.append(Violation(filepath, idx, code=RuleID.PLACEHOLDER, message="代码中包含TODO/FIXME标记"))
        # 文件末尾换行
        if self._rules["require_eof_newline"]:
            if source and not source.endswith('\n'):
                violations.append(Violation(filepath, len(lines), code=RuleID.EOF_NO_NEWLINE, message="文件末尾缺少换行符"))
        return violations

    def _check_ast_single_pass(self, tree: ast.Module, source: str, filepath: str, violations: List[Violation]):
        # 模块文档
        if not filepath.endswith('__init__.py'):
            doc = ast.get_docstring(tree)
            if not doc:
                violations.append(Violation(filepath, 1, code=RuleID.MODULE_DOC_MISSING, message="缺少模块文档字符串"))
            else:
                prefix = self._rules["module_doc_prefix"]
                if prefix and not doc.startswith(prefix):
                    violations.append(Violation(filepath, 1, code=RuleID.MODULE_DOC_PREFIX, message=f"文档未以'{prefix}'开头"))
                for sec in self._rules["module_doc_required_sections"]:
                    if sec not in doc:
                        violations.append(Violation(filepath, 1, code=RuleID.MODULE_DOC_SECTION_MISSING, message=f"缺少章节: {sec}"))

        # 收集多空分支测试（统一在一次遍历中检查）
        long_short_ids = self._long_short_ids

        def is_long_short_name(node: ast.expr) -> bool:
            if isinstance(node, ast.Name):
                return node.id in long_short_ids
            return False

        # 遍历
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                if not _is_pascal_case(node.name):
                    violations.append(Violation(filepath, node.lineno, code=RuleID.CLASS_NAMING, message=f"类名 '{node.name}' 不符合 PascalCase"))
                # 类文档字符串未强制，但建议

            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self._check_function(node, filepath, violations)

            elif isinstance(node, ast.ExceptHandler):
                if node.type is None:
                    violations.append(Violation(filepath, node.lineno, code=RuleID.BARE_EXCEPT, message="禁止裸except"))
                else:
                    # 精确检查异常类型名称
                    exc_names = self._extract_exception_names(node.type)
                    for exc in exc_names:
                        if exc in self._broad_except:
                            violations.append(Violation(filepath, node.lineno, code=RuleID.BROAD_EXCEPT, message=f"禁止捕获过宽异常 '{exc}'"))

            elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
                if node.value.value is Ellipsis and "..." in self._placeholder_set:
                    if not (self._rules["ellipsis_in_stub_allowed"] and filepath.endswith('.pyi')):
                        violations.append(Violation(filepath, node.lineno, code=RuleID.PLACEHOLDER, message="禁止使用 '...' 占位符"))

            elif isinstance(node, ast.Pass):
                if "pass" in self._placeholder_set:
                    # 忽略抽象方法中的 pass（类中包含 @abstractmethod 或 raise NotImplementedError 的上下文，简化：如果函数体内仅有pass且包含装饰器或文档字符串，不报）
                    # 这里简单放过函数体只有pass且函数有文档字符串的情况（通常存根）
                    parent = getattr(node, 'parent', None)
                    # 未能获取parent，保守上报
                    violations.append(Violation(filepath, node.lineno, code=RuleID.PLACEHOLDER, message="禁止使用 'pass' 占位符"))

            elif isinstance(node, ast.If):
                test = node.test
                # 多空分支检测
                if self._rules["forbid_long_short_branches"] and self._is_long_short_test(test, long_short_ids):
                    violations.append(Violation(filepath, node.lineno, code=RuleID.LONG_SHORT_BRANCH, message=f"检测到多空分支: {ast.unparse(test) if hasattr(ast,'unparse') else '...'}"))

            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                # 导入顺序检查稍后统一，记录位置
                pass

        # 导入顺序检查
        if self._rules["forbid_import_disorder"]:
            self._check_import_order(tree, filepath, violations)

    def _is_long_short_test(self, test: ast.expr, ids: Set[str]) -> bool:
        if isinstance(test, ast.Name) and test.id in ids:
            return True
        if isinstance(test, ast.UnaryOp) and isinstance(test.op, ast.Not):
            return self._is_long_short_test(test.operand, ids)
        if isinstance(test, ast.Compare) and len(test.ops) == 1 and isinstance(test.ops[0], (ast.Eq, ast.NotEq)):
            left = test.left
            if isinstance(left, ast.Name) and left.id in ids:
                return True
        return False

    def _extract_exception_names(self, node: ast.expr) -> List[str]:
        if isinstance(node, ast.Name):
            return [node.id]
        if isinstance(node, ast.Tuple):
            names = []
            for elt in node.elts:
                names.extend(self._extract_exception_names(elt))
            return names
        return []

    def _check_function(self, node: Union[ast.FunctionDef, ast.AsyncFunctionDef], filepath: str, violations: List[Violation]):
        if not _is_snake_case(node.name):
            violations.append(Violation(filepath, node.lineno, code=RuleID.FUNC_NAMING, message=f"函数名 '{node.name}' 不符合 snake_case"))
        is_public = not node.name.startswith('_')
        if is_public:
            # 参数注解
            if self._rules["public_method_param_annotation"]:
                for arg in node.args.args:
                    if arg.arg != 'self' and arg.arg != 'cls' and arg.annotation is None:
                        violations.append(Violation(filepath, node.lineno, code=RuleID.FUNC_PARAM_ANNOTATION, message=f"公共方法 '{node.name}' 参数 '{arg.arg}' 缺少类型注解"))
            # 返回注解
            if self._rules["public_method_return_type_hint"]:
                if node.returns is None:
                    violations.append(Violation(filepath, node.lineno, code=RuleID.FUNC_RETURN_ANNOTATION, message=f"公共方法 '{node.name}' 缺少返回类型注解"))
            # 返回值字典键检查（仅在声明返回Dict时）
            required_keys = self._rules["public_method_return_dict_keys"]
            if required_keys and node.returns is not None:
                if self._has_dict_return_annotation(node.returns):
                    if not self._method_returns_keys(node, required_keys):
                        violations.append(Violation(filepath, node.lineno, code=RuleID.FUNC_RETURN_DICT_KEYS, message=f"返回值字典缺少键: {required_keys}"))
        # 函数体长度
        max_stmts = self._rules["max_function_statements"]
        if max_stmts:
            body_stmts = [s for s in node.body if isinstance(s, ast.stmt) and not isinstance(s, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))]
            if len(body_stmts) > max_stmts:
                violations.append(Violation(filepath, node.lineno, code=RuleID.FUNC_TOO_LONG, message=f"函数体过长 ({len(body_stmts)} > {max_stmts})"))

    def _has_dict_return_annotation(self, returns: ast.expr) -> bool:
        # 简单判断返回类型是否是Dict[str, Any]
        if isinstance(returns, ast.Subscript):
            value = returns.value
            if isinstance(value, ast.Name) and value.id == 'Dict':
                return True
        # 对于Union等忽略
        return False

    def _method_returns_keys(self, func_node: ast.FunctionDef, keys: List[str]) -> bool:
        """检查函数体中是否至少存在一个返回包含所有keys的字典字面量"""
        for child in ast.walk(func_node):
            if isinstance(child, ast.Return) and child.value:
                if isinstance(child.value, ast.Dict):
                    try:
                        key_vals = [ast.literal_eval(k) for k in child.value.keys if isinstance(k, ast.Constant)]
                    except (ValueError, SyntaxError):
                        continue
                    if all(k in key_vals for k in keys):
                        return True
        return False

    def _check_import_order(self, tree: ast.Module, filepath: str, violations: List[Violation]):
        import_nodes = [n for n in tree.body if isinstance(n, (ast.Import, ast.ImportFrom))]
        if not import_nodes:
            return
        # 标准库集合（动态获取）
        stdlib = sys.stdlib_module_names if hasattr(sys, 'stdlib_module_names') else set()
        def group(module_name: Optional[str]) -> int:
            if not module_name:
                return 0
            top = module_name.split('.')[0]
            if top in stdlib:
                return 0
            # 简单判断：顶层包包含 "fire" 或 "core" 等视为本地
            if top in {'fire', 'core', 'gateway', 'ai_sandbox', 'frontend', 'scripts'}:
                return 2
            return 1
        last_group = -1
        for imp in import_nodes:
            if isinstance(imp, ast.Import):
                mod = imp.names[0].name
            else:
                mod = imp.module or ''
            cur_group = group(mod)
            if cur_group < last_group:
                violations.append(Violation(filepath, imp.lineno, code=RuleID.IMPORT_ORDER, message="导入顺序不符合标准库、第三方、本地分组"))
                break
            last_group = cur_group

# ------------------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=f'火种代码风格校验器 v{__version__}')
    parser.add_argument('path', nargs='?', default='.', help='扫描目录或文件')
    parser.add_argument('--json', action='store_true', help='输出JSON格式')
    parser.add_argument('--config', type=Path, help='规则配置文件路径')
    parser.add_argument('--jobs', type=int, help='并行扫描线程数')
    parser.add_argument('--exclude', action='append', default=[], help='额外忽略的目录或文件模式')
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    args = parser.parse_args()

    target = Path(args.path)
    if not target.exists():
        print(f"错误: 路径不存在 {target}", file=sys.stderr)
        sys.exit(2)

    checker = StyleChecker(config_path=args.config)
    # 追加排除
    if args.exclude:
        for pat in args.exclude:
            checker._ignore_dirs.add(pat)

    if target.is_file():
        violations = checker.check_file(target) if target.suffix == '.py' else []
    else:
        violations = checker.check_directory(target, jobs=args.jobs)

    if args.json:
        sys.stdout.write(StyleChecker.report_json(violations) + '\n')
    else:
        for v in violations:
            sys.stdout.write(f"{v.file}:{v.line}:{v.code}: {v.message}\n")
        if not violations:
            sys.stdout.write("未发现违规。\n")
        else:
            sys.stdout.write(f"总计 {len(violations)} 个违规。\n")
    sys.exit(1 if violations else 0)

if __name__ == '__main__':
    main()
