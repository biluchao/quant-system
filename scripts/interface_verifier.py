#!/usr/bin/env python3
"""
火种系统 · 接口契约校验器 (Interface Verifier)

核心职责：
1. 解析每个模块文档头中的“接口契约”，验证承诺的公共方法存在且签名匹配。
2. 检查文档“外部依赖”与实际 import 是否一致，缺失或冗余均报告。
3. 扫描模块内所有跨模块调用，验证被引用的模块、类、方法真实存在。
4. 输出结构化报告（JSON/文本），错误码严格分级，直接嵌入 CI/CD 流水线。

外部依赖（真实模块接口）：
- Python 3.8+ 标准库：ast, argparse, json, logging, os, re, sys, pathlib, hashlib, typing

接口契约：
- verify(target_dir: Path, exclude: Optional[Set[str]] = None, format: str = "text") -> Dict[str, Any]
  返回 {"status": "ok/error", "errors": List[ErrorItem], "summary": str}
- 错误项 ErrorItem 为 TypedDict，字段固定，确保下游解析稳定。

异常与降级：
- 目标目录不存在：CRITICAL 并返回非零退出码。
- 单个文件解析失败：记录 HIGH 错误并继续，绝不中断整体检查。
- 可选依赖（如 typing_extensions）缺失：自动降级，不影响主流程。

资源管理：
- 所有文件操作使用 with 和 pathlib，确保句柄释放。
- AST 缓存使用模块级 LRU，不绑定实例，内存上限可控。
- 报告输出一次写入，不累积内存。
"""

from __future__ import annotations

import ast
import argparse
import hashlib
import json
import logging
import os
import re
import sys
from functools import lru_cache
from pathlib import Path
from typing import (Any, Dict, List, Optional, Set, Tuple, TypedDict)

# ---------------------------------------------------------------------------
# 金融级常量与类型
# ---------------------------------------------------------------------------
DEFAULT_TARGET_DIR = Path(__file__).resolve().parent.parent / "core"
DEFAULT_EXCLUDE_FILES: Set[str] = {"__init__.py", "assembler.py", "module_loader.py"}
SUPPORTED_PYTHON = (3, 8)

class Severity:
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

class ErrorCode:
    CONTRACT_METHOD_MISSING = "E1001"
    CONTRACT_RETURN_MISMATCH = "E1002"
    CONTRACT_PARAM_COUNT_MISMATCH = "E1003"
    CONTRACT_PARAM_TYPE_MISMATCH = "E1004"
    DEPENDENCY_MISSING = "E2001"
    DEPENDENCY_REDUNDANT = "E2002"
    CROSS_MODULE_CLASS_NOT_FOUND = "E3001"
    CROSS_MODULE_METHOD_NOT_FOUND = "E3002"
    CROSS_MODULE_ROOT_UNKNOWN = "E3003"
    SYNTAX_ERROR = "E9999"
    DOCSTRING_MISSING = "W0001"
    CONTRACT_PARSE_ERROR = "W0002"
    TYPE_HINT_MISSING = "W0003"
    PERFORMANCE_AST_REDUNDANT = "W0004"


class ErrorItem(TypedDict, total=False):
    """符合金融机构数据契约的错误项结构。"""
    file: str
    line: int
    severity: str
    code: str
    message: str
    suggestion: str
    checksum: str  # 用于去重与审计


# ---------------------------------------------------------------------------
# 工具函数 (模块级，避免实例缓存污染)
# ---------------------------------------------------------------------------
def _normalize_type_str(type_str: str) -> str:
    """规范化类型字符串：移除所有空白，统一泛型书写。"""
    return re.sub(r"\s+", "", type_str)


def _safe_unparse(node: ast.AST) -> str:
    """兼容 Python 3.8 的 AST 反解析，覆盖常见节点。"""
    if hasattr(ast, "unparse"):
        try:
            return ast.unparse(node)
        except Exception:
            pass
    # 回退逻辑
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return f"{_safe_unparse(node.value)}.{node.attr}"
    if isinstance(node, ast.Subscript):
        return f"{_safe_unparse(node.value)}[{_safe_unparse(node.slice)}]"
    if isinstance(node, ast.Constant):
        return repr(node.value)
    if isinstance(node, ast.Index):          # Python 3.8
        return _safe_unparse(node.value)
    if isinstance(node, ast.Tuple):
        return f"({', '.join(_safe_unparse(e) for e in node.elts)})"
    return "Any"


@lru_cache(maxsize=256)
def _parse_file_cached(filepath: str) -> Optional[ast.Module]:
    """模块级 AST 解析缓存，不绑定实例，可跨实例复用。"""
    try:
        source = Path(filepath).read_text(encoding='utf-8')
        return ast.parse(source, filename=filepath)
    except SyntaxError:
        return None
    except Exception:
        return None


def _error_checksum(file: str, line: int, code: str, message: str) -> str:
    """生成错误指纹，基于核心字段，不含 severity（避免去重失效）。"""
    raw = f"{file}:{line}:{code}:{message}"
    return hashlib.md5(raw.encode()).hexdigest()


# ---------------------------------------------------------------------------
# 主校验器
# ---------------------------------------------------------------------------
class InterfaceVerifier:
    """接口契约校验器，金融级静态分析工具。"""

    def __init__(self, target_dir: Path, exclude_files: Optional[Set[str]] = None) -> None:
        self.target_dir = target_dir
        self.exclude_files = exclude_files or DEFAULT_EXCLUDE_FILES
        self.errors: List[ErrorItem] = []
        self._error_checksums: Set[str] = set()
        self._available_modules: Dict[str, Dict[str, Any]] = {}

    # -----------------------------------------------------------------------
    # 公开方法
    # -----------------------------------------------------------------------
    def verify(self) -> Dict[str, Any]:
        """执行所有校验，返回错误列表和摘要。"""
        if not self.target_dir.is_dir():
            self._add_critical(str(self.target_dir), 0,
                               f"目标目录不存在: {self.target_dir}")
            return self._build_result()

        self._build_available_modules()
        logger.info("已索引 %d 个模块", len(self._available_modules))

        for fname in sorted(os.listdir(self.target_dir)):
            if fname.endswith('.py') and fname not in self.exclude_files:
                self._process_file(self.target_dir / fname)

        return self._build_result()

    # -----------------------------------------------------------------------
    # 模块索引构建
    # -----------------------------------------------------------------------
    def _build_available_modules(self) -> None:
        """构建目标目录下所有模块的公共接口索引。"""
        self._available_modules.clear()
        for fname in sorted(os.listdir(self.target_dir)):
            if not fname.endswith('.py') or fname in self.exclude_files:
                continue
            mod_name = fname[:-3]
            filepath_str = str(self.target_dir / fname)
            tree = _parse_file_cached(filepath_str)
            if tree is None:
                continue
            classes: Dict[str, List[Dict[str, Any]]] = {}
            module_funcs: List[Dict[str, Any]] = []
            # 使用单次 ast.walk 收集所有类和顶层函数
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    # 避免重复添加（嵌套类只添加一次，后续覆盖没关系）
                    classes[node.name] = self._extract_class_methods(node)
                elif isinstance(node, ast.FunctionDef) and node.parent is tree:  # 仅顶层
                    module_funcs.append(self._function_info(node))
            # 如果 walk 没有覆盖顶层函数（因为父节点检查），补一遍顶层
            for node in tree.body:
                if isinstance(node, ast.FunctionDef):
                    if not any(f['name'] == node.name for f in module_funcs):
                        module_funcs.append(self._function_info(node))
            self._available_modules[mod_name] = {
                "classes": classes,
                "module_functions": module_funcs,
                "file": filepath_str,
            }

    @staticmethod
    def _extract_class_methods(class_node: ast.ClassDef) -> List[Dict[str, Any]]:
        return [InterfaceVerifier._function_info(m) for m in class_node.body if isinstance(m, ast.FunctionDef)]

    @staticmethod
    def _function_info(func: ast.FunctionDef) -> Dict[str, Any]:
        # 判断是否为类方法/静态方法
        is_classmethod = any(
            isinstance(d, ast.Name) and d.id == 'classmethod' for d in func.decorator_list
        )
        is_staticmethod = any(
            isinstance(d, ast.Name) and d.id == 'staticmethod' for d in func.decorator_list
        )
        params = InterfaceVerifier._get_params(func, is_classmethod, is_staticmethod)
        return {
            "name": func.name,
            "is_public": not func.name.startswith('_'),
            "is_classmethod": is_classmethod,
            "is_staticmethod": is_staticmethod,
            "params": params,
            "return_type": _safe_unparse(func.returns) if func.returns else "None",
        }

    @staticmethod
    def _get_params(func: ast.FunctionDef, is_classmethod: bool, is_staticmethod: bool) -> List[str]:
        """提取参数字符串列表，正确处理 self/cls 和可变参数。"""
        params = []
        for arg in func.args.args:
            if arg.arg == 'self' and not is_staticmethod:
                continue
            if arg.arg == 'cls' and is_classmethod:
                continue
            annotation = _safe_unparse(arg.annotation) if arg.annotation else ""
            params.append(f"{arg.arg}: {annotation}" if annotation else arg.arg)
        # vararg
        if func.args.vararg:
            annotation = _safe_unparse(func.args.vararg.annotation) if func.args.vararg.annotation else ""
            params.append(f"*{func.args.vararg.arg}: {annotation}" if annotation else f"*{func.args.vararg.arg}")
        # kwarg
        if func.args.kwarg:
            annotation = _safe_unparse(func.args.kwarg.annotation) if func.args.kwarg.annotation else ""
            params.append(f"**{func.args.kwarg.arg}: {annotation}" if annotation else f"**{func.args.kwarg.arg}")
        return params

    # -----------------------------------------------------------------------
    # 单文件处理管线
    # -----------------------------------------------------------------------
    def _process_file(self, filepath: Path) -> None:
        filepath_str = str(filepath)
        tree = _parse_file_cached(filepath_str)
        if tree is None:
            self._add_error(filepath_str, 1, Severity.HIGH, ErrorCode.SYNTAX_ERROR,
                            "文件解析失败", "检查语法。")
            return
        docstring = ast.get_docstring(tree) or ""
        if not docstring:
            self._add_error(filepath_str, 1, Severity.LOW, ErrorCode.DOCSTRING_MISSING,
                            "模块缺少文档字符串", "添加标准文档头。")
        self._check_contracts(filepath_str, tree, docstring)
        self._check_dependencies(filepath_str, tree, docstring)
        self._check_cross_calls(filepath_str, tree)
        self._check_code_smells(filepath_str, tree)

    # -----------------------------------------------------------------------
    # 契约解析与实现检查
    # -----------------------------------------------------------------------
    def _check_contracts(self, filepath: str, tree: ast.Module, docstring: str) -> None:
        contracts = self._parse_contracts(docstring)
        if not contracts:
            return
        available = self._build_local_method_registry(tree)
        for contract in contracts:
            self._validate_contract(filepath, contract, available)

    def _build_local_method_registry(self, tree: ast.Module) -> Dict[str, Dict[str, Any]]:
        registry: Dict[str, Dict[str, Any]] = {}
        # 模块级函数
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and not node.name.startswith('_'):
                info = self._function_info(node)
                info["scope"] = "module"
                registry[node.name] = info
        # 类方法
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                for item in node.body:
                    if isinstance(item, ast.FunctionDef) and not item.name.startswith('_'):
                        full = f"{node.name}.{item.name}"
                        info = self._function_info(item)
                        info["scope"] = node.name
                        registry[full] = info
        return registry

    def _parse_contracts(self, docstring: str) -> List[Dict[str, Any]]:
        """解析'接口契约'段落，严格限制上下文，避免跨章节污染。"""
        contracts: List[Dict[str, Any]] = []
        lines = docstring.split('\n')
        in_section = False
        for i, raw_line in enumerate(lines):
            line = raw_line.strip()
            # 检测章节标题（以中文或关键字开头）
            if re.match(r'^(外部依赖|接口契约|异常与降级|资源管理|核心职责)', line):
                in_section = ('接口契约' in line)
                continue
            if in_section:
                # 空行或非列表项退出
                if line == '' or not line.startswith('- '):
                    in_section = False
                    continue
                # 尝试匹配方法签名，支持复合返回类型直到行尾
                m = re.match(
                    r'-\s*((?P<class>[\w]+)\.)?(?P<method>\w+)\((?P<params>.*)\)\s*->\s*(?P<ret>.+)$',
                    line
                )
                if m:
                    class_name = m.group('class')
                    method = m.group('method')
                    params_str = m.group('params').strip()
                    ret = m.group('ret').strip()
                    full_method = f"{class_name}.{method}" if class_name else method
                    contracts.append({
                        "method": full_method,
                        "params": self._parse_params_string(params_str),
                        "return_type": ret,
                        "line": i + 1,
                    })
                else:
                    self._add_error(filepath="(docstring)", line=i + 1,
                                    severity=Severity.MEDIUM, code=ErrorCode.CONTRACT_PARSE_ERROR,
                                    message=f"无法解析契约行: {line}",
                                    suggestion="使用格式 '- method(param: type) -> RetType'")
        return contracts

    @staticmethod
    def _parse_params_string(params_str: str) -> List[Dict[str, str]]:
        """健壮解析参数字符串，处理默认值、嵌套泛型和可变参数。"""
        if not params_str:
            return []
        params = []
        # 简单状态机处理嵌套括号
        depth = 0
        current = []
        for ch in params_str:
            if ch in '([{':
                depth += 1
            elif ch in ')]}':
                depth -= 1
            if ch == ',' and depth == 0:
                params.append(''.join(current).strip())
                current = []
            else:
                current.append(ch)
        if current:
            params.append(''.join(current).strip())

        parsed = []
        for part in params:
            part = part.strip()
            if not part:
                continue
            # 提取默认值： split on '='
            if '=' in part:
                type_and_default = part.split('=', 1)
                type_part = type_and_default[0].strip()
                # 忽略默认值，只保留类型
            else:
                type_part = part
            if ':' in type_part:
                name, _, type_str = type_part.partition(':')
                parsed.append({"name": name.strip(), "type": type_str.strip()})
            else:
                parsed.append({"name": type_part, "type": "Any"})
        return parsed

    def _validate_contract(self, filepath: str, contract: Dict, available: Dict) -> None:
        method_name = contract["method"]
        if method_name in available:
            self._compare_signatures(filepath, contract, available[method_name])
            return
        # 模糊匹配候选
        candidates = [k for k in available if k.endswith(f".{method_name}")]
        if not candidates:
            self._add_error(filepath, contract["line"], Severity.HIGH,
                            ErrorCode.CONTRACT_METHOD_MISSING,
                            f"契约方法 {method_name} 未在公共接口中找到",
                            "实现该方法或修改文档。")
        elif len(candidates) == 1:
            self._compare_signatures(filepath, contract, available[candidates[0]])
        else:
            self._add_error(filepath, contract["line"], Severity.MEDIUM,
                            ErrorCode.CONTRACT_METHOD_MISSING,
                            f"契约方法 {method_name} 匹配多个候选 ({', '.join(candidates)})，归属不明确",
                            "使用 ClassName.method 格式明确指定。")

    def _compare_signatures(self, filepath: str, contract: Dict, actual: Dict) -> None:
        # 比较返回类型
        if contract["return_type"] and actual["return_type"]:
            norm_contract = _normalize_type_str(contract["return_type"])
            norm_actual = _normalize_type_str(actual["return_type"])
            if norm_contract != norm_actual:
                self._add_error(filepath, contract["line"], Severity.HIGH,
                                ErrorCode.CONTRACT_RETURN_MISMATCH,
                                f"返回类型不匹配: 声明 {contract['return_type']}，实际 {actual['return_type']}",
                                "更新实现或文档。")
        # 比较参数数量（排除 self/cls 已在 _get_params 处理，直接比较长度）
        contract_params = contract["params"]
        actual_params = actual["params"]  # 已经正确剔除了 self/cls
        if len(contract_params) != len(actual_params):
            self._add_error(filepath, contract["line"], Severity.HIGH,
                            ErrorCode.CONTRACT_PARAM_COUNT_MISMATCH,
                            f"参数数量不匹配: 声明 {len(contract_params)}，实际 {len(actual_params)}",
                            "检查签名。")
        else:
            # 逐个比较类型
            for cp, ap in zip(contract_params, actual_params):
                ct = cp.get("type", "")
                at = ap.get("type", "") if isinstance(ap, dict) else ap.split(':')[-1].strip()
                if ct and at:
                    if _normalize_type_str(ct) != _normalize_type_str(at):
                        self._add_error(filepath, contract["line"], Severity.MEDIUM,
                                        ErrorCode.CONTRACT_PARAM_TYPE_MISMATCH,
                                        f"参数 {cp['name']} 类型不匹配: 声明 {ct}，实际 {at}",
                                        "检查参数类型注解。")

    # -----------------------------------------------------------------------
    # 外部依赖一致性检查
    # -----------------------------------------------------------------------
    def _check_dependencies(self, filepath: str, tree: ast.Module, docstring: str) -> None:
        doc_deps = self._parse_doc_deps(docstring)
        import_deps = self._extract_imports(tree)
        # 检查文档声明但未导入的依赖（允许文档只写模块或类路径，智能匹配模块部分）
        for dep in doc_deps:
            # 尝试去除可能的类名部分，匹配导入模块
            mod_part = dep.rsplit('.', 1)[0] if '.' in dep else dep
            if not any(mod_part == imp or imp.startswith(mod_part) for imp in import_deps):
                self._add_error(filepath, 1, Severity.MEDIUM, ErrorCode.DEPENDENCY_MISSING,
                                f"文档声明依赖 '{dep}' 但未找到对应导入",
                                "补充 import 或修正文档。")
        # 冗余检查：导入的第三方库/内部模块未在文档声明
        stdlib = sys.stdlib_module_names if hasattr(sys, 'stdlib_module_names') else set()
        for imp in import_deps:
            top = imp.split('.')[0]
            if top not in stdlib and top != 'core':
                # 未在文档中提及
                if not any(imp in d or imp.startswith(d) for d in doc_deps):
                    self._add_error(filepath, 1, Severity.LOW, ErrorCode.DEPENDENCY_REDUNDANT,
                                    f"导入 '{imp}' 未在文档外部依赖中声明",
                                    "更新文档或移除无用导入。")

    def _parse_doc_deps(self, docstring: str) -> Set[str]:
        """从‘外部依赖’段落提取依赖路径。"""
        deps: Set[str] = set()
        lines = docstring.split('\n')
        in_section = False
        for line in lines:
            stripped = line.strip()
            if '外部依赖' in stripped:
                in_section = True
                continue
            if in_section:
                if stripped == '' or not stripped.startswith('- '):
                    in_section = False
                    continue
                m = re.match(r'-\s*([\w.]+)\s*:', stripped)
                if m:
                    deps.add(m.group(1))
        return deps

    def _extract_imports(self, tree: ast.Module) -> Set[str]:
        imports: Set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.add(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:  # 忽略相对导入（module is None）
                    imports.add(node.module)
        return imports

    # -----------------------------------------------------------------------
    # 跨模块调用检查
    # -----------------------------------------------------------------------
    def _check_cross_calls(self, filepath: str, tree: ast.Module) -> None:
        import_table = self._build_import_table(tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                self._validate_call(filepath, node, import_table)

    def _build_import_table(self, tree: ast.Module) -> Dict[str, Tuple[str, Optional[str]]]:
        table: Dict[str, Tuple[str, Optional[str]]] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    name = alias.asname or alias.name
                    table[name] = (alias.name, None)
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ''  # 相对导入为空
                if not module:
                    continue
                for alias in node.names:
                    name = alias.asname or alias.name
                    table[name] = (module, alias.name)
        return table

    def _validate_call(self, filepath: str, call: ast.Call, import_table: Dict) -> None:
        chain = []
        node = call.func
        while isinstance(node, ast.Attribute):
            chain.append(node.attr)
            node = node.value
        if isinstance(node, ast.Name):
            chain.append(node.id)
        else:
            return
        chain.reverse()
        if len(chain) < 2:
            return

        root = chain[0]
        if root in import_table:
            mod_name, obj_name = import_table[root]
            self._validate_import_call(filepath, call.lineno, mod_name, obj_name, chain[1:])
        elif root in self._available_modules:
            self._validate_direct_module_call(filepath, call.lineno, root, chain[1:])
        # 其他情况忽略（本地调用等）

    def _validate_import_call(self, filepath: str, lineno: int,
                              mod_name: str, obj_name: Optional[str],
                              remaining: List[str]) -> None:
        if mod_name.startswith('core.'):
            mod_key = mod_name[5:]
        else:
            mod_key = mod_name
        if mod_key not in self._available_modules:
            return
        module = self._available_modules[mod_key]
        if obj_name is None:
            if len(remaining) < 1:
                return
            class_name = remaining[0]
            if class_name not in module["classes"]:
                self._add_error(filepath, lineno, Severity.HIGH,
                                ErrorCode.CROSS_MODULE_CLASS_NOT_FOUND,
                                f"模块 {mod_key} 中不存在类 {class_name}",
                                "检查类名或导入路径。")
                return
            if len(remaining) >= 2:
                method_name = remaining[1]
                if not self._class_has_method(module, class_name, method_name):
                    self._add_error(filepath, lineno, Severity.HIGH,
                                    ErrorCode.CROSS_MODULE_METHOD_NOT_FOUND,
                                    f"类 {class_name} 中不存在方法 {method_name}",
                                    "确认方法名。")
        else:
            class_name = obj_name
            if class_name not in module["classes"]:
                self._add_error(filepath, lineno, Severity.HIGH,
                                ErrorCode.CROSS_MODULE_CLASS_NOT_FOUND,
                                f"模块 {mod_key} 中不存在类 {class_name}",
                                "检查导入语句。")
                return
            if remaining:
                method_name = remaining[0]
                if not self._class_has_method(module, class_name, method_name):
                    self._add_error(filepath, lineno, Severity.HIGH,
                                    ErrorCode.CROSS_MODULE_METHOD_NOT_FOUND,
                                    f"类 {class_name} 中不存在方法 {method_name}",
                                    "确认方法名。")

    def _validate_direct_module_call(self, filepath: str, lineno: int,
                                     mod_name: str, chain: List[str]) -> None:
        module = self._available_modules.get(mod_name)
        if not module:
            return
        if len(chain) < 1:
            return
        class_name = chain[0]
        if class_name not in module["classes"]:
            self._add_error(filepath, lineno, Severity.HIGH,
                            ErrorCode.CROSS_MODULE_CLASS_NOT_FOUND,
                            f"模块 {mod_name} 中不存在类 {class_name}",
                            "检查类名。")
            return
        if len(chain) >= 2:
            method_name = chain[1]
            if not self._class_has_method(module, class_name, method_name):
                self._add_error(filepath, lineno, Severity.HIGH,
                                ErrorCode.CROSS_MODULE_METHOD_NOT_FOUND,
                                f"类 {class_name} 中不存在方法 {method_name}",
                                "确认方法名。")

    @staticmethod
    def _class_has_method(module: Dict, class_name: str, method: str) -> bool:
        methods = module["classes"].get(class_name, [])
        return any(m["name"] == method for m in methods)

    # -----------------------------------------------------------------------
    # 代码气味检查
    # -----------------------------------------------------------------------
    def _check_code_smells(self, filepath: str, tree: ast.Module) -> None:
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                # 排除特殊方法和私有方法
                if node.name.startswith('_') or node.name in ('__init__', '__new__', '__call__'):
                    continue
                if node.returns is None:
                    self._add_error(filepath, node.lineno, Severity.LOW,
                                    ErrorCode.TYPE_HINT_MISSING,
                                    f"公共方法 {node.name} 缺少返回类型注解",
                                    "添加 -> 返回类型。")

    # -----------------------------------------------------------------------
    # 错误报告
    # -----------------------------------------------------------------------
    def _add_error(self, filepath: str, line: int, severity: str,
                   code: str, message: str, suggestion: str = "") -> None:
        checksum = _error_checksum(filepath, line, code, message)
        if checksum in self._error_checksums:
            return
        self._error_checksums.add(checksum)
        item: ErrorItem = {
            "file": filepath,
            "line": line,
            "severity": severity,
            "code": code,
            "message": message,
            "suggestion": suggestion,
            "checksum": checksum,
        }
        self.errors.append(item)
        log_level = {"HIGH": logging.ERROR, "MEDIUM": logging.WARNING, "LOW": logging.INFO}.get(severity, logging.WARNING)
        logger.log(log_level, "%s:%d [%s] %s", filepath, line, code, message)

    def _add_critical(self, filepath: str, line: int, message: str) -> None:
        self._add_error(filepath, line, Severity.HIGH, ErrorCode.SYNTAX_ERROR, message, "立即修复。")

    def _build_result(self) -> Dict[str, Any]:
        high_count = sum(1 for e in self.errors if e["severity"] == Severity.HIGH)
        return {
            "status": "ok" if high_count == 0 else "error",
            "errors": self.errors,
            "summary": f"共 {len(self.errors)} 个缺陷: HIGH={high_count}",
        }


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------
def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="火种系统 - 接口契约校验器",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--target", type=Path, default=DEFAULT_TARGET_DIR,
                        help=f"目标模块目录 (默认: {DEFAULT_TARGET_DIR})")
    parser.add_argument("--exclude", nargs="*", default=None,
                        help="额外排除的文件名")
    parser.add_argument("--format", choices=["text", "json"], default="text",
                        help="输出格式")
    parser.add_argument("--quiet", action="store_true", help="减少日志输出")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format='%(levelname)s: %(message)s',
    )

    exclude_set = DEFAULT_EXCLUDE_FILES.copy()
    if args.exclude:
        exclude_set.update(args.exclude)

    verifier = InterfaceVerifier(args.target, exclude_set)
    result = verifier.verify()

    if args.format == "json":
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        if result["status"] == "ok":
            print("✅ 接口契约校验通过")
        else:
            print(f"❌ 发现 {len(result['errors'])} 个缺陷：")
            for e in result["errors"]:
                print(f"[{e['severity']}] {e['file']}:{e['line']} {e['message']}")

    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
