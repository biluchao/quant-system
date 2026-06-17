#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors. All Rights Reserved.
# -----------------------------------------------------------------------------
# 火种系统 · 软件物料清单生成器 v3.1.0 (generate_sbom.sh)
#
# 核心职责:
#   1. 以最小权限、完全离线可切换方式，安全生成符合 SPDX/CycloneDX 的 SBOM
#   2. 强制执行供应链完整性：校验和验证、签名、自洽性检查
#   3. 生成不可否认的审计证据链：时间戳、主机指纹、构建元数据
#
# 用法: 见 -h 或 docs/runbook.md
# 依赖: bash >= 4.2, curl, tar, jq, python3 (可选), gpg (可选)
# -----------------------------------------------------------------------------
set -o pipefail

# ── 全局常量（只读） ──────────────────────────────────────
readonly SCRIPT_NAME="${0##*/}"
readonly SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
readonly PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd -P)"
readonly VERSION="3.1.0"
readonly SYFT_MIN_VERSION="0.97.0"
readonly DEFAULT_FORMAT="spdx-json"
readonly DEFAULT_OUTPUT="${PROJECT_ROOT}/build/sbom/sbom.spdx.json"
readonly DEFAULT_TIMEOUT_SECONDS=900
readonly LOCK_FILE="${PROJECT_ROOT}/.sbom.lock"
readonly TEMP_DIR_BASE="/tmp"
readonly SYFT_CHECKSUMS_URL="https://github.com/anchore/syft/releases/download"
readonly SBOM_MAX_SIZE_MB=100

# ── 全局状态（可写） ──────────────────────────────────────
declare -a CLEANUP_FILES=()
declare -a CLEANUP_DIRS=()
LOCK_FD=""
GLOBAL_UMASK=""
SCRIPT_START_TIME=""
HOST_FINGERPRINT=""

# ── 初始化安全环境 ────────────────────────────────────────
init_security() {
    GLOBAL_UMASK=$(umask)
    umask 027
    SCRIPT_START_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    HOST_FINGERPRINT=$(hostname -f 2>/dev/null || hostname 2>/dev/null || echo "unknown")
    # 禁止 core dump
    ulimit -c 0 2>/dev/null || true
}

# ── 结构化日志 ────────────────────────────────────────────
log() {
    local level="$1"; shift
    local msg="$*"
    local ts pid
    ts=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")
    pid="$$"
    printf '{"ts":"%s","level":"%s","pid":"%s","script":"%s","msg":"%s"}\n' \
        "$ts" "$level" "$pid" "$SCRIPT_NAME" "$msg" >&2
}
log_info()  { log "INFO" "$@"; }
log_warn()  { log "WARN" "$@"; }
log_error() { log "ERROR" "$@"; }

# ── 安全退出清理 ──────────────────────────────────────────
cleanup() {
    local exit_code=$?
    # 释放锁（先释放再清理）
    if [[ -n "${LOCK_FD:-}" ]]; then
        flock -u "$LOCK_FD" 2>/dev/null || true
    fi
    # 清理临时文件
    local f d
    for f in "${CLEANUP_FILES[@]:-}"; do
        [[ -n "$f" ]] && rm -f "$f" 2>/dev/null || true
    done
    for d in "${CLEANUP_DIRS[@]:-}"; do
        [[ -n "$d" ]] && rm -rf "$d" 2>/dev/null || true
    done
    # 恢复 umask
    [[ -n "${GLOBAL_UMASK:-}" ]] && umask "$GLOBAL_UMASK" 2>/dev/null || true
    exit "$exit_code"
}
trap cleanup EXIT

# ── 注册清理目标 ──────────────────────────────────────────
register_file_cleanup() { CLEANUP_FILES+=("$1"); }
register_dir_cleanup()  { CLEANUP_DIRS+=("$1"); }

# ── 排他锁（含诊断信息）───────────────────────────────────
acquire_lock() {
    local lock_dir
    lock_dir=$(dirname "$LOCK_FILE")
    mkdir -p "$lock_dir" 2>/dev/null || true
    # 使用文件描述符 9 作为锁
    exec 9>"$LOCK_FILE" || { log_error "无法创建锁文件"; exit 2; }
    if ! flock -n 9; then
        log_error "无法获取排他锁，可能已有实例运行中。锁持有者信息:"
        cat "$LOCK_FILE" 2>/dev/null | while read -r line; do
            log_error "  $line"
        done
        exit 2
    fi
    # 写入诊断信息
    printf 'pid=%s host=%s ts=%s\n' "$$" "$HOST_FINGERPRINT" "$SCRIPT_START_TIME" > "$LOCK_FILE"
    LOCK_FD="9"
}

# ── 路径安全审计 ──────────────────────────────────────────
validate_path_safe() {
    local path="$1"
    local abs_path
    # 禁止空路径
    [[ -z "$path" ]] && { log_error "路径为空"; exit 1; }
    # 禁止控制字符
    if printf '%s' "$path" | grep -qP '[[:cntrl:]]'; then
        log_error "路径包含控制字符"; exit 1
    fi
    # 解析绝对路径并检测遍历
    abs_path=$(cd "$(dirname "$path")" 2>/dev/null && pwd -P)/$(basename "$path") || true
    if [[ -z "$abs_path" ]]; then
        log_error "无法解析路径: $path"; exit 1
    fi
    # 禁止写入系统关键目录
    local forbidden=("/etc" "/proc" "/sys" "/dev" "/boot" "/var/spool/cron" "/var/spool/at")
    local fb
    for fb in "${forbidden[@]}"; do
        if [[ "$abs_path" == "$fb" || "$abs_path" == "$fb/"* ]]; then
            log_error "禁止写入受保护路径: $abs_path (匹配 $fb)"; exit 1
        fi
    done
    # 禁止路径遍历
    if [[ "$abs_path" == *".."* ]]; then
        log_error "路径包含 '..' 遍历: $abs_path"; exit 1
    fi
}

# ── 检查工具可用性 ────────────────────────────────────────
require_tool() {
    local tool="$1"
    if ! command -v "$tool" &>/dev/null; then
        log_error "必需的工具不可用: $tool"
        exit 1
    fi
}

# ── 安全下载（含校验和验证）───────────────────────────────
safe_download() {
    local url="$1" output="$2" expected_checksum="${3:-}"
    # 禁止非 HTTPS
    if [[ "$url" != https://* ]]; then
        log_error "拒绝非 HTTPS 下载: $url"; exit 1
    fi
    log_info "下载: $url"
    if ! curl -sSfL --proto =https --retry 3 --max-time 120 -o "$output" "$url"; then
        log_error "下载失败: $url"; return 1
    fi
    # 校验大小
    local size
    size=$(stat -c%s "$output" 2>/dev/null || stat -f%z "$output" 2>/dev/null || echo 0)
    if [[ "$size" -eq 0 ]]; then
        log_error "下载文件为空: $url"; return 1
    fi
    # 检查是否为 HTML（简单启发式）
    if head -c 100 "$output" | grep -qi '<html\|<!doctype'; then
        log_error "下载文件可能为 HTML 错误页面: $url"; return 1
    fi
    if [[ -n "$expected_checksum" ]]; then
        local actual
        actual=$(sha256sum "$output" 2>/dev/null | awk '{print $1}' || shasum -a 256 "$output" 2>/dev/null | awk '{print $1}')
        if [[ "$actual" != "$expected_checksum" ]]; then
            log_error "校验和不匹配: 期望 $expected_checksum, 实际 $actual"; return 1
        fi
        log_info "校验和验证通过"
    fi
    return 0
}

# ── 确保 syft 可用（安全安装）─────────────────────────────
ensure_syft() {
    # 检查是否已存在且版本满足
    if command -v syft &>/dev/null; then
        local ver
        ver=$(syft version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)
        if [[ -n "$ver" ]] && printf '%s\n%s\n' "$SYFT_MIN_VERSION" "$ver" | sort -V -c 2>/dev/null; then
            log_info "syft 已可用: v$ver"
            return 0
        fi
    fi

    log_info "开始安全安装 syft ..."
    require_tool curl
    require_tool tar

    local os_name arch
    os_name=$(uname -s | tr '[:upper:]' '[:lower:]')
    arch=$(uname -m)
    case "$arch" in
        x86_64)  arch="amd64" ;;
        aarch64) arch="arm64" ;;
        arm64)   ;;
        *) log_error "不支持的 CPU 架构: $arch"; exit 1 ;;
    esac

    local syft_ver="${SYFT_VERSION:-v${SYFT_MIN_VERSION}}"
    # 校验版本号格式
    if [[ ! "$syft_ver" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        log_error "SYFT_VERSION 格式无效: $syft_ver (期望 vX.Y.Z)"; exit 1
    fi

    local base_url="${SYFT_CHECKSUMS_URL}/${syft_ver}"
    local archive="syft_${os_name}_${arch}.tar.gz"
    local checksum_file="syft_${syft_ver}_checksums.txt"

    local tmp_dir
    tmp_dir=$(mktemp -d "${TEMP_DIR_BASE}/${SCRIPT_NAME}_XXXXXX") || exit 1
    register_dir_cleanup "$tmp_dir"
    cd "$tmp_dir" || exit 1

    # 下载校验和文件
    safe_download "$base_url/$checksum_file" "$checksum_file" || exit 1
    # 提取期望的校验和
    local expected_sum
    expected_sum=$(grep -F " $archive" "$checksum_file" 2>/dev/null | awk '{print $1}')
    if [[ -z "$expected_sum" ]]; then
        log_error "未在校验和文件中找到 $archive"; exit 1
    fi

    # 下载二进制
    safe_download "$base_url/$archive" "$archive" "$expected_sum" || exit 1

    # 安全解压
    tar -xzf "$archive" --no-same-owner --no-same-permissions syft || { log_error "解压失败"; exit 1; }

    # 安装到用户本地
    local install_dir="${HOME}/.local/bin"
    mkdir -p "$install_dir"
    cp -f syft "$install_dir/syft" || { log_error "复制 syft 失败（磁盘满？）"; exit 1; }
    chmod 0750 "$install_dir/syft"
    chmod -x "$install_dir/syft" 2>/dev/null; chmod +x "$install_dir/syft"

    # 更新 PATH（仅本次运行）
    if [[ ":$PATH:" != *":$install_dir:"* ]]; then
        export PATH="$install_dir:$PATH"
    fi
    hash -r 2>/dev/null || true

    if ! command -v syft &>/dev/null; then
        log_error "安装后仍找不到 syft"; exit 1
    fi
    log_info "syft 安装完成: $(syft version 2>/dev/null | head -1)"
}

# ── 生成 SBOM ─────────────────────────────────────────────
generate_sbom() {
    local format="$1" output="$2" exclude_dev="$3" timeout_sec="$4"

    # 验证格式
    case "$format" in
        spdx-json|cyclonedx-json|spdx-tag-value) ;;
        *)
            log_error "不支持的格式: $format"; exit 1 ;;
    esac

    validate_path_safe "$output"
    local out_dir
    out_dir=$(dirname "$output")
    mkdir -p "$out_dir" || { log_error "创建输出目录失败: $out_dir"; exit 1; }

    # 构建 syft 参数数组
    local syft_args=("$PROJECT_ROOT" "-o" "$format" "--file" "$output")
    if [[ "${exclude_dev:-false}" == "true" ]]; then
        syft_args+=("--exclude-dev")
    fi

    log_info "SBOM 生成开始 (格式: $format, 超时: ${timeout_sec}s)"

    local syft_stderr
    syft_stderr=$(mktemp) || exit 1
    register_file_cleanup "$syft_stderr"

    local syft_rc=0
    # 使用 timeout 命令（若可用）
    if command -v timeout &>/dev/null; then
        timeout "$timeout_sec" syft "${syft_args[@]}" 2>"$syft_stderr" || syft_rc=$?
    else
        syft "${syft_args[@]}" 2>"$syft_stderr" || syft_rc=$?
    fi

    if [[ $syft_rc -ne 0 ]]; then
        log_error "syft 扫描失败 (退出码 $syft_rc):"
        cat "$syft_stderr" >&2
        exit 2
    fi

    # 验证输出
    if [[ ! -s "$output" ]]; then
        log_error "SBOM 文件为空"; exit 2
    fi

    local file_size
    file_size=$(stat -c%s "$output" 2>/dev/null || stat -f%z "$output" 2>/dev/null || echo 0)
    if [[ "$file_size" -gt $((SBOM_MAX_SIZE_MB * 1024 * 1024)) ]]; then
        log_error "SBOM 文件过大: $file_size 字节 (限制 ${SBOM_MAX_SIZE_MB}MB)"; exit 2
    fi

    # JSON 格式校验
    if [[ "$format" == *json* ]]; then
        if command -v python3 &>/dev/null; then
            python3 -c "
import json, sys
try:
    with open(sys.argv[1], 'r') as f:
        json.load(f)
except Exception as e:
    sys.exit(f'JSON 校验失败: {e}')
" "$output" || { log_error "SBOM JSON 结构无效"; exit 2; }
        fi
    fi

    # 安全权限
    chmod 0640 "$output" 2>/dev/null || true

    # 注入元数据（若 jq 可用）
    if command -v jq &>/dev/null; then
        local meta_tmp
        meta_tmp=$(mktemp) || exit 1
        register_file_cleanup "$meta_tmp"
        jq \
            --arg script "$SCRIPT_NAME" \
            --arg version "$VERSION" \
            --arg host "$HOST_FINGERPRINT" \
            --arg ts "$SCRIPT_START_TIME" \
            --arg ci "${CI_JOB_ID:-local}" \
            '.creationInfo.creators += ["Tool: spark-sbom-generator/\($version)"] |
             .creationInfo.created = $ts |
             .creationInfo.comment = "Host: \($host) CI: \($ci)"' \
            "$output" > "$meta_tmp" 2>/dev/null && mv "$meta_tmp" "$output"
        log_info "SBOM 元数据注入完成"
    else
        log_warn "jq 不可用，跳过元数据注入"
    fi

    log_info "SBOM 生成成功: $output ($file_size bytes)"
}

# ── 自洽性验证 ────────────────────────────────────────────
verify_sbom_self_consistency() {
    local sbom_file="$1"
    if command -v syft &>/dev/null; then
        log_info "执行 SBOM 自洽性验证 ..."
        if syft convert "$sbom_file" -o json >/dev/null 2>&1; then
            log_info "SBOM 自洽性验证通过"
        else
            log_warn "SBOM 自洽性验证失败，syft 无法解析自身输出"
        fi
    fi
}

# ── GPG 签名 ──────────────────────────────────────────────
sign_sbom() {
    local sbom_file="$1"
    require_tool gpg
    # 检查是否有可用密钥
    if ! gpg --list-secret-keys --with-colons 2>/dev/null | grep -q '^sec:'; then
        log_error "未找到 GPG 私钥，无法签名"; exit 1
    fi
    log_info "对 SBOM 进行 GPG 签名 ..."
    gpg --detach-sign --armor --batch --yes "$sbom_file" || { log_error "签名失败"; exit 1; }
    chmod 0640 "${sbom_file}.asc" 2>/dev/null || true
    log_info "SBOM 签名完成: ${sbom_file}.asc"
}

# ── 帮助信息 ──────────────────────────────────────────────
show_help() {
    cat <<EOF
火种 SBOM 生成器 v${VERSION}
用法: $SCRIPT_NAME [选项]

选项:
  -o, --output FILE    输出文件路径 (默认: $DEFAULT_OUTPUT)
  -f, --format FORMAT  输出格式: spdx-json, cyclonedx-json, spdx-tag-value
                         (默认: $DEFAULT_FORMAT)
  -e, --exclude-dev    排除开发依赖
  --skip-install       跳过 syft 安装检查（离线模式）
  --sign               使用 GPG 对 SBOM 进行签名
  --timeout SECONDS    设置超时秒数 (默认: $DEFAULT_TIMEOUT_SECONDS)
  -h, --help           显示此帮助
  -v, --version        显示版本

环境变量:
  SYFT_VERSION         指定 syft 版本 (如 v1.0.0)
  CI_JOB_ID            CI 构建 ID (自动注入元数据)

退出码:
  0  成功
  1  参数/环境错误
  2  锁定/扫描失败
  3  签名失败
EOF
}

# ── 主入口 ────────────────────────────────────────────────
main() {
    init_security

    local format="$DEFAULT_FORMAT"
    local output="$DEFAULT_OUTPUT"
    local exclude_dev=false
    local skip_install=false
    local sign_output=false
    local timeout_sec="$DEFAULT_TIMEOUT_SECONDS"

    while [[ $# -gt 0 ]]; do
        case "$1" in
            -o|--output)     output="$2"; shift 2 ;;
            -f|--format)     format="$2"; shift 2 ;;
            -e|--exclude-dev) exclude_dev=true; shift ;;
            --skip-install)  skip_install=true; shift ;;
            --sign)          sign_output=true; shift ;;
            --timeout)       timeout_sec="$2"; shift 2 ;;
            -h|--help)       show_help; exit 0 ;;
            -v|--version)    echo "v$VERSION"; exit 0 ;;
            *)               log_error "未知选项: $1"; show_help; exit 1 ;;
        esac
    done

    # 验证超时为数字
    if [[ ! "$timeout_sec" =~ ^[0-9]+$ ]] || [[ "$timeout_sec" -lt 1 ]]; then
        log_error "超时必须为正整数: $timeout_sec"; exit 1
    fi

    # 安全校验
    validate_path_safe "$PROJECT_ROOT"
    acquire_lock

    # 确保 syft 可用
    if ! $skip_install; then
        ensure_syft
    else
        require_tool syft
    fi

    # 生成 SBOM
    generate_sbom "$format" "$output" "$exclude_dev" "$timeout_sec"

    # 自洽性验证
    verify_sbom_self_consistency "$output"

    # 签名（可选）
    if $sign_output; then
        sign_sbom "$output"
    fi

    log_info "所有操作完成"
    exit 0
}

main "$@"
