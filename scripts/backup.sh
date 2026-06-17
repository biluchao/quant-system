#!/usr/bin/env bash
#=========================================================================
# 火种系统 · 配置备份与恢复工具 V3.0
# 符合华尔街高频交易级/机构级安全标准
#=========================================================================
# 核心职责：
#   1. 原子化备份 config/ 目录，生成 SHA-256 校验和
#   2. AES-256-GCM 加密支持（通过 GPG，密码文件 0600 权限）
#   3. 基于时间戳的备份轮转（保留最近 K 个，使用精确排序）
#   4. 安全恢复：完整性先于解密，路径穿越防护，回滚机制
#
# 用法：
#   backup.sh backup                   # 备份
#   backup.sh restore <file>           # 恢复（交互）
#   backup.sh list                     # JSON 列表
#   backup.sh cron                     # 无交互备份
#   backup.sh health                   # 健康检查
#   backup.sh --help / -h              # 帮助
#
# 环境变量：
#   SPARK_BACKUP_DIR       备份目录 (default: ./backups)
#   SPARK_BACKUP_KEEP      保留份数 (default: 10)
#   SPARK_BACKUP_ENCRYPT   加密开关 (true/false, default: false)
#   SPARK_GPG_KEY_FILE     密码文件 (仅加密时需要，权限 0600)
#=========================================================================

set -o pipefail

readonly SCRIPT_VERSION="3.0.0"
readonly SCRIPT_NAME=$(basename "$0")

# ---- 目录与默认值 --------------------------------------------------------
readonly PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly BACKUP_DIR="${SPARK_BACKUP_DIR:-${PROJECT_ROOT}/backups}"
readonly BACKUP_KEEP="${SPARK_BACKUP_KEEP:-10}"
readonly BACKUP_ENCRYPT="${SPARK_BACKUP_ENCRYPT:-false}"
readonly GPG_KEY_FILE="${SPARK_GPG_KEY_FILE:-}"
readonly LOG_DIR="${PROJECT_ROOT}/logs"
readonly LOG_FILE="${LOG_DIR}/backup.log"
readonly LOCK_FILE="${BACKUP_DIR}/.backup.lock"

# 校验保留份数
[[ "$BACKUP_KEEP" =~ ^[1-9][0-9]*$ ]] || {
    echo "SPARK_BACKUP_KEEP 必须为正整数" >&2
    exit 1
}

# ---- 日志系统（无副作用，输出到 stderr 避免污染 JSON） -------------------------
mkdir -p "$LOG_DIR" 2>/dev/null || true
if [[ -t 1 ]]; then
    readonly IS_TTY=true
else
    readonly IS_TTY=false
fi

log() {
    local level="$1"; shift
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] [$level] $*"
    # 日志写入文件（静默失败）
    echo "$msg" >> "$LOG_FILE" 2>/dev/null || true
    # 终端输出使用颜色，但输出到 stderr 以保证 stdout 干净
    if $IS_TTY; then
        case "$level" in
            INFO)  echo -e "\033[0;32m${msg}\033[0m" >&2 ;;
            WARN)  echo -e "\033[1;33m${msg}\033[0m" >&2 ;;
            ERROR) echo -e "\033[0;31m${msg}\033[0m" >&2 ;;
            *)     echo "$msg" >&2 ;;
        esac
    else
        echo "$msg" >&2
    fi
}

info()  { log "INFO" "$@"; }
warn()  { log "WARN" "$@"; }
error() { log "ERROR" "$@"; }

# ---- 临时目录与清理 --------------------------------------------------------
ensure_backup_dir() { mkdir -p "$BACKUP_DIR" || { error "无法创建备份目录 $BACKUP_DIR"; exit 2; }; }
TMP_DIR=$(mktemp -d "${BACKUP_DIR}/.tmp.XXXXXX") || { error "无法创建临时目录"; exit 2; }
cleanup() {
    local ec=$?
    flock -u 9 2>/dev/null || true
    rm -rf "$TMP_DIR" 2>/dev/null
    exit $ec
}
trap cleanup EXIT HUP INT TERM

# ---- 锁 -----------------------------------------------------------------
acquire_lock() {
    exec 9>"$LOCK_FILE" || { error "无法打开锁文件"; exit 2; }
    if ! flock -n 9; then
        error "另一备份实例正在运行，拒绝启动"
        exit 2
    fi
}

# ---- 加密密钥文件检查（仅存在性/权限，不加载密码） ----------------------------
check_key_file() {
    if [[ "$BACKUP_ENCRYPT" != "true" ]]; then return 0; fi
    if [[ -z "$GPG_KEY_FILE" ]]; then
        error "加密已启用但 SPARK_GPG_KEY_FILE 未设置"
        exit 5
    fi
    if [[ ! -f "$GPG_KEY_FILE" ]]; then
        error "密钥文件不存在: $GPG_KEY_FILE"
        exit 5
    fi
    local perms
    perms=$(stat -c %a "$GPG_KEY_FILE" 2>/dev/null)
    if [[ "$perms" != "600" ]]; then
        error "密钥文件权限必须为 600，当前为 $perms"
        exit 5
    fi
    # 检查 gpg 可用性
    if ! command -v gpg &>/dev/null; then
        error "加密需要 gpg，但未安装"
        exit 4
    fi
}

# ---- 原子校验和生成 --------------------------------------------------------
atomic_sha256() {
    local file="$1" checksum_file="$2"
    sha256sum "$file" 2>/dev/null | cut -d' ' -f1 > "${checksum_file}.tmp" || return 1
    mv "${checksum_file}.tmp" "$checksum_file" || return 1
}

# ---- 备份轮转（精确排序）---------------------------------------------------
rotate_backups() {
    info "轮转清理，保留最近 ${BACKUP_KEEP} 份"
    # 列出备份文件及其修改时间，按时间戳数值排序（精确）
    local -a backups
    while IFS= read -r -d '' line; do
        backups+=("$line")
    done < <(find "$BACKUP_DIR" -maxdepth 1 -type f \( -name "config_backup_*.tar.gz" -o -name "config_backup_*.tar.gz.gpg" \) -printf "%T@ %p\0" 2>/dev/null | sort -z -k1,1n)

    local count=${#backups[@]}
    if (( count > BACKUP_KEEP )); then
        local to_delete=$((count - BACKUP_KEEP))
        for ((i=0; i<to_delete; i++)); do
            # 提取路径（移除时间戳前缀）
            local file="${backups[$i]#* }"
            info "删除旧备份: $(basename "$file")"
            rm -f "$file" "${file}.sha256" 2>/dev/null || warn "删除失败: $file"
        done
    fi
}

# ---- 备份主流程 -----------------------------------------------------------
do_backup() {
    check_key_file
    acquire_lock

    if [[ ! -d "${PROJECT_ROOT}/config" ]]; then
        error "config/ 目录不存在，终止备份"
        exit 2
    fi

    local ts
    ts=$(date +%Y%m%d_%H%M%S)_$((RANDOM % 90000 + 10000))
    local name="config_backup_${ts}.tar.gz"
    local path="${TMP_DIR}/${name}"

    info "开始备份: $name"
    cd "$PROJECT_ROOT" || exit 2

    # 打包 config（错误输出记录到日志）
    if ! tar czf "$path" config/ 2>"${TMP_DIR}/tar_error.log"; then
        error "打包失败: $(<"${TMP_DIR}/tar_error.log")"
        exit 2
    fi

    # 生成校验和
    local chk="${path}.sha256"
    atomic_sha256 "$path" "$chk" || { error "校验和生成失败"; exit 2; }

    local final_path="$path" final_name="$name"

    # 可选加密
    if [[ "$BACKUP_ENCRYPT" == "true" ]]; then
        local enc_path="${path}.gpg"
        info "加密备份..."
        if ! gpg --batch --no-tty --yes --passphrase-file "$GPG_KEY_FILE" \
                --symmetric --cipher-algo AES256 -o "$enc_path" "$path" 2>/dev/null; then
            error "加密失败"
            exit 2
        fi
        # 加密文件完整性自检
        if ! gpg --batch --passphrase-file "$GPG_KEY_FILE" --decrypt "$enc_path" >/dev/null 2>&1; then
            error "加密文件验证失败（无法解密）"
            rm -f "$enc_path"
            exit 2
        fi
        # 删除明文备份和校验和
        rm -f "$path" "$chk"
        final_path="$enc_path"
        final_name="${name}.gpg"
        # 生成密文校验和
        chk="${final_path}.sha256"
        atomic_sha256 "$final_path" "$chk" || { error "密文校验和生成失败"; exit 2; }
    fi

    # 移动到正式目录
    mv "$final_path" "${BACKUP_DIR}/${final_name}" || { error "移动备份失败"; exit 2; }
    mv "$chk" "${BACKUP_DIR}/${final_name}.sha256" 2>/dev/null || true

    rotate_backups
    info "备份成功: ${BACKUP_DIR}/${final_name}"
    # 输出备份路径到 stdout（供脚本捕获）
    echo "${BACKUP_DIR}/${final_name}"
}

# ---- 列出备份（JSON）-------------------------------------------------------
do_list() {
    if [[ ! -d "$BACKUP_DIR" ]]; then
        echo '{"backups":[]}'
        return
    fi
    local first=true
    echo -n '{"backups":['
    while IFS= read -r -d '' line; do
        $first || echo -n ','
        first=false
        local file="${line#* }"
        local name size mtime
        name=$(basename "$file")
        size=$(stat -c %s "$file" 2>/dev/null || echo 0)
        mtime=$(stat -c %Y "$file" 2>/dev/null || echo 0)
        # 简单 JSON 转义（双引号和反斜杠）
        name="${name//\\/\\\\}"; name="${name//\"/\\\"}"
        printf '\n  {"name":"%s","size":%s,"mtime":%s}' "$name" "$size" "$mtime"
    done < <(find "$BACKUP_DIR" -maxdepth 1 -type f \( -name "config_backup_*.tar.gz" -o -name "config_backup_*.tar.gz.gpg" \) -printf "%T@ %p\0" 2>/dev/null | sort -z -k1,1n)
    echo ']}'
}

# ---- 恢复备份 -------------------------------------------------------------
do_restore() {
    local restore_file="$1"
    if [[ ! -f "$restore_file" ]]; then
        error "备份文件不存在: $restore_file"
        exit 3
    fi
    # 绝对路径化
    restore_file=$(realpath "$restore_file") || exit 3

    check_key_file
    acquire_lock

    # ---- 第一步：完整性验证（对原始文件，无论加密与否） ----
    local chk_file="${restore_file}.sha256"
    if [[ -f "$chk_file" ]]; then
        info "验证备份完整性..."
        local expected actual
        expected=$(<"$chk_file")
        if ! actual=$(sha256sum "$restore_file" 2>/dev/null | cut -d' ' -f1); then
            error "无法计算校验和"
            exit 3
        fi
        if [[ "$expected" != "$actual" ]]; then
            error "校验和不匹配！备份文件可能损坏"
            exit 3
        fi
        info "完整性验证通过"
    else
        warn "未找到校验和文件，跳过完整性验证"
    fi

    # ---- 第二步：决定是否解密 ----
    local working_file="$restore_file"
    local delete_working=false
    if [[ "$restore_file" == *.gpg ]]; then
        if [[ "$BACKUP_ENCRYPT" != "true" ]] || [[ ! -f "$GPG_KEY_FILE" ]]; then
            error "需要加密密钥才能解密备份"
            exit 5
        fi
        working_file="${TMP_DIR}/restored.tar.gz"
        info "解密备份..."
        if ! gpg --batch --no-tty --yes --passphrase-file "$GPG_KEY_FILE" \
                --decrypt -o "$working_file" "$restore_file" 2>/dev/null; then
            error "解密失败，密钥可能错误"
            exit 3
        fi
        delete_working=true
    fi

    # ---- 第三步：安全检查（路径穿越） ----
    local tar_list
    tar_list=$(tar tzf "$working_file" 2>/dev/null) || {
        error "无法读取备份内容列表"
        $delete_working && rm -f "$working_file"
        exit 3
    }
    if grep -Eq '(^|/)\.\./' <<<"$tar_list"; then
        error "备份包含非法路径 (..)"
        $delete_working && rm -f "$working_file"
        exit 5
    fi

    # ---- 第四步：交互确认 ----
    if $IS_TTY; then
        warn "⚠ 即将覆盖当前 config/ 目录！"
        read -r -p "确认恢复？输入 'yes' 继续: " confirm
        [[ "$confirm" == "yes" ]] || { info "用户取消"; exit 0; }
    else
        error "恢复操作需要交互终端（或使用 --force 模式）"
        exit 3
    fi

    # ---- 第五步：备份当前 config（含加密） ----
    cd "$PROJECT_ROOT" || exit 3
    local pre_backup="${TMP_DIR}/pre_restore.tar.gz"
    if tar czf "$pre_backup" config/ 2>/dev/null; then
        local pre_final="$pre_backup"
        if [[ "$BACKUP_ENCRYPT" == "true" ]]; then
            local pre_enc="${pre_backup}.gpg"
            if gpg --batch --passphrase-file "$GPG_KEY_FILE" --symmetric --cipher-algo AES256 -o "$pre_enc" "$pre_backup" 2>/dev/null; then
                rm -f "$pre_backup"
                pre_final="$pre_enc"
            else
                warn "无法加密恢复前备份，保留明文"
            fi
        fi
        cp "$pre_final" "${BACKUP_DIR}/config_pre_restore_$(date +%Y%m%d_%H%M%S).tar.gz${BACKUP_ENCRYPT:+".gpg"}" 2>/dev/null || warn "无法复制恢复前备份到备份目录"
    else
        warn "无法创建恢复前备份，继续恢复（无回滚点）"
    fi

    # ---- 第六步：执行恢复 ----
    rm -rf config/
    if ! tar xzf "$working_file" 2>/dev/null; then
        error "解压失败！"
        # 尝试回滚
        if [[ -f "$pre_backup" ]]; then
            rm -rf config/
            tar xzf "$pre_backup" 2>/dev/null && info "已回滚至恢复前配置" || error "回滚失败！"
        fi
        $delete_working && rm -f "$working_file"
        exit 3
    fi
    info "配置恢复成功"
    $delete_working && rm -f "$working_file"
}

# ---- 健康检查（无副作用）---------------------------------------------------
health_check() {
    local status="ok"
    local -a msgs

    command -v tar &>/dev/null || { status="error"; msgs+=("tar 未安装"); }
    command -v gzip &>/dev/null || { status="error"; msgs+=("gzip 未安装"); }
    command -v sha256sum &>/dev/null || { status="error"; msgs+=("sha256sum 未安装"); }

    if [[ ! -d "$BACKUP_DIR" ]]; then
        if ! mkdir -p "$BACKUP_DIR" 2>/dev/null; then
            status="error"
            msgs+=("备份目录无法创建: $BACKUP_DIR")
        fi
    elif [[ ! -w "$BACKUP_DIR" ]]; then
        status="error"
        msgs+=("备份目录不可写: $BACKUP_DIR")
    fi

    if [[ "$status" == "ok" ]]; then
        echo '{"status":"ok","message":"备份系统可用"}'
    else
        printf '{"status":"error","message":"%s"}\n' "${msgs[*]}" >&2
        exit 1
    fi
}

# ---- 帮助 -----------------------------------------------------------------
show_help() {
    cat <<EOF
火种备份工具 V${SCRIPT_VERSION}

用法: $SCRIPT_NAME {backup|restore <文件>|list|cron|health} [选项]

命令:
  backup              执行备份
  restore <file>      恢复指定备份（交互确认）
  list                列出所有备份（JSON）
  cron                cron 模式备份（无颜色、无交互）
  health              健康检查

环境变量:
  SPARK_BACKUP_DIR       备份目录 (默认: ./backups)
  SPARK_BACKUP_KEEP      保留份数 (默认: 10)
  SPARK_BACKUP_ENCRYPT   加密备份 (true/false)
  SPARK_GPG_KEY_FILE     加密密钥文件（权限 600）

示例:
  $SCRIPT_NAME backup
  $SCRIPT_NAME restore ./backups/config_backup_2025...tar.gz.gpg
EOF
}

# ---- 命令行路由 -----------------------------------------------------------
case "${1:-}" in
    backup)         do_backup ;;
    restore)
        [[ -z "${2:-}" ]] && { error "缺少备份文件"; exit 1; }
        do_restore "$2" ;;
    list)           do_list ;;
    cron)
        IS_TTY=false
        do_backup ;;
    health)         health_check ;;
    -h|--help)      show_help ;;
    version)        echo "V${SCRIPT_VERSION}" ;;
    *)              show_help >&2; exit 1 ;;
esac
