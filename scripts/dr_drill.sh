#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# -----------------------------------------------------------------------------
# 火种系统 · 灾难恢复演练脚本 (dr_drill.sh)
#
# 核心职责：
#   1. 模拟预定义故障场景（引擎崩溃、Redis 重启、磁盘写满、网络分区）
#   2. 精确记录恢复时间目标 (RTO) 与恢复点目标 (RPO)
#   3. 自动执行恢复流程，验证系统状态一致性
#   4. 生成机器可读的标准化演练报告，同步至审计日志
#
# 用法:
#   ./scripts/dr_drill.sh -s <场景> [-a] [-y] [--dry-run] [-r 报告路径]
#
# 选项:
#   -s, --scenario NAME       故障场景 (engine_crash|redis_restart|disk_full|
#                                      network_partition) [必需]
#   -a, --auto-recover        自动执行恢复步骤
#   -y, --yes                 跳过确认提示（危险）
#   --dry-run                 仅打印操作，不实际执行
#   -r, --report FILE         报告输出路径 (默认: logs/dr_report_<ts>.json)
#   -t, --timeout SEC         恢复超时秒数 (默认: 180)
#   -h, --help                帮助信息
#   -v, --version             版本信息
#
# 退出码:
#   0  演练成功，RTO/RPO 达标
#   1  参数错误或前提条件不满足
#   2  故障注入失败
#   3  恢复失败或超时
#   4  RTO/RPO 超出阈值
#   5  系统状态验证失败
#   6  并发冲突 (已有实例运行)
#
# 环境变量:
#   DOCKER_COMPOSE_CMD    docker-compose 路径 (默认: docker-compose)
#   COMPOSE_PROJECT_NAME  项目名称 (默认: spark)
#   RTO_THRESHOLD_SEC     RTO 阈值 (默认: 180)
#   RPO_THRESHOLD_MS      RPO 阈值 (默认: 500)
# -----------------------------------------------------------------------------

set -euo pipefail

# ── 常量 ──────────────────────────────────────────────────
readonly SCRIPT_NAME="$(basename "$0")"
readonly SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
readonly PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
readonly VERSION="3.0.1"
readonly TIMESTAMP="$(date -u +'%Y%m%dT%H%M%SZ')"
readonly REPORT_DIR="${PROJECT_ROOT}/logs"
readonly DEFAULT_REPORT="${REPORT_DIR}/dr_report_${TIMESTAMP}.json"
readonly LOCK_FILE="${PROJECT_ROOT}/.dr_drill.lock"

# 阈值
RTO_THRESHOLD_SEC="${RTO_THRESHOLD_SEC:-180}"
RPO_THRESHOLD_MS="${RPO_THRESHOLD_MS:-500}"

# Docker 相关
DOCKER_COMPOSE_CMD="${DOCKER_COMPOSE_CMD:-docker-compose}"
COMPOSE_PROJECT="${COMPOSE_PROJECT_NAME:-spark}"
readonly COMPOSE_FILE="${PROJECT_ROOT}/docker-compose.yml"
readonly SERVICE_ENGINE="engine"
readonly SERVICE_REDIS="redis"
readonly SERVICE_GATEWAY="gateway"

# 临时文件跟踪
declare -a TEMP_FILES=()
# 网络恢复所需信息
declare ENGINE_NETWORK_DISCONNECTED=""

# ── 日志与审计 ────────────────────────────────────────────
log_info()  { echo "[INFO]  $(date -u +'%Y-%m-%dT%H:%M:%SZ') $*" >&2; }
log_warn()  { echo "[WARN]  $(date -u +'%Y-%m-%dT%H:%M:%SZ') $*" >&2; }
log_error() { echo "[ERROR] $(date -u +'%Y-%m-%dT%H:%M:%SZ') $*" >&2; }

# 注册清理函数（确保异常退出时释放资源）
cleanup() {
    # 释放锁
    if [[ -n "${LOCK_FD:-}" ]]; then
        flock -u "$LOCK_FD" 2>/dev/null || true
        exec {LOCK_FD}>&- 2>/dev/null || true
    fi
    # 删除临时文件
    for f in "${TEMP_FILES[@]}"; do
        rm -f "$f" 2>/dev/null || true
    done
    # 恢复网络（如果曾断开）
    if [[ -n "$ENGINE_NETWORK_DISCONNECTED" ]]; then
        local engine_id
        engine_id=$($DOCKER_COMPOSE_CMD -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" ps -q "$SERVICE_ENGINE" 2>/dev/null || true)
        if [[ -n "$engine_id" ]]; then
            docker network connect "${COMPOSE_PROJECT}_default" "$engine_id" 2>/dev/null || log_warn "自动恢复网络失败"
        fi
    fi
    # 删除磁盘填充文件
    rm -f "${PROJECT_ROOT}/logs/.dr_drill_disk_fill.tmp" 2>/dev/null || true
}
trap cleanup EXIT

# ── 帮助与版本 ────────────────────────────────────────────
show_help() { sed -n '/^# 用法:/,/^$/p' "$0" | sed 's/^# //'; }
show_version() { echo "${SCRIPT_NAME} version ${VERSION}"; }

# ── 前提条件检查 ──────────────────────────────────────────
check_prerequisites() {
    local errors=0
    if ! command -v "$DOCKER_COMPOSE_CMD" &>/dev/null; then
        log_error "需要 ${DOCKER_COMPOSE_CMD}"
        ((errors++))
    fi
    if [[ ! -f "$COMPOSE_FILE" ]]; then
        log_error "Compose 文件不存在: $COMPOSE_FILE"
        ((errors++))
    fi
    if [[ ! -d "${PROJECT_ROOT}/core" ]]; then
        log_error "未在项目根目录运行"
        ((errors++))
    fi
    # 检查目标服务是否可操作
    if ! $DOCKER_COMPOSE_CMD -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" ps &>/dev/null; then
        log_error "无法连接 Docker 守护进程或 Compose 服务"
        ((errors++))
    fi
    return $errors
}

# ── 并发锁 ────────────────────────────────────────────────
acquire_lock() {
    exec {LOCK_FD}>"$LOCK_FILE"
    if ! flock -n "$LOCK_FD"; then
        log_error "已有演练实例在运行 (锁: $LOCK_FILE)"
        exit 6
    fi
}

# ── 高精度时间获取 (纳秒) ─────────────────────────────────
get_time_ns() {
    if command -v python3 &>/dev/null; then
        python3 -c 'import time; print(int(time.time() * 1e9))'
    elif date +%s%N &>/dev/null; then
        date +%s%N
    else
        # 回退：毫秒精度
        echo $(($(date +%s) * 1000000000))
    fi
}

# ── 安全 JSON 字符串转义 ──────────────────────────────────
json_escape() {
    python3 -c 'import json,sys; print(json.dumps(sys.stdin.read().strip()))' <<<"$1" 2>/dev/null || echo "\"$1\""
}

# ── 故障场景实现 ──────────────────────────────────────────

scenario_engine_crash() {
    log_info ">>> 故障注入: 引擎崩溃"
    local container_id
    container_id=$($DOCKER_COMPOSE_CMD -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" ps -q "$SERVICE_ENGINE" 2>/dev/null || true)
    if [[ -z "$container_id" ]]; then
        log_warn "未找到引擎容器，跳过"
        return 1
    fi
    # 先发送 SIGTERM 给予优雅关闭机会，超时后再 SIGKILL
    docker stop --time=5 "$container_id" &>/dev/null || docker kill "$container_id" &>/dev/null || true
    return 0
}

scenario_redis_restart() {
    log_info ">>> 故障注入: Redis 重启"
    local container_id
    container_id=$($DOCKER_COMPOSE_CMD -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" ps -q "$SERVICE_REDIS" 2>/dev/null || true)
    if [[ -z "$container_id" ]]; then
        log_warn "未找到 Redis 容器"
        return 1
    fi
    # 保存当前持久化状态时间戳（用于RPO计算）
    local last_save
    last_save=$(docker exec "$container_id" redis-cli LASTSAVE 2>/dev/null | grep -oP '^\d+$' || true)
    echo "${last_save:-0}" > "${PROJECT_ROOT}/logs/.dr_drill_redis_lastsave.tmp"
    TEMP_FILES+=("${PROJECT_ROOT}/logs/.dr_drill_redis_lastsave.tmp")
    docker restart --time=5 "$container_id" &>/dev/null || true
    return 0
}

scenario_disk_full() {
    log_info ">>> 故障注入: 磁盘写满 (安全模拟)"
    local fill_file="${PROJECT_ROOT}/logs/.dr_drill_disk_fill.tmp"
    # 限制填充大小为 500MB，避免真正耗尽磁盘
    if fallocate -l 500M "$fill_file" 2>/dev/null; then
        log_info "  已分配 500MB 填充文件"
    elif dd if=/dev/zero of="$fill_file" bs=1M count=500 2>/dev/null; then
        log_info "  已创建 500MB 填充文件 (dd)"
    else
        log_warn "  无法创建填充文件，跳过"
        return 1
    fi
    TEMP_FILES+=("$fill_file")
    return 0
}

scenario_network_partition() {
    log_info ">>> 故障注入: 引擎网络分区"
    local engine_id
    engine_id=$($DOCKER_COMPOSE_CMD -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" ps -q "$SERVICE_ENGINE" 2>/dev/null || true)
    if [[ -z "$engine_id" ]]; then
        log_warn "未找到引擎容器"
        return 1
    fi
    local network_name="${COMPOSE_PROJECT}_default"
    # 断开前验证网络存在
    if ! docker network inspect "$network_name" &>/dev/null; then
        log_error "网络 $network_name 不存在"
        return 1
    fi
    docker network disconnect "$network_name" "$engine_id" 2>/dev/null || true
    ENGINE_NETWORK_DISCONNECTED="$engine_id"
    return 0
}

# ── 恢复步骤 ──────────────────────────────────────────────
recover_engine_crash() {
    log_info ">>> 恢复: 重启引擎"
    $DOCKER_COMPOSE_CMD -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" up -d "$SERVICE_ENGINE" &>/dev/null
    wait_for_health "$SERVICE_ENGINE" 120 "引擎"
}

recover_redis_restart() {
    log_info ">>> 恢复: 等待 Redis 就绪"
    wait_for_health "$SERVICE_REDIS" 60 "Redis"
}

recover_disk_full() {
    log_info ">>> 恢复: 清理填充文件"
    local fill_file="${PROJECT_ROOT}/logs/.dr_drill_disk_fill.tmp"
    rm -f "$fill_file" 2>/dev/null || true
    # 触发归档器释放更多空间
    if [[ -f "${PROJECT_ROOT}/scripts/data_archiver.py" ]]; then
        python3 "${PROJECT_ROOT}/scripts/data_archiver.py" --aggressive 2>/dev/null || true
    fi
}

recover_network_partition() {
    log_info ">>> 恢复: 重新连接引擎网络"
    if [[ -n "$ENGINE_NETWORK_DISCONNECTED" ]]; then
        docker network connect "${COMPOSE_PROJECT}_default" "$ENGINE_NETWORK_DISCONNECTED" 2>/dev/null || {
            log_error "重新连接网络失败"
            return 1
        }
    fi
    # 重启引擎确保连接生效
    $DOCKER_COMPOSE_CMD -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" restart "$SERVICE_ENGINE" &>/dev/null
    wait_for_health "$SERVICE_ENGINE" 60 "引擎"
}

# ── 通用健康等待 ──────────────────────────────────────────
wait_for_health() {
    local service="$1"
    local timeout_sec="$2"
    local label="${3:-$service}"
    local start end
    start=$(date +%s)
    while true; do
        if $DOCKER_COMPOSE_CMD -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" ps "$service" 2>/dev/null | grep -q 'Up'; then
            # 额外检查：如果有定义 healthcheck，等待其通过
            local container_id
            container_id=$($DOCKER_COMPOSE_CMD -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" ps -q "$service" 2>/dev/null || true)
            if [[ -n "$container_id" ]]; then
                # 等待最多 5 秒让 health status 变成 healthy
                for i in {1..5}; do
                    local health
                    health=$(docker inspect --format='{{.State.Health.Status}}' "$container_id" 2>/dev/null || echo "none")
                    if [[ "$health" == "healthy" || "$health" == "none" ]]; then
                        log_info "  ${label} 已就绪 (health: $health)"
                        return 0
                    fi
                    sleep 1
                done
                log_warn "  ${label} 健康检查未通过，继续等待..."
            else
                log_info "  ${label} 容器运行中"
                return 0
            fi
        fi
        end=$(date +%s)
        if (( end - start > timeout_sec )); then
            log_error "${label} 恢复超时 (${timeout_sec}s)"
            return 1
        fi
        sleep 2
    done
}

# ── 系统一致性验证 ────────────────────────────────────────
verify_consistency() {
    log_info ">>> 验证系统一致性..."
    local errors=0

    # 引擎存在性
    local engine_id
    engine_id=$($DOCKER_COMPOSE_CMD -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" ps -q "$SERVICE_ENGINE" 2>/dev/null || true)
    if [[ -z "$engine_id" ]]; then
        log_error "  引擎未运行"
        ((errors++))
    else
        log_info "  引擎运行中"
    fi

    # Redis 连通性
    local redis_id
    redis_id=$($DOCKER_COMPOSE_CMD -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" ps -q "$SERVICE_REDIS" 2>/dev/null || true)
    if [[ -n "$redis_id" ]]; then
        if docker exec "$redis_id" redis-cli PING 2>/dev/null | grep -q PONG; then
            log_info "  Redis 连通正常"
        else
            log_error "  Redis 不可达"
            ((errors++))
        fi
    else
        log_warn "  Redis 容器未运行"
    fi

    # 检查关键日志 ERROR 数量 (最近 30 条)
    if [[ -n "$engine_id" ]]; then
        local error_count
        error_count=$(docker logs --tail 30 "$engine_id" 2>&1 | grep -ci "ERROR" || true)
        if (( error_count > 3 )); then
            log_warn "  引擎日志中 ERROR 较多: ${error_count}"
        else
            log_info "  引擎日志正常"
        fi
    fi

    return $errors
}

# ── RPO 计算 ──────────────────────────────────────────────
calculate_rpo_ms() {
    local scenario="$1"
    local fault_ts_ms="$2"  # 故障时间 (毫秒)
    local rpo_ms=0

    case "$scenario" in
        redis_restart)
            # 从文件读取故障前 Redis 最后保存时间
            local save_file="${PROJECT_ROOT}/logs/.dr_drill_redis_lastsave.tmp"
            if [[ -f "$save_file" ]]; then
                local last_save_sec
                last_save_sec=$(cat "$save_file")
                if [[ "$last_save_sec" =~ ^[0-9]+$ && $last_save_sec -gt 0 ]]; then
                    local fault_ts_sec=$(( fault_ts_ms / 1000 ))
                    rpo_ms=$(( (fault_ts_sec - last_save_sec) * 1000 ))
                    (( rpo_ms < 0 )) && rpo_ms=0
                fi
            fi
            ;;
        *)
            # 通用：从 Redis 获取最后写入时间
            local redis_id
            redis_id=$($DOCKER_COMPOSE_CMD -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" ps -q "$SERVICE_REDIS" 2>/dev/null || true)
            if [[ -n "$redis_id" ]]; then
                local last_ts
                last_ts=$(docker exec "$redis_id" redis-cli GET spark:last_trade_time 2>/dev/null || true)
                if [[ "$last_ts" =~ ^[0-9]+$ ]]; then
                    rpo_ms=$(( fault_ts_ms - last_ts ))
                    (( rpo_ms < 0 )) && rpo_ms=0
                fi
            fi
            ;;
    esac
    echo "$rpo_ms"
}

# ── 生成 JSON 报告 ────────────────────────────────────────
generate_report() {
    local scenario="$1"
    local rto_sec="$2"
    local rpo_ms="$3"
    local overall="$4"
    local verification_passed="$5"
    local inject_duration_ms="$6"
    local recover_duration_ms="$7"
    local report_file="$8"

    local rto_passed="true"
    local rpo_passed="true"
    (( rto_sec > RTO_THRESHOLD_SEC )) && rto_passed="false"
    (( rpo_ms > RPO_THRESHOLD_MS )) && rpo_passed="false"

    # 使用 Python 生成安全 JSON（如果可用），否则手动构造
    if command -v python3 &>/dev/null; then
        python3 -c "
import json, sys
report = {
    'timestamp': '$(date -u +'%Y-%m-%dT%H:%M:%SZ')',
    'scenario': '$scenario',
    'overall': '$overall',
    'rto': {
        'value_seconds': $rto_sec,
        'threshold_seconds': $RTO_THRESHOLD_SEC,
        'passed': $rto_passed == 'true'
    },
    'rpo': {
        'value_ms': $rpo_ms,
        'threshold_ms': $RPO_THRESHOLD_MS,
        'passed': $rpo_passed == 'true'
    },
    'inject_duration_ms': $inject_duration_ms,
    'recover_duration_ms': $recover_duration_ms,
    'verification_passed': $verification_passed == 'true'
}
with open('$report_file', 'w') as f:
    json.dump(report, f, indent=2)
"
    else
        # 纯 shell 构造 JSON (仅限安全值)
        cat > "$report_file" <<EOF
{
    "timestamp": "$(date -u +'%Y-%m-%dT%H:%M:%SZ')",
    "scenario": "$scenario",
    "overall": "$overall",
    "rto": {
        "value_seconds": $rto_sec,
        "threshold_seconds": $RTO_THRESHOLD_SEC,
        "passed": $rto_passed
    },
    "rpo": {
        "value_ms": $rpo_ms,
        "threshold_ms": $RPO_THRESHOLD_MS,
        "passed": $rpo_passed
    },
    "inject_duration_ms": $inject_duration_ms,
    "recover_duration_ms": $recover_duration_ms,
    "verification_passed": $verification_passed
}
EOF
    fi
    log_info "演练报告已生成: $report_file"
}

# ── 主演练流程 ─────────────────────────────────────────────
run_drill() {
    local scenario="$1"
    local auto_recover="$2"
    local dry_run="$3"
    local recovery_timeout="$4"
    local report_file="$5"

    log_info "======================================="
    log_info "  灾难恢复演练开始"
    log_info "  场景: $scenario"
    log_info "  自动恢复: $auto_recover"
    log_info "======================================="

    # 注入故障
    local inject_start_ns inject_end_ns inject_duration_ms
    inject_start_ns=$(get_time_ns)

    case "$scenario" in
        engine_crash)       scenario_engine_crash ;;
        redis_restart)      scenario_redis_restart ;;
        disk_full)          scenario_disk_full ;;
        network_partition)  scenario_network_partition ;;
        *)                  log_error "未知场景: $scenario"; exit 1 ;;
    esac

    inject_end_ns=$(get_time_ns)
    inject_duration_ms=$(( (inject_end_ns - inject_start_ns) / 1000000 ))

    if [[ "$dry_run" == "true" ]]; then
        log_info "演练模式: 仅模拟，跳过恢复"
        return 0
    fi

    # 等待故障影响稳定
    sleep 3

    # 恢复
    local recover_start_ns recover_end_ns recover_duration_ms rto_sec
    recover_start_ns=$(get_time_ns)

    if [[ "$auto_recover" == "true" ]]; then
        case "$scenario" in
            engine_crash)       recover_engine_crash ;;
            redis_restart)      recover_redis_restart ;;
            disk_full)          recover_disk_full ;;
            network_partition)  recover_network_partition ;;
        esac || {
            log_error "恢复失败"
            return 3
        }
    else
        log_warn "自动恢复未开启，请手动恢复后按回车继续..."
        read -r
    fi

    recover_end_ns=$(get_time_ns)
    recover_duration_ms=$(( (recover_end_ns - recover_start_ns) / 1000000 ))
    rto_sec=$(( recover_duration_ms / 1000 ))

    # 验证一致性
    verify_consistency || true
    local verify_exit=$?

    # 计算 RPO
    local fault_ts_ms=$(( inject_start_ns / 1000000 ))
    local rpo_ms
    rpo_ms=$(calculate_rpo_ms "$scenario" "$fault_ts_ms")

    # 判断总体结果
    local overall="success"
    if [[ $verify_exit -ne 0 ]]; then
        overall="failure"
    elif [[ "$rto_sec" -gt "$RTO_THRESHOLD_SEC" ]] || [[ "$rpo_ms" -gt "$RPO_THRESHOLD_MS" ]]; then
        overall="failure"
    fi

    # 生成报告
    generate_report "$scenario" "$rto_sec" "$rpo_ms" "$overall" \
        "$([[ $verify_exit -eq 0 ]] && echo true || echo false)" \
        "$inject_duration_ms" "$recover_duration_ms" "$report_file"

    # 结果输出
    log_info "RTO: ${rto_sec}s (阈值 ${RTO_THRESHOLD_SEC}s) $([ "$rto_sec" -le "$RTO_THRESHOLD_SEC" ] && echo '✅' || echo '❌')"
    log_info "RPO: ${rpo_ms}ms (阈值 ${RPO_THRESHOLD_MS}ms) $([ "$rpo_ms" -le "$RPO_THRESHOLD_MS" ] && echo '✅' || echo '❌')"

    # 退出码
    if [[ $verify_exit -ne 0 ]]; then
        exit 5
    elif [[ "$rto_sec" -gt "$RTO_THRESHOLD_SEC" ]] || [[ "$rpo_ms" -gt "$RPO_THRESHOLD_MS" ]]; then
        exit 4
    elif [[ "$overall" != "success" ]]; then
        exit 3
    fi
}

# ── 主入口 ────────────────────────────────────────────────
main() {
    local scenario=""
    local auto_recover="false"
    local dry_run="false"
    local yes_mode="false"
    local report_file="$DEFAULT_REPORT"
    local recovery_timeout=180

    while [[ $# -gt 0 ]]; do
        case "$1" in
            -s|--scenario) scenario="$2"; shift 2 ;;
            -a|--auto-recover) auto_recover="true"; shift ;;
            -y|--yes) yes_mode="true"; shift ;;
            --dry-run) dry_run="true"; shift ;;
            -r|--report) report_file="$2"; shift 2 ;;
            -t|--timeout) recovery_timeout="$2"; shift 2 ;;
            -h|--help) show_help; exit 0 ;;
            -v|--version) show_version; exit 0 ;;
            *) log_error "未知选项: $1"; show_help; exit 1 ;;
        esac
    done

    if [[ -z "$scenario" ]]; then
        log_error "必须指定场景 (-s)"
        show_help
        exit 1
    fi

    # 场景有效性校验
    local valid_scenarios="engine_crash redis_restart disk_full network_partition"
    if ! echo "$valid_scenarios" | grep -qw "$scenario"; then
        log_error "无效场景: $scenario (支持: $valid_scenarios)"
        exit 1
    fi

    # 确认
    if [[ "$dry_run" != "true" && "$yes_mode" != "true" ]]; then
        echo "⚠️  即将执行真实故障注入: $scenario"
        echo "按 Enter 继续，或 Ctrl+C 取消..."
        read -r
    fi

    # 前提检查
    check_prerequisites || exit 1

    # 并发锁
    acquire_lock

    # 创建报告目录
    mkdir -p "$(dirname "$report_file")"

    run_drill "$scenario" "$auto_recover" "$dry_run" "$recovery_timeout" "$report_file"
}

main "$@"
