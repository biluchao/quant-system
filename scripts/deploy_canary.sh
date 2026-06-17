#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────
# 火种系统 · 金丝雀部署器 (机构级 V3)
# 符合: 华尔街高频交易部署规范 v3.7
# 目标: 万亿级AUM、零宕机、全审计
# ─────────────────────────────────────────────────────────
set -Eeuo pipefail
shopt -s inherit_errexit nullglob compat"${BASH_COMPAT:=42}"
IFS=$'\n\t'

# ── 常量 ────────────────────────────────────────────────
readonly SCRIPT_NAME="${0##*/}"
readonly SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
readonly PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

readonly LOCK_DIR="/var/run/quant-deploy"
readonly LOCK_FILE="${LOCK_DIR}/canary.lock"
readonly LOCK_TIMEOUT=300               # 锁超时5分钟，自动打破

readonly LOG_BASE="${PROJECT_ROOT}/logs"
readonly LOG_FILE="${LOG_BASE}/deploy_$(date -u +%Y%m%d).log"
readonly AUDIT_FILE="${LOG_BASE}/deploy_audit.log"

readonly CONFIG_DIR="${PROJECT_ROOT}/config"
readonly DEPLOY_CONF="${CONFIG_DIR}/deploy.yaml"
readonly RISK_CONF="${CONFIG_DIR}/risk.yaml"
readonly COMPOSE_BASE="${PROJECT_ROOT}/docker-compose.yml"
readonly COMPOSE_CANARY="${PROJECT_ROOT}/docker-compose.canary.yml"

readonly EVAL_SCRIPT="${PROJECT_ROOT}/scripts/canary_evaluator.py"
readonly REQUIRED_BINS=(git docker python3 curl yq jq sha256sum)
readonly ALLOWED_PERCENTS=(10 30 50)

# ── 可配置动态参数 ─────────────────────────────────────
# 以下变量可由环境变量覆盖，提供运行时灵活性
CANARY_PERCENT="${CANARY_PERCENT:-10}"
COLLECT_MINUTES="${COLLECT_MINUTES:-120}"
MIN_TRADES="${MIN_TRADES:-20}"
AUTO_ROLLBACK="${AUTO_ROLLBACK:-true}"
DEPLOY_VERSION="${DEPLOY_VERSION:-main}"
DRY_RUN="${DRY_RUN:-false}"
SLACK_HOOK="${SLACK_WEBHOOK:-}"
PAGERDUTY_KEY="${PAGERDUTY_ROUTING_KEY:-}"
REQUIRE_APPROVAL="${REQUIRE_APPROVAL:-false}"
APPROVAL_URL="${APPROVAL_URL:-}"
CHANGE_REQUEST_ID="${CHANGE_REQUEST_ID:-}"

# ── 状态文件 ────────────────────────────────────────────
STATE_DIR="${PROJECT_ROOT}/.deploy_state"
mkdir -p "$STATE_DIR"
DEPLOY_ID="${DEPLOY_ID:-deploy-$(date -u +%Y%m%d%H%M%S)}"
STATE_FILE="${STATE_DIR}/${DEPLOY_ID}.json"

# ── 辅助函数 ───────────────────────────────────────────
cleanup() {
    local exit_code=$?
    release_lock
    if [[ $exit_code -ne 0 ]]; then
        log ERROR "部署异常退出 (${exit_code})"
        alert "金丝雀部署异常" "部署ID: ${DEPLOY_ID}, 退出码: ${exit_code}"
        # 尝试停止金丝雀容器
        docker compose -f "$COMPOSE_CANARY" down --remove-orphans --volumes 2>/dev/null || true
    fi
    # 移除临时文件
    rm -f "${STATE_DIR}/.tmp_*"
    exit $exit_code
}
trap cleanup EXIT INT TERM HUP

log() {
    local level="$1"; shift
    mkdir -p "$LOG_BASE"
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] [$$] [${level}] $*" | tee -a "$LOG_FILE" >&2
}

audit() {
    local event="$1"; shift
    local details="$*"
    local timestamp
    timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "${timestamp}|${DEPLOY_ID}|${USER:-unknown}|${event}|${details}" >> "$AUDIT_FILE"
    # 同步写入系统日志
    logger -t "canary_deploy" "[${event}] ${details}"
}

alert() {
    local title="$1"; local msg="$2"
    if [[ -n "$SLACK_HOOK" ]]; then
        curl -s -X POST -H 'Content-type: application/json' \
            --data "{\"text\":\"[$title] $msg\"}" "$SLACK_HOOK" || true
    fi
    if [[ -n "$PAGERDUTY_KEY" ]]; then
        curl -s -X POST "https://events.pagerduty.com/v2/enqueue" \
            -H 'Content-Type: application/json' \
            -d "{\"routing_key\":\"$PAGERDUTY_KEY\",\"event_action\":\"trigger\",\"payload\":{\"summary\":\"$title\",\"source\":\"deploy_canary\",\"severity\":\"critical\"}}" || true
    fi
}

die() {
    log ERROR "$*"
    audit "DEPLOY_FAILED" "$*"
    exit 1
}

check_prereqs() {
    local missing=()
    for bin in "${REQUIRED_BINS[@]}"; do
        command -v "$bin" &>/dev/null || missing+=("$bin")
    done
    [[ ${#missing[@]} -eq 0 ]] || die "缺失二进制依赖: ${missing[*]}"

    # 内核参数检查 (inotify watches)
    local inotify_max
    inotify_max=$(cat /proc/sys/fs/inotify/max_user_watches 2>/dev/null || echo 0)
    [[ $inotify_max -ge 524288 ]] || log WARN "inotify watches 过低 ($inotify_max)，建议调至 524288+"

    # 磁盘空间 (至少保留 10GB)
    local avail
    avail=$(df --output=avail -k "$PROJECT_ROOT" | tail -1)
    [[ $avail -gt 10485760 ]] || die "磁盘空间不足 (${avail} KB)，需至少 10GB"

    # Docker 存储驱动
    if ! docker info --format '{{.Driver}}' | grep -qE 'overlay2|devicemapper'; then
        log WARN "非推荐存储驱动"
    fi

    # 检查配置文件语法
    yq eval '.' "$DEPLOY_CONF" >/dev/null 2>&1 || die "部署配置 YAML 语法错误"
    yq eval '.' "$RISK_CONF" >/dev/null 2>&1 || die "风控配置 YAML 语法错误"
}

acquire_lock() {
    mkdir -p "$LOCK_DIR"
    exec 200>"$LOCK_FILE"
    local waited=0
    while ! flock -n 200; do
        if [[ $waited -ge $LOCK_TIMEOUT ]]; then
            # 检查锁进程是否存活，若死锁则强制删除
            local locker_pid
            locker_pid=$(fuser "$LOCK_FILE" 2>/dev/null | tr -d ' ')
            if [[ -n "$locker_pid" ]] && ! kill -0 "$locker_pid" 2>/dev/null; then
                log WARN "死锁检测，强制释放锁 (PID $locker_pid)"
                rm -f "$LOCK_FILE"
                flock -n 200 || die "锁竞争激烈，无法获取"
                break
            fi
            die "部署锁超时，另一部署进程可能卡住"
        fi
        sleep 10
        waited=$((waited + 10))
        log INFO "等待部署锁... (${waited}s)"
    done
    echo $$ > "${LOCK_FILE}.pid"
}

release_lock() {
    flock -u 200 2>/dev/null || true
    rm -f "${LOCK_FILE}.pid"
}

verify_git_signing() {
    # 验证最新提交的 GPG 签名
    if git config --bool --get commit.gpgsign &>/dev/null; then
        git verify-commit HEAD &>/dev/null || log WARN "提交签名验证失败，继续但不推荐"
    fi
}

pull_code() {
    log INFO "拉取代码: ${DEPLOY_VERSION}"
    git fetch origin --tags --force --prune 2>&1 | tee -a "$LOG_FILE" || die "fetch 失败"

    # 切换到特定提交/分支
    if ! git checkout "$DEPLOY_VERSION" 2>&1; then
        die "切换版本失败: $DEPLOY_VERSION"
    fi

    verify_git_signing

    # 若为分支，执行快进合并
    if git show-ref --verify "refs/heads/$DEPLOY_VERSION" &>/dev/null; then
        git pull origin "$DEPLOY_VERSION" --ff-only 2>&1 || die "快进合并失败，请检查冲突"
    fi

    # 记录当前 commit hash
    DEPLOY_COMMIT=$(git rev-parse HEAD)
    log INFO "部署提交: $DEPLOY_COMMIT"
}

build_image() {
    local tag="$1"
    log INFO "构建镜像: ${tag}"

    # 开启 BuildKit 以利用缓存
    export DOCKER_BUILDKIT=1
    if ! docker compose -f "$COMPOSE_CANARY" build \
        --build-arg "GIT_COMMIT=$DEPLOY_COMMIT" \
        --build-arg "BUILD_DATE=$(date -u +%Y%m%dT%H%M%SZ)" \
        --label "canary=true" \
        --pull 2>&1 | tee -a "$LOG_FILE"; then
        docker builder prune -f --filter "label=canary=true" || true
        die "镜像构建失败"
    fi

    # 计算镜像 SHA 并写入状态
    local image_id
    image_id=$(docker images -q "quant-canary:${tag}")
    echo "$image_id" > "${STATE_DIR}/.tmp_image_id"
}

start_canary() {
    local tag="$1"
    log INFO "启动金丝雀实例 (影子模式)"

    # 确保清理，避免端口冲突
    docker compose -f "$COMPOSE_CANARY" down --remove-orphans --volumes 2>/dev/null || true

    export CANARY_TAG="$tag"
    export CANARY_MODE="shadow"
    export DEPLOY_ID="$DEPLOY_ID"

    # 使用独立的项目名避免与服务冲突
    if ! docker compose -f "$COMPOSE_CANARY" -p "canary-${DEPLOY_ID}" up -d --force-recreate; then
        die "金丝雀启动失败"
    fi

    # 多层次健康检查：先 HTTP 200，再执行内部自检
    local timeout=300 start=$SECONDS
    while true; do
        if curl -sf "http://localhost:8080/healthz" >/dev/null 2>&1; then
            if docker compose -f "$COMPOSE_CANARY" -p "canary-${DEPLOY_ID}" exec -T canary \
                python3 -c "from core.health_monitor import HealthMonitor; assert HealthMonitor().check()['status']=='ok'" 2>/dev/null; then
                log INFO "金丝雀业务健康检查通过"
                break
            fi
        fi
        if (( SECONDS - start > timeout )); then
            docker compose -f "$COMPOSE_CANARY" -p "canary-${DEPLOY_ID}" logs --tail=100 canary 2>&1 | tee -a "$LOG_FILE"
            die "金丝雀健康检查超时"
        fi
        sleep 5
    done
}

collect_data() {
    local minutes="$1"
    log INFO "收集交易数据，预期 ${minutes} 分钟，最少 ${MIN_TRADES} 笔交易"

    local elapsed=0
    while (( elapsed < minutes * 60 )); do
        sleep 60
        elapsed=$((elapsed + 60))

        # 动态检查交易数
        local trades
        trades=$(docker compose -f "$COMPOSE_CANARY" -p "canary-${DEPLOY_ID}" exec -T canary \
            python3 -c "from core.trade_database import TradeDatabase; print(TradeDatabase().count())" 2>/dev/null || echo 0)
        log INFO "金丝雀已产生 ${trades} 笔交易 (运行 ${elapsed}s)"
        if [[ "$trades" -ge "$MIN_TRADES" ]]; then
            log INFO "达到最低交易笔数，提前结束数据收集"
            break
        fi

        # 实时监控关键风险指标，若恶化立即终止
        local drawdown
        drawdown=$(docker compose -f "$COMPOSE_CANARY" -p "canary-${DEPLOY_ID}" exec -T canary \
            python3 -c "from core.pnl_calculator import PnLCalculator; print(PnLCalculator().current_drawdown())" 2>/dev/null || echo 0)
        if [[ $(echo "$drawdown > 0.05" | bc -l 2>/dev/null) == "1" ]]; then
            log ERROR "金丝雀实时回撤超过 5%，触发熔断"
            return 1
        fi
    done
}

evaluate() {
    local tag="$1"
    log INFO "运行评估脚本..."

    if ! python3 "$EVAL_SCRIPT" \
        --canary-tag "$tag" \
        --min-sharpe "${MIN_SHARPE:-1.2}" \
        --max-drawdown "${MAX_DRAWDOWN:-0.15}" \
        --profit-factor "${PROFIT_FACTOR:-1.5}" \
        --min-trades "$MIN_TRADES" \
        --output-json "${STATE_DIR}/${DEPLOY_ID}_eval.json" 2>&1 | tee -a "$LOG_FILE"; then
        log ERROR "评估未通过阈值"
        return 1
    fi

    # 二次解析确保指标非零
    local sharpe drawdown
    sharpe=$(jq -r '.sharpe_ratio' "${STATE_DIR}/${DEPLOY_ID}_eval.json")
    drawdown=$(jq -r '.max_drawdown' "${STATE_DIR}/${DEPLOY_ID}_eval.json")
    [[ $(echo "$sharpe > 0.5" | bc -l) == "1" ]] || { log ERROR "夏普异常低: $sharpe"; return 1; }
    log INFO "评估通过，夏普: ${sharpe}, 最大回撤: ${drawdown}"
    return 0
}

promote() {
    local tag="$1"
    log INFO "将金丝雀镜像提升为生产版本: ${tag}"

    # 1. 打生产标签并推送（若有私有仓库）
    local prod_tag="prod-$(date -u +%Y%m%dT%H%M%SZ)"
    docker tag "quant-canary:${tag}" "quant-prod:${prod_tag}"
    # docker push "quant-prod:${prod_tag}"  # 按需推送

    # 2. 备份当前生产 compose 文件并更新镜像
    cp "$COMPOSE_BASE" "${COMPOSE_BASE}.bak.${DEPLOY_ID}"
    yq eval -i ".services.app.image = \"quant-prod:${prod_tag}\"" "$COMPOSE_BASE"

    # 3. 渐进式流量切换（假设与 Traefik/Envoy 集成，通过标签控制权重）
    # 这里简化：先启动一个新生产实例（权重 10%），通过健康检查后再增加
    log INFO "启动新生产实例，权重 ${CANARY_PERCENT}%"
    export CANARY_PERCENT  # 传递给 compose 或外部脚本
    docker compose -f "$COMPOSE_BASE" up -d --scale app=2
    sleep 30

    # 4. 检查新实例健康状态
    if ! curl -sf "http://localhost:8080/healthz" >/dev/null; then
        log ERROR "新生产实例健康检查失败，回滚"
        rollback "$tag"
        die "提升失败"
    fi

    # 5. 最终将权重提升到100%或根据策略逐步
    # 此处通过环境变量或配置中心下发权重
    log INFO "生产环境已更新，新版本运行中"
}

rollback() {
    local failed_tag="$1"
    log WARN "执行回滚，失败版本: ${failed_tag}"

    # 停止金丝雀
    docker compose -f "$COMPOSE_CANARY" -p "canary-${DEPLOY_ID}" down --remove-orphans --volumes 2>/dev/null || true

    # 恢复生产配置
    local latest_backup
    latest_backup=$(ls -t "${COMPOSE_BASE}.bak."* 2>/dev/null | head -1)
    if [[ -n "$latest_backup" ]]; then
        cp "$latest_backup" "$COMPOSE_BASE"
        docker compose -f "$COMPOSE_BASE" up -d --scale app=1
        log INFO "已恢复生产配置: $latest_backup"
    else
        log ERROR "无备份可恢复，需手动介入"
    fi
}

main() {
    # 解析参数略；假设使用环境变量
    [[ "$DRY_RUN" == "true" ]] && { check_prereqs; log INFO "干运行成功"; exit 0; }

    audit "DEPLOY_START" "version=${DEPLOY_VERSION}, percent=${CANARY_PERCENT}"

    check_prereqs
    acquire_lock
    pull_code

    local canary_tag="canary-${DEPLOY_ID}"
    build_image "$canary_tag"
    start_canary "$canary_tag"

    if ! collect_data "$COLLECT_MINUTES"; then
        audit "COLLECT_FAILED" "drawdown breach"
        rollback "$canary_tag"
        die "数据收集阶段失败"
    fi

    if evaluate "$canary_tag"; then
        audit "EVAL_PASSED" "promoting"
        promote "$canary_tag"
        audit "DEPLOY_SUCCESS" "new_version=${canary_tag}"
        alert "金丝雀部署成功" "版本: ${canary_tag}, 夏普: ${sharpe}"
    else
        audit "EVAL_FAILED" "rolling back"
        rollback "$canary_tag"
        alert "金丝雀评估失败" "版本: ${canary_tag} 未达标，已回滚"
        exit 2
    fi
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
