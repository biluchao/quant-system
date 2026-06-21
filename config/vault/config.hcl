# =============================================================================
# 火种量化交易系统 · HashiCorp Vault 配置 v4.0.0
# =============================================================================
# 用途: 管理所有敏感凭证（API 密钥、数据库密码、审计签名密钥等）
# 环境: 生产环境推荐使用集成存储 (Raft) 实现高可用，并启用 TLS、自动解封、严格审计与监控
# 使用: vault server -config=config/schema/vault/config.hcl
# =============================================================================

# --- 存储后端 (Raft 集成存储) -------------------------------------------------
# 生产环境必须使用 Raft 实现高可用与一致性，数据目录需高性能 SSD
# 每个节点的 node_id 应唯一，建议通过 ${HOSTNAME} 或环境变量注入
storage "raft" {
  path    = "/vault/data"
  node_id = "node-1"                      # 生产环境每个节点唯一，可通过环境变量注入

  retry_join {
    leader_api_addr = "https://vault-1.internal:8200"
  }
  retry_join {
    leader_api_addr = "https://vault-2.internal:8200"
  }
  retry_join {
    leader_api_addr = "https://vault-3.internal:8200"
  }
}

# --- 监听器 (TLS 强制) ---------------------------------------------------------
# 生产环境必须启用 TLS，使用受信证书；监听内网地址，外部通过反向代理访问
listener "tcp" {
  address       = "10.0.0.10:8200"        # 绑定到内部网络接口，请根据实际网络规划调整
  tls_cert_file = "/vault/config/server.crt"
  tls_key_file  = "/vault/config/server.key"
  tls_min_version = "tls12"
  tls_cipher_suites = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"
  tls_disable_client_certs = true        # 如需要 mTLS 应设为 false 并配置 ca_cert_file
  # 生产环境建议开启代理协议支持（根据实际负载均衡器）
  # proxy_protocol_behavior = "use_always"
  # 速率限制 (防止暴力攻击)
  rate_limit {
    rate      = 1000.0
    burst     = 2000
    mode      = "enforcing"              # 生产环境启用强制执行
  }
}

# --- 自动解封 (使用云 KMS 或 Transit) ------------------------------------------
# 生产环境必须避免手动解封，推荐使用 AWS KMS、Azure Key Vault 或 Vault Transit
# 以下示例为 AWS KMS，实际密钥 ID 应从环境变量或 Vault Agent 注入，避免硬编码
seal "awskms" {
  region     = "ap-east-1"
  kms_key_id = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"  # 生产环境务必替换，并通过 Vault 变量管理
  # 也可使用 transit seal:
  # seal "transit" {
  #   address = "https://vault.internal:8200"
  #   token   = "hvs.xxxxxxxxxxxxxxxxx"
  #   key_name = "autounseal"
  # }
}

# --- 密钥引擎与默认租期 ------------------------------------------------------
default_lease_ttl = "168h"                # 默认租期 7 天
max_lease_ttl     = "720h"                # 最大租期 30 天

# --- 审计日志 (强制开启，并记录原始请求与响应，带 HMAC 签名) ------------------
audit {
  type   = "file"
  path   = "/vault/logs/audit.log"
  format = "json"
  options {
    file_mode = "0600"
    max_size_mb = 1024
    max_backups = 10
    # 默认启用 HMAC，确保敏感字段不被明文记录（生产环境切勿关闭）
    hmac = true
  }
}

# --- 全局设置 ---------------------------------------------------------------
api_addr       = "https://vault.internal:8200"   # 对外公告地址
cluster_addr   = "https://vault.internal:8201"   # 集群通信地址

# 安全设置：生产环境必须启用 mlock 以防止密钥交换到磁盘
disable_mlock  = false
# 需要设置适当的 capability (IPC_LOCK) 或在 Kubernetes 中设置 securityContext

# 日志格式：JSON 便于集中式日志系统收集分析
log_format = "json"
log_level  = "warn"

# UI 建议在生产环境禁用，通过独立的安全管理界面操作
ui = false

# 最大请求体大小限制（防止内存耗尽）
max_request_size = 33554432  # 32 MB

# PID 文件路径（便于进程管理）
pid_file = "/vault/pid/vault.pid"

# --- 遥测 (Prometheus 指标) -------------------------------------------------
# 开启指标暴露，供监控系统采集
telemetry {
  prometheus_retention_time = "30s"
  disable_hostname          = true
  # 可选：直接暴露给 Prometheus，需结合监听器 ACL 保障安全
  # unauthenticated_metrics_access = true
}

# --- 集群配置 (生产多节点) ---------------------------------------------------
# 集群名称，所有节点必须一致
cluster_name = "spark-vault"

# --- 服务注册 (如使用 Consul) -------------------------------------------------
# service_registration "consul" {
#   address = "consul.internal:8500"
#   scheme  = "https"
#   token   = "xxxxxxxxxxxx"
# }

# --- 许可与高级特性 (如有) ---------------------------------------------------
# license_path = "/vault/config/license.hclic"
