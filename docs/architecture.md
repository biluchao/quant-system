火种（Spark）量化交易系统 - 架构设计规范 v1.0.0

版本 日期 作者 变更说明
1.0.0 2026-06-20 AI Architect 初稿

目录

1. 概述
2. 设计原则
3. 系统上下文与边界
4. 容器化部署架构
5. 核心组件分层详述
6. 事件驱动数据流
7. 安全与合规
8. 容错与灾备
9. 热重载与持续演进
10. 部署环境
11. 关键量化模型与算法索引
12. 术语表
13. 附录A：完整文件树

---

1. 概述

火种（Spark）是一套面向币安BTCUSDT永续合约的超机构级量化交易系统，以自适应移动均线哲学为决策原点，融合粒子滤波连续状态推断、稀疏高斯过程分类、CMA-ES在线参数进化与订单流微观结构感知。系统设计对标Citadel Securities/Renaissance Technologies生产标准，支持万亿美金名义账户的毫秒级决策。

2. 设计原则

· 单一职责与原子热重载：每个文件承担1-2个无状态计算或单一资源管理，通过module_loader实现多文件原子替换。
· 自动装配与门禁：Assembler解析文档头依赖进行拓扑注入；五重代码质量门禁阻塞合并。
· 确定性优先：核心计算使用Decimal消除浮点误差。
· 失败安全：任何模块异常返回安全侧值，风控违规必须立即降级。
· 可观测性：所有决策点须暴露Prometheus指标，附带trace-id。

3. 系统上下文与边界

系统通过三个主要接口与外部交互：

· 市场数据：币安WebSocket行情（aggTrade, depth, kline），经AWS Direct Connect或专线接入，延迟<5ms。
· 交易指令：币安REST/WebSocket私有API，所有请求携带幂等键。
· 用户与监管：前端仪表板（仅限内网），审计日志实时同步至AWS S3。

4. 容器化部署架构

```
┌──────────────────────────────────────────────────────────────┐
│                     前端 (Flask + WebSocket)                   │
│                仪表板 | AI对话 | 手动控制 | 认证               │
└──────────────────────────┬───────────────────────────────────┘
                           │ Redis Streams (内部消息总线)
┌──────────────────────────┼───────────────────────────────────┐
│                    策略引擎 (C++/Python 混合)                  │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────────────┐ │
│  │ 事件总线     │  │ 粒子滤波状态  │  │ 高斯过程分类器       │ │
│  │ (event_bus)  │  │ 推断         │  │ (gp_classifier)      │ │
│  └──────┬───────┘  └──────┬───────┘  └──────────┬───────────┘ │
│  ┌──────┴─────────────────┴─────────────────────┴───────────┐ │
│  │              自适应参数调谐器 (CMA-ES)                    │ │
│  └────────────────────────────┬─────────────────────────────┘ │
│  ┌───────────┐  ┌───────────┐  ┌────────────┐  ┌───────────┐ │
│  │风控中心    │  │订单管理    │  │执行算法     │  │盈亏计算    │ │
│  └───────────┘  └───────────┘  └────────────┘  └───────────┘ │
└──────────────────────────┬───────────────────────────────────┘
                           │
┌──────────────────────────┴───────────────────────────────────┐
│                     网关层 (Go/Rust/Python)                    │
│   WebSocket行情接入 | 深度维护 | 订单路由 | 用户数据流         │
└──────────────────────────┬───────────────────────────────────┘
                           │
┌──────────────────────────┴───────────────────────────────────┐
│  币安交易所 (BTCUSDT 等合约)                                   │
└──────────────────────────────────────────────────────────────┘
```

5. 核心组件分层详述

5.1 数据采集与预处理

· gateway/market_data_ingestor.py：解析ws消息，发布标准化Kline/Depth/Tick事件。
· gateway/ws_client.py：连接管理、心跳、重连。
· core/orderbook_snapshot.py：增量维护50档订单簿。
· core/event_bus.py：高性能事件队列，支持多优先级订阅者。

5.2 信号计算

5.2.1 趋势与均线

· ma_core, ma_adaptation, atr, z_score, angle_curvature
· 自适应MA长度通过贝叶斯变点检测（在线算法），范围18~34，过渡采用EMA平滑。

5.2.2 不回归五条件

· divergence_lambda, escape_velocity, impulse_strength, low_freq_energy, five_conditions
· 低频能量使用Goertzel算法增量更新。

5.2.3 粒子滤波状态推断

· particle_filter.py：双变量OU过程，50粒子，每笔成交到达时更新，分层重采样。

5.2.4 假突破处理

· false_breakout.py：容忍带自适应，SPRT序贯检验控制误报率。

5.2.5 支撑/压力概率云

· sr_level_extractor, sr_zone_manager, sr_touch_evaluator
· 在线核密度估计，角色互换保留60%记忆。

5.3 概率融合与决策

· factor_collector → gp_classifier → bayesian_score
· 14因子输入稀疏变分GP，输出预测均值μ与标准差σ。
· trend_state_inference 结合粒子滤波与假突破检测，滞后确认状态切换。

5.4 订单执行与风控

5.4.1 事前风控 (risk_manager)

· 硬限制：总杠杆≤5倍，单笔保证金≤可用50%，每秒订单数≤8。
· 连续亏损5次触发熔断。

5.4.2 事中风控 (execution_planner, execution_algo)

· 基于Almgren-Chriss最优执行框架拆单，冲击模型参数每10分钟在线更新。
· 每发送子单前回调风控二次确认。

5.4.3 事后风控

· 每笔成交写入审计日志，异常检测。

5.5 自适应学习与在线优化 (adaptive_parameter_tuner)

· 样本收集后使用CMA-ES优化，目标最大化30天滚动Calmar比率。
· 优化结果经影子模式验证后应用，参数变化限制±15%。

5.6 AI沙箱

· 按需启动DeepSeek 1.3b模型，容器隔离，只读挂载状态快照。

6. 事件驱动数据流

1. 市场数据到达网关，封装为事件推入event_bus。
2. 策略引擎订阅事件，依次调用信号计算链。
3. 状态推断结果结合贝叶斯得分产生决策（开/加/减/平）。
4. 决策经风控审核后，由execution_planner生成最优执行计划，order_manager发送子单。
5. 成交回报通过用户数据流返回，更新持仓与盈亏。

7. 安全与合规

· 前端仅限内网访问，所有写操作需谷歌验证码+密码确认。
· API密钥通过Vault管理，运行时注入，绝无明文存储。
· 所有依赖通过SBOM管理，每周自动CVE扫描。

8. 容错与灾备

· 系统重启时按优先级从Redis、交易所REST API、SQLite恢复状态。
· 网络中断>20秒全平敞口。
· 数据流异常时切换备用连接。

9. 热重载与持续演进

· module_loader监控core/目录文件变更，依赖感知拓扑排序。
· 新模块health_check通过后原子替换，失败保留旧版本。
· 影子模式运行24h，金丝雀10%资金运行1周，无异常则全量。

10. 部署环境

· 硬件：4核CPU、8GB RAM、80GB SSD，香港节点。
· OS：Ubuntu 22.04 LTS，Docker Compose编排。
· 容器：engine（绑核2个）、gateway、redis、frontend、ai-sandbox。

11. 关键量化模型与算法索引

模型/算法 文件 核心函数
粒子滤波 particle_filter.py predict(), update(), resample()
高斯过程分类 gp_classifier.py predict(), online_update(), train()
最优执行 execution_algo.py _execute_plan()
CMA-ES优化 adaptive_parameter_tuner.py _optimize_cma()
假突破SPRT false_breakout.py check_recovery_signal()
波动率目标 risk_budget.py _compute_max_exposure()

12. 术语表

· OU过程：Ornstein-Uhlenbeck，均值回复随机过程。
· Calmar比率：年化收益率/最大回撤。
· ES：Expected Shortfall，条件尾部期望。
· SPRT：序贯概率比检验。
· SVGP：稀疏变分高斯过程。
· CMA-ES：协方差矩阵自适应进化策略。

13. 附录A：完整文件树

```
quant-system/
├── main.py
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── config/
│   ├── strategy.yaml
│   ├── risk.yaml
│   ├── model_hyperparams.yaml
│   ├── learning.yaml
│   ├── execution.yaml
│   └── instruments.yaml
├── core/
│   ├── event_bus.py
│   ├── particle_filter.py
│   ├── gp_classifier.py
│   ├── trend_state_inference.py
│   ├── adaptive_parameter_tuner.py
│   ├── ma_core.py
│   ├── ma_adaptation.py
│   ├── atr.py
│   ├── z_score.py
│   ├── angle_curvature.py
│   ├── divergence_lambda.py
│   ├── escape_velocity.py
│   ├── impulse_strength.py
│   ├── low_freq_energy.py
│   ├── five_conditions.py
│   ├── false_breakout.py
│   ├── sr_level_extractor.py
│   ├── sr_zone_manager.py
│   ├── sr_touch_evaluator.py
│   ├── factor_collector.py
│   ├── bayesian_score.py
│   ├── position_keeper.py
│   ├── position_sizer.py
│   ├── profit_tracker.py
│   ├── add_order_trigger.py
│   ├── stop_loss_calculator.py
│   ├── breakeven_guard.py
│   ├── risk_budget.py
│   ├── risk_manager.py
│   ├── order_validator.py
│   ├── execution_planner.py
│   ├── execution_algo.py
│   ├── order_manager.py
│   ├── pnl_calculator.py
│   ├── funding_fee_tracker.py
│   ├── health_monitor.py
│   ├── degradation_controller.py
│   ├── rl_meta_controller.py
│   ├── module_loader.py
│   ├── data_structs.py
│   └── ...
├── gateway/
│   ├── ws_client.py
│   ├── market_data_ingestor.py
│   ├── order_dispatcher.py
│   └── user_stream.py
├── ai_sandbox/
│   ├── wakeup_service.py
│   ├── deepseek_interface.py
│   └── ...
├── frontend/
│   ├── app.py
│   ├── auth_service.py
│   └── ...
├── scripts/
│   ├── quality_gate.py
│   ├── canary_evaluator.py
│   ├── model_retrain.py
│   └── ...
├── tests/
│   └── ...
└── docs/
    ├── architecture.md
    └── math_index.md
```
