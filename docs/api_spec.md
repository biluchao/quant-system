火种量化交易系统 API 规范 v1.0.0

本文档定义火种（Spark）量化交易系统内部模块间的接口协议、事件类型以及对外网关的交互规范。所有接口遵循单一职责、幂等、超时与降级原则，确保金融级安全与高可用。

目录

1. 事件总线协议
2. 核心模块接口
3. 订单与执行接口
4. 风控与状态管理接口
5. 市场数据与网关接口
6. AI 沙箱接口
7. 数据结构定义
8. 错误码与降级协议

---

1. 事件总线协议

事件总线 (EventBus) 是系统内部的核心通信通道，采用优先级队列 (PriorityQueue) 和发布/订阅模式。

1.1 事件优先级

优先级 值 描述 示例
CRITICAL 0 风控止损、断连 止损触发、交易所断线
HIGH 1 成交回报、订单更新 订单成交、部分成交
MEDIUM 2 K线闭合、深度快照 3分钟K线闭合
LOW 3 指标计算、心跳 粒子滤波完成通知

1.2 标准事件类型

事件常量 字符串标识 数据载荷 说明
EVENT_TICK "tick" TickEvent 逐笔成交
EVENT_DEPTH "depth" DepthEvent 深度快照
EVENT_KLINE_CLOSE "kline_close" KlineEvent K线闭合
EVENT_DATA_QUALITY_ALERT "data_quality_alert" {"reason": str, "symbol": str} 数据质量异常
EVENT_ORDER_CREATED "order_created" {"client_order_id": str, "exchange_id": str} 订单已创建
EVENT_ORDER_FILLED "order_filled" {"order_id": str, "filled_qty": float} 订单成交
EVENT_ORDER_CANCELLED "order_cancelled" {"order_id": str} 订单已撤销
EVENT_TRADE_CLOSED "trade_closed" TradeRecord 交易平仓
EVENT_HEARTBEAT "heartbeat" {"timestamp": int} 心跳
EVENT_SYSTEM_ALERT "system_alert" {"alert_type": str, "message": str} 系统告警
EVENT_STATE_CHANGE "state_change" {"subtype": str, "data": dict} 状态变更（趋势、风控等）

1.3 发布与订阅

· 发布：EventBus.publish(event_type: str, data: Any, priority: Priority = MEDIUM) -> bool
· 订阅：EventBus.subscribe(event_type: str, callback: Callable)
· 支持通配符 '*' 订阅所有事件。
· 背压保护：队列满时丢弃 LOW 优先级事件，CRITICAL 事件写入死信队列。

---

2. 核心模块接口

每个模块通过其公开方法提供标准化返回值（字典），包含 status, reason, warnings 等字段。

2.1 粒子滤波器

· 模块：ParticleFilter
· 方法：
  · predict(dt: float) -> None
  · update(observation: float) -> None
  · resample(threshold_ratio: float) -> bool
  · get_estimates() -> Dict[str, Any]
    ```json
    {
      "mu_mean": 0.12,
      "mu_std": 0.05,
      "theta_mean": -0.03,
      "theta_std": 0.02,
      "prob_divergence": 0.78,
      "ess": 42.5,
      "mu_quantiles": [0.05, 0.12, 0.35],
      "theta_quantiles": [-0.1, -0.03, 0.01]
    }
    ```
· 契约：调用 predict → update → resample → get_estimates 循环。

2.2 趋势状态推断

· 模块：TrendStateInference
· 方法：evaluate(market_data: Dict) -> Dict
  · 输入：{"z_score": float, ...}
  · 输出：
    ```json
    {
      "state": "diverging",
      "confidence": 0.85,
      "action": "hold_or_add",
      "reason": "趋势发散",
      "mu_mean": 0.12,
      "prob_divergence": 0.78,
      "ess_ratio": 0.85,
      "timestamp_ns": 1734512345678901234
    }
    ```
· 状态机：oscillating → diverging / retracing / recovery，通过滞后机制避免频繁切换。

2.3 自适应参数调谐器

· 模块：AdaptiveParameterTuner
· 方法：
  · trigger_update() -> Tuple[bool, str]
  · get_recommended_params() -> Dict[str, float]
  · apply_params(params: Dict) -> bool
· 契约：优化后推荐参数需经人工或影子模式确认方可应用。

2.4 风险预算

· 模块：RiskBudget
· 方法：
  · compute_risk_metrics(force: bool = False) -> Dict
    ```json
    {
      "volatility": 0.35,
      "var_95": 0.45,
      "es_95": 0.52,
      "max_exposure": 1500000.0,
      "equity": 500000.0,
      "total_exposure": 800000.0,
      "leverage": 1.6,
      "es_ratio": 0.015,
      "data_points": 25
    }
    ```
  · check_exposure(new_order: Dict) -> Tuple[bool, str]
    · 输入订单需含 quantity, price, side, symbol
    · 返回 (True, "OK") 或 (False, "LEVERAGE") 等。

---

3. 订单与执行接口

3.1 订单管理器

· 模块：OrderManager
· 方法：
  · submit_order(order: Dict) -> Dict
    · 输入 {"symbol", "side", "quantity", "price", "order_type"}
    · 返回 {"status": "ok", "order_id": "client_order_id", "reason": "..."}
  · cancel_order(order_id: str) -> Dict
  · get_order(order_id: str) -> Optional[Dict]
· 幂等键：client_order_id 格式 spark-{machine_id}-{timestamp}-{uuid7}-{side}{symbol}。

3.2 执行计划生成器

· 模块：ExecutionPlanner
· 方法：plan(order: Dict, strategy: str, total_time_sec: float) -> Dict
  · 返回执行计划，含子单列表 slices，每个子单含 quantity, delay_ms, price。
· 策略：adaptive (默认), twap, passive, aggressive。

3.3 执行算法

· 模块：ExecutionAlgo
· 方法：
  · execute_plan(plan_id: str) -> bool
  · pause(plan_id: str) -> bool
  · resume(plan_id: str) -> bool
  · cancel(plan_id: str) -> bool

---

4. 风控与状态管理接口

4.1 独立风控中心

· 模块：RiskManager
· 方法：
  · approve_order(order: Dict) -> Tuple[bool, str]
  · record_trade_result(profit: float) -> None
  · reset_circuit_breaker(reason: str) -> None
· 熔断条件：连续亏损 ≥ 5 次、日内亏损 ≥ 8% 等。

4.2 盈亏计算器

· 模块：PnlCalculator
· 方法：
  · get_unrealized_pnl(symbol: str, mark_price: float) -> float
  · get_realized_pnl(symbol: str, since: float) -> float
  · get_total_pnl(symbol: str, mark_prices: Dict) -> Dict

---

5. 市场数据与网关接口

5.1 WebSocket 行情接入

· 网关：gateway/market_data_ingestor.py
· 订阅：btcusdt@aggTrade, btcusdt@depth@100ms, btcusdt@kline_3m
· 标准化输出：TickEvent, DepthEvent, KlineEvent 通过事件总线发布。

5.2 订单派发

· 网关：gateway/order_dispatcher.py
· 方法：send_order(order: Dict) -> Dict, cancel_order(order_id: str, symbol: str) -> bool
· 认证：使用 Vault 注入的 API Key，每次请求签名。

---

6. AI 沙箱接口

· 启动：ai_sandbox/wakeup_service.py 按需启动 DeepSeek 容器。
· 交互：通过自然语言查询，返回结构化建议：
  ```json
  {
    "type": "param_suggestion",
    "params": {"stop_loss_atr_mult": 2.2},
    "confidence": 0.78,
    "reason": "当前波动率上升，建议收紧止损"
  }
  ```
· 安全：修改操作需谷歌验证码+密码确认。

---

7. 数据结构定义

所有核心数据结构定义于 core/data_structs.py，采用不可变 frozen dataclass 并统一使用 Decimal 存储价格与数量。

7.1 K线 (Kline)

字段 类型 说明
symbol str 交易品种
interval str K线周期 (如 "3m")
open Decimal 开盘价
high Decimal 最高价
low Decimal 最低价
close Decimal 收盘价
volume Decimal 成交量
start_time_ms int 起始时间戳(ms)
is_closed bool 是否已闭合

7.2 逐笔成交 (Tick)

字段 类型 说明
symbol str 
price Decimal 成交价
quantity Decimal 成交量
timestamp_ms int 交易所时间戳
is_buyer_maker bool 主动方向

7.3 深度快照 (DepthSnapshot)

字段 类型 说明
symbol str 
bids Tuple[Tuple[Decimal,Decimal]] 买盘 (价, 量)
asks Tuple[Tuple[Decimal,Decimal]] 卖盘 (价, 量)
last_update_id int 最后更新ID

7.4 订单 (Order)

字段 类型 说明
symbol str 
side str BUY / SELL
quantity Decimal 
price Optional[Decimal] 限价单价格，市价单为 None
order_type str LIMIT / MARKET
client_order_id str 幂等键

---

8. 错误码与降级协议

所有模块遵循统一的返回规范：

```json
{
  "status": "ok" | "error" | "rejected" | "degraded",
  "reason": "人类可读原因",
  "warnings": ["警告列表"],
  "... 业务字段": "..."
}
```

常见错误码：

· "INVALID_ORDER"：订单参数非法
· "LEVERAGE"：杠杆超限
· "ES"：预期损失超限
· "EXPOSURE"：敞口超限
· "NO_EQUITY"：无法获取权益

降级策略：任何外部依赖不可用时，模块自动回退到安全默认值并记录 WARNING，拒绝新开仓，保留现有持仓。
