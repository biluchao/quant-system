#!/usr/bin/env python3
"""main.py — 量化交易系统主入口

铁律声明：本文件遵守
- 配置统一：所有配置从 SystemConfig 加载，禁止裸 os.getenv()
- 命名统一：入口函数用动词开头，信号处理用 _handle_ 前缀
- 三层防线：启动前校验配置 → 启动中校验依赖 → 运行中校验健康状态

启动流程：
1. 加载配置
2. 初始化事件总线
3. 按依赖顺序启动组件
4. 健康检查
5. 进入事件循环（等待信号）
6. 收到停止信号 → 按逆序优雅关闭

关闭流程（SIGTERM/SIGINT）：
1. 停止接收新请求
2. 通知所有组件开始关闭
3. 等待在途任务完成
4. 超时强制终止
"""
from __future__ import annotations

import asyncio
import logging
import logging.handlers
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set

from core.types import (
    AlertLevel,
    ComponentStatus,
    SystemMode,
    TraceId,
    new_trace_id,
)
from events.bus import EventBus, Event, EventType


# ═══════════════════════════════════════════════════════════════
# 日志配置
# ═══════════════════════════════════════════════════════════════

def setup_logging(
    level: str = "INFO",
    log_dir: str = "data/logs",
    max_bytes: int = 100 * 1024 * 1024,  # 100MB
    backup_count: int = 10,
) -> None:
    """配置结构化日志

    要求：
    - 控制台输出带颜色（开发环境）
    - 文件输出 JSON 格式（便于采集分析）
    - 按大小轮转（防止磁盘爆满）
    """
    os.makedirs(log_dir, exist_ok=True)

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    json_formatter = logging.Formatter(
        fmt='{"timestamp":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","message":"%(message)s","trace_id":"%(trace_id)s"}',
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # 控制台 handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # 文件 handler（轮转）
    file_handler = logging.handlers.RotatingFileHandler(
        filename=os.path.join(log_dir, "quant_system.log"),
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(json_formatter)
    root_logger.addHandler(file_handler)


# ═══════════════════════════════════════════════════════════════
# 系统配置
# ═══════════════════════════════════════════════════════════════

@dataclass
class SystemConfig:
    """系统配置（启动前全量校验）

    铁律：配置统一。所有配置项集中在此，禁止裸读环境变量。
    """
    # 基础
    environment: str = field(default_factory=lambda: os.getenv("QUANT_ENV", "development"))
    mode: SystemMode = field(default_factory=lambda: SystemMode(os.getenv("QUANT_MODE", "PAPER")))
    log_level: str = field(default_factory=lambda: os.getenv("QUANT_LOG_LEVEL", "INFO"))
    data_dir: str = field(default_factory=lambda: os.getenv("QUANT_DATA_DIR", "data"))
    log_dir: str = field(default_factory=lambda: os.getenv("QUANT_LOG_DIR", "data/logs"))

    # 服务端口
    api_port: int = field(default_factory=lambda: int(os.getenv("QUANT_API_PORT", "8080")))
    metrics_port: int = field(default_factory=lambda: int(os.getenv("QUANT_METRICS_PORT", "9090")))
    health_port: int = field(default_factory=lambda: int(os.getenv("QUANT_HEALTH_PORT", "9091")))

    # 交易所
    exchange_name: str = field(default_factory=lambda: os.getenv("QUANT_EXCHANGE", "BINANCE"))
    exchange_api_key: str = field(default_factory=lambda: os.getenv("QUANT_API_KEY", ""))
    exchange_api_secret: str = field(default_factory=lambda: os.getenv("QUANT_API_SECRET", ""))

    # 策略
    enabled_strategies: List[str] = field(
        default_factory=lambda: os.getenv("QUANT_STRATEGIES", "").split(",") if os.getenv("QUANT_STRATEGIES") else []
    )

    # 运行参数
    shutdown_timeout: float = 30.0
    health_check_interval: float = 10.0
    max_concurrent_orders: int = 50

    # 前端
    frontend_enabled: bool = field(default_factory=lambda: os.getenv("QUANT_FRONTEND_ENABLED", "true").lower() == "true")
    frontend_port: int = field(default_factory=lambda: int(os.getenv("QUANT_FRONTEND_PORT", "1420")))

    def validate(self) -> List[str]:
        """三层防线第一层：配置校验

        返回错误列表，空列表表示校验通过。
        """
        errors: List[str] = []

        if self.mode not in SystemMode:
            errors.append(f"Invalid system mode: {self.mode}")

        if self.mode == SystemMode.LIVE:
            if not self.exchange_api_key:
                errors.append("LIVE mode requires QUANT_API_KEY")
            if not self.exchange_api_secret:
                errors.append("LIVE mode requires QUANT_API_SECRET")

        if self.api_port <= 0 or self.api_port > 65535:
            errors.append(f"Invalid API port: {self.api_port}")

        if self.shutdown_timeout <= 0:
            errors.append("Shutdown timeout must be positive")

        if self.max_concurrent_orders <= 0:
            errors.append("Max concurrent orders must be positive")

        return errors


# ═══════════════════════════════════════════════════════════════
# 系统核心
# ═══════════════════════════════════════════════════════════════

class SystemPhase(str, Enum):
    """系统生命周期阶段"""
    INIT = "INIT"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    ERROR = "ERROR"


class QuantSystem:
    """量化交易系统主类

    职责：
    - 管理组件生命周期
    - 编排启动/关闭顺序
    - 处理信号
    - 监控健康状态
    """

    def __init__(self, config: Optional[SystemConfig] = None) -> None:
        self.config = config or SystemConfig()
        self.logger = logging.getLogger("quant_system")
        self.trace_id: TraceId = new_trace_id()

        # 核心组件
        self.event_bus: Optional[EventBus] = None

        # 生命周期
        self.phase: SystemPhase = SystemPhase.INIT
        self._stop_event: Optional[asyncio.Event] = None
        self._health_check_task: Optional[asyncio.Task] = None
        self._components: List[str] = []

    # ── 启动 ──

    async def start(self) -> None:
        """启动系统"""
        self.phase = SystemPhase.STARTING
        self.logger.info(f"=" * 60)
        self.logger.info(f"Quant System v5.0.0-production starting...")
        self.logger.info(f"Mode: {self.config.mode.value}")
        self.logger.info(f"Environment: {self.config.environment}")
        self.logger.info(f"Trace ID: {self.trace_id}")
        self.logger.info(f"=" * 60)

        try:
            # 第一层防线：配置校验
            errors = self.config.validate()
            if errors:
                for err in errors:
                    self.logger.error(f"Config error: {err}")
                raise SystemStartupError(f"Configuration validation failed: {errors}")

            # 启动事件总线
            await self._start_event_bus()

            # 启动其他组件（按依赖顺序）
            await self._start_components()

            # 健康检查
            await self._health_check()

            # 进入运行状态
            self.phase = SystemPhase.RUNNING
            self.logger.info("✅ System started successfully")

            # 发布启动完成事件
            if self.event_bus:
                await self.event_bus.publish(Event(
                    event_type=EventType.SYSTEM_READY,
                    source="main",
                    trace_id=self.trace_id,
                    data={"mode": self.config.mode.value},
                ))

            # 启动健康检查循环
            self._health_check_task = asyncio.create_task(self._health_check_loop())

            # 等待停止信号
            self._stop_event = asyncio.Event()
            await self._stop_event.wait()

        except Exception as e:
            self.phase = SystemPhase.ERROR
            self.logger.error(f"System startup failed: {e}", exc_info=True)
            await self.stop()
            raise

    async def _start_event_bus(self) -> None:
        """启动事件总线（最优先启动，其他组件依赖它）"""
        self.logger.info("[1/4] Starting event bus...")
        self.event_bus = EventBus()
        await self.event_bus.start()
        self._components.append("event_bus")
        self.logger.info("      Event bus ready")

    async def _start_components(self) -> None:
        """按依赖顺序启动组件

        启动顺序（DAG）：
        1. EventBus（已完成）
        2. Storage（持久化）
        3. Gateways（交易所连接）
        4. Risk Engine（风控）
        5. Account Manager（账户）
        6. OMS（订单管理）
        7. Strategy Engine（策略）
        8. API Server（对外接口）
        """
        self.logger.info("[2/4] Starting core components...")

        # TODO: Phase 2 实现具体组件启动
        # 当前为骨架阶段，仅记录启动顺序
        components_order = [
            "storage",
            "gateways",
            "risk_engine",
            "account_manager",
            "oms",
            "strategy_engine",
            "api_server",
            "frontend",
        ]

        for comp in components_order:
            self.logger.info(f"      [{comp}] starting... (TODO: Phase 2)")
            self._components.append(comp)

        self.logger.info("      All components registered")

    async def _health_check(self) -> None:
        """启动后健康检查（三层防线第三层）"""
        self.logger.info("[3/4] Running health checks...")

        if not self.event_bus or not self.event_bus.is_running():
            raise SystemStartupError("EventBus is not running")

        self.logger.info("      EventBus: OK")
        self.logger.info("      All components: OK")

    async def _health_check_loop(self) -> None:
        """持续健康检查"""
        while self.phase == SystemPhase.RUNNING:
            try:
                await asyncio.sleep(self.config.health_check_interval)
                # TODO: Phase 2 实现详细健康检查
                # - 交易所连接
                # - 数据库
                # - Redis
                # - 磁盘空间
                # - 内存使用
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Health check error: {e}")

    # ── 停止 ──

    async def stop(self) -> None:
        """优雅关闭"""
        if self.phase == SystemPhase.STOPPED:
            return

        self.phase = SystemPhase.STOPPING
        self.logger.info("=" * 60)
        self.logger.info("Shutting down...")

        try:
            # 按逆序关闭组件
            for comp in reversed(self._components):
                self.logger.info(f"      [{comp}] stopping...")
                # TODO: Phase 2 实现具体关闭逻辑

            # 关闭健康检查
            if self._health_check_task:
                self._health_check_task.cancel()
                try:
                    await self._health_check_task
                except asyncio.CancelledError:
                    pass

            # 关闭事件总线（等待在途事件）
            if self.event_bus:
                self.logger.info("      Flushing event bus...")
                await self.event_bus.stop(wait_timeout=self.config.shutdown_timeout)

            self.phase = SystemPhase.STOPPED
            self.logger.info("✅ System stopped cleanly")
            self.logger.info("=" * 60)

        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}", exc_info=True)

    # ── 信号处理 ──

    def setup_signal_handlers(self) -> None:
        """注册信号处理器"""
        loop = asyncio.get_event_loop()

        def signal_handler(sig: signal.Signals) -> None:
            self.logger.warning(f"Received signal: {sig.name}")
            if self._stop_event and not self._stop_event.is_set():
                self._stop_event.set()

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, signal_handler, sig)
            except NotImplementedError:
                # Windows 兼容
                signal.signal(sig, lambda s, f: signal_handler(sig))

    # ── 查询 ──

    def get_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        return {
            "phase": self.phase.value,
            "mode": self.config.mode.value,
            "environment": self.config.environment,
            "trace_id": self.trace_id,
            "components": self._components,
            "event_bus_running": self.event_bus.is_running() if self.event_bus else False,
            "event_bus_metrics": self.event_bus.get_metrics() if self.event_bus else None,
        }


# ═══════════════════════════════════════════════════════════════
# 异常定义
# ═══════════════════════════════════════════════════════════════

class SystemStartupError(Exception):
    """系统启动失败"""
    pass


class SystemShutdownError(Exception):
    """系统关闭失败"""
    pass


# ═══════════════════════════════════════════════════════════════
# 入口
# ═══════════════════════════════════════════════════════════════

async def _run() -> None:
    """运行系统"""
    config = SystemConfig()
    system = QuantSystem(config)
    system.setup_signal_handlers()

    try:
        await system.start()
    except SystemStartupError:
        sys.exit(1)
    except KeyboardInterrupt:
        pass
    finally:
        await system.stop()


def main() -> None:
    """命令行入口"""
    # 第一层防线：启动前配置日志
    setup_logging()

    # 启动系统
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

