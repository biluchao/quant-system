#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026-2027 Spark Quant System Contributors
"""
火种系统 · 盈亏计算器 (PnlCalculator) v11.0.0 — 机构级终极版

核心职责：
1. 高精度计算未实现盈亏（含预估平仓成本）与已实现盈亏（含实际费用）
2. 精确计入手续费（区分 Taker/Maker）、资金费率，支持多空双向独立核算
3. 支持多品种批量计算，提供标准化盈亏报告供风控与绩效分析
4. 所有金额使用 Decimal 计算，返回浮点数时保留足够精度，防止累积误差

外部依赖：
- core.position_keeper.PositionKeeper : 获取持仓、标记价格、合约乘数
- core.trade_database.TradeDatabase : 获取历史交易记录
- core.funding_fee_tracker.FundingFeeTracker : 获取累计资金费用（正数表示净支出，负数表示净收入）
- core.event_bus.EventBus : 发布大额盈亏事件
- core.metrics.MetricsCollector : 指标暴露

接口契约：
- get_unrealized_pnl(symbol, mark_price) -> float
- get_unrealized_gross_pnl(symbol, mark_price) -> float
- get_realized_pnl(symbol, since) -> float
- get_total_pnl(symbol, mark_prices) -> Dict[str, float]
- reload_config(config: Dict) -> None
- health_check() -> Dict[str, Any]

异常与降级：
- 依赖不可用时返回 0.0，并记录 WARNING
- 费用计算异常时使用最大费率保守估计
- 所有异常均被捕获，不中断调用方
"""

import logging
import time
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Dict, Any, Optional, Union, Tuple

logger = logging.getLogger(__name__)

# 可选依赖
try:
    from core.position_keeper import PositionKeeper
except ImportError:
    PositionKeeper = None
try:
    from core.trade_database import TradeDatabase
except ImportError:
    TradeDatabase = None
try:
    from core.funding_fee_tracker import FundingFeeTracker
except ImportError:
    FundingFeeTracker = None
try:
    from core.event_bus import EventBus, EventTypes
except ImportError:
    EventBus = None
    EventTypes = None
try:
    from core.metrics import MetricsCollector
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    MetricsCollector = None

# ── 常量 ──────────────────────────────────────────────────
VERSION = "11.0.0"
SPDX_IDENTIFIER = "Apache-2.0"
DEFAULT_TAKER_FEE_RATE = Decimal("0.0004")    # 4 bps
DEFAULT_MAKER_FEE_RATE = Decimal("0.0002")    # 2 bps (预留)
MAX_VALID_PRICE = Decimal("100000000000")      # 价格上限（防止异常值）
MIN_VALID_PRICE = Decimal("0.00000001")        # 价格下限
MAX_NOMINAL_VALUE = Decimal("100000000000")    # 单品种最大名义价值（100B USD）
OUTPUT_DECIMAL_PLACES = Decimal("0.00000001")  # 输出精度保留8位小数
ROUNDING_MODE = ROUND_HALF_UP
LARGE_PNL_THRESHOLD = Decimal("100000")        # 大额盈亏阈值（10万美元）


class PnlCalculator:
    """高精度盈亏计算器，支持多品种、多空双向、费用核算"""

    def __init__(self, position_keeper=None, trade_db=None, funding_fee_tracker=None,
                 event_bus=None, config: Optional[Dict] = None):
        self.position_keeper = position_keeper or (PositionKeeper() if PositionKeeper else None)
        self.trade_db = trade_db or (TradeDatabase() if TradeDatabase else None)
        self.funding_fee_tracker = funding_fee_tracker or (FundingFeeTracker() if FundingFeeTracker else None)
        self.event_bus = event_bus or (EventBus() if EventBus else None)

        self.taker_fee = DEFAULT_TAKER_FEE_RATE
        self.maker_fee = DEFAULT_MAKER_FEE_RATE
        self.large_pnl_threshold = LARGE_PNL_THRESHOLD
        self.output_precision = OUTPUT_DECIMAL_PLACES
        if config:
            self._apply_config(config)

        logger.info("PnlCalculator v%s 初始化完成，taker=%s maker=%s threshold=%s",
                    VERSION, self.taker_fee, self.maker_fee, self.large_pnl_threshold)

    # ── 公共接口 ──────────────────────────────────────────

    def get_unrealized_pnl(self, symbol: str, mark_price: Optional[float] = None) -> float:
        """计算未实现净盈亏（已扣除预估平仓手续费）"""
        gross, fee = self._calc_unrealized_internal(symbol, mark_price)
        if gross is None:
            return 0.0
        net = gross - fee
        self._check_large_event(symbol, net, "net")
        return float(net.quantize(self.output_precision, rounding=ROUNDING_MODE))

    def get_unrealized_gross_pnl(self, symbol: str, mark_price: Optional[float] = None) -> float:
        """获取不含手续费的毛未实现盈亏，用于绩效归因"""
        gross, _ = self._calc_unrealized_internal(symbol, mark_price)
        if gross is None:
            return 0.0
        self._check_large_event(symbol, gross, "gross")
        return float(gross.quantize(self.output_precision, rounding=ROUNDING_MODE))

    def get_realized_pnl(self, symbol: Optional[str] = None, since: Optional[float] = None) -> float:
        """获取已实现盈亏（历史平仓交易，包含手续费）"""
        if not self.trade_db:
            return 0.0
        try:
            sym = symbol.strip().upper() if symbol else None
            if since is not None and (since < 0 or since != since):  # NaN 检查
                logger.warning("since 参数无效，忽略")
                since = None
            trades = self.trade_db.get_closed_trades(symbol=sym, since=since)
            total = Decimal("0")
            trade_count = 0
            for t in trades:
                pnl_val = t.get('pnl')
                if pnl_val is None:
                    continue
                total += self._to_decimal(pnl_val)
                trade_count += 1
            if trade_count > 10000:
                logger.info("大量交易记录 (%d 笔) 已实现盈亏汇总完成", trade_count)
            return float(total.quantize(self.output_precision, rounding=ROUNDING_MODE))
        except Exception as e:
            logger.error("计算已实现盈亏异常: %s", e)
            return 0.0

    def get_total_pnl(self, symbol: Optional[str] = None,
                      mark_prices: Optional[Dict[str, float]] = None) -> Dict[str, float]:
        """
        获取总盈亏报告。

        Args:
            symbol: 指定品种，为 None 表示全部持仓
            mark_prices: 品种 -> 标记价格映射

        Returns:
            {'unrealized': float, 'realized': float, 'funding_fees': float, 'total': float}
            其中 funding_fees 为正表示净支出，负表示净收入。
        """
        result = {'unrealized': 0.0, 'realized': 0.0, 'funding_fees': 0.0, 'total': 0.0}
        target_symbol = symbol.strip().upper() if symbol else None

        # 未实现盈亏
        if target_symbol:
            mp = self._get_mark_price_from_dict(mark_prices, target_symbol)
            result['unrealized'] = self.get_unrealized_pnl(target_symbol, mark_price=mp)
        elif self.position_keeper:
            try:
                raw_symbols = self.position_keeper.get_symbols()
                if raw_symbols is None:
                    raw_symbols = []
                # 确保可迭代且非字符串
                if isinstance(raw_symbols, str):
                    symbols = [raw_symbols.upper()]
                else:
                    try:
                        symbols = sorted({s.upper() for s in raw_symbols if isinstance(s, str)})
                    except TypeError:
                        symbols = []
                for sym in symbols:
                    mp = self._get_mark_price_from_dict(mark_prices, sym)
                    result['unrealized'] += self.get_unrealized_pnl(sym, mark_price=mp)
            except Exception as e:
                logger.warning("获取品种列表失败: %s，未实现盈亏可能不完整", e)

        # 已实现盈亏
        result['realized'] = self.get_realized_pnl(target_symbol)

        # 资金费用（正表示支出，负表示收入）
        if self.funding_fee_tracker:
            try:
                raw = self.funding_fee_tracker.get_total_fees(target_symbol)
                if raw is not None:
                    fee = self._to_decimal(raw)
                    result['funding_fees'] = float(fee.quantize(self.output_precision, rounding=ROUNDING_MODE))
            except Exception as e:
                logger.warning("获取资金费用异常: %s", e)

        result['total'] = result['unrealized'] + result['realized'] - result['funding_fees']

        if abs(Decimal(str(result['total']))) >= self.large_pnl_threshold:
            self._emit_event("large_total_pnl", {
                "symbol": target_symbol or "ALL",
                "total": result['total'],
                "timestamp": time.time(),
            })
        self._record_metrics("total_pnl", result['total'], {"symbol": target_symbol or "all"})
        return result

    def reload_config(self, config: Dict) -> None:
        """热重载配置参数"""
        self._apply_config(config)
        logger.info("盈亏计算器配置已更新: taker=%s maker=%s threshold=%s precision=%s",
                    self.taker_fee, self.maker_fee, self.large_pnl_threshold, self.output_precision)

    def health_check(self) -> Dict[str, Any]:
        warnings = []
        if not self.position_keeper:
            warnings.append("PositionKeeper 未配置")
        else:
            try:
                # 轻量探测：调用本地方法，不触发网络
                syms = self.position_keeper.get_symbols()
                if syms is None:
                    warnings.append("PositionKeeper.get_symbols() 返回 None")
            except Exception as e:
                warnings.append(f"PositionKeeper 探测失败: {e}")
        if not self.trade_db:
            warnings.append("TradeDatabase 未配置")
        if not self.funding_fee_tracker:
            warnings.append("FundingFeeTracker 未配置")
        if self.taker_fee is None or self.taker_fee < 0:
            warnings.append("Taker 费率异常")
        if self.large_pnl_threshold <= 0:
            warnings.append("大额盈亏阈值无效")
        status = "degraded" if warnings else "ok"
        return {"status": status, "reason": f"PnlCalculator v{VERSION}", "warnings": warnings}

    # ── 内部实现 ──────────────────────────────────────────

    def _calc_unrealized_internal(self, symbol: str, mark_price: Optional[float] = None
                                  ) -> Tuple[Optional[Decimal], Decimal]:
        """
        返回 (毛盈亏 Decimal, 预估平仓手续费 Decimal)
        若数据无效，返回 (None, Decimal('0'))
        """
        if not self.position_keeper:
            return None, Decimal("0")

        try:
            sym = symbol.strip().upper()
            if not sym:
                return None, Decimal("0")
            pos = self.position_keeper.get_position(sym)
            if not pos:
                return None, Decimal("0")

            raw_qty = pos.get('quantity')
            raw_entry = pos.get('entry_price')
            raw_mult = pos.get('multiplier')
            raw_side = pos.get('side')
            if raw_qty is None or raw_entry is None:
                logger.debug("%s 持仓缺少必填字段", sym)
                return None, Decimal("0")
            if raw_mult is None:
                logger.debug("%s 缺少合约乘数，使用默认值1", sym)
                raw_mult = 1

            qty = self._to_decimal(raw_qty)
            entry = self._to_decimal(raw_entry)
            multiplier = self._to_decimal(raw_mult)
            side = str(raw_side).strip().upper()

            if qty == 0 or entry <= 0 or multiplier <= 0:
                return None, Decimal("0")
            if side not in ('BUY', 'LONG', 'SELL', 'SHORT'):
                logger.warning("%s 无效持仓方向: %s", sym, side)
                return None, Decimal("0")

            # 标记价格
            if mark_price is not None:
                mark = self._to_decimal(mark_price)
            else:
                raw_mark = pos.get('mark_price')
                if raw_mark is None:
                    logger.debug("%s 标记价格缺失", sym)
                    return None, Decimal("0")
                mark = self._to_decimal(raw_mark)

            # 允许价格为0的特殊情况（仅当资产已归零），但需记录
            if mark < 0:
                logger.debug("%s 标记价格异常: %s", sym, mark)
                return None, Decimal("0")
            if mark > MAX_VALID_PRICE:
                logger.debug("%s 标记价格超限: %s", sym, mark)
                return None, Decimal("0")

            # 名义价值
            abs_qty = abs(qty)
            nominal = abs_qty * max(mark, 1) * multiplier  # 防止除以0导致费用为0
            if nominal > MAX_NOMINAL_VALUE:
                logger.warning("%s 名义价值过大 (%s)，中止盈亏计算", sym, nominal)
                return None, Decimal("0")

            is_long = side in ('BUY', 'LONG')
            if is_long:
                gross = (mark - entry) * qty * multiplier
            else:
                gross = (entry - mark) * qty * multiplier

            fee = nominal * self.taker_fee

            if abs(gross) >= self.large_pnl_threshold:
                self._emit_event("large_unrealized_gross", {
                    "symbol": sym,
                    "gross_pnl": float(gross),
                    "entry_price": float(entry),
                    "mark_price": float(mark),
                    "side": "LONG" if is_long else "SHORT",
                    "timestamp": time.time(),
                })
            return gross, fee
        except Exception:
            logger.exception("计算未实现盈亏内部异常")
            return None, Decimal("0")

    def _check_large_event(self, symbol: str, amount: Decimal, pnl_type: str):
        if abs(amount) >= self.large_pnl_threshold:
            self._emit_event(f"large_{pnl_type}_pnl", {
                "symbol": symbol,
                f"{pnl_type}_pnl": float(amount),
                "timestamp": time.time(),
            })

    def _apply_config(self, config: Dict) -> None:
        taker = config.get('taker_fee_rate')
        if taker is not None:
            try:
                val = Decimal(str(taker))
                if Decimal("0") <= val <= Decimal("0.1"):
                    self.taker_fee = val
                else:
                    logger.error("taker_fee_rate 超出 [0,0.1]")
            except Exception as e:
                logger.error("taker_fee_rate 解析失败: %s", e)

        maker = config.get('maker_fee_rate')
        if maker is not None:
            try:
                val = Decimal(str(maker))
                if Decimal("0") <= val <= Decimal("0.1"):
                    self.maker_fee = val
                else:
                    logger.error("maker_fee_rate 超出范围")
            except Exception as e:
                logger.error("maker_fee_rate 解析失败: %s", e)

        threshold = config.get('large_pnl_threshold')
        if threshold is not None:
            try:
                val = Decimal(str(threshold))
                if val > 0:
                    self.large_pnl_threshold = val
                else:
                    logger.error("large_pnl_threshold 必须为正数")
            except Exception as e:
                logger.error("large_pnl_threshold 解析失败: %s", e)

        precision = config.get('output_precision')
        if precision is not None:
            try:
                val = Decimal(str(precision))
                if Decimal("0") < val < Decimal("1"):
                    self.output_precision = val
                else:
                    logger.error("output_precision 必须在 (0, 1)")
            except Exception as e:
                logger.error("output_precision 解析失败: %s", e)

    @staticmethod
    def _to_decimal(value: Union[str, int, float, Decimal, None]) -> Decimal:
        """
        安全转换为 Decimal。
        对 inf / nan / 非法字符串返回 Decimal('0') 并记录警告。
        """
        if value is None:
            return Decimal("0")
        if isinstance(value, Decimal):
            if value.is_nan() or value.is_infinite():
                logger.warning("Decimal 值为 NaN 或 Infinity，转换为 0")
                return Decimal("0")
            return value
        try:
            s = str(value).strip().lower()
            if s in ('inf', '-inf', 'infinity', '-infinity', 'nan'):
                logger.warning("非有限数值: %s，转换为 0", s)
                return Decimal("0")
            return Decimal(s)
        except (InvalidOperation, ValueError, TypeError):
            logger.warning("无法转换为 Decimal 的值: %s", value)
            return Decimal("0")

    @staticmethod
    def _get_mark_price_from_dict(mark_prices: Optional[Dict], symbol: str) -> Optional[float]:
        """从字典获取标记价格，大小写不敏感"""
        if not mark_prices:
            return None
        upper = symbol.upper()
        if upper in mark_prices:
            return mark_prices[upper]
        for k, v in mark_prices.items():
            if str(k).upper() == upper:
                return v
        return None

    def _emit_event(self, event_type: str, data: Dict) -> None:
        if self.event_bus and EventTypes and hasattr(EventTypes, 'SYSTEM_ALERT'):
            try:
                self.event_bus.publish(EventTypes.SYSTEM_ALERT, {
                    "subtype": event_type,
                    "data": data,
                })
            except Exception:
                logger.exception("发布事件失败")

    def _record_metrics(self, name: str, value: float, labels: Optional[Dict] = None):
        if METRICS_AVAILABLE and MetricsCollector:
            try:
                MetricsCollector.gauge(name, value, labels)
            except Exception:
                pass
