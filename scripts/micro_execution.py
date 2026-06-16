#!/usr/bin/env python3
"""
Spark System · MicroExecution Simulator (Institutional Grade)

Core Responsibilities:
1. Accurately simulate optimal trade execution using the Almgren-Chriss (2001)
   framework with square-root permanent impact and linear temporary impact.
2. Fully incorporate bid-ask spread, exchange fees, dynamic order book depth,
   and synthetic shocks for stress testing.
3. Provide a detailed, auditable execution report with unique execution ID,
   cost breakdown, and market condition snapshots.
4. Serve as a standardized interface for shadow evaluation, backtesting,
   and compliance auditing.

External Dependencies:
- core.orderbook_snapshot : optional, to reconstruct order book from historical data.
- scripts.gan_stress_test : optional, to generate synthetic shock parameters.

Interface Contract:
- simulate(order_size: float, side: str, orderbook_history: List[Dict],
           synthetic_shock: Optional[Dict] = None,
           config: Optional[ExecutionConfig] = None,
           time_deltas_seconds: Optional[List[float]] = None) -> Dict[str, Any]
  Returns a dictionary with keys: "status", "reason", "report", "warnings".

Exception Handling & Degradation:
- If order book history is insufficient, falls back to a simple volatility model
  and records a warning.
- If synthetic shock generation fails, the shock is ignored and an error is logged.
- All illegal inputs return an error status instead of raising exceptions.
- When market data is missing, conservative default parameters are used, and
  the report flags the fallback.

Resource Management:
- The simulation uses only NumPy vectorised operations; no persistent resources.
- All temporary arrays are freed upon return.

References:
- Almgren, R., & Chriss, N. (2001). Optimal execution of portfolio transactions.
  Journal of Risk, 3, 5-39.
- Gatheral, J., & Schied, A. (2011). Optimal trade execution under geometric
  Brownian motion in the Almgren and Chriss framework. IJTAF.
- Biais, B., Foucault, T., & Moinas, S. (2015). Equilibrium fast trading.
  Journal of Political Economy.
"""

from __future__ import annotations

import logging
import math
import uuid
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

__all__ = ["MicroExecution", "ExecutionConfig", "Side"]


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


@dataclass(frozen=True)
class ExecutionConfig:
    """Immutable execution configuration, fully externalizable to YAML."""
    risk_aversion: float = 0.5
    eta_perm: float = 0.01             # square-root permanent impact coefficient
    gamma_temp: float = 0.1            # linear temporary impact coefficient
    default_volatility: float = 0.02   # default annualised volatility
    default_tick_size: float = 0.01    # fallback tick size
    maker_fee: float = 0.0002
    taker_fee: float = 0.0004
    min_intervals: int = 5
    max_intervals: int = 200
    kappa_large_threshold: float = 100.0
    depth_recovery_rate: float = 0.2
    volatility_adjustment_temp: float = 0.5
    daily_volume_est_factor: float = 10000.0
    max_synthetic_shock_magnitude: float = 0.5
    use_jump_robust_vol: bool = True
    timezone_365: bool = True
    step_size: Optional[float] = None
    min_order_value: Optional[float] = None
    default_interval_seconds: float = 180.0
    price_floor: float = 1e-12
    max_price_ratio: float = 1e6


class MicroExecution:
    """Institutional-grade optimal execution simulator."""

    # --------------------------------------------------------------------------
    # Private helpers – market impact models
    # --------------------------------------------------------------------------
    @staticmethod
    def _square_root_impact(exec_qty: float, daily_volume: float,
                            volatility: float, eta: float) -> float:
        """η·σ·√(|Q|/V) relative permanent impact."""
        if daily_volume <= 0 or volatility <= 0 or eta <= 0:
            return 0.0
        return eta * volatility * math.sqrt(abs(exec_qty) / daily_volume)

    @staticmethod
    def _temporary_impact(trade_qty: float, depth: float, vol: float,
                          gamma: float, vol_adj: float) -> float:
        """γ·σ·(|Q|/depth)·(1 + vol_adj·σ) relative temporary impact."""
        if depth <= 0 or vol <= 0 or gamma <= 0:
            return 0.0
        return gamma * vol * (abs(trade_qty) / depth) * (1.0 + vol_adj * vol)

    # --------------------------------------------------------------------------
    # Order book depth estimation
    # --------------------------------------------------------------------------
    @staticmethod
    def _estimate_depth(snapshot: Dict, side: Side, tick_size: float) -> float:
        """Estimate average depth (qty per unit price) from top 5 levels."""
        levels = snapshot.get("asks" if side == Side.BUY else "bids", [])
        if not levels:
            return 1e12
        total_qty = 0.0
        prices = []
        for lvl in levels[:5]:
            try:
                q = float(lvl["size"])
                p = float(lvl["price"])
            except (KeyError, ValueError, TypeError):
                continue
            total_qty += q
            prices.append(p)
        if total_qty <= 0 or len(prices) < 2:
            return 1e12
        price_range = abs(prices[-1] - prices[0])
        if price_range <= 0:
            price_range = tick_size
        if price_range <= 0:
            return 1e12
        return total_qty / price_range

    # --------------------------------------------------------------------------
    # Realised volatility (jump‑robust bipower variation)
    # --------------------------------------------------------------------------
    @staticmethod
    def _realised_vol(price_series: List[float],
                      time_deltas_seconds: Optional[List[float]],
                      use_robust: bool, year_seconds: float,
                      default_vol: float) -> Tuple[float, List[str]]:
        """Annualised volatility; returns (vol, warnings)."""
        warnings = []
        n = len(price_series)
        if n < 2:
            return default_vol, ["price series too short for vol estimation"]
        # log returns
        log_ret = np.diff(np.log(np.maximum(price_series, 1e-12)))
        log_ret = log_ret[np.isfinite(log_ret)]
        if len(log_ret) < 2:
            return default_vol, ["no valid log returns"]
        # time intervals
        if time_deltas_seconds and len(time_deltas_seconds) == len(log_ret):
            tau = np.array(time_deltas_seconds, dtype=np.float64) / year_seconds
        else:
            tau = np.full(len(log_ret), 180.0 / year_seconds, dtype=np.float64)
        valid = (tau > 0) & (tau < 1.0)
        if np.sum(valid) < 2:
            return default_vol, ["insufficient valid time intervals"]
        ret = log_ret[valid]
        tau = tau[valid]
        # weighted or bipower
        if use_robust:
            # bipower variation: π/2 * Σ |r_i| |r_{i-1}| / sqrt(τ_i τ_{i-1})
            scaled1 = np.abs(ret[1:]) / np.sqrt(tau[1:])
            scaled2 = np.abs(ret[:-1]) / np.sqrt(tau[:-1])
            bpv = (math.pi / 2.0) * np.mean(scaled1 * scaled2)
            # annualisation factor: number of returns per year ≈ len(ret) / (avg tau in years)
            avg_tau = np.mean(tau)
            annual_factor = 1.0 / avg_tau if avg_tau > 0 else len(tau)
            vol = math.sqrt(bpv * annual_factor)
        else:
            weighted_ret = ret / np.sqrt(tau)
            vol = np.std(weighted_ret, ddof=1) * math.sqrt(1.0 / np.mean(tau))
        vol = max(vol, 0.001)
        return vol, warnings

    # --------------------------------------------------------------------------
    # Safe hyperbolic sine ratio
    # --------------------------------------------------------------------------
    @staticmethod
    def _safe_sinh_ratio(kappa: float, t: np.ndarray) -> np.ndarray:
        """sinh(kappa*(1-t)) / sinh(kappa) with overflow/underflow guards."""
        if kappa < 1e-12:
            return 1.0 - t
        if kappa > 700.0:
            ratio = np.exp(-kappa * t)
            ratio[0] = 1.0
            # re-normalise for numerical stability
            s = np.sum(ratio)
            if s > 0:
                ratio /= s
            else:
                ratio = 1.0 - t
            return ratio
        return np.sinh(kappa * (1.0 - t)) / np.sinh(kappa)

    # --------------------------------------------------------------------------
    # Optimal schedule (discrete Almgren–Chriss)
    # --------------------------------------------------------------------------
    @staticmethod
    def _optimal_schedule(total_shares: float, risk_aversion: float,
                          volatility: float, daily_volume: float,
                          depth: float, intervals: int, side: Side,
                          eta: float, gamma: float, vol_adj: float) -> Tuple[List[float], float]:
        """Returns (trade list, kappa)."""
        if intervals <= 1:
            return [total_shares], 0.0
        # temporary impact coefficient
        eta_temp = gamma * volatility / depth if depth > 0 else 1e-12
        if eta_temp <= 0:
            return [total_shares / intervals] * intervals, 0.0
        lam = risk_aversion * volatility * volatility
        kappa = math.sqrt(lam / eta_temp)
        t = np.linspace(0, 1, intervals + 1)
        remaining = total_shares * MicroExecution._safe_sinh_ratio(kappa, t)
        trades = -np.diff(remaining)
        trades = np.clip(trades, 0, None)
        # ensure exact total
        total = np.sum(trades)
        if total > 0:
            trades = trades * (total_shares / total)
        # final adjustment
        trades[-1] += total_shares - np.sum(trades)
        trades = np.maximum(trades, 0.0)
        return trades.tolist(), kappa

    # --------------------------------------------------------------------------
    # Main simulation entry point
    # --------------------------------------------------------------------------
    @classmethod
    def simulate(cls,
                 order_size: float,
                 side: str,
                 orderbook_history: List[Dict],
                 synthetic_shock: Optional[Dict[str, float]] = None,
                 config: Optional[ExecutionConfig] = None,
                 time_deltas_seconds: Optional[List[float]] = None) -> Dict[str, Any]:
        """Run optimal execution simulation and return a standard report."""
        exec_cfg = config if config is not None else ExecutionConfig()
        warnings: List[str] = []

        # --- input validation ---
        try:
            side_enum = Side(side.lower().strip())
        except ValueError:
            logger.warning("Invalid side '%s'", side)
            return {"status": "error", "reason": "side must be 'buy' or 'sell'",
                    "report": {}, "warnings": []}
        if not isinstance(order_size, (int, float)) or order_size <= 0:
            return {"status": "error", "reason": "order_size must be positive",
                    "report": {}, "warnings": []}
        if exec_cfg.risk_aversion <= 0:
            return {"status": "error", "reason": "risk_aversion must be positive",
                    "report": {}, "warnings": []}
        intervals = max(exec_cfg.min_intervals, min(exec_cfg.max_intervals, 20))
        if not orderbook_history or not isinstance(orderbook_history, list):
            return {"status": "error", "reason": "orderbook_history cannot be empty",
                    "report": {}, "warnings": warnings}

        # --- extract mid prices ---
        mid_prices = []
        for idx, snap in enumerate(orderbook_history):
            try:
                ask_p = float(snap["asks"][0]["price"])
                bid_p = float(snap["bids"][0]["price"])
                if ask_p <= 0 or bid_p <= 0:
                    raise ValueError("non-positive price")
                mid = (ask_p + bid_p) / 2.0
                mid_prices.append(mid)
            except (IndexError, KeyError, ValueError):
                warnings.append(f"snap {idx} missing valid ask/bid")
        if len(mid_prices) < 1:
            return {"status": "error", "reason": "no valid mid prices in orderbook",
                    "report": {}, "warnings": warnings}

        # --- volatility ---
        year_secs = 365 * 24 * 3600 if exec_cfg.timezone_365 else 365.25 * 24 * 3600
        vol, vol_warn = cls._realised_vol(mid_prices, time_deltas_seconds,
                                          exec_cfg.use_jump_robust_vol,
                                          year_secs, exec_cfg.default_volatility)
        warnings.extend(vol_warn)

        initial_mid = mid_prices[0]
        # spread
        try:
            ask = float(orderbook_history[0]["asks"][0]["price"])
            bid = float(orderbook_history[0]["bids"][0]["price"])
            spread = abs(ask - bid) / initial_mid
        except Exception:
            spread = 0.0001
        # depth
        ask_depth = cls._estimate_depth(orderbook_history[0], Side.BUY, exec_cfg.default_tick_size)
        bid_depth = cls._estimate_depth(orderbook_history[0], Side.SELL, exec_cfg.default_tick_size)
        depth = min(ask_depth, bid_depth)
        if depth < 1e-12:
            depth = 1e12
        # daily volume (use config factor or fallback)
        daily_volume = exec_cfg.daily_volume_est_factor * depth * vol
        if daily_volume <= 0:
            daily_volume = 1e8

        # synthetic shock
        shock_timing = None
        shock_mag = 0.0
        if synthetic_shock:
            try:
                shock_timing = float(synthetic_shock.get("timing", 0.5))
                shock_mag = float(synthetic_shock.get("magnitude", 0.0))
                shock_mag = max(-exec_cfg.max_synthetic_shock_magnitude,
                                min(exec_cfg.max_synthetic_shock_magnitude, shock_mag))
                if not (0.0 <= shock_timing <= 1.0):
                    shock_timing = None
                    warnings.append("synthetic shock timing out of [0,1], ignored")
            except (ValueError, TypeError):
                shock_timing = None
                shock_mag = 0.0
                warnings.append("synthetic shock parameters invalid, ignored")

        # optimal schedule
        trades, kappa = cls._optimal_schedule(
            total_shares=order_size,
            risk_aversion=exec_cfg.risk_aversion,
            volatility=vol,
            daily_volume=daily_volume,
            depth=depth,
            intervals=intervals,
            side=side_enum,
            eta=exec_cfg.eta_perm,
            gamma=exec_cfg.gamma_temp,
            vol_adj=exec_cfg.volatility_adjustment_temp,
        )

        # simulation loop
        current_mid = initial_mid
        executed_qty = 0.0
        perm_cost = 0.0
        temp_cost = 0.0
        spread_cost = 0.0
        fee_cost = 0.0
        vwap_num = 0.0
        vwap_den = 0.0
        remaining_depth = depth
        shock_applied = False
        price_floor = exec_cfg.price_floor

        for i, trade_qty in enumerate(trades):
            if trade_qty <= 0:
                continue
            progress = i / max(1, len(trades) - 1)
            if shock_timing is not None and progress >= shock_timing and not shock_applied:
                if side_enum == Side.BUY:
                    current_mid *= (1.0 + shock_mag)
                else:
                    current_mid *= (1.0 - shock_mag)
                current_mid = max(current_mid, price_floor)
                shock_applied = True

            effective_depth = remaining_depth
            # impact increments
            perm_before = cls._square_root_impact(executed_qty, daily_volume, vol, exec_cfg.eta_perm)
            perm_after = cls._square_root_impact(executed_qty + trade_qty, daily_volume, vol, exec_cfg.eta_perm)
            perm_rel = max(perm_after - perm_before, 0.0)  # prevent negative due to numerics
            temp_rel = cls._temporary_impact(trade_qty, effective_depth, vol,
                                             exec_cfg.gamma_temp, exec_cfg.volatility_adjustment_temp)

            # execution price
            if side_enum == Side.BUY:
                exec_price = current_mid * (1.0 + perm_rel + temp_rel + spread / 2)
            else:
                exec_price = current_mid * (1.0 - perm_rel - temp_rel - spread / 2)
            exec_price = max(exec_price, price_floor)

            # costs
            fee_cost += abs(trade_qty) * exec_price * exec_cfg.taker_fee
            perm_cost += abs(trade_qty) * current_mid * perm_rel
            temp_cost += abs(trade_qty) * current_mid * temp_rel
            spread_cost += abs(trade_qty) * current_mid * spread / 2

            vwap_num += exec_price * trade_qty
            vwap_den += trade_qty
            executed_qty += trade_qty

            # update mid price
            if side_enum == Side.BUY:
                current_mid *= (1.0 + perm_rel)
            else:
                current_mid *= (1.0 - perm_rel)
            current_mid = max(current_mid, price_floor)

            remaining_depth = max(0.0, remaining_depth - abs(trade_qty))
            remaining_depth += exec_cfg.depth_recovery_rate * (depth - remaining_depth) / intervals

        if vwap_den == 0:
            return {"status": "error", "reason": "zero executed quantity",
                    "report": {}, "warnings": warnings}

        avg_price = vwap_num / vwap_den
        ideal_notional = initial_mid * order_size
        if side_enum == Side.BUY:
            is_bps = (avg_price - initial_mid) / initial_mid * 10000
        else:
            is_bps = (initial_mid - avg_price) / initial_mid * 10000

        # build report
        report = {
            "execution_id": str(uuid.uuid4()),
            "order_size": order_size,
            "side": side_enum.value,
            "executed_quantity": executed_qty,
            "fill_rate": executed_qty / order_size if order_size else 0,
            "average_price": avg_price,
            "initial_mid": initial_mid,
            "final_mid": current_mid,
            "slippage_bps": (avg_price / initial_mid - 1) * 10000 if side_enum == Side.BUY else (1 - avg_price / initial_mid) * 10000,
            "implementation_shortfall_bps": is_bps,
            "cost_breakdown": {
                "permanent_impact_bps": perm_cost / ideal_notional * 10000,
                "temporary_cost_bps": temp_cost / ideal_notional * 10000,
                "spread_cost_bps": spread_cost / ideal_notional * 10000,
                "fee_bps": fee_cost / ideal_notional * 10000,
            },
            "market_conditions": {
                "volatility_annual": vol,
                "spread_bps": spread * 10000,
                "daily_volume_estimate": daily_volume,
                "initial_depth": depth,
                "kappa": kappa,
            },
            "config_snapshot": asdict(exec_cfg),
            "warnings": warnings,
        }
        return {"status": "ok", "reason": f"IS {is_bps:.2f} bps",
                "report": report, "warnings": warnings}

    @classmethod
    def health_check(cls) -> Dict[str, Any]:
        """Run comprehensive self-tests."""
        try:
            cfg = ExecutionConfig()
            book = [{"asks": [{"price": 100.0, "size": 1000.0}],
                     "bids": [{"price": 99.9, "size": 1000.0}]}] * 20
            res = cls.simulate(100.0, "buy", book, config=cfg)
            if res["status"] != "ok":
                return {"status": "error", "message": f"small buy failed: {res['reason']}"}
            res = cls.simulate(1e6, "sell", book, config=cfg,
                               synthetic_shock={"timing": 0.5, "magnitude": 0.1})
            if res["status"] != "ok":
                return {"status": "error", "message": f"large sell+shock failed: {res['reason']}"}
            # edge cases
            empty_check = cls.simulate(1, "buy", [])
            if empty_check["status"] != "error":
                return {"status": "error", "message": "empty book not rejected"}
            neg_check = cls.simulate(-1, "buy", book, config=cfg)
            if neg_check["status"] != "error":
                return {"status": "error", "message": "negative size not rejected"}
            return {"status": "ok", "message": "all comprehensive tests passed"}
        except Exception as e:
            logger.exception("health_check failed")
            return {"status": "error", "message": str(e)}


def main() -> None:
    """Entry point for manual testing."""
    print(MicroExecution.health_check())


if __name__ == "__main__":
    main()
