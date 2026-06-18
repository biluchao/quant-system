#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 粒子滤波器 (ParticleFilter) v6.0.0 — 机构级终极版

核心职责：
1. 双变量 Ornstein-Uhlenbeck 过程的粒子滤波，联合估计趋势强度 μ 与回归强度 θ
2. θ 通过可配置的线性耦合系数 γ 参与观测模型，提升滤波完整性
3. 全部超参数可热更新，并自动校验与裁剪，更新后立即适配粒子状态
4. 观测异常值自动检测与降权，保障滤波器稳健性
5. 支持审计日志、性能统计、状态快照、强制重采样、多样性保护
6. 明确非线程安全，由调用方保证串行访问，避免并发竞态

外部依赖：
- numpy (>=1.20) : 数学计算与随机采样
- core.metrics.MetricsCollector (可选) : Prometheus 指标
- core.event_bus.EventBus (可选) : 发布滤波器异常事件

接口契约：
- predict(dt) -> None
- update(observation, obs_noise=None) -> None
- resample(threshold_ratio=None) -> bool
- force_resample() -> None
- get_estimates() -> Dict[str, Any]
- reset() -> None
- copy() -> ParticleFilter           (独立随机状态)
- get_state() -> Dict
- set_state(state: Dict) -> None
- health_check() -> Dict[str, Any]

异常与降级：
- 所有数值异常均被捕获，滤波器状态保持不变
- 非法观测跳过更新并告警
- 重采样失败回退系统重采样，并注入扰动
"""

import logging
import math
import copy
import time
from typing import Dict, Any, Optional, Tuple, List

logger = logging.getLogger(__name__)

VERSION = "6.0.0"
SPDX_IDENTIFIER = "Apache-2.0"

try:
    import numpy as np
    from numpy.random import default_rng, Generator
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False
    np = None
    default_rng = None
    Generator = None

try:
    from core.metrics import MetricsCollector
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    MetricsCollector = None

try:
    from core.event_bus import EventBus, EventTypes
    EVENT_BUS_AVAILABLE = True
except ImportError:
    EVENT_BUS_AVAILABLE = False
    EventBus = None
    EventTypes = None

# ── 常量 ──────────────────────────────────────────────────
DEFAULT_NUM_PARTICLES = 50
DEFAULT_RESAMPLE_THRESHOLD_RATIO = 0.5
LOG_LIKELIHOOD_CLIP = (-690.0, 0.0)            # exp(-690) ≈ 1e-300
MIN_OBS_NOISE = 1e-8
MAX_OBS_NOISE = 10.0
MIN_PROCESS_NOISE = 0.0
DEFAULT_STATE_CLIP_BOUND = 8.0
DIVERSITY_NOISE_BASE_SCALE = 0.01
MIN_UNIQUE_PARTICLE_FRACTION = 0.1
GAMMA_COUPLING_DEFAULT = 0.1
MAX_DT_FOR_PREDICT = 10.0
OUTLIER_SIGMA_THRESHOLD = 4.0                # 观测异常值阈值（标准差的倍数）
SEARCHSORTED_EPS = 1e-12                      # 防止 searchsorted 越界的微小偏移
MAX_PARTICLES = 10000


def _weighted_quantile(values: np.ndarray, weights: np.ndarray, quantiles: np.ndarray) -> np.ndarray:
    """加权分位数，线性插值，values 与 weights 同步排序"""
    if len(values) != len(weights):
        raise ValueError("values 和 weights 长度必须相等")
    if len(values) == 0:
        return np.full_like(quantiles, np.nan)
    sorter = np.argsort(values)
    sorted_values = values[sorter]
    sorted_weights = weights[sorter]
    cumsum = np.cumsum(sorted_weights)
    total = cumsum[-1]
    if total <= 0:
        return np.linspace(sorted_values[0], sorted_values[-1], len(quantiles))
    cumsum = cumsum / total
    idx = np.searchsorted(cumsum, quantiles, side='left')
    idx = np.clip(idx, 0, len(sorted_values) - 1)
    return sorted_values[idx]


class ParticleFilter:
    """双变量 OU 过程粒子滤波器，支持 θ 耦合观测，异常值稳健，全参数热更新"""

    def __init__(self, num_particles: int = DEFAULT_NUM_PARTICLES,
                 mu_ou: float = 0.1, theta_ou: float = 0.3,
                 sigma_mu: float = 0.05, sigma_theta: float = 0.1,
                 observation_noise: float = 0.02,
                 gamma_coupling: float = GAMMA_COUPLING_DEFAULT,
                 state_clip_bound: float = DEFAULT_STATE_CLIP_BOUND,
                 seed: Optional[int] = None,
                 max_particles: int = MAX_PARTICLES,
                 rng: Optional['Generator'] = None,
                 event_bus=None):
        if not NUMPY_AVAILABLE:
            raise ImportError("numpy 是粒子滤波的必要依赖")
        if not isinstance(num_particles, int) or num_particles < 5 or num_particles > max_particles:
            raise ValueError(f"粒子数必须为整数且在 [5, {max_particles}] 之间")
        if sigma_mu < 0 or sigma_theta < 0 or observation_noise <= MIN_OBS_NOISE:
            raise ValueError("过程/观测噪声必须为正 (观测噪声 > 1e-8)")
        if mu_ou < 0 or theta_ou < 0:
            raise ValueError("均值回复速率必须非负")
        if not isinstance(gamma_coupling, (int, float)):
            raise ValueError("gamma_coupling 必须为数值")
        if state_clip_bound <= 0:
            raise ValueError("状态裁剪边界必须为正")

        self.num_particles = num_particles
        self.mu_ou = mu_ou
        self.theta_ou = theta_ou
        self.sigma_mu = sigma_mu
        self.sigma_theta = sigma_theta
        self.obs_noise = observation_noise
        self.gamma = gamma_coupling
        self.state_clip_bound = state_clip_bound

        # 随机数生成器（独立状态）
        if rng is not None:
            if not isinstance(rng, Generator):
                raise TypeError("rng 必须是 numpy.random.Generator")
            self.rng = rng
        else:
            self.rng = default_rng(seed)

        # 粒子状态
        self.mu_particles = self.rng.normal(0.0, 1.0, size=num_particles).astype(np.float64)
        self.theta_particles = self.rng.normal(0.0, 1.0, size=num_particles).astype(np.float64)

        # 权重
        self.log_weights = np.full(num_particles, -math.log(num_particles), dtype=np.float64)
        self.weights = np.full(num_particles, 1.0 / num_particles, dtype=np.float64)
        self.ess = num_particles

        # 统计
        self._resample_count = 0
        self._update_count = 0
        self._predict_count = 0
        self._last_observation = None

        # 性能计时（可选）
        self._last_update_time: Optional[float] = None
        self._last_resample_time: Optional[float] = None

        # 审计日志
        self._audit_log: List[Dict] = []

        # 事件总线（可选）
        self.event_bus = event_bus

        # 缓存观测噪声归一化常数，提高 update 效率
        self._precalc_log_norm_const()

        logger.info("ParticleFilter v%s 初始化: N=%d, μ_ou=%.3f, γ=%.2f, seed=%s, clip=%.1f",
                    VERSION, num_particles, mu_ou, gamma_coupling, seed, state_clip_bound)

    def _precalc_log_norm_const(self):
        self._cached_log_norm = -0.5 * math.log(2 * math.pi * self.obs_noise * self.obs_noise)

    # ── 公共接口 ──────────────────────────────────────────

    def predict(self, dt: float = 1.0) -> None:
        """前向传播粒子状态 dt 时间单位"""
        if dt <= 1e-9:
            return
        if dt > MAX_DT_FOR_PREDICT:
            logger.warning("dt=%.2f 超过最大值 %.1f，已裁剪", dt, MAX_DT_FOR_PREDICT)
            dt = MAX_DT_FOR_PREDICT

        drift_mu = -self.mu_ou * self.mu_particles * dt
        drift_theta = -self.theta_ou * self.theta_particles * dt

        vol_mu = self.sigma_mu * math.sqrt(dt)
        vol_theta = self.sigma_theta * math.sqrt(dt)

        # 仅当 vol > 0 时生成随机数，否则扩散为 0
        if vol_mu > 0:
            diffusion_mu = self.rng.normal(0, vol_mu, size=self.num_particles)
        else:
            diffusion_mu = 0.0
        if vol_theta > 0:
            diffusion_theta = self.rng.normal(0, vol_theta, size=self.num_particles)
        else:
            diffusion_theta = 0.0

        self.mu_particles += drift_mu + diffusion_mu
        self.theta_particles += drift_theta + diffusion_theta

        # 裁剪极端值
        self.mu_particles = np.clip(self.mu_particles, -self.state_clip_bound, self.state_clip_bound)
        self.theta_particles = np.clip(self.theta_particles, -self.state_clip_bound, self.state_clip_bound)

        # 替换 NaN
        self._sanitize_particles()

        self._predict_count += 1

    def update(self, observation: float, obs_noise: Optional[float] = None) -> None:
        """
        根据观测值更新粒子权重。
        观测模型: obs ~ N(μ + γ * θ, noise²)
        支持异常值检测：若观测偏离加权均值超过 OUTLIER_SIGMA_THRESHOLD * noise，则降低其影响（仍用于更新）
        """
        if not math.isfinite(observation):
            logger.warning("非有限观测值 %.4f，跳过更新", observation)
            return

        self._update_count += 1
        self._last_observation = observation

        noise = obs_noise if obs_noise is not None else self.obs_noise
        if noise <= MIN_OBS_NOISE:
            noise = self.obs_noise
        if noise > MAX_OBS_NOISE:
            logger.warning("观测噪声 %.4f 超过上限，裁剪为 %.4f", noise, MAX_OBS_NOISE)
            noise = MAX_OBS_NOISE

        # 预测观测：μ + γ * θ
        predicted_obs = self.mu_particles + self.gamma * self.theta_particles

        # 异常值检测：基于当前加权均值与标准差
        w = self.weights
        mean_pred = np.average(predicted_obs, weights=w)
        var_pred = np.average((predicted_obs - mean_pred) ** 2, weights=w)
        std_pred = math.sqrt(max(0.0, var_pred))
        if std_pred > 1e-8:
            deviation = abs(observation - mean_pred) / (noise + std_pred)
            if deviation > OUTLIER_SIGMA_THRESHOLD:
                logger.info("检测到异常观测 (偏离 %.1fσ): %.4f", deviation, observation)
                # 降低该观测的有效噪声（增大方差），相当于减弱其影响
                noise *= deviation / OUTLIER_SIGMA_THRESHOLD

        # 对数似然
        log_norm_const = -0.5 * math.log(2 * math.pi * noise * noise)
        squared_diff = (observation - predicted_obs) ** 2
        log_likelihood = log_norm_const - 0.5 * squared_diff / (noise * noise)

        # 裁剪
        log_likelihood = np.clip(log_likelihood, LOG_LIKELIHOOD_CLIP[0], LOG_LIKELIHOOD_CLIP[1])

        self.log_weights += log_likelihood

        max_log = np.max(self.log_weights)
        if not math.isfinite(max_log) or max_log < LOG_LIKELIHOOD_CLIP[0]:
            self._reset_weights()
            self._audit_event("weight_degeneracy", "所有权重退化，已重置为均匀")
        else:
            self.log_weights -= max_log
            self.weights = np.exp(self.log_weights)
            sum_w = np.sum(self.weights)
            if sum_w > 0:
                self.weights /= sum_w
            else:
                self._reset_weights()

        ess_inv = np.sum(self.weights ** 2)
        self.ess = 1.0 / ess_inv if ess_inv > 1e-12 else self.num_particles

        self._last_update_time = time.perf_counter()

    def resample(self, threshold_ratio: Optional[float] = None) -> bool:
        """
        如果有效样本数低于阈值比例则重采样。
        """
        if threshold_ratio is None:
            threshold_ratio = DEFAULT_RESAMPLE_THRESHOLD_RATIO
        threshold = max(1, int(threshold_ratio * self.num_particles))

        if self.ess >= threshold:
            return False

        self._do_resample()
        return True

    def force_resample(self) -> None:
        """强制重采样，忽略有效样本数，并记录审计"""
        self._do_resample()
        logger.info("强制重采样执行")

    def get_estimates(self) -> Dict[str, Any]:
        """获取加权后验统计，使用无偏加权方差，并附加收敛诊断"""
        w = self.weights
        x = self.mu_particles
        y = self.theta_particles

        # 清除 NaN 影响
        if np.any(np.isnan(x)) or np.any(np.isnan(y)):
            self._sanitize_particles()
            w = self.weights
            x = self.mu_particles
            y = self.theta_particles

        mu_mean = float(np.average(x, weights=w))
        theta_mean = float(np.average(y, weights=w))

        V1 = np.sum(w)
        V2 = np.sum(w ** 2)
        denom = V1 - V2
        if denom <= 1e-12:
            mu_var = 0.0
            theta_var = 0.0
        else:
            mu_var = np.sum(w * (x - mu_mean) ** 2) / denom
            theta_var = np.sum(w * (y - theta_mean) ** 2) / denom
        mu_std = math.sqrt(max(0.0, mu_var))
        theta_std = math.sqrt(max(0.0, theta_var))

        prob_div = float(np.sum(w[x > 0]))

        q_vals = np.array([0.05, 0.25, 0.5, 0.75, 0.95])
        mu_q = _weighted_quantile(x, w, q_vals).tolist()
        theta_q = _weighted_quantile(y, w, q_vals).tolist()

        # 粒子多样性指标
        unique_fraction = len(np.unique(x.round(decimals=6))) / self.num_particles

        return {
            "mu_mean": mu_mean,
            "mu_std": mu_std,
            "theta_mean": theta_mean,
            "theta_std": theta_std,
            "prob_divergence": prob_div,
            "ess": self.ess,
            "mu_quantiles": mu_q,
            "theta_quantiles": theta_q,
            "unique_fraction": unique_fraction,
        }

    def reset(self) -> None:
        """重置滤波器至先验状态，清除所有统计与审计日志"""
        self.mu_particles = self.rng.normal(0.0, 1.0, size=self.num_particles)
        self.theta_particles = self.rng.normal(0.0, 1.0, size=self.num_particles)
        self._reset_weights()
        self._resample_count = 0
        self._update_count = 0
        self._predict_count = 0
        self._last_observation = None
        self._audit_log.clear()
        logger.info("粒子滤波器已重置")

    def copy(self) -> 'ParticleFilter':
        """
        深拷贝滤波器状态，但随机数生成器重新初始化（独立序列）。
        拷贝后的滤波器与原滤波器状态一致但随机序列独立，避免同步。
        """
        # 先获取当前状态
        state = self.get_state()
        # 创建新滤波器，使用新的随机种子
        new_pf = ParticleFilter(num_particles=self.num_particles,
                                mu_ou=self.mu_ou, theta_ou=self.theta_ou,
                                sigma_mu=self.sigma_mu, sigma_theta=self.sigma_theta,
                                observation_noise=self.obs_noise,
                                gamma_coupling=self.gamma,
                                state_clip_bound=self.state_clip_bound,
                                seed=None,  # 随机种子
                                rng=default_rng())  # 独立随机生成器
        new_pf.set_state(state)
        return new_pf

    # ── 状态序列化与审计 ──────────────────────────────────

    def get_state(self) -> Dict:
        return {
            "mu_particles": self.mu_particles.tolist(),
            "theta_particles": self.theta_particles.tolist(),
            "log_weights": self.log_weights.tolist(),
            "weights": self.weights.tolist(),
            "ess": self.ess,
            "num_particles": self.num_particles,
            "update_count": self._update_count,
            "resample_count": self._resample_count,
            "hyperparams": {
                "mu_ou": self.mu_ou,
                "theta_ou": self.theta_ou,
                "sigma_mu": self.sigma_mu,
                "sigma_theta": self.sigma_theta,
                "obs_noise": self.obs_noise,
                "gamma": self.gamma,
                "state_clip_bound": self.state_clip_bound,
            },
            "audit_log": self._audit_log.copy(),
        }

    def set_state(self, state: Dict) -> None:
        """从快照恢复滤波器，校验一致性，并重新计算 ess"""
        if state["num_particles"] != self.num_particles:
            raise ValueError("状态快照中的粒子数与当前滤波器不匹配")
        self.mu_particles = np.array(state["mu_particles"], dtype=np.float64)
        self.theta_particles = np.array(state["theta_particles"], dtype=np.float64)
        self.log_weights = np.array(state["log_weights"], dtype=np.float64)
        self.weights = np.array(state["weights"], dtype=np.float64)
        self._update_count = state.get("update_count", 0)
        self._resample_count = state.get("resample_count", 0)
        # 重新计算有效样本数
        ess_inv = np.sum(self.weights ** 2)
        self.ess = 1.0 / ess_inv if ess_inv > 1e-12 else self.num_particles
        # 恢复超参数（不覆盖现有值，除非显式提供）
        hp = state.get("hyperparams", {})
        if hp:
            self.mu_ou = hp.get("mu_ou", self.mu_ou)
            self.theta_ou = hp.get("theta_ou", self.theta_ou)
            self.sigma_mu = hp.get("sigma_mu", self.sigma_mu)
            self.sigma_theta = hp.get("sigma_theta", self.sigma_theta)
            self.obs_noise = hp.get("obs_noise", self.obs_noise)
            self.gamma = hp.get("gamma", self.gamma)
            self.state_clip_bound = hp.get("state_clip_bound", self.state_clip_bound)
            self._precalc_log_norm_const()
        # 审计日志
        self._audit_log = state.get("audit_log", [])
        # 裁剪现有粒子以适应可能的新边界
        self.mu_particles = np.clip(self.mu_particles, -self.state_clip_bound, self.state_clip_bound)
        self.theta_particles = np.clip(self.theta_particles, -self.state_clip_bound, self.state_clip_bound)
        logger.info("粒子滤波器状态已从快照恢复")

    def _audit_event(self, event_type: str, details: str):
        """记录内部审计事件"""
        entry = {"time": time.time(), "type": event_type, "details": details}
        self._audit_log.append(entry)
        self._emit_alert(event_type, details)

    # ── 超参数热更新 ──────────────────────────────────────

    def set_hyperparams(self, mu_ou=None, theta_ou=None, sigma_mu=None, sigma_theta=None,
                        obs_noise=None, gamma=None, state_clip_bound=None):
        """批量更新超参数，自动记录审计日志，并适配当前粒子状态"""
        changes = []
        if mu_ou is not None:
            if mu_ou < 0: raise ValueError("mu_ou 必须非负")
            self.mu_ou = mu_ou
            changes.append("mu_ou")
        if theta_ou is not None:
            if theta_ou < 0: raise ValueError("theta_ou 必须非负")
            self.theta_ou = theta_ou
            changes.append("theta_ou")
        if sigma_mu is not None:
            if sigma_mu < 0: raise ValueError("sigma_mu 必须非负")
            self.sigma_mu = sigma_mu
            changes.append("sigma_mu")
        if sigma_theta is not None:
            if sigma_theta < 0: raise ValueError("sigma_theta 必须非负")
            self.sigma_theta = sigma_theta
            changes.append("sigma_theta")
        if obs_noise is not None:
            if obs_noise <= MIN_OBS_NOISE or obs_noise > MAX_OBS_NOISE:
                raise ValueError(f"obs_noise 必须在 ({MIN_OBS_NOISE}, {MAX_OBS_NOISE}]")
            self.obs_noise = obs_noise
            self._precalc_log_norm_const()
            changes.append("obs_noise")
        if gamma is not None:
            self.gamma = gamma
            changes.append("gamma")
        if state_clip_bound is not None:
            if state_clip_bound <= 0: raise ValueError("state_clip_bound 必须 > 0")
            self.state_clip_bound = state_clip_bound
            # 裁剪现有粒子
            self.mu_particles = np.clip(self.mu_particles, -state_clip_bound, state_clip_bound)
            self.theta_particles = np.clip(self.theta_particles, -state_clip_bound, state_clip_bound)
            changes.append("state_clip_bound")
        if changes:
            self._audit_event("hyperparams_updated", f"修改: {', '.join(changes)}")
            logger.info("超参数更新: %s", changes)

    # ── 内部方法 ──────────────────────────────────────────

    def _do_resample(self):
        """实际执行重采样，包含多样性保护与性能计时"""
        start = time.perf_counter()
        try:
            indices = self._stratified_resample()
        except Exception as e:
            logger.warning("分层重采样失败，回退系统重采样: %s", e)
            indices = self._systematic_resample()

        unique_fraction = len(np.unique(indices)) / self.num_particles
        if unique_fraction < MIN_UNIQUE_PARTICLE_FRACTION:
            logger.warning("粒子多样性不足 (%.2f)，注入状态比例噪声", unique_fraction)
            scale = DIVERSITY_NOISE_BASE_SCALE * self.state_clip_bound
            self.mu_particles += self.rng.normal(0, scale, self.num_particles)
            self.theta_particles += self.rng.normal(0, scale, self.num_particles)
            self._sanitize_particles()
            # 重新分层
            indices = self._stratified_resample() if np.random.rand() > 0.5 else self._systematic_resample()

        self.mu_particles = self.mu_particles[indices]
        self.theta_particles = self.theta_particles[indices]
        self._reset_weights()
        self._resample_count += 1
        self._last_resample_time = start
        logger.info("重采样完成 (第 %d 次), ESS=%.1f", self._resample_count, self.num_particles)
        self._audit_event("resample", f"ess_before={self.ess}, count={self._resample_count}")

    def _reset_weights(self):
        self.log_weights = np.full(self.num_particles, -math.log(self.num_particles))
        self.weights = np.full(self.num_particles, 1.0 / self.num_particles)
        self.ess = self.num_particles

    def _sanitize_particles(self):
        # 用粒子均值与标准差替换 NaN
        for particles in (self.mu_particles, self.theta_particles):
            nan_mask = np.isnan(particles)
            if np.any(nan_mask):
                mean_val = np.mean(particles[~nan_mask]) if np.any(~nan_mask) else 0.0
                std_val = np.std(particles[~nan_mask]) if np.any(~nan_mask) else 1.0
                particles[nan_mask] = self.rng.normal(mean_val, max(std_val, 0.01), size=np.sum(nan_mask))

    def _stratified_resample(self) -> np.ndarray:
        cumsum = np.cumsum(self.weights)
        if cumsum[-1] <= 0:
            return self.rng.choice(self.num_particles, size=self.num_particles, replace=True)
        cumsum /= cumsum[-1]
        rand = (self.rng.random(self.num_particles) + np.arange(self.num_particles)) / self.num_particles
        # 确保严格小于 1，避免 searchsorted 越界
        rand = np.minimum(rand, 1.0 - SEARCHSORTED_EPS)
        return np.searchsorted(cumsum, rand, side='left')

    def _systematic_resample(self) -> np.ndarray:
        cumsum = np.cumsum(self.weights)
        if cumsum[-1] <= 0:
            return self.rng.choice(self.num_particles, size=self.num_particles, replace=True)
        cumsum /= cumsum[-1]
        r0 = self.rng.random() / self.num_particles
        rand = r0 + np.arange(self.num_particles) / self.num_particles
        rand = np.minimum(rand, 1.0 - SEARCHSORTED_EPS)
        return np.searchsorted(cumsum, rand, side='left')

    def _emit_alert(self, alert_type: str, message: str):
        if self.event_bus:
            try:
                self.event_bus.publish(EventTypes.SYSTEM_ALERT, {
                    "source": "ParticleFilter",
                    "type": alert_type,
                    "message": message
                })
            except Exception:
                pass

    def _record_metrics(self, name: str, value: float):
        if METRICS_AVAILABLE and MetricsCollector:
            try:
                MetricsCollector.counter(name, value)
            except Exception:
                pass

    # ── 健康检查与诊断 ────────────────────────────────────

    @classmethod
    def health_check(cls) -> Dict[str, Any]:
        if not NUMPY_AVAILABLE:
            return {"status": "error", "reason": "numpy 不可用", "warnings": ["numpy missing"]}
        try:
            pf = cls(num_particles=50, seed=42)
            pf.predict(1.0)
            pf.update(0.5)
            # 使 ESS 降至极低以测试强制重采样
            for _ in range(50):
                pf.update(0.0, obs_noise=1e-6)
            pf.force_resample()
            est = pf.get_estimates()
            state = pf.get_state()
            pf2 = cls(num_particles=50, seed=99)
            pf2.set_state(state)
            # 验证恢复一致性
            est2 = pf2.get_estimates()
            assert abs(est['mu_mean'] - est2['mu_mean']) < 1e-10
            return {"status": "ok", "reason": f"v{VERSION} 自检通过", "warnings": []}
        except Exception as e:
            return {"status": "error", "reason": str(e), "warnings": [str(e)]}
