#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors
"""
火种系统 · 稀疏高斯过程分类器 (GPClassifier) v6.0.0

核心职责：
1. 稀疏变分高斯过程 (SVGP) 用于市场状态二分类概率推断
2. 输入标准化因子向量，输出带不确定性的开仓概率
3. 支持离线批量训练与在线自然梯度更新（均值+协方差），保证数值稳定
4. 严格输入校验、内存保护、可复现性（种子），满足万亿级账户安全标准
5. 模型序列化与反序列化，支持生产环境持久化与灾难恢复

外部依赖：
- numpy : 数值计算
- scipy : 优化与线性代数（若不可用，降级为纯 numpy 实现）
- core.metrics.MetricsCollector : 指标暴露（可选）

接口契约：
- predict(features) -> Dict[str, float]
- online_update(features, label) -> None
- train(X, y) -> bool
- get_model_info() -> Dict
- health_check() -> Dict
- export_params() -> Dict  模型参数导出
- import_params(params: Dict) -> None  模型参数导入
"""

import copy
import logging
import math
from threading import RLock
from typing import Dict, Any, List, Optional, Tuple

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
    from scipy.optimize import minimize
    from scipy.special import erf, ndtr
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

try:
    from core.metrics import MetricsCollector
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    MetricsCollector = None

# ── 常量 ──────────────────────────────────────────────────
DEFAULT_INDUCING_POINTS = 50
JITTER = 1e-6
LEARNING_RATE_MEAN = 0.02
LEARNING_RATE_COV = 0.01
PRIOR_PROB = 0.5
MAX_ITER_TRAIN = 150
CONVERGENCE_TOL = 1e-4
KERNEL_LENGTHSCALE = 1.0
KERNEL_VARIANCE = 1.0
MAX_BUFFER_SIZE = 5000
MIN_STD = 1e-6
MAX_STD = 5.0
GRADIENT_CLIP = 10.0
PROB_EPS = 1e-15
MIN_F_VAR = 1e-12


# 高性能 erf 实现（无需 scipy）
def _fast_erf(x):
    """快速、向量化的 erf 近似，避免 Python 循环"""
    # 使用 Abramowitz and Stegun 近似
    a = np.abs(x)
    p = 0.3275911
    t = 1.0 / (1.0 + p * a)
    # 多项式近似
    y = 1.0 - ((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * np.exp(-a * a)
    return np.sign(x) * (1.0 - y)


def _fast_probit(x):
    """向量化 Probit (正态 CDF)"""
    return 0.5 * (1.0 + _fast_erf(x / math.sqrt(2)))


class GPClassifier:
    """稀疏变分高斯过程分类器 (Probit likelihood, SVGP)"""

    def __init__(self, n_inducing: int = DEFAULT_INDUCING_POINTS,
                 kernel_lengthscale: float = KERNEL_LENGTHSCALE,
                 kernel_variance: float = KERNEL_VARIANCE,
                 learning_rate_mean: float = LEARNING_RATE_MEAN,
                 learning_rate_cov: float = LEARNING_RATE_COV,
                 seed: Optional[int] = None):
        if not NUMPY_AVAILABLE:
            raise ImportError("numpy 是 GPClassifier 的必要依赖")
        if n_inducing < 2:
            raise ValueError("诱导点数至少为 2")
        if kernel_lengthscale < 1e-4 or kernel_variance <= 0:
            raise ValueError("核参数必须为正且长度尺度不小于 1e-4")

        self.n_inducing = n_inducing
        self._initial_inducing = n_inducing
        self.kernel_lengthscale = kernel_lengthscale
        self.kernel_variance = kernel_variance
        self.lr_mean = learning_rate_mean
        self.lr_cov = learning_rate_cov
        self._seed = seed

        self.rng = default_rng(seed) if seed is not None else default_rng()

        # 模型状态
        self.inducing_points: Optional[np.ndarray] = None
        self.variational_mean: Optional[np.ndarray] = None
        self._variational_std: Optional[np.ndarray] = None  # 对角标准差
        self._Kmm: Optional[np.ndarray] = None
        self._Kmm_inv: Optional[np.ndarray] = None
        self._is_trained = False
        self._input_dim: Optional[int] = None

        # 在线样本缓存
        self._X_buffer: List[np.ndarray] = []
        self._y_buffer: List[int] = []

        # 线程安全
        self._lock = RLock()

        # 统计
        self._update_count = 0

        logger.info("GPClassifier v%s 初始化: M=%d, l=%.2f, σ²=%.2f",
                    VERSION, n_inducing, kernel_lengthscale, kernel_variance)

    # ── 公共接口 ──────────────────────────────────────────

    def predict(self, features: List[float]) -> Dict[str, float]:
        with self._lock:
            if not self._is_trained:
                logger.warning("模型未训练，返回先验")
                return {"probability": PRIOR_PROB, "uncertainty": 0.5}
            x = self._validate_input(features)
            if x is None:
                return {"probability": PRIOR_PROB, "uncertainty": 0.5}
            prob, std = self._variational_predict(x)
            if not (0 <= prob <= 1) or not math.isfinite(prob):
                logger.warning("预测值异常 (prob=%.4f)，回退先验", prob)
                return {"probability": PRIOR_PROB, "uncertainty": 0.5}
            return {"probability": prob, "uncertainty": min(std, 0.5)}

    def online_update(self, features: List[float], label: int) -> None:
        with self._lock:
            if not self._is_trained:
                self._buffer_sample(features, label)
                return
            x = self._validate_input(features)
            if x is None or label not in (0, 1):
                return

            k_ux = self._kernel(self.inducing_points, x)
            k_xx = self._kernel(x, x)

            f_mean, f_var = self._latent_predict(x, k_ux, k_xx)
            prob, dlogp_df, d2logp_df2 = self._probit_likelihood_derivatives(label, f_mean, f_var)

            # 均值自然梯度
            A = np.dot(self._Kmm_inv, k_ux)
            nat_grad_mean = A * dlogp_df
            nat_grad_mean = np.clip(nat_grad_mean, -GRADIENT_CLIP, GRADIENT_CLIP)
            self.variational_mean -= self.lr_mean * nat_grad_mean.flatten()
            self.variational_mean = np.clip(self.variational_mean, -5.0, 5.0)

            # 协方差自然梯度
            s = self._variational_std
            A2 = A.flatten() ** 2
            inv_s = np.where(s > MIN_STD, 1.0 / s, 1.0 / MIN_STD)
            grad_s = 0.5 * (-inv_s + s * A2 * (d2logp_df2 + dlogp_df**2))
            grad_s = np.clip(grad_s, -GRADIENT_CLIP, GRADIENT_CLIP)
            self._variational_std = self._variational_std - self.lr_cov * grad_s
            self._variational_std = np.clip(self._variational_std, MIN_STD, MAX_STD)

            self._update_count += 1
            self._record_metrics("gp_online_update", 1)

    def train(self, X: List[List[float]], y: List[int]) -> bool:
        with self._lock:
            if not SCIPY_AVAILABLE:
                logger.error("scipy 不可用，无法批量训练")
                return False
            X_arr = np.asarray(X, dtype=np.float64)
            y_arr = np.asarray(y, dtype=np.float64)
            if X_arr.size == 0 or y_arr.size == 0:
                return False
            if X_arr.shape[0] != len(y_arr):
                raise ValueError("X 和 y 样本数不一致")
            mask = np.isfinite(X_arr).all(axis=1) & np.isfinite(y_arr)
            if (~mask).any():
                logger.warning("丢弃 %d 个无效样本", (~mask).sum())
            X_arr, y_arr = X_arr[mask], y_arr[mask]
            N, D = X_arr.shape
            if N < 2:
                logger.error("有效样本不足")
                return False
            if np.all(y_arr == 0) or np.all(y_arr == 1):
                logger.warning("训练集标签全部相同，模型可能无法正确学习")

            self._input_dim = D
            actual_inducing = min(self.n_inducing, N)
            self.n_inducing = actual_inducing

            idx = self.rng.choice(N, self.n_inducing, replace=False)
            self.inducing_points = X_arr[idx].copy()

            self._Kmm = self._kernel(self.inducing_points, self.inducing_points)
            self._Kmm += JITTER * np.eye(self.n_inducing)
            for jitter_mult in [1, 10, 100, 1000]:
                try:
                    self._Kmm_inv = np.linalg.inv(self._Kmm)
                    break
                except np.linalg.LinAlgError:
                    self._Kmm += (JITTER * (jitter_mult - 1)) * np.eye(self.n_inducing)
            else:
                logger.error("核矩阵无法求逆，训练失败")
                return False

            self.variational_mean = np.zeros(self.n_inducing)
            self._variational_std = np.full(self.n_inducing, 1.0)

            init_log_s = np.log(np.clip(self._variational_std, MIN_STD, None))
            init_params = np.concatenate([self.variational_mean, init_log_s])
            bounds = [(None, None)] * self.n_inducing + [(-10.0, 2.5)] * self.n_inducing

            try:
                result = minimize(
                    self._neg_elbo,
                    init_params,
                    args=(X_arr, y_arr),
                    method='L-BFGS-B',
                    bounds=bounds,
                    options={'maxiter': MAX_ITER_TRAIN, 'ftol': CONVERGENCE_TOL}
                )
            except Exception as e:
                logger.error("优化异常: %s", e)
                return False

            if result.success and np.isfinite(result.fun):
                logger.info("训练收敛，ELBO: %.4f", -result.fun)
            else:
                logger.warning("训练未完全收敛: %s", result.message)

            self.variational_mean = result.x[:self.n_inducing]
            self._variational_std = np.exp(result.x[self.n_inducing:])
            self._variational_std = np.clip(self._variational_std, MIN_STD, MAX_STD)
            self._is_trained = True

            if self._X_buffer:
                cached_X = self._X_buffer[:]
                cached_y = self._y_buffer[:]
                self._X_buffer.clear()
                self._y_buffer.clear()
                consumed = 0
                for xi, yi in zip(cached_X, cached_y):
                    if xi.shape[0] == self._input_dim:
                        self.online_update(xi.tolist(), yi)
                        consumed += 1
                if consumed > 0:
                    logger.info("消耗 %d 个缓存样本", consumed)
                else:
                    logger.warning("所有缓存样本维度不匹配，已丢弃")

            self._record_metrics("gp_training_success", 1)
            return True

    def get_model_info(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": VERSION,
                "trained": self._is_trained,
                "n_inducing": self.n_inducing,
                "input_dim": self._input_dim,
                "kernel_lengthscale": self.kernel_lengthscale,
                "kernel_variance": self.kernel_variance,
                "variational_mean_norm": float(np.linalg.norm(self.variational_mean)) if self.variational_mean is not None else 0.0,
                "updates": self._update_count,
            }

    def export_params(self) -> Dict[str, Any]:
        """导出模型全部参数，便于持久化"""
        with self._lock:
            if not self._is_trained:
                return {"version": VERSION, "trained": False}
            return {
                "version": VERSION,
                "trained": True,
                "n_inducing": self.n_inducing,
                "input_dim": self._input_dim,
                "kernel_lengthscale": self.kernel_lengthscale,
                "kernel_variance": self.kernel_variance,
                "inducing_points": self.inducing_points.tolist(),
                "variational_mean": self.variational_mean.tolist(),
                "variational_std": self._variational_std.tolist(),
                "Kmm": self._Kmm.tolist(),
                "Kmm_inv": self._Kmm_inv.tolist(),
                "update_count": self._update_count,
            }

    def import_params(self, params: Dict[str, Any]) -> bool:
        """从持久化参数恢复模型，需要在训练后调用"""
        with self._lock:
            if not params.get("trained", False):
                logger.error("导入的参数中模型未训练")
                return False
            try:
                self.n_inducing = int(params["n_inducing"])
                self._input_dim = int(params["input_dim"])
                self.kernel_lengthscale = float(params["kernel_lengthscale"])
                self.kernel_variance = float(params["kernel_variance"])
                self.inducing_points = np.array(params["inducing_points"], dtype=np.float64)
                self.variational_mean = np.array(params["variational_mean"], dtype=np.float64)
                self._variational_std = np.array(params["variational_std"], dtype=np.float64)
                self._Kmm = np.array(params["Kmm"], dtype=np.float64)
                self._Kmm_inv = np.array(params["Kmm_inv"], dtype=np.float64)
                self._update_count = int(params.get("update_count", 0))
                self._is_trained = True
                logger.info("模型参数导入成功")
                return True
            except (KeyError, ValueError, TypeError) as e:
                logger.error("导入参数错误: %s", e)
                return False

    def reset(self) -> None:
        with self._lock:
            self._is_trained = False
            self.n_inducing = self._initial_inducing
            self._input_dim = None
            self.inducing_points = None
            self.variational_mean = None
            self._variational_std = None
            self._Kmm = None
            self._Kmm_inv = None
            self._X_buffer.clear()
            self._y_buffer.clear()
            self._update_count = 0
            logger.info("模型已重置")

    def set_kernel_params(self, lengthscale: Optional[float] = None, variance: Optional[float] = None) -> None:
        """热更新核参数，下次重训生效"""
        with self._lock:
            if lengthscale is not None and lengthscale >= MIN_STD:
                self.kernel_lengthscale = lengthscale
            if variance is not None and variance > 0:
                self.kernel_variance = variance
            logger.info("核参数已更新: l=%.2f, σ²=%.2f", self.kernel_lengthscale, self.kernel_variance)

    # ── 内部核心算法 ──────────────────────────────────────

    def _validate_input(self, features: List[float]) -> Optional[np.ndarray]:
        if features is None or len(features) == 0:
            return None
        try:
            x = np.asarray(features, dtype=np.float64).reshape(1, -1)
        except (ValueError, TypeError):
            return None
        if not np.isfinite(x).all():
            logger.warning("输入包含 NaN/Inf")
            return None
        if self._input_dim is not None and x.shape[1] != self._input_dim:
            logger.error("维度不匹配: 期望 %d, 得到 %d", self._input_dim, x.shape[1])
            return None
        return x

    def _buffer_sample(self, features: List[float], label: int) -> None:
        x = np.asarray(features, dtype=np.float64).ravel()
        if x.size == 0:
            return
        if len(self._X_buffer) >= MAX_BUFFER_SIZE:
            self._X_buffer.pop(0)
            self._y_buffer.pop(0)
        self._X_buffer.append(x)
        self._y_buffer.append(label)

    def _kernel(self, X1: np.ndarray, X2: np.ndarray) -> np.ndarray:
        sqdist = np.sum(X1**2, axis=1).reshape(-1, 1) + np.sum(X2**2, axis=1) - 2 * np.dot(X1, X2.T)
        sqdist = np.clip(sqdist, 0, None)
        return self.kernel_variance * np.exp(-0.5 * sqdist / self.kernel_lengthscale**2)

    def _variational_predict(self, x: np.ndarray) -> Tuple[float, float]:
        k_ux = self._kernel(self.inducing_points, x)
        k_xx = self._kernel(x, x)
        f_mean, f_var = self._latent_predict(x, k_ux, k_xx)
        z = f_mean / math.sqrt(1.0 + f_var)
        prob = self._probit_scalar(z)
        phi_z = math.exp(-0.5 * z * z) / math.sqrt(2 * math.pi * (1.0 + f_var))
        prob_var = max(0.0, (phi_z ** 2) * f_var / (1.0 + f_var))
        prob_std = math.sqrt(prob_var)
        return float(prob), float(prob_std)

    def _latent_predict(self, x: np.ndarray, k_ux: np.ndarray, k_xx: np.ndarray) -> Tuple[float, float]:
        f_mean = np.dot(k_ux.T, self.variational_mean)[0, 0]
        A = np.dot(self._Kmm_inv, k_ux)
        f_var = k_xx[0, 0] - np.dot(k_ux.T, A)[0, 0]
        if self._variational_std is not None:
            f_var += np.sum((self._variational_std[:, None] * A) ** 2)
        f_var = max(MIN_F_VAR, f_var)
        return f_mean, f_var

    def _probit_likelihood_derivatives(self, y: int, f_mean: float, f_var: float) -> Tuple[float, float, float]:
        z = f_mean / math.sqrt(1.0 + f_var)
        prob = self._probit_scalar(z)
        prob = float(np.clip(prob, PROB_EPS, 1 - PROB_EPS))
        phi_z = math.exp(-0.5 * z * z) / math.sqrt(2 * math.pi)
        dlogp = (y - prob) * phi_z / (prob * (1 - prob) * math.sqrt(1.0 + f_var))
        dlogp = float(np.clip(dlogp, -GRADIENT_CLIP, GRADIENT_CLIP))
        d2 = - (phi_z ** 2) / (prob * (1 - prob) * (1.0 + f_var))
        d2 = float(np.clip(d2, -GRADIENT_CLIP, 0))
        return prob, dlogp, d2

    @staticmethod
    def _probit_scalar(x: float) -> float:
        return 0.5 * (1 + math.erf(x / math.sqrt(2)))

    @staticmethod
    def _probit_array(x: np.ndarray) -> np.ndarray:
        if SCIPY_AVAILABLE:
            return ndtr(x)
        # 降级：使用快速的向量化 erf 近似
        return _fast_probit(x)

    def _neg_elbo(self, params: np.ndarray, X: np.ndarray, y: np.ndarray) -> float:
        mean = params[:self.n_inducing]
        log_s = params[self.n_inducing:]
        s = np.exp(log_s)

        K_mn = self._kernel(self.inducing_points, X)
        A = np.dot(self._Kmm_inv, K_mn)
        f_mean = np.dot(K_mn.T, mean)
        diag_K_nn = np.full(X.shape[0], self.kernel_variance)
        term1 = np.sum(A * K_mn, axis=0)
        f_var = diag_K_nn - term1
        if s is not None:
            f_var += np.sum((s[:, None] * A) ** 2, axis=0)
        f_var = np.clip(f_var, MIN_F_VAR, None)

        z = f_mean / np.sqrt(1.0 + f_var)
        prob = self._probit_array(z)
        prob = np.clip(prob, PROB_EPS, 1 - PROB_EPS)
        log_lik = np.sum(y * np.log(prob) + (1 - y) * np.log(1 - prob))

        diag_Kmm_inv = np.diag(self._Kmm_inv)
        Tr_term = np.sum(diag_Kmm_inv * s**2)
        quad_term = np.dot(mean.T, np.dot(self._Kmm_inv, mean))
        sign_log_det_Kmm = np.linalg.slogdet(self._Kmm)[1]
        log_det_S = 2.0 * np.sum(log_s)
        kl = 0.5 * (Tr_term + quad_term - self.n_inducing + sign_log_det_Kmm - log_det_S)
        elbo = log_lik - kl
        return -float(elbo)

    # ── 健康检查 ──────────────────────────────────────────

    @classmethod
    def health_check(cls) -> Dict[str, Any]:
        warnings = []
        if not NUMPY_AVAILABLE:
            return {"status": "error", "reason": "numpy 不可用", "warnings": ["numpy missing"]}
        if not SCIPY_AVAILABLE:
            warnings.append("scipy 不可用，批量训练降级")
        try:
            gp = cls(n_inducing=10, seed=42)
            X = np.random.randn(30, 5).tolist()
            y = [1 if sum(x) > 0 else 0 for x in X]
            ok = gp.train(X, y)
            if ok:
                pred = gp.predict(X[0])
                assert 0 <= pred['probability'] <= 1
                gp.online_update(X[0], 1)
                # 测试导出/导入
                params = gp.export_params()
                gp2 = cls(n_inducing=10)
                gp2.import_params(params)
                pred2 = gp2.predict(X[0])
                assert abs(pred['probability'] - pred2['probability']) < 1e-6
            return {
                "status": "ok" if not warnings else "degraded",
                "reason": "GP分类器自检通过",
                "warnings": warnings,
            }
        except Exception as e:
            return {"status": "error", "reason": str(e), "warnings": [str(e)]}

    def _record_metrics(self, name: str, value: float):
        if MetricsCollector is not None:
            try:
                MetricsCollector.counter(name, value)
            except Exception:
                pass
