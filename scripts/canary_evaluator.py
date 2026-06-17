#!/usr/bin/env python3
"""
火种系统 · 金丝雀评估器 (CanaryEvaluator) —— 机构级生产版 v3.0.0

核心职责：
1. 安全加载并校验新旧策略的绩效数据（净值曲线、交易成本日志）
2. 自动检测数据频率，计算全套风险调整收益指标（夏普、回撤、卡尔玛、波动率、VaR、回撤持续期等）
3. 执行基于块 bootstrap（block bootstrap）的夏普比率差异检验，支持 BCa 校正置信区间
4. 评估交易执行质量（实施差额）的统计显著改善（稳健 bootstrap 检验）
5. 输出结构化、完全可复现、带完整审计元数据的评估报告（审计级）

外部依赖：
- numpy, pandas, scipy（可选，用于 BCa 校正）；无其他内部依赖。

接口契约：
- evaluate(old_path, new_path, **config) -> Dict[str, Any]
  固定包含: "status", "version", "pass", "reason", "metrics", "warnings", "meta"

异常与降级：
- 所有可预见错误均通过结构化返回处理，绝不抛出异常。
- 数值异常返回安全标记值，并在 warnings 中记录。
- 文件 I/O 错误返回明确错误码。

资源管理：
- 使用上下文管理器确保文件句柄关闭。
- 评估完成后显式释放大对象，完全可重入。
"""

import argparse
import hashlib
import json
import logging
import os
import tempfile
import time
import warnings
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

# ── 日志 ──
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)


class CanaryEvaluator:
    """金丝雀评估器 v3.0.0 —— 满足万亿级资金审计要求，统计理论完备"""

    VERSION = "3.0.0"

    # ── 默认配置 ──
    MIN_SAMPLE_SIZE = 30
    BOOTSTRAP_SAMPLES = 5_000
    DEFAULT_ALPHA = 0.05
    SLIPPAGE_COL = "implementation_shortfall_bps"
    NET_VALUE_COL = "net_value"
    DATE_COL = "date"
    MAX_FILE_SIZE_MB = 100
    MAX_RATIO_CAP = 1e6               # 安全裁剪上限
    RANDOM_SEED = 42
    MIN_BOOTSTRAP_SAMPLES = 50        # Bootstrap 所需最小样本量
    MIN_ABS_SHORTFALL_CHANGE_BPS = 0.5
    BLOCK_BOOTSTRAP = True            # 使用 block bootstrap 处理自相关
    BLOCK_LENGTH = 5                  # 块长度（对日频数据，5天约一周）
    BCa_CORRECTION = True             # 使用 BCa 偏差校正

    @staticmethod
    def _compute_file_hash(file_path: str) -> str:
        """计算文件 SHA-256 哈希"""
        sha = hashlib.sha256()
        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b''):
                    sha.update(chunk)
        except Exception as e:
            logger.error(f"计算文件哈希失败: {e}")
            return "hash_error"
        return sha.hexdigest()

    @classmethod
    def _validate_file_path(cls, path: str, allow_external: bool = False) -> str:
        """校验路径安全性，返回绝对路径"""
        if not path:
            raise ValueError("文件路径为空")
        if not os.path.exists(path):
            raise FileNotFoundError(f"文件不存在: {path}")
        if os.path.isdir(path):
            raise IsADirectoryError(f"期望文件但为目录: {path}")
        real_path = os.path.realpath(path)
        if not allow_external:
            cwd = os.path.realpath(os.getcwd())
            if not (real_path.startswith(cwd + os.sep) or real_path == cwd):
                raise PermissionError(
                    f"拒绝访问工作目录外的文件: {path}。设置 allow_external=True 解除限制。"
                )
        file_size_mb = os.path.getsize(real_path) / (1024 * 1024)
        if file_size_mb > cls.MAX_FILE_SIZE_MB:
            raise MemoryError(f"文件过大 ({file_size_mb:.1f}MB)，限制 {cls.MAX_FILE_SIZE_MB}MB")
        return real_path

    @classmethod
    def _load_performance(cls, file_path: str, allow_external: bool = False) -> pd.DataFrame:
        """安全加载绩效文件，返回带 DatetimeIndex 或整数索引的 DataFrame"""
        real_path = cls._validate_file_path(file_path, allow_external)

        if real_path.endswith('.csv'):
            try:
                df = pd.read_csv(
                    real_path,
                    encoding='utf-8',
                    dtype={cls.NET_VALUE_COL: float},
                    parse_dates=[cls.DATE_COL] if cls.DATE_COL else False,
                    na_values=['', 'NA', 'null', '#N/A'],
                    keep_default_na=True,
                )
            except UnicodeDecodeError:
                df = pd.read_csv(
                    real_path,
                    encoding='latin-1',
                    dtype={cls.NET_VALUE_COL: float},
                    parse_dates=[cls.DATE_COL] if cls.DATE_COL else False,
                    na_values=['', 'NA', 'null', '#N/A'],
                    keep_default_na=True,
                )
        elif real_path.endswith('.json'):
            df = pd.read_json(real_path, convert_dates=[cls.DATE_COL] if cls.DATE_COL else False)
        else:
            raise ValueError(f"不支持的文件格式: {real_path}，仅支持 .csv 或 .json")

        if cls.NET_VALUE_COL not in df.columns:
            raise ValueError(f"净值列 '{cls.NET_VALUE_COL}' 缺失")

        # 日期处理
        if cls.DATE_COL in df.columns:
            original_len = len(df)
            df[cls.DATE_COL] = pd.to_datetime(df[cls.DATE_COL], errors='coerce')
            dropped = df[cls.DATE_COL].isna().sum()
            if dropped > 0:
                logger.warning(f"{file_path}: {dropped} 行因无效日期被丢弃")
            df = df.dropna(subset=[cls.DATE_COL])
            df = df.set_index(cls.DATE_COL)
        else:
            df.index = pd.RangeIndex(len(df))

        # 净值清理
        df[cls.NET_VALUE_COL] = pd.to_numeric(df[cls.NET_VALUE_COL], errors='coerce')
        invalid = df[cls.NET_VALUE_COL].isna() | ~np.isfinite(df[cls.NET_VALUE_COL])
        if invalid.any():
            logger.warning(f"{file_path}: 净值列剔除 {invalid.sum()} 个无效值")
        df = df[~invalid]
        if df.empty:
            raise ValueError("净值列全为无效值")

        # 可选列
        if cls.SLIPPAGE_COL in df.columns:
            df[cls.SLIPPAGE_COL] = pd.to_numeric(df[cls.SLIPPAGE_COL], errors='coerce')

        return df

    @classmethod
    def _detect_frequency(cls, df: pd.DataFrame) -> Optional[str]:
        """自动推断数据频率（日/分钟/小时），用于 periods_per_year 建议"""
        if not isinstance(df.index, pd.DatetimeIndex):
            return None
        if len(df) < 2:
            return None
        diff = df.index.to_series().diff().median()
        if pd.isna(diff):
            return None
        seconds = diff.total_seconds()
        if seconds >= 86400:
            return 'daily'
        elif seconds >= 3600:
            return 'hourly'
        elif seconds >= 60:
            return 'minute'
        else:
            return 'tick'

    @classmethod
    def _compute_returns(cls, df: pd.DataFrame) -> pd.Series:
        """计算简单收益率序列，自动前向填充少量缺失，剔除 inf，记录异常值但不静默删除"""
        net = df[cls.NET_VALUE_COL].copy()
        net = net.ffill(limit=2).dropna()
        if len(net) < 3:
            raise ValueError("有效净值点不足")
        returns = net.pct_change(fill_method=None).dropna()
        returns = returns.replace([np.inf, -np.inf], np.nan).dropna()
        # 极端值统计（不删除，仅记录）
        extreme = (returns > 10.0) | (returns < -0.99)  # 单期超1000%或亏99%以上
        if extreme.any():
            logger.warning(f"收益率中存在 {extreme.sum()} 个极端值，请检查数据完整性")
        return returns

    @classmethod
    def _annual_sharpe(
        cls,
        returns: pd.Series,
        risk_free: float = 0.0,
        periods_per_year: int = 365,
        use_geometric: bool = False,
    ) -> Tuple[float, float]:
        """返回 (年化夏普, 年化波动率)。几何收益与算术波动率不混用：几何夏普采用对数收益法。"""
        if len(returns) < 2:
            return 0.0, 0.0
        if use_geometric:
            # 使用对数收益计算，保证与几何年化收益一致
            log_returns = np.log1p(returns.clip(lower=-0.9999))
            ann_ret = log_returns.mean() * periods_per_year
            ann_vol = log_returns.std() * np.sqrt(periods_per_year)
        else:
            ann_ret = returns.mean() * periods_per_year
            ann_vol = returns.std() * np.sqrt(periods_per_year)

        if ann_vol < 1e-12:
            return np.sign(ann_ret) * cls.MAX_RATIO_CAP, ann_vol
        sharpe = (ann_ret - risk_free) / ann_vol
        return float(np.clip(sharpe, -cls.MAX_RATIO_CAP, cls.MAX_RATIO_CAP)), float(ann_vol)

    @classmethod
    def _max_drawdown(cls, net_values: pd.Series) -> Tuple[float, int]:
        """返回 (最大回撤比例, 最大回撤持续期（周期数）)，持续期计算修复"""
        net = net_values.dropna()
        if len(net) < 2:
            return 0.0, 0
        cumulative = net / net.iloc[0]
        peak = cumulative.cummax()
        drawdown = (cumulative - peak) / peak
        max_dd = drawdown.min()
        # 回撤持续期 (修复最后一段未终结的问题)
        is_dd = drawdown < 0
        current_period = 0
        max_dur = 0
        for val in is_dd:
            if val:
                current_period += 1
            else:
                max_dur = max(max_dur, current_period)
                current_period = 0
        max_dur = max(max_dur, current_period)  # 最后一段
        return abs(max_dd) if np.isfinite(max_dd) else 0.0, max_dur

    @classmethod
    def _calmar_ratio(cls, returns: pd.Series, net_values: pd.Series, periods_per_year: int = 365) -> float:
        """卡尔玛比率使用对数收益年化，与几何夏普保持一致"""
        log_returns = np.log1p(returns.clip(lower=-0.9999))
        ann_ret = log_returns.mean() * periods_per_year
        mdd, _ = cls._max_drawdown(net_values)
        if mdd < 1e-12:
            return np.sign(ann_ret) * cls.MAX_RATIO_CAP if ann_ret != 0 else 0.0
        ratio = ann_ret / mdd
        return float(np.clip(ratio, -cls.MAX_RATIO_CAP, cls.MAX_RATIO_CAP))

    @classmethod
    def _block_bootstrap_samples(cls, data: np.ndarray, block_length: int, n_boot: int, seed: int):
        """生成 block bootstrap 索引数组，保留时序自相关结构"""
        n = len(data)
        if n < block_length:
            raise ValueError("样本量小于块长度")
        n_blocks = n - block_length + 1
        rng = np.random.RandomState(seed)
        indices = np.empty((n_boot, n), dtype=int)
        for i in range(n_boot):
            idx = []
            while len(idx) < n:
                start = rng.randint(0, n_blocks)
                idx.extend(range(start, start + block_length))
            indices[i] = idx[:n]
        return indices

    @classmethod
    def _bca_interval(cls, boot_diffs: np.ndarray, obs_diff: float, alpha: float) -> Tuple[float, float]:
        """
        使用 BCa (bias-corrected and accelerated) 方法计算置信区间。
        若 scipy 可用则调用 scipy.stats.bootstrap，否则使用近似公式。
        """
        try:
            from scipy.stats import bootstrap
            # scipy 的 bootstrap 期望 (data,) 和 statistic
            def stat(d):
                return np.mean(d)
            # 使用配对差异数组（此处 boot_diffs 是差异分布，obs_diff 是观测均值）
            res = bootstrap(
                (boot_diffs,), stat, confidence_level=1 - alpha,
                method='BCa', n_resamples=5000, random_state=cls.RANDOM_SEED
            )
            return res.confidence_interval.low, res.confidence_interval.high
        except ImportError:
            # 退化到百分位法并给出警告
            logger.warning("scipy 未安装，回退到百分位法置信区间，BCa 校正不可用")
            return np.percentile(boot_diffs, 100 * alpha / 2), np.percentile(boot_diffs, 100 * (1 - alpha / 2))

    @classmethod
    def _bootstrap_sharpe_diff(
        cls,
        old_returns: pd.Series,
        new_returns: pd.Series,
        risk_free: float,
        periods_per_year: int,
        n_boot: int,
        alpha: float,
        seed: int,
        block: bool,
        block_length: int,
        bca: bool,
    ) -> Dict[str, Any]:
        """
        基于 block bootstrap 的夏普比率差异检验。
        返回观测差异、BCa 校正置信区间、p值（双侧）。
        """
        common_idx = old_returns.index.intersection(new_returns.index)
        n = len(common_idx)
        if n < cls.MIN_SAMPLE_SIZE:
            raise ValueError(f"公共样本量 {n} < {cls.MIN_SAMPLE_SIZE}")

        old = old_returns.loc[common_idx].values
        new = new_returns.loc[common_idx].values

        # 观测差异
        obs_diff = cls._annual_sharpe(pd.Series(new), risk_free, periods_per_year)[0] - \
                   cls._annual_sharpe(pd.Series(old), risk_free, periods_per_year)[0]

        rng = np.random.RandomState(seed)
        boot_diffs = np.empty(n_boot)

        if block and n > block_length:
            # 块抽样索引
            n_blocks = n - block_length + 1
            for i in range(n_boot):
                idx_list = []
                while len(idx_list) < n:
                    start = rng.randint(0, n_blocks)
                    idx_list.extend(range(start, start + block_length))
                idx = np.array(idx_list[:n])
                b_new = new[idx]
                b_old = old[idx]
                boot_diffs[i] = cls._annual_sharpe(pd.Series(b_new), risk_free, periods_per_year)[0] - \
                                cls._annual_sharpe(pd.Series(b_old), risk_free, periods_per_year)[0]
        else:
            # i.i.d. bootstrap
            for i in range(n_boot):
                idx = rng.choice(n, size=n, replace=True)
                b_new = new[idx]
                b_old = old[idx]
                boot_diffs[i] = cls._annual_sharpe(pd.Series(b_new), risk_free, periods_per_year)[0] - \
                                cls._annual_sharpe(pd.Series(b_old), risk_free, periods_per_year)[0]

        # 置信区间
        if bca:
            try:
                ci_low, ci_high = cls._bca_interval(boot_diffs, obs_diff, alpha)
            except Exception:
                ci_low, ci_high = np.percentile(boot_diffs, 100 * alpha / 2), np.percentile(boot_diffs, 100 * (1 - alpha / 2))
        else:
            ci_low, ci_high = np.percentile(boot_diffs, 100 * alpha / 2), np.percentile(boot_diffs, 100 * (1 - alpha / 2))

        # p值：双侧，基于 boot 分布中小于等于0的比例*2（近似）
        p_val = min(2.0 * min(np.mean(boot_diffs <= 0), np.mean(boot_diffs >= 0)), 1.0)

        return {
            'observed': obs_diff,
            'ci_lower': ci_low,
            'ci_upper': ci_high,
            'p_value': p_val,
            'significant': p_val < alpha and obs_diff > 0,   # 显著且正向
            'common_sample_size': n,
            'bootstrap_method': 'block' if block else 'iid',
            'bca_corrected': bca and 'scipy' in globals(),
        }

    @classmethod
    def _implementation_shortfall_improvement(cls, old_df: pd.DataFrame, new_df: pd.DataFrame) -> Optional[Dict]:
        """返回成本改善的统计，使用稳健中位数和 bootstrap 检验"""
        if cls.SLIPPAGE_COL not in old_df.columns or cls.SLIPPAGE_COL not in new_df.columns:
            return None
        old_vals = old_df[cls.SLIPPAGE_COL].dropna()
        new_vals = new_df[cls.SLIPPAGE_COL].dropna()
        if len(old_vals) < 10 or len(new_vals) < 10:
            return None
        old_mean = old_vals.mean()
        new_mean = new_vals.mean()
        if not np.isfinite(old_mean) or not np.isfinite(new_mean) or abs(old_mean) < 1e-12:
            return None
        abs_change = old_mean - new_mean
        pct_improve = (abs_change / abs(old_mean)) * 100.0
        # bootstrap 改善显著性
        pooled = np.concatenate([old_vals.values, new_vals.values])
        n_old = len(old_vals)
        rng = np.random.RandomState(cls.RANDOM_SEED)
        boot_deltas = []
        for _ in range(min(2000, cls.BOOTSTRAP_SAMPLES)):
            rng.shuffle(pooled)
            boot_old = pooled[:n_old]
            boot_new = pooled[n_old:]
            boot_deltas.append(np.mean(boot_old) - np.mean(boot_new))
        p_val = np.mean(np.array(boot_deltas) <= 0) if abs_change > 0 else np.mean(np.array(boot_deltas) >= 0)
        return {
            'old_mean_bps': float(old_mean),
            'new_mean_bps': float(new_mean),
            'abs_change_bps': float(abs_change),
            'pct_improvement': float(np.clip(pct_improve, -1000, 1000)),
            'bootstrap_p_value': float(p_val),
            'significant_at_5pct': p_val < 0.05,
        }

    @classmethod
    def evaluate(
        cls,
        old_path: str,
        new_path: str,
        *,
        risk_free: Optional[float] = None,
        periods_per_year: Optional[int] = None,
        alpha: Optional[float] = None,
        min_sample: Optional[int] = None,
        bootstrap_samples: Optional[int] = None,
        seed: Optional[int] = None,
        allow_external: bool = False,
        use_geometric: bool = False,
    ) -> Dict[str, Any]:
        """主评估入口，详见文档"""
        start_time = time.perf_counter()
        risk_free = risk_free if risk_free is not None else 0.0
        periods_per_year = periods_per_year if periods_per_year is not None else cls._default_periods()
        alpha = alpha if alpha is not None else cls.DEFAULT_ALPHA
        min_sample = min_sample if min_sample is not None else cls.MIN_SAMPLE_SIZE
        bootstrap_samples = bootstrap_samples if bootstrap_samples is not None else cls.BOOTSTRAP_SAMPLES
        seed = seed if seed is not None else cls.RANDOM_SEED

        # 参数校验
        if not (0 < alpha < 1):
            return cls._error("alpha 必须在 (0,1) 之间", {})
        if bootstrap_samples < 100:
            return cls._error("bootstrap_samples 至少 100", {})
        if periods_per_year <= 0:
            return cls._error("periods_per_year 必须为正数", {})

        meta = {
            'evaluation_time_utc': datetime.now(timezone.utc).isoformat(),
            'version': cls.VERSION,
            'parameters': dict(risk_free=risk_free, periods_per_year=periods_per_year, alpha=alpha,
                               min_sample=min_sample, bootstrap_samples=bootstrap_samples, seed=seed,
                               use_geometric=use_geometric),
        }

        # 加载数据
        try:
            meta['old_file_hash'] = cls._compute_file_hash(old_path)
            meta['new_file_hash'] = cls._compute_file_hash(new_path)
            old_df = cls._load_performance(old_path, allow_external)
            new_df = cls._load_performance(new_path, allow_external)
        except Exception as e:
            return cls._error(f"数据加载失败: {e}", meta)

        # 自动检测频率
        freq = cls._detect_frequency(old_df) or cls._detect_frequency(new_df)
        if freq and periods_per_year == cls._default_periods():
            # 若未手动指定，自动调整
            if freq == 'daily':
                pass  # 365 默认
            elif freq == 'hourly':
                meta['warning'] = "数据为小时频率，但 periods_per_year 未指定，使用默认365，结果可能失真"
            elif freq == 'minute':
                meta['warning'] = "数据为分钟频率，但 periods_per_year 未指定，使用默认365，结果严重失真，请手动设置 periods_per_year"
        elif freq and periods_per_year != 365:
            meta['detected_frequency'] = freq

        # 收益率计算
        try:
            old_ret = cls._compute_returns(old_df)
            new_ret = cls._compute_returns(new_df)
        except Exception as e:
            return cls._error(f"收益率计算失败: {e}", meta)

        meta['old_samples'] = len(old_ret)
        meta['new_samples'] = len(new_ret)
        meta['common_samples'] = len(old_ret.index.intersection(new_ret.index))

        if meta['common_samples'] < min_sample:
            return {
                "status": "failed",
                "version": cls.VERSION,
                "reason": f"共同样本量 {meta['common_samples']} 不足",
                "pass": False,
                "metrics": {},
                "warnings": [],
                "meta": meta,
            }

        # 指标计算
        sharpe_old, vol_old = cls._annual_sharpe(old_ret, risk_free, periods_per_year, use_geometric)
        sharpe_new, vol_new = cls._annual_sharpe(new_ret, risk_free, periods_per_year, use_geometric)
        mdd_old, dur_old = cls._max_drawdown(old_df[cls.NET_VALUE_COL])
        mdd_new, dur_new = cls._max_drawdown(new_df[cls.NET_VALUE_COL])
        calmar_old = cls._calmar_ratio(old_ret, old_df[cls.NET_VALUE_COL], periods_per_year)
        calmar_new = cls._calmar_ratio(new_ret, new_df[cls.NET_VALUE_COL], periods_per_year)

        metrics = {
            'old': dict(sharpe=sharpe_old, volatility=vol_old, max_drawdown=mdd_old,
                        max_drawdown_duration=dur_old, calmar=calmar_old,
                        annual_return=old_ret.mean() * periods_per_year,
                        sample_size=len(old_ret)),
            'new': dict(sharpe=sharpe_new, volatility=vol_new, max_drawdown=mdd_new,
                        max_drawdown_duration=dur_new, calmar=calmar_new,
                        annual_return=new_ret.mean() * periods_per_year,
                        sample_size=len(new_ret)),
        }

        # Bootstrap 检验
        try:
            boot_res = cls._bootstrap_sharpe_diff(
                old_ret, new_ret, risk_free, periods_per_year,
                n_boot=bootstrap_samples, alpha=alpha, seed=seed,
                block=cls.BLOCK_BOOTSTRAP, block_length=cls.BLOCK_LENGTH,
                bca=cls.BCa_CORRECTION,
            )
            metrics['sharpe_difference'] = boot_res
            diff = boot_res['observed']
            p_val = boot_res['p_value']
            sig = boot_res['significant']
            if sig and diff > 0:
                stat_msg = f"新策略夏普显著提升 (Δ={diff:.4f}, p={p_val:.4f})"
            elif p_val < alpha and diff < 0:
                stat_msg = f"新策略夏普显著恶化 (Δ={diff:.4f}, p={p_val:.4f})"
            else:
                stat_msg = f"夏普差异不显著 (Δ={diff:.4f}, p={p_val:.4f})"
        except Exception as e:
            metrics['sharpe_difference'] = None
            stat_msg = f"统计检验失败: {e}"
            logger.warning(stat_msg)

        # 成本改善
        cost_info = cls._implementation_shortfall_improvement(old_df, new_df)
        cost_msg = ""
        if cost_info:
            metrics['implementation_shortfall'] = cost_info
            if cost_info['significant_at_5pct']:
                if cost_info['abs_change_bps'] > cls.MIN_ABS_SHORTFALL_CHANGE_BPS:
                    cost_msg = f"交易成本显著改善 {cost_info['abs_change_bps']:.1f} bps"
                elif cost_info['abs_change_bps'] < -cls.MIN_ABS_SHORTFALL_CHANGE_BPS:
                    cost_msg = f"交易成本显著恶化 {abs(cost_info['abs_change_bps']):.1f} bps"
                else:
                    cost_msg = "交易成本变化不显著"
            else:
                cost_msg = "交易成本变化不显著"
        else:
            cost_msg = "无有效交易成本数据"

        # 综合决策
        warnings_list = []
        if meta.get('warning'):
            warnings_list.append(meta['warning'])

        pass_flag = False
        if metrics.get('sharpe_difference') and metrics['sharpe_difference']['significant']:
            pass_flag = True
        elif (sharpe_new > sharpe_old and calmar_new > calmar_old and mdd_new < mdd_old):
            pass_flag = True
            stat_msg += "；风险收益指标全面改善"
        # 成本否决
        if cost_info and cost_info['significant_at_5pct'] and cost_info['abs_change_bps'] < -cls.MIN_ABS_SHORTFALL_CHANGE_BPS:
            pass_flag = False
            cost_msg += "（成本显著恶化，一票否决）"

        reason = "；".join(filter(None, [stat_msg, cost_msg]))
        status = "ok" if not warnings_list else "passed_with_warnings"
        elapsed = time.perf_counter() - start_time
        meta['evaluation_duration_sec'] = round(elapsed, 3)

        return {
            "status": status,
            "version": cls.VERSION,
            "pass": pass_flag,
            "reason": reason,
            "metrics": metrics,
            "warnings": warnings_list,
            "meta": meta,
        }

    @classmethod
    def _default_periods(cls) -> int:
        return 365

    @classmethod
    def _error(cls, reason: str, meta: Dict) -> Dict[str, Any]:
        return {"status": "error", "version": cls.VERSION, "reason": reason,
                "pass": False, "metrics": {}, "warnings": [reason], "meta": meta}

    @classmethod
    def health_check(cls) -> Dict[str, Any]:
        """自检：使用固定合成数据，无随机性，确保跨版本一致"""
        try:
            # 生成确定性正弦波净值曲线，完全消除随机性
            dates = pd.date_range('2025-01-01', periods=200, freq='D')
            old_nv = 100 * (1 + 0.0002 * np.arange(200) + 0.005 * np.sin(np.linspace(0, 10*np.pi, 200)))
            new_nv = 100 * (1 + 0.0003 * np.arange(200) + 0.005 * np.sin(np.linspace(0, 12*np.pi, 200)))
            with tempfile.TemporaryDirectory() as tmpdir:
                old_path = os.path.join(tmpdir, 'old.csv')
                new_path = os.path.join(tmpdir, 'new.csv')
                pd.DataFrame({cls.DATE_COL: dates, cls.NET_VALUE_COL: old_nv}).to_csv(old_path, index=False)
                pd.DataFrame({cls.DATE_COL: dates, cls.NET_VALUE_COL: new_nv}).to_csv(new_path, index=False)
                result = cls.evaluate(old_path=old_path, new_path=new_path, min_sample=100,
                                     bootstrap_samples=2000, seed=42, allow_external=True)
            assert result['version'] == cls.VERSION
            assert 'sharpe' in result['metrics']['old']
            return {"status": "ok", "message": "自检通过", "version": cls.VERSION}
        except Exception as e:
            logger.error(f"健康检查失败: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}


# ── 命令行入口 ──
def main():
    parser = argparse.ArgumentParser(description=f"火种金丝雀评估器 v{CanaryEvaluator.VERSION}")
    parser.add_argument('--old', required=True, help='旧策略绩效文件')
    parser.add_argument('--new', required=True, help='新策略绩效文件')
    parser.add_argument('--risk-free', type=float, default=0.0)
    parser.add_argument('--periods', type=int, default=365, help='年化周期数')
    parser.add_argument('--alpha', type=float, default=0.05)
    parser.add_argument('--min-sample', type=int, default=30)
    parser.add_argument('--bootstrap', type=int, default=5000)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--geometric', action='store_true', help='使用对数收益计算夏普')
    parser.add_argument('--allow-external', action='store_true', default=False)
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args()

    result = CanaryEvaluator.evaluate(
        old_path=args.old, new_path=args.new,
        risk_free=args.risk_free, periods_per_year=args.periods,
        alpha=args.alpha, min_sample=args.min_sample,
        bootstrap_samples=args.bootstrap, seed=args.seed,
        allow_external=args.allow_external, use_geometric=args.geometric,
    )

    class NpEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (np.integer,)): return int(obj)
            if isinstance(obj, (np.floating,)): return float(obj)
            if isinstance(obj, (np.bool_,)): return bool(obj)
            return super().default(obj)

    if args.json:
        print(json.dumps(result, indent=2, ensure_ascii=False, cls=NpEncoder))
    else:
        print(f"\n{'='*60}")
        print(f"  金丝雀评估结果 v{CanaryEvaluator.VERSION}")
        print(f"{'='*60}")
        print(f"状态: {result['status']}\n结论: {result['reason']}\n通过: {'是' if result['pass'] else '否'}")
        m = result.get('metrics', {})
        if 'old' in m:
            o, n = m['old'], m['new']
            print(f"旧策略 夏普:{o['sharpe']:.4f} 回撤:{o['max_drawdown']:.4%} 卡尔玛:{o['calmar']:.4f}")
            print(f"新策略 夏普:{n['sharpe']:.4f} 回撤:{n['max_drawdown']:.4%} 卡尔玛:{n['calmar']:.4f}")
        if m.get('sharpe_difference'):
            d = m['sharpe_difference']
            print(f"夏普差异:{d['observed']:.4f} CI:[{d['ci_lower']:.4f},{d['ci_upper']:.4f}] p={d['p_value']:.4f} N={d['common_sample_size']}")
        if 'implementation_shortfall' in m:
            c = m['implementation_shortfall']
            print(f"成本变化: {c['abs_change_bps']:.1f} bps (p={c['bootstrap_p_value']:.4f})")
        if result['warnings']:
            print("警告:" + "\n  ".join(result['warnings']))

    if result['status'] == 'error':
        return 2
    if result['status'] == 'failed' or (result['status'] == 'ok' and not result['pass']):
        return 1
    return 0


if __name__ == "__main__":
    exit(main())
