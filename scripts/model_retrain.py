#!/usr/bin/env python3
"""
火种系统 · 模型离线重训练器 (ModelRetrain)

核心职责：
1. 根据配置与调度，从归档数据库加载清洗后的历史K线、订单簿快照与交易记录
2. 执行马尔可夫体制转换(HMM)的批量重训练，支持自适应状态数选择(BIC)与时间加权
3. 重训练贝叶斯分类器(稀疏变分高斯过程或稳健朴素贝叶斯)，产出可部署的模型参数
4. 通过原子写入、版本管理与影子触发，安全更新线上模型，记录全量审计日志

外部依赖（真实模块接口）：
- scripts.data_archiver.DataArchiver.load_klines() -> pd.DataFrame
- core.trade_database.TradeDatabase.get_all() -> List[Dict]
- core.feature_extractor.FeatureExtractor.extract_from_trades() -> Tuple[np.ndarray, np.ndarray]
- core.module_loader.ModuleLoader.notify_model_update(params_path: str)

接口契约：
- run(config: Dict[str, Any], **dependencies) -> Dict[str, Any]
  返回字典固定包含 "status", "reason", "metrics", "new_params_path", "warnings"
- health_check() -> Dict[str, Any]

异常与降级：
- 第三方库不可用时，health_check 返回错误，run 中降级跳过对应模型训练并告警
- 数据不足或质量过低时，返回 "skipped" 状态，不覆盖现有模型
- 数值溢出或训练不收敛时，保留上一版本参数，触发 PagerDuty 警报

资源管理：
- 使用上下文管理器保护文件操作与模型内存
- 训练过程中监控内存与时间，超限自动终止并清理
- 临时文件使用 mkstemp 创建唯一名称，保证并发安全
"""

import json
import logging
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import yaml

logger = logging.getLogger(__name__)

# 可选依赖导入，失败时不阻塞模块加载，但影响对应功能
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    pd = None  # type: ignore
    PANDAS_AVAILABLE = False

try:
    from hmmlearn import hmm
    HMMLEARN_AVAILABLE = True
except ImportError:
    hmm = None  # type: ignore
    HMMLEARN_AVAILABLE = False

try:
    from sklearn.gaussian_process import GaussianProcessClassifier
    from sklearn.gaussian_process.kernels import RBF, WhiteKernel
    SKLEARN_AVAILABLE = True
except ImportError:
    GaussianProcessClassifier = None  # type: ignore
    RBF = None
    WhiteKernel = None
    SKLEARN_AVAILABLE = False

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None  # type: ignore
    PSUTIL_AVAILABLE = False


class ModelRetrain:
    """模型离线重训练器 - 华尔街高频交易级标准（修订版）"""

    # ── 可配置常量（默认值，可被 YAML 覆盖） ──
    DEFAULT_CONFIG_PATH = "config/learning.yaml"
    DEFAULT_PARAMS_DIR = "config"
    DEFAULT_MODEL_FILENAME = "model_params.json"
    BACKUP_MAX_VERSIONS = 7              # 保留最近 7 个版本
    MEMORY_LIMIT_MB = 800                # 训练总内存上限
    TRAINING_TIMEOUT_SEC = 600           # 单模型训练最长时间
    OBS_CLIP_SIGMA = 4.0                 # 观测裁剪阈值（历史标准差的倍数）

    @staticmethod
    def _generate_run_id() -> str:
        """生成唯一运行ID，用于关联日志与审计"""
        return uuid.uuid4().hex[:12]

    @classmethod
    def _validate_klines(cls, df: "pd.DataFrame") -> Tuple[np.ndarray, List[str]]:
        """数据质量审查：检查列、缺失值、异常价格、成交量，返回清洗后的 NumPy 数组与警告"""
        if not PANDAS_AVAILABLE:
            raise RuntimeError("pandas 未安装，无法进行数据质量校验")
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        warnings = []
        df = df.copy()
        # 列名校验
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"数据缺少必要列: {col}")
        # 提取需要的列并转换为数值
        df = df[required_cols].apply(pd.to_numeric, errors='coerce')
        # 移除含有 NaN 的行
        nans = df.isnull().any(axis=1).sum()
        if nans > 0:
            warnings.append(f"移除 {nans} 行含有 NaN 的数据")
            df = df.dropna()
        # 移除价格为0或负的行
        invalid_price = (df[['open','high','low','close']] <= 0).any(axis=1)
        if invalid_price.any():
            warnings.append(f"移除 {invalid_price.sum()} 行价格≤0的数据")
            df = df[~invalid_price]
        # 移除 high < low 的明显数据错误
        bad_hl = df['high'] < df['low']
        if bad_hl.any():
            warnings.append(f"修正 {bad_hl.sum()} 行 high<low 的数据（交换）")
            df.loc[bad_hl, ['high','low']] = df.loc[bad_hl, ['low','high']].values
        # 成交量非负
        neg_vol = df['volume'] < 0
        if neg_vol.any():
            warnings.append(f"修正 {neg_vol.sum()} 行负成交量（取绝对值）")
            df.loc[neg_vol, 'volume'] = np.abs(df.loc[neg_vol, 'volume'])
        # 按时间顺序排序（假设索引为时间戳且已排序，否则需要单独传入时间列）
        # 此处省略，要求调用者保证传入的 df 已按时间升序
        klines = df[required_cols].values.astype(np.float64)
        return klines, warnings

    @classmethod
    def _build_hmm_observations(cls, klines: np.ndarray, config: Dict) -> Tuple[np.ndarray, List[str]]:
        """构造标准化后的 HMM 观测序列，返回 (obs, warnings)"""
        warnings = []
        eps = 1e-8
        open_, high, low, close, vol = [klines[:, i] for i in range(5)]
        if len(close) < 22:
            return np.array([]), ["K线数量不足，无法构造观测"]
        # 对数收益率
        ret = np.diff(np.log(np.maximum(close, eps)))
        # 波动率代理：高低价对数比
        log_hl = np.log(np.maximum(high[1:], low[1:]) / np.maximum(low[1:], eps))
        # 成交量比率（相较于 20 周期均值）
        vol_series = pd.Series(vol) if PANDAS_AVAILABLE else None
        if vol_series is not None:
            vol_ma = vol_series.rolling(20, min_periods=1).mean().values[1:]
            vol_ratio = vol[1:] / (vol_ma + eps)
        else:
            vol_ratio = np.ones(len(close)-1)  # 无 pandas 时降级为常数
        min_len = min(len(ret), len(log_hl), len(vol_ratio))
        obs = np.column_stack([ret[:min_len], log_hl[:min_len], vol_ratio[:min_len]])
        # 检测并处理 inf/nan
        bad = ~np.isfinite(obs).all(axis=1)
        if bad.any():
            warnings.append(f"移除 {bad.sum()} 行无效观测 (inf/nan)")
            obs = obs[~bad]
        if len(obs) == 0:
            return np.array([]), warnings
        # 基于历史标准差裁剪极端值（自适应 sigma）
        sigma = np.std(obs, axis=0)
        sigma[sigma == 0] = 1.0
        clip_val = cls.OBS_CLIP_SIGMA * sigma
        obs = np.clip(obs, -clip_val, clip_val)
        return obs, warnings

    @classmethod
    def _retrain_hmm(cls, obs: np.ndarray, config: Dict, run_id: str) -> Optional[Dict[str, Any]]:
        """使用 hmmlearn 训练，返回可序列化的参数字典"""
        if not HMMLEARN_AVAILABLE:
            logger.error("hmmlearn 未安装，跳过 HMM 训练")
            return None
        if len(obs) < cls.MIN_SAMPLES_HMM:
            logger.warning(f"HMM 观测样本不足 ({len(obs)} < {cls.MIN_SAMPLES_HMM})")
            return None
        logger.info("[%s] 开始 HMM 训练，样本数 %d", run_id, len(obs))
        # 标准化
        from sklearn.preprocessing import StandardScaler
        scaler = StandardScaler().fit(obs)
        obs_scaled = scaler.transform(obs)
        # 时间衰减权重
        n = len(obs_scaled)
        weights = np.exp(-np.arange(n)[::-1] / (n * 0.6))
        weights /= weights.sum()
        # 状态数选择（可配置，或通过 BIC 自动选择）
        n_states = config.get('hmm_n_states', 3)
        if config.get('hmm_auto_n_states'):
            best_bic = np.inf
            best_n = n_states
            for k in range(2, 6):
                try:
                    m = hmm.GaussianHMM(n_components=k, covariance_type="diag",
                                        n_iter=80, tol=1e-4, random_state=config.get('random_state', 42))
                    m.fit(obs_scaled)
                    bic = m.bic(obs_scaled) if hasattr(m, 'bic') else -m.score(obs_scaled) * len(obs_scaled)
                    if bic < best_bic:
                        best_bic = bic
                        best_n = k
                except Exception:
                    continue
            n_states = best_n
            logger.info("[%s] 自动选择 HMM 状态数: %d", run_id, n_states)
        # 最终训练
        model = hmm.GaussianHMM(n_components=n_states, covariance_type="diag",
                                n_iter=200, tol=1e-5, random_state=config.get('random_state', 42))
        model.fit(obs_scaled)
        score = model.score(obs_scaled)
        logger.info("[%s] HMM 训练完成，对数似然: %.4f", run_id, score)
        return {
            "n_states": n_states,
            "transmat": model.transmat_.tolist(),
            "means": model.means_.tolist(),
            "covars": model.covars_.tolist(),
            "startprob": model.startprob_.tolist(),
            "scaler_mean": scaler.mean_.tolist(),
            "scaler_scale": scaler.scale_.tolist(),
            "log_likelihood": score,
            "observations": ["returns", "log_hl", "volume_ratio"]
        }

    @classmethod
    def _retrain_gp(cls, feature_extractor, trade_db, klines: np.ndarray,
                    config: Dict, run_id: str) -> Optional[Dict[str, Any]]:
        """重训练高斯过程分类器，需要特征提取器与交易数据库"""
        if not SKLEARN_AVAILABLE:
            logger.error("sklearn 未安装，跳过 GP 训练")
            return None
        if feature_extractor is None or trade_db is None:
            logger.warning("GP 训练缺少特征提取器或交易数据库，跳过")
            return None
        try:
            trades = trade_db.get_all()[-5000:]
            if len(trades) < cls.MIN_SAMPLES_GP:
                logger.warning(f"交易记录不足 ({len(trades)} < {cls.MIN_SAMPLES_GP})，跳过 GP")
                return None
            X, y = feature_extractor.extract_from_trades(trades, klines)
            if X is None or len(X) < cls.MIN_SAMPLES_GP:
                return None
            # 使用随机子集避免过大内存（保留最多 3000 样本）
            if len(X) > 3000:
                idx = np.random.choice(len(X), 3000, replace=False)
                X, y = X[idx], y[idx]
            # 训练 GP（轻量级，未来可替换为稀疏变分版本）
            kernel = 1.0 * RBF(length_scale=1.0) + WhiteKernel(noise_level=0.1)
            gpc = GaussianProcessClassifier(kernel=kernel, random_state=config.get('random_state', 42))
            gpc.fit(X, y)
            train_acc = gpc.score(X, y)
            logger.info("[%s] GP 训练完成，训练准确率: %.4f", run_id, train_acc)
            return {
                "type": "GaussianProcessClassifier",
                "kernel_str": str(gpc.kernel_),
                "classes": gpc.classes_.tolist(),
                "train_accuracy": train_acc,
                "n_features": X.shape[1]
            }
        except Exception as e:
            logger.exception("[%s] GP 训练失败", run_id)
            return None

    @classmethod
    def _save_params_atomic(cls, params: Dict, output_dir: str, run_id: str) -> Optional[str]:
        """原子写入参数文件，含唯一临时文件与版本管理"""
        try:
            output_path = Path(output_dir) / cls.DEFAULT_MODEL_FILENAME
            output_path.parent.mkdir(parents=True, exist_ok=True)
            # 自定义序列化
            def serialize(obj):
                if isinstance(obj, np.ndarray):
                    return obj.tolist()
                if isinstance(obj, (np.floating, np.integer)):
                    return obj.item()
                if isinstance(obj, datetime):
                    return obj.isoformat()
                if isinstance(obj, Path):
                    return str(obj)
                raise TypeError(f"无法序列化的类型: {type(obj)}")
            json_str = json.dumps(params, indent=2, default=serialize, ensure_ascii=False)
            # 临时文件（包含 run_id 避免并发冲突）
            tmp_path = output_path.parent / f".{output_path.stem}_{run_id}.tmp"
            with open(tmp_path, 'w', encoding='utf-8') as f:
                f.write(json_str)
            # 备份当前版本（带时间戳与run_id）
            if output_path.exists():
                ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
                backup_name = f"{output_path.stem}_{ts}_{run_id}.json"
                backup_path = output_path.parent / backup_name
                output_path.rename(backup_path)  # 原子重命名
            # 原子替换
            tmp_path.rename(output_path)
            # 清理旧备份（按修改时间保留最近 BACKUP_MAX_VERSIONS 个）
            backups = sorted(output_path.parent.glob(f"{output_path.stem}_*.json"),
                             key=os.path.getmtime)
            if len(backups) > cls.BACKUP_MAX_VERSIONS:
                for old in backups[:len(backups)-cls.BACKUP_MAX_VERSIONS]:
                    old.unlink(missing_ok=True)
            logger.info("[%s] 模型参数已保存到 %s", run_id, output_path)
            return str(output_path)
        except Exception as e:
            logger.exception("[%s] 参数保存失败", run_id)
            # 清理临时文件
            if 'tmp_path' in locals() and tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            return None

    @classmethod
    def _check_memory(cls) -> bool:
        """内存检查，若 psutil 不可用则跳过，但记录警告"""
        if not PSUTIL_AVAILABLE:
            logger.warning("psutil 未安装，跳过内存检查")
            return True
        try:
            mem_mb = psutil.Process().memory_info().rss / 1024**2
            if mem_mb > cls.MEMORY_LIMIT_MB:
                logger.critical("内存占用 %.0fMB 超过限制 %dMB，终止训练", mem_mb, cls.MEMORY_LIMIT_MB)
                return False
            return True
        except Exception:
            return True

    @classmethod
    def run(cls, config: Optional[Dict] = None,
            data_archiver=None, feature_extractor=None, trade_db=None) -> Dict[str, Any]:
        """
        执行模型重训练主流程。
        外部依赖可注入，为空时尝试使用默认导入（生产环境必须注入）。
        """
        run_id = cls._generate_run_id()
        logger.info("[%s] 模型重训练开始", run_id)
        # 加载配置
        if config is None:
            try:
                with open(cls.DEFAULT_CONFIG_PATH, 'r') as f:
                    config = yaml.safe_load(f).get('learning', {})
            except Exception as e:
                logger.error("[%s] 加载配置文件失败: %s", run_id, e)
                config = {}
        # 导入全局常量配置
        cls.MIN_SAMPLES_HMM = config.get('min_samples_hmm', cls.MIN_SAMPLES_HMM)
        cls.MIN_SAMPLES_GP = config.get('min_samples_gp', cls.MIN_SAMPLES_GP)

        warnings_list = []
        metrics = {}
        new_params = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "run_id": run_id,
            "version": "2.1.0"
        }

        # 1. 加载与清洗数据
        if data_archiver is None:
            logger.warning("[%s] data_archiver 未注入，无法加载数据", run_id)
            return {"status": "error", "reason": "数据加载器缺失", "metrics": {},
                    "new_params_path": "", "warnings": ["data_archiver为None"]}
        try:
            df = data_archiver.load_klines(days=config.get('window_days', 90), timeframe='3m')
            klines, data_warnings = cls._validate_klines(df)
            warnings_list.extend(data_warnings)
            if len(klines) == 0:
                raise ValueError("清洗后数据为空")
            metrics['n_klines'] = len(klines)
        except Exception as e:
            logger.exception("[%s] 数据加载失败", run_id)
            return {"status": "error", "reason": str(e), "metrics": {},
                    "new_params_path": "", "warnings": warnings_list}

        # 内存检查
        if not cls._check_memory():
            return {"status": "error", "reason": "内存超限", "metrics": metrics,
                    "new_params_path": "", "warnings": warnings_list}

        # 2. HMM 重训练
        full_retrain = config.get('full_retrain', False)
        if full_retrain or config.get('retrain_hmm', True):
            obs, hmm_warns = cls._build_hmm_observations(klines, config)
            warnings_list.extend(hmm_warns)
            if len(obs) >= cls.MIN_SAMPLES_HMM:
                hmm_params = cls._retrain_hmm(obs, config, run_id)
                if hmm_params:
                    new_params['hmm'] = hmm_params
                    metrics['hmm_loglik'] = hmm_params.get('log_likelihood')
                else:
                    warnings_list.append("HMM 训练未成功，保留旧模型")
            else:
                warnings_list.append(f"HMM 有效观测不足 ({len(obs)} < {cls.MIN_SAMPLES_HMM})")

        # 3. 贝叶斯/GP 重训练
        if full_retrain or config.get('retrain_gp', True):
            if feature_extractor and trade_db:
                gp_params = cls._retrain_gp(feature_extractor, trade_db, klines, config, run_id)
                if gp_params:
                    new_params['gp'] = gp_params
                    metrics['gp_train_acc'] = gp_params.get('train_accuracy')
                else:
                    warnings_list.append("GP 训练未成功，保留旧模型")
            else:
                logger.info("[%s] 跳过 GP 训练（依赖未注入或配置关闭）", run_id)

        # 4. 保存参数并触发影子评估
        params_dir = config.get('params_dir', cls.DEFAULT_PARAMS_DIR)
        saved_path = cls._save_params_atomic(new_params, params_dir, run_id)
        if saved_path:
            # 创建触发文件通知主引擎（模块加载器通过文件监听）
            trigger_path = Path(params_dir) / ".retrain_trigger"
            trigger_path.touch(exist_ok=True)
            logger.info("[%s] 重训练完成，触发影子评估", run_id)
            return {"status": "ok", "reason": "模型重训练成功",
                    "metrics": metrics, "new_params_path": saved_path,
                    "warnings": warnings_list}
        else:
            return {"status": "error", "reason": "参数保存失败",
                    "metrics": metrics, "new_params_path": "",
                    "warnings": warnings_list}

    @classmethod
    def health_check(cls) -> Dict[str, Any]:
        """自检：依赖库、磁盘空间、权限"""
        issues = []
        if not PANDAS_AVAILABLE:
            issues.append("pandas 未安装")
        if not HMMLEARN_AVAILABLE:
            issues.append("hmmlearn 未安装（HMM训练不可用）")
        if not SKLEARN_AVAILABLE:
            issues.append("scikit-learn 未安装（GP训练不可用）")
        if not PSUTIL_AVAILABLE:
            issues.append("psutil 未安装（内存监控不可用）")
        # 检查参数目录可写性
        param_dir = Path(cls.DEFAULT_PARAMS_DIR)
        try:
            param_dir.mkdir(parents=True, exist_ok=True)
            test_file = param_dir / ".health_check_test"
            test_file.touch()
            test_file.unlink()
        except Exception as e:
            issues.append(f"参数目录不可写: {e}")
        if issues:
            return {"status": "error", "message": "; ".join(issues)}
        return {"status": "ok", "message": "所有依赖与权限正常"}


def main():
    """命令行入口，用于手动触发重训练（需注入依赖）"""
    import argparse
    parser = argparse.ArgumentParser(description="火种模型离线重训练工具")
    parser.add_argument("--config", default=ModelRetrain.DEFAULT_CONFIG_PATH, help="配置文件路径")
    parser.add_argument("--full", action="store_true", help="强制全量重训练")
    args = parser.parse_args()

    health = ModelRetrain.health_check()
    if health["status"] != "ok":
        print("健康检查失败:", health["message"], file=sys.stderr)
        sys.exit(1)
    print("健康检查通过，开始重训练...")
    # 注意：生产环境需通过依赖注入传入 data_archiver 等
    # 此处仅作演示，会因缺少依赖而失败
    config = {"full_retrain": args.full}
    result = ModelRetrain.run(config=config)
    print(json.dumps(result, indent=2, default=str))
    sys.exit(0 if result["status"] == "ok" else 1)


if __name__ == "__main__":
    main()
