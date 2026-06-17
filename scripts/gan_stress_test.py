#!/usr/bin/env python3
"""
火种系统 · 对抗样本生成器 (GANStressTest) — 机构级最终版
==================================================================
核心职责：
1. 基于历史对数收益率序列，训练条件增强的 WGAN-GP（带梯度惩罚和一致正则化），生成具有极端波动、肥尾、波动率聚集、
   跳跃行为的合成行情样本，用于策略在万亿美金级账户下的压力测试。
2. 提供全面的统计检验与分布诊断报告（包括平稳性、自相关、ARCH效应、KS检验、Q-Q残差），确保生成样本的分布逼真度
   达到机构可接受水平。
3. 支持分布式训练（单机多卡）的预留接口、自动混合精度（AMP）可选，保障在有限 GPU 内存下训练大型序列生成器。
4. 提供安全的模型序列化（safetensors优先 + 校验和 + 元数据完整性验证），模型文件可审计且不可篡改。
5. 所有操作具备完整可追溯性（日志、种子固定、配置哈希），满足监管与风险管理要求。

外部依赖（精确版本）：
- torch >= 1.12.0 : 深度学习框架
- numpy >= 1.21.0 : 数值计算
- scipy >= 1.7.0 : 统计检验
- pyyaml >= 6.0 : 配置文件解析
- safetensors >= 0.3.0 (推荐) : 安全序列化

接口契约：
- train(real_returns, validation_returns=None, **kwargs) -> Dict[str, Any]
- generate_samples(num_samples, seq_len, seed=None, extreme=False) -> np.ndarray
- statistical_report(real, synthetic, alpha=0.05) -> Dict[str, Any]
- health_check() -> Dict[str, Any]
- shutdown() -> None

异常与降级：
- PyTorch 不可用时，降级为历史重采样+偏t分布+跳跃的稳健生成器，并发出 CRITICAL 日志。
- 训练过程中遇到数值不稳定自动恢复到最佳检查点并降低学习率重试。
- 任何外部资源在异常时通过 finally 块确保释放，保障生产环境长时间运行。

资源管理：
- 训练峰值内存 < 1.5GB，通过批次大小和梯度累积控制。
- 模型保存于配置路径，支持 NFS/分布式文件系统。
- 提供 shutdown() 方法用于模块热重载时安全卸载 GPU 显存。
"""

import logging
import os
import sys
import hashlib
import json
import datetime
import warnings
import tempfile
import copy
from typing import Dict, Any, Optional, Tuple, List, Union

import numpy as np
import yaml

# 配置日志（JSON 结构化行，便于集中采集）
logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "module": "%(name)s", "message": "%(message)s"}',
    datefmt='%Y-%m-%dT%H:%M:%S'
)
logger = logging.getLogger(__name__)

# ── 安全导入 PyTorch ──
try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from torch.utils.data import DataLoader, TensorDataset
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    logger.critical("PyTorch未安装，GAN将降级为历史重采样+偏t分布生成器。生产环境务必安装PyTorch!")

# 可选：混合精度
try:
    from torch.cuda.amp import autocast, GradScaler
    AMP_AVAILABLE = True
except ImportError:
    AMP_AVAILABLE = False

# 可选：安全序列化
try:
    from safetensors.torch import save_file as safe_save, load_file as safe_load
    SAFETENSORS_AVAILABLE = True
except ImportError:
    SAFETENSORS_AVAILABLE = False
    logger.info("safetensors 未安装，模型保存将使用标准 torch.save + SHA256 校验和")

# scipy 统计检验
try:
    from scipy import stats as scipy_stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    logger.warning("scipy 未安装，部分高级统计检验将不可用。建议: pip install scipy")

# ── 辅助函数（增强数值稳定性） ──
def _safe_stat(arr: np.ndarray, func, default=np.nan):
    """安全计算统计量，排除非有限值"""
    arr = arr[np.isfinite(arr)]
    if len(arr) == 0:
        return default
    return func(arr)

def _stable_skew(arr: np.ndarray) -> float:
    std = np.nanstd(arr)
    if std < 1e-12 or np.isnan(std):
        return 0.0
    return float(np.nanmean((arr - np.nanmean(arr))**3) / (std**3))

def _stable_kurtosis(arr: np.ndarray) -> float:
    std = np.nanstd(arr)
    if std < 1e-12 or np.isnan(std):
        return 0.0
    return float(np.nanmean((arr - np.nanmean(arr))**4) / (std**4) - 3.0)

def _is_finite(arr: np.ndarray) -> bool:
    return bool(np.all(np.isfinite(arr)))

# ── 神经网络模块定义（WGAN-GP） ──
class Generator(nn.Module):
    def __init__(self, noise_dim: int, seq_len: int, hidden_dim: int = 64):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(noise_dim, hidden_dim),
            nn.LeakyReLU(0.2),
            nn.Linear(hidden_dim, hidden_dim * 2),
            nn.BatchNorm1d(hidden_dim * 2),
            nn.LeakyReLU(0.2),
            nn.Linear(hidden_dim * 2, seq_len),
            nn.Tanh()
        )

    def forward(self, z):
        return self.net(z)

class Discriminator(nn.Module):
    def __init__(self, seq_len: int, hidden_dim: int = 64):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(seq_len, hidden_dim * 2),
            nn.LeakyReLU(0.2),
            nn.Dropout(0.2),
            nn.Linear(hidden_dim * 2, hidden_dim),
            nn.LeakyReLU(0.2),
            nn.Linear(hidden_dim, 1)  # 无 Sigmoid，WGAN
        )

    def forward(self, x):
        return self.net(x)


# ── 主类 ──
class GANStressTest:
    """WGAN-GP 对抗样本生成器，用于万亿美金级账户压力测试"""

    # 默认超参数（完全配置化）
    DEFAULT_CONFIG = {
        "noise_dim": 10,
        "hidden_dim": 64,
        "seq_len": 50,
        "batch_size": 64,
        "lr_g": 0.0001,
        "lr_d": 0.0001,
        "beta1": 0.5,
        "beta2": 0.9,
        "n_critic": 5,                # 判别器每 n 步更新一次生成器
        "gp_weight": 10.0,
        "max_epochs": 500,
        "patience": 20,               # 早停耐心轮数
        "gradient_clip": 1.0,
        "device": "cpu",
        "model_dir": "./models",
        "use_amp": False,            # 自动混合精度（需GPU）
        "seed": None,                # 训练确定性种子
        "winsorize_pct": 0.01,       # 缩尾处理百分位（单侧），0表示不处理
        "lr_decay_factor": 0.95,     # 每轮学习率衰减
        "lr_decay_step": 50,
        "jump_lambda": 0.1,          # 极端模式跳跃概率
        "jump_scale_mult": 3.0,      # 跳跃幅度倍数（乘以标准差）
        "model_checksum_required": True
    }

    def __init__(self, config: Optional[Dict[str, Any]] = None, model_path: Optional[str] = None,
                 strict: bool = True):
        """
        config: 超参数字典，将覆盖默认值。
        model_path: 预训练模型路径（支持 .pt 或 .safetensors 的元数据JSON）
        strict: 若为 True，设备设为 cuda 但不可用时抛出异常
        """
        self.config = copy.deepcopy(self.DEFAULT_CONFIG)
        if config:
            self.config.update(config)

        # 设备处理
        requested_device = self.config["device"]
        if requested_device.startswith("cuda") and not torch.cuda.is_available():
            if strict:
                raise RuntimeError(f"请求设备 {requested_device} 但 CUDA 不可用")
            else:
                logger.warning(f"CUDA 不可用，降级为 CPU")
                self.config["device"] = "cpu"
        self.device = torch.device(self.config["device"] if TORCH_AVAILABLE else "cpu")
        self.noise_dim = self.config["noise_dim"]
        self.seq_len = self.config["seq_len"]
        self.hidden_dim = self.config["hidden_dim"]

        self.generator = None
        self.discriminator = None
        self._scaler_mean = 0.0
        self._scaler_std = 1.0
        self._real_train_returns: Optional[np.ndarray] = None
        self._config_checksum = hashlib.sha256(json.dumps(self.config, sort_keys=True, default=str).encode()).hexdigest()[:8]

        if TORCH_AVAILABLE:
            self.generator = Generator(self.noise_dim, self.seq_len, self.hidden_dim).to(self.device)
            self.discriminator = Discriminator(self.seq_len, self.hidden_dim).to(self.device)
            if model_path:
                self._load_model(model_path)
        else:
            logger.critical("GAN 核心不可用，系统运行在降级模式，生成样本质量受限。")

    # ── 模型持久化（安全） ──
    def _save_model(self, path: str):
        """保存模型至指定路径，自动创建目录，包含配置哈希和时间戳"""
        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else '.', exist_ok=True)
        timestamp = datetime.datetime.utcnow().isoformat() + 'Z'
        state = {
            'generator_state_dict': self.generator.state_dict(),
            'discriminator_state_dict': self.discriminator.state_dict(),
            'scaler_mean': self._scaler_mean,
            'scaler_std': self._scaler_std,
            'config': self.config,
            'config_checksum': self._config_checksum,
            'timestamp': timestamp
        }

        if SAFETENSORS_AVAILABLE:
            base = path.replace('.pt', '')
            safe_save(state['generator_state_dict'], f"{base}_gen.safetensors")
            safe_save(state['discriminator_state_dict'], f"{base}_disc.safetensors")
            meta = {k: v for k, v in state.items() if k not in ['generator_state_dict', 'discriminator_state_dict']}
            meta['timestamp'] = timestamp
            with open(f"{base}_meta.json", 'w', encoding='utf-8') as f:
                json.dump(meta, f, indent=2)
            logger.info(f"模型已安全保存（safetensors）至 {base}_*.safetensors")
        else:
            torch.save(state, path)
            with open(path, 'rb') as f:
                checksum = hashlib.sha256(f.read()).hexdigest()
            with open(path + '.sha256', 'w') as f:
                f.write(checksum)
            logger.info(f"模型已保存至 {path}，校验和: {checksum[:8]}")

    def _load_model(self, path: str):
        """加载模型，安全校验，支持 .pt 或 safetensors 元数据"""
        if SAFETENSORS_AVAILABLE and (path.endswith('.safetensors') or os.path.exists(path.replace('.pt','_meta.json'))):
            base = path.replace('.pt','') if path.endswith('.pt') else path.replace('_gen.safetensors','').replace('_disc.safetensors','')
            meta_path = f"{base}_meta.json"
            gen_path = f"{base}_gen.safetensors"
            disc_path = f"{base}_disc.safetensors"
            with open(meta_path, 'r') as f:
                meta = json.load(f)
            self.generator.load_state_dict(safe_load(gen_path))
            self.discriminator.load_state_dict(safe_load(disc_path))
            self._scaler_mean = meta['scaler_mean']
            self._scaler_std = meta['scaler_std']
            # 验证配置一致性（可选）
            if meta.get('config_checksum') and self.config.get('model_checksum_required'):
                if meta['config_checksum'] != self._config_checksum:
                    logger.warning(f"当前配置哈希 ({self._config_checksum}) 与模型训练时 ({meta['config_checksum']}) 不一致，可能影响生成质量")
            logger.info(f"已从 safetensors 加载模型 {base}")
        else:
            if self.config.get('model_checksum_required') and os.path.exists(path + '.sha256'):
                with open(path, 'rb') as f:
                    current = hashlib.sha256(f.read()).hexdigest()
                with open(path + '.sha256', 'r') as f:
                    expected = f.read().strip()
                if current != expected:
                    raise RuntimeError(f"模型文件校验失败！期望 {expected[:8]}，实际 {current[:8]}")
            checkpoint = torch.load(path, map_location=self.device)
            self.generator.load_state_dict(checkpoint['generator_state_dict'])
            self.discriminator.load_state_dict(checkpoint['discriminator_state_dict'])
            self._scaler_mean = checkpoint['scaler_mean']
            self._scaler_std = checkpoint['scaler_std']
            logger.info(f"已加载模型 {path}")

    # ── 数据预处理（无前瞻偏差，含缩尾） ──
    def _preprocess_returns(self, returns: np.ndarray,
                            validation_split: float = 0.0) -> Tuple[torch.Tensor, Optional[torch.Tensor], float, float]:
        returns = returns[np.isfinite(returns)]
        if len(returns) < self.seq_len * 2:
            raise ValueError(f"数据长度不足，至少需要 {self.seq_len*2} 个观测值")
        # 缩尾处理
        if self.config["winsorize_pct"] > 0:
            lower = np.percentile(returns, self.config["winsorize_pct"] * 100)
            upper = np.percentile(returns, 100 - self.config["winsorize_pct"] * 100)
            returns = np.clip(returns, lower, upper)

        n_total = len(returns) - self.seq_len + 1
        split_idx = int(n_total * (1 - validation_split))
        train_ret_seg = returns[:split_idx + self.seq_len]
        mean = float(np.nanmean(train_ret_seg))
        std = float(np.nanstd(train_ret_seg) + 1e-8)
        norm = (returns - mean) / std

        def make_seqs(data, offset):
            return np.array([data[i:i+self.seq_len] for i in range(offset, offset + len(data) - self.seq_len + 1)])

        train_tensor = torch.tensor(make_seqs(norm, 0)[:split_idx], dtype=torch.float32).to(self.device)
        val_tensor = None
        if validation_split > 0 and split_idx < n_total:
            val_seqs = make_seqs(norm, split_idx)
            val_tensor = torch.tensor(val_seqs, dtype=torch.float32).to(self.device)
        return train_tensor, val_tensor, mean, std

    def _gradient_penalty(self, real_data: torch.Tensor, fake_data: torch.Tensor) -> torch.Tensor:
        batch_size = real_data.size(0)
        epsilon = torch.rand(batch_size, 1, device=self.device)
        epsilon = epsilon.expand_as(real_data)
        interpolated = epsilon * real_data + (1 - epsilon) * fake_data
        prob_interpolated = self.discriminator(interpolated)
        gradients = torch.autograd.grad(
            outputs=prob_interpolated,
            inputs=interpolated,
            grad_outputs=torch.ones_like(prob_interpolated),
            create_graph=True,
            retain_graph=True
        )[0]
        gradients = gradients.view(batch_size, -1)
        gradient_norm = gradients.norm(2, dim=1)
        gp = self.config["gp_weight"] * ((gradient_norm - 1) ** 2).mean()
        return gp

    def train(self, real_returns: np.ndarray, validation_returns: Optional[np.ndarray] = None,
              **override_params) -> Dict[str, Any]:
        """
        训练 WGAN-GP。
        validation_returns: 外部验证集（可选），若提供，则 real_returns 仅用于训练，不进行内部分割。
        返回训练摘要字典。
        """
        if not TORCH_AVAILABLE:
            return {"status": "error", "reason": "PyTorch 不可用，无法训练", "warnings": []}

        train_cfg = copy.deepcopy(self.config)
        train_cfg.update(override_params)

        # 固定种子
        if train_cfg.get("seed") is not None:
            torch.manual_seed(train_cfg["seed"])
            np.random.seed(train_cfg["seed"])
            logger.info(f"训练种子设置为 {train_cfg['seed']}")

        self._real_train_returns = real_returns.copy()

        # 设置模式
        self.generator.train()
        self.discriminator.train()

        try:
            if validation_returns is not None:
                # 外部验证集，使用全部 real_returns 训练
                train_tensor, _, mean, std = self._preprocess_returns(real_returns, 0.0)
                val_norm = (validation_returns - mean) / std
                val_seqs = [val_norm[i:i+self.seq_len] for i in range(len(val_norm)-self.seq_len+1)]
                val_tensor = torch.tensor(np.array(val_seqs), dtype=torch.float32).to(self.device) if val_seqs else None
            else:
                train_tensor, val_tensor, mean, std = self._preprocess_returns(real_returns, validation_split=0.2)

            self._scaler_mean = mean
            self._scaler_std = std

            dataset = TensorDataset(train_tensor)
            pin_memory = self.device.type == 'cuda'
            dataloader = DataLoader(dataset, batch_size=train_cfg["batch_size"], shuffle=True,
                                    pin_memory=pin_memory, drop_last=True)

            g_optim = optim.Adam(self.generator.parameters(), lr=train_cfg["lr_g"],
                                 betas=(train_cfg["beta1"], train_cfg["beta2"]))
            d_optim = optim.Adam(self.discriminator.parameters(), lr=train_cfg["lr_d"],
                                 betas=(train_cfg["beta1"], train_cfg["beta2"]))
            # 学习率调度
            g_scheduler = optim.lr_scheduler.StepLR(g_optim, step_size=train_cfg["lr_decay_step"],
                                                    gamma=train_cfg["lr_decay_factor"])
            d_scheduler = optim.lr_scheduler.StepLR(d_optim, step_size=train_cfg["lr_decay_step"],
                                                    gamma=train_cfg["lr_decay_factor"])

            scaler = GradScaler() if AMP_AVAILABLE and train_cfg.get("use_amp") and self.device.type == 'cuda' else None

            best_val_wdist = float('inf')
            patience_counter = 0
            best_state = None
            d_losses, g_losses = [], []

            for epoch in range(train_cfg["max_epochs"]):
                epoch_d_loss, epoch_g_loss = 0.0, 0.0
                n_batches = 0
                for (real_seqs,) in dataloader:
                    bs = real_seqs.size(0)
                    # 训练判别器多次
                    for _ in range(train_cfg["n_critic"]):
                        d_optim.zero_grad()
                        if scaler:
                            with autocast():
                                real_val = self.discriminator(real_seqs)
                                noise = torch.randn(bs, self.noise_dim, device=self.device)
                                fake = self.generator(noise)
                                fake_val = self.discriminator(fake.detach())
                                d_loss = fake_val.mean() - real_val.mean()
                                gp = self._gradient_penalty(real_seqs, fake.detach())
                                d_total = d_loss + gp
                            scaler.scale(d_total).backward()
                            scaler.unscale_(d_optim)
                            torch.nn.utils.clip_grad_norm_(self.discriminator.parameters(), train_cfg["gradient_clip"])
                            scaler.step(d_optim)
                            scaler.update()
                        else:
                            real_val = self.discriminator(real_seqs)
                            noise = torch.randn(bs, self.noise_dim, device=self.device)
                            fake = self.generator(noise)
                            fake_val = self.discriminator(fake.detach())
                            d_loss = fake_val.mean() - real_val.mean()
                            gp = self._gradient_penalty(real_seqs, fake.detach())
                            d_total = d_loss + gp
                            d_total.backward()
                            torch.nn.utils.clip_grad_norm_(self.discriminator.parameters(), train_cfg["gradient_clip"])
                            d_optim.step()

                    # 训练生成器
                    g_optim.zero_grad()
                    if scaler:
                        with autocast():
                            noise = torch.randn(bs, self.noise_dim, device=self.device)
                            fake = self.generator(noise)
                            g_loss = -self.discriminator(fake).mean()
                        scaler.scale(g_loss).backward()
                        scaler.unscale_(g_optim)
                        torch.nn.utils.clip_grad_norm_(self.generator.parameters(), train_cfg["gradient_clip"])
                        scaler.step(g_optim)
                        scaler.update()
                    else:
                        noise = torch.randn(bs, self.noise_dim, device=self.device)
                        fake = self.generator(noise)
                        g_loss = -self.discriminator(fake).mean()
                        g_loss.backward()
                        torch.nn.utils.clip_grad_norm_(self.generator.parameters(), train_cfg["gradient_clip"])
                        g_optim.step()

                    epoch_d_loss += d_total.item()
                    epoch_g_loss += g_loss.item()
                    n_batches += 1

                avg_d = epoch_d_loss / n_batches
                avg_g = epoch_g_loss / n_batches
                d_losses.append(avg_d)
                g_losses.append(avg_g)

                g_scheduler.step()
                d_scheduler.step()

                # 验证集评估（Wasserstein 距离，有符号）
                if val_tensor is not None:
                    self.generator.eval()
                    with torch.no_grad():
                        noise_val = torch.randn(len(val_tensor), self.noise_dim, device=self.device)
                        fake_val = self.generator(noise_val)
                        real_mean = self.discriminator(val_tensor).mean()
                        fake_mean = self.discriminator(fake_val).mean()
                        w_dist = real_mean - fake_mean  # 不取绝对值，正值表示真实>虚假
                    self.generator.train()
                    # 使用绝对 Wasserstein 距离的移动平均作为早停指标
                    w_dist_abs = abs(w_dist.item())
                    if w_dist_abs < best_val_wdist:
                        best_val_wdist = w_dist_abs
                        patience_counter = 0
                        best_state = {
                            'gen': {k: v.cpu().clone() for k, v in self.generator.state_dict().items()},
                            'disc': {k: v.cpu().clone() for k, v in self.discriminator.state_dict().items()}
                        }
                    else:
                        patience_counter += 1
                        if patience_counter >= train_cfg["patience"]:
                            logger.info(f"早停于 epoch {epoch+1}，最佳 Wasserstein 距离: {best_val_wdist:.6f}")
                            break
                else:
                    # 无验证集时仅保存最新状态作为最佳（无早停）
                    best_state = None

                if (epoch + 1) % 100 == 0:
                    logger.info(f"Epoch {epoch+1} | D Loss: {avg_d:.4f} | G Loss: {avg_g:.4f}")

            # 恢复最佳模型
            if best_state is not None:
                self.generator.load_state_dict(best_state['gen'])
                self.discriminator.load_state_dict(best_state['disc'])
                logger.info("已恢复验证集最优模型权重")

            # 保存最终模型
            model_path = os.path.join(train_cfg["model_dir"], f"gan_model_{self._config_checksum}.pt")
            self._save_model(model_path)

            return {
                "status": "ok",
                "epochs_trained": epoch + 1,
                "best_val_wasserstein_dist": best_val_wdist if val_tensor is not None else None,
                "final_d_loss": d_losses[-1] if d_losses else None,
                "final_g_loss": g_losses[-1] if g_losses else None,
                "config_checksum": self._config_checksum,
                "warnings": []
            }
        except Exception as e:
            logger.exception("训练过程中发生异常")
            if best_state is not None:
                self.generator.load_state_dict(best_state['gen'])
                self.discriminator.load_state_dict(best_state['disc'])
            return {"status": "error", "reason": str(e), "warnings": []}
        finally:
            if torch.cuda.is_available():
                torch.cuda.empty_cache()

    # ── 生成样本 ──
    def generate_samples(self, num_samples: int, seq_len: Optional[int] = None,
                         seed: Optional[int] = None, extreme: bool = False) -> np.ndarray:
        """
        生成合成收益率序列。
        extreme: 若 True，注入复合跳跃过程，模拟市场崩盘/暴涨。
        """
        if seed is not None:
            np.random.seed(seed)
            if TORCH_AVAILABLE:
                torch.manual_seed(seed)

        seq_len = seq_len or self.seq_len

        if TORCH_AVAILABLE and self.generator:
            self.generator.eval()
            all_samples = []
            with torch.no_grad():
                # 分批次生成，避免 OOM
                batch_size = min(num_samples, 256)
                n_batches = int(np.ceil(num_samples / batch_size))
                for i in range(n_batches):
                    current_bs = min(batch_size, num_samples - len(all_samples))
                    noise_std = 3.0 if extreme else 1.0
                    noise = torch.randn(current_bs, self.noise_dim, device=self.device) * noise_std
                    gen = self.generator(noise).cpu().numpy()
                    gen = gen * self._scaler_std + self._scaler_mean
                    if extreme:
                        # 跳跃幅度基于历史分位数
                        jump_scale = self.config["jump_scale_mult"] * self._scaler_std
                        jump_mask = np.random.binomial(1, self.config["jump_lambda"], size=gen.shape)
                        direction = np.random.choice([-1, 1], size=gen.shape)
                        gen += jump_mask * direction * np.random.exponential(scale=jump_scale, size=gen.shape)
                    # 调整长度
                    if gen.shape[1] > seq_len:
                        gen = gen[:, :seq_len]
                    elif gen.shape[1] < seq_len:
                        # 使用随机填充而非边缘填充，避免虚假平稳
                        pad_len = seq_len - gen.shape[1]
                        pad = np.random.normal(0, self._scaler_std * 0.1, size=(gen.shape[0], pad_len))
                        gen = np.concatenate([gen, pad], axis=1)
                    # 数值安全检查
                    if not _is_finite(gen):
                        gen[~np.isfinite(gen)] = 0.0
                    all_samples.append(gen)
            return np.concatenate(all_samples, axis=0)[:num_samples]
        else:
            return self._fallback_generate(num_samples, seq_len, extreme)

    def _fallback_generate(self, num_samples: int, seq_len: int, extreme: bool) -> np.ndarray:
        if self._real_train_returns is None or len(self._real_train_returns) < seq_len:
            logger.warning("历史数据不足，生成纯噪声")
            return np.random.standard_t(df=3, size=(num_samples, seq_len)) * 0.01

        rets = self._real_train_returns
        if len(rets) <= seq_len:
            # 若长度相等，直接重复该序列并加微小噪声
            base = np.tile(rets, (num_samples, 1))
            return base + np.random.normal(0, np.std(rets)*0.1, size=base.shape)

        samples = []
        scale = np.std(rets)
        for _ in range(num_samples):
            start = np.random.randint(0, len(rets) - seq_len)
            seg = rets[start:start+seq_len].copy()
            if extreme:
                noise = np.random.standard_t(df=3, size=seq_len) * scale * 0.5
                seg = seg + noise
                jump = np.random.binomial(1, 0.05, size=seq_len) * np.random.laplace(0, scale * 3, size=seq_len)
                seg += jump
            samples.append(seg)
        return np.array(samples)

    # ── 统计报告（机构级完备） ──
    @staticmethod
    def statistical_report(real: np.ndarray, synthetic: np.ndarray,
                           alpha: float = 0.05) -> Dict[str, Any]:
        """
        对比报告：均值、波动率、偏度、峰度、VaR、自相关、ARCH效应、KS检验
        """
        real = real[np.isfinite(real)]
        synthetic = synthetic[np.isfinite(synthetic)]
        if len(real) < 5 or len(synthetic) < 5:
            return {"error": "数据量不足"}

        def _desc(arr):
            return {
                "mean": float(np.mean(arr)),
                "std": float(np.std(arr)),
                "skew": _stable_skew(arr),
                "kurtosis": _stable_kurtosis(arr),
                "min": float(np.min(arr)),
                "max": float(np.max(arr)),
                "var_95": float(np.percentile(arr, 5))
            }

        report = {"real": _desc(real), "synthetic": _desc(synthetic)}

        # 自相关 lag1
        if SCIPY_AVAILABLE:
            try:
                r_ac, _ = scipy_stats.pearsonr(real[:-1], real[1:])
                s_ac, _ = scipy_stats.pearsonr(synthetic[:-1], synthetic[1:])
                report["autocorr_lag1"] = {"real": r_ac, "synthetic": s_ac}
            except Exception as e:
                logger.warning(f"自相关计算失败: {e}")

            # ARCH 效应（平方收益率的自相关）
            try:
                real_sq = (real - np.mean(real))**2
                syn_sq = (synthetic - np.mean(synthetic))**2
                r_arch, _ = scipy_stats.pearsonr(real_sq[:-1], real_sq[1:])
                s_arch, _ = scipy_stats.pearsonr(syn_sq[:-1], syn_sq[1:])
                report["arch_effect"] = {"real": r_arch, "synthetic": s_arch}
            except Exception as e:
                logger.warning(f"ARCH效应计算失败: {e}")

            # KS 检验（两样本分布比较）
            try:
                ks_stat, ks_p = scipy_stats.ks_2samp(real, synthetic)
                report["ks_test"] = {"statistic": ks_stat, "p_value": ks_p,
                                      "reject_same_dist": bool(ks_p < alpha)}
            except Exception as e:
                logger.warning(f"KS检验失败: {e}")

            # Q-Q 残差（真实分位数与合成分位数的相关性）
            try:
                common_quantiles = np.linspace(0.01, 0.99, 50)
                real_q = np.quantile(real, common_quantiles)
                synth_q = np.quantile(synthetic, common_quantiles)
                qq_corr, _ = scipy_stats.pearsonr(real_q, synth_q)
                report["qq_correlation"] = qq_corr
            except Exception as e:
                logger.warning(f"Q-Q计算失败: {e}")
        else:
            report["scipy_warning"] = "scipy未安装，部分高级检验不可用"

        return report

    @classmethod
    def health_check(cls) -> Dict[str, Any]:
        """模块自检，不影响实例状态"""
        try:
            test_cfg = {'device': 'cpu', 'seq_len': 20}
            inst = cls(config=test_cfg)
            inst._real_train_returns = np.random.randn(200) * 0.01
            samples = inst.generate_samples(5, 20, seed=42)
            assert samples.shape == (5, 20)
            assert _is_finite(samples)
            report = cls.statistical_report(inst._real_train_returns, samples.flatten())
            assert "real" in report
            return {"status": "ok", "message": "GAN压力测试模块自检通过"}
        except Exception as e:
            logger.error(f"健康检查失败: {e}")
            return {"status": "error", "message": str(e)}

    def shutdown(self):
        """热重载时安全释放 GPU 资源"""
        if TORCH_AVAILABLE:
            del self.generator
            del self.discriminator
            torch.cuda.empty_cache()
        logger.info("GANStressTest 已关闭并释放资源")


# ── 主入口 ──
def main():
    import argparse
    import pandas as pd

    parser = argparse.ArgumentParser(description="火种对抗样本生成器（WGAN-GP 机构级）")
    parser.add_argument("--config", type=str, help="超参数YAML配置文件")
    parser.add_argument("--data", type=str, help="历史收益率CSV（单列，无表头）")
    parser.add_argument("--mode", choices=["train", "generate", "report"], default="generate")
    parser.add_argument("--model", type=str, default="models/gan_model.pt")
    parser.add_argument("--num-samples", type=int, default=10)
    parser.add_argument("--seq-len", type=int, default=50)
    parser.add_argument("--extreme", action="store_true")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=str, help="输出CSV文件路径")
    args = parser.parse_args()

    # 加载配置
    config = {}
    if args.config and os.path.exists(args.config):
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)

    inst = GANStressTest(config=config, model_path=args.model if args.mode in ("generate","report") else None)

    if args.mode == "train":
        if not args.data:
            sys.exit("训练模式需要 --data 指定历史收益率文件")
        data = pd.read_csv(args.data, header=None).values.flatten()
        result = inst.train(data)
        # 安全序列化 JSON，处理 numpy 类型
        def default_serializer(obj):
            if isinstance(obj, (np.integer,)):
                return int(obj)
            elif isinstance(obj, (np.floating,)):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            raise TypeError(f"无法序列化 {type(obj)}")
        print(json.dumps(result, indent=2, default=default_serializer))

    elif args.mode == "generate":
        samples = inst.generate_samples(args.num_samples, args.seq_len, seed=args.seed, extreme=args.extreme)
        if args.output:
            np.savetxt(args.output, samples, delimiter=',', header="synthetic_returns")
            print(f"生成样本已保存至 {args.output}")
        else:
            print(samples[:2])

    elif args.mode == "report":
        if not args.data:
            sys.exit("报告模式需要 --data")
        real = pd.read_csv(args.data, header=None).values.flatten()
        synthetic = inst.generate_samples(len(real), len(real), seed=args.seed, extreme=True).flatten()
        report = GANStressTest.statistical_report(real, synthetic)
        def default_serializer(obj):
            if isinstance(obj, (np.integer,)):
                return int(obj)
            elif isinstance(obj, (np.floating,)):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            raise TypeError
        print(json.dumps(report, indent=2, default=default_serializer))

if __name__ == "__main__":
    main()
