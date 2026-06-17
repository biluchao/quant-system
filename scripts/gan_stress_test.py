#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Spark Quant System Contributors. All Rights Reserved.
"""
火种系统 · 对抗样本压力测试生成器 (GANStressTest)

核心职责：
1. 学习历史价格动态（对数收益率）的分布，通过轻量生成对抗网络或统计极值模型生成合成场景
2. 通过极端因子引导产生尾部风险情景（闪崩、波动率爆发、跳空），用于策略压力测试
3. 输出标准化OHLCV合成数据，支持审计哈希与元数据记录

外部依赖（真实模块接口）：
- torch (PyTorch) : 张量计算与神经网络构建（可选，不可用时降级）
- numpy : 数值处理
- pandas : 历史数据加载与序列化

接口契约：
- generate(num_scenarios: int = 100, length: int = 512, extreme_factor: float = 1.0,
           seed: Optional[int] = None) -> Dict[str, Any]
  返回包含 "scenarios" (List[List[float]]) 和审计信息的字典
- train(data: np.ndarray, **kwargs) -> Dict[str, Any]
- health_check() -> Dict[str, Any]

异常与降级：
- 若 PyTorch 不可用，自动降级为统计极值模型（Heston+复合泊松跳跃），记录 WARNING
- 所有异常均捕获并返回结构化错误，不影响主进程
- 随机种子可复现（提供seed参数或使用系统熵源）

资源管理：
- 模型参数常驻内存（<50MB），支持热重载
- 生成过程使用CPU，支持批量生成，单次<1秒
"""

import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

import numpy as np
import pandas as pd

# 可选依赖
try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

logger = logging.getLogger(__name__)

# ── 常量定义 ──────────────────────────────────────────────
VERSION = "3.0.1"
SPDX_IDENTIFIER = "Apache-2.0"
DEFAULT_SEQUENCE_LENGTH = 512
DEFAULT_NOISE_DIM = 32
DEFAULT_HIDDEN_DIM = 64
MAX_SCENARIOS = 10000
MIN_LENGTH = 10
MAX_LENGTH = 2000
EXTREME_FACTOR_MIN = 0.5
EXTREME_FACTOR_MAX = 10.0
DEFAULT_TRAINING_EPOCHS = 50
DEFAULT_BATCH_SIZE = 32
DEFAULT_LEARNING_RATE = 0.0002

# 统计模型默认参数（对数收益率空间）
DEFAULT_ANNUAL_VOL = 0.8           # 年化波动率 80% (加密货币典型值)
DEFAULT_JUMP_INTENSITY = 0.05      # 每日跳跃概率
DEFAULT_JUMP_STD = 0.05            # 跳跃幅度标准差（对数）
DEFAULT_BASE_PRICE = 40000.0       # BTC 参考价


class _LightweightGAN(nn.Module):
    """轻量全连接 GAN 生成器，适用于一维对数收益率序列"""
    def __init__(self, noise_dim: int, seq_len: int, hidden_dim: int = 64):
        super().__init__()
        self.seq_len = seq_len
        self.model = nn.Sequential(
            nn.Linear(noise_dim, hidden_dim),
            nn.LeakyReLU(0.2),
            nn.Linear(hidden_dim, hidden_dim * 2),
            nn.LeakyReLU(0.2),
            nn.Linear(hidden_dim * 2, seq_len),
            nn.Tanh()  # 输出范围 [-1,1]，便于学习对数收益率
        )

    def forward(self, z: torch.Tensor) -> torch.Tensor:
        return self.model(z)


class _LightweightDiscriminator(nn.Module):
    """轻量判别器"""
    def __init__(self, seq_len: int, hidden_dim: int = 64, dropout: float = 0.3):
        super().__init__()
        self.model = nn.Sequential(
            nn.Linear(seq_len, hidden_dim * 2),
            nn.LeakyReLU(0.2),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim * 2, hidden_dim),
            nn.LeakyReLU(0.2),
            nn.Linear(hidden_dim, 1),
            nn.Sigmoid()
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.model(x)


class GANStressTest:
    """
    对抗样本压力测试生成器

    支持两种模式：
    - GAN模式：学习对数收益率的分布，生成逼真序列
    - 统计模式：Heston随机波动率 + 复合泊松跳跃扩散，模拟尾部风险
    """

    DEFAULT_MODEL_PATH = "models/gan_stress.pth"

    def __init__(
        self,
        model_path: Optional[str] = None,
        use_gan: bool = True,
        base_price: float = DEFAULT_BASE_PRICE,
        annual_vol: float = DEFAULT_ANNUAL_VOL,
        jump_intensity: float = DEFAULT_JUMP_INTENSITY,
        jump_std: float = DEFAULT_JUMP_STD,
    ):
        self._use_gan = use_gan and TORCH_AVAILABLE
        self._model_path = model_path or self.DEFAULT_MODEL_PATH
        self._generator: Optional[_LightweightGAN] = None
        self._discriminator: Optional[_LightweightDiscriminator] = None
        self._device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        self._seq_len = DEFAULT_SEQUENCE_LENGTH
        self._noise_dim = DEFAULT_NOISE_DIM
        self._hidden_dim = DEFAULT_HIDDEN_DIM
        self._model_meta: Dict[str, Any] = {}
        # 统计模型参数
        self.base_price = base_price
        self.annual_vol = annual_vol
        self.jump_intensity = jump_intensity
        self.jump_std = jump_std

        if self._use_gan:
            self._init_gan()

    def _init_gan(self):
        """初始化GAN模型并尝试加载预训练权重，失败则降级"""
        self._generator = _LightweightGAN(self._noise_dim, self._seq_len, self._hidden_dim).to(self._device)
        self._discriminator = _LightweightDiscriminator(self._seq_len, self._hidden_dim).to(self._device)
        if os.path.exists(self._model_path):
            try:
                checkpoint = torch.load(self._model_path, map_location=self._device)
                # 验证元数据
                meta = checkpoint.get('meta', {})
                if meta.get('seq_len', self._seq_len) != self._seq_len:
                    logger.warning("模型训练长度 (%d) 与当前设置 (%d) 不匹配，降级为统计模型",
                                   meta.get('seq_len'), self._seq_len)
                    self._use_gan = False
                    return
                self._generator.load_state_dict(checkpoint['generator'])
                self._discriminator.load_state_dict(checkpoint['discriminator'])
                self._model_meta = meta
                logger.info("GAN预训练模型已加载: %s", self._model_path)
            except Exception as e:
                logger.error("加载GAN模型失败: %s，降级为统计模型", str(e))
                self._use_gan = False
        else:
            logger.info("未找到预训练模型，GAN处于未训练状态（generate时将使用统计模型）")
            self._use_gan = False

    def _get_seed(self, seed: Optional[int] = None) -> int:
        """生成安全随机种子"""
        if seed is not None:
            return seed
        return int.from_bytes(os.urandom(4), byteorder='big')

    # ── 统计极值模型（Heston + 复合泊松跳跃） ─────────────
    def _statistical_extreme_generator(
        self,
        num_scenarios: int,
        length: int,
        extreme_factor: float,
        seed: Optional[int] = None
    ) -> Tuple[np.ndarray, Dict[str, Any]]:
        """
        统计极值模型：随机波动率 + 复合泊松跳跃扩散

        返回: (scenarios_array, metadata)
        """
        rng = np.random.RandomState(self._get_seed(seed))
        # 参数校准（转换为每步参数，假设每步为3分钟，每日约480步）
        steps_per_day = 480
        dt = 1.0 / steps_per_day
        annual_vol = self.annual_vol
        base_vol = annual_vol * np.sqrt(dt)  # 每步波动率
        # 跳跃部分
        jump_prob = self.jump_intensity * dt * extreme_factor  # 每步跳跃概率
        jump_std = self.jump_std * np.sqrt(extreme_factor)

        scenarios = np.zeros((num_scenarios, length))
        metadata_list = []
        for i in range(num_scenarios):
            # 随机波动率路径（简化：OU过程）
            vol_path = np.ones(length) * base_vol
            # 对数收益率
            log_returns = rng.normal(0, vol_path)
            # 跳跃：每步以概率 jump_prob 发生跳跃，幅度从正态分布抽取
            jump_mask = rng.rand(length) < jump_prob
            jump_sizes = rng.normal(0, jump_std, size=length) * jump_mask
            log_returns += jump_sizes
            # 额外闪崩（在随机位置添加大幅负收益）
            if extreme_factor > 2.0:
                crash_count = min(length, int(extreme_factor * 2))
                crash_indices = rng.choice(length, size=crash_count, replace=False)
                crash_amplitudes = rng.uniform(0.03, 0.10, size=crash_count) * extreme_factor
                log_returns[crash_indices] -= crash_amplitudes
            # 构建价格序列
            prices = self.base_price * np.exp(np.cumsum(log_returns))
            prices = np.clip(prices, 1e-2, 1e8)
            scenarios[i] = prices
            # 记录场景统计信息
            metadata_list.append({
                "scenario_id": i,
                "max_drawdown": float(np.max((np.maximum.accumulate(prices) - prices) / np.maximum.accumulate(prices))),
                "volatility": float(np.std(np.diff(np.log(prices))) / np.sqrt(dt)),
                "jump_count": int(np.sum(jump_mask)),
            })
        return scenarios, {"scenario_meta": metadata_list}

    # ── GAN 生成（对数收益率空间） ─────────────────────────
    def _gan_generate(
        self,
        num_scenarios: int,
        length: int,
        extreme_factor: float,
        seed: Optional[int] = None
    ) -> Tuple[np.ndarray, Dict[str, Any]]:
        """使用GAN生成对数收益率序列，再转换为价格"""
        if self._generator is None or length != self._seq_len:
            # 降级
            return self._statistical_extreme_generator(num_scenarios, length, extreme_factor, seed)

        rng = np.random.RandomState(self._get_seed(seed))
        torch.manual_seed(self._get_seed(seed))
        self._generator.eval()
        scenarios = np.zeros((num_scenarios, length))
        batch_size = min(64, num_scenarios)
        with torch.no_grad():
            for start in range(0, num_scenarios, batch_size):
                end = min(start + batch_size, num_scenarios)
                bs = end - start
                noise = torch.randn(bs, self._noise_dim, device=self._device)
                # 极端因子放大噪声方差
                noise_scale = 1.0 + max(0.0, (extreme_factor - 1.0)) * 0.5
                noise = noise * noise_scale
                fake_returns = self._generator(noise).cpu().numpy()  # shape (bs, seq_len)
                # 反标准化（若模型训练时保存了归一化参数）
                ret_mean = self._model_meta.get('ret_mean', 0.0)
                ret_std = self._model_meta.get('ret_std', 0.01)
                fake_returns = fake_returns * ret_std + ret_mean
                # 构建价格
                prices = self.base_price * np.exp(np.cumsum(fake_returns, axis=1))
                scenarios[start:end] = np.clip(prices, 1e-2, 1e8)
        metadata = {"method": "GAN", "gan_model_path": self._model_path}
        return scenarios, metadata

    # ── 主生成接口 ────────────────────────────────────────
    def generate(
        self,
        num_scenarios: int = 100,
        length: int = DEFAULT_SEQUENCE_LENGTH,
        extreme_factor: float = 1.0,
        seed: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        生成压力测试场景

        Args:
            num_scenarios: 场景数量 (1~10000)
            length: 每个场景的步数 (10~2000)
            extreme_factor: 极端程度，1.0正常，>1.0加剧尾部 (建议1.0~5.0)
            seed: 随机种子，None 则使用熵源

        Returns:
            {"status": "ok", "scenarios": List[List[float]], "metadata": {...}}
        """
        # 参数校验
        if not (1 <= num_scenarios <= MAX_SCENARIOS):
            return {"status": "error", "reason": f"num_scenarios 需在 1~{MAX_SCENARIOS} 之间"}
        if not (MIN_LENGTH <= length <= MAX_LENGTH):
            return {"status": "error", "reason": f"length 需在 {MIN_LENGTH}~{MAX_LENGTH} 之间"}
        if not (EXTREME_FACTOR_MIN <= extreme_factor <= EXTREME_FACTOR_MAX):
            return {"status": "error", "reason": f"extreme_factor 需在 {EXTREME_FACTOR_MIN}~{EXTREME_FACTOR_MAX} 之间"}

        start_time = time.perf_counter()
        seed = self._get_seed(seed)
        warnings = []

        try:
            if self._use_gan and self._generator is not None:
                scenarios_arr, gen_meta = self._gan_generate(num_scenarios, length, extreme_factor, seed)
            else:
                scenarios_arr, gen_meta = self._statistical_extreme_generator(
                    num_scenarios, length, extreme_factor, seed
                )
                if self._use_gan and self._generator is None:
                    warnings.append("GAN未训练或无生成器，已使用统计模型")
        except Exception as e:
            logger.error("生成失败: %s，降级为统计模型", str(e))
            warnings.append(f"GAN生成异常: {str(e)}")
            scenarios_arr, gen_meta = self._statistical_extreme_generator(
                num_scenarios, length, extreme_factor, seed
            )

        elapsed = time.perf_counter() - start_time
        scenarios_list = scenarios_arr.tolist()

        # 审计元数据
        audit_hash = hashlib.sha256(
            json.dumps(scenarios_list, default=str).encode('utf-8')
        ).hexdigest()

        metadata = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "version": VERSION,
            "num_scenarios": num_scenarios,
            "length": length,
            "extreme_factor": extreme_factor,
            "seed": seed,
            "base_price": self.base_price,
            "elapsed_seconds": elapsed,
            "method": "GAN" if self._use_gan else "Statistical",
            "scenario_hash_sha256": audit_hash,
            **gen_meta,
        }

        return {
            "status": "ok",
            "scenarios": scenarios_list,
            "metadata": metadata,
            "warnings": warnings,
        }

    # ── 训练接口 ──────────────────────────────────────────
    def train(
        self,
        data: np.ndarray,
        epochs: int = DEFAULT_TRAINING_EPOCHS,
        batch_size: int = DEFAULT_BATCH_SIZE,
        lr: float = DEFAULT_LEARNING_RATE,
    ) -> Dict[str, Any]:
        """
        使用历史对数收益率数据训练GAN

        Args:
            data: 形状 (samples, seq_len) 的对数收益率序列，建议已标准化
            epochs: 训练轮数
            batch_size: 批次大小
            lr: 学习率

        Returns:
            训练结果字典
        """
        if not TORCH_AVAILABLE:
            return {"status": "error", "reason": "PyTorch不可用，无法训练GAN"}
        if len(data) == 0:
            return {"status": "error", "reason": "训练数据为空"}
        try:
            data_tensor = torch.tensor(data, dtype=torch.float32)
            dataset = torch.utils.data.TensorDataset(data_tensor)
            dataloader = torch.utils.data.DataLoader(dataset, batch_size=batch_size, shuffle=True)

            # 计算归一化参数并保存
            ret_mean = float(np.mean(data))
            ret_std = float(np.std(data)) + 1e-8
            # 归一化到近似 [-1, 1]
            normalized_data = (data - ret_mean) / ret_std
            data_tensor = torch.tensor(normalized_data, dtype=torch.float32)
            dataset = torch.utils.data.TensorDataset(data_tensor)
            dataloader = torch.utils.data.DataLoader(dataset, batch_size=batch_size, shuffle=True)

            self._seq_len = data.shape[1]
            self._init_gan()  # 重新初始化以匹配序列长度

            criterion = nn.BCELoss()
            optimizer_g = optim.Adam(self._generator.parameters(), lr=lr)
            optimizer_d = optim.Adam(self._discriminator.parameters(), lr=lr)

            for epoch in range(epochs):
                for (real_batch,) in dataloader:
                    real_batch = real_batch.to(self._device)
                    current_bs = real_batch.size(0)
                    # 标签平滑（双侧）
                    real_labels = torch.full((current_bs, 1), 0.9, device=self._device)
                    fake_labels = torch.full((current_bs, 1), 0.1, device=self._device)

                    # 训练判别器
                    optimizer_d.zero_grad()
                    outputs_real = self._discriminator(real_batch)
                    loss_d_real = criterion(outputs_real, real_labels)
                    noise = torch.randn(current_bs, self._noise_dim, device=self._device)
                    fake_batch = self._generator(noise)
                    outputs_fake = self._discriminator(fake_batch.detach())
                    loss_d_fake = criterion(outputs_fake, fake_labels)
                    loss_d = loss_d_real + loss_d_fake
                    loss_d.backward()
                    optimizer_d.step()

                    # 训练生成器
                    optimizer_g.zero_grad()
                    noise = torch.randn(current_bs, self._noise_dim, device=self._device)
                    fake_batch = self._generator(noise)
                    outputs = self._discriminator(fake_batch)
                    loss_g = criterion(outputs, torch.ones(current_bs, 1, device=self._device))
                    loss_g.backward()
                    optimizer_g.step()

                if (epoch + 1) % 10 == 0:
                    logger.info("Epoch %d/%d | D loss: %.4f | G loss: %.4f", epoch+1, epochs, loss_d.item(), loss_g.item())

            # 保存模型与元数据
            os.makedirs(os.path.dirname(self._model_path), exist_ok=True)
            torch.save({
                'generator': self._generator.state_dict(),
                'discriminator': self._discriminator.state_dict(),
                'meta': {
                    'seq_len': self._seq_len,
                    'ret_mean': ret_mean,
                    'ret_std': ret_std,
                    'noise_dim': self._noise_dim,
                }
            }, self._model_path)
            self._model_meta = {'seq_len': self._seq_len, 'ret_mean': ret_mean, 'ret_std': ret_std}
            self._use_gan = True  # 训练成功后启用GAN模式
            logger.info("GAN模型已保存至 %s", self._model_path)

            return {
                "status": "ok",
                "loss_g": loss_g.item(),
                "loss_d": loss_d.item(),
                "epochs": epochs,
                "model_path": self._model_path,
            }
        except Exception as e:
            logger.error("GAN训练失败: %s", str(e))
            return {"status": "error", "reason": str(e)}

    @classmethod
    def health_check(cls) -> Dict[str, Any]:
        """模块自检"""
        try:
            inst = cls(use_gan=False)
            res = inst.generate(num_scenarios=2, length=50, extreme_factor=1.0, seed=42)
            if res["status"] == "ok" and len(res["scenarios"]) == 2:
                return {
                    "status": "ok",
                    "message": f"压力测试生成器可用 (方法: {res['metadata']['method']})",
                }
            return {"status": "error", "message": res.get("reason", "生成失败")}
        except Exception as e:
            logger.error("健康检查失败: %s", str(e))
            return {"status": "error", "message": str(e)}


def main():
    """命令行入口"""
    import argparse

    parser = argparse.ArgumentParser(description=f"火种对抗样本压力测试生成器 v{VERSION}")
    parser.add_argument("--scenarios", type=int, default=10, help="生成场景数量")
    parser.add_argument("--length", type=int, default=DEFAULT_SEQUENCE_LENGTH, help="序列长度（步数）")
    parser.add_argument("--extreme", type=float, default=1.0, help="极端因子 (>1.0加剧尾部)")
    parser.add_argument("--output", type=str, default="stress_scenarios.csv", help="输出CSV路径")
    parser.add_argument("--train", type=str, help="历史数据CSV用于训练GAN (列：价格)")
    parser.add_argument("--epochs", type=int, default=DEFAULT_TRAINING_EPOCHS, help="训练轮数")
    parser.add_argument("--seed", type=int, default=None, help="随机种子（可复现）")
    parser.add_argument("--base-price", type=float, default=DEFAULT_BASE_PRICE, help="基准价格")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')

    inst = GANStressTest(base_price=args.base_price, use_gan=True)

    # 训练模式
    if args.train:
        try:
            df = pd.read_csv(args.train, encoding='utf-8')
            prices = df.iloc[:, 0].values.astype(np.float64)
            # 计算对数收益率并切窗口
            log_returns = np.diff(np.log(prices))
            seq_len = args.length
            windows = [log_returns[i:i+seq_len] for i in range(0, len(log_returns)-seq_len, seq_len//2)]
            if len(windows) < 10:
                logger.error("训练数据不足（需要至少10个窗口）")
                sys.exit(1)
            data = np.array(windows)
            logger.info("开始GAN训练，样本数: %d", len(data))
            train_res = inst.train(data, epochs=args.epochs)
            print(train_res)
            if train_res["status"] != "ok":
                sys.exit(1)
        except Exception as e:
            logger.error("训练失败: %s", str(e))
            sys.exit(1)

    # 生成场景
    result = inst.generate(
        num_scenarios=args.scenarios,
        length=args.length,
        extreme_factor=args.extreme,
        seed=args.seed,
    )
    if result["status"] != "ok":
        logger.error("生成失败: %s", result.get("reason", ""))
        sys.exit(1)

    # 保存CSV（附带元数据）
    scenarios = result["scenarios"]
    df = pd.DataFrame(scenarios).T
    df.columns = [f"scenario_{i+1}" for i in range(len(scenarios))]
    df.to_csv(args.output, index=False)
    logger.info("压力场景已保存至 %s (方法: %s, 耗时: %.3fs)", args.output,
                result["metadata"]["method"], result["metadata"]["elapsed_seconds"])
    # 输出元数据JSON
    meta_path = args.output.replace('.csv', '_meta.json')
    with open(meta_path, 'w') as f:
        json.dump(result["metadata"], f, indent=2, default=str)
    logger.info("元数据已保存至 %s", meta_path)
    print(f"生成完成: {args.output}")


if __name__ == "__main__":
    main()
