火种量化交易系统 · 数学公式索引

本文档建立系统内关键量化模型与算法的数学表达式到代码实现的映射，便于开发、维护与审计追溯。

版本 日期 作者
1.0.0 2026-06-20 AI Architect

---

索引表

模块 公式/算法 数学表达（LaTeX） 所在文件 核心函数/类
粒子滤波 (Particle Filter) 状态转移 (OU过程) d\mu_t = -\kappa_\mu \mu_t dt + \sigma_\mu dW^\mu_t d\theta_t = -\kappa_\theta \theta_t dt + \sigma_\theta dW^\theta_t core/particle_filter.py ParticleFilter.predict()
粒子滤波 观测模型 (价格偏离) P(z_t \mid \mu_t) = \mathcal{N}(z_t; \mu_t, \sigma_{obs}^2) core/particle_filter.py ParticleFilter.update()
粒子滤波 有效样本数 (ESS) N_{eff} = \frac{1}{\sum_{i=1}^N (w^{(i)})^2} core/particle_filter.py ParticleFilter.resample()
粒子滤波 分层重采样 u^{(i)} = \frac{i-1 + U_i}{N},\; U_i \sim \mathcal{U}(0,1) core/particle_filter.py ParticleFilter._stratified_resample()
高斯过程分类 (GP Classifier) RBF 核 k(\mathbf{x}, \mathbf{x}') = \sigma_f^2 \exp\left(-\frac{\|\mathbf{x} - \mathbf{x}'\|^2}{2\ell^2}\right) core/gp_classifier.py GPClassifier._kernel()
高斯过程分类 稀疏变分后验 (SVGP) q(\mathbf{f}) = \mathcal{N}(\mathbf{m}, \mathbf{S}) core/gp_classifier.py GPClassifier._variational_predict()
高斯过程分类 Probit 似然 \Phi(z) = \frac{1}{2}\left[1 + \mathrm{erf}\left(\frac{z}{\sqrt{2}}\right)\right] core/gp_classifier.py GPClassifier._probit()
高斯过程分类 自然梯度更新 (均值) \(\mathbf{m} \leftarrow \mathbf{m} - \eta \mathbf{K}_{mm}^{-1} \mathbf{k}_{u*} \frac{\partial \log p(y f)}{\partial f}) core/gp_classifier.py
自适应参数调谐 (CMA-ES) 采样 \mathbf{x}_k = \mathbf{m} + \sigma \mathbf{B} \mathbf{D} \mathbf{z}_k,\; \mathbf{z}_k \sim \mathcal{N}(0,I) core/adaptive_parameter_tuner.py AdaptiveParameterTuner._optimize_cma()
自适应参数调谐 均值更新 \mathbf{m} \leftarrow \sum_{i=1}^\mu w_i \mathbf{x}_{i:\lambda} core/adaptive_parameter_tuner.py AdaptiveParameterTuner._optimize_cma()
自适应参数调谐 协方差矩阵自适应 \mathbf{C} \leftarrow (1-c_1-c_\mu) \mathbf{C} + c_1 \mathbf{p}_c \mathbf{p}_c^T + c_\mu \sum w_i \mathbf{y}_i \mathbf{y}_i^T core/adaptive_parameter_tuner.py AdaptiveParameterTuner._optimize_cma()
最优执行 (Almgren-Chriss) 临时冲击 h(v_t) = \eta v_t core/execution_algo.py ExecutionAlgo._execute_plan()
最优执行 最优交易率 v^*(t) = \frac{\kappa X}{\sinh(\kappa T)} \cosh(\kappa(T-t)) core/execution_algo.py ExecutionAlgo._compute_slices()
最优执行 拆分权重 w_i \propto e^{-\kappa t_i} core/execution_planner.py ExecutionPlanner._compute_slices()
风险预算 (RiskBudget) 年化波动率 \sigma_{ann} = \sigma_{daily} \sqrt{365} core/risk_budget.py RiskBudget._compute_volatility()
风险预算 历史VaR \text{VaR}_\alpha = - \text{Percentile}(r, 1-\alpha) core/risk_budget.py RiskBudget._compute_var_es()
风险预算 Expected Shortfall \text{ES}_\alpha = -\mathbb{E}[r \mid r \leq -\text{VaR}_\alpha] core/risk_budget.py RiskBudget._compute_var_es()
风险预算 波动率目标敞口 \text{Exposure}_{\text{target}} = \text{Equity} \cdot \min\left(\frac{\sigma_{\text{target}}}{\sigma_{\text{realized}}}, L_{\max}, H_{\max}\right) core/risk_budget.py RiskBudget._compute_max_exposure()
假突破检测 (False Breakout) SPRT 序贯检验 \Lambda_n = \prod_{i=1}^n \frac{P(x_i \mid H_1)}{P(x_i \mid H_0)} core/false_breakout.py FalseBreakoutDetector.check_recovery_signal()
贝叶斯推断 (Bayesian Score) 朴素贝叶斯后验 P(\text{entry} \mid \mathbf{F}) \propto P(\text{entry}) \prod_i P(F_i \mid \text{entry}) core/bayesian_score.py BayesianInference.compute_score()
移动平均核心 (MA Core) 简单移动平均 \text{SMA}_t(n) = \frac{1}{n}\sum_{i=0}^{n-1} C_{t-i} core/ma_core.py MACore.value()
移动平均核心 均线斜率 \text{slope}_t \approx \text{SMA}_t - \text{SMA}_{t-1} core/ma_core.py MACore.slope()
移动平均核心 均线角度 \theta_t = \arctan\left(\frac{\text{slope}_t}{\text{scale}}\right) core/ma_core.py MACore.angle_deg()
ATR 计算 平均真实波幅 \text{ATR}_t = \frac{1}{n}\left[(n-1)\text{ATR}_{t-1} + \text{TR}_t\right] core/atr.py ATR.update()
不回归五条件 脉冲强度 \(I = \frac{ C_{\text{cross}} - \text{MA26}_{\text{cross}} }{\text{ATR}} \times \sqrt{\frac{V_{\text{cross}}}{\bar{V}}})
不回归五条件 局部李雅普诺夫指数 \(\lambda_t = \ln\left(\frac{ C_t - \text{MA26}_t }{
不回归五条件 低频能量占比 (Goertzel) E_{\text{low}} = \frac{\sum_{f < f_c} P(f)}{\sum P(f)} core/low_freq_energy.py LowFreqEnergy.compute()
盈亏计算 (PnL) 未实现盈亏 \text{UPnL}_{\text{long}} = (P_{\text{mark}} - P_{\text{entry}}) \times Q \times \text{multiplier} core/pnl_calculator.py PnlCalculator.get_unrealized_pnl()
盈亏计算 已实现盈亏 \text{RPnL} = \sum_{\text{trades}} \text{pnl} core/pnl_calculator.py PnlCalculator.get_realized_pnl()
趋势状态推断 (Hysteresis) 滞后计数 \text{state\_counter}[s] = \begin{cases} \min(\text{counter}+1, \text{MAX}) & s = \text{target} \\ 0 & \text{otherwise} \end{cases} core/trend_state_inference.py TrendStateInference._apply_hysteresis()
趋势状态推断 趋势候选判定 \text{candidate} = \begin{cases} \text{DIVERGING} & \mu > \mu_{div} \land p_{div} \geq p_{th} \\ \text{RETRACING} & \mu < \mu_{ret} \land p_{div} \leq 1-p_{th} \\ \text{OSCILLATING} & \text{otherwise} \end{cases} core/trend_state_inference.py TrendStateInference._determine_candidate()
支撑/压力线 厚度计算 \Delta_i = \max\left(0.2 \cdot \text{ATR}, 5 \cdot \text{tick\_size}\right) \times \frac{1}{\sqrt{n_{\text{touch}} + 1}} core/sr_zone_manager.py SRZoneManager.update_thickness()

---

以上索引覆盖了系统主要量化模型与数学公式，每个条目均对应到具体的源文件和函数，便于代码审查、模型验证和文档维护。
