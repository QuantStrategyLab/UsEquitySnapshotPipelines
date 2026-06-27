# TECL / XLK 优化研究

[English](tecl-xlk-optimization-research.md)

研究日期：2026-06-28

状态：**仅研究（已归档）**。不要将 `tecl_xlk_trend_income` 提升为
`runtime_enabled`。该 profile 在重叠窗口上未通过相对 live **TQQQ** 与
**SOXL** proxy 的 promotion gate；代码、回测入口与产物仅保留供人工复查。

## 最终结论（2026-06-28）

| 检查项 | TECL/XLK（研究） | TQQQ live proxy | SOXL live proxy |
| --- | ---: | ---: | ---: |
| 2024+ CAGR | 24.8% | — | 172.0% |
| 2024+ 最大回撤 | -46.0% | — | -34.2% |
| 合成 25 年全样本 CAGR | 17.0% | — | — |
| 合成 25 年全样本最大回撤 | -58.9% | — | — |

**决策：** 保持 `catalog.status=research_enabled`；不加入
`get_runtime_enabled_profiles()`。收入层保持 `status=research`，
`evidence_status=rejected_vs_live_leveraged`。

主要产物：

- `data/output/tecl_xlk_trend_income_research_20260628/`
- `data/output/tecl_xlk_synthetic_long_history_20260628/`
- `data/output/tecl_xlk_stress_comparison_20260628.csv`
- `docs/tecl-xlk-optimization-research.md`

## 目标

评估能否将 SOXL/SOXX 式 tiered blend 移植到科技杠杆（`TECL` / `XLK`），配合
BOXX 停车、基于 XLK 的动态波动率 delever，以及 TECL 专属 retention profile
（`tecl_step_rebound_0.25_0.50`、`tecl_step_softzero_rebound_0.25_0.50`）。

## 实现

- 策略：`us_equity_strategies.strategies.tecl_xlk_trend_income`
- 回测：`us_equity_snapshot_pipelines.tecl_xlk_trend_income_backtest`
- 管理标的：`TECL`、`XLK`、`BOXX`、`SCHD`、`DGRO`、`SGOV`、`SPYI`、`QQQI`
- 趋势门控源：`XLK`
- 默认研究回测窗口：`2024-01-30` 至最新可用日期（受 BOXX 上市时间约束，见限制）
- 基线 smoke 关闭收入层
- 换手成本：5 bps

示例：

```bash
cd UsEquitySnapshotPipelines
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python -m us_equity_snapshot_pipelines.tecl_xlk_trend_income_backtest \
  --download \
  --output-dir data/output/tecl_xlk_trend_income_research_20260628 \
  --disable-income-layer
```

产物目录：`data/output/tecl_xlk_trend_income_research_20260628`

输出：`summary.csv`、`signal_history.csv`、`weights_history.csv`、
`trades.csv`、`portfolio_returns.csv`、`price_history.csv`、`backtest_config.json`

## 基线参数（SOXL 式种子）

自当前 SOXL/SOXX 研究默认复制，**不修改**生产 TQQQ / SOXL runtime 配置：

- `trend_ma_window`：140
- `trend_entry_buffer` / `trend_mid_buffer` / `trend_exit_buffer`：0.08 / 0.06 / 0.02
- `blend_gate_tecl_weight` / `blend_gate_active_xlk_weight`：tiered blend（见策略模块）
- XLK 动态波动率 delever：10 日年化 realized vol 的 252 日滚动 p95，floor/cap 50%–75%
- 默认 retention policy：`tecl_step_rebound_0.25_0.50`（插件产出 profile）

## Promotion Gate

按研究计划，候选方案不得在全样本与关键压力窗口上**同时**牺牲 CAGR 与最大回撤。
与 `soxl_soxx_trend_income` 在相同日历窗口、关闭收入层、相同换手假设下对比。

## 结果（2026-06-28）

以下均使用 yfinance 收盘价、5 bps 换手、关闭收入层。SOXL 基线使用相同下载窗口与标志。

| 窗口 | 策略 | CAGR | 最大回撤 | 换手/年 | 备注 |
| --- | --- | ---: | ---: | ---: | --- |
| 2024-01-31 至 2026-06-25 | TECL/XLK | 24.77% | -46.04% | 10.55 | 4 次 TECL delever |
| 2024-01-31 至 2026-06-25 | SOXL/SOXX | 172.03% | -34.24% | 11.20 | 20 次 SOXL delever |
| 2023-07-26 至 2026-06-25（BOXX 可用期） | TECL/XLK | 17.05% | -46.04% | 10.53 | |
| 2023-07-26 至 2026-06-25 | SOXL/SOXX | 114.32% | -35.98% | 11.40 | |
| 2025-06-03 至 2026-06-25（近约 1 年） | TECL/XLK | 113.20% | -21.70% | 15.81 | |
| 2025-06-03 至 2026-06-25 | SOXL/SOXX | 513.98% | -34.24% | 16.47 | |
| 2026-03-03 至 2026-06-25（近约 3 月） | TECL/XLK | 229.26% | -21.51% | 18.10 | |
| 2026-03-03 至 2026-06-25 | SOXL/SOXX | 2197.17% | -22.16% | 36.53 | |

### Gate 判定

**未通过 promotion gate。** 各测试窗口 TECL/XLK 的 CAGR 均低于 SOXL/SOXX。全样本与
2024+ 窗口最大回撤更差；仅近 3 月切片回撤略好（-21.51% vs -22.16%），但 CAGR 仍明显落后。

建议：保持 `tecl_xlk_trend_income` 为 `research_enabled`。在考虑 runtime 推广前，
可继续做有界参数扫描（`trend_ma_window`、entry/mid/exit buffer、TECL/XLK 权重、
XLK vol 窗口/分位/floor/cap、retention ratio/redirect）。

## 限制

- **BOXX 上市时间**：BOXX 历史约从 2022 年底开始。若回测在 BOXX 停车，若无 BIL
  或现金 proxy 回填，无法早于 BOXX（及收入层标的）可用日启动。2023 以前的长历史
  压力测试需单独合成回放（基于 XLK 的 3x TECL 腿，产物中须明确标注 synthetic）。
- **2020 COVID / 2022 加息熊市窗口**：默认下载自 2023-01-01，本 artifact 未覆盖。
  可使用已提交的长历史输入，或 `scripts/research_volatility_delever_retention_policies.py`
  式合成输入。
- **行业暴露差异**：TECL 为科技集中；SOXL 为半导体。SOXL 式参数仅为种子，非可迁移最优。
- **插件证据**：AI/OSINT/通知文本仅作人工复核，不得自动提高 retention。

## 测试

```bash
# UsEquityStrategies
cd UsEquityStrategies
PYTHONPATH=src python -m pytest tests/test_volatility_delever_retention.py -q

# QuantStrategyPlugins
cd QuantStrategyPlugins
PYTHONPATH=src python -m pytest tests/test_market_regime_control_plugin.py tests/test_volatility_delever_price_rebound.py -q

# UsEquitySnapshotPipelines
cd UsEquitySnapshotPipelines
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python -m pytest tests/test_tecl_xlk_trend_income_backtest.py -q
```

以上测试于 2026-06-28 通过。

## 后续步骤（初版计划）

1. 在 BOXX 可用窗口扫描 `trend_ma_window` 与 tiered blend 权重。
2. 测试 XLK vol delever redirect 至 `XLK` 而非 `BOXX`（对标 SOXL→SOXX）。
3. 构建 XLK 合成 TECL 长历史 artifact，做 25 年式 gate 检查。
4. 仅当候选在全样本与 COVID/加息熊市/post-2022/近期窗口上 CAGR 不劣于 SOXL、
   且回撤不更差时，再复审 promotion。

## 跟进研究（2026-06-28）

### 新增工具

- `tecl_xlk_trend_income_research_inputs.py`：BIL→BOXX 停车 proxy，可选 XLK→TECL 合成腿（仅研究，manifest 中显式标注）。
- `scripts/build_tecl_xlk_long_history_inputs.py`：自 2018 下载，BIL 回填 BOXX 前历史。
- `scripts/sweep_tecl_xlk_trend_income.py`：110 组有界核心扫描（`trend_ma_window`、趋势 buffer、TECL 权重、vol 分位、redirect）。

命令：

```bash
cd UsEquitySnapshotPipelines
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_tecl_xlk_long_history_inputs.py \
  --output-dir data/output/tecl_xlk_long_history_inputs_20260628

PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/sweep_tecl_xlk_trend_income.py \
  --prices data/output/tecl_xlk_trend_income_research_20260628/price_history.csv \
  --output-dir data/output/tecl_xlk_trend_income_sweep_20260628 \
  --start 2024-01-30
```

产物：

- `data/output/tecl_xlk_long_history_inputs_20260628/`（BIL 填充 BOXX，真实 TECL/XLK）
- `data/output/tecl_xlk_trend_income_sweep_20260628/tecl_core_sweep_ranked.csv`
- `data/output/tecl_xlk_long_history_window_smoke_20260628.csv`

### 2024+ 有界扫描（110 变体）

基线：CAGR 24.77%，最大回撤 -46.04%，4 次 TECL delever。

| 排名 | 变体 | CAGR | 最大回撤 | 相对基线 dual gate |
| --- | --- | ---: | ---: | --- |
| 1 | `vol_off` | 24.80% | -46.04% | 通过（CAGR +0.04 pp） |
| 2 | `manifest_default` | 24.77% | -46.04% | 通过 |
| 3+ | `ma*_tw0.75_xlk_p90/p95` | 25.28% | -48.71% | 失败（CAGR 略升、回撤恶化） |

110 组中 `dual_gate_pass_count = 2`。2024+ 窗口无参数化变体在**同时**改善 CAGR 与回撤。
TECL 权重低于 0.75 时，redirect `XLK` vs `BOXX` 结果相同；0.75 权重下更紧 buffer 提高换手与回撤。

### 长历史窗口 smoke（2018+，BIL BOXX proxy）

| 变体 | 2018+ 全样本 CAGR | 全样本 MDD | COVID 2020 MDD | 2022 加息 MDD | 2024+ CAGR |
| --- | ---: | ---: | ---: | ---: | ---: |
| manifest_default | 37.11% | -46.04% | -30.89% | -27.33% | 23.84% |
| vol_off | 33.34% | -46.04% | -41.81% | -27.33% | 23.87% |

关闭 XLK 波动率 delever（`vol_off`）仅给 2024+ 带来可忽略 CAGR 提升，但显著恶化
COVID 窗口回撤并降低全样本 CAGR。**保留 manifest 默认（vol delever 开启，redirect XLK）。**

### 长历史有界扫描（2018+，BIL BOXX proxy）

`data/output/tecl_xlk_long_history_sweep_20260628/`

| 指标 | manifest_default | 最优扫描（`ma100_b0.06_0.04_0.02_tw0.75_boxx_p90`） |
| --- | ---: | ---: |
| 2018+ 全样本 CAGR | 37.11% | 39.63% |
| 全样本最大回撤 | -46.04% | -46.03% |
| COVID 2020 最大回撤 | -30.89% | -33.26% |
| 2022 加息最大回撤 | -27.33% | -39.76% |

长窗口上 31/110 变体通过简单 dual gate，多为更紧 entry/mid buffer（0.06/0.04）与
TECL 权重 0.75。最优长样本 CAGR 以 COVID/2022 压力回撤恶化为代价。**在未于相同窗口
跑赢 SOXL 且通过原 runtime gate 前，不推广这些参数。**

### 更新建议（第二轮）

- 保持 `research_enabled`，不提升为 `runtime_enabled`。
- 不因本轮扫描修改 catalog 中 SOXL 式种子默认。
- 下一有界实验：在 vol delever 开启下降低 TECL 进攻权重（0.55–0.65），或科技板块
  专属 RSI/Bollinger 阈值——不再扩大宽网格。
- 25 年合成 gate：使用 `build_tecl_xlk_long_history_inputs.py --synthesize-tecl-from-xlk`，
  产物标注 `synthetic_tecl_from_xlk` 后再与 TQQQ/SOXL 长历史 retention 脚本对比。

## 跟进研究第三轮（2026-06-28）

### 合成长历史（XLK → TECL 3x，已标注）

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_tecl_xlk_long_history_inputs.py \
  --start 1999-12-01 \
  --synthesize-tecl-from-xlk \
  --output-dir data/output/tecl_xlk_synthetic_long_history_20260628
```

产物：`data/output/tecl_xlk_synthetic_long_history_20260628/`  
`inputs_mode=synthetic_tecl_from_xlk`。BOXX 使用 BIL proxy，并在 BIL 上市前做
flat-cash 回填（仅研究）。

### 窄扫描 TECL 权重（`--mode narrow`，14 变体）

TECL 权重 0.55 / 0.60 / 0.65，vol delever 与 manifest 类似（redirect XLK，p95）。

**2024+ 窗口**（`data/output/tecl_xlk_narrow_sweep_20260628/`）

| 变体 | CAGR | 最大回撤 | 相对默认 |
| --- | ---: | ---: | --- |
| manifest_default | 24.77% | -46.04% | 基线 |
| tw0.65 | 24.16% | -43.28% | -0.6 pp CAGR，+2.8 pp 回撤 |
| tw0.60 | 23.44% | -40.42% | -1.3 pp CAGR，+5.6 pp 回撤 |
| tw0.55 | 22.64% | -37.47% | -2.1 pp CAGR，+8.6 pp 回撤 |

降低 TECL 进攻权重单调改善回撤、牺牲 CAGR。无窄扫描变体**同时**优于默认 CAGR 与回撤；
dual-gate 通过数仍为 2（`manifest_default` 与 `vol_off`）。

**合成 25 年窗口**（`data/output/tecl_xlk_stress_comparison_20260628.csv`）

| 策略 | 全样本 CAGR | 全样本 MDD | 互联网泡沫 MDD | 金融危机 MDD | COVID MDD | 2022 MDD | 2024+ CAGR |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TECL 默认（合成） | 17.0% | -58.9% | -34.5% | -26.6% | -30.7% | -27.3% | 30.3% |
| TECL tw0.65 | 16.1% | -56.4% | -33.4% | -25.3% | -28.9% | -25.8% | 29.2% |
| TECL tw0.55 | 14.3% | -51.2% | -31.2% | -22.7% | -25.2% | -22.8% | 26.8% |
| SOXL 默认（真实，2024+） | 172.0% | -34.2% | n/a | n/a | n/a | n/a | 172.0% |

合成 TECL 在 dotcom/GFC 压力下单窗回撤约 25%–35%，但全样本最大回撤仍约 -59%。
重叠 live 窗口上 SOXL 仍在 CAGR 与回撤上占优。

### 第三轮建议

- **不推广**至 `runtime_enabled`；相对 SOXL 的 gate 差距仍然很大。
- **保留 manifest 默认**（`blend_gate_tecl_weight=0.70`），不修改生产参数。
- **可选研究 profile**（仅 catalog，非 runtime）：`tecl_defensive_tw0.55`，面向
  回撤敏感卫星研究——约牺牲 2 pp CAGR 换取 2024+ 约 9 pp 更好回撤；若推进需单独 catalog 条目。
- **保持 vol delever 开启**；`vol_off` 仍损害合成长样本 CAGR，且不改善最大回撤。
- 下一有界步骤：在固定 tw0.65 下试科技板块 RSI/Bollinger 阈值，不再扩展权重网格。

**暂无进一步 promotion 计划。** 仅当新的有界假设在相同窗口上以可复现产物同时跑赢
live TQQQ 与 SOXL 时，再重新打开评审。
