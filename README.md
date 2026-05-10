# UsEquitySnapshotPipelines

[English](#english) | [中文](#中文)

---

<a id="english"></a>
## English

`UsEquitySnapshotPipelines` is the upstream feature-snapshot and release pipeline repo for US equity strategies.
It is intentionally separate from broker execution repos.

## Boundary

This repo owns:

- universe and price-input preparation for snapshot-backed US equity strategies
- feature snapshot generation
- candidate ranking / target preview artifacts
- snapshot manifest and release status summary generation
- optional GCS publishing helpers

This repo does not own:

- broker API access
- account / position reconciliation
- order placement
- Telegram runtime notifications
- Cloud Run service configuration

Downstream platforms (`InteractiveBrokersPlatform`, `LongBridgePlatform`, `CharlesSchwabPlatform`) should keep consuming only the published artifact contract.

## Current migrated profile

| Profile | Status | Scheduled artifact cadence | Notes |
| --- | --- | --- | --- |
| `tech_communication_pullback_enhancement` | migrated upstream pipeline | monthly | snapshot builder, ranking, release summary, publish flow live here |
| `russell_1000_multi_factor_defensive` | migrated upstream pipeline | monthly | source-input refresh, snapshot builder, backtest CLI, ranking, release summary, publish flow live here |
| `mega_cap_leader_rotation_top50_balanced` | migrated upstream pipeline | monthly scheduled + manual publish | snapshot builder, ranking, release summary, and publish flow for the balanced Top50 live profile |

This table describes artifact publishing cadence only. Strategy-level cadence remains documented in `UsEquityStrategies`; broker execution schedules should follow that strategy-layer source.

## Local smoke command

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src python -m pytest -q
```

Build a tech/communication pullback snapshot:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_tech_communication_pullback_snapshot.py \
  --prices /path/to/price_history.csv \
  --universe /path/to/universe.csv \
  --as-of 2026-04-01 \
  --output-dir data/output/tech_communication_pullback_enhancement
```

The command writes:

- `tech_communication_pullback_enhancement_feature_snapshot_latest.csv`
- `tech_communication_pullback_enhancement_feature_snapshot_latest.csv.manifest.json`
- `tech_communication_pullback_enhancement_ranking_latest.csv`
- `release_status_summary.json`

See `docs/operator_runbook.md` and `docs/operator_runbook.zh-CN.md` for the manual GitHub Actions publish flow.
The scheduled workflows run monthly: first they refresh the shared Russell 1000 input data, including the latest weighted holdings snapshot used by the mega-cap Top50 profile, then they build and publish the scheduled snapshot profiles from those refreshed inputs.

## Monthly AI Review

The first-stage monthly review control plane is reporting-only:

- `monthly_review.yml` runs after a successful `Publish Snapshot Artifacts` workflow or by manual dispatch.
- It downloads the publish run artifacts, builds `data/output/monthly_report_bundle/`, and creates or updates a `monthly-review` issue.
- `ai_review.yml` reviews that issue and posts a bilingual artifact/contract-health comment.
- It also creates a separate `codex-bridge` remediation issue for the VPS `ccbot-bridge` / Codex runner.
- Codex remediation PRs are merged only by `auto_merge_codex_pr.yml` when CI is green, the PR is not draft, the `auto-merge-ok` label is present, and changed files stay inside the low-risk review/reporting surface.
- If a Codex remediation PR fails CI or receives a changes-requested review, `codex_pr_feedback.yml` comments back on the source `codex-bridge` issue so ccbot can dispatch Codex to fix the same PR branch. It allows up to three automatic feedback rounds, then removes `codex-bridge` so the issue waits for human review.

This keeps US equity snapshot review aligned with the broader monthly audit control plane while keeping code changes and merging in separate, auditable steps.

Prepare / refresh shared Russell 1000 source inputs:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/update_russell_1000_input_data.py \
  --output-dir data/input/refreshed/r1000_official_monthly_v2_alias \
  --universe-start 2018-01-01 \
  --price-start 2018-01-01 \
  --extra-symbols QQQ,SPY,BOXX
```

The source-input refresh writes:

- `r1000_price_history.csv`
- `r1000_universe_history.csv`
- `r1000_symbol_aliases.csv`
- `r1000_universe_snapshot_metadata.csv`
- `r1000_latest_holdings_snapshot.csv` for scheduled mega-cap Top50 ranking

Build a Russell 1000 snapshot:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_russell_1000_feature_snapshot.py \
  --prices /path/to/r1000_price_history.csv \
  --universe /path/to/r1000_universe_history.csv \
  --as-of 2026-04-01 \
  --output-dir data/output/russell_1000_multi_factor_defensive
```


Build the balanced Top50 profile:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_mega_cap_leader_rotation_top50_balanced_snapshot.py \
  --prices /path/to/r1000_price_history.csv \
  --universe /path/to/r1000_latest_holdings_snapshot.csv \
  --as-of 2026-04-01 \
  --dynamic-universe-size 50 \
  --output-dir data/output/mega_cap_leader_rotation_top50_balanced
```

This writes a separate `mega_cap_leader_rotation_top50_balanced` contract. The
runtime profile applies the fixed 50% Top2 cap50 + 50% Top4 cap25 sleeve blend.

Backtest Russell 1000 from the same input files:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_russell_1000_multi_factor_defensive.py \
  --prices /path/to/r1000_price_history.csv \
  --universe /path/to/r1000_universe_history.csv \
  --start 2019-01-01 \
  --output-dir data/output/russell_1000_multi_factor_defensive_backtest
```

## Research-only backtests

Static `mega_cap_leader_rotation` pools remain research-only. The runtime path now publishes only
`mega_cap_leader_rotation_top50_balanced` for this family.

Run the SOXL/SOXX trend-income research backtest with local prices:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python -m us_equity_snapshot_pipelines.soxl_soxx_trend_income_backtest \
  --prices data/output/soxl_soxx_trend_income_archive_2026-05-04/price_history.csv \
  --start 2024-01-30 \
  --turnover-cost-bps 5 \
  --output-dir data/output/soxl_soxx_trend_income_research
```

The SOXL/SOXX research backtest builds the same SOXX RSI and Bollinger-band
inputs consumed by the runtime strategy. To test a regime-aware overheat gate,
add `--dynamic-rsi-quantile-window 252 --dynamic-rsi-quantile 0.90
--dynamic-rsi-floor 70`. This models `max(70, rolling 252d RSI 90th
percentile)` while keeping the production manifest unchanged.

To research a Chandelier-style SOXL delever overlay, add
`--enable-chandelier-stop --chandelier-stop-symbol SOXX --chandelier-window 22
--chandelier-atr-multiple 3`. This is disabled by default and only reroutes the
research backtest's SOXL target value into BOXX for triggered days; it does not
change the production strategy manifest. See
`docs/soxl-soxx-chandelier-stop-research.md` for the initial read.

---

<a id="中文"></a>
## 中文

`UsEquitySnapshotPipelines` 是 US equity 策略的上游特征快照和发布流水线仓库。
它和券商执行仓库保持分离。

## 边界

这个仓库负责：

- 为快照型 US equity 策略准备 universe 和价格输入
- 生成 feature snapshot
- 生成候选排名和 target preview artifacts
- 生成 snapshot manifest 和 release status summary
- 提供可选的 GCS 发布辅助逻辑

这个仓库不负责：

- 券商 API 接入
- 账户 / 持仓对账
- 下单
- Telegram 运行时通知
- Cloud Run service 配置

下游平台仓库（`InteractiveBrokersPlatform`、`LongBridgePlatform`、`CharlesSchwabPlatform`）应只消费已发布的 artifact contract。

## 当前已迁移 profile

| Profile | 状态 | Artifact 计划频率 | 说明 |
| --- | --- | --- | --- |
| `tech_communication_pullback_enhancement` | 已迁移到上游 pipeline | monthly | snapshot builder、ranking、release summary 和 publish flow 在本仓库 |
| `russell_1000_multi_factor_defensive` | 已迁移到上游 pipeline | monthly | source-input refresh、snapshot builder、backtest CLI、ranking、release summary 和 publish flow 在本仓库 |
| `mega_cap_leader_rotation_top50_balanced` | 已迁移到上游 pipeline | monthly scheduled + manual publish | balanced Top50 live profile 的 snapshot builder、ranking、release summary 和 publish flow 在本仓库 |

这个表只描述 artifact 发布频率。策略层频率仍以 `UsEquityStrategies` 为准；券商执行计划应跟随策略层来源。

## 本地 smoke 命令

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src python -m pytest -q
```

构建 tech/communication pullback snapshot：

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_tech_communication_pullback_snapshot.py \
  --prices /path/to/price_history.csv \
  --universe /path/to/universe.csv \
  --as-of 2026-04-01 \
  --output-dir data/output/tech_communication_pullback_enhancement
```

该命令会写出：

- `tech_communication_pullback_enhancement_feature_snapshot_latest.csv`
- `tech_communication_pullback_enhancement_feature_snapshot_latest.csv.manifest.json`
- `tech_communication_pullback_enhancement_ranking_latest.csv`
- `release_status_summary.json`

手动 GitHub Actions 发布流程见 `docs/operator_runbook.md` 和 `docs/operator_runbook.zh-CN.md`。
计划任务每月运行：先刷新共享 Russell 1000 输入数据，包括 mega-cap Top50 profile 使用的最新加权持仓快照，然后构建并发布计划内的 snapshot profiles。

## 月度 AI Review

第一阶段月度 review 控制面只做报告，不直接修改代码：

- `monthly_review.yml` 在 `Publish Snapshot Artifacts` workflow 成功后运行，也支持手工触发。
- 它下载 publish run artifacts，构建 `data/output/monthly_report_bundle/`，并创建或更新 `monthly-review` issue。
- `ai_review.yml` review 该 issue，并发布双语 artifact/contract-health 评论。
- 它也会为 VPS `ccbot-bridge` / Codex runner 创建单独的 `codex-bridge` remediation issue。
- Codex remediation PR 只有在 CI 通过、PR 非 draft、带有 `auto-merge-ok` label，且变更限制在低风险 review/reporting surface 内时，才由 `auto_merge_codex_pr.yml` 合并。
- 如果 Codex remediation PR 的 CI 失败或收到 changes-requested review，`codex_pr_feedback.yml` 会回评源 `codex-bridge` issue，让 ccbot 派发 Codex 修同一条 PR 分支。最多允许三轮自动反馈，然后移除 `codex-bridge`，等待人工 review。

这个流程让 US equity snapshot review 与更大的月度审计控制面对齐，同时把代码修改和合并保留在独立、可审计的步骤里。

## 研究和回测

静态 `mega_cap_leader_rotation` 池仍然只是 research-only。运行时路径目前只发布该系列的 `mega_cap_leader_rotation_top50_balanced`。

SOXL/SOXX trend-income 的 research backtest 可以使用本地价格数据运行。默认研究路径会构建 runtime 策略使用的 SOXX RSI 和 Bollinger-band 输入；也可以通过动态 RSI 分位、Chandelier-style delever overlay 等参数研究替代保护机制。Chandelier overlay 默认关闭，只在研究回测中把触发日的 SOXL target value reroute 到 BOXX，不改变生产 manifest。初始研究说明见 `docs/soxl-soxx-chandelier-stop-research.md`。

To research alternative SOXL delever gates without changing production, use
`--soxl-delever-overlay volatility|drawdown|momentum` with
`--soxl-delever-symbol`, `--soxl-delever-window`,
`--soxl-delever-threshold`, `--soxl-delever-retention-ratio`, and
`--soxl-delever-redirect-symbol`. The current best research candidate is a
SOXX 20-day volatility gate at `0.50` that redirects SOXL into SOXX. See
`docs/tqqq-soxl-optimization-research.md` for the TQQQ/SOXL no-regression
optimization sweep.

For long-history core SOXL/SOXX validation, provide a BOXX-compatible cash
proxy such as BIL under the `BOXX` symbol and add `--disable-income-layer`.
That avoids QQQI/SPYI inception dates truncating the 2010+ SOXL sample.

To create replayable SOXL/SOXX archives, use the archive runner instead of
one-off research commands. It writes `price_history.csv`, `summary.csv`,
portfolio/trade/signal outputs, `backtest_config.json`,
`data_quality_report.csv`, and `source_manifest.json` with file hashes and
redacted source metadata.

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python -m us_equity_snapshot_pipelines.soxl_soxx_trend_income_archive \
  --mode live-full \
  --download \
  --archive-date 2026-05-08
```

For long-history core validation, use the built-in `BOXX=BIL` download alias
and disabled income layer:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python -m us_equity_snapshot_pipelines.soxl_soxx_trend_income_archive \
  --mode core-long \
  --download \
  --archive-date 2026-05-08
```

If Yahoo rate-limits downloads, set `YFINANCE_PROXY` in the shell or pass
`--proxy`; proxy values are intentionally redacted from `source_manifest.json`.

Run the first-pass mega-cap leader rotation backtest with local input files:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_mega_cap_leader_rotation.py \
  --prices /path/to/mega_cap_price_history.csv \
  --universe /path/to/mega_cap_universe.csv \
  --pool expanded \
  --start 2016-01-01 \
  --turnover-cost-bps 5 \
  --output-dir data/output/mega_cap_leader_rotation_backtest
```

Or let the research CLI download the static pool through yfinance:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_mega_cap_leader_rotation.py \
  --download \
  --pool expanded \
  --price-start 2015-01-01 \
  --start 2016-01-01 \
  --turnover-cost-bps 5 \
  --output-dir data/output/mega_cap_leader_rotation_backtest
```

The command writes `summary.csv`, `portfolio_returns.csv`,
`weights_history.csv`, `turnover_history.csv`, `candidate_scores.csv`,
`trades.csv`, `exposure_history.csv`, and `reference_returns.csv`.

To reduce today's-winners look-ahead bias, run the historical dynamic mega-cap
variant. It downloads monthly iShares Russell 1000 holdings snapshots and uses
the top fund-weight names available at each rebalance. The documented start
uses the earliest monthly iShares JSON snapshot range that resolved reliably in
research. Known duplicate share classes such as `GOOG` / `GOOGL` are collapsed
to one issuer before taking the top-N universe:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_mega_cap_leader_rotation.py \
  --download \
  --dynamic-universe \
  --universe-start 2017-09-01 \
  --price-start 2015-01-01 \
  --start 2017-10-01 \
  --mega-universe-size 20 \
  --top-n 4 \
  --single-name-cap 0.25 \
  --turnover-cost-bps 5 \
  --output-dir data/output/mega_cap_leader_rotation_dynamic_universe_top20_backtest
```

For small paper/live accounts, add `--portfolio-total-equity` and
`--min-position-value-usd` to let the research backtest reduce the effective
top-N when the configured account size cannot support the requested number of
minimum-sized stock positions. The per-rebalance effective count is written to
`exposure_history.csv`.

Run the default robustness matrix across `mag7` / `expanded`, top 3 / 4 / 5,
single-name caps 25% / 30% / 35%, and defense on / off:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_mega_cap_leader_rotation_robustness.py \
  --prices data/output/mega_cap_leader_rotation_backtest/input/mega_cap_leader_rotation_expanded_price_history.csv \
  --output-dir data/output/mega_cap_leader_rotation_robustness
```

Or download the union research pool and run the matrix in one step:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_mega_cap_leader_rotation_robustness.py \
  --download \
  --price-start 2015-01-01 \
  --start 2016-01-01 \
  --turnover-cost-bps 5 \
  --output-dir data/output/mega_cap_leader_rotation_robustness
```

The robustness command writes `robustness_summary.csv` sorted by Sharpe, CAGR,
drawdown, and turnover, plus `robustness_summary_by_run.csv` in raw run order.

Validate a dynamic Russell Top50 universe candidate against point-in-time
availability lags and yearly stability before treating high Top2 results as
real:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/validate_mega_cap_leader_rotation_dynamic_universe.py \
  --prices data/output/mega_cap_leader_rotation_dynamic_top50_validation_price_refresh/input/mega_cap_leader_rotation_expanded_price_history.csv \
  --universe data/output/mega_cap_leader_rotation_dynamic_universe_top50_backtest/input/mega_cap_leader_rotation_dynamic_top50_universe_history.csv \
  --start 2018-01-31 \
  --end 2026-04-10 \
  --universe-lag-days 0,1,5,21 \
  --strategy-configs top2_cap50:2:0.50,top3_cap35:3:0.35 \
  --risk-on-exposure 1.0 \
  --soft-defense-exposure 1.0 \
  --hard-defense-exposure 1.0 \
  --turnover-cost-bps 5 \
  --output-dir data/output/mega_cap_leader_rotation_dynamic_top50_validation
```

The validation command writes `validation_summary.csv` and
`yearly_validation_summary.csv`; it also writes `rolling_window_summary.csv`
when `--rolling-window-years` is provided. See
`docs/mega-cap-leader-rotation-dynamic-validation.md` for the current
research-only conclusion.

For pre-2017 history where official IWB/Russell 1000 holdings are unavailable,
build an explicitly labeled proxy universe from point-in-time long-history
prices. Prefer inputs with `market_value` or `shares_outstanding`; if those are
absent the command falls back to an ADV20 dollar-volume proxy and marks the
output as lower-confidence research:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/research_russell_1000_proxy_long_history.py \
  --prices /path/to/long_history_us_equity_prices.csv \
  --start 2000-01-31 \
  --universe-size 1000 \
  --strategy-configs top2_cap50:2:0.50,top3_cap35:3:0.35 \
  --risk-modes no_defense:1:1:1 \
  --rolling-window-years 3,5 \
  --turnover-cost-bps 5 \
  --output-dir data/output/russell_1000_proxy_long_history
```

The proxy command writes `russell_1000_proxy_universe_history.csv`,
`russell_1000_proxy_metadata.csv`, and, unless `--skip-validation` is set, the
same validation summary files as the dynamic Top50 validator. Treat these
outputs as a survivorship-bias and data-quality audit surface, not official
Russell 1000 history.

Run the longest currently available point-in-time Top50 validation with 3-year
and 5-year rolling windows:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/validate_mega_cap_leader_rotation_dynamic_universe.py \
  --prices data/output/mega_cap_leader_rotation_dynamic_top50_validation_price_refresh/input/mega_cap_leader_rotation_expanded_price_history.csv \
  --universe data/output/mega_cap_leader_rotation_dynamic_universe_top50_backtest/input/mega_cap_leader_rotation_dynamic_top50_universe_history.csv \
  --start 2017-10-02 \
  --end 2026-04-16 \
  --universe-lag-days 21 \
  --strategy-configs top2_cap50:2:0.50,top3_cap35:3:0.35,top4_cap25:4:0.25 \
  --risk-modes no_defense:1:1:1 \
  --max-names-per-sector-values 0,2 \
  --rolling-window-years 3,5 \
  --turnover-cost-bps 5 \
  --output-dir data/output/mega_cap_leader_rotation_dynamic_top50_long_cycle_validation
```

Run the research-only Top2 / Top4 concentration variants, including fixed
dual-sleeve blends and Top2 shadow drawdown switches:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/research_mega_cap_leader_rotation_concentration_variants.py \
  --prices data/output/mega_cap_leader_rotation_dynamic_top50_validation_price_refresh/input/mega_cap_leader_rotation_expanded_price_history.csv \
  --universe data/output/mega_cap_leader_rotation_dynamic_universe_top50_backtest/input/mega_cap_leader_rotation_dynamic_top50_universe_history.csv \
  --start 2017-10-02 \
  --end 2026-04-16 \
  --universe-lag-days 21 \
  --blend-top2-weights 0.25,0.50,0.75 \
  --dynamic-drawdown-thresholds 0.08,0.10,0.12 \
  --rolling-window-years 3,5 \
  --turnover-cost-bps 5 \
  --output-dir data/output/mega_cap_leader_rotation_dynamic_top50_concentration_variants
```

Run the research-only rebalance-frequency and daily-risk variants for the
balanced Top2 / Top4 sleeve:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/research_mega_cap_leader_rotation_frequency_risk.py \
  --prices data/output/mega_cap_leader_rotation_dynamic_top50_validation_price_refresh/input/mega_cap_leader_rotation_expanded_price_history.csv \
  --universe data/output/mega_cap_leader_rotation_dynamic_universe_top50_backtest/input/mega_cap_leader_rotation_dynamic_top50_universe_history.csv \
  --start 2017-10-02 \
  --end 2026-04-16 \
  --universe-lag-days 21 \
  --rebalance-frequencies monthly,biweekly,weekly \
  --daily-risk-modes none,hard_cash,partial_cash \
  --blend-top2-weight 0.50 \
  --rolling-window-years 3,5 \
  --turnover-cost-bps 5 \
  --output-dir data/output/mega_cap_leader_rotation_dynamic_top50_frequency_risk
```

The same validation CLI can run a broader lagged stability grid across Top2 /
Top3 / Top4, QQQ-defense settings, and sector concentration caps:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/validate_mega_cap_leader_rotation_dynamic_universe.py \
  --prices data/output/mega_cap_leader_rotation_dynamic_top50_validation_price_refresh/input/mega_cap_leader_rotation_expanded_price_history.csv \
  --universe data/output/mega_cap_leader_rotation_dynamic_universe_top50_backtest/input/mega_cap_leader_rotation_dynamic_top50_universe_history.csv \
  --start 2018-01-31 \
  --end 2026-04-10 \
  --universe-lag-days 1,21 \
  --strategy-configs top2_cap40:2:0.40,top2_cap50:2:0.50,top3_cap30:3:0.30,top3_cap35:3:0.35,top4_cap25:4:0.25 \
  --risk-modes no_defense:1:1:1,partial_defense:1:0.5:0.2,cash_defense:1:0:0 \
  --max-names-per-sector-values 0,1,2 \
  --turnover-cost-bps 5 \
  --output-dir data/output/mega_cap_leader_rotation_dynamic_top50_stability_grid
```

Run the research-only MAG7 leveraged pullback / high-trim backtest from an
existing MAG7 price-history file:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_mag7_leveraged_pullback.py \
  --prices data/output/mega_cap_leader_rotation_mag7_backtest/input/mega_cap_leader_rotation_mag7_price_history.csv \
  --start 2016-01-01 \
  --frequency weekly \
  --top-n 3 \
  --leverage-multiple 2.0 \
  --max-product-exposure 0.8 \
  --soft-product-exposure 0.5 \
  --hard-product-exposure 0.15 \
  --leveraged-expense-rate 0.01 \
  --single-name-cap 0.25 \
  --turnover-cost-bps 5 \
  --output-dir data/output/mag7_leveraged_pullback_backtest
```

This strategy is not a live profile. It buys strong MAG7 names on controlled
pullbacks, trims exposure near short-term highs, caps single-name exposure, and
models the invested sleeve as daily-reset 2x long products rather than margin
borrowing. The command writes
`summary.csv`, `portfolio_returns.csv`, `weights_history.csv`,
`turnover_history.csv`, `candidate_scores.csv`, `trades.csv`,
`exposure_history.csv`, and `reference_returns.csv`.

Run the research-only 2018-to-present Trump/Biden trade-war / TACO-like panic
rebound event study:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_taco_panic_rebound.py \
  --download \
  --event-set full \
  --price-start 2018-01-01 \
  --ranking-horizon 42 \
  --output-dir data/output/taco_panic_rebound_2018_present_research
```

This is an event-window research tool, not a live strategy. It uses a fixed
trade-war event calendar, finds the post-shock trough within the configured
window, then reports 5 / 10 / 21 / 42 / 63 trading-day rebounds by symbol. Use
`--event-set first-term`, `--event-set biden`, or `--event-set second-term` for
single-period diagnostics. Use `--event-set geopolitical-deescalation` for the
research-only 2026 U.S.-Iran de-escalation / ceasefire bucket, or
`--event-set full-plus-geopolitical-deescalation` to append that bucket to the
default trade-war calendar. The default `full` event set intentionally does not
include the geopolitical bucket. The command writes `event_calendar.csv`,
`event_windows.csv`, `shock_symbol_summary.csv`, and
`softening_symbol_summary.csv`.

Run a research-only TACO panic rebound portfolio sleeve backtest using the full
2018-to-present event calendar and presidential-period summaries:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_taco_panic_rebound_portfolio.py \
  --download \
  --preset steady \
  --event-set full \
  --price-start 2018-01-01 \
  --start 2018-01-01 \
  --account-sleeve-ratio 0.10 \
  --output-dir data/output/taco_panic_rebound_2018_present_portfolio
```

The portfolio backtest models a separate event-rebound sleeve, not a full-account
allocation. `steady` uses a lower-volatility basket (`QLD`, `TQQQ`, `ROM`,
`USD`, `NVDA`, `AMD`); `aggressive` uses `TQQQ`, `TECL`, `SOXL`, `NVDA`, and
`AMD`. The output includes both the standalone sleeve return and a cash-backed
account overlay row for the configured sleeve ratio, plus `period_summary.csv`
for Trump 1, Biden, and Trump 2-to-date.

Run the V1 price-stress TACO overlay comparison against the current
`tqqq_growth_income` fixed dual-drive research baseline:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_taco_panic_rebound_overlay_compare.py \
  --download \
  --event-set full \
  --price-start 2014-01-01 \
  --start 2015-01-01 \
  --overlay-sleeve-ratios 0.05,0.10 \
  --audit-modes crisis_veto \
  --output-dir data/output/tqqq_taco_price_stress_overlay_2015
```

This is the first-version definition for the TQQQ/TACO overlay research:
`QQQ` / `TQQQ` price pressure opens the policy-event scanner, which classifies
trade-war / tariff shock or softening events, and the overlay can use a small
slice of the baseline `BOXX` / cash sleeve to buy `TQQQ`. VIX and macro data are
not used as hard vetoes or position-size reducers in V1.

The explicit geopolitical research buckets are separate from the default live
calendar. `geopolitical-deescalation` only includes de-escalation / ceasefire /
talks events, while `geopolitical-conflict-and-deescalation` also includes the
war-escalation stress events for sensitivity testing. Prefer the
de-escalation-only bucket when studying a potential small TACO sleeve because it
does not try to buy the war headline itself.

The optional dual-review backtest is a deterministic audit proxy, not a
historical replay of live model responses. The event calendar simulates the
proposer rubric, and the auditor can only veto candidates that fall inside
predeclared systemic-crisis windows or event ids supplied with
`--audit-veto-event-ids`. This makes the conflict policy backtestable without
pretending that a model actually ran in 2018/2019. The output writes
`summary.csv`, `deltas_vs_base.csv`, `diagnostics.csv`,
`audit_diagnostics.csv`, `recognized_event_calendar.csv`,
`audit_decisions.csv`, `taco_trades_by_scenario.csv`, and per-strategy return /
weight series.

For a longer black-swan stress sample, use a synthetic daily-reset `TQQQ` proxy
from `QQQ` because real `TQQQ` did not exist during the internet bubble or the
2008 financial crisis. The optional price-only crisis guard is also a
deterministic proxy, not a model replay:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_taco_panic_rebound_overlay_compare.py \
  --download \
  --event-set full \
  --price-start 1999-03-10 \
  --start 1999-03-10 \
  --attack-symbol SYNTH_TQQQ \
  --synthetic-attack-from QQQ \
  --synthetic-attack-multiple 3 \
  --safe-symbol SHY \
  --include-price-crisis-guard \
  --overlay-sleeve-ratios 0.05 \
  --output-dir data/output/tqqq_taco_price_stress_overlay_1999_synthetic
```

Use this long sample to inspect `dotcom_bubble_burst`,
`gfc_peak_to_trough`, and `lost_decade_2000_2009` rows. Treat the synthetic
`TQQQ` and crisis-guard rows as stress-test evidence, not as a perfect
reconstruction of tradable historical products.

Run the standalone Crisis Guard research matrix separately from TACO:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_crisis_regime_guard.py \
  --download \
  --price-start 1999-03-10 \
  --start 1999-03-10 \
  --attack-symbol SYNTH_TQQQ \
  --synthetic-attack-from QQQ \
  --synthetic-attack-multiple 3 \
  --safe-symbol SHY \
  --context-gates none,rubric \
  --drawdown-thresholds=-0.20,-0.25,-0.30 \
  --risk-multipliers 0,0.25,0.5 \
  --confirm-days 5 \
  --output-dir data/output/crisis_regime_guard_1999_synthetic
```

This matrix compares the base TQQQ profile with price-only crisis guard variants
across dot-com, GFC, COVID, 2022, and post-2015 periods. `--context-gates none`
keeps the pure price-only guard. `rubric` enables the deterministic two-step
crisis rubric. The confirmed price crisis signal opens
the context scanner, which classifies the triggered crisis candidate as
bubble-burst risk, financial-crisis risk, or non-systemic bear / policy shock.
The audit rubric can only approve or veto protection. The simulated rubric uses a
bubble proxy (`QQQ` trailing 252-day return above 75%, remembered for 126
trading days by default) plus a financial-stress proxy (`XLF` drawdown and
relative weakness vs `SPY`) when entering protection, then keeps the guard
active until the price crisis signal turns off. This is a deterministic
stand-in for a future live crisis module, not a model-driven backtest.

The output writes `summary.csv`, `deltas_vs_base.csv`,
`guard_diagnostics.csv`, `context_diagnostics.csv`, `guard_events.csv`,
`context_opinions.csv`, and per-strategy return / weight / signal / context series.
`context_opinions.csv` is intentionally sparse: it
records only dates where the confirmed price crisis signal would have opened the
context scanner. The matrix is
for research only and defaults to disabled unless explicitly requested: it is
intended to show the cost of false positives as well as the benefit in
2000/2008-style crises before any crisis module is allowed to affect live
allocations.

See `docs/crisis-response-v1.md` for the frozen V1 contract,
`docs/crisis-response-research-roadmap.md` for post-V1 historical-crash
research, and `docs/crisis-context-research-v2.md` for the research-only
context pack.

Build the V2 crisis context pack before changing any routing logic:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_crisis_context_pack.py \
  --download \
  --event-set full \
  --price-start 1999-03-10 \
  --start 1999-03-10 \
  --output-dir data/output/crisis_context_v2
```

This writes `crisis_context_features.csv` and `context_diagnostics.csv`. If
yfinance is rate limited, pass `--prices` with a saved price-history CSV. If a
legitimate proxy is available, set `YFINANCE_PROXY` or pass `--download-proxy`.
The V2 pack includes research-only COVID exogenous / policy-rescue windows and
tariff shock / softening windows so 2020 and 2018-2019 false positives can be
audited before any context affects live routing. It writes raw financial /
credit context separately from stricter systemic financial-crisis context.

Run the unified Crisis Response research when comparing fake-crisis TACO entries
with true-crisis defense in one historical research report:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_crisis_response.py \
  --download \
  --event-set full \
  --price-start 1999-03-10 \
  --start 1999-03-10 \
  --attack-symbol SYNTH_TQQQ \
  --synthetic-attack-from QQQ \
  --synthetic-attack-multiple 3 \
  --safe-symbol SHY \
  --overlay-sleeve-ratios 0.05 \
  --crisis-drawdown=-0.20 \
  --crisis-risk-multiplier 0.25 \
  --crisis-confirm-days 5 \
  --output-dir data/output/crisis_response_1999_synthetic
```

This unified research treats both modules as one event-response research
surface for audit continuity. It is not the production-facing plugin split. The
price-stress scanner opens the TACO path for trade-war / tariff fake-crisis
events. The confirmed crisis-price signal opens the crisis-context path for
bubble-burst or financial-crisis candidates. If the true-crisis guard is active,
TACO entries are suppressed; otherwise approved policy / tariff shocks can use
the small TACO sleeve. The output includes `response_decisions.csv`, which is
the main audit file for whether each candidate was routed to `taco_fake_crisis`,
`true_crisis`, or `no_action`.

To compare the frozen V1 deterministic rubric with the research-only V2 context pack, add
the explicit mode flag. The default remains `v1_rubric`.

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_crisis_response.py \
  --download \
  --event-set full \
  --price-start 1999-03-10 \
  --start 1999-03-10 \
  --attack-symbol SYNTH_TQQQ \
  --synthetic-attack-from QQQ \
  --synthetic-attack-multiple 3 \
  --safe-symbol SHY \
  --overlay-sleeve-ratios 0.05 \
  --crisis-drawdown=-0.20 \
  --crisis-risk-multiplier 0.25 \
  --crisis-confirm-days 5 \
  --crisis-context-mode v2_context_pack \
  --output-dir data/output/crisis_response_1999_synthetic_v2_context
```

When this mode is used, the output also includes
`crisis_context_features.csv`, and `context_opinions.csv` records the V2 suggested
route behind each confirmed crisis-price trigger.

External valuation context stays audit-only unless explicitly enabled. To test
historical PE/CAPE/earnings-quality context, pass an `as_of` CSV with columns
such as `nasdaq_100_trailing_pe`, then add `--external-context` and
`--external-valuation-mode price_or_external` or `price_and_external`.

Build the Phase 1 log-only Crisis Response shadow signal after the TQQQ daily
price/context refresh:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_crisis_response_shadow_signal.py \
  --prices data/output/crisis_response_shadow/input/price_history.csv \
  --external-context data/output/crisis_response_shadow/input/external_context.csv \
  --event-set full \
  --start 1999-03-10 \
  --output-dir data/output/crisis_response_shadow
```

This writes `latest_signal.json`, dated JSON/CSV signal files, and an evidence
CSV under `data/output/crisis_response_shadow/`. The shadow builder is designed
to run on the same daily cadence as the TQQQ artifact
pipeline, but it is `shadow_only`: no broker writes, no order placement, and no
live allocation mutation. Downstream notifications may read the latest JSON and
display it as an observation beside the TQQQ status, not as an executable trade
instruction. Paper, advisory, and live plugin modes are not part of the current
contract.

The crisis shadow plugin is intentionally defense-only. It does not emit
`taco_fake_crisis`, does not suggest a TACO sleeve, and does not buy event
rebounds. Its executable route is only a true-crisis `defend` signal, with the
defensive destination documented as cash or a money-market / Treasury-bill
parking sleeve. Policy, tariff, or geopolitical panic can still appear as
watch-only context, but TACO routing lives outside this crisis plugin and needs
its own research path.

Build the independent TACO rebound research signal when you need a deterministic
event-rebound audit file. MAGS usage is intentionally research-only for now;
the preferred promoted direction is a separate TQQQ TACO overlay candidate after
its own validation:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_taco_rebound_shadow_signal.py \
  --prices data/output/taco_rebound_shadow/input/price_history.csv \
  --event-set geopolitical-deescalation \
  --start 2026-01-01 \
  --geopolitical-deescalation-sleeve 0.10 \
  --tariff-softening-sleeve 0.05 \
  --max-sleeve 0.10 \
  --output-dir data/output/taco_rebound_research/research_only
```

This TACO research artifact does not select stocks and does not mutate a
strategy artifact. It only writes a small rebound-budget suggestion for
backtests. Conflict escalation events default to a zero sleeve; de-escalation /
ceasefire / talks events can raise a bounded rebound budget when the
price-stress scanner is open.

For platform-style deployment, use the sidecar plugin runner instead of wiring
plugins into a strategy function. The runner reads strategy-scoped plugin
mounts from TOML and executes only explicitly configured plugins. Current
Crisis Response writes artifacts here; any platform execution is downstream.
`taco_rebound_shadow` is deliberately blocked in the runner while MAGS remains
research-only and the TQQQ overlay path has not been promoted.
Use `docs/examples/strategy_plugins.example.toml` as the schema example. Real
runtime TOML should live with the deployment or platform configuration, not as a
committed live config in this repository.

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/run_strategy_plugins.py --config /path/to/strategy_plugins.toml
```

This is intentionally a sidecar. If the runner is disabled or fails, the
underlying strategy artifact build remains independent; downstream systems read
the plugin's `latest_signal.json`, verify its `strategy` / `plugin` identity,
and treat the plugin as notification-only shadow context.
Plugin mounts are strategy-limited by code and tests. Keep the crisis plugin
scoped to defensive TQQQ/SOXL-style risk-off behavior. Keep MAGS/TACO
experiments in research commands until a future PR adds a validated TQQQ TACO
overlay plugin or explicitly promotes another strategy mount.

The only supported plugin mode is `shadow`. The runner rejects `paper`,
`advisory`, and `live` plugin modes; the current product decision is that the
plugin is a notification and review signal, not an automated execution layer.
This repository still writes artifacts only, so the payload separates platform
behavior fields such as `broker_order_allowed=false` from repository capability
fields such as `repository_broker_write_allowed=false`. Keep `default_mode =
"shadow"` as the fallback and omit per-plugin `mode` unless a legacy config
needs to spell out the same value.

Mode meanings:

| Mode | Meaning |
| --- | --- |
| `shadow` | Signal, evidence, logs, and optional notification context only |

The `Publish Strategy Plugins` GitHub workflow runs the
`tqqq_growth_income` / `crisis_response_shadow` plugin in `shadow` mode on a
weekday post-close schedule. Its default GCS prefix is:

```text
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/tqqq_growth_income/plugins/crisis_response_shadow
```

Downstream platforms should mount only the resulting `latest_signal.json` and
must not duplicate `mode` in platform config.
