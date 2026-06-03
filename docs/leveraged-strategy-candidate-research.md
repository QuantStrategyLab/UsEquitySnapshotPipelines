# Leveraged Strategy Candidate Research Gate

This note documents the research-only gate for the current TQQQ/SOXL live proxies, parameter variants of those existing strategies, and a bounded set of genuinely new leveraged supplemental strategies.

`research_gate_passed=true` means the candidate passed the research-only robustness screen. It is **not** a live-enable signal. In this run every candidate has `live_enable_candidate=false` and `replacement_candidate=false`.

## Boundary: optimization is not a new strategy

This run keeps three groups separate:

1. `current_live_proxy`: local proxy for the current live TQQQ/SOXL strategy behavior.
2. `optimization_variant`: parameter or sleeve variants of existing TQQQ/SOXL logic. These are **not** counted as new strategies.
3. `leveraged_supplement`: genuinely new supplemental leveraged sleeves. Only this group is counted when discussing newly added strategies.

## Anti-overfitting rules

- Use fixed, simple rules; do not tune lookback windows or weights from the latest output.
- Keep the new supplemental set bounded to about five candidates per run.
- Use broad, liquid leveraged ETF families with long enough public history for short, medium, and long checks.
- Use one conservative trend rule across the new candidates:
  - signal source closes above its 200-day moving average;
  - 20-day moving average slope is positive;
  - otherwise move to the safe sleeve.
- Apply a simple research gate across all periods:
  - enough trading days in every window;
  - positive CAGR in every window;
  - positive Sharpe in every window;
  - max drawdown above `-45%` in every window;
  - if worst drawdown is around `25%+`, long-window CAGR must beat SPY. Passing this check only allows paper review; it still does not allow live enable.
- Rank by a robustness score using minimum Sharpe, median Sharpe, median excess CAGR versus SPY, worst drawdown, and turnover penalty.

## Candidate groups

### Current live proxies

| Candidate | Rule | Purpose |
| --- | --- | --- |
| `live_tqqq_dual_drive_45_45_proxy` | QQQ MA200/pullback risk gate; risk-on QQQ/TQQQ/BIL 45/45/10 | Proxy for current TQQQ live defaults |
| `live_soxl_soxx_trend_57_proxy` | SOXL above 150d MA holds SOXL/BIL; otherwise SOXX/BIL | Proxy for current SOXL live defaults |

### Optimization variants of existing strategies

These are **not** new strategies.

| Candidate | Rule | Purpose |
| --- | --- | --- |
| `opt_tqqq_dual_drive_40_40` | Same TQQQ live proxy rule, lower QQQ/TQQQ exposure | Lower-risk TQQQ parameter variant |
| `opt_tqqq_qld_tqqq_60_20` | Same TQQQ live proxy rule, QLD/TQQQ/BIL 60/20/20 | TQQQ replacement-review sleeve variant |
| `opt_soxl_soxx_signal_soxx_50` | SOXX trend signal, lower SOXL/SOXX deployment | SOXL replacement-review variant |

### Genuinely new supplemental strategies

This is the bounded new strategy set for this run after removing candidates whose long-window CAGR did not beat SPY.

| New rank | Candidate | Rule | Purpose |
| ---: | --- | --- | --- |
| 1 | `new_qld_qqq_trend_70_20` | QQQ MA200 + MA20-slope; risk-on QLD/QQQ/BIL 70/20/10 | 2x Nasdaq growth sleeve with less path dependency than TQQQ |
| 2 | `new_tecl_xlk_trend_50_30` | XLK MA200 + MA20-slope; risk-on TECL/XLK/BIL 50/30/20 | 3x technology-sector supplemental sleeve |
| 3 | `new_rom_xlk_trend_70_20` | XLK MA200 + MA20-slope; risk-on ROM/XLK/BIL 70/20/10 | 2x technology-sector supplemental sleeve |
| 4 | `new_usd_smh_trend_50_30` | SMH MA200 + MA20-slope; risk-on USD/SMH/BIL 50/30/20 | 2x semiconductor sleeve related to SOXL domain; rejected by drawdown gate |

## Research basis

The external research/input layer was limited to high-level instrument and risk validation, not parameter fitting:

- ProShares describes QLD as seeking 2x daily Nasdaq-100 exposure and warns that longer holding-period returns can differ from the daily target.
- ProShares describes TQQQ as seeking 3x daily Nasdaq-100 exposure, useful as the current live comparison context.
- ProShares lists ROM and USD as leveraged technology/semiconductor products used for the new supplemental universe.
- Direxion describes TECL as seeking 300% daily exposure to the Technology Select Sector Index.
- Direxion describes SOXL as seeking 300% daily exposure to the NYSE Semiconductor Index, and warns that leveraged ETFs should not be expected to deliver the stated multiple over periods longer than a day.

Reference URLs:

- https://www.proshares.com/our-etfs/leveraged-and-inverse/qld
- https://www.proshares.com/our-etfs/leveraged-and-inverse/tqqq
- https://www.proshares.com/our-etfs/leveraged-and-inverse/rom
- https://www.proshares.com/funds/usd.html
- https://www.direxion.com/products/direxion-daily-technology-bull-3x-etf
- https://www.direxion.com/product/daily-semiconductor-bull-bear-3x-etfs

## Local command

From `/Users/lisiyi/Projects/UsEquitySnapshotPipelines`:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_leveraged_strategy_candidates.py \
  --download \
  --price-start 2010-01-01 \
  --periods short:2025-06-01:,medium:2023-06-01:,long:2011-01-01: \
  --turnover-cost-bps 5 \
  --output-dir data/output/leveraged_strategy_candidate_research_20260603
```

Outputs:

- `downloaded_leveraged_price_history.csv`
- `period_summary.csv`
- `ranking.csv`
- `portfolio_returns.csv`

## 2026-06-03 local run summary

Output directory:

`data/output/leveraged_strategy_candidate_research_20260603`

Downloaded price range: `2010-01-04` to `2026-06-02`.

Periods:

- short: `2025-06-01` to latest available date;
- medium: `2023-06-01` to latest available date;
- long: `2011-01-01` to latest available date.

### Overall ranking

| Rank | Candidate | Group | Min Sharpe | Median excess CAGR vs SPY | Long excess CAGR vs SPY | Worst drawdown | Research gate | Decision |
| ---: | --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| 1 | `live_soxl_soxx_trend_57_proxy` | current live proxy | 0.968 | 62.87% | 18.97% | -43.02% | pass | keep current live |
| 2 | `opt_tqqq_qld_tqqq_60_20` | optimization variant | 1.029 | 8.61% | 8.61% | -25.64% | pass | no replacement |
| 3 | `live_tqqq_dual_drive_45_45_proxy` | current live proxy | 1.039 | 8.83% | 8.83% | -25.38% | pass | keep current live |
| 4 | `opt_tqqq_dual_drive_40_40` | optimization variant | 1.047 | 6.50% | 6.50% | -22.80% | pass | no replacement |
| 5 | `new_qld_qqq_trend_70_20` | new leveraged supplement | 0.949 | 3.06% | 3.06% | -23.87% | pass | paper review only |
| 6 | `new_tecl_xlk_trend_50_30` | new leveraged supplement | 0.770 | 2.61% | 1.93% | -32.80% | pass | paper review only |
| 7 | `new_rom_xlk_trend_70_20` | new leveraged supplement | 0.745 | 0.28% | 0.28% | -29.25% | pass | paper review only |
| 8 | `opt_soxl_soxx_signal_soxx_50` | optimization variant | 0.899 | 37.86% | 14.35% | -56.84% | fail | reject / no replacement |
| 9 | `new_usd_smh_trend_50_30` | new leveraged supplement | 0.676 | 12.26% | 0.11% | -47.20% | fail | reject / not live enable |



### Screened out before inclusion

`new_upro_spy_trend_50_30` was screened during research but removed from the candidate set because its long-window CAGR was below SPY: 10.18% versus SPY 14.29%, with -29.05% worst drawdown. Per the current rule, new strategies that do not beat the market are not kept in the candidate list.

### Final promotion decision

No candidate in this run reached the bar for replacing an existing live strategy or entering live enable:

- `replacement_candidate=false` for all optimization variants; current TQQQ/SOXL live proxies stay in place.
- `live_enable_candidate=false` for all new leveraged supplements.
- Passing new supplements are limited to paper observation / supplemental review only.

### New strategy ranking only

| New rank | Candidate | Long CAGR | Long excess CAGR vs SPY | Worst drawdown | Decision |
| ---: | --- | ---: | ---: | ---: | --- |
| 1 | `new_qld_qqq_trend_70_20` | 17.36% | 3.06% | -23.87% | Best new candidate, but paper review only; not live enable |
| 2 | `new_tecl_xlk_trend_50_30` | 16.22% | 1.93% | -32.80% | Passes research gate but high drawdown; paper review only; not live enable |
| 3 | `new_rom_xlk_trend_70_20` | 14.57% | 0.28% | -29.25% | Marginal SPY outperformance; paper review only; not live enable |
| 4 | `new_usd_smh_trend_50_30` | 14.40% | 0.11% | -47.20% | Reject for live; drawdown too deep |

### TQQQ/SOXL optimization interpretation

- TQQQ live proxy remains the default recommendation. `opt_tqqq_qld_tqqq_60_20` is close but does not improve enough to replace it. `opt_tqqq_dual_drive_40_40` reduces drawdown but gives up CAGR, so it is only a conservative-mode candidate.
- SOXL live proxy remains the default recommendation. `opt_soxl_soxx_signal_soxx_50` has attractive returns but fails the max-drawdown gate and should not replace live.
- The newly added supplemental candidates should not be wired into live automatically. The strongest new candidate is `new_qld_qqq_trend_70_20`; `new_tecl_xlk_trend_50_30` and `new_rom_xlk_trend_70_20` require paper observation because their long-window performance does not beat their sector benchmarks even though they beat SPY.

## Live integration boundary

Do not wire any leveraged supplemental candidate into `UsEquityStrategies` runtime manifests or broker platform defaults from this run. `research_gate_passed=true` is not a live-enable flag, and this run sets `live_enable_candidate=false` for every candidate.

Before enabling live, require:

1. separate paper-trading observation;
2. manual approval of leverage and product risk;
3. runtime strategy contract review;
4. broker schedule, sizing, and kill-switch configuration;
5. follow-up comparison against current live TQQQ/SOXL sleeves after at least one new data refresh.
