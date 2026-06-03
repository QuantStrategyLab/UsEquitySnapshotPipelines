# US Equity Strategy Candidate Research Gate

This note documents the research-only gate for comparing the current live US equity snapshot strategy against ordinary ETF, new snapshot-backed, and parameter-optimization candidates.

`live_enabled_candidate=true` is intentionally unused in this run. The current decision is stricter: a candidate must be clearly better than the existing live-enabled Russell 1000 snapshot strategy before it can remain in the formal candidate set or be considered for promotion. In this run every row has `live_enabled_candidate=false`.

## Current decision

No non-leveraged ordinary ETF strategy, new snapshot strategy, or optimization variant reached the bar for replacing or supplementing the current live-enabled strategy.

Final default ranking contains only the current live baseline. Optimization variants, ordinary ETF strategies, and new snapshot strategy candidates from the earlier pass were screened out before inclusion because they either exceeded the 30% drawdown limit or did not clearly beat the current live baseline.

## Anti-overfitting and promotion rules

- Use fixed, literature-backed rules; do not optimize parameters against the latest output.
- Separate current live baseline, optimization variants, and genuinely new supplemental strategies.
- New ordinary/snapshot strategies must keep worst drawdown within 30%, beat SPY, and also beat the current live-enabled Russell 1000 snapshot baseline before they are kept in the formal candidate set.
- Optimization variants must keep worst drawdown within 30% and strictly improve the live baseline across a conservative promotion check before replacement review:
  - higher robustness score;
  - higher long-window excess CAGR;
  - no worse worst drawdown;
  - no worse minimum Sharpe.
- Require three period windows for comparison:
  - short: recent regime check;
  - medium: multi-year cycle check;
  - long: full available-history robustness check.
- `live_enabled_candidate=false` unless the candidate clears both the 30% drawdown gate and the live-baseline comparison gate. No candidate cleared both in this run.

## Candidate groups in final default ranking

### Current live baseline

| Candidate | Type | Rule |
| --- | --- | --- |
| `live_r1000_multi_factor_defensive` | snapshot Russell 1000 | current runtime-enabled default factor stack |

### Optimization variants

None retained in the final default ranking.

### Genuinely new supplemental strategies

None retained in the final default candidate set.

## Screened out before inclusion

The following strategies were researched in the prior pass but removed from the formal candidate set because they did not clearly outperform the existing live-enabled Russell 1000 snapshot baseline.

Live baseline comparison row:

| Candidate | Min Sharpe | Median excess CAGR vs SPY | Long excess CAGR vs SPY | Worst drawdown | Robustness score |
| --- | ---: | ---: | ---: | ---: | ---: |
| `live_r1000_multi_factor_defensive` | 0.855 | 18.26% | 3.18% | -27.62% | 1.843 |

Screened ordinary/snapshot candidates:

| Screened candidate | Type | Long excess CAGR vs SPY | Worst drawdown | Reason |
| --- | --- | ---: | ---: | --- |
| `snapshot_r1000_low_vol_momentum` | new snapshot | 2.26% | -23.87% | Better drawdown/min Sharpe, but lower long excess CAGR and lower robustness score than live baseline |
| `snapshot_r1000_sector_balanced_relative_strength` | new snapshot | -0.32% | -23.85% | Long-window excess below live baseline and below SPY |
| `ordinary_sector_momentum_top3` | ordinary ETF | -2.79% | -27.47% | Long-window excess below live baseline and below SPY |
| `ordinary_dual_momentum_qqq_spy_ief` | ordinary ETF | -2.97% | -31.17% | Long-window excess below live baseline and worse drawdown |
| `ordinary_factor_momentum_low_vol_top2` | ordinary ETF | -4.57% | -31.43% | Long-window excess below live baseline and below SPY; fails original long-excess gate |

Screened optimization variants:

| Optimization variant | Long excess CAGR vs SPY | Worst drawdown | Reason |
| --- | ---: | ---: | --- |
| `opt_r1000_core_momentum_16` | 5.79% | -30.71% | Exceeds the 30% drawdown limit; no replacement |
| `opt_r1000_defensive_diversified_32` | 1.38% | -25.00% | Drawdown is within 30%, but long excess CAGR and robustness score are below live baseline; no replacement |

## Research basis

The screened ordinary and new snapshot candidates use simple momentum/trend/low-volatility concepts with broad external support:

- Trend following / time-series momentum: AQR's trend-following research, including *A Century of Evidence on Trend-Following Investing*.
- Tactical asset allocation moving-average filters: Meb Faber's *A Quantitative Approach to Tactical Asset Allocation*.
- Dual momentum: Gary Antonacci's relative + absolute momentum framework in *Risk Premia Harvesting Through Dual Momentum*.
- Momentum and low-volatility factor proxies: iShares describes MTUM as exposure to U.S. large/mid-cap stocks with relatively higher price momentum; MSCI describes its USA Minimum Volatility Index as a constrained minimum-variance strategy on the large/mid-cap USA universe.
- Sector rotation: S&P Dow Jones documents an S&P 500 sector-rotation index with valuation and momentum overlay.

Reference URLs:

- https://www.aqr.com/insights/research/journal-article/a-century-of-evidence-on-trend-following-investing
- https://papers.ssrn.com/sol3/Delivery.cfm/SSRN_ID2403936_code649342.pdf?abstractid=962461
- https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2042750
- https://www.ishares.com/us/products/251614/ishares-msci-usa-momentum-factor-etf
- https://www.msci.com/indexes/index/139133
- https://www.spglobal.com/spdji/en/indices/dividends-factors/sp-500-high-momentum-value-sector-rotation/

These references are inputs for candidate design only. Live admission still depends on local backtest output and operational review.

## Local command

From `/Users/lisiyi/Projects/UsEquitySnapshotPipelines`:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_us_equity_strategy_candidates.py \
  --download \
  --price-start 2017-01-01 \
  --periods short:2025-06-01:2026-04-02,medium:2023-06-01:2026-04-02,long:2018-01-01:2026-04-02 \
  --r1000-prices ../_local_runs/r1000_multifactor_defensive_20260403_official_monthly_v2_alias/r1000_price_history.csv \
  --r1000-universe ../_local_runs/r1000_multifactor_defensive_20260403_official_monthly_v2_alias/r1000_universe_history.csv \
  --turnover-cost-bps 5 \
  --output-dir data/output/us_equity_strategy_candidate_research_20260603
```

Outputs:

- `downloaded_etf_price_history.csv`
- `period_summary.csv`
- `ranking.csv`

## 2026-06-03 local run summary

Output directory:

`data/output/us_equity_strategy_candidate_research_20260603`

Periods:

- short: `2025-06-01` to `2026-04-02`
- medium: `2023-06-01` to `2026-04-02`
- long: `2018-01-01` to `2026-04-02`

### Final ranking

| Rank | Candidate | Group | Min Sharpe | Median excess CAGR vs SPY | Long excess CAGR vs SPY | Worst drawdown | Live enabled | Decision |
| ---: | --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| 1 | `live_r1000_multi_factor_defensive` | current live baseline | 0.855 | 18.26% | 3.18% | -27.62% | false | keep current live |

## Live integration boundary

Do not wire a candidate into `UsEquityStrategies` runtime manifests or broker platform defaults from this run.

This run concludes:

1. no ordinary ETF strategy is retained;
2. no new snapshot strategy is retained;
3. no optimization variant is retained in the final ranking;
4. every row has `live_enabled_candidate=false`;
5. future candidates must keep worst drawdown within 30%.
