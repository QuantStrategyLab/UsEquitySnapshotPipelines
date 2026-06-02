# US Equity Strategy Candidate Research Gate

This note documents the research-only gate for comparing current live US equity snapshot strategy variants and adding ordinary ETF / new snapshot-backed candidates.
It is intentionally conservative: candidates can be considered for live review only after the same fixed rules pass short, medium, and long windows.

`live_enabled_candidate=true` means "eligible for live integration review". It does **not** modify broker runtime enablement.

## Anti-overfitting rules

- Use fixed, literature-backed rules; do not optimize parameters against the latest output.
- Separate current live baseline, optimization variants, and genuinely new supplemental strategies.
- Keep genuinely new supplemental strategy count bounded to about five per run.
- Require three period windows before live integration review:
  - short: recent regime check;
  - medium: multi-year cycle check;
  - long: full available-history robustness check.
- Apply a simple live gate across all periods:
  - enough trading days in every window;
  - positive CAGR in every window;
  - positive Sharpe in every window;
  - max drawdown above `-45%` in every window;
  - long-window excess CAGR no worse than `-3%` versus the candidate benchmark.
- Rank by a robustness score using minimum Sharpe, median Sharpe, median excess CAGR, worst drawdown, and turnover penalty.

## Candidate groups

### Current live baseline

| Candidate | Type | Rule |
| --- | --- | --- |
| `live_r1000_multi_factor_defensive` | snapshot Russell 1000 | current runtime-enabled default factor stack |

### Optimization variants of the current live snapshot strategy

These are **not** new strategies. They reuse the current Russell 1000 price-only factor stack and only change portfolio construction parameters.

| Candidate | Type | Rule |
| --- | --- | --- |
| `opt_r1000_core_momentum_16` | optimization variant | current factor stack, more concentrated 16-name construction |
| `opt_r1000_defensive_diversified_32` | optimization variant | current factor stack, more diversified 32-name construction |

### Genuinely new supplemental strategies

These are the bounded supplemental set for this research run: three ordinary ETF strategies plus two new snapshot rules.

| Candidate | Type | Rule |
| --- | --- | --- |
| `ordinary_dual_momentum_qqq_spy_ief` | ordinary ETF | QQQ/SPY relative 12-1 momentum plus absolute trend gate; IEF defense |
| `ordinary_sector_momentum_top3` | ordinary ETF | top-3 monthly sector momentum with 200-day trend filter |
| `ordinary_factor_momentum_low_vol_top2` | ordinary ETF | top-2 factor ETF momentum score penalized by realized volatility |
| `snapshot_r1000_low_vol_momentum` | new snapshot Russell 1000 | positive momentum/trend names ranked with explicit low-volatility and drawdown penalties |
| `snapshot_r1000_sector_balanced_relative_strength` | new snapshot Russell 1000 | rank sectors by relative strength, then select leaders within selected sectors under sector caps |

## Research basis

The ordinary and new snapshot candidates use simple momentum/trend/low-volatility concepts with broad external support:

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

ETF data was downloaded through yfinance from `2017-01-03` to `2026-06-02`, then clipped to the configured period windows. Russell 1000 snapshot candidates used the local point-in-time source files under `../_local_runs/r1000_multifactor_defensive_20260403_official_monthly_v2_alias/`.

### Ranking

| Rank | Candidate | Group | Min Sharpe | Median excess CAGR | Worst drawdown | Gate | Review action |
| ---: | --- | --- | ---: | ---: | ---: | --- | --- |
| 1 | `opt_r1000_core_momentum_16` | optimization | 0.863 | 21.05% | -30.71% | pass | replacement review |
| 2 | `live_r1000_multi_factor_defensive` | current live baseline | 0.855 | 18.26% | -27.62% | pass | keep baseline |
| 3 | `snapshot_r1000_low_vol_momentum` | new snapshot | 0.875 | 16.81% | -23.87% | pass | supplemental review |
| 4 | `opt_r1000_defensive_diversified_32` | optimization | 0.818 | 13.27% | -25.00% | pass | replacement review |
| 5 | `ordinary_sector_momentum_top3` | new ordinary | 0.693 | -0.95% | -27.47% | pass | supplemental review |
| 6 | `snapshot_r1000_sector_balanced_relative_strength` | new snapshot | 0.686 | 1.16% | -23.85% | pass | supplemental review |
| 7 | `ordinary_dual_momentum_qqq_spy_ief` | new ordinary | 0.614 | -2.97% | -31.17% | pass | supplemental review |
| 8 | `ordinary_factor_momentum_low_vol_top2` | new ordinary | 0.560 | -2.51% | -31.43% | fail: long excess CAGR below -3% | reject |

### Replacement interpretation

- `opt_r1000_core_momentum_16` has the best robustness score and higher CAGR than live baseline in all three periods, but it increases max drawdown in all three periods and medium-window Sharpe is lower. It should **not** automatically replace live; it is suitable for paper/replacement review only.
- `opt_r1000_defensive_diversified_32` reduces medium/long drawdown but gives up CAGR and Sharpe in all three periods. It is **not** a growth replacement for live; it may be a lower-risk variant if drawdown reduction is prioritized.
- `snapshot_r1000_low_vol_momentum` is not an optimization variant, but it is the strongest new supplemental snapshot candidate: lower worst drawdown than live baseline and higher Sharpe, with slightly lower medium/long CAGR. It is better suited as a supplemental/risk-balanced sleeve than a direct replacement.

## Live integration boundary

Do not wire a candidate into `UsEquityStrategies` runtime manifests or broker platform defaults solely because `live_enabled_candidate=true`.
That flag is only a precondition for a follow-up integration plan covering:

1. runtime strategy contract and input ownership;
2. broker-specific schedule and rollback controls;
3. artifact publishing path for snapshot candidates;
4. paper-trading observation window;
5. final manual approval.
