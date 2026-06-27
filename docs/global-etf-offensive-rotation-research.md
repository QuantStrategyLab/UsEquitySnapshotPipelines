# Global ETF Offensive Rotation Research

This is a research-only track for testing whether an offensive Global ETF profile can behave more like the Russell leader-rotation line: prioritize broad-market outperformance while keeping the existing live `global_etf_rotation` defensive profile unchanged.

## Boundary

- No live manifest or broker behavior is changed by this research.
- The current `global_etf_rotation` profile remains the defensive baseline.
- Offensive candidates are deterministic, rule-based, and backtestable; AI is not part of signal generation.
- Passing pure offensive candidates are marked `paper_review_only`, not auto-promoted to live.
- Passing liveable composites are marked `live_design_review`; this still does not change live runtime by itself.
- Passing the stricter baseline-relative live gate is marked `live_promotion_review`; this still requires manual approval before any live manifest/runtime change.

## Candidate set

The runner compares:

1. `live_global_etf_rotation_defensive_baseline` — current quarterly top-2 confidence/vol-gated baseline.
2. `offensive_growth_top2_monthly` — aggressive growth/cyclical ETF pool, monthly equal-weight top 2.
3. `offensive_growth_top1_monthly` — concentrated monthly top 1.
4. `offensive_growth_top2_conf75_monthly` — monthly top 2 with 75/25 confidence tilt when the top name is decisive and volatility is acceptable.
5. `offensive_growth_top2_weak_canary_monthly` — disables daily all-BIL canary exits to measure the offensive trade-off.
6. `offensive_growth_eaa_top2_monthly` — EAA-inspired generalized momentum score: high momentum, lower volatility, lower correlation to the offensive pool.
7. `offensive_growth_fast_top2_monthly` — VAA-inspired fast 1/3/6-month momentum score with SMA eligibility.
8. `offensive_growth_dual_momentum_top2_monthly` — dual-momentum growth pool: rank by 1/3/6/12-month relative strength, but require positive absolute momentum and 200-day SMA eligibility.
9. `offensive_growth_daa_cash_fraction_top2_monthly` — DAA-inspired canary breadth: each bad canary adds a proportional safe-haven sleeve.
10. `offensive_growth_eaa_daa_cash_fraction_monthly` — combines EAA-style selection with DAA-style proportional canary cash fraction.

Liveable composite candidates are generated only when both child strategies exist in the run:

11. `liveable_blend_baseline90_fast10` — 90% current defensive baseline + 10% fast offensive sleeve.
12. `liveable_blend_baseline90_dual10` — 90% current defensive baseline + 10% dual-momentum offensive sleeve.
13. `liveable_baseline_relative_decay_brake_baseline90_fast10_floor0` — 90/10 sleeve that cuts the fast sleeve to 0% on the next monthly rebalance only when the fast sleeve underperforms the defensive baseline over both trailing 63 and 126 trading-day windows.
14. `liveable_blend_baseline85_fast15` — 85% current defensive baseline + 15% fast offensive sleeve.
15. `liveable_blend_baseline80_fast20` — 80% current defensive baseline + 20% fast offensive sleeve.
16. `liveable_blend_baseline75_fast25` — 75% current defensive baseline + 25% fast offensive sleeve.
17. `liveable_blend_baseline70_fast30` — 70% current defensive baseline + 30% fast offensive sleeve.
18. `liveable_regime_qqqtrend_baseline70_fast30` — 30% fast offensive sleeve only when QQQ is above its 200-day trend and fast momentum is positive; otherwise 100% defensive baseline.
19. `liveable_volmanaged_baseline70_fast30` — same QQQ trend gate, but scales the fast sleeve down when 63-day realized QQQ volatility is above 18%.

Composite returns are recomputed from combined daily weights and the raw asset return matrix, then transaction costs are applied to combined-weight turnover. This keeps the composite layer deterministic and avoids treating the child strategy returns as black boxes.

The offensive pool tilts toward QQQ/VUG/IWF/MTUM, tech/semi/software, growth sectors, cyclicals, and selected international beta ETFs. Defensive commodities and low-vol sectors from the live pool are intentionally reduced.

## Literature scan used for expansion

- Keller and Butler, *A Century of Generalized Momentum; From Flexible Asset Allocations (FAA) to Elastic Asset Allocation (EAA)*: motivates ranking by return, volatility, and correlation rather than return-only momentum. Source: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2543979
- Keller and Keuning, *Breadth Momentum and Vigilant Asset Allocation (VAA)*: motivates dual momentum, relative strength, and a faster crash-protection filter. Source: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3002624
- Keller and Keuning, *Breadth Momentum and the Canary Universe: Defensive Asset Allocation (DAA)*: motivates a separate canary universe and cash fraction governed by bad canaries, instead of all-or-nothing de-risking. Source: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3212862
- Keller and Keuning, *Hybrid Asset Allocation (HAA)*: motivates replacing bad TopX assets with cash while keeping lower cash fractions than more defensive canary designs. Source: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4346906
- Antonacci, *Risk Premia Harvesting Through Dual Momentum*: reinforces combining absolute and relative momentum for lower volatility/drawdown than relative-only momentum. Source: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2042750
- Keller and Keuning, *Protective Asset Allocation (PAA)*: reinforces proportional crash protection rather than only all-in/all-out defensive switching. Source: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2759734
- Moreira and Muir, *Volatility-Managed Portfolios*: motivates reducing risky exposure when realized volatility is high. Source: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2659431
- Moskowitz, Ooi, and Pedersen, *Time Series Momentum*: supports using intermediate-horizon trend persistence as a deterministic risk-on/off input. Source: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2089463
- Hurst, Ooi, and Pedersen, *A Century of Evidence on Trend-Following Investing*: motivates trend-following as a defensive/risk-management overlay rather than only a return enhancer. Source: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2993026
- Clare, Seaton, Smith, and Thomas, *Trend Following, Stop Losses and the Frequency of Trading*: motivates simple monthly trend rules and warns that higher-frequency trading can damage returns. Source: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2126476
- Bailey, Borwein, López de Prado, and Zhu, *The Probability of Backtest Overfitting*: motivates avoiding promotion based on the best single in-sample result after testing many variants. Source: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253
- Bailey and López de Prado, *The Deflated Sharpe Ratio*: motivates adjusting expectations for selection bias, non-normal returns, and short samples. Source: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551
- White, *A Reality Check for Data Snooping*: reinforces that repeated rule selection can create apparently good results by chance. Source: https://www.ssc.wisc.edu/~bhansen/718/White2000.pdf
- Daniel and Moskowitz, *Momentum Crashes*: motivates special caution for momentum overlays in high-volatility rebound regimes. Source: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2371227
- SEC Investor Bulletin on ETFs: reinforces that bid-ask spreads are a direct ETF trading cost and tighter spreads are generally associated with more liquid, higher-volume ETFs. Source: https://www.investor.gov/introduction-investing/general-resources/news-alerts/alerts-bulletins/investor-bulletins-24
- FINRA ETF overview: reinforces that ETFs trade intraday like stocks and are subject to bid-ask spreads. Source: https://www.finra.org/investors/investing/investment-products/exchange-traded-funds-and-products
- Vanguard ETF guidance: reinforces checking ETF liquidity, trading volume, and bid-ask spread before execution. Source: https://investor.vanguard.com/investor-resources-education/understanding-investment-types/choosing-between-funds-individual-securities
- Almgren and Chriss, *Optimal Execution of Portfolio Transactions*: motivates treating execution cost as a function of trade size, liquidity, and market impact rather than only a flat turnover haircut. Source: https://www.smallake.kr/wp-content/uploads/2016/03/optliq.pdf
- Amihud, *Illiquidity and Stock Returns*: motivates using daily dollar volume as a rough, reproducible liquidity/price-impact proxy when intraday spread data is not available. Source: https://www.cis.upenn.edu/~mkearns/finread/amihud.pdf

## Gate

Primary benchmark is SPY. Secondary benchmark is QQQ.

A candidate only passes research gate when:

- all configured short/medium/long windows have enough data;
- CAGR and Sharpe are positive in every window;
- long-window CAGR beats SPY;
- median cross-window CAGR beats SPY;
- it is QQQ-competitive, meaning either long-window CAGR beats QQQ or worst drawdown is at least 5 percentage points better than QQQ.

### Baseline-relative live gate

The research gate is intentionally broader than a live promotion rule. A `liveable_candidate` only passes the stricter live gate when it also clears all of these current-baseline checks:

- long-window CAGR is at least 0.25 percentage points above the current live baseline;
- long-window max drawdown is not more than 2 percentage points worse than the baseline;
- median turnover/year is not more than 2 turns/year above the baseline;
- calendar-year CAGR beats the baseline in at least 50% of comparable windows;
- rolling 3Y CAGR beats the baseline in at least 50% of comparable windows;
- rolling 5Y CAGR beats the baseline in at least 60% of comparable windows;
- worst rolling 3Y/5Y CAGR shortfall versus baseline is no worse than -3 percentage points;
- worst window drawdown is not more than 3 percentage points worse than the baseline.

Passing this gate produces `candidate_for_live_promotion_review`, not an automatic live change.

## Run command

```bash
PYTHONPATH=src:/Users/lisiyi/Projects/UsEquityStrategies/src \
  /Users/lisiyi/Projects/UsEquitySnapshotPipelines/.venv/bin/python \
  -m us_equity_snapshot_pipelines.global_etf_offensive_rotation_research \
  --download \
  --price-start 2010-01-01 \
  --output-dir data/output/global_etf_offensive_rotation_research_YYYYMMDD
```

Outputs:

- `period_summary.csv`
- `ranking.csv`
- `portfolio_returns.csv`
- `portfolio_returns_with_benchmarks.csv`
- `rebalance_events.csv`
- `candidate_robustness_windows.csv`
- `candidate_robustness_summary.csv`
- `live_readiness_summary.csv`
- `candidate_liquidity_summary.csv`
- `candidate_liquidity_symbol_summary.csv`
- `cost_stress_live_readiness_summary.csv` when `--cost-stress-bps` is provided
- `walk_forward_selection_windows.csv`
- `walk_forward_selection_summary.csv`
- `dynamic_cost_*` files when `--dynamic-cost` or `--dynamic-cost-navs` is provided
- `dynamic_cost_nav_stress_live_readiness_summary.csv` when one or more dynamic-cost NAV assumptions are evaluated
- `dynamic_cost_nav_stress_walk_forward_summary.csv` and `dynamic_cost_nav_stress_walk_forward_windows.csv` when dynamic-cost NAV assumptions are evaluated
- `weights_<candidate>.csv`
- `downloaded_price_history.csv`
- `recommendation.md`
- `live_decision_summary.json`
- `run_manifest.json`

`live_decision_summary.json` is the automation-facing summary of the same gate
logic used in `recommendation.md`. It records the decision state, preferred
candidate, defensive baseline candidate, promotion blockers, and highest passing
cost/NAV assumptions as strict JSON so monthly review jobs can consume it
without parsing Markdown.

## 2026-06-28 dual-momentum sleeve compare

Command:

```bash
PYTHONPATH=src python3 -m us_equity_snapshot_pipelines.global_etf_offensive_rotation_research \
  --prices data/output/global_etf_baseline_relative_decay_narrow_verify_20260620/downloaded_price_history.csv \
  --output-dir data/output/global_etf_dual_momentum_compare_20260628 \
  --variants live_global_etf_rotation_defensive_baseline,offensive_growth_fast_top2_monthly,offensive_growth_dual_momentum_top2_monthly \
  --liveable-composites liveable_blend_baseline90_fast10,liveable_blend_baseline90_dual10 \
  --cost-stress-bps 5,10,25 \
  --dynamic-cost --dynamic-cost-nav 500000
```

Data range: reused 2010-01-04 through 2026-06-18 price history from the 2026-06-20 narrow verify run.

| Rank | Candidate | Gate | Long CAGR | Long excess vs SPY | Long excess vs QQQ | Worst drawdown | Live readiness (NAV $500k) | Action |
| ---: | --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| 1 | `offensive_growth_fast_top2_monthly` | pass | 15.98% | +2.17% | -3.70% | -28.07% | n/a (offensive only) | paper review only |
| 2 | `liveable_blend_baseline90_fast10` | pass | 15.13% | +1.32% | -4.55% | -22.18% | pass (`candidate_for_live_promotion_review`) | live design review; walk-forward blocked |
| 3 | `liveable_blend_baseline90_dual10` | pass | 14.75% | +0.93% | -4.93% | -23.48% | fail (`long_cagr_not_above_baseline`) | live design review only |
| 4 | `live_global_etf_rotation_defensive_baseline` | pass | 14.81% | +0.99% | -4.88% | -23.31% | keep current live | keep current live |
| 5 | `offensive_growth_dual_momentum_top2_monthly` | fail | 12.30% | -1.51% | -7.38% | -30.85% | reject | reject |

Conclusion:

- Dual-momentum as a **standalone offensive sleeve fails** the research gate on this ETF universe: long-window CAGR trails SPY and it is not QQQ-competitive on return or drawdown.
- As a **10% composite sleeve**, dual-momentum passes the broad research gate but **loses to fast10** on long CAGR (+14.75% vs +15.13%), baseline-relative excess (+0.93% vs +1.32% vs SPY), and drawdown (-23.48% vs -22.18%).
- Under dynamic-cost NAV $500k assumptions, only `liveable_blend_baseline90_fast10` clears the baseline-relative live gate; dual10 fails calendar/rolling baseline win-rate checks.
- Walk-forward/OOS gate blocks both composites (`worst_oos_excess_too_low`); `live_decision_summary.json` records `decision_state=walk_forward_blocked`.
- **Next step:** keep researching fast10-based sleeves and brakes; do not promote dual10. Dual-momentum remains a documented negative result, not a live replacement candidate.

## 2026-06-28 fast10 brake compare

Command:

```bash
PYTHONPATH=src python3 -m us_equity_snapshot_pipelines.global_etf_offensive_rotation_research \
  --prices data/output/global_etf_baseline_relative_decay_narrow_verify_20260620/downloaded_price_history.csv \
  --output-dir data/output/global_etf_fast10_brake_compare_20260628 \
  --variants live_global_etf_rotation_defensive_baseline,offensive_growth_fast_top2_monthly \
  --liveable-composites liveable_blend_baseline90_fast10,liveable_baseline_relative_decay_brake_baseline90_fast10_floor0,liveable_trend_drawdown_brake_baseline85_fast15_floor10,liveable_trend_drawdown_brake_baseline85_fast15_floor0 \
  --walk-forward-candidates liveable_blend_baseline90_fast10,liveable_baseline_relative_decay_brake_baseline90_fast10_floor0,liveable_trend_drawdown_brake_baseline85_fast15_floor10 \
  --cost-stress-bps 5,10,25 \
  --dynamic-cost --dynamic-cost-nav 500000
```

| Rank | Candidate | Long CAGR | Long excess vs SPY | Max DD | Live readiness (NAV $500k) | Action |
| ---: | --- | ---: | ---: | ---: | --- | --- |
| 1 | `liveable_trend_drawdown_brake_baseline85_fast15_floor10` | 15.27% | +1.46% | -22.30% | pass | live design review; walk-forward blocked |
| 2 | `liveable_blend_baseline90_fast10` | 15.13% | +1.32% | -22.18% | pass | live design review; walk-forward blocked |
| 3 | `liveable_baseline_relative_decay_brake_baseline90_fast10_floor0` | 14.91% | +1.10% | -22.18% | fail (calendar/5y win rate) | continue research |
| 4 | `live_global_etf_rotation_defensive_baseline` | 14.81% | +0.99% | -23.31% | keep current live | keep current live |

Conclusion:

- **Best composite so far:** `liveable_trend_drawdown_brake_baseline85_fast15_floor10` — highest long CAGR (+0.46pp vs baseline) with modest drawdown improvement.
- **Do not pursue:** `baseline_relative_decay_brake` (relative decay cuts sleeve too aggressively) and `floor0` trend brake (calendar baseline win-rate fails live gate).
- Walk-forward still blocks all composites (`worst_oos_excess_too_low`, 2025 OOS window).

## 2026-06-28 walk-forward sensitivity (trend_drawdown floor10)

Five sensitivity runs under `data/output/global_etf_wf_sensitivity_*`:

| Config | Live gate | WF gate | Notes |
| --- | --- | --- | --- |
| train5 / min0 / NAV $500k | pass | fail | Closest pass; only `worst_oos_excess_too_low` (-6.7% in 2025 test year) |
| train5 / min25 / NAV $500k | pass | fail | Stricter train threshold worsens WF |
| train7 / min0 / NAV $500k–$1M | pass | fail | Fewer OOS windows (4), 25% win rate — do not use 7y train |
| NAV $300k / $1M | pass | fail | NAV has negligible effect on WF outcome |

Recommendation: keep live defensive baseline; continue shadow review of `liveable_trend_drawdown_brake_baseline85_fast15_floor10`. Do not auto-promote until walk-forward OOS gate clears.

## 2026-06-28 worst OOS window diagnostics (2025 root cause)

Command (built-in diagnostics module):

```bash
PYTHONPATH=src python3 -m us_equity_snapshot_pipelines.global_etf_oos_window_diagnostics \
  --artifact-dir data/output/global_etf_wf_sensitivity_train5_min0_nav500k
```

Artifacts: `worst_oos_window_diagnostics/` under the research output dir.

### Walk-forward context

Train window `2020-01-01_2024-12-31` selected `liveable_trend_drawdown_brake_baseline85_fast15_floor10` for test year **2025** (train excess +1.06% vs baseline). OOS result:

| Metric | floor10 composite | Defensive baseline |
| --- | ---: | ---: |
| 2025 calendar return | +50.4% | +57.0% |
| Excess vs baseline | **-6.68%** | — |
| Drawdown delta vs baseline | -0.17% (slightly worse) | — |

This single window drives `worst_oos_excess_too_low` across all WF sensitivity configs.

### Monthly excess return decomposition (2025)

| Month | Excess vs baseline | Notes |
| --- | ---: | --- |
| Feb | -1.00% | Early underperformance |
| Apr | -0.79% | Drawdown month; brake at 10% sleeve from Mar |
| Sep | -1.09% | Strong rally; baseline defensive rotation captured more |
| Nov | **-1.72%** | Worst month |
| Dec | -1.26% | Continued lag |
| Jul–Aug | 0.00% | Brake likely at floor; matched baseline |

### Overlay changes in 2025

| Effective date | Overlay weight | Signal |
| --- | ---: | --- |
| 2025-03-03 | 10% | QQQ trend/drawdown brake (floor10) |
| 2025-06-02 | 15% | Brake released back to cap |

### Candidate comparison within 2025 window

| Candidate | 2025 return | Excess vs baseline |
| --- | ---: | ---: |
| `liveable_blend_baseline90_fast10` | +52.2% | -4.8% |
| `liveable_trend_drawdown_brake_baseline85_fast15_floor0` | +51.5% | -5.6% |
| `liveable_trend_drawdown_brake_baseline85_fast15_floor10` | +50.4% | **-6.7%** |

### Root-cause interpretation

1. **Not a drawdown-protection win:** floor10 did not improve 2025 drawdown versus baseline; the fast sleeve simply dragged return in a strong equity year.
2. **Brake timing hurt in a bull regime:** the Mar–May brake cut the fast sleeve to 10% during recovery; baseline's quarterly defensive rotation outperformed without the overlay drag.
3. **All fast-sleeve composites lost to baseline in 2025:** even plain 90/10 blend underperformed by -4.8%. The WF gate failure is not floor10-specific — any promoted offensive sleeve would likely fail the -3% worst-OOS threshold in 2025.
4. **Implication:** raising `walk_forward_min_train_excess_cagr` or shortening train window does not fix 2025; the defensive baseline itself was the best choice that year. Continue shadow review of floor10 for in-sample edge, but do not expect WF clearance until a future OOS year shows positive sleeve contribution or the gate threshold is explicitly revised.

## 2026-06-20 research run

Command used local downloaded Yahoo/yfinance data after the initial download completed:

```bash
PYTHONPATH=src \
  /Users/lisiyi/Projects/UsEquitySnapshotPipelines/.venv/bin/python \
  -m us_equity_snapshot_pipelines.global_etf_offensive_rotation_research \
  --prices data/output/global_etf_offensive_rotation_research_20260620/downloaded_price_history.csv \
  --output-dir data/output/global_etf_offensive_rotation_research_20260620
```

Data range: 2010-01-04 through 2026-06-18, 35 symbols, 141,232 price rows.

| Rank | Candidate | Gate | Long CAGR | Long excess vs SPY | Long excess vs QQQ | Worst drawdown | Action |
| ---: | --- | --- | ---: | ---: | ---: | ---: | --- |
| 1 | `offensive_growth_fast_top2_monthly` | pass | 15.98% | +2.17% | -3.70% | -28.07% | paper review only |
| 2 | `live_global_etf_rotation_defensive_baseline` | pass | 14.81% | +0.99% | -4.88% | -23.31% | keep current live |
| 3 | `offensive_growth_top2_weak_canary_monthly` | fail | 17.01% | +3.19% | -2.67% | -32.37% | reject |
| 4 | `offensive_growth_top2_conf75_monthly` | fail | 13.69% | -0.12% | -5.99% | -29.75% | reject |
| 5 | `offensive_growth_daa_cash_fraction_top2_monthly` | fail | 12.54% | -1.27% | -7.14% | -24.05% | reject |
| 6 | `offensive_growth_top2_monthly` | fail | 13.03% | -0.79% | -6.65% | -28.39% | reject |
| 7 | `offensive_growth_eaa_daa_cash_fraction_monthly` | fail | 8.44% | -5.38% | -11.25% | -25.70% | reject |
| 8 | `offensive_growth_eaa_top2_monthly` | fail | 7.31% | -6.50% | -12.37% | -39.23% | reject |
| 9 | `offensive_growth_top1_monthly` | fail | 5.79% | -8.03% | -13.90% | -35.16% | reject |

Conclusion:

- The web-expanded batch found one paper-review candidate: `offensive_growth_fast_top2_monthly`.
- This candidate is not auto-live. It passes the current research gate because it beats SPY over the long window and is QQQ-competitive through drawdown advantage: it trails QQQ by 3.70% CAGR, but its -28.07% worst drawdown is 7.05 percentage points better than QQQ's -35.12%.
- Compared with the current defensive baseline, it improves long-window CAGR by 1.17 percentage points but worsens worst drawdown by 4.76 percentage points and increases turnover.
- EAA-style return/volatility/correlation ranking did not improve this ETF universe in the current implementation; both EAA variants failed the long SPY gate.
- DAA-style proportional canary cash fraction lowered drawdown relative to the simple monthly top-2 variant, but failed the long SPY gate.
- Next step before any promotion: run robustness splits and sensitivity checks for `offensive_growth_fast_top2_monthly` only; do not tune the full candidate grid.

## 2026-06-20 robustness split

Additional split run:

```bash
PYTHONPATH=src \
  /Users/lisiyi/Projects/UsEquitySnapshotPipelines/.venv/bin/python \
  -m us_equity_snapshot_pipelines.global_etf_offensive_rotation_research \
  --prices data/output/global_etf_offensive_rotation_research_20260620/downloaded_price_history.csv \
  --periods 'pre_covid_2015_2019:2015-01-01:2019-12-31,covid_rate_2020_2022:2020-01-01:2022-12-30,post_2023:2023-01-01:,recent_2025:2025-06-01:' \
  --output-dir data/output/global_etf_offensive_rotation_robustness_20260620
```

| Period | Candidate | CAGR | Excess vs SPY | Excess vs QQQ | Max drawdown | Note |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| 2015-2019 | `offensive_growth_fast_top2_monthly` | 5.17% | -6.43% | -11.58% | -28.07% | weak; below both benchmarks |
| 2020-2022 | `offensive_growth_fast_top2_monthly` | 15.72% | +8.08% | +7.26% | -17.67% | strong crisis/rate cycle result |
| 2023-2026 | `offensive_growth_fast_top2_monthly` | 33.93% | +10.97% | -1.27% | -24.51% | beats SPY, close to QQQ with slightly worse drawdown than QQQ in this window |
| 2025-2026 | `offensive_growth_fast_top2_monthly` | 109.91% | +82.76% | +68.67% | -15.05% | very strong recent window; high recency contribution |

Robustness conclusion:

- Keep `offensive_growth_fast_top2_monthly` as paper-review only.
- It is not strong enough for default live promotion yet because 2015-2019 materially underperformed SPY and QQQ.
- If promoted later, it should likely be a separate offensive profile, not a replacement for the current defensive `global_etf_rotation`.
- Before promotion, test out-of-sample-like variants that do not retune the formula: e.g. fixed 1/3/6 fast score, same universe, same SPY/QQQ gates, and stress windows including 2015-2019 underperformance.

## Candidate-level rolling robustness diagnostics

The main runner now also writes:

- `candidate_robustness_windows.csv`
- `candidate_robustness_summary.csv`

Default focus candidates are:

- `offensive_growth_fast_top2_monthly`
- `liveable_blend_baseline90_fast10`
- `liveable_blend_baseline85_fast15`
- `liveable_blend_baseline80_fast20`
- `liveable_blend_baseline75_fast25`
- `liveable_blend_baseline70_fast30`
- `liveable_regime_qqqtrend_baseline70_fast30`
- `liveable_volmanaged_baseline70_fast30`
- `live_global_etf_rotation_defensive_baseline`

For the 2010-2026 dataset, `offensive_growth_fast_top2_monthly` diagnostics were:

| Window type | Count | SPY CAGR win rate | QQQ CAGR win rate | QQQ competitive rate | Median excess vs SPY | Worst excess vs SPY | Worst drawdown | Median turnover/year |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Calendar year | 11 | 36.36% | 27.27% | 36.36% | -6.29% | -17.01% | -23.19% | 6.59 |
| Rolling 3Y | 10 | 40.00% | 20.00% | 60.00% | -1.40% | -9.02% | -28.07% | 7.19 |
| Rolling 5Y | 9 | 55.56% | 11.11% | 77.78% | +0.95% | -6.43% | -28.07% | 7.01 |

Worst SPY-relative windows for `offensive_growth_fast_top2_monthly`:

| Window type | Window | CAGR | Excess vs SPY | Excess vs QQQ | Max drawdown | Sharpe |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Calendar year | 2015 | -15.70% | -17.01% | -25.62% | -21.15% | -1.62 |
| Calendar year | 2023 | 12.46% | -14.14% | -43.38% | -12.99% | 0.70 |
| Calendar year | 2017 | 8.74% | -13.31% | -24.47% | -10.38% | 0.62 |
| Rolling 3Y | 2017-01-01 to 2019-12-31 | 6.12% | -9.02% | -16.53% | -23.19% | 0.43 |
| Calendar year | 2019 | 23.46% | -7.99% | -15.79% | -16.87% | 1.10 |

Promotion implication:

- `offensive_growth_fast_top2_monthly` should stay paper-review only.
- It passes the original long-window gate, but annual and rolling diagnostics show inconsistent benchmark outperformance.
- It may be useful as a separate offensive sleeve if the portfolio explicitly accepts multi-year SPY/QQQ underperformance risk.
- It also trades materially more than the current defensive baseline: rolling 5Y median turnover/year is about 7.01 versus 3.91 for the baseline.
- It should not replace the current defensive Global ETF profile.

## 2026-06-20 liveable composite expansion

After the robustness split showed that replacing the defensive baseline with the pure fast offensive strategy is too unstable, the next research pass tested deterministic liveable composites. Command:

```bash
PYTHONPATH=src \
  /Users/lisiyi/Projects/UsEquitySnapshotPipelines/.venv/bin/python \
  -m us_equity_snapshot_pipelines.global_etf_offensive_rotation_research \
  --download \
  --price-start 2010-01-01 \
  --output-dir data/output/global_etf_liveable_rotation_research_20260620
```

Data range: 2010-01-04 through 2026-06-18, 35 symbols, 141,232 price rows.

| Rank | Candidate | Gate | Long CAGR | Long excess vs SPY | Long excess vs QQQ | Worst drawdown | Median turnover/year | Action |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | `liveable_blend_baseline70_fast30` | pass | 15.65% | +1.83% | -4.04% | -20.08% | 4.05 | live design review |
| 2 | `liveable_regime_qqqtrend_baseline70_fast30` | pass | 15.60% | +1.79% | -4.08% | -23.98% | 4.10 | live design review |
| 3 | `liveable_blend_baseline80_fast20` | pass | 15.41% | +1.60% | -4.27% | -21.12% | 3.74 | live design review |
| 4 | `liveable_volmanaged_baseline70_fast30` | pass | 15.19% | +1.37% | -4.49% | -23.98% | 4.07 | live design review |
| 5 | `offensive_growth_fast_top2_monthly` | pass | 15.98% | +2.17% | -3.70% | -28.07% | 6.40 | paper review only |
| 6 | `live_global_etf_rotation_defensive_baseline` | pass | 14.81% | +0.99% | -4.88% | -23.31% | 3.12 | keep current live |

Composite conclusion:

- The most promising research-rank line is `liveable_blend_baseline70_fast30`: it improves long-window CAGR versus the current baseline by about 0.84 percentage points, improves max drawdown by about 3.23 percentage points, and keeps turnover much lower than the pure offensive sleeve.
- The dynamic QQQ-trend and volatility-managed overlays are valid backtestable rules, but in this run they did not beat the simpler fixed 70/30 sleeve. They remain research candidates, not preferred defaults.
- At this stage `live_design_review` means “worth manual strategy-design review,” not “ready for production default.”
- If this line moves toward live, implement it as a strategy-level sleeve/overlay feature that consumes deterministic child strategy weights. It should not be a plugin: the plugin architecture should continue to produce signals only, while this rule is a transparent, backtestable portfolio construction rule.

## 2026-06-20 baseline-relative live gate

The live gate was added after the liveable composite pass to avoid promoting the highest long-window/ranking result when it is not consistently better than the current live baseline. Command:

```bash
PYTHONPATH=src \
  /Users/lisiyi/Projects/UsEquitySnapshotPipelines/.venv/bin/python \
  -m us_equity_snapshot_pipelines.global_etf_offensive_rotation_research \
  --download \
  --price-start 2010-01-01 \
  --output-dir data/output/global_etf_live_gate_research_20260620
```

Data range: 2010-01-04 through 2026-06-18, 35 symbols, 141,232 price rows.

| Candidate | Live gate | Long excess vs baseline | Long drawdown delta vs baseline | Turnover delta vs baseline | Calendar win vs baseline | Rolling 3Y win vs baseline | Rolling 5Y win vs baseline | Worst rolling excess vs baseline | Action |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `liveable_blend_baseline80_fast20` | pass | +0.61% | +2.18% | +0.62 | 54.55% | 80.00% | 66.67% | -0.97% | live promotion review |
| `liveable_blend_baseline70_fast30` | fail | +0.84% | +3.23% | +0.94 | 54.55% | 70.00% | 55.56% | -1.47% | continue research |
| `liveable_regime_qqqtrend_baseline70_fast30` | fail | +0.80% | -0.67% | +0.98 | 45.45% | 80.00% | 88.89% | -1.65% | continue research |
| `liveable_volmanaged_baseline70_fast30` | fail | +0.38% | -0.67% | +0.95 | 36.36% | 60.00% | 66.67% | -2.11% | continue research |

Live gate conclusion:

- The current best live-candidate version is `liveable_blend_baseline80_fast20`, not the higher-ranked 70/30 research variant.
- 80/20 gives up some long-window CAGR versus 70/30, but it clears the baseline-relative rolling 5Y win-rate and worst-shortfall gates.
- 70/30 remains useful as an aggressive research candidate, but it fails the stricter live gate because rolling 5Y baseline win rate is 55.56%, below the 60% threshold.
- Dynamic QQQ-trend and volatility-managed overlays remain research-only because their calendar-year baseline win rates are below 50%.
- This is still not an automatic production change. Before live migration, the next validation should be a data-provider cross-check and a concrete strategy-level implementation plan that preserves the current defensive baseline as the default/off switch.

## 2026-06-20 sleeve sensitivity live gate

To avoid selecting `liveable_blend_baseline80_fast20` as a single overfit point, the static baseline/offensive sleeve was expanded to a 10%-30% fast-sleeve ladder in 5-point increments. Command:

```bash
PYTHONPATH=src \
  /Users/lisiyi/Projects/UsEquitySnapshotPipelines/.venv/bin/python \
  -m us_equity_snapshot_pipelines.global_etf_offensive_rotation_research \
  --download \
  --price-start 2010-01-01 \
  --output-dir data/output/global_etf_sleeve_sensitivity_research_20260620
```

Data range: 2010-01-04 through 2026-06-18, 35 symbols, 141,232 price rows.

| Candidate | Live gate | Long excess vs baseline | Long drawdown delta vs baseline | Turnover delta vs baseline | Calendar win vs baseline | Rolling 3Y win vs baseline | Rolling 5Y win vs baseline | Worst rolling excess vs baseline | Action |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `liveable_blend_baseline80_fast20` | pass | +0.61% | +2.18% | +0.62 | 54.55% | 80.00% | 66.67% | -0.97% | live promotion review |
| `liveable_blend_baseline85_fast15` | pass | +0.47% | +1.66% | +0.47 | 54.55% | 80.00% | 66.67% | -0.72% | live promotion review |
| `liveable_blend_baseline90_fast10` | pass | +0.33% | +1.13% | +0.31 | 54.55% | 80.00% | 66.67% | -0.48% | live promotion review |
| `liveable_blend_baseline70_fast30` | fail | +0.84% | +3.23% | +0.94 | 54.55% | 70.00% | 55.56% | -1.47% | continue research |
| `liveable_regime_qqqtrend_baseline70_fast30` | fail | +0.80% | -0.67% | +0.98 | 45.45% | 80.00% | 88.89% | -1.65% | continue research |
| `liveable_blend_baseline75_fast25` | fail | +0.73% | +2.71% | +0.78 | 54.55% | 70.00% | 55.56% | -1.22% | continue research |
| `liveable_volmanaged_baseline70_fast30` | fail | +0.38% | -0.67% | +0.95 | 36.36% | 60.00% | 66.67% | -2.11% | continue research |

Sleeve sensitivity conclusion:

- The robust static sleeve plateau is 10%-20% fast offensive exposure. All three variants pass the baseline-relative live gate.
- The highest-excess passing point is still `liveable_blend_baseline80_fast20`, so 20% remains the current preferred live-promotion-review candidate.
- 25% and 30% fail the rolling 5Y baseline win-rate gate, so the live version should cap the offensive sleeve at 20% unless a future out-of-sample/data-source cross-check changes the evidence.
- The result strengthens the case that 80/20 is not an isolated single-parameter artifact: adjacent 85/15 and 90/10 variants also pass, but 80/20 is the upper edge of the robust plateau.

## 2026-06-20 transaction-cost stress

The default research run uses 5 bps turnover cost. To test whether the live candidate survives more conservative execution assumptions, the same downloaded price history was rerun at 10 bps, 15 bps, and 25 bps turnover cost.

Commands used the same price file from `data/output/global_etf_cost_stress_25bps_research_20260620/downloaded_price_history.csv` for 10 bps and 15 bps runs, and a fresh 25 bps download for the 25 bps run.

The runner now supports generating the same cross-cost live-readiness table in one pass:

```bash
PYTHONPATH=src \
  /Users/lisiyi/Projects/UsEquitySnapshotPipelines/.venv/bin/python \
  -m us_equity_snapshot_pipelines.global_etf_offensive_rotation_research \
  --prices data/output/global_etf_cost_stress_25bps_research_20260620/downloaded_price_history.csv \
  --turnover-cost-bps 5 \
  --cost-stress-bps 5,10,15,25 \
  --output-dir data/output/global_etf_cost_stress_bundle_20260620
```

This writes `cost_stress_live_readiness_summary.csv` without emitting full per-cost weight files.

| Turnover cost | Passing static sleeve candidates | Best passing candidate | Key failure mode |
| ---: | --- | --- | --- |
| 5 bps | 90/10, 85/15, 80/20 | `liveable_blend_baseline80_fast20` | 25/75 and 30/70 fail rolling 5Y baseline win-rate |
| 10 bps | 90/10 only | `liveable_blend_baseline90_fast10` | 85/15 and above fail rolling 5Y baseline win-rate |
| 15 bps | none | none | all static sleeves fail rolling 5Y baseline win-rate |
| 25 bps | none | none | all static sleeves fail rolling 5Y baseline win-rate; 90/10 also misses long baseline excess gate |

Detailed static-sleeve live gate output:

| Cost | Candidate | Live gate | Long excess vs baseline | Rolling 5Y win vs baseline | Worst rolling excess vs baseline | Reason |
| ---: | --- | --- | ---: | ---: | ---: | --- |
| 5 bps | `liveable_blend_baseline90_fast10` | pass | +0.33% | 66.67% | -0.48% | pass |
| 5 bps | `liveable_blend_baseline85_fast15` | pass | +0.47% | 66.67% | -0.72% | pass |
| 5 bps | `liveable_blend_baseline80_fast20` | pass | +0.61% | 66.67% | -0.97% | pass |
| 10 bps | `liveable_blend_baseline90_fast10` | pass | +0.31% | 66.67% | -0.49% | pass |
| 10 bps | `liveable_blend_baseline85_fast15` | fail | +0.44% | 55.56% | -0.74% | rolling 5Y baseline win-rate below 60% |
| 10 bps | `liveable_blend_baseline80_fast20` | fail | +0.57% | 55.56% | -0.99% | rolling 5Y baseline win-rate below 60% |
| 15 bps | `liveable_blend_baseline90_fast10` | fail | +0.29% | 55.56% | -0.50% | rolling 5Y baseline win-rate below 60% |
| 15 bps | `liveable_blend_baseline80_fast20` | fail | +0.52% | 55.56% | -1.02% | rolling 5Y baseline win-rate below 60% |
| 25 bps | `liveable_blend_baseline90_fast10` | fail | +0.25% | 55.56% | -0.53% | long baseline excess and rolling 5Y gates fail |
| 25 bps | `liveable_blend_baseline80_fast20` | fail | +0.44% | 55.56% | -1.07% | rolling 5Y baseline win-rate below 60% |

Cost-stress conclusion:

- `liveable_blend_baseline80_fast20` is the preferred candidate only under the default 5 bps execution-cost assumption.
- If the live promotion gate requires a 10 bps stress pass, the candidate should be reduced to `liveable_blend_baseline90_fast10`.
- If the live promotion gate requires 15 bps or 25 bps stress pass, none of the current static sleeves are ready for live promotion.
- Before any live implementation, execution assumptions must be fixed explicitly. For highly liquid ETF sleeves, 5 bps may be reasonable; for a conservative stress gate, this research is not yet live-ready.

## 2026-06-20 liquidity diagnostics

The runner now writes candidate-level and symbol-level liquidity diagnostics based on rolling median dollar volume:

- `candidate_liquidity_summary.csv`
- `candidate_liquidity_symbol_summary.csv`

Default settings:

- rolling dollar-volume window: 63 trading days;
- low-liquidity threshold: $50 million rolling median dollar volume;
- safe-like assets (`BIL`, `BOXX`, `SGOV`, `CASH`) are still reported, but risk-only low-liquidity weight excludes them so safe-haven history does not mask risky ETF execution quality.

Command:

```bash
PYTHONPATH=src \
  /Users/lisiyi/Projects/UsEquitySnapshotPipelines/.venv/bin/python \
  -m us_equity_snapshot_pipelines.global_etf_offensive_rotation_research \
  --prices data/output/global_etf_cost_stress_25bps_research_20260620/downloaded_price_history.csv \
  --turnover-cost-bps 5 \
  --cost-stress-bps 5,10,15,25 \
  --output-dir data/output/global_etf_liquidity_diagnostics_20260620_risk_only
```

Selected summary rows:

| Candidate | Median weighted dollar volume | Worst risk-held dollar volume | Median risk low-liquidity weight | Max risk low-liquidity weight |
| --- | ---: | ---: | ---: | ---: |
| `liveable_blend_baseline90_fast10` | $467.3M | $2.65M | 0.00% | 55.00% |
| `liveable_blend_baseline85_fast15` | $471.8M | $2.65M | 0.00% | 57.50% |
| `liveable_blend_baseline80_fast20` | $471.7M | $2.65M | 0.00% | 60.00% |
| `liveable_blend_baseline75_fast25` | $480.3M | $2.65M | 0.00% | 62.50% |
| `liveable_blend_baseline70_fast30` | $480.7M | $2.65M | 0.00% | 65.00% |
| `live_global_etf_rotation_defensive_baseline` | $466.9M | $2.77M | 0.00% | 50.00% |

Low-liquidity risk symbols observed for `liveable_blend_baseline80_fast20`:

| Symbol | Held days | Avg weight when held | Max weight | Median dollar volume | Worst dollar volume | Low-liquidity day rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `IHI` | 343 | 29.80% | 50.00% | $14.88M | $2.65M | 100.00% |
| `ITA` | 218 | 18.67% | 40.00% | $38.96M | $32.76M | 76.61% |
| `DBA` | 123 | 40.00% | 40.00% | $9.57M | $7.11M | 100.00% |

Liquidity conclusion:

- The static sleeve candidates do not show persistent low-liquidity exposure: median risk low-liquidity weight is 0% across 90/10 through 70/30.
- However, all variants can experience high low-liquidity risk exposure in specific windows because the existing defensive baseline can hold lower-dollar-volume ETFs such as `IHI`, `ITA`, and `DBA`.
- This is not a blocker for the fast sleeve itself, but live migration should include execution rules: use limit orders, avoid market-open/close liquidity stress, and consider replacing or capping low-dollar-volume baseline ETFs if account size grows.
- Liquidity diagnostics support using 5-10 bps as plausible stress assumptions for small-to-moderate notional, but 15-25 bps remains the conservative stress band.

## 2026-06-20 dynamic execution-cost NAV stress

Flat bps cost stress is useful, but it does not distinguish a high-volume ETF trade from a low-volume ETF trade. The runner now supports a deterministic dynamic execution-cost model using only daily close/volume data:

- base cost bps: default 5 bps;
- low-liquidity penalty: based on 63-day rolling median dollar volume versus the $50 million threshold, capped at 25 bps;
- participation penalty: optional, based on estimated trade dollars divided by rolling dollar volume; default threshold is 2% of rolling dollar volume, capped at 25 bps;
- cost is applied by traded symbol weight, so the same total turnover costs more when the rebalance touches lower-liquidity or higher-participation symbols.

This is still a conservative proxy, not a replacement for live bid/ask spread or broker execution data.

Command:

```bash
PYTHONPATH=src \
  /Users/lisiyi/Projects/UsEquitySnapshotPipelines/.venv/bin/python \
  -m us_equity_snapshot_pipelines.global_etf_offensive_rotation_research \
  --prices data/output/global_etf_dynamic_cost_research_20260620/downloaded_price_history.csv \
  --turnover-cost-bps 5 \
  --cost-stress-bps 5,10,15,25 \
  --dynamic-cost \
  --dynamic-cost-navs 100000,250000,1000000 \
  --output-dir data/output/global_etf_dynamic_cost_nav_stress_20260620
```

Data range: 2010-01-04 through 2026-06-18, 35 symbols, 141,232 price rows.

Selected dynamic-cost live gate rows:

| NAV | Candidate | Live gate | Long excess vs baseline | Rolling 5Y win vs baseline | Annualized cost drag | Max effective cost on trade days | Max participation | Reason |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| $100k | `liveable_blend_baseline85_fast15` | pass | +0.46% | 66.67% | 0.28% | 17.50 bps | 1.50% | pass |
| $100k | `liveable_blend_baseline90_fast10` | pass | +0.32% | 66.67% | 0.26% | 17.50 bps | 1.58% | pass |
| $100k | `liveable_blend_baseline80_fast20` | fail | +0.59% | 55.56% | 0.29% | 17.50 bps | 1.41% | rolling 5Y baseline win-rate below 60% |
| $250k | `liveable_blend_baseline85_fast15` | pass | +0.46% | 66.67% | 0.28% | 17.50 bps | 3.74% | pass |
| $250k | `liveable_blend_baseline90_fast10` | pass | +0.32% | 66.67% | 0.26% | 17.50 bps | 3.96% | pass |
| $250k | `liveable_blend_baseline80_fast20` | fail | +0.59% | 55.56% | 0.29% | 17.50 bps | 3.52% | rolling 5Y baseline win-rate below 60% |
| $1,000k | `liveable_blend_baseline85_fast15` | pass | +0.46% | 66.67% | 0.30% | 23.45 bps | 14.96% | pass |
| $1,000k | `liveable_blend_baseline90_fast10` | pass | +0.32% | 66.67% | 0.29% | 24.10 bps | 15.84% | pass |
| $1,000k | `liveable_blend_baseline80_fast20` | fail | +0.60% | 55.56% | 0.31% | 22.82 bps | 14.08% | rolling 5Y baseline win-rate below 60% |

Dynamic-cost conclusion:

- Under the current dynamic proxy, `liveable_blend_baseline80_fast20` is no longer the preferred live candidate because the stricter rolling 5Y baseline win-rate gate falls to 55.56%.
- `liveable_blend_baseline85_fast15` is the current preferred candidate across the tested NAV ladder: it has higher long excess than 90/10 while still passing the live gate at $100k, $250k, and $1M.
- `liveable_blend_baseline90_fast10` remains the more conservative fallback if the live rule prioritizes lowest execution sensitivity over return uplift.
- No runtime manifest should change until the execution model assumptions are approved. For live design, use `85/15` as the leading candidate, keep `90/10` as fallback, and treat `80/20` as research-only unless real execution data proves costs are materially lower than this proxy.

## 2026-06-20 walk-forward / OOS promotion gate

To reduce the winner-picking risk from repeatedly comparing multiple sleeve candidates, the runner now writes walk-forward selection diagnostics:

- train window: previous 5 calendar years;
- test window: next calendar year only;
- selection rule: among the allowed liveable candidates, pick the highest training-window excess CAGR versus the current live baseline, subject to positive train excess and no more than 3 percentage points drawdown degradation; if no candidate qualifies, keep the baseline for that OOS year;
- OOS gate: at least 3 promoted OOS windows, OOS win-rate at least 50%, positive median OOS excess CAGR, worst OOS excess CAGR no worse than -3 percentage points, and worst OOS drawdown degradation no worse than -3 percentage points.

This is intentionally harsher than the full-sample live gate. It tests whether the candidate-selection process itself survives forward-only evaluation.

### Full candidate-set walk-forward

Command:

```bash
PYTHONPATH=src \
  /Users/lisiyi/Projects/UsEquitySnapshotPipelines/.venv/bin/python \
  -m us_equity_snapshot_pipelines.global_etf_offensive_rotation_research \
  --prices data/output/global_etf_dynamic_cost_research_20260620/downloaded_price_history.csv \
  --turnover-cost-bps 5 \
  --cost-stress-bps 5,10,15,25 \
  --dynamic-cost \
  --dynamic-cost-navs 100000,250000,1000000 \
  --output-dir data/output/global_etf_dynamic_cost_oos_20260620
```

Result across the NAV ladder:

| NAV | OOS windows | Promoted windows | OOS win rate | Median OOS excess | Worst OOS excess | Selected candidates | Gate |
| ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| $100k | 7 | 6 | 50.00% | -0.15% | -14.32% | 70/30 static, QQQ-trend 70/30 | fail |
| $250k | 7 | 6 | 50.00% | -0.15% | -14.32% | 70/30 static, QQQ-trend 70/30 | fail |
| $1M | 7 | 6 | 50.00% | -0.15% | -14.28% | 70/30 static, QQQ-trend 70/30 | fail |

Conclusion: the full candidate-set selection process is not live-ready. The training window tends to select aggressive 70/30 variants, which then fail OOS, especially in the 2025 partial-year window.

### Safe-sleeve walk-forward (`85/15`, `90/10` only)

Command:

```bash
PYTHONPATH=src \
  /Users/lisiyi/Projects/UsEquitySnapshotPipelines/.venv/bin/python \
  -m us_equity_snapshot_pipelines.global_etf_offensive_rotation_research \
  --prices data/output/global_etf_dynamic_cost_research_20260620/downloaded_price_history.csv \
  --turnover-cost-bps 5 \
  --cost-stress-bps 5,10,15,25 \
  --dynamic-cost \
  --dynamic-cost-navs 100000,250000,1000000 \
  --walk-forward-candidates liveable_blend_baseline85_fast15,liveable_blend_baseline90_fast10 \
  --output-dir data/output/global_etf_dynamic_cost_oos_safe_sleeves_20260620
```

Result across the NAV ladder:

| NAV | OOS windows | Promoted windows | OOS win rate | Median OOS excess | Worst OOS excess | Selected candidates | Gate |
| ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| $100k | 7 | 5 | 60.00% | +1.03% | -7.27% | 85/15 four times, 90/10 once | fail |
| $250k | 7 | 5 | 60.00% | +1.03% | -7.27% | 85/15 four times, 90/10 once | fail |
| $1M | 7 | 5 | 60.00% | +1.03% | -7.25% | 85/15 four times, 90/10 once | fail |

Safe-sleeve conclusion:

- Restricting the candidate set to `85/15` and `90/10` materially improves OOS behavior: win-rate rises to 60% and median OOS excess is positive.
- It still fails the current live gate because the worst OOS excess CAGR is about -7.3 percentage points, worse than the -3 percentage point threshold.
- Current recommendation: do **not** auto-promote any global ETF offensive sleeve yet. The best next research direction is to add a deterministic drawdown/regime brake to `85/15`, then re-run the same dynamic-cost and walk-forward gates without expanding the candidate search space.

## 2026-06-20 trend/drawdown brake follow-up

Two deterministic `85/15` brake candidates were added after the safe-sleeve OOS failure:

- `liveable_trend_drawdown_brake_baseline85_fast15_floor10`: normally holds the 15% fast sleeve, but reduces it to 10% on the next monthly rebalance when QQQ is below its 200-day SMA, QQQ fast momentum is negative, or the 63-day QQQ drawdown is worse than -8%.
- `liveable_trend_drawdown_brake_baseline85_fast15_floor0`: same signal, but reduces the fast sleeve to 0%, returning to the current defensive baseline.

These are still research-only portfolio construction rules. They do not require a plugin and do not change the live manifest.

### Floor 10 brake

Command:

```bash
PYTHONPATH=src \
  /Users/lisiyi/Projects/UsEquitySnapshotPipelines/.venv/bin/python \
  -m us_equity_snapshot_pipelines.global_etf_offensive_rotation_research \
  --prices data/output/global_etf_dynamic_cost_research_20260620/downloaded_price_history.csv \
  --turnover-cost-bps 5 \
  --cost-stress-bps 5,10,15,25 \
  --dynamic-cost \
  --dynamic-cost-navs 100000,250000,1000000 \
  --walk-forward-candidates liveable_trend_drawdown_brake_baseline85_fast15_floor10,liveable_blend_baseline90_fast10 \
  --output-dir data/output/global_etf_dynamic_cost_oos_brake_only_20260620
```

Result:

| NAV | OOS windows | Promoted windows | OOS win rate | Median OOS excess | Worst OOS excess | Selected candidates | Gate |
| ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| $100k | 7 | 5 | 60.00% | +0.81% | -6.72% | floor10 brake five times | fail |
| $250k | 7 | 5 | 60.00% | +0.81% | -6.72% | floor10 brake five times | fail |
| $1M | 7 | 5 | 60.00% | +0.81% | -6.70% | floor10 brake five times | fail |

### Floor 0 brake

Command:

```bash
PYTHONPATH=src \
  /Users/lisiyi/Projects/UsEquitySnapshotPipelines/.venv/bin/python \
  -m us_equity_snapshot_pipelines.global_etf_offensive_rotation_research \
  --prices data/output/global_etf_dynamic_cost_research_20260620/downloaded_price_history.csv \
  --turnover-cost-bps 5 \
  --cost-stress-bps 5,10,15,25 \
  --dynamic-cost \
  --dynamic-cost-navs 100000,250000,1000000 \
  --walk-forward-candidates liveable_trend_drawdown_brake_baseline85_fast15_floor0,liveable_blend_baseline90_fast10 \
  --output-dir data/output/global_etf_dynamic_cost_oos_brake_floor0_20260620
```

Result:

| NAV | OOS windows | Promoted windows | OOS win rate | Median OOS excess | Worst OOS excess | Selected candidates | Gate |
| ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| $100k | 7 | 6 | 50.00% | +0.01% | -4.87% | floor0 brake four times, 90/10 twice | fail |
| $250k | 7 | 6 | 50.00% | +0.01% | -4.87% | floor0 brake four times, 90/10 twice | fail |
| $1M | 7 | 6 | 50.00% | +0.01% | -4.86% | floor0 brake four times, 90/10 twice | fail |

Brake conclusion:

- The brake works directionally: worst OOS excess improves from about -7.3% (`85/15`/`90/10`) to -6.7% with floor10, and to -4.9% with floor0.
- It still fails the live OOS gate. Floor0 is also too defensive in the full-sample live gate because calendar baseline win-rate falls below 50%.
- Current recommendation remains unchanged: keep the current defensive baseline live. The offensive sleeve is not ready for default live promotion.
- Further research should avoid adding more unconstrained variants. If continuing, test either a stricter promotion threshold that keeps baseline when train excess is marginal, or a regime rule driven by the strategy's own baseline-relative drawdown rather than QQQ-only drawdown.

## 2026-06-20 minimum train-edge walk-forward threshold

The walk-forward selection rule now supports `--walk-forward-min-train-excess-cagr`. The default is `0.0`, which preserves the previous behavior: any positive training-window excess CAGR can qualify. A positive threshold requires the candidate's training-window excess CAGR versus the live baseline to be at least the configured value before it can be promoted into the next OOS year.

This was tested on the more defensive candidate set:

- `liveable_trend_drawdown_brake_baseline85_fast15_floor0`
- `liveable_blend_baseline90_fast10`

Command pattern:

```bash
PYTHONPATH=src \
  /Users/lisiyi/Projects/UsEquitySnapshotPipelines/.venv/bin/python \
  -m us_equity_snapshot_pipelines.global_etf_offensive_rotation_research \
  --prices data/output/global_etf_dynamic_cost_research_20260620/downloaded_price_history.csv \
  --turnover-cost-bps 5 \
  --cost-stress-bps 5,10,15,25 \
  --dynamic-cost \
  --dynamic-cost-navs 100000,250000,1000000 \
  --walk-forward-candidates liveable_trend_drawdown_brake_baseline85_fast15_floor0,liveable_blend_baseline90_fast10 \
  --walk-forward-min-train-excess-cagr <threshold> \
  --output-dir data/output/global_etf_dynamic_cost_oos_brake_floor0_mintrain_<threshold>_20260620
```

Threshold ladder result:

| Min train excess | Promotion windows | Keep-baseline windows | OOS win rate | Median OOS excess | Worst OOS excess | Gate |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 0.50% | 4 | 3 | 50.00% | +0.26% | -4.87% | fail |
| 0.75% | 2 | 5 | 50.00% | -1.26% | -4.87% | fail |
| 1.00% | 0 | 7 | n/a | n/a | n/a | fail |

Minimum-train-edge conclusion:

- A 0.50% threshold skips some marginal promotions but still promotes `90/10` into the weak 2025 OOS window, leaving worst OOS excess around -4.9%.
- A 0.75% threshold promotes too rarely and still includes the 2025 failure.
- A 1.00% threshold keeps the baseline every year; this is safe but provides no evidence that an offensive sleeve is live-ready.
- Current recommendation remains unchanged: no Global ETF offensive sleeve should be default live. The only live-safe action is to keep the existing defensive baseline.

## 2026-06-20 baseline-relative decay brake candidate

A single pre-registered Global ETF follow-up candidate has been added to avoid
continuing an unconstrained parameter grid:

- `liveable_baseline_relative_decay_brake_baseline90_fast10_floor0` normally
  holds the 90/10 defensive-baseline/fast-offensive blend.
- On the next monthly rebalance it cuts the fast sleeve to 0% only when the fast
  sleeve's trailing 63-day gross return lags the defensive baseline by more than
  3 percentage points **and** its trailing 126-day gross return also lags the
  baseline.
- The brake is strategy-relative rather than QQQ-only, so it tests whether the
  offensive sleeve is currently adding value over the already-live defensive
  profile.
- This is still research-only. It does not change the current live
  `global_etf_rotation` defensive baseline, runtime manifest, or broker
  behavior.

Actual narrow gate run:

```bash
PYTHONPATH=src uv run python -m us_equity_snapshot_pipelines.global_etf_offensive_rotation_research \
  --prices data/output/global_etf_baseline_relative_decay_brake_20260620/downloaded_price_history.csv \
  --variants live_global_etf_rotation_defensive_baseline,offensive_growth_fast_top2_monthly \
  --liveable-composites liveable_baseline_relative_decay_brake_baseline90_fast10_floor0,liveable_blend_baseline90_fast10 \
  --robustness-candidates liveable_baseline_relative_decay_brake_baseline90_fast10_floor0,liveable_blend_baseline90_fast10,live_global_etf_rotation_defensive_baseline,offensive_growth_fast_top2_monthly \
  --walk-forward-candidates liveable_baseline_relative_decay_brake_baseline90_fast10_floor0,liveable_blend_baseline90_fast10 \
  --walk-forward-min-train-excess-cagr 0.005 \
  --turnover-cost-bps 5 \
  --cost-stress-bps 5,10,15,25 \
  --dynamic-cost \
  --dynamic-cost-navs 100000,250000,1000000 \
  --output-dir data/output/global_etf_baseline_relative_decay_brake_narrow_20260620
```

Result summary:

| Evidence | `liveable_blend_baseline90_fast10` | `liveable_baseline_relative_decay_brake_baseline90_fast10_floor0` |
| --- | ---: | ---: |
| Research gate | pass | pass |
| Live-readiness long excess vs baseline | +0.33% | +0.11% |
| Calendar baseline CAGR win rate | 54.55% | 45.45% |
| Rolling 5Y baseline CAGR win rate | 66.67% | 55.56% |
| Live-readiness gate | pass | fail |
| Dynamic-cost NAV stress live gate, $100k/$250k/$1M | pass/pass/pass | fail/fail/fail |
| Walk-forward gate after dynamic costs | fail | not selected |
| Worst OOS excess vs baseline after dynamic costs | about -4.86% to -4.87% | n/a |

Conclusion:

- The baseline-relative decay brake does **not** improve the live case. It is
  too defensive/late: long excess falls to about +0.11%, calendar baseline win
  rate drops below 50%, rolling 5Y baseline win rate is below 60%, and the
  live-readiness gate fails under static and dynamic-cost assumptions.
- The static `90/10` sleeve still looks better than the brake in live-readiness
  and dynamic-cost summaries, but it fails the walk-forward gate because the
  2025 OOS window is about -4.9 percentage points below the current defensive
  baseline.
- Therefore the Global ETF conclusion remains unchanged: no offensive sleeve
  should be default live. Keep the current defensive baseline live.


## 2026-06-20 live-decay monitor hook

A generic evidence-only live-decay monitor is now available as
`useq-build-live-decay-monitor`. For Global ETF offensive research, use it after a
candidate has already passed the existing backtest, dynamic-cost, and
walk-forward gates; do not use it to rescue a failed candidate.

Intended command pattern:

```bash
uv run useq-build-live-decay-monitor \
  --returns data/output/global_etf_offensive_rotation_research_YYYYMMDD/portfolio_returns_with_benchmarks.csv \
  --input-format wide \
  --strategies liveable_blend_baseline90_fast10,live_global_etf_rotation_defensive_baseline \
  --primary-benchmark QQQ \
  --secondary-benchmark SPY \
  --windows 63,126,252 \
  --output-dir data/output/global_etf_live_decay_YYYYMMDD
```

Outputs:

- `live_decay_window_summary.csv`
- `live_decay_strategy_summary.csv`
- `live_decay_report.md`
- `live_decay_monitor_manifest.json`

Design notes:

- A `review` state is a human follow-up signal only. It does not switch the
  live strategy, sell holdings, or change the runtime manifest.
- The monitor supports optional `expected_excess_cagr_vs_primary` inputs so the
  same trailing 3/6/12-month evidence can be compared against a frozen
  backtest-implied edge.
- For Global ETF, the live conclusion is still unchanged: keep the current
  defensive baseline unless a pre-registered offensive sleeve passes the OOS and
  cost gates first.

Actual monitor run on the narrow baseline-relative decay output:

```bash
PYTHONPATH=src uv run python -m us_equity_snapshot_pipelines.live_decay_monitor \
  --returns data/output/global_etf_baseline_relative_decay_brake_narrow_20260620/portfolio_returns_with_benchmarks.csv \
  --input-format wide \
  --strategies liveable_baseline_relative_decay_brake_baseline90_fast10_floor0,liveable_blend_baseline90_fast10,live_global_etf_rotation_defensive_baseline \
  --primary-benchmark QQQ \
  --secondary-benchmark SPY \
  --windows 63,126,252 \
  --min-observations 60 \
  --output-dir data/output/global_etf_baseline_relative_decay_live_decay_20260620
```

Recent-decay result:

| Strategy | Overall decay state | Triggered window | Worst excess vs QQQ | Worst excess vs SPY |
| --- | --- | --- | ---: | ---: |
| `liveable_baseline_relative_decay_brake_baseline90_fast10_floor0` | review | trailing 63D | -128.25% | -49.39% |
| `liveable_blend_baseline90_fast10` | review | trailing 63D | -128.25% | -49.39% |
| `live_global_etf_rotation_defensive_baseline` | review | trailing 63D | -143.95% | -65.09% |

This monitor result is not a trading instruction; it is additional evidence
that the current high-beta rebound regime is unfavorable for promoting a Global
ETF offensive sleeve by default.

Research context:

- Bailey and López de Prado's Deflated Sharpe Ratio work supports discounting
  performance evidence after multiple trials and short/non-normal samples:
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551
- Bailey et al.'s PBO/CSCV work reinforces that apparently strong in-sample
  strategy choices can fail out of sample:
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253
- Daniel and Moskowitz's momentum-crash work supports monitoring momentum-like
  sleeves for high-volatility rebound failure modes:
  https://www.nber.org/papers/w20439
- Moreira and Muir's volatility-management evidence supports treating realized
  volatility/decay as a risk-control input, but not as proof that this Global ETF
  offensive sleeve is live-ready:
  https://www.nber.org/papers/w22208
