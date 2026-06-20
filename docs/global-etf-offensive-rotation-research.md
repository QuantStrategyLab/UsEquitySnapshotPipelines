# Global ETF Offensive Rotation Research

This is a research-only track for testing whether an offensive Global ETF profile can behave more like the Russell leader-rotation line: prioritize broad-market outperformance while keeping the existing live `global_etf_rotation` defensive profile unchanged.

## Boundary

- No live manifest or broker behavior is changed by this research.
- The current `global_etf_rotation` profile remains the defensive baseline.
- Offensive candidates are deterministic, rule-based, and backtestable; AI is not part of signal generation.
- Passing pure offensive candidates are marked `paper_review_only`, not auto-promoted to live.
- Passing liveable composites are marked `live_design_review`; this still does not change live runtime by itself.

## Candidate set

The runner compares:

1. `live_global_etf_rotation_defensive_baseline` — current quarterly top-2 confidence/vol-gated baseline.
2. `offensive_growth_top2_monthly` — aggressive growth/cyclical ETF pool, monthly equal-weight top 2.
3. `offensive_growth_top1_monthly` — concentrated monthly top 1.
4. `offensive_growth_top2_conf75_monthly` — monthly top 2 with 75/25 confidence tilt when the top name is decisive and volatility is acceptable.
5. `offensive_growth_top2_weak_canary_monthly` — disables daily all-BIL canary exits to measure the offensive trade-off.
6. `offensive_growth_eaa_top2_monthly` — EAA-inspired generalized momentum score: high momentum, lower volatility, lower correlation to the offensive pool.
7. `offensive_growth_fast_top2_monthly` — VAA-inspired fast 1/3/6-month momentum score with SMA eligibility.
8. `offensive_growth_daa_cash_fraction_top2_monthly` — DAA-inspired canary breadth: each bad canary adds a proportional safe-haven sleeve.
9. `offensive_growth_eaa_daa_cash_fraction_monthly` — combines EAA-style selection with DAA-style proportional canary cash fraction.

Liveable composite candidates are generated only when both child strategies exist in the run:

10. `liveable_blend_baseline80_fast20` — 80% current defensive baseline + 20% fast offensive sleeve.
11. `liveable_blend_baseline70_fast30` — 70% current defensive baseline + 30% fast offensive sleeve.
12. `liveable_regime_qqqtrend_baseline70_fast30` — 30% fast offensive sleeve only when QQQ is above its 200-day trend and fast momentum is positive; otherwise 100% defensive baseline.
13. `liveable_volmanaged_baseline70_fast30` — same QQQ trend gate, but scales the fast sleeve down when 63-day realized QQQ volatility is above 18%.

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

## Gate

Primary benchmark is SPY. Secondary benchmark is QQQ.

A candidate only passes research gate when:

- all configured short/medium/long windows have enough data;
- CAGR and Sharpe are positive in every window;
- long-window CAGR beats SPY;
- median cross-window CAGR beats SPY;
- it is QQQ-competitive, meaning either long-window CAGR beats QQQ or worst drawdown is at least 5 percentage points better than QQQ.

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
- `rebalance_events.csv`
- `weights_<candidate>.csv`
- `downloaded_price_history.csv`
- `recommendation.md`
- `run_manifest.json`

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
- `liveable_blend_baseline80_fast20`
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

- The most promising liveable line is `liveable_blend_baseline70_fast30`: it improves long-window CAGR versus the current baseline by about 0.84 percentage points, improves max drawdown by about 3.23 percentage points, and keeps turnover much lower than the pure offensive sleeve.
- The dynamic QQQ-trend and volatility-managed overlays are valid backtestable rules, but in this run they did not beat the simpler fixed 70/30 sleeve. They remain research candidates, not preferred defaults.
- Even the best composite should not be auto-promoted yet: calendar-year and rolling robustness still show inconsistent SPY/QQQ outperformance. `live_design_review` means “worth manual strategy-design review,” not “ready for production default.”
- If this line moves toward live, implement it as a strategy-level sleeve/overlay feature that consumes deterministic child strategy weights. It should not be a plugin: the plugin architecture should continue to produce signals only, while this rule is a transparent, backtestable portfolio construction rule.
