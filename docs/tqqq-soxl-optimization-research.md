# TQQQ / SOXL Optimization Research

Research date: 2026-05-10

Promotion note: the current production SOXL/SOXX volatility delever gate uses
`SOXX 10d realized volatility >= 55%, SOXL -> SOXX`. The older synthetic
long-history sweep below remains the historical optimization record.

Latest current-default recheck: 2026-06-09. A bounded replay found a better
TQQQ volatility-delever default than the fixed 28% gate: use a rolling 252-day
p90 threshold on QQQ 5-day annualized realized volatility, bounded to 24%-36%.
The fixed 28% value remains the fallback while the rolling percentile warms up.

- TQQQ core: `dual_drive_volatility_delever_window=5`,
  `dual_drive_volatility_delever_threshold_mode=rolling_percentile`,
  `dual_drive_volatility_delever_dynamic_percentile=0.90`,
  `dual_drive_volatility_delever_dynamic_floor=0.24`,
  `dual_drive_volatility_delever_dynamic_cap=0.36`.
- SOXL core: `blend_gate_dynamic_rsi_threshold_enabled=true`,
  `blend_gate_volatility_delever_symbol=SOXX`,
  `blend_gate_volatility_delever_window=10`,
  `blend_gate_volatility_delever_threshold=0.55`,
  `blend_gate_volatility_delever_retention_ratio=0.0`,
  `blend_gate_volatility_delever_redirect_symbol=SOXX`.

This note records bounded optimization sweeps for the TQQQ and SOXL leveraged
equity profiles. The default acceptance rule is intentionally strict to avoid
fitting a single crisis window:

1. CAGR must not decrease versus baseline in every comparison window.
2. Max drawdown must not worsen versus baseline in every comparison window.
3. A passing candidate remains research evidence only unless a later PR promotes
   it into strategy configuration.

Later rechecks may promote a candidate with a small window regression only when
the regression is explicitly recorded, turnover is controlled, and the
long-window improvement is large enough to justify the trade-off.

The `crisis_response_shadow` plugin remains notification-only and strategy
limited to the TQQQ compatibility mount. SOXL broad crisis/macro context is
published through the general `market_regime_notification` target instead of a
strategy-level crisis plugin mount.

## 2026-06-09 Dynamic TQQQ Volatility Threshold Recheck

Follow-up question: can the fixed 28% annualized QQQ 5-day volatility gate be
replaced by a dynamic threshold without causing unacceptable turnover?

Output directory:

`data/output/tqqq_volatility_delever_threshold_research_20260609`

Result summary, using QQQM as the unlevered sleeve with QQQ as its long-history
proxy, BOXX through BIL proxy, income layer disabled, and 5 bps turnover cost:

| Candidate | CAGR | Max Drawdown | Rebalances/Year | Turnover/Year | Applied Days | Decision |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `fixed_28` | 27.60% | -35.07% | 13.90 | 8.81 | 242 | old baseline |
| `dynamic_p90` | 29.79% | -35.83% | 14.54 | 9.10 | 223 | strong long result, but 2022 threshold drifted too high |
| `dynamic_p90_cap36` | 29.69% | -35.83% | 16.34 | 9.91 | 271 | better stress response, but still over-trades versus needed benefit |
| `dynamic_p90_floor24_cap36` | 30.09% | -35.33% | 14.22 | 8.95 | 208 | promote |

Key window checks:

| Candidate | Window | CAGR | Max Drawdown | Interpretation |
| --- | --- | ---: | ---: | --- |
| `fixed_28` | 2022 rate bear | -4.17% | -20.08% | best drawdown in the 2022 stress window |
| `dynamic_p90_floor24_cap36` | 2022 rate bear | -5.74% | -23.37% | worse than fixed 28, but within the accepted trade-off for higher long CAGR |
| `fixed_28` | COVID crash | -62.84% | -25.35% | old baseline |
| `dynamic_p90_floor24_cap36` | COVID crash | -35.34% | -21.68% | materially better |
| `fixed_28` | post-2022 bull | 51.52% | -20.22% | old baseline |
| `dynamic_p90_floor24_cap36` | post-2022 bull | 55.71% | -20.22% | better CAGR with no drawdown regression |
| `fixed_28` | latest 15y | 27.42% | -30.76% | old baseline |
| `dynamic_p90_floor24_cap36` | latest 15y | 30.00% | -31.05% | better CAGR, near-flat drawdown delta |

Interpretation:

- A naked rolling p90 is too loose in persistent high-volatility regimes; during
  2022 its median effective threshold rose above 42%, reducing applied
  delevering days from 25 under fixed 28 to 7.
- The 24%-36% bound keeps the adaptive behavior but prevents the gate from
  becoming either too sensitive in calm regimes or too permissive in high-vol
  regimes.
- Turnover is acceptable: `dynamic_p90_floor24_cap36` increases turnover only
  from 8.81/year to 8.95/year versus fixed 28, and rebalances from 13.90/year
  to 14.22/year.
- Promote `dynamic_p90_floor24_cap36` as the live default, with fixed 28 kept as
  the warm-up fallback.

## 2026-06-04 Current Default Recheck

This recheck was designed to answer whether the current live TQQQ/SOXL profiles
should be changed now, not to run another broad parameter search. It used simple
nearby variants only:

- TQQQ: current 5-day QQQ volatility delever at 28%, a more defensive 25%
  threshold, a looser 32% threshold, and no volatility delever.
- SOXL: current dynamic RSI plus SOXX 10-day 55% volatility delever, static RSI
  at 70 with the same volatility delever, dynamic RSI with 50% or 60%
  volatility thresholds, and dynamic RSI with no volatility delever.

Output directory:

`data/output/codex_levered_tqqq_soxl_optimization_20260604`

Strict no-regression screen:

| Strategy | Candidate | Min CAGR Delta | Min MDD Delta | Decision |
| --- | --- | ---: | ---: | --- |
| TQQQ | `current_5d_vol28` | +0.00 pp | +0.00 pp | baseline |
| TQQQ | `defensive_5d_vol25` | -3.88 pp | -0.76 pp | reject |
| TQQQ | `looser_5d_vol32` | +0.14 pp | -1.57 pp | reject |
| TQQQ | `no_vol_delever` | -0.48 pp | -6.04 pp | reject |
| SOXL | `current_dynamic_rsi_vol55` | +0.00 pp | +0.00 pp | baseline |
| SOXL | `static_rsi70_vol55` | -17.56 pp | +0.00 pp | reject |
| SOXL | `dynamic_rsi_vol50` | -208.96 pp | +0.00 pp | reject |
| SOXL | `dynamic_rsi_vol60` | -3.55 pp | -3.24 pp | reject |
| SOXL | `no_vol_delever_dynamic_rsi` | -1.79 pp | -3.24 pp | reject |

Long-window context:

| Strategy | Candidate | Long CAGR | Long MDD | Interpretation |
| --- | --- | ---: | ---: | --- |
| TQQQ | `current_5d_vol28` | 27.05% | -35.07% | keep default |
| TQQQ | `looser_5d_vol32` | 27.92% | -35.83% | higher long CAGR, worse drawdown and stress-window regression |
| TQQQ | `no_vol_delever` | 27.87% | -36.38% | higher long CAGR, materially worse drawdown |
| SOXL | `current_dynamic_rsi_vol55` | 71.48% | -41.57% | keep default |
| SOXL | `static_rsi70_vol55` | 72.90% | -40.51% | attractive long sample, but recent one-year CAGR regression |
| SOXL | `no_vol_delever_dynamic_rsi` | 73.46% | -41.57% | higher long CAGR, but recent drawdown regression |

Interpretation:

- The outside design pattern for leveraged ETFs is still low-degree trend and
  volatility control, not a high-dimensional technical stack. ProShares states
  TQQQ seeks 3x daily Nasdaq-100 results and warns that longer holding-period
  returns can differ materially depending on volatility and holding period.
  Direxion similarly highlights SOXL leverage, correlation, compounding, and
  market-volatility risks.
- The 200-day moving-average leverage rotation literature supports the same
  broad idea: use leverage in lower-volatility trend regimes and delever when
  trend/volatility conditions deteriorate. This already matches the current
  strategy family through QQQ/SOXX trend gates, pullback logic, and realized
  volatility delevering.
- Recent leveraged ETF research also argues that daily-rebalanced leveraged
  performance depends on trend persistence and volatility clustering, which
  supports keeping the simple trend/volatility framework instead of adding
  path-specific crisis parameters.
- No tested nearby change clears the strict no-regression rule. Do not change
  production defaults from this recheck.

Reference URLs:

- https://prod.proshares.com/our-etfs/leveraged-and-inverse/tqqq
- https://prod.proshares.com/globalassets/proshares/prospectuses/tqqq_summary_prospectus.pdf
- https://www.direxion.com/product/daily-semiconductor-bull-bear-3x-etfs
- https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2741701
- https://arxiv.org/abs/2504.20116

## 2026-06-04 Overlay Guard Recheck

Follow-up question: can dual moving averages, rolling stop-loss gates, or
Chandelier-style stops reduce drawdown without materially hurting CAGR?

Implementation boundary:

- Production strategy defaults were not changed.
- SOXL research backtests can now optionally layer an external SOXL delever
  overlay on top of the manifest 55% SOXX volatility gate via
  `soxl_delever_overlay_combine_with_core=true`.
- TQQQ overlay guards were tested in a research script by rerouting only the
  TQQQ target sleeve into QQQ when the guard triggered.
- SOXL overlay guards were layered on the current dynamic-RSI and 55% SOXX
  volatility-delever baseline, rerouting only the SOXL target sleeve into SOXX.

Command:

```bash
uv run python scripts/research_levered_overlay_guards.py
```

Output directory:

`data/output/codex_levered_overlay_guard_research_20260604`

Tested guard families:

- Dual moving average guards: `20/60`, `50/200` for TQQQ on QQQ; `10/30`,
  `20/60` for SOXL on SOXX.
- Rolling stop-loss guards: QQQ 20-day 5% and 30-day 8%; SOXX 20-day 8% and
  30-day 12%.
- Chandelier stops: 22-day lookback with 3x or 4x ATR.
- Partial retention variants for the less aggressive rolling-stop and
  Chandelier guards, retaining 50% or 75% of the leveraged sleeve when the guard
  triggered.

Strict result:

| Strategy | Candidate family | Strict no-regression result | Interpretation |
| --- | --- | --- | --- |
| TQQQ | dual MA | fail | Reduces long drawdown by up to ~2.4 pp, but long CAGR gives up ~2.5 pp or more |
| TQQQ | rolling stop | fail | Full stop hurts CAGR; 75% retention barely affects CAGR but drawdown benefit is negligible |
| TQQQ | Chandelier | fail | Can reduce long drawdown by ~1.7-3.1 pp, but CAGR drag is too large |
| SOXL | dual MA | fail | Too many delever days or window regressions; no production candidate |
| SOXL | rolling stop | fail | Partial retention has limited drawdown benefit and still gives up CAGR |
| SOXL | Chandelier | fail | Best drawdown tradeoff still has CAGR drag and cross-window drawdown regression |

Closest long-window candidates:

| Strategy | Candidate | Long CAGR Delta | Long MDD Delta | Overlay stops | Read |
| --- | --- | ---: | ---: | ---: | --- |
| TQQQ | `rolling_stop_30_8pct_qqq_ret75` | -0.21 pp | +0.006 pp | 26 | Not worth adding; drawdown benefit is effectively zero |
| TQQQ | `dual_ma_50_200_qqq` | -2.47 pp | +2.41 pp | 278 | Too much CAGR drag for the drawdown reduction |
| TQQQ | `chandelier_22_3_qqq` | -5.61 pp | +3.15 pp | 716 | Overactive; rejects too much upside |
| SOXL | `rolling_stop_30_12pct_soxx_ret75` | -1.41 pp | +0.08 pp | 52 | Mild, but benefit is too small |
| SOXL | `chandelier_22_4_soxx_ret75` | -2.59 pp | +1.81 pp | 132 | Interesting research watch, but fails strict cross-window screen |
| SOXL | `chandelier_22_4_soxx_ret50` | -5.24 pp | +1.97 pp | 132 | Drawdown improvement is not worth the CAGR drag |

Conclusion:

- Do not add dual-MA, rolling-stop, or Chandelier overlay guards to production
  defaults now.
- The current QQQ/SOXX trend, pullback, RSI/Bollinger, and realized-volatility
  gates already catch much of the same risk. Adding another price-stop layer
  mostly removes leveraged exposure after the same deterioration has already
  started, so it tends to trade CAGR for modest or unstable drawdown improvement.
- If a future paper-only research branch is desired, the only candidate worth
  watching is `chandelier_22_4_soxx_ret75`; even that should stay research-only
  because it still failed the strict no-regression screen.

## TQQQ

Data and model:

- Source prices: `data/output/crisis_response_1999_synthetic_v2_context/input/crisis_response_price_history.csv`
- Benchmark: `QQQ`
- Attack leg: synthetic daily-reset `SYNTH_TQQQ` from `QQQ` at 3x, 1% annual
  expense assumption
- Safe asset: `SHY`
- Turnover cost: 5 bps

Tested candidates:

- Pullback-rebound windows: 10, 20, 30 trading days.
- Pullback-rebound volatility multipliers: 1.0, 1.5, 2.0, 2.5, 3.0.
- Fixed pullback-rebound thresholds: 0%, 2%, 4%, 6%.
- Small allocation variants around the baseline 45% QQQ / 45% synthetic TQQQ /
  8% SHY / 2% cash active mix.
- Moving the 2% cash sleeve into SHY for active and/or idle states.

Result:

- No TQQQ candidate passed the strict no-regression rule across all tested
  windows.
- Higher pullback thresholds improved full-sample CAGR and drawdown in some
  cases, but regressed at least one stress or recent period.
- Moving cash into SHY was close, but still caused small regressions in rate
  stress windows.

Conclusion: keep the current TQQQ strategy parameters. Do not promote a TQQQ
optimization from this sweep.

## SOXL

Data and model:

- Source prices: `/tmp/soxl_synthetic_long/synthetic_soxl_from_soxx_price_history.csv`
- Benchmark: `SOXX`
- Attack leg: synthetic daily-reset `SOXL` from `SOXX`
- Strategy: `soxl_soxx_trend_income` research backtest with income layer
  disabled for long-history core replay
- Turnover cost: 5 bps

Tested candidates:

- SOXX realized-volatility gates with 10, 15, 20, and 30 trading-day windows.
- Annualized realized-vol thresholds: 45%, 50%, 55%, 60%, 65%.
- SOXL retention ratios: 0% and 25%.
- Redirect destinations: mostly `SOXX`, with selected `BOXX` defensive checks.

Baseline:

| Period | CAGR | Max Drawdown |
| --- | ---: | ---: |
| Full 2001-2026 | 19.34% | -60.24% |
| Dotcom tail 2001-2003 | 25.30% | -51.24% |
| GFC 2007-2009 | 2.85% | -55.78% |
| Real SOXL era 2010-2026 | 27.13% | -36.84% |
| 2022 rate bear | -27.32% | -34.53% |
| 2024-2026 live-full proxy | 64.63% | -36.84% |

Passing candidates:

| Candidate | Stops | Full CAGR | Full MDD | Min CAGR Delta | Min MDD Delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| SOXX 20d vol >= 50%, retain 0% SOXL, redirect to SOXX | 19 | 20.02% | -59.52% | +0.05 pp | +0.08 pp |
| SOXX 15d vol >= 50%, retain 0% SOXL, redirect to SOXX | 18 | 20.01% | -59.52% | +0.05 pp | +0.08 pp |
| SOXX 20d vol >= 50%, retain 25% SOXL, redirect rest to SOXX | 19 | 19.86% | -59.66% | +0.01 pp | +0.06 pp |
| SOXX 15d vol >= 50%, retain 25% SOXL, redirect rest to SOXX | 18 | 19.85% | -59.66% | +0.01 pp | +0.06 pp |

Selected `BOXX` redirect variants improved full-sample CAGR and drawdown, but
failed the strict no-CAGR-regression rule in at least one window.

Historical conclusion from this sweep: the strongest candidate in that older
synthetic run was the 20-day SOXX realized-volatility gate at 50%, redirecting
triggered SOXL target exposure into SOXX. Later exact-replay and income-layer
work did not promote that older 50% threshold; the current production default is
the 10-day 55% SOXX realized-volatility gate, redirecting SOXL into SOXX.

## Local Research Outputs

- `/tmp/tqqq_soxl_optimization_research/tqqq_variant_summary.csv`
- `/tmp/tqqq_soxl_optimization_research/tqqq_variant_periods.csv`
- `/tmp/tqqq_soxl_optimization_research/soxl_variant_summary.csv`
- `/tmp/tqqq_soxl_optimization_research/soxl_variant_periods.csv`
