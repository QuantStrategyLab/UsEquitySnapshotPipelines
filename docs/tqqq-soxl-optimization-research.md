# TQQQ / SOXL Optimization Research

Research date: 2026-05-10

Promotion note: the current production SOXL/SOXX volatility delever gate uses
`SOXX 10d realized volatility >= 50%, SOXL -> SOXX`, promoted from the exact
real-product replay in `soxl-soxx-chandelier-stop-research.md`. The synthetic
long-history sweep below remains the historical optimization record.

This note records a bounded optimization sweep for the TQQQ and SOXL leveraged
equity profiles. The acceptance rule is intentionally strict to avoid fitting a
single crisis window:

1. CAGR must not decrease versus baseline in every comparison window.
2. Max drawdown must not worsen versus baseline in every comparison window.
3. A passing candidate remains research evidence only unless a later PR promotes
   it into strategy configuration.

The `crisis_response_shadow` plugin remains notification-only and strategy
limited to the TQQQ compatibility mount. SOXL broad crisis/macro context is
published through the general `market_regime_notification` target instead of a
strategy-level crisis plugin mount.

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

Conclusion: the strongest SOXL research candidate is the 20-day SOXX
realized-volatility gate at 50%, redirecting triggered SOXL target exposure into
SOXX. It passed this bounded sweep, but should remain research-only until a
separate PR promotes it with implementation tests and operator review.

## Local Research Outputs

- `/tmp/tqqq_soxl_optimization_research/tqqq_variant_summary.csv`
- `/tmp/tqqq_soxl_optimization_research/tqqq_variant_periods.csv`
- `/tmp/tqqq_soxl_optimization_research/soxl_variant_summary.csv`
- `/tmp/tqqq_soxl_optimization_research/soxl_variant_periods.csv`
