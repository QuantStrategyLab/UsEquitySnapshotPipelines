# TQQQ Volatility Delever Recheck

Review date: 2026-06-13

Decision: keep the current TQQQ volatility-delever default. The recent repeated
trigger is expected under the current short-term volatility regime and does not
justify raising the threshold.

## Current Live Parameters

The live `tqqq_growth_income` manifest uses:

- `dual_drive_volatility_delever_window=5`
- `dual_drive_volatility_delever_threshold_mode=rolling_percentile`
- `dual_drive_volatility_delever_dynamic_lookback=252`
- `dual_drive_volatility_delever_dynamic_percentile=0.90`
- `dual_drive_volatility_delever_dynamic_min_periods=126`
- `dual_drive_volatility_delever_dynamic_floor=0.24`
- `dual_drive_volatility_delever_dynamic_cap=0.36`

The fixed `dual_drive_volatility_delever_threshold=0.28` field remains the
warm-up fallback, not the normal live trigger once enough rolling history exists.

## Method

This was a narrow recheck, not a new broad optimization sweep. The goal was to
answer whether the recent repeated TQQQ volatility-delever trigger indicates a
bad live parameter.

Replay setup:

- Strategy entry point: `tqqq_growth_income_archive.run_backtest`
- Research script baseline:
  `scripts/research_tqqq_volatility_delever_thresholds.py`
- Price data: latest available TQQQ strategy publish artifact through
  2026-06-12
- TQQQ core replay with income layer, market-regime control, macro risk
  governor, and crisis defense disabled to isolate the local TQQQ/QQQ
  volatility-delever gate
- QQQM proxy: copied from QQQ
- BOXX proxy: constant 100 cash series, because the latest artifact did not
  include BOXX/BIL and live yfinance access was rate-limited during the review
- Turnover cost: default archive setting

The BOXX proxy limitation affects absolute cash-sleeve returns. It does not
affect the QQQ 5-day realized-volatility trigger calculation, and all variants
were compared against the same proxy. A BIL-backed replay should be preferred
before any future parameter-promotion PR.

## Full-Period Results

Full replay period: 2010-11-16 through 2026-06-12.

| Variant | CAGR | Max Drawdown | Sharpe | Trigger Days | Trigger Rate | State Changes | Decision |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `current_p90_f24_c36` | 29.17% | -35.33% | 1.088 | 213 | 5.44% | 151 | keep |
| `p90_f26_c36` | 29.17% | -35.80% | 1.083 | 172 | 4.39% | 119 | no change; near-flat CAGR but worse drawdown |
| `p85_f24_c36` | 29.13% | -34.36% | 1.095 | 265 | 6.77% | 179 | research watch only; more sensitive |
| `p90_f24_c38` | 28.80% | -35.33% | 1.075 | 201 | 5.13% | 149 | reject |
| `p90_f24_c34` | 28.78% | -35.33% | 1.080 | 225 | 5.74% | 151 | reject |
| `p90_f22_c36` | 28.55% | -35.83% | 1.072 | 236 | 6.03% | 175 | reject |
| `p95_f24_c36` | 28.27% | -35.98% | 1.052 | 146 | 3.73% | 99 | reject; fewer triggers but weaker long result |
| `fixed30` | 27.82% | -35.53% | 1.045 | 168 | 4.29% | 111 | reject |
| `fixed32` | 27.57% | -35.83% | 1.032 | 134 | 3.42% | 87 | reject |
| `p90_f28_c40` | 27.19% | -35.80% | 1.016 | 130 | 3.32% | 103 | reject |

The current parameter remains the best full-period CAGR and has the strongest
full-period Sharpe among the less-sensitive variants. `p85_f24_c36` improves
drawdown and Sharpe slightly, but it increases trigger frequency and state
changes; that is not a clean live replacement.

## Window Checks

| Variant | Window | CAGR | Max Drawdown | Interpretation |
| --- | --- | ---: | ---: | --- |
| `current_p90_f24_c36` | latest 15 years | 29.75% | -28.46% | best long-window CAGR among reviewed variants |
| `p85_f24_c36` | latest 15 years | 29.62% | -27.38% | slightly smoother, more active |
| `p95_f24_c36` | latest 15 years | 28.87% | -28.66% | looser gate loses return |
| `fixed32` | latest 15 years | 28.14% | -28.49% | fewer triggers, weaker return |
| `current_p90_f24_c36` | 2022 rate bear | -5.87% | -23.38% | not the best 2022 row, but acceptable in long trade-off |
| `fixed30` | 2022 rate bear | -4.16% | -21.78% | better 2022 row, worse long result |
| `current_p90_f24_c36` | COVID crash | -35.37% | -21.68% | best tested crash-window result |
| `fixed32` | COVID crash | -64.89% | -26.92% | materially worse crash response |
| `current_p90_f24_c36` | post-2022 bull | 50.88% | -20.32% | near top result with better Sharpe than looser gates |
| `p95_f24_c36` | post-2022 bull | 50.94% | -20.32% | marginally higher CAGR, weaker full-period result |

No higher-threshold variant gives a robust enough improvement to justify a
production change. The alternatives that reduce trigger count mostly do so by
accepting weaker long-window return or weaker crash-window behavior.

## Recent Trigger Review

Latest available signal row:

- Signal date: 2026-06-11
- Effective date: 2026-06-12
- QQQ 5-day annualized realized volatility metric: 50.44%
- Current dynamic threshold: 24.21%
- Triggered: true

Recent current-parameter trigger dates:

- 2026-06-05
- 2026-06-08
- 2026-06-09
- 2026-06-10
- 2026-06-11

Recent trigger rates for the current parameter:

| Recent signal window | Trigger Days | Trigger Rate |
| --- | ---: | ---: |
| Last 15 signal days | 5 | 33.33% |
| Last 30 signal days | 5 | 16.67% |
| Last 60 signal days | 5 | 8.33% |
| Last 126 signal days | 5 | 3.97% |
| Last 252 signal days | 9 | 3.57% |

The recent trigger cluster is real, but it is not parameter-specific. The
latest metric is above 50%, so `p95_f24_c36`, `fixed30`, `fixed32`, and
`p90_f28_c40` also trigger over the same latest short window. Avoid raising the
threshold to fit this episode; doing so would make the gate too permissive in a
future crash.

## Conclusion

Keep `dynamic_p90_floor24_cap36` as the live TQQQ volatility-delever default.

The current gate is not over-triggering on the long sample: it triggered on
about 5.4% of full-period signal days in the official strategy replay. The June
2026 cluster is explained by unusually high short-term QQQ realized volatility,
not by a broken threshold.

No strategy code or manifest patch is recommended from this review.
