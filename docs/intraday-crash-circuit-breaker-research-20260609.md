# Intraday Crash Circuit Breaker Research - 2026-06-09

## Question

Evaluate whether the crypto-platform daily crash circuit breaker pattern should be adapted to the TQQQ and SOXL U.S. equity strategies.

The tested rule is:

- Check the risk sleeve hourly.
- If the intraday risk-sleeve return from the previous daily close is below the threshold, exit the risk sleeve for the rest of that trading day.
- Re-enter on the next trading day according to the normal strategy weights.
- Apply a one-way 5 bps circuit execution cost on the risk sleeve.

## Data And Scope

- Strategies: current local core profiles for `tqqq_growth_income` and `soxl_soxx_trend_income`.
- Income sleeves: disabled for this research because the available local research price files are core-only.
- Daily prices: existing local research artifacts from `data/output/*_research_20260609/normalized_price_history.csv`.
- Hourly prices: Yahoo chart 1h data downloaded through public proxy pools.
- Hourly coverage: `2024-06-10 13:30:00` to `2026-06-03 19:30:00`, 13,844 rows across `TQQQ`, `QQQM`, `SOXL`, and `SOXX`.
- Comparison window: `2024-06-10` to `2026-06-02`.

Yahoo hourly data uses raw intraday price scale, while the local daily research files can be adjusted or proxy-scaled. The research code aligns each symbol's hourly close to the local daily close by date before computing intraday threshold breaches.

## Main Results

| Profile | Variant | Events | CAGR | CAGR Delta | Max Drawdown | MaxDD Delta | Sharpe |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TQQQ core | baseline | 0 | 49.17% | 0.00 pp | -16.62% | 0.00 pp | 1.59 |
| TQQQ core | circuit -5% | 9 | 48.56% | -0.61 pp | -19.07% | -2.45 pp | 1.57 |
| SOXL core | baseline | 0 | 197.61% | 0.00 pp | -34.25% | 0.00 pp | 2.24 |
| SOXL core | circuit -5% | 57 | 219.98% | +22.37 pp | -32.51% | +1.74 pp | 2.47 |

## Threshold Sweep

TQQQ does not benefit from the crypto-style hourly crash circuit breaker in this window. The -5% version slightly reduces CAGR and worsens drawdown. A -4% threshold improves CAGR by 0.77 pp but worsens drawdown by 4.41 pp, so it is not an attractive risk-control tradeoff.

SOXL benefits materially around the -4% to -6% range:

| SOXL Variant | Events | CAGR Delta | MaxDD Delta | Sharpe |
| --- | ---: | ---: | ---: | ---: |
| circuit -3% | 104 | +4.91 pp | +3.94 pp | 2.38 |
| circuit -4% | 73 | +12.70 pp | +3.56 pp | 2.42 |
| circuit -5% | 57 | +22.37 pp | +1.74 pp | 2.47 |
| circuit -6% | 44 | +29.20 pp | +0.56 pp | 2.49 |
| circuit -7% | 33 | +24.54 pp | -3.46 pp | 2.44 |

## Conclusion

Do not add the hourly crash circuit breaker to TQQQ based on this test.

For SOXL, the rule is worth promoting to a deeper validation pass. If we want to mirror the crypto platform exactly, -5% is the clean default candidate. If optimizing this specific U.S. equity window, -6% had the best CAGR and Sharpe, but with less drawdown improvement than -5%.

Before live promotion, validate on a broader data source if available. Yahoo's free hourly history only covered the recent window, so this is not yet a full-history intraday study.

## Longer-Period Follow-Up

After the initial 1h study, two daily OHLC proxy studies were added to test longer periods:

- `daily_close_proxy`: optimistic proxy. It triggers only when the daily risk-sleeve close breaches the threshold and assumes threshold fill.
- `daily_low_proxy`: pessimistic proxy. It triggers when the daily risk-sleeve low breaches the threshold and assumes threshold or gap-open fill. This overstates trigger frequency versus an hourly-close monitor.

The true hourly result for the overlap window sits between these two proxies, so the proxies should be read as a sensitivity range rather than as exact live behavior.

### True 1h Slices

| Profile | Window | Variant | Events | CAGR Delta | MaxDD Delta |
| --- | --- | --- | ---: | ---: | ---: |
| TQQQ core | 2024H2 | fixed -5% | 5 | +4.36 pp | +0.31 pp |
| TQQQ core | 2025 | fixed -5% | 3 | +0.62 pp | +0.00 pp |
| TQQQ core | 2026YTD | fixed -5% | 1 | -12.53 pp | -2.50 pp |
| SOXL core | 2024H2 | fixed -5% | 8 | +26.80 pp | +7.73 pp |
| SOXL core | 2025 | fixed -5% | 31 | -11.79 pp | -0.35 pp |
| SOXL core | 2026YTD | fixed -5% | 18 | +757.24 pp | +1.74 pp |

SOXL's recent hourly result is positive overall, but it is not uniformly positive across every sub-period. The fixed -5% rule lost CAGR in 2025 and gained strongly in 2024H2 and 2026YTD.

### Long-History Proxy Read

| Profile | Proxy | Variant | Full CAGR Delta | Full MaxDD Delta |
| --- | --- | --- | ---: | ---: |
| TQQQ core | daily close | fixed -5% | +5.18 pp | +4.85 pp |
| TQQQ core | daily low | fixed -5% | -1.31 pp | +1.79 pp |
| TQQQ core | daily close | dynamic vol 0.75x | +25.29 pp | +17.84 pp |
| TQQQ core | daily low | dynamic vol 0.75x | -2.64 pp | +2.42 pp |
| SOXL core | daily close | fixed -5% | +101.22 pp | +11.57 pp |
| SOXL core | daily low | fixed -5% | -4.40 pp | -3.01 pp |
| SOXL core | daily close | dynamic vol 0.75x | +194.79 pp | +21.34 pp |
| SOXL core | daily low | dynamic vol 0.75x | -5.10 pp | +0.74 pp |

The large spread between daily-close and daily-low proxies means the long-history evidence is inconclusive without real historical intraday bars. It does, however, show that a naive continuous low-trigger version would be too sensitive.

### Updated Recommendation

Keep TQQQ unchanged.

For SOXL, do not promote a hard fixed -5% rule directly. If this feature moves forward, use a volatility-scaled hourly-close rule with a bounded threshold, then validate with better intraday history:

```text
intraday_circuit_threshold = -clip(k * rolling_20d_risk_sleeve_vol, 0.03, 0.06)
```

Initial candidates are `k=0.75` and `k=1.0`. The trigger should be based on hourly close, not daily low or continuous low, to avoid excessive false exits.

## Rolling Window Check Within 15 Years

Additional rolling-window summaries were generated from the two long-history proxy return streams:

- `rolling_window_detail.csv`: every evaluated calendar, trailing, and rolling window.
- `rolling_window_scorecard.csv`: win rates and median/worst deltas by window type.

The trailing windows ending `2026-06-02` show why the proxy conclusion remains conservative.

### TQQQ Trailing Windows

| Proxy | Window | fixed -5% CAGR Delta | fixed -5% MaxDD Delta | dynamic 0.75x CAGR Delta | dynamic 0.75x MaxDD Delta |
| --- | --- | ---: | ---: | ---: | ---: |
| daily close | trailing 1y | +3.1 pp | -0.0 pp | +24.2 pp | +3.8 pp |
| daily close | trailing 5y | +2.6 pp | +1.1 pp | +23.4 pp | +9.7 pp |
| daily close | trailing 15y | +5.3 pp | +5.3 pp | +25.4 pp | +11.6 pp |
| daily low | trailing 1y | -4.2 pp | -3.6 pp | -20.4 pp | -6.7 pp |
| daily low | trailing 5y | -3.9 pp | -3.7 pp | -7.8 pp | +1.5 pp |
| daily low | trailing 15y | -1.1 pp | +0.6 pp | -2.7 pp | +2.3 pp |

TQQQ only looks attractive under the optimistic daily-close proxy. Under daily-low sensitivity, all trailing CAGR deltas are negative. This supports leaving TQQQ unchanged.

### SOXL Trailing Windows

| Proxy | Window | fixed -5% CAGR Delta | fixed -5% MaxDD Delta | dynamic 0.75x CAGR Delta | dynamic 0.75x MaxDD Delta |
| --- | --- | ---: | ---: | ---: | ---: |
| daily close | trailing 1y | +1396.4 pp | +18.1 pp | +2037.2 pp | +21.0 pp |
| daily close | trailing 5y | +98.4 pp | +10.5 pp | +192.2 pp | +20.2 pp |
| daily close | trailing 10y | +101.2 pp | +11.6 pp | +194.8 pp | +21.3 pp |
| daily low | trailing 1y | -203.5 pp | -3.8 pp | -248.1 pp | -2.2 pp |
| daily low | trailing 5y | -6.5 pp | -2.8 pp | -17.5 pp | -3.1 pp |
| daily low | trailing 10y | -4.4 pp | -3.0 pp | -5.1 pp | +0.7 pp |

SOXL's sensitivity range is even wider. The optimistic proxy says the rule is extremely valuable; the pessimistic low-trigger proxy says it can over-trigger and destroy recent gains. Because true hourly checks use hourly close, not daily low, the live-like answer should sit between those boundaries.

### Scorecard Read

For daily-low sensitivity, `fixed -5%` and `dynamic 0.75x` both had 0% CAGR win rate across trailing windows for both TQQQ and SOXL. That is the main reason this research still stops short of live promotion.

The next useful step is not more parameter sweeping on daily proxies. It is obtaining a longer true intraday history or implementing SOXL as a shadow signal first and comparing live hourly-close triggers against the daily strategy for several months.

## Broker Data Source Probe

IBKR was tested through the running Gateway VM in `interactivebrokersquant` using read-only TWS API calls. `formatDate=2` was required because the Gateway container did not have Python `tzdata`, and the default `US/Eastern` timestamp parsing path failed in `ib_insync`.

Results:

- `SOXL`, `SOXX`, `TQQQ`, and `QQQ` all returned `15 Y / 1 hour / TRADES / useRTH=True` history.
- Each symbol returned roughly 26.3k hourly bars.
- Local files were written under `data/output/ibkr_hourly_15y_20260609/`.
- `QQQ` was duplicated as `QQQM` for the long-history TQQQ proxy, matching the existing local daily proxy approach.

LongBridge was also tested with the `longbridgequant` paper OpenAPI credentials:

- Quote and recent `Period.Min_60` candlesticks worked for `SOXL.US` and `TQQQ.US`.
- `history_candlesticks_by_date(..., Period.Min_60, ...)` failed for 2011, 2020, 2021, 2022, 2023, and 2024 with `out of minute kline begin date`.
- 2025 and 2026 minute/hour history worked.

So IBKR is currently the viable source for true 15-year hourly research. LongBridge is useful for recent hourly validation but not for the 15-year study.

## True IBKR 15-Year Hourly Result

Using the IBKR hourly files, the fixed-threshold hourly-close study was rerun over the true long intraday window.

| Profile | Variant | Events | CAGR Delta | MaxDD Delta | Sharpe |
| --- | --- | ---: | ---: | ---: | ---: |
| TQQQ core | fixed -3% | 310 | -1.24 pp | +2.55 pp | 1.12 |
| TQQQ core | fixed -5% | 66 | +0.47 pp | -0.78 pp | 1.16 |
| TQQQ core | fixed -8% | 7 | +0.04 pp | -1.34 pp | 1.14 |
| SOXL core | fixed -3% | 523 | +7.72 pp | -2.22 pp | 1.53 |
| SOXL core | fixed -5% | 268 | +0.01 pp | -4.81 pp | 1.41 |
| SOXL core | fixed -8% | 83 | +6.26 pp | +1.53 pp | 1.47 |

The true IBKR hourly data changes the prior proxy conclusion:

- TQQQ still does not have a compelling crash-circuit candidate. Fixed -5% adds a little CAGR but worsens max drawdown, and tighter thresholds reduce CAGR.
- SOXL fixed -5% is not attractive over the long true-hourly window. It is essentially flat on CAGR and materially worsens drawdown.
- SOXL fixed -8% is the best simple threshold from this run because it improves both CAGR and max drawdown.

The volatility-scaled threshold with `floor=3%` and `cap=6%` was also tested:

| Profile | Variant | Events | Avg Threshold | CAGR Delta | MaxDD Delta | Sharpe |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| TQQQ core | dynamic 0.75x | 310 | -3.00% | -1.24 pp | +2.55 pp | 1.12 |
| TQQQ core | dynamic 1.00x | 304 | -3.04% | -1.14 pp | +2.55 pp | 1.12 |
| SOXL core | dynamic 1.00x | 402 | -3.95% | +5.31 pp | -3.19 pp | 1.48 |
| SOXL core | dynamic 2.00x | 209 | -5.62% | +2.30 pp | -1.38 pp | 1.43 |

Dynamic thresholds with the earlier `3%-6%` bounds were too tight for SOXL if max drawdown improvement is required. The next SOXL candidate should therefore test a looser volatility-scaled band, for example `5%-9%`, and compare it directly against fixed -8%.

## Daily / Hourly / 15-Minute Scheme Follow-Up

The follow-up test expanded the question from a binary crash circuit breaker into a three-layer trading scheme:

- Daily bars still decide the core direction and target weights.
- Hourly bars decide intraday risk state: fixed exit, volatility-scaled exit, two-step half/full de-risking, and same-day re-entry after recovery.
- 15-minute bars estimate execution timing impact for weight changes: first 15-minute bar, first hour, mid-day, TWAP, and last 15-minute bar.

IBKR was able to provide 15-year 15-minute history, but not through a single `15 Y / 15 mins` request. The successful path was chunked retrieval:

- Most symbols worked with `5 Y / 15 mins` chunks.
- `QQQ` needed additional `1 Y` and `6 M` chunks after some 5-year chunks timed out.
- Final local 15-minute file: `data/output/ibkr_15m_15y_20260609/ibkr_15y_15m_combined_for_research_complete.csv`.
- Final coverage: about 97.5k rows per symbol for `TQQQ`, `QQQ`, `QQQM`, `SOXL`, and `SOXX`, covering June 2011 to June 2026.

The reproducible command is:

```bash
PYTHONPATH=/home/ubuntu/Projects/UsEquityStrategies/src:/home/ubuntu/Projects/UsEquitySnapshotPipelines/src \
  .venv/bin/python scripts/research_intraday_scheme.py \
  --output-dir data/output/ibkr_intraday_scheme_research_20260610 \
  --hourly-prices data/output/ibkr_hourly_15y_20260609/ibkr_15y_1h_combined_for_research.csv \
  --intraday-15m-prices data/output/ibkr_15m_15y_20260609/ibkr_15y_15m_combined_for_research_complete.csv
```

Outputs:

- `summary.csv`: full-period summary by profile and variant.
- `hourly_events.csv`: hourly risk events.
- `execution_events.csv`: 15-minute execution timing adjustments.
- `period_detail.csv`: full, trailing, calendar-year, and regime windows.
- `window_scorecard.csv`: win rates and median/worst deltas by window type.

### Full-Period Result

| Profile | Variant | Events | CAGR Delta | MaxDD Delta | Sharpe Delta |
| --- | --- | ---: | ---: | ---: | ---: |
| TQQQ core | hourly dynamic 1.5x, 5%-9%, same-day re-entry | 66 | +0.91 pp | +0.81 pp | +0.04 |
| TQQQ core | hourly two-step -5%/-8% | 66 | +0.27 pp | +0.34 pp | +0.01 |
| TQQQ core | execution first 15m | 219 | -2.60 pp | -4.32 pp | -0.08 |
| TQQQ core | execution last 15m | 219 | -5.93 pp | -15.59 pp | -0.18 |
| SOXL core | hourly fixed -8% exit | 83 | +6.26 pp | +1.53 pp | +0.09 |
| SOXL core | hourly two-step -5%/-8% | 268 | +3.39 pp | -0.30 pp | +0.06 |
| SOXL core | execution first 15m | 324 | +4.87 pp | +3.01 pp | +0.06 |
| SOXL core | execution last 15m | 324 | -4.57 pp | -2.83 pp | -0.05 |

### Window Scorecard Read

For TQQQ, the best hourly rule was the volatility-scaled `5%-9%` same-day re-entry version. It improved the full-period result, but it was not universally robust:

- Calendar windows: 50% CAGR win rate, 56% max-drawdown win rate.
- Regime windows: 67% CAGR win rate, 50% max-drawdown win rate.
- Trailing windows: 60% CAGR win rate, 20% max-drawdown win rate.

This is better than a hard fixed -5% or -8% circuit, but still not strong enough to promote automatically. TQQQ should stay as a shadow/research feature unless we add a stricter regime filter.

For SOXL, fixed hourly -8% remained the strongest risk overlay:

- Calendar windows: 70% CAGR win rate, 70% max-drawdown win rate.
- Regime windows: 100% CAGR win rate, 80% max-drawdown win rate.
- Trailing windows: 100% CAGR win rate, 80% max-drawdown win rate.

This supports SOXL fixed -8% hourly-close risk control as the cleanest candidate.

### 15-Minute Layer Interpretation

The 15-minute layer is useful, but it should not be treated as a separate alpha signal yet. The current model estimates execution timing impact by comparing target-weight execution at different intraday slots versus the close-to-close daily backtest assumption. It does not model order book depth, live spreads, partial fills, or venue-level routing.

The result still gives a practical design rule:

- Do not wait until late day by default. `last_15m` was materially worse for TQQQ and worse for SOXL over the full period.
- If the daily system changes weights, the first 15-minute bar is the safest default execution slot among the tested choices.
- For SOXL, first-15-minute execution looked better than the daily close-to-close assumption over the full period, but this should be read as timing sensitivity rather than guaranteed edge.

### Updated Recommendation

Hourly and 15-minute bars are not limited to crash circuit breakers:

- Hourly bars can handle risk-state transitions: exit, half de-risk, re-entry, and volatility-scaled thresholds.
- 15-minute bars should handle execution quality: first executable slot, delayed execution, TWAP comparison, and monitoring whether late execution is harmful.

Implementation recommendation:

- TQQQ: do not enable a hard crash circuit. Keep `hourly_dynamic_1_5_5_9_reentry_half` as a shadow rule only.
- SOXL: promote `hourly_fixed_8_exit` to the next validation stage as the primary candidate.
- Execution: if we add 15-minute logic to production, start with first-15-minute execution monitoring, not a complex optimizer.

## Additional Validation Pass

The next pass checked three remaining risks:

- Whether the IBKR intraday files had hidden data-quality issues.
- Whether SOXL fixed `-8%` was a one-point overfit.
- Whether the hourly rule still works after adding a 15-minute execution timing assumption.

During the data-quality audit, most short sessions matched expected U.S. equity half days or the final incomplete current day. A few non-half-day gaps appeared in the `SMART` historical source around `2017-01-05`, `2018-09-17`, `2019-08-05`, and `2019-09-13`. Re-querying the same dates with an explicit exchange, instead of `SMART`, returned complete bars. The local research files were therefore patched into v2 files:

- `data/output/ibkr_hourly_15y_20260609/ibkr_15y_1h_combined_for_research_complete_v2.csv`
- `data/output/ibkr_15m_15y_20260609/ibkr_15y_15m_combined_for_research_complete_v2.csv`

The v2 data-quality summary showed:

- No duplicate `symbol,time` rows.
- No nonpositive closes.
- Median full-session bars were exactly `7` for 1-hour data and `26` for 15-minute data.
- Remaining short sessions were 34-35 days per symbol, consistent with scheduled half days plus data-window boundaries.

The v2 reproducible commands are:

```bash
PYTHONPATH=/home/ubuntu/Projects/UsEquityStrategies/src:/home/ubuntu/Projects/UsEquitySnapshotPipelines/src \
  .venv/bin/python scripts/research_intraday_scheme.py \
  --output-dir data/output/ibkr_intraday_scheme_research_v2_20260610 \
  --hourly-prices data/output/ibkr_hourly_15y_20260609/ibkr_15y_1h_combined_for_research_complete_v2.csv \
  --intraday-15m-prices data/output/ibkr_15m_15y_20260609/ibkr_15y_15m_combined_for_research_complete_v2.csv

PYTHONPATH=/home/ubuntu/Projects/UsEquityStrategies/src:/home/ubuntu/Projects/UsEquitySnapshotPipelines/src \
  .venv/bin/python scripts/research_intraday_scheme_validation.py \
  --output-dir data/output/ibkr_intraday_scheme_validation_v2_20260610 \
  --hourly-prices data/output/ibkr_hourly_15y_20260609/ibkr_15y_1h_combined_for_research_complete_v2.csv \
  --intraday-15m-prices data/output/ibkr_15m_15y_20260609/ibkr_15y_15m_combined_for_research_complete_v2.csv
```

### v2 Core Result

| Profile | Variant | Events | CAGR Delta | MaxDD Delta | Sharpe |
| --- | --- | ---: | ---: | ---: | ---: |
| TQQQ core | hourly dynamic 1.5x, 5%-9%, same-day re-entry | 66 | +1.04 pp | +0.81 pp | 1.17 |
| TQQQ core | execution first 15m | 219 | -2.60 pp | -4.32 pp | 1.06 |
| TQQQ core | execution last 15m | 219 | -5.93 pp | -15.59 pp | 0.96 |
| SOXL core | hourly fixed -8% exit | 83 | +6.24 pp | +1.53 pp | 1.47 |
| SOXL core | execution first 15m | 324 | +4.87 pp | +3.01 pp | 1.44 |
| SOXL core | execution last 15m | 324 | -4.57 pp | -2.83 pp | 1.33 |

### Threshold Robustness

The fixed-threshold sweep from `-3%` to `-12%` confirmed the earlier conclusion:

- TQQQ fixed thresholds do not produce a clean risk/return improvement. Tighter `-3%` and `-4%` improve drawdown but reduce CAGR; `-5%` improves CAGR but worsens drawdown.
- SOXL has a useful plateau around `-8%` to `-9%`. `-8%` is still the best full-period risk/return tradeoff: `+6.24 pp` CAGR delta and `+1.53 pp` max-drawdown delta. `-9%` still improves CAGR by `+5.11 pp`, but max-drawdown improvement falls to only `+0.19 pp`.

### Cost Robustness

Cost sensitivity was run at `0`, `5`, `10`, `20`, and `50` bps one-way circuit cost:

| Profile | Candidate | 5 bps CAGR Delta | 20 bps CAGR Delta | 50 bps CAGR Delta | Read |
| --- | --- | ---: | ---: | ---: | --- |
| TQQQ | dynamic 1.5x, 5%-9%, same-day re-entry | +1.04 pp | +0.18 pp | -1.52 pp | Too cost-sensitive for live promotion |
| TQQQ | two-step -5%/-8% | +0.33 pp | -0.12 pp | -1.03 pp | Weak |
| SOXL | fixed -8% exit | +6.24 pp | +4.12 pp | -0.06 pp | Robust up to normal/slightly high costs |
| SOXL | two-step -5%/-8% | +3.36 pp | -0.84 pp | -8.96 pp | Too many events; cost-sensitive |

### Combined Hourly + 15-Minute Execution

The combined overlay applies the hourly risk rule and then applies the 15-minute execution timing adjustment. This is still a research approximation, not a fill simulator, but it is a stricter check than looking at the two layers separately.

| Profile | Combined Variant | CAGR Delta | MaxDD Delta | Read |
| --- | --- | ---: | ---: | --- |
| TQQQ | dynamic re-entry + first 15m | -1.57 pp | -6.87 pp | Reject for live |
| TQQQ | dynamic re-entry + last 15m | -4.94 pp | -17.66 pp | Reject |
| SOXL | fixed -8% + first 15m | +11.28 pp | +4.44 pp | Strongest candidate |
| SOXL | fixed -8% + last 15m | +1.35 pp | -1.37 pp | Avoid late execution |
| SOXL | two-step -5%/-8% + first 15m | +8.35 pp | -0.16 pp | Return improves, drawdown does not |

### Final Validation Read

The remaining validation strengthens the practical recommendation:

- TQQQ: do not promote intraday risk control now. The hourly-only dynamic re-entry rule looks acceptable in isolation, but execution-aware validation turns it negative.
- SOXL: fixed `-8%` hourly-close exit remains the best candidate. It survives data cleanup, threshold sweep, and normal cost assumptions.
- SOXL execution: if implemented, pair the `-8%` hourly rule with first-15-minute execution monitoring. Late-day execution materially weakens the result.
- Avoid the two-step half/full rule for production. It adds complexity and event count without enough drawdown improvement.
