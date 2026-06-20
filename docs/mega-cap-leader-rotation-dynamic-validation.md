# Russell Top50 Leader Rotation Dynamic Universe Validation

This note records the point-in-time validation added for the dynamic Russell
Top50 leader-rotation research. It is research-only and must not be used to
promote a runtime profile by itself.

## Current Conclusion

Static MAGS / static MAG7 style pools remain rejected for runtime promotion because
they are structurally hindsight-biased.

Dynamic Top50 Top2 is not rejected. It remains the aggressive candidate if an
approximately `-40%` max drawdown is acceptable, but it is still sensitive to
universe availability timing. With refreshed Top50 prices, adding a
one-trading-day universe lag reduced the Top2 CAGR from `48.69%` to `37.89%`
and worsened max drawdown from `-34.22%` to `-38.79%`. The edge remains, but
the timing sensitivity is material.

Dynamic Top50 is more promising than leveraged MAGS because it produced similar
or better returns without requiring 2x products. The cleaner candidate after
the expanded grid is `top4_cap25_no_defense` using a lagged universe baseline:
`31.06%` CAGR, `-27.28%` max drawdown, and `1.14` Calmar under a 21-trading-day
universe lag. `top3_cap35_no_defense_sector2` is the higher-return candidate at
`31.33%` CAGR and `-29.29%` max drawdown. Top2 remains an aggressive variant,
not the default.

The longest honest validation available in this repo starts from the first
point-in-time Top50 universe snapshot (`2017-09-29`). On the `2017-10-02` to
`2026-04-16` sample, the lagged Top2 variant improved QQQ materially, but one
3-year window still trailed QQQ slightly. This keeps the idea in research:
promising, but not yet runtime-enabled.

The concentration-variant research found a cleaner balance than either pure
Top2 or a threshold-based dynamic switch: a fixed `50% Top2 / 50% Top4` sleeve
mix produced `36.41%` CAGR and `-30.56%` max drawdown. The dynamic Top2
drawdown switch reduced drawdown too, but it was more parameter-sensitive and
had higher turnover.

The rebalance-frequency and daily-risk research did not improve the current
balanced candidate. Monthly rebalancing remained better than weekly or
biweekly rebalancing, and daily soft/hard risk exposure cuts reduced returns
without improving max drawdown enough to justify the complexity.

## Validation Setup

- Validation date: `2026-04-17`
- Universe input:
  `data/output/mega_cap_leader_rotation_dynamic_universe_top50_backtest/input/mega_cap_leader_rotation_dynamic_top50_universe_history.csv`
- Refreshed price input:
  `data/output/mega_cap_leader_rotation_dynamic_top50_validation_price_refresh/input/mega_cap_leader_rotation_expanded_price_history.csv`
- Backtest window: `2018-01-31` to `2026-04-10`
- Benchmark: `QQQ`
- Broad benchmark: `SPY`
- Safe asset: `BOXX`
- Turnover cost: `5` bps
- Defense exposure override: `risk_on=1.0`, `soft_defense=1.0`,
  `hard_defense=1.0`
- Universe availability lags: `0`, `1`, `5`, and `21` trading days
- Tested configurations:
  - `top2_cap50`: top 2, 50% single-name cap
  - `top3_cap35`: top 3, 35% single-name cap
  - expanded grid: Top2/Top3/Top4, no-defense / partial-defense /
    cash-defense, and per-sector caps of none / 1 / 2
- Long-cycle validation window: `2017-10-02` to `2026-04-16`
- Long-cycle rolling windows: complete calendar years only, `2018` to `2025`

The refreshed price download covered 91 of 94 dynamic Top50 symbols. Missing
symbols were `CELG`, `DWDP`, and `UTX`; they were old delisted or merger-era
symbols in low-to-mid Top50 ranks and should remain a documented data-quality
gap.

## Lag Validation Result

| Run | CAGR | MaxDD | Sharpe | Calmar | Total Return |
| --- | ---: | ---: | ---: | ---: | ---: |
| top2_cap50_lag0 | 48.69% | -34.22% | 1.24 | 1.42 | 2475.12% |
| top2_cap50_lag1 | 37.89% | -38.79% | 1.05 | 0.98 | 1288.53% |
| top2_cap50_lag5 | 37.89% | -38.79% | 1.05 | 0.98 | 1288.53% |
| top2_cap50_lag21 | 37.44% | -38.79% | 1.04 | 0.97 | 1252.12% |
| top3_cap35_lag0 | 36.82% | -29.17% | 1.13 | 1.26 | 1202.80% |
| top3_cap35_lag1 | 32.02% | -28.76% | 1.04 | 1.11 | 872.48% |
| top3_cap35_lag5 | 32.02% | -28.76% | 1.04 | 1.11 | 872.48% |
| top3_cap35_lag21 | 30.37% | -28.64% | 1.00 | 1.06 | 777.55% |

Interpretation:

- Top2 is still strong after lagging, but the one-day lag cuts a large part of
  the apparent edge. That is a warning sign for data availability assumptions.
- Top3 is less spectacular, but the drawdown and lag sensitivity are cleaner.
- A live candidate should prefer robustness over the highest no-lag result.

## Yearly Stability

Top2 yearly strategy return:

| Year | Lag0 | Lag1 | Lag5 | Lag21 | QQQ |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2018 | 7.94% | -6.74% | -6.74% | -6.74% | -7.79% |
| 2019 | -2.31% | 2.51% | 2.51% | 2.51% | 38.96% |
| 2020 | 137.96% | 106.72% | 106.72% | 106.72% | 48.41% |
| 2021 | 25.88% | 18.17% | 18.17% | 18.17% | 27.42% |
| 2022 | 22.61% | 22.61% | 22.61% | 22.61% | -32.58% |
| 2023 | 104.42% | 82.75% | 82.75% | 82.75% | 54.86% |
| 2024 | 55.29% | 55.29% | 55.29% | 55.29% | 25.58% |
| 2025 | 52.84% | 24.68% | 24.68% | 21.41% | 20.77% |
| 2026 | 37.05% | 37.05% | 37.05% | 37.05% | -0.40% |

Top3 yearly strategy return:

| Year | Lag0 | Lag1 | Lag5 | Lag21 | QQQ |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2018 | 4.82% | 6.59% | 6.59% | 6.59% | -7.79% |
| 2019 | 8.73% | 7.81% | 7.81% | 7.81% | 38.96% |
| 2020 | 96.09% | 90.51% | 90.51% | 90.51% | 48.41% |
| 2021 | 15.96% | 12.11% | 12.11% | 13.87% | 27.42% |
| 2022 | 1.49% | 10.23% | 10.23% | 10.23% | -32.58% |
| 2023 | 58.60% | 58.12% | 58.12% | 47.20% | 54.86% |
| 2024 | 53.47% | 50.60% | 50.60% | 50.60% | 25.58% |
| 2025 | 55.03% | 18.95% | 18.95% | 13.52% | 20.77% |
| 2026 | 31.28% | 26.92% | 26.92% | 26.92% | -0.40% |

Top2's strongest years survive the lag test, but 2025 is very sensitive. Top3
also weakens in 2025, but it has less drawdown stress and less concentration
risk.

## Expanded Stability Grid

The second validation grid focused on lagged baselines rather than the no-lag
headline result:

- Universe lags: `1` and `21` trading days.
- Risk modes:
  - `no_defense`: `risk_on=1.0`, `soft_defense=1.0`, `hard_defense=1.0`
  - `partial_defense`: `risk_on=1.0`, `soft_defense=0.5`,
    `hard_defense=0.2`
  - `cash_defense`: `risk_on=1.0`, `soft_defense=0.0`,
    `hard_defense=0.0`
- Sector caps: disabled, max 1 name per sector, max 2 names per sector.

Best 21-trading-day lag candidates:

| Run | CAGR | MaxDD | Sharpe | Calmar | Avg Stock |
| --- | ---: | ---: | ---: | ---: | ---: |
| top2_cap50_no_defense_sectorall_lag21 | 37.44% | -38.79% | 1.04 | 0.97 | 100.0% |
| top3_cap35_no_defense_sector2_lag21 | 31.33% | -29.29% | 1.04 | 1.07 | 100.0% |
| top4_cap25_no_defense_sectorall_lag21 | 31.06% | -27.28% | 1.06 | 1.14 | 100.0% |
| top3_cap30_no_defense_sector2_lag21 | 28.60% | -26.48% | 1.04 | 1.08 | 90.0% |
| top4_cap25_partial_defense_sector2_lag21 | 24.46% | -22.86% | 0.99 | 1.07 | 88.3% |

Interpretation:

- Top2 has the best return, but its max drawdown is too close to `-40%` and it
  relies on two 50% positions. Keep it as aggressive research only.
- Top4 without QQQ defense is the best current robust baseline. It avoids
  leverage, keeps CAGR around `31%`, and holds max drawdown below `-28%` in the
  21-day lag test.
- Top3 with max 2 names per sector is the higher-return robust candidate, but
  it accepts roughly two extra drawdown points versus Top4.
- QQQ defense helps drawdown, but it gives up too much return for the default.
  The best conservative defense candidate was
  `top4_cap25_partial_defense_sector2_lag21`: `24.46%` CAGR and `-22.86%`
  max drawdown.
- A strict one-name-per-sector cap hurt results. A max-2 sector cap is
  acceptable for Top3, but Top4 did not need it in this sample.

Selected 21-day lag yearly returns:

| Year | Top2 cap50 | Top3 cap35 sector2 | Top4 cap25 | QQQ |
| --- | ---: | ---: | ---: | ---: |
| 2018 | -6.74% | 6.59% | -0.46% | -7.79% |
| 2019 | 2.51% | 6.37% | 13.57% | 38.96% |
| 2020 | 106.72% | 90.51% | 73.39% | 48.41% |
| 2021 | 18.17% | 13.87% | 11.73% | 27.42% |
| 2022 | 22.61% | 10.23% | 9.79% | -32.58% |
| 2023 | 82.75% | 47.20% | 52.73% | 54.86% |
| 2024 | 55.29% | 52.68% | 41.49% | 25.58% |
| 2025 | 21.41% | 20.63% | 28.94% | 20.77% |
| 2026 | 37.05% | 26.78% | 36.76% | -0.40% |

## Long-Cycle Validation

This run uses the longest point-in-time Top50 history currently available in
the repo. It does not claim a 2000-2016 validation because that would require a
historical point-in-time Russell Top50 constituent source.

Command output:
`data/output/mega_cap_leader_rotation_dynamic_top50_long_cycle_validation`

Setup:

- Backtest window: `2017-10-02` to `2026-04-16`
- Universe lag: `21` trading days
- Risk mode: `no_defense`
- Sector caps tested: disabled and max 2 names per sector
- Turnover cost: `5` bps
- Rolling windows: `3` and `5` complete calendar years

Full-period result:

| Run | CAGR | MaxDD | Sharpe | Calmar | Total Return | Turnover/Year |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Top2 cap50 | 39.83% | -38.79% | 1.10 | 1.03 | 1649.57% | 3.51 |
| Top3 cap35 sector2 | 33.36% | -29.29% | 1.09 | 1.14 | 1067.56% | 3.71 |
| Top4 cap25 | 32.27% | -27.28% | 1.10 | 1.18 | 988.51% | 3.57 |
| QQQ | n/a | n/a | n/a | n/a | 365.54% | n/a |
| SPY | n/a | n/a | n/a | n/a | 218.94% | n/a |

Yearly returns:

| Year | Top2 cap50 | Top3 cap35 sector2 | Top4 cap25 | QQQ | SPY |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2017 | 11.02% | 8.00% | 5.35% | 7.31% | 6.76% |
| 2018 | 4.72% | 21.51% | 11.43% | -0.13% | -4.57% |
| 2019 | 2.51% | 6.37% | 13.57% | 38.96% | 31.22% |
| 2020 | 106.72% | 90.51% | 73.39% | 48.41% | 18.33% |
| 2021 | 18.17% | 13.87% | 11.73% | 27.42% | 28.73% |
| 2022 | 22.61% | 10.23% | 9.79% | -32.58% | -18.18% |
| 2023 | 82.75% | 47.20% | 52.73% | 54.86% | 26.18% |
| 2024 | 55.29% | 52.68% | 41.49% | 25.58% | 24.89% |
| 2025 | 21.41% | 20.63% | 28.94% | 20.77% | 17.72% |
| 2026 | 42.26% | 29.02% | 37.76% | 4.39% | 3.18% |

3-year rolling CAGR:

| Window | Top2 cap50 | Top3 cap35 sector2 | Top4 cap25 | QQQ |
| --- | ---: | ---: | ---: | ---: |
| 2018-2020 | 30.49% | 35.10% | 30.00% | 27.28% |
| 2019-2021 | 35.86% | 32.20% | 30.12% | 38.07% |
| 2020-2022 | 44.28% | 33.82% | 28.68% | 8.46% |
| 2021-2023 | 38.62% | 22.86% | 23.44% | 10.05% |
| 2022-2024 | 51.69% | 35.41% | 33.47% | 9.48% |
| 2023-2025 | 51.20% | 39.55% | 40.84% | 33.02% |

5-year rolling CAGR:

| Window | Top2 cap50 | Top3 cap35 sector2 | Top4 cap25 | QQQ |
| --- | ---: | ---: | ---: | ---: |
| 2018-2022 | 26.36% | 25.37% | 21.94% | 12.11% |
| 2019-2023 | 41.31% | 30.30% | 29.91% | 22.42% |
| 2020-2024 | 53.47% | 40.01% | 35.70% | 19.93% |
| 2021-2025 | 38.03% | 27.82% | 27.94% | 15.11% |

Rolling-window interpretation:

- Top2 had the best full-period return and every 5-year rolling window beat
  QQQ. The weak point is the `2019-2021` 3-year window, where it trailed QQQ by
  about `2.20` percentage points annualized.
- Top4 is the cleaner risk baseline: lower CAGR than Top2, but the best Calmar
  and a much smaller max drawdown. It can still lag QQQ in strong broad-tech
  bull windows, so it should not be described as a guaranteed benchmark
  replacement.
- Top3 sector2 sits between them. Its return is higher than Top4, but it gives
  up roughly two drawdown points.
- None of these variants currently uses TACO or Crisis Response. They are
  standalone leader-rotation strategy tests.

## Concentration Variant Research

This run tests two ways to reduce Top2 concentration risk without adding TACO,
Crisis Response, AI, or cash defense:

1. Fixed dual sleeve: blend Top2 and Top4 daily weights.
2. Dynamic concentration: run Top2 as a shadow sleeve, but switch the traded
   sleeve to Top4 after the Top2 shadow drawdown breaches a threshold.

Command output:
`data/output/mega_cap_leader_rotation_dynamic_top50_concentration_variants`

Setup:

- Backtest window: `2017-10-02` to `2026-04-16`
- Universe lag: `21` trading days
- Base sleeves:
  - Top2: top 2, 50% single-name cap
  - Top4: top 4, 25% single-name cap
- Dynamic switch thresholds: Top2 shadow drawdown of `-8%`, `-10%`, and `-12%`
- Turnover cost: `5` bps
- Rolling windows: `3` and `5` complete calendar years

Full-period result:

| Run | CAGR | MaxDD | Sharpe | Calmar | Total Return | Turnover/Year |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Top2 cap50 | 39.83% | -38.79% | 1.10 | 1.03 | 1649.57% | 3.51 |
| Top4 cap25 | 32.27% | -27.28% | 1.10 | 1.18 | 988.51% | 3.57 |
| 25% Top2 / 75% Top4 | 34.42% | -28.19% | 1.12 | 1.22 | 1149.50% | 3.56 |
| 50% Top2 / 50% Top4 | 36.41% | -30.56% | 1.12 | 1.19 | 1315.98% | 3.54 |
| 75% Top2 / 25% Top4 | 38.21% | -34.78% | 1.11 | 1.10 | 1484.13% | 3.53 |
| Dynamic Top2 DD -8% -> Top4 | 32.72% | -28.43% | 1.00 | 1.15 | 1020.92% | 5.24 |
| Dynamic Top2 DD -10% -> Top4 | 34.48% | -30.08% | 1.02 | 1.15 | 1153.66% | 5.10 |
| Dynamic Top2 DD -12% -> Top4 | 34.07% | -33.47% | 1.01 | 1.02 | 1121.40% | 4.98 |

Selected yearly returns:

| Year | Top2 | Top4 | 50/50 blend | Dynamic DD -10% | QQQ |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2017 | 11.02% | 5.35% | 8.16% | 11.02% | 7.31% |
| 2018 | 4.72% | 11.43% | 8.21% | 3.10% | -0.13% |
| 2019 | 2.51% | 13.57% | 8.00% | 13.57% | 38.96% |
| 2020 | 106.72% | 73.39% | 90.11% | 89.92% | 48.41% |
| 2021 | 18.17% | 11.73% | 15.32% | 7.74% | 27.42% |
| 2022 | 22.61% | 9.79% | 16.33% | 13.45% | -32.58% |
| 2023 | 82.75% | 52.73% | 67.46% | 69.03% | 54.86% |
| 2024 | 55.29% | 41.49% | 48.80% | 45.55% | 25.58% |
| 2025 | 21.41% | 28.94% | 25.77% | 25.61% | 20.77% |
| 2026 | 42.26% | 37.76% | 40.18% | 34.44% | 4.39% |

Rolling-window summary:

| Run | Window | Min CAGR | Median CAGR | Worst MaxDD | Min Excess vs QQQ |
| --- | ---: | ---: | ---: | ---: | ---: |
| Top2 | 3Y | 30.49% | 41.45% | -38.79% | -2.20% |
| Top2 | 5Y | 26.36% | 39.67% | -38.79% | 14.25% |
| Top4 | 3Y | 23.44% | 30.06% | -27.28% | -7.95% |
| Top4 | 5Y | 21.94% | 28.93% | -27.28% | 7.49% |
| 50/50 blend | 3Y | 30.54% | 35.04% | -30.56% | -4.72% |
| 50/50 blend | 5Y | 24.46% | 34.61% | -30.56% | 12.34% |
| Dynamic DD -10% | 3Y | 27.56% | 32.51% | -30.08% | -5.55% |
| Dynamic DD -10% | 5Y | 22.18% | 32.73% | -30.08% | 10.07% |

Interpretation:

- The fixed `50% Top2 / 50% Top4` blend is the cleanest balanced candidate in
  this run. It keeps CAGR above `36%` while reducing max drawdown by roughly
  eight percentage points versus pure Top2.
- A more conservative `25% Top2 / 75% Top4` blend had the best Calmar in this
  grid, but it gives up more return. It is the conservative blend candidate.
- The dynamic drawdown switch is viable, but not better than the fixed blend.
  The `-10%` Top2 shadow drawdown threshold produced `34.48%` CAGR and
  `-30.08%` max drawdown, with higher turnover than the fixed blend. The
  result is also threshold-sensitive, so it should not be promoted without more
  out-of-sample data.
- The fixed blend is easier to reason about operationally: both sleeves can run
  continuously, and overlap naturally reduces concentration without waiting for
  a late stress trigger.

## Frequency And Daily Risk Research

This run tests whether the `50% Top2 / 50% Top4` balanced candidate improves
with faster rebalance schedules or daily risk exposure cuts.

Command output:
`data/output/mega_cap_leader_rotation_dynamic_top50_frequency_risk`

Setup:

- Backtest window: `2017-10-02` to `2026-04-16`
- Universe lag: `21` trading days
- Base sleeve mix: `50% Top2 / 50% Top4`
- Rebalance frequencies: monthly, every 10 trading sessions, and weekly
- Daily risk modes:
  - `none`: no daily exposure overlay
  - `hard_cash`: daily hard-defense days go to BOXX, soft-defense days stay
    fully invested
  - `partial_cash`: daily soft-defense days scale stocks to 50%, hard-defense
    days go to BOXX
- Turnover cost: `5` bps

Full-period result:

| Run | CAGR | MaxDD | Sharpe | Calmar | Turnover/Year | Avg Stock |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Monthly, no daily risk | 36.41% | -30.56% | 1.12 | 1.19 | 3.56 | 99.02% |
| Biweekly, no daily risk | 29.47% | -29.94% | 0.99 | 0.98 | 4.36 | 98.65% |
| Weekly, no daily risk | 30.41% | -30.63% | 1.01 | 0.99 | 5.01 | 98.88% |
| Monthly, hard cash | 30.82% | -32.34% | 1.03 | 0.95 | 6.02 | 94.50% |
| Monthly, partial cash | 25.94% | -30.78% | 0.94 | 0.84 | 8.17 | 86.37% |
| Biweekly, partial cash | 22.73% | -27.77% | 0.87 | 0.82 | 8.68 | 86.00% |

Selected yearly returns:

| Year | Monthly | Biweekly | Weekly | Monthly hard cash | Monthly partial cash | QQQ |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2018 | 8.21% | 17.30% | 22.65% | 3.67% | -1.68% | -0.13% |
| 2019 | 8.00% | 15.15% | 25.18% | 7.45% | 4.06% | 38.96% |
| 2020 | 90.11% | 82.33% | 93.64% | 74.75% | 62.77% | 48.41% |
| 2021 | 15.32% | 11.29% | 1.70% | 15.32% | 15.32% | 27.42% |
| 2022 | 16.33% | -5.40% | 5.32% | 4.76% | -2.57% | -32.58% |
| 2023 | 67.46% | 58.56% | 50.93% | 67.46% | 58.36% | 54.86% |
| 2024 | 48.80% | 37.82% | 5.77% | 48.80% | 48.80% | 25.58% |
| 2025 | 25.77% | 3.58% | 22.76% | 11.53% | 8.97% | 20.77% |

Rolling-window summary:

| Run | Window | Min CAGR | Median CAGR | Worst MaxDD | Min Excess vs QQQ |
| --- | ---: | ---: | ---: | ---: | ---: |
| Monthly, no daily risk | 3Y | 30.54% | 35.04% | -30.56% | -4.72% |
| Monthly, no daily risk | 5Y | 24.46% | 34.61% | -30.56% | 12.34% |
| Biweekly, no daily risk | 3Y | 18.75% | 29.43% | -29.94% | -5.31% |
| Biweekly, no daily risk | 5Y | 19.01% | 24.81% | -29.94% | 3.90% |
| Weekly, no daily risk | 3Y | 17.48% | 26.41% | -30.63% | -7.81% |
| Weekly, no daily risk | 5Y | 16.02% | 26.60% | -30.63% | 0.91% |
| Monthly, hard cash | 3Y | 24.91% | 28.90% | -32.34% | -8.64% |
| Monthly, hard cash | 5Y | 18.69% | 29.08% | -32.34% | 6.58% |
| Monthly, partial cash | 3Y | 18.57% | 23.70% | -30.78% | -13.02% |
| Monthly, partial cash | 5Y | 13.38% | 24.21% | -30.78% | 1.26% |

Daily risk-regime observations:

- The daily risk detector marked `17.2%` of days as soft defense and `4.5%` as
  hard defense in this sample.
- In 2022, it marked 169 soft-defense days and 65 hard-defense days. That
  sounds intuitive for an inflation/Fed bear market, but it was harmful for
  this strategy because the leader-rotation sleeve was correctly holding
  energy and health-care winners. Cutting exposure muted the strategy's main
  2022 advantage.
- Weekly and biweekly rebalancing increased turnover and often replaced
  winners too quickly. Monthly rebalancing kept the beneficial hold discipline
  and remains the default research cadence.

Interpretation:

- Do not promote weekly or biweekly rebalancing from this sample. The drawdown
  improvement was small or nonexistent, while CAGR and rolling-window stability
  degraded.
- Do not add daily cash-defense exposure cuts to the balanced Top2/Top4
  candidate yet. It behaved like an over-broad crisis filter and hurt 2022,
  exactly the kind of environment this leader-rotation strategy handled well on
  its own.
- If daily risk logic is revisited later, it should be narrower than the
  current QQQ/breadth soft-hard detector and should distinguish true systemic
  liquidity crises from ordinary bear markets where sector leadership still
  works.

### Monthly, Quarterly, And Semiannual Sensitivity

Follow-up research on `2026-06-04` added quarterly and semiannual rebalance
support to the frequency-risk research CLI. The strict point-in-time dynamic
Top50 quarterly/semiannual retest could not be reconstructed from the retained
source artifacts because the published input bundle keeps the interval universe
history and latest holdings snapshot, but not every historical holdings snapshot
with rank weights. Current iShares historical JSON URLs now return HTML, so the
historical weights are not reproducible from those URLs alone.

As a bounded sensitivity check, the research used the `2026-05-14` Top50
holdings as a static universe with the refreshed Russell 1000 price history from
`2019-01-02` to `2026-06-01`, no daily risk overlay, and `5` bps turnover cost.

| Rebalance cadence | CAGR | MaxDD | Sharpe | Calmar | Turnover/Year |
| --- | ---: | ---: | ---: | ---: | ---: |
| Monthly | 76.71% | -46.69% | 1.66 | 1.64 | 3.02 |
| Quarterly | 63.39% | -44.74% | 1.46 | 1.42 | 2.24 |
| Semiannual | 45.70% | -47.65% | 1.14 | 0.96 | 1.53 |

Interpretation: keep monthly as the default cadence. Quarterly reduced turnover
and improved max drawdown by only about two percentage points while giving up
meaningful CAGR and Calmar. Semiannual rebalancing was too slow for this
leader-rotation profile and had the weakest rolling-window stability.

## Commands

Refresh Top50 price input through yfinance. Use `YFINANCE_PROXY` only when Yahoo
rate-limits the VPS IP:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_mega_cap_leader_rotation.py \
  --download \
  --symbols AAPL,ABBV,ABT,ACN,ADBE,AMAT,AMD,AMGN,AMT,AMZN,AVGO,AXP,BA,BAC,BKNG,BMY,BRKB,C,CAT,CELG,CMCSA,COP,COST,CRM,CSCO,CVS,CVX,DHR,DIS,DWDP,ELV,FB,GE,GEV,GILD,GOOG,GOOGL,GS,HD,HON,IBM,INTC,INTU,ISRG,JNJ,JPM,KLAC,KO,LIN,LLY,LOW,LRCX,MA,MCD,MDT,META,MMM,MO,MRK,MS,MSFT,MU,NEE,NFLX,NKE,NOW,NVDA,ORCL,PCLN,PEP,PFE,PG,PGR,PLTR,PM,PYPL,QCOM,RTX,SBUX,SLB,T,TMO,TSLA,TXN,UBER,UNH,UNP,UPS,UTX,V,VZ,WFC,WMT,XOM \
  --universe data/output/mega_cap_leader_rotation_dynamic_universe_top50_backtest/input/mega_cap_leader_rotation_dynamic_top50_universe_history.csv \
  --price-start 2015-01-01 \
  --start 2018-01-31 \
  --end 2026-04-10 \
  --top-n 2 \
  --single-name-cap 0.50 \
  --risk-on-exposure 1.0 \
  --soft-defense-exposure 1.0 \
  --hard-defense-exposure 1.0 \
  --turnover-cost-bps 5 \
  --output-dir data/output/mega_cap_leader_rotation_dynamic_top50_validation_price_refresh
```

Run lag and yearly validation:

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

Run the expanded stability grid:

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

Run the longest available point-in-time Top50 validation with rolling windows:

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

Run the concentration-variant research:

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

Run the rebalance-frequency and daily-risk research:

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

The validation command writes:

- `validation_summary.csv`
- `yearly_validation_summary.csv`
- `rolling_window_summary.csv` when `--rolling-window-years` is provided

The concentration research command writes:

- `concentration_variant_summary.csv`
- `concentration_variant_yearly_summary.csv`
- `concentration_variant_rolling_summary.csv`
- `concentration_variant_mode_history.csv`

The frequency and daily-risk research command writes:

- `frequency_risk_summary.csv`
- `frequency_risk_yearly_summary.csv`
- `frequency_risk_rolling_summary.csv`
- `frequency_risk_daily_history.csv`

## Promotion Guardrails

Before considering any runtime strategy based on this research:

1. Replace stale ticker gaps (`CELG`, `DWDP`, `UTX`) with a point-in-time symbol
   mapping or a vendor source that keeps delisted history.
2. Prefer a lagged result, not the no-lag result, as the baseline.
3. Prefer `top4_cap25_no_defense` as the robust research baseline. Treat
   `top3_cap35_no_defense_sector2` as the higher-return candidate and Top2 as
   an aggressive variant.
4. Treat `50% Top2 / 50% Top4` as the current balanced research candidate, not
   a runtime-enabled profile. It still needs more validation before promotion.
5. Keep the balanced candidate monthly by default. Weekly/biweekly rebalancing
   and broad daily cash-defense overlays did not improve the research result.
6. Keep this separate from MAGS and dynamic leveraged pullback. This is a
   Russell Top50 leader-rotation idea, not a MAGS plugin.
7. Do not combine it with TACO or Crisis Response until the base strategy is
   independently validated.

## 2026-06-20 Product-Data v2 Refresh Smoke

The old iShares `1467271812596.ajax` historical JSON/CSV path can now return
HTML for IWB holdings requests. The source input layer therefore added a
BlackRock/iShares product-data v2 holdings source with this fallback order:

1. `blackrock_product_data_v2` through the iShares-hosted varnish API path;
2. the same product-data path on the BlackRock host as host fallback;
3. the legacy iShares JSON endpoint as source fallback;
4. existing latest-snapshot secondary/fallback logic in the source-input
   pipeline.

Source pages used for manual verification:

- iShares IWB product page: https://www.ishares.com/us/products/239707/ishares-russell-1000-etf
- BlackRock IWB product page: https://www.blackrock.com/us/individual/products/239707/ishares-russell-1000-etf

A bounded smoke run was executed to prove the new source can rebuild a dynamic
Top50 universe and feed the existing backtest path. This is deliberately a short
post-2024 validation window and must not be interpreted as live-ready evidence.
It is a data-source and pipeline continuity check only.

Command:

```bash
PYTHONPATH=src \
  /Users/lisiyi/Projects/UsEquitySnapshotPipelines/.venv/bin/python \
  -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_backtest \
  --download --dynamic-universe \
  --universe-start 2024-01-01 \
  --price-start 2023-01-01 \
  --mega-universe-size 50 \
  --start 2024-03-01 \
  --top-n 4 \
  --single-name-cap 0.25 \
  --risk-on-exposure 1.0 \
  --soft-defense-exposure 1.0 \
  --hard-defense-exposure 1.0 \
  --turnover-cost-bps 5 \
  --output-dir data/output/russell_top50_product_data_smoke_20260620
```

Pipeline output:

- 30 point-in-time IWB holdings snapshots;
- 1,500 dynamic Top50 universe rows;
- 66,857 downloaded price rows;
- output directory:
  `data/output/russell_top50_product_data_smoke_20260620`.

Top4 cap25 smoke result from `2024-03-01` through `2026-06-18`:

| Strategy | Universe | CAGR | MaxDD | Sharpe | Calmar | Total return | QQQ total return | SPY total return |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Top4 cap25 | dynamic Top50 | 75.79% | -24.03% | 1.66 | 3.15 | 265.38% | 70.78% | 51.41% |

Lag validation on the same short-window data:

| Run | Lag | CAGR | MaxDD | Sharpe | Calmar | Total return |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Top2 cap50 | 21 | 84.16% | -34.92% | 1.49 | 2.41 | 306.60% |
| Top3 cap35 sector2 | 21 | 66.15% | -28.64% | 1.51 | 2.31 | 221.00% |
| Top4 cap25 | 21 | 74.53% | -25.92% | 1.66 | 2.88 | 259.41% |
| Top4 cap25 sector2 | 21 | 70.76% | -25.92% | 1.75 | 2.73 | 241.83% |

Concentration follow-up on the same 21-trading-day lag smoke data:

| Run | CAGR | MaxDD | Sharpe | Calmar | Turnover/Year | Comment |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Top2 cap50 | 84.16% | -34.92% | 1.49 | 2.41 | 4.14 | highest return, still aggressive drawdown |
| Top4 cap25 | 74.53% | -25.92% | 1.66 | 2.88 | 3.81 | cleaner robust baseline |
| 25% Top2 / 75% Top4 | 77.60% | -28.19% | 1.64 | 2.75 | 3.89 | conservative blend |
| 50% Top2 / 50% Top4 | 80.24% | -30.45% | 1.60 | 2.64 | 3.97 | balanced research candidate |
| 75% Top2 / 25% Top4 | 82.43% | -32.69% | 1.55 | 2.52 | 4.05 | more aggressive blend |
| Dynamic Top2 DD -10% -> Top4 | 79.90% | -30.08% | 1.47 | 2.66 | 5.12 | not clearly better than fixed blend |

Interpretation:

- The new product-data v2 source is good enough to resume full PIT Top50
  research; it fixed the historical holdings refresh blocker that appeared when
  the legacy iShares endpoint returned HTML.
- The short-window results remain directionally consistent with the earlier
  retained research: Top2 has the highest return but too much drawdown for a
  default live candidate; Top4 is the cleaner robust baseline; a fixed Top2/Top4
  blend is operationally simpler and more stable than a drawdown-threshold
  switch.
- This smoke window is too short and too favorable to promote live. The next
  required step is a full `2017-09` to current refresh using product-data v2,
  followed by the same 21-trading-day lag, rolling-window, and concentration
  gates.

## 2026-06-20 Product-Data v2 Full PIT Refresh

After the bounded smoke test, the product-data v2 source was used to rebuild the
full retained point-in-time Top50 window from `2017-09` through `2026-06-18`.
This removes the previous blocker where the legacy iShares historical JSON path
returned HTML.

Command:

```bash
PYTHONPATH=src \
  /Users/lisiyi/Projects/UsEquitySnapshotPipelines/.venv/bin/python \
  -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_backtest \
  --download --dynamic-universe \
  --universe-start 2017-09-01 \
  --price-start 2016-01-01 \
  --mega-universe-size 50 \
  --start 2017-10-02 \
  --top-n 4 \
  --single-name-cap 0.25 \
  --risk-on-exposure 1.0 \
  --soft-defense-exposure 1.0 \
  --hard-defense-exposure 1.0 \
  --turnover-cost-bps 5 \
  --output-dir data/output/russell_top50_product_data_full_20260620
```

Pipeline output:

- 106 point-in-time IWB holdings snapshots;
- 5,300 dynamic Top50 universe rows;
- 247,106 downloaded price rows;
- output directory:
  `data/output/russell_top50_product_data_full_20260620`.

Known price gaps remain `CELG`, `DWDP`, and `UTX`; Yahoo reported them as
possibly delisted. This is consistent with the earlier retained research and is
still a data-quality caveat before live promotion.

### Full-window lag validation

Command output:
`data/output/russell_top50_product_data_full_validation_20260620`

Setup:

- Backtest window: `2017-10-02` through `2026-06-18`
- Universe lags: `0`, `1`, and `21` trading days
- Risk mode: no daily or monthly cash defense (`risk_on=1`, `soft=1`, `hard=1`)
- Turnover cost: `5` bps
- Rolling windows: complete calendar-year `3Y` and `5Y`

Selected 21-trading-day lag rows:

| Run | CAGR | MaxDD | Sharpe | Calmar | Total return | QQQ total return | SPY total return | Turnover/Year |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Top2 cap50 | 49.71% | -38.12% | 1.24 | 1.30 | 3259.63% | 438.34% | 240.30% | 3.50 |
| Top3 cap35 | 41.12% | -28.64% | 1.22 | 1.44 | 1908.01% | 438.34% | 240.30% | 3.64 |
| Top3 cap35 sector2 | 40.84% | -28.70% | 1.23 | 1.42 | 1873.74% | 438.34% | 240.30% | 3.67 |
| Top4 cap25 | 39.64% | -27.28% | 1.25 | 1.45 | 1732.21% | 438.34% | 240.30% | 3.53 |
| Top4 cap25 sector2 | 37.50% | -28.71% | 1.26 | 1.31 | 1501.15% | 438.34% | 240.30% | 3.62 |

Rolling-window stress versus QQQ/SPY:

| Run | Window | Worst QQQ excess CAGR | Worst QQQ window | Worst SPY excess CAGR | Worst strategy MaxDD |
| --- | ---: | ---: | --- | ---: | ---: |
| Top2 cap50 | 3Y | -2.20% | 2019-2021 | +9.85% | -38.12% |
| Top2 cap50 | 5Y | +14.67% | 2018-2022 | +17.45% | -38.12% |
| Top3 cap35 | 3Y | -4.38% | 2019-2021 | +7.67% | -28.64% |
| Top3 cap35 | 5Y | +8.75% | 2019-2023 | +11.56% | -28.64% |
| Top4 cap25 | 3Y | -8.16% | 2019-2021 | +3.89% | -27.28% |
| Top4 cap25 | 5Y | +7.36% | 2019-2023 | +12.49% | -27.28% |

Interpretation:

- Top2 remains the highest-return sleeve, but its `-38.12%` drawdown is too high
  for a default live profile unless the strategy is explicitly positioned as a
  high-volatility aggressive sleeve.
- Top4 remains the cleanest robust baseline: it has the lowest drawdown in the
  core grid and every 5-year window beats QQQ and SPY, but it can still lag QQQ
  in a strong broad-tech bull window such as `2019-2021`.
- Top3 does not dominate Top4: it adds return versus Top4, but gives up roughly
  1.4 drawdown points and still does not remove the `2019-2021` QQQ lag.

### Full-window concentration validation

Command output:
`data/output/russell_top50_product_data_full_concentration_20260620`

Selected 21-trading-day lag rows:

| Run | CAGR | MaxDD | Sharpe | Calmar | Total return | Turnover/Year | Comment |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Top2 cap50 | 49.71% | -38.12% | 1.24 | 1.30 | 3259.63% | 3.50 | highest return, too much default drawdown |
| Top4 cap25 | 39.64% | -27.28% | 1.25 | 1.45 | 1732.21% | 3.53 | clean robust baseline |
| 25% Top2 / 75% Top4 | 42.44% | -28.19% | 1.27 | 1.51 | 2077.58% | 3.52 | best current conservative live-design candidate |
| 50% Top2 / 50% Top4 | 45.06% | -30.64% | 1.27 | 1.47 | 2451.86% | 3.52 | best current balanced offensive candidate |
| 75% Top2 / 25% Top4 | 47.48% | -34.47% | 1.26 | 1.38 | 2848.69% | 3.51 | more aggressive, weaker risk fit |
| Dynamic Top2 DD -10% -> Top4 | 43.41% | -30.08% | 1.17 | 1.44 | 2210.95% | 5.14 | higher turnover, not better than fixed blend |

Rolling-window stress for selected concentration candidates:

| Run | Window | Worst QQQ excess CAGR | Worst QQQ window | Worst SPY excess CAGR | Worst strategy MaxDD |
| --- | ---: | ---: | --- | ---: | ---: |
| Top4 cap25 | 3Y | -8.16% | 2019-2021 | +3.89% | -27.28% |
| Top4 cap25 | 5Y | +7.36% | 2019-2023 | +12.49% | -27.28% |
| 25% Top2 / 75% Top4 | 3Y | -6.41% | 2019-2021 | +5.64% | -28.19% |
| 25% Top2 / 75% Top4 | 5Y | +10.42% | 2019-2023 | +13.95% | -28.19% |
| 50% Top2 / 50% Top4 | 3Y | -4.83% | 2019-2021 | +7.22% | -30.64% |
| 50% Top2 / 50% Top4 | 5Y | +12.49% | 2018-2022 | +15.27% | -30.64% |
| Dynamic Top2 DD -10% -> Top4 | 3Y | -5.77% | 2019-2021 | +6.28% | -30.08% |
| Dynamic Top2 DD -10% -> Top4 | 5Y | +10.24% | 2018-2022 | +13.02% | -30.08% |

Concentration interpretation:

- The fixed blends still dominate the dynamic drawdown switch on simplicity and
  turnover. The dynamic switch has higher turnover and lower Sharpe than the
  `50% Top2 / 50% Top4` fixed blend while offering similar drawdown.
- `25% Top2 / 75% Top4` is the most conservative live-design candidate because
  it stays below a `-30%` max drawdown while improving return and rolling-window
  QQQ excess versus pure Top4.
- `50% Top2 / 50% Top4` is the best balanced offensive candidate if a drawdown
  slightly above `-30%` is acceptable. It has materially better CAGR and less
  severe 3-year QQQ underperformance than Top4.
- `75% Top2 / 25% Top4` and pure Top2 are research-only unless the target live
  mandate explicitly accepts mid-to-high `30%` drawdowns.

### Full-window frequency and daily-risk validation

Command output:
`data/output/russell_top50_product_data_full_frequency_risk_20260620`

This reran the earlier frequency/daily-risk checks on the refreshed product-data
v2 inputs using the `50% Top2 / 50% Top4` balanced candidate.

| Run | CAGR | MaxDD | Sharpe | Calmar | Turnover/Year | Avg stock exposure |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Monthly, no daily risk | 45.06% | -30.64% | 1.27 | 1.47 | 3.52 | 99.04% |
| Monthly, hard cash | 39.23% | -32.34% | 1.19 | 1.21 | 5.93 | 94.61% |
| Monthly, partial cash | 33.93% | -30.85% | 1.11 | 1.10 | 7.99 | 86.58% |
| Biweekly, no daily risk | 37.59% | -29.94% | 1.14 | 1.26 | 4.36 | 98.68% |
| Weekly, no daily risk | 38.12% | -30.63% | 1.15 | 1.24 | 5.02 | 98.90% |

Frequency/risk interpretation:

- Keep monthly rebalancing as the live-design default. Weekly/biweekly trading
  increases turnover and reduces CAGR/Calmar.
- Do not add broad daily cash-defense overlays to this leader-rotation profile.
  They reduce return, increase turnover, and do not improve drawdown enough.
- If risk control is revisited, it should be narrow and momentum-crash-specific,
  not a broad QQQ/breadth cash switch. Momentum-crash research highlights that
  crash risk is concentrated after market declines with high volatility and sharp
  rebounds, while dual/absolute momentum research supports simple trend filters
  for drawdown control. Those ideas should be tested as separate, pre-registered
  variants rather than added ad hoc to the base live candidate.

Relevant research references for future variants:

- Daniel and Moskowitz, *Momentum Crashes*: momentum crashes are associated with
  panic states after market declines and high volatility. Source:
  https://www.nber.org/system/files/working_papers/w20439/w20439.pdf
- Moskowitz, Ooi, and Pedersen, *Time Series Momentum*: motivates simple trend
  and absolute-momentum filters. Source:
  https://w4.stern.nyu.edu/facdir/lpederse/papers/TimeSeriesMomentum.pdf
- Antonacci, *Risk Premia Harvesting Through Dual Momentum*: supports combining
  relative and absolute momentum to reduce volatility and drawdown. Source:
  https://papers.ssrn.com/sol3/Delivery.cfm/SSRN_ID2881657_code1556771.pdf?abstractid=2042750
- AQR, *Understanding Defensive Equity*: useful if later testing a quality/low-vol
  overlay, but that would change this from pure leader rotation toward a more
  defensive factor strategy. Source:
  https://www.aqr.com/-/media/AQR/Documents/Insights/White-Papers/Understanding-Defensive-Equity.pdf

### Product-data v2 live-design recommendation

Current recommendation after the full PIT refresh:

1. Do **not** promote pure Top2 as the default runtime profile.
2. Treat `25% Top2 / 75% Top4` as the conservative live-design candidate.
3. Treat `50% Top2 / 50% Top4` as the balanced offensive live-design candidate
   if the live mandate accepts about `-31%` historical max drawdown.
4. Keep Top4 as the fallback robust baseline.
5. Keep monthly rebalance, 21-trading-day universe lag, no broad daily cash
   defense, and 5 bps turnover cost as the default live-design assumptions.
6. Before runtime enablement, add an explicit live-readiness gate that requires:
   - full PIT data generated by product-data v2 or better;
   - no unhandled source fallback streak;
   - 21-trading-day lag validation;
   - positive 5-year rolling excess CAGR versus QQQ and SPY;
   - positive 3-year rolling excess CAGR versus SPY;
   - documented QQQ lag windows rather than hiding them;
   - max drawdown target below `-30%` for conservative, or below `-32%` for
     balanced offensive;
   - no broad daily cash-defense overlay unless a separately backtested
     momentum-crash-specific rule passes the same gate.

## 2026-06-20 Live-Readiness Gate Output

The live-design recommendation above is now reproducible through a deterministic
post-backtest gate. The evaluator consumes already-generated concentration or
validation summaries and rolling-window summaries, so it does not rerun the
backtest or expand the parameter search.

Command:

```bash
PYTHONPATH=src \
  /Users/lisiyi/Projects/UsEquitySnapshotPipelines/.venv/bin/python \
  -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_live_readiness \
  --summary data/output/russell_top50_product_data_full_concentration_20260620/concentration_variant_summary.csv \
  --rolling data/output/russell_top50_product_data_full_concentration_20260620/concentration_variant_rolling_summary.csv \
  --output-dir data/output/russell_top50_product_data_full_live_readiness_20260620
```

Gate output:
`data/output/russell_top50_product_data_full_live_readiness_20260620/live_readiness_summary.csv`

Current pass/fail result:

| Run | Role | Gate profile | Live gate | Reason | Recommended action |
| --- | --- | --- | --- | --- | --- |
| `base_top2_cap50` | aggressive research | research-only | fail | research-only role and max drawdown below balanced threshold | research only |
| `base_top4_cap25` | robust baseline | fallback | pass | pass | fallback live-design review |
| `blend_top2_25_top4_75` | conservative live design | conservative | pass | pass | conservative live-design review |
| `blend_top2_50_top4_50` | balanced offensive live design | balanced offensive | pass | pass | balanced offensive live-design review |
| `blend_top2_75_top4_25` | aggressive blend research | research-only | fail | research-only role and max drawdown below balanced threshold | research only |
| dynamic Top2 drawdown switches | dynamic/daily-risk research | research-only | fail | dynamic/daily-risk candidate | research only |

Implemented gate requirements:

- requires `21` trading-day universe lag when the input has a lag column;
- rejects research-only roles, pure Top2, aggressive 75/25 blend, and dynamic or
  daily-risk candidates by default;
- requires full-period total return above QQQ and SPY;
- requires positive `5Y` rolling excess CAGR versus QQQ and SPY;
- requires positive `3Y` rolling excess CAGR versus SPY;
- allows documented `3Y` rolling QQQ underperformance, because the refreshed
  full PIT test shows every core candidate lags QQQ in `2019-2021` while still
  beating SPY;
- conservative/fallback max drawdown threshold: `-30%`;
- balanced offensive max drawdown threshold: `-32%`.

This gate keeps the strategy in live-design review rather than runtime-enabled.
The next implementation step, if approved, is to add a runtime profile behind a
disabled-by-default flag using either `25% Top2 / 75% Top4` or `50% Top2 / 50% Top4`,
then run the same gate in CI or as a research workflow before enabling live.
