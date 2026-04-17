# Mega-Cap Leader Rotation Dynamic Universe Validation

This note records the point-in-time validation added for the dynamic Russell
Top50 leader-rotation research. It is research-only and must not be used to
promote a live strategy by itself.

## Current Conclusion

Static MAGS / static MAG7 style pools remain rejected for live promotion because
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
promising, but not yet a live profile.

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

Before considering any live strategy based on this research:

1. Replace stale ticker gaps (`CELG`, `DWDP`, `UTX`) with a point-in-time symbol
   mapping or a vendor source that keeps delisted history.
2. Prefer a lagged result, not the no-lag result, as the baseline.
3. Prefer `top4_cap25_no_defense` as the robust research baseline. Treat
   `top3_cap35_no_defense_sector2` as the higher-return candidate and Top2 as
   an aggressive variant.
4. Treat `50% Top2 / 50% Top4` as the current balanced research candidate, not
   a live profile. It still needs more validation before promotion.
5. Keep the balanced candidate monthly by default. Weekly/biweekly rebalancing
   and broad daily cash-defense overlays did not improve the research result.
6. Keep this separate from MAGS and dynamic leveraged pullback. This is a
   Russell Top50 leader-rotation idea, not a MAGS plugin.
7. Do not combine it with TACO or Crisis Response until the base strategy is
   independently validated.
