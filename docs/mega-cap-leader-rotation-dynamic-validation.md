# Mega-Cap Leader Rotation Dynamic Universe Validation

This note records the point-in-time validation added for the dynamic Russell
Top50 leader-rotation research. It is research-only and must not be used to
promote a live strategy by itself.

## Current Conclusion

Static MAGS / static MAG7 style pools remain rejected for live promotion because
they are structurally hindsight-biased.

Dynamic Top50 Top2 is not rejected, but it is too sensitive to universe
availability timing to promote now. With refreshed Top50 prices, adding a
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

The validation command writes:

- `validation_summary.csv`
- `yearly_validation_summary.csv`

## Promotion Guardrails

Before considering any live strategy based on this research:

1. Replace stale ticker gaps (`CELG`, `DWDP`, `UTX`) with a point-in-time symbol
   mapping or a vendor source that keeps delisted history.
2. Prefer a lagged result, not the no-lag result, as the baseline.
3. Prefer `top4_cap25_no_defense` as the robust research baseline. Treat
   `top3_cap35_no_defense_sector2` as the higher-return candidate and Top2 as
   an aggressive variant.
4. Keep this separate from MAGS and dynamic leveraged pullback. This is a
   Russell Top50 leader-rotation idea, not a MAGS plugin.
5. Do not combine it with TACO or Crisis Response until the base strategy is
   independently validated.
