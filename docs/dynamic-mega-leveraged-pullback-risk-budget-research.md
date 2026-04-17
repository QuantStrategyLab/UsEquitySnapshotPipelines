# Dynamic Mega Leveraged Pullback Risk Budget Research

This note records the current MAGS / dynamic mega leveraged pullback risk-budget
research. It is research-only. Do not change live defaults from this note
without a separate promotion decision and a fresh verification run.

## Current Conclusion

Keep the current robust default as:

- `candidate_universe_size=15`
- `top_n=3`
- `frequency=weekly`
- `return_mode=leveraged_product`
- `market_trend_symbol=QQQ`
- `max_product_exposure=0.80`
- `single_name_cap=0.25`
- `soft_product_exposure=0.0`
- `hard_product_exposure=0.0`

This profile keeps drawdown close to QQQ while materially improving CAGR. The
main candidate for higher risk is raising `max_product_exposure` to `0.85` or
`0.90`, but those settings intentionally accept larger drawdowns in weak years.

Do not add TACO to this strategy for now. TACO remains research-only for MAGS,
and the current optimization space is the base strategy's risk budget.

## Test Setup

- Price input:
  `data/output/mega_cap_leader_rotation_dynamic_universe_top20_backtest/input/mega_cap_leader_rotation_dynamic_top20_price_history.csv`
- Universe input:
  `data/output/mega_cap_leader_rotation_dynamic_universe_top20_backtest/input/mega_cap_leader_rotation_dynamic_top20_universe_history.csv`
- Backtest window: `2017-10-02` to `2026-04-13`
- Benchmark: `QQQ`
- Product modeling: daily-reset 2x product with `leveraged_expense_rate=0.01`
- Transaction cost: default `5` bps turnover cost

## Main Risk-Budget Table

| Profile | CAGR | MaxDD | Sharpe | Calmar | 2022 | 2025 | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| QQQ | 19.25% | -35.12% | 0.87 | 0.55 | -32.58% | +20.77% | Benchmark |
| max70 / cap25 | 28.06% | -31.94% | 1.01 | 0.88 | -17.98% | -3.63% | Conservative |
| max75 / cap25 | 29.58% | -33.91% | 1.02 | 0.87 | -18.39% | -4.31% | Middle risk |
| max80 / cap25 | 30.96% | -34.80% | 1.03 | 0.89 | -18.79% | -4.73% | Current robust default |
| max85 / cap25 | 32.33% | -35.68% | 1.04 | 0.91 | -19.20% | -5.17% | Offensive candidate |
| max90 / cap25 | 33.46% | -36.25% | 1.05 | 0.92 | -19.56% | -5.50% | Aggressive candidate |
| max90 / cap30 | 34.53% | -39.68% | 1.01 | 0.87 | -21.82% | -7.19% | Not recommended |

Interpretation:

- `max80 / cap25` is the best default if the drawdown target is near QQQ's
  historical max drawdown in this sample.
- `max85 / cap25` and `max90 / cap25` improve CAGR and Calmar, but they also
  increase weak-year losses. Treat them as explicit risk-budget choices, not
  free alpha.
- `max90 / cap30` is too concentrated. It improves CAGR but worsens drawdown and
  weak-year behavior enough that the risk-adjusted result is worse than
  `max90 / cap25`.

## Rejected Variants

| Variant | CAGR | MaxDD | Reason |
| --- | ---: | ---: | --- |
| top20 / top4 / max80 / cap25 | 27.29% | -35.59% | More diversification diluted leader exposure without reducing drawdown. |
| monthly / top15 / top3 / max80 / cap25 | 22.05% | -39.92% | Lower turnover hurt responsiveness and worsened drawdown. |
| 1x stock baseline | 14.92% | -16.88% | Lower risk, but underperformed QQQ and does not fit the target return profile. |

The strategy currently benefits from concentrated exposure to a small number of
strong mega-cap leaders. Forced diversification and slower rebalancing both
weaken that edge.

## Commands

Current robust default:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_dynamic_mega_leveraged_pullback.py \
  --prices data/output/mega_cap_leader_rotation_dynamic_universe_top20_backtest/input/mega_cap_leader_rotation_dynamic_top20_price_history.csv \
  --universe data/output/mega_cap_leader_rotation_dynamic_universe_top20_backtest/input/mega_cap_leader_rotation_dynamic_top20_universe_history.csv \
  --start 2017-10-02 \
  --candidate-universe-size 15 \
  --top-n 3 \
  --return-mode leveraged_product \
  --market-trend-symbol QQQ \
  --max-product-exposure 0.80 \
  --single-name-cap 0.25 \
  --output-dir data/output/dynamic_mega_leveraged_pullback_optimization_research/baseline_check
```

Offensive candidate:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_dynamic_mega_leveraged_pullback.py \
  --prices data/output/mega_cap_leader_rotation_dynamic_universe_top20_backtest/input/mega_cap_leader_rotation_dynamic_top20_price_history.csv \
  --universe data/output/mega_cap_leader_rotation_dynamic_universe_top20_backtest/input/mega_cap_leader_rotation_dynamic_top20_universe_history.csv \
  --start 2017-10-02 \
  --candidate-universe-size 15 \
  --top-n 3 \
  --return-mode leveraged_product \
  --market-trend-symbol QQQ \
  --max-product-exposure 0.85 \
  --single-name-cap 0.25 \
  --output-dir data/output/dynamic_mega_leveraged_pullback_optimization_research/max85_cap25
```

Aggressive candidate:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_dynamic_mega_leveraged_pullback.py \
  --prices data/output/mega_cap_leader_rotation_dynamic_universe_top20_backtest/input/mega_cap_leader_rotation_dynamic_top20_price_history.csv \
  --universe data/output/mega_cap_leader_rotation_dynamic_universe_top20_backtest/input/mega_cap_leader_rotation_dynamic_top20_universe_history.csv \
  --start 2017-10-02 \
  --candidate-universe-size 15 \
  --top-n 3 \
  --return-mode leveraged_product \
  --market-trend-symbol QQQ \
  --max-product-exposure 0.90 \
  --single-name-cap 0.25 \
  --output-dir data/output/dynamic_mega_leveraged_pullback_optimization_research/max90_cap25
```

## Promotion Guardrails

Before changing any default or live configuration:

1. Re-run the selected profile with current data.
2. Confirm 2022 and 2025 weak-year behavior remains acceptable.
3. Keep `single_name_cap <= 0.25` unless a separate concentration review proves
   the higher cap is worth the drawdown.
4. Do not promote TACO, bear-candidate, or monthly-rebalance changes as part of
   the same decision.
5. Record the exact command, input files, and result table in the PR or runbook.
