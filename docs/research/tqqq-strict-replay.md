# `tqqq_growth_income` strict replay: pre-registration

Registered 2026-11-27 (Owner-approved before execution). Research only: no live change, no orders, no strategy or parameter change.

## Question

Earlier historical notebooks showed very high returns for `tqqq_growth_income` under optimistic assumptions. How much of the complete `tqqq_growth_income` strategy survives a strict replay, and where does the gap come from?

## Rules fixed before running

1. **Strategy.** Complete `tqqq_growth_income` rules from `UsEquityStrategies` at commit `1fcfeac3dfad95db6b70ed3af19ce49c62f2c3ef` (`tqqq_growth_income_manifest.default_config` & `build_rebalance_plan`, matching `tqqq_growth_income_archive` unchanged):
   - Target core sleeve: `45% TQQQ / 45% QQQM` (`QQQ` proxy before `QQQM` listing on `2020-10-13`) / `2%` cash reserve / remainder `BOXX` (`BIL` cash proxy).
   - Trend & risk filters: `QQQ MA200` + `MA20` slope, `20d` pullback rebound (`2.0x` volatility-scaled), `5d` rolling `p90 / 252d` volatility delever (`[0.24, 0.36]`), `rebalance_threshold_ratio: 0.01`.
   - Income layer (`income_layer_start_usd: 250000.0`): inactive on a USD 1,500 starting account; long-history replay disables the USD 250k+ diversified income sleeve (`income_layer_enabled: false`) consistently across both baselines and strict replay.
   - The workflow installs commit `1fcfeac3dfad95db6b70ed3af19ce49c62f2c3ef` for this study only; the project's own pin is unchanged.
2. **Data.** Public daily adjusted closes from Yahoo Finance (`BIL`, `QQQ`, `QQQM`, `SPY`, `TQQQ`), downloaded from `2009-01-01` to the run's end date. Only observed ETF history is used, never synthetic 3x data. `BIL`, `QQQ`, `SPY`, `TQQQ` must cover every XNYS session from `2011-04-01` onward with `260` warm-up sessions for `QQQ` and `TQQQ`; `QQQM` must cover every XNYS session from `2020-10-14` onward (`QQQ` serves as the `QQQM` proxy prior to `QQQM` listing). Access is gated by the pinned, expiring record `config/research_mandates/tqqq-strict-replay-2026-11-27.json` on `main`.
3. **Periods.** Full history from `2011-04-01`, and `2023-01-01` onward reported separately. The later period is a historical subperiod, not an untouched holdout.
4. **Methods, side by side.**
   - `original_simulator`: unchanged `tqqq_growth_income_archive.run_backtest` on USD 1,500 (`same-close` execution at signal close, 5 bp turnover cost, no tax, no USD 0.35 minimum commission).
   - `original`: the same convention recomputed here as a cross-check only; the reported baseline is `original_simulator`.
   - `strict`: the same targets executed at the next session's close on a fresh USD 1,500 account. Each trade pays its own spread (`3 bp` for 3x ETFs, `1 bp` for 1x ETFs; stress `x2` and `x3`) and a USD 0.35 minimum commission. A configurable yearly tax (`--tax-rate` / `QSL_RESEARCH_TAX_RATE`, default 0%; example 20% stress profile) applies to each calendar year's positive gain. Trades occur only when a target weight moves by more than 1 percentage point (`rebalance_threshold_ratio: 0.01`).
5. **Reading.** Compare `original_simulator` vs `strict` across full sample and `2023+`, record the trial for future `DSR`/`PBO`, and check whether `strict` max drawdown breaches the balanced tier's 30% cap (`D <= 30%`). No strategy is changed by this study.
