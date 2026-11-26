# SOXL core-only strict replay: pre-registration

Registered 2026-11-26, before any run. Research only: no live change, no orders, no strategy
or parameter change.

## Question

The SOXL core-only strategy (SOXX trend, RSI cap and volatility delever across SOXL, SOXX
and cash, without plugins) showed a very high historical CAGR in earlier optimization
notebooks. How much of it survives a strict replay, and where does the gap come from?

## Rules fixed before running

1. **Strategy.** `soxl_trend_simulator` from
   `QuantStrategyLab/UsEquityStrategies@1fcfeac3dfad95db6b70ed3af19ce49c62f2c3ef`, with its
   default core-only parameters unchanged. BIL stands in for BOXX, which only trades from
   2022-12. The workflow installs that exact commit for this study only; the project's own
   dependency pin is unchanged.
2. **Data.** Public daily adjusted closes from Yahoo Finance (BIL, SOXL, SOXX, SPY), downloaded
   from 2009-01-01 to the run's end date. Only observed ETF history is used, never a series
   reconstructed from 1x data. Access is gated by the pinned, expiring record
   `config/research_mandates/soxl-strict-replay-2026-11-26.json`, and the workflow runs only on
   `main`.
3. **Periods.** Full history from 2011-04-01, and 2023-01-01 onward reported separately. The
   later period is a historical subperiod, not an untouched holdout.
4. **Methods, side by side.**
   - `original_simulator`: the unchanged simulator (a signal from one close traded at that same
     close, 5 bp turnover cost, no tax).
   - `original`: the same convention recomputed here, as a cross-check.
   - `strict`: the same targets executed at the next session's close on a fresh USD 1,500
     account. Each trade pays its own spread (3 bp for 3x ETFs, 1 bp for 1x ETFs; stress x2 and
     x3) and a USD 0.35 minimum commission. A 20% tax applies to each calendar year's positive
     gain. Trades occur only when a target weight moves by more than 1 percentage point, the
     strategy's own `rebalance_threshold_ratio`.
5. **Reading.** The gap between `original` and `strict` measures what same-close execution and
   missing costs added. The balanced tier's 30% drawdown cap applies to `strict`. No strategy is
   changed by this study.
