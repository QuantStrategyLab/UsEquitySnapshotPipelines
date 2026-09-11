# SOXL/SOXX Core-only P3 Input Materializer

This module is the missing data-to-context half between the frozen
three-asset SOXL P1 identity and the isolated SOXL P3 source runner.  It is a
pure offline verifier, not a P1 publisher or a completed P3 evidence system.

It accepts only a canonical private `bars.json` with three separately dated
`SOXL`, `SOXX`, and `BOXX` adjusted daily OHLCV series.  SOXL/SOXX may carry
pre-BOXX history solely to warm up their deterministic indicators; replay
starts only on dates where all three real assets have bars.  This is not a
proxy, carried price, or synthetic BOXX row.  Before deriving any indicator,
it checks:

- the exact SOXL P2 v3 candidate binding and immutable P1 manifest;
- the canonical member bytes, member SHA-256, cutoff date, and each
  per-symbol source content SHA-256;
- ordered SOXL/SOXX indicator histories of at least 252 rows, common
  SOXL/SOXX dates, at least 252 available indicator sessions before each
  emitted context, and a real three-asset intersection for every replay row;
- bounded materialized output suitable for the current isolated runner.

It independently derives the exact daily fields required by the frozen public
strategy adapter: 140-session trend means, SOXX 20-session mean/slope, Wilder
14-session RSI, 20-session population-standard-deviation Bollinger upper band,
10-session sample-standard-deviation realized volatility annualized by 252,
and the frozen 252-session 95th-percentile volatility threshold bounded to
50–75%.  Daily `as_of` values are UTC date markers only; they are not claimed
provider timestamps or broker fills.  The isolated runner remains responsible
for next-complete-session execution timing.

This module does **not** fetch Alpaca data, validate complete XNYS holiday
coverage, publish a P1 root, write storage, schedule work, access credentials,
place paper/shadow/live orders, or authorize promotion.  The local P3 facade
now selects the frozen folds and trailing OOS window, executes all 5/10/15 bps
scenarios through the isolated runtime, and packages metrics-and-hashes only.
The separate P1 publisher verifies complete XNYS coverage before it can create
a root.  A later non-live scheduler must persist only sanitized evidence and
fail closed.

## Fixed retrospective return attribution

The same verified materialized P1 input can be used for a bounded aggregate
attribution study:

```bash
python scripts/run_soxl_three_asset_learning.py \
  --p1-binding <binding.json> \
  --input-manifest <manifest.json> \
  --bars-member <bars.json> \
  --ues-project <pinned-ues-checkout> \
  --p2-candidate config/soxl_soxx_core_only_p2_v3.json \
  --attribution
```

The command has no parameter-search surface. It always evaluates the original
`0.65` mid-weight baseline, a 97% SOXX buy-and-hold comparison with 3% retained
cash, and fixed 97%-invested full-tier weights (`0.70/0.20/0.10`) that rebalance
under the original next-session model. Each variant runs at 5, 10, and 15 bps
with initial equity of USD 100,000 and uses development sessions through
2025-07-31 only. The two comparisons describe the aggregate rule difference;
they do not separately identify a causal trend or volatility contribution and
are not frozen P2 candidates.

For every interval, the existing stateful replay attributes the pre-trade
equity move to the prior simulated SOXL, SOXX, and BOXX quantities, then checks
that the remaining equity change equals the original simulated execution cost.
Cash interest and external flow are zero assumptions. The verified P1 adjusted
close remains the price basis, so the study does not add a dividend, fund-fee,
or ETF-expense adjustment. Output contains only aggregate dollar PnL,
initial-equity contribution percentage points, return, drawdown, turnover,
cost, dates, counts, identities, and reconciliation residuals. It omits daily
prices, returns, positions, and decisions and remains retrospective research
with no order, sizing, promotion, paper, shadow, or live authority.

## Fixed volatility-delever ablation

The same runner also exposes one fixed retrospective on/off comparison:

```bash
python scripts/run_soxl_three_asset_learning.py \
  --p1-binding <binding.json> \
  --input-manifest <manifest.json> \
  --bars-member <bars.json> \
  --ues-project <pinned-ues-checkout> \
  --p2-candidate config/soxl_soxx_core_only_p2_v3.json \
  --volatility-ablation
```

This mode evaluates only `baseline_mid_065` and
`baseline_without_volatility_delever`, both at 10 bps, USD 100,000 initial
equity, zero cash interest, and the same verified P1 adjusted-close sessions
through 2025-07-31. The second variant copies the frozen P2 runtime config and
changes only `blend_gate_volatility_delever_enabled` to `false`; the baseline
uses the original config path unchanged. Output keeps the attribution schema
and identifies `study_variant=volatility_delever_on_off_v1`. This analysis
variant is not a new strategy identity, does not make a causal claim, and has
no order, promotion, shadow, or live authority. The original `--attribution`
nine-result study remains unchanged.
