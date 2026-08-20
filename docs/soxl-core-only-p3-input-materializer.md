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

- the exact SOXL P2 v2 candidate binding and immutable P1 manifest;
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

This change does **not** fetch Alpaca data, validate complete XNYS holiday
coverage, publish a P1 root, run the strategy, create P3 evidence folds,
write storage, schedule work, access credentials, place paper/shadow/live
orders, or authorize promotion.  A future P1 publisher must verify complete
XNYS coverage.  A later P3 evidence verifier must select the frozen folds and
trailing OOS window, execute all 5/10/15 bps scenarios, package only
non-sensitive evidence, and fail closed.
