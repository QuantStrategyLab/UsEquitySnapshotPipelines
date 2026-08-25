# SOXL/SOXX Core-only P2 v4 Free Split-Close Candidate

`soxl_soxx_core_only_p2_v4_free_split_close` is a new research-only candidate.
It does not replace P2 v3, does not modify any live or paper platform, and
does not authorize an order.

## Why it is separate from v3

P2 v3 consumes Alpaca SIP daily bars adjusted for splits, dividends, and
spin-offs.  The free-source candidate consumes split-adjusted closes only.
Those are distinct economic data contracts, even though the frozen SOXX trend
and volatility-delever runtime parameters are intentionally unchanged.  Their
P1 inputs, P3 materializations, evidence hashes, and backtests must therefore
remain separate.

## P1 admission rule

For each of `SOXL`, `SOXX`, and `BOXX`:

1. Twelve Data provides the candidate canonical split-adjusted close series.
2. Yahoo Finance independently verifies the same expected XNYS session set.
3. QPK's pinned multi-source policy compares only `close` at its fixed 1 bp
   relative tolerance.  Volume and unused OHLC fields are outside this
   close-only candidate scope; their defaults are not relaxed globally.
4. Either source unavailable, malformed, incomplete, or disagreeing means
   `PARK` for that day.  No fallback, averaging, substitution, retry-driven
   strategy change, or P1 root is allowed.

The immutable root contains canonical `closes.json`, redacted
`assurance.json`, the binding, and a manifest that binds both source snapshot
hashes.  It does not persist Yahoo price rows as a hidden fallback series.

## P3 and promotion boundary

P3 revalidates the binding, manifest, canonical closes, assurance receipt,
P2 config hash, UES revision, QPK revision, and UES lockfile before an offline
replay.  It produces only the fixed three chronological windows and trailing
252-XNYS-session OOS replay at 5/10/15 bps cost stress.

Passing P1/P3 is evidence collection only.  It cannot tune parameters, reload
a strategy on any brokerage platform, enable paper/shadow/live execution, or
promote v3 or v4 automatically.  A real-source P1/P3 evidence review and an
explicit separate approval remain required before considering any platform
change.
