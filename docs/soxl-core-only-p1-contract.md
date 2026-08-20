# SOXL/SOXX Core-only P1 Input Contract

This is the data-identity half of the SOXL/SOXX core-only research route. It
binds exactly the frozen P2 v2 candidate, its UsEquityStrategies revision, and
three observed daily-bar inputs: `SOXL`, `SOXX`, and `BOXX`.

The contract requires SIP, total-return-adjusted daily data on the XNYS
calendar and carries a caller-supplied completed-session cutoff into the
immutable manifest. Every future root must contain all three sources and their
content digests under one binding; no proxy, carried price, source mixing, or
legacy nine-asset `latest` artifact is valid.

This is intentionally only a pure contract and manifest validator. It does
not fetch data, read Alpaca credentials, write cloud storage, start a GitHub
workflow, replay the strategy, or place an order. The remaining migration work
is a bounded P1 publisher that validates complete session coverage, followed
by a distinct P3 verifier that runs the pinned public strategy adapter.
