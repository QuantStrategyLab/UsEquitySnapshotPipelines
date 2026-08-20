# SOXL/SOXX Core-only P1 Input Contract

This is the data-identity half of the SOXL/SOXX core-only research route. It
binds exactly the frozen P2 v2 candidate, its UsEquityStrategies revision, and
three observed daily-bar inputs: `SOXL`, `SOXX`, and `BOXX`.

The contract requires SIP, total-return-adjusted daily data on the XNYS
calendar and carries a caller-supplied completed-session cutoff into the
immutable manifest. Every future root must contain all three sources and their
content digests under one binding; no proxy, carried price, source mixing, or
legacy nine-asset `latest` artifact is valid.

The repository now includes a bounded local P1 publisher.  It accepts only an
injected Alpaca SIP provider, validates every expected XNYS session, produces
the canonical three-asset member, and publishes the local root atomically only
after all checks pass.  It does not read Alpaca credentials itself, access
cloud storage, start a GitHub workflow, replay the strategy, or place an
order.  No verified root has been acquired yet.  The remaining migration work
is a non-live scheduler plus a separate sanitized P3 persistence boundary.
