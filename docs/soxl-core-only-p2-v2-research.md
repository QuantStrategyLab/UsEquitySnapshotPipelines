# SOXL/SOXX Core-only P2 v2 Research Candidate

`config/soxl_soxx_core_only_p2_v2.json` is the clean successor to the
historical SOXL research route.  It is a frozen **P2 research candidate**,
not a backtest result, data workflow, paper/shadow deployment, or trading
configuration.

## What is retained

The candidate keeps only the deterministic strategy rules already implemented
in the pinned UsEquityStrategies source:

- SOXX trend bands choose full, mid, or defensive exposure;
- SOXL, SOXX, and BOXX are the only tradable assets;
- the strategy's own ten-day realized-volatility rule can redirect the SOXL
  sleeve to SOXX.

The configuration and exact source revision are frozen before new evidence is
read.  A later replay must use the rule at the next tradable session and the
declared cost stresses.

## What is deliberately excluded

The legacy income sleeve, option overlays, AI extensions, old external
market-regime/latest-signal control, and volatility-retention policy are all
disabled.  A future signal plugin may publish facts, but it cannot be mounted
here or alter targets unless a distinct frozen candidate is created and
validated.

## Current state and next gate

The historical fixed-cutoff SOXL pipeline remains a useful research record,
but cannot supply current evidence or a daily scheduler.  The
[three-asset P1 input contract](soxl-core-only-p1-contract.md), local P1
publisher, and offline P3 verifier/facade now exist, but no root has been
acquired and no scheduler or evidence store exists.  It must not reuse TQQQ
data, TQQQ evidence, or legacy SOXL `latest` artifacts.

P4/P5/P6, broker orders, paper, shadow, and live operation remain outside this
candidate.
