# TQQQ P3 strategy-performance contract

## Purpose

`strategy_performance.v2.json` is a small, sanitized projection of a terminal
TQQQ P3 evidence package. It lets a downstream research watcher compare two
verified observations without receiving raw market data or execution material.

## Publication rule

The scheduled P1/P3 workflow writes the artifact only when P3 ends with
`EVIDENCE_V2_COMPLETE`. GitHub retains the artifact for 35 days. A missing,
parked, malformed, or non-verified P3 result publishes nothing.

## Contents and exclusions

The artifact contains the strategy profile/candidate identity, `as_of`, five
comparable metrics (`sharpe`, `cagr`, `calmar`, `win_rate`, and `max_dd`), and
P1/P2/P3/revision digests. Its authority is fixed to research-only, no-order,
and zero-size; P4, P5, and P6 are explicitly unauthorized.

It must not contain raw bars, GCS paths, credentials, account identifiers,
orders, fills, capital, or promotion authority.

## Consumer rule

A consumer must verify the artifact identity and P3 status, then compare two
chronologically distinct observations of the same candidate. The first valid
observation is a baseline, not a degradation signal. Any resulting research
task remains an inactive, no-order candidate; it does not change a strategy,
create a paper/live order, merge code, or promote an artifact.
