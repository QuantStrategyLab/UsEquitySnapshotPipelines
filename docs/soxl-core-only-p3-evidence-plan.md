# SOXL/SOXX Core-only P3 Evidence Plan

This pure planner freezes the evidence windows already declared by the frozen
SOXL P2 v3 candidate: three chronological evaluation windows, the trailing
252-session OOS window ending at the verified P1 cutoff, and every 5/10/15
bps cost scenario.  It accepts only a digest-checked output from the P1-bars
materializer and rejects an unavailable boundary, changed candidate identity,
cutoff before `2026-08-04`, short OOS, malformed session ordering, or changed
cost grid.

The planner does not run the strategy or claim an outcome.  The local offline
P3 facade uses these exact requests with the isolated runner, calculates
bounded metrics, and produces a non-sensitive summary.  The separate local
P1 publisher is responsible for actual XNYS session coverage and immutable
input publication; a later scheduler/persistence boundary remains responsible
for a daily evidence record.
