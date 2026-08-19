# TQQQ P2 v5 Daily Research Contract

`tqqq_core_only_p2_v5` is a frozen, research-only candidate for the daily
P1-to-P3 lane.  It does not alter the TQQQ strategy rule, source revision,
runtime configuration, tradable universe, costs, or parameter values used by
P2 v4.

The only new variable is the completed `date_cutoff` in a verified P1 binding.
Each immutable daily root carries that date, its binding digest, the exact four
canonical Alpaca-SIP symbol payloads, and a manifest.  P3 derives its OOS
window as the final 252 XNYS sessions ending at that bound cutoff.  The three
historical evidence folds remain fixed and no daily outcome may change them.

This candidate is deliberately bounded as follows:

- P1 and P3 may acquire observed bars, assess input health, publish one
  immutable private research root, and run an offline no-order replay.
- A missing or malformed input produces `DEFERRED` or `QUARANTINED`; it does
  not substitute a provider, fill a gap, change a strategy parameter, or
  invalidate prior roots.
- P4 paper, P5 shadow, broker orders, capital allocation, and P6 live remain
  outside this contract.  They require separately implemented policies; P6 is
  explicitly user-activated.

This change provides the testable P2/P3 foundation only.  It does **not** add a
schedule, read credentials, contact Alpaca, write GCS, or start any workflow.
The next delivery wires a separately reviewed GitHub Actions daily controller
to this contract and records its health/evidence status.
