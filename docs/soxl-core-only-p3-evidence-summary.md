# SOXL/SOXX Core-only P3 Evidence Summary

This module is the final pure-computation layer of the current SOXL P3
template.  It reconstructs the fixed P3 evidence plan from the materialized
P1 input, sends only those requests to an already isolated source runner, and
returns a metrics-and-hash summary.

It verifies the P1/P2/materialized/plan bindings, checks every runner and
replay digest, requires one stable source-runtime identity for every request,
and records only terminal equity, net return, maximum drawdown, turnover,
cost, execution count, and result hashes.  It deliberately excludes raw bars,
derived indicators, positions, target weights, orders, credentials, and
provider responses.

The module does not acquire or publish P1 data, create an immutable P1 root,
access cloud storage, schedule a workflow, perform a real strategy run on
market data by itself, calculate a promotion decision, create paper/shadow
activity, or authorize live trading.  A later adapter must supply the exact
frozen UES source/runtime and persist only this sanitized summary after all
requests succeed.
