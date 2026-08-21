# Research Strategy Candidate Registry v1

`strategy_candidate_registry` is the P2 catalogue for frozen research
candidates. It is intentionally small and contains two current
single-strategy candidates: `tqqq_core_only_p2_v5` and
`soxl_soxx_core_only_p2_v3`.

Each entry binds a candidate identifier, immutable source revisions, frozen
configuration digest, and data-contract identifier.  Its canonical digest can
be carried by P1/P3 evidence.  The current entry permits exactly P1, P2, and
P3; it does not authorize paper, shadow, broker access, orders, capital, or
live trading.

Both entries permit exactly P1, P2, and P3. The SOXL entry records its own
three-asset observed-data contract and source revisions; it does not inherit
TQQQ evidence or authority. Neither entry authorizes paper, shadow, broker
access, orders, capital, or live trading.

The schema also describes two future candidate kinds without activating them:

- A `portfolio` declares at least two distinct candidate identifiers.  A future
  pure PortfolioComposer must freeze the component versions, weights,
  rebalancing rule, shared data cutoff, costs, and portfolio risk rule before
  that entry can be added to the catalogue.
- A `plugin` declares exactly one versioned PluginBinding.  Plugin bindings are
  read-only research inputs; they cannot mutate strategy parameters or become
  an execution path.

This registry deliberately does not reuse QuantPlatformKit's
`CandidateRiskIdentity`: that type is a mandate-bound execution-risk identity
and belongs to later P4+ work.  The registry also does not resolve components,
call providers, read credentials, schedule jobs, or create P4/P5/P6 authority.

The complementary [multi-strategy research-driver template](multi-strategy-driver-template.md)
records whether a strategy-specific P1--P3 route is already wired or still
requires migration. It does not alter this registry's candidate digests and
does not make a legacy strategy schedulable.
