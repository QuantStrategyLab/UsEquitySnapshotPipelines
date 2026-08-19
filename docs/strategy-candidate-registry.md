# Research Strategy Candidate Registry v1

`strategy_candidate_registry` is the P2 catalogue for frozen research
candidates.  It is intentionally small and contains only the current
`tqqq_core_only_p2_v5` single-strategy candidate.

Each entry binds a candidate identifier, immutable source revisions, frozen
configuration digest, and data-contract identifier.  Its canonical digest can
be carried by P1/P3 evidence.  The current entry permits exactly P1, P2, and
P3; it does not authorize paper, shadow, broker access, orders, capital, or
live trading.

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
