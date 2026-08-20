# Multi-Strategy Research Driver Template v1

This template is the shared **research route description** for independently
frozen strategies. It is not a generic execution engine and it does not make
one strategy's data, parameters, evidence, or promotion result reusable by
another strategy.

Every route records only:

- a research identity and immutable P2 configuration digest;
- its own P1 input-contract identifier;
- the P3 replay entrypoint that must verify that strategy's own input;
- one explicit state: `DAILY_RESEARCH_WIRED` or `MIGRATION_REQUIRED`;
- explicit blockers whenever a legacy research route is not ready for a daily
  scheduler.

The catalogue is deliberately limited to P1--P3 and has exact
`research_only`, `no_order`, and no-P4/P5/P6 authority. It does not resolve a
route, acquire data, schedule work, read credentials, load a plugin, change
targets, create a broker adapter, or write an artifact.

## Current routes

| Route | State | Meaning |
| --- | --- | --- |
| TQQQ core-only v5 | `DAILY_RESEARCH_WIRED` | The existing TQQQ controller is the first concrete implementation. Its P1 data, frozen P2 candidate, P3 replay, v6 observation record, and forward observation remain strategy-specific. |
| SOXL/SOXX core-only P2 v2 | `MIGRATION_REQUIRED` | The fresh [P2 v2 candidate](soxl-core-only-p2-v2-research.md) has a three-asset P1 identity, an independent P1-bars-to-context materializer, and isolated stateful replay primitives. It still has no daily P1 publisher, fixed-window P3 evidence verifier, or scheduler. Historical SOXL research remains context only and cannot be scheduled or reused as current evidence. |

## Safe extension rule

To add a strategy such as SOXL, IBIT, or a portfolio, first freeze its own P2
configuration. The route can move from `MIGRATION_REQUIRED` to
`DAILY_RESEARCH_WIRED` only after a dedicated P1 adapter, P3 verifier, tests,
and non-live workflow exist. The strategy's evidence cannot be inherited from
TQQQ, and a plugin remains a read-only signal until a separate frozen strategy
candidate explicitly consumes it.
