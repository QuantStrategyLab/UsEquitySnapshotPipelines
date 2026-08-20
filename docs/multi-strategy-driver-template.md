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
| SOXL/SOXX core-only legacy research | `MIGRATION_REQUIRED` | Existing SOXL research is valuable historical input, but its fixed historical cutoff and legacy source binding cannot be treated as a current daily P1/P2/P3 route. It must receive a new frozen candidate and independently verified daily input before scheduling. |

## Safe extension rule

To add a strategy such as SOXL, IBIT, or a portfolio, first create a route with
its own input contract and frozen configuration. The route can move from
`MIGRATION_REQUIRED` to `DAILY_RESEARCH_WIRED` only after a dedicated P1
adapter, P2 candidate, P3 verifier, tests, and non-live workflow exist. The
strategy's evidence cannot be inherited from TQQQ, and a plugin remains a
read-only signal until a separate frozen strategy candidate explicitly
consumes it.
