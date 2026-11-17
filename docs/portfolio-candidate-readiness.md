English | [简体中文](portfolio-candidate-readiness.zh-CN.md)

# Portfolio Candidate Readiness Re-Evaluation (Research Only)

`portfolio-candidate-readiness` is a low-frequency, sanitized re-evaluator. It
reads only the Actions P1/P3 terminal artifacts already published by the TQQQ v5
and SOXL v3 daily workflows and emits `qsl.portfolio-candidate-readiness.v1`.

It downloads or reads no bars, accesses no GCS, credentials, accounts, or
brokers, and never triggers, retries, or waits on any daily workflow.

Status becomes `AI_RESEARCH_PROPOSAL_READY` only when all of the following hold:

- Both registered single-strategy candidates have an `ACCEPTED` P1;
- Both candidates have a complete current P3 terminal record; and
- Both P1 records share the same `date_cutoff`.

When both P1 terminals exist, any unmet condition is emitted honestly as
`PARKED` with reason codes; if a P1 terminal is missing, no status record is
fabricated and the job fails closed without creating an Issue or invoking AI. A
sanitized readiness artifact is still retained for 35 days for read-only
consumption by the console or AIAuditBridge.

If and only if `AI_RESEARCH_PROPOSAL_READY` is reached, the workflow creates or
updates one deduplicated AI research task in this repository. That task signals
only that a portfolio research hypothesis may be proposed: it selects no
weights, freezes no P2 candidate, seals no common P1 root, runs no P3 replay,
and grants no P4 paper, P5 shadow, P6 live, account, or order authority. A real
portfolio still requires independently frozen candidates, common historical P1
inputs, cost/risk policy, and P3 evidence.
