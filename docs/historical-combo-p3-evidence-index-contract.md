English | [简体中文](historical-combo-p3-evidence-index-contract.zh-CN.md)

# Historical Combo P3 Evidence Index Contract

`qsl.us-equity-historical-combo-p3-evidence-index.v1` is the retained **summary
index** produced after a future historical portfolio replay completes. It binds
only: candidate ID, P1 input SHA-256, P2 frozen candidate SHA-256, full P3
evidence SHA-256, outcome status/verdict, and the GitHub Actions run identity
that generated it.

The index contains no raw market data, constituent holdings, secrets, orders, or
funds data. It enforces `RESEARCH_ONLY` and permanently disables promotion,
paper, shadow, and live authorization. Even when the verdict is
`PASS_RESEARCH_EVIDENCE_NOT_PROMOTION`, it means only that research evidence
completed, not that the strategy passed promotion or is eligible to execute.

No portfolio P3 runner or P3 evidence exists yet. This index only prepares a
verifiable P1 -> P2 -> P3 identity chain for a future runner; by itself it reads
no data, runs no backtest, writes nothing remotely, and triggers no workflow.
