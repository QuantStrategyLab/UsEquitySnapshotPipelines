English | [简体中文](historical-combo-p3-synthetic-replay-contract.zh-CN.md)

# Historical Combo P3 Synthetic Cost and OOS Replay Contract

`qsl.us-equity-historical-combo-p3-synthetic-replay.v1` is an **offline test
slice** layered over the existing portfolio P1/P2/P3 contracts. It accepts only
a caller-injected `synthetic_fixture` to verify that frozen identities,
segmented OOS windows, turnover cost calculation, and metric summaries reproduce
deterministically.

It is neither a historical market-data replayer nor evidence for portfolio
performance, promotion, paper, shadow, or live trading.

## Pre-checks and strict binding

Every run first calls `verify_historical_combo_p3_inputs`. The replayer accepts
the fixture only when pre-verification returns `READY_FOR_P3_REPLAY`; otherwise
it returns `PARKED`. The fixture then self-verifies and binds:

- Frozen P1/P2 digests and the P3 preflight digest;
- P1's `common_cutoff`, PIT declaration digest, and cost declaration digest;
- Non-overlapping OOS segments inside P2's frozen holdout, sorted by time and
  segment ID; and
- Ascending synthetic daily returns per segment covering only frozen `leg_id`s.

Any drift in component set, digests, PIT declaration, or cost declaration returns
`PARKED`. Any segment or observation date after `common_cutoff` halts with
`FUTURE_LEAKAGE_DETECTED`; observations from the selection window are also
rejected. Fixing weights, strategy revisions, cost assumptions, and OOS
boundaries prevents a fixture from introducing lookahead via edited inputs.

## Synthetic costs and metrics

Cost scenarios come strictly from P1's frozen `turnover_cost_bps` array and
cannot be added, removed, or tuned by the fixture. P2 currently requires all
sleeves to be positive and sum to 100%, so each segment starts at the existing
target weights and computes one-way turnover to rebalance back to target weights
after each day's return drift; the final observation day does not invent a
next-period rebalance or liquidation cost.

Output lists synthetic gross/net total return, net max drawdown, one-way
turnover, and applied turnover cost rate per segment, alongside cross-segment
mean, worst-case, and total turnover summaries. Borrow and cash assumptions are
preserved verbatim, though in this fully invested long-only P2 model they do
not apply and are never fabricated as realized returns or costs.

Output status may only be `SYNTHETIC_REPLAY_COMPLETE_NOT_REAL_EVIDENCE` or
`PARKED`, with fixed flags:

- `research_only=true`, `execution_authorized=false`,
  `real_market_evidence=false`;
- `promotion_recommendation=null`;
- `paper_authorized=false`, `shadow_authorized=false`,
  `live_authorized=false`.

This completion status **cannot** be passed to
`historical_combo_p3_evidence_index`, which accepts only
`HISTORICAL_COMBO_RESEARCH_EVIDENCE_COMPLETE` from real, complete P3 research
evidence. This slice touches no market data, files, network, credentials, GCS,
workflows, schedulers, accounts, or brokers, and writes no artifacts.

## Prerequisites before wiring real historical data

Any future standalone real-replay adapter must separately provide:
license-compliant, reproducible common P1 raw inputs; per-observation as-of,
revision, and corporate-action provenance; frozen trading calendar and
signal-to-fill timing; auditable cost, borrow, cash, and liquidity models; full
holdout and rolling OOS coverage; and a restricted artifact path writing real
results and evidence digests to the P3 evidence index. It must never reuse or
upgrade any synthetic result from this slice.
