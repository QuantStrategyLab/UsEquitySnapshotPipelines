English | [简体中文](historical-combo-p1-p3-input-contract.zh-CN.md)

# Historical Combo P1 Input and P3 Pre-Verification Contract

This slice connects the existing portfolio research contracts without running any
backtest:

- UES `qsl.us-equity-historical-combo-p2-candidate.v1` remains the sole owner
  of frozen P2 candidates;
- UES `qsl.us-equity-virtual-combo-target.v1` remains the sole owner of
  virtual portfolio targets;
- UESP `qsl.us-equity-historical-combo-p3-evidence-index.v1` remains the
  retained index for future completed P3 evidence; and
- `qsl.us-equity-historical-combo-p1-input-binding.v1` and
  `qsl.us-equity-historical-combo-p3-input-verification.v1` fix and verify
  input identities prior to P3 replay.

Accordingly, this module reads or downloads no market data, accesses no
credentials or GCS, produces no portfolio returns, cost results, drawdowns, or
promotion conclusions, and holds no scheduler, paper, shadow, broker, or order
capability.

## P1: Common historical input identity

The P1 binding is a self-hashed SHA-256 metadata object that fixes:

- Portfolio candidate ID, candidate revision, and configuration digest;
- At least two component strategies strictly sorted by `leg_id`, each with its
  revision, configuration digest, own P1 input digest, and identical
  `source_date_cutoff`;
- The common `common_cutoff`;
- A mandatory `AS_OF_COMMON_CUTOFF` PIT declaration: no future data, no
  post-hoc revisions, and fixed signal and fill timing;
- A non-empty, ascending explicit cost-stress array plus borrow and cash yield
  assumptions; and
- Frozen virtual-combo policy digest, `portfolio_risk_budget` policy digest, and
  a digest projected from the existing virtual portfolio target.

That digest first verifies the upstream virtual target's own SHA-256 and retains
only its schema, status, policy, input, and target digests without copying
virtual weights or risk metrics. Because the P2 descriptor back-references the
P1 input digest, P1 cannot embed the P2 descriptor directly without creating a
circular identity. P1 fixes the P2 policy and virtual target, and P3
pre-verification checks the two-way reference between the P2 descriptor and P1.

## P3: Pre-verification, not performance evidence

`verify_historical_combo_p3_inputs` accepts only the P1 binding and an existing
UES P2 descriptor. Before any future replay runner is invoked, it checks that:

- P2 self-digest, research-only flag, and all P4/P5/P6 flags are valid/false;
- P2 `p1_input_sha256` matches the P1 self-digest;
- The candidate and every component strategy revision/config digest match P1;
- The P2 risk-budget digest matches the budget digest frozen in P1; and
- Neither the selection window nor the holdout window ends after `common_cutoff`.

When intact, it returns `READY_FOR_P3_REPLAY`. This means only that the input
identity is valid to hand to a future standalone replayer; it never indicates
strategy validity, promotion readiness, paper/shadow eligibility, or live
readiness. It returns verifiable digests (P1/P2 digests, common cutoff, cost
declaration digest, virtual target digest) and returns no market data, weights,
performance, or orders.

Missing fields, digest mismatches, component revision drift, risk-policy drift,
or any selection/holdout date after `common_cutoff` return `PARKED`. `PARKED`
cannot be cleared by this module and triggers no workflow or external write.

## What is still missing

There is still no real, license-compliant common historical P1 input, no real
cost/OOS historical replayer, no P3 performance evidence, and no P4/P5/P6
wiring. The existing `historical_combo_p3_synthetic_replay` validates segmented
OOS and cost calculation on injected synthetic fixtures only and cannot replace
or be promoted into real historical performance evidence. Any future real replay
implementation must first consume this `READY_FOR_P3_REPLAY` summary and then
record completed results in the P3 evidence index; neither synthetic fixtures
nor `READY` status represents research approval or execution eligibility.
