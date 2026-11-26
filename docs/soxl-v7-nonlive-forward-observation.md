English | [简体中文](soxl-v7-nonlive-forward-observation.zh-CN.md)

# SOXL V7 Non-Live Forward Observation

`SOXL V7 Non-Live Forward Observation` runs after each fully closed XNYS trading day and emits one create-only P4 receipt for the frozen candidate
`soxl_soxx_core_only_p2_v7_longterm_compounding_cash_reserve`.
It runs both candidate Shadow decisions and a **simulated** 10 bps Paper replay; neither connects to a broker, alters platform runtime targets, or submits orders.

## Automated scope

The workflow builds sanitized inputs from the private P1 root, binding P1/P2/P3, risk policy, strategy release version, and the previous receipt digest. When data is incomplete, the prior receipt is unreadable, identities mismatch, or risk/control state is abnormal, it writes `PARKED` or a pause record and never bypasses the condition.

Observation sequence numbers must advance strictly by XNYS trading day. If free data sources are not yet ready for the current day, the workflow does not append a newer date out of order; subsequent runs pick the **earliest missing trading day** to backfill, creating at most one receipt per run. Continuous observation resumes automatically once data recovers instead of stalling the 252-session window permanently over a skipped day.

When P1 parks because free dual sources could not verify, the workflow writes sanitized reason codes and handling directives into the summary. For example, `YAHOO_SETTLEMENT_LAG` indicates Twelve Data already covers the closed session while Yahoo lacks only that latest bar; it still cannot publish from a single source and instead marks `RETRY_NEXT_SCHEDULED_SESSION`. Any other provider, coverage, or price-agreement failure stays `PARK_AND_DIAGNOSE` with no automatic source switching, tolerance relaxation, or data fabrication.

## Read-only website projection

Each existing V7 record projects into an independent `qsl_control_plane_source_snapshot.v1` source `uesp-soxl-v7-nonlive-forward` for display via the existing control-plane sync endpoint. Projection first verifies the record's own SHA, frozen candidate/config/P4 policy, pinned P3 and dependency digests, 252-session controller, receipt index, XNYS session list, and `no_order=true`, no broker dependencies, and no Live authority. It uses only the checked-in record's controller state and never fabricates P3 or paired results from health strings.

`FORWARD_ACTIVE` displays as P4 Shadow observation recommending continued automatic Shadow evaluation; paused, parked, or blocked states display as P4 parked; completion of 252 sessions displays as P4 evidence pending for further research without advancing to P6. `computed_at`, `forward_observation.observed_at`, and last observed date preserve the original record timestamps; the new `generated_at` indicates projection time only, so freshness age still tracks original observation time under existing trading-day staleness rules.

To manually republish website projection, dispatch the workflow with `date_cutoff` and `publish_only=true`. This mode reads only the existing immutable record at the matching GCS path; a missing record halts with `PUBLISH_ONLY_RECORD_MISSING` without falling back to P1 acquisition, Shadow, or simulated Paper runners. Normal runs execute the same projection after a create-only record succeeds. Upload issues one `urllib` POST using a token passed strictly through environment variables and the `Authorization` header, with no automatic retry on ambiguous outcomes. Because the record is already persisted, a display-sync failure never requires re-running observation.

## Explicit non-goals

This automation is solely a P4 evidence collection and safe-park/recovery mechanism:

- It is not a broker Paper account and enables no platform `runtime target`;
- It is not automatic strategy promotion and cannot move a candidate to live trading;
- After 252 trading sessions it stops at `FORWARD_COMPLETE_HUMAN_REVIEW`; first live run, live restart, capital scaling, or parameter edits require separate human approval and fresh verification.

It can therefore complete and protect forward evidence unattended without expanding trading authority.

## P4 Financial Evaluation and Manual Research Review

This stage wires only the downstream review path for frozen V7. When `SOXL_V7_RESEARCH_REVIEW_ENABLED=true`, the existing observation entrypoint invokes the pinned P4 evaluator only after `FORWARD_COMPLETE_HUMAN_REVIEW`, consuming the same P1 manifest, candidate, and initial 252-XNYS window across the fixed `5 / 10 / 15 bps` financial evaluations. Intermediate 20/60 checkpoints, the 252-day policy, frozen UES/QPK replay, data sources, and execution authority remain unchanged. Reaching the session count does not imply financial approval.

Evaluation failure: persists the negative verdict, creates no manual review ticket, and performs no automatic recomputation or tuning.
Evaluation pass: generates a QPK research ticket honestly labeled `drift_status=not_applicable` and
`v7_nonlive_shadow_and_simulated_paper`, never disguised as drift tuning or paired shadow.
The ticket lists costs, strategy and SOXX drawdown/Calmar metrics, candidate identity, and evidence references. Website accept/reject records research intent with
`live_authority_granted=false`; no strategy is enabled, no position changes, and no order call is made.

To prevent losing financial evaluation results if execution interrupts after final observation writes, the workflow first persists the original record, financial summary, and nullable ticket in one create-only `completed-review.json` before publishing the observation receipt.
Subsequent runs recover this pinned result instead of re-acquiring data or re-running evaluation, including negative results.
`ticket.json`, `attempted.json`, and `terminal.json` store the initial candidate, first delivery attempt, and verified human decision respectively. Attempt state is persisted before POST; on unknown POST outcomes, the workflow reads back state without re-writing, parking on missing readback, material conflicts, or storage errors. Human decisions are validated through QPK's existing reconcile flow before persistence.

These objects reside in a separate prefix of the existing bucket,
`strategy-lifecycle/v1/us_equity/soxl-v7-research-review/`, isolated from the date-ordered observation records directory.
Before enabling, operators must verify read/create permissions on that prefix for the existing WIF identity and configure dedicated `RESEARCH_PROMOTION_SYNC_URL` and `RESEARCH_PROMOTION_SYNC_TOKEN` bindings in environment `market-data-nonlive`. Also set `QSL_CONSOLE_ORIGIN` to the bare console origin (for example `https://console.example.com`); the sync URL must equal that origin followed by `/api/internal/sync-research-promotion-ticket`, otherwise delivery parks before the token is sent.
The URL must target the console's `/api/internal/sync-research-promotion-ticket` endpoint; borrowing other tokens is forbidden.
With the flag unset by default, no remote review-sync path is active. Local tests do not verify those production bindings.

The QPK 736 ticket client runs in an isolated environment defined by `scripts/requirements-soxl-v7-review-control.txt`, leaving main environment `5c` and frozen replay `f30` dependencies untouched. CI runs ticket-recovery tests separately to prevent stale QPK mismatches in the main environment. Cross-repo integration runs `tests/test_soxl_v7_review_console_integration.py` using explicit `QRT_WORKER_PATH` and local Node with labeled synthetic market/replay fixtures against a real Worker and in-memory KV, covering accept/reject, QPK sync/readback/reconcile, and cold starts.

If the flag is enabled only after final observation completes and no `completed-review.json` exists, the flow emits
`PARKED_FIXED_FINANCIAL_CHECKPOINT_MISSING` rather than fabricating a PASS from the final count.
Any missing initial financial evaluation must be recovered separately within the original frozen materials; automatic historical recomputation is not authorized.
