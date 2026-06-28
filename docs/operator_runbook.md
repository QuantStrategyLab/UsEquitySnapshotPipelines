# Operator Runbook

[简体中文](operator_runbook.zh-CN.md)

This repo is the upstream artifact producer for snapshot-backed US equity strategies. Broker platform repos remain downstream consumers.

## Snapshot Profiles

The only runtime-facing snapshot profile produced here is:

- `russell_top50_leader_rotation`

`russell_1000_multi_factor_defensive` is retired from this repository's runtime contract after failing to justify its complexity versus direct SPY exposure. `tech_communication_pullback_enhancement` and the retired dynamic/leveraged Russell Top50 research variants are archived research-only and are no longer exposed by publish or health workflows.

## Manual Local Build

Russell Top50 leader rotation:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_russell_top50_leader_rotation_snapshot.py \
  --prices /path/to/r1000_price_history.csv \
  --universe /path/to/r1000_latest_holdings_snapshot.csv \
  --as-of 2026-04-01 \
  --dynamic-universe-size 50 \
  --output-dir data/output/russell_top50_leader_rotation
```

## Manual GitHub Actions Build

Use the `Publish Snapshot Artifacts` workflow.

Required input:

- `profile`, currently only `russell_top50_leader_rotation`

For production data, set both:

- `prices_path`
- `universe_path`

Optional inputs:

- `as_of_date`
- `artifact_dir`
- `gcs_prefix`
- `current_holdings`
- `portfolio_total_equity`
- `min_adv20_usd` for Russell Top50 testing overrides

For the strategy-plugin publish workflow, manual GCS prefix overrides are only
accepted when `execute_publish=true` if they remain under
`gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/<scope>/plugins/<plugin>`.

`Publish Strategy Plugins` builds both strategy artifacts and the unified
`notification_targets.market_regime_notification` artifact. The manual-review
plugin bot should consume that notification target only; TQQQ, SOXL, and other
strategy artifacts remain for strategy runtime consumption, with any actual
position effect reported by the strategy run notification.

Unified alert delivery uses `STRATEGY_PLUGIN_ALERT_*` vars/secrets, defaults to
Chinese via `STRATEGY_PLUGIN_ALERT_LANG=zh`, and uses
`STRATEGY_PLUGIN_ALERT_STATE_GCS_URI` for cross-run dedupe. If delivery
credentials are missing, the workflow writes skipped diagnostics without
blocking artifact publication.

The workflow always uploads generated files as a GitHub Actions artifact.

## Scheduled Publish

`Update Source Input Data` runs once per month at `00:15 UTC` on the 1st day of the month. It refreshes the shared Russell 1000 inputs used by the monthly snapshot profile:

```text
gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_price_history.csv
gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_universe_history.csv
gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_symbol_aliases.csv
gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_universe_snapshot_metadata.csv
gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_latest_holdings_snapshot.csv
```

`Publish Snapshot Artifacts` runs after source-input refresh and builds:

```text
profiles=russell_top50_leader_rotation
prices_path=gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_price_history.csv
universe_path=gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_latest_holdings_snapshot.csv
execute_publish=true
```

Default scheduled output prefix:

| Profile | Extra config | GCS prefix |
| --- | --- | --- |
| `russell_top50_leader_rotation` | none | `gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/russell_top50_leader_rotation_staging` |

## Russell Phase-1 Shadow Cycle (named variants + rollback review)

Runtime code lives in `UsEquityStrategies`. The pipeline repo archives operator-facing shadow review rows after a deterministic evaluation against the published feature snapshot.

Named runtime variants:

| `leader_rotation_profile_variant` | Role |
| --- | --- |
| `blend_top2_50_top4_50` | Current default balanced offensive shape |
| `blend_top2_25_top4_75` | Conservative override |
| `top4_baseline` | Rollback / fallback (no Top2 sleeve) |

Paper or operator-review runtime config:

```python
{
    "leader_rotation_profile_variant": "blend_top2_50_top4_50",
    "leader_rotation_shadow_variants": True,
}
```

The publish workflow runs this automatically after a successful snapshot build. Outputs are included in the uploaded GitHub Actions artifact.

Local shadow cycle from a published snapshot:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/run_russell_leader_rotation_shadow_cycle.py \
  --feature-snapshot data/output/russell_top50_leader_rotation_staging_YYYYMMDD/russell_top50_leader_rotation_feature_snapshot_latest.csv \
  --snapshot-as-of YYYY-MM-DD \
  --output-dir data/output/russell_top50_shadow_cycle_YYYYMMDD
```

Outputs:

- `russell_leader_rotation_runtime_diagnostics.json`
- `russell_leader_rotation_variant_comparison.json`
- `russell_top50_leader_rotation_shadow_review_rows.csv`
- `russell_top50_leader_rotation_shadow_review_manifest.json`

Rollback procedure (runtime config only; does not change research artifacts):

1. Keep actual positions on the approved active variant unless an operator explicitly approves a switch.
2. To roll back live shape, set `leader_rotation_profile_variant` to `top4_baseline`.
3. To use the conservative shape, set `leader_rotation_profile_variant` to `blend_top2_25_top4_75`.
4. Keep `leader_rotation_shadow_variants=True` in paper/operator-review mode until one shadow cycle is archived for the month.

The publish workflow keeps a defensive month-end trading-day guard: if the resolved `snapshot_as_of` is not the last NYSE trading day of that snapshot month, it writes a skip artifact and does not publish to GCS.

## Monthly AI Review

After a successful scheduled `Publish Snapshot Artifacts` run, `Monthly Snapshot Review` downloads that run's artifacts and assembles:

```text
data/output/monthly_report_bundle/monthly_report_bundle.json
data/output/monthly_report_bundle/ai_review_input.md
data/output/monthly_report_bundle/job_summary.md
```

The workflow creates or updates a GitHub issue labeled `monthly-review`. By default it dispatches `QuantStrategyLab/CodexAuditBridge`, which calls the Quant HTTPS/443 service-backed Codex path and posts the audit result back to the issue. The review focuses on:

- artifact completeness for the expected snapshot profile
- contract version, snapshot date, row count, and ranking-preview health
- missing or stale evidence that should block downstream confidence
- downstream impact for broker/runtime repositories

The monthly review workflow dispatches `CodexAuditBridge`; the bridge owns provider selection through `CODEX_AUDIT_PROVIDER`. `auto` is the default and calls the service-backed Codex path first, falls back to the configured API reviewers when Codex service execution fails, and fails loudly when no API fallback key is configured. Set `CODEX_AUDIT_BRIDGE_REF` to pin the bridge workflow ref; the default is `main`. `codex` uses only the service-backed Codex path and disables API fallback; `api` posts a combined API review; `openai` and `anthropic` post a single-provider API review only.

Direct bridge PRs are not auto-merged by default. Set `CODEX_AUDIT_AUTO_MERGE=true` only after branch protection or repository rulesets, CI gates, the source `Auto Merge Codex Remediation PR` guard, and the `Codex PR Feedback` retry workflow are confirmed. When guarded auto-merge is requested, `Monthly Snapshot Review` first runs `scripts/sync_codex_auto_merge_labels.py` to create the configured `auto-merge-ok` / `human-review-required` labels if they are missing. Before flipping the variable, run `scripts/plan_codex_auto_merge_enablement.py` and complete its enablement preflight checklist, including review of any existing branch protection or ruleset settings. The source auto-merge workflow is hard-gated by `CODEX_AUDIT_AUTO_MERGE`, so it skips when the variable is unset or false even if a PR already carries `auto-merge-ok`. Required CI checks default to `test`; set repository variable `CODEX_AUDIT_REQUIRED_STATUS_CHECKS` to a comma- or newline-separated list before enablement if branch protection uses different check names. `Monthly Snapshot Review` still dispatches CodexAuditBridge when readiness is incomplete, but downgrades that dispatch to `auto_merge=false` so audit and PR creation continue without requesting guarded auto-merge. If the default workflow token cannot read branch protection, rulesets, or label settings in the deployed repository, configure `CODEX_AUDIT_READINESS_TOKEN` as a source-repository secret for readiness checks only. The intended unattended path is: Codex opens a small remediation PR, CI passes, the PR carries the monthly remediation marker and `auto-merge-ok`, then the source merge guard confirms there is no active requested-changes review decision, classifies the changed-file surface, and enforces `max_changed_files` (`20` by default) and `max_changed_lines` (`1200` by default) before merging. The auto-merge workflow skips stale successful CI runs when the run head SHA no longer matches the current PR head, and the final merge is also pinned to the CI `workflow_run.head_sha`; a newer push to the same PR branch requires a fresh successful CI run. High-risk changes such as strategy logic, live profile contracts, dependencies, secrets, broker/runtime settings, live allocation, data artifacts, or strategy deletion/disablement must stay human-reviewed; CodexAuditBridge labels those generated PRs `human-review-required` and records the risk files in the source issue.
Each monthly review bundle artifact also includes `codex_auto_merge_label_sync.md`, `codex_auto_merge_readiness.md`, `codex_auto_merge_enablement_plan.md`, and the rendered `codex_auto_merge_preflight_comment.md` when the audit path is enabled. The workflow also upserts that short guarded auto-merge preflight comment on the monthly review issue, including the label-sync, readiness, and enablement checklist snapshots. Operators can use those records to audit the preflight state that was used before any guarded auto-merge dispatch decision.

If CI fails on a same-repository Codex remediation PR, or a reviewer requests changes on that PR, `Codex PR Feedback` first skips stale CI-failure runs whose head SHA no longer matches the current PR head. For current failures or requested changes, it removes any stale configured guarded auto-merge label from that PR on a best-effort basis after validating that the policy labels are readable and distinct; if the policy is invalid or the auto-merge and human-review labels collide, it skips label mutation instead of guessing. It then comments the failure or review summary back to the source `codex-bridge` issue and dispatches `CodexAuditBridge` again for the same issue. Feedback retries run the same guarded auto-merge readiness check; when readiness is incomplete, the retry still runs but passes `auto_merge=false`. The bridge resolves the latest feedback marker and updates the existing PR branch when it is still open, same-repository, and tied to the same monthly issue. The workflow permits `CODEX_AUDIT_MAX_FEEDBACK_ROUNDS` automatic feedback rounds, defaulting to `3`; invalid values fall back to `3`, and values are clamped to `1` through `10`. Once that limit is reached, it removes `codex-bridge`, marks the PR with the configured human-review label on a best-effort basis, creating that label first if it is missing and permissions allow it, and leaves the issue for human review. The feedback workflow uploads `codex-pr-feedback-ci-<run_id>` or `codex-pr-feedback-review-<run_id>` diagnostics even when later steps fail; inspect those artifacts for `comment.md`, `comment_pages.json`, `pr.json`, and `codex_auto_merge_readiness.md` before rerunning manually.

When guarded auto-merge is enabled, `Auto Merge Codex Remediation PR` uploads `codex-auto-merge-<run_id>` diagnostics on every run. Use `decision.json`, `summary.md`, `pr.json`, `guard_decision_comment.md`, and `readiness.md` from that artifact to explain why a PR merged, skipped, failed readiness, or was blocked by the merge guard. If the merge guard skips a PR, or if the final merge-time readiness check fails after the merge guard was otherwise ready, the workflow also upserts a short decision comment on the monthly review issue so high-risk/manual-review cases are visible without opening artifacts first; it also tries to remove stale guarded auto-merge labels from the skipped PR and add the human-review label for high-risk decisions. The decision comment is written first; label hygiene action results are then appended to the same comment and artifact on a best-effort basis. If the follow-up comment update fails, the artifact records that failure, so a permission issue does not hide the reason unattended merge stopped.

See [`snapshot-ai-audit-automation.md`](snapshot-ai-audit-automation.md) for the risk tiers and unattended-maintenance policy.
