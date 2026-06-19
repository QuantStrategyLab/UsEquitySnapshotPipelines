# Operator Runbook

[简体中文](operator_runbook.zh-CN.md)

This repo is the upstream artifact producer for snapshot-backed US equity strategies. Broker platform repos remain downstream consumers.

## Snapshot Profiles

The only runtime-facing snapshot profile produced here is:

- `russell_top50_leader_rotation`

`russell_1000_multi_factor_defensive` is retired from this repository's runtime contract after failing to justify its complexity versus direct SPY exposure. `tech_communication_pullback_enhancement`, `mega_cap_leader_rotation_dynamic_top20`, `mega_cap_leader_rotation_aggressive`, and `dynamic_mega_leveraged_pullback` are archived research-only and are no longer exposed by publish or health workflows.

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

Direct bridge PRs are not auto-merged by default. Set `CODEX_AUDIT_AUTO_MERGE=true` only after branch protection and CI gates are confirmed. The legacy ccbot-style remediation path should still open a draft PR first and add `auto-merge-ok` only after targeted tests pass and the change stays inside low-risk docs/tests/monthly-review surfaces.

If CI fails on a Codex remediation PR, or a reviewer requests changes, `Codex PR Feedback` comments the failure or review summary back to the source `codex-bridge` issue. That issue update lets the VPS bridge dispatch Codex again to fix the same PR branch. The workflow permits up to three automatic feedback rounds, then removes `codex-bridge` and leaves the issue for human review.
