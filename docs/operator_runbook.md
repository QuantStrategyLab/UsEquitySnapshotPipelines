# Operator Runbook

[简体中文](operator_runbook.zh-CN.md)

This repo is the upstream artifact producer for snapshot-backed US equity strategies. Broker platform repos remain downstream consumers.

## Snapshot Profiles

Runtime-facing snapshot profiles:

- `russell_top50_leader_rotation`
- `global_etf_rotation`

`russell_1000_multi_factor_defensive` is retired from this repository's runtime contract after failing to justify its complexity versus direct SPY exposure. `tech_communication_pullback_enhancement` profile contract and all its source modules have been removed (archived research-only; see git history for the last snapshot before cleanup).

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
| `global_etf_rotation` | none | `gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/global_etf_rotation` |

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

The publish workflow runs this automatically after a successful Russell snapshot build. Outputs are included in the uploaded GitHub Actions artifact.

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
- `russell_top50_leader_rotation_rebalance_trades.csv`

Rollback procedure (runtime config only; does not change research artifacts):

1. Keep actual positions on the approved active variant unless an operator explicitly approves a switch.
2. To roll back live shape, set `leader_rotation_profile_variant` to `top4_baseline`.
3. To use the conservative shape, set `leader_rotation_profile_variant` to `blend_top2_25_top4_75`.
4. Keep `leader_rotation_shadow_variants=True` in paper/operator-review mode until one shadow cycle is archived for the month.

### Russell Live Ledger

After a rebalance executes in the broker, the shadow live ledger tracks forward performance,
slippage, and position weights against the theoretical target.

The publish workflow emits a `russell_top50_leader_rotation_rebalance_trades.csv` in the
uploaded artifact. Download it and run the ledger:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_shadow_live_ledger \
  --rebalance-trades data/output/russell_top50_shadow_cycle_YYYYMMDD/russell_top50_leader_rotation_rebalance_trades.csv \
  --daily-returns path/to/daily_returns.csv \
  --portfolio-nav 1234567.89 \
  --output-dir data/output/russell_live_ledger_YYYYMMDD
```

Or use the `Run Russell Live Ledger` workflow dispatch in GitHub Actions.

Outputs:

- `shadow_live_trade_ledger.csv`
- `shadow_live_holdings_ledger.csv`
- `shadow_live_rebalance_summary.csv`
- `shadow_live_ledger_manifest.json`

## Global ETF Shadow Cycle (variant comparison)

Evaluates the Global ETF rotation strategy with 4 runtime configurations against the published feature snapshot:

| Variant | Effect |
| --- | --- |
| `active` | Current default (confidence-weighted top-2 with volatility gate) |
| `equal_weight` | Equal-weight top-2 (disables confidence weighting) |
| `no_vol_gate` | Top-2 without volatility gate |
| `top_1` | Single-best pick (top_n=1) |

Local shadow cycle from a published snapshot:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/run_global_etf_rotation_shadow_cycle.py \
  --feature-snapshot data/output/global_etf_rotation_staging/global_etf_rotation_feature_snapshot_latest.csv \
  --snapshot-as-of YYYY-MM-DD \
  --output-dir data/output/global_etf_shadow_cycle_YYYYMMDD
```

Outputs:

- `global_etf_rotation_runtime_diagnostics.json`
- `global_etf_rotation_variant_comparison.json`

The publish workflow runs this automatically after a successful Global ETF snapshot build.

The publish workflow keeps a defensive month-end trading-day guard: if the resolved `snapshot_as_of` is not the last NYSE trading day of that snapshot month, it writes a skip artifact and does not publish to GCS.

## Monthly AI Review

`Monthly Snapshot Review` is a report-only workflow: it builds the existing health,
promotion-readiness, and review evidence bundle, uploads that bundle, and creates
or updates a `monthly-review` issue.

AIAuditBridge review, retry, and merge dispatch paths are retired. GitHub Codex
App is the sole AI reviewer for pull requests. This workflow does not dispatch an
AI reviewer, create remediation pull requests, retry feedback, or request or
perform automatic merges. The evidence bundle is advisory only and is not an
approval or merge signal.

Repository settings, legacy variables, and secrets are outside this workflow's
contract. Do not infer their runtime state from this document; any settings change
requires its own authenticated, explicitly authorized procedure.
