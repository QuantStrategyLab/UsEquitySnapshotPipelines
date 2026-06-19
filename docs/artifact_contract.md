# US equity snapshot artifact contract

The upstream pipeline publishes strategy artifacts; broker platforms only consume them.

## Standard files

For each profile, the standard local output directory contains:

- `<profile>_feature_snapshot_latest.csv`
- `<profile>_feature_snapshot_latest.csv.manifest.json`
- `<profile>_ranking_latest.csv`
- `release_status_summary.json`

## Manifest fields

Required baseline fields:

- `manifest_type = feature_snapshot`
- `contract_version`
- `strategy_profile`
- `config_name`
- `config_path`
- `config_sha256`
- `snapshot_path`
- `snapshot_sha256`
- `snapshot_as_of`
- `row_count`
- `generated_at`
- `source_project = UsEquitySnapshotPipelines`

## Current runtime path discipline

Current runtime-facing US equity snapshot artifacts are published under the single profile-specific `us_equity` prefix:

```text
gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/russell_top50_leader_rotation_staging/
```

`russell_1000_multi_factor_defensive` is retired from the runtime artifact contract. `tech_communication_pullback_enhancement` remains archived research-only and is no longer exposed by scheduled publish, artifact health, or runtime switching workflows.
