# US equity snapshot artifact contract


## 中文摘要

- 用途：本文档围绕 `US equity snapshot artifact contract`，用于理解 `UsEquitySnapshotPipelines` 的配置、运行、部署、研究或验收边界。
- 主要覆盖：`Standard files`、`Manifest fields`、`Current runtime path discipline`。
- 阅读顺序：先确认边界、输入输出和权限要求，再执行文档里的命令、CI、dry-run、发布或切换步骤。
- 风险提示：涉及实盘、密钥、权限、Cloud Run、交易所或券商 API 的变更，必须先在测试环境或 dry-run 验证；不要只凭示例直接修改生产。
- 英文正文保留更完整的命令、字段名和配置键；如果摘要和正文不一致，以正文中的实际命令和配置为准。
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

Current runtime-facing US equity snapshot artifacts are published under profile-specific `us_equity` prefixes:

```text
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/russell_1000_multi_factor_defensive_staging/
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/mega_cap_leader_rotation_top50_balanced_staging/
```

`tech_communication_pullback_enhancement` remains as archived research-only code and is no longer exposed by scheduled publish, artifact health, or runtime switching workflows.
