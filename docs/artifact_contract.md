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

Migration starts by keeping the existing platform GCS paths unchanged. For the HK line this means the platform can keep reading:

```text
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/interactive_brokers/tech_communication_pullback_enhancement/tech_communication_pullback_enhancement_feature_snapshot_latest.csv
```

## Profile-specific input notes

`dynamic_mega_leveraged_pullback` publishes the same standard snapshot,
manifest, ranking, and release summary files as other snapshot profiles. Its
builder additionally requires an operator-maintained product map with
`underlying_symbol`, `trade_symbol`, `product_leverage`, and optional
`product_available` / `product_expense_ratio` columns. Missing mappings never
fall back to underlying stocks, and available rows must be approximately 2x
long products.

A later cleanup can move to a neutral prefix such as:

```text
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/tech_communication_pullback_enhancement/
```
