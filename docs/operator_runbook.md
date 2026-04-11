# Operator runbook

This repo is the upstream artifact producer for snapshot-backed US equity strategies. Broker platform repos remain downstream consumers.

## Current profiles

- `tech_communication_pullback_enhancement`
- `russell_1000_multi_factor_defensive`

## Manual local build

Tech/communication pullback:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_tech_communication_pullback_snapshot.py \
  --prices /path/to/price_history.csv \
  --universe /path/to/universe.csv \
  --as-of 2026-04-01 \
  --output-dir data/output/tech_communication_pullback_enhancement
```

Russell 1000:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_russell_1000_feature_snapshot.py \
  --prices /path/to/r1000_price_history.csv \
  --universe /path/to/r1000_universe_history.csv \
  --as-of 2026-04-01 \
  --output-dir data/output/russell_1000_multi_factor_defensive
```

## Manual GitHub Actions build

Use the `Publish Snapshot Artifacts` workflow.

Required inputs:

- `profile`
- `prices_path`
- `universe_path`

Optional inputs:

- `as_of_date`
- `artifact_dir`
- `gcs_prefix`
- `config_path` for `tech_communication_pullback_enhancement`
- `current_holdings` for hold-bonus preview
- `portfolio_total_equity` for tech pullback dynamic position-count preview
- `min_adv20_usd` for Russell testing / overrides

The workflow always uploads the generated files as a GitHub Actions artifact.

## Publishing mode

Leave `execute_publish=false` first. In that mode the workflow only prints the `gcloud storage cp` plan and does not write to GCS.

When the dry-run plan looks correct, rerun with:

- `gcs_prefix` set to the target GCS prefix
- `execute_publish=true`

The initial migration should publish to a staging prefix first, for example:

```text
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/tech_communication_pullback_enhancement_staging
```

Do not overwrite the current HK production prefix until the platform guard has been tested against the staging artifact.

## Current production path policy

Do not change Cloud Run env in this step. HK can keep reading the existing production path until the new upstream publisher has been verified.

Current HK production path:

```text
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/interactive_brokers/tech_communication_pullback_enhancement/tech_communication_pullback_enhancement_feature_snapshot_latest.csv
```

Recommended future neutral prefix:

```text
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/tech_communication_pullback_enhancement/
```

## GitHub environment variables for GCS publish

Set these repository or environment variables before using `execute_publish=true`:

- `GCP_PROJECT_ID` (defaults to `interactivebrokersquant` if unset)
- `GCP_WORKLOAD_IDENTITY_PROVIDER`
- `GCP_WORKLOAD_IDENTITY_SERVICE_ACCOUNT`

The service account must have permission to write the chosen GCS prefix.
