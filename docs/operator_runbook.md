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

Russell 1000 backtest:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_russell_1000_multi_factor_defensive.py \
  --prices /path/to/r1000_price_history.csv \
  --universe /path/to/r1000_universe_history.csv \
  --start 2019-01-01 \
  --output-dir data/output/russell_1000_multi_factor_defensive_backtest
```

## Manual GitHub Actions build

Use the `Publish Snapshot Artifacts` workflow.

Required inputs:

- `profile`

For production data, set both:

- `prices_path`
- `universe_path`

Each can be a checked-out local path, a `gs://...` path, or an `http(s)://...` URL. For `tech_communication_pullback_enhancement`, `config_path` also supports local / `gs://` / `http(s)` sources.

For workflow smoke tests, set `use_sample_data=true`; then `prices_path` and `universe_path` can stay blank.

Optional inputs:

- `as_of_date`
- `artifact_dir`
- `gcs_prefix`
- `config_path` for `tech_communication_pullback_enhancement`
- `current_holdings` for hold-bonus preview
- `portfolio_total_equity` for tech pullback dynamic position-count preview
- `min_adv20_usd` for Russell testing / overrides

The workflow always uploads the generated files as a GitHub Actions artifact.

## Scheduled publish

`Update Source Input Data` runs automatically on weekdays at `22:30 UTC`
(`06:30` the next day in Asia/Shanghai). It refreshes the shared Russell 1000
source inputs used by snapshot profiles:

```text
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_price_history.csv
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_universe_history.csv
```

The refresh workflow downloads the existing price history first, refreshes the
latest overlap window, downloads full history for newly discovered symbols, and
then publishes the merged input files. It also forces `QQQ`, `SPY`, and `BOXX`
into the price input so both QQQ-benchmark and SPY-benchmark snapshot profiles
have the reference symbols they need.

`Publish Snapshot Artifacts` then runs automatically on weekdays at `23:30 UTC`
(`07:30` the next day in Asia/Shanghai), leaving time for the source-input
refresh to finish first. Scheduled publish builds both migrated snapshot profiles
from the same refreshed source inputs:

```text
prices_path=gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_price_history.csv
universe_path=gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_universe_history.csv
execute_publish=true
```

Default scheduled output prefixes:

| Profile | Extra config | GCS prefix |
| --- | --- | --- |
| `tech_communication_pullback_enhancement` | `gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/tech_communication_pullback_enhancement/growth_pullback_tech_communication_pullback_enhancement.json` | `gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/tech_communication_pullback_enhancement_staging` |
| `russell_1000_multi_factor_defensive` | none | `gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/russell_1000_multi_factor_defensive_staging` |

These defaults can be overridden with repository variables:

- `SCHEDULED_US_EQUITY_PRICES_PATH`
- `SCHEDULED_US_EQUITY_UNIVERSE_PATH`
- `SCHEDULED_TECH_COMMUNICATION_PULLBACK_CONFIG_PATH`
- `SCHEDULED_TECH_COMMUNICATION_PULLBACK_GCS_PREFIX`
- `SCHEDULED_RUSSELL_1000_CONFIG_PATH`
- `SCHEDULED_RUSSELL_1000_GCS_PREFIX`

The older `SCHEDULED_US_EQUITY_CONFIG_PATH` and
`SCHEDULED_US_EQUITY_GCS_PREFIX` variables are still honored for
`tech_communication_pullback_enhancement`.

If the input refresh fails, the snapshot publish workflow can still run against
the last successfully published source inputs. Check `Update Source Input Data`
before trusting a new snapshot after data-provider errors.

Manual source-input dry-run example:

```bash
gh workflow run "Update Source Input Data" \
  --repo QuantStrategyLab/UsEquitySnapshotPipelines \
  -f execute_publish=false \
  -f output_prefix=gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias \
  -f universe_start=2018-01-01 \
  -f price_start=2018-01-01 \
  -f price_overlap_days=10 \
  -f benchmark_symbol=QQQ \
  -f safe_haven=BOXX \
  -f extra_symbols=QQQ,SPY,BOXX
```

Production-source dry-run example:

```bash
gh workflow run "Publish Snapshot Artifacts" \
  --repo QuantStrategyLab/UsEquitySnapshotPipelines \
  -f profile=tech_communication_pullback_enhancement \
  -f use_sample_data=false \
  -f prices_path=gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_price_history.csv \
  -f universe_path=gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_universe_history.csv \
  -f config_path=gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/tech_communication_pullback_enhancement/growth_pullback_tech_communication_pullback_enhancement.json \
  -f gcs_prefix=gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/tech_communication_pullback_enhancement_staging \
  -f execute_publish=false
```

Russell 1000 production-source dry-run example:

```bash
gh workflow run "Publish Snapshot Artifacts" \
  --repo QuantStrategyLab/UsEquitySnapshotPipelines \
  -f profile=russell_1000_multi_factor_defensive \
  -f use_sample_data=false \
  -f prices_path=gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_price_history.csv \
  -f universe_path=gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_universe_history.csv \
  -f gcs_prefix=gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/russell_1000_multi_factor_defensive_staging \
  -f execute_publish=false
```

Smoke-test command:

```bash
gh workflow run "Publish Snapshot Artifacts" \
  --repo QuantStrategyLab/UsEquitySnapshotPipelines \
  -f profile=tech_communication_pullback_enhancement \
  -f use_sample_data=true \
  -f gcs_prefix=gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/workflow_smoke/tech_communication_pullback_enhancement \
  -f execute_publish=false
```

## Publishing mode

Leave `execute_publish=false` first. In that mode the workflow only prints the `gcloud storage cp` plan and does not write to GCS.

When the dry-run plan looks correct, rerun with:

- `gcs_prefix` set to the target GCS prefix
- `execute_publish=true`

The initial migration should publish to staging prefixes first, for example:

```text
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/tech_communication_pullback_enhancement_staging
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/russell_1000_multi_factor_defensive_staging
```

Do not overwrite the current HK production prefix until the platform guard has been tested against the staging artifact.

## Current HK path policy

HK currently reads the neutral staging prefix produced by this workflow:

```text
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/tech_communication_pullback_enhancement_staging/tech_communication_pullback_enhancement_feature_snapshot_latest.csv
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/tech_communication_pullback_enhancement_staging/tech_communication_pullback_enhancement_feature_snapshot_latest.csv.manifest.json
```

The older Interactive Brokers scoped path should not be used for new LongBridge
deployments:

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
