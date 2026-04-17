# Operator runbook

This repo is the upstream artifact producer for snapshot-backed US equity strategies. Broker platform repos remain downstream consumers.

## Current profiles

- `tech_communication_pullback_enhancement`
- `russell_1000_multi_factor_defensive`
- `mega_cap_leader_rotation_dynamic_top20`
- `dynamic_mega_leveraged_pullback`

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


Mega-cap dynamic top20:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_mega_cap_leader_rotation_dynamic_top20_snapshot.py \
  --prices /path/to/mega_cap_leader_rotation_dynamic_top20_price_history.csv \
  --universe /path/to/mega_cap_leader_rotation_dynamic_top20_universe_history.csv \
  --as-of 2026-04-01 \
  --output-dir data/output/mega_cap_leader_rotation_dynamic_top20
```

The universe input must already be the dynamic top20 history, or a ranked
Russell universe containing `mega_rank`, `source_weight`, `weight`,
`source_market_value`, or `market_value`. The scheduled GitHub Actions path uses
`r1000_latest_holdings_snapshot.csv` from the monthly source-input refresh.

Mega-cap aggressive:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_mega_cap_leader_rotation_aggressive_snapshot.py \
  --prices /path/to/r1000_price_history.csv \
  --universe /path/to/r1000_latest_holdings_snapshot.csv \
  --as-of 2026-04-01 \
  --dynamic-universe-size 50 \
  --output-dir data/output/mega_cap_leader_rotation_aggressive
```

This publishes the separate `mega_cap_leader_rotation_aggressive` artifact
contract for the top-3/no-defense runtime profile.

Dynamic mega leveraged pullback:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_dynamic_mega_leveraged_pullback_snapshot.py \
  --prices /path/to/r1000_price_history.csv \
  --universe /path/to/r1000_latest_holdings_snapshot.csv \
  --product-map /path/to/dynamic_mega_2x_product_map.csv \
  --as-of 2026-04-01 \
  --output-dir data/output/dynamic_mega_leveraged_pullback
```

The product map is required for this profile. The builder does not fall back to
buying the underlying stock when a 2x product mapping is missing; unmapped rows
are marked unavailable, and available rows must point to approximately 2x long
products.

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
- `product_map_path` for `dynamic_mega_leveraged_pullback`
- `current_holdings` for hold-bonus preview
- `portfolio_total_equity` for dynamic position-count preview
- `min_adv20_usd` for Russell / mega-cap testing overrides

Scheduled monthly publish includes `tech_communication_pullback_enhancement`,
`russell_1000_multi_factor_defensive`, and
`mega_cap_leader_rotation_dynamic_top20`,
`mega_cap_leader_rotation_top50_balanced`, and
`dynamic_mega_leveraged_pullback`. The mega-cap scheduled paths consume
`r1000_price_history.csv` plus `r1000_latest_holdings_snapshot.csv` from the
source-input refresh workflow. `dynamic_mega_leveraged_pullback` also requires
`SCHEDULED_DYNAMIC_MEGA_LEVERAGED_PULLBACK_PRODUCT_MAP_PATH`.

The workflow always uploads the generated files as a GitHub Actions artifact.

## Scheduled publish

`Update Source Input Data` runs automatically once per month at `00:15 UTC` on
the 1st day of the month (`08:15` the same day in Asia/Shanghai). This is after
the regular US close for the prior US trading date and refreshes the shared
Russell 1000 source inputs used by monthly snapshot profiles:

```text
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_price_history.csv
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_universe_history.csv
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_symbol_aliases.csv
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_universe_snapshot_metadata.csv
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_latest_holdings_snapshot.csv
```

The refresh workflow downloads the existing price history first, refreshes the
latest overlap window, downloads full history for newly discovered symbols, and
then publishes the merged input files. It also forces `QQQ`, `SPY`, and `BOXX`
into the price input so both QQQ-benchmark and SPY-benchmark snapshot profiles
have the reference symbols they need.

`Publish Snapshot Artifacts` then runs automatically once per month at
`00:45 UTC` on the 1st day of the month (`08:45` the same day in
Asia/Shanghai), leaving time for the source-input refresh to finish first.
Scheduled publish builds all scheduled snapshot profiles from the refreshed
source inputs:

```text
profiles=tech_communication_pullback_enhancement,russell_1000_multi_factor_defensive,mega_cap_leader_rotation_dynamic_top20,mega_cap_leader_rotation_top50_balanced,dynamic_mega_leveraged_pullback
prices_path=gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_price_history.csv
tech_and_russell_universe_path=gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_universe_history.csv
mega_dynamic_top20_universe_path=gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_latest_holdings_snapshot.csv
dynamic_mega_leveraged_product_map_path=<operator managed gs://.../dynamic_mega_2x_product_map.csv>
execute_publish=true
```

The scheduled publish path is intentionally monthly. This describes artifact
publishing cadence only; strategy-level cadence remains documented in
`UsEquityStrategies`. The publish workflow also keeps a defensive month-end
trading-day guard: if the resolved `snapshot_as_of` is not the last NYSE trading
day of that snapshot month, the workflow writes a skip artifact and does not
publish to GCS.

Default scheduled output prefixes:

| Profile | Extra config | GCS prefix |
| --- | --- | --- |
| `tech_communication_pullback_enhancement` | `gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/tech_communication_pullback_enhancement/growth_pullback_tech_communication_pullback_enhancement.json` | `gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/tech_communication_pullback_enhancement_staging` |
| `russell_1000_multi_factor_defensive` | none | `gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/russell_1000_multi_factor_defensive_staging` |
| `mega_cap_leader_rotation_dynamic_top20` | none | `gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/mega_cap_leader_rotation_dynamic_top20_staging` |
| `mega_cap_leader_rotation_top50_balanced` | none | `gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/mega_cap_leader_rotation_top50_balanced_staging` |
| `dynamic_mega_leveraged_pullback` | `SCHEDULED_DYNAMIC_MEGA_LEVERAGED_PULLBACK_PRODUCT_MAP_PATH` | `gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/dynamic_mega_leveraged_pullback_staging` |

These defaults can be overridden with repository variables:

- `SCHEDULED_US_EQUITY_PRICES_PATH`
- `SCHEDULED_US_EQUITY_UNIVERSE_PATH`
- `SCHEDULED_TECH_COMMUNICATION_PULLBACK_CONFIG_PATH`
- `SCHEDULED_TECH_COMMUNICATION_PULLBACK_GCS_PREFIX`
- `SCHEDULED_RUSSELL_1000_CONFIG_PATH`
- `SCHEDULED_RUSSELL_1000_GCS_PREFIX`
- `SCHEDULED_MEGA_CAP_DYNAMIC_TOP20_PRICES_PATH`
- `SCHEDULED_MEGA_CAP_DYNAMIC_TOP20_UNIVERSE_PATH`
- `SCHEDULED_MEGA_CAP_DYNAMIC_TOP20_GCS_PREFIX`
- `SCHEDULED_MEGA_CAP_TOP50_BALANCED_PRICES_PATH`
- `SCHEDULED_MEGA_CAP_TOP50_BALANCED_UNIVERSE_PATH`
- `SCHEDULED_MEGA_CAP_TOP50_BALANCED_GCS_PREFIX`
- `SCHEDULED_DYNAMIC_MEGA_LEVERAGED_PULLBACK_PRICES_PATH`
- `SCHEDULED_DYNAMIC_MEGA_LEVERAGED_PULLBACK_UNIVERSE_PATH`
- `SCHEDULED_DYNAMIC_MEGA_LEVERAGED_PULLBACK_PRODUCT_MAP_PATH`
- `SCHEDULED_DYNAMIC_MEGA_LEVERAGED_PULLBACK_GCS_PREFIX`

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

Dynamic mega leveraged production-source dry-run example:

```bash
gh workflow run "Publish Snapshot Artifacts" \
  --repo QuantStrategyLab/UsEquitySnapshotPipelines \
  -f profile=dynamic_mega_leveraged_pullback \
  -f use_sample_data=false \
  -f prices_path=gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_price_history.csv \
  -f universe_path=gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_latest_holdings_snapshot.csv \
  -f product_map_path=gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/dynamic_mega_leveraged_pullback/dynamic_mega_2x_product_map.csv \
  -f gcs_prefix=gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/dynamic_mega_leveraged_pullback_staging \
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
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/dynamic_mega_leveraged_pullback_staging
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
