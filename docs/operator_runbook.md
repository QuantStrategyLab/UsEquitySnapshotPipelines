# Operator Runbook

This repo is the upstream artifact producer for snapshot-backed US equity strategies. Broker platform repos remain downstream consumers.

## Snapshot Profiles

These are artifact profiles produced here for downstream runtimes:

- `tech_communication_pullback_enhancement`
- `russell_1000_multi_factor_defensive`
- `mega_cap_leader_rotation_top50_balanced`

`mega_cap_leader_rotation_dynamic_top20`, `mega_cap_leader_rotation_aggressive`, and `dynamic_mega_leveraged_pullback` are no longer publishable snapshot profiles. Top50 balanced is the retained mega-cap runtime path.

## Manual Local Build

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

Mega-cap Top50 balanced:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_mega_cap_leader_rotation_top50_balanced_snapshot.py \
  --prices /path/to/r1000_price_history.csv \
  --universe /path/to/r1000_latest_holdings_snapshot.csv \
  --as-of 2026-04-01 \
  --dynamic-universe-size 50 \
  --output-dir data/output/mega_cap_leader_rotation_top50_balanced
```

## Manual GitHub Actions Build

Use the `Publish Snapshot Artifacts` workflow.

Required input:

- `profile`

For production data, set both:

- `prices_path`
- `universe_path`

Optional inputs:

- `as_of_date`
- `artifact_dir`
- `gcs_prefix`
- `config_path` for `tech_communication_pullback_enhancement`
- `current_holdings`
- `portfolio_total_equity`
- `min_adv20_usd` for Russell / mega-cap testing overrides

The workflow always uploads generated files as a GitHub Actions artifact.

## Scheduled Publish

`Update Source Input Data` runs once per month at `00:15 UTC` on the 1st day of the month. It refreshes the shared Russell 1000 inputs used by monthly snapshot profiles:

```text
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_price_history.csv
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_universe_history.csv
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_symbol_aliases.csv
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_universe_snapshot_metadata.csv
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_latest_holdings_snapshot.csv
```

`Publish Snapshot Artifacts` runs once per month at `00:45 UTC` and builds:

```text
profiles=tech_communication_pullback_enhancement,russell_1000_multi_factor_defensive,mega_cap_leader_rotation_top50_balanced
prices_path=gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_price_history.csv
tech_and_russell_universe_path=gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_universe_history.csv
mega_top50_balanced_universe_path=gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_latest_holdings_snapshot.csv
execute_publish=true
```

Default scheduled output prefixes:

| Profile | Extra config | GCS prefix |
| --- | --- | --- |
| `tech_communication_pullback_enhancement` | `gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/inputs/tech_communication_pullback_enhancement/growth_pullback_tech_communication_pullback_enhancement.json` | `gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/tech_communication_pullback_enhancement_staging` |
| `russell_1000_multi_factor_defensive` | none | `gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/russell_1000_multi_factor_defensive_staging` |
| `mega_cap_leader_rotation_top50_balanced` | none | `gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/mega_cap_leader_rotation_top50_balanced_staging` |

The publish workflow keeps a defensive month-end trading-day guard: if the resolved `snapshot_as_of` is not the last NYSE trading day of that snapshot month, it writes a skip artifact and does not publish to GCS.

## Troubleshooting

- If source-input refresh fails, snapshot publish may still run against the last successful source inputs.
- If a snapshot-backed profile fails to load, check the artifact manifest, schema, and `as_of` freshness window first.
- If scheduled publish skips, inspect the skip artifact for the resolved trading day and month-end guard result.
