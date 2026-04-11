# UsEquitySnapshotPipelines

`UsEquitySnapshotPipelines` is the upstream feature-snapshot and release pipeline repo for US equity strategies.
It is intentionally separate from broker execution repos.

## Boundary

This repo owns:

- universe and price-input preparation for snapshot-backed US equity strategies
- feature snapshot generation
- candidate ranking / target preview artifacts
- snapshot manifest and release status summary generation
- optional GCS publishing helpers

This repo does not own:

- broker API access
- account / position reconciliation
- order placement
- Telegram runtime notifications
- Cloud Run service configuration

Downstream platforms (`InteractiveBrokersPlatform`, `LongBridgePlatform`, `CharlesSchwabPlatform`) should keep consuming only the published artifact contract.

## Current migrated profile

| Profile | Status | Notes |
| --- | --- | --- |
| `tech_communication_pullback_enhancement` | migrated upstream pipeline | snapshot builder, ranking, release summary, publish flow live here |
| `russell_1000_multi_factor_defensive` | migrated upstream pipeline | source-input refresh, snapshot builder, backtest CLI, ranking, release summary, publish flow live here |

## Local smoke command

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src python -m pytest -q
```

Build a tech/communication pullback snapshot:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_tech_communication_pullback_snapshot.py \
  --prices /path/to/price_history.csv \
  --universe /path/to/universe.csv \
  --as-of 2026-04-01 \
  --output-dir data/output/tech_communication_pullback_enhancement
```

The command writes:

- `tech_communication_pullback_enhancement_feature_snapshot_latest.csv`
- `tech_communication_pullback_enhancement_feature_snapshot_latest.csv.manifest.json`
- `tech_communication_pullback_enhancement_ranking_latest.csv`
- `release_status_summary.json`

See `docs/operator_runbook.md` for the manual GitHub Actions publish flow.
The scheduled publish workflow now builds both migrated snapshot profiles from the shared Russell 1000 input refresh.

Prepare / refresh shared Russell 1000 source inputs:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/update_russell_1000_input_data.py \
  --output-dir data/input/refreshed/r1000_official_monthly_v2_alias \
  --universe-start 2018-01-01 \
  --price-start 2018-01-01 \
  --extra-symbols QQQ,SPY,BOXX
```

The source-input refresh writes:

- `r1000_price_history.csv`
- `r1000_universe_history.csv`
- `r1000_symbol_aliases.csv`
- `r1000_universe_snapshot_metadata.csv`

Build a Russell 1000 snapshot:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_russell_1000_feature_snapshot.py \
  --prices /path/to/r1000_price_history.csv \
  --universe /path/to/r1000_universe_history.csv \
  --as-of 2026-04-01 \
  --output-dir data/output/russell_1000_multi_factor_defensive
```

Backtest Russell 1000 from the same input files:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_russell_1000_multi_factor_defensive.py \
  --prices /path/to/r1000_price_history.csv \
  --universe /path/to/r1000_universe_history.csv \
  --start 2019-01-01 \
  --output-dir data/output/russell_1000_multi_factor_defensive_backtest
```
