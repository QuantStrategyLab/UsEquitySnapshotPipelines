# Migration inventory

## Snapshot-backed profiles

| Profile | Current migration state | Current upstream inputs |
| --- | --- | --- |
| `tech_communication_pullback_enhancement` | Archived research-only; no scheduled publish or health workflow exposure | price history, active universe with sector, canonical growth-pullback config |
| `russell_top50_leader_rotation_aggressive` | Runtime snapshot profile; source-input refresh, snapshot builder, ranking, release summary, publish flow live here | Russell 1000 price history, latest Russell Top50 holdings snapshot |

`russell_1000_multi_factor_defensive` has been retired from this repository's runtime contract. It is no longer a snapshot profile, scheduled publish target, health-check target, or source-refresh dispatch target.

## Local owner modules

### `russell_top50_leader_rotation_aggressive`

- `src/us_equity_snapshot_pipelines/russell_1000_history.py`
- `src/us_equity_snapshot_pipelines/yfinance_prices.py`
- `src/us_equity_snapshot_pipelines/mega_cap_leader_rotation_snapshot.py`
- `src/us_equity_snapshot_pipelines/russell_top50_leader_rotation_aggressive.py`
- `scripts/update_russell_1000_input_data.py`
- `scripts/build_russell_top50_leader_rotation_aggressive_snapshot.py`

## Remaining intentional dependency

This repository still imports `us_equity_strategies.strategies.*` for archived research and non-snapshot plugin/archive tooling. Runtime-facing snapshot publish for this repository is limited to `russell_top50_leader_rotation_aggressive`.

## Execution repos remain downstream only

Broker/runtime repositories should consume the published snapshot manifest and snapshot CSV; artifact production remains owned here.
