# Migration inventory

## Snapshot-backed profiles

| Profile | Current migration state | Current upstream inputs |
| --- | --- | --- |
| `tech_communication_pullback_enhancement` | Snapshot feature builder, ranking, release summary, publish flow moved here | price history, active universe with sector, canonical growth-pullback config |
| `russell_1000_multi_factor_defensive` | Source-input refresh, snapshot feature builder, ranking, release summary, backtest CLI, publish flow moved here | Russell 1000 universe history, price history |

## Local owner modules

### `tech_communication_pullback_enhancement`

- `src/us_equity_snapshot_pipelines/tech_communication_pullback_snapshot.py`
- `src/us_equity_snapshot_pipelines/tech_communication_pullback.py`
- `scripts/build_tech_communication_pullback_snapshot.py`

### `russell_1000_multi_factor_defensive`

- `src/us_equity_snapshot_pipelines/russell_1000_history.py`
- `src/us_equity_snapshot_pipelines/yfinance_prices.py`
- `src/us_equity_snapshot_pipelines/russell_1000_multi_factor_defensive_snapshot.py`
- `src/us_equity_snapshot_pipelines/russell_1000_multi_factor_defensive.py`
- `src/us_equity_snapshot_pipelines/russell_1000_multi_factor_backtest.py`
- `scripts/update_russell_1000_input_data.py`
- `scripts/build_russell_1000_feature_snapshot.py`
- `scripts/backtest_russell_1000_multi_factor_defensive.py`

## Remaining intentional dependency

This repository still imports `us_equity_strategies.strategies.*` for runtime constants and signal/weight calculation. That keeps the offline snapshot preview aligned with the live strategy engine without moving broker execution code here.

## Execution repos remain downstream only

- `LongBridgePlatform` reads `LONGBRIDGE_FEATURE_SNAPSHOT_PATH` / manifest path.
- `InteractiveBrokersPlatform` reads `IBKR_FEATURE_SNAPSHOT_PATH` / manifest path.
- `CharlesSchwabPlatform` reads `SCHWAB_FEATURE_SNAPSHOT_PATH` / manifest path.

Do not move broker API, order placement, account reconciliation, or Telegram runtime notification logic into this repository.
