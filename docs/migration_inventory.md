# Migration inventory

## Snapshot-backed profiles

| Profile | Current migration state | Current upstream inputs |
| --- | --- | --- |
| `tech_communication_pullback_enhancement` | Initial pipeline moved here | price history, active universe with sector, canonical growth-pullback config |
| `russell_1000_multi_factor_defensive` | Initial snapshot artifact pipeline moved here; data-prep/backtest wrappers still pending | Russell 1000 universe history, price history |

## Existing source files to migrate in phases

### `tech_communication_pullback_enhancement`

- `UsEquityStrategies/scripts/generate_qqq_tech_enhancement_feature_snapshot.py`
- `UsEquityStrategies/src/us_equity_strategies/snapshots/qqq_tech_enhancement.py`
- `InteractiveBrokersPlatform/research/configs/growth_pullback_tech_communication_pullback_enhancement.json`
- `LongBridgePlatform/research/configs/growth_pullback_tech_communication_pullback_enhancement.json`
- `CharlesSchwabPlatform/research/configs/growth_pullback_tech_communication_pullback_enhancement.json`

### `russell_1000_multi_factor_defensive`

- `UsEquityStrategies/scripts/build_russell_1000_universe_history.py`
- `UsEquityStrategies/scripts/fetch_russell_1000_price_history.py`
- `UsEquityStrategies/scripts/generate_russell_1000_feature_snapshot.py`
- `UsEquityStrategies/scripts/run_russell_1000_data_prep_task.py`
- `UsEquityStrategies/scripts/run_russell_1000_snapshot_task.py`
- `UsEquityStrategies/src/us_equity_strategies/data_prep/russell_1000_history.py`
- `UsEquityStrategies/src/us_equity_strategies/snapshots/russell_1000_multi_factor_defensive.py`
- `UsEquityStrategies/src/us_equity_strategies/backtests/russell_1000_multi_factor_defensive.py`

## Execution repos remain downstream only

- `LongBridgePlatform` reads `LONGBRIDGE_FEATURE_SNAPSHOT_PATH` / manifest path.
- `InteractiveBrokersPlatform` reads `IBKR_FEATURE_SNAPSHOT_PATH` / manifest path.
- `CharlesSchwabPlatform` reads `SCHWAB_FEATURE_SNAPSHOT_PATH` / manifest path.

Do not move broker API, order placement, account reconciliation, or Telegram runtime notification logic into this repository.
