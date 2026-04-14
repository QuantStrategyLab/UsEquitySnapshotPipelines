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

| Profile | Status | Scheduled artifact cadence | Notes |
| --- | --- | --- | --- |
| `tech_communication_pullback_enhancement` | migrated upstream pipeline | monthly | snapshot builder, ranking, release summary, publish flow live here |
| `russell_1000_multi_factor_defensive` | migrated upstream pipeline | monthly | source-input refresh, snapshot builder, backtest CLI, ranking, release summary, publish flow live here |
| `mega_cap_leader_rotation_dynamic_top20` | migrated upstream pipeline | monthly scheduled + manual publish | snapshot builder, ranking, release summary, and publish flow live here; scheduled publish uses the latest weighted Russell 1000 holdings snapshot to derive top20 |

This table describes artifact publishing cadence only. Strategy-level cadence remains documented in `UsEquityStrategies`; broker execution schedules should follow that strategy-layer source.

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
The scheduled workflows run monthly: first they refresh the shared Russell 1000 input data, including the latest weighted holdings snapshot used by mega-cap dynamic top20, then they build and publish the scheduled snapshot profiles from those refreshed inputs.

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


Build a mega-cap dynamic top20 snapshot from a previously prepared dynamic universe history:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_mega_cap_leader_rotation_dynamic_top20_snapshot.py \
  --prices /path/to/mega_cap_leader_rotation_dynamic_top20_price_history.csv \
  --universe /path/to/mega_cap_leader_rotation_dynamic_top20_universe_history.csv \
  --as-of 2026-04-01 \
  --output-dir data/output/mega_cap_leader_rotation_dynamic_top20
```

The dynamic top20 builder intentionally requires `mega_rank`, `source_weight`,
`weight`, `source_market_value`, or `market_value` when the active universe has
more than 20 names. The monthly scheduled path uses
`r1000_latest_holdings_snapshot.csv`, which preserves the iShares `weight` and
`market_value` fields, to avoid accidentally publishing a broad Russell 1000
snapshot under the concentrated mega-cap profile.

Backtest Russell 1000 from the same input files:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_russell_1000_multi_factor_defensive.py \
  --prices /path/to/r1000_price_history.csv \
  --universe /path/to/r1000_universe_history.csv \
  --start 2019-01-01 \
  --output-dir data/output/russell_1000_multi_factor_defensive_backtest
```

## Research-only backtests

`mega_cap_leader_rotation_dynamic_top20` is now a selectable snapshot-backed profile documented in
`../UsEquityStrategies/docs/research/mega_cap_leader_rotation.md`. Static `mega_cap_leader_rotation` pools remain research-only.

Run the first-pass mega-cap leader rotation backtest with local input files:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_mega_cap_leader_rotation.py \
  --prices /path/to/mega_cap_price_history.csv \
  --universe /path/to/mega_cap_universe.csv \
  --pool expanded \
  --start 2016-01-01 \
  --turnover-cost-bps 5 \
  --output-dir data/output/mega_cap_leader_rotation_backtest
```

Or let the research CLI download the static pool through yfinance:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_mega_cap_leader_rotation.py \
  --download \
  --pool expanded \
  --price-start 2015-01-01 \
  --start 2016-01-01 \
  --turnover-cost-bps 5 \
  --output-dir data/output/mega_cap_leader_rotation_backtest
```

The command writes `summary.csv`, `portfolio_returns.csv`,
`weights_history.csv`, `turnover_history.csv`, `candidate_scores.csv`,
`trades.csv`, `exposure_history.csv`, and `reference_returns.csv`.

To reduce today's-winners look-ahead bias, run the historical dynamic mega-cap
variant. It downloads monthly iShares Russell 1000 holdings snapshots and uses
the top fund-weight names available at each rebalance. The documented start
uses the earliest monthly iShares JSON snapshot range that resolved reliably in
research. Known duplicate share classes such as `GOOG` / `GOOGL` are collapsed
to one issuer before taking the top-N universe:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_mega_cap_leader_rotation.py \
  --download \
  --dynamic-universe \
  --universe-start 2017-09-01 \
  --price-start 2015-01-01 \
  --start 2017-10-01 \
  --mega-universe-size 20 \
  --top-n 4 \
  --single-name-cap 0.25 \
  --turnover-cost-bps 5 \
  --output-dir data/output/mega_cap_leader_rotation_dynamic_universe_top20_backtest
```

For small paper/live accounts, add `--portfolio-total-equity` and
`--min-position-value-usd` to let the research backtest reduce the effective
top-N when the configured account size cannot support the requested number of
minimum-sized stock positions. The per-rebalance effective count is written to
`exposure_history.csv`.

Run the default robustness matrix across `mag7` / `expanded`, top 3 / 4 / 5,
single-name caps 25% / 30% / 35%, and defense on / off:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_mega_cap_leader_rotation_robustness.py \
  --prices data/output/mega_cap_leader_rotation_backtest/input/mega_cap_leader_rotation_expanded_price_history.csv \
  --output-dir data/output/mega_cap_leader_rotation_robustness
```

Or download the union research pool and run the matrix in one step:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_mega_cap_leader_rotation_robustness.py \
  --download \
  --price-start 2015-01-01 \
  --start 2016-01-01 \
  --turnover-cost-bps 5 \
  --output-dir data/output/mega_cap_leader_rotation_robustness
```

The robustness command writes `robustness_summary.csv` sorted by Sharpe, CAGR,
drawdown, and turnover, plus `robustness_summary_by_run.csv` in raw run order.
