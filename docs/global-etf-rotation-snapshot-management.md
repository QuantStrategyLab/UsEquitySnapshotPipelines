# Global ETF Rotation Snapshot-Side Management

Global ETF Rotation now consumes a runtime feature snapshot. Ranking-pool governance, candidate audit, and future universe changes belong in `UsEquitySnapshotPipelines` so they can be reviewed as transparent snapshot-side evidence before runtime/broker changes.

## Boundary

- Runtime signal logic remains in `UsEquityStrategies`, but runtime input is `feature_snapshot`.
- Universe membership and candidate additions are audited in `UsEquitySnapshotPipelines`.
- AI may propose rule changes, but only deterministic gates and scores decide whether a candidate enters the research ranking.
- Downstream runtime/broker repositories consume the promoted snapshot artifact and should not receive new pool members until the audit artifacts and backtest evidence are reviewed.

## CLI

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
  .venv/bin/python -m us_equity_snapshot_pipelines.global_etf_rotation_snapshot \
  --price-start 2023-01-01 \
  --output-dir data/output/global_etf_rotation_universe_audit_YYYYMMDD
```

Outputs:

- `global_etf_rotation_feature_snapshot_latest.csv`
- `global_etf_rotation_feature_snapshot_latest.csv.manifest.json`
- `global_etf_rotation_ranking_latest.csv`
- `release_status_summary.json`
- `downloaded_price_history.csv`
- `candidate_snapshot.csv`
- `gate_results.csv`
- `ranking.csv`
- `promotion_decision.json`
- `run_manifest.json`
- `audit_report.md`

## Current state

`global_etf_rotation` is snapshot-backed at runtime. Runtime/broker repositories must provide the feature snapshot path and manifest path, using contract version `global_etf_rotation.feature_snapshot.v1`.
