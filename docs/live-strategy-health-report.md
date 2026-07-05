# Live Strategy Health Report

This report is an evidence layer for deciding whether a live or research strategy should be kept, watched, or reviewed for retirement. It is intentionally separate from live manifests and broker settings. It is background evidence about content or strategy health, not an AiGateway online service health signal and not an automatic trading or auto-approval basis.

The report does **not**:

- place trades;
- mutate live allocations;
- remove a strategy from runtime settings;
- promote a research candidate to live use.

## Inputs

The command expects a CSV with one date column and daily return columns:

- `as_of`: trading date by default;
- one or more strategy return columns;
- one primary benchmark return column, usually `buy_hold_SPY`.

Example:

```bash
PYTHONPATH=src python3 -m us_equity_snapshot_pipelines.live_strategy_health \
  --returns data/output/global_etf_rotation_optimization_research_20260620/portfolio_and_tracker_returns.csv \
  --strategies live_quarterly_conf_vol_gate,monthly_conf_vol_gate \
  --primary-benchmark buy_hold_SPY \
  --output-dir data/output/live_strategy_health_global_etf_YYYYMMDD
```

Installed entry point:

```bash
useq-build-live-strategy-health-report \
  --returns path/to/portfolio_and_tracker_returns.csv \
  --strategies strategy_a,strategy_b \
  --primary-benchmark buy_hold_SPY \
  --output-dir data/output/live_strategy_health_report
```

## Outputs

- `strategy_health_summary.csv`: one row per strategy with overall health state.
- `strategy_health_windows.csv`: per-window metrics and reasons.
- `strategy_health_report.md`: Markdown report for review issues or PRs.
- `run_manifest.json`: command metadata and policy thresholds.

The monthly review workflow runs `scripts/build_monthly_live_strategy_health_reports.py` before assembling the AI review bundle. The helper scans the monthly artifact root for `portfolio_and_tracker_returns.csv`, infers strategy return columns by excluding `as_of` and `buy_hold_*` tracker columns, and writes a health report back into the same artifact root.

`scripts/run_monthly_report_bundle.py` then automatically discovers directories containing `strategy_health_summary.csv` under the monthly artifact root. When present, the monthly AI review issue includes the strategy health summary. If any strategy is marked `review_for_retirement`, the monthly bundle status becomes `warning` so the review cannot silently ignore it.

If a discovered returns matrix cannot be parsed or evaluated, the helper writes
`strategy_health_error.json` and `strategy_health_error.md` under a
`live_strategy_health_error_*` directory and continues processing other return
matrices. The monthly bundle treats these errors as evidence gaps and marks the
review status as `warning`.

## Health states

| State | Meaning |
| --- | --- |
| `keep` | No retirement or watch gate was triggered. |
| `watch` | At least one window needs monitoring; do not change live defaults based on this state alone. |
| `review_for_retirement` | The full window underperformed the primary benchmark without enough drawdown advantage. Requires manual review before removal. |
| `insufficient_data` | There are not enough overlapping observations to judge the window. |

Default policy:

- minimum observations: `60`;
- minimum excess CAGR versus primary benchmark: `0%`;
- required drawdown advantage when underperforming: `3%`;
- watch threshold for drawdown worse than benchmark: `5%`.

This rule is deliberately conservative. A strategy that slightly underperforms SPY but materially reduces drawdown becomes `watch`, not automatic retirement. A strategy that underperforms while taking similar or worse drawdown becomes `review_for_retirement`.

## Operating rule

Use this report before deleting, renaming, or replacing a live profile. It should be combined with the strategy-specific research artifacts, recent production behavior, and platform constraints. Do not optimize parameters solely to clear this report; if a strategy fails, the next step is evidence review, not automatic curve fitting.
