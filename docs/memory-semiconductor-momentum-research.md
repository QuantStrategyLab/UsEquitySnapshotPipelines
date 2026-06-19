# Memory Semiconductor Momentum Research

This is a research-only universe-audit track for memory semiconductor exposure. It is intentionally separate from live strategy manifests and uses the same transparent gate pattern that snapshot-side research pipelines use.

## Architecture boundary

AI can propose or improve the selection mechanism, but it cannot directly select live holdings. A proposal must be converted into a structured `SelectionRuleSpec`, then the deterministic universe-audit engine applies hard gates, scoring, ranking, and promotion decisions.

The runtime and broker repositories should only consume promoted artifacts after short/medium/long validation and explicit review. This module currently writes research artifacts only.

## Rationale

Roundhill Memory ETF (`DRAM`) gives targeted exposure to memory semiconductor companies, but it launched in April 2026. That is too little history for live ETF-rotation rules, which rely on longer momentum and trend filters.

## Guardrails

A memory ETF or semiconductor proxy can enter the research ranking only when it has both:

- at least 252 trading days, and
- at least 13 month-end closes.

Until then, `DRAM` is observation-only. Memory stocks such as `MU`, `WDC`, `STX`, and `SNDK` are tracker-only by default so the research does not become a post-hoc selection of recent winners.

## Current default universe

- Memory ETF observation: `DRAM`
- Seasoned semiconductor ETF proxies: `SMH`, `SOXX`, `XSD`, `PSI`
- Memory stock trackers: `MU`, `WDC`, `STX`, `SNDK`
- Benchmarks: `SPY`, `QQQ`

## CLI

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
  .venv/bin/python -m us_equity_snapshot_pipelines.memory_semiconductor_momentum_research \
  --price-start 2024-01-01 \
  --price-end 2026-06-20 \
  --output-dir data/output/memory_semiconductor_momentum_research_YYYYMMDD
```

Outputs:

- `downloaded_price_history.csv`
- `candidate_snapshot.csv`
- `gate_results.csv`
- `ranking.csv`
- `promotion_decision.json`
- `run_manifest.json`
- `audit_report.md`

## Rollout rule

This module is only a research gate. Do not wire it into `UsEquityStrategies` live profiles until the seasoning guardrail and short/medium/long validation pass.
