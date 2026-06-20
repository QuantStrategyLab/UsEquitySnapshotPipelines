# Live Strategy Research Expansion Plan - 2026-06-20

This is a research-only design note for moving from promising backtests to a
small set of liveable strategy candidates. It does not enable live trading,
change runtime manifests, or approve any new default allocation.

## Current working conclusions

| Line | Current evidence | Research decision |
| --- | --- | --- |
| Russell Top50 leader rotation | Best current offensive line. The fixed `Top4`, `25/75 Top2/Top4`, and `50/50 Top2/Top4` matrix already has promotion, stress, overfit, liquidity, and live-decay hooks. | Keep as the primary live-candidate line. Do not widen the parameter grid. |
| Global ETF offensive sleeve | The small offensive sleeve improved some long-window metrics but failed walk-forward/OOS versus the current defensive baseline. | Keep defensive baseline live. Only test narrow, pre-registered sleeve variants. |
| IBIT Smart DCA | Dynamic MVRV Z-Score exit/parking plugin exists as deterministic signal production for `ibit_smart_dca`; strategy-side DCA/BOXX consumption still needs full backtest/live-design proof. | Treat as a separate asymmetric satellite, not a replacement for the equity offensive line. |
| BOXX / cash-like sleeves | BOXX-like parking can improve idle-cash carry, but it is execution/cash management, not alpha. | Strategy must be able to sell parking sleeves to fund scheduled buys regardless of plugin status. |

## Web-expanded evidence map

The next research should use these ideas only as hypotheses. None of the papers
below is evidence that a repo-specific candidate is live-ready.

- Dual momentum combines relative strength with absolute trend; Antonacci argues
  that absolute momentum mainly reduces volatility and drawdown, while the
  combination tends to work better than either alone. Source:
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2042750
- VAA/DAA/PAA-style canary and breadth momentum papers support proportional
  crash protection rather than arbitrary all-in/all-out cash switches. Sources:
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3002624,
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3212862,
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2759734
- Time-series momentum has broad cross-asset evidence, but it should be treated
  as a risk-control layer with turnover/cost gates in this repo. Source:
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2089463
- Industry momentum and residual momentum suggest that a pure price-momentum
  stock ranking can be decomposed into industry/factor and idiosyncratic pieces.
  Sources: https://www.aqr.com/Insights/Research/Journal-Article/Do-Industries-Explain-Momentum
  and https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2319861
- Momentum crash research supports guarding only the aggressive momentum sleeve
  in panic/rebound regimes, not broadly de-risking every strategy. Sources:
  https://www.nber.org/papers/w20439 and
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2041429
- White Reality Check, Hansen SPA, and Model Confidence Set literature supports
  testing the selected winner against the full pre-registered candidate set, not
  only reporting the best backtest. Sources:
  https://www.ssc.wisc.edu/~bhansen/718/White2000.pdf,
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=264569,
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=522382
- Crypto momentum evidence is mixed and horizon-sensitive; IBIT exit/DCA work
  should therefore be judged against a buy-only DCA baseline and a BOXX-parking
  baseline, not against isolated signal accuracy. Sources:
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3913263 and
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=5209907
- BOXX is an actively managed ETF using box spreads and may also hold cash,
  cash equivalents, money market funds, or treasury bills. It is a cash-like
  implementation choice with fund-specific risks and turnover/tax behavior, so
  backtests should keep it as a configurable parking asset. Sources:
  https://funds.alphaarchitect.com/boxetf/ and
  https://etfarchitect.com/wp-content/uploads/compliance/etf/summary_prospectus/BOXX_Summary_Prospectus.pdf

## Pre-registered research candidates

### Candidate A - Russell residual/industry-aware diagnostic, not replacement

Goal: determine whether the current Russell winner is mostly sector/industry
crowding or idiosyncratic stock leadership.

Rules to test:

1. Keep the approved Top4 / 25-75 / 50-50 matrix unchanged as the benchmark.
2. Add diagnostic rankings only:
   - `industry_neutral_score`: rank within available sector/industry buckets,
     then select from top residual ranks;
   - `beta_residual_score`: rank trailing 6/12-month return after subtracting a
     simple QQQ/SPY beta component;
   - `sector_cap_shadow`: keep original ranking but cap incremental Top2 sleeve
     contribution from a single sector.
3. Do not promote any diagnostic variant unless it beats the current `50/50`
   candidate after cost, source-lag, overfit, and liquidity gates.

Why low risk: it starts as a diagnostic explaining the existing winner. It does
not replace the current live candidate or add runtime behavior.

### Candidate B - Russell sleeve-level volatility/crash brake

Goal: reduce momentum-crash exposure without turning the whole portfolio into a
cash-timing system.

Rules to test:

1. Apply only to the Top2 sleeve inside the fixed 25-75 or 50-50 blend.
2. Monthly evaluation only.
3. Brake triggers only when all are true:
   - trailing market drawdown is material;
   - realized volatility is elevated versus its rolling history;
   - recent rebound risk is present, such as strong short-horizon QQQ/SPY bounce
     after a drawdown.
4. Brake action can only move the affected Top2 sleeve into Top4 or BOXX/SGOV;
   it cannot change the Top4 fallback or Global ETF/IBIT sleeves.

Promotion gate: must improve worst 3Y/5Y rolling drawdown or left-tail return
without reducing long-window excess versus QQQ below the current fixed blend.

### Candidate C - Global ETF offensive sleeve as risk budget, not replacement

Goal: test whether a small offensive ETF sleeve can add return without hurting
OOS stability.

Allowed variants only:

| Variant | Rule |
| --- | --- |
| `baseline90_fast10_static` | 90% defensive baseline + 10% existing fast sleeve |
| `baseline85_fast15_static` | 85% defensive baseline + 15% existing fast sleeve |
| `baseline90_fast10_trend_gate` | 10% fast sleeve only when QQQ absolute momentum is positive |
| `baseline90_fast10_canary_fraction` | 10% fast sleeve scaled by canary breadth, not binary off |

Hard stop: reject if worst walk-forward excess versus current defensive baseline
is below `-3%` CAGR or if 2015-2019 remains materially below both SPY and QQQ.

### Candidate D - IBIT Smart DCA with parking asset and deterministic exit signal

Goal: decide whether IBIT should be a live satellite with buy-only DCA plus an
optional deterministic z-score escape/parking overlay.

Strategy-side behavior to backtest:

1. Base DCA engine:
   - scheduled contributions buy IBIT;
   - idle cash is represented by configurable parking asset, default `BOXX`;
   - when a DCA buy occurs and cash is parked, sell parking asset first, then buy
     IBIT;
   - this behavior exists whether or not the z-score plugin is enabled.
2. Plugin-off baseline:
   - no sell signal;
   - scheduled buys only;
   - parking asset only funds future scheduled buys.
3. Plugin-on deterministic consumption:
   - consume `ibit_zscore_exit.v1` target allocations;
   - soft route reduces IBIT to configured partial exposure;
   - hard route reduces to configured defensive exposure;
   - return to normal only by explicit rule, such as falling below the rolling
     soft threshold minus hysteresis or after a fixed re-entry schedule.
4. Data design:
   - use spot BTC history before IBIT inception as the research proxy;
   - use actual IBIT after inception;
   - keep MVRV/Z-score source lineage, fallback freshness, and missing-data
     behavior in the manifest.

Promotion gate:

- plugin-on must beat buy-only DCA on net CAGR or reduce max drawdown by enough
  to justify any CAGR give-up;
- plugin-on must beat BOXX-only parking and scheduled buy baseline after costs;
- no default live enablement unless the rule is deterministic, replayable, and
  has no stale-data dependency at rebalance time.

### Candidate E - Multi-sleeve portfolio review, after sleeves pass individually

Goal: avoid over-optimizing weights while still asking whether approved sleeves
combine better than one line alone.

Pre-registered allocations only:

| Portfolio | Russell | Global ETF defensive | IBIT DCA satellite | Parking |
| --- | ---: | ---: | ---: | ---: |
| `offensive_core_80_10_10` | 80% | 10% | 10% | residual |
| `offensive_core_70_20_10` | 70% | 20% | 10% | residual |
| `offensive_core_60_25_15` | 60% | 25% | 15% | residual |

Rules:

- A sleeve can enter this portfolio review only after its own promotion artifact
  passes.
- Weights are fixed from this table; no optimizer is allowed.
- Portfolio promotion requires passing QQQ/SPY relative gates, drawdown gates,
  live-decay monitor, cost stress, and sleeve-level attribution.

## Recommended implementation order

1. **Document and gate only**: keep this file as the pre-registration source for
   new candidates. Any future code runner should write the candidate ID exactly
   as named here.
2. **IBIT DCA backtest runner**: implement strategy-side parking/DCA/plugin
   consumption before changing runtime. This directly addresses whether BOXX can
   fund future IBIT buys and whether z-score escape adds net value.
3. **Russell diagnostic runner**: add sector/residual diagnostics as shadow
   artifacts only. If they fail, no runtime impact.
4. **Global ETF narrow sleeve rerun**: run only the four variants listed above;
   do not add new ETF universes or scoring formulas in the same pass.
5. **Multi-sleeve review bundle**: only after at least Russell plus one satellite
   have independently passed.

## Architecture boundary

- `UsEquitySnapshotPipelines` should own all research runners, promotion
  manifests, live-decay monitors, and evidence bundles.
- `UsEquityStrategies` should only consume approved deterministic runtime config
  after a promotion artifact exists.
- Plugins should only produce bounded signals for compatible strategies. They
  should not select strategy variants, allocate capital across unrelated
  strategies, or trigger runtime promotion.
- Cash/BOXX handling belongs to strategy portfolio construction and execution
  simulation, not to the plugin signal itself.

## Next concrete code candidate

The lowest-risk next implementation is now available as
`useq-research-ibit-smart-dca`. It resolves a live-design ambiguity already
raised in this thread:

- `parking_only`: BOXX/parking baseline for cash-like opportunity cost;
- plugin disabled: scheduled buy-only DCA, with BOXX/parking sold to fund buys;
- plugin enabled: same DCA engine plus deterministic z-score target allocation;
- optional `--btc-proxy-symbol` backfills pre-IBIT history by scaling the BTC
  proxy to the first real IBIT close, and records proxy lineage in the manifest;
- performance metrics are cash-flow adjusted through a unitized return series,
  so scheduled contributions do not inflate CAGR; ending NAV is reported
  separately;
- outputs: `ibit_dca_period_summary.csv`, `ibit_dca_trade_ledger.csv`,
  `ibit_dca_holdings_ledger.csv`, `ibit_dca_signal_consumption.csv`,
  `ibit_dca_live_readiness_summary.csv`, and
  `ibit_dca_research_manifest.json`;
- gate: plugin-on must show positive net value versus plugin-off buy-only DCA,
  must not lose to the parking-only baseline, and should be reviewed against
  QQQ/SPY excess-CAGR columns before any default enablement.

Command pattern with a prepared price CSV:

```bash
uv run useq-research-ibit-smart-dca \
  --prices data/output/ibit_smart_dca_inputs/prices.csv \
  --zscore-metrics data/output/ibit_zscore_metrics/ibit_zscore_metrics.csv \
  --initial-parking-value 10000 \
  --contribution-amount 500 \
  --parking-symbol BOXX \
  --btc-proxy-symbol BTC \
  --output-dir data/output/ibit_smart_dca_research_YYYYMMDD
```

Command pattern with direct yfinance price download:

```bash
uv run useq-research-ibit-smart-dca \
  --download \
  --price-start 2014-01-01 \
  --zscore-metrics data/output/ibit_zscore_metrics/ibit_zscore_metrics.csv \
  --initial-parking-value 10000 \
  --contribution-amount 500 \
  --parking-symbol BOXX \
  --btc-proxy-symbol BTC \
  --output-dir data/output/ibit_smart_dca_research_YYYYMMDD
```

With `--download`, the runner downloads `IBIT`, the parking symbol, QQQ/SPY,
and the BTC proxy through the shared yfinance helper. The default BTC proxy
source is requested as `BTC-USD` and normalized back to symbol `BTC` in research
artifacts.

Monthly review integration:

- `scripts/build_scheduled_ibit_dca_research.py` builds a scheduled,
  research-only artifact from the same normalized z-score metrics used by the
  `ibit_zscore_exit` plugin publish job and a fresh yfinance price download.
  The publish workflow uploads this research artifact for review, but it does
  not publish the research output to the runtime plugin GCS prefix.
- `scripts/run_monthly_report_bundle.py` auto-discovers
  `ibit_dca_research_manifest.json` under the artifact root, or accepts explicit
  `--ibit-dca-research-manifest` paths.
- The monthly bundle surfaces IBIT DCA variants, parking symbol, BTC proxy
  lineage, row counts, and evidence status in both `job_summary.md` and
  `ai_review_input.md`.
- These artifacts are research-only. They must not enable IBIT runtime changes
  unless a separate promotion artifact and human approval exist.

## 2026-06-20 public-data smoke result

Using the free `api.bitcoin-data.com/v1/mvrv-zscore` endpoint available in this
environment and yfinance prices, the scheduled builder produced a valid artifact
covering the BOXX-available window from 2022-12-28 through 2026-06-20. This is a
smoke result, not a promotion artifact.

Key readout:

- `parking_only`: CAGR about 4.69%, max drawdown about -0.12%.
- `buy_only_dca`: CAGR about 46.01%, max drawdown about -52.11%.
- `plugin_on`: identical to `buy_only_dca` in this run.
- Plugin route counts: `normal=42`; no `risk_reduced` or `risk_off` route fired.
- Gate: `fail`, because an overlay that never changes allocation adds no net
  value versus buy-only DCA, even if buy-only DCA itself beats parking-only and
  QQQ/SPY over this short window.

Implication: keep `ibit_zscore_exit` as research/shadow evidence. Do not default
enable the escape overlay until a longer or more stressful replay shows positive
net value versus buy-only DCA or sufficient drawdown improvement after costs.
The plugin registration is therefore kept `notification_only` with position
control disabled; it may emit deterministic evidence, but it must not be
consumed for automated IBIT allocation changes without a separate passing
promotion artifact.
