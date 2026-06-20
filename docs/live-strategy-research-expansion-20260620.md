# Live Strategy Research Expansion Plan - 2026-06-20

This is a research-only design note for moving from promising backtests to a
small set of liveable strategy candidates. It does not enable live trading,
change runtime manifests, or approve any new default allocation.

## Current working conclusions

| Line | Current evidence | Research decision |
| --- | --- | --- |
| Russell Top50 leader rotation | Best current offensive line. The fixed `Top4`, `25/75 Top2/Top4`, and `50/50 Top2/Top4` matrix already has promotion, stress, overfit, liquidity, and live-decay hooks. | Keep as the primary live-candidate line. Do not widen the parameter grid. |
| Global ETF offensive sleeve | The small offensive sleeve improved some long-window metrics but failed walk-forward/OOS versus the current defensive baseline. | Keep defensive baseline live. Only test narrow, pre-registered sleeve variants. |
| IBIT Smart DCA | Dynamic MVRV Z-Score exit/parking plugin and strategy-side DCA/BOXX replay now exist as research artifacts; the current z-score overlay failed long-history promotion gates. | Treat buy-only DCA as a separate asymmetric satellite idea, but keep the z-score overlay research-only/notification-only. |
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
- 10-month/200-day absolute trend is a useful low-parameter crash-control
  hypothesis, but it should be tested as a deterministic risk gate rather than
  tuned as an optimizer. Faber's tactical asset allocation paper reports
  equity-like returns with materially lower volatility/drawdowns for simple
  moving-average timing across asset classes. Source:
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=962461
- Volatility-managed portfolios motivate scaling risky sleeves down when
  realized volatility is high. Because later literature debates real-time
  robustness, this repo should test volatility as a bounded sleeve budget, not
  as a standalone alpha claim. Source:
  https://www.nber.org/papers/w22208
- Nasdaq and S&P overlays must respect what the wrappers actually represent:
  QQQ seeks to track the Nasdaq-100 before fees/expenses and is not a defensive
  manager, while SPY seeks to track the S&P 500 broad large-cap benchmark.
  Sources:
  https://fundcompli.rightprospectus.com/documents/Invesco/P-QQQ-PRO-1.pdf
  and https://www.ssga.com/us/en/intermediary/etfs/state-street-spdr-sp-500-etf-trust-spy
- IBIT is a bitcoin exposure wrapper with fund-specific fee, custody, and trust
  structure constraints. Backtests should treat pre-IBIT spot BTC proxy history
  as research-only lineage, not as proof that the ETF would have traded the same
  way before inception. Source:
  https://www.ishares.com/us/products/333011/ishares-bitcoin-trust-etf
- Yahoo adjusted close is adjusted for splits and dividend distributions, which
  is the right default for ETF total-return-style research, while raw close
  remains useful for data-quality sensitivity checks. Source:
  https://help.yahoo.com/kb/SLN28256.html
- Crypto DCA can reduce entry-timing dependence but does not protect against
  losses in falling markets. IBIT DCA promotion must therefore be based on
  full-path drawdown, contribution-adjusted returns, and continued-buy
  feasibility, not only ending NAV. Source:
  https://www.fidelity.com/learning-center/trading-investing/crypto/dollar-cost-averaging
- Buy-the-dip evidence is not enough for live promotion. AQR's "Hold the Dip"
  framing is a useful warning that dip-buying often lags buy-and-hold because
  buy-and-hold is always earning the risk premium. Source:
  https://www.aqr.com/-/media/AQR/Documents/Alternative-Thinking/AQR-Alternative-Thinking---Hold-the-Dip.pdf

## Expanded liveability design - IBIT and Nasdaq/S&P overlays

This section is a pre-registration layer for the next research pass. It expands
the idea space while keeping the live path narrow: plugins may emit bounded,
deterministic signals, but strategy code owns allocation, funding, and
promotion decisions.

### IBIT liveable path

The current long-history smoke says buy-only DCA is the only plausible live
mechanic so far; the z-score escape overlay remains research-only until it
adds value versus buy-only DCA.

Candidate variants to test next:

| Candidate | Rule | Liveability purpose | Promotion blocker |
| --- | --- | --- | --- |
| `ibit_buy_only_dca_live_candidate` | Scheduled IBIT buys funded by cash/parking; no z-score selling. | Establish the minimum deterministic satellite. | Fails if contribution-adjusted drawdown or cash needs are not acceptable. |
| `ibit_zscore_soft_escape_v1` | Reduce to partial IBIT exposure only above rolling z-score soft threshold; re-enter only after hysteresis. | Test whether valuation exits reduce left-tail without killing compounding. | Fails if CAGR give-up is not compensated by drawdown reduction. |
| `ibit_zscore_hard_escape_v1` | Move to defensive exposure only above hard threshold; return by explicit lower threshold or monthly re-entry schedule. | Test rare extreme-cycle escape only. | Fails if routes are too rare, stale, or data coverage is below gate. |
| `ibit_vol_scaled_dca_v1` | Keep scheduled buys, but scale contribution size down when 63-day realized BTC/IBIT volatility is above rolling percentile. | Test volatility budgeting without discretionary timing. | Fails if it becomes hidden market timing or underperforms buy-only after costs. |
| `ibit_trend_pause_v1` | Pause new buys only when BTC/IBIT is below long trend and short momentum is negative; do not liquidate existing IBIT. | Test whether avoiding falling-knife purchases helps without abandoning DCA. | Fails if it misses rebounds and lowers long-window excess. |

IBIT gates:

1. Use unitized, cash-flow-adjusted returns; ending NAV is reported separately.
2. Compare every overlay against both `buy_only_dca` and `parking_only`.
3. Require replayable signal lineage: source URL/path, freshness, coverage,
   first available date, proxy rows filled, and adjusted/raw close setting.
4. Require at least short/mid/long windows and era splits across BTC bull,
   bear, sideways, and post-ETF regimes.
5. No overlay can become live when z-score or proxy coverage gate fails; it may
   remain `notification_only`.

### Nasdaq/S&P deterministic overlay path

The Nasdaq/S&P plugin should be a market-regime signal for compatible equity
strategies, not a standalone allocation engine. QQQ is the growth/Nasdaq risk
proxy; SPY is the broad-market anchor. The plugin should only answer:
"how much offensive risk budget is allowed next rebalance?"

Candidate variants to test next:

| Candidate | Rule | Compatible consumers | Promotion blocker |
| --- | --- | --- | --- |
| `market_trend_gate_10m_v1` | Offensive sleeve allowed only when QQQ and/or SPY is above 10-month/200-day trend. | Russell Top2 sleeve, Global ETF fast sleeve. | Fails if it reduces 5Y excess versus QQQ without enough drawdown improvement. |
| `qqq_spy_relative_budget_v1` | Scale Nasdaq/growth offensive sleeve by QQQ-vs-SPY 6/12M relative momentum. | Nasdaq-heavy sleeves only. | Fails if it simply chases QQQ and raises turnover/crowding. |
| `vol_budget_v1` | Scale offensive sleeve down when 63-day QQQ/SPY realized volatility is above its rolling percentile. | Top2 sleeve, fast ETF sleeve. | Fails if high-vol de-risking misses recoveries enough to lower OOS Sharpe/excess. |
| `panic_rebound_brake_v1` | Temporarily cap aggressive momentum sleeve after deep drawdown plus sharp rebound. | Momentum-heavy sleeves only. | Fails if it de-risks broad defensive baseline or creates whipsaw. |
| `breadth_proxy_canary_v1` | Use QQQ/SPY plus a small fixed ETF canary set only as a fractional budget signal. | Global ETF offensive sleeve. | Fails if canary data availability or parameter count becomes too fragile. |

Nasdaq/S&P gates:

1. Monthly-only evaluation unless a separate intraday/circuit-breaker study is
   explicitly scoped.
2. Compare against current live baseline, QQQ buy-and-hold, and SPY
   buy-and-hold. A strategy with QQQ-like drawdown must beat QQQ; a strategy
   with lower drawdown may pass only with explicit drawdown-adjusted rationale.
3. Require rolling 3Y/5Y windows, walk-forward/OOS splits, era splits, turnover
   and slippage stress, and live-decay monitoring before promotion.
4. Require plugin outputs to be bounded target budgets, for example
   `offensive_budget_multiplier` in `[0, 1]`; plugins must not select symbols,
   weights, or portfolio-level allocations across unrelated strategies.
5. If a candidate only improves one historical crash but loses most other
   windows, mark it `research_reject_or_continue`, not `live_candidate`.

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

## IBIT long-history replay v2 design

The first scheduled smoke artifact only covered the BOXX-available window,
because simulated trades require both the IBIT/proxy asset and the parking
asset to have valid prices. The v2 research path extends the replay window while
keeping runtime behavior unchanged:

- `BTC` remains the default IBIT pre-inception proxy, downloaded as `BTC-USD`
  and normalized to symbol `BTC` in artifacts.
- `BIL` is now the scheduled parking proxy for BOXX pre-inception history. It is
  scaled to the first actual BOXX close and used only to backfill missing BOXX
  rows before BOXX exists.
- The scheduled research builder explicitly uses `price_field=adjusted_close`.
  Yahoo's definition says adjusted close accounts for splits and dividend
  distributions, which is the closer research proxy for ETF total-return-style
  DCA/parking replay than raw close:
  https://help.yahoo.com/kb/SLN28256.html
- The manifest records both proxy lineages:
  - `proxy_rows_filled` for IBIT/BTC proxy fill;
  - `parking_proxy_rows_filled` for BOXX/cash-like proxy fill;
  - `price_field` for adjusted-vs-raw close provenance;
  - `first_actual_ibit_date` and `first_actual_parking_date` when available.
- Strategy behavior is unchanged: scheduled IBIT buys still sell parking shares
  first, regardless of whether the z-score plugin is enabled.
- Promotion review also requires a z-score coverage gate. At least 80% of
  monthly `plugin_on` rebalance signals must have z-score data available;
  earlier price-history months before the first z-score metric are allowed in
  the replay, but they are counted as `zscore_unavailable` and block promotion
  if coverage is too low.

Research rationale:

- Bitcoin DCA should be judged through long and stressful regimes, not only the
  post-IBIT ETF window. Fidelity's DCA primer highlights that scheduled crypto
  purchases reduce timing dependence but do not guarantee profit or loss
  protection: https://www.fidelity.com/learning-center/trading-investing/crypto/dollar-cost-averaging
- MarketVector's 2026 Bitcoin drawdown study reinforces that a 50% Bitcoin
  decline is not a reliable bottom signal and that DCA primarily changes
  dispersion/downside behavior rather than solving timing:
  https://www.marketvector.com/insights/mvis-onehundred/buying-bitcoin-after-a-50percent-crash-rarely-works
- MVRV Z-Score is an on-chain valuation metric built from market value,
  realised value, and a standardized deviation; it should therefore be treated
  as a deterministic valuation signal, not an AI/discretionary plugin:
  https://www.bitcoinmagazinepro.com/charts/mvrv-zscore/
- BOXX remains the preferred live parking symbol, but its own prospectus notes
  box-spread, cash-equivalent, liquidity, low-rate, and frequent-trading risks.
  Backtests must keep it configurable and must not treat parking carry as alpha:
  https://etfarchitect.com/wp-content/uploads/compliance/etf/summary_prospectus/BOXX_Summary_Prospectus.pdf
- BIL/SGOV-style Treasury-bill ETFs are acceptable research proxies for
  cash-like history, but they are proxy assumptions. For example, BlackRock
  describes SGOV as tracking 0-3 month Treasury bills and lists a May 26, 2020
  fund inception, so it is not sufficient for a 2014 replay by itself:
  https://www.ishares.com/us/products/314116/ishares-0-3-month-treasury-bond-etf

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

## 2026-06-20 long-history replay v2 smoke

After adding the parking proxy path, the same free MVRV Z-Score endpoint and
yfinance price downloader produced a valid long-history research artifact:

```bash
PYTHONPATH=. uv run python scripts/build_scheduled_ibit_dca_research.py \
  --zscore-metrics /tmp/.../ibit_zscore_metrics.csv \
  --output-dir /tmp/.../research \
  --price-start 2014-01-01 \
  --initial-parking-value 10000 \
  --contribution-amount 500 \
  --parking-symbol BOXX \
  --parking-proxy-symbol BIL \
  --price-field adjusted_close \
  --btc-proxy-symbol BTC \
  --plugin-config-json '{"dynamic_min_periods":365}'
```

Result summary:

- Simulated artifact span: `2014-09-17` through `2026-06-20`, limited at the
  start by available `BTC-USD` yfinance history.
- IBIT/BTC proxy rows filled: `3403`; first actual IBIT date:
  `2024-01-11`.
- BOXX/BIL parking proxy rows filled: `3202`; first actual BOXX date:
  `2022-12-28`.
- Z-score history span: `2022-06-20` through `2026-06-19`, `1460` rows.
- Monthly plugin signals: `141` total, all `normal`.
- Signal data-status counts: `available=48`, `zscore_unavailable=93`. Early
  price-history months before the first z-score metric keep normal IBIT
  exposure and are explicitly counted rather than silently treated as valid
  z-score evidence.
- Z-score coverage gate: `fail`; available signal ratio is about `34.04%`,
  below the `80%` minimum required for promotion review.

Live-readiness summary:

| Variant | CAGR | Max drawdown | Excess vs QQQ | Excess vs SPY | Gate |
| --- | ---: | ---: | ---: | ---: | --- |
| `parking_only` | 1.88% | -0.23% | -17.78% | -11.97% | baseline |
| `buy_only_dca` | 54.11% | -83.40% | +34.44% | +40.25% | baseline |
| `plugin_on` | 54.11% | -83.40% | +34.44% | +40.25% | fail |

Implication: the long-history replay infrastructure is now good enough to
evaluate BTC-era DCA and parking-proxy assumptions, but the current free-source
z-score overlay still adds no value in this replay. The correct live stance
remains unchanged: buy-only DCA/parking mechanics can be strategy behavior, but
the z-score escape overlay stays research-only/notification-only until a
separate promotion artifact proves positive net value.

## 2026-06-20 live-enable decision refresh

This pass re-ran the narrow, pre-registered evidence gates instead of expanding
the parameter search. The goal was to avoid selecting a backtest winner that
only looks strong in-sample.

### Global ETF narrow sleeve

Command shape:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
  .venv/bin/python -m us_equity_snapshot_pipelines.global_etf_offensive_rotation_research \
  --download \
  --price-start 2010-01-01 \
  --variants live_global_etf_rotation_defensive_baseline,offensive_growth_fast_top2_monthly \
  --liveable-composites liveable_baseline_relative_decay_brake_baseline90_fast10_floor0,liveable_blend_baseline90_fast10 \
  --robustness-candidates liveable_baseline_relative_decay_brake_baseline90_fast10_floor0,liveable_blend_baseline90_fast10,live_global_etf_rotation_defensive_baseline,offensive_growth_fast_top2_monthly \
  --walk-forward-candidates liveable_baseline_relative_decay_brake_baseline90_fast10_floor0,liveable_blend_baseline90_fast10 \
  --walk-forward-min-train-excess-cagr 0.005 \
  --turnover-cost-bps 5 \
  --cost-stress-bps 5,10,15,25 \
  --dynamic-cost \
  --dynamic-cost-navs 100000,250000,1000000 \
  --output-dir data/output/global_etf_baseline_relative_decay_narrow_verify_20260620
```

Result:

- `liveable_blend_baseline90_fast10` passed static live-readiness and dynamic
  NAV stress at `$100k`, `$250k`, and `$1M`, with long-window excess CAGR
  versus the current defensive baseline around `+0.33%`.
- It still failed the walk-forward/OOS promotion gate:
  `walk_forward_gate_passed=false`, reason `worst_oos_excess_too_low`.
- The OOS selection kept the current baseline in 4 of 7 windows, promoted
  `90/10` in 3 windows, had median OOS excess around `+1.57%`, but worst OOS
  excess was about `-4.9%`, below the `-3%` hard floor.
- The baseline-relative decay brake was worse than static `90/10`: it failed
  long-excess, calendar win-rate, and rolling 5Y baseline win-rate gates.

Decision: **do not replace the current live Global ETF defensive baseline**.
The `90/10` sleeve remains a research candidate only. It is not live-enabled
because the OOS downside gate failed.

### IBIT Smart DCA / z-score plugin

Fresh long-history replay using public MVRV Z-Score data and yfinance prices:

- Simulated span: `2014-09-17` through `2026-06-20`.
- IBIT/BTC proxy rows filled: `3403`; BOXX/BIL parking proxy rows filled:
  `3202`.
- `parking_only`: CAGR about `1.88%`, max drawdown about `-0.23%`.
- `buy_only_dca`: CAGR about `54.11%`, max drawdown about `-83.40%`.
- `plugin_on`: same CAGR and drawdown as `buy_only_dca`; gate `fail`.
- Z-score signal coverage failed: `48` available monthly plugin signals vs
  `93` `zscore_unavailable` signals, available ratio about `34.04%` vs the
  `80%` minimum.

Decision: **do not live-enable z-score position control for IBIT**. Keep
`ibit_zscore_exit` `notification_only`; the DCA/parking replay remains useful
research infrastructure but is not a promotion artifact.
