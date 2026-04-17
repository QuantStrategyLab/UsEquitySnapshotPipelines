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
- `r1000_latest_holdings_snapshot.csv` for scheduled mega-cap dynamic top20 ranking

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

Build the aggressive mega-cap profile from a larger ranked universe or curated
expanded universe file:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_mega_cap_leader_rotation_aggressive_snapshot.py \
  --prices /path/to/r1000_price_history.csv \
  --universe /path/to/r1000_latest_holdings_snapshot.csv \
  --as-of 2026-04-01 \
  --dynamic-universe-size 50 \
  --output-dir data/output/mega_cap_leader_rotation_aggressive
```

This profile uses the same feature schema as dynamic top20, but writes a
separate `mega_cap_leader_rotation_aggressive` contract and defaults to a
higher-risk top-3/no-defense runtime profile.

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

Run the research-only MAG7 leveraged pullback / high-trim backtest from an
existing MAG7 price-history file:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_mag7_leveraged_pullback.py \
  --prices data/output/mega_cap_leader_rotation_mag7_backtest/input/mega_cap_leader_rotation_mag7_price_history.csv \
  --start 2016-01-01 \
  --frequency weekly \
  --top-n 3 \
  --leverage-multiple 2.0 \
  --max-product-exposure 0.8 \
  --soft-product-exposure 0.5 \
  --hard-product-exposure 0.15 \
  --leveraged-expense-rate 0.01 \
  --single-name-cap 0.25 \
  --turnover-cost-bps 5 \
  --output-dir data/output/mag7_leveraged_pullback_backtest
```

This strategy is not a live profile. It buys strong MAG7 names on controlled
pullbacks, trims exposure near short-term highs, caps single-name exposure, and
models the invested sleeve as daily-reset 2x long products rather than margin
borrowing. The command writes
`summary.csv`, `portfolio_returns.csv`, `weights_history.csv`,
`turnover_history.csv`, `candidate_scores.csv`, `trades.csv`,
`exposure_history.csv`, and `reference_returns.csv`.

To test a point-in-time dynamic leader pool instead of the static MAG7 list,
pass a universe history. The same CLI can also read a concatenated Roundhill
MAGS holdings CSV directly; rows with `Account=MAGS` are mapped back to the
seven underlying stocks, and swap exposure is combined with stock exposure:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_dynamic_mega_leveraged_pullback.py \
  --prices data/output/mega_cap_leader_rotation_dynamic_universe_top20_backtest/input/mega_cap_leader_rotation_dynamic_top20_price_history.csv \
  --universe data/output/mega_cap_leader_rotation_dynamic_universe_top20_backtest/input/mega_cap_leader_rotation_dynamic_top20_universe_history.csv \
  --start 2023-12-01 \
  --candidate-universe-size 10 \
  --top-n 3 \
  --leverage-multiple 2.0 \
  --return-mode leveraged_product \
  --output-dir data/output/dynamic_mega_leveraged_pullback_backtest
```

Use `--return-mode margin_stock --margin-borrow-rate 0.055` to test the same
selection and risk gate as 2x margin-financed underlying stock exposure. The
default mode remains daily-reset 2x long products with `--leveraged-expense-rate`.

To test the separated TACO rebound plugin as a small left-side budget boost,
pass a deterministic signal file with `as_of` and `sleeve_suggestion` columns.
The budget is additive to the strategy's normal product exposure, capped by
`--rebound-budget-cap`, and blocked by default during `hard_defense`:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_dynamic_mega_leveraged_pullback.py \
  --prices data/output/mega_cap_leader_rotation_dynamic_universe_top20_backtest/input/mega_cap_leader_rotation_dynamic_top20_price_history.csv \
  --universe data/output/mega_cap_leader_rotation_dynamic_universe_top20_backtest/input/mega_cap_leader_rotation_dynamic_top20_universe_history.csv \
  --start 2023-12-01 \
  --candidate-universe-size 10 \
  --top-n 3 \
  --leverage-multiple 2.0 \
  --return-mode leveraged_product \
  --rebound-budget-signals data/output/dynamic_mega_leveraged_pullback/plugins/taco_rebound_shadow/signals.csv \
  --rebound-budget-cap 0.10 \
  --output-dir data/output/dynamic_mega_leveraged_pullback_taco_rebound_budget_backtest
```

This does not let TACO select stocks and does not turn a small TACO budget into
a full right-side risk-on allocation. The base strategy's risk gate and
candidate ranking still decide what can be bought.

To research bear-market dip buying inside the MAGS-style pullback strategy,
enable the research-only bear candidate switch. The default is `off`.
`market_safe` only allows below-200SMA single-name rebound candidates while the
market trend filter is not in `soft_defense` or `hard_defense`; `market_bear`
only allows them while the market filter is defensive. The switch does not
change live routing and does not override the product exposure caps:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_dynamic_mega_leveraged_pullback.py \
  --prices data/output/mega_cap_leader_rotation_dynamic_universe_top20_backtest/input/mega_cap_leader_rotation_dynamic_top20_price_history.csv \
  --universe data/output/mega_cap_leader_rotation_dynamic_universe_top20_backtest/input/mega_cap_leader_rotation_dynamic_top20_universe_history.csv \
  --start 2017-10-02 \
  --candidate-universe-size 15 \
  --top-n 3 \
  --return-mode leveraged_product \
  --bear-candidate-mode market_safe \
  --bear-candidate-max-size-multiplier 0.35 \
  --output-dir data/output/dynamic_mega_leveraged_pullback_bear_research
```

Use `--return-mode margin_stock --leverage-multiple 1.0 --margin-borrow-rate 0`
to compare the same selection rules against unlevered underlying stocks.

Run a small parameter matrix:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_dynamic_mega_leveraged_pullback_matrix.py \
  --prices data/output/mega_cap_leader_rotation_dynamic_universe_top20_backtest/input/mega_cap_leader_rotation_dynamic_top20_price_history.csv \
  --universe data/output/mega_cap_leader_rotation_dynamic_universe_top20_backtest/input/mega_cap_leader_rotation_dynamic_top20_universe_history.csv \
  --start 2017-10-02 \
  --candidate-universe-sizes 7,10,15,20 \
  --top-n-values 2,3,4 \
  --return-mode leveraged_product \
  --output-dir data/output/dynamic_mega_leveraged_pullback_matrix
```

Run the research-only 2018-to-present Trump/Biden trade-war / TACO-like panic
rebound event study:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_taco_panic_rebound.py \
  --download \
  --event-set full \
  --price-start 2018-01-01 \
  --ranking-horizon 42 \
  --output-dir data/output/taco_panic_rebound_2018_present_research
```

This is an event-window research tool, not a live strategy. It uses a fixed
trade-war event calendar, finds the post-shock trough within the configured
window, then reports 5 / 10 / 21 / 42 / 63 trading-day rebounds by symbol. Use
`--event-set first-term`, `--event-set biden`, or `--event-set second-term` for
single-period diagnostics. Use `--event-set geopolitical-deescalation` for the
research-only 2026 U.S.-Iran de-escalation / ceasefire bucket, or
`--event-set full-plus-geopolitical-deescalation` to append that bucket to the
default trade-war calendar. The default `full` event set intentionally does not
include the geopolitical bucket. The command writes `event_calendar.csv`,
`event_windows.csv`, `shock_symbol_summary.csv`, and
`softening_symbol_summary.csv`.

Run a research-only TACO panic rebound portfolio sleeve backtest using the full
2018-to-present event calendar and presidential-period summaries:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_taco_panic_rebound_portfolio.py \
  --download \
  --preset steady \
  --event-set full \
  --price-start 2018-01-01 \
  --start 2018-01-01 \
  --account-sleeve-ratio 0.10 \
  --output-dir data/output/taco_panic_rebound_2018_present_portfolio
```

The portfolio backtest models a separate event-rebound sleeve, not a full-account
allocation. `steady` uses a lower-volatility basket (`QLD`, `TQQQ`, `ROM`,
`USD`, `NVDA`, `AMD`); `aggressive` uses `TQQQ`, `TECL`, `SOXL`, `NVDA`, and
`AMD`. The output includes both the standalone sleeve return and a cash-backed
account overlay row for the configured sleeve ratio, plus `period_summary.csv`
for Trump 1, Biden, and Trump 2-to-date.

Run the V1 price-stress TACO overlay comparison against the current
`tqqq_growth_income` fixed dual-drive research baseline:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_taco_panic_rebound_overlay_compare.py \
  --download \
  --event-set full \
  --price-start 2014-01-01 \
  --start 2015-01-01 \
  --overlay-sleeve-ratios 0.05,0.10 \
  --audit-modes crisis_veto \
  --output-dir data/output/tqqq_taco_price_stress_overlay_2015
```

This is the first-version definition for the TQQQ/TACO overlay research:
`QQQ` / `TQQQ` price pressure opens the policy-event scanner, which classifies
trade-war / tariff shock or softening events, and the overlay can use a small
slice of the baseline `BOXX` / cash sleeve to buy `TQQQ`. VIX and macro data are
not used as hard vetoes or position-size reducers in V1.

The explicit geopolitical research buckets are separate from the default live
calendar. `geopolitical-deescalation` only includes de-escalation / ceasefire /
talks events, while `geopolitical-conflict-and-deescalation` also includes the
war-escalation stress events for sensitivity testing. Prefer the
de-escalation-only bucket when studying a potential small TACO sleeve because it
does not try to buy the war headline itself.

The optional dual-review backtest is a deterministic audit proxy, not a
historical replay of live model responses. The event calendar simulates the
proposer rubric, and the auditor can only veto candidates that fall inside
predeclared systemic-crisis windows or event ids supplied with
`--audit-veto-event-ids`. This makes the conflict policy backtestable without
pretending that a model actually ran in 2018/2019. The output writes
`summary.csv`, `deltas_vs_base.csv`, `diagnostics.csv`,
`audit_diagnostics.csv`, `recognized_event_calendar.csv`,
`audit_decisions.csv`, `taco_trades_by_scenario.csv`, and per-strategy return /
weight series.

For a longer black-swan stress sample, use a synthetic daily-reset `TQQQ` proxy
from `QQQ` because real `TQQQ` did not exist during the internet bubble or the
2008 financial crisis. The optional price-only crisis guard is also a
deterministic proxy, not a model replay:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_taco_panic_rebound_overlay_compare.py \
  --download \
  --event-set full \
  --price-start 1999-03-10 \
  --start 1999-03-10 \
  --attack-symbol SYNTH_TQQQ \
  --synthetic-attack-from QQQ \
  --synthetic-attack-multiple 3 \
  --safe-symbol SHY \
  --include-price-crisis-guard \
  --overlay-sleeve-ratios 0.05 \
  --output-dir data/output/tqqq_taco_price_stress_overlay_1999_synthetic
```

Use this long sample to inspect `dotcom_bubble_burst`,
`gfc_peak_to_trough`, and `lost_decade_2000_2009` rows. Treat the synthetic
`TQQQ` and crisis-guard rows as stress-test evidence, not as a perfect
reconstruction of tradable historical products.

Run the standalone Crisis Guard research matrix separately from TACO:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_crisis_regime_guard.py \
  --download \
  --price-start 1999-03-10 \
  --start 1999-03-10 \
  --attack-symbol SYNTH_TQQQ \
  --synthetic-attack-from QQQ \
  --synthetic-attack-multiple 3 \
  --safe-symbol SHY \
  --context-gates none,rubric \
  --drawdown-thresholds=-0.20,-0.25,-0.30 \
  --risk-multipliers 0,0.25,0.5 \
  --confirm-days 5 \
  --output-dir data/output/crisis_regime_guard_1999_synthetic
```

This matrix compares the base TQQQ profile with price-only crisis guard variants
across dot-com, GFC, COVID, 2022, and post-2015 periods. `--context-gates none`
keeps the pure price-only guard. `rubric` enables the deterministic two-step
crisis rubric. The confirmed price crisis signal opens
the context scanner, which classifies the triggered crisis candidate as
bubble-burst risk, financial-crisis risk, or non-systemic bear / policy shock.
The audit rubric can only approve or veto protection. The simulated rubric uses a
bubble proxy (`QQQ` trailing 252-day return above 75%, remembered for 126
trading days by default) plus a financial-stress proxy (`XLF` drawdown and
relative weakness vs `SPY`) when entering protection, then keeps the guard
active until the price crisis signal turns off. This is a deterministic
stand-in for a future live crisis module, not a model-driven backtest.

The output writes `summary.csv`, `deltas_vs_base.csv`,
`guard_diagnostics.csv`, `context_diagnostics.csv`, `guard_events.csv`,
`context_opinions.csv`, and per-strategy return / weight / signal / context series.
`context_opinions.csv` is intentionally sparse: it
records only dates where the confirmed price crisis signal would have opened the
context scanner. The matrix is
for research only and defaults to disabled unless explicitly requested: it is
intended to show the cost of false positives as well as the benefit in
2000/2008-style crises before any crisis module is allowed to affect live
allocations.

See `docs/crisis-response-v1.md` for the frozen V1 contract,
`docs/crisis-response-research-roadmap.md` for post-V1 historical-crash
research, and `docs/crisis-context-research-v2.md` for the research-only
context pack.

Build the V2 crisis context pack before changing any routing logic:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_crisis_context_pack.py \
  --download \
  --event-set full \
  --price-start 1999-03-10 \
  --start 1999-03-10 \
  --output-dir data/output/crisis_context_v2
```

This writes `crisis_context_features.csv` and `context_diagnostics.csv`. If
yfinance is rate limited, pass `--prices` with a saved price-history CSV. If a
legitimate proxy is available, set `YFINANCE_PROXY` or pass `--download-proxy`.
The V2 pack includes research-only COVID exogenous / policy-rescue windows and
tariff shock / softening windows so 2020 and 2018-2019 false positives can be
audited before any context affects live routing. It writes raw financial /
credit context separately from stricter systemic financial-crisis context.

Run the unified Crisis Response research when comparing fake-crisis TACO entries
with true-crisis defense in one historical research report:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_crisis_response.py \
  --download \
  --event-set full \
  --price-start 1999-03-10 \
  --start 1999-03-10 \
  --attack-symbol SYNTH_TQQQ \
  --synthetic-attack-from QQQ \
  --synthetic-attack-multiple 3 \
  --safe-symbol SHY \
  --overlay-sleeve-ratios 0.05 \
  --crisis-drawdown=-0.20 \
  --crisis-risk-multiplier 0.25 \
  --crisis-confirm-days 5 \
  --output-dir data/output/crisis_response_1999_synthetic
```

This unified research treats both modules as one event-response research
surface for audit continuity. It is not the production-facing plugin split. The
price-stress scanner opens the TACO path for trade-war / tariff fake-crisis
events. The confirmed crisis-price signal opens the crisis-context path for
bubble-burst or financial-crisis candidates. If the true-crisis guard is active,
TACO entries are suppressed; otherwise approved policy / tariff shocks can use
the small TACO sleeve. The output includes `response_decisions.csv`, which is
the main audit file for whether each candidate was routed to `taco_fake_crisis`,
`true_crisis`, or `no_action`.

To compare the frozen V1 deterministic rubric with the research-only V2 context pack, add
the explicit mode flag. The default remains `v1_rubric`.

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_crisis_response.py \
  --download \
  --event-set full \
  --price-start 1999-03-10 \
  --start 1999-03-10 \
  --attack-symbol SYNTH_TQQQ \
  --synthetic-attack-from QQQ \
  --synthetic-attack-multiple 3 \
  --safe-symbol SHY \
  --overlay-sleeve-ratios 0.05 \
  --crisis-drawdown=-0.20 \
  --crisis-risk-multiplier 0.25 \
  --crisis-confirm-days 5 \
  --crisis-context-mode v2_context_pack \
  --output-dir data/output/crisis_response_1999_synthetic_v2_context
```

When this mode is used, the output also includes
`crisis_context_features.csv`, and `context_opinions.csv` records the V2 suggested
route behind each confirmed crisis-price trigger.

External valuation context stays audit-only unless explicitly enabled. To test
historical PE/CAPE/earnings-quality context, pass an `as_of` CSV with columns
such as `nasdaq_100_trailing_pe`, then add `--external-context` and
`--external-valuation-mode price_or_external` or `price_and_external`.

Build the Phase 1 log-only Crisis Response shadow signal after the TQQQ daily
price/context refresh:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_crisis_response_shadow_signal.py \
  --prices data/output/crisis_response_shadow/input/price_history.csv \
  --external-context data/output/crisis_response_shadow/input/external_context.csv \
  --event-set full \
  --start 1999-03-10 \
  --output-dir data/output/crisis_response_shadow
```

This writes `latest_signal.json`, dated JSON/CSV signal files, and an evidence
CSV under `data/output/crisis_response_shadow/`. The shadow builder is designed
to run on the same daily cadence as the TQQQ artifact
pipeline, but it is `shadow_only`: no broker writes, no order placement, and no
live allocation mutation. Downstream notifications may read the latest JSON and
display it as an observation beside the TQQQ status, not as an executable trade
instruction. Advisory or live promotion remains a separate manual decision
after enough deterministic shadow logs have accumulated.

The crisis shadow plugin is intentionally defense-only. It does not emit
`taco_fake_crisis`, does not suggest a TACO sleeve, and does not buy event
rebounds. Its executable route is only a true-crisis `defend` signal, with the
defensive destination documented as cash or a money-market / Treasury-bill
parking sleeve. Policy, tariff, or geopolitical panic can still appear as
watch-only context, but TACO routing lives in a separate plugin.

Build the independent TACO rebound shadow signal for a left-side rebound
strategy such as `dynamic_mega_leveraged_pullback`:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_taco_rebound_shadow_signal.py \
  --prices data/output/taco_rebound_shadow/input/price_history.csv \
  --event-set geopolitical-deescalation \
  --start 2026-01-01 \
  --geopolitical-deescalation-sleeve 0.10 \
  --tariff-softening-sleeve 0.05 \
  --max-sleeve 0.10 \
  --output-dir data/output/dynamic_mega_leveraged_pullback/plugins/taco_rebound_shadow
```

This TACO plugin does not select stocks and does not mutate a strategy artifact.
It only writes a small rebound-budget suggestion that a left-side strategy can
consume after its own candidate selection and risk gates. Conflict escalation
events default to a zero sleeve; de-escalation / ceasefire / talks events can
raise a bounded rebound budget when the price-stress scanner is open.

For platform-style deployment, use the sidecar plugin runner instead of wiring
plugins into a strategy function. The runner reads strategy-scoped plugin
mounts from TOML and executes only explicitly configured plugins. Current
Crisis Response and TACO rebound plugins write artifacts here; any platform
execution is downstream.
Use `docs/examples/strategy_plugins.example.toml` as the schema example. Real
runtime TOML should live with the deployment or platform configuration, not as a
committed live config in this repository.

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/run_strategy_plugins.py --config /path/to/strategy_plugins.toml
```

This is intentionally a sidecar. If the runner is disabled or fails, the
underlying strategy artifact build remains independent; downstream systems read
the plugin's `latest_signal.json`, verify its `strategy` / `plugin` identity,
and implement the configured `mode` directly.
The same plugin can be mounted to another strategy by adding another
`[[strategy_plugins]]` entry with different inputs and output scope. Keep the
crisis plugin scoped to defensive TQQQ-style risk-off behavior, and mount
`taco_rebound_shadow` separately to left-side pullback profiles when researching
event-rebound budget changes.

Supported modes are `shadow`, `paper`, `advisory`, and `live`. `mode` is the
single plugin behavior contract: if it is `shadow`, the downstream platform
treats it as shadow; if it is `paper`, `advisory`, or `live`, the platform
implements that mode's semantics. Platform risk checks, kill switches, and data
freshness guards may block unsafe execution, but they should not reinterpret the
configured mode. This repository still writes artifacts only, so the payload
separates platform-mode fields such as `broker_order_allowed` from repository
capability fields such as `repository_broker_write_allowed=false`. Use
`default_mode` as the fallback and set per-plugin `mode` only when a plugin
should override that default.

Mode meanings:

| Mode | Meaning |
| --- | --- |
| `shadow` | Signal, evidence, logs, and optional notification context only |
| `paper` | Paper ledger of hypothetical plugin actions; no real allocation change |
| `advisory` | Human-confirmed recommendation; no automatic execution |
| `live` | Platform adapter may affect execution under explicit risk limits |

The `Publish Strategy Plugins` GitHub workflow runs the
`tqqq_growth_income` / `crisis_response_shadow` plugin in `shadow` mode on a
weekday post-close schedule. Its default GCS prefix is:

```text
gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/tqqq_growth_income/plugins/crisis_response_shadow
```

Downstream platforms should mount only the resulting `latest_signal.json` and
must not duplicate `mode` in platform config.
