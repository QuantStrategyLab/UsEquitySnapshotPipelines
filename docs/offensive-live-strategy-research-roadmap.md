# Offensive Live Strategy Research Roadmap

This note consolidates the current Russell Top50 leader-rotation and Global ETF offensive-rotation research into a live-design roadmap. It is research guidance only: it does not enable any live runtime profile, broker behavior, or production manifest change.

## Current decision snapshot

| Strategy line | Current status | Preferred action |
| --- | --- | --- |
| Russell Top50 leader rotation | Best current offensive live-design line. Product-data v2 PIT refresh and deterministic live-readiness gate are in place. `25% Top2 / 75% Top4` and `50% Top2 / 50% Top4` pass live-design review gates. Latest `UsEquityStrategies` already uses the balanced 50/50 blend as the runtime shape. | Harden the runtime with named variants, explicit diagnostics, rollback to Top4, and shadow/live-readiness checks before further promotion. |
| Global ETF offensive sleeve | Not ready for live promotion. The best safe-sleeve variants improved median OOS behavior but still failed the worst-OOS excess gate. | Keep existing defensive `global_etf_rotation` live baseline. Continue research only if the next test is pre-registered and narrow. |
| Dynamic/cash-risk overlays | Broad daily cash-defense variants have not justified default use. | Do not add to the default live version. Only test narrowly defined momentum-crash brakes. |

## Non-negotiable live-design rules

1. **Strategy logic, not plugin logic.** The live strategy must consume deterministic, backtestable portfolio-construction rules. Plugins may produce signals, but a default live rule must not depend on an AI or discretionary plugin.
2. **Research gate is separate from runtime.** Runtime should not decide whether a strategy is live-ready. A research workflow should produce a pass/fail promotion artifact, and runtime should only execute approved config.
3. **No ad hoc parameter expansion.** New experiments must be pre-registered with a small candidate set. If a candidate is added after seeing failures, it must go through the same OOS and cost gates.
4. **Benchmark-relative promotion.** A strategy with about `-30%` drawdown must beat the relevant benchmark over long windows and must not rely on one recent regime to pass.
5. **Execution assumptions are part of the strategy.** Turnover cost, liquidity, and participation assumptions must be fixed before promotion. If a candidate only works at unrealistically low costs, keep it research-only.

## Literature-informed expansion ideas

These are design inputs for future tests, not evidence that any specific variant should be promoted.

### 1. Momentum structure: stock, industry, and factor layers

Moskowitz and Grinblatt document that industry momentum explains a meaningful part of individual-stock momentum profits. This matters for Russell Top50 because mega-cap winners can cluster in a few industries or factor regimes. Source: https://www-stat.wharton.upenn.edu/~steele/Courses/956/Resource/Momentum/MoskowitzGrinblatt99.pdf

Ehsani and Linnainmaa argue that individual-stock momentum can be related to momentum in factor returns, and that factor momentum is pervasive. Source: https://www.nber.org/system/files/working_papers/w25551/w25551.pdf

Actionable implication for this repo:

- Do not blindly add more concentrated Top1/Top2 variants.
- If testing diversification, prefer a small set of sector/factor-aware variants:
  - `Top4` fallback remains baseline.
  - `25% Top2 / 75% Top4` and `50% Top2 / 50% Top4` stay the only live-design candidates.
  - A new research-only variant can test whether sector-aware ranking reduces crash concentration, but it must beat the current fixed blends after costs and OOS gates.

### 2. Momentum crash regimes

Daniel and Moskowitz show that momentum crashes are partly forecastable and occur in panic states after market declines, when volatility is high, and alongside rebounds. Source: https://www.nber.org/system/files/working_papers/w20439/w20439.pdf

Barroso and Santa-Clara show that momentum risk is time-varying and that risk-managed
momentum can materially reduce crash exposure. Source:
https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2041429

Moreira and Muir show that volatility-managed portfolios often improve factor
Sharpe ratios by reducing exposure when volatility is high. Source:
https://www.nber.org/papers/w22208

Actionable implication:

- A crash brake should not be a generic all-weather cash filter.
- If tested, define it narrowly as a **Top2 sleeve reducer**, not a full portfolio de-risker:
  - only affects the aggressive Top2 sleeve;
  - evaluated monthly, not daily, unless a separate turnover/cost test justifies daily checks;
  - activates only when drawdown, volatility, and rebound-risk conditions are all present.

### 3. Time-series / dual momentum as risk control

Moskowitz, Ooi, and Pedersen document time-series momentum across liquid instruments, with persistence over 1-12 months and reversal over longer horizons. Source: https://w4.stern.nyu.edu/facdir/lpederse/papers/TimeSeriesMomentum.pdf

Antonacci's dual-momentum work supports combining relative momentum with absolute momentum to reduce volatility and drawdown. Source: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2042750

Actionable implication:

- Use absolute momentum as a risk-control input only after the fixed-blend candidate is established.
- For Russell, the first runtime candidate should remain a fixed Top2/Top4 blend. Absolute momentum filters belong in a later research-only branch.
- For Global ETF, the existing defensive baseline already uses stronger risk-control behavior; offensive sleeves must prove incremental value versus that baseline, not only versus SPY.

### 4. Overfitting controls

Bailey, Borwein, López de Prado, and Zhu propose estimating the probability of backtest overfitting in investment simulations via combinatorially symmetric cross-validation. Source: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253

Bailey and López de Prado's Deflated Sharpe Ratio adjusts for selection bias, non-normal returns, and multiple trials. Source: https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf

White's Reality Check highlights data-snooping risk when the same data is reused to select a successful rule. Source: https://www.ssc.wisc.edu/~bhansen/718/White2000.pdf

Actionable implication:

- The next live candidate should not be chosen from a broad grid.
- For Russell, freeze the candidate set to:
  - `base_top4_cap25` fallback;
  - `blend_top2_25_top4_75` conservative;
  - `blend_top2_50_top4_50` balanced offensive;
  - pure Top2 and dynamic switches remain research-only comparators.
- Add PBO/CSCV-style diagnostics only after the candidate matrix is small and stable; do not use it as a license to search more parameters.

## Recommended next implementation sequence

### Phase 1: Russell named runtime variants and rollback controls

Goal: make the current Russell runtime behavior explicit and reversible. Latest
`UsEquityStrategies` already represents the runtime default as the balanced
`50% Top2 / 50% Top4` blend, so this phase should not silently change the default.

Scope:

- Repository likely affected: `UsEquityStrategies` for runtime/manifest config; `UsEquitySnapshotPipelines` only if snapshot metadata needs an explicit variant field.
- Add or harden a named config key such as `leader_rotation_profile_variant` with allowed values:
  - `top4_baseline` — rollback/fallback behavior;
  - `blend_top2_25_top4_75` — conservative candidate;
  - `blend_top2_50_top4_50` — balanced offensive candidate and current runtime shape.
- Keep the existing runtime default unchanged; use the named key for diagnostics,
  conservative override, and emergency rollback.
- Add a diagnostic-only `leader_rotation_shadow_variants` path so live/paper
  runs can compare balanced, conservative, and Top4 rollback target weights
  without changing actual returned positions. The shadow output should include
  target-weight deltas versus the active runtime variant for operator review.
- Runtime should compute the same deterministic weights as backtest, using snapshot ranking and approved constants.

Gate before any further promotion:

- latest product-data v2 or better input source;
- live-readiness summary passes for the chosen variant;
- no stale-universe fallback streak;
- snapshot manifest is present and contract version matches;
- one shadow cycle compares current live balanced target weights, conservative
  target weights, and Top4 rollback target weights.

Shadow/live-readiness checklist:

1. Runtime config keeps actual positions on the approved active variant.
2. `leader_rotation_shadow_variants` is enabled in paper or operator-review mode.
3. Diagnostics include all three named variants and `weight_delta_vs_active` for
   each shadow variant, plus the largest single-name increase/decrease summary.
4. `leader_rotation_shadow_review_rows` is archived as the compact operator
   review artifact, with one row per shadow variant and a stable
   `leader_rotation_shadow_review_schema_version`.
5. The monthly snapshot is inside the configured execution window; outside-window
   no-op events are not counted as a valid shadow comparison.
6. The active variant, conservative variant, and Top4 rollback variant are
   compared against the same feature snapshot, current holdings, account equity,
   safe haven, and income-layer settings.
7. Operator review records `turnover_delta_vs_active`, the largest
   positive/negative single-name deltas, and whether they are acceptable given
   account size and liquidity.
8. No promotion is made unless the chosen variant also passes the latest
   `mega_cap_leader_rotation_live_readiness` gate and the result is archived
   with the runtime snapshot manifest.

Runtime shadow review archive:

After the `UsEquityStrategies` runtime emits
`leader_rotation_shadow_review_rows`, archive the rows through:

```bash
PYTHONPATH=src python -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_shadow_review \
  --diagnostics-json /path/to/release_status_summary_or_runtime_diagnostics.json \
  --output-dir data/output/russell_top50_shadow_review_YYYYMMDD \
  --profile russell_top50_leader_rotation \
  --snapshot-as-of YYYY-MM-DD
```

Outputs:

- `russell_top50_leader_rotation_shadow_review_rows.csv`
- `russell_top50_leader_rotation_shadow_review_rows.json`
- `russell_top50_leader_rotation_shadow_review_manifest.json`

The archive builder accepts either a runtime diagnostics object or a
`release_status_summary.json` object with nested `diagnostics`. It keeps only
the stable row fields listed below and rejects review notes that appear to
contain account, token, secret, password, cookie, JWT, or authorization text.
This keeps operator artifacts useful for review without leaking broker/account
state.

2026-06-20 local cross-repository integration check:

- Runtime source: `UsEquityStrategies` PR `#151`, branch
  `codex/russell-runtime-variants`.
- Pipeline source: this roadmap PR, branch `codex/offensive-live-next-research`.
- Product-data input: `data/output/russell_top50_product_data_full_20260620_rerun`
  rebuilt from product-data v2.
- Snapshot as-of: `2026-05-29`; runtime evaluation date: `2026-06-01`, inside
  the monthly execution window.

Result:

- Runtime active variant: `blend_top2_50_top4_50`.
- Runtime target weights: `MU 37.5%`, `SNDK 37.5%`, `AMD 12.5%`, `INTC 12.5%`.
- Runtime emitted `3` shadow review rows.
- Archive builder wrote:
  - `russell_top50_leader_rotation_shadow_review_rows.csv`
  - `russell_top50_leader_rotation_shadow_review_rows.json`
  - `russell_top50_leader_rotation_shadow_review_manifest.json`
- The archived rows contained no account/token/secret/password/cookie/JWT/
  authorization text.

Shadow comparison from that integration run:

| Shadow variant | Turnover delta vs active | Largest increase | Largest decrease |
| --- | ---: | --- | --- |
| `top4_baseline` | 25.00% | `AMD +12.50%` | `MU -12.50%` |
| `blend_top2_25_top4_75` | 12.50% | `AMD +6.25%` | `MU -6.25%` |
| `blend_top2_50_top4_50` | 0.00% | none | none |

Review row schema:

`UsEquityStrategies` defines the row order through
`SHADOW_REVIEW_ROW_FIELDS` and emits the same order in
`leader_rotation_shadow_review_row_fields`. Current fields:

- `schema_version`
- `active_variant`
- `shadow_variant`
- `selected_count`
- `realized_stock_weight`
- `safe_haven_weight`
- `turnover_delta_vs_active`
- `largest_increase_symbol`
- `largest_increase_delta`
- `largest_decrease_symbol`
- `largest_decrease_delta`
- `review_note`

`review_note` should stay deterministic and non-sensitive: active variant,
shadow variant, one-way turnover delta, and largest single-name
increase/decrease only. Do not include account identifiers, broker account
state, tokens, or per-account private notes in the archived row.

### Phase 2: Russell OOS / robustness hardening

Goal: prove the fixed-blend candidates are not only full-sample winners.

Additional source-data risk: FTSE Russell announced that Russell US Indexes move
from annual to semi-annual reconstitution in 2026, with the June 2026 newly
reconstituted indexes taking effect after the US close on June 26, 2026. Source:
https://www.lseg.com/en/ftse-russell/russell-reconstitution

Minimal tests:

- rolling 3Y/5Y gate already exists; add a walk-forward summary with fixed candidates only;
- cost ladder: 5, 10, 15, 25 bps;
- liquidity/ADV stress for individual stocks, including max participation estimate by portfolio NAV;
- source-lag sensitivity: 21, 42 trading-day lag.

Do not add new strategy formulas in this phase.

Implemented stress artifact:

```bash
PYTHONPATH=src python -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_stress_readiness \
  --prices data/output/russell_top50_product_data_full/prices.csv \
  --universe data/output/russell_top50_product_data_full/universe.csv \
  --output-dir data/output/russell_top50_product_data_full_stress_readiness_YYYYMMDD \
  --turnover-cost-bps-values 5,10,15,25 \
  --universe-lag-days-values 21,42 \
  --min-adv20-usd-values 20000000 \
  --rolling-window-years 3,5
```

Outputs:

- `stress_live_readiness_detail.csv`: one row per fixed candidate and stress
  scenario.
- `stress_live_readiness_summary.csv`: one row per fixed candidate with
  all-scenario pass/fail, worst drawdown, worst rolling drawdown, minimum
  rolling benchmark excess, maximum turnover, maximum cost stress, maximum source
  lag, and maximum ADV floor.

Promotion rule: a candidate may move from research artifact to live-design
review only if it still passes across the pre-registered cost/source-lag/ADV
stress matrix. This is a stricter check than the single baseline
`live_readiness_summary.csv` and should be archived alongside the monthly
snapshot manifest.

2026-06-20 product-data rerun evidence:

The full point-in-time input refresh was rerun locally with product-data v2:

- 106 dynamic Top50 universe snapshots;
- 5,300 dynamic universe rows;
- 247,107 price rows;
- expected delisted price gaps remained `CELG`, `DWDP`, and `UTX`.

The stress matrix was then run with `5,10,15,25` bps turnover costs, `21,42`
trading-day universe lags, `20,000,000` minimum ADV20, and `3,5` year rolling
windows. Output paths:

- `data/output/russell_top50_product_data_full_20260620_rerun`
- `data/output/russell_top50_product_data_full_stress_readiness_20260620_rerun`

Stress summary:

| Run | Stress scenarios | Passed | Worst MaxDD | Min 3Y QQQ excess CAGR | Min 5Y QQQ excess CAGR | Max turnover/year | Action |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `base_top4_cap25` | 8 | 8 | -27.81% | -9.07% | +6.46% | 3.53 | stress live-design review |
| `blend_top2_25_top4_75` | 8 | 8 | -29.00% | -7.33% | +9.51% | 3.52 | stress live-design review |
| `blend_top2_50_top4_50` | 8 | 8 | -31.49% | -5.76% | +11.65% | 3.52 | stress live-design review |

Interpretation:

- The `50% Top2 / 50% Top4` balanced profile remains the best offensive
  candidate because it has the strongest rolling QQQ excess profile across the
  fixed candidates, but its worst drawdown is above `-30%`; it should be labelled
  aggressive/offensive, not defensive.
- The `25% Top2 / 75% Top4` conservative profile stays below `-30%` drawdown and
  improves rolling QQQ excess versus pure Top4; it remains the lower-risk live
  design candidate.
- The 42-trading-day source-lag stress reduces CAGR materially, so source-lag
  monitoring should be part of the live promotion checklist even when the gate
  passes.

### Phase 3: Momentum-crash brake research only

Goal: test one narrow risk brake for the aggressive Top2 sleeve.

Pre-registered candidates:

1. `blend_top2_50_top4_50_no_brake` — current balanced candidate.
2. `blend_top2_50_top4_50_crash_brake_floor25` — Top2 sleeve falls from 50% to 25% only in panic/rebound-risk state.
3. `blend_top2_25_top4_75_no_brake` — conservative candidate.

Suggested panic/rebound-risk state:

- QQQ below 200-day SMA;
- QQQ 63-day drawdown worse than `-8%`;
- QQQ 21-day rebound positive or realized volatility above its trailing 1-year median.

Promotion requirement:

- The brake must improve drawdown or worst rolling excess without reducing long CAGR enough to fail the current fixed-blend gate.
- If it increases turnover materially, keep it research-only.

Implemented research artifact:

```bash
PYTHONPATH=src python -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_research \
  --prices data/output/russell_top50_product_data_full_20260620_rerun/input/mega_cap_leader_rotation_dynamic_top50_price_history.csv \
  --universe data/output/russell_top50_product_data_full_20260620_rerun/input/mega_cap_leader_rotation_dynamic_top50_universe_history.csv \
  --output-dir data/output/russell_top50_product_data_full_crash_brake_20260620_rerun \
  --start 2017-10-02 \
  --universe-lag-days 21 \
  --rolling-window-years 3,5 \
  --turnover-cost-bps 5 \
  --min-adv20-usd 20000000 \
  --drawdown-threshold 0.08 \
  --baseline-top2-weight 0.50 \
  --floor-top2-weight 0.25
```

The rule is deliberately narrow: it does not move the whole portfolio to cash.
It only reduces the Top2 sleeve from `50%` to `25%` when QQQ is below its
200-day SMA, QQQ 63-day drawdown is worse than `-8%`, and either QQQ 21-day
return is positive or QQQ 63-day realized volatility is above its trailing
one-year median.

2026-06-20 product-data rerun result:

| Lag | Run | CAGR | MaxDD | Sharpe | Turnover/year | Min rolling QQQ excess CAGR | Interpretation |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | --- |
| 21 | `blend_top2_50_top4_50_no_brake` | 45.06% | -30.64% | 1.27 | 3.52 | -4.83% | offensive baseline |
| 21 | `crash_brake_top2_50_floor25` | 44.35% | -29.99% | 1.26 | 3.77 | -5.20% | modest drawdown improvement, worse return/turnover |
| 21 | `blend_top2_25_top4_75_no_brake` | 42.44% | -28.19% | 1.27 | 3.52 | -6.41% | conservative fallback |
| 42 | `blend_top2_50_top4_50_no_brake` | 39.95% | -30.31% | 1.19 | 3.24 | -1.91% | source-lag stress baseline |
| 42 | `crash_brake_top2_50_floor25` | 38.99% | -30.67% | 1.17 | 3.51 | -2.35% | worse drawdown and worse return |
| 42 | `blend_top2_25_top4_75_no_brake` | 37.53% | -28.81% | 1.18 | 3.32 | -3.90% | lower-risk fallback |

Conclusion: this specific panic/rebound Top2 sleeve brake does **not** deserve
default live activation. It reduces 21-day-lag max drawdown by only about
`65` bps, worsens rolling QQQ excess, increases turnover, and fails to improve
the 42-day source-lag stress. Keep it as a research-only comparator unless a
future pre-registered variant improves drawdown without degrading rolling excess
and turnover.

### Phase 4: Global ETF stays defensive unless OOS improves

Goal: avoid turning a weak OOS result into production complexity.

Current recommendation:

- No Global ETF offensive sleeve should become default live now.
- The only candidate family worth revisiting is the conservative `85/15` or `90/10` static sleeve, but only if a stricter train-edge rule or real execution-cost data changes the OOS failure.
- Do not implement a runtime offensive overlay for Global ETF before the OOS gate passes.

### Phase 5: Offensive v2 research backlog, pre-registered before new tuning

Goal: broaden the search space without turning it into an unconstrained
parameter hunt. The next batch should add only small, interpretable hypotheses
that can be measured against the current Russell fixed-blend leader line and the
current Global ETF defensive baseline.

Web-expanded source scan, 2026-06-20:

- Moskowitz, Ooi, and Pedersen document 1-12 month time-series momentum across
  liquid asset classes. Source: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2089463
- Moskowitz and Grinblatt document a strong industry-momentum effect and show
  that industry momentum contributes substantially to individual-stock momentum
  profits. Source: https://www.aqr.com/Insights/Research/Journal-Article/Do-Industries-Explain-Momentum
- Blitz, Huij, and Martens document residual momentum, suggesting that
  market/factor-adjusted momentum can behave differently from raw total-return
  momentum. Source: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2319861
- Novy-Marx argues that earnings/fundamental momentum explains much of price
  momentum. Source: https://www.nber.org/system/files/working_papers/w20984/w20984.pdf
- Daniel and Moskowitz show momentum crash risk is partly forecastable in panic,
  high-volatility rebound states. Source: https://www.nber.org/system/files/working_papers/w20439/w20439.pdf
- Moreira and Muir show that volatility-managed portfolios can improve factor
  Sharpe ratios by reducing exposure when volatility is high. Source: https://www.nber.org/papers/w22208
- Harvey, Liu, and Zhu warn that factor discovery needs much stricter evidence
  after multiple testing. Source: https://www.nber.org/system/files/working_papers/w20592/w20592.pdf

Pre-registered candidate families:

| Family | First test | Data needed | Live-readiness stance |
| --- | --- | --- | --- |
| Sector-aware Russell concentration | Keep the existing Top2/Top4 formulas, but cap selected names per sector at `1` for research-only Top2, Top4, and fixed blends. | Existing Russell dynamic universe `sector` field; no new vendor. | Research-only until it beats fixed 25/75 and 50/50 blends after cost, source-lag, ADV, and rolling gates. |
| Residual / beta-adjusted Russell momentum | Rank by price momentum residualized versus QQQ/SPY or sector ETF proxy, with a frozen lookback. | Existing price history is enough for a first proxy; sector ETF data only if needed. | Not a live candidate until it beats raw-score blends and survives overfitting gates. |
| Fundamental / earnings momentum | Add earnings-surprise or analyst-revision confirmation only if a reliable point-in-time source is available. | Needs PIT earnings/revision data; free Yahoo-like snapshots are not enough for live promotion. | Do not implement live by default without auditable PIT data. |
| Global ETF safe offensive sleeve | Keep current defensive baseline live; only revisit 85/15 or 90/10 if real execution data or a stricter train-edge rule clears walk-forward failure. | Existing ETF price/volume data plus future execution logs. | Current conclusion stays no default live change. |

Implemented research hook:

The Russell concentration runner can now add the first sector-aware research
family without changing the default output:

```bash
PYTHONPATH=src python -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_concentration_variants \
  --prices data/output/russell_top50_product_data_full_YYYYMMDD/input/mega_cap_leader_rotation_dynamic_top50_price_history.csv \
  --universe data/output/russell_top50_product_data_full_YYYYMMDD/input/mega_cap_leader_rotation_dynamic_top50_universe_history.csv \
  --output-dir data/output/russell_top50_sector_cap_research_YYYYMMDD \
  --start 2017-10-02 \
  --universe-lag-days 21 \
  --blend-top2-weights 0.25,0.5 \
  --dynamic-drawdown-thresholds "" \
  --rolling-window-years 3,5 \
  --turnover-cost-bps 5 \
  --min-adv20-usd 20000000 \
  --include-sector-capped-variants \
  --sector-cap-values 1
```

Additional summary field:

- `Max Names Per Sector` is populated for the sector-capped rows and empty for
  existing baseline/fixed-blend/dynamic rows.

Promotion bar:

- Sector-capped rows are not live candidates by construction.
- A sector-capped blend can only move to live-design review if it improves
  worst rolling QQQ/SPY excess or drawdown versus `blend_top2_50_top4_50`
  without materially reducing long CAGR, and still passes the same stress
  matrix as the current fixed blends.
- If the result only reduces drawdown by giving up offensive edge, prefer the
  existing `blend_top2_25_top4_75` conservative profile instead of adding a new
  runtime variant.

2026-06-20 sector-cap product-data rerun:

Input was rebuilt from product-data v2 in:

- `data/output/russell_top50_product_data_full_20260620_sectorcap_rerun`
- 106 dynamic Top50 snapshots;
- 5,300 universe rows;
- 247,120 price rows;
- expected delisted gaps remained `CELG`, `DWDP`, and `UTX`.

Research command:

```bash
PYTHONPATH=src python -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_concentration_variants \
  --prices data/output/russell_top50_product_data_full_20260620_sectorcap_rerun/input/mega_cap_leader_rotation_dynamic_top50_price_history.csv \
  --universe data/output/russell_top50_product_data_full_20260620_sectorcap_rerun/input/mega_cap_leader_rotation_dynamic_top50_universe_history.csv \
  --output-dir data/output/russell_top50_sector_cap_research_20260620 \
  --start 2017-10-02 \
  --universe-lag-days 21 \
  --blend-top2-weights 0.25,0.5 \
  --dynamic-drawdown-thresholds none \
  --rolling-window-years 3,5 \
  --turnover-cost-bps 5 \
  --min-adv20-usd 20000000 \
  --include-sector-capped-variants \
  --sector-cap-values 1
```

Selected full-window rows:

| Run | CAGR | MaxDD | Sharpe | Turnover/year | Action |
| --- | ---: | ---: | ---: | ---: | --- |
| `blend_top2_25_top4_75` | 42.44% | -28.19% | 1.27 | 3.52 | keep conservative live-design candidate |
| `blend_top2_50_top4_50` | 45.06% | -30.64% | 1.27 | 3.52 | keep balanced offensive live-design candidate |
| `sector_cap1_top2_cap50` | 43.61% | -40.83% | 1.23 | 3.85 | reject |
| `sector_cap1_top4_cap25` | 25.32% | -31.60% | 1.05 | 4.05 | reject |
| `sector_cap1_blend_top2_25_top4_75` | 30.04% | -30.17% | 1.14 | 3.98 | reject |
| `sector_cap1_blend_top2_50_top4_50` | 34.67% | -33.11% | 1.19 | 3.93 | reject |

Rolling-window result:

| Run | Worst 3Y QQQ excess | Worst 3Y SPY excess | Worst 5Y QQQ excess | Worst 5Y SPY excess |
| --- | ---: | ---: | ---: | ---: |
| `blend_top2_25_top4_75` | -6.41% | +5.64% | +10.42% | +13.95% |
| `blend_top2_50_top4_50` | -4.83% | +7.22% | +12.49% | +15.27% |
| `sector_cap1_blend_top2_25_top4_75` | -15.36% | -3.31% | -3.55% | +1.64% |
| `sector_cap1_blend_top2_50_top4_50` | -14.09% | -2.04% | +2.04% | +4.82% |

Live-readiness command:

```bash
PYTHONPATH=src python -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_live_readiness \
  --summary data/output/russell_top50_sector_cap_research_20260620/concentration_variant_summary.csv \
  --rolling data/output/russell_top50_sector_cap_research_20260620/concentration_variant_rolling_summary.csv \
  --output-dir data/output/russell_top50_sector_cap_live_readiness_20260620
```

Sector-cap conclusion:

- `max_names_per_sector=1` is too blunt for this mega-cap universe. It forces
  the strategy away from the dominant winner clusters without improving drawdown.
- It reduces CAGR materially, worsens rolling 3Y QQQ/SPY excess, increases
  turnover, and in several variants also worsens drawdown.
- All `sector_cap1_*` rows are classified as `sector_capped_research` and
  `research_only` by the live-readiness gate.
- Do not run further stress/live promotion on this exact sector-cap rule. If
  sector information is revisited, use it as a diagnostic or soft penalty rather
  than a hard one-name-per-sector cap.

2026-06-20 sector soft-penalty follow-up:

After hard sector caps failed, a softer pre-registered test subtracted a fixed
score penalty for repeated sector selections. This keeps the winner cluster
available when its score edge is large, but mildly prefers cross-sector
alternatives when scores are close.

Implementation:

- Backtest ranking accepts `sector_score_penalty`, default `0.0`.
- Concentration runner emits research-only variants when
  `--include-sector-soft-penalty-variants` is enabled.
- Live-readiness gate classifies all `sector_penalty*` rows as
  `sector_soft_penalty_research` / `research_only`.

Research command:

```bash
PYTHONPATH=src python -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_concentration_variants \
  --prices data/output/russell_top50_product_data_full_20260620_sectorcap_rerun/input/mega_cap_leader_rotation_dynamic_top50_price_history.csv \
  --universe data/output/russell_top50_product_data_full_20260620_sectorcap_rerun/input/mega_cap_leader_rotation_dynamic_top50_universe_history.csv \
  --output-dir data/output/russell_top50_sector_soft_penalty_research_20260620 \
  --start 2017-10-02 \
  --universe-lag-days 21 \
  --blend-top2-weights 0.25,0.5 \
  --dynamic-drawdown-thresholds none \
  --rolling-window-years 3,5 \
  --turnover-cost-bps 5 \
  --min-adv20-usd 20000000 \
  --include-sector-soft-penalty-variants \
  --sector-score-penalty-values 0.25,0.5
```

Selected full-window rows:

| Run | CAGR | MaxDD | Sharpe | Turnover/year | Action |
| --- | ---: | ---: | ---: | ---: | --- |
| `blend_top2_25_top4_75` | 42.44% | -28.19% | 1.27 | 3.52 | keep conservative live-design candidate |
| `blend_top2_50_top4_50` | 45.06% | -30.64% | 1.27 | 3.52 | keep balanced offensive live-design candidate |
| `sector_penalty0p25_top2_cap50` | 50.62% | -38.22% | 1.27 | 3.50 | high-return research-only |
| `sector_penalty0p25_blend_top2_25_top4_75` | 38.18% | -27.91% | 1.22 | 3.67 | reject as live replacement |
| `sector_penalty0p25_blend_top2_50_top4_50` | 42.45% | -31.38% | 1.25 | 3.62 | reject as live replacement |
| `sector_penalty0p5_blend_top2_25_top4_75` | 35.03% | -27.91% | 1.17 | 3.77 | reject |
| `sector_penalty0p5_blend_top2_50_top4_50` | 38.13% | -31.38% | 1.18 | 3.72 | reject |

Rolling-window result:

| Run | Worst 3Y QQQ excess | Worst 3Y SPY excess | Worst 5Y QQQ excess | Worst 5Y SPY excess |
| --- | ---: | ---: | ---: | ---: |
| `blend_top2_25_top4_75` | -6.41% | +5.64% | +10.42% | +13.95% |
| `blend_top2_50_top4_50` | -4.83% | +7.22% | +12.49% | +15.27% |
| `sector_penalty0p25_top2_cap50` | +1.54% | +13.59% | +17.23% | +20.02% |
| `sector_penalty0p25_blend_top2_25_top4_75` | -3.64% | +8.41% | +6.09% | +12.69% |
| `sector_penalty0p25_blend_top2_50_top4_50` | -1.74% | +10.31% | +10.74% | +15.73% |
| `sector_penalty0p5_blend_top2_25_top4_75` | -7.79% | +4.26% | +4.91% | +9.93% |
| `sector_penalty0p5_blend_top2_50_top4_50` | -4.74% | +7.31% | +9.23% | +12.01% |

Soft-penalty conclusion:

- A mild `0.25` penalty improves pure Top2 return and rolling benchmark excess,
  but it does **not** solve the pure Top2 drawdown problem: max drawdown remains
  about `-38%`, so it stays research-only.
- The liveable blends are worse than the existing fixed blends. They either give
  up too much CAGR or increase drawdown/turnover without enough incremental
  rolling-excess benefit.
- This confirms that sector information is useful as a diagnostic for explaining
  winner clustering, but current sector-aware selection rules should not replace
  the approved fixed Top2/Top4 blends.
- The next higher-quality expansion should be residual/beta-adjusted momentum or
  factor/industry momentum diagnostics, not more sector cap/penalty tuning.

### Phase 6: Residual / beta-adjusted Russell momentum follow-up

Goal: test whether the Top2/Top4 winner selection can keep offensive upside
while reducing high-beta crowding and momentum-crash exposure.

Web-expanded source scan, 2026-06-20:

- Blitz, Huij, and Martens' residual momentum work supports testing momentum
  after removing common factor exposure rather than relying only on total-return
  momentum. Source: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2319861
- Frazzini and Pedersen's betting-against-beta work supports treating high beta
  exposure as a separate risk dimension, not just as return momentum. Source:
  https://www.nber.org/system/files/working_papers/w16601/w16601.pdf
- Daniel and Moskowitz show momentum crashes are partly forecastable in panic,
  high-volatility rebound states. Source:
  https://www.nber.org/system/files/working_papers/w20439/w20439.pdf
- Moreira and Muir show volatility-managed portfolios can improve factor Sharpe
  ratios by cutting exposure when volatility is high. Source:
  https://www.nber.org/papers/w22208

Implementation:

- Backtest feature snapshots now compute:
  - `beta_126_vs_benchmark`;
  - `beta_126_vs_broad_benchmark`;
  - `resid_mom_6m_vs_benchmark`;
  - `resid_mom_6m_vs_broad_benchmark`.
- Ranking accepts two disabled-by-default research knobs:
  - `residual_momentum_weight`;
  - `beta_penalty_weight`.
- Concentration runner emits research-only variants when
  `--include-residual-momentum-variants` is enabled.
- Live-readiness gate classifies all `resid*` and `beta*` rows as
  `residual_beta_research` / `research_only`.

Research command:

```bash
PYTHONPATH=src python -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_concentration_variants \
  --prices data/output/russell_top50_product_data_full_20260620_residual_rerun/input/mega_cap_leader_rotation_dynamic_top50_price_history.csv \
  --universe data/output/russell_top50_product_data_full_20260620_residual_rerun/input/mega_cap_leader_rotation_dynamic_top50_universe_history.csv \
  --output-dir data/output/russell_top50_residual_beta_research_20260620 \
  --start 2017-10-02 \
  --universe-lag-days 21 \
  --blend-top2-weights 0.25,0.5 \
  --dynamic-drawdown-thresholds none \
  --rolling-window-years 3,5 \
  --turnover-cost-bps 5 \
  --min-adv20-usd 20000000 \
  --include-residual-momentum-variants \
  --residual-momentum-weights 0.25,0.5 \
  --beta-penalty-weights 0.25
```

Selected full-window rows:

| Run | CAGR | MaxDD | Sharpe | Turnover/year | Action |
| --- | ---: | ---: | ---: | ---: | --- |
| `blend_top2_25_top4_75` | 42.44% | -28.19% | 1.27 | 3.52 | keep conservative live-design candidate |
| `blend_top2_50_top4_50` | 45.06% | -30.64% | 1.27 | 3.52 | keep balanced offensive live-design candidate |
| `resid0p25_top2_cap50` | 45.93% | -38.03% | 1.20 | 3.62 | reject |
| `resid0p5_top2_cap50` | 44.98% | -34.92% | 1.19 | 3.79 | reject |
| `beta0p25_top2_cap50` | 47.43% | -34.21% | 1.28 | 3.73 | high-return research-only |
| `resid0p25_blend_top2_25_top4_75` | 40.54% | -31.29% | 1.24 | 3.60 | reject |
| `resid0p25_blend_top2_50_top4_50` | 42.54% | -31.34% | 1.24 | 3.60 | reject |
| `resid0p5_blend_top2_25_top4_75` | 40.87% | -31.29% | 1.26 | 3.57 | reject |
| `resid0p5_blend_top2_50_top4_50` | 42.46% | -31.40% | 1.25 | 3.65 | reject |
| `beta0p25_blend_top2_25_top4_75` | 35.73% | -28.90% | 1.21 | 3.82 | reject |
| `beta0p25_blend_top2_50_top4_50` | 39.74% | -30.57% | 1.25 | 3.79 | reject |

Rolling-window result:

| Run | Worst 3Y QQQ excess | Worst 3Y SPY excess | Worst 5Y QQQ excess | Worst 5Y SPY excess |
| --- | ---: | ---: | ---: | ---: |
| `blend_top2_25_top4_75` | -6.41% | +5.64% | +10.42% | +13.95% |
| `blend_top2_50_top4_50` | -4.83% | +7.22% | +12.49% | +15.27% |
| `resid0p5_top2_cap50` | -3.48% | +8.57% | +12.67% | +14.46% |
| `beta0p25_top2_cap50` | -0.99% | +11.06% | +14.75% | +17.53% |
| `resid0p5_blend_top2_25_top4_75` | -5.49% | +6.56% | +9.50% | +13.21% |
| `resid0p5_blend_top2_50_top4_50` | -4.62% | +7.43% | +10.74% | +13.88% |
| `beta0p25_blend_top2_25_top4_75` | -6.98% | +5.07% | +7.00% | +12.19% |
| `beta0p25_blend_top2_50_top4_50` | -4.83% | +7.22% | +9.81% | +15.87% |

Residual/beta conclusion:

- Residual momentum did not improve the liveable blends. It reduced offensive
  CAGR and increased drawdown versus both fixed blends.
- A `0.25` beta penalty is useful as a diagnostic: pure Top2 keeps high CAGR,
  improves rolling QQQ/SPY excess, and reduces max drawdown from about `-38%`
  to about `-34%`. However, it still fails the live drawdown bar and remains
  research-only.
- Beta-penalized blends do not beat the current fixed 25/75 or 50/50 blends.
  The 25/75 version gives up too much CAGR, and the 50/50 version is materially
  below the existing 50/50 CAGR without enough drawdown improvement.
- Do not promote residual/beta ranking to live. The next research should keep
  the current selection formula and instead test **exposure management**:
  QQQ realized-volatility scaling, panic-rebound crash filters, or simple
  Top2-to-Top4 exposure throttling. That direction targets the remaining Top2
  drawdown problem more directly than further rank-score tuning.

### Phase 7: Exposure-management follow-up

Goal: keep the current winner-selection formula, but test whether a deterministic
exposure rule can reduce crash-state risk or improve risk-adjusted returns
without introducing daily overtrading.

Implementation:

- Concentration runner emits rebalance-date volatility-managed variants when
  `--include-volatility-managed-variants` is enabled.
- Concentration runner emits panic-rebound guard variants when
  `--include-panic-rebound-guard-variants` is enabled.
- The panic-rebound guard is deliberately narrow:
  - QQQ 126-trading-day drawdown at or below `-10%`;
  - QQQ 21-trading-day rebound at or above `+3%`;
  - QQQ 63-trading-day annualized volatility at or above `25%`;
  - stock exposure cut to `50%` on the base strategy rebalance date.
- Live-readiness gate classifies `voltarget*` and `panic*` rows as research-only.

Volatility-managed command:

```bash
PYTHONPATH=src python -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_concentration_variants \
  --prices data/output/russell_top50_product_data_full_20260620_residual_rerun/input/mega_cap_leader_rotation_dynamic_top50_price_history.csv \
  --universe data/output/russell_top50_product_data_full_20260620_residual_rerun/input/mega_cap_leader_rotation_dynamic_top50_universe_history.csv \
  --output-dir data/output/russell_top50_voltarget_research_20260620 \
  --start 2017-10-02 \
  --universe-lag-days 21 \
  --blend-top2-weights 0.25,0.5 \
  --dynamic-drawdown-thresholds none \
  --rolling-window-years 3,5 \
  --turnover-cost-bps 5 \
  --min-adv20-usd 20000000 \
  --include-volatility-managed-variants \
  --vol-target-values 0.18,0.22 \
  --vol-target-window 63 \
  --vol-target-min-stock-exposure 0.5
```

Volatility-managed result:

| Run | CAGR | MaxDD | Sharpe | Turnover/year | Action |
| --- | ---: | ---: | ---: | ---: | --- |
| `blend_top2_25_top4_75` | 42.44% | -28.19% | 1.27 | 3.52 | keep |
| `blend_top2_50_top4_50` | 45.06% | -30.64% | 1.27 | 3.52 | keep |
| `voltarget18_min50_blend_top2_25_top4_75` | 33.97% | -27.13% | 1.16 | 3.65 | reject |
| `voltarget18_min50_blend_top2_50_top4_50` | 36.07% | -30.43% | 1.16 | 3.66 | reject |
| `voltarget22_min50_blend_top2_25_top4_75` | 37.49% | -28.19% | 1.19 | 3.70 | reject |
| `voltarget22_min50_blend_top2_50_top4_50` | 39.83% | -31.38% | 1.20 | 3.71 | reject |

Volatility-managed conclusion:

- Broad QQQ volatility targeting is too blunt for this long-only mega-cap
  leader strategy.
- Even when the scaler updates only on base rebalance dates, it gives up too
  much CAGR and rolling excess for too little drawdown benefit.
- Do not promote volatility targeting as a default runtime layer.

Panic-rebound guard command:

```bash
PYTHONPATH=src python -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_concentration_variants \
  --prices data/output/russell_top50_product_data_full_20260620_residual_rerun/input/mega_cap_leader_rotation_dynamic_top50_price_history.csv \
  --universe data/output/russell_top50_product_data_full_20260620_residual_rerun/input/mega_cap_leader_rotation_dynamic_top50_universe_history.csv \
  --output-dir data/output/russell_top50_panic_guard_research_20260620 \
  --start 2017-10-02 \
  --universe-lag-days 21 \
  --blend-top2-weights 0.25,0.5 \
  --dynamic-drawdown-thresholds none \
  --rolling-window-years 3,5 \
  --turnover-cost-bps 5 \
  --min-adv20-usd 20000000 \
  --include-panic-rebound-guard-variants \
  --panic-guard-drawdown-threshold 0.10 \
  --panic-guard-rebound-threshold 0.03 \
  --panic-guard-vol-threshold 0.25 \
  --panic-guard-stock-exposure 0.50
```

Panic-rebound selected full-window rows:

| Run | CAGR | MaxDD | Sharpe | Turnover/year | Action |
| --- | ---: | ---: | ---: | ---: | --- |
| `blend_top2_25_top4_75` | 42.44% | -28.19% | 1.27 | 3.52 | current conservative live-design candidate |
| `blend_top2_50_top4_50` | 45.06% | -30.64% | 1.27 | 3.52 | current balanced offensive live-design candidate |
| `panicdd10_ret3_vol25_stock50_blend_top2_25_top4_75` | 43.67% | -28.19% | 1.30 | 3.83 | promising research-only |
| `panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50` | 46.30% | -30.64% | 1.30 | 3.83 | promising research-only |
| `panicdd10_ret3_vol25_stock50_top2_cap50` | 48.60% | -38.12% | 1.23 | 3.56 | still not liveable |

Panic-rebound rolling result:

| Run | Worst 3Y QQQ excess | Worst 3Y SPY excess | Worst 5Y QQQ excess | Worst 5Y SPY excess |
| --- | ---: | ---: | ---: | ---: |
| `blend_top2_25_top4_75` | -6.41% | +5.64% | +10.42% | +13.95% |
| `blend_top2_50_top4_50` | -4.83% | +7.22% | +12.49% | +15.27% |
| `panicdd10_ret3_vol25_stock50_blend_top2_25_top4_75` | -6.41% | +5.64% | +12.23% | +15.01% |
| `panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50` | -4.83% | +7.22% | +13.36% | +16.14% |

Panic-rebound conclusion:

- This is the first overlay in the current batch that improves both 25/75 and
  50/50 full-window CAGR and Sharpe without worsening historical max drawdown.
- It does not fix pure Top2 drawdown, so pure Top2 remains rejected.
- Because the rule was selected after a small threshold scan, it must stay
  research-only until it passes the same source-lag, stress, and preferably
  walk-forward/OOS gates as the existing fixed blends.
- Next step: add a dedicated stress-readiness run for this specific rule, then
  decide whether it graduates from `research_only` to `live_design_review`.

### Phase 8: Panic-rebound guard stress-readiness

Goal: verify that the promising panic-rebound guard is not just a single
21-trading-day-lag / 5 bps artifact.

Validation note:

- Harvey, Liu, and Zhu argue that financial factor discovery needs stricter
  hurdles after repeated testing. Source:
  https://www.nber.org/system/files/working_papers/w20592/w20592.pdf
- Bailey, Borwein, Lopez de Prado, and Zhu propose measuring the probability of
  backtest overfitting when selecting strategies from simulations. Source:
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253
- Therefore this panic guard should not be promoted merely because it improved
  the first full-window run. It needs stress and OOS evidence.

Implementation:

- Stress-readiness now can include panic-rebound guard variants in each stress
  scenario.
- Stress outputs include `metric_gate_passed_excluding_research_role`, which
  strips only the intentional `research_only_role` blocker. Drawdown, lag,
  rolling excess, and benchmark-return failures still fail the metric gate.

Stress command:

```bash
PYTHONPATH=src python -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_stress_readiness \
  --prices data/output/russell_top50_product_data_full_20260620_residual_rerun/input/mega_cap_leader_rotation_dynamic_top50_price_history.csv \
  --universe data/output/russell_top50_product_data_full_20260620_residual_rerun/input/mega_cap_leader_rotation_dynamic_top50_universe_history.csv \
  --output-dir data/output/russell_top50_panic_guard_stress_readiness_20260620 \
  --start 2017-10-02 \
  --turnover-cost-bps-values 5,10,15 \
  --universe-lag-days-values 21,42,63 \
  --min-adv20-usd-values 20000000 \
  --blend-top2-weights 0.25,0.5 \
  --candidate-runs blend_top2_25_top4_75,blend_top2_50_top4_50,panicdd10_ret3_vol25_stock50_blend_top2_25_top4_75,panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50 \
  --rolling-window-years 3,5 \
  --include-panic-rebound-guard-variants \
  --panic-guard-drawdown-threshold 0.10 \
  --panic-guard-rebound-threshold 0.03 \
  --panic-guard-vol-threshold 0.25 \
  --panic-guard-stock-exposure 0.50
```

Stress summary:

| Run | Scenarios | Passed live gate | Passed metric gate excluding research role | Worst MaxDD | Min 3Y QQQ excess | Min 5Y QQQ excess | Max turnover/year | Action |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `blend_top2_25_top4_75` | 9 | 9 | 9 | -29.86% | -6.87% | +9.97% | 3.52 | stress live-design review |
| `blend_top2_50_top4_50` | 9 | 9 | 9 | -31.07% | -5.30% | +12.07% | 3.52 | stress live-design review |
| `panicdd10_ret3_vol25_stock50_blend_top2_25_top4_75` | 9 | 0 | 9 | -29.86% | -6.87% | +11.19% | 3.83 | stress-passed research |
| `panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50` | 9 | 0 | 9 | -31.07% | -5.30% | +12.89% | 3.83 | stress-passed research |

Stress conclusion:

- The panic guard passed all 9 metric gates after stripping only the explicit
  `research_only_role` blocker.
- It preserved worst drawdown versus the corresponding fixed blend, improved
  worst 5Y QQQ excess, and kept turnover below `4x/year` even with the guard.
- The fixed blends still remain the only rows that formally pass live gate
  because panic rows are intentionally classified as research-only.
- This is now a **stress-passed research candidate**, not yet a default live
  candidate. The remaining promotion blocker is walk-forward/OOS robustness
  after acknowledging that the threshold was selected post hoc.
- Next step: add a small walk-forward/OOS diagnostic for fixed 50/50 versus
  panic-guard 50/50 and fixed 25/75 versus panic-guard 25/75. If OOS passes,
  the canonical panic guard can be promoted to `live_design_review` but should
  still ship default-off or in shadow mode first.


### Phase 9: Panic-rebound guard walk-forward/OOS diagnostic

Goal: decide whether the stress-passed panic guard can graduate from
`research_only` to `live_design_review` after accounting for post-hoc threshold
selection.

Implementation:

- Added a dedicated walk-forward/OOS diagnostic that compares each canonical
  panic-guard blend only against its matching fixed-blend baseline:
  - `panicdd10_ret3_vol25_stock50_blend_top2_25_top4_75` versus
    `blend_top2_25_top4_75`;
  - `panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50` versus
    `blend_top2_50_top4_50`.
- The diagnostic uses rolling training windows and the next calendar year as OOS.
- A panic row is counted as a promotion-quality OOS window only when the prior
  training window passes the pre-defined promotion criteria.
- The gate requires enough promotion OOS windows, positive median OOS excess,
  acceptable worst OOS excess, acceptable drawdown degradation, and minimum OOS
  win rate versus the fixed baseline.

Walk-forward/OOS command:

```bash
PYTHONPATH=src python -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_walk_forward \
  --prices data/output/russell_top50_product_data_full_20260620_residual_rerun/input/mega_cap_leader_rotation_dynamic_top50_price_history.csv \
  --universe data/output/russell_top50_product_data_full_20260620_residual_rerun/input/mega_cap_leader_rotation_dynamic_top50_universe_history.csv \
  --output-dir data/output/russell_top50_panic_guard_walk_forward_oos_20260620 \
  --start 2017-10-02 \
  --universe-lag-days 21 \
  --turnover-cost-bps 5 \
  --min-adv20-usd 20000000 \
  --blend-top2-weights 0.25,0.5 \
  --train-years 3 \
  --min-oos-windows 3 \
  --min-oos-win-rate 0.50 \
  --min-worst-oos-excess-cagr -0.03 \
  --max-oos-drawdown-degradation 0.03 \
  --panic-guard-drawdown-threshold 0.10 \
  --panic-guard-rebound-threshold 0.03 \
  --panic-guard-vol-threshold 0.25 \
  --panic-guard-stock-exposure 0.50
```

Walk-forward/OOS summary:

| Panic run | Baseline run | Total windows | Promotion OOS windows | OOS win rate | Median OOS excess CAGR | Worst OOS excess CAGR | Worst OOS drawdown delta | Gate |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `panicdd10_ret3_vol25_stock50_blend_top2_25_top4_75` | `blend_top2_25_top4_75` | 5 | 3 | 33.33% | 0.00% | 0.00% | 0.00% | fail |
| `panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50` | `blend_top2_50_top4_50` | 5 | 3 | 33.33% | 0.00% | 0.00% | 0.00% | fail |

Window-level interpretation:

- The 2022 OOS window was positive, but the prior train window did not qualify
  for promotion because the train excess CAGR was not positive.
- The 2023 OOS window was positive for both panic-guard blends and had better
  Sharpe than the matching fixed blend.
- The 2024 and 2025 OOS windows added no incremental value versus the matching
  fixed blend.
- As a result, the promotion-quality OOS win rate was only `33.33%`, below the
  `50%` threshold, and median OOS excess was not positive.

Walk-forward/OOS conclusion:

- The canonical panic-rebound guard **does not graduate** to
  `live_design_review`.
- Even though it improved full-window metrics and passed stress metrics, the
  OOS diagnostic does not show enough repeatable incremental edge versus the
  fixed blends.
- Keep all `panicdd10_ret3_vol25_stock50_*` rows classified as research-only.
- Do not default-enable this overlay in runtime. If revisited, it should first
  run as a shadow-only diagnostic with a pre-registered non-threshold or
  structural hypothesis, not as another threshold grid.


### Phase 10: Candidate-matrix overfit diagnostics

Goal: add a lightweight overfit/OOS stability review before any new Russell
variant can move from research output to live-design review. This phase does
not add a new trading rule and does not change runtime behavior.

Research basis:

- White's Reality Check frames the data-snooping problem that appears when the
  same data is reused for model selection. Source:
  https://www.ssc.wisc.edu/~bhansen/718/White2000.pdf
- Bailey, Borwein, Lopez de Prado, and Zhu propose PBO/CSCV as a way to assess
  backtest overfitting in investment simulations. Source:
  https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf
- Bailey and Lopez de Prado's Deflated Sharpe Ratio highlights selection bias
  and non-normal returns as reasons not to trust the best Sharpe from a broad
  search at face value. Source:
  https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf

Implementation:

- Added `mega_cap_leader_rotation_overfit_diagnostics`, a diagnostic-only module
  that consumes existing research artifacts instead of rerunning or changing the
  strategy.
- Inputs:
  - `concentration_variant_summary.csv`;
  - `concentration_variant_rolling_summary.csv`;
  - optional `walk_forward_oos_summary.csv`.
- Outputs:
  - `overfit_candidate_diagnostics.csv`;
  - `overfit_rank_windows.csv`;
  - `overfit_promotion_gate_summary.csv`.

The diagnostic is intentionally labelled as a **PBO proxy**, not a formal CSCV
or Deflated Sharpe implementation. The current research artifacts are aggregated
rolling windows, not the full independent return panel needed to claim strict
CSCV/PBO significance.

Command template:

```bash
PYTHONPATH=src python -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_overfit_diagnostics \
  --summary data/output/russell_top50_panic_guard_research_YYYYMMDD/concentration_variant_summary.csv \
  --rolling data/output/russell_top50_panic_guard_research_YYYYMMDD/concentration_variant_rolling_summary.csv \
  --walk-forward-summary data/output/russell_top50_panic_guard_walk_forward_oos_YYYYMMDD/walk_forward_oos_summary.csv \
  --output-dir data/output/russell_top50_overfit_diagnostics_YYYYMMDD
```

Diagnostic fields include:

- full-sample CAGR rank and rank percentile;
- whether the candidate is in the full-sample top quantile;
- rolling rank percentile, top-quantile rate, and bottom-half rate;
- positive QQQ/SPY excess rate across rolling windows;
- worst rolling benchmark excess and worst rolling drawdown;
- optional walk-forward/OOS gate status;
- `overfit_risk_label`, `overfit_risk_reason`, and `recommended_action`.

The promotion summary is machine-readable but deliberately narrow:

- `overfit_gate_passed` means the overfit/OOS diagnostic does not block the row.
- `live_promotion_gate_passed` additionally requires the row to belong to a
  promotable fixed-blend or Top4 fallback family.
- `gate_scope` is always `blocker_only_not_positive_evidence`; passing this file
  is not sufficient evidence for live promotion without the live-readiness and
  stress gates.

Promotion rule update:

- A full-sample winner that fails walk-forward/OOS remains research-only even if
  stress metrics pass.
- A full-sample top-quantile candidate with frequent rolling bottom-half ranks
  should be rejected or kept as diagnostic-only.
- Fixed blends can stay live-design candidates only if they are not merely
  full-sample winners but also show stable rolling benchmark excess and do not
  fail OOS diagnostics.
- Research variants must not be promoted from this diagnostic alone; the output
  is a blocker/risk classifier, not a positive proof of live readiness.

2026-06-20 product-data rerun result:

Inputs were rebuilt locally in:

- `data/output/russell_top50_product_data_full_20260620_overfit_rerun`
- 106 dynamic Top50 snapshots;
- 5,300 dynamic universe rows;
- 247,106 price rows;
- expected delisted gaps remained `CELG`, `DWDP`, and `UTX`.

Research artifacts:

- `data/output/russell_top50_panic_guard_research_20260620_overfit_rerun`
- `data/output/russell_top50_panic_guard_walk_forward_oos_20260620_overfit_rerun`
- `data/output/russell_top50_overfit_diagnostics_20260620_overfit_rerun`
  - `overfit_candidate_diagnostics.csv`
  - `overfit_rank_windows.csv`
  - `overfit_promotion_gate_summary.csv`

Overfit diagnostic command:

```bash
PYTHONPATH=src python -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_overfit_diagnostics \
  --summary data/output/russell_top50_panic_guard_research_20260620_overfit_rerun/concentration_variant_summary.csv \
  --rolling data/output/russell_top50_panic_guard_research_20260620_overfit_rerun/concentration_variant_rolling_summary.csv \
  --walk-forward-summary data/output/russell_top50_panic_guard_walk_forward_oos_20260620_overfit_rerun/walk_forward_oos_summary.csv \
  --output-dir data/output/russell_top50_overfit_diagnostics_20260620_overfit_rerun
```

Selected diagnostics:

| Run | Full-sample CAGR | MaxDD | Rolling bottom-half rate | Positive QQQ excess rate | Walk-forward OOS win rate | Risk | Action |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50` | 46.30% | -30.64% | 0.00% | 90.00% | 33.33% | high | keep research-only, OOS failed |
| `panicdd10_ret3_vol25_stock50_blend_top2_25_top4_75` | 43.67% | -28.19% | 100.00% | 90.00% | 33.33% | high | keep research-only, OOS failed |
| `blend_top2_50_top4_50` | 45.06% | -30.64% | 0.00% | 90.00% | n/a | low | live-candidate stability review |
| `blend_top2_25_top4_75` | 42.44% | -28.19% | 100.00% | 90.00% | n/a | low | live-candidate stability review |
| `base_top4_cap25` | 39.64% | -27.28% | 100.00% | 90.00% | n/a | low | fallback stability review |

Promotion gate summary:

| Run | Overfit gate | Live-promotion gate | Reason |
| --- | --- | --- | --- |
| `panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50` | fail | fail | `overfit_high_risk;walk_forward_gate_failed;not_promotable_candidate_family` |
| `panicdd10_ret3_vol25_stock50_blend_top2_25_top4_75` | fail | fail | `overfit_high_risk;walk_forward_gate_failed;not_promotable_candidate_family` |
| `blend_top2_50_top4_50` | pass | pass | `pass` |
| `blend_top2_25_top4_75` | pass | pass | `pass` |
| `base_top4_cap25` | pass | pass | `pass` |
| `base_top2_cap50` | pass | fail | `not_promotable_candidate_family` |

Interpretation:

- The panic-rebound blend rows remain high risk because they failed the
  walk-forward/OOS gate: promotion-quality OOS win rate was only `33.33%`, below
  the `50%` threshold, and median OOS excess was not positive.
- The `50/50` fixed blend remains the preferred aggressive live-design candidate
  on this diagnostic because it has strong full-sample return, positive QQQ
  excess in `90%` of rolling windows, and no OOS failure flag. Its drawdown still
  requires offensive/aggressive labeling.
- The `25/75` fixed blend remains the conservative live-design candidate. Its
  rolling rank is lower than high-return research variants, but it still has
  positive QQQ excess in `90%` of rolling windows and materially lower drawdown
  than the `50/50` profile.
- Pure Top2 still has the highest full-sample CAGR, but `-38%` drawdown keeps it
  research-only regardless of the overfit diagnostic.

Next robustness step:

- Do not tune more panic thresholds. If stricter statistical testing is added,
  prefer a Hansen SPA / Reality Check style evaluation over the frozen candidate
  matrix, or build a fuller return-panel-based CSCV/PBO artifact. Until then,
  this PBO-proxy diagnostic should act as a blocker, not as positive evidence
  for promotion.


### Phase 11: Return-panel Reality Check diagnostic

Goal: move beyond aggregate overfit proxies by exporting a frozen-candidate daily
return panel and running a bootstrap Reality Check diagnostic. This phase does
not change strategy rules, live manifests, or runtime behavior.

Research basis:

- White's Reality Check uses bootstrap resampling to evaluate whether the best
  observed rule could be a data-snooping artifact after many candidates were
  considered. Source: https://www.ssc.wisc.edu/~bhansen/718/White2000.pdf
- Hansen's SPA test improves on the Reality Check by being more powerful and
  less sensitive to poor/irrelevant alternatives. Source:
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=264569
- The data-snooping literature on technical trading rules commonly evaluates
  White RC, Hansen SPA, and stepwise extensions together. Source:
  https://papers.ssrn.com/sol3/Delivery.cfm/SSRN_ID1618184_code1196373.pdf?abstractid=1343896

Implementation:

- Concentration research now also writes
  `concentration_variant_daily_returns.csv`, with one row per date and run:
  - `Date`;
  - `Run`;
  - `Variant Type`;
  - `Strategy Return`;
  - `QQQ Return`;
  - `SPY Return`.
- Added `mega_cap_leader_rotation_reality_check`, which consumes that daily
  return panel and runs a circular block bootstrap Reality Check over candidate
  excess returns.
- Outputs:
  - `reality_check_candidate_summary.csv`;
  - `reality_check_global_summary.csv`.

Important scope limit:

- This is a Reality Check style return-panel diagnostic, not a full Hansen SPA,
  stepwise SPA, or CSCV/PBO implementation.
- `diagnostic_scope` is `return_panel_bootstrap_not_live_gate`; passing it is
  not sufficient for live promotion.
- A candidate that fails walk-forward/OOS remains research-only even if it is
  the best full-sample bootstrap candidate.

2026-06-20 product-data rerun artifacts:

- `data/output/russell_top50_panic_guard_research_20260620_reality_check_rerun`
  - includes `concentration_variant_daily_returns.csv`;
- `data/output/russell_top50_reality_check_20260620_rerun`;
- `data/output/russell_top50_fixed_reality_check_20260620_rerun`;
- `data/output/russell_top50_fixed_reality_check_spy_20260620_rerun`.

All commands used `1,000` bootstrap iterations, a `21` trading-day circular
block size, random seed `42`, and alpha `0.10`.

Expanded fixed-plus-panic matrix versus QQQ:

| Candidate set | Best run | Annualized mean excess | Reality Check p-value | Pass | Live interpretation |
| --- | --- | ---: | ---: | --- | --- |
| Top4, fixed blends, panic blends | `panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50` | 21.66% | 0.0060 | yes | still research-only because OOS blocker failed |

Frozen fixed live-candidate matrix versus QQQ:

| Candidate set | Best run | Annualized mean excess | Reality Check p-value | Pass | Live interpretation |
| --- | --- | ---: | ---: | --- | --- |
| Top4, `25/75`, `50/50` | `blend_top2_50_top4_50` | 20.88% | 0.0060 | yes | supports statistical review, still needs live/stress/OOS gates |

Frozen fixed live-candidate matrix versus SPY:

| Candidate set | Best run | Annualized mean excess | Reality Check p-value | Pass | Live interpretation |
| --- | --- | ---: | ---: | --- | --- |
| Top4, `25/75`, `50/50` | `blend_top2_50_top4_50` | 27.15% | 0.0030 | yes | supports statistical review, still needs live/stress/OOS gates |

Interpretation:

- The return-panel Reality Check gives the fixed `50/50` candidate additional
  support versus both QQQ and SPY inside the frozen live-candidate matrix.
- It also shows why this diagnostic cannot be a live gate by itself: the panic
  `50/50` overlay is the best full-sample bootstrap candidate in the expanded
  matrix, but it already failed the walk-forward/OOS blocker.
- Therefore the current hierarchy remains unchanged:
  - `blend_top2_50_top4_50` is the preferred aggressive/offensive candidate;
  - `blend_top2_25_top4_75` remains the lower-drawdown conservative candidate;
  - `base_top4_cap25` remains fallback;
  - panic overlay remains research-only.

Next research step:

- If we want stricter statistical validation, run the SPA-style diagnostic over
  the same daily return panel rather than tuning more strategy thresholds.
- If we want better live confidence before more statistics, expand PIT history or
  add execution/liquidity participation diagnostics; both are more useful than
  another overlay threshold grid.


### Phase 12: Execution liquidity and participation diagnostics

Goal: add a capacity check that estimates whether the fixed Russell candidates
can be executed at plausible portfolio NAV levels without exceeding a fixed
share of recent average dollar volume. This phase does not change returns,
ranking, live manifests, or runtime behavior.

Research basis:

- Average daily trading volume is commonly used as a liquidity proxy; higher
  ADTV generally makes entering or exiting positions easier, while low ADTV can
  create execution challenges. Source:
  https://www.investopedia.com/terms/a/averagedailytradingvolume.asp
- Execution and market-impact research often frames order size through a
  participation rate. Source:
  https://haas.berkeley.edu/wp-content/uploads/hiddenImpact13.pdf
- VWAP/market-impact examples commonly connect participation rate, order size,
  and execution horizon; if an order is too large for a chosen participation
  rate, it needs more time to execute. Source:
  https://www.quantrocket.com/codeload/quant-finance-lectures/quant_finance_lectures/Lecture28-Market-Impact-Models.ipynb.html

Implementation:

- Concentration research now also writes
  `concentration_variant_rebalance_trades.csv`, with one row per run, date, and
  symbol whenever target weight changes. It contains weights only, not account
  identifiers or real account balances.
- Added `mega_cap_leader_rotation_liquidity_diagnostics`, which consumes:
  - `concentration_variant_rebalance_trades.csv`;
  - price history with close and volume.
- The diagnostic computes rolling ADV dollar volume, trade notional under
  supplied hypothetical NAV values, and participation rate after spreading the
  order across a configurable number of execution days.
- Outputs:
  - `liquidity_trade_detail.csv`;
  - `liquidity_summary.csv`.

Command template:

```bash
PYTHONPATH=src python -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_liquidity_diagnostics \
  --trades data/output/russell_top50_panic_guard_research_YYYYMMDD/concentration_variant_rebalance_trades.csv \
  --prices data/output/russell_top50_product_data_full_YYYYMMDD/input/mega_cap_leader_rotation_dynamic_top50_price_history.csv \
  --output-dir data/output/russell_top50_fixed_liquidity_YYYYMMDD \
  --portfolio-nav-values 100000,500000,1000000,5000000,10000000 \
  --adv-window 20 \
  --execution-days 1 \
  --max-participation-rate 0.01 \
  --exclude-symbols BOXX,QQQ,SPY \
  --candidate-runs base_top4_cap25,blend_top2_25_top4_75,blend_top2_50_top4_50
```

2026-06-20 product-data rerun artifacts:

- `data/output/russell_top50_panic_guard_research_20260620_liquidity_rerun`
  - includes `concentration_variant_rebalance_trades.csv`;
- `data/output/russell_top50_fixed_liquidity_20260620_rerun`;
- `data/output/russell_top50_fixed_liquidity_2day_20260620_rerun`.

Assumptions:

- ADV window: `20` trading days;
- one-day execution gate: max `1%` of ADV;
- stock liquidity only; `BOXX`, `QQQ`, and `SPY` are excluded from this stock
  participation diagnostic;
- NAV values are hypothetical model sizes, not account data.

One-day execution result:

| Run | NAV | Max trade notional | Max participation | Gate | Action |
| --- | ---: | ---: | ---: | --- | --- |
| `base_top4_cap25` | $10,000,000 | $2,500,000 | 0.72% | pass | liquidity live review |
| `blend_top2_25_top4_75` | $10,000,000 | $3,125,000 | 0.90% | pass | liquidity live review |
| `blend_top2_50_top4_50` | $5,000,000 | $1,875,000 | 0.54% | pass | liquidity live review |
| `blend_top2_50_top4_50` | $10,000,000 | $3,750,000 | 1.08% | fail | reduce NAV or extend execution days |

Two-day execution result at `$10,000,000` NAV:

| Run | Max trade notional | Max participation | Gate |
| --- | ---: | ---: | --- |
| `base_top4_cap25` | $2,500,000 | 0.36% | pass |
| `blend_top2_25_top4_75` | $3,125,000 | 0.45% | pass |
| `blend_top2_50_top4_50` | $3,750,000 | 0.54% | pass |

Interpretation:

- Liquidity is not a blocker for small to mid account sizes in this product-data
  run.
- For one-day execution with a strict `1%` ADV participation cap, the aggressive
  `50/50` profile is comfortable through `$5,000,000` NAV and only slightly
  exceeds the cap at `$10,000,000`.
- Splitting the monthly rebalance over two trading days brings the `$10,000,000`
  `50/50` profile back under the `1%` cap.
- This supports keeping `50/50` as the preferred offensive candidate for
  moderate NAV, but runtime/operator docs should include a NAV/execution-days
  capacity note before live promotion.

Next live-readiness implication:

- Promotion artifacts should archive liquidity summaries for the chosen NAV
  assumption.
- Runtime should not hard-code account size into strategy logic; execution sizing
  belongs in operator/broker layer diagnostics.
- If NAV grows materially beyond the tested range, rerun this diagnostic with
  larger NAV values or multi-day execution assumptions.

### Phase 13: Integrated live-promotion review

Goal: combine the independent live-readiness, stress, overfit, liquidity, and
return-panel statistical-support artifacts into one machine-readable promotion
review. This phase does not change strategy logic, live manifests, broker
behavior, or runtime defaults.

Implementation:

- Added `mega_cap_leader_rotation_promotion_review`, an aggregation-only module.
- Inputs:
  - `concentration_variant_summary.csv`;
  - `live_readiness_summary.csv`;
  - `stress_live_readiness_summary.csv`;
  - `overfit_promotion_gate_summary.csv`;
  - `liquidity_summary.csv`;
  - optional QQQ/SPY `reality_check_candidate_summary.csv` files.
  - optional QQQ/SPY `spa_candidate_summary.csv` files.
- Output:
  - `live_promotion_review.csv`.

Required gates:

1. baseline live-readiness gate;
2. stress-readiness gate;
3. overfit/OOS blocker gate;
4. liquidity participation gate for the chosen NAV/execution-days assumption.

Reality Check and SPA-style results are treated as statistical support, not as
hard live gates. A candidate that fails any required gate stays research-only
even if it wins one or both bootstrap diagnostics.

Command template:

```bash
PYTHONPATH=src python -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_promotion_review \
  --summary data/output/russell_top50_panic_guard_research_YYYYMMDD/concentration_variant_summary.csv \
  --live-readiness data/output/russell_top50_live_readiness_YYYYMMDD/live_readiness_summary.csv \
  --stress-summary data/output/russell_top50_stress_readiness_YYYYMMDD/stress_live_readiness_summary.csv \
  --overfit-promotion data/output/russell_top50_overfit_diagnostics_YYYYMMDD/overfit_promotion_gate_summary.csv \
  --liquidity-summary data/output/russell_top50_fixed_liquidity_YYYYMMDD/liquidity_summary.csv \
  --reality-check-qqq data/output/russell_top50_fixed_reality_check_YYYYMMDD/reality_check_candidate_summary.csv \
  --reality-check-spy data/output/russell_top50_fixed_reality_check_spy_YYYYMMDD/reality_check_candidate_summary.csv \
  --spa-qqq data/output/russell_top50_fixed_spa_YYYYMMDD/spa_candidate_summary.csv \
  --spa-spy data/output/russell_top50_fixed_spa_spy_YYYYMMDD/spa_candidate_summary.csv \
  --candidate-runs base_top4_cap25,blend_top2_25_top4_75,blend_top2_50_top4_50 \
  --portfolio-nav 5000000 \
  --output-dir data/output/russell_top50_live_promotion_review_YYYYMMDD
```

2026-06-20 product-data rerun artifacts:

- `data/output/russell_top50_live_readiness_20260620_promotion_review_rerun`;
- `data/output/russell_top50_stress_readiness_20260620_promotion_review_rerun`;
- `data/output/russell_top50_live_promotion_review_20260620_rerun`;
- `data/output/russell_top50_live_promotion_review_10m_2day_20260620_rerun`.

The promotion stress matrix used `5,10,15` bps turnover costs, `21,42,63`
trading-day universe lags, `20,000,000` minimum ADV20, and `3,5` year rolling
windows. All three fixed candidates passed all `9` stress scenarios:

| Run | Stress scenarios | Passed | Worst MaxDD | Min 3Y QQQ excess CAGR | Min 5Y QQQ excess CAGR | Max turnover/year |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `base_top4_cap25` | 9 | 9 | -29.07% | -8.62% | +6.84% | 3.53 |
| `blend_top2_25_top4_75` | 9 | 9 | -29.86% | -6.87% | +9.97% | 3.52 |
| `blend_top2_50_top4_50` | 9 | 9 | -31.07% | -5.30% | +12.07% | 3.52 |

Integrated review result at `$5,000,000` NAV with one-day execution:

| Run | CAGR | MaxDD | Required gates | Statistical support | Decision |
| --- | ---: | ---: | --- | --- | --- |
| `blend_top2_50_top4_50` | 45.06% | -30.64% | pass | QQQ and SPY Reality Check | preferred aggressive live-design review |
| `blend_top2_25_top4_75` | 42.44% | -28.19% | pass | not Reality Check winner | conservative live-design review |
| `base_top4_cap25` | 39.64% | -27.28% | pass | not Reality Check winner | fallback live-design review |
| `panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50` | 46.30% | -30.64% | fail | not Reality Check winner | research-only |

After Phase 14, the same review can also consume SPA-style support files. If
both Reality Check and SPA-style diagnostics support the same QQQ/SPY winner,
`statistical_support_level` becomes
`qqq_and_spy_reality_check_and_spa`. This changes the statistical evidence
label only; it does not change `required_gates_passed`.

2026-06-20 SPA-integrated rerun:

- Product-data input:
  `data/output/russell_top50_product_data_full_20260620_spa_rerun`;
- fixed-candidate concentration output:
  `data/output/russell_top50_fixed_concentration_spa_20260620_rerun`;
- era split output:
  `data/output/russell_top50_era_split_20260620_rerun`;
- MCS-style output:
  `data/output/russell_top50_mcs_style_20260620_rerun`;
- integrated promotion review:
  `data/output/russell_top50_live_promotion_review_spa_20260620_rerun`.

Result:

| Run | Required gates | Statistical support | Era context | MCS-style context | Recommended action |
| --- | --- | --- | --- | --- | --- |
| `blend_top2_50_top4_50` | pass | `qqq_and_spy_reality_check_and_spa` | 3 of 4 eras best CAGR | only confidence-set member | preferred aggressive live-design review |
| `blend_top2_25_top4_75` | pass | not Reality Check or SPA winner | robust conservative, no best-CAGR era | excluded by best candidate | conservative live-design review |
| `base_top4_cap25` | pass | not Reality Check or SPA winner | early-era winner and fallback | excluded by best candidate | fallback live-design review |

Integrated review result at `$10,000,000` NAV with two-day execution produced
the same promotion ordering. Under the stricter one-day execution assumption,
`blend_top2_50_top4_50` should stay capped around `$5,000,000` NAV or use a
multi-day execution plan because its `$10,000,000` one-day participation was
slightly above the `1%` ADV cap.

Promotion conclusion:

- `blend_top2_50_top4_50` is the preferred offensive design if the mandate
  accepts a historical drawdown around `-31%` and the account size/execution
  plan fits the liquidity diagnostic.
- `blend_top2_25_top4_75` is the conservative design if the mandate prioritizes
  keeping drawdown below `-30%`.
- `base_top4_cap25` remains the rollback/fallback design.
- Panic-rebound overlays, pure Top2, sector-aware ranking, residual/beta
  ranking, and volatility targeting remain research-only.

### Phase 14: SPA-style statistical support diagnostic

Goal: add a stricter frozen-candidate statistical diagnostic than the first
Reality Check artifact without introducing a new production dependency or
changing any strategy rule.

Research basis:

- Hansen's Superior Predictive Ability test improves on White's Reality Check by
  using a studentized statistic and sample-dependent null distribution, making
  it less sensitive to poor or irrelevant alternatives. Source:
  https://www.tandfonline.com/doi/abs/10.1198/073500105000000063
- The `arch.bootstrap.SPA` implementation documents the same practical shape:
  block bootstrap, studentization, and lower/consistent/upper re-centering
  p-values. Source:
  https://arch.readthedocs.io/en/latest/multiple-comparison/generated/arch.bootstrap.SPA.html
- Hansen, Lunde, and Nason's Model Confidence Set is a useful future direction
  when the objective changes from “does the best candidate beat a benchmark?”
  to “which candidates remain statistically indistinguishable from the best?”
  Source: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=522382

Implementation:

- Added `mega_cap_leader_rotation_spa_check`, which consumes the same
  `concentration_variant_daily_returns.csv` artifact as the Reality Check
  diagnostic.
- It computes candidate excess returns versus a selected benchmark column,
  studentizes each candidate, and runs a circular block bootstrap with three
  re-centering p-values:
  - lower;
  - consistent;
  - upper.
- Outputs:
  - `spa_candidate_summary.csv`;
  - `spa_global_summary.csv`.

Scope limit:

- `diagnostic_scope` is
  `studentized_spa_style_bootstrap_not_live_gate`.
- The local implementation is a dependency-free SPA-style diagnostic, not a
  claim to replace a fully validated econometrics package such as
  `arch.bootstrap.SPA`.
- Passing SPA-style support is still not sufficient for live promotion. The
  required gates remain live-readiness, stress-readiness, overfit/OOS blocker,
  and liquidity participation.

Command template:

```bash
PYTHONPATH=src python -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_spa_check \
  --daily-returns data/output/russell_top50_panic_guard_research_YYYYMMDD/concentration_variant_daily_returns.csv \
  --output-dir data/output/russell_top50_fixed_spa_YYYYMMDD \
  --benchmark-column "QQQ Return" \
  --candidate-runs base_top4_cap25,blend_top2_25_top4_75,blend_top2_50_top4_50 \
  --bootstrap-iterations 1000 \
  --block-size 21 \
  --random-seed 42 \
  --alpha 0.10
```

Recommended interpretation:

- Run SPA-style diagnostics separately versus QQQ and SPY for the frozen fixed
  live-candidate matrix.
- Treat `SPA Consistent P Value <= 0.10` for `blend_top2_50_top4_50` as
  additional statistical support, not as a hard gate.
- Feed the QQQ/SPY SPA outputs into
  `mega_cap_leader_rotation_promotion_review` through `--spa-qqq` and
  `--spa-spy` after the daily-return artifact is available.
- If SPA support disagrees with the Reality Check support, keep the candidate in
  live-design review only if the required non-statistical gates still pass and
  document the conflict in the promotion artifact.
- Do not run SPA over a broad post-hoc parameter grid and then use the winner
  as a new live candidate. The diagnostic is only credible when the candidate
  matrix is frozen before the test.

2026-06-20 product-data rerun result:

Both commands used `1,000` bootstrap iterations, a `21` trading-day circular
block size, random seed `42`, and alpha `0.10`.

Frozen fixed live-candidate matrix versus QQQ:

| Candidate set | Best run | Annualized mean excess | SPA consistent p-value | Pass |
| --- | --- | ---: | ---: | --- |
| Top4, `25/75`, `50/50` | `blend_top2_50_top4_50` | 20.88% | 0.0060 | yes |

Frozen fixed live-candidate matrix versus SPY:

| Candidate set | Best run | Annualized mean excess | SPA consistent p-value | Pass |
| --- | --- | ---: | ---: | --- |
| Top4, `25/75`, `50/50` | `blend_top2_50_top4_50` | 27.15% | 0.0030 | yes |

Interpretation:

- SPA-style support agrees with the earlier Reality Check support: the frozen
  fixed-candidate winner is `blend_top2_50_top4_50` versus both QQQ and SPY.
- This strengthens the statistical-support label for the preferred offensive
  design but does not remove its drawdown label: historical max drawdown remains
  about `-31%`.
- No new live candidate is introduced by this test.

### Phase 15: Pre-registered era/regime split diagnostic

Goal: check whether the fixed live-candidate hierarchy depends on a single
market phase. This diagnostic consumes the daily return panel and slices results
into pre-registered eras. It does not add a strategy rule, optimize a parameter,
or change runtime behavior.

Research basis:

- Regime/subperiod analysis is useful because factor and momentum behavior can
  change materially across market conditions; a candidate that only works in one
  regime is weaker live evidence than one that survives multiple regimes.
  Source: https://insight.factset.com/understanding-regime-changes-for-robustness-in-backtesting
- Backtest-overfitting research emphasizes that performance should be checked
  across multiple time partitions and not only through a single full-sample
  result. Source: https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf
- Deflated Sharpe Ratio research highlights selection bias and multiple-testing
  inflation. Source: https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf

Implementation:

- Added `mega_cap_leader_rotation_era_split_diagnostics`.
- Inputs:
  - `concentration_variant_daily_returns.csv`;
  - optional comma-separated era specs in `name:start:end` format;
  - optional candidate run filter.
- Outputs:
  - `era_split_candidate_summary.csv`;
  - `era_split_promotion_summary.csv`.

Default pre-registered eras:

| Era | Date range | Intent |
| --- | --- | --- |
| `2017_2019_early_live_window` | 2017-10-02 to 2019-12-31 | early retained PIT window before the COVID/liquidity regime |
| `2020_2021_covid_liquidity` | 2020-01-01 to 2021-12-31 | COVID crash and liquidity-led rebound |
| `2022_bear_rate_shock` | 2022-01-01 to 2022-12-31 | rate-shock bear market |
| `2023_2026_ai_recovery` | 2023-01-01 to 2026-12-31 | AI/mega-cap recovery window through available data |

Command:

```bash
PYTHONPATH=src python -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_era_split_diagnostics \
  --daily-returns data/output/russell_top50_fixed_concentration_spa_20260620_rerun/concentration_variant_daily_returns.csv \
  --output-dir data/output/russell_top50_era_split_20260620_rerun \
  --candidate-runs base_top4_cap25,blend_top2_25_top4_75,blend_top2_50_top4_50 \
  --min-observations 60 \
  --min-best-era-count 2 \
  --min-positive-qqq-excess-rate 0.75 \
  --min-positive-spy-excess-rate 0.75 \
  --min-worst-qqq-excess-cagr -0.10 \
  --min-worst-spy-excess-cagr -0.03 \
  --min-worst-max-drawdown -0.35
```

2026-06-20 product-data rerun result:

| Run | Era count | Best CAGR eras | Positive QQQ excess eras | Positive SPY excess eras | Worst QQQ excess | Worst SPY excess | Worst MaxDD | Action |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `blend_top2_50_top4_50` | 4 | 3 | 3 | 3 | -8.13% | -2.53% | -30.45% | era-supported preferred offensive review |
| `blend_top2_25_top4_75` | 4 | 0 | 3 | 3 | -7.04% | -1.43% | -28.19% | era-supported conservative review |
| `base_top4_cap25` | 4 | 1 | 3 | 3 | -6.00% | -0.39% | -27.28% | era-supported fallback review |

Era detail:

| Era | Best fixed candidate | `50/50` CAGR | `50/50` QQQ excess | `50/50` SPY excess | Interpretation |
| --- | --- | ---: | ---: | ---: | --- |
| `2017_2019_early_live_window` | `base_top4_cap25` | 11.28% | -8.13% | -2.53% | caveat: offensive blend lagged both benchmarks |
| `2020_2021_covid_liquidity` | `blend_top2_50_top4_50` | 48.18% | +10.58% | +24.71% | offensive blend led |
| `2022_bear_rate_shock` | `blend_top2_50_top4_50` | 16.53% | +49.42% | +34.90% | offensive blend led in bear/rate shock |
| `2023_2026_ai_recovery` | `blend_top2_50_top4_50` | 81.69% | +46.49% | +58.74% | offensive blend led, but also had its worst era drawdown |

Interpretation:

- The era split strengthens the `50/50` preferred-offensive case because it wins
  `3` of `4` pre-registered eras and passes the broad era robustness thresholds.
- It also adds an important caveat: the early `2017-2019` window favors Top4,
  and every fixed candidate underperforms QQQ in that era.
- `25/75` remains a legitimate conservative design, not because it wins eras,
  but because it preserves much of the offensive profile with lower drawdown.
- This diagnostic should be archived as context in promotion review, not used to
  search for a new era-switching strategy. Do not add a regime switch unless a
  separately pre-registered rule passes the same live/stress/OOS/statistical
  checks.

### Phase 16: MCS-style pairwise confidence-set diagnostic

Goal: determine whether the preferred offensive `50/50` candidate is merely the
highest point estimate or whether its return advantage over `25/75` and Top4 is
statistically distinguishable enough to justify keeping it as the preferred
aggressive design.

Research basis:

- Hansen, Lunde, and Nason introduce the Model Confidence Set as a procedure
  that returns a set of models expected to contain the best model at a chosen
  confidence level. Source:
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=522382
- Their paper emphasizes that uninformative data can produce a set with many
  models, while informative data can narrow the set. Source:
  https://www.kevinsheppard.com/files/teaching/mfe/advanced-econometrics/Hansen_Lunde_Nason.pdf
- The `arch.bootstrap.MCS` documentation describes the practical interface as a
  loss matrix plus block bootstrap. Source:
  https://arch.readthedocs.io/en/stable/multiple-comparison/generated/arch.bootstrap.MCS.html

Implementation:

- Added `mega_cap_leader_rotation_mcs_diagnostics`.
- Inputs:
  - `concentration_variant_daily_returns.csv`;
  - optional candidate run filter.
- Outputs:
  - `mcs_style_candidate_summary.csv`;
  - `mcs_style_pairwise_summary.csv`;
  - `mcs_style_global_summary.csv`.

Scope limit:

- `diagnostic_scope` is
  `mcs_style_pairwise_return_confidence_set_not_live_gate`.
- This is a dependency-free pairwise confidence-set diagnostic, not a full
  implementation of the Hansen-Lunde-Nason sequential MCS elimination algorithm.
- It compares candidate daily returns directly. Since all candidates share the
  same benchmark in the daily-return panel, comparing excess returns would yield
  the same pairwise differences.
- MCS-style evidence is context in `live_promotion_review.csv`; it is not a hard
  required gate.

Command:

```bash
PYTHONPATH=src python -m us_equity_snapshot_pipelines.mega_cap_leader_rotation_mcs_diagnostics \
  --daily-returns data/output/russell_top50_fixed_concentration_spa_20260620_rerun/concentration_variant_daily_returns.csv \
  --output-dir data/output/russell_top50_mcs_style_20260620_rerun \
  --candidate-runs base_top4_cap25,blend_top2_25_top4_75,blend_top2_50_top4_50 \
  --bootstrap-iterations 1000 \
  --block-size 21 \
  --random-seed 42 \
  --alpha 0.10
```

2026-06-20 product-data rerun result:

| Best run | Compared run | Annualized advantage | Paired bootstrap p-value | Compared candidate status |
| --- | --- | ---: | ---: | --- |
| `blend_top2_50_top4_50` | `blend_top2_25_top4_75` | +2.44% | 0.0260 | excluded by best |
| `blend_top2_50_top4_50` | `base_top4_cap25` | +4.88% | 0.0260 | excluded by best |

Confidence-set result:

| Run | In MCS-style confidence set | Dominated by best | Recommended action |
| --- | --- | --- | --- |
| `blend_top2_50_top4_50` | yes | no | MCS-style best candidate |
| `blend_top2_25_top4_75` | no | yes | excluded by best |
| `base_top4_cap25` | no | yes | excluded by best |

Interpretation:

- The MCS-style diagnostic supports the `50/50` candidate as more than a noisy
  point-estimate winner inside this frozen fixed-candidate set.
- This reduces the argument for defaulting to `25/75` purely because its
  drawdown is lower. `25/75` remains the conservative override, not the preferred
  offensive default.
- The result does not remove the drawdown caveat. The preferred live label
  remains aggressive/offensive because `50/50` historical max drawdown is about
  `-31%`.
- Do not use this diagnostic to promote broader parameter-grid winners. It is
  credible only for the already frozen Top4 / 25-75 / 50-50 candidate matrix.

### Phase 17: Promotion review bundle automation

Goal: make the research evidence reproducible through one orchestration command
after the core backtest, live-readiness, stress, overfit, and liquidity artifacts
already exist. This avoids manually running separate Reality Check, SPA, era
split, MCS-style, and promotion-review commands.

Implementation:

- Added `mega_cap_leader_rotation_promotion_bundle`.
- Inputs:
  - `concentration_variant_summary.csv`;
  - `concentration_variant_daily_returns.csv`;
  - `live_readiness_summary.csv`;
  - `stress_live_readiness_summary.csv`;
  - `overfit_promotion_gate_summary.csv`;
  - `liquidity_summary.csv`.
- Outputs:
  - `reality_check_qqq/reality_check_candidate_summary.csv`;
  - `reality_check_qqq/reality_check_global_summary.csv`;
  - `reality_check_spy/reality_check_candidate_summary.csv`;
  - `reality_check_spy/reality_check_global_summary.csv`;
  - `spa_qqq/spa_candidate_summary.csv`;
  - `spa_qqq/spa_global_summary.csv`;
  - `spa_spy/spa_candidate_summary.csv`;
  - `spa_spy/spa_global_summary.csv`;
  - `era_split/era_split_candidate_summary.csv`;
  - `era_split/era_split_promotion_summary.csv`;
  - `mcs_style/mcs_style_candidate_summary.csv`;
  - `mcs_style/mcs_style_pairwise_summary.csv`;
  - `mcs_style/mcs_style_global_summary.csv`;
  - `dsr_pbo_qqq/dsr_pbo_candidate_summary.csv`;
  - `dsr_pbo_qqq/dsr_pbo_cscv_splits.csv`;
  - `dsr_pbo_qqq/dsr_pbo_global_summary.csv`;
  - `dsr_pbo_spy/dsr_pbo_candidate_summary.csv`;
  - `dsr_pbo_spy/dsr_pbo_cscv_splits.csv`;
  - `dsr_pbo_spy/dsr_pbo_global_summary.csv`;
  - `live_promotion_review.csv`;
  - `promotion_bundle_manifest.json`.

Command:

```bash
uv run useq-research-russell-top50-leader-rotation-promotion-bundle \
  --summary data/output/russell_top50_fixed_concentration_spa_20260620_rerun/concentration_variant_summary.csv \
  --daily-returns data/output/russell_top50_fixed_concentration_spa_20260620_rerun/concentration_variant_daily_returns.csv \
  --live-readiness data/output/russell_top50_live_readiness_spa_20260620_rerun/live_readiness_summary.csv \
  --stress-summary data/output/russell_top50_stress_readiness_spa_20260620_rerun/stress_live_readiness_summary.csv \
  --overfit-promotion data/output/russell_top50_overfit_spa_20260620_rerun/overfit_promotion_gate_summary.csv \
  --liquidity-summary data/output/russell_top50_fixed_liquidity_spa_20260620_rerun/liquidity_summary.csv \
  --candidate-runs base_top4_cap25,blend_top2_25_top4_75,blend_top2_50_top4_50 \
  --portfolio-nav 5000000 \
  --bootstrap-iterations 1000 \
  --block-size 21 \
  --random-seed 42 \
  --alpha 0.10 \
  --cscv-groups 8 \
  --output-dir data/output/russell_top50_promotion_bundle_20260620_rerun
```

2026-06-20 local bundle rerun:

- Output directory:
  `data/output/russell_top50_promotion_bundle_20260620_rerun`;
- Script entry point smoke output:
  `data/output/russell_top50_promotion_bundle_entrypoint_20260620_rerun`;
- Manifest output smoke:
  `data/output/russell_top50_promotion_bundle_manifest_20260620_rerun`;
- Integrated review result stayed unchanged:
  - `blend_top2_50_top4_50`: required gates pass,
    `qqq_and_spy_reality_check_and_spa`, MCS-style best candidate, preferred
    aggressive live-design review;
  - `blend_top2_25_top4_75`: required gates pass, conservative live-design
    review;
  - `base_top4_cap25`: required gates pass, fallback live-design review.

Recommended monthly research flow after this phase:

1. Generate or refresh product-data PIT input and fixed-candidate concentration
   output.
2. Generate live-readiness, stress-readiness, overfit, and liquidity artifacts.
3. Run `useq-research-russell-top50-leader-rotation-promotion-bundle` once.
4. Archive `promotion_bundle_manifest.json`, `live_promotion_review.csv`, and
   the nested statistical/context outputs as the operator/research evidence
   pack.

Monthly review integration:

- `scripts/run_monthly_report_bundle.py` now auto-discovers
  `promotion_bundle_manifest.json` under the configured artifact root, or
  accepts explicit `--promotion-bundle-manifest` paths.
- The monthly AI review input includes a research-only promotion-bundle section
  with manifest schema, candidate runs, declared artifact counts, and compact
  promotion review rows.
- Missing promotion manifests do not block the monthly snapshot review. Invalid
  manifests are surfaced as review warnings because they indicate an evidence
  archival problem.

Manifest:

- `promotion_bundle_manifest.json` has
  `manifest_type=russell_top50_promotion_bundle` and
  `artifact_schema_version=russell_top50_promotion_bundle.v1`.
- It records:
  - input paths and hashes when the inputs are local files;
  - candidate runs;
  - portfolio NAV;
  - bootstrap configuration;
  - DSR/PBO-style CSCV configuration;
  - output artifact paths and SHA256 hashes;
  - compact review rows with required-gate pass/fail, statistical support,
    promotion decision, and recommended action.

This bundle is still research-only. It does not enable or change any live
runtime profile.

### Phase 18: Shadow-live and anti-overfitting research backlog

Goal: turn the current gate-passing Russell candidate into a liveable research
package without widening the strategy search space. This phase should prioritize
auditability, decay detection, and implementation realism over finding another
higher-CAGR backtest.

Web-expanded source scan, 2026-06-20:

- Bailey and López de Prado's Deflated Sharpe Ratio work supports correcting
  Sharpe evidence for selection bias, multiple testing, sample length, and
  non-normal returns. Source:
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551
- Bailey, Borwein, López de Prado, and Zhu's PBO work supports estimating
  backtest overfitting risk through combinatorially symmetric cross-validation
  before relying on a selected backtest winner. Source:
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253
- Andrew Lo's Sharpe-ratio statistics work supports treating Sharpe estimates
  as noisy, autocorrelation-sensitive quantities rather than precise rankings.
  Source: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=377260
- Harvey, Liu, and Zhu's multiple-testing work supports using stricter evidence
  bars after many factor or strategy trials. Source:
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2249314
- McLean and Pontiff's post-publication decay evidence supports adding live
  decay monitors even when historical factor evidence is strong. Source:
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2156623
- Hou, Xue, and Zhang's anomaly replication evidence supports treating broad
  anomaly mining as a high-risk expansion path. Source:
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2961979
- Almgren and Chriss' execution-cost framing supports a separate capacity and
  implementation-shortfall layer before runtime promotion. Source:
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=53501
- Perold's implementation shortfall framing supports explicitly comparing a
  paper decision portfolio with the actually implementable portfolio before live
  promotion. Source:
  https://www.hbs.edu/faculty/Pages/item.aspx?num=2083
- CFA Institute's trade-strategy material supports using implementation
  shortfall as the standard total-cost lens for trade execution review. Source:
  https://www.cfainstitute.org/insights/professional-learning/refresher-readings/2026/trade-strategy-execution
- SEC Marketing Rule language treats performance not actually achieved by a
  portfolio as hypothetical performance, which supports keeping shadow-live
  ledger outputs clearly labelled as research-only evidence. Source:
  https://www.law.cornell.edu/cfr/text/17/275.206%284%29-1
- Daniel and Moskowitz's momentum-crash work and Moreira/Muir volatility
  management remain useful context for crash-state diagnostics, but the current
  project-specific panic/volatility overlays failed the live gate and should not
  be resurrected without new pre-registered evidence. Sources:
  https://www.nber.org/papers/w20439 and
  https://www.nber.org/papers/w22208

Recommended next deliverables:

1. Add a shadow-live ledger that records monthly selected names, target weights,
   expected trades, realized next-session prices, estimated slippage, and
   benchmark-relative forward returns. This is now available through
   `useq-build-russell-top50-shadow-live-ledger`, which consumes
   `concentration_variant_rebalance_trades.csv` and
   `concentration_variant_daily_returns.csv`, with optional long-form
   Date/Symbol/Close price history for signal-to-next-session price checks.
2. Add DSR/PBO-style reporting for the frozen Top4 / 25-75 / 50-50 matrix, using
   the already archived promotion bundle as input evidence. This is now wired
   into `useq-research-russell-top50-leader-rotation-promotion-bundle` as
   `dsr_pbo_qqq/*` and `dsr_pbo_spy/*` research artifacts. Do not apply it to a
   newly expanded candidate grid unless the grid is pre-registered first.
3. Add a capacity/implementation-shortfall stress table that varies portfolio
   NAV, participation cap, split-trade days, and slippage assumptions. Promotion
   should fail closed if the preferred candidate only works at unrealistic
   execution assumptions. This is now available through
   `useq-research-russell-top50-leader-rotation-capacity-stress`, consuming
   `shadow_live_rebalance_summary.csv` and optionally `liquidity_summary.csv`.
4. Add live-decay monitors over rolling 3/6/12-month windows versus QQQ/SPY and
   versus the backtest-implied expectation. These should be review signals, not
   automatic strategy switches.
5. Keep anomaly/factor expansion as a lower-priority research track. Any new
   factor must enter through pre-registered candidates and the promotion bundle,
   not ad-hoc ranking tweaks.

This phase should still avoid changing live manifests. The safest next runtime
step is shadow-live observability, not automatic trading.

Shadow-live ledger command template:

```bash
uv run useq-build-russell-top50-shadow-live-ledger \
  --rebalance-trades data/output/russell_top50_fixed_concentration_spa_20260620_rerun/concentration_variant_rebalance_trades.csv \
  --daily-returns data/output/russell_top50_fixed_concentration_spa_20260620_rerun/concentration_variant_daily_returns.csv \
  --candidate-runs blend_top2_50_top4_50 \
  --portfolio-nav 5000000 \
  --slippage-bps 5 \
  --forward-window-days 21 \
  --output-dir data/output/russell_top50_shadow_live_YYYYMMDD
```

Shadow-live ledger outputs:

- `shadow_live_trade_ledger.csv`;
- `shadow_live_holdings_ledger.csv`;
- `shadow_live_rebalance_summary.csv`;
- `shadow_live_ledger_manifest.json`.

Monthly review integration:

- `scripts/run_monthly_report_bundle.py` auto-discovers
  `shadow_live_ledger_manifest.json` under the artifact root, or accepts
  explicit `--shadow-live-ledger-manifest` paths.
- Missing shadow-live ledgers do not block monthly snapshot review. Invalid
  manifests are surfaced as warnings because they indicate an evidence archival
  problem.

Capacity stress command template:

```bash
uv run useq-research-russell-top50-leader-rotation-capacity-stress \
  --shadow-live-summary data/output/russell_top50_shadow_live_YYYYMMDD/shadow_live_rebalance_summary.csv \
  --liquidity-summary data/output/russell_top50_fixed_liquidity_spa_20260620_rerun/liquidity_summary.csv \
  --portfolio-nav-values 1000000,5000000,10000000,25000000 \
  --slippage-bps-values 5,10,25,50 \
  --split-trade-days-values 1,2,3 \
  --min-median-net-excess-vs-qqq 0 \
  --output-dir data/output/russell_top50_capacity_stress_YYYYMMDD
```

Capacity stress outputs:

- `capacity_stress_detail.csv`;
- `capacity_stress_summary.csv`;
- `capacity_stress_manifest.json`.

Monthly review integration:

- `scripts/run_monthly_report_bundle.py` auto-discovers
  `capacity_stress_manifest.json` under the artifact root, or accepts explicit
  `--capacity-stress-manifest` paths.
- Missing capacity stress reports do not block monthly snapshot review. Invalid
  manifests are surfaced as warnings because they indicate an evidence archival
  problem.

## Architecture recommendation

### Current architecture understanding

- `UsEquitySnapshotPipelines` owns data refresh, research runners, feature snapshots, and live-readiness artifacts.
- `UsEquityStrategies` owns runtime manifests and strategy entrypoints.
- The plugin layer should remain a signal producer; deterministic portfolio construction should live in strategy runtime/backtest code.

### Low-risk path

1. Keep all promotion research and gate outputs in `UsEquitySnapshotPipelines`.
2. Add runtime variants in `UsEquityStrategies` only after a gate-passing artifact exists.
3. Use config flags to keep the current runtime default unchanged.
4. Reuse existing snapshot/ranking contracts; only add fields if runtime cannot infer the selected variant safely.

### Not recommended

- Do not make the Russell blend a plugin. It is not an external signal; it is portfolio construction.
- Do not use a scheduler to toggle variants at runtime. Scheduler belongs to data/signal refresh, not strategy promotion.
- Do not replace Global ETF defensive baseline with the offensive sleeve based on long-window CAGR alone.
- Do not expand the candidate grid until the current small candidate set has OOS/cost/liquidity diagnostics.

## Final live-version candidate hierarchy

1. **Preferred offensive candidate:** Russell `50% Top2 / 50% Top4` balanced
   offensive blend, supported by live/stress/overfit/liquidity gates and QQQ/SPY
   Reality Check diagnostics. It must be labelled aggressive because historical
   max drawdown is around `-31%`.
2. **Conservative candidate:** Russell `25% Top2 / 75% Top4` blend, for mandates
   that prioritize keeping drawdown below `-30%`.
3. **Fallback:** Russell `Top4 cap25` baseline.
4. **Not live now:** Global ETF offensive sleeve, pure Russell Top2,
   panic-rebound overlays, dynamic drawdown switches, sector-aware ranking,
   residual/beta ranking, volatility targeting, and broad daily cash filters.

The next code change should therefore be small and reversible: harden Russell
runtime variants with named config/diagnostics, keep the current balanced default
unchanged, and run shadow comparison before any further live-promotion change.
