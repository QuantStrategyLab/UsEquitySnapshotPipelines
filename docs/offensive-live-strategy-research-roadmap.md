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

1. **First liveable candidate:** Russell `25% Top2 / 75% Top4` conservative blend.
2. **Aggressive candidate if mandate accepts about `-31%` historical max drawdown:** Russell `50% Top2 / 50% Top4` balanced offensive blend.
3. **Fallback:** Russell `Top4 cap25` baseline.
4. **Not live now:** Global ETF offensive sleeve, pure Russell Top2, dynamic drawdown switches, broad daily cash filters.

The next code change should therefore be small and reversible: harden Russell
runtime variants with named config/diagnostics, keep the current balanced default
unchanged, and run shadow comparison before any further live-promotion change.
