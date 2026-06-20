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

Minimal tests:

- rolling 3Y/5Y gate already exists; add a walk-forward summary with fixed candidates only;
- cost ladder: 5, 10, 15, 25 bps;
- liquidity/ADV stress for individual stocks, including max participation estimate by portfolio NAV;
- source-lag sensitivity: 21, 42 trading-day lag.

Do not add new strategy formulas in this phase.

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

### Phase 4: Global ETF stays defensive unless OOS improves

Goal: avoid turning a weak OOS result into production complexity.

Current recommendation:

- No Global ETF offensive sleeve should become default live now.
- The only candidate family worth revisiting is the conservative `85/15` or `90/10` static sleeve, but only if a stricter train-edge rule or real execution-cost data changes the OOS failure.
- Do not implement a runtime offensive overlay for Global ETF before the OOS gate passes.

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
