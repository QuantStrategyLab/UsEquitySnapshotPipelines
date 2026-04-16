# Crisis Response Research Roadmap

This note is for post-V1 research. The frozen V1 contract lives in
`docs/crisis-response-v1.md`; future work should add evidence around it rather
than silently changing V1 semantics.

Live promotion and shadow-plugin work must follow
`docs/crisis-response-live-promotion-spec.md`.

## Research question

Can deterministic context features classify historical market shocks into
`true_crisis`, `taco_fake_crisis`, or `no_action` with enough evidence to
improve protection without reducing post-2015 / post-2010 bull-market returns?

## Historical shock taxonomy

| Shock | Main character | Desired route | Notes |
| --- | --- | --- | --- |
| 2000-2002 dot-com bust | valuation bubble + earnings disappointment + capital tightening | `true_crisis` | Valuation and speculative-quality context matter more than a plain moving average. |
| 2007-2009 GFC | housing credit, mortgage losses, banking / shadow-banking stress | `true_crisis` | Financial-sector relative weakness and credit stress matter more than tech valuation. |
| 2011 debt-ceiling / euro stress | sovereign-credit stress and bank / credit weakness, but no confirmed 2008-style price route | `systemic_stress_watch` | Watch-only unless the price scanner also confirms. |
| 2020 COVID crash | exogenous sudden stop + massive policy response | usually `no_action` in V1 | Too fast for slow crisis guard; avoid fighting policy-rescue rebounds unless liquidity stress persists. |
| 2022 rate bear | inflation + Fed tightening + duration/valuation compression | usually `no_action` | Protecting here easily harms long-run compounding; classify separately from financial crisis. |
| 2018-2019 trade war / tariff shocks | policy/headline panic without systemic break | `taco_fake_crisis` | Good candidate for small TACO sleeve, not main-book defense. |
| 2025+ tariff / policy shocks | policy/headline panic unless paired with systemic stress | `taco_fake_crisis` or `no_action` | Keep small sleeve and require audit logs. |

## Candidate Context Features

These features should be added as deterministic context, not used as direct
trading rules until separately validated.

### Valuation / bubble context

- Nasdaq-100 trailing and forward P/E.
- Nasdaq-100 CAPE or cyclically adjusted earnings proxy, if available.
- QQQ 1-year and 2-year return acceleration.
- QQQ / SPY relative strength and concentration in mega-cap leaders.
- IPO / unprofitable growth proxies, if available through a reliable source.

Purpose: distinguish 2000-style bubble-burst risk from an ordinary correction.

### Financial-crisis context

- XLF / SPY relative strength.
- KRE / SPY relative strength.
- HYG / IEF and LQD / IEF relative strength.
- A stricter systemic threshold that distinguishes ordinary bank / credit
  weakness from 2008-style crisis stress.
- A joint financial-sector plus credit-stress confirmation so early 2008 can be
  recognized before either single-family proxy reaches a severe threshold.
- Credit spreads and funding stress if the data source is stable.
- Bank-stock drawdowns and bank news summaries.

Purpose: distinguish 2008-style financial stress from a Nasdaq valuation bear.

### Rate / inflation context

- CPI trend and surprises.
- Fed funds / expected policy path.
- 10Y Treasury yield and real yields.
- Yield-curve inversion and re-steepening.

Purpose: label 2022-style rate bears as `no_action` unless they also have
financial-system stress.

### Policy / TACO context

- Tariff / sanctions / trade-war / administration headline classification,
  including both escalation and softening windows.
- Whether the shock is reversible by policy softening.
- Whether credit / bank / liquidity stress is absent.
- Whether a non-systemic shock overlaps an explicit policy-rescue window.

Purpose: route reversible policy panic to `taco_fake_crisis` and keep it small.

### Exogenous / rescue context

- Pandemic, war, terrorist attack, natural-disaster, and other sudden-stop
  labels.
- Emergency central-bank liquidity support.
- Fiscal rescue / stimulus windows.
- Whether financial-stress proxies persist after the rescue window ends.

Purpose: keep 2020-style fast crashes from becoming slow true-crisis defense
solely because credit proxies temporarily break during the shock.

## Anti-overfitting acceptance tests

A new context feature cannot affect live routing unless it passes all checks:

1. Post-2015 and real-TQQQ post-2010 windows do not lose CAGR materially.
2. 2022 is not incorrectly converted into a broad true-crisis defense unless
   financial-stress evidence also appears.
3. 2018-2019 trade-war shocks remain TACO or no-action, not true crisis.
4. 2000 and 2008 improve or stay close to current V1 true-crisis results.
5. The feature works as a broad context input, not a single-date hindsight flag.
6. Context output remains sparse: only after a scanner opens, not daily polling.

## Suggested next experiments

1. Build the V2 historical context pack in
   `docs/crisis-context-research-v2.md` and keep it research-only.
2. Add a historical valuation context file with monthly Nasdaq-100 trailing P/E,
   forward P/E, and CAPE where available.
3. Add a financial-stress context file with XLF/SPY, KRE/SPY, HYG/IEF, LQD/IEF,
   and credit spread proxies.
4. Add rate / inflation context data for CPI, Fed funds, 10Y yield, and real
   yield so 2022-style bear markets can be separated from true crises.
5. Run the unified response backtest with each context family independently.
6. Only then test combined context. Avoid optimizing all thresholds together.
7. Keep `response_decisions.csv` and `crisis_context_features.csv` as the main
   artifacts for every experiment.

## Context Audit Effectiveness Checks

The research backtest now writes explicit audit-stability reports so that a
better equity curve is not mistaken for proof that the context audit layer is
reliable:

- `ai_audit_effectiveness.csv` measures expected historical route versus actual
  audit behavior. No-action controls such as 2020 and 2022 should have zero
  false-positive true-crisis days.
- `2011_debt_euro_stress` is treated as `systemic_stress_watch`: financial /
  credit stress is allowed in context, but the final true-crisis guard should
  stay inactive unless the price scanner confirms.
- `ai_route_period_summary.csv` and `ai_route_confusion_matrix.csv` show whether
  each historical period is being classified for the right reason.
- `ai_false_positive_true_crisis.csv` and
  `ai_false_negative_true_crisis.csv` list the exact dates that need review.
- `ai_decision_pnl_attribution.csv` separates investment impact from
  classification quality by bucket.

For 2022 specifically, the acceptance target is stable `rate_bear` /
`no_action` classification unless financial-system stress evidence also appears.
The strategy may lose money in that rate-bear year; the audit failure would be
misclassifying it as a broad `true_crisis`.

## Provisional Valuation Trial

An initial PE-enabled trial used a temporary, non-committed Nasdaq-100 monthly
trailing P/E sample from Trendonify as provisional research context. Trendonify
publishes a 1990-2026 table and describes the metric as trailing P/E based on
index level divided by aggregate EPS. The site also disclaims that its data is
for reference only, not trading. Siblis Research publishes Nasdaq-100 P/E, EPS,
forward P/E, and CAPE examples, but the complete historical database is a data
subscription. Because of those source limitations, this trial is directional
evidence only and should be repeated with an authorized point-in-time dataset.

Shared setup:

- Price input: same 1999-03-10 to 2026-04-16 synthetic TQQQ research sample
  used by V2.
- Valuation input: month-end `nasdaq_100_trailing_pe`, forward-filled only
  after each month-end date.
- Threshold: `nasdaq_100_trailing_pe >= 60`.
- V2 remains price-gated: valuation context can only affect final defense after
  the confirmed crisis-price scanner opens.

Preliminary unified-response results:

| Mode | Dotcom burst return | Dotcom burst max drawdown | Dotcom full-cycle return | Lost decade return | Full 2015-to-date |
| --- | ---: | ---: | ---: | ---: | ---: |
| V2 baseline, valuation off | -87.30% | -87.63% | -65.16% | -18.60% | unchanged |
| `price_or_external` | -77.96% | -78.54% | -39.54% | +3.09% | unchanged |
| `price_and_external` | -94.39% | -94.54% | -84.62% | -64.07% | unchanged |
| `external_only` | -91.21% | -91.44% | -75.89% | -58.88% | unchanged |

Final `true_crisis_signal` days:

| Mode | Dotcom | GFC | COVID | 2022 | Post-2015 |
| --- | ---: | ---: | ---: | ---: | ---: |
| V2 baseline, valuation off | 278 / 638 | 133 / 356 | 0 / 52 | 0 / 251 | 0 / 2838 |
| `price_or_external` | 460 / 638 | 133 / 356 | 0 / 52 | 0 / 251 | 0 / 2838 |
| `price_and_external` | 0 / 638 | 133 / 356 | 0 / 52 | 0 / 251 | 0 / 2838 |
| `external_only` | 185 / 638 | 133 / 356 | 0 / 52 | 0 / 251 | 0 / 2838 |

Interpretation:

- `price_or_external` is the best candidate mode. It lets either the price
  bubble proxy or extreme valuation context support the true-crisis route after
  price confirmation, materially improving 2000 without hurting post-2015,
  COVID, 2022, or trade-war windows in this trial.
- `price_and_external` is too strict because valuation and price-bubble windows
  do not overlap reliably on the confirmed drawdown dates.
- `external_only` is too narrow and enters late relative to price-plus-context
  evidence.
- The result should not be treated as production evidence until the valuation
  context is rebuilt from a source with clear licensing, methodology, and
  point-in-time availability.
