# Crisis Response Research Roadmap

This note is for post-V1 research. The frozen V1 contract lives in
`docs/crisis-response-v1.md`; future work should add evidence around it rather
than silently changing V1 semantics.

## Research question

Can AI classify historical market shocks into `true_crisis`,
`taco_fake_crisis`, or `no_action` with enough evidence to improve protection
without reducing post-2015 / post-2010 bull-market returns?

## Historical shock taxonomy

| Shock | Main character | Desired route | Notes |
| --- | --- | --- | --- |
| 2000-2002 dot-com bust | valuation bubble + earnings disappointment + capital tightening | `true_crisis` | Valuation and speculative-quality context matter more than a plain moving average. |
| 2007-2009 GFC | housing credit, mortgage losses, banking / shadow-banking stress | `true_crisis` | Financial-sector relative weakness and credit stress matter more than tech valuation. |
| 2020 COVID crash | exogenous sudden stop + massive policy response | usually `no_action` in V1 | Too fast for slow crisis guard; avoid fighting policy-rescue rebounds unless liquidity stress persists. |
| 2022 rate bear | inflation + Fed tightening + duration/valuation compression | usually `no_action` | Protecting here easily harms long-run compounding; classify separately from financial crisis. |
| 2018-2019 trade war / tariff shocks | policy/headline panic without systemic break | `taco_fake_crisis` | Good candidate for small TACO sleeve, not main-book defense. |
| 2025+ tariff / policy shocks | policy/headline panic unless paired with systemic stress | `taco_fake_crisis` or `no_action` | Keep small sleeve and require audit logs. |

## Candidate AI context features

These features should be supplied to AI as context, not used as direct trading
rules until separately validated.

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

- Tariff / sanctions / trade-war / administration headline classification.
- Whether the shock is reversible by policy softening.
- Whether credit / bank / liquidity stress is absent.

Purpose: route reversible policy panic to `taco_fake_crisis` and keep it small.

## Anti-overfitting acceptance tests

A new AI feature cannot affect live routing unless it passes all checks:

1. Post-2015 and real-TQQQ post-2010 windows do not lose CAGR materially.
2. 2022 is not incorrectly converted into a broad true-crisis defense unless
   financial-stress evidence also appears.
3. 2018-2019 trade-war shocks remain TACO or no-action, not true crisis.
4. 2000 and 2008 improve or stay close to current V1 true-crisis results.
5. The feature works as a broad context input, not a single-date hindsight flag.
6. AI output remains sparse: only after a scanner opens, not daily polling.

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
