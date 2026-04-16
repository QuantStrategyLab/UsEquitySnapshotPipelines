# Crisis Context Research V2

This note defines the next research layer after the frozen V1 crisis-response
contract. V2 does not change live routing and does not change V1 parameters. It
builds a point-in-time context pack that can be reviewed only after a scanner
opens.

Any shadow, advisory, or live-promotion work must follow
`docs/crisis-response-live-promotion-spec.md`.

## Goal

Turn historical crash explanations into auditable context features:

- 2000-style valuation bubble: trailing price acceleration and QQQ/SPY relative
  strength, with a 126-trading-day research memory so the context can still be
  present when the confirmed drawdown scanner opens. Optional valuation and
  earnings-quality columns can be supplied by an external context table.
- 2008-style financial crisis: severe XLF/KRE drawdown, severe HYG/IEF or
  LQD/IEF credit weakness, or jointly confirmed financial-sector plus credit
  stress. Lighter single-family financial / credit context is still written for
  audit but does not by itself route to `true_crisis`.
- 2020-style exogenous shock: event context plus policy-rescue windows that
  default to `no_action` so short-lived liquidity stress is not misread as a
  slow true-crisis regime.
- 2022-style rate bear: duration/rate proxy stress that defaults to `no_action`
  unless financial-system stress appears.
- 2018-2019 and 2025+ tariff/policy shocks or softenings: policy context that
  routes to the small `taco_fake_crisis` sleeve unless valuation-bubble context
  is active.

## Research Output

Run:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_crisis_context_pack.py \
  --download \
  --event-set full \
  --price-start 1999-03-10 \
  --start 1999-03-10 \
  --output-dir data/output/crisis_context_v2
```

If Yahoo / yfinance is rate limited, use a previously saved price CSV:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_crisis_context_pack.py \
  --prices data/output/crisis_response_1999_synthetic/input/crisis_response_price_history.csv \
  --event-set full \
  --start 1999-03-10 \
  --output-dir data/output/crisis_context_v2
```

If you have a legitimate proxy for yfinance, either set:

```bash
YFINANCE_PROXY=http://user:pass@host:port
```

or pass:

```bash
--download-proxy http://user:pass@host:port
```

The output files are:

- `crisis_context_features.csv`: daily point-in-time features and suggested
  research route.
- `context_diagnostics.csv`: period-level counts for each context family and
  suggested route.
- `input/crisis_context_price_history.csv`: downloaded prices when `--download`
  is used.

The same context pack can be evaluated inside the unified crisis-response
research without changing the frozen V1 default:

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

This writes the normal unified response outputs plus
`crisis_context_features.csv`. `ai_opinions.csv` remains sparse and is written
only for confirmed crisis-price trigger days. The filename is retained for
compatibility with earlier research outputs.

The unified response run also writes audit-effectiveness reports. These are
research-only checks for stability of the context / audit layer, not new
trading rules:

- `ai_audit_effectiveness.csv`: expected historical route versus actual audit
  behavior, including false-positive true-crisis days in control windows and
  false-negative true-crisis days after the price scanner has confirmed.
- `ai_route_period_summary.csv`: period-level counts for suggested
  `true_crisis`, `taco_fake_crisis`, and `no_action` routes.
- `ai_route_confusion_matrix.csv`: expected route versus suggested route counts.
- `ai_false_positive_true_crisis.csv`: dates where a no-action or TACO control
  window still activated the true-crisis guard.
- `ai_false_negative_true_crisis.csv`: dates where an expected true-crisis
  window had a confirmed price-crisis signal but the audit route vetoed defense.
- `ai_decision_pnl_attribution.csv`: base versus unified-response returns on
  true-crisis, bubble-fragility, and normal / TACO decision buckets.

The key stability check for 2022 is that `biden_2022_bear` can show rate-bear
or no-action evidence without producing false-positive true-crisis days.

The default severe financial thresholds are intentionally stricter than the raw
context flags: XLF/KRE drawdown <= -35% or credit-relative return <= -12%.
V2 also treats simultaneous financial-sector weakness and credit weakness as a
jointly confirmed systemic-financial context. A single moderate bank or credit
flag remains audit-only. This keeps ordinary single-family noise from becoming
`true_crisis` while reducing early 2008 false negatives.

The default valuation-bubble context uses 252-trading-day QQQ acceleration and
QQQ/SPY relative strength, then persists an active bubble flag for 126 trading
days. That mirrors the research need to detect 2000-style bubble-burst risk
after the price scanner confirms a drawdown, not only on the exact day the
trailing return peak is still present.

## External Context Schema

Optional `--external-context` accepts a CSV with an `as_of` column. Columns are
forward-filled point-in-time and written with an `external_` prefix. By default,
external valuation fields are audit-only and do not change routing. To test PE
or valuation data in research, set `--external-valuation-mode` explicitly:

- `off`: default; write external fields and valuation flags, but keep routing
  based on price bubble / financial / policy / exogenous context.
- `price_or_external`: route to valuation-bubble context when either price
  bubble proxy or external valuation context is extreme.
- `price_and_external`: route to valuation-bubble context only when both price
  bubble proxy and external valuation context are active.
- `external_only`: route valuation-bubble context from external valuation
  context alone.

Suggested external columns:

- `nasdaq_100_trailing_pe`
- `nasdaq_100_forward_pe`
- `nasdaq_100_cape_proxy`
- `unprofitable_growth_proxy`
- `nasdaq_100_pct_above_200d`
- `nasdaq_100_pct_above_50d`
- `nasdaq_100_new_high_new_low_spread`
- `nasdaq_100_advance_decline_line_drawdown`
- `nasdaq_100_negative_earnings_share`
- `nasdaq_100_earnings_revision_3m`
- `nasdaq_100_margin_revision_3m`
- `cpi_yoy`
- `fed_funds_rate`
- `ten_year_yield`
- `real_yield`
- `credit_spread_baa`
- `credit_spread_hy`
- `policy_shock_score`
- `exogenous_shock_score`
- `policy_rescue_score`

Default external valuation thresholds:

- `nasdaq_100_trailing_pe >= 60`
- `nasdaq_100_forward_pe >= 45`
- `nasdaq_100_cape_proxy >= 45`
- `unprofitable_growth_proxy >= 0.35`

Default external breadth / earnings-quality fragility thresholds:

- `nasdaq_100_pct_above_200d <= 0.45`
- `nasdaq_100_pct_above_50d <= 0.35`
- `nasdaq_100_new_high_new_low_spread <= -0.10`
- `nasdaq_100_advance_decline_line_drawdown <= -0.10`
- `nasdaq_100_negative_earnings_share >= 0.25`
- `nasdaq_100_earnings_revision_3m <= -0.05`
- `nasdaq_100_margin_revision_3m <= -0.02`

A provisional trial with month-end Nasdaq-100 trailing P/E found
`price_or_external` to be the only promising initial mode: it improved the 2000
dot-com window while leaving post-2015, COVID, and 2022 final price-gated
actions unchanged. That trial used a non-committed reference dataset and should
be repeated with an authorized point-in-time valuation source before any live
decision.

Example PE-enabled context pack run:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_crisis_context_pack.py \
  --prices data/output/crisis_response_1999_synthetic_v2_context/input/crisis_response_price_history.csv \
  --external-context data/input/research/nasdaq_100_valuation_context.csv \
  --event-set full \
  --start 1999-03-10 \
  --external-valuation-mode price_or_external \
  --output-dir data/output/crisis_context_v2_valuation
```

Example unified response run with the same explicit PE mode:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_crisis_response.py \
  --prices data/output/crisis_response_1999_synthetic_v2_context/input/crisis_response_price_history.csv \
  --external-context data/input/research/nasdaq_100_valuation_context.csv \
  --event-set full \
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
  --external-valuation-mode price_or_external \
  --output-dir data/output/crisis_response_1999_synthetic_v2_valuation
```

Optional severe valuation-bubble research overlay:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_crisis_response.py \
  --prices data/output/crisis_response_1999_synthetic_v2_context/input/crisis_response_price_history.csv \
  --external-context data/input/research/nasdaq_100_valuation_context.csv \
  --event-set full \
  --start 1999-03-10 \
  --attack-symbol SYNTH_TQQQ \
  --synthetic-attack-from QQQ \
  --synthetic-attack-multiple 3 \
  --safe-symbol SHY \
  --overlay-sleeve-ratios 0.05 \
  --crisis-drawdown=-0.20 \
  --crisis-risk-multiplier 0.25 \
  --severe-crisis-risk-multiplier 0.10 \
  --severe-crisis-context valuation_bubble \
  --crisis-confirm-days 5 \
  --crisis-context-mode v2_context_pack \
  --external-valuation-mode price_or_external \
  --output-dir data/output/crisis_response_1999_synthetic_v2_valuation_severe
```

Provisional severe trials with the same non-committed P/E sample:

| Variant | Dot-com burst MDD | Dot-com full-cycle return | Lost decade return | GFC MDD | 2015-to-date return |
| --- | ---: | ---: | ---: | ---: | ---: |
| V2 context, no external valuation | -87.63% | -65.16% | -18.60% | -41.52% | +2241.24% |
| `price_or_external`, 0.25 crisis risk | -78.54% | -39.54% | +3.09% | -41.52% | +2241.24% |
| External-valuation severe 0.10 | -76.82% | -34.54% | +4.33% | -41.52% | +2241.24% |
| Valuation-bubble severe 0.10 | -73.09% | -23.98% | +21.16% | -41.52% | +2241.24% |
| Valuation-bubble severe 0.00 | -86.15% | -60.98% | -26.11% | -41.52% | +2241.24% |

The 0.10 valuation-bubble severe setting is the best provisional compromise in
this matrix. It only tightens true-crisis days routed as `valuation_bubble`, so
the GFC financial-system route and post-2015 windows remain unchanged in this
sample. A full exit at 0.00 is worse because the signal arrives late enough that
missing intermediate rebounds matters.

Optional bubble-fragility pre-crisis overlay:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_crisis_response.py \
  --prices data/output/crisis_response_1999_synthetic_v2_context/input/crisis_response_price_history.csv \
  --external-context data/input/research/nasdaq_100_valuation_context.csv \
  --event-set full \
  --start 1999-03-10 \
  --attack-symbol SYNTH_TQQQ \
  --synthetic-attack-from QQQ \
  --synthetic-attack-multiple 3 \
  --safe-symbol SHY \
  --overlay-sleeve-ratios 0.05 \
  --crisis-drawdown=-0.20 \
  --crisis-risk-multiplier 0.25 \
  --severe-crisis-risk-multiplier 0.10 \
  --severe-crisis-context valuation_bubble \
  --bubble-fragility-risk-multiplier 0.10 \
  --bubble-fragility-context external_valuation \
  --bubble-fragility-drawdown=-0.08 \
  --bubble-fragility-ma-days 100 \
  --bubble-fragility-ma-slope-days 20 \
  --bubble-fragility-confirm-days 5 \
  --crisis-confirm-days 5 \
  --crisis-context-mode v2_context_pack \
  --external-valuation-mode price_or_external \
  --output-dir data/output/crisis_response_1999_synthetic_v2_fragility
```

The fragility gate is intentionally separate from the true-crisis gate. It
requires external valuation context plus early price deterioration, then reduces
growth exposure before the slower price-crisis scanner confirms. In the same
provisional P/E sample, the external-valuation fragility signal first appears on
2000-04-18, versus 2000-10-23 for the final `true_crisis_signal`.

Provisional combined severe plus fragility trials:

| Variant | Dot-com burst MDD | Dot-com full-cycle return | Lost decade return | GFC MDD | 2015-to-date return |
| --- | ---: | ---: | ---: | ---: | ---: |
| Valuation-bubble severe 0.10, no fragility | -73.09% | -23.98% | +21.16% | -41.52% | +2241.24% |
| External-valuation fragility 0.50 | -58.76% | +16.48% | +85.64% | -41.52% | +2241.24% |
| External-valuation fragility 0.25 | -50.79% | +41.31% | +125.22% | -41.52% | +2241.24% |
| External-valuation fragility 0.10 | -50.79% | +57.69% | +151.32% | -41.52% | +2241.24% |
| External-valuation fragility 0.00 | -53.26% | +32.02% | +110.42% | -41.52% | +2241.24% |
| Valuation-bubble fragility 0.25 | -54.60% | +28.25% | +102.86% | -41.52% | +2241.24% |

External-valuation fragility 0.10 is the strongest provisional row in this
matrix. The zero-risk variant is worse, and the broader `valuation_bubble`
fragility context leaks into the 2010 live-proxy window, so it is less clean than
the external-valuation version. These results still depend on a provisional
P/E sample and must be repeated with an authorized point-in-time valuation
dataset before promotion.

Stricter breadth / quality confirmation is available through:

```bash
  --bubble-fragility-context external_breadth_or_quality
```

That context requires external valuation plus a weak breadth or earnings-quality
flag before the price-deterioration gate can reduce exposure. A mechanism check
using the same provisional P/E sample found that PE-only data correctly produces
no fragility signal, while adding a synthetic breadth / earnings-quality window
restores the expected early warning. The synthetic window is only a code-path
check, not historical evidence.

| Variant | First fragility day | Dot-com burst MDD | Dot-com full-cycle return | Lost decade return | 2015-to-date return |
| --- | ---: | ---: | ---: | ---: | ---: |
| `external_breadth_or_quality`, PE-only | none | -73.09% | -23.98% | +21.16% | +2241.24% |
| `external_breadth_or_quality`, synthetic breadth / quality | 2000-05-05 | -50.79% | +54.21% | +145.79% | +2241.24% |

## Route Priority

The V2 context pack uses conservative research labels:

1. Exogenous shock plus policy rescue -> `no_action`.
2. Exogenous shock without policy rescue -> `no_action`.
3. Policy rescue without valuation-bubble evidence -> `no_action`.
4. Valuation-bubble context -> `true_crisis`.
5. Policy or tariff shock / softening -> `taco_fake_crisis`.
6. Severe or jointly confirmed financial-system stress outside those windows
   -> `true_crisis`.
7. Rate bear without financial-system stress -> `no_action`.
8. No active context -> `no_action`.

The policy and exogenous priorities are intentional false-positive controls:
COVID-style sudden stops and tariff shock / softening windows should not become
`true_crisis` solely because short-window credit or bank proxies weaken.

These are suggested research routes only. V1 remains frozen, and no V2 context
feature should affect live allocation until it passes the roadmap acceptance
tests.
