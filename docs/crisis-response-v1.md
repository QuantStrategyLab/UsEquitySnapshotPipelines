# Crisis Response V1

This is the frozen V1 research contract for the unified event-response plugin.
It combines the former TACO fake-crisis sleeve and true-crisis guard into one
router so future research can add signals without changing the validated
baseline semantics.

## Default state

- Production default: disabled.
- Initial rollout mode: paper / shadow logging.
- Live impact, if enabled later: bounded by strategy configuration; do not let
  AI directly place orders or bypass max-impact limits.

## Inputs

- Price stress scanner for TACO candidates.
- Confirmed crisis-price scanner for true-crisis candidates.
- Sparse AI opinions only after a scanner opens; no daily AI polling.
- Trade-war / tariff / policy event calendar for TACO research.

## Routing

Priority is intentionally conservative:

1. `true_crisis`: bubble-burst or financial-crisis risk. Activate crisis guard
   and suppress new TACO entries while active.
2. `taco_fake_crisis`: policy / tariff / trade-war shock without active
   true-crisis guard. Allow small TACO sleeve.
3. `no_action`: unclear, non-systemic bear market, rate bear, or AI conflict.

## Frozen V1 parameters

- TACO sleeve: start with `0.05` account sleeve in research / paper.
- Crisis drawdown: `QQQ` drawdown from trailing high <= `-0.20`.
- Crisis confirmation: `5` trading days.
- Crisis risk multiplier: `0.25`; do not full-clear risk exposure in V1.
- Bubble proxy: `QQQ` 252-trading-day return >= `0.75`, remembered for 126
  trading days.
- Financial proxy: `XLF` drawdown and relative weakness vs `SPY`.
- Safe asset in long history tests: `SHY`; live safe asset can be mapped by the
  execution platform, for example BOXX/SGOV/CASH, but that is not part of this
  V1 research contract.

## Backtest commands

Long synthetic TQQQ stress sample:

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

Real TQQQ live-history sample:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_crisis_response.py \
  --download \
  --event-set full \
  --price-start 2010-02-11 \
  --start 2010-02-11 \
  --attack-symbol TQQQ \
  --synthetic-attack-multiple 0 \
  --safe-symbol SHY \
  --overlay-sleeve-ratios 0.05 \
  --crisis-drawdown=-0.20 \
  --crisis-risk-multiplier 0.25 \
  --crisis-confirm-days 5 \
  --output-dir data/output/crisis_response_real_tqqq_2010
```

## Required audit outputs

- `response_decisions.csv`: main route audit; each candidate must land in
  `true_crisis`, `taco_fake_crisis`, or `no_action`.
- `ai_opinions.csv`: sparse AI/rubric opinions only on confirmed crisis-price
  trigger days.
- `true_crisis_signal.csv`: final crisis-guard active series.
- `taco_event_calendar.csv`: events allowed into the TACO sleeve.

## Research guardrails

Future signals can be added only as candidate context features. They should not
change V1 routing unless a backtest proves all of the following:

1. No material degradation in post-2015 / post-2010 bull-market windows.
2. No forced defense during 2022-style rate-bear regimes unless the new evidence
   clearly shows systemic crisis risk.
3. Clear auditability: every route must explain why it was true crisis, fake
   crisis, or no action.
4. Parameter robustness: small changes should not flip the conclusion.
