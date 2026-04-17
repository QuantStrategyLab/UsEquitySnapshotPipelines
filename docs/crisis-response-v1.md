# Crisis Response V1

This is the frozen V1 research contract for deterministic event-response
research. The historical research backtest can still compare TACO fake-crisis
entries and true-crisis defense in one report so the validated baseline remains
auditable.

The production-facing plugin contract is now split:

- `crisis_response_shadow` is defense-only for TQQQ-style black-swan risk. It
  may emit `true_crisis` / `defend` or `no_action` / `watch_only`; it must not
  emit TACO routes or sleeves.
- `taco_rebound_shadow` is the separate event-rebound budget signal for
  left-side rebound strategies such as `dynamic_mega_leveraged_pullback`.

## Default state

- Production default: disabled.
- Initial rollout mode: paper / shadow logging.
- Live impact, if enabled later: bounded by strategy configuration; do not let
  any external model or unreviewed logic place orders or bypass max-impact
  limits.
- Crisis plugin live/shadow destination: cash or money-market / Treasury-bill
  parking sleeve, selected by the downstream platform.

## Inputs

- Price stress scanner for historical TACO research and the separate
  `taco_rebound_shadow` plugin.
- Confirmed crisis-price scanner for true-crisis candidates.
- Sparse rubric/context opinions only after a scanner opens; no daily model
  polling.
- Trade-war / tariff / policy event calendar for TACO research only. It must not
  cause `crisis_response_shadow` to buy rebounds.

## Routing

The historical research router uses this conservative priority:

1. `true_crisis`: bubble-burst or financial-crisis risk. Activate crisis guard
   and suppress new TACO entries while active.
2. `taco_fake_crisis`: policy / tariff / trade-war shock without active
   true-crisis guard. Allow small TACO sleeve.
3. `no_action`: unclear, non-systemic bear market, rate bear, or context
   conflict.

The production-facing crisis shadow plugin narrows this to:

1. `true_crisis`: suggest defense only.
2. `no_action`: keep the base strategy unchanged; policy/TACO context can be
   logged but not traded by this plugin.

## Frozen V1 parameters

- TACO sleeve: start with `0.05` account sleeve in historical research or the
  separate `taco_rebound_shadow` paper/shadow plugin. It is not part of
  `crisis_response_shadow`.
- Crisis drawdown: `QQQ` drawdown from trailing high <= `-0.20`.
- Crisis confirmation: `5` trading days.
- Crisis risk multiplier: `0.25`; do not full-clear risk exposure in V1.
- Defense-only shadow/live crisis plugin default: `0.0` risk multiplier when a
  promoted true-crisis route is explicitly enabled downstream.
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

- `response_decisions.csv`: historical research route audit; each candidate
  must land in `true_crisis`, `taco_fake_crisis`, or `no_action`.
- `context_opinions.csv`: sparse rubric/context opinions only on
  confirmed crisis-price trigger days.
- `true_crisis_signal.csv`: final crisis-guard active series.
- `taco_event_calendar.csv`: historical TACO research events. Production-facing
  TACO logging belongs to `taco_rebound_shadow`.

## Research guardrails

Future signals can be added only as candidate context features. They should not
change V1 routing unless a backtest proves all of the following:

1. No material degradation in post-2015 / post-2010 bull-market windows.
2. No forced defense during 2022-style rate-bear regimes unless the new evidence
   clearly shows systemic crisis risk.
3. Clear auditability: every route must explain why it was true crisis, fake
   crisis, or no action.
4. Parameter robustness: small changes should not flip the conclusion.
