# Crisis Context Research V2

This note defines the next research layer after the frozen V1 crisis-response
contract. V2 does not change live routing and does not change V1 parameters. It
builds a point-in-time context pack that can be reviewed by AI only after a
scanner opens.

## Goal

Turn historical crash explanations into auditable context features:

- 2000-style valuation bubble: trailing price acceleration and QQQ/SPY relative
  strength, with a 126-trading-day research memory so the context can still be
  present when the confirmed drawdown scanner opens. Optional valuation and
  earnings-quality columns can be supplied by an external context table.
- 2008-style financial crisis: severe XLF/KRE drawdown or severe HYG/IEF and
  LQD/IEF credit weakness. Lighter financial / credit context is still written
  for audit but does not by itself route to `true_crisis`.
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
only for confirmed crisis-price trigger days.

The default severe financial thresholds are intentionally stricter than the raw
context flags: XLF/KRE drawdown <= -35% or credit-relative return <= -12%.
This keeps 2018-2019 trade-war financial noise out of `true_crisis` while
still identifying a large part of the 2008 crisis window.

The default valuation-bubble context uses 252-trading-day QQQ acceleration and
QQQ/SPY relative strength, then persists an active bubble flag for 126 trading
days. That mirrors the research need to detect 2000-style bubble-burst risk
after the price scanner confirms a drawdown, not only on the exact day the
trailing return peak is still present.

## External Context Schema

Optional `--external-context` accepts a CSV with an `as_of` column. Columns are
forward-filled point-in-time and written with an `external_` prefix. The V2 code
does not route on these columns yet; they are context for later validation.

Suggested external columns:

- `nasdaq_100_trailing_pe`
- `nasdaq_100_forward_pe`
- `nasdaq_100_cape_proxy`
- `unprofitable_growth_proxy`
- `cpi_yoy`
- `fed_funds_rate`
- `ten_year_yield`
- `real_yield`
- `credit_spread_baa`
- `credit_spread_hy`
- `policy_shock_score`
- `exogenous_shock_score`
- `policy_rescue_score`

## Route Priority

The V2 context pack uses conservative research labels:

1. Exogenous shock plus policy rescue -> `no_action`.
2. Exogenous shock without policy rescue -> `no_action`.
3. Policy rescue without valuation-bubble evidence -> `no_action`.
4. Valuation-bubble context -> `true_crisis`.
5. Policy or tariff shock / softening -> `taco_fake_crisis`.
6. Severe financial-system stress outside those windows -> `true_crisis`.
7. Rate bear without financial-system stress -> `no_action`.
8. No active context -> `no_action`.

The policy and exogenous priorities are intentional false-positive controls:
COVID-style sudden stops and tariff shock / softening windows should not become
`true_crisis` solely because short-window credit or bank proxies weaken.

These are suggested research routes only. V1 remains frozen, and no V2 context
feature should affect live allocation until it passes the roadmap acceptance
tests.
