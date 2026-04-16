# Crisis Context Research V2

This note defines the next research layer after the frozen V1 crisis-response
contract. V2 does not change live routing and does not change V1 parameters. It
builds a point-in-time context pack that can be reviewed by AI only after a
scanner opens.

## Goal

Turn historical crash explanations into auditable context features:

- 2000-style valuation bubble: trailing price acceleration and QQQ/SPY relative
  strength, with optional valuation and earnings-quality columns supplied by an
  external context table.
- 2008-style financial crisis: XLF/KRE drawdown and relative weakness, plus
  HYG/IEF and LQD/IEF credit weakness where data exists.
- 2020-style exogenous shock: event context that defaults to `no_action` unless
  separate systemic stress appears.
- 2022-style rate bear: duration/rate proxy stress that defaults to `no_action`
  unless financial-system stress appears.
- 2018-2019 and 2025+ tariff/policy shocks: policy context that routes to the
  small `taco_fake_crisis` sleeve unless true-crisis context is active.

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

## Route Priority

The V2 context pack uses conservative research labels:

1. Financial-system stress -> `true_crisis`.
2. Valuation-bubble context -> `true_crisis`.
3. Policy or tariff shock without systemic stress -> `taco_fake_crisis`.
4. Exogenous shock without systemic stress -> `no_action`.
5. Rate bear without financial-system stress -> `no_action`.
6. No active context -> `no_action`.

These are suggested research routes only. V1 remains frozen, and no V2 context
feature should affect live allocation until it passes the roadmap acceptance
tests.
