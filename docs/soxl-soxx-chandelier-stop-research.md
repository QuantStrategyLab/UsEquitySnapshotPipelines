# SOXL/SOXX Chandelier Stop Research

Status: research-only as of 2026-05-10. Do not promote this overlay into the
runtime strategy without a separate evidence review and explicit approval.

## Question

Can a Chandelier-style stop reduce SOXL crash exposure without damaging the
existing SOXX trend-income strategy's long-run compounding?

## Implementation

The research backtest can now compute a stop line from the selected stop symbol,
defaulting to `SOXX`.

- If `open` / `high` / `low` are present in `price_history.csv`, the stop uses
  true range and a rolling ATR.
- If only `close` is present, it falls back to close-only true range so older
  archives remain replayable.
- Triggered days reroute only the SOXL target value into BOXX inside the
  research backtest. SOXX and income sleeves are left to the base strategy.
- The production manifest remains unchanged because the overlay is disabled
  unless `--enable-chandelier-stop` is passed.

Example:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python -m us_equity_snapshot_pipelines.soxl_soxx_trend_income_backtest \
  --prices data/output/soxl_soxx_trend_income_live_full_archive_2026-05-08/price_history.csv \
  --start 2024-01-30 \
  --turnover-cost-bps 5 \
  --enable-chandelier-stop \
  --chandelier-stop-symbol SOXX \
  --chandelier-window 22 \
  --chandelier-atr-multiple 3 \
  --output-dir data/output/soxl_soxx_chandelier_stop_research
```

## Initial Read

Preliminary close-only sweeps were not strong enough for live promotion. The
existing 2026-05-08 archives contain close-only prices, so this smoke used the
fallback true range. Parameters were `SOXX`, `window=22`, `atr_multiple=3`.

| Window | Variant | CAGR | Max drawdown | Chandelier stop days |
| --- | ---: | ---: | ---: | ---: |
| 2024-01-31 to 2026-05-07 live-full | Baseline archive | 130.18% | -36.10% | n/a |
| 2024-01-31 to 2026-05-07 live-full | Close-only Chandelier | 87.02% | -25.08% | 96 |
| 2010-09-29 to 2026-05-07 core-long | Baseline archive | 46.41% | -44.00% | n/a |
| 2010-09-29 to 2026-05-07 core-long | Close-only Chandelier | 19.89% | -49.12% | 491 |

The short live-full window gets lower drawdown but gives up too much CAGR. The
longer core SOXL/SOXX window degrades both CAGR and drawdown.

This supports keeping the current live strategy unchanged and using the new
flags only for bounded research sweeps.
