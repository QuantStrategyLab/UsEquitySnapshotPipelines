# SOXL/SOXX Chandelier Stop Research

Status: research-only as of 2026-05-10. Do not promote this overlay into the
runtime strategy without a separate evidence review and explicit approval.

Current-policy note: the 50% volatility-gate result below is historical
research evidence. The current runtime default redirects SOXL into SOXX when
SOXX 10d realized volatility is at least 55%. See
`docs/tqqq-soxl-optimization-research.md` for the broader optimization record.

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

The short full-exposure window gets lower drawdown but gives up too much CAGR. The
longer core SOXL/SOXX window degrades both CAGR and drawdown.

This supports keeping the current baseline runtime strategy unchanged and using the new
flags only for bounded research sweeps.

## Re-entry hysteresis and cooldown candidate

The July 2026 Longbridge history exposed a separate failure mode from a hard
stop: the volatility gate could redirect SOXL to SOXX on one session and restore
SOXL as soon as the metric fell just below its entry threshold.  This is a
whipsaw/re-entry-control question, not evidence that an unconditional hard stop
should be promoted.

The research backtest now supports two **research-only** controls for its
external volatility overlay:

- `--soxl-delever-reentry-hysteresis 0.05` keeps the overlay active until 10d
  annualized SOXX volatility falls 5 percentage points below the entry
  threshold.
- `--soxl-delever-reentry-cooldown-days 1` holds the overlay for one following
  trading session after each trigger.  A two-day value means two following
  sessions, not two calendar days.

Both controls are stateful but causal: each day uses only that day's indicator
and the previous overlay state.  They are not manifest parameters and cannot
change paper or live orders.  The bounded candidates are `5pp/1d` and
`7.5pp/2d`, evaluated against the current dynamic P95 (floor 50%, cap 75%)
baseline with 5 bps turnover costs, the full post-SOXL-inception history, and
separate out-of-sample windows.  A candidate must improve re-entry robustness
without giving up the existing baseline's risk-adjusted performance before any
separate strategy-parameter proposal is considered.

Run the reproducible public-data sweep:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/research_soxl_dynamic_volatility_delever_thresholds.py \
  --download --price-start 2010-03-11 --start-date 2010-09-29 \
  --output-dir /tmp/soxl_volatility_reentry_research
```

The generated `variant_summary.csv` records both raw volatility-trigger days
and extra stateful hold days, so a result cannot hide a higher exposure time
behind the same trigger count.

### First reproducible read (2026-08-24)

The first comparison used the SOXL market-regime plugin input from GitHub
Actions run `32534485450`, whose source price download ended on 2026-08-21.
It covers the post-SOXL-inception period.  The artifact did not contain BOXX or
BIL, so this run explicitly used `--constant-cash-proxy`: parking capital earns
0%, rather than silently substituting another risk asset.  Consequently the
absolute returns are conservative proxy results; the variants are compared on
the same dates and cash treatment.

| Variant | CAGR | Max drawdown | Sharpe | Calmar | Read |
| --- | ---: | ---: | ---: | ---: | --- |
| Current dynamic P95 core | 48.04% | -49.55% | 1.078 | 0.969 | Baseline |
| P95 + 5pp hysteresis / 1 day | 47.72% | -49.68% | 1.076 | 0.961 | Reject |
| P95 + 7.5pp hysteresis / 2 days | 47.99% | -48.17% | 1.080 | 0.996 | Provisional candidate |

The surviving candidate created 64 stateful hold days in addition to 142 raw
overlay-trigger days.  It also reduced 2026 YTD drawdown from -49.55% to
-48.17% in this replay, while its annualized return was marginally lower.
It needs a BOXX/BIL-backed repeat and a predeclared out-of-sample split before
any live-parameter proposal.  No strategy manifest, plugin mode, paper order,
or live order was changed by this research.

## Follow-Up Overlay Sweep

The follow-up sweep tested additional SOXL delever gates under the same
research-only rule: do not accept a candidate that reduces CAGR in either the
2024-01-31 live-full window or the 2010-09-29 core-long window.

Families tested:

- Chandelier stop with SOXX / SOXL stop symbols.
- Rolling drawdown gate.
- Short-window realized-volatility gate.
- Short-window momentum shock gate.
- Dual moving-average gate for bounded research sweeps, for example SOXL
  `10/30` with partial retention and redirect into SOXX.
- SOXL retention ratios of `0%`, `25%`, `50%`, and `75%`.
- Redirect targets of `BOXX` and `SOXX`.

At the time, the only clean common winner in the exact replay was a SOXX
volatility gate:

```bash
--soxl-delever-overlay volatility \
--soxl-delever-symbol SOXX \
--soxl-delever-window 10 \
--soxl-delever-threshold 0.50 \
--soxl-delever-retention-ratio 0.0 \
--soxl-delever-redirect-symbol SOXX
```

| Window | Variant | CAGR | Max drawdown | Delever days |
| --- | ---: | ---: | ---: | ---: |
| 2024-01-31 to 2026-05-07 live-full | Baseline archive | 130.18% | -36.10% | 0 |
| 2024-01-31 to 2026-05-07 live-full | SOXX 10d vol >= 50%, SOXL -> SOXX | 131.39% | -30.61% | 8 |
| 2010-09-29 to 2026-05-07 core-long | Baseline archive | 46.41% | -44.00% | 0 |
| 2010-09-29 to 2026-05-07 core-long | SOXX 10d vol >= 50%, SOXL -> SOXX | 47.80% | -42.31% | 20 |

Interpretation:

- This candidate satisfies the no-CAGR-sacrifice constraint in both validation
  windows.
- Redirecting into SOXX, rather than BOXX, avoids abandoning the semiconductor
  trend while removing SOXL leverage during volatility spikes.
- The trigger count is sparse enough to avoid behaving like a monthly health
  audit or permanent risk throttle.
- This is still research evidence, not a production default. Promotion should
  require a separate PR in the strategy repo and an explicit live-policy
  decision.
