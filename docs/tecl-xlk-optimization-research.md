# TECL / XLK Optimization Research

Research date: 2026-06-28

Status: **research-only (archived)**. Do not promote `tecl_xlk_trend_income` to
`runtime_enabled`. The profile failed the promotion gate versus live **TQQQ** and
**SOXL** proxies on overlapping windows; code, backtest entrypoints, and artifacts
are retained for manual replay only.

## Final disposition (2026-06-28)

| Check | TECL/XLK (research) | TQQQ live proxy | SOXL live proxy |
| --- | ---: | ---: | ---: |
| 2024+ CAGR | 24.8% | — | 172.0% |
| 2024+ max drawdown | -46.0% | — | -34.2% |
| Synthetic 25y full CAGR | 17.0% | — | — |
| Synthetic 25y max drawdown | -58.9% | — | — |

**Decision:** keep `catalog.status=research_enabled`; do not add to
`get_runtime_enabled_profiles()`. Income layer remains `status=research` with
`evidence_status=rejected_vs_live_leveraged`.

Primary artifacts:

- `data/output/tecl_xlk_trend_income_research_20260628/`
- `data/output/tecl_xlk_synthetic_long_history_20260628/`
- `data/output/tecl_xlk_stress_comparison_20260628.csv`
- `docs/tecl-xlk-optimization-research.md`

## Goal

Evaluate whether a SOXL/SOXX-like tiered blend can be transplanted to
technology leverage (`TECL` / `XLK`) with BOXX parking, XLK-based dynamic
volatility delever, and TECL-specific retention profiles
(`tecl_step_rebound_0.25_0.50`, `tecl_step_softzero_rebound_0.25_0.50`).

## Implementation

- Strategy: `us_equity_strategies.strategies.tecl_xlk_trend_income`
- Backtest: `us_equity_snapshot_pipelines.tecl_xlk_trend_income_backtest`
- Managed symbols: `TECL`, `XLK`, `BOXX`, `SCHD`, `DGRO`, `SGOV`, `SPYI`, `QQQI`
- Trend gate source: `XLK`
- Default research backtest window: `2024-01-30` through latest available date
  (BOXX inception constraint; see limitations below)
- Income layer disabled for the baseline smoke below
- Turnover cost: 5 bps

Example:

```bash
cd UsEquitySnapshotPipelines
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python -m us_equity_snapshot_pipelines.tecl_xlk_trend_income_backtest \
  --download \
  --output-dir data/output/tecl_xlk_trend_income_research_20260628 \
  --disable-income-layer
```

Artifact directory: `data/output/tecl_xlk_trend_income_research_20260628`

Outputs: `summary.csv`, `signal_history.csv`, `weights_history.csv`,
`trades.csv`, `portfolio_returns.csv`, `price_history.csv`, `backtest_config.json`

## Baseline Parameters (SOXL-like seed)

Copied from the current SOXL/SOXX research defaults without changing production
TQQQ or SOXL runtime configs:

- `trend_ma_window`: 140
- `trend_entry_buffer` / `trend_mid_buffer` / `trend_exit_buffer`: 0.08 / 0.06 / 0.02
- `blend_gate_tecl_weight` / `blend_gate_active_xlk_weight`: tiered blend (see strategy module)
- XLK dynamic volatility delever: rolling 252-day p95 on 10-day annualized realized vol, floor/cap 50%-75%
- Retention policy default: `tecl_step_rebound_0.25_0.50` (plugin-emitted profile)

## Promotion Gate

Per the research plan, a candidate must not simultaneously sacrifice CAGR and
max drawdown versus a comparable baseline across the full sample and key stress
windows. Compare against `soxl_soxx_trend_income` on the same calendar windows
with income layer disabled and identical turnover assumptions.

## Results (2026-06-28)

All numbers below use yfinance close prices, 5 bps turnover, income layer off.
SOXL baseline uses the same download window and flags.

| Window | Strategy | CAGR | Max drawdown | Turnover/yr | Notes |
| --- | --- | ---: | ---: | ---: | --- |
| 2024-01-31 to 2026-06-25 | TECL/XLK | 24.77% | -46.04% | 10.55 | 4 TECL delever stops |
| 2024-01-31 to 2026-06-25 | SOXL/SOXX | 172.03% | -34.24% | 11.20 | 20 SOXL delever stops |
| 2023-07-26 to 2026-06-25 (BOXX-available) | TECL/XLK | 17.05% | -46.04% | 10.53 | |
| 2023-07-26 to 2026-06-25 | SOXL/SOXX | 114.32% | -35.98% | 11.40 | |
| 2025-06-03 to 2026-06-25 (recent ~1y) | TECL/XLK | 113.20% | -21.70% | 15.81 | |
| 2025-06-03 to 2026-06-25 | SOXL/SOXX | 513.98% | -34.24% | 16.47 | |
| 2026-03-03 to 2026-06-25 (recent ~3m) | TECL/XLK | 229.26% | -21.51% | 18.10 | |
| 2026-03-03 to 2026-06-25 | SOXL/SOXX | 2197.17% | -22.16% | 36.53 | |

### Gate verdict

**Fail promotion gate.** On every tested window TECL/XLK underperforms SOXL/SOXX
on CAGR. Max drawdown is worse on the full BOXX-available and 2024+ windows;
only the recent 3-month slice shows a marginally better drawdown (-21.51% vs
-22.16%) while still losing badly on CAGR.

Recommendation: keep `tecl_xlk_trend_income` at `research_enabled`. Continue
parameter sweeps (`trend_ma_window`, entry/mid/exit buffers, TECL/XLK blend
weights, XLK vol window/percentile/floor/cap, retention ratio/redirect) before
any runtime promotion.

## Limitations

- **BOXX inception**: BOXX history begins around late 2022. Backtests that park
  in BOXX cannot start before BOXX (and managed income symbols) are available
  without a BIL or cash proxy backfill. Long pre-2023 TECL/XLK stress tests
  require a separate synthetic replay (XLK-based 3x TECL leg explicitly labeled).
- **2020 COVID / 2022 rate-bear windows**: not replayed in this artifact
  because the default download starts 2023-01-01. Use committed long-history
  inputs or `scripts/research_volatility_delever_retention_policies.py` style
  synthetic inputs for those eras.
- **Sector mismatch**: TECL is concentrated technology; SOXL is semiconductors.
  SOXL-like parameters are a seed only, not a transferable optimum.
- **Plugin evidence**: AI/OSINT/notification text remains manual-review only and
  must not auto-increase retention.

## Tests

```bash
# UsEquityStrategies
cd UsEquityStrategies
PYTHONPATH=src python -m pytest tests/test_volatility_delever_retention.py -q

# QuantStrategyPlugins
cd QuantStrategyPlugins
PYTHONPATH=src python -m pytest tests/test_market_regime_control_plugin.py tests/test_volatility_delever_price_rebound.py -q

# UsEquitySnapshotPipelines
cd UsEquitySnapshotPipelines
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python -m pytest tests/test_tecl_xlk_trend_income_backtest.py -q
```

All of the above passed on 2026-06-28.

## Next Steps

1. Sweep `trend_ma_window` and tiered blend weights on the BOXX-available window.
2. Test XLK vol delever redirect to `XLK` instead of `BOXX` (mirrors SOXL→SOXX).
3. Build XLK-synthetic TECL long-history artifact for 25y-style gate checks.
4. Re-run promotion review only if a candidate beats SOXL on CAGR without worse
   drawdown on full sample and COVID/rate-bear/post-2022/recent windows.

## Follow-Up Research (2026-06-28)

### New tooling

- `tecl_xlk_trend_income_research_inputs.py`: BIL→BOXX parking proxy and optional
  XLK→TECL synthetic leg (research-only, explicitly labeled in manifest).
- `scripts/build_tecl_xlk_long_history_inputs.py`: downloads from 2018 with BIL
  proxy for pre-BOXX history.
- `scripts/sweep_tecl_xlk_trend_income.py`: bounded 110-variant core sweep over
  `trend_ma_window`, trend buffers, TECL weight, vol percentile, redirect target.

Commands:

```bash
cd UsEquitySnapshotPipelines
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_tecl_xlk_long_history_inputs.py \
  --output-dir data/output/tecl_xlk_long_history_inputs_20260628

PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/sweep_tecl_xlk_trend_income.py \
  --prices data/output/tecl_xlk_trend_income_research_20260628/price_history.csv \
  --output-dir data/output/tecl_xlk_trend_income_sweep_20260628 \
  --start 2024-01-30
```

Artifacts:

- `data/output/tecl_xlk_long_history_inputs_20260628/` (BIL-filled BOXX, real TECL/XLK)
- `data/output/tecl_xlk_trend_income_sweep_20260628/tecl_core_sweep_ranked.csv`
- `data/output/tecl_xlk_long_history_window_smoke_20260628.csv`

### 2024+ bounded sweep (110 variants)

Baseline: CAGR 24.77%, max drawdown -46.04%, 4 TECL delever stops.

| Rank | Variant | CAGR | Max DD | Dual gate vs baseline |
| --- | --- | ---: | ---: | --- |
| 1 | `vol_off` | 24.80% | -46.04% | pass (marginal CAGR +0.04 pp) |
| 2 | `manifest_default` | 24.77% | -46.04% | pass |
| 3+ | `ma*_tw0.75_xlk_p90/p95` | 25.28% | -48.71% | fail (higher CAGR, worse DD) |

`dual_gate_pass_count = 2` out of 110. No parameterized variant improves both CAGR
and drawdown versus the manifest default on the 2024+ window. Redirecting delever
into `XLK` vs `BOXX` did not change outcomes when TECL weight was below 0.75; at
0.75 weight the tighter buffers increased turnover and drawdown.

### Long-history window smoke (2018+, BIL BOXX proxy)

| Variant | Full 2018+ CAGR | Full MDD | COVID 2020 MDD | Rate bear 2022 MDD | 2024+ CAGR |
| --- | ---: | ---: | ---: | ---: | ---: |
| manifest_default | 37.11% | -46.04% | -30.89% | -27.33% | 23.84% |
| vol_off | 33.34% | -46.04% | -41.81% | -27.33% | 23.87% |

Disabling XLK volatility delever (`vol_off`) gives a negligible 2024+ CAGR bump but
materially worsens COVID-window drawdown and reduces full-sample CAGR. **Keep the
manifest default (vol delever enabled, redirect XLK).**

### Long-history bounded sweep (2018+, BIL BOXX proxy)

`data/output/tecl_xlk_long_history_sweep_20260628/`

| Metric | manifest_default | Best sweep (`ma100_b0.06_0.04_0.02_tw0.75_boxx_p90`) |
| --- | ---: | ---: |
| Full 2018+ CAGR | 37.11% | 39.63% |
| Full max drawdown | -46.04% | -46.03% |
| COVID 2020 max DD | -30.89% | -33.26% |
| Rate bear 2022 max DD | -27.33% | -39.76% |

31/110 variants pass the simple dual gate on this long window, mostly variants with
tighter entry/mid buffers (`0.06/0.04`) and TECL weight 0.75. The best long-sample
CAGR lift comes with worse COVID and 2022 stress drawdowns. **Do not promote these
parameters** without beating SOXL on the same windows and clearing the original
runtime gate.

### Updated recommendation

- Stay `research_enabled`; do not promote to `runtime_enabled`.
- Do not change catalog defaults from the SOXL-like seed based on this sweep.
- Next bounded experiment: lower TECL offensive weight (0.55–0.65) with vol delever
  on, or sector-specific RSI/Bollinger thresholds — not another broad grid.
- For 25y synthetic gate: run `build_tecl_xlk_long_history_inputs.py` with
  `--synthesize-tecl-from-xlk` and label outputs `synthetic_tecl_from_xlk` before
  comparing to TQQQ/SOXL long-history retention scripts.

## Follow-Up Research Round 3 (2026-06-28)

### Synthetic long history (XLK → TECL 3x, labeled)

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_tecl_xlk_long_history_inputs.py \
  --start 1999-12-01 \
  --synthesize-tecl-from-xlk \
  --output-dir data/output/tecl_xlk_synthetic_long_history_20260628
```

Artifact: `data/output/tecl_xlk_synthetic_long_history_20260628/`  
`inputs_mode=synthetic_tecl_from_xlk`. BOXX uses BIL proxy plus flat-cash backfill
before BIL inception (research-only).

### Narrow TECL weight sweep (`--mode narrow`, 14 variants)

TECL weights 0.55 / 0.60 / 0.65 with manifest-like vol delever (redirect XLK, p95).

**2024+ window** (`data/output/tecl_xlk_narrow_sweep_20260628/`)

| Variant | CAGR | Max DD | vs default |
| --- | ---: | ---: | --- |
| manifest_default | 24.77% | -46.04% | baseline |
| tw0.65 | 24.16% | -43.28% | -0.6 pp CAGR, +2.8 pp DD |
| tw0.60 | 23.44% | -40.42% | -1.3 pp CAGR, +5.6 pp DD |
| tw0.55 | 22.64% | -37.47% | -2.1 pp CAGR, +8.6 pp DD |

Lowering TECL offensive weight monotonically improves drawdown but sacrifices CAGR.
No narrow variant beats **both** default CAGR and drawdown; dual-gate pass count = 2
(same as full sweep: `manifest_default` and `vol_off` only).

**Synthetic 25y window** (`data/output/tecl_xlk_stress_comparison_20260628.csv`)

| Strategy | Full CAGR | Full MDD | Dotcom MDD | GFC MDD | COVID MDD | 2022 MDD | 2024+ CAGR |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TECL default (synth) | 17.0% | -58.9% | -34.5% | -26.6% | -30.7% | -27.3% | 30.3% |
| TECL tw0.65 | 16.1% | -56.4% | -33.4% | -25.3% | -28.9% | -25.8% | 29.2% |
| TECL tw0.55 | 14.3% | -51.2% | -31.2% | -22.7% | -25.2% | -22.8% | 26.8% |
| SOXL default (real, 2024+) | 172.0% | -34.2% | n/a | n/a | n/a | n/a | 172.0% |

Synthetic TECL survives dotcom/GFC stress with ~25–35% window drawdowns, but
full-sample max drawdown remains ~-59%. SOXL on the overlapping live window still
dominates on CAGR with better drawdown.

### Round 3 recommendation

- **Do not promote** to `runtime_enabled`; SOXL gate still fails by a wide margin.
- **Keep manifest default** (`blend_gate_tecl_weight=0.70`) in catalog; do not
  change production parameters.
- **Optional research profile** (catalog-only, not runtime): `tecl_defensive_tw0.55`
  for drawdown-sensitive satellite research — trades ~2 pp CAGR for ~9 pp better
  2024+ drawdown. Requires separate catalog entry if pursued.
- **Vol delever stays enabled**; `vol_off` still hurts synthetic long-sample CAGR
  without improving max drawdown.
- Next bounded step: sector-specific RSI/Bollinger thresholds at fixed tw0.65, not
  further weight grid expansion.

**No further promotion work planned.** Re-open only if a new bounded hypothesis
beats live TQQQ and SOXL on the same windows with reproducible artifacts.



