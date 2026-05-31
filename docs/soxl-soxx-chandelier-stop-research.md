# SOXL/SOXX Chandelier Stop Research


## 中文摘要

- 用途：本文档围绕 `SOXL/SOXX Chandelier Stop Research`，用于理解 `UsEquitySnapshotPipelines` 的配置、运行、部署、研究或验收边界。
- 主要覆盖：`Question`、`Implementation`、`Initial Read`、`Follow-Up Overlay Sweep`。
- 阅读顺序：先确认边界、输入输出和权限要求，再执行文档里的命令、CI、dry-run、发布或切换步骤。
- 风险提示：涉及实盘、密钥、权限、Cloud Run、交易所或券商 API 的变更，必须先在测试环境或 dry-run 验证；不要只凭示例直接修改生产。
- 英文正文保留更完整的命令、字段名和配置键；如果摘要和正文不一致，以正文中的实际命令和配置为准。
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
