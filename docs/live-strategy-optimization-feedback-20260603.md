# Live Strategy Optimization Feedback

Date: 2026-06-03

This note summarizes the conservative optimization review for the current
runtime-enabled US equity and snapshot-backed strategies. It is a research
feedback document only; it does not change live strategy defaults or live-enable
any new profile.

## Scope

Current runtime-enabled profiles reviewed from `UsEquityStrategies/main`:

| Profile | Role | Decision |
| --- | --- | --- |
| `global_etf_rotation` | defensive ETF rotation | Keep current default |
| `mega_cap_leader_rotation_top50_balanced` | balanced mega-cap snapshot rotation | Keep current default |
| `nasdaq_sp500_smart_dca` | buy-only cash deployment | Keep current default; operational tuning only |
| `russell_1000_multi_factor_defensive` | defensive Russell 1000 snapshot baseline | Keep current default |
| `soxl_soxx_trend_income` | offensive semiconductor leveraged sleeve | Keep current default |
| `tech_communication_pullback_enhancement` | tech/communication snapshot pullback sleeve | Keep current default |
| `tqqq_growth_income` | offensive Nasdaq dual-drive leveraged sleeve | Keep current default; optional conservative override only |

## Evidence outputs

The CSV summary used for this review is generated under:

```text
data/output/live_strategy_optimization_feedback_20260603/
```

Files:

- `feedback_evidence_summary.csv`: normalized evidence table across existing
  live proxies, optimization variants, and relevant research references.
- `feedback_proposals.csv`: conservative proposal list and anti-overfit notes.

`data/output` is intentionally ignored by Git. Regenerate these files from the
current research artifacts when needed rather than treating them as source files.

Primary evidence sources:

| Evidence directory | Coverage |
| --- | --- |
| `data/output/leveraged_strategy_candidate_research_20260603/` | TQQQ/SOXL current live proxies and optimization variants over short/medium/long windows |
| `data/output/us_equity_strategy_candidate_research_20260603/` | Russell 1000 snapshot baseline gate over short/medium/long windows |
| `data/output/tech_communication_pullback_enhancement_backtest_20260603/` | Tech/Communication profile refresh over short/medium/long windows |
| `data/output/global_etf_rotation_research_20260603/` | Global ETF production-like research snapshot |
| `data/output/mega_cap_leader_rotation_*` | Mega-cap static/dynamic research references |

## Anti-overfitting rules used

- Keep current live baseline, optimization variants, and new strategies
  separate.
- Do not count parameter tweaks of existing live strategies as new strategies.
- Prefer one-knob or very small rule changes over broad parameter grids.
- Require short, medium, and long windows where the existing research runner
  supports them.
- Do not replace a live default unless the alternative clearly improves the
  relevant baseline without materially increasing drawdown or reducing annualized
  return.
- Do not optimize directly against a single drawdown boundary breach.

## Decisions by profile

### `tqqq_growth_income`

Decision: keep the current `45% QQQ / 45% TQQQ` dual-drive default.

Evidence from the leveraged candidate gate:

| Candidate | Long CAGR | Worst drawdown | Review action |
| --- | ---: | ---: | --- |
| `live_tqqq_dual_drive_45_45_proxy` | 23.12% | -25.38% | keep current live |
| `opt_tqqq_dual_drive_40_40` | 20.80% | -22.80% | no replacement |
| `opt_tqqq_qld_tqqq_60_20` | 22.90% | -25.64% | no replacement |

Feedback:

- `40/40` is acceptable only as a conservative account-level override when the
  operator explicitly prefers lower drawdown over return.
- Do not replace the default with `40/40`; the drawdown improvement comes with a
  material CAGR reduction.
- Do not add new signal filters without a separate short/medium/long gate.

### `soxl_soxx_trend_income`

Decision: keep the current SOXL/SOXX live core.

Evidence from the leveraged candidate gate:

| Candidate | Long CAGR | Worst drawdown | Review action |
| --- | ---: | ---: | --- |
| `live_soxl_soxx_trend_57_proxy` | 33.27% | -43.02% | keep current live |
| `opt_soxl_soxx_signal_soxx_50` | 28.65% | -56.84% | no replacement; gate failed |

Feedback:

- Do not adopt the SOXX-signal 50 variant. It looks more conservative by naming,
  but the tested drawdown is worse.
- Keep the current trend, overheat, and volatility-delever defaults.

### `global_etf_rotation`

Decision: keep the current default and do not add further tuning.

Evidence snapshot:

| Strategy | CAGR | Benchmark CAGR | Max drawdown | Sharpe |
| --- | ---: | ---: | ---: | ---: |
| `global_etf_rotation` vs SPY | 18.33% | 14.88% | -30.50% | 0.83 |

Feedback:

- The existing confidence plus relative-volatility gate is already a small,
  explainable improvement folded into the default path.
- Further tuning of z-gap thresholds, volatility ratios, or unequal weights risks
  fitting the latest sample.
- Keep it as a defensive rotation profile, not a QQQ replacement.

### `tech_communication_pullback_enhancement`

Decision: keep live; do not reduce exposure just to fit a hard 30% drawdown
line.

Evidence from the refreshed short/medium/long backtest:

| Period | CAGR | QQQ CAGR | SPY CAGR | Max drawdown | Sharpe |
| --- | ---: | ---: | ---: | ---: | ---: |
| short | 79.94% | 16.11% | 15.26% | -15.23% | 2.29 |
| medium | 56.90% | 20.84% | 18.82% | -19.18% | 1.89 |
| long | 23.48% | 18.17% | 13.26% | -30.84% | 1.03 |

Feedback:

- The long-window drawdown is slightly worse than 30%, but the profile beats QQQ
  and SPY across all three windows.
- Do not tune exposure down solely because of the `-30.84%` boundary breach; that
  would be a high overfit risk unless a separate exposure study proves the edge
  is preserved.

### `russell_1000_multi_factor_defensive`

Decision: keep the current defensive baseline.

Evidence from the US equity snapshot candidate gate:

| Candidate | Worst drawdown | Min Sharpe | Median Sharpe | Review action |
| --- | ---: | ---: | ---: | --- |
| `live_r1000_multi_factor_defensive` | -27.62% | 0.85 | 1.39 | current live baseline |

Feedback:

- No ordinary ETF, new snapshot, or optimization candidate cleared the stricter
  replacement/supplemental bar in the current gate output.
- If an account needs lower drawdown, a future research-only pass can test one
  simple exposure knob such as lower soft-defense stock exposure. Do not change
  factor weights without a fresh gate.

### `mega_cap_leader_rotation_top50_balanced`

Decision: keep the current Top50 balanced live profile.

Reference evidence from older mega-cap research:

| Research reference | CAGR | Max drawdown | Interpretation |
| --- | ---: | ---: | --- |
| static expanded reference | 30.22% | -38.66% | Higher return, but drawdown too high for this review |
| dynamic top20 dedup full2018 | 20.54% | -23.14% | Lower drawdown, but materially lower CAGR |

Feedback:

- Do not revert the current live profile to the older static or dynamic research
  variants.
- The current blended live design should stay in place; monitor any slight
  drawdown boundary breach separately rather than selecting the best-looking old
  robustness row.

### `nasdaq_sp500_smart_dca`

Decision: keep current buy-only design; do not rank it as an alpha replacement.

Feedback:

- This profile is a cash-deployment tool, not a sell/rebalance alpha strategy.
- Optimization should stay operational: base investment size, cash reserve,
  minimum order size, whole-share execution, and whether the account should use
  QQQM/SPLG or direct QQQ/SPY.
- Do not add sell or rebalance logic unless it is treated as a separate strategy
  with its own cash-flow simulation.

## Final recommendation

No current evidence supports replacing live defaults.

Actionable feedback only:

1. Keep all current live profiles enabled with their existing defaults.
2. Allow `tqqq_growth_income` `40/40` only as an explicit conservative
   account-level override, not a default replacement.
3. Consider a future research-only Russell 1000 soft-defense exposure test if a
   lower-drawdown account mandate appears.
4. Avoid further parameter-grid tuning for Global ETF, TQQQ, SOXL, and
   Tech/Communication unless a new hypothesis is defined before looking at the
   latest backtest output.
