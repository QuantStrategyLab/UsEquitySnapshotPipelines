English | [简体中文](soxl-v7-long-horizon-risk-observation.zh-CN.md)

# SOXL V7 Long-Horizon Risk Observation

`soxl_core_only_v7_long_horizon_risk_observation.py` converts a completed, frozen SOXL V7 P3 replay into parallel private `qsl.long_horizon_risk_observation.v1` and `qsl.long_horizon_risk_observation.v2` artifacts for `QuantRuntimeSettings`'s long-horizon compounding risk Composer.

It is not a strategy parameter, risk policy, P4/P5/P6 admission, or platform reload. It fetches no market data, accesses no credentials, accounts, brokers, or network, and creates no cloud objects.

## Evidence conventions

- `WALK_FORWARD`: rolling OOS replay over the fixed `trailing_252_xnys_session_oos` window at 10 bps net cost; this is P3 historical OOS evidence, not V7 P4 forward confirmation.
- `STRESS`: 15 bps net-cost replay over the same frozen contiguous 756-session window.
- `BOOTSTRAP`: 8 paths generated from contiguous-window 10 bps strategy/SOXX return pairs using fixed 21-session moving blocks and a deterministic seed derived from the P3 digest hash. Paired resampling preserves contemporaneous alignment between strategy and unleveraged benchmark returns.

Each path records the observed trading-day count `session_count`; adjacent daily return count must equal `session_count - 1`. A fixed 252-XNYS-session OOS path therefore holds 251 adjacent daily returns and still covers one full observation year.

## Honest capability declaration in V2

V2 does not model SOXL V7 as an ordinary ETF strategy whose historical daily returns scale linearly: daily leverage, dynamic cash reserves, trading costs, and stateful replay mean every risk scale requires re-running P3. It therefore fixes `return_evaluation=REPLAY_REQUIRED`, `portfolio_scope=SINGLE_CANDIDATE`, and zero external cash flows; the current generic Composer parks with `RETURN_SCALE_REPLAY_REQUIRED` and emits no executable sizing or drawdown thresholds.

Upstream P1 provides **split-adjusted close prices** only, without a verified dividend-inclusive total-return series. V2's SOXX benchmark is accordingly labeled `SPLIT_ADJUSTED_PRICE_RETURN` and bound to a `price_field=split_adjusted_close` definition digest, never mislabeled as `TOTAL_RETURN_NET_OF_COST`. Until a comparable total-return benchmark and a replay-aware Composer exist, this remains a parking condition rather than a reason to relax gates.

## Private boundaries

The offline P3 facade creates observation files only when explicitly passed `--risk-observation-output <new-private-v1-file>` or `--risk-observation-v2-output <new-private-v2-file>`. Target files must reside in a caller-prepared protected directory and must not already exist; writes are create-only with `0600` permissions. Default workflows omit these flags so return paths never upload to Actions artifacts, Job Summaries, repositories, consoles, or AI context.

Only when both private observations are requested together may `--risk-observation-comparison-output <new-private-receipt>` be supplied. It emits `qsl.soxl_core_only_v7_long_horizon_risk_observation_comparison.v1`, containing only candidate, evidence, and scenario-path digests plus V1/V2 observation digests proving path parity; it contains no raw return paths and never runs the Composer, compares recommendations, writes risk policy, or triggers platform actions.

Downstream restricted ingress must verify the observation hash before the control plane explicitly binds the owner-selected conservative, balanced, or growth preference. The Composer emits sanitized advice only and cannot write back SOXL configuration, reload any platform, or submit orders.
