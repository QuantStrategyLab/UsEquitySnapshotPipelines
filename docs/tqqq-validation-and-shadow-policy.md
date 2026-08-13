# TQQQ Validation and Shadow Policy

TQQQ strategy performance is evaluated in P3 with historical data; it does not
wait for a new year of calendar time before the first meaningful result.

## P3 historical validation contract

- Freeze candidate, configuration, cost assumptions, calendar, acceptance, and
  a complete trial ledger before the final replay.  Periods already used for
  selection are `seen development`, never untouched evidence.
- Use chronological, purged walk-forward folds with causal warm-up, fresh
  episode state, no pre-window trading, and no state carry-over between
  windows.
- Require a locked historical OOS span of at least 12 months; report all
  3/6/12/24-month systematic windows and bull/bear/sideways coverage.
- Evaluate 5/10/15 bp per-side cost scenarios against QQQ and BOXX, while reporting TQQQ,
  QQQM, BOXX, cash, and operational PARK separately.
- Report the frozen trial ledger and pre-specified overfitting diagnostics.
  A provider or execution failure is `INCONCLUSIVE`, not a strategy failure.

## Existing runner coverage

`lifecycle.tqqq_promotion_runner.run_tqqq_promotion_research` already runs
caller-supplied immutable replay material through `BacktestOrchestrator`.  Its
frozen plan has three typed chronological `PurgedWalkForwardFold` windows,
20-calendar-day purge and 20-calendar-day embargo settings (not 20 XNYS
sessions), and a locked XNYS OOS from
2025-07-02 through 2026-07-31 (272 sessions, more than 12 calendar months).
It runs the 5/10/15 bp scenarios, preserves fresh episode state per window,
and emits relative return, Sharpe, drawdown, VaR/CVaR, information coefficient,
turnover, trade-count, allocation, and PARK evidence.  The evidence producer
freezes a complete TQQQ-only trial ledger, executes every deterministic
3/6/12/24-month seen-development window through `BacktestOrchestrator`, and
reports the resulting windows at the pre-specified 5 bp base cost.  The
existing 5/10/15 bp cost-stress replay remains separate.  Cost scenarios and
rolling windows are reporting dimensions, not additional candidate trials.
The ledger currently has one frozen candidate, so CSCV/PBO and Deflated Sharpe
are explicitly `NOT_APPLICABLE` (no value is fabricated); a future ledger with
multiple trials but incomplete aligned return panels is `INCONCLUSIVE`.

The single aggregate development-plan binding and the backtest artifact digest
protect the reporting contract; individual horizons do not publish separate
digests.  UESP only orchestrates and reports caller-supplied historical replay
results.  TQQQ signal, allocation, RiskEngine, and runtime logic remain in
`UsEquityStrategies`.

This is historical replay coverage only.  It does not make a provider call by
itself, does not activate a scheduler, and does not authorize paper, shadow,
live, orders, or capital.  A completed replay remains research-only and needs
a separate human promotion decision.

## Gaps and next gated slice

The historical-diagnostics contract is still research-only.  Its trial
ledger and PBO/Deflated-Sharpe statuses are reporting controls, not promotion
acceptance.  A structural evidence package cannot be treated as human
promotion authority.

Any future expansion must remain TQQQ-only and tests-first, without a generic
diagnostics framework, provider call, credential read, scheduler activation,
or paper/shadow/live/order/capital path.

## P4 optional forward observation

Forward paper/shadow observation validates runtime parity, scheduling, signal
timing, reconnect behavior, data gaps, and monitoring.  It is additional
evidence, not the only valid P3 backtest route and not a substitute for P3
performance evidence.

P4 does not require a research laptop or Gateway to stay online for a year.
Daily collection may be introduced later as a bounded adapter only when its
operational value is demonstrated.  A missed daily run cannot retroactively
invalidate historical P3 evidence or reset an unrelated holdout clock.

The removed daily forward collector and LaunchAgent were never activated.  Any
future paper/shadow collector must be freshly scoped at P4 and must not inherit
provider, credential, promotion, paper, shadow, live, order, or capital authority
from this document.
