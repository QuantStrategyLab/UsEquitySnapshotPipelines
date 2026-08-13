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
also freezes the deterministic enumeration of every 3/6/12/24-month
seen-development window; that enumeration is not a trial ledger, not execution
or reporting of those windows.

This is historical replay coverage only.  It does not make a provider call by
itself, does not activate a scheduler, and does not authorize paper, shadow,
live, orders, or capital.  A completed replay remains research-only and needs
a separate human promotion decision.

## Gaps and next gated slice

The current runner does **not** record a complete frozen trial ledger, does
not execute or report systematic 3/6/12/24-month window results, and does
**not** implement PBO or Deflated Sharpe.  It therefore cannot yet claim the
full overfitting-diagnostic part of this P3 contract, nor can an existing
structural evidence package be treated as promotion acceptance.

The sole next slice is a fresh-human-authorized, tests-first, TQQQ-only
historical-diagnostics addition to the existing runner/evidence contract:
freeze and validate the trial-ledger input before replay, execute and report
the systematic 3/6/12/24-month windows, then add explicitly specified PBO and
Deflated Sharpe reporting.  It must not introduce a generic diagnostics
framework, call a provider, read credentials, activate a scheduler, or enter
paper/shadow/live/order/capital paths.

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
