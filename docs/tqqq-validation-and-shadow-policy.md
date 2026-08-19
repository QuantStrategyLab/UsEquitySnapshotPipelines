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
  A temporary provider or Gateway failure is `INPUT_UNAVAILABLE`, therefore
  `INCONCLUSIVE` and `PARKED`, not a strategy failure. It must not alter the
  frozen data identity, cause a fallback-provider substitution, or reset the
  locked historical OOS span. A later acquisition is a new, separately
  identified input attempt.

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
live, orders, or capital. A completed replay remains research-only. Any future
P4+ authority requires its own separately defined autonomous policy. It must
not be inferred from the P1/P3 scope record.

## P1/P3 evidence index retention

The manual P1/P3 workflow first places its raw P1 root in private cloud object
storage so that P3 can verify and replay the exact same input.  This root
includes the Alpaca bars, binding, manifest, and completion marker.  It is a
short-term transfer/replay store, not a local-only snapshot or a long-term
research archive: the current lifecycle has seven active days followed by a
seven-day soft-delete window.

This change does not extend that raw-data lifecycle.  Any extension, backup,
or other long-term raw-data retention requires a separate Alpaca licence and
retention decision.  Until then, raw P1 data remains private, short-lived, and
non-redistributable.

After a successful P3 replay, the workflow may retain a logically separate,
create-only private evidence-metadata index.  It is in the same short-term
private storage scope as the raw P1 root and therefore shares its lifecycle;
it is not currently a separately retained or durable audit store.  Its bounded
fields are the frozen candidate identity, P1 manifest digest, P3 evidence
digest and verdict, both producer identities, and research-only/no-order
claims.  It never contains Alpaca bars, the full P3 package, or a public
GitHub Actions artifact.  Any separate or long-term evidence-metadata
retention also requires a separate licence and retention decision.

Before P1 can read Alpaca, the workflow resolves an expiring, checked-in
non-live scope record by identifier and records its canonical receipt digest in
P3 provenance. It checks the same record again before P3. The record may cover
only P1 data acquisition, the associated private create-only root upload, P3
read/replay, and the associated private evidence-index upload; it explicitly
forbids paper, shadow, live, order, and capital actions. There is intentionally
no active scope record in the repository.

The three raw P1 files are uploaded before one separate create-only completion
marker that binds their hashes and manifest digest. P3 requires that marker and
verifies it against the downloaded root, so a partial remote upload fails closed
instead of being treated as replayable evidence.

The P1 input producer and P3 index producer must name the same repository
commit and tree. This prevents an input captured by one code revision from
being presented as a conclusion produced by another revision in the one-shot
P1-to-P3 chain.

The record narrows and makes a requested run reproducible, but is only a
no-order technical scope record. It is not a pre-authorized autonomous policy,
and it is not evidence that one is active. Future unattended P1 requires a
separately defined, externally verified, non-execution data-acquisition
authorization for the exact P1/P3 scope. That authorization is not active and
this repository does not read, verify, or inject it today; until it exists, the
workflow must remain undispatched. Neither the record nor its digest grants
paper, shadow, live, order, capital, or P4–P6 promotion authority.

If P3 parks, the GitHub Actions summary retains only the sanitized failure class,
stage, and whether replay began. It does not retain raw provider data, paths,
or exception details. This makes a later retry decision auditable without
turning Actions logs into a research-data store.

## Gaps and next gated slice

The historical-diagnostics contract is still research-only.  Its trial
ledger and PBO/Deflated-Sharpe statuses are reporting controls, not promotion
acceptance. A structural evidence package cannot be treated as P4+ promotion
authority.

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
