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
frozen plan has three typed chronological `PurgedWalkForwardFold` windows, a
252-session post-training purge and zero embargo, and a locked XNYS OOS from
2025-08-01 through 2026-07-31 (251 sessions).
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

The core runner is historical replay only: it has no provider, scheduler,
broker, order, or capital port.  The separate P2 v5 daily controller may call
that runner only after it has verified a candidate-bound P1 root.  A completed
replay remains research-only. Any P4+ authority requires its own separately
defined autonomous policy; it cannot be inferred from P1/P3 evidence.

## P1/P3 retention and execution paths

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

After a successful P3 replay, the controller may retain a logically separate,
create-only private metadata record.  It is in the same short-term private
storage scope as the raw P1 root and therefore shares its lifecycle; it is not
a separately retained or durable audit store. Its bounded fields are the
frozen candidate identity, P1 manifest digest, input-health digest, P3 evidence
digest/verdict or sanitized PARK details, and research-only/no-order claims.
It never contains Alpaca bars, the full P3 package, or a public GitHub Actions
artifact. Any separate or long-term evidence-metadata retention requires a
separate licence and retention decision.

### Current P2 v5 scheduled research path

`.github/workflows/tqqq-p1-p3-daily-research.yml` is the current personal,
unattended P1/P3 path. It runs after the XNYS close on scheduled weekdays and
derives the latest completed XNYS session at run time. The checked-in,
immutable P2 v5 candidate is its personal research policy: its canonical
digest is carried into P3 as the no-order research receipt. There is no
per-run mandate or reviewer.

The controller is limited to Alpaca SIP acquisition, input-health assessment,
short-term private create-only root/status storage, offline P3 replay, and,
after a complete P3 result, publication of one digest-bound sanitized forward
observation under a separate session-keyed prefix. The observation is not a P5
run: it is an input artifact for a separate future P5 scheduler and carries
neither a policy receipt nor any broker, credential, order, capital, paper,
shadow, or live authority. A future P5 identity must be limited to that
sanitized prefix, never the raw P1-root prefix. An
unavailable or invalid input records `DEFERRED` or `QUARANTINED` and skips P3;
it never fills a gap, substitutes a provider, retries with changed inputs, or
changes a strategy parameter. P4 paper, P5 shadow, broker orders, capital, and
P6 live remain unavailable from this controller. This is not a claim that the
same strategy has no Paper, Shadow, or Live evidence on another platform; that
evidence is platform- and lane-specific and does not grant this controller a
new execution authority.

A separate recovery controller may inspect only sanitized daily status metadata
and, only for a `runtime_internal_failure` after P3 replay began, make one
offline replay attempt against the same verified P1 manifest. It records a
create-only, digest-bound terminal outcome and never invokes Alpaca, changes
the P1 root, substitutes data, or loops. It does not publish a delayed P5
forward observation: an old replay is historical P3 evidence, not a timely
forward allocation signal.

### Historical P2 v2 disposition

`tqqq_core_only_p2_v2` is a frozen historical candidate, not a runnable
research route. Its first evidence fold begins on 2022-01-03 but it requires
BOXX as a tradable asset; the immutable input contract permits BOXX only from
2022-12-28. Consequently no P1 root can both satisfy its source-coverage rule
and support the complete v2 replay. A pre-inception proxy, carried price, cash
substitution, or synthetic BOXX row is prohibited, so the correct outcome is
`PARKED`, not a fabricated evidence package.

The P3 CLI recognizes this disposition at its configuration boundary. It emits
a sanitized `config_contract_failure` before opening a snapshot, starting a
replay, or creating an output directory.

P2 v4 corrected this common-availability geometry without changing the public
TQQQ research adapter. P2 v5 is the only active candidate in the registry and
the only scheduled P1/P3 route. Historical v2 configuration and adapter
selection remain inspectable for provenance, but must not be reactivated or
treated as P3 evidence.

### Legacy manual v1 compatibility path

The older `tqqq-p1-p3-one-shot.yml` compatibility workflow is manual v1 only.
It resolves an expiring checked-in non-live scope record by identifier before
reading Alpaca and records that receipt in P3 provenance. No active legacy v1
scope record is checked in, so this legacy path remains undispatched. It does
not govern the P2 v5 scheduled controller.

The three raw P1 files are uploaded before one separate create-only completion
marker that binds their hashes and manifest digest. P3 requires that marker and
verifies it against the downloaded root, so a partial remote upload fails closed
instead of being treated as replayable evidence.

The P1 input producer and P3 index producer must name the same repository
commit and tree. This prevents an input captured by one code revision from
being presented as a conclusion produced by another revision in the one-shot
P1-to-P3 chain.

The legacy record is only a no-order technical scope record, not an autonomous
policy. Its constraints and digest do not grant paper, shadow, live, order,
capital, or P4--P6 authority.

If P3 parks, the GitHub Actions summary retains only the sanitized failure class,
stage, and whether replay began. It does not retain raw provider data, paths,
or exception details. This makes a later retry decision auditable without
turning Actions logs into a research-data store.

## Gaps and next gated slice

The historical-diagnostics contract is still research-only.  Its trial
ledger and PBO/Deflated-Sharpe statuses are reporting controls, not promotion
acceptance. A structural evidence package cannot be treated as P4+ promotion
authority.

Any P4+ expansion must remain TQQQ-only and tests-first. It must not reuse the
P1/P3 credentials, data authority, scheduler, or research receipt as paper,
shadow, order, capital, or live authority.

## P4 optional forward observation

Forward paper/shadow observation validates runtime parity, scheduling, signal
timing, reconnect behavior, data gaps, and monitoring.  It is additional
evidence, not the only valid P3 backtest route and not a substitute for P3
performance evidence.

P4 does not require a research laptop or Gateway to stay online for a year.
The current daily P1/P3 collector is a bounded observed-data adapter, not a
P4 collector. A missed daily run cannot retroactively invalidate historical P3
evidence or reset an unrelated holdout clock.

The removed daily forward collector and LaunchAgent were never activated.  Any
future paper/shadow collector must be freshly scoped at P4 and must not inherit
provider, credential, promotion, paper, shadow, live, order, or capital authority
from this document.

### Candidate naming and lineage

The immutable machine identifier (`tqqq_core_only_p2_v2`, `v4`, or `v5`) is
retained because manifests, source pins, SHA256 bindings, and CI contracts use
it.  Human-facing surfaces should also show the candidate's freeze date and
lineage, for example:

`TQQQ Core Only / 2026-08-19 / Successor (lineage: p2_v5)`

Dates improve review and dashboard readability without renaming historical
identities.  A date label must never replace the immutable candidate ID or be
used to relabel evidence from another candidate.
