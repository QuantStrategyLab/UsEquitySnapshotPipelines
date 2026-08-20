# SOXL P3 Strategy Performance Contract

`strategy_performance.v2` is the small, sanitized bridge from a completed
SOXL P3 evidence summary to the issue-only research watcher.  It is neither a
parameter baseline, an optimization verdict, nor an execution instruction.

The projection accepts only a digest-verified
`qsl.soxl-soxx-core-only-p3-evidence-summary.v1` and selects exactly one
predeclared replay: `trailing_252_xnys_session_oos` at 10 bps.  It carries the
P1 manifest digest, frozen P2 digest, source revision and producer revision,
plus five finite comparable metrics: Sharpe, CAGR, Calmar, win rate and maximum
drawdown.  It excludes raw bars, indicators, target weights, positions,
accounts, orders, credentials, storage paths and all P4–P6 authority.

The watcher must compare this record only with an earlier, separately
completed record for the same candidate.  Any watcher outcome remains a
research Issue/diagnosis input; it cannot alter the candidate, schedule a
replay, submit an order, or promote the strategy.
