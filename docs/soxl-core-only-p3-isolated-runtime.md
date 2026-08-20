# SOXL core-only P3 isolated runtime

The SOXL/SOXX core-only P2 v3 candidate is pinned to UES commit
`7756fe32585e85cf1d09a163203a02e3eee39fe1` and QuantPlatformKit commit
`3acab1923a97b805b077c85c6c19657be0143bac`.  Its checked UESP runtime lock
matches that source chain; historical candidates retain their own frozen
identity rather than inheriting P2 v3.

`scripts/run_soxl_core_only_p3_isolated.py` is therefore the narrow execution
bridge:

1. The outer process accepts a local UES checkout only if its HEAD is exactly
   the P2 UES revision, it is clean, and its `uv.lock` has the frozen digest.
2. It validates the complete P2 candidate JSON against its frozen SHA-256, so
   the runtime configuration cannot be injected or retuned by the caller.
3. It launches the same script through that checkout's `uv run --locked`
   environment.
4. The inner process accepts only a JSON research context for SOXL, SOXX, and
   BOXX, takes its runtime configuration only from that validated P2 candidate,
   calls the frozen public source adapter (whose source-level name remains P2
   v2), and returns three target values plus a minimal deterministic diagnostic
   summary.  The P1/P3 identity remains P2 v3 throughout.
5. The outer process verifies the inner canonical decision digest and emits a
   second result digest that includes the execution identity.

For P3 historical replay, the same bridge also supports a strictly ordered
batch of at most 1,024 contexts.  It keeps all contexts in the same verified
UES process, rejects duplicate or out-of-order as-of timestamps, and hashes
the whole ordered batch.  This is a replay efficiency boundary only: it does
not permit tuning, changing the P2 configuration, or mixing source revisions.

For strategy-faithful historical replay, it also has a stateful next-session
mode.  It marks the carried portfolio at each session's prices, executes the
previous session's target on the next complete session, deducts the requested
5/10/15-bps one-way turnover cost, then creates the next target.  The last
signal is explicitly unexecuted.  This avoids the invalid shortcut of treating
every historical decision as though it started from the same cash-only
portfolio.  The local P3 materializer, fixed evidence planner, summary layer,
and offline facade now supply these sessions from validated local P1 bars and
evaluate the frozen windows; this runner only guarantees the source strategy
computation and its provenance.

It is not a P1 publisher or daily P3 scheduler.  The local facade validates
the immutable P1 binding/manifest, derives the allowed point-in-time
indicators, runs all frozen replay windows and cost scenarios, and returns a
metrics-and-hashes summary.  This executable cannot fetch data, access
credentials, call storage or workflows, assess risk, size, record, or create
orders.  It also never uses a mutable source branch or `latest` artifact.
