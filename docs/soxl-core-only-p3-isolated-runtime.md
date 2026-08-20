# SOXL core-only P3 isolated runtime

The SOXL/SOXX core-only P2 v2 candidate is pinned to UES commit
`7756fe32585e85cf1d09a163203a02e3eee39fe1` and QuantPlatformKit commit
`3acab1923a97b805b077c85c6c19657be0143bac`.  The main UESP runtime remains
pinned to the older TQQQ dependency set, so a global dependency upgrade would
alter the existing TQQQ evidence path.

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
   calls the public core-only P2 v2 adapter, and returns three target values
   plus a minimal deterministic diagnostic summary.
5. The outer process verifies the inner canonical decision digest and emits a
   second result digest that includes the execution identity.

It is not yet the complete P3 verifier.  The next component must validate the
immutable P1 binding/manifest, derive the allowed point-in-time indicators,
run all frozen replay windows and cost scenarios, then bind this isolated
execution result into P3 evidence.  This executable cannot fetch data, access
credentials, call storage or workflows, assess risk, size, record, or create
orders.  It also never uses a mutable source branch or `latest` artifact.
