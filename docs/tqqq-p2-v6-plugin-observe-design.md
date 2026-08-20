# TQQQ P2 v6 Plugin Observe Contract (design-only)

Status: `DESIGN_ONLY_NOT_RUNTIME`

This is a small, local P2/P3 validation contract around the active
`tqqq_core_only_p2_v5` candidate.  It does not register a new active
candidate, modify v5/P3 callers, change strategy targets, or add a workflow.

For a supplied P1 identity, P2 v6 binds exactly:

- the v5 base candidate/config/revision;
- P1 binding digest, manifest digest, input-root digest, and completed cutoff;
- one `qsl.strategy-plugin-signal.v2` plugin id, payload digest, producer
  revision/code/config digests, and entrypoint;
- the exact rule `observe_only`, `strategy_target_transform=none`, no execution
  authority, and no AI input.

P3 receives mappings that have already been materialized by a caller. It
re-validates the V2 envelope and all P1 identities, then emits only a
sanitized result: hashes, cutoff, plugin identity, and a comparison proving
that the independently supplied observer target mapping is byte-for-byte the
v5 base mapping. It never emits
the signal payload, bars, root paths, targets, credentials, account data, or
execution instructions.

Any missing or mismatched P1 reference, envelope/provenance mismatch, AI
field, action/target field, or invalid contract returns `PARKED`. No fallback,
resolver, `latest` artifact, P4/P5/P6 path, or automatic retry is part of this
design.
