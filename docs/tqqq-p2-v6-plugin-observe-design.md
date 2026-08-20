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

P3 receives mappings that have already been materialized by a caller. The
generic seam re-validates the V2 envelope and all P1 identities. The strict
`qqq_price_regime_observer` seam additionally re-computes the close-only
signal from a caller-supplied, already P1-verified QQQ bar sequence, including
the installed QSP module digest and pinned configuration. It then emits only a
sanitized result: hashes, cutoff, plugin identity, and a comparison proving
that the independently supplied observer target mapping is byte-for-byte the
v5 base mapping. It never emits
the signal payload, bars, root paths, targets, credentials, account data, or
execution instructions.

Any missing or mismatched P1 reference, envelope/provenance or recomputation
mismatch, AI field, action/target field, or invalid contract returns `PARKED`.
No fallback, resolver, `latest` artifact, P4/P5/P6 path, workflow, storage
write, or automatic retry is part of this design.

`tqqq_p2_v6_qqq_price_regime_root` adds the only permitted bridge from a local
P1 root: it verifies the unchanged v5 root before extracting QQQ bars, derives
an identity from all verified root members, then calls the strict recomputation
seam. It returns no bars or signal payload and writes nothing. A future daily
controller may use this local bridge only after its durable observation
retention boundary is separately decided; this PR does not modify a workflow.
