# TQQQ P2 v6 Plugin Observe Contract (design-only)

Status: `DAILY_OBSERVATION_ONLY_NOT_STRATEGY_RUNTIME`

This is a small P2/P3 observation contract around the active
`tqqq_core_only_p2_v5` candidate. The daily P1/P3 research workflow invokes
it only after the same v5 P3 evidence is complete and a bound v5 forward
observation exists. It does not register a new active candidate or change
strategy targets.

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
No fallback, resolver, `latest` artifact, strategy-consumption path, or
automatic retry is part of this design.

`tqqq_p2_v6_qqq_price_regime_root` is the only permitted bridge from a local
P1 root: it verifies the unchanged v5 root before extracting QQQ bars, derives
an identity from all verified root members, then calls the strict recomputation
seam. It returns no bars or signal payload. The existing daily controller
writes only the resulting redacted record as a GitHub Actions artifact retained
for 35 days. It does not upload the v6 record to GCS, publish it to the control
plane, or grant durable/long-term retention.

The v6 record is emitted only if independently recomputing the signal from the
verified P1 root succeeds and the exact v5 forward targets match. A v6 failure
is summarized as `PARKED`; it does not alter or fail the completed v5 research
record. No strategy consumes the signal, and no P4/P5/P6 action is enabled.
