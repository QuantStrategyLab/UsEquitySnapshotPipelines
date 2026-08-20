# SOXL/SOXX Core-only P3 Offline Evidence Run

`scripts/run_soxl_core_only_p3_evidence.py` is the local facade for the
current SOXL core-only P3 template.  It accepts explicit local paths to a P1
binding, immutable input manifest, canonical `bars.json` member, frozen P2
candidate file, and a clean UES checkout at the P2-pinned revision.

For each invocation it verifies and materializes the local P1 member,
rebuilds the fixed P3 plan, and calls the existing isolated UES runtime once
per fixed evidence request.  Every temporary replay input is removed before
the next request.  On full success it writes a metrics-and-hashes-only P3
summary to stdout.  Invalid paths, malformed provenance, an unpinned runtime,
or a failed replay return a sanitized `PARKED` result; raw bars, derived
indicators, targets, orders, credentials, provider responses, and paths are
never emitted.

This is deliberately a local, non-persistent adapter.  It does not acquire
or publish P1 data, create an immutable root, access cloud resources, create
a workflow, write an evidence store, start paper/shadow execution, make a
promotion decision, or authorize live trading.  A later P1 publisher and P3
artifact/scheduler boundary remain required before a real daily P3 run.
