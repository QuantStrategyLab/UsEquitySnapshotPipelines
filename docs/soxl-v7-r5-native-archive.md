# SOXL V7 R5 native archive tool

## Scope and identity

This one-shot development tool wraps the existing SOXL V7 P1 observer and corrected native P3. It keeps the candidate contract, price basis, fractional-share replay, costs, 3% USD reserve, and internal BOXX behavior unchanged. It does not establish joint-account admission or execution authority.

The producer must contain correction `6ced4b83b3f450f8d77d20a17637881e55b454bb`; the native UES replay must be at `07b164d95f2ab4d4c54fd993f6f2040bd207d664`. The tool checks the frozen candidate config digest and the P1 date cutoff of `2026-08-25`. Its completion receipt binds the producer revision and tree, dependency lock digest, candidate config, source observations, P1 bytes, each native replay input/output, and the P3 summary.

## Preconditions

The dedicated workflow is manual only. It uses the existing `market-data-nonlive` identity and locked runtime. The private root is supplied by `SOXL_V7_R5_PRIVATE_ROOT`; the tool checks its digest against the authorized exact root. The root is deliberately absent from this public repository and workflow output. The workflow has no artifact upload, release, broker, deployment, schedule, or AI task step.

Before `execute`, independently establish that **both** existing suppliers and the actual subscription permit the bounded acquisition, internal research use, and private retention. Record the evidence digest in the restricted environment as `SOXL_V7_R5_LICENSE_EVIDENCE_SHA256`. A digest supplied to the program is only a binding reference; it is not evidence of permission by itself. The existing Twelve Data key remains the only supplier credential. If the permission or private-root settings are absent, do not dispatch `execute`.

First dispatch `preflight` once. It writes only a small create-only probe and reads its returned generation. A matching existing probe can be verified without overwrite. Do not run the acquisition if the probe fails. The `execute` mode verifies the probe, then creates an immutable `attempt.json` before any market request; a second `execute` cannot repeat acquisition under the same exact root. Partial failure requires inspection of exact saved receipts and a separately reviewed archive-only recovery, never a new full acquisition.

## Fixed budgets and output

The HTTP transport permits only the existing Twelve Data and Yahoo chart hosts, no redirects or proxy, at most 24 requests and 100 MiB of response bodies. It rejects duplicate Yahoo session dates before downstream normalization. Private storage is create-only, generation-pinned on readback, with an 80-operation and 250 MiB aggregate ceiling including the preflight allowance. Individual uploads are capped at 8 MiB to avoid uncontrolled resumable transfers. The job timeout is 45 minutes.

Six source snapshots are saved separately from the native four-file P1 root. After generation-pinned P1 readback, the original P1 verifier runs on a new private directory. P3 is then executed once through the original frozen replay runner. The wrapper archives each replay input and numeric output, plus the original 15-scenario summary; readback recomputes that summary from archived outputs without another strategy replay. `complete.json` is written last. Public output contains only status, counts, candidate identity and digests. No raw observations or daily ledger are uploaded to GitHub artifacts.

The native P3 result, whether financially favorable or negative, remains development evidence. The original V7 candidate and its joint-account `NOT_ADMITTED` status are unchanged. A successful P3 is not paper, shadow, live, or trade permission.

## Current execution state

Local synthetic tests exercise exact-root rejection, create-only fixed-generation readback, bounded transfer, duplicate-date rejection, license binding, and prevention of a second market acquisition. No market acquisition, private probe, native P3, or cloud archive has run from this tool. The restricted environment did not contain the two R5-specific settings at the time this tool was prepared; supplier permission and private-root configuration must be verified before dispatch.
