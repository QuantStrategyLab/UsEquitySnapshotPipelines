# TQQQ P1/P3 non-live mandates

This directory intentionally contains no active scope record. Before the
P1/P3 workflow can read Alpaca data, a bounded canonical JSON record named
`<mandate_id>.json` must exist. The record is only a no-order technical scope
record; it does not itself constitute a pre-authorized autonomous policy or
prove that one is active.

The record is deliberately narrow: it permits only TQQQ P1 data acquisition,
the attached private create-only P1 upload, P3 private-root read/replay, and
the attached private P3 evidence-index upload. It expires within 31 days and
asserts no paper, shadow, live, order, or capital authority. Its canonical
SHA-256 is carried into the P3 research evidence as a provenance binding; it
never grants P4–P6 promotion authority. Future unattended P1 requires a
separately defined, externally verified, non-execution data-acquisition
authorization for this exact P1/P3 scope. That authorization is not active.
The repository and `market-data-nonlive` environment do not read, verify, or
inject it today; otherwise the workflow must remain unused.
