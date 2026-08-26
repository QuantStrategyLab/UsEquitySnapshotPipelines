# TQQQ P1/P3 non-live mandates

Before a P1/P3 workflow can read Alpaca data, a bounded canonical JSON record
named `<mandate_id>.json` must exist. The record is a no-order technical scope
record, reviewed and expiring; it does not grant autonomous policy promotion.

The record is deliberately narrow: it permits only TQQQ P1 data acquisition,
the attached private create-only P1 upload, P3 private-root read/replay, and
the attached private P3 evidence-index upload. It expires within 31 days and
asserts no paper, shadow, live, order, or capital authority. Its canonical
SHA-256 is carried into the P3 research evidence as a provenance binding; it
never grants P4–P6 promotion authority. Future unattended P1 requires a
separately defined, externally verified, non-execution data-acquisition
authorization for this exact P1/P3 scope. A workflow validates the checked-in
scope record before it reads a provider, and it remains manual-only. The
record must use the schema that is bound to the exact candidate: V1 records
cannot authorize V7, and V7 records cannot authorize V1.

## Default state and external approval

Until an exact, reviewed scope record is merged, no authorization is active.
That authorization is not active until the record is merged. The repository
and `market-data-nonlive` environment do not read, verify, or
inject it today; they only validate the checked-in scope record. A record must
therefore be created from a separately defined, externally verified, non-execution data-acquisition
authorization and identify the authorized GitHub account in its attestation.
