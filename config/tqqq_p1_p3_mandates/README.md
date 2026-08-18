# TQQQ P1/P3 non-live mandates

This directory intentionally contains no active scope record. Before the
manual P1/P3 workflow can read Alpaca data, an authorized operator must add one
canonical JSON record named `<mandate_id>.json`. The record narrows a run but
does not itself prove human approval.

The record is deliberately narrow: it permits only TQQQ P1 data acquisition
and P3 historical replay, expires within 31 days, and asserts no paper,
shadow, live, order, or capital authority.  Its canonical SHA-256 is carried
into the P3 research evidence as a provenance binding; it never grants P4–P6
promotion authority. At present, the repository and `tqqq-p1-p3-nonlive`
environment must be configured externally with required human review before any
record may be treated as an approved run; otherwise the workflow must remain
unused.
