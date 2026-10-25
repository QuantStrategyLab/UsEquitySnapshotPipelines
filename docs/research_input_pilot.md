# Cloud-only research input pilot

For exact readback of a predeclared benchmark window from an existing archive,
see [frozen benchmark input readback](benchmark_reference_readback.md). It retains
provider-adjusted security returns as a diagnostic, without issuing account policy.

The manual `Research Input Pilot` workflow inventories exact-generation objects
and acquires explicitly configured historical inputs into a new private prefix.
It has no strategy replay, publisher, broker, release or deployment entry point.

Use the existing `market-data-nonlive` environment, protected Alpaca client and
research storage identity. `RS04_INPUT_ROOT` supplies the private research root;
no private resource is a source-code default. Insufficient access stops the run.
The workflow does not create identities, permissions, variables or secrets.

Upload a private JSON plan under `<approved-research-input-root>` and dispatch
with its URI, generation, byte size and SHA-256. The plan has these fields:

| Field | Meaning |
| --- | --- |
| `scope` | Must be `research_input_only`. |
| `dataset_id` | New lowercase identifier; all writes are create-only beneath it. |
| `end` | Inclusive completed-session cutoff, at least two days old. |
| `paid_data_usd`, `cloud_cap_usd`, `estimated_cloud_usd` | Zero paid data; pre-run cloud estimate within a cap of at most USD 10. |
| `retention_estimate_days` | 1–90 day storage-cost horizon; no automatic deletion. |
| `instruments` | Up to 20 `{symbol,start,inception}` records. Never request before inception. |
| `rates` | Optional direct Board H.15 / NY Fed SOFR requests, with source notices. |
| `inspect_objects` | Optional exact `{uri,generation,bytes,role,sha256?}` references. |

Alpaca requests retain separate `raw_unadjusted` (`adjustment=raw`) and
`split_dividend_adjusted` (`adjustment=all`) identities, plus corporate actions.
No returns, adjusted execution prices or total-return index are constructed.
Vendor-native sessions, closes and volume semantics remain explicitly labelled.
The pinned XNYS schedule measures missing sessions; truncation remains a gap.
Current downloads do not establish historical publication-time availability.

Each run stops before exceeding 120 HTTP attempts, 96 MiB HTTP responses,
128 MiB retained objects, 512 MiB storage transfer, 3,000 storage operations, or
20 minutes of application execution. The workflow timeout is 30 minutes. No call
is retried, no source fallback occurs, and unknown writes stop immediately.
Estimate the combined cost of every intended dispatch before running; these
resource limits are not a billing meter or independent budget authorization.

Real responses and the private `input-inventory.json` stay in the approved cloud.
Only `coverage-summary.json` may be exported. Its allowlist contains coverage,
counts, timestamps and hashes, never source bodies, prices, positions, fills or
private locators. The private inventory records per-file generations and SHA-256
and calls QPK's official `_plugin_bundle_sha256` on candidate plugin bytes. The
aggregate is a candidate identity pending confirmation of file-set completeness,
consumer mapping and approval. It does not create an approved plugin set.

The input inventory and CI results cannot qualify a release or establish an
untouched out-of-sample interval. Missing config, policy, history and fill
bindings remain unresolved. Retention beyond the cost horizon needs an explicit
decision; the workflow never deletes or overwrites prior evidence.
