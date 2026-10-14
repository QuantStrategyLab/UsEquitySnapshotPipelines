# RS-04 cloud archive recovery

The registered `b5a3dd40cb0d4260fb15492108bde7ab544ac0d191807943637b8216a6808482`
archive was produced on 2026-10-11 in a private box, according to the UES
RS-04e preregistration. No GCS URI or cloud producer was registered. The older
`cb14a511...` archive has different coverage and is not a replacement. Repository
renames do not relocate GCS objects. Current UES, UESP, QPK and QRS repository
identities must be checked independently of archive discovery.

`RS-04 Cloud Archive` is a manual, default-branch, first-attempt-only workflow.
It uses the existing research OIDC identity and `market-data-nonlive` environment.
No IAM, production configuration, broker, server or deployment operation exists.

## Configuration

| Setting | Default and meaning |
| --- | --- |
| `mode` | `inventory`: `gsutil ls -L -a` within the approved input root, with no object body reads |
| `input_root` | Empty; falls back to repository variable `RS04_INPUT_ROOT`. Must be `gs://<approved-private-bucket>/research/v2/input/`. No personal bucket is embedded in code. |
| `dataset_id` | `rs04-alpaca-raw-20261015`. Use a new directory for a separately authorized acquisition; existing objects cannot be overwritten. |
| `effective_config_uri` | Empty: records `LIVE_CONFIG_URI_UNAVAILABLE`. If supplied, must be an existing JSON object under the approved input root. Reads its exact generation and stores key names/types only, never values or value digests. |
| OIDC variables | Existing `GCP_WORKLOAD_IDENTITY_PROVIDER`, `GCP_WORKLOAD_IDENTITY_SERVICE_ACCOUNT`, `GCP_PROJECT_ID` |
| Provider credentials | Existing protected `ALPACA_API_KEY_ID` and `ALPACA_API_SECRET_KEY` in the research environment; consumed only by the regeneration step |

Inventory exports names, generations, sizes and only the `sha256` custom metadata
field. Objects without that metadata cannot be ruled out by SHA-256; inventory
must not claim that the registered content never existed or was not moved.
Native cloud/provider diagnostics and all other custom metadata remain suppressed.
Storage uses the repository-locked SDK version and disables its background bucket
metadata query, following the existing read-only archive reader.

Regeneration freezes the approved Alpaca SIP raw daily bars plus corporate
actions, six symbols, 2021-12-01 through 2026-08-25 (BOXX begins 2022-12-28).
It requires the registered 1187 / 917 bar counts, checks ordering and OHLCV, follows
bounded pagination without retries, and uses create-only generation preconditions
plus exact-generation byte readback. Limits: 60 provider requests, 128 MiB total,
4 MiB per page, 20 pages per input; 2000 objects / 4 MiB for metadata inventory.
Data is handled only by the Linux cloud runner and approved private GCS storage.
There are no Actions data artifacts, caches, price logs or local acquisition entry.

`manifest.json` uses canonical JSON and excludes retrieval timestamps, pagination
tokens and GCS generations: identical provider responses produce identical content
digests. Vendor revisions or different pagination may change that digest. This is
not a promise to reproduce `b5a3...` byte-for-byte. The final create-only
`registration.json` records the actual new digest, exact generations, retrieval
time and producer revision; failed partial archives have no registration marker.
The old digest remains as provenance, not as an accepted alias for new bytes.

A configuration structure snapshot is useful for diagnosing missing overrides,
but cannot supply numeric settings for a real live-rule replay. It does not prove
that the supplied object is current or an effective platform merge. Corporate
actions fetched today do not establish historical publication timestamps.
Historical external context, parity records, sell fees and trial-universe evidence
remain independent comparison prerequisites. The existing B3 cloud entry currently
always returns `PARKED`; archive recovery alone must not remove those protections.
Register and review the new source identity in the comparison consumer before
one new comparison dispatch. No live or shadow recommendation follows from
archive creation.
