# IWB single-filing capture preparation

## Current status and scope

This is an **offline-tested capture entry point**, not a completed SEC collection
loop. No real SEC index/XML retrieval, provider/model/broker call, cloud storage
operation, workflow, production promotion, or trading action is part of this
implementation or its tests. It adds no registry or new dependencies and does not
change the existing parser, namespace rules, binder, or cutoff selector.

The separately authorized real attempt on 2026-10-04 stopped after its only
index request with `SEC_INDEX_INPUT_REJECTED`; no XML was requested. The previous
entry point wrote raw files only after parsing/binding succeeded, so that attempt
left no raw bytes or response-completion metadata. The rejected structure and
parser root cause remain unknown. This retention change cannot reconstruct that
missing evidence and does not authorize another real request or change a parser.

The one fixed target is:

| Field | Required target |
| --- | --- |
| Accession | `0001004726-26-003726` |
| Report period | `2026-03-31` |
| CIK | `0001100663` |
| Series | `S000004347` |
| Class | `C000012077` |
| Fund ticker | `IWB` |
| Index | `https://www.sec.gov/Archives/edgar/data/1100663/0001004726-26-003726-index.htm` |
| XML | `https://www.sec.gov/Archives/edgar/data/1100663/000100472626003726/primary_doc.xml` |

Both URLs must be supplied explicitly and match these exact strings. Alternative
hosts, HTTP, query strings, other documents/accessions, and silent redirects fail
closed. The index must match accession/period/CIK before the XML request starts.
XML fund identity, report period, and any declared accession/form must agree with
the index. An omitted XML accession is not fabricated; the fixed request URL and
parsed index provide that binding through the existing adapter.

## Explicit execution, after separate acceptance

Importing the module, `--help`, missing arguments, invalid targets, and missing
explicit client identity produce zero requests. Nothing schedules this script.
The following is an operator template, **not a command run during preparation**:

```bash
# First configure SEC_CAPTURE_USER_AGENT with your authorized SEC client identity
# outside public logs. There is no built-in name, contact, email, or fallback UA.
PYTHONPATH=src:. python -m scripts.capture_iwb_sec_filing_once \
  --run \
  --index-url https://www.sec.gov/Archives/edgar/data/1100663/0001004726-26-003726-index.htm \
  --xml-url https://www.sec.gov/Archives/edgar/data/1100663/000100472626003726/primary_doc.xml \
  --accepted-timezone America/New_York \
  --user-agent-env SEC_CAPTURE_USER_AGENT \
  --version-id YOUR_NEW_LOCAL_VERSION_ID \
  --output-dir YOUR_PRIVATE_LOCAL_DIRECTORY
```

The execution operator must explicitly configure the referenced environment
variable. Its value is sent unchanged as User-Agent and is never echoed or saved
in the receipt. The tool does not discover a user's email, invent an identity,
configure credentials, change proxy settings, or rotate User-Agent/proxies to
work around a rejection. Existing runtime proxy configuration remains in force;
its security and actual path still require operator acceptance.

`--accepted-timezone` is mandatory even when the returned Accepted field includes
an offset. It supplies an explicit IANA timezone to the existing parser when the
field is offset-free; the existing DST/ambiguity rules remain unchanged.

## Transport and failure boundaries

- At most two serial requests: complete index, verified local raw/response
  receipt storage, index parsing/identity checks, then complete XML
- At least one monotonic second from index response completion to the XML start
- `timeout=30` for each urllib request; monotonic elapsed checks reject any
  successfully returned response exceeding 30 seconds. The underlying urllib
  timeout is a socket timeout, not an OS-level hard interruption of the process
- Index at most 1 MiB; XML at most 8 MiB; bounded reads include an extra-byte probe
  when necessary, and EOF is required even when Content-Length is present
- No redirects, retries, pagination, accession discovery, extra documents, XSD
  downloads, CAPTCHA solving, or cloud/broker/provider calls
- HTTP 403/429 and other non-200 statuses fail immediately. Recognized HTTP-200
  challenge bodies fail; there is no attempt to bypass them or retain them as
  complete-response evidence
- Only absent or `identity` Content-Encoding is supported; gzip/br/other
  compression, duplicate/invalid Content-Length, oversized or length-mismatched
  responses, empty bodies, and malformed/truncated supported HTML/XML fail
- Exact fund identity/period/URL conflicts fail closed. Existing holding
  `unsupported`/`unresolved` statuses and reasons are retained, including missing
  tickers and code-reuse/multi-ticker conflicts. No holding is silently deleted,
  no identifier is turned into an invented ticker, and no row becomes tradable
- An absent Content-Length relies on clean transport EOF plus the existing
  supported-document completeness checks. A response with no length that is
  syntactically complete cannot prove additional unseen source content exists

Errors expose only bounded error codes and an optional HTTP status. Response
headers, error bodies, User-Agent values, proxy credentials, and exception details
are not printed. Raw source bodies are kept locally because the purpose is exact
evidence retention; the operator must choose an appropriately private directory.

## Completion-time receipt and create-only storage

The aware wall-clock timestamp is sampled immediately after each response's EOF,
before parsing, response context cleanup, or local writes, and recorded in UTC
with its actual subsecond precision. Both times are retained when both safe
complete responses are received.
`observed_at = max(index_completed_at, xml_completed_at)`, preserving subsecond
precision. It is never Accepted, report-period end, a fabricated lag, or the later
disk-write/readback time. The operator remains responsible for a trustworthy
machine clock. A future independently observed revision of this same accession
must have a new version ID and its own raw hashes/completion times.

The existing functions are used directly:

1. `parse_iwb_sec_filing_index_html`
2. `parse_iwb_sec_nport_xml_bytes` with the parsed expected index
3. `bind_iwb_sec_filing_input_version` using the receipt-derived observed time

The cutoff selector's behavior is unchanged: no selection before observation,
selection at the exact observation instant, and no retrospective overwrite by
a later same-accession revision. Accepted still orders source filing revisions;
it never establishes availability.

The output directory contains a newly reserved version directory with:

- `filing-index.html`: unchanged index bytes
- `filing-index.html.response.json`: create-only index response metadata
- `primary_doc.xml`: unchanged XML bytes
- `primary_doc.xml.response.json`: create-only XML response metadata
- `manifest.json`: success receipt only after both raw/response-receipt exclusive
  writes, exact-byte readbacks, size/SHA-256 checks, and all existing parsing,
  fixed-identity checks, and binding succeeded

Each complete response that passes the existing HTTP/URL/encoding/budget/deadline,
EOF/length, and challenge boundaries is immediately stored before its document
parser runs. Its response receipt records only the fixed URL, HTTP 200, byte
length, SHA-256, actual UTC `completed_at`, and these explicit boundaries:

```json
{
  "schema_version": "qsl.research.iwb_sec_single_filing_response_receipt.v1",
  "status": "response_received_not_validated",
  "raw_xsd_validation": "not_validated",
  "production_eligible": false,
  "trading_eligible": false
}
```

This is a transport-receipt record, not a validated filing or success manifest.
It contains no User-Agent/email, proxy setting, or response headers. Its status
is not rewritten on success; the separately published manifest provides the
existing parsing/binding result without repeating or overwriting raw writes.
An index-parser/identity failure retains only the index raw and response receipt,
and starts no XML request. An XML-parser/binding failure retains both raw/receipt
pairs. Neither failure publishes `manifest.json` or reports `capture_complete`.
Non-200, challenge, oversized, deadline-exceeded, and transport-incomplete
responses produce no raw/receipt pair for the rejected response; an earlier safe
index pair remains. Clean transport EOF can still yield malformed or unsupported
document syntax; retained bytes do not claim document completeness or validity.
Raw or receipt storage/readback failure stops before that document's parser and
before any later request. A storage failure may leave an incomplete file or a
raw file without its receipt; it never fabricates verified receipt metadata.

Version reservation and raw writes are create-only; an existing version directory
fails before any request. The manifest is first exclusively written and verified
as `.manifest.pending`, then atomically hard-linked to the new canonical
`manifest.json` without replacement. This requires a local filesystem supporting
hard links. Failure before publication leaves no canonical success manifest.
Failed attempt directories/raw/response receipts are retained for diagnosis;
they are not deleted on failure and must never be reused or overwritten. A
post-publication pending-file cleanup failure may
leave the verified pending copy alongside the valid receipt. This design does
not promise power-loss durability or a distributed transaction.

The receipt records the raw sizes/SHA-256 values, response URLs and completion
times, fixed identity, Accepted/timezone, combined binding hash, request limits,
and holding-status counts. It explicitly records:

```json
{
  "qualification": "raw_capture_unqualified",
  "raw_xsd_validation": "not_validated",
  "first_public_visibility_proven": false,
  "historical_pit_proven": false,
  "production_eligible": false,
  "trading_eligible": false
}
```

The binder's existing `synthetic_subset_not_verified_sec_sample` schema claim is
retained unchanged. Receipt success does not upgrade that claim. It only records
what this execution completely received at its observation time and verified
locally. It does not prove SEC's first public visibility, prior historical
availability, full equity-universe completeness, full raw XSD validity, corporate
action/terminal-price evidence, or production/trading qualification.

## Offline checks and pending acceptance

`tests/test_capture_iwb_sec_filing_once.py` constructs all HTML/XML in the test;
its fake opener, clock, and storage never read a real filing or make a request.
Local filesystem tests verify exact bytes, create-only behavior, corrupted
readbacks, parser-failure evidence retention, rejection without fabricated
response receipts, storage gating, UTC completion times, and atomic success
manifest publication. Run the focused regression checks
with the repository's configured offline Python environment:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:. python -m pytest -q \
  tests/test_capture_iwb_sec_filing_once.py \
  tests/test_russell_1000_proxy_long_history.py
```

Before any real execution can be called an accepted collection loop, the reviewer
must still accept all of the following:

- Normal authorized SEC access from the intended runtime/network path
- The execution operator's explicitly configured real client identity/User-Agent
- Complete real index/XML responses and a trustworthy response-completion clock
- The actual raw XML and the relevant official N-PORT XSD/schema/version review;
  this script neither downloads nor validates that XSD
- Review of raw identity/period/namespace/holding statuses and successful local
  exact-byte readbacks; failure/challenge must stop without evasive access

Official review references (provided for later acceptance, not accessed by these
tests):

- [SEC technical specifications](https://www.sec.gov/submit-filings/technical-specifications)
- [Accessing EDGAR data](https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data)
- [SEC webmaster FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions)
