# SOXL P1--P3 Daily Research Driver

The scheduled driver attempts one new candidate-bound SOXL P1 root after the
preceding XNYS close.  It derives the cutoff from the pinned XNYS calendar,
uses only the frozen P2 v3 identity, and uploads raw P1 members create-only.
`p1-complete.json` is uploaded last, so P3 accepts a remote root only after it
binds the exact local member hashes and manifest digest.

Provider availability outcomes are successful `DEFERRED` terminal records;
malformed or contract-invalid inputs are successful `PARKED` terminal records.
Both are visible as short-lived sanitized workflow artifacts and the next
scheduled date is tried normally.  They do not tune, use a fallback provider,
or replay an uncompleted remote root.

## Optional portable decision-data projection

After an accepted native P1 root has been stored, the driver may derive a
separate private, create-only daily `decision-price-series.json` projection.
It verifies the native root and its `bars.json` hash, adapts only the
SOXL/SOXX daily close-and-volume series, then verifies the parent root again
before publishing.  The derived manifest commits the parent P1 manifest digest.
The data member uploads before the manifest so an execution-side reader treats
the manifest as the completion record.

The object is confined to the private short-lived
`decision-data-projections/v1/us_equity/soxl_soxx_trend_income/` prefix.  It
does not publish raw data to GitHub, expose a storage path to the control
plane, alter P1/P3, change strategy parameters, or confer P4/P5/P6 or order
authority.  This is an `artifact_optional` observer stage: any derivation or
upload problem records `DECISION_DATA_PROJECTION_STATUS=PARKED` while the
existing accepted P1/P3 path continues.  A later execution adapter must verify
the projection independently before it could consume it.

The fixed Alpaca SIP transport has one small exception for a transient HTTP
`403`: it waits 60 seconds and resubmits the exact same request once.  Its
source, cutoff, parameters, and data identity remain unchanged.  A second
`403`, or any other first failure, remains the normal sanitized `DEFERRED`
outcome; it never selects another provider or retries indefinitely.

The existing short-lived P1 terminal artifact also records the sanitized retry
state `NOT_TRIGGERED`, `SIP_403_RECOVERED`, or `SIP_403_EXHAUSTED`, alongside
the P1 status and reason code.  It contains no raw bars, credentials, account
data, orders, or authority.

Only an accepted root reaches P3.  P3 checks the remote completion marker,
uses an exact detached UES source revision, runs the fixed evidence plan, and
uploads metrics-and-hashes plus the fixed OOS performance observation.  It
does not write orders or authorize P4, P5, or P6.  A future control-plane
source registration may read the sanitized performance artifact for issue-only
AI diagnosis after at least two completed records exist.
