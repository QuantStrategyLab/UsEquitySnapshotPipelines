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

Only an accepted root reaches P3.  P3 checks the remote completion marker,
uses an exact detached UES source revision, runs the fixed evidence plan, and
uploads metrics-and-hashes plus the fixed OOS performance observation.  It
does not write orders or authorize P4, P5, or P6.  A future control-plane
source registration may read the sanitized performance artifact for issue-only
AI diagnosis after at least two completed records exist.
