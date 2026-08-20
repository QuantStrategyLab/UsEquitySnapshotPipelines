# SOXL/SOXX Core-Only P2 v3 Research Candidate

P2 v3 is a new immutable research identity, not a mutation of the archived
P2 v2 candidate.  It preserves v2's source revision, runtime parameters,
evaluation windows, costs, and explicit exclusions.  The new identity records
that the UESP runtime lock now matches the frozen UES and QuantPlatformKit
source chain, and that candidate-bound data-only P1 plus offline P3 contracts
exist.

This admits only the next research step: a future daily driver may acquire a
fresh immutable P1 root and run the candidate-bound P3 replay.  Unavailable
or malformed inputs must record `DEFERRED` or `PARKED`; they must not tune
parameters or fall back to another provider.  P2 v3 still forbids paper,
shadow, live, broker orders, and automatic promotion.  Those later stages
need their own P4--P6 contracts and evidence.

The existing P1/P3 implementation remains bound to v2 until a follow-up
identity migration changes its input contract and isolated replay checks to
v3.  Therefore this document does not claim that v3 is scheduled yet.
