# Snapshot AI review policy

## Current boundary

GitHub Codex App is the repository's sole AI reviewer. The retired AIAuditBridge
review, retry, remediation, and merge automation is not an active repository
path and must not be re-enabled through repository variables, secrets, or
workflow dispatch.

## Monthly report generation

`Monthly Snapshot Review` is a disabled report-only workflow pending a separate
activation decision. When enabled by that future decision, it may build the
existing monthly evidence bundle, publish the artifact, and create or update a
`monthly-review` issue. It does not dispatch an AI reviewer, retry review
feedback, create remediation pull requests, or prepare or perform auto-merge.

The generated evidence is advisory. It is not an approval, trading, provider,
or automated merge signal. GitHub Codex App review and any merge settlement
remain independently bounded and manually controlled.

## Cleanup boundary

Retired AIAuditBridge workflow files are removed in Phase A. Inert scripts,
policy files, repository variables, and secret names are a separately tracked
Phase B cleanup backlog; this policy does not authorize their deletion or
configuration changes.
