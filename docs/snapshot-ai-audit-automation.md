# Snapshot AI audit automation

## Current contract

GitHub Codex App is the sole AI reviewer for pull requests. AIAuditBridge
review, feedback-retry, and automatic-merge workflow paths are retired.

`Monthly Snapshot Review` is report-only: it produces the existing monthly
snapshot evidence bundle and a `monthly-review` issue. It does not dispatch an
AI reviewer, create remediation pull requests, retry feedback, or request or
perform automatic merges. Evidence is advisory only; it never authorizes a
merge, production change, or live action.

## Review and merge boundaries

Deterministic CI checks, strict repository rules, and human review requirements
remain authoritative. A clean Codex review is evidence for the applicable pull
request head only; it is not a bypass for required checks, unresolved findings,
or repository protections. Merge behavior and repository settings are governed
by separately authorized control-plane procedures.

## Legacy references

Some retained scripts, policy files, variables, and secrets may use historical
AIAudit names. They are not active workflow dispatch, retry, or merge paths.
This document does not claim their runtime configuration or permissions. Changes
to settings, rulesets, webhooks, secrets, variables, or permissions require a
separate authenticated authorization.
