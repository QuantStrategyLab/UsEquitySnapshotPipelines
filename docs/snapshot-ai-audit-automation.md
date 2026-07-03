# Snapshot AI audit automation policy

This policy defines how the monthly snapshot AI audit can run mostly unattended
while keeping high-risk operations behind human review.

## Target flow

1. `Publish Snapshot Artifacts` succeeds on a scheduled source-input refresh.
2. `Monthly Snapshot Review` downloads artifacts, builds live strategy health
   evidence, creates/updates the monthly review issue, and dispatches
   `AIAuditBridge`.
3. `AIAuditBridge` reviews the issue and may open a remediation PR for safe,
   focused fixes.
4. Source-repository CI must pass.
5. `Auto Merge Codex Remediation PR` evaluates the PR marker, labels, file
   surface, and risk level before merging.

The merge gate is fail-closed: any file outside the monthly-review allowlist is
treated as high-risk and skipped.
The PR marker must also match the issue number encoded in the Codex branch name
(`codex/monthly-review-issue-<issue>-...`); a generic or mismatched monthly
marker is not enough to merge.
The workflow resolves only same-repository PRs for the Codex branch before
running the merge guard, then the guard independently verifies the PR head owner
and repository again.
The auto-merge workflow also requires the successful CI `workflow_run` head
repository to match the source repository before the job starts.
It resolves the current same-repository PR head and skips stale successful CI
runs whose `workflow_run.head_sha` no longer matches that current PR head.
The merge command is pinned with `--match-head-commit` to the CI
`workflow_run.head_sha`, so a PR branch pushed after the successful CI run is not
merged by that older run.
The configured human-review label is a hard veto: a PR carrying
`human-review-required` will not be auto-merged even if it also carries
`auto-merge-ok`.
When the merge guard skips a PR, the auto-merge workflow upserts a concise
decision comment back to the monthly review issue so operators can see the
reason and risk level without opening workflow artifacts first.
The same skip path upserts the guard-decision comment first, then removes
any stale configured auto-merge label from that PR and adds the configured
human-review label when the guard classified the risk as high. Label hygiene is
best-effort after the comment is written; the action results are appended back
to the same guard-decision comment and written to the diagnostic artifact when
possible. If the follow-up comment update fails, the diagnostic artifact records
that failure, and a permission or transient API failure does not hide the guard
decision itself. If the merge guard is ready but the final merge-time readiness check fails,
the workflow converts that failure into the same visible guard-decision comment
and label-hygiene path before failing the job, so operators do not need to open
artifacts first to see why the unattended merge stopped.

The changed-file allowlist is stored in
`.github/codex_auto_merge_policy.json`. The source merge guard reads this file,
and AIAuditBridge can read the same policy from its temporary source
checkout before adding `auto-merge-ok`. AIAuditBridge reads the baseline
policy before Codex edits run, so a remediation PR cannot grant itself a broader
auto-merge surface by editing the policy file. Keep this file in sync with any
new monthly-review automation surface. `blocked_path_patterns` are evaluated
before low-risk prefixes, so secret-like paths under `docs/` or `tests/` still
require human review. Invalid policy JSON or invalid blocked-path regular
expressions fail closed and require human review.
The same policy also defines `human_review_label` (`human-review-required` by
default), which is used for high-risk generated PRs.
`auto_merge_label` and `human_review_label` must be distinct; matching labels
make the policy invalid and fail closed.
Only `version: 1` is currently supported; missing or future policy versions are
also fail-closed until the guard code is intentionally updated.

## Risk tiers

| Tier | Examples | Automation policy |
| --- | --- | --- |
| Low | `docs/`, `tests/`, `README.md`, `README.zh-CN.md` | May be remediated and auto-merged when the PR has the Codex monthly marker, `auto-merge-ok`, is not draft, CI passed, no PR review is currently requesting changes, and the changed-file count plus total additions/deletions stay within `max_changed_files` (`20` by default) and `max_changed_lines` (`1200` by default). |
| Medium | Monthly-review evidence/reporting helpers such as `scripts/build_monthly_live_strategy_health_reports.py`, `scripts/run_monthly_report_bundle.py`, `scripts/post_monthly_ai_review_issue.py`, and the read-only enablement planner | May be remediated when narrowly scoped. Auto-merge is still gated by `auto-merge-ok`, CI, no active requested-changes review decision, the source merge guard summary, and the same changed-file / changed-line caps. |
| High | `src/` strategy logic, profile contracts, `pyproject.toml`, dependencies, secrets, runtime/broker settings, live allocation, publish permissions, data artifacts, workflow files, auto-merge policy files, auto-merge/readiness/merge-guard code, file removals/renames/copies, or strategy deletion/disablement | Never auto-merge. Keep the generated PR for review, label it `human-review-required`, and include evidence plus recommended next checks in the source issue. |

## Live strategy health evidence

The live strategy health report is advisory evidence only. A
`review_for_retirement` state can justify a human review task, but it must not
automatically remove, disable, or reallocate a strategy. Strategy retirement
still requires a separate review of out-of-sample evidence, costs, runtime
impact, and downstream broker behavior.

## Operational switches

- Keep `CODEX_AUDIT_ENABLED=true` to run the monthly audit automatically.
- Set `CODEX_AUDIT_AUTO_MERGE=true` only after branch protection or repository
  rulesets, CI, and the source merge guard are active.
- The source `Auto Merge Codex Remediation PR` workflow is also hard-gated by
  `CODEX_AUDIT_AUTO_MERGE`; when the variable is unset or not exactly `true`,
  `True`, or `TRUE`, the workflow skips even if a PR already has
  `auto-merge-ok`.
- `Monthly Snapshot Review` runs `scripts/check_codex_auto_merge_readiness.py`
  before dispatching AIAuditBridge. When `CODEX_AUDIT_AUTO_MERGE=true`, the
  readiness gate requires the local auto-merge workflow/policy guard surface,
  the Codex feedback retry workflow, the configured source labels (`auto-merge-ok` and `human-review-required` by
  default), a protected source branch or active ruleset, and the required CI
  status check context. The expected checks default to `test`; set repository
  variable `CODEX_AUDIT_REQUIRED_STATUS_CHECKS` to a comma- or newline-separated
  list if branch protection uses different check names.
  If any check is missing or unreadable, the workflow fails closed for guarded
  auto-merge by dispatching AIAuditBridge with `auto_merge=false`; the AI
  audit and PR creation path still runs.
- When guarded auto-merge is requested, `Monthly Snapshot Review` first runs
  `scripts/sync_codex_auto_merge_labels.py` to create the configured source
  labels if they are missing. This step is soft-fail: a token or permission
  problem is recorded in the monthly artifact bundle and the preflight issue
  comment, and readiness still downgrades `auto_merge=false`.
- If the default workflow token cannot read the readiness inputs in a deployed
  repository, configure `CODEX_AUDIT_READINESS_TOKEN` as a source-repository
  secret with read access to labels, branch protection, and status-check
  configuration. The token is used only by readiness checks; issue creation and
  bridge dispatch keep using their existing tokens.
- Use `scripts/plan_codex_auto_merge_enablement.py` to generate a read-only
  enablement plan and copyable branch-protection commands. The planner never
  mutates GitHub settings; apply the generated commands only after human review.
  It uses the same `CODEX_AUDIT_REQUIRED_STATUS_CHECKS` value as readiness when
  rendering branch-protection and verification commands.
- Ensure the bridge token can create/apply the configured auto-merge label
  (`auto-merge-ok` by default). AIAuditBridge creates the label on demand
  before applying it; if token permissions block label creation, create the
  label manually before enabling `CODEX_AUDIT_AUTO_MERGE=true`.
- Ensure the bridge token can also create/apply the high-risk review label
  (`human-review-required`). If this label cannot be applied, the source issue
  comment still records the failure and the PR remains without `auto-merge-ok`.
- `Codex PR Feedback` may dispatch a bounded retry only for same-repository
  Codex remediation PRs with a valid monthly remediation marker. The retry
  ignores stale CI-failure `workflow_run` events whose head SHA no longer
  matches the current PR head. For current failures or requested changes, it
  first removes any stale configured guarded auto-merge label from the PR on a
  best-effort basis, but only after validating the policy labels with the same
  distinct-label guard used by readiness. If the policy is invalid or the
  auto-merge and human-review labels collide, feedback skips label mutation
  rather than guessing. The retry then updates the existing PR branch when it is
  still open and tied to the same monthly issue; after the feedback-round limit,
  the workflow removes `codex-bridge`, marks the PR with the configured
  human-review label on a best-effort basis, creating that label first if it is
  missing and permissions allow it, and leaves the issue for manual review.
  Feedback retries run
  the same readiness gate as the monthly review; if guarded auto-merge is not
  ready, the retry still dispatches AIAuditBridge with `auto_merge=false`.
- Automatic feedback retries default to `3` rounds. Set repository variable
  `CODEX_AUDIT_MAX_FEEDBACK_ROUNDS` to adjust this without editing the workflow;
  invalid values fall back to `3`, and values are clamped to the safe range
  `1` to `10`.
- Treat workflow artifacts as the durable audit trail for unattended runs:
  `monthly-snapshot-review-<month>` keeps the review bundle, label-sync report,
  readiness report, enablement plan, and rendered preflight comment;
  `codex-pr-feedback-ci-<run>` and `codex-pr-feedback-review-<run>` keep retry
  inputs and rendered comments; `codex-auto-merge-<run>` keeps `pr.json`,
  `decision.json`, `summary.md`, `guard_decision_comment.md`, and merge-time
  `readiness.md`. Inspect these artifacts before manually rerunning a failed or
  skipped unattended step.
- Keep high-risk follow-ups manual even when the monthly issue was generated by
  an unattended run.

## Guarded auto-merge canary

- 2026-06-20: Ran a controlled same-repository canary through issue #117.
  The canary changes this documentation file only and uses the normal
  `codex/monthly-review-issue-*` branch, monthly remediation marker,
  `auto-merge-ok` label, CI, and source merge guard path.


## Required guardrails

- Do not expose or persist secrets in audit prompts, comments, logs, or PRs.
- Do not edit generated `data/` artifacts.
- Do not change live strategy behavior from a single monthly report.
- Do not remove, rename, or copy files through unattended remediation; create a review PR instead.
- Keep remediation PRs small and tied to concrete monthly-review evidence.
- If CI or review feedback repeats, stop after the configured retry limit and
  leave the issue for human review.
