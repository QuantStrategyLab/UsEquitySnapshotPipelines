from __future__ import annotations

from pathlib import Path


MONTHLY_REVIEW = Path(".github/workflows/monthly_review.yml")
CODEX_FEEDBACK = Path(".github/workflows/codex_pr_feedback.yml")
PUBLISH_SNAPSHOT_ARTIFACTS = Path(".github/workflows/publish-snapshot-artifacts.yml")
UPDATE_SOURCE_INPUT_DATA = Path(".github/workflows/update-source-input-data.yml")


def test_monthly_review_workflow_creates_issue_and_triggers_codex_first() -> None:
    workflow = MONTHLY_REVIEW.read_text(encoding="utf-8")

    assert "Publish Snapshot Artifacts" in workflow
    assert "github.event.workflow_run.event == 'workflow_run'" in workflow
    assert "contains(fromJSON('[\"schedule\",\"workflow_run\"]'), github.event.workflow_run.event)" not in workflow
    assert "actions: write" in workflow
    assert "Install monthly review dependencies" in workflow
    assert 'python -m pip install "pandas>=2.0"' in workflow
    assert workflow.index("Install monthly review dependencies") < workflow.index("Build live strategy health reports")
    assert workflow.index("Build live strategy health reports") < workflow.index("Build live decay monitors")
    assert workflow.index("Build live decay monitors") < workflow.index("Build monthly review bundle")
    assert "gh run download" in workflow
    assert "scripts/build_monthly_live_strategy_health_reports.py" in workflow
    assert "scripts/build_monthly_live_decay_monitors.py" in workflow
    assert "--output-root \"${artifact_root}\"" in workflow
    assert "scripts/run_monthly_report_bundle.py" in workflow
    assert "scripts/post_monthly_ai_review_issue.py" in workflow
    assert "Ensure guarded auto-merge labels" in workflow
    assert "scripts/sync_codex_auto_merge_labels.py" in workflow
    assert "scripts/check_codex_auto_merge_readiness.py" in workflow
    assert "scripts/plan_codex_auto_merge_enablement.py" in workflow
    assert "scripts/post_codex_auto_merge_preflight_comment.py" in workflow
    assert "--auto-merge \"${CODEX_AUDIT_AUTO_MERGE}\"" in workflow
    assert "CODEX_AUDIT_REQUIRED_STATUS_CHECKS: ${{ vars.CODEX_AUDIT_REQUIRED_STATUS_CHECKS || 'test' }}" in workflow
    assert '--required-status-checks "${CODEX_AUDIT_REQUIRED_STATUS_CHECKS}"' in workflow
    assert "codex_auto_merge_readiness.md" in workflow
    assert "codex_auto_merge_label_sync.md" in workflow
    assert "codex_auto_merge_enablement_plan.md" in workflow
    assert "--readiness-file data/output/monthly_report_bundle/codex_auto_merge_readiness.md" in workflow
    assert "--label-sync-file data/output/monthly_report_bundle/codex_auto_merge_label_sync.md" in workflow
    assert "--enablement-plan-file data/output/monthly_report_bundle/codex_auto_merge_enablement_plan.md" in workflow
    assert "--output-file data/output/monthly_report_bundle/codex_auto_merge_preflight_comment.md" in workflow
    assert "CODEX_AUDIT_READINESS_TOKEN || secrets.GITHUB_TOKEN" in workflow
    assert "id: auto_merge_readiness" in workflow
    assert "continue-on-error: true" in workflow
    assert "AUTO_MERGE_REQUESTED: ${{ env.CODEX_AUDIT_AUTO_MERGE }}" in workflow
    assert "AUTO_MERGE_READINESS_OUTCOME: ${{ steps.auto_merge_readiness.outcome || 'skipped' }}" in workflow
    assert 'auto_merge_requested = enabled("AUTO_MERGE_REQUESTED")' in workflow
    assert 'auto_merge_ready = os.environ.get("AUTO_MERGE_READINESS_OUTCOME", "").strip() == "success"' in workflow
    assert "auto_merge = auto_merge_requested and auto_merge_ready" in workflow
    assert "dispatching Codex audit with auto_merge=false" in workflow
    assert '"auto_merge": str(auto_merge).lower()' in workflow
    assert "AUTO_MERGE: ${{ env.CODEX_AUDIT_AUTO_MERGE }}" not in workflow
    audit_enabled_condition = "if: success() && contains(fromJSON('[\"true\",\"True\",\"TRUE\"]'), env.CODEX_AUDIT_ENABLED)"
    assert workflow.count(audit_enabled_condition) == 6
    assert "env.CODEX_AUDIT_ENABLED != 'false'" not in workflow
    assert "monthly-review" in workflow
    assert "CODEX_AUDIT_ENABLED" in workflow
    assert "CODEX_AUDIT_BRIDGE_REF" in workflow
    assert '"ref": os.environ["CODEX_AUDIT_BRIDGE_REF"]' in workflow
    assert "CodexAuditBridge" in workflow
    assert "monthly-snapshot-review-${{ github.ref_name }}" in workflow
    assert "cancel-in-progress: false" in workflow
    assert "CODEX_AUDIT_DISPATCH_TOKEN" in workflow
    assert "permission-actions: write" in workflow
    assert "CODEX_AUDIT_PROVIDER" in workflow
    assert "CODEX_AUDIT_PROVIDER || 'auto'" in workflow
    assert '"provider": provider' in workflow
    assert '"anthropic"' in workflow
    assert '"api"' in workflow
    assert "codex_audit.yml" in workflow
    assert "actions/workflows/codex_audit.yml/dispatches" in workflow
    assert workflow.index("Ensure guarded auto-merge labels") < workflow.index("Check guarded auto-merge readiness")
    assert workflow.index("Check guarded auto-merge readiness") < workflow.index("Trigger Monthly Review Automation")
    assert workflow.index("Build guarded auto-merge enablement plan") < workflow.index("Trigger Monthly Review Automation")
    assert workflow.index("Comment guarded auto-merge preflight summary") < workflow.index("Trigger Monthly Review Automation")
    assert workflow.index("codex_auto_merge_enablement_plan.md") < workflow.index("Upload monthly review bundle")
    assert workflow.index("codex_auto_merge_label_sync.md") < workflow.index("Upload monthly review bundle")
    assert workflow.index("codex_auto_merge_preflight_comment.md") < workflow.index("Upload monthly review bundle")
    assert workflow.index("Trigger Monthly Review Automation") < workflow.index("Upload monthly review bundle")
    assert "if: always()" in workflow
    assert "monthly-snapshot-review-${{ steps.bundle.outputs.report_month || 'unknown' }}" in workflow
    assert "if-no-files-found: warn" in workflow
    assert "Codex audit bridge dispatch is disabled by CODEX_AUDIT_ENABLED=false" in workflow
    assert 'raise RuntimeError("Codex audit bridge dispatch is disabled' not in workflow
    assert "LEGACY_AI_REVIEW_ENABLED" not in workflow
    assert "actions/workflows/ai_review.yml/dispatches" not in workflow
    assert "/repos/{target_repository}/dispatches" not in workflow
    assert "gh workflow run ai_review.yml" not in workflow
    assert "auto_optimization" not in workflow


def test_source_local_legacy_ai_review_workflow_is_removed() -> None:
    assert not Path(".github/workflows/ai_review.yml").exists()


def test_auto_merge_workflow_requires_codex_branch_and_guarded_label() -> None:
    workflow = Path(".github/workflows/auto_merge_codex_pr.yml").read_text(encoding="utf-8")

    assert "Auto Merge Codex Remediation PR" in workflow
    assert "contents: write" in workflow
    assert "issues: write" in workflow
    assert "pull-requests: write" in workflow
    assert "codex/monthly-review-issue-" in workflow
    assert "vars.CODEX_AUDIT_AUTO_MERGE" in workflow
    assert "vars.CODEX_AUDIT_REQUIRED_STATUS_CHECKS" in workflow
    assert '["true","True","TRUE"]' in workflow
    assert "github.event.workflow_run.head_repository.full_name == github.repository" in workflow
    assert workflow.count('--repo "${{ github.repository }}"') == 6
    assert "--json number,headRefOid,headRepository,isCrossRepository" in workflow
    assert ".headRepository.nameWithOwner" in workflow
    assert "No same-repository open PR found" in workflow
    assert "pr_head_sha" in workflow
    assert "github.event.workflow_run.head_sha" in workflow
    assert "Skipping auto-merge for stale CI success" in workflow
    assert "ref: ${{ github.event.repository.default_branch }}" in workflow
    assert (
        "files,changedFiles,additions,deletions,reviewDecision,labels,baseRefName,headRefName,"
        "headRepositoryOwner,headRepository,isCrossRepository"
    ) in workflow
    assert '--expected-base-ref "${{ github.event.repository.default_branch }}"' in workflow
    assert '--expected-head-ref "${{ github.event.workflow_run.head_branch }}"' in workflow
    assert '--expected-head-owner "${{ github.repository_owner }}"' in workflow
    assert '--expected-head-repository "${{ github.repository }}"' in workflow
    assert "--require-same-repository" in workflow
    assert "scripts/evaluate_codex_pr_merge.py" in workflow
    assert "Comment auto-merge guard decision" in workflow
    assert "scripts/post_codex_auto_merge_decision_comment.py" in workflow
    assert "--decision-json data/output/codex_auto_merge/decision.json" in workflow
    assert "--output-file data/output/codex_auto_merge/guard_decision_comment.md" in workflow
    assert "--sync-labels" in workflow
    assert "Check guarded auto-merge readiness before merge" in workflow
    assert "id: merge_readiness" in workflow
    assert "scripts/check_codex_auto_merge_readiness.py" in workflow
    assert "CODEX_AUDIT_READINESS_TOKEN || secrets.GITHUB_TOKEN" in workflow
    assert "CODEX_AUDIT_REQUIRED_STATUS_CHECKS: ${{ vars.CODEX_AUDIT_REQUIRED_STATUS_CHECKS || 'test' }}" in workflow
    assert '--required-status-checks "${CODEX_AUDIT_REQUIRED_STATUS_CHECKS}"' in workflow
    assert "--auto-merge true" in workflow
    assert "--summary-file data/output/codex_auto_merge/readiness.md" in workflow
    assert "Comment merge-time readiness failure" in workflow
    assert "readiness_decision.json" in workflow
    assert "readiness_guard_decision_comment.md" in workflow
    assert "merge_readiness_failed" in workflow
    assert "Fail on merge-time readiness failure" in workflow
    assert "steps.merge_readiness.outcome == 'success'" in workflow
    assert workflow.index("Comment auto-merge guard decision") < workflow.index("Check guarded auto-merge readiness before merge")
    assert workflow.index("Check guarded auto-merge readiness before merge") < workflow.index("Merge Codex remediation PR")
    assert workflow.index("Comment merge-time readiness failure") < workflow.index("Fail on merge-time readiness failure")
    assert workflow.index("Fail on merge-time readiness failure") < workflow.index("Merge Codex remediation PR")
    assert "gh pr merge" in workflow
    assert "--match-head-commit" in workflow
    assert "github.event.workflow_run.head_sha" in workflow
    assert "Upload Codex auto-merge diagnostics" in workflow
    assert "uses: actions/upload-artifact@v7" in workflow
    assert "codex-auto-merge-${{ github.run_id }}" in workflow
    assert "path: data/output/codex_auto_merge/" in workflow
    assert "if-no-files-found: warn" in workflow
    assert "auto-merge-ok" not in workflow  # label check lives in evaluate_codex_pr_merge.py


def test_codex_feedback_workflow_requeues_failed_ci_and_review_feedback() -> None:
    workflow = CODEX_FEEDBACK.read_text(encoding="utf-8")

    assert "workflow_run:" in workflow
    assert "pull_request_review:" in workflow
    assert "contents: read" in workflow
    assert workflow.count("uses: actions/checkout@v6") == 2
    assert "codex/monthly-review-issue-" in workflow
    assert "CODEX_AUDIT_BRIDGE_REPOSITORY" in workflow
    assert "CODEX_AUDIT_DISPATCH_TOKEN" in workflow
    assert "github.event.workflow_run.head_repository.full_name == github.repository" in workflow
    assert "github.event.pull_request.head.repo.full_name == github.repository" in workflow
    assert 'gh pr list --repo "${GITHUB_REPOSITORY}"' in workflow
    assert "--json number,title,url,body,headRefOid,headRepository,isCrossRepository" in workflow
    assert ".headRepository.nameWithOwner" in workflow
    assert "Skipping stale CI failure because the workflow_run head SHA no longer matches the current PR head" in workflow
    assert 'gh api --paginate --slurp' in workflow
    assert '"/repos/${GITHUB_REPOSITORY}/issues/${issue_number}/comments?per_page=100"' in workflow
    assert "data/output/codex_feedback/comment_pages.json" in workflow
    assert 'gh issue view "${issue_number}" --repo "${GITHUB_REPOSITORY}" --comments' not in workflow
    assert 'gh issue edit "${issue_number}" --repo "${GITHUB_REPOSITORY}"' in workflow
    assert 'gh issue comment "${issue_number}" --repo "${GITHUB_REPOSITORY}"' in workflow
    assert "Dispatch Codex feedback retry" in workflow
    assert "gh workflow run codex_audit.yml" in workflow
    assert workflow.count("Check guarded auto-merge readiness") == 2
    assert workflow.count("id: auto_merge_readiness") == 2
    assert workflow.count("CODEX_AUDIT_READINESS_TOKEN || secrets.GITHUB_TOKEN") == 2
    assert "CODEX_AUDIT_REQUIRED_STATUS_CHECKS: ${{ vars.CODEX_AUDIT_REQUIRED_STATUS_CHECKS || 'test' }}" in workflow
    assert workflow.count('--required-status-checks "${CODEX_AUDIT_REQUIRED_STATUS_CHECKS}"') == 2
    assert workflow.count("continue-on-error: true") >= 2
    assert "scripts/check_codex_auto_merge_readiness.py" in workflow
    assert "--summary-file data/output/codex_feedback/codex_auto_merge_readiness.md" in workflow
    assert "AUTO_MERGE_REQUESTED: ${{ env.CODEX_AUDIT_AUTO_MERGE }}" in workflow
    assert "AUTO_MERGE_READINESS_OUTCOME: ${{ steps.auto_merge_readiness.outcome || 'skipped' }}" in workflow
    assert "auto_merge_requested=\"false\"" in workflow
    assert "dispatching Codex feedback retry with auto_merge=false" in workflow
    assert "AUTO_MERGE: ${{ env.CODEX_AUDIT_AUTO_MERGE }}" not in workflow
    assert '--field issue_number="${ISSUE_NUMBER}"' in workflow
    assert '--field task="monthly_snapshot_audit"' in workflow
    assert "steps.feedback.outputs.dispatch_feedback == 'true'" in workflow
    assert "codex-monthly-remediation:issue-" in workflow
    assert "gh issue comment" in workflow
    assert "CODEX_AUDIT_MAX_FEEDBACK_ROUNDS" in workflow
    assert workflow.count("MAX_CODEX_FEEDBACK_ROUNDS: ${{ vars.CODEX_AUDIT_MAX_FEEDBACK_ROUNDS || '3' }}") == 2
    assert workflow.count('configured_max_rounds = int(os.environ.get("MAX_CODEX_FEEDBACK_ROUNDS", "3"))') == 2
    assert workflow.count("configured_max_rounds = 3") == 2
    assert workflow.count("max_rounds = min(max(configured_max_rounds, 1), 10)") == 2
    assert "gh issue edit" in workflow
    assert 'auto_merge_label' in workflow
    assert 'human_review_label' in workflow
    assert workflow.count("load_policy_labels(DEFAULT_POLICY_PATH)") == 2
    assert workflow.count('if [ -n "${guard_label}" ]; then') == 2
    assert workflow.count('if [ -n "${human_review_label}" ]; then') == 2
    assert workflow.count("Skipping stale guarded auto-merge label cleanup") == 2
    assert workflow.count("Skipped stale guarded auto-merge label cleanup") == 2
    assert '--remove-label "${guard_label}"' in workflow
    assert workflow.count('gh label create "${human_review_label}"') == 2
    assert "--color d93f0b" in workflow
    assert "Codex remediation PR requires human review before merge." in workflow
    assert '--add-label "${human_review_label}"' in workflow
    assert workflow.count("Removed stale guarded auto-merge label") == 2
    assert workflow.count("Marked PR #") == 2
    assert workflow.count("Skipped adding human-review label after retry limit") == 2
    assert "--remove-label codex-bridge" in workflow
    assert workflow.count("Upload Codex feedback diagnostics") == 2
    assert workflow.count("uses: actions/upload-artifact@v7") == 2
    assert "codex-pr-feedback-ci-${{ github.run_id }}" in workflow
    assert "codex-pr-feedback-review-${{ github.run_id }}" in workflow
    assert workflow.count("path: data/output/codex_feedback/") == 2
    assert workflow.count("if-no-files-found: warn") == 2
    assert "Codex PR Retry Limit Reached" in workflow
    assert workflow.count("will try to mark the PR with the configured human-review label") == 2
    assert "re-apply `codex-bridge` only if another automated Codex pass is still appropriate" in workflow
    assert "Codex PR CI Feedback" in workflow
    assert "Codex PR Review Feedback" in workflow


def test_automated_snapshot_publish_runs_after_source_input_refresh() -> None:
    workflow = PUBLISH_SNAPSHOT_ARTIFACTS.read_text(encoding="utf-8")

    assert "Verify main CI succeeded before publish" in workflow
    assert "bash .github/scripts/verify_main_ci_success.sh" in workflow
    assert "workflow_run:" in workflow
    assert 'workflows: ["Update Source Input Data"]' in workflow
    assert "cron: '45 0 1 * *'" not in workflow
    assert "github.event.workflow_run.conclusion == 'success'" in workflow
    assert "github.event.workflow_run.event == 'schedule'" in workflow
    assert '[ "${GITHUB_EVENT_NAME}" = "workflow_run" ]' in workflow

    matrix_line = next(line for line in workflow.splitlines() if "fromJSON(github.event_name == 'schedule'" in line)
    workflow_run_matrix = matrix_line.split("github.event_name != 'workflow_dispatch'", maxsplit=1)[1]
    assert '["global_etf_rotation"]' in matrix_line
    assert '["russell_top50_leader_rotation"]' in workflow_run_matrix
    scheduled_matrix = matrix_line
    assert "russell_1000_multi_factor_defensive" not in scheduled_matrix
    assert "tech_communication_pullback_enhancement" not in scheduled_matrix
    assert "tech_communication_pullback_enhancement" not in workflow
    assert "mega_cap_leader_rotation_dynamic_top20" not in scheduled_matrix
    assert "mega_cap_leader_rotation_aggressive" not in scheduled_matrix
    assert "dynamic_mega_leveraged_pullback" not in scheduled_matrix


def test_manual_source_input_publish_dispatches_live_snapshot_profiles() -> None:
    workflow = UPDATE_SOURCE_INPUT_DATA.read_text(encoding="utf-8")

    assert "actions: write" in workflow
    assert "Trigger snapshot artifact publish for manual refresh" in workflow
    assert "github.event_name == 'workflow_dispatch' && env.EXECUTE_PUBLISH == 'true'" in workflow
    assert "gh workflow run publish-snapshot-artifacts.yml" in workflow
    assert '--field profile="russell_1000_multi_factor_defensive"' not in workflow
    assert '--field universe_path="${OUTPUT_PREFIX%/}/r1000_universe_history.csv"' not in workflow
    assert '--field profile="russell_top50_leader_rotation"' in workflow
    assert '--field universe_path="${OUTPUT_PREFIX%/}/r1000_latest_holdings_snapshot.csv"' in workflow
    assert '--field source_input_manifest_path="${source_input_manifest_path}"' in workflow
    assert '--field execute_publish="true"' in workflow
