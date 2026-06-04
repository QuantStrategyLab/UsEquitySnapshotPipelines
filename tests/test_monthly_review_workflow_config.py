from __future__ import annotations

from pathlib import Path


MONTHLY_REVIEW = Path(".github/workflows/monthly_review.yml")
CODEX_FEEDBACK = Path(".github/workflows/codex_pr_feedback.yml")
PUBLISH_SNAPSHOT_ARTIFACTS = Path(".github/workflows/publish-snapshot-artifacts.yml")
UPDATE_SOURCE_INPUT_DATA = Path(".github/workflows/update-source-input-data.yml")


def test_monthly_review_workflow_creates_issue_and_triggers_codex_first() -> None:
    workflow = MONTHLY_REVIEW.read_text(encoding="utf-8")

    assert "Publish Snapshot Artifacts" in workflow
    assert "contains(fromJSON('[\"schedule\",\"workflow_run\"]'), github.event.workflow_run.event)" in workflow
    assert "actions: write" in workflow
    assert "gh run download" in workflow
    assert "scripts/run_monthly_report_bundle.py" in workflow
    assert "scripts/post_monthly_ai_review_issue.py" in workflow
    assert "monthly-review" in workflow
    assert "SELFHOSTED_CODEX_REVIEW_ENABLED" in workflow
    assert "CodexAuditBridge" in workflow
    assert "monthly-snapshot-review-${{ github.ref_name }}" in workflow
    assert "cancel-in-progress: false" in workflow
    assert "CODEX_AUDIT_DISPATCH_TOKEN" in workflow
    assert "permission-actions: write" in workflow
    assert "SELFHOSTED_CODEX_REVIEW_PROVIDER" in workflow
    assert "SELFHOSTED_CODEX_REVIEW_PROVIDER || 'auto'" in workflow
    assert '"provider": provider' in workflow
    assert '"anthropic"' in workflow
    assert '"api"' in workflow
    assert "selfhosted_monthly_review.yml" in workflow
    assert "actions/workflows/selfhosted_monthly_review.yml/dispatches" in workflow
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
    assert "codex/monthly-review-issue-" in workflow
    assert "scripts/evaluate_codex_pr_merge.py" in workflow
    assert "gh pr merge" in workflow
    assert "auto-merge-ok" not in workflow  # label check lives in evaluate_codex_pr_merge.py


def test_codex_feedback_workflow_requeues_failed_ci_and_review_feedback() -> None:
    workflow = CODEX_FEEDBACK.read_text(encoding="utf-8")

    assert "workflow_run:" in workflow
    assert "pull_request_review:" in workflow
    assert "codex/monthly-review-issue-" in workflow
    assert "codex-monthly-remediation:issue-" in workflow
    assert "gh issue comment" in workflow
    assert 'MAX_CODEX_FEEDBACK_ROUNDS: "3"' in workflow
    assert "gh issue edit" in workflow
    assert "--remove-label codex-bridge" in workflow
    assert "Codex PR Retry Limit Reached" in workflow
    assert "Codex PR CI Feedback" in workflow
    assert "Codex PR Review Feedback" in workflow


def test_automated_snapshot_publish_runs_after_source_input_refresh() -> None:
    workflow = PUBLISH_SNAPSHOT_ARTIFACTS.read_text(encoding="utf-8")

    assert "workflow_run:" in workflow
    assert 'workflows: ["Update Source Input Data"]' in workflow
    assert "cron: '45 0 1 * *'" not in workflow
    assert "github.event.workflow_run.conclusion == 'success'" in workflow
    assert "github.event.workflow_run.event == 'schedule'" in workflow
    assert '[ "${GITHUB_EVENT_NAME}" = "workflow_run" ]' in workflow

    matrix_line = next(line for line in workflow.splitlines() if "fromJSON(github.event_name != 'workflow_dispatch'" in line)
    scheduled_matrix = matrix_line.split("|| format", maxsplit=1)[0]
    assert '["russell_1000_multi_factor_defensive","mega_cap_leader_rotation_top50_balanced"]' in scheduled_matrix
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
    assert '--field profile="russell_1000_multi_factor_defensive"' in workflow
    assert '--field universe_path="${OUTPUT_PREFIX%/}/r1000_universe_history.csv"' in workflow
    assert '--field profile="mega_cap_leader_rotation_top50_balanced"' in workflow
    assert '--field universe_path="${OUTPUT_PREFIX%/}/r1000_latest_holdings_snapshot.csv"' in workflow
    assert '--field source_input_manifest_path="${source_input_manifest_path}"' in workflow
    assert '--field execute_publish="true"' in workflow
