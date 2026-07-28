from __future__ import annotations

from pathlib import Path


MONTHLY_REVIEW = Path(".github/workflows/monthly_review.yml")
CODEX_FEEDBACK = Path(".github/workflows/codex_pr_feedback.yml")
PUBLISH_SNAPSHOT_ARTIFACTS = Path(".github/workflows/publish-snapshot-artifacts.yml")
UPDATE_SOURCE_INPUT_DATA = Path(".github/workflows/update-source-input-data.yml")


def test_monthly_review_workflow_is_report_only_and_creates_issue() -> None:
    workflow = MONTHLY_REVIEW.read_text(encoding="utf-8")

    assert "Publish Snapshot Artifacts" in workflow
    assert "github.event.workflow_run.event == 'workflow_run'" in workflow
    assert "contents: read" in workflow
    assert "actions: read" in workflow
    assert "issues: write" in workflow
    assert "actions: write" not in workflow
    assert "Install monthly review dependencies" in workflow
    assert "Build monthly review bundle" in workflow
    assert "scripts/post_monthly_ai_review_issue.py" in workflow
    assert "Upload monthly review bundle" in workflow
    assert "AIAuditBridge" not in workflow
    assert "CODEX_AUDIT_" not in workflow
    assert "auto-merge" not in workflow.lower()
    assert "actions/create-github-app-token" not in workflow
    assert "actions/workflows/codex_audit.yml/dispatches" not in workflow


def test_retired_aiaudit_workflows_are_removed() -> None:
    assert not Path(".github/workflows/codex_pr_feedback.yml").exists()
    assert not Path(".github/workflows/auto_merge_codex_pr.yml").exists()
    assert not Path(".github/workflows/ai_review.yml").exists()

def test_retired_codex_bootstrap_paths_are_removed() -> None:
    for path in (
        ".github/codex_auto_merge_policy.json",
        "scripts/evaluate_codex_pr_merge.py",
        "scripts/gate_codex_app_review.py",
        "scripts/post_codex_auto_merge_decision_comment.py",
        "scripts/post_codex_auto_merge_preflight_comment.py",
        "scripts/sync_codex_auto_merge_labels.py",
        "scripts/static_pr_guard.py",
        "scripts/check_codex_auto_merge_readiness.py",
        "scripts/plan_codex_auto_merge_enablement.py",
        "tests/test_check_codex_auto_merge_readiness.py",
        "tests/test_plan_codex_auto_merge_enablement.py",
        "tests/test_evaluate_codex_pr_merge.py",
        "tests/test_gate_codex_app_review.py",
        "tests/test_post_codex_auto_merge_decision_comment.py",
        "tests/test_post_codex_auto_merge_preflight_comment.py",
        "tests/test_sync_codex_auto_merge_labels.py",
        "tests/test_run_codex_pr_review.py",
    ):
        assert not Path(path).exists()

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
