from __future__ import annotations

from pathlib import Path


MONTHLY_REVIEW = Path(".github/workflows/monthly_review.yml")
AI_REVIEW = Path(".github/workflows/ai_review.yml")
CODEX_FEEDBACK = Path(".github/workflows/codex_pr_feedback.yml")


def test_monthly_review_workflow_creates_issue_and_triggers_ai_review() -> None:
    workflow = MONTHLY_REVIEW.read_text(encoding="utf-8")

    assert "Publish Snapshot Artifacts" in workflow
    assert "actions: write" in workflow
    assert "gh run download" in workflow
    assert "scripts/run_monthly_report_bundle.py" in workflow
    assert "scripts/post_monthly_ai_review_issue.py" in workflow
    assert "monthly-review" in workflow
    assert "gh workflow run ai_review.yml" in workflow
    assert "auto_optimization" not in workflow


def test_ai_review_workflow_posts_bilingual_review_comment() -> None:
    workflow = AI_REVIEW.read_text(encoding="utf-8")

    assert "anthropics/claude-code-action@v1" in workflow
    assert "UsEquitySnapshotPipelines" in workflow
    assert "Return only the final bilingual review" in workflow
    assert "scripts/render_monthly_ai_review.py" in workflow
    assert "scripts/post_monthly_ai_review_comment.py" in workflow
    assert "scripts/post_codex_remediation_issue.py" in workflow
    assert "codex_issue" in workflow
    assert "Do not use Bash" in workflow


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
