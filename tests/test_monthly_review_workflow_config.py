from __future__ import annotations

from pathlib import Path


MONTHLY_REVIEW = Path(".github/workflows/monthly_review.yml")
AI_REVIEW = Path(".github/workflows/ai_review.yml")


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
    assert "auto_merge" not in workflow


def test_ai_review_workflow_posts_bilingual_review_comment() -> None:
    workflow = AI_REVIEW.read_text(encoding="utf-8")

    assert "anthropics/claude-code-action@v1" in workflow
    assert "UsEquitySnapshotPipelines" in workflow
    assert "Return only the final bilingual review" in workflow
    assert "scripts/render_monthly_ai_review.py" in workflow
    assert "scripts/post_monthly_ai_review_comment.py" in workflow
    assert "Do not use Bash" in workflow
