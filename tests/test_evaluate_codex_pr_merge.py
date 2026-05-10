from __future__ import annotations

from scripts.evaluate_codex_pr_merge import AUTO_MERGE_LABEL, evaluate_changed_files, evaluate_pr


def test_evaluate_changed_files_allows_only_monthly_review_surface() -> None:
    allowed = evaluate_changed_files(
        [
            "README.md",
            "docs/operator_runbook.md",
            "tests/test_monthly_report_bundle.py",
            "scripts/run_monthly_report_bundle.py",
            ".github/workflows/monthly_review.yml",
            ".github/workflows/codex_pr_feedback.yml",
        ]
    )
    blocked = evaluate_changed_files(["src/us_equity_snapshot_pipelines/contracts.py", "pyproject.toml"])

    assert allowed["allowed"]
    assert not blocked["allowed"]
    assert blocked["blocked_files"] == ["src/us_equity_snapshot_pipelines/contracts.py", "pyproject.toml"]


def test_evaluate_pr_requires_marker_label_non_draft_and_allowed_files() -> None:
    pr = {
        "isDraft": False,
        "body": "<!-- codex-monthly-remediation:issue-12 -->",
        "url": "https://github.com/example/repo/pull/1",
        "labels": [{"name": AUTO_MERGE_LABEL}],
        "files": [{"path": "docs/operator_runbook.md"}],
    }

    decision = evaluate_pr(pr)

    assert decision["should_merge"]
    assert decision["reason"] == "ready"


def test_evaluate_pr_blocks_draft_or_sensitive_files() -> None:
    draft = evaluate_pr(
        {
            "isDraft": True,
            "body": "<!-- codex-monthly-remediation:issue-12 -->",
            "labels": [{"name": AUTO_MERGE_LABEL}],
            "files": [{"path": "docs/operator_runbook.md"}],
        }
    )
    sensitive = evaluate_pr(
        {
            "isDraft": False,
            "body": "<!-- codex-monthly-remediation:issue-12 -->",
            "labels": [{"name": AUTO_MERGE_LABEL}],
            "files": [{"path": "src/us_equity_snapshot_pipelines/contracts.py"}],
        }
    )

    assert not draft["should_merge"]
    assert draft["reason"] == "draft_pr"
    assert not sensitive["should_merge"]
    assert sensitive["reason"] == "blocked_files"
