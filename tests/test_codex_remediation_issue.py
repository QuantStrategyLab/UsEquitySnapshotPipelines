from __future__ import annotations

from scripts.post_codex_remediation_issue import (
    AUTO_MERGE_LABEL,
    CODEX_LABEL,
    TASK_LABEL,
    build_issue_body,
    build_issue_title,
)


def test_build_issue_body_queues_ccbot_codex_with_guardrails() -> None:
    body = build_issue_body(
        source_issue_number=12,
        source_issue_title="Monthly Snapshot AI Review: 2026-04",
        source_issue_url="https://github.com/example/repo/issues/12",
        review_markdown="## English\nReview",
    )

    assert "<!-- codex-monthly-remediation:12 -->" in body
    assert "ccbot-bridge" in body
    assert "codex/monthly-review-issue-12" in body
    assert "<!-- codex-monthly-remediation:issue-12 -->" in body
    assert AUTO_MERGE_LABEL in body
    assert "strategy selection logic" in body
    assert "Open a draft PR first" in body


def test_constants_match_expected_labels() -> None:
    assert CODEX_LABEL == "codex-bridge"
    assert TASK_LABEL == "monthly-codex-remediation"
    assert build_issue_title(12, "Review") == "Codex Monthly Remediation: #12 Review"
