"""Tests for the read-only GitHub Codex App final-review evidence gate."""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import scripts.gate_codex_app_review as gate


HEAD_SHA = "a" * 40
BOT_REVIEWER = {
    "login": "chatgpt-codex-connector[bot]",
    "id": 199175422,
    "type": "Bot",
}
BOT_THREAD_AUTHOR = {
    "login": "chatgpt-codex-connector",
    "__typename": "Bot",
}


def pr(*, draft: bool = False, labels: list[str] | None = None, head: str = HEAD_SHA) -> dict[str, object]:
    return {
        "draft": draft,
        "head": {"sha": head},
        "labels": [{"name": label} for label in (labels or ["codex-final-review"])],
    }


def approved_review(*, commit_id: str = HEAD_SHA, reviewer: dict[str, object] | None = None, body: str = "") -> dict[str, object]:
    return {
        "id": 17,
        "commit_id": commit_id,
        "state": "APPROVED",
        "body": body,
        "user": reviewer or BOT_REVIEWER,
        "submitted_at": "2026-07-26T00:00:00Z",
        "html_url": "https://github.com/QuantStrategyLab/UsEquitySnapshotPipelines/pull/1#pullrequestreview-17",
    }


def assert_blocked(result: object, expected: str) -> None:
    assert result.exit_code == 1
    assert expected in result.title


def test_draft_pr_is_blocked() -> None:
    assert_blocked(gate.evaluate_final_review(pr(draft=True), [approved_review()], []), "draft")


def test_missing_explicit_final_review_intent_is_blocked() -> None:
    payload = pr()
    payload["labels"] = []
    assert_blocked(gate.evaluate_final_review(payload, [approved_review()], []), "intent")


@pytest.mark.parametrize("wrong_reviewer", [
    {**BOT_REVIEWER, "login": "untrusted[bot]"},
    {**BOT_REVIEWER, "id": 1},
])
def test_wrong_reviewer_identity_or_app_is_blocked(wrong_reviewer: dict[str, object]) -> None:
    assert_blocked(gate.evaluate_final_review(pr(), [approved_review(reviewer=wrong_reviewer)], []), "trusted")


def test_stale_review_head_is_blocked() -> None:
    assert_blocked(gate.evaluate_final_review(pr(), [approved_review(commit_id="b" * 40)], []), "current head")


@pytest.mark.parametrize("severity", ["P0", "P1", "P2"])
def test_unresolved_p0_p1_p2_findings_are_blocked(severity: str) -> None:
    threads = [{
        "isResolved": False,
        "comments": [{
            "review_id": 17,
            "body": f"![{severity} Badge](https://example.invalid/{severity}) finding",
            "author": BOT_REVIEWER,
        }],
    }]
    assert_blocked(gate.evaluate_final_review(pr(), [approved_review()], threads), "unresolved P0/P1/P2")


def test_graphql_comment_connection_is_normalized_before_finding_evaluation() -> None:
    threads = [{
        "isResolved": False,
        "comments": {"nodes": [{
            "pullRequestReview": {"databaseId": 99},
            "body": "P3 informational comment",
            "author": BOT_THREAD_AUTHOR,
        }]},
    }]
    assert gate.unresolved_blocking_findings(threads, 17) is False


def test_review_pagination_probes_page_two_with_the_same_page_size() -> None:
    reviews = [approved_review() for _ in range(100)]
    with patch("scripts.gate_codex_app_review.github_request", side_effect=[reviews, []]) as request:
        with patch("scripts.gate_codex_app_review.get_review_threads", return_value=[]):
            gate.get_review_evidence("t", "owner/repo", 1)
    assert request.call_args_list[1].args[2].endswith("per_page=100&page=2")


def test_static_secret_and_blocked_path_guard_runs_before_final_review_evaluation() -> None:
    files = [{"filename": ".env", "status": "added", "additions": 1, "deletions": 0}]
    diff = "diff --git a/.env b/.env\n--- /dev/null\n+++ b/.env\n+password = \"not-a-real-secret\""
    issues = gate.static_guard_issues(files, diff)
    assert issues
    assert_blocked(gate.evaluate_gate(pr(), [approved_review()], [], static_issues=issues), "static")


def test_negated_or_informational_severity_text_does_not_block_approval() -> None:
    result = gate.evaluate_final_review(pr(), [approved_review(body="No P0/P1/P2 findings.")], [])
    assert result.exit_code == 0


def test_api_error_timeout_or_missing_review_is_blocked() -> None:
    assert_blocked(gate.evaluate_final_review(pr(), [], [], evidence_error="API timeout"), "evidence unavailable")
    assert_blocked(gate.evaluate_final_review(pr(), [], []), "missing")


def test_push_after_review_invalidates_the_old_review() -> None:
    assert_blocked(
        gate.evaluate_final_review(pr(head="c" * 40), [approved_review(commit_id=HEAD_SHA)], []),
        "current head",
    )


def test_observer_sources_cannot_invoke_a_reviewer_or_external_provider() -> None:
    workflow = (ROOT / ".github/workflows/codex_review_gate.yml").read_text(encoding="utf-8").lower()
    script = (ROOT / "scripts/gate_codex_app_review.py").read_text(encoding="utf-8").lower()
    observed = workflow + "\n" + script
    for forbidden in (
        "aiauditbridge",
        "workflow_dispatch",
        "repository_dispatch",
        "time.sleep",
        "id-token: write",
        "openai_api_key",
        "anthropic_api_key",
        "provider_api_key",
    ):
        assert forbidden not in observed


def test_exact_head_trusted_approved_review_with_no_blockers_passes() -> None:
    result = gate.evaluate_final_review(pr(), [approved_review()], [])
    assert result.exit_code == 0
    assert "approved" in result.title
