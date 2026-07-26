"""Tests for the read-only GitHub Codex App final-review evidence gate."""
from __future__ import annotations

import sys
from pathlib import Path

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
        "comments": [{"review_id": 17, "body": f"{severity}: fail closed", "author": BOT_REVIEWER}],
    }]
    assert_blocked(gate.evaluate_final_review(pr(), [approved_review()], threads), "unresolved P0/P1/P2")


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
