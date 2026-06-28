"""Tests for gate_codex_app_review.py — App review → check run translation."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.gate_codex_app_review import (
    BOT_LOGIN,
    CHECK_NAME,
    get_codex_review,
    get_existing_check_run,
    review_decision,
)


# ── review_decision ──────────────────────────────────────────────────────────


class TestReviewDecision:
    def test_changes_requested_blocks(self):
        review = {"state": "CHANGES_REQUESTED", "submitted_at": "2026-01-01", "html_url": "", "body": "Bad code"}
        conclusion, title, summary = review_decision(review)
        assert conclusion == "failure"
        assert "BLOCKED" in title
        assert "Bad code" in summary

    def test_approved_passes(self):
        review = {"state": "APPROVED", "submitted_at": "2026-01-01", "html_url": "", "body": ""}
        conclusion, title, summary = review_decision(review)
        assert conclusion == "success"
        assert "approved" in title.lower()

    def test_commented_passes(self):
        review = {"state": "COMMENTED", "submitted_at": "2026-01-01", "html_url": "", "body": ""}
        conclusion, _, _ = review_decision(review)
        assert conclusion == "success"

    def test_dismissed_passes(self):
        review = {"state": "DISMISSED", "submitted_at": "2026-01-01", "html_url": "", "body": ""}
        conclusion, _, _ = review_decision(review)
        assert conclusion == "success"

    def test_pending_passes(self):
        review = {"state": "PENDING", "submitted_at": "2026-01-01", "html_url": "", "body": ""}
        conclusion, _, _ = review_decision(review)
        assert conclusion == "success"

    def test_none_review_passes(self):
        conclusion, title, _ = review_decision(None)
        assert conclusion == "success"
        assert "no review" in title.lower()

    def test_lowercase_state_handled(self):
        review = {"state": "changes_requested", "submitted_at": "2026-01-01", "html_url": "", "body": ""}
        conclusion, _, _ = review_decision(review)
        assert conclusion == "failure"

    def test_truncates_long_body(self):
        review = {"state": "CHANGES_REQUESTED", "submitted_at": "2026-01-01", "html_url": "", "body": "x" * 1000}
        _, _, summary = review_decision(review)
        assert len(summary) < 800  # truncated


# ── get_codex_review ─────────────────────────────────────────────────────────


class TestGetCodexReview:
    def test_returns_latest_bot_review(self):
        from unittest.mock import patch
        mock_resp = [
            {"id": 1, "user": {"login": "other-user"}, "state": "COMMENTED"},
            {"id": 2, "user": {"login": BOT_LOGIN}, "state": "APPROVED"},
            {"id": 3, "user": {"login": BOT_LOGIN}, "state": "CHANGES_REQUESTED"},
        ]
        with patch("scripts.gate_codex_app_review.github_request", return_value=mock_resp):
            result = get_codex_review("token", "repo", 1)
            assert result is not None
            assert result["state"] == "CHANGES_REQUESTED"

    def test_returns_none_no_bot(self):
        from unittest.mock import patch
        with patch("scripts.gate_codex_app_review.github_request",
                   return_value=[{"id": 1, "user": {"login": "human"}, "state": "COMMENTED"}]):
            assert get_codex_review("t", "r", 1) is None

    def test_returns_none_empty(self):
        from unittest.mock import patch
        with patch("scripts.gate_codex_app_review.github_request", return_value=[]):
            assert get_codex_review("t", "r", 1) is None

    def test_returns_none_malformed(self):
        from unittest.mock import patch
        with patch("scripts.gate_codex_app_review.github_request", return_value={"bad": True}):
            assert get_codex_review("t", "r", 1) is None


# ── get_existing_check_run ───────────────────────────────────────────────────


class TestGetExistingCheckRun:
    def test_finds_run(self):
        from unittest.mock import patch
        mock = {"check_runs": [{"id": 99, "name": "other"}, {"id": 100, "name": CHECK_NAME}]}
        with patch("scripts.gate_codex_app_review.github_request", return_value=mock):
            result = get_existing_check_run("t", "r", "sha")
            assert result["id"] == 100

    def test_none_when_missing(self):
        from unittest.mock import patch
        with patch("scripts.gate_codex_app_review.github_request",
                   return_value={"check_runs": [{"id": 99, "name": "ci"}]}):
            assert get_existing_check_run("t", "r", "sha") is None


# ── Wait mode constants ──────────────────────────────────────────────────────


class TestWaitMode:
    def test_check_name_is_stable(self):
        assert CHECK_NAME == "Codex Review Gate"

    def test_bot_login_is_correct(self):
        assert BOT_LOGIN == "chatgpt-codex-connector[bot]"
