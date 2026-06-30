"""Tests for gate_codex_app_review.py — static guard + App review gate."""
from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.gate_codex_app_review import (
    app_decision,
    compile_patterns,
    get_codex_review,
    load_policy,
    scan_diff,
    BOT_LOGIN,
)


# ── app_decision ─────────────────────────────────────────────────────────────


class TestAppDecision:
    def test_changes_requested_blocks(self):
        rc, title, _ = app_decision({"state": "CHANGES_REQUESTED", "submitted_at": "", "html_url": "", "body": "Bad"})
        assert rc == 1
        assert "BLOCKED" in title

    def test_approved_passes(self):
        rc, _, _ = app_decision({"state": "APPROVED", "submitted_at": "", "html_url": "", "body": ""})
        assert rc == 0

    def test_commented_passes(self):
        rc, _, _ = app_decision({"state": "COMMENTED", "submitted_at": "", "html_url": "", "body": ""})
        assert rc == 0

    def test_dismissed_passes(self):
        rc, _, _ = app_decision({"state": "DISMISSED", "submitted_at": "", "html_url": "", "body": ""})
        assert rc == 0

    def test_none_passes(self):
        rc, title, _ = app_decision(None)
        assert rc == 0
        assert "no review" in title.lower()

    def test_lowercase_handled(self):
        rc, _, _ = app_decision({"state": "changes_requested", "submitted_at": "", "html_url": "", "body": ""})
        assert rc == 1


# ── get_codex_review ─────────────────────────────────────────────────────────


class TestGetCodexReview:
    def test_returns_latest_bot_review(self):
        from unittest.mock import patch
        mock = [
            {"id": 1, "user": {"login": "other"}, "state": "COMMENTED"},
            {"id": 2, "user": {"login": BOT_LOGIN}, "state": "APPROVED"},
            {"id": 3, "user": {"login": BOT_LOGIN}, "state": "CHANGES_REQUESTED"},
        ]
        with patch("scripts.gate_codex_app_review.github_request", return_value=mock):
            r = get_codex_review("t", "r", 1)
            assert r["state"] == "CHANGES_REQUESTED"

    def test_none_when_no_bot(self):
        from unittest.mock import patch
        with patch("scripts.gate_codex_app_review.github_request",
                   return_value=[{"user": {"login": "human"}}]):
            assert get_codex_review("t", "r", 1) is None

    def test_none_when_empty(self):
        from unittest.mock import patch
        with patch("scripts.gate_codex_app_review.github_request", return_value=[]):
            assert get_codex_review("t", "r", 1) is None

    def test_none_when_malformed(self):
        from unittest.mock import patch
        with patch("scripts.gate_codex_app_review.github_request", return_value={"bad": True}):
            assert get_codex_review("t", "r", 1) is None


# ── scan_diff ────────────────────────────────────────────────────────────────


class TestScanDiff:
    def test_detects_hardcoded_secret(self):
        diff = 'diff --git a/x.py b/x.py\n--- a/x.py\n+++ b/x.py\n@@ -1 +1,2 @@\n+api_key = "sk-abcdefghijklmnopqrstuvwxyz1234567890"'
        issues = scan_diff(diff, [])
        assert len(issues) == 1
        assert "Hardcoded secret" in issues[0]

    def test_detects_blocked_file(self):
        from scripts.gate_codex_app_review import compile_patterns
        patterns = compile_patterns(load_policy())
        diff = 'diff --git a/config/.env b/config/.env\nnew file mode 100644\n--- /dev/null\n+++ b/config/.env'
        issues = scan_diff(diff, patterns)
        assert len(issues) == 1
        assert "Blocked file" in issues[0]

    def test_pass_clean_diff(self):
        diff = 'diff --git a/x.py b/x.py\n--- a/x.py\n+++ b/x.py\n@@ -1 +1,2 @@\n+def foo():\n+    return 42'
        assert scan_diff(diff, []) == []

    def test_skips_placeholder_secrets(self):
        diff = 'diff --git a/x.py b/x.py\n--- a/x.py\n+++ b/x.py\n@@ -1 +1,2 @@\n+password = "your-password-here"'
        assert scan_diff(diff, []) == []

    def test_detects_credential_file(self):
        from scripts.gate_codex_app_review import compile_patterns
        patterns = compile_patterns(load_policy())
        diff = 'diff --git a/src/credentials.py b/src/credentials.py\n--- a/src/credentials.py\n+++ b/src/credentials.py'
        issues = scan_diff(diff, patterns)
        assert len(issues) == 1
        assert "credentials" in issues[0].lower()


# ── policy ───────────────────────────────────────────────────────────────────


class TestPolicy:
    def test_load_default(self):
        p = load_policy()
        assert p["version"] == 1
        assert len(p["blocked_path_patterns"]) > 0

    def test_patterns_compile(self):
        patterns = compile_patterns(load_policy())
        assert len(patterns) > 0
        for pat in patterns:
            assert pat.search(".env")
