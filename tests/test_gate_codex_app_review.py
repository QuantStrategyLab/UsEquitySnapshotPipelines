"""Tests for gate_codex_app_review.py — static guard + App review gate."""
from __future__ import annotations

import sys
import json
from pathlib import Path
from unittest.mock import patch

import pytest


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

    def test_commented_fails_closed(self):
        rc, _, _ = app_decision({"state": "COMMENTED", "submitted_at": "", "html_url": "", "body": ""})
        assert rc == 1

    def test_dismissed_fails_closed(self):
        rc, _, _ = app_decision({"state": "DISMISSED", "submitted_at": "", "html_url": "", "body": ""})
        assert rc == 1

    def test_none_fails_closed(self):
        rc, title, _ = app_decision(None)
        assert rc == 1
        assert "FAIL CLOSED" in title

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
        fake_key = "sk-" + "abcdefghijklmnopqrstuvwxyz1234567890"
        diff = f'diff --git a/x.py b/x.py\n--- a/x.py\n+++ b/x.py\n@@ -1 +1,2 @@\n+api_key = "{fake_key}"'
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


# ── V2 evaluator/bootstrap contract ─────────────────────────────────────────


FULL_HEAD = "a81e04e21778abf4396c9f437171451d1d390168"
MAINTAINER = {"login": "maintainer", "id": 7, "type": "User"}
BOT = {"login": BOT_LOGIN, "id": 199175422, "type": "Bot"}


def final_request(*, head: str = FULL_HEAD) -> str:
    return json.dumps({
        "request_kind": "codex-final-review",
        "repo": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "pr_number": 203,
        "requested_head": head,
    })


class TestV2Contract:
    def test_clean_prefix_requires_one_10_plus_hex_field(self):
        from scripts.gate_codex_app_review import parse_clean_review_prefix

        assert parse_clean_review_prefix("Reviewed commit: a81e04e217") == "a81e04e217"
        for body in ("Reviewed commit: abc", "Reviewed commit: xyz1234567",
                     "Reviewed commit: a81e04e217\nReviewed commit: b81e04e217"):
            with pytest.raises(ValueError):
                parse_clean_review_prefix(body)

    def test_commit_prefix_resolution_is_full_sha_or_fail_closed(self):
        from scripts.gate_codex_app_review import resolve_commit_prefix

        class Client:
            def get(self, path):
                assert path.endswith("/commits/a81e04e217")
                return {"sha": FULL_HEAD}

        assert resolve_commit_prefix(Client(), "QuantStrategyLab/UsEquitySnapshotPipelines", "a81e04e217") == FULL_HEAD
        for result in ({"sha": "short"}, {"sha": FULL_HEAD[:-1] + "z"}, [], None):
            class BadClient:
                def get(self, _): return result
            with pytest.raises(ValueError):
                resolve_commit_prefix(BadClient(), "owner/repo", "a81e04e217")

    def test_trusted_identity_and_maintainer_are_independently_strict(self):
        from scripts.gate_codex_app_review import is_trusted_app_user, is_trusted_maintainer

        assert is_trusted_app_user(BOT)
        assert not is_trusted_app_user({**BOT, "id": 1})
        assert not is_trusted_app_user({**BOT, "type": "User"})

        class Client:
            def get(self, path):
                assert path.endswith("/collaborators/maintainer/permission")
                return {"permission": "maintain"}

        assert is_trusted_maintainer(Client(), "owner/repo", MAINTAINER)

    def test_final_request_is_unique_complete_and_full_head_bound(self):
        from scripts.gate_codex_app_review import select_final_review_request

        request = {"id": 11, "html_url": "https://example/request", "body": final_request(),
                   "created_at": "2026-07-26T01:00:00Z", "user": MAINTAINER}
        class Client:
            def get(self, path): return {"permission": "write"}

        chosen = select_final_review_request(Client(), "QuantStrategyLab/UsEquitySnapshotPipelines", 203, [request])
        assert chosen["id"] == 11
        with pytest.raises(ValueError):
            select_final_review_request(Client(), "QuantStrategyLab/UsEquitySnapshotPipelines", 203, [request, request])
        bad = {**request, "body": final_request(head="a81e04e217")}
        with pytest.raises(ValueError):
            select_final_review_request(Client(), "QuantStrategyLab/UsEquitySnapshotPipelines", 203, [bad])

    def test_events_resolve_one_pr_and_have_deterministic_dedupe_key(self):
        from scripts.gate_codex_app_review import event_context

        common = {"number": 203, "head": {"sha": FULL_HEAD}, "draft": False}
        contexts = [
            event_context("pull_request", {"pull_request": common, "action": "synchronize"}),
            event_context("pull_request_review", {"pull_request": common, "review": {"id": 22}}),
            event_context("issue_comment", {"issue": {"number": 203, "pull_request": {}}, "comment": {"id": 33}}),
        ]
        assert [c.pr_number for c in contexts] == [203, 203, 203]
        assert len({c.dedupe_key for c in contexts}) == 3
        with pytest.raises(ValueError):
            event_context("issue_comment", {"issue": {"number": 203}, "comment": {"id": 33}})

    def test_rest_pagination_rejects_duplicate_and_incomplete_pages(self):
        from scripts.gate_codex_app_review import paginate_rest

        class Client:
            def __init__(self, pages): self.pages = iter(pages)
            def get(self, _): return next(self.pages)

        assert paginate_rest(Client() if False else type("C", (), {"get": lambda self, _: [{"id": 1}]})(), "/x") == [{"id": 1}]
        with pytest.raises(ValueError):
            paginate_rest(Client([[{"id": 1}] * 100, [{"id": 1}]]), "/x")
        with pytest.raises(ValueError):
            paginate_rest(Client([{"not": "a list"}]), "/x")

    def test_thread_and_nested_comment_pagination_are_complete(self):
        from scripts.gate_codex_app_review import paginate_review_threads

        class Client:
            def __init__(self): self.calls = 0
            def graphql(self, query, variables):
                self.calls += 1
                if "PullRequestReviewThread" in query:
                    return {"data": {"node": {"comments": {"nodes": [{"id": "c2"}], "pageInfo": {"hasNextPage": False, "endCursor": None}}}}}
                return {"data": {"repository": {"pullRequest": {"reviewThreads": {"nodes": [{"id": "t1", "isResolved": False, "comments": {"nodes": [{"id": "c1"}], "pageInfo": {"hasNextPage": True, "endCursor": "next"}}}], "pageInfo": {"hasNextPage": False, "endCursor": None}}}}}}

        threads = paginate_review_threads(Client(), "QuantStrategyLab", "UsEquitySnapshotPipelines", 203)
        assert [c["id"] for c in threads[0]["comments"]] == ["c1", "c2"]

    def test_findings_block_p0_p1_p2_but_not_p3_or_negated_text(self):
        from scripts.gate_codex_app_review import classify_findings

        assert classify_findings("P1: missing validation") == ["P1"]
        assert classify_findings("P3: optional wording") == []
        assert classify_findings("P1: no unresolved finding") == []
        assert classify_findings("No P1 findings remain") == []

    def test_static_guard_api_error_fails_closed_before_ai_evidence(self):
        from scripts.gate_codex_app_review import run_static_guard

        with patch("scripts.gate_codex_app_review.github_request", side_effect=RuntimeError("422")):
            with pytest.raises(RuntimeError):
                run_static_guard("token", "owner/repo", 203)

    def test_clean_evaluation_binds_request_resolved_current_and_evaluation_head(self):
        from scripts.gate_codex_app_review import evaluate_v2_evidence

        request = {"id": 11, "html_url": "https://example/request", "body": final_request(),
                   "created_at": "2026-07-26T01:00:00Z", "user": MAINTAINER}
        response = {"id": 12, "html_url": "https://example/response", "body": "Reviewed commit: a81e04e217",
                    "created_at": "2026-07-26T01:01:00Z", "user": BOT}

        class Client:
            def get(self, path):
                if path == "/repos/QuantStrategyLab/UsEquitySnapshotPipelines/pulls/203":
                    return {"head": {"sha": FULL_HEAD}}
                if "/comments?" in path or "/reviews?" in path:
                    return [request, response] if "/comments?" in path else []
                if path.endswith("/permission"):
                    return {"permission": "maintain"}
                if path.endswith("/comments/11"):
                    return request
                if path.endswith("/comments/12"):
                    return response
                if path.endswith("/commits/a81e04e217"):
                    return {"sha": FULL_HEAD}
                raise AssertionError(path)
            def graphql(self, *_):
                return {"data": {"repository": {"pullRequest": {"reviewThreads": {"nodes": [], "pageInfo": {"hasNextPage": False, "endCursor": None}}}}}}

        evidence = evaluate_v2_evidence(Client(), "QuantStrategyLab/UsEquitySnapshotPipelines", 203)
        assert evidence["response_type"] == "issue_comment"
        assert evidence["requested_head"] == evidence["resolved_head"] == evidence["current_head"] == FULL_HEAD

    def test_finding_review_requires_exact_full_head_and_can_be_the_response(self):
        from scripts.gate_codex_app_review import evaluate_v2_evidence

        request = {"id": 11, "html_url": "https://example/request", "body": final_request(),
                   "created_at": "2026-07-26T01:00:00Z", "user": MAINTAINER}
        review = {"id": 21, "html_url": "https://example/review", "body": "P3: wording suggestion",
                  "submitted_at": "2026-07-26T01:01:00Z", "user": BOT, "commit_id": FULL_HEAD, "state": "COMMENTED"}

        class Client:
            def get(self, path):
                if path == "/repos/QuantStrategyLab/UsEquitySnapshotPipelines/pulls/203":
                    return {"head": {"sha": FULL_HEAD}}
                if "/comments?" in path:
                    return [request]
                if "/reviews?" in path:
                    return [review]
                if path.endswith("/permission"):
                    return {"permission": "write"}
                if path.endswith("/comments/11"):
                    return request
                if path.endswith("/reviews/21"):
                    return review
                raise AssertionError(path)
            def graphql(self, *_):
                return {"data": {"repository": {"pullRequest": {"reviewThreads": {"nodes": [], "pageInfo": {"hasNextPage": False, "endCursor": None}}}}}}

        assert evaluate_v2_evidence(Client(), "QuantStrategyLab/UsEquitySnapshotPipelines", 203)["response_type"] == "pull_request_review"
        review["commit_id"] = "b" * 40
        with pytest.raises(ValueError):
            evaluate_v2_evidence(Client(), "QuantStrategyLab/UsEquitySnapshotPipelines", 203)

    def test_unresolved_current_head_thread_blocks_but_negated_severity_does_not(self):
        from scripts.gate_codex_app_review import evaluate_v2_evidence

        request = {"id": 11, "html_url": "https://example/request", "body": final_request(),
                   "created_at": "2026-07-26T01:00:00Z", "user": MAINTAINER}
        review = {"id": 21, "html_url": "https://example/review", "body": "P3: note",
                  "submitted_at": "2026-07-26T01:01:00Z", "user": BOT, "commit_id": FULL_HEAD, "state": "COMMENTED"}

        class Client:
            def get(self, path):
                if path == "/repos/QuantStrategyLab/UsEquitySnapshotPipelines/pulls/203":
                    return {"head": {"sha": FULL_HEAD}}
                if "/comments?" in path:
                    return [request]
                if "/reviews?" in path:
                    return [review]
                if path.endswith("/permission"):
                    return {"permission": "admin"}
                if path.endswith("/comments/11"):
                    return request
                if path.endswith("/reviews/21"):
                    return review
                raise AssertionError(path)
            def graphql(self, *_):
                linked = {"id": "r", "state": "COMMENTED", "author": {"login": BOT_LOGIN, "databaseId": 199175422, "__typename": "Bot"}, "commit": {"oid": FULL_HEAD}}
                comment = {"id": "c", "body": "P1: missing validation", "author": linked["author"], "pullRequestReview": linked}
                return {"data": {"repository": {"pullRequest": {"reviewThreads": {"nodes": [{"id": "t", "isResolved": False, "comments": {"nodes": [comment], "pageInfo": {"hasNextPage": False, "endCursor": None}}}], "pageInfo": {"hasNextPage": False, "endCursor": None}}}}}}

        with pytest.raises(ValueError, match="unresolved trusted finding"):
            evaluate_v2_evidence(Client(), "QuantStrategyLab/UsEquitySnapshotPipelines", 203)

    def test_workflow_is_read_only_and_has_no_poll_or_reviewer_side_effect(self):
        workflow = (ROOT / ".github/workflows/codex_review_gate.yml").read_text(encoding="utf-8")
        assert "issue_comment:" in workflow
        assert "issues: read" in workflow and "pull-requests: read" in workflow
        assert "issues: write" not in workflow and "pull-requests: write" not in workflow
        assert "id-token" not in workflow and "sleep" not in workflow.lower()

    def test_review_thread_query_reads_bot_database_id_through_actor_fragment(self):
        from scripts.gate_codex_app_review import THREADS_QUERY

        assert "... on Bot { databaseId }" in THREADS_QUERY
