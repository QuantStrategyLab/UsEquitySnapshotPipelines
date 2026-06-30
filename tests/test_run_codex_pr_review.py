"""Tests for run_codex_pr_review.py — policy classification, findings evaluation, output parsing."""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.run_codex_pr_review import (
    build_pr_comment,
    build_review_prompt,
    classify_file_risk,
    evaluate_findings,
    parse_review_output,
)


# ---------------------------------------------------------------------------
# classify_file_risk
# ---------------------------------------------------------------------------

DEFAULT_POLICY = {
    "version": 1,
    "blocked_path_patterns": [
        r"(^|/)(\.env|.*secret.*|.*credential.*|.*token.*|.*private.*|.*\.pem|.*\.key)$",
    ],
    "risk_policy": {
        "low": {
            "prefixes": ["docs/", "tests/"],
            "exact": ["README.md", "README.zh-CN.md"],
            "reason": "docs/tests/readme",
        },
        "medium": {
            "exact": ["scripts/run_monthly_report_bundle.py"],
            "reason": "monthly helper",
        },
        "high": {
            "reason": "source code change",
        },
    },
}


class TestClassifyFileRisk:
    def test_low_risk_docs(self):
        level, _ = classify_file_risk("docs/api.md", DEFAULT_POLICY)
        assert level == "low"

    def test_low_risk_tests(self):
        level, _ = classify_file_risk("tests/test_auth.py", DEFAULT_POLICY)
        assert level == "low"

    def test_low_risk_readme(self):
        level, _ = classify_file_risk("README.md", DEFAULT_POLICY)
        assert level == "low"

    def test_low_risk_readme_zh(self):
        level, _ = classify_file_risk("README.zh-CN.md", DEFAULT_POLICY)
        assert level == "low"

    def test_high_risk_blocked_secret(self):
        level, _ = classify_file_risk("config/credentials.json", DEFAULT_POLICY)
        assert level == "high"

    def test_high_risk_blocked_env(self):
        level, _ = classify_file_risk(".env", DEFAULT_POLICY)
        assert level == "high"

    def test_high_risk_blocked_token(self):
        level, _ = classify_file_risk("src/auth_token.py", DEFAULT_POLICY)
        assert level == "high"

    def test_high_risk_blocked_pem(self):
        level, _ = classify_file_risk("certs/server.pem", DEFAULT_POLICY)
        assert level == "high"

    def test_high_risk_source_code(self):
        level, _ = classify_file_risk("src/trading/engine.py", DEFAULT_POLICY)
        assert level == "high"

    def test_medium_risk_monthly_script(self):
        level, _ = classify_file_risk(
            "scripts/run_monthly_report_bundle.py", DEFAULT_POLICY
        )
        assert level == "medium"

    def test_strips_dot_slash_prefix(self):
        level, _ = classify_file_risk(
            "./docs/guide.md", DEFAULT_POLICY
        )
        assert level == "low"

    def test_blocked_private_key(self):
        level, _ = classify_file_risk(
            "infra/private_key.pem", DEFAULT_POLICY
        )
        assert level == "high"


# ---------------------------------------------------------------------------
# parse_review_output
# ---------------------------------------------------------------------------


class TestParseReviewOutput:
    def test_plain_json(self):
        text = '{"summary": "ok", "findings": []}'
        result = parse_review_output(text)
        assert result == {"summary": "ok", "findings": []}

    def test_json_in_code_fence(self):
        text = '```json\n{"summary": "ok", "findings": []}\n```'
        result = parse_review_output(text)
        assert result == {"summary": "ok", "findings": []}

    def test_json_with_surrounding_text(self):
        text = 'Here is my review:\n\n{"summary": "looks good", "findings": [{"severity": "low", "file": "a.py"}]}\n\nDone.'
        result = parse_review_output(text)
        assert result["summary"] == "looks good"
        assert len(result["findings"]) == 1

    def test_invalid_json_raises(self):
        with pytest.raises(Exception):
            parse_review_output("not json at all")

    def test_empty_object(self):
        result = parse_review_output("{}")
        assert result == {}


# ---------------------------------------------------------------------------
# evaluate_findings
# ---------------------------------------------------------------------------


class TestEvaluateFindings:
    def test_critical_on_source_code_blocks(self):
        findings = [
            {
                "severity": "critical",
                "category": "security",
                "file": "src/auth.py",
                "line": 42,
                "description": "SQL injection",
                "suggestion": "Use parameterized queries",
            }
        ]
        changed_files = [{"filename": "src/auth.py", "status": "modified"}]
        decision = evaluate_findings(findings, changed_files, DEFAULT_POLICY)
        assert decision["blocked"] is True
        assert len(decision["blocking_findings"]) == 1

    def test_low_on_source_code_does_not_block(self):
        findings = [
            {
                "severity": "low",
                "category": "style",
                "file": "src/engine.py",
                "line": 10,
                "description": "Variable name could be clearer",
                "suggestion": "Rename to order_book",
            }
        ]
        changed_files = [{"filename": "src/engine.py", "status": "modified"}]
        decision = evaluate_findings(findings, changed_files, DEFAULT_POLICY)
        assert decision["blocked"] is False
        assert len(decision["non_blocking_findings"]) == 1

    def test_critical_on_docs_does_not_block(self):
        findings = [
            {
                "severity": "critical",
                "category": "bug",
                "file": "docs/guide.md",
                "line": 5,
                "description": "Wrong info",
                "suggestion": "Fix it",
            }
        ]
        changed_files = [{"filename": "docs/guide.md", "status": "modified"}]
        decision = evaluate_findings(findings, changed_files, DEFAULT_POLICY)
        assert decision["blocked"] is False
        assert len(decision["non_blocking_findings"]) == 1

    def test_critical_on_tests_does_not_block(self):
        findings = [
            {
                "severity": "critical",
                "category": "bug",
                "file": "tests/test_trade.py",
                "line": 20,
                "description": "Bad test",
                "suggestion": "Fix it",
            }
        ]
        changed_files = [{"filename": "tests/test_trade.py", "status": "modified"}]
        decision = evaluate_findings(findings, changed_files, DEFAULT_POLICY)
        assert decision["blocked"] is False

    def test_mixed_findings_blocking_wins(self):
        findings = [
            {
                "severity": "high",
                "category": "bug",
                "file": "src/trade.py",
                "line": 100,
                "description": "Wrong sign in calculation",
                "suggestion": "Flip the sign",
            },
            {
                "severity": "low",
                "category": "style",
                "file": "src/trade.py",
                "line": 50,
                "description": "Unused import",
                "suggestion": "Remove it",
            },
        ]
        changed_files = [{"filename": "src/trade.py", "status": "modified"}]
        decision = evaluate_findings(findings, changed_files, DEFAULT_POLICY)
        assert decision["blocked"] is True
        assert len(decision["blocking_findings"]) == 1
        assert len(decision["non_blocking_findings"]) == 1

    def test_file_not_in_changed_set_does_not_block(self):
        findings = [
            {
                "severity": "critical",
                "category": "bug",
                "file": "src/other.py",
                "line": 1,
                "description": "Bad",
                "suggestion": "Fix",
            }
        ]
        changed_files = [{"filename": "src/main.py", "status": "modified"}]
        decision = evaluate_findings(findings, changed_files, DEFAULT_POLICY)
        assert decision["blocked"] is False

    def test_no_findings(self):
        decision = evaluate_findings([], [], DEFAULT_POLICY)
        assert decision["blocked"] is False
        assert decision["total_findings"] == 0

    def test_medium_severity_does_not_block(self):
        findings = [
            {
                "severity": "medium",
                "category": "performance",
                "file": "src/cache.py",
                "line": 30,
                "description": "N+1 query pattern",
                "suggestion": "Use batch query",
            }
        ]
        changed_files = [{"filename": "src/cache.py", "status": "modified"}]
        decision = evaluate_findings(findings, changed_files, DEFAULT_POLICY)
        assert decision["blocked"] is False


# ---------------------------------------------------------------------------
# build_pr_comment
# ---------------------------------------------------------------------------


class TestBuildPrComment:
    def test_no_findings(self):
        decision = {
            "blocked": False,
            "blocking_findings": [],
            "non_blocking_findings": [],
            "total_findings": 0,
            "summary": "All good",
        }
        comment = build_pr_comment(decision, "https://github.com/o/r/pulls/1")
        assert "<!-- codex-pr-review -->" in comment
        assert "All good" in comment

    def test_blocking_findings(self):
        decision = {
            "blocked": True,
            "blocking_findings": [
                {
                    "severity": "critical",
                    "category": "security",
                    "file": "src/auth.py",
                    "line": 42,
                    "description": "SQL injection",
                    "suggestion": "Use parameterized queries",
                }
            ],
            "non_blocking_findings": [],
            "total_findings": 1,
            "summary": "Blocked!",
        }
        comment = build_pr_comment(decision, "https://github.com/o/r/pulls/99")
        assert "🚫 Blocking Issues" in comment
        assert "SQL injection" in comment
        assert "CRITICAL" in comment

    def test_non_blocking_findings(self):
        decision = {
            "blocked": False,
            "blocking_findings": [],
            "non_blocking_findings": [
                {
                    "severity": "low",
                    "category": "style",
                    "file": "src/util.py",
                    "line": 5,
                    "description": "Unused import",
                    "suggestion": "Remove",
                }
            ],
            "total_findings": 1,
            "summary": "OK",
        }
        comment = build_pr_comment(decision, "https://github.com/o/r/pulls/1")
        assert "Other Findings" in comment
        assert "Unused import" in comment
        assert "LOW" in comment


# ---------------------------------------------------------------------------
# build_review_prompt
# ---------------------------------------------------------------------------


class TestBuildReviewPrompt:
    def test_includes_pr_context(self):
        prompt = build_review_prompt(
            "diff content here", "Fix login bug", "Fixes #42\n\n", "myorg/myrepo"
        )
        assert "myorg/myrepo" in prompt
        assert "Fix login bug" in prompt
        assert "diff content here" in prompt
        assert "reviewing a pull request" in prompt.lower()

    def test_truncates_large_diff(self):
        big_diff = "\n".join(f"line {i}" for i in range(5000))
        prompt = build_review_prompt(big_diff, "T", "", "r")
        assert len(prompt.splitlines()) < len(big_diff.splitlines()) + 100
