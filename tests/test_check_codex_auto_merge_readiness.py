from __future__ import annotations

import json
from pathlib import Path

import scripts.check_codex_auto_merge_readiness as readiness
from scripts.check_codex_auto_merge_readiness import (
    GitHubApiError,
    evaluate_readiness,
    parse_required_status_check_args,
    render_summary,
)


def write_policy(path: Path, *, low_prefixes: list[str] | None = None) -> None:
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "auto_merge_label": "auto-merge-ok",
                "human_review_label": "human-review-required",
                "max_changed_files": 20,
                "max_changed_lines": 1200,
                "risk_policy": {
                    "low": {"prefixes": ["docs/"] if low_prefixes is None else low_prefixes, "exact": []},
                    "medium": {"exact": []},
                    "high": {},
                },
            }
        ),
        encoding="utf-8",
    )


def test_readiness_skips_without_auto_merge_request(tmp_path: Path) -> None:
    decision = evaluate_readiness(auto_merge=False, repo="QuantStrategyLab/UsEquitySnapshotPipelines", branch="main", token="", policy_path=tmp_path / "none")
    assert decision["ready"] and decision["skipped"] and decision["errors"] == []


def test_readiness_checks_labels_and_strict_required_checks(tmp_path: Path, monkeypatch) -> None:
    policy = tmp_path / "policy.json"
    write_policy(policy)

    def fake_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        assert method == "GET" and token == "token"
        if url.endswith(("/labels/auto-merge-ok", "/labels/human-review-required")):
            return {"name": url.rsplit("/", 1)[-1]}
        if url.endswith("/branches/main/protection"):
            return {"required_status_checks": {"strict": True, "contexts": ["test"]}}
        raise AssertionError(url)

    monkeypatch.setattr(readiness, "github_request", fake_request)
    decision = evaluate_readiness(auto_merge=True, repo="QuantStrategyLab/UsEquitySnapshotPipelines", branch="main", token="token", policy_path=policy)
    assert decision["ready"] and not decision["skipped"]
    assert "Remote labels exist and branch protection or rulesets require status checks: test." in decision["checks"]


def test_readiness_fails_closed_for_control_plane_policy_path(tmp_path: Path, monkeypatch) -> None:
    policy = tmp_path / "policy.json"
    write_policy(policy, low_prefixes=[".github/"])
    monkeypatch.setattr(readiness, "github_request", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError()))
    decision = evaluate_readiness(auto_merge=True, repo="QuantStrategyLab/UsEquitySnapshotPipelines", branch="main", token="token", policy_path=policy)
    assert not decision["ready"]
    assert ".github/workflows/*" in decision["errors"][0]


def test_readiness_reports_missing_label(tmp_path: Path, monkeypatch) -> None:
    policy = tmp_path / "policy.json"
    write_policy(policy)

    def fake_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        if url.endswith("/labels/auto-merge-ok"):
            raise GitHubApiError(method, url, 404, "Not Found")
        if url.endswith("/labels/human-review-required"):
            return {"name": "human-review-required"}
        if url.endswith("/branches/main/protection"):
            return {"required_status_checks": {"strict": True, "contexts": ["test"]}}
        raise AssertionError(url)

    monkeypatch.setattr(readiness, "github_request", fake_request)
    decision = evaluate_readiness(auto_merge=True, repo="QuantStrategyLab/UsEquitySnapshotPipelines", branch="main", token="token", policy_path=policy)
    assert decision["errors"] == ["auto-merge label is missing: auto-merge-ok"]
    assert "auto-merge label is missing" in render_summary(decision)


def test_required_status_check_parser_accepts_csv_and_rejects_blank() -> None:
    assert parse_required_status_check_args([], "test, lint") == ("test", "lint")
    assert parse_required_status_check_args([], None) == ("test",)
