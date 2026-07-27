from __future__ import annotations

import json
from pathlib import Path

import scripts.check_codex_auto_merge_readiness as readiness
from scripts.check_codex_auto_merge_readiness import (
    GitHubApiError,
    ReadinessError,
    evaluate_readiness,
    parse_required_status_check_args,
    render_summary,
    validate_required_status_checks,
)


def write_policy(
    path: Path,
    *,
    label: str = "auto-merge-ok",
    human_review_label: str = "human-review-required",
    low_prefixes: list[str] | None = None,
    low_exact: list[str] | None = None,
    medium_exact: list[str] | None = None,
) -> None:
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "auto_merge_label": label,
                "human_review_label": human_review_label,
                "monthly_marker_prefix": "<!-- codex-monthly-remediation:issue-",
                "max_changed_files": 20,
                "max_changed_lines": 1200,
                "blocked_path_patterns": [".*secret.*"],
                "risk_policy": {
                    "low": {
                        "prefixes": ["docs/"] if low_prefixes is None else low_prefixes,
                        "exact": ["README.md"] if low_exact is None else low_exact,
                        "reason": "low",
                    },
                    "medium": {
                        "exact": (
                            ["scripts/build_monthly_live_strategy_health_reports.py"]
                            if medium_exact is None
                            else medium_exact
                        ),
                        "reason": "medium",
                    },
                    "high": {"reason": "high"},
                },
            }
        ),
        encoding="utf-8",
    )



def test_evaluate_readiness_skips_when_auto_merge_is_false(tmp_path: Path) -> None:
    decision = evaluate_readiness(
        auto_merge=False,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="",
        policy_path=tmp_path / "missing-policy.json",
    )

    assert decision["ready"]
    assert decision["skipped"]
    assert decision["errors"] == []


def test_evaluate_readiness_passes_with_label_and_protected_branch(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    write_policy(policy_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        assert method == "GET"
        assert token == "token"
        if url.endswith(("/labels/auto-merge-ok", "/labels/human-review-required")):
            return {"name": url.rsplit("/", 1)[-1]}
        if url.endswith("/branches/main/protection"):
            return {"required_status_checks": {"strict": True, "contexts": ["test"]}}
        raise AssertionError(url)

    monkeypatch.setattr(readiness, "github_request", fake_github_request)

    decision = evaluate_readiness(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
        policy_path=policy_path,
    )

    assert decision["ready"]
    assert not decision["skipped"]
    assert decision["label"] == "auto-merge-ok"
    assert decision["human_review_label"] == "human-review-required"
    assert decision["errors"] == []
    assert "Remote labels exist and branch protection or rulesets require status checks: test." in decision["checks"]


def test_evaluate_readiness_fails_when_policy_allows_control_plane_exact_path(
    tmp_path: Path, monkeypatch
) -> None:
    policy_path = tmp_path / "policy.json"
    write_policy(policy_path, medium_exact=["scripts/evaluate_codex_pr_merge.py"])

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        raise AssertionError("remote readiness should not run when local policy guardrail fails")

    monkeypatch.setattr(readiness, "github_request", fake_github_request)

    decision = evaluate_readiness(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
        policy_path=policy_path,
    )

    assert not decision["ready"]
    assert decision["errors"] == [
        "auto-merge policy must keep control-plane paths high-risk: scripts/evaluate_codex_pr_merge.py"
    ]


def test_evaluate_readiness_fails_when_policy_allows_control_plane_prefix(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    write_policy(policy_path, low_prefixes=[".github/"])

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        raise AssertionError("remote readiness should not run when local policy guardrail fails")

    monkeypatch.setattr(readiness, "github_request", fake_github_request)

    decision = evaluate_readiness(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
        policy_path=policy_path,
    )

    assert not decision["ready"]
    assert decision["errors"] == [
        ("auto-merge policy must keep control-plane paths high-risk: "
        ".github/codex_auto_merge_policy.json, .github/workflows/*")
    ]


def test_evaluate_readiness_fails_when_label_is_missing(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    write_policy(policy_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        if url.endswith("/labels/auto-merge-ok"):
            raise GitHubApiError(method, url, 404, '{"message":"Not Found"}')
        if url.endswith("/labels/human-review-required"):
            return {"name": "human-review-required"}
        if url.endswith("/branches/main/protection"):
            return {"required_status_checks": {"strict": True, "contexts": ["test"]}}
        raise AssertionError(url)

    monkeypatch.setattr(readiness, "github_request", fake_github_request)

    decision = evaluate_readiness(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
        policy_path=policy_path,
    )

    assert not decision["ready"]
    assert decision["errors"] == ["auto-merge label is missing: auto-merge-ok"]
    assert "auto-merge label is missing" in render_summary(decision)


def test_evaluate_readiness_fails_when_human_review_label_is_missing(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    write_policy(policy_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        if url.endswith("/labels/auto-merge-ok"):
            return {"name": "auto-merge-ok"}
        if url.endswith("/labels/human-review-required"):
            raise GitHubApiError(method, url, 404, '{"message":"Not Found"}')
        if url.endswith("/branches/main/protection"):
            return {"required_status_checks": {"strict": True, "contexts": ["test"]}}
        raise AssertionError(url)

    monkeypatch.setattr(readiness, "github_request", fake_github_request)

    decision = evaluate_readiness(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
        policy_path=policy_path,
    )

    assert not decision["ready"]
    assert decision["errors"] == ["human-review label is missing: human-review-required"]
    assert "human-review label is missing" in render_summary(decision)


def test_evaluate_readiness_fails_when_policy_labels_match(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    write_policy(policy_path, human_review_label="auto-merge-ok")

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        raise AssertionError("remote readiness should not run when policy labels collide")

    monkeypatch.setattr(readiness, "github_request", fake_github_request)

    decision = evaluate_readiness(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
        policy_path=policy_path,
    )

    assert not decision["ready"]
    assert decision["errors"] == ["auto-merge and human-review labels must be distinct before enabling auto-merge"]


def test_evaluate_readiness_accepts_branch_endpoint_fallback_when_protection_detail_is_forbidden(
    tmp_path: Path, monkeypatch
) -> None:
    policy_path = tmp_path / "policy.json"
    write_policy(policy_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        if url.endswith(("/labels/auto-merge-ok", "/labels/human-review-required")):
            return {"name": url.rsplit("/", 1)[-1]}
        if url.endswith("/branches/main/protection"):
            raise GitHubApiError(method, url, 403, '{"message":"Resource not accessible by integration"}')
        if url.endswith("/branches/main"):
            return {
                "protected": True,
                "protection": {
                    "required_status_checks": {
                        "contexts": ["test"],
                        "checks": [{"context": "test"}],
                    }
                },
            }
        raise AssertionError(url)

    monkeypatch.setattr(readiness, "github_request", fake_github_request)

    decision = evaluate_readiness(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
        policy_path=policy_path,
    )

    assert decision["ready"]
    assert decision["errors"] == []


def test_evaluate_readiness_rejects_branch_endpoint_fallback_without_required_checks(
    tmp_path: Path, monkeypatch
) -> None:
    policy_path = tmp_path / "policy.json"
    write_policy(policy_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        if url.endswith(("/labels/auto-merge-ok", "/labels/human-review-required")):
            return {"name": url.rsplit("/", 1)[-1]}
        if url.endswith("/branches/main/protection"):
            raise GitHubApiError(method, url, 403, '{"message":"Resource not accessible by integration"}')
        if url.endswith("/branches/main"):
            return {
                "protected": True,
                "protection": {"required_status_checks": {"contexts": ["lint"]}},
            }
        if url.endswith("/rules/branches/main?per_page=100"):
            return []
        raise AssertionError(url)

    monkeypatch.setattr(readiness, "github_request", fake_github_request)

    decision = evaluate_readiness(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
        policy_path=policy_path,
    )

    assert not decision["ready"]
    assert decision["errors"] == ["required status checks missing for main: test"]


def test_evaluate_readiness_fails_when_branch_is_not_protected(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    write_policy(policy_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        if url.endswith(("/labels/auto-merge-ok", "/labels/human-review-required")):
            return {"name": url.rsplit("/", 1)[-1]}
        if url.endswith("/branches/main/protection"):
            raise GitHubApiError(method, url, 404, '{"message":"Branch not protected"}')
        if url.endswith("/rules/branches/main?per_page=100"):
            return []
        raise AssertionError(url)

    monkeypatch.setattr(readiness, "github_request", fake_github_request)

    decision = evaluate_readiness(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
        policy_path=policy_path,
    )

    assert not decision["ready"]
    assert decision["errors"] == ["branch protection is not enabled for main"]


def test_evaluate_readiness_accepts_strict_required_status_check_ruleset(
    tmp_path: Path, monkeypatch
) -> None:
    policy_path = tmp_path / "policy.json"
    write_policy(policy_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        if url.endswith(("/labels/auto-merge-ok", "/labels/human-review-required")):
            return {"name": url.rsplit("/", 1)[-1]}
        if url.endswith("/branches/main/protection"):
            raise GitHubApiError(method, url, 404, '{"message":"Branch not protected"}')
        if url.endswith("/rules/branches/main?per_page=100"):
            return [
                {
                    "type": "required_status_checks",
                    "parameters": {
                        "strict_required_status_checks_policy": True,
                        "required_status_checks": [{"context": "test"}],
                    },
                }
            ]
        raise AssertionError(url)

    monkeypatch.setattr(readiness, "github_request", fake_github_request)

    decision = evaluate_readiness(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
        policy_path=policy_path,
    )

    assert decision["ready"]
    assert decision["errors"] == []


def test_evaluate_readiness_rejects_non_strict_required_status_check_ruleset(
    tmp_path: Path, monkeypatch
) -> None:
    policy_path = tmp_path / "policy.json"
    write_policy(policy_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        if url.endswith(("/labels/auto-merge-ok", "/labels/human-review-required")):
            return {"name": url.rsplit("/", 1)[-1]}
        if url.endswith("/branches/main/protection"):
            raise GitHubApiError(method, url, 404, '{"message":"Branch not protected"}')
        if url.endswith("/rules/branches/main?per_page=100"):
            return [
                {
                    "type": "required_status_checks",
                    "parameters": {
                        "strict_required_status_checks_policy": False,
                        "required_status_checks": [{"context": "test"}],
                    },
                }
            ]
        raise AssertionError(url)

    monkeypatch.setattr(readiness, "github_request", fake_github_request)

    decision = evaluate_readiness(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
        policy_path=policy_path,
    )

    assert not decision["ready"]
    assert decision["errors"] == ["required status checks must require branches to be up to date for main"]


def test_evaluate_readiness_accepts_ruleset_when_legacy_protection_lacks_required_check(
    tmp_path: Path, monkeypatch
) -> None:
    policy_path = tmp_path / "policy.json"
    write_policy(policy_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        if url.endswith(("/labels/auto-merge-ok", "/labels/human-review-required")):
            return {"name": url.rsplit("/", 1)[-1]}
        if url.endswith("/branches/main/protection"):
            return {"required_status_checks": {"strict": True, "contexts": ["lint"]}}
        if url.endswith("/rules/branches/main?per_page=100"):
            return [
                {
                    "type": "required_status_checks",
                    "parameters": {
                        "strict_required_status_checks_policy": True,
                        "required_status_checks": [{"context": "test"}],
                    },
                }
            ]
        raise AssertionError(url)

    monkeypatch.setattr(readiness, "github_request", fake_github_request)

    decision = evaluate_readiness(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
        policy_path=policy_path,
    )

    assert decision["ready"]
    assert decision["errors"] == []


def test_evaluate_readiness_fails_when_required_status_check_is_missing(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    write_policy(policy_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        if url.endswith(("/labels/auto-merge-ok", "/labels/human-review-required")):
            return {"name": url.rsplit("/", 1)[-1]}
        if url.endswith("/branches/main/protection"):
            return {"required_status_checks": {"strict": True, "contexts": ["lint"]}}
        if url.endswith("/rules/branches/main?per_page=100"):
            return []
        raise AssertionError(url)

    monkeypatch.setattr(readiness, "github_request", fake_github_request)

    decision = evaluate_readiness(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
        policy_path=policy_path,
    )

    assert not decision["ready"]
    assert decision["errors"] == ["required status checks missing for main: test"]


def test_validate_required_status_checks_normalizes_values() -> None:
    assert validate_required_status_checks((" test ", "lint")) == ("test", "lint")


def test_parse_required_status_check_args_accepts_csv_and_repeated_values() -> None:
    assert parse_required_status_check_args(["lint"], "test,build\nsecurity") == (
        "lint",
        "test",
        "build",
        "security",
    )


def test_parse_required_status_check_args_uses_default_when_unset() -> None:
    assert parse_required_status_check_args(None, "") == ("test",)


def test_validate_required_status_checks_rejects_empty_values() -> None:
    try:
        validate_required_status_checks(("", "   "))
    except ReadinessError as exc:
        assert str(exc) == "at least one required status check must be configured before enabling auto-merge"
    else:
        raise AssertionError("expected blank required status checks to fail")


def test_validate_required_status_checks_rejects_multiline_values() -> None:
    try:
        validate_required_status_checks(("test\nlint",))
    except ReadinessError as exc:
        assert str(exc) == "required status checks must be single-line values before enabling auto-merge"
    else:
        raise AssertionError("expected multiline required status check to fail")


def test_evaluate_readiness_fails_closed_for_blank_required_status_checks(
    tmp_path: Path, monkeypatch
) -> None:
    policy_path = tmp_path / "policy.json"
    write_policy(policy_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        raise AssertionError("remote readiness should not run with invalid required status checks")

    monkeypatch.setattr(readiness, "github_request", fake_github_request)

    decision = evaluate_readiness(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
        policy_path=policy_path,
        required_status_checks=("",),
    )

    assert not decision["ready"]
    assert decision["errors"] == [
        "at least one required status check must be configured before enabling auto-merge"
    ]


def test_evaluate_readiness_fails_when_status_checks_are_not_strict(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    write_policy(policy_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        if url.endswith(("/labels/auto-merge-ok", "/labels/human-review-required")):
            return {"name": url.rsplit("/", 1)[-1]}
        if url.endswith("/branches/main/protection"):
            return {"required_status_checks": {"strict": False, "contexts": ["test"]}}
        if url.endswith("/rules/branches/main?per_page=100"):
            return []
        raise AssertionError(url)

    monkeypatch.setattr(readiness, "github_request", fake_github_request)

    decision = evaluate_readiness(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
        policy_path=policy_path,
    )

    assert not decision["ready"]
    assert decision["errors"] == ["required status checks must require branches to be up to date for main"]


def test_evaluate_readiness_reports_network_errors_without_traceback(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    write_policy(policy_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        raise GitHubApiError(method, url, 0, "certificate verify failed")

    monkeypatch.setattr(readiness, "github_request", fake_github_request)

    decision = evaluate_readiness(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
        policy_path=policy_path,
    )

    assert not decision["ready"]
    assert decision["errors"] == [
        "auto-merge label check failed: certificate verify failed",
        "human-review label check failed: certificate verify failed",
        "branch protection check failed: certificate verify failed",
    ]



def test_readiness_has_no_retired_workflow_parameters() -> None:
    import inspect

    parameters = inspect.signature(evaluate_readiness).parameters
    assert "workflow_path" not in parameters
    assert "feedback_workflow_path" not in parameters
    assert "monthly_workflow_path" not in parameters


def test_cli_has_no_retired_workflow_options(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["readiness", "--repo", "org/repo", "--branch", "main"])

    args = readiness.parse_args()

    assert not hasattr(args, "workflow_file")
    assert not hasattr(args, "feedback_workflow_file")
    assert not hasattr(args, "monthly_workflow_file")
