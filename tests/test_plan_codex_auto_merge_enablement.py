from __future__ import annotations

import pytest

from scripts.plan_codex_auto_merge_enablement import (
    branch_protection_payload,
    discover_branch_protection_status_checks,
    discover_branch_ruleset_status_checks,
    discover_check_contexts,
    discover_repository_variable,
    render_branch_protection_command,
    render_enablement_plan,
    render_label_commands,
)
from scripts.check_codex_auto_merge_readiness import GitHubApiError, ReadinessError
import scripts.plan_codex_auto_merge_enablement as planner


def test_branch_protection_payload_requires_strict_status_checks() -> None:
    payload = branch_protection_payload(("test", "lint"))

    assert payload["required_status_checks"] == {"strict": True, "contexts": ["test", "lint"]}
    assert payload["required_pull_request_reviews"] is None
    assert payload["restrictions"] is None


def test_branch_protection_payload_rejects_blank_status_checks() -> None:
    with pytest.raises(
        ReadinessError,
        match="at least one required status check must be configured before enabling auto-merge",
    ):
        branch_protection_payload(("", "   "))


def test_render_branch_protection_command_is_dry_run_copyable() -> None:
    command = render_branch_protection_command("QuantStrategyLab/UsEquitySnapshotPipelines", "main", ("test",))

    assert "gh api --method PUT /repos/QuantStrategyLab/UsEquitySnapshotPipelines/branches/main/protection" in command
    assert '"contexts": [\n      "test"\n    ]' in command
    assert command.endswith("JSON")


def test_render_branch_protection_command_url_encodes_branch_name() -> None:
    command = render_branch_protection_command(
        "QuantStrategyLab/UsEquitySnapshotPipelines",
        "release/2026-06",
        ("test",),
    )

    assert "/branches/release%2F2026-06/protection" in command


def test_render_label_commands_are_copyable_and_idempotent() -> None:
    commands = render_label_commands("QuantStrategyLab/UsEquitySnapshotPipelines")

    assert "gh label create auto-merge-ok --repo QuantStrategyLab/UsEquitySnapshotPipelines" in commands
    assert "gh label edit auto-merge-ok --repo QuantStrategyLab/UsEquitySnapshotPipelines" in commands
    assert "gh label create human-review-required --repo QuantStrategyLab/UsEquitySnapshotPipelines" in commands
    assert "gh label edit human-review-required --repo QuantStrategyLab/UsEquitySnapshotPipelines" in commands
    assert "Codex remediation PR requires human review before merge" in commands


def test_render_enablement_plan_includes_readiness_enable_and_rollback_commands() -> None:
    plan = render_enablement_plan(
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        required_status_checks=("test",),
        readiness={
            "ready": False,
            "skipped": False,
            "label": "auto-merge-ok",
            "human_review_label": "human-review-required",
            "checks": [
                "Loaded auto-merge policy label `auto-merge-ok`.",
                "Loaded high-risk human-review label `human-review-required`.",
            ],
            "errors": ["branch protection is not enabled for main"],
        },
        discovered_check_contexts=["test", "monthly-review"],
        discovered_branch_protection_status_checks=["test (strict)"],
        discovered_ruleset_status_checks=["test (strict)", "lint (non-strict)"],
        auto_merge_variable_value="false",
        discovery_warnings=["Could not read commit statuses for abc123: HTTP 403"],
        protection_discovery_warnings=["No other branch protection checks found."],
        ruleset_discovery_warnings=["No other active branch rulesets found."],
    )

    assert "This plan is read-only" in plan
    assert "## Current guarded auto-merge variable" in plan
    assert "- `CODEX_AUDIT_AUTO_MERGE`: `false`" in plan
    assert "runs only when this value is `true`, `True`, or `TRUE`" in plan
    assert "Human-review label: `human-review-required`" in plan
    assert "feedback retry workflow are stable" in plan
    assert "both `auto-merge-ok` and `human-review-required` labels exist" in plan
    assert "gh label create human-review-required --repo QuantStrategyLab/UsEquitySnapshotPipelines" in plan
    assert "branch protection is not enabled for main" in plan
    assert "## Discovered check contexts" in plan
    assert "- `test`" in plan
    assert "## Discovered branch ruleset required checks" in plan
    assert "- `test (strict)`" in plan
    assert "- `lint (non-strict)`" in plan
    assert "No other active branch rulesets found." in plan
    assert "Could not read commit statuses for abc123: HTTP 403" in plan
    assert "## Discovered branch protection required checks" in plan
    assert "No other branch protection checks found." in plan
    assert "Compare the expected checks with the discovered contexts, branch protection checks, and ruleset checks" in plan
    assert "## Enablement preflight checklist" in plan
    assert "- [ ] Readiness reports `Ready: yes`" in plan
    assert "- [ ] `CODEX_AUDIT_AUTO_MERGE` is still `false` or unset before the final enablement step." in plan
    assert "- [ ] Required status checks are configured as strict/up-to-date checks: `test`." in plan
    assert "- [ ] Any existing branch protection or ruleset settings were reviewed" in plan
    assert "- [ ] The first guarded Codex PR after enablement will be monitored" in plan
    assert "GitHub's branch protection `PUT` API" in plan
    assert "not unintentionally overwritten" in plan
    assert "CODEX_AUDIT_READINESS_TOKEN" in plan
    assert "scripts/check_codex_auto_merge_readiness.py" in plan
    assert "--required-status-check test" in plan
    assert "gh variable set CODEX_AUDIT_AUTO_MERGE --repo QuantStrategyLab/UsEquitySnapshotPipelines --body true" in plan
    assert "gh variable set CODEX_AUDIT_AUTO_MERGE --repo QuantStrategyLab/UsEquitySnapshotPipelines --body false" in plan


def test_render_enablement_plan_warns_when_expected_check_is_not_discovered() -> None:
    plan = render_enablement_plan(
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        required_status_checks=("test",),
        readiness={
            "ready": False,
            "skipped": False,
            "label": "auto-merge-ok",
            "human_review_label": "human-review-required",
            "checks": [],
            "errors": ["branch protection is not enabled for main"],
        },
        discovered_check_contexts=["monthly-review"],
    )

    assert "Expected status checks not found among discovered contexts: test" in plan


def test_discover_check_contexts_reads_check_runs_and_statuses(monkeypatch) -> None:
    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        assert method == "GET"
        assert token == "token"
        if url.endswith("/branches/main"):
            return {"commit": {"sha": "abc123456789"}}
        if url.endswith("/commits/abc123456789/check-runs?per_page=100"):
            return {"check_runs": [{"name": "test"}, {"name": "monthly-review"}, {"name": ""}]}
        if url.endswith("/commits/abc123456789/statuses?per_page=100"):
            return [{"context": "legacy-ci"}, {"context": ""}]
        raise AssertionError(url)

    monkeypatch.setattr(planner, "github_request", fake_github_request)

    contexts, warnings = discover_check_contexts(
        api_url="https://api.github.com",
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
    )

    assert contexts == ["legacy-ci", "monthly-review", "test"]
    assert warnings == []


def test_discover_branch_ruleset_status_checks_reports_strict_and_non_strict(monkeypatch) -> None:
    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        assert method == "GET"
        assert token == "token"
        assert url.endswith("/rules/branches/main?per_page=100")
        return [
            {
                "type": "required_status_checks",
                "parameters": {
                    "strict_required_status_checks_policy": True,
                    "required_status_checks": [{"context": "test"}],
                },
            },
            {
                "type": "required_status_checks",
                "parameters": {
                    "strict_required_status_checks_policy": False,
                    "required_status_checks": [{"context": "lint"}],
                },
            },
        ]

    monkeypatch.setattr(planner, "github_request", fake_github_request)

    contexts, warnings = discover_branch_ruleset_status_checks(
        api_url="https://api.github.com",
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
    )

    assert contexts == ["lint (non-strict)", "test (strict)"]
    assert warnings == []


def test_discover_branch_ruleset_status_checks_reports_missing_rules(monkeypatch) -> None:
    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        return []

    monkeypatch.setattr(planner, "github_request", fake_github_request)

    contexts, warnings = discover_branch_ruleset_status_checks(
        api_url="https://api.github.com",
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
    )

    assert contexts == []
    assert warnings == ["No required status check rulesets found for main."]


def test_discover_branch_protection_status_checks_reports_strict_contexts(monkeypatch) -> None:
    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        assert method == "GET"
        assert token == "token"
        assert url.endswith("/branches/main/protection")
        return {
            "required_status_checks": {
                "strict": True,
                "contexts": ["test"],
                "checks": [{"context": "lint"}, {"context": ""}],
            }
        }

    monkeypatch.setattr(planner, "github_request", fake_github_request)

    contexts, warnings = discover_branch_protection_status_checks(
        api_url="https://api.github.com",
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
    )

    assert contexts == ["lint (strict)", "test (strict)"]
    assert warnings == []


def test_discover_branch_protection_status_checks_reports_missing_protection(monkeypatch) -> None:
    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        raise GitHubApiError(method, url, 404, "Not Found")

    monkeypatch.setattr(planner, "github_request", fake_github_request)

    contexts, warnings = discover_branch_protection_status_checks(
        api_url="https://api.github.com",
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
    )

    assert contexts == []
    assert warnings == ["No branch protection found for main."]


def test_discover_repository_variable_reads_current_value(monkeypatch) -> None:
    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        assert method == "GET"
        assert token == "token"
        assert url.endswith("/actions/variables/CODEX_AUDIT_AUTO_MERGE")
        return {"name": "CODEX_AUDIT_AUTO_MERGE", "value": "false"}

    monkeypatch.setattr(planner, "github_request", fake_github_request)

    value, warnings = discover_repository_variable(
        api_url="https://api.github.com",
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        token="token",
    )

    assert value == "false"
    assert warnings == []


def test_discover_repository_variable_reports_missing_variable(monkeypatch) -> None:
    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        raise GitHubApiError(method, url, 404, "Not Found")

    monkeypatch.setattr(planner, "github_request", fake_github_request)

    value, warnings = discover_repository_variable(
        api_url="https://api.github.com",
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        token="token",
    )

    assert value is None
    assert warnings == ["Repository variable CODEX_AUDIT_AUTO_MERGE is not set."]


def test_discover_check_contexts_reports_branch_errors(monkeypatch) -> None:
    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        raise GitHubApiError(method, url, 403, "Forbidden")

    monkeypatch.setattr(planner, "github_request", fake_github_request)

    contexts, warnings = discover_check_contexts(
        api_url="https://api.github.com",
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
    )

    assert contexts == []
    assert warnings == ["Could not read branch main for check context discovery: HTTP 403"]
