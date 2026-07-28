from __future__ import annotations

from scripts.check_codex_auto_merge_readiness import GitHubApiError, ReadinessError
import scripts.plan_codex_auto_merge_enablement as planner
from scripts.plan_codex_auto_merge_enablement import (
    branch_protection_payload,
    discover_branch_protection_status_checks,
    discover_branch_ruleset_status_checks,
    discover_check_contexts,
    render_branch_protection_command,
    render_enablement_plan,
)


def test_branch_protection_payload_is_strict_and_rejects_blank() -> None:
    assert branch_protection_payload(("test",))["required_status_checks"] == {"strict": True, "contexts": ["test"]}
    try:
        branch_protection_payload(("",))
    except ReadinessError:
        pass
    else:
        raise AssertionError("blank required check must fail")


def test_rendered_plan_is_read_only_and_never_enables_auto_merge() -> None:
    plan = render_enablement_plan(
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        required_status_checks=("test",),
        readiness={"ready": False, "skipped": False, "label": "auto-merge-ok", "human_review_label": "human-review-required", "checks": [], "errors": ["missing"]},
        discovered_check_contexts=["test"],
    )
    assert "This plan is read-only" in plan
    assert "Automatic merge remains disabled." in plan
    assert "CODEX_AUDIT_AUTO_MERGE" not in plan
    assert "gh variable set" not in plan
    assert "gh api --method PUT" in plan
    assert render_branch_protection_command("owner/repo", "release/x", ("test",)).find("release%2Fx") >= 0


def test_discovery_reads_contexts_and_rule_statuses(monkeypatch) -> None:
    def fake_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        if url.endswith("/branches/main"):
            return {"commit": {"sha": "abc"}}
        if url.endswith("/commits/abc/check-runs?per_page=100"):
            return {"check_runs": [{"name": "test"}]}
        if url.endswith("/commits/abc/statuses?per_page=100"):
            return [{"context": "lint"}]
        if url.endswith("/rules/branches/main?per_page=100"):
            return [{"type": "required_status_checks", "parameters": {"strict_required_status_checks_policy": True, "required_status_checks": [{"context": "test"}]}}]
        if url.endswith("/branches/main/protection"):
            return {"required_status_checks": {"strict": True, "contexts": ["test"]}}
        raise AssertionError(url)

    monkeypatch.setattr(planner, "github_request", fake_request)
    assert discover_check_contexts(api_url="https://api.github.com", repo="owner/repo", branch="main", token="token")[0] == ["lint", "test"]
    assert discover_branch_ruleset_status_checks(api_url="https://api.github.com", repo="owner/repo", branch="main", token="token")[0] == ["test (strict)"]
    assert discover_branch_protection_status_checks(api_url="https://api.github.com", repo="owner/repo", branch="main", token="token")[0] == ["test (strict)"]


def test_discovery_fails_closed_to_warnings(monkeypatch) -> None:
    monkeypatch.setattr(planner, "github_request", lambda method, url, token, payload=None, *, timeout=30: (_ for _ in ()).throw(GitHubApiError(method, url, 404, "Not Found")))
    assert discover_branch_ruleset_status_checks(api_url="https://api.github.com", repo="owner/repo", branch="main", token="token")[1] == ["No active branch rulesets found for main."]
    assert discover_branch_protection_status_checks(api_url="https://api.github.com", repo="owner/repo", branch="main", token="token")[1] == ["No branch protection found for main."]
