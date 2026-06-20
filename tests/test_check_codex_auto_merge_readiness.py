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
    validate_auto_merge_workflow,
    validate_codex_feedback_workflow,
    validate_monthly_review_workflow,
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


def write_workflow(path: Path) -> None:
    path.write_text(
        """
name: Auto Merge Codex Remediation PR
on:
  workflow_run:
    workflows: ["CI"]
    types: [completed]
jobs:
  auto-merge:
    if: github.event.workflow_run.conclusion == 'success' && github.event.workflow_run.head_repository.full_name == github.repository && startsWith(github.event.workflow_run.head_branch, 'codex/monthly-review-issue-') && contains(fromJSON('["true","True","TRUE"]'), vars.CODEX_AUDIT_AUTO_MERGE)
    permissions:
      contents: write
      issues: write
          pull-requests: write
    steps:
      - run: |
          gh pr list --repo "${{ github.repository }}" --json number,headRepository,isCrossRepository --jq 'headRepository.nameWithOwner'
          gh pr view 12 --repo "${{ github.repository }}" --json files,changedFiles,additions,deletions,reviewDecision
          gh api --paginate --slurp "/repos/${{ github.repository }}/pulls/${{ steps.pr.outputs.pr_number }}/files?per_page=100" > data/output/codex_auto_merge/pr_files_pages.json
          python3 - <<'PY'
          item = {}
          files = [{"status": item.get("status", ""), "previous_filename": item.get("previous_filename", "")}]
          PY
          python3 scripts/evaluate_codex_pr_merge.py --require-same-repository
      - name: Comment auto-merge guard decision
        run: python3 scripts/post_codex_auto_merge_decision_comment.py --repo "${{ github.repository }}" --pr-json data/output/codex_auto_merge/pr.json --decision-json data/output/codex_auto_merge/decision.json --output-file data/output/codex_auto_merge/guard_decision_comment.md --sync-labels
      - name: Check guarded auto-merge readiness before merge
        id: merge_readiness
        continue-on-error: true
        env:
          GITHUB_TOKEN: ${{ secrets.CODEX_AUDIT_READINESS_TOKEN || secrets.GITHUB_TOKEN }}
          CODEX_AUDIT_REQUIRED_STATUS_CHECKS: ${{ vars.CODEX_AUDIT_REQUIRED_STATUS_CHECKS || 'test' }}
        run: python3 scripts/check_codex_auto_merge_readiness.py --repo "${{ github.repository }}" --branch main --auto-merge true --required-status-checks "${CODEX_AUDIT_REQUIRED_STATUS_CHECKS}"
      - name: Comment merge-time readiness failure
        run: python3 scripts/post_codex_auto_merge_decision_comment.py --repo "${{ github.repository }}" --pr-json data/output/codex_auto_merge/pr.json --decision-json data/output/codex_auto_merge/readiness_decision.json --output-file data/output/codex_auto_merge/readiness_guard_decision_comment.md --sync-labels # merge_readiness_failed
      - name: Fail on merge-time readiness failure
        run: exit 1
      - run: |
          if [ "${{ steps.merge_readiness.outcome == 'success' }}" = "true" ]; then gh pr merge 12 --repo "${{ github.repository }}" --rebase --delete-branch --match-head-commit "${{ github.event.workflow_run.head_sha }}"; fi
      - name: Upload Codex auto-merge diagnostics
        if: always()
        uses: actions/upload-artifact@v7
        with:
          name: codex-auto-merge-${{ github.run_id }}
          path: data/output/codex_auto_merge/
          if-no-files-found: warn
""",
        encoding="utf-8",
    )


def test_validate_auto_merge_workflow_requires_changed_line_fields(tmp_path: Path) -> None:
    workflow_path = tmp_path / "auto_merge.yml"
    write_workflow(workflow_path)
    workflow_path.write_text(
        workflow_path.read_text(encoding="utf-8").replace(",additions,deletions", ""),
        encoding="utf-8",
    )

    errors = validate_auto_merge_workflow(workflow_path)

    assert "auto-merge workflow missing PR additions for changed-line guard" in errors
    assert "auto-merge workflow missing PR deletions for changed-line guard" in errors


def test_validate_auto_merge_workflow_requires_review_decision(tmp_path: Path) -> None:
    workflow_path = tmp_path / "auto_merge.yml"
    write_workflow(workflow_path)
    workflow_path.write_text(
        workflow_path.read_text(encoding="utf-8").replace(",reviewDecision", ""),
        encoding="utf-8",
    )

    errors = validate_auto_merge_workflow(workflow_path)

    assert "auto-merge workflow missing PR review decision guard" in errors



def test_validate_auto_merge_workflow_requires_paginated_file_status_metadata(tmp_path: Path) -> None:
    workflow_path = tmp_path / "auto_merge.yml"
    write_workflow(workflow_path)
    workflow_path.write_text(
        workflow_path.read_text(encoding="utf-8")
        .replace(
            'gh api --paginate --slurp "/repos/${{ github.repository }}/pulls/${{ steps.pr.outputs.pr_number }}/files?per_page=100" > data/output/codex_auto_merge/pr_files_pages.json\n',
            "",
        )
        .replace('"status": item.get("status", ""), ', "")
        .replace('"previous_filename": item.get("previous_filename", "")', ""),
        encoding="utf-8",
    )

    errors = validate_auto_merge_workflow(workflow_path)

    assert "auto-merge workflow missing paginated PR file metadata fetch" in errors
    assert "auto-merge workflow missing PR file status guard" in errors
    assert "auto-merge workflow missing PR previous filename capture" in errors

def test_evaluate_readiness_skips_when_auto_merge_is_false(tmp_path: Path) -> None:
    decision = evaluate_readiness(
        auto_merge=False,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="",
        policy_path=tmp_path / "missing-policy.json",
        workflow_path=tmp_path / "missing-workflow.yml",
    )

    assert decision["ready"]
    assert decision["skipped"]
    assert decision["errors"] == []


def test_evaluate_readiness_passes_with_label_and_protected_branch(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    workflow_path = tmp_path / "auto_merge.yml"
    write_policy(policy_path)
    write_workflow(workflow_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        assert method == "GET"
        assert token == "token"
        if url.endswith("/labels/auto-merge-ok") or url.endswith("/labels/human-review-required"):
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
        workflow_path=workflow_path,
    )

    assert decision["ready"]
    assert not decision["skipped"]
    assert decision["label"] == "auto-merge-ok"
    assert decision["human_review_label"] == "human-review-required"
    assert decision["errors"] == []
    assert "Remote labels exist and branch protection or rulesets require status checks: test." in decision["checks"]
    assert (
        "Codex feedback workflow contains same-repository retry, limit, and Bridge dispatch checks."
        in decision["checks"]
    )
    assert "Monthly review workflow contains readiness-gated Bridge dispatch checks." in decision["checks"]


def test_evaluate_readiness_fails_when_policy_allows_control_plane_exact_path(
    tmp_path: Path, monkeypatch
) -> None:
    policy_path = tmp_path / "policy.json"
    workflow_path = tmp_path / "auto_merge.yml"
    write_policy(policy_path, medium_exact=["scripts/evaluate_codex_pr_merge.py"])
    write_workflow(workflow_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        raise AssertionError("remote readiness should not run when local policy guardrail fails")

    monkeypatch.setattr(readiness, "github_request", fake_github_request)

    decision = evaluate_readiness(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
        policy_path=policy_path,
        workflow_path=workflow_path,
    )

    assert not decision["ready"]
    assert decision["errors"] == [
        "auto-merge policy must keep control-plane paths high-risk: scripts/evaluate_codex_pr_merge.py"
    ]


def test_evaluate_readiness_fails_when_policy_allows_control_plane_prefix(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    workflow_path = tmp_path / "auto_merge.yml"
    write_policy(policy_path, low_prefixes=[".github/"])
    write_workflow(workflow_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        raise AssertionError("remote readiness should not run when local policy guardrail fails")

    monkeypatch.setattr(readiness, "github_request", fake_github_request)

    decision = evaluate_readiness(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
        policy_path=policy_path,
        workflow_path=workflow_path,
    )

    assert not decision["ready"]
    assert decision["errors"] == [
        "auto-merge policy must keep control-plane paths high-risk: "
        ".github/codex_auto_merge_policy.json, .github/workflows/*"
    ]


def test_evaluate_readiness_fails_when_label_is_missing(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    workflow_path = tmp_path / "auto_merge.yml"
    write_policy(policy_path)
    write_workflow(workflow_path)

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
        workflow_path=workflow_path,
    )

    assert not decision["ready"]
    assert decision["errors"] == ["auto-merge label is missing: auto-merge-ok"]
    assert "auto-merge label is missing" in render_summary(decision)


def test_evaluate_readiness_fails_when_human_review_label_is_missing(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    workflow_path = tmp_path / "auto_merge.yml"
    write_policy(policy_path)
    write_workflow(workflow_path)

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
        workflow_path=workflow_path,
    )

    assert not decision["ready"]
    assert decision["errors"] == ["human-review label is missing: human-review-required"]
    assert "human-review label is missing" in render_summary(decision)


def test_evaluate_readiness_fails_when_policy_labels_match(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    workflow_path = tmp_path / "auto_merge.yml"
    write_policy(policy_path, human_review_label="auto-merge-ok")
    write_workflow(workflow_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        raise AssertionError("remote readiness should not run when policy labels collide")

    monkeypatch.setattr(readiness, "github_request", fake_github_request)

    decision = evaluate_readiness(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
        policy_path=policy_path,
        workflow_path=workflow_path,
    )

    assert not decision["ready"]
    assert decision["errors"] == ["auto-merge and human-review labels must be distinct before enabling auto-merge"]


def test_evaluate_readiness_fails_when_branch_is_not_protected(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    workflow_path = tmp_path / "auto_merge.yml"
    write_policy(policy_path)
    write_workflow(workflow_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        if url.endswith("/labels/auto-merge-ok") or url.endswith("/labels/human-review-required"):
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
        workflow_path=workflow_path,
    )

    assert not decision["ready"]
    assert decision["errors"] == ["branch protection is not enabled for main"]


def test_evaluate_readiness_accepts_strict_required_status_check_ruleset(
    tmp_path: Path, monkeypatch
) -> None:
    policy_path = tmp_path / "policy.json"
    workflow_path = tmp_path / "auto_merge.yml"
    write_policy(policy_path)
    write_workflow(workflow_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        if url.endswith("/labels/auto-merge-ok") or url.endswith("/labels/human-review-required"):
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
        workflow_path=workflow_path,
    )

    assert decision["ready"]
    assert decision["errors"] == []


def test_evaluate_readiness_rejects_non_strict_required_status_check_ruleset(
    tmp_path: Path, monkeypatch
) -> None:
    policy_path = tmp_path / "policy.json"
    workflow_path = tmp_path / "auto_merge.yml"
    write_policy(policy_path)
    write_workflow(workflow_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        if url.endswith("/labels/auto-merge-ok") or url.endswith("/labels/human-review-required"):
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
        workflow_path=workflow_path,
    )

    assert not decision["ready"]
    assert decision["errors"] == ["required status checks must require branches to be up to date for main"]


def test_evaluate_readiness_accepts_ruleset_when_legacy_protection_lacks_required_check(
    tmp_path: Path, monkeypatch
) -> None:
    policy_path = tmp_path / "policy.json"
    workflow_path = tmp_path / "auto_merge.yml"
    write_policy(policy_path)
    write_workflow(workflow_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        if url.endswith("/labels/auto-merge-ok") or url.endswith("/labels/human-review-required"):
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
        workflow_path=workflow_path,
    )

    assert decision["ready"]
    assert decision["errors"] == []


def test_evaluate_readiness_fails_when_required_status_check_is_missing(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    workflow_path = tmp_path / "auto_merge.yml"
    write_policy(policy_path)
    write_workflow(workflow_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        if url.endswith("/labels/auto-merge-ok") or url.endswith("/labels/human-review-required"):
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
        workflow_path=workflow_path,
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
    workflow_path = tmp_path / "auto_merge.yml"
    write_policy(policy_path)
    write_workflow(workflow_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        raise AssertionError("remote readiness should not run with invalid required status checks")

    monkeypatch.setattr(readiness, "github_request", fake_github_request)

    decision = evaluate_readiness(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
        policy_path=policy_path,
        workflow_path=workflow_path,
        required_status_checks=("",),
    )

    assert not decision["ready"]
    assert decision["errors"] == [
        "at least one required status check must be configured before enabling auto-merge"
    ]


def test_evaluate_readiness_fails_when_status_checks_are_not_strict(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    workflow_path = tmp_path / "auto_merge.yml"
    write_policy(policy_path)
    write_workflow(workflow_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        if url.endswith("/labels/auto-merge-ok") or url.endswith("/labels/human-review-required"):
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
        workflow_path=workflow_path,
    )

    assert not decision["ready"]
    assert decision["errors"] == ["required status checks must require branches to be up to date for main"]


def test_evaluate_readiness_reports_network_errors_without_traceback(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    workflow_path = tmp_path / "auto_merge.yml"
    write_policy(policy_path)
    write_workflow(workflow_path)

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        raise GitHubApiError(method, url, 0, "certificate verify failed")

    monkeypatch.setattr(readiness, "github_request", fake_github_request)

    decision = evaluate_readiness(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        branch="main",
        token="token",
        policy_path=policy_path,
        workflow_path=workflow_path,
    )

    assert not decision["ready"]
    assert decision["errors"] == [
        "auto-merge label check failed: certificate verify failed",
        "human-review label check failed: certificate verify failed",
        "branch protection check failed: certificate verify failed",
    ]


def test_evaluate_readiness_fails_when_auto_merge_workflow_lacks_guard(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    workflow_path = tmp_path / "auto_merge.yml"
    write_policy(policy_path)
    workflow_path.write_text("name: unsafe\n", encoding="utf-8")

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        if url.endswith("/labels/auto-merge-ok") or url.endswith("/labels/human-review-required"):
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
        workflow_path=workflow_path,
    )

    assert not decision["ready"]
    assert "auto-merge workflow missing source merge guard script" in decision["errors"]
    assert "auto-merge workflow missing same-repository guard" in decision["errors"]
    assert "auto-merge workflow missing auto-merge guard decision comment" in decision["errors"]
    assert "auto-merge workflow missing auto-merge guard decision comment script" in decision["errors"]
    assert "auto-merge workflow missing auto-merge guard decision label hygiene" in decision["errors"]
    assert "auto-merge workflow missing merge-time readiness failure comment" in decision["errors"]
    assert "auto-merge workflow missing merge-time readiness failure decision" in decision["errors"]
    assert "auto-merge workflow missing merge-time readiness failure hard stop" in decision["errors"]
    assert "auto-merge workflow missing merge gated by readiness outcome" in decision["errors"]
    assert "auto-merge workflow missing auto-merge diagnostic artifact upload" in decision["errors"]
    assert "auto-merge workflow missing auto-merge diagnostic artifact always uploads" in decision["errors"]
    assert "auto-merge workflow missing auto-merge diagnostic artifact path" in decision["errors"]


def test_validate_codex_feedback_workflow_requires_retry_dispatch_guards(tmp_path: Path) -> None:
    workflow_path = tmp_path / "codex_pr_feedback.yml"
    workflow_path.write_text("name: unsafe\n", encoding="utf-8")

    errors = validate_codex_feedback_workflow(workflow_path)

    assert "Codex feedback workflow missing CI same-repository guard" in errors
    assert "Codex feedback workflow missing review same-repository guard" in errors
    assert "Codex feedback workflow missing Bridge dispatch command" in errors
    assert "Codex feedback workflow missing feedback retry limit" in errors
    assert "Codex feedback workflow missing feedback stale auto-merge label lookup" in errors
    assert "Codex feedback workflow missing feedback stale auto-merge policy label validation" in errors
    assert "Codex feedback workflow missing feedback stale auto-merge label cleanup skip" in errors
    assert "Codex feedback workflow missing feedback stale auto-merge label cleanup conditional" in errors
    assert "Codex feedback workflow missing feedback stale auto-merge label cleanup" in errors
    assert "Codex feedback workflow missing paginated feedback comment fetch" in errors
    assert "Codex feedback workflow missing feedback comment pages artifact" in errors
    assert "Codex feedback workflow missing feedback diagnostic artifact upload" in errors
    assert "Codex feedback workflow missing feedback diagnostic artifact always uploads" in errors
    assert "Codex feedback workflow missing CI feedback diagnostic artifact" in errors
    assert "Codex feedback workflow missing review feedback diagnostic artifact" in errors


def test_validate_monthly_review_workflow_requires_readiness_gated_dispatch(tmp_path: Path) -> None:
    workflow_path = tmp_path / "monthly_review.yml"
    workflow_path.write_text("name: unsafe\n", encoding="utf-8")

    errors = validate_monthly_review_workflow(workflow_path)

    assert "Monthly review workflow missing Bridge dispatch workflow" in errors
    assert "Monthly review workflow missing guarded label sync step" in errors
    assert "Monthly review workflow missing guarded label sync script" in errors
    assert "Monthly review workflow missing guarded label sync artifact" in errors
    assert "Monthly review workflow missing preflight label sync input" in errors
    assert "Monthly review workflow missing readiness check" in errors
    assert "Monthly review workflow missing readiness soft-fail" in errors
    assert "Monthly review workflow missing readiness outcome gate" in errors
    assert "Monthly review workflow missing auto-merge downgrade" in errors
    assert "Monthly review workflow missing diagnostic bundle always uploads" in errors
    assert "Monthly review workflow missing diagnostic bundle missing-file warning" in errors


def test_evaluate_readiness_fails_when_codex_feedback_workflow_lacks_retry_guard(
    tmp_path: Path, monkeypatch
) -> None:
    policy_path = tmp_path / "policy.json"
    workflow_path = tmp_path / "auto_merge.yml"
    feedback_workflow_path = tmp_path / "codex_pr_feedback.yml"
    write_policy(policy_path)
    write_workflow(workflow_path)
    feedback_workflow_path.write_text("name: unsafe\n", encoding="utf-8")

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        if url.endswith("/labels/auto-merge-ok") or url.endswith("/labels/human-review-required"):
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
        workflow_path=workflow_path,
        feedback_workflow_path=feedback_workflow_path,
    )

    assert not decision["ready"]
    assert "Codex feedback workflow missing Bridge dispatch command" in decision["errors"]
    assert "Codex feedback workflow missing CI same-repository guard" in decision["errors"]


def test_evaluate_readiness_fails_when_monthly_review_workflow_lacks_readiness_gate(
    tmp_path: Path, monkeypatch
) -> None:
    policy_path = tmp_path / "policy.json"
    workflow_path = tmp_path / "auto_merge.yml"
    monthly_workflow_path = tmp_path / "monthly_review.yml"
    write_policy(policy_path)
    write_workflow(workflow_path)
    monthly_workflow_path.write_text("name: unsafe\n", encoding="utf-8")

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        if url.endswith("/labels/auto-merge-ok") or url.endswith("/labels/human-review-required"):
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
        workflow_path=workflow_path,
        monthly_workflow_path=monthly_workflow_path,
    )

    assert not decision["ready"]
    assert "Monthly review workflow missing readiness check" in decision["errors"]
    assert "Monthly review workflow missing auto-merge downgrade" in decision["errors"]
