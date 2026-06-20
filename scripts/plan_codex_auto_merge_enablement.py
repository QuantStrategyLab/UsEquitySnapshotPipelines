from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
from typing import Any
import urllib.parse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.check_codex_auto_merge_readiness import (
    DEFAULT_API_URL,
    DEFAULT_AUTO_MERGE_WORKFLOW,
    DEFAULT_POLICY_PATH,
    DEFAULT_REQUIRED_STATUS_CHECKS,
    GitHubApiError,
    ReadinessError,
    evaluate_readiness,
    github_request,
    parse_required_status_check_args,
    render_summary,
    validate_required_status_checks,
)

LABEL_COMMANDS = (
    (
        "auto-merge-ok",
        "0E8A16",
        "Guarded Codex remediation PR may be auto-merged after source CI and merge guard pass",
    ),
    (
        "human-review-required",
        "B60205",
        "Codex remediation PR requires human review before merge",
    ),
)
AUTO_MERGE_VARIABLE = "CODEX_AUDIT_AUTO_MERGE"


def branch_protection_payload(required_status_checks: tuple[str, ...]) -> dict[str, Any]:
    required_status_checks = validate_required_status_checks(required_status_checks)
    return {
        "required_status_checks": {
            "strict": True,
            "contexts": list(required_status_checks),
        },
        "enforce_admins": False,
        "required_pull_request_reviews": None,
        "restrictions": None,
    }


def render_branch_protection_command(repo: str, branch: str, required_status_checks: tuple[str, ...]) -> str:
    payload = json.dumps(branch_protection_payload(required_status_checks), ensure_ascii=False, indent=2)
    encoded_branch = urllib.parse.quote(branch, safe="")
    return "\n".join(
        [
            f"gh api --method PUT /repos/{repo}/branches/{encoded_branch}/protection --input - <<'JSON'",
            payload,
            "JSON",
        ]
    )


def render_label_commands(repo: str) -> str:
    lines: list[str] = []
    for name, color, description in LABEL_COMMANDS:
        lines.append(
            " || ".join(
                [
                    (
                        f"gh label create {name} --repo {repo} --color {color} "
                        f"--description {json.dumps(description)}"
                    ),
                    (
                        f"gh label edit {name} --repo {repo} --color {color} "
                        f"--description {json.dumps(description)}"
                    ),
                ]
            )
        )
    return "\n".join(lines)


def discover_check_contexts(*, api_url: str, repo: str, branch: str, token: str) -> tuple[list[str], list[str]]:
    if not token.strip():
        return [], ["GITHUB_TOKEN is not set; skipped remote check context discovery."]

    api_url = api_url.rstrip("/")
    contexts: set[str] = set()
    warnings: list[str] = []
    try:
        branch_payload = github_request("GET", f"{api_url}/repos/{repo}/branches/{branch}", token)
    except GitHubApiError as exc:
        return [], [f"Could not read branch {branch} for check context discovery: HTTP {exc.status_code}"]
    if not isinstance(branch_payload, dict):
        return [], [f"Could not read branch {branch} for check context discovery: invalid response"]

    commit = branch_payload.get("commit")
    sha = commit.get("sha") if isinstance(commit, dict) else None
    if not isinstance(sha, str) or not sha.strip():
        return [], [f"Could not read branch {branch} commit SHA for check context discovery"]
    sha = sha.strip()

    try:
        check_runs = github_request("GET", f"{api_url}/repos/{repo}/commits/{sha}/check-runs?per_page=100", token)
    except GitHubApiError as exc:
        warnings.append(f"Could not read check runs for {sha[:12]}: HTTP {exc.status_code}")
    else:
        if isinstance(check_runs, dict):
            for item in check_runs.get("check_runs") or []:
                if isinstance(item, dict):
                    name = str(item.get("name", "")).strip()
                    if name:
                        contexts.add(name)

    try:
        statuses = github_request("GET", f"{api_url}/repos/{repo}/commits/{sha}/statuses?per_page=100", token)
    except GitHubApiError as exc:
        warnings.append(f"Could not read commit statuses for {sha[:12]}: HTTP {exc.status_code}")
    else:
        if isinstance(statuses, list):
            for item in statuses:
                if isinstance(item, dict):
                    context = str(item.get("context", "")).strip()
                    if context:
                        contexts.add(context)

    if not contexts and not warnings:
        warnings.append(f"No check run or status contexts found for {branch}@{sha[:12]}.")
    return sorted(contexts), warnings


def discover_branch_ruleset_status_checks(
    *,
    api_url: str,
    repo: str,
    branch: str,
    token: str,
) -> tuple[list[str], list[str]]:
    if not token.strip():
        return [], ["GITHUB_TOKEN is not set; skipped branch ruleset discovery."]

    api_url = api_url.rstrip("/")
    encoded_branch = urllib.parse.quote(branch, safe="")
    try:
        payload = github_request(
            "GET",
            f"{api_url}/repos/{repo}/rules/branches/{encoded_branch}?per_page=100",
            token,
        )
    except GitHubApiError as exc:
        if exc.status_code == 404:
            return [], [f"No active branch rulesets found for {branch}."]
        return [], [f"Could not read branch rulesets for {branch}: HTTP {exc.status_code}"]
    if not isinstance(payload, list):
        return [], [f"Could not read branch rulesets for {branch}: invalid response"]

    contexts: set[str] = set()
    strict_contexts: set[str] = set()
    for rule in payload:
        if not isinstance(rule, dict) or rule.get("type") != "required_status_checks":
            continue
        parameters = rule.get("parameters")
        if not isinstance(parameters, dict):
            continue
        strict = parameters.get("strict_required_status_checks_policy") is True
        required_checks = parameters.get("required_status_checks") or []
        if not isinstance(required_checks, list):
            continue
        for item in required_checks:
            if not isinstance(item, dict):
                continue
            context = str(item.get("context", "")).strip()
            if not context:
                continue
            contexts.add(context)
            if strict:
                strict_contexts.add(context)

    discovered = [f"{context} ({'strict' if context in strict_contexts else 'non-strict'})" for context in sorted(contexts)]
    warnings = []
    if not discovered:
        warnings.append(f"No required status check rulesets found for {branch}.")
    return discovered, warnings


def discover_branch_protection_status_checks(
    *,
    api_url: str,
    repo: str,
    branch: str,
    token: str,
) -> tuple[list[str], list[str]]:
    if not token.strip():
        return [], ["GITHUB_TOKEN is not set; skipped branch protection discovery."]

    api_url = api_url.rstrip("/")
    encoded_branch = urllib.parse.quote(branch, safe="")
    try:
        payload = github_request("GET", f"{api_url}/repos/{repo}/branches/{encoded_branch}/protection", token)
    except GitHubApiError as exc:
        if exc.status_code == 404:
            return [], [f"No branch protection found for {branch}."]
        return [], [f"Could not read branch protection for {branch}: HTTP {exc.status_code}"]
    if not isinstance(payload, dict):
        return [], [f"Could not read branch protection for {branch}: invalid response"]

    required_status_checks = payload.get("required_status_checks")
    if not isinstance(required_status_checks, dict):
        return [], [f"Branch protection has no required status checks for {branch}."]
    strict = required_status_checks.get("strict") is True
    contexts = {
        str(item).strip()
        for item in required_status_checks.get("contexts") or []
        if str(item).strip()
    }
    checks = required_status_checks.get("checks") or []
    if isinstance(checks, list):
        contexts.update(
            str(item.get("context", "")).strip()
            for item in checks
            if isinstance(item, dict) and str(item.get("context", "")).strip()
        )
    discovered = [f"{context} ({'strict' if strict else 'non-strict'})" for context in sorted(contexts)]
    warnings = []
    if not discovered:
        warnings.append(f"No branch protection required status checks found for {branch}.")
    return discovered, warnings


def discover_repository_variable(
    *,
    api_url: str,
    repo: str,
    token: str,
    name: str = AUTO_MERGE_VARIABLE,
) -> tuple[str | None, list[str]]:
    if not token.strip():
        return None, [f"GITHUB_TOKEN is not set; skipped {name} repository variable discovery."]

    api_url = api_url.rstrip("/")
    encoded_name = urllib.parse.quote(name, safe="")
    try:
        payload = github_request("GET", f"{api_url}/repos/{repo}/actions/variables/{encoded_name}", token)
    except GitHubApiError as exc:
        if exc.status_code == 404:
            return None, [f"Repository variable {name} is not set."]
        return None, [f"Could not read repository variable {name}: HTTP {exc.status_code}"]
    if not isinstance(payload, dict):
        return None, [f"Could not read repository variable {name}: invalid response"]

    value = payload.get("value")
    if not isinstance(value, str):
        return None, [f"Repository variable {name} has an invalid value."]
    return value, []


def render_enablement_plan(
    *,
    repo: str,
    branch: str,
    required_status_checks: tuple[str, ...],
    readiness: dict[str, Any],
    discovered_check_contexts: list[str] | None = None,
    discovered_branch_protection_status_checks: list[str] | None = None,
    discovered_ruleset_status_checks: list[str] | None = None,
    auto_merge_variable_value: str | None = None,
    discovery_warnings: list[str] | None = None,
    protection_discovery_warnings: list[str] | None = None,
    ruleset_discovery_warnings: list[str] | None = None,
    variable_discovery_warnings: list[str] | None = None,
) -> str:
    branch_command = render_branch_protection_command(repo, branch, required_status_checks)
    label_commands = render_label_commands(repo)
    required_checks = ", ".join(f"`{item}`" for item in required_status_checks)
    discovered_check_contexts = discovered_check_contexts or []
    discovered_branch_protection_status_checks = discovered_branch_protection_status_checks or []
    discovered_ruleset_status_checks = discovered_ruleset_status_checks or []
    discovery_warnings = discovery_warnings or []
    protection_discovery_warnings = protection_discovery_warnings or []
    ruleset_discovery_warnings = ruleset_discovery_warnings or []
    variable_discovery_warnings = variable_discovery_warnings or []
    expected_missing = sorted(set(required_status_checks) - set(discovered_check_contexts)) if discovered_check_contexts else []
    variable_section_lines = [
        "## Current guarded auto-merge variable",
        "",
    ]
    if auto_merge_variable_value is None:
        variable_section_lines.append(f"- `{AUTO_MERGE_VARIABLE}`: unknown or not set")
    else:
        variable_section_lines.append(f"- `{AUTO_MERGE_VARIABLE}`: `{auto_merge_variable_value}`")
    variable_section_lines.append("- The source auto-merge workflow runs only when this value is `true`, `True`, or `TRUE`.")
    if variable_discovery_warnings:
        variable_section_lines.extend(["", "### Variable discovery warnings"])
        variable_section_lines.extend(f"- {item}" for item in variable_discovery_warnings)
    variable_section = "\n".join(variable_section_lines).strip() + "\n\n"
    discovered_section = ""
    if discovered_check_contexts or discovery_warnings:
        discovered_section_lines = ["## Discovered check contexts", ""]
        if discovered_check_contexts:
            discovered_section_lines.extend(f"- `{item}`" for item in discovered_check_contexts)
        if expected_missing:
            discovery_warnings = [
                f"Expected status checks not found among discovered contexts: {', '.join(expected_missing)}",
                *discovery_warnings,
            ]
        if discovery_warnings:
            discovered_section_lines.extend(["", "### Discovery warnings"])
            discovered_section_lines.extend(f"- {item}" for item in discovery_warnings)
        discovered_section = "\n".join(discovered_section_lines).strip() + "\n\n"
    protection_section = ""
    if discovered_branch_protection_status_checks or protection_discovery_warnings:
        protection_section_lines = ["## Discovered branch protection required checks", ""]
        if discovered_branch_protection_status_checks:
            protection_section_lines.extend(f"- `{item}`" for item in discovered_branch_protection_status_checks)
        if protection_discovery_warnings:
            protection_section_lines.extend(["", "### Branch protection discovery warnings"])
            protection_section_lines.extend(f"- {item}" for item in protection_discovery_warnings)
        protection_section = "\n".join(protection_section_lines).strip() + "\n\n"
    ruleset_section = ""
    if discovered_ruleset_status_checks or ruleset_discovery_warnings:
        ruleset_section_lines = ["## Discovered branch ruleset required checks", ""]
        if discovered_ruleset_status_checks:
            ruleset_section_lines.extend(f"- `{item}`" for item in discovered_ruleset_status_checks)
        if ruleset_discovery_warnings:
            ruleset_section_lines.extend(["", "### Ruleset discovery warnings"])
            ruleset_section_lines.extend(f"- {item}" for item in ruleset_discovery_warnings)
        ruleset_section = "\n".join(ruleset_section_lines).strip() + "\n\n"
    preflight_section = (
        "## Enablement preflight checklist\n\n"
        "- [ ] Readiness reports `Ready: yes` after labels and branch protection or rulesets are configured.\n"
        f"- [ ] `{AUTO_MERGE_VARIABLE}` is still `false` or unset before the final enablement step.\n"
        f"- [ ] Required status checks are configured as strict/up-to-date checks: {required_checks}.\n"
        "- [ ] Any existing branch protection or ruleset settings were reviewed before applying the generated `PUT` command.\n"
        "- [ ] The first guarded Codex PR after enablement will be monitored, and the rollback command below is ready.\n\n"
    )
    return (
        "# Codex guarded auto-merge enablement plan\n\n"
        "This plan is read-only. It does not modify GitHub repository settings.\n\n"
        "## Current readiness\n\n"
        f"{render_summary(readiness)}\n"
        f"{variable_section}"
        f"{discovered_section}"
        f"{protection_section}"
        f"{ruleset_section}"
        f"{preflight_section}"
        "## Manual enablement steps\n\n"
        "1. Confirm that source CI check contexts, merge guard, and feedback retry workflow are stable, and both `auto-merge-ok` and "
        "`human-review-required` labels exist. This plan expects "
        f"{required_checks}.\n"
        "   Compare the expected checks with the discovered contexts, branch protection checks, and ruleset checks above before applying branch protection.\n"
        "2. Create or update the labels used by the unattended and human-review paths:\n\n"
        "```bash\n"
        f"{label_commands}\n"
        "```\n\n"
        "3. Apply minimal branch protection or an equivalent active repository ruleset that requires the CI status check before merge.\n"
        "   The command below uses GitHub's branch protection `PUT` API. If branch protection already exists, "
        "review and merge the generated payload with the existing rules before running it so review, "
        "restriction, or admin-enforcement settings are not unintentionally overwritten:\n\n"
        "```bash\n"
        f"{branch_command}\n"
        "```\n\n"
        "4. Verify readiness again before enabling dispatch-time auto-merge requests:\n\n"
        "   If GitHub Actions' default token cannot read branch protection or label settings in this repository, "
        "configure `CODEX_AUDIT_READINESS_TOKEN` as a source-repository secret for readiness checks only.\n\n"
        "```bash\n"
        "GITHUB_TOKEN=\"$(gh auth token)\" .venv/bin/python scripts/check_codex_auto_merge_readiness.py \\\n"
        f"  --repo {repo} \\\n"
        f"  --branch {branch} \\\n"
        "  --auto-merge true \\\n"
        + "".join(f"  --required-status-check {check} \\\n" for check in required_status_checks)
        + "  --summary-file data/output/codex_auto_merge_readiness.md\n"
        "```\n\n"
        "5. Only after readiness reports `Ready: yes`, enable guarded requests from monthly review:\n\n"
        "```bash\n"
        "gh variable set CODEX_AUDIT_AUTO_MERGE --repo "
        f"{repo} --body true\n"
        "```\n\n"
        "## Rollback\n\n"
        "```bash\n"
        "gh variable set CODEX_AUDIT_AUTO_MERGE --repo "
        f"{repo} --body false\n"
        "```\n\n"
        "High-risk PRs remain blocked by the source merge guard even after enablement.\n"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render a read-only Codex guarded auto-merge enablement plan.")
    parser.add_argument("--repo", required=True)
    parser.add_argument("--branch", required=True)
    parser.add_argument("--required-status-check", action="append")
    parser.add_argument(
        "--required-status-checks",
        default=os.environ.get("CODEX_AUDIT_REQUIRED_STATUS_CHECKS", ""),
        help="Comma- or newline-separated required status check contexts. Repeated --required-status-check is still supported.",
    )
    parser.add_argument("--policy-file", type=Path, default=DEFAULT_POLICY_PATH)
    parser.add_argument("--workflow-file", type=Path, default=DEFAULT_AUTO_MERGE_WORKFLOW)
    parser.add_argument("--api-url", default=DEFAULT_API_URL)
    parser.add_argument("--output-file", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        required_status_checks = parse_required_status_check_args(
            args.required_status_check,
            args.required_status_checks,
        )
    except ReadinessError as exc:
        print(f"Invalid required status checks: {exc}", file=sys.stderr)
        return 2
    token = os.environ.get("GITHUB_TOKEN", "")
    readiness = evaluate_readiness(
        auto_merge=True,
        repo=args.repo,
        branch=args.branch,
        token=token,
        policy_path=args.policy_file,
        workflow_path=args.workflow_file,
        api_url=args.api_url,
        required_status_checks=required_status_checks,
    )
    discovered_contexts, discovery_warnings = discover_check_contexts(
        api_url=args.api_url,
        repo=args.repo,
        branch=args.branch,
        token=token,
    )
    discovered_ruleset_checks, ruleset_discovery_warnings = discover_branch_ruleset_status_checks(
        api_url=args.api_url,
        repo=args.repo,
        branch=args.branch,
        token=token,
    )
    discovered_protection_checks, protection_discovery_warnings = discover_branch_protection_status_checks(
        api_url=args.api_url,
        repo=args.repo,
        branch=args.branch,
        token=token,
    )
    auto_merge_variable_value, variable_discovery_warnings = discover_repository_variable(
        api_url=args.api_url,
        repo=args.repo,
        token=token,
    )
    plan = render_enablement_plan(
        repo=args.repo,
        branch=args.branch,
        required_status_checks=required_status_checks,
        readiness=readiness,
        discovered_check_contexts=discovered_contexts,
        discovered_branch_protection_status_checks=discovered_protection_checks,
        discovered_ruleset_status_checks=discovered_ruleset_checks,
        auto_merge_variable_value=auto_merge_variable_value,
        discovery_warnings=discovery_warnings,
        protection_discovery_warnings=protection_discovery_warnings,
        ruleset_discovery_warnings=ruleset_discovery_warnings,
        variable_discovery_warnings=variable_discovery_warnings,
    )
    if args.output_file:
        args.output_file.parent.mkdir(parents=True, exist_ok=True)
        args.output_file.write_text(plan, encoding="utf-8")
    print(plan, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
