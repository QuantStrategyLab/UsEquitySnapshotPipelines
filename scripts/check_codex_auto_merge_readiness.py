from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


DEFAULT_API_URL = "https://api.github.com"
DEFAULT_POLICY_PATH = Path(".github/codex_auto_merge_policy.json")
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_REQUIRED_STATUS_CHECKS = ("test",)
DEFAULT_HUMAN_REVIEW_LABEL = "human-review-required"
CONTROL_PLANE_EXACT_PATHS = (
    ".github/codex_auto_merge_policy.json",
    "scripts/check_codex_auto_merge_readiness.py",
    "scripts/evaluate_codex_pr_merge.py",
    "scripts/post_codex_auto_merge_decision_comment.py",
    "scripts/sync_codex_auto_merge_labels.py",
)
CONTROL_PLANE_PREFIXES = (".github/workflows/",)


class ReadinessError(RuntimeError):
    pass


class GitHubApiError(ReadinessError):
    def __init__(self, method: str, url: str, status_code: int, response_body: str) -> None:
        self.method = method
        self.url = url
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(f"GitHub API {method} {url} failed: {status_code} {response_body[:600]}")


def parse_bool(value: str | bool | None) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def validate_repo(repo: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo):
        raise ReadinessError(f"Invalid repository name: {repo!r}")
    return repo


def validate_branch(branch: str) -> str:
    normalized = branch.strip()
    if not normalized or normalized.startswith("-") or ".." in normalized or any(ch in normalized for ch in " ~^:?*["):
        raise ReadinessError(f"Invalid branch name: {branch!r}")
    return normalized


def validate_required_status_checks(required_status_checks: tuple[str, ...]) -> tuple[str, ...]:
    normalized: list[str] = []
    for check in required_status_checks:
        if not isinstance(check, str):
            raise ReadinessError("required status checks must be string values before enabling auto-merge")
        check = check.strip()
        if not check:
            continue
        if "\n" in check or "\r" in check:
            raise ReadinessError("required status checks must be single-line values before enabling auto-merge")
        normalized.append(check)
    if not normalized:
        raise ReadinessError("at least one required status check must be configured before enabling auto-merge")
    return tuple(normalized)


def parse_required_status_check_args(
    required_status_check: list[str] | None,
    required_status_checks: str | None,
) -> tuple[str, ...]:
    raw_checks: list[str] = list(required_status_check or [])
    if required_status_checks:
        raw_checks.extend(item.strip() for item in re.split(r"[\n,]", required_status_checks) if item.strip())
    if not raw_checks:
        raw_checks = list(DEFAULT_REQUIRED_STATUS_CHECKS)
    return validate_required_status_checks(tuple(raw_checks))


def github_request(
    method: str,
    url: str,
    token: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> Any:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "us-equity-codex-auto-merge-readiness",
    }
    if payload is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            charset = response.headers.get_content_charset("utf-8")
            raw = response.read().decode(charset)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise GitHubApiError(method, url, exc.code, body) from exc
    except urllib.error.URLError as exc:
        raise GitHubApiError(method, url, 0, str(exc.reason)) from exc
    return json.loads(raw) if raw else None


def _normalize_policy_path(value: str) -> str:
    normalized = value.strip()
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def _policy_path_list(section: dict[str, Any], key: str, display_name: str) -> list[str]:
    value = section.get(key, [])
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ReadinessError(
            f"auto-merge policy must define {display_name} as a string list before enabling auto-merge"
        )
    return [normalized for item in value if (normalized := _normalize_policy_path(item))]


def _control_plane_exact_match(path: str) -> str | None:
    if path in CONTROL_PLANE_EXACT_PATHS:
        return path
    for prefix in CONTROL_PLANE_PREFIXES:
        if path.startswith(prefix):
            return f"{prefix}*"
    return None


def _control_plane_prefix_matches(prefix: str) -> list[str]:
    matches: list[str] = []
    for path in CONTROL_PLANE_EXACT_PATHS:
        if path.startswith(prefix):
            matches.append(path)
    for control_prefix in CONTROL_PLANE_PREFIXES:
        if control_prefix.startswith(prefix) or prefix.startswith(control_prefix):
            matches.append(f"{control_prefix}*")
    return matches


def validate_policy_control_plane_guardrails(payload: dict[str, Any]) -> None:
    risk_policy = payload.get("risk_policy")
    if not isinstance(risk_policy, dict):
        raise ReadinessError("auto-merge policy must define risk_policy before enabling auto-merge")
    low_policy = risk_policy.get("low")
    medium_policy = risk_policy.get("medium")
    if not isinstance(low_policy, dict) or not isinstance(medium_policy, dict):
        raise ReadinessError(
            "auto-merge policy must define risk_policy.low and risk_policy.medium before enabling auto-merge"
        )

    auto_merge_matches: list[str] = []
    for path in _policy_path_list(low_policy, "exact", "risk_policy.low.exact") + _policy_path_list(
        medium_policy, "exact", "risk_policy.medium.exact"
    ):
        match = _control_plane_exact_match(path)
        if match:
            auto_merge_matches.append(match)
    for prefix in _policy_path_list(low_policy, "prefixes", "risk_policy.low.prefixes"):
        auto_merge_matches.extend(_control_plane_prefix_matches(prefix))
    if auto_merge_matches:
        matches = ", ".join(sorted(set(auto_merge_matches)))
        raise ReadinessError(f"auto-merge policy must keep control-plane paths high-risk: {matches}")


def load_policy_labels(policy_path: Path) -> dict[str, str]:
    try:
        payload = json.loads(policy_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReadinessError("auto-merge policy must be readable JSON before enabling auto-merge") from exc
    if not isinstance(payload, dict):
        raise ReadinessError("auto-merge policy must be a JSON object before enabling auto-merge")
    if payload.get("version") != 1:
        raise ReadinessError("auto-merge policy version must be 1 before enabling auto-merge")
    if type(payload.get("max_changed_files")) is not int or payload["max_changed_files"] < 1:
        raise ReadinessError("auto-merge policy must define positive integer max_changed_files")
    if type(payload.get("max_changed_lines")) is not int or payload["max_changed_lines"] < 1:
        raise ReadinessError("auto-merge policy must define positive integer max_changed_lines")
    label = payload.get("auto_merge_label")
    if not isinstance(label, str) or not label.strip() or "\n" in label or "\r" in label:
        raise ReadinessError("auto-merge policy must define a single-line auto_merge_label")
    human_review_label = payload.get("human_review_label")
    if (
        not isinstance(human_review_label, str)
        or not human_review_label.strip()
        or "\n" in human_review_label
        or "\r" in human_review_label
    ):
        raise ReadinessError("auto-merge policy must define a single-line human_review_label")
    label = label.strip()
    human_review_label = human_review_label.strip()
    if label == human_review_label:
        raise ReadinessError("auto-merge and human-review labels must be distinct before enabling auto-merge")
    validate_policy_control_plane_guardrails(payload)
    return {"auto_merge_label": label, "human_review_label": human_review_label}


def _protection_status_check_contexts(protection: dict[str, Any]) -> set[str]:
    required_status_checks = protection.get("required_status_checks")
    if not isinstance(required_status_checks, dict):
        return set()
    contexts = {str(item).strip() for item in required_status_checks.get("contexts") or [] if str(item).strip()}
    checks = required_status_checks.get("checks") or []
    if isinstance(checks, list):
        contexts.update(
            str(item.get("context", "")).strip()
            for item in checks
            if isinstance(item, dict) and str(item.get("context", "")).strip()
        )
    return contexts


def _validate_branch_protection(
    protection: dict[str, Any],
    *,
    branch: str,
    required_status_checks: tuple[str, ...],
) -> list[str]:
    errors: list[str] = []
    status_checks = protection.get("required_status_checks")
    if not isinstance(status_checks, dict):
        return [f"required status checks are not enabled for {branch}"]
    if status_checks.get("strict") is not True:
        errors.append(f"required status checks must require branches to be up to date for {branch}")
    contexts = _protection_status_check_contexts(protection)
    missing = [context for context in required_status_checks if context not in contexts]
    if missing:
        errors.append(f"required status checks missing for {branch}: {', '.join(missing)}")
    return errors


def _ruleset_status_check_contexts(rules: list[Any], *, strict_only: bool = False) -> set[str]:
    contexts: set[str] = set()
    for rule in rules:
        if not isinstance(rule, dict) or rule.get("type") != "required_status_checks":
            continue
        parameters = rule.get("parameters")
        if not isinstance(parameters, dict):
            continue
        if strict_only and parameters.get("strict_required_status_checks_policy") is not True:
            continue
        checks = parameters.get("required_status_checks") or []
        if not isinstance(checks, list):
            continue
        contexts.update(
            str(item.get("context", "")).strip()
            for item in checks
            if isinstance(item, dict) and str(item.get("context", "")).strip()
        )
    return contexts


def _validate_branch_endpoint_protection(
    branch_payload: dict[str, Any],
    *,
    branch: str,
    required_status_checks: tuple[str, ...],
) -> list[str]:
    if branch_payload.get("protected") is not True:
        return [f"branch protection is not enabled for {branch}"]
    protection = branch_payload.get("protection")
    if not isinstance(protection, dict):
        return [f"branch protection response is invalid for {branch}"]
    status_checks = protection.get("required_status_checks")
    if not isinstance(status_checks, dict):
        return [f"required status checks are not enabled for {branch}"]
    if status_checks.get("strict") is False:
        return [f"required status checks must require branches to be up to date for {branch}"]
    contexts = _protection_status_check_contexts(protection)
    missing = [context for context in required_status_checks if context not in contexts]
    if missing:
        return [f"required status checks missing for {branch}: {', '.join(missing)}"]
    return []


def _check_branch_endpoint_readiness(
    *,
    api_url: str,
    repo: str,
    encoded_branch: str,
    branch: str,
    token: str,
    required_status_checks: tuple[str, ...],
) -> list[str]:
    try:
        branch_payload = github_request("GET", f"{api_url}/repos/{repo}/branches/{encoded_branch}", token)
    except GitHubApiError as exc:
        if exc.status_code == 404:
            return [f"branch protection is not enabled for {branch}"]
        if exc.status_code == 0:
            return [f"branch protection branch fallback failed: {exc.response_body}"]
        return [f"branch protection branch fallback failed with HTTP {exc.status_code}"]
    if not isinstance(branch_payload, dict):
        return [f"branch protection branch fallback response is invalid for {branch}"]
    return _validate_branch_endpoint_protection(
        branch_payload,
        branch=branch,
        required_status_checks=required_status_checks,
    )


def _validate_branch_rules(
    rules: list[Any],
    *,
    branch: str,
    required_status_checks: tuple[str, ...],
) -> list[str]:
    if not rules:
        return [f"branch protection is not enabled for {branch}"]
    contexts = _ruleset_status_check_contexts(rules)
    if not contexts:
        return [f"required status checks are not enabled for {branch}"]
    missing = [context for context in required_status_checks if context not in contexts]
    if missing:
        return [f"required status checks missing for {branch}: {', '.join(missing)}"]
    strict_contexts = _ruleset_status_check_contexts(rules, strict_only=True)
    if any(context not in strict_contexts for context in required_status_checks):
        return [f"required status checks must require branches to be up to date for {branch}"]
    return []


def _check_branch_ruleset_readiness(
    *,
    api_url: str,
    repo: str,
    encoded_branch: str,
    branch: str,
    token: str,
    required_status_checks: tuple[str, ...],
) -> list[str]:
    try:
        rules = github_request("GET", f"{api_url}/repos/{repo}/rules/branches/{encoded_branch}?per_page=100", token)
    except GitHubApiError as exc:
        if exc.status_code == 404:
            return [f"branch protection is not enabled for {branch}"]
        if exc.status_code == 0:
            return [f"branch ruleset check failed: {exc.response_body}"]
        return [f"branch ruleset check failed with HTTP {exc.status_code}"]
    if not isinstance(rules, list):
        return [f"branch ruleset response is invalid for {branch}"]
    return _validate_branch_rules(
        rules,
        branch=branch,
        required_status_checks=required_status_checks,
    )


def check_remote_readiness(
    *,
    api_url: str,
    repo: str,
    branch: str,
    token: str,
    label: str,
    human_review_label: str,
    required_status_checks: tuple[str, ...] = DEFAULT_REQUIRED_STATUS_CHECKS,
) -> list[str]:
    required_status_checks = validate_required_status_checks(required_status_checks)
    errors: list[str] = []
    encoded_label = urllib.parse.quote(label, safe="")
    encoded_human_review_label = urllib.parse.quote(human_review_label, safe="")
    encoded_branch = urllib.parse.quote(branch, safe="")

    label_checks = [
        ("auto-merge", label, encoded_label),
        ("human-review", human_review_label, encoded_human_review_label),
    ]
    for label_kind, raw_label, encoded in label_checks:
        try:
            github_request("GET", f"{api_url}/repos/{repo}/labels/{encoded}", token)
        except GitHubApiError as exc:
            if exc.status_code == 404:
                errors.append(f"{label_kind} label is missing: {raw_label}")
            elif exc.status_code == 0:
                errors.append(f"{label_kind} label check failed: {exc.response_body}")
            else:
                errors.append(f"{label_kind} label check failed with HTTP {exc.status_code}")

    branch_protection_missing = False
    should_check_branch_rulesets = True
    branch_protection_errors: list[str] = []
    try:
        protection = github_request("GET", f"{api_url}/repos/{repo}/branches/{encoded_branch}/protection", token)
    except GitHubApiError as exc:
        if exc.status_code == 404:
            branch_protection_missing = True
        elif exc.status_code == 403:
            branch_protection_errors.extend(
                _check_branch_endpoint_readiness(
                    api_url=api_url,
                    repo=repo,
                    encoded_branch=encoded_branch,
                    branch=branch,
                    token=token,
                    required_status_checks=required_status_checks,
                )
            )
        elif exc.status_code == 0:
            should_check_branch_rulesets = False
            branch_protection_errors.append(f"branch protection check failed: {exc.response_body}")
        else:
            branch_protection_errors.append(f"branch protection check failed with HTTP {exc.status_code}")
    else:
        if not isinstance(protection, dict):
            branch_protection_errors.append(f"branch protection response is invalid for {branch}")
        else:
            branch_protection_errors.extend(
                _validate_branch_protection(
                    protection,
                    branch=branch,
                    required_status_checks=required_status_checks,
                )
            )

    if branch_protection_errors or branch_protection_missing:
        if should_check_branch_rulesets:
            branch_ruleset_errors = _check_branch_ruleset_readiness(
                api_url=api_url,
                repo=repo,
                encoded_branch=encoded_branch,
                branch=branch,
                token=token,
                required_status_checks=required_status_checks,
            )
            if branch_ruleset_errors:
                errors.extend(branch_ruleset_errors if branch_protection_missing else branch_protection_errors)
        else:
            errors.extend(branch_protection_errors)

    return errors


def evaluate_readiness(
    *,
    auto_merge: bool,
    repo: str,
    branch: str,
    token: str,
    policy_path: Path = DEFAULT_POLICY_PATH,
    api_url: str = DEFAULT_API_URL,
    required_status_checks: tuple[str, ...] = DEFAULT_REQUIRED_STATUS_CHECKS,
) -> dict[str, Any]:
    errors: list[str] = []
    checks: list[str] = []
    if not auto_merge:
        return {
            "ready": True,
            "skipped": True,
            "label": None,
            "human_review_label": None,
            "checks": ["CODEX_AUDIT_AUTO_MERGE is false; readiness checks skipped."],
            "errors": [],
        }

    repo = validate_repo(repo)
    branch = validate_branch(branch)
    required_status_check_errors: list[str] = []
    try:
        required_status_checks = validate_required_status_checks(required_status_checks)
    except ReadinessError as exc:
        required_status_check_errors.append(str(exc))
        errors.extend(required_status_check_errors)
        required_status_checks = tuple()

    try:
        labels = load_policy_labels(policy_path)
    except ReadinessError as exc:
        label = None
        human_review_label = None
        errors.append(str(exc))
    else:
        label = labels["auto_merge_label"]
        human_review_label = labels["human_review_label"]
        checks.append(f"Loaded auto-merge policy label `{label}`.")
        checks.append(f"Loaded high-risk human-review label `{human_review_label}`.")


    if not token.strip():
        errors.append("GITHUB_TOKEN is required when CODEX_AUDIT_AUTO_MERGE=true")
    elif label and human_review_label and not required_status_check_errors:
        remote_errors = check_remote_readiness(
            api_url=api_url.rstrip("/"),
            repo=repo,
            branch=branch,
            token=token.strip(),
            label=label,
            human_review_label=human_review_label,
            required_status_checks=required_status_checks,
        )
        if remote_errors:
            errors.extend(remote_errors)
        else:
            checks.append(
                "Remote labels exist and branch protection or rulesets require status checks: "
                f"{', '.join(required_status_checks)}."
            )

    return {
        "ready": not errors,
        "skipped": False,
        "label": label,
        "human_review_label": human_review_label,
        "checks": checks,
        "errors": errors,
    }


def render_summary(decision: dict[str, Any]) -> str:
    lines = [
        "## Codex Auto-Merge Readiness",
        f"- Ready: `{'yes' if decision['ready'] else 'no'}`",
        f"- Skipped: `{'yes' if decision['skipped'] else 'no'}`",
    ]
    if decision.get("label"):
        lines.append(f"- Auto-merge label: `{decision['label']}`")
    if decision.get("human_review_label"):
        lines.append(f"- Human-review label: `{decision['human_review_label']}`")
    if decision.get("checks"):
        lines.extend(["", "### Checks"])
        lines.extend(f"- {item}" for item in decision["checks"])
    if decision.get("errors"):
        lines.extend(["", "### Errors"])
        lines.extend(f"- {item}" for item in decision["errors"])
    return "\n".join(lines).strip() + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check whether guarded Codex auto-merge is safe to request.")
    parser.add_argument("--repo", required=True)
    parser.add_argument("--branch", required=True)
    parser.add_argument("--auto-merge", default=os.environ.get("CODEX_AUDIT_AUTO_MERGE", "false"))
    parser.add_argument("--policy-file", type=Path, default=DEFAULT_POLICY_PATH)
    parser.add_argument("--api-url", default=DEFAULT_API_URL)
    parser.add_argument("--required-status-check", action="append")
    parser.add_argument(
        "--required-status-checks",
        default=os.environ.get("CODEX_AUDIT_REQUIRED_STATUS_CHECKS", ""),
        help="Comma- or newline-separated required status check contexts. Repeated --required-status-check is still supported.",
    )
    parser.add_argument("--summary-file", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    decision = evaluate_readiness(
        auto_merge=parse_bool(args.auto_merge),
        repo=args.repo,
        branch=args.branch,
        token=os.environ.get("GITHUB_TOKEN", ""),
        policy_path=args.policy_file,
        api_url=args.api_url,
        required_status_checks=parse_required_status_check_args(
            args.required_status_check,
            args.required_status_checks,
        ),
    )
    summary = render_summary(decision)
    if args.summary_file:
        args.summary_file.parent.mkdir(parents=True, exist_ok=True)
        args.summary_file.write_text(summary, encoding="utf-8")
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as handle:
            handle.write(summary)
    print(f"ready={'true' if decision['ready'] else 'false'}")
    print(f"skipped={'true' if decision['skipped'] else 'false'}")
    if decision["errors"]:
        print("Codex auto-merge readiness failed:", file=sys.stderr)
        for error in decision["errors"]:
            print(f"- {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
