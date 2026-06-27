from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
from typing import Any


AUTO_MERGE_LABEL = "auto-merge-ok"
HUMAN_REVIEW_LABEL = "human-review-required"
MARKER_PREFIX = "<!-- codex-monthly-remediation:issue-"
POLICY_PATH = Path(".github/codex_auto_merge_policy.json")
DEFAULT_MAX_CHANGED_FILES = 20
DEFAULT_MAX_CHANGED_LINES = 1200
CONTROL_PLANE_EXACT_PATHS = (
    ".github/codex_auto_merge_policy.json",
    "scripts/check_codex_auto_merge_readiness.py",
    "scripts/evaluate_codex_pr_merge.py",
    "scripts/post_codex_auto_merge_decision_comment.py",
    "scripts/sync_codex_auto_merge_labels.py",
)
CONTROL_PLANE_PREFIXES = (".github/workflows/",)
DEFAULT_POLICY = {
    "version": 1,
    "auto_merge_label": AUTO_MERGE_LABEL,
    "human_review_label": HUMAN_REVIEW_LABEL,
    "monthly_marker_prefix": MARKER_PREFIX,
    "max_changed_files": DEFAULT_MAX_CHANGED_FILES,
    "max_changed_lines": DEFAULT_MAX_CHANGED_LINES,
    "blocked_path_patterns": [
        r"(^|/)(\.env|.*secret.*|.*credential.*|.*token.*|.*private.*|.*\.pem|.*\.key)$",
    ],
    "risk_policy": {
        "low": {
            "prefixes": ["docs/", "tests/"],
            "exact": ["README.md", "README.zh-CN.md"],
            "reason": "docs/tests/readme-only monthly-review surface",
        },
        "medium": {
            "exact": [
                "scripts/build_monthly_live_strategy_health_reports.py",
                "scripts/build_monthly_russell_crash_brake_research.py",
                "scripts/build_monthly_russell_crash_brake_review_chain.py",
                "scripts/build_monthly_global_etf_promotion_bundles.py",
                "scripts/build_monthly_live_replacement_reviews.py",
                "scripts/build_monthly_plugin_promotion_reviews.py",
                "scripts/build_promotion_readiness_report.py",
                "scripts/run_monthly_report_bundle.py",
                "scripts/post_monthly_ai_review_issue.py",
                "scripts/post_codex_auto_merge_preflight_comment.py",
                "scripts/plan_codex_auto_merge_enablement.py",
            ],
            "reason": "monthly-review evidence/reporting helper changed",
        },
        "high": {"reason": "blocked/high-risk files require human review"},
    }
}


def _fail_closed_policy(reason: str) -> dict[str, Any]:
    return {
        "policy_errors": [reason],
        "blocked_path_patterns": [r".*"],
        "risk_policy": {
            "low": {"prefixes": [], "exact": [], "reason": reason},
            "medium": {"exact": [], "reason": reason},
            "high": {"reason": reason},
        },
    }


def _valid_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip()) and "\n" not in value and "\r" not in value


def _valid_string_list(value: Any, *, allow_empty: bool = True) -> bool:
    return isinstance(value, list) and (allow_empty or bool(value)) and all(isinstance(item, str) for item in value)


def _policy_schema_error(payload: dict[str, Any]) -> str | None:
    if "version" not in payload:
        return "invalid auto-merge policy schema requires human review"
    if payload.get("version") != 1:
        return "unsupported auto-merge policy version requires human review"
    if not _valid_string(payload.get("auto_merge_label")):
        return "invalid auto-merge policy schema requires human review"
    if not _valid_string(payload.get("human_review_label")):
        return "invalid auto-merge policy schema requires human review"
    if payload["auto_merge_label"].strip() == payload["human_review_label"].strip():
        return "auto-merge and human-review labels must be distinct requires human review"
    if not _valid_string(payload.get("monthly_marker_prefix")):
        return "invalid auto-merge policy schema requires human review"
    if type(payload.get("max_changed_files")) is not int or payload["max_changed_files"] < 1:
        return "invalid auto-merge policy schema requires human review"
    if type(payload.get("max_changed_lines")) is not int or payload["max_changed_lines"] < 1:
        return "invalid auto-merge policy schema requires human review"
    if not _valid_string_list(payload.get("blocked_path_patterns"), allow_empty=False):
        return "invalid auto-merge policy schema requires human review"
    risk_policy = payload.get("risk_policy")
    if not isinstance(risk_policy, dict):
        return "invalid auto-merge policy schema requires human review"
    low_policy = risk_policy.get("low")
    medium_policy = risk_policy.get("medium")
    high_policy = risk_policy.get("high")
    if not isinstance(low_policy, dict) or not isinstance(medium_policy, dict) or not isinstance(high_policy, dict):
        return "invalid auto-merge policy schema requires human review"
    if not _valid_string_list(low_policy.get("prefixes")):
        return "invalid auto-merge policy schema requires human review"
    if not _valid_string_list(low_policy.get("exact")):
        return "invalid auto-merge policy schema requires human review"
    if not _valid_string_list(medium_policy.get("exact")):
        return "invalid auto-merge policy schema requires human review"
    if not _valid_string(high_policy.get("reason")):
        return "invalid auto-merge policy schema requires human review"
    control_plane_matches = _control_plane_auto_merge_matches(payload)
    if control_plane_matches:
        matches = ", ".join(control_plane_matches)
        return f"auto-merge policy must keep control-plane paths high-risk: {matches}"
    return None


def _normalize_path(path: str) -> str:
    normalized = path.strip()
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


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


def _control_plane_auto_merge_matches(payload: dict[str, Any]) -> list[str]:
    risk_policy = payload["risk_policy"]
    low_policy = risk_policy["low"]
    medium_policy = risk_policy["medium"]
    matches: list[str] = []
    for raw_path in [*low_policy.get("exact", []), *medium_policy.get("exact", [])]:
        match = _control_plane_exact_match(_normalize_path(raw_path))
        if match:
            matches.append(match)
    for raw_prefix in low_policy.get("prefixes", []):
        matches.extend(_control_plane_prefix_matches(_normalize_path(raw_prefix)))
    return sorted(set(matches))


def load_policy(policy_path: Path | None = None) -> dict[str, Any]:
    path = policy_path or POLICY_PATH
    if not path.exists():
        return DEFAULT_POLICY
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _fail_closed_policy("invalid auto-merge policy requires human review")
    if not isinstance(payload, dict):
        return _fail_closed_policy("invalid auto-merge policy requires human review")
    schema_error = _policy_schema_error(payload)
    if schema_error:
        return _fail_closed_policy(schema_error)
    return payload


def _policy_section(policy: dict[str, Any], name: str) -> dict[str, Any]:
    risk_policy = policy.get("risk_policy")
    if not isinstance(risk_policy, dict):
        risk_policy = DEFAULT_POLICY["risk_policy"]
    section = risk_policy.get(name)
    fallback = DEFAULT_POLICY["risk_policy"][name]
    return section if isinstance(section, dict) else fallback


def _blocked_patterns(policy: dict[str, Any]) -> tuple[list[re.Pattern[str]], list[str]]:
    errors = [str(item) for item in policy.get("policy_errors", []) if str(item).strip()]
    raw_patterns = policy.get("blocked_path_patterns")
    if raw_patterns is None:
        raw_patterns = DEFAULT_POLICY["blocked_path_patterns"]
    elif not isinstance(raw_patterns, list):
        errors.append("invalid blocked_path_patterns list requires human review")
        return [re.compile(r".*")], errors
    patterns: list[re.Pattern[str]] = []
    for raw_pattern in raw_patterns:
        if not isinstance(raw_pattern, str):
            errors.append("invalid blocked_path_patterns list requires human review")
            return [re.compile(r".*")], errors
        pattern = str(raw_pattern)
        if not pattern.strip():
            continue
        try:
            patterns.append(re.compile(pattern, re.IGNORECASE))
        except re.error:
            errors.append("invalid blocked_path_patterns regex requires human review")
            return [re.compile(r".*")], errors
    return patterns, errors


def _string_list(value: Any, field_name: str, errors: list[str]) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        errors.append(f"invalid {field_name} list requires human review")
        return []
    items: list[str] = []
    for item in value:
        if not isinstance(item, str):
            errors.append(f"invalid {field_name} list requires human review")
            return []
        if item.strip():
            items.append(item)
    return items


def _policy_string(policy: dict[str, Any], field_name: str, fallback: str, errors: list[str]) -> str:
    value = policy.get(field_name, fallback)
    if not isinstance(value, str) or not value.strip() or "\n" in value or "\r" in value:
        errors.append(f"invalid {field_name} string requires human review")
        return fallback
    return value.strip()


def _policy_positive_int(policy: dict[str, Any], field_name: str, fallback: int, errors: list[str]) -> int:
    value = policy.get(field_name, fallback)
    if type(value) is not int or value < 1:
        errors.append(f"invalid {field_name} integer requires human review")
        return fallback
    return value


def _optional_non_negative_int(value: Any, field_name: str, errors: list[str]) -> int | None:
    if value is None:
        return None
    if type(value) is not int or value < 0:
        errors.append(f"invalid {field_name} count requires human review")
        return None
    return value


def evaluate_changed_files(
    changed_files: list[str],
    policy: dict[str, Any] | None = None,
    *,
    additions: int | None = None,
    deletions: int | None = None,
) -> dict[str, Any]:
    policy = policy or load_policy()
    low_policy = _policy_section(policy, "low")
    medium_policy = _policy_section(policy, "medium")
    high_policy = _policy_section(policy, "high")
    blocked_patterns, policy_errors = _blocked_patterns(policy)
    max_changed_files = _policy_positive_int(policy, "max_changed_files", DEFAULT_MAX_CHANGED_FILES, policy_errors)
    max_changed_lines = _policy_positive_int(policy, "max_changed_lines", DEFAULT_MAX_CHANGED_LINES, policy_errors)
    normalized_additions = _optional_non_negative_int(additions, "additions", policy_errors)
    normalized_deletions = _optional_non_negative_int(deletions, "deletions", policy_errors)
    low_exact = set(_string_list(low_policy.get("exact"), "risk_policy.low.exact", policy_errors))
    low_prefixes = tuple(_string_list(low_policy.get("prefixes"), "risk_policy.low.prefixes", policy_errors))
    medium_exact = set(_string_list(medium_policy.get("exact"), "risk_policy.medium.exact", policy_errors))
    blocked: list[str] = []
    medium_risk: list[str] = []
    low_risk_count = 0
    normalized_paths = [_normalize_path(path) for path in changed_files if _normalize_path(path)]
    if len(normalized_paths) > max_changed_files:
        policy_errors.append(
            f"changed file count exceeds auto-merge limit requires human review: {len(normalized_paths)} > {max_changed_files}"
        )
    changed_lines: int | None = None
    if normalized_additions is not None and normalized_deletions is not None:
        changed_lines = normalized_additions + normalized_deletions
        if changed_lines > max_changed_lines:
            policy_errors.append(
                f"changed line count exceeds auto-merge limit requires human review: {changed_lines} > {max_changed_lines}"
            )
    for raw_path in changed_files:
        path = _normalize_path(raw_path)
        if not path:
            continue
        if policy_errors:
            blocked.append(path)
            continue
        if any(pattern.search(path) for pattern in blocked_patterns):
            blocked.append(path)
            continue
        if path in low_exact or any(path.startswith(prefix) for prefix in low_prefixes):
            low_risk_count += 1
            continue
        if path in medium_exact:
            medium_risk.append(path)
            continue
        blocked.append(path)
    if blocked:
        risk_level = "high"
        risk_reasons = policy_errors or [str(high_policy.get("reason") or DEFAULT_POLICY["risk_policy"]["high"]["reason"])]
    elif medium_risk:
        risk_level = "medium"
        risk_reasons = [str(medium_policy.get("reason") or DEFAULT_POLICY["risk_policy"]["medium"]["reason"])]
    else:
        risk_level = "low"
        risk_reasons = [str(low_policy.get("reason") or DEFAULT_POLICY["risk_policy"]["low"]["reason"])]
    return {
        "allowed": not blocked,
        "blocked_files": blocked,
        "risk_level": risk_level,
        "risk_reasons": risk_reasons,
        "policy_errors": policy_errors,
        "low_risk_file_count": low_risk_count,
        "medium_risk_files": medium_risk,
        "additions": normalized_additions,
        "deletions": normalized_deletions,
        "changed_lines": changed_lines,
    }


def _label_names(pr: dict[str, Any]) -> set[str]:
    labels = pr.get("labels", []) or []
    if not isinstance(labels, list):
        return set()
    names: set[str] = set()
    for label in labels:
        raw_name = label.get("name", "") if isinstance(label, dict) else label
        name = str(raw_name or "").strip()
        if name:
            names.add(name)
    return names


def _owner_login(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("login", "")).strip()
    return str(value or "").strip()


def _repository_name_with_owner(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("nameWithOwner", "") or value.get("name", "")).strip()
    return str(value or "").strip()


def _cross_repository_state(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _reported_changed_file_count(value: Any) -> int | None:
    return value if type(value) is int and value >= 0 else None


def _reported_changed_line_count(value: Any) -> int | None:
    return value if type(value) is int and value >= 0 else None


def _review_decision(value: Any, errors: list[str]) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        errors.append("invalid review decision requires human review")
        return ""
    decision = value.strip().upper()
    if decision and decision not in {"APPROVED", "CHANGES_REQUESTED", "REVIEW_REQUIRED"}:
        errors.append("unknown review decision requires human review")
    return decision


def _changed_file_paths(pr: dict[str, Any]) -> list[str]:
    files = pr.get("files", []) or []
    if not isinstance(files, list):
        return []
    return [str(item.get("path", "")) for item in files if isinstance(item, dict)]


def _changed_file_status_errors(files: Any) -> list[str]:
    if not isinstance(files, list):
        return []
    errors: list[str] = []
    blocked_statuses = {"removed", "renamed", "copied"}
    allowed_statuses = {"added", "modified", "changed"}
    for item in files:
        if not isinstance(item, dict) or "status" not in item:
            continue
        status = str(item.get("status") or "").strip().lower()
        if not status:
            errors.append("empty file status requires human review")
        elif status in blocked_statuses:
            errors.append(f"file status `{status}` requires human review")
        elif status not in allowed_statuses:
            errors.append(f"unknown file status `{status}` requires human review")
    return sorted(set(errors))


def _missing_changed_file_status_metadata(files: Any) -> bool:
    return (
        not isinstance(files, list)
        or not files
        or any(not isinstance(item, dict) or "status" not in item for item in files)
    )


def _monthly_issue_number_from_head_ref(head_ref: str) -> str | None:
    match = re.fullmatch(r"codex/monthly-review-issue-(\d+)(?:-.+)?", str(head_ref or "").strip())
    return match.group(1) if match else None


def _metadata_errors(
    pr: dict[str, Any],
    *,
    expected_base_ref: str = "",
    expected_head_ref: str = "",
    expected_head_owner: str = "",
    expected_head_repository: str = "",
    require_same_repository: bool = False,
) -> list[str]:
    errors: list[str] = []
    base_ref = str(pr.get("baseRefName", "") or "").strip()
    head_ref = str(pr.get("headRefName", "") or "").strip()
    head_owner = _owner_login(pr.get("headRepositoryOwner"))
    head_repository = _repository_name_with_owner(pr.get("headRepository"))
    is_cross_repository = _cross_repository_state(pr.get("isCrossRepository"))
    files = pr.get("files", []) or []
    changed_files = _changed_file_paths(pr)
    reported_changed_file_count = _reported_changed_file_count(pr.get("changedFiles"))
    reported_additions = _reported_changed_line_count(pr.get("additions"))
    reported_deletions = _reported_changed_line_count(pr.get("deletions"))
    if expected_base_ref and base_ref != expected_base_ref:
        errors.append("unexpected PR base ref requires human review")
    if expected_head_ref and head_ref != expected_head_ref:
        errors.append("unexpected PR head ref requires human review")
    if expected_head_ref.startswith("codex/monthly-review-issue-") and not _monthly_issue_number_from_head_ref(
        expected_head_ref
    ):
        errors.append("monthly PR head ref issue number missing requires human review")
    if expected_head_owner and head_owner != expected_head_owner:
        errors.append("unexpected PR head owner requires human review")
    if expected_head_repository and head_repository != expected_head_repository:
        errors.append("unexpected PR head repository requires human review")
    if require_same_repository and is_cross_repository is not False:
        errors.append("cross-repository PR requires human review")
    if not isinstance(files, list):
        errors.append("invalid changed file list requires human review")
    elif any(not isinstance(item, dict) for item in files):
        errors.append("invalid changed file list requires human review")
    else:
        errors.extend(_changed_file_status_errors(files))
    if any(not path.strip() for path in changed_files):
        errors.append("invalid changed file path requires human review")
    if pr.get("changedFiles") is not None and reported_changed_file_count is None:
        errors.append("invalid changed file count requires human review")
    elif reported_changed_file_count is not None and reported_changed_file_count != len(changed_files):
        errors.append("changed file list mismatch requires human review")
    if pr.get("additions") is not None and reported_additions is None:
        errors.append("invalid additions count requires human review")
    if pr.get("deletions") is not None and reported_deletions is None:
        errors.append("invalid deletions count requires human review")
    return errors


def evaluate_pr(
    pr: dict[str, Any],
    policy: dict[str, Any] | None = None,
    *,
    expected_base_ref: str = "",
    expected_head_ref: str = "",
    expected_head_owner: str = "",
    expected_head_repository: str = "",
    require_same_repository: bool = False,
) -> dict[str, Any]:
    policy = policy or load_policy()
    body = pr.get("body") or ""
    changed_files = _changed_file_paths(pr)
    file_guard = evaluate_changed_files(
        changed_files,
        policy=policy,
        additions=_reported_changed_line_count(pr.get("additions")),
        deletions=_reported_changed_line_count(pr.get("deletions")),
    )
    policy_errors = list(file_guard["policy_errors"])
    metadata_errors = _metadata_errors(
        pr,
        expected_base_ref=expected_base_ref,
        expected_head_ref=expected_head_ref,
        expected_head_owner=expected_head_owner,
        expected_head_repository=expected_head_repository,
        require_same_repository=require_same_repository,
    )
    auto_merge_label = _policy_string(policy, "auto_merge_label", AUTO_MERGE_LABEL, policy_errors)
    human_review_label = _policy_string(policy, "human_review_label", HUMAN_REVIEW_LABEL, policy_errors)
    marker_prefix = _policy_string(policy, "monthly_marker_prefix", MARKER_PREFIX, policy_errors)
    review_decision = _review_decision(pr.get("reviewDecision"), metadata_errors) if "reviewDecision" in pr else ""
    labels = _label_names(pr)
    expected_issue_number = _monthly_issue_number_from_head_ref(expected_head_ref)
    expected_marker = f"{marker_prefix}{expected_issue_number} -->" if expected_issue_number else ""
    has_marker = expected_marker in body if expected_marker else marker_prefix in body
    has_merge_label = auto_merge_label in labels
    has_human_review_label = human_review_label in labels
    is_draft = bool(pr.get("isDraft"))
    if (
        has_merge_label
        and not has_human_review_label
        and has_marker
        and not is_draft
        and file_guard["allowed"]
        and not policy_errors
        and not metadata_errors
        and (pr.get("additions") is None or pr.get("deletions") is None)
    ):
        policy_errors.append("missing changed line counts require human review")
    if (
        has_merge_label
        and not has_human_review_label
        and has_marker
        and not is_draft
        and file_guard["allowed"]
        and not policy_errors
        and not metadata_errors
    ):
        if "reviewDecision" not in pr:
            metadata_errors.append("missing review decision requires human review")
        elif review_decision in {"CHANGES_REQUESTED", "REVIEW_REQUIRED"}:
            metadata_errors.append(f"review decision `{review_decision}` requires human review")
    if (
        has_merge_label
        and not has_human_review_label
        and has_marker
        and not is_draft
        and file_guard["allowed"]
        and not policy_errors
        and not metadata_errors
        and _missing_changed_file_status_metadata(pr.get("files", []) or [])
    ):
        metadata_errors.append("missing changed file status metadata requires human review")
    should_merge = (
        has_marker
        and has_merge_label
        and not has_human_review_label
        and not is_draft
        and file_guard["allowed"]
        and not policy_errors
        and not metadata_errors
    )
    if policy_errors:
        reason = "policy_errors"
    elif metadata_errors:
        reason = "pr_metadata_mismatch"
    elif not has_marker:
        reason = "missing_marker"
    elif has_human_review_label:
        reason = "human_review_required"
    elif not has_merge_label:
        reason = "missing_auto_merge_label"
    elif is_draft:
        reason = "draft_pr"
    elif not file_guard["allowed"]:
        reason = "blocked_files"
    else:
        reason = "ready"
    risk_level = "high" if policy_errors or metadata_errors or has_human_review_label else file_guard["risk_level"]
    risk_reasons = (
        policy_errors
        or metadata_errors
        or ([f"human-review label `{human_review_label}` requires manual review"] if has_human_review_label else [])
        or file_guard["risk_reasons"]
    )
    return {
        "should_merge": should_merge,
        "reason": reason,
        "blocked_files": file_guard["blocked_files"],
        "risk_level": risk_level,
        "risk_reasons": risk_reasons,
        "policy_errors": policy_errors,
        "metadata_errors": metadata_errors,
        "base_ref": str(pr.get("baseRefName", "") or "").strip(),
        "head_ref": str(pr.get("headRefName", "") or "").strip(),
        "head_owner": _owner_login(pr.get("headRepositoryOwner")),
        "head_repository": _repository_name_with_owner(pr.get("headRepository")),
        "is_cross_repository": _cross_repository_state(pr.get("isCrossRepository")),
        "reported_changed_file_count": _reported_changed_file_count(pr.get("changedFiles")),
        "reported_additions": _reported_changed_line_count(pr.get("additions")),
        "reported_deletions": _reported_changed_line_count(pr.get("deletions")),
        "review_decision": review_decision if "reviewDecision" in pr else "n/a",
        "auto_merge_label": auto_merge_label,
        "human_review_label": human_review_label,
        "has_human_review_label": has_human_review_label,
        "monthly_marker_prefix": marker_prefix,
        "medium_risk_files": file_guard["medium_risk_files"],
        "changed_file_count": len([path for path in changed_files if path.strip()]),
        "changed_lines": file_guard["changed_lines"],
    }


def render_summary(pr: dict[str, Any], decision: dict[str, Any]) -> str:
    lines = [
        "## Codex Auto-Merge Gate",
        f"- PR: {pr.get('url', 'n/a')}",
        f"- Draft: `{'yes' if pr.get('isDraft') else 'no'}`",
        f"- Base ref: `{decision.get('base_ref') or 'n/a'}`",
        f"- Head ref: `{decision.get('head_ref') or 'n/a'}`",
        f"- Head owner: `{decision.get('head_owner') or 'n/a'}`",
        f"- Head repository: `{decision.get('head_repository') or 'n/a'}`",
        f"- Cross repository: `{_format_cross_repository(decision.get('is_cross_repository'))}`",
        f"- Changed files: `{decision['changed_file_count']}`",
        f"- Reported changed files: `{decision.get('reported_changed_file_count') if decision.get('reported_changed_file_count') is not None else 'n/a'}`",
        f"- Additions: `{decision.get('reported_additions') if decision.get('reported_additions') is not None else 'n/a'}`",
        f"- Deletions: `{decision.get('reported_deletions') if decision.get('reported_deletions') is not None else 'n/a'}`",
        f"- Changed lines: `{decision.get('changed_lines') if decision.get('changed_lines') is not None else 'n/a'}`",
        f"- Review decision: `{decision.get('review_decision') or 'none'}`",
        f"- Risk level: `{decision['risk_level']}`",
        f"- Blocked files: `{len(decision['blocked_files'])}`",
        f"- Human-review label present: `{'yes' if decision.get('has_human_review_label') else 'no'}`",
        f"- Decision: `{'merge' if decision['should_merge'] else 'skip'}`",
        f"- Reason: `{decision['reason']}`",
    ]
    if decision.get("risk_reasons"):
        lines.extend(["", "### Risk reasons"])
        lines.extend(f"- {reason}" for reason in decision["risk_reasons"])
    if decision.get("medium_risk_files"):
        lines.extend(["", "### Medium-risk files"])
        lines.extend(f"- `{path}`" for path in decision["medium_risk_files"])
    if decision["blocked_files"]:
        lines.extend(["", "### Blocked files"])
        lines.extend(f"- `{path}`" for path in decision["blocked_files"])
    return "\n".join(lines).strip() + "\n"


def _format_cross_repository(value: Any) -> str:
    if value is True:
        return "yes"
    if value is False:
        return "no"
    return "unknown"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate whether a Codex remediation PR may be auto-merged.")
    parser.add_argument("--pr-json", required=True, type=Path)
    parser.add_argument("--summary-file", required=True, type=Path)
    parser.add_argument("--decision-file", required=True, type=Path)
    parser.add_argument("--expected-base-ref", default="")
    parser.add_argument("--expected-head-ref", default="")
    parser.add_argument("--expected-head-owner", default="")
    parser.add_argument("--expected-head-repository", default="")
    parser.add_argument("--require-same-repository", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    pr = json.loads(args.pr_json.read_text(encoding="utf-8"))
    decision = evaluate_pr(
        pr,
        expected_base_ref=args.expected_base_ref,
        expected_head_ref=args.expected_head_ref,
        expected_head_owner=args.expected_head_owner,
        expected_head_repository=args.expected_head_repository,
        require_same_repository=args.require_same_repository,
    )
    args.summary_file.parent.mkdir(parents=True, exist_ok=True)
    args.summary_file.write_text(render_summary(pr, decision), encoding="utf-8")
    args.decision_file.write_text(json.dumps(decision, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"should_merge={'true' if decision['should_merge'] else 'false'}")
    print(f"reason={decision['reason']}")
    print(f"risk_level={decision['risk_level']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
