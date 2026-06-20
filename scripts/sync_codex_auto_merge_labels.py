from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
from typing import Any
import urllib.parse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.check_codex_auto_merge_readiness import (  # noqa: E402
    DEFAULT_API_URL,
    DEFAULT_POLICY_PATH,
    GitHubApiError,
    ReadinessError,
    github_request,
    load_policy_labels,
    parse_bool,
    validate_repo,
)

DEFAULT_AUTO_MERGE_COLOR = "0e8a16"
DEFAULT_HUMAN_REVIEW_COLOR = "d93f0b"
DEFAULT_AUTO_MERGE_DESCRIPTION = "Guarded Codex remediation auto-merge may proceed after source CI and merge guard pass."
DEFAULT_HUMAN_REVIEW_DESCRIPTION = "Codex remediation PR requires human review before merge."


class LabelSyncError(RuntimeError):
    pass


def _single_line(value: str) -> str:
    normalized = value.strip()
    if not normalized or "\n" in normalized or "\r" in normalized:
        raise LabelSyncError("label names must be non-empty single-line values")
    return normalized


def ensure_label(
    *,
    api_url: str,
    repo: str,
    token: str,
    name: str,
    color: str,
    description: str,
) -> dict[str, str]:
    label_name = _single_line(name)
    encoded_label = urllib.parse.quote(label_name, safe="")
    labels_url = f"{api_url.rstrip('/')}/repos/{repo}/labels"
    label_url = f"{labels_url}/{encoded_label}"
    try:
        github_request("GET", label_url, token)
    except GitHubApiError as exc:
        if exc.status_code != 404:
            raise
        try:
            github_request(
                "POST",
                labels_url,
                token,
                {
                    "name": label_name,
                    "color": color,
                    "description": description,
                },
            )
        except GitHubApiError as create_exc:
            if create_exc.status_code != 422:
                raise
            # Another workflow run may have created the label after our GET.
            github_request("GET", label_url, token)
            return {"label": label_name, "status": "exists_after_race"}
        return {"label": label_name, "status": "created"}
    return {"label": label_name, "status": "exists"}


def sync_codex_auto_merge_labels(
    *,
    auto_merge: bool,
    repo: str,
    token: str,
    policy_path: Path = DEFAULT_POLICY_PATH,
    api_url: str = DEFAULT_API_URL,
) -> dict[str, Any]:
    if not auto_merge:
        return {
            "ready": True,
            "skipped": True,
            "actions": ["CODEX_AUDIT_AUTO_MERGE is false; label sync skipped."],
            "errors": [],
            "auto_merge_label": None,
            "human_review_label": None,
        }

    actions: list[str] = []
    errors: list[str] = []
    try:
        validated_repo = validate_repo(repo)
        labels = load_policy_labels(policy_path)
    except ReadinessError as exc:
        return {
            "ready": False,
            "skipped": False,
            "actions": actions,
            "errors": [str(exc)],
            "auto_merge_label": None,
            "human_review_label": None,
        }

    auto_merge_label = labels["auto_merge_label"]
    human_review_label = labels["human_review_label"]
    if not token.strip():
        return {
            "ready": False,
            "skipped": False,
            "actions": actions,
            "errors": ["GITHUB_TOKEN is required to sync guarded auto-merge labels"],
            "auto_merge_label": auto_merge_label,
            "human_review_label": human_review_label,
        }

    label_specs = [
        (auto_merge_label, DEFAULT_AUTO_MERGE_COLOR, DEFAULT_AUTO_MERGE_DESCRIPTION),
        (human_review_label, DEFAULT_HUMAN_REVIEW_COLOR, DEFAULT_HUMAN_REVIEW_DESCRIPTION),
    ]
    for label, color, description in label_specs:
        try:
            result = ensure_label(
                api_url=api_url,
                repo=validated_repo,
                token=token.strip(),
                name=label,
                color=color,
                description=description,
            )
        except (GitHubApiError, LabelSyncError) as exc:
            errors.append(f"label `{label}` sync failed: {exc}")
        else:
            if result["status"] == "created":
                actions.append(f"Created label `{label}`.")
            elif result["status"] == "exists_after_race":
                actions.append(f"Label `{label}` already existed after a concurrent create.")
            else:
                actions.append(f"Label `{label}` already exists.")

    return {
        "ready": not errors,
        "skipped": False,
        "actions": actions,
        "errors": errors,
        "auto_merge_label": auto_merge_label,
        "human_review_label": human_review_label,
    }


def render_summary(decision: dict[str, Any]) -> str:
    lines = [
        "## Codex Auto-Merge Label Sync",
        f"- Ready: `{'yes' if decision['ready'] else 'no'}`",
        f"- Skipped: `{'yes' if decision['skipped'] else 'no'}`",
    ]
    if decision.get("auto_merge_label"):
        lines.append(f"- Auto-merge label: `{decision['auto_merge_label']}`")
    if decision.get("human_review_label"):
        lines.append(f"- Human-review label: `{decision['human_review_label']}`")
    if decision.get("actions"):
        lines.extend(["", "### Actions"])
        lines.extend(f"- {item}" for item in decision["actions"])
    if decision.get("errors"):
        lines.extend(["", "### Errors"])
        lines.extend(f"- {item}" for item in decision["errors"])
    return "\n".join(lines).strip() + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ensure guarded Codex auto-merge labels exist in the source repo.")
    parser.add_argument("--repo", required=True)
    parser.add_argument("--auto-merge", default=os.environ.get("CODEX_AUDIT_AUTO_MERGE", "false"))
    parser.add_argument("--policy-file", type=Path, default=DEFAULT_POLICY_PATH)
    parser.add_argument("--api-url", default=DEFAULT_API_URL)
    parser.add_argument("--summary-file", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    decision = sync_codex_auto_merge_labels(
        auto_merge=parse_bool(args.auto_merge),
        repo=args.repo,
        token=os.environ.get("GITHUB_TOKEN", ""),
        policy_path=args.policy_file,
        api_url=args.api_url,
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
    return 0 if decision["ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
