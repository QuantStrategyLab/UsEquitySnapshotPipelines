from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, Callable
import urllib.error
import urllib.parse
import urllib.request


DEFAULT_API_URL = "https://api.github.com"
DEFAULT_TIMEOUT_SECONDS = 30
MARKER_PREFIX = "<!-- codex-auto-merge-guard:pr-"
MAX_LIST_ITEMS = 20
DEFAULT_AUTO_MERGE_LABEL = "auto-merge-ok"
DEFAULT_HUMAN_REVIEW_LABEL = "human-review-required"

RequestFn = Callable[[str, str, str, dict[str, Any] | None], Any]


class GuardDecisionCommentError(RuntimeError):
    pass


def github_request(
    method: str,
    url: str,
    token: str,
    payload: dict[str, Any] | None = None,
) -> Any:
    data = None
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "us-equity-codex-auto-merge-guard-decision",
    }
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=DEFAULT_TIMEOUT_SECONDS) as response:
        charset = response.headers.get_content_charset("utf-8")
        raw = response.read().decode(charset)
        return json.loads(raw) if raw else None


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def guard_marker(pr_number: int) -> str:
    return f"{MARKER_PREFIX}{pr_number} -->"


def extract_monthly_issue_number(pr: dict[str, Any], decision: dict[str, Any]) -> int | None:
    body = str(pr.get("body") or "")
    marker_prefix = str(decision.get("monthly_marker_prefix") or "<!-- codex-monthly-remediation:issue-")
    match = re.search(re.escape(marker_prefix) + r"(\d+)\s*-->", body)
    if not match:
        return None
    return int(match.group(1))


def _string_items(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if str(item).strip()]


def _changed_file_paths(pr: dict[str, Any]) -> list[str]:
    files = pr.get("files")
    if not isinstance(files, list):
        return []
    paths: list[str] = []
    for item in files:
        if isinstance(item, dict):
            path = str(item.get("path") or "").strip()
            if path:
                paths.append(path)
    return paths


def _decision_label(decision: dict[str, Any], field_name: str, fallback: str) -> str:
    value = decision.get(field_name)
    if isinstance(value, str) and value.strip() and "\n" not in value and "\r" not in value:
        return value.strip()
    return fallback


def _request_error_message(exc: urllib.error.HTTPError | urllib.error.URLError) -> str:
    if isinstance(exc, urllib.error.HTTPError):
        return f"HTTP {exc.code}"
    return str(exc.reason)


def _bullet_list(items: list[str], *, max_items: int = MAX_LIST_ITEMS, code: bool = False) -> str:
    if not items:
        return "- None"
    shown = items[:max_items]
    lines = [f"- `{item}`" if code else f"- {item}" for item in shown]
    if len(items) > max_items:
        lines.append(f"- _{len(items) - max_items} more item(s) omitted; see the workflow artifact for the full details._")
    return "\n".join(lines)


def build_guard_decision_comment(*, pr: dict[str, Any], decision: dict[str, Any]) -> str:
    pr_number = positive_int(str(pr.get("number") or 0))
    marker = guard_marker(pr_number)
    pr_url = str(pr.get("url") or "") or f"PR #{pr_number}"
    reason = str(decision.get("reason") or "unknown")
    risk_level = str(decision.get("risk_level") or "unknown")
    review_decision = str(decision.get("review_decision") or "n/a")
    changed_files = _changed_file_paths(pr)
    blocked_files = _string_items(decision.get("blocked_files"))
    medium_risk_files = _string_items(decision.get("medium_risk_files"))
    risk_reasons = _string_items(decision.get("risk_reasons"))
    next_action = (
        "Treat this as a human-review item before merge."
        if risk_level == "high" or reason in {"human_review_required", "policy_errors", "pr_metadata_mismatch", "blocked_files"}
        else "Review the PR labels and guard output before manually rerunning auto-merge."
    )
    return (
        f"{marker}\n"
        "## Codex Auto-Merge Guard Decision\n\n"
        "The source auto-merge workflow evaluated this Codex remediation PR and did **not** merge it. "
        "This comment is updated automatically so blocked unattended maintenance does not require opening workflow artifacts first.\n\n"
        f"- PR: {pr_url}\n"
        f"- Decision: `skip`\n"
        f"- Reason: `{reason}`\n"
        f"- Risk level: `{risk_level}`\n"
        f"- Review decision: `{review_decision}`\n"
        f"- Changed files: `{len(changed_files)}`\n"
        f"- Changed lines: `{decision.get('changed_lines') if decision.get('changed_lines') is not None else 'n/a'}`\n"
        f"- Next action: {next_action}\n\n"
        "### Risk reasons\n\n"
        f"{_bullet_list(risk_reasons)}\n\n"
        "### Blocked files\n\n"
        f"{_bullet_list(blocked_files, code=True)}\n\n"
        "### Medium-risk files\n\n"
        f"{_bullet_list(medium_risk_files, code=True)}\n"
    )


def append_label_actions_to_comment(body: str, label_actions: list[str]) -> str:
    if not label_actions:
        return body
    return f"{body.rstrip()}\n\n### Label hygiene\n\n{_bullet_list(label_actions)}\n"


def write_output_file(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def append_github_output(values: dict[str, str]) -> None:
    output = os.environ.get("GITHUB_OUTPUT")
    if not output:
        return
    with open(output, "a", encoding="utf-8") as handle:
        for key, value in values.items():
            print(f"{key}={value}", file=handle)


def find_existing_guard_comment(comments: list[Any], marker: str) -> int | None:
    for comment in comments:
        if not isinstance(comment, dict):
            continue
        body = comment.get("body")
        comment_id = comment.get("id")
        if isinstance(body, str) and body.startswith(marker) and isinstance(comment_id, int):
            return comment_id
    return None


def list_issue_comments(
    *,
    api_url: str,
    repo: str,
    token: str,
    issue_number: int,
    request_fn: RequestFn = github_request,
) -> list[Any]:
    comments: list[Any] = []
    page = 1
    while True:
        payload = request_fn(
            "GET",
            f"{api_url}/repos/{repo}/issues/{issue_number}/comments?per_page=100&page={page}",
            token,
            None,
        )
        if not isinstance(payload, list):
            raise GuardDecisionCommentError(f"issue comments response is invalid for issue #{issue_number}")
        comments.extend(payload)
        if len(payload) < 100:
            break
        page += 1
    return comments


def upsert_guard_decision_comment(
    *,
    api_url: str,
    repo: str,
    token: str,
    issue_number: int,
    body: str,
    marker: str,
    request_fn: RequestFn = github_request,
) -> tuple[str, str]:
    comments = list_issue_comments(
        api_url=api_url,
        repo=repo,
        token=token,
        issue_number=issue_number,
        request_fn=request_fn,
    )
    existing_id = find_existing_guard_comment(comments, marker)
    if existing_id is not None:
        updated = request_fn(
            "PATCH",
            f"{api_url}/repos/{repo}/issues/comments/{existing_id}",
            token,
            {"body": body},
        )
        return "updated", str(updated.get("html_url", "")) if isinstance(updated, dict) else ""
    created = request_fn(
        "POST",
        f"{api_url}/repos/{repo}/issues/{issue_number}/comments",
        token,
        {"body": body},
    )
    return "created", str(created.get("html_url", "")) if isinstance(created, dict) else ""


def sync_guard_decision_labels(
    *,
    api_url: str,
    repo: str,
    token: str,
    pr_number: int,
    decision: dict[str, Any],
    request_fn: RequestFn = github_request,
) -> list[str]:
    actions: list[str] = []
    auto_merge_label = _decision_label(decision, "auto_merge_label", DEFAULT_AUTO_MERGE_LABEL)
    human_review_label = _decision_label(decision, "human_review_label", DEFAULT_HUMAN_REVIEW_LABEL)
    if auto_merge_label == human_review_label:
        return [
            "Skipped guard decision label sync because auto-merge and human-review labels are identical.",
        ]
    encoded_auto_merge_label = urllib.parse.quote(auto_merge_label, safe="")

    try:
        request_fn(
            "DELETE",
            f"{api_url}/repos/{repo}/issues/{pr_number}/labels/{encoded_auto_merge_label}",
            token,
            None,
        )
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            actions.append(
                f"Could not remove stale auto-merge label `{auto_merge_label}` from PR #{pr_number}: "
                f"{_request_error_message(exc)}."
            )
        else:
            actions.append(f"Auto-merge label `{auto_merge_label}` was already absent.")
    except urllib.error.URLError as exc:
        actions.append(
            f"Could not remove stale auto-merge label `{auto_merge_label}` from PR #{pr_number}: "
            f"{_request_error_message(exc)}."
        )
    else:
        actions.append(f"Removed stale auto-merge label `{auto_merge_label}` from PR #{pr_number}.")

    if str(decision.get("risk_level") or "").strip().lower() == "high":
        try:
            request_fn(
                "POST",
                f"{api_url}/repos/{repo}/issues/{pr_number}/labels",
                token,
                {"labels": [human_review_label]},
            )
        except (urllib.error.HTTPError, urllib.error.URLError) as exc:
            actions.append(
                f"Could not ensure human-review label `{human_review_label}` on PR #{pr_number}: "
                f"{_request_error_message(exc)}."
            )
        else:
            actions.append(f"Ensured human-review label `{human_review_label}` on PR #{pr_number}.")
    return actions


def try_update_comment_with_label_actions(
    *,
    api_url: str,
    repo: str,
    token: str,
    issue_number: int,
    body: str,
    marker: str,
    request_fn: RequestFn = github_request,
) -> tuple[str, str, str]:
    try:
        action, comment_url = upsert_guard_decision_comment(
            api_url=api_url,
            repo=repo,
            token=token,
            issue_number=issue_number,
            body=body,
            marker=marker,
            request_fn=request_fn,
        )
    except (GuardDecisionCommentError, urllib.error.HTTPError, urllib.error.URLError) as exc:
        return "", "", _request_error_message(exc) if isinstance(exc, urllib.error.URLError) else str(exc)
    return action, comment_url, ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Comment the Codex auto-merge guard skip decision back to the monthly issue.")
    parser.add_argument("--repo", required=True)
    parser.add_argument("--pr-json", required=True, type=Path)
    parser.add_argument("--decision-json", required=True, type=Path)
    parser.add_argument("--output-file", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sync-labels", action="store_true")
    parser.add_argument("--api-url", default=DEFAULT_API_URL)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    pr = json.loads(args.pr_json.read_text(encoding="utf-8"))
    decision = json.loads(args.decision_json.read_text(encoding="utf-8"))
    body = build_guard_decision_comment(pr=pr, decision=decision)
    marker = guard_marker(positive_int(str(pr.get("number") or 0)))
    outputs: dict[str, str] = {}
    if args.output_file:
        write_output_file(args.output_file, body)
        outputs["guard_decision_comment_file"] = str(args.output_file)
        print(f"guard_decision_comment_file={args.output_file}")

    issue_number = extract_monthly_issue_number(pr, decision)
    if issue_number is None:
        outputs.update({"guard_decision_comment_action": "skipped_missing_issue_marker", "guard_decision_comment_url": ""})
        append_github_output(outputs)
        print("guard_decision_comment_action=skipped_missing_issue_marker")
        print("guard_decision_comment_url=")
        return 0

    if args.dry_run:
        outputs.update({"guard_decision_comment_action": "dry_run", "guard_decision_comment_url": ""})
        append_github_output(outputs)
        print("guard_decision_comment_action=dry_run")
        print("guard_decision_comment_url=")
        return 0

    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if not token:
        if outputs:
            append_github_output(outputs)
        print("GITHUB_TOKEN is required", file=sys.stderr)
        return 1
    try:
        action, comment_url = upsert_guard_decision_comment(
            api_url=args.api_url.rstrip("/"),
            repo=args.repo,
            token=token,
            issue_number=issue_number,
            body=body,
            marker=marker,
            request_fn=github_request,
        )
        label_actions: list[str] = []
        if args.sync_labels:
            label_actions = sync_guard_decision_labels(
                api_url=args.api_url.rstrip("/"),
                repo=args.repo,
                token=token,
                pr_number=positive_int(str(pr.get("number") or 0)),
                decision=decision,
                request_fn=github_request,
            )
            if label_actions:
                body = append_label_actions_to_comment(body, label_actions)
                if args.output_file:
                    write_output_file(args.output_file, body)
                label_comment_action, label_comment_url, label_comment_error = try_update_comment_with_label_actions(
                    api_url=args.api_url.rstrip("/"),
                    repo=args.repo,
                    token=token,
                    issue_number=issue_number,
                    body=body,
                    marker=marker,
                    request_fn=github_request,
                )
                if label_comment_action:
                    action = label_comment_action
                    comment_url = label_comment_url or comment_url
                    label_actions.append("Recorded label hygiene actions in the guard decision comment.")
                else:
                    label_actions.append(
                        f"Could not update guard decision comment with label hygiene actions: {label_comment_error}."
                    )
                    body = append_label_actions_to_comment(
                        build_guard_decision_comment(pr=pr, decision=decision),
                        label_actions,
                    )
                    if args.output_file:
                        write_output_file(args.output_file, body)
    except GuardDecisionCommentError as exc:
        if outputs:
            append_github_output(outputs)
        print(f"Guard decision comment failed: {exc}", file=sys.stderr)
        return 1
    except urllib.error.HTTPError as exc:
        if outputs:
            append_github_output(outputs)
        detail = exc.read().decode("utf-8", errors="replace")
        print(f"GitHub API request failed: {exc.code} {detail}", file=sys.stderr)
        return 1
    except urllib.error.URLError as exc:
        if outputs:
            append_github_output(outputs)
        print(f"GitHub API request failed: {exc.reason}", file=sys.stderr)
        return 1

    outputs.update({"guard_decision_comment_action": action, "guard_decision_comment_url": comment_url})
    if args.sync_labels:
        outputs["guard_decision_label_actions"] = "; ".join(label_actions)
    append_github_output(outputs)
    print(f"guard_decision_comment_action={action}")
    print(f"guard_decision_comment_url={comment_url}")
    if args.sync_labels:
        print(f"guard_decision_label_actions={'; '.join(label_actions)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
