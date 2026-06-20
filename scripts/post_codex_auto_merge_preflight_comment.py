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
MARKER_PREFIX = "<!-- codex-auto-merge-preflight:"
MAX_SECTION_CHARS = 12_000

RequestFn = Callable[[str, str, str, dict[str, Any] | None], Any]


class PreflightCommentError(RuntimeError):
    pass


def marker_component(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip()).strip("-")
    return normalized or "unknown"


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
        "User-Agent": "us-equity-codex-auto-merge-preflight",
    }
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=DEFAULT_TIMEOUT_SECONDS) as response:
        charset = response.headers.get_content_charset("utf-8")
        raw = response.read().decode(charset)
        return json.loads(raw) if raw else None


def preflight_marker(report_month: str) -> str:
    return f"{MARKER_PREFIX}{marker_component(report_month)} -->"


def read_optional_text(path: Path, *, missing_message: str | None = None) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        if missing_message:
            return missing_message.format(path=path).rstrip() + "\n"
        return (
            f"_Preflight file was not generated: `{path}`. "
            "Treat guarded auto-merge as not ready until the full artifact is available._\n"
        )


def truncate_section(text: str, *, max_chars: int = MAX_SECTION_CHARS) -> str:
    text = text.strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + "\n\n_Section truncated; see the workflow artifact for the full file._"


def extract_markdown_section(markdown: str, heading: str) -> str:
    lines = markdown.splitlines()
    start: int | None = None
    for index, line in enumerate(lines):
        if line.strip() == heading:
            start = index
            break
    if start is None:
        return "_Section not found; see the workflow artifact for the full file._"
    collected: list[str] = []
    for index, line in enumerate(lines[start:], start=start):
        if index != start and line.startswith("## "):
            break
        collected.append(line)
    return "\n".join(collected).strip()


def build_preflight_comment(
    *,
    report_month: str,
    readiness_text: str,
    enablement_plan_text: str,
    label_sync_text: str | None = None,
) -> str:
    marker = preflight_marker(report_month)
    readiness = truncate_section(readiness_text)
    label_sync = truncate_section(label_sync_text) if label_sync_text is not None else ""
    checklist = truncate_section(extract_markdown_section(enablement_plan_text, "## Enablement preflight checklist"))
    label_sync_section = f"### Label sync snapshot\n\n{label_sync}\n\n" if label_sync else ""
    return (
        f"{marker}\n"
        "## Guarded Auto-Merge Preflight\n\n"
        "This comment is updated by the monthly snapshot review workflow before dispatching CodexAuditBridge. "
        "It is informational only and does not approve or enable guarded auto-merge. "
        "The complete label-sync report, readiness report, and enablement plan are attached to the monthly review bundle artifact.\n\n"
        f"{label_sync_section}"
        "### Readiness snapshot\n\n"
        f"{readiness}\n\n"
        "### Enablement checklist\n\n"
        f"{checklist}\n"
    )


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


def find_existing_preflight_comment(comments: list[Any], marker: str) -> int | None:
    for comment in comments:
        if not isinstance(comment, dict):
            continue
        body = comment.get("body")
        comment_id = comment.get("id")
        if isinstance(body, str) and body.startswith(marker) and isinstance(comment_id, int):
            return comment_id
    return None


def list_issue_comments(*, api_url: str, repo: str, token: str, issue_number: int, request_fn: RequestFn = github_request) -> list[Any]:
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
            raise PreflightCommentError(f"issue comments response is invalid for issue #{issue_number}")
        comments.extend(payload)
        if len(payload) < 100:
            break
        page += 1
    return comments


def upsert_preflight_comment(
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
    existing_id = find_existing_preflight_comment(comments, marker)
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


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or update the Codex guarded auto-merge preflight issue comment.")
    parser.add_argument("--repo", required=True)
    parser.add_argument("--issue-number", required=True, type=positive_int)
    parser.add_argument("--report-month", required=True)
    parser.add_argument("--readiness-file", required=True, type=Path)
    parser.add_argument("--label-sync-file", type=Path)
    parser.add_argument("--enablement-plan-file", required=True, type=Path)
    parser.add_argument("--output-file", type=Path, help="Optional local file for the rendered comment body.")
    parser.add_argument("--dry-run", action="store_true", help="Render the comment body without calling GitHub.")
    parser.add_argument("--api-url", default=DEFAULT_API_URL)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    marker = preflight_marker(args.report_month)
    body = build_preflight_comment(
        report_month=args.report_month,
        readiness_text=read_optional_text(args.readiness_file),
        label_sync_text=(
            read_optional_text(
                args.label_sync_file,
                missing_message=(
                    "_Label sync file was not generated: `{path}`. "
                    "This usually means guarded auto-merge was not requested, or label sync failed before writing its report._"
                ),
            )
            if args.label_sync_file
            else None
        ),
        enablement_plan_text=read_optional_text(args.enablement_plan_file),
    )
    outputs: dict[str, str] = {}
    if args.output_file:
        write_output_file(args.output_file, body)
        outputs["preflight_comment_file"] = str(args.output_file)
        print(f"preflight_comment_file={args.output_file}")
    if args.dry_run:
        outputs.update({"preflight_comment_action": "dry_run", "preflight_comment_url": ""})
        append_github_output(outputs)
        print("preflight_comment_action=dry_run")
        print("preflight_comment_url=")
        return 0

    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if not token:
        if outputs:
            append_github_output(outputs)
        print("GITHUB_TOKEN is required", file=sys.stderr)
        return 1
    try:
        action, comment_url = upsert_preflight_comment(
            api_url=args.api_url.rstrip("/"),
            repo=args.repo,
            token=token,
            issue_number=args.issue_number,
            body=body,
            marker=marker,
        )
    except PreflightCommentError as exc:
        if outputs:
            append_github_output(outputs)
        print(f"Preflight comment failed: {exc}", file=sys.stderr)
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

    outputs.update({"preflight_comment_action": action, "preflight_comment_url": comment_url})
    append_github_output(outputs)
    print(f"preflight_comment_action={action}")
    print(f"preflight_comment_url={comment_url}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
