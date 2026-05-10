from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


DEFAULT_API_URL = "https://api.github.com"
CODEX_LABEL = "codex-bridge"
TASK_LABEL = "monthly-codex-remediation"
AUTO_MERGE_LABEL = "auto-merge-ok"
MARKER_PREFIX = "<!-- codex-monthly-remediation:"


def build_marker(source_issue_number: int) -> str:
    return f"{MARKER_PREFIX}{source_issue_number} -->"


def build_issue_title(source_issue_number: int, source_issue_title: str) -> str:
    return f"Codex Monthly Remediation: #{source_issue_number} {source_issue_title}".strip()


def build_issue_body(
    *,
    source_issue_number: int,
    source_issue_title: str,
    source_issue_url: str,
    review_markdown: str,
) -> str:
    return f"""{build_marker(source_issue_number)}
# Codex Monthly Remediation

- Source review issue: [{source_issue_title}]({source_issue_url})
- Source review issue number: `{source_issue_number}`
- Automation queue: `ccbot-bridge`
- Merge label: `{AUTO_MERGE_LABEL}`

## Task

Read the AI monthly review below and make only low-risk, mechanical improvements that are directly supported by the review.

Allowed auto-fix categories:

- documentation and operator runbook wording
- monthly review/reporting scripts
- monthly review workflow wiring
- tests for monthly review/reporting behavior
- error handling for missing artifacts or malformed review inputs

Blocked categories:

- strategy selection logic
- ranking formulas
- universe construction
- broker/runtime execution behavior
- dependency or packaging changes
- production publish targets

Open a draft PR first. After targeted tests pass, mark the PR ready and add `{AUTO_MERGE_LABEL}` only if every changed file is inside the allowed low-risk surface. Leave the PR draft if any production-risk file is touched or if tests cannot be run.

The PR body must include:

```text
<!-- codex-monthly-remediation:issue-{source_issue_number} -->
```

Use branch prefix:

```text
codex/monthly-review-issue-{source_issue_number}
```

## AI Monthly Review

{review_markdown.strip()}
""".strip() + "\n"


def github_request(method: str, url: str, token: str, payload: dict[str, Any] | None = None) -> Any:
    data = None
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "us-equity-codex-remediation",
    }
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(request) as response:
        charset = response.headers.get_content_charset("utf-8")
        raw = response.read().decode(charset)
        return json.loads(raw) if raw else None


def ensure_label(api_url: str, repo: str, token: str, name: str, color: str, description: str) -> None:
    encoded = urllib.parse.quote(name, safe="")
    try:
        github_request("GET", f"{api_url}/repos/{repo}/labels/{encoded}", token)
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            raise
        github_request(
            "POST",
            f"{api_url}/repos/{repo}/labels",
            token,
            {"name": name, "color": color, "description": description},
        )


def _body_marker(body: str) -> str:
    for line in body.splitlines():
        if line.startswith(MARKER_PREFIX):
            return line.strip()
    return ""


def find_existing_issue(api_url: str, repo: str, token: str, marker: str) -> dict[str, Any] | None:
    issues = github_request(
        "GET",
        f"{api_url}/repos/{repo}/issues?state=open&labels={urllib.parse.quote(TASK_LABEL)}&per_page=100",
        token,
    )
    return next((issue for issue in issues if _body_marker(issue.get("body", "")) == marker), None)


def upsert_issue(
    *,
    api_url: str,
    repo: str,
    token: str,
    title: str,
    body: str,
) -> tuple[str, int, str]:
    ensure_label(api_url, repo, token, CODEX_LABEL, "5319E7", "Queue this issue for the VPS ccbot Codex bridge")
    ensure_label(api_url, repo, token, TASK_LABEL, "1D76DB", "Monthly AI review remediation task for Codex")
    ensure_label(api_url, repo, token, AUTO_MERGE_LABEL, "0E8A16", "Allow guarded auto-merge after CI")
    payload = {"title": title, "body": body, "labels": [CODEX_LABEL, TASK_LABEL]}
    existing = find_existing_issue(api_url, repo, token, _body_marker(body))
    if existing:
        updated = github_request("PATCH", f"{api_url}/repos/{repo}/issues/{existing['number']}", token, payload)
        return "updated", int(updated["number"]), str(updated["html_url"])
    created = github_request("POST", f"{api_url}/repos/{repo}/issues", token, payload)
    return "created", int(created["number"]), str(created["html_url"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or update the Codex monthly remediation issue.")
    parser.add_argument("--repo", required=True)
    parser.add_argument("--source-issue-number", required=True, type=int)
    parser.add_argument("--source-issue-title", required=True)
    parser.add_argument("--source-issue-url", required=True)
    parser.add_argument("--review-file", required=True, type=Path)
    parser.add_argument("--api-url", default=DEFAULT_API_URL)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        print("GITHUB_TOKEN is required", file=sys.stderr)
        return 1
    body = build_issue_body(
        source_issue_number=args.source_issue_number,
        source_issue_title=args.source_issue_title,
        source_issue_url=args.source_issue_url,
        review_markdown=args.review_file.read_text(encoding="utf-8"),
    )
    try:
        action, issue_number, issue_url = upsert_issue(
            api_url=args.api_url.rstrip("/"),
            repo=args.repo,
            token=token,
            title=build_issue_title(args.source_issue_number, args.source_issue_title),
            body=body,
        )
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        print(f"GitHub API request failed: {exc.code} {detail}", file=sys.stderr)
        return 1

    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as handle:
            print(f"issue_action={action}", file=handle)
            print(f"issue_number={issue_number}", file=handle)
            print(f"issue_url={issue_url}", file=handle)
    print(f"issue_action={action}")
    print(f"issue_number={issue_number}")
    print(f"issue_url={issue_url}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
