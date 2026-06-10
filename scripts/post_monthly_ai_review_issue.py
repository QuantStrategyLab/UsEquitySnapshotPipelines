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
DEFAULT_LABEL = "monthly-review"
DEFAULT_TIMEOUT_SECONDS = 30


def github_request(
    method: str,
    url: str,
    token: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> Any:
    data = None
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "us-equity-monthly-ai-review",
    }
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset("utf-8")
        raw = response.read().decode(charset)
        return json.loads(raw) if raw else None


def ensure_label(api_url: str, repo: str, token: str, label: str) -> None:
    encoded = urllib.parse.quote(label, safe="")
    label_url = f"{api_url}/repos/{repo}/labels/{encoded}"
    try:
        github_request("GET", label_url, token)
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            raise
        github_request(
            "POST",
            f"{api_url}/repos/{repo}/labels",
            token,
            {
                "name": label,
                "color": "0E8A16",
                "description": "Automated monthly snapshot AI review",
            },
        )


def find_existing_issue(api_url: str, repo: str, token: str, label: str, title: str) -> dict[str, Any] | None:
    issues = github_request(
        "GET",
        f"{api_url}/repos/{repo}/issues?state=open&labels={urllib.parse.quote(label)}&per_page=100",
        token,
    )
    return next((issue for issue in issues if issue.get("title") == title), None)


def upsert_issue(
    *,
    api_url: str,
    repo: str,
    token: str,
    title: str,
    body: str,
    label: str,
) -> tuple[str, int, str]:
    ensure_label(api_url, repo, token, label)
    payload = {"title": title, "body": body, "labels": [label]}
    existing = find_existing_issue(api_url, repo, token, label, title)
    if existing:
        updated = github_request("PATCH", f"{api_url}/repos/{repo}/issues/{existing['number']}", token, payload)
        return "updated", int(updated["number"]), str(updated["html_url"])
    created = github_request("POST", f"{api_url}/repos/{repo}/issues", token, payload)
    return "created", int(created["number"]), str(created["html_url"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or update the monthly AI review issue.")
    parser.add_argument("--repo", required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--body-file", required=True, type=Path)
    parser.add_argument("--label", default=DEFAULT_LABEL)
    parser.add_argument("--api-url", default=DEFAULT_API_URL)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        print("GITHUB_TOKEN is required", file=sys.stderr)
        return 1
    try:
        action, issue_number, issue_url = upsert_issue(
            api_url=args.api_url.rstrip("/"),
            repo=args.repo,
            token=token,
            title=args.title,
            body=args.body_file.read_text(encoding="utf-8"),
            label=args.label,
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
