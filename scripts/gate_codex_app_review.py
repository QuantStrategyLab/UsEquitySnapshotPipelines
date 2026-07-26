#!/usr/bin/env python3
"""Fail-closed, read-only evidence gate for a GitHub Codex App final review."""
from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

API_BASE = "https://api.github.com"
BOT_LOGIN = "chatgpt-codex-connector[bot]"
BOT_ID = 199175422
FINAL_REVIEW_LABEL = "codex-final-review"
POLICY_PATH = Path(".github/codex_auto_merge_policy.json")
BLOCKING_FINDING = re.compile(r"!\[P[012] Badge\]\(", re.IGNORECASE)
SENSITIVE_VALUE = re.compile(
    r'(?:api[_\s]?key|secret|password|token|credential|private[_\s]?key)\s*[:=]\s*["\']'
    r'(?!\$\{\{|\{\{|example|placeholder|test|your[-_\s]|xxx|TODO|CHANGEME)[^"\']{12,}["\']',
    re.IGNORECASE,
)


@dataclass(frozen=True)
class GateResult:
    exit_code: int
    title: str
    summary: str


def env(name: str) -> str:
    return os.environ.get(name, "").strip()


def github_request(token: str, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
    url = f"{API_BASE}{path}" if path.startswith("/") else path
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "codex-final-review-evidence-gate",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        raise RuntimeError("GitHub review evidence request failed") from exc
    try:
        return json.loads(body) if body else {}
    except json.JSONDecodeError as exc:
        raise RuntimeError("GitHub review evidence response was not JSON") from exc


def github_text_request(token: str, path: str) -> str:
    url = f"{API_BASE}{path}" if path.startswith("/") else path
    request = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.v3.diff",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "codex-final-review-evidence-gate",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.read().decode("utf-8", errors="replace")
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        raise RuntimeError("GitHub static evidence request failed") from exc


def load_policy() -> dict[str, Any]:
    if POLICY_PATH.exists():
        try:
            policy = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
            if isinstance(policy, dict):
                return policy
        except (OSError, json.JSONDecodeError):
            pass
    return {
        "blocked_path_patterns": [
            r"(^|/)(\.env|.*secret.*|.*credential.*|.*token.*|.*private.*|.*\.pem|.*\.key)$",
        ],
        "max_changed_files": 50,
        "max_changed_lines": 5000,
    }


def compile_patterns(policy: dict[str, Any]) -> list[re.Pattern[str]]:
    patterns: list[re.Pattern[str]] = []
    for raw in policy.get("blocked_path_patterns", []):
        if isinstance(raw, str) and raw.strip():
            try:
                patterns.append(re.compile(raw, re.IGNORECASE))
            except re.error:
                continue
    return patterns


def scan_diff(diff_text: str, patterns: list[re.Pattern[str]]) -> list[str]:
    violations: list[str] = []
    current_file = ""
    for line in diff_text.splitlines():
        if line.startswith("diff --git "):
            parts = line.split(" ")
            current_file = parts[3][2:] if len(parts) >= 4 and parts[3].startswith("b/") else ""
            if current_file and any(pattern.search(current_file) for pattern in patterns):
                violations.append(f"Blocked file: {current_file}")
            continue
        if line.startswith("+++ b/"):
            current_file = line[6:]
            continue
        if line.startswith("+") and not line.startswith("+++") and SENSITIVE_VALUE.search(line[1:]):
            violations.append(f"Hardcoded secret: {current_file}")
    return list(dict.fromkeys(violations))


def static_guard_issues(files: list[dict[str, Any]], diff_text: str) -> list[str]:
    policy = load_policy()
    issues = scan_diff(diff_text, compile_patterns(policy))
    additions = sum(file.get("additions", 0) or 0 for file in files)
    deletions = sum(file.get("deletions", 0) or 0 for file in files)
    if len(files) > policy.get("max_changed_files", 50):
        issues.append("Too many changed files")
    if additions + deletions > policy.get("max_changed_lines", 5000):
        issues.append("Too many changed lines")
    for file in files:
        status = (file.get("status") or "").lower()
        if status in {"removed", "renamed", "copied"}:
            issues.append(f"File {status}: {file.get('filename', '?')}")
    return issues


def get_static_issues(token: str, repo: str, pr_number: int) -> list[str]:
    files: list[dict[str, Any]] = []
    page = 1
    while True:
        batch = github_request(token, "GET", f"/repos/{repo}/pulls/{pr_number}/files?per_page=100&page={page}")
        if not isinstance(batch, list) or not all(isinstance(file, dict) for file in batch):
            raise RuntimeError("GitHub static evidence was malformed")
        files.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    diff_text = github_text_request(token, f"/repos/{repo}/pulls/{pr_number}")
    return static_guard_issues(files, diff_text)


def trusted_codex_author(author: object) -> bool:
    if not isinstance(author, dict):
        return False
    account_id = author.get("id", author.get("databaseId"))
    account_type = author.get("type", author.get("__typename"))
    return (
        author.get("login") == BOT_LOGIN
        and account_id == BOT_ID
        and account_type == "Bot"
    )


def has_final_review_intent(pr: dict[str, Any]) -> bool:
    labels = pr.get("labels")
    if not isinstance(labels, list):
        return False
    return any(isinstance(label, dict) and label.get("name") == FINAL_REVIEW_LABEL for label in labels)


def unresolved_blocking_findings(threads: list[dict[str, Any]], review_id: int) -> bool:
    for thread in threads:
        if thread.get("isResolved") is not False:
            continue
        comments = thread.get("comments")
        if isinstance(comments, dict):
            comments = comments.get("nodes")
        if not isinstance(comments, list):
            return True
        for comment in comments:
            if not isinstance(comment, dict):
                return True
            review = comment.get("pullRequestReview")
            comment_review_id = comment.get("review_id")
            if comment_review_id is None and isinstance(review, dict):
                comment_review_id = review.get("databaseId")
            if comment_review_id != review_id:
                continue
            if BLOCKING_FINDING.search(str(comment.get("body") or "")):
                return True
    return False


def evaluate_final_review(
    pr: dict[str, Any],
    reviews: list[dict[str, Any]],
    review_threads: list[dict[str, Any]],
    *,
    evidence_error: str = "",
) -> GateResult:
    if evidence_error:
        return GateResult(1, "Codex final review evidence unavailable", "GitHub review evidence could not be verified.")
    if pr.get("draft") is True:
        return GateResult(1, "Codex final review blocked: draft PR", "Mark the PR ready before final review.")
    if not has_final_review_intent(pr):
        return GateResult(1, "Codex final review blocked: missing intent", f"Apply `{FINAL_REVIEW_LABEL}` first.")

    head = ((pr.get("head") or {}).get("sha") or "").strip()
    if not head:
        return GateResult(1, "Codex final review evidence unavailable", "PR head SHA was missing.")

    trusted_reviews = [review for review in reviews if trusted_codex_author(review.get("user"))]
    if not trusted_reviews:
        return GateResult(1, "Codex final review blocked: missing trusted review", "No trusted GitHub Codex App review exists.")

    review = trusted_reviews[-1]
    if review.get("commit_id") != head:
        return GateResult(1, "Codex final review blocked: review is not at current head", "Pushes require a new final review.")
    if (review.get("state") or "").upper() != "APPROVED":
        return GateResult(1, "Codex final review blocked: not approved", "The trusted final review must be APPROVED.")
    if BLOCKING_FINDING.search(str(review.get("body") or "")):
        return GateResult(1, "Codex final review blocked: unresolved P0/P1/P2", "Blocking severity was found in the final review.")

    review_id = review.get("id")
    if not isinstance(review_id, int):
        return GateResult(1, "Codex final review evidence unavailable", "Trusted review identity was incomplete.")
    if unresolved_blocking_findings(review_threads, review_id):
        return GateResult(1, "Codex final review blocked: unresolved P0/P1/P2", "Blocking findings remain unresolved.")

    return GateResult(0, "Codex final review approved", "Trusted approval matches the current PR head with no unresolved P0/P1/P2.")


def evaluate_gate(
    pr: dict[str, Any],
    reviews: list[dict[str, Any]],
    review_threads: list[dict[str, Any]],
    *,
    static_issues: list[str] | None = None,
    evidence_error: str = "",
) -> GateResult:
    if evidence_error:
        return evaluate_final_review(pr, reviews, review_threads, evidence_error=evidence_error)
    if static_issues:
        return GateResult(1, "Codex final review blocked: static guard", "Deterministic static guard found blocked changes.")
    return evaluate_final_review(pr, reviews, review_threads)


def get_review_threads(token: str, repo: str, pr_number: int) -> list[dict[str, Any]]:
    owner, name = repo.split("/", 1)
    query = """
query ReviewThreads($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      reviewThreads(first: 100) {
        nodes {
          isResolved
          comments(first: 100) {
            nodes {
              body
              author { login __typename }
              pullRequestReview { databaseId }
            }
            pageInfo { hasNextPage }
          }
        }
        pageInfo { hasNextPage }
      }
    }
  }
}
"""
    response = github_request(
        token,
        "POST",
        f"{API_BASE}/graphql",
        {"query": query, "variables": {"owner": owner, "name": name, "number": pr_number}},
    )
    pull_request = ((response.get("data") or {}).get("repository") or {}).get("pullRequest")
    threads = (pull_request or {}).get("reviewThreads") if isinstance(pull_request, dict) else None
    if response.get("errors") or not isinstance(threads, dict) or threads.get("pageInfo", {}).get("hasNextPage"):
        raise RuntimeError("GitHub review thread evidence was incomplete")
    nodes = threads.get("nodes")
    if not isinstance(nodes, list) or not all(isinstance(node, dict) for node in nodes):
        raise RuntimeError("GitHub review thread evidence was malformed")
    if any(((node.get("comments") or {}).get("pageInfo") or {}).get("hasNextPage") for node in nodes):
        raise RuntimeError("GitHub review comment evidence was incomplete")
    return nodes


def get_review_evidence(token: str, repo: str, pr_number: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    reviews: list[dict[str, Any]] = []
    page = 1
    while True:
        batch = github_request(token, "GET", f"/repos/{repo}/pulls/{pr_number}/reviews?per_page=100&page={page}")
        if not isinstance(batch, list) or not all(isinstance(review, dict) for review in batch):
            raise RuntimeError("GitHub review evidence was malformed")
        reviews.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return reviews, get_review_threads(token, repo, pr_number)


def step_summary(result: GateResult) -> None:
    path = env("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as summary:
            summary.write(f"## {result.title}\n\n{result.summary}\n")


def main() -> int:
    token = env("GH_TOKEN") or env("GITHUB_TOKEN")
    repo = env("GITHUB_REPOSITORY")
    event_path = Path(env("GITHUB_EVENT_PATH"))
    if not token or not repo or not event_path.is_file():
        result = GateResult(1, "Codex final review evidence unavailable", "GitHub PR event context was missing.")
        print(result.title, file=sys.stderr)
        return result.exit_code

    try:
        event = json.loads(event_path.read_text(encoding="utf-8"))
        pr = event["pull_request"]
        pr_number = int(pr["number"])
        static_issues = get_static_issues(token, repo, pr_number)
        reviews, threads = get_review_evidence(token, repo, pr_number)
        result = evaluate_gate(pr, reviews, threads, static_issues=static_issues)
    except (KeyError, TypeError, ValueError, OSError, json.JSONDecodeError, RuntimeError) as exc:
        result = evaluate_gate({}, [], [], evidence_error=type(exc).__name__)

    print(result.title)
    step_summary(result)
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
