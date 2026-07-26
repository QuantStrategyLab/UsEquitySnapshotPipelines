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
BLOCKING_SEVERITY = re.compile(r"\bP[012]\b", re.IGNORECASE)


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
        if not isinstance(comments, list):
            return True
        for comment in comments:
            if not isinstance(comment, dict):
                return True
            review = comment.get("pullRequestReview")
            comment_review_id = comment.get("review_id")
            if comment_review_id is None and isinstance(review, dict):
                comment_review_id = review.get("databaseId")
            if comment_review_id != review_id or not trusted_codex_author(comment.get("author")):
                continue
            if BLOCKING_SEVERITY.search(str(comment.get("body") or "")):
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
    if BLOCKING_SEVERITY.search(str(review.get("body") or "")):
        return GateResult(1, "Codex final review blocked: unresolved P0/P1/P2", "Blocking severity was found in the final review.")

    review_id = review.get("id")
    if not isinstance(review_id, int):
        return GateResult(1, "Codex final review evidence unavailable", "Trusted review identity was incomplete.")
    if unresolved_blocking_findings(review_threads, review_id):
        return GateResult(1, "Codex final review blocked: unresolved P0/P1/P2", "Blocking findings remain unresolved.")

    return GateResult(0, "Codex final review approved", "Trusted approval matches the current PR head with no unresolved P0/P1/P2.")


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
              author { login databaseId __typename }
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
    reviews = github_request(token, "GET", f"/repos/{repo}/pulls/{pr_number}/reviews?per_page=100")
    if not isinstance(reviews, list) or not all(isinstance(review, dict) for review in reviews):
        raise RuntimeError("GitHub review evidence was malformed")
    if len(reviews) == 100:
        more_reviews = github_request(token, "GET", f"/repos/{repo}/pulls/{pr_number}/reviews?per_page=1&page=2")
        if not isinstance(more_reviews, list):
            raise RuntimeError("GitHub review evidence was malformed")
        if more_reviews:
            raise RuntimeError("GitHub review evidence was incomplete")
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
        reviews, threads = get_review_evidence(token, repo, pr_number)
        result = evaluate_final_review(pr, reviews, threads)
    except (KeyError, TypeError, ValueError, OSError, json.JSONDecodeError, RuntimeError) as exc:
        result = evaluate_final_review({}, [], [], evidence_error=type(exc).__name__)

    print(result.title)
    step_summary(result)
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
