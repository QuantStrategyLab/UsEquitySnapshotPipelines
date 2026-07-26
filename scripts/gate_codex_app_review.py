#!/usr/bin/env python3
"""Fail-closed, read-only V2 evidence evaluator for Codex App final reviews."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

API_BASE = "https://api.github.com"
BOT_LOGIN = "chatgpt-codex-connector[bot]"
BOT_ID = 199175422
BOT_TYPE = "Bot"
POLICY_PATH = Path(".github/codex_auto_merge_policy.json")
FULL_SHA = re.compile(r"^[0-9a-f]{40}$", re.IGNORECASE)
CLEAN_PREFIX = re.compile(r"(?im)^\s*Reviewed\s+commit\s*:\s*`?([0-9a-f]+)`?\s*$")
FINDING = re.compile(r"(?im)^\s*(?:[-*]\s*)?(?:\[\s*)?(P[0-3])(?:\s*\])?\s*[:\-]\s*(.+?)\s*$")
NEGATED = re.compile(r"^(?:no|none|not|resolved|fixed|false|0)\b", re.IGNORECASE)


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def github_request(token: str, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
    url = f"{API_BASE}{path}" if not path.startswith("https://") else path
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {
        "Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "codex-final-review-gate",
    }
    if payload is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            text = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GitHub API {method} {path}: {exc.code} {detail[:300]}") from exc
    except OSError as exc:
        raise RuntimeError(f"GitHub API {method} {path}: {exc}") from exc
    try:
        return json.loads(text) if text else {}
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"GitHub API {method} {path}: malformed JSON") from exc


class GitHubClient:
    def __init__(self, token: str):
        self.token = token

    def get(self, path: str) -> Any:
        return github_request(self.token, "GET", path)

    def graphql(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        result = github_request(self.token, "POST", "/graphql", {"query": query, "variables": variables})
        if not isinstance(result, dict) or result.get("errors") or not isinstance(result.get("data"), dict):
            raise ValueError("GraphQL response incomplete")
        return result


def step_summary(text: str) -> None:
    path = env("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as output:
            output.write(text + "\n")


def load_policy() -> dict[str, Any]:
    if POLICY_PATH.exists():
        try:
            value = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
            if isinstance(value, dict):
                return value
        except (OSError, json.JSONDecodeError):
            pass
    return {
        "version": 1,
        "blocked_path_patterns": [r"(^|/)(\.env|.*secret.*|.*credential.*|.*token.*|.*private.*|.*\.pem|.*\.key)$"],
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


_SENSITIVE = re.compile(
    r'(?:api[_\s]?key|secret|password|token|credential|private[_\s]?key)\s*[:=]\s*["\']'
    r'(?!\$\{\{|\{\{|example|placeholder|test|your[-_\s]|xxx|TODO|CHANGEME)[^"\']{12,}["\']', re.IGNORECASE)


def scan_diff(diff_text: str, path_patterns: list[re.Pattern[str]]) -> list[str]:
    violations: list[str] = []
    current = ""
    for line in diff_text.splitlines():
        if line.startswith("diff --git "):
            parts = line.split(" ")
            current = parts[3][2:] if len(parts) >= 4 and parts[3].startswith("b/") else ""
            if current and any(pattern.search(current) for pattern in path_patterns):
                violations.append(f"Blocked file: {current}")
        elif line.startswith("+++ b/"):
            current = line[6:]
        elif line.startswith("+") and not line.startswith("+++") and _SENSITIVE.search(line[1:]):
            violations.append(f"Hardcoded secret: {current}")
    return list(dict.fromkeys(violations))


def check_metadata(files: list[dict[str, Any]], policy: dict[str, Any]) -> list[str]:
    if not all(isinstance(item, dict) for item in files):
        raise ValueError("PR files response malformed")
    issues: list[str] = []
    additions = sum(item.get("additions", 0) or 0 for item in files)
    deletions = sum(item.get("deletions", 0) or 0 for item in files)
    for item in files:
        name = item.get("filename", "?")
        state = str(item.get("status", "")).lower()
        if state == "removed":
            issues.append(f"File deleted: {name}")
        elif state == "renamed":
            issues.append(f"File renamed: {item.get('previous_filename', '?')} -> {name}")
    if len(files) > policy.get("max_changed_files", 50):
        issues.append("Too many changed files")
    if additions + deletions > policy.get("max_changed_lines", 5000):
        issues.append("Too many changed lines")
    return issues


def run_static_guard(token: str, repo: str, pr_number: int) -> int:
    """Evaluate deterministic guards first; API errors deliberately propagate."""
    files = paginate_rest(GitHubClient(token), f"/repos/{repo}/pulls/{pr_number}/files")
    request = urllib.request.Request(
        f"{API_BASE}/repos/{repo}/pulls/{pr_number}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github.v3.diff",
                 "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "codex-final-review-gate"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            diff = response.read().decode("utf-8", errors="replace")
    except (urllib.error.HTTPError, OSError) as exc:
        raise RuntimeError(f"static diff read failed: {exc}") from exc
    issues = check_metadata(files, load_policy()) + scan_diff(diff, compile_patterns(load_policy()))
    if issues:
        print("STATIC -> BLOCKED: " + "; ".join(issues))
        step_summary("## codex-final-review: static guard blocked\n\n" + "\n".join(f"- {item}" for item in issues))
        return 1
    print("STATIC -> clean")
    return 0


@dataclass(frozen=True)
class EventContext:
    pr_number: int
    source_id: str
    dedupe_key: str
    event_head: str | None


def event_context(event_name: str, event: dict[str, Any]) -> EventContext:
    if event_name in {"pull_request", "pull_request_review"}:
        pr = event.get("pull_request")
        if not isinstance(pr, dict) or not isinstance(pr.get("number"), int):
            raise ValueError("event has no pull request")
        source = event.get("review", {}).get("id") if event_name == "pull_request_review" else event.get("delivery") or event.get("action")
        head = (pr.get("head") or {}).get("sha")
        if source is None:
            raise ValueError("event source id missing")
        if head is not None and (not isinstance(head, str) or not FULL_SHA.fullmatch(head)):
            raise ValueError("event head malformed")
        return EventContext(pr["number"], f"{event_name}:{source}", f"{event_name}:{pr['number']}:{source}:{head or ''}", head)
    if event_name == "issue_comment":
        issue, comment = event.get("issue"), event.get("comment")
        if not isinstance(issue, dict) or not isinstance(issue.get("pull_request"), dict) or not isinstance(comment, dict):
            raise ValueError("issue_comment is not a PR top-level comment")
        if not isinstance(issue.get("number"), int) or not isinstance(comment.get("id"), int):
            raise ValueError("issue_comment identity malformed")
        source = f"issue_comment:{comment['id']}"
        return EventContext(issue["number"], source, f"issue_comment:{issue['number']}:{comment['id']}", None)
    raise ValueError(f"unsupported event: {event_name}")


def paginate_rest(client: Any, path: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    separator = "&" if "?" in path else "?"
    page = 1
    while True:
        batch = client.get(f"{path}{separator}per_page=100&page={page}")
        if not isinstance(batch, list) or not all(isinstance(item, dict) for item in batch):
            raise ValueError("REST pagination malformed")
        for item in batch:
            identity = item.get("id")
            if identity is None or str(identity) in seen:
                raise ValueError("REST pagination duplicate or identity missing")
            seen.add(str(identity))
            items.append(item)
        if len(batch) < 100:
            return items
        page += 1


def is_trusted_app_user(user: Any) -> bool:
    return isinstance(user, dict) and user.get("login") == BOT_LOGIN and user.get("id") == BOT_ID and user.get("type") == BOT_TYPE


def is_trusted_maintainer(client: Any, repo: str, user: Any) -> bool:
    if not isinstance(user, dict) or not isinstance(user.get("login"), str) or not isinstance(user.get("id"), int):
        return False
    if user.get("type") not in {"User", "Organization"}:
        return False
    permission = client.get(f"/repos/{repo}/collaborators/{user['login']}/permission")
    return isinstance(permission, dict) and permission.get("permission") in {"admin", "maintain", "write"}


def parse_final_review_request(body: Any, repo: str, pr_number: int) -> dict[str, Any]:
    if not isinstance(body, str):
        raise ValueError("request body missing")
    try:
        request = json.loads(body.strip())
    except json.JSONDecodeError as exc:
        raise ValueError("request must be one JSON object") from exc
    if not isinstance(request, dict) or request.get("request_kind") != "codex-final-review":
        raise ValueError("not final-review request")
    head = request.get("requested_head")
    if request.get("repo") != repo or request.get("pr_number") != pr_number or not isinstance(head, str) or not FULL_SHA.fullmatch(head):
        raise ValueError("request binding malformed")
    return request


def select_final_review_request(client: Any, repo: str, pr_number: int, comments: list[dict[str, Any]]) -> dict[str, Any]:
    matches: list[dict[str, Any]] = []
    for comment in comments:
        if comment.get("in_reply_to_id") is not None:
            continue
        try:
            parse_final_review_request(comment.get("body"), repo, pr_number)
        except ValueError:
            continue
        if not is_trusted_maintainer(client, repo, comment.get("user")):
            raise ValueError("request author is not a trusted maintainer")
        if not isinstance(comment.get("id"), int) or not isinstance(comment.get("html_url"), str) or not isinstance(comment.get("created_at"), str):
            raise ValueError("request immutable identity incomplete")
        matches.append(comment)
    if len(matches) != 1:
        raise ValueError("final-review request is missing or not unique")
    return matches[0]


def parse_clean_review_prefix(body: Any) -> str:
    if not isinstance(body, str):
        raise ValueError("clean response body missing")
    prefixes = CLEAN_PREFIX.findall(body)
    if len(prefixes) != 1 or len(prefixes[0]) < 10:
        raise ValueError("clean response must contain one 10+ hexadecimal Reviewed commit field")
    return prefixes[0].lower()


def is_relevant_event(event_name: str, event: dict[str, Any]) -> bool:
    """Ignore unrelated PR conversation events without recording a V2 decision."""
    if event_name == "pull_request":
        return True
    if event_name == "pull_request_review":
        return is_trusted_app_user((event.get("review") or {}).get("user"))
    if event_name == "issue_comment":
        comment = event.get("comment") or {}
        if is_trusted_app_user(comment.get("user")):
            try:
                parse_clean_review_prefix(comment.get("body"))
                return True
            except ValueError:
                return False
        try:
            request = json.loads(comment.get("body", ""))
        except (TypeError, json.JSONDecodeError):
            return False
        return isinstance(request, dict) and request.get("request_kind") == "codex-final-review"
    return False


def resolve_commit_prefix(client: Any, repo: str, prefix: str) -> str:
    if len(prefix) < 10 or not re.fullmatch(r"[0-9a-f]+", prefix, re.IGNORECASE):
        raise ValueError("commit prefix malformed")
    try:
        result = client.get(f"/repos/{repo}/commits/{prefix}")
    except Exception as exc:
        raise ValueError("commit prefix could not be uniquely resolved") from exc
    sha = result.get("sha") if isinstance(result, dict) else None
    if not isinstance(sha, str) or not FULL_SHA.fullmatch(sha):
        raise ValueError("commit prefix resolution malformed or ambiguous")
    return sha.lower()


def parse_time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp missing")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("timestamp malformed") from exc


def classify_findings(body: Any) -> list[str]:
    if not isinstance(body, str):
        return []
    result: list[str] = []
    for severity, text in FINDING.findall(body):
        if severity in {"P0", "P1", "P2"} and not NEGATED.match(text.strip()):
            result.append(severity)
    return result


THREADS_QUERY = """
query($owner:String!, $name:String!, $number:Int!, $after:String) {
  repository(owner:$owner, name:$name) { pullRequest(number:$number) {
    reviewThreads(first:100, after:$after) { nodes { id isResolved comments(first:100) {
      nodes { id body author { login databaseId __typename } pullRequestReview { id state author { login databaseId __typename } commit { oid } } }
      pageInfo { hasNextPage endCursor } } } pageInfo { hasNextPage endCursor }
  } } }
}"""
THREAD_COMMENTS_QUERY = """
query($thread:ID!, $after:String) { node(id:$thread) { ... on PullRequestReviewThread {
  comments(first:100, after:$after) { nodes { id body author { login databaseId __typename } pullRequestReview { id state author { login databaseId __typename } commit { oid } } } pageInfo { hasNextPage endCursor } }
} } }"""


def graphql_user(user: Any) -> dict[str, Any]:
    if not isinstance(user, dict):
        return {}
    return {"login": user.get("login"), "id": user.get("databaseId"), "type": "Bot" if user.get("__typename") == "Bot" else user.get("__typename")}


def paginate_review_threads(client: Any, owner: str, name: str, pr_number: int) -> list[dict[str, Any]]:
    threads: list[dict[str, Any]] = []
    seen_threads: set[str] = set()
    cursor: str | None = None
    while True:
        result = client.graphql(THREADS_QUERY, {"owner": owner, "name": name, "number": pr_number, "after": cursor})
        try:
            connection = result["data"]["repository"]["pullRequest"]["reviewThreads"]
            nodes, page_info = connection["nodes"], connection["pageInfo"]
        except (KeyError, TypeError) as exc:
            raise ValueError("reviewThreads pagination incomplete") from exc
        if not isinstance(nodes, list) or not isinstance(page_info, dict):
            raise ValueError("reviewThreads pagination malformed")
        for thread in nodes:
            if not isinstance(thread, dict) or not isinstance(thread.get("id"), str) or thread["id"] in seen_threads:
                raise ValueError("reviewThreads duplicate or malformed")
            seen_threads.add(thread["id"])
            comments = thread.get("comments")
            if not isinstance(comments, dict) or not isinstance(comments.get("nodes"), list) or not isinstance(comments.get("pageInfo"), dict):
                raise ValueError("thread comments incomplete")
            full_comments = list(comments["nodes"])
            comment_ids = {item.get("id") for item in full_comments if isinstance(item, dict)}
            if len(comment_ids) != len(full_comments) or None in comment_ids:
                raise ValueError("thread comment identity malformed")
            comment_page = comments["pageInfo"]
            if not isinstance(comment_page.get("hasNextPage"), bool):
                raise ValueError("thread comment pageInfo malformed")
            while comment_page["hasNextPage"]:
                next_cursor = comment_page.get("endCursor")
                if not isinstance(next_cursor, str):
                    raise ValueError("thread comment cursor missing")
                extra = client.graphql(THREAD_COMMENTS_QUERY, {"thread": thread["id"], "after": next_cursor})
                try:
                    connection = extra["data"]["node"]["comments"]
                    batch, comment_page = connection["nodes"], connection["pageInfo"]
                except (KeyError, TypeError) as exc:
                    raise ValueError("nested comment pagination incomplete") from exc
                if not isinstance(batch, list) or not isinstance(comment_page, dict) or not isinstance(comment_page.get("hasNextPage"), bool):
                    raise ValueError("nested comment pagination malformed")
                for comment in batch:
                    if not isinstance(comment, dict) or not isinstance(comment.get("id"), str) or comment["id"] in comment_ids:
                        raise ValueError("nested comment duplicate or malformed")
                    comment_ids.add(comment["id"])
                    full_comments.append(comment)
            threads.append({"id": thread["id"], "isResolved": thread.get("isResolved"), "comments": full_comments})
        if not isinstance(page_info.get("hasNextPage"), bool):
            raise ValueError("reviewThreads pageInfo malformed")
        if not page_info["hasNextPage"]:
            return threads
        cursor = page_info.get("endCursor")
        if not isinstance(cursor, str):
            raise ValueError("reviewThreads cursor missing")


def get_pr_head(client: Any, repo: str, pr_number: int) -> str:
    pr = client.get(f"/repos/{repo}/pulls/{pr_number}")
    head = (pr.get("head") or {}).get("sha") if isinstance(pr, dict) else None
    if not isinstance(head, str) or not FULL_SHA.fullmatch(head):
        raise ValueError("current PR head unavailable")
    return head.lower()


def authenticate_comment(client: Any, repo: str, comment: dict[str, Any]) -> dict[str, Any]:
    comment_id = comment.get("id")
    if not isinstance(comment_id, int):
        raise ValueError("comment identity missing")
    canonical = client.get(f"/repos/{repo}/issues/comments/{comment_id}")
    fields = ("id", "body", "html_url", "created_at")
    if not isinstance(canonical, dict) or any(canonical.get(field) != comment.get(field) for field in fields):
        raise ValueError("comment immutable record mismatch")
    if canonical.get("user") != comment.get("user"):
        raise ValueError("comment author record mismatch")
    return canonical


def authenticate_review(client: Any, repo: str, pr_number: int, review: dict[str, Any]) -> dict[str, Any]:
    review_id = review.get("id")
    if not isinstance(review_id, int):
        raise ValueError("review identity missing")
    canonical = client.get(f"/repos/{repo}/pulls/{pr_number}/reviews/{review_id}")
    fields = ("id", "body", "html_url", "submitted_at", "commit_id", "state")
    if not isinstance(canonical, dict) or any(canonical.get(field) != review.get(field) for field in fields):
        raise ValueError("review immutable record mismatch")
    if canonical.get("user") != review.get("user"):
        raise ValueError("review author record mismatch")
    return canonical


def evaluate_v2_evidence(client: Any, repo: str, pr_number: int, context: EventContext | None = None) -> dict[str, Any]:
    """Return only an auditable decision; all insufficient evidence raises ValueError."""
    evaluation_head = get_pr_head(client, repo, pr_number)
    if context and context.event_head and context.event_head.lower() != evaluation_head:
        raise ValueError("event head drift")
    comments = paginate_rest(client, f"/repos/{repo}/issues/{pr_number}/comments")
    request = authenticate_comment(client, repo, select_final_review_request(client, repo, pr_number, comments))
    request_data = parse_final_review_request(request["body"], repo, pr_number)
    request_time = parse_time(request["created_at"])
    if request_data["requested_head"].lower() != evaluation_head:
        raise ValueError("request and evaluation head mismatch")

    reviews = paginate_rest(client, f"/repos/{repo}/pulls/{pr_number}/reviews")
    owner, name = repo.split("/", 1)
    threads = paginate_review_threads(client, owner, name, pr_number)
    current_reviews: list[dict[str, Any]] = []
    finding_severities: list[str] = []
    for review in reviews:
        if not is_trusted_app_user(review.get("user")):
            continue
        commit_id, state = review.get("commit_id"), str(review.get("state", "")).upper()
        if not isinstance(commit_id, str) or not FULL_SHA.fullmatch(commit_id):
            raise ValueError("trusted review commit malformed")
        if commit_id.lower() != evaluation_head:
            continue
        if state not in {"APPROVED", "CHANGES_REQUESTED", "COMMENTED", "DISMISSED"}:
            raise ValueError("trusted review state indeterminate")
        if parse_time(review.get("submitted_at")) <= request_time:
            raise ValueError("review response is not later than request")
        current_reviews.append(authenticate_review(client, repo, pr_number, review))
        finding_severities.extend(classify_findings(review.get("body")))

    for thread in threads:
        if not isinstance(thread.get("isResolved"), bool):
            raise ValueError("thread resolution missing")
        for comment in thread["comments"]:
            author = graphql_user(comment.get("author"))
            if not is_trusted_app_user(author):
                continue
            linked = comment.get("pullRequestReview")
            if not isinstance(linked, dict) or not is_trusted_app_user(graphql_user(linked.get("author"))):
                raise ValueError("trusted thread comment lacks trusted review linkage")
            commit = (linked.get("commit") or {}).get("oid")
            if not isinstance(commit, str) or not FULL_SHA.fullmatch(commit):
                raise ValueError("trusted thread commit malformed")
            if commit.lower() == evaluation_head and not thread["isResolved"]:
                finding_severities.extend(classify_findings(comment.get("body")))
    if finding_severities:
        raise ValueError("unresolved trusted finding: " + ", ".join(sorted(set(finding_severities))))

    clean: list[dict[str, Any]] = []
    for comment in comments:
        if comment.get("in_reply_to_id") is None and is_trusted_app_user(comment.get("user")):
            try:
                prefix = parse_clean_review_prefix(comment.get("body"))
            except ValueError:
                continue
            if parse_time(comment.get("created_at")) <= request_time:
                raise ValueError("clean response is not later than request")
            clean.append({"comment": authenticate_comment(client, repo, comment), "prefix": prefix})
    if len(clean) + len(current_reviews) != 1:
        raise ValueError("response evidence missing or ambiguous")
    if clean:
        response = clean[0]["comment"]
        resolved_head = resolve_commit_prefix(client, repo, clean[0]["prefix"])
        if resolved_head != request_data["requested_head"].lower() or resolved_head != evaluation_head:
            raise ValueError("request/resolved/evaluation head mismatch")
        response_type = "issue_comment"
    else:
        response = current_reviews[0]
        resolved_head = evaluation_head
        response_type = "pull_request_review"
    current_head = get_pr_head(client, repo, pr_number)
    if current_head != evaluation_head or current_head != resolved_head:
        raise ValueError("current PR head drift")
    evidence = {
        "request_id": request["id"], "response_id": response["id"], "response_type": response_type,
        "requested_head": request_data["requested_head"].lower(),
        "resolved_head": resolved_head, "current_head": current_head, "evaluation_head": evaluation_head,
        "response_body_sha256": hashlib.sha256(str(response.get("body", "")).encode()).hexdigest(),
        "evaluated_at": datetime.now().astimezone().isoformat(),
    }
    evidence["evidence_sha256"] = hashlib.sha256(json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return evidence


def get_codex_review(token: str, repo: str, pr_number: int) -> dict[str, Any] | None:
    reviews = github_request(token, "GET", f"/repos/{repo}/pulls/{pr_number}/reviews?per_page=100")
    if not isinstance(reviews, list):
        return None
    for review in reversed(reviews):
        if isinstance(review, dict) and review.get("user", {}).get("login") == BOT_LOGIN:
            return review
    return None


def app_decision(review: dict[str, Any] | None) -> tuple[int, str, str]:
    if review is None:
        return 1, "Codex: evidence insufficient — FAIL CLOSED", "No V2-bound review evidence was available."
    state = str(review.get("state", "")).upper()
    if state == "CHANGES_REQUESTED":
        return 1, "Codex: changes requested — MERGE BLOCKED", str(review.get("body", ""))[:500]
    if state == "APPROVED":
        return 0, "Codex: approved", "Trusted approval is present."
    return 1, "Codex: evidence insufficient — FAIL CLOSED", f"Unsupported review state: {state}"


def main() -> int:
    token, repo = env("GH_TOKEN") or env("GITHUB_TOKEN"), env("GITHUB_REPOSITORY")
    event_path, event_name = Path(env("GITHUB_EVENT_PATH")), env("GITHUB_EVENT_NAME")
    if not token or not repo or not event_path.is_file() or "/" not in repo:
        print("::error::V2 evaluator requires token, repository, and event payload", file=sys.stderr)
        return 1
    try:
        event = json.loads(event_path.read_text(encoding="utf-8"))
        if not isinstance(event, dict):
            raise ValueError("event payload malformed")
        context = event_context(event_name, event)
        if not is_relevant_event(event_name, event):
            print(f"V2_EVALUATOR_NOT_APPLICABLE: {context.dedupe_key}")
            return 0
        if run_static_guard(token, repo, context.pr_number) != 0:
            return 1
        evidence = evaluate_v2_evidence(GitHubClient(token), repo, context.pr_number, context)
    except (OSError, json.JSONDecodeError, RuntimeError, ValueError) as exc:
        print(f"V2_EVIDENCE_CONTRACT_INSUFFICIENT_FAIL_CLOSED: {exc}")
        step_summary(f"## codex-final-review: V2_EVIDENCE_CONTRACT_INSUFFICIENT_FAIL_CLOSED\n\n{exc}")
        return 1
    print("V2_EVIDENCE_CONTRACT_PASS " + json.dumps(evidence, sort_keys=True))
    step_summary("## codex-final-review: V2 evidence accepted\n\n`" + evidence["evidence_sha256"] + "`")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
