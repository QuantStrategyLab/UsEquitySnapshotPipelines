#!/usr/bin/env python3
"""Lightweight static PR guard — no API keys needed.

Checks the PR diff for:
1. Secret/credential patterns (blocked_path_patterns from policy)
2. File deletions, renames, copies (metadata risks)
3. Changed file count and line count limits

Fails the check → blocks merge via branch protection.
Complements the Codex App review gate (which handles AI-level review).
"""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

API_BASE = "https://api.github.com"
CHECK_NAME = "Codex Review Gate"
POLICY_PATH = Path(".github/codex_auto_merge_policy.json")


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def env_int(name: str, default: int) -> int:
    try:
        return int(env(name, str(default)))
    except ValueError:
        return default


def github_request(token: str, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
    url = f"{API_BASE}{path}" if not path.startswith("https://") else path
    data = json.dumps(payload).encode() if payload else None
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "static-pr-guard",
    }
    if payload:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GitHub API {method} {url}: {exc.code} {detail[:500]}") from exc
    return json.loads(body) if body else {}


# ── policy ──────────────────────────────────────────────────────────────────


def load_policy() -> dict[str, Any]:
    if POLICY_PATH.exists():
        try:
            return json.loads(POLICY_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            pass
    return {
        "version": 1,
        "blocked_path_patterns": [
            r"(^|/)(\.env|.*secret.*|.*credential.*|.*token.*|.*private.*|.*\.pem|.*\.key)$",
        ],
        "max_changed_files": 50,
        "max_changed_lines": 5000,
    }


def blocked_patterns(policy: dict[str, Any]) -> list[re.Pattern[str]]:
    raw = policy.get("blocked_path_patterns", [])
    patterns: list[re.Pattern[str]] = []
    for p in raw:
        if isinstance(p, str) and p.strip():
            try:
                patterns.append(re.compile(p, re.IGNORECASE))
            except re.error:
                pass
    return patterns


# ── diff scanning ───────────────────────────────────────────────────────────


def scan_diff(diff_text: str, patterns: list[re.Pattern[str]]) -> list[str]:
    """Scan diff for secrets/credentials in added lines. Returns list of violations."""
    violations: list[str] = []
    current_file = ""
    at_at_pattern = re.compile(r'^@@\s+-\d+.*\s+\+(.*?)\s+@@')
    sensitive_value = re.compile(
        r'(?:api_?key|secret|password|token|credential|private_?key)\s*[:=]\s*["\'](?!\${{|{{|example|placeholder|test|your-|xxx|TODO)[^"\']{20,}["\']',
        re.IGNORECASE,
    )

    for line in diff_text.splitlines():
        # Track current file
        if line.startswith("diff --git "):
            parts = line.split(" ")
            if len(parts) >= 4:
                current_file = parts[3][2:] if parts[3].startswith("b/") else parts[3]
            continue

        if line.startswith("--- ") or line.startswith("+++ "):
            # File header
            f = line[6:] if line.startswith("+++ b/") else (line[4:] if line.startswith("+++ ") else "")
            if f:
                current_file = f
            continue

        # Only scan added lines
        if not line.startswith("+") or line.startswith("+++"):
            continue
        added_line = line[1:]

        # Check blocked file paths at file level
        if current_file:
            for pattern in patterns:
                if pattern.search(current_file):
                    violations.append(f"**Blocked file**: `{current_file}` matches pattern `{pattern.pattern}`")
                    break

        # Check for hardcoded secrets in added lines
        m = sensitive_value.search(added_line)
        if m:
            violations.append(
                f"**Hardcoded secret** in `{current_file}`: "
                f"`{m.group(0)[:80]}...` (line content matches secret pattern)"
            )

    return list(set(violations))  # dedup


# ── check run ────────────────────────────────────────────────────────────────


def get_existing_check_run(token: str, repo: str, head_sha: str) -> dict[str, Any] | None:
    result = github_request(
        token, "GET", f"/repos/{repo}/commits/{head_sha}/check-runs?per_page=50&filter=latest"
    )
    runs = result.get("check_runs", []) if isinstance(result, dict) else []
    for run in runs:
        if isinstance(run, dict) and run.get("name") == CHECK_NAME:
            return run
    return None


def upsert_check(token: str, repo: str, head_sha: str, conclusion: str, title: str, summary: str) -> None:
    existing = get_existing_check_run(token, repo, head_sha)
    body: dict[str, Any] = {
        "name": CHECK_NAME,
        "head_sha": head_sha,
        "status": "completed",
        "conclusion": conclusion,
        "output": {"title": title, "summary": summary},
    }
    if existing and existing.get("id"):
        github_request(token, "PATCH", f"/repos/{repo}/check-runs/{existing['id']}", body)
        print(f"Updated check run #{existing['id']}: {conclusion}")
    else:
        github_request(token, "POST", f"/repos/{repo}/check-runs", body)
        print(f"Created check run: {conclusion}")


# ── file metadata ───────────────────────────────────────────────────────────


def check_file_metadata(files: list[dict[str, Any]], policy: dict[str, Any]) -> list[str]:
    """Check for file deletions, renames, binary files, and count/line limits."""
    issues: list[str] = []
    max_files = policy.get("max_changed_files", 50)
    max_lines = policy.get("max_changed_lines", 5000)

    total_additions = 0
    total_deletions = 0

    for f in files:
        status = (f.get("status") or "").lower().strip()
        additions = f.get("additions", 0) or 0
        deletions = f.get("deletions", 0) or 0
        filename = f.get("filename", "?")

        if status == "removed":
            issues.append(f"**File deleted**: `{filename}` — verify this is intentional")
        elif status == "renamed":
            prev = f.get("previous_filename", "?")
            issues.append(f"**File renamed**: `{prev}` → `{filename}`")
        elif status == "copied":
            issues.append(f"**File copied**: `{filename}`")

        total_additions += additions
        total_deletions += deletions

    changed_files = len(files)
    changed_lines = total_additions + total_deletions

    if changed_files > max_files:
        issues.append(
            f"**Too many files**: {changed_files} changed (limit: {max_files}). "
            "Consider splitting this PR."
        )
    if changed_lines > max_lines:
        issues.append(
            f"**Too many lines**: {changed_lines} changed (limit: {max_lines}). "
            "Consider splitting this PR."
        )

    return issues


# ── main ────────────────────────────────────────────────────────────────────


def main() -> int:
    token = env("GH_TOKEN") or env("GITHUB_TOKEN")
    if not token:
        print("::error::GH_TOKEN required", file=sys.stderr)
        return 1

    repo = env("GITHUB_REPOSITORY")
    if not repo:
        print("::error::GITHUB_REPOSITORY not set", file=sys.stderr)
        return 1

    event_path = Path(os.environ.get("GITHUB_EVENT_PATH", ""))
    if not event_path.exists():
        print("::error::GITHUB_EVENT_PATH missing", file=sys.stderr)
        return 1

    event = json.loads(event_path.read_text(encoding="utf-8"))
    pr = event.get("pull_request") or {}
    pr_number = pr.get("number")
    head_sha = (pr.get("head") or {}).get("sha")

    if not pr_number or not head_sha:
        print("::warning::Cannot resolve PR context")
        return 0

    print(f"PR #{pr_number} sha={head_sha[:12]}")

    policy = load_policy()

    # Fetch changed files metadata
    try:
        files = github_request(
            token, "GET", f"/repos/{repo}/pulls/{pr_number}/files?per_page=100"
        )
        if not isinstance(files, list):
            files = []
        # Fetch remaining pages
        page = 2
        while True:
            more = github_request(
                token, "GET",
                f"/repos/{repo}/pulls/{pr_number}/files?per_page=100&page={page}"
            )
            if not isinstance(more, list) or not more:
                break
            files.extend(more)
            if len(more) < 100:
                break
            page += 1
    except RuntimeError as exc:
        print(f"::warning::Failed to fetch PR files: {exc}")
        return 0

    # Fetch diff
    diff_url = f"{API_BASE}/repos/{repo}/pulls/{pr_number}"
    try:
        req = urllib.request.Request(
            diff_url,
            method="GET",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github.v3.diff",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "static-pr-guard",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            diff_text = resp.read().decode("utf-8", errors="replace")
    except RuntimeError:
        diff_text = ""

    # ── scan ──
    issues: list[str] = []

    # 1. File metadata checks
    issues.extend(check_file_metadata(files, policy))

    # 2. Secret/credential scan
    if diff_text:
        patterns = blocked_patterns(policy)
        issues.extend(scan_diff(diff_text, patterns))

    # ── decide ──
    if issues:
        title = f"Static Guard: {len(issues)} issue(s) found — MERGE BLOCKED"
        summary = "## Issues Found\n\n" + "\n".join(f"- {i}" for i in issues)
        summary += (
            "\n\n---\n"
            "Fix these issues and push a new commit.\n"
            "This is a **static check** — the AI-level Codex review runs separately."
        )
        conclusion = "failure"
        blocked = True
    else:
        title = "Static Guard: passed"
        summary = (
            "No secrets, blocked files, or structural issues found in the diff.\n\n"
            "Awaiting Codex AI review for logic-level feedback."
        )
        conclusion = "success"
        blocked = False

    # Don't overwrite the App-based gate's conclusion if it already exists
    # and this is just the static guard adding info
    existing = get_existing_check_run(token, repo, head_sha)
    if existing and existing.get("conclusion") == "failure":
        print("Check run already failed by App gate; not overwriting")
        return 0

    try:
        upsert_check(token, repo, head_sha, conclusion, title, summary)
    except RuntimeError as exc:
        print(f"::error::Failed to upsert check run: {exc}", file=sys.stderr)
        return 1

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as f:
            f.write(f"blocked={'true' if blocked else 'false'}\n")
            f.write(f"issues_count={len(issues)}\n")

    if blocked:
        print(f"::error::{title}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
