from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


AUTO_MERGE_LABEL = "auto-merge-ok"
MARKER_PREFIX = "<!-- codex-monthly-remediation:issue-"
ALLOWED_PREFIXES = (
    "docs/",
    "tests/",
)
ALLOWED_EXACT = {
    "README.md",
    ".github/workflows/monthly_review.yml",
    ".github/workflows/ai_review.yml",
    ".github/workflows/auto_merge_codex_pr.yml",
    ".github/workflows/codex_pr_feedback.yml",
    "scripts/run_monthly_report_bundle.py",
    "scripts/post_monthly_ai_review_issue.py",
    "scripts/render_monthly_ai_review.py",
    "scripts/post_monthly_ai_review_comment.py",
    "scripts/post_codex_remediation_issue.py",
    "scripts/evaluate_codex_pr_merge.py",
}


def _normalize_path(path: str) -> str:
    normalized = path.strip()
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def evaluate_changed_files(changed_files: list[str]) -> dict[str, Any]:
    blocked: list[str] = []
    for raw_path in changed_files:
        path = _normalize_path(raw_path)
        if not path:
            continue
        if path in ALLOWED_EXACT or any(path.startswith(prefix) for prefix in ALLOWED_PREFIXES):
            continue
        blocked.append(path)
    return {"allowed": not blocked, "blocked_files": blocked}


def _label_names(pr: dict[str, Any]) -> set[str]:
    labels = pr.get("labels", []) or []
    return {str(label.get("name", label)).strip() for label in labels if str(label.get("name", label)).strip()}


def evaluate_pr(pr: dict[str, Any]) -> dict[str, Any]:
    body = pr.get("body") or ""
    changed_files = [str(item.get("path", "")) for item in pr.get("files", [])]
    file_guard = evaluate_changed_files(changed_files)
    labels = _label_names(pr)
    has_marker = MARKER_PREFIX in body
    has_merge_label = AUTO_MERGE_LABEL in labels
    is_draft = bool(pr.get("isDraft"))
    should_merge = has_marker and has_merge_label and not is_draft and file_guard["allowed"]
    if not has_marker:
        reason = "missing_marker"
    elif not has_merge_label:
        reason = "missing_auto_merge_label"
    elif is_draft:
        reason = "draft_pr"
    elif not file_guard["allowed"]:
        reason = "blocked_files"
    else:
        reason = "ready"
    return {
        "should_merge": should_merge,
        "reason": reason,
        "blocked_files": file_guard["blocked_files"],
        "changed_file_count": len([path for path in changed_files if path.strip()]),
    }


def render_summary(pr: dict[str, Any], decision: dict[str, Any]) -> str:
    lines = [
        "## Codex Auto-Merge Gate",
        f"- PR: {pr.get('url', 'n/a')}",
        f"- Draft: `{'yes' if pr.get('isDraft') else 'no'}`",
        f"- Changed files: `{decision['changed_file_count']}`",
        f"- Blocked files: `{len(decision['blocked_files'])}`",
        f"- Decision: `{'merge' if decision['should_merge'] else 'skip'}`",
        f"- Reason: `{decision['reason']}`",
    ]
    if decision["blocked_files"]:
        lines.extend(["", "### Blocked files"])
        lines.extend(f"- `{path}`" for path in decision["blocked_files"])
    return "\n".join(lines).strip() + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate whether a Codex remediation PR may be auto-merged.")
    parser.add_argument("--pr-json", required=True, type=Path)
    parser.add_argument("--summary-file", required=True, type=Path)
    parser.add_argument("--decision-file", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    pr = json.loads(args.pr_json.read_text(encoding="utf-8"))
    decision = evaluate_pr(pr)
    args.summary_file.parent.mkdir(parents=True, exist_ok=True)
    args.summary_file.write_text(render_summary(pr, decision), encoding="utf-8")
    args.decision_file.write_text(json.dumps(decision, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"should_merge={'true' if decision['should_merge'] else 'false'}")
    print(f"reason={decision['reason']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
