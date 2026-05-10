from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def extract_latest_assistant_text(execution_log: list[dict[str, Any]]) -> str:
    for turn in reversed(execution_log):
        if turn.get("type") != "assistant":
            continue
        content_items = turn.get("message", {}).get("content", [])
        text_parts = [
            item.get("text", "").strip()
            for item in content_items
            if item.get("type") == "text" and item.get("text", "").strip()
        ]
        if text_parts:
            return "\n\n".join(text_parts).strip()
    raise ValueError("No assistant review text found in execution log")


def build_full_review_markdown(primary_review_text: str, *, primary_title: str = "Claude Primary Review") -> str:
    normalized = primary_review_text.strip()
    duplicate_title = f"## {primary_title}"
    if normalized.startswith(duplicate_title):
        normalized = normalized[len(duplicate_title) :].lstrip()
    return "\n".join([f"## {primary_title}", "", normalized]).strip() + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render markdown for the monthly AI review.")
    parser.add_argument("--execution-file", required=True, type=Path)
    parser.add_argument("--output-file", required=True, type=Path)
    parser.add_argument("--primary-title", default="Claude Primary Review")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    execution_log = json.loads(args.execution_file.read_text(encoding="utf-8"))
    markdown = build_full_review_markdown(
        extract_latest_assistant_text(execution_log),
        primary_title=args.primary_title,
    )
    args.output_file.parent.mkdir(parents=True, exist_ok=True)
    args.output_file.write_text(markdown, encoding="utf-8")
    print(f"review_markdown={args.output_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
