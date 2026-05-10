from __future__ import annotations

from scripts.render_monthly_ai_review import build_full_review_markdown, extract_latest_assistant_text


def test_extract_latest_assistant_text_uses_last_assistant_message() -> None:
    execution_log = [
        {"type": "assistant", "message": {"content": [{"type": "text", "text": "old"}]}},
        {"type": "user", "message": {"content": [{"type": "text", "text": "ignored"}]}},
        {"type": "assistant", "message": {"content": [{"type": "text", "text": "final"}]}},
    ]

    assert extract_latest_assistant_text(execution_log) == "final"


def test_build_full_review_markdown_deduplicates_primary_title() -> None:
    markdown = build_full_review_markdown("## Claude Primary Review\n\n## English\nReview")

    assert markdown.count("## Claude Primary Review") == 1
    assert "## English" in markdown
