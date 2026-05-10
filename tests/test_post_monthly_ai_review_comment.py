from __future__ import annotations

from scripts.post_monthly_ai_review_comment import COMMENT_MARKER, build_comment_body


def test_build_comment_body_includes_marker_and_run_link() -> None:
    body = build_comment_body("## English\nReview", "https://example.com/run")

    assert body.startswith(COMMENT_MARKER)
    assert "AI Monthly Review" in body
    assert "https://example.com/run" in body
