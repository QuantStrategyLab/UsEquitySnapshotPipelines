from __future__ import annotations

import io
import urllib.error

from scripts.post_codex_auto_merge_preflight_comment import (
    PreflightCommentError,
    append_github_output,
    build_preflight_comment,
    find_existing_preflight_comment,
    list_issue_comments,
    main,
    marker_component,
    preflight_marker,
    read_optional_text,
    truncate_section,
    upsert_preflight_comment,
    write_output_file,
)


def test_build_preflight_comment_includes_marker_readiness_and_checklist() -> None:
    readiness = """## Codex Auto-Merge Readiness
- Ready: `no`
- Skipped: `no`

### Errors
- branch protection is not enabled for main
"""
    plan = """# Codex guarded auto-merge enablement plan

## Enablement preflight checklist

- [ ] Readiness reports `Ready: yes`.
- [ ] Rollback command is ready.

## Manual enablement steps

1. Do not skip readiness.
"""
    label_sync = """## Codex Auto-Merge Label Sync
- Ready: `yes`

### Actions
- Label `auto-merge-ok` already exists.
"""

    comment = build_preflight_comment(
        report_month="2026-06",
        readiness_text=readiness,
        label_sync_text=label_sync,
        enablement_plan_text=plan,
    )

    assert comment.startswith("<!-- codex-auto-merge-preflight:2026-06 -->")
    assert "## Guarded Auto-Merge Preflight" in comment
    assert "informational only and does not approve or enable guarded auto-merge" in comment
    assert "### Label sync snapshot" in comment
    assert "Label `auto-merge-ok` already exists." in comment
    assert "- Ready: `no`" in comment
    assert "## Enablement preflight checklist" in comment
    assert "- [ ] Rollback command is ready." in comment
    assert "## Manual enablement steps" not in comment


def test_marker_component_removes_newlines_and_unsafe_characters() -> None:
    assert marker_component(" 2026-06\nextra/value ") == "2026-06-extra-value"
    assert preflight_marker("\n") == "<!-- codex-auto-merge-preflight:unknown -->"


def test_read_optional_text_marks_missing_preflight_as_not_ready(tmp_path) -> None:
    missing = tmp_path / "missing.md"

    text = read_optional_text(missing)

    assert "Preflight file was not generated" in text
    assert "Treat guarded auto-merge as not ready" in text


def test_read_optional_text_uses_custom_missing_message(tmp_path) -> None:
    missing = tmp_path / "missing.md"

    text = read_optional_text(missing, missing_message="_Missing custom file: `{path}`._")

    assert text == f"_Missing custom file: `{missing}`._\n"


def test_write_output_file_creates_parent_directories(tmp_path) -> None:
    output = tmp_path / "nested" / "comment.md"

    write_output_file(output, "comment body")

    assert output.read_text(encoding="utf-8") == "comment body"


def test_append_github_output_writes_key_value_pairs(tmp_path, monkeypatch) -> None:
    github_output = tmp_path / "github_output.txt"
    monkeypatch.setenv("GITHUB_OUTPUT", str(github_output))

    append_github_output({"preflight_comment_file": "comment.md", "preflight_comment_action": "dry_run"})

    assert github_output.read_text(encoding="utf-8") == (
        "preflight_comment_file=comment.md\n"
        "preflight_comment_action=dry_run\n"
    )


def test_find_existing_preflight_comment_matches_marker_prefix() -> None:
    marker = preflight_marker("2026-06")

    comment_id = find_existing_preflight_comment(
        [
            {"id": 1, "body": "unrelated"},
            {"id": 2, "body": marker + "\n## Guarded Auto-Merge Preflight"},
        ],
        marker,
    )

    assert comment_id == 2


def test_upsert_preflight_comment_updates_existing_comment() -> None:
    calls: list[tuple[str, str, dict[str, object] | None]] = []
    marker = preflight_marker("2026-06")

    def fake_request(method: str, url: str, token: str, payload=None):
        calls.append((method, url, payload))
        if method == "GET":
            return [{"id": 42, "body": marker + "\nold"}]
        if method == "PATCH":
            return {"html_url": "https://example.test/comment/42"}
        raise AssertionError((method, url, payload))

    action, url = upsert_preflight_comment(
        api_url="https://api.github.com",
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        token="token",
        issue_number=12,
        body=marker + "\nnew",
        marker=marker,
        request_fn=fake_request,
    )

    assert action == "updated"
    assert url == "https://example.test/comment/42"
    assert calls[1] == (
        "PATCH",
        "https://api.github.com/repos/QuantStrategyLab/UsEquitySnapshotPipelines/issues/comments/42",
        {"body": marker + "\nnew"},
    )


def test_upsert_preflight_comment_creates_missing_comment() -> None:
    calls: list[tuple[str, str, dict[str, object] | None]] = []
    marker = preflight_marker("2026-06")

    def fake_request(method: str, url: str, token: str, payload=None):
        calls.append((method, url, payload))
        if method == "GET":
            return []
        if method == "POST":
            return {"html_url": "https://example.test/comment/43"}
        raise AssertionError((method, url, payload))

    action, url = upsert_preflight_comment(
        api_url="https://api.github.com",
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        token="token",
        issue_number=12,
        body=marker + "\nnew",
        marker=marker,
        request_fn=fake_request,
    )

    assert action == "created"
    assert url == "https://example.test/comment/43"
    assert calls[1] == (
        "POST",
        "https://api.github.com/repos/QuantStrategyLab/UsEquitySnapshotPipelines/issues/12/comments",
        {"body": marker + "\nnew"},
    )


def test_list_issue_comments_reads_paginated_comments() -> None:
    calls: list[str] = []

    def fake_request(method: str, url: str, token: str, payload=None):
        calls.append(url)
        if url.endswith("page=1"):
            return [{"id": index, "body": "first page"} for index in range(100)]
        if url.endswith("page=2"):
            return [{"id": 101, "body": "second page"}]
        raise AssertionError(url)

    comments = list_issue_comments(
        api_url="https://api.github.com",
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        token="token",
        issue_number=12,
        request_fn=fake_request,
    )

    assert len(comments) == 101
    assert calls == [
        "https://api.github.com/repos/QuantStrategyLab/UsEquitySnapshotPipelines/issues/12/comments?per_page=100&page=1",
        "https://api.github.com/repos/QuantStrategyLab/UsEquitySnapshotPipelines/issues/12/comments?per_page=100&page=2",
    ]


def test_list_issue_comments_fails_closed_on_invalid_response() -> None:
    def fake_request(method: str, url: str, token: str, payload=None):
        return {"message": "unexpected"}

    try:
        list_issue_comments(
            api_url="https://api.github.com",
            repo="QuantStrategyLab/UsEquitySnapshotPipelines",
            token="token",
            issue_number=12,
            request_fn=fake_request,
        )
    except PreflightCommentError as exc:
        assert "issue comments response is invalid for issue #12" in str(exc)
    else:
        raise AssertionError("PreflightCommentError was not raised")


def test_truncate_section_adds_artifact_hint() -> None:
    truncated = truncate_section("a" * 12_001, max_chars=12_000)

    assert len(truncated) > 12_000
    assert "Section truncated" in truncated


def test_main_dry_run_writes_comment_without_github_token(tmp_path, monkeypatch, capsys) -> None:
    readiness = tmp_path / "readiness.md"
    plan = tmp_path / "plan.md"
    output = tmp_path / "comment.md"
    readiness.write_text("## Codex Auto-Merge Readiness\n- Ready: `no`\n", encoding="utf-8")
    plan.write_text("## Enablement preflight checklist\n\n- [ ] Ready.\n", encoding="utf-8")
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.setattr(
        "sys.argv",
        [
            "post_codex_auto_merge_preflight_comment.py",
            "--repo",
            "QuantStrategyLab/UsEquitySnapshotPipelines",
            "--issue-number",
            "12",
            "--report-month",
            "2026-06",
            "--readiness-file",
            str(readiness),
            "--enablement-plan-file",
            str(plan),
            "--output-file",
            str(output),
            "--dry-run",
        ],
    )

    assert main() == 0

    captured = capsys.readouterr()
    assert "preflight_comment_file=" in captured.out
    assert "preflight_comment_action=dry_run" in captured.out
    rendered = output.read_text(encoding="utf-8")
    assert rendered.startswith("<!-- codex-auto-merge-preflight:2026-06 -->")
    assert "informational only" in rendered


def test_main_writes_output_file_before_missing_token_failure(tmp_path, monkeypatch, capsys) -> None:
    readiness = tmp_path / "readiness.md"
    plan = tmp_path / "plan.md"
    output = tmp_path / "comment.md"
    readiness.write_text("## Codex Auto-Merge Readiness\n- Ready: `no`\n", encoding="utf-8")
    plan.write_text("## Enablement preflight checklist\n\n- [ ] Ready.\n", encoding="utf-8")
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.setattr(
        "sys.argv",
        [
            "post_codex_auto_merge_preflight_comment.py",
            "--repo",
            "QuantStrategyLab/UsEquitySnapshotPipelines",
            "--issue-number",
            "12",
            "--report-month",
            "2026-06",
            "--readiness-file",
            str(readiness),
            "--enablement-plan-file",
            str(plan),
            "--output-file",
            str(output),
        ],
    )

    assert main() == 1

    captured = capsys.readouterr()
    assert "preflight_comment_file=" in captured.out
    assert "GITHUB_TOKEN is required" in captured.err
    assert output.read_text(encoding="utf-8").startswith("<!-- codex-auto-merge-preflight:2026-06 -->")


def test_main_keeps_output_metadata_when_comment_upsert_fails(tmp_path, monkeypatch, capsys) -> None:
    readiness = tmp_path / "readiness.md"
    plan = tmp_path / "plan.md"
    output = tmp_path / "comment.md"
    github_output = tmp_path / "github_output.txt"
    readiness.write_text("## Codex Auto-Merge Readiness\n- Ready: `no`\n", encoding="utf-8")
    plan.write_text("## Enablement preflight checklist\n\n- [ ] Ready.\n", encoding="utf-8")
    monkeypatch.setenv("GITHUB_TOKEN", "token")
    monkeypatch.setenv("GITHUB_OUTPUT", str(github_output))
    monkeypatch.setattr(
        "scripts.post_codex_auto_merge_preflight_comment.upsert_preflight_comment",
        lambda **kwargs: (_ for _ in ()).throw(PreflightCommentError("invalid response")),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "post_codex_auto_merge_preflight_comment.py",
            "--repo",
            "QuantStrategyLab/UsEquitySnapshotPipelines",
            "--issue-number",
            "12",
            "--report-month",
            "2026-06",
            "--readiness-file",
            str(readiness),
            "--enablement-plan-file",
            str(plan),
            "--output-file",
            str(output),
        ],
    )

    assert main() == 1

    captured = capsys.readouterr()
    assert "Preflight comment failed: invalid response" in captured.err
    assert f"preflight_comment_file={output}" in github_output.read_text(encoding="utf-8")
    assert output.read_text(encoding="utf-8").startswith("<!-- codex-auto-merge-preflight:2026-06 -->")


def test_main_keeps_output_metadata_when_github_http_error_occurs(tmp_path, monkeypatch, capsys) -> None:
    readiness = tmp_path / "readiness.md"
    plan = tmp_path / "plan.md"
    output = tmp_path / "comment.md"
    github_output = tmp_path / "github_output.txt"
    readiness.write_text("## Codex Auto-Merge Readiness\n- Ready: `no`\n", encoding="utf-8")
    plan.write_text("## Enablement preflight checklist\n\n- [ ] Ready.\n", encoding="utf-8")
    monkeypatch.setenv("GITHUB_TOKEN", "token")
    monkeypatch.setenv("GITHUB_OUTPUT", str(github_output))
    monkeypatch.setattr(
        "scripts.post_codex_auto_merge_preflight_comment.upsert_preflight_comment",
        lambda **kwargs: (_ for _ in ()).throw(
            urllib.error.HTTPError(
                url="https://api.github.com/repos/example/issues/12/comments",
                code=403,
                msg="Forbidden",
                hdrs=None,
                fp=io.BytesIO(b"forbidden"),
            )
        ),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "post_codex_auto_merge_preflight_comment.py",
            "--repo",
            "QuantStrategyLab/UsEquitySnapshotPipelines",
            "--issue-number",
            "12",
            "--report-month",
            "2026-06",
            "--readiness-file",
            str(readiness),
            "--enablement-plan-file",
            str(plan),
            "--output-file",
            str(output),
        ],
    )

    assert main() == 1

    captured = capsys.readouterr()
    assert "GitHub API request failed: 403 forbidden" in captured.err
    assert f"preflight_comment_file={output}" in github_output.read_text(encoding="utf-8")
    assert output.read_text(encoding="utf-8").startswith("<!-- codex-auto-merge-preflight:2026-06 -->")


def test_main_keeps_output_metadata_when_github_url_error_occurs(tmp_path, monkeypatch, capsys) -> None:
    readiness = tmp_path / "readiness.md"
    plan = tmp_path / "plan.md"
    output = tmp_path / "comment.md"
    github_output = tmp_path / "github_output.txt"
    readiness.write_text("## Codex Auto-Merge Readiness\n- Ready: `no`\n", encoding="utf-8")
    plan.write_text("## Enablement preflight checklist\n\n- [ ] Ready.\n", encoding="utf-8")
    monkeypatch.setenv("GITHUB_TOKEN", "token")
    monkeypatch.setenv("GITHUB_OUTPUT", str(github_output))
    monkeypatch.setattr(
        "scripts.post_codex_auto_merge_preflight_comment.upsert_preflight_comment",
        lambda **kwargs: (_ for _ in ()).throw(urllib.error.URLError("network unreachable")),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "post_codex_auto_merge_preflight_comment.py",
            "--repo",
            "QuantStrategyLab/UsEquitySnapshotPipelines",
            "--issue-number",
            "12",
            "--report-month",
            "2026-06",
            "--readiness-file",
            str(readiness),
            "--enablement-plan-file",
            str(plan),
            "--output-file",
            str(output),
        ],
    )

    assert main() == 1

    captured = capsys.readouterr()
    assert "GitHub API request failed: network unreachable" in captured.err
    assert f"preflight_comment_file={output}" in github_output.read_text(encoding="utf-8")
    assert output.read_text(encoding="utf-8").startswith("<!-- codex-auto-merge-preflight:2026-06 -->")
