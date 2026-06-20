from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import scripts.sync_codex_auto_merge_labels as label_sync
from scripts.check_codex_auto_merge_readiness import GitHubApiError
from scripts.sync_codex_auto_merge_labels import render_summary, sync_codex_auto_merge_labels


def write_policy(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "auto_merge_label": "auto-merge-ok",
                "human_review_label": "human-review-required",
                "monthly_marker_prefix": "<!-- codex-monthly-remediation:issue-",
                "max_changed_files": 20,
                "max_changed_lines": 1200,
                "blocked_path_patterns": [".*secret.*"],
                "risk_policy": {
                    "low": {"prefixes": ["docs/"], "exact": ["README.md"], "reason": "low"},
                    "medium": {
                        "exact": ["scripts/build_monthly_live_strategy_health_reports.py"],
                        "reason": "medium",
                    },
                    "high": {"reason": "high"},
                },
            }
        ),
        encoding="utf-8",
    )


def test_sync_skips_without_remote_calls_when_auto_merge_is_false(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    write_policy(policy_path)

    def fail_if_called(*args, **kwargs):
        raise AssertionError("label sync should not call GitHub when auto-merge is false")

    monkeypatch.setattr(label_sync, "github_request", fail_if_called)

    decision = sync_codex_auto_merge_labels(
        auto_merge=False,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        token="",
        policy_path=policy_path,
    )

    assert decision["ready"]
    assert decision["skipped"]
    assert decision["errors"] == []
    assert "label sync skipped" in decision["actions"][0]


def test_sync_creates_missing_guarded_labels(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    write_policy(policy_path)
    calls: list[tuple[str, str, dict[str, str] | None]] = []

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        calls.append((method, url, payload))
        assert token == "token"
        if method == "GET" and (url.endswith("/labels/auto-merge-ok") or url.endswith("/labels/human-review-required")):
            raise GitHubApiError(method, url, 404, '{"message":"Not Found"}')
        if method == "POST" and url.endswith("/labels"):
            return {"name": payload["name"]}
        raise AssertionError(url)

    monkeypatch.setattr(label_sync, "github_request", fake_github_request)

    decision = sync_codex_auto_merge_labels(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        token="token",
        policy_path=policy_path,
    )

    assert decision["ready"]
    assert decision["errors"] == []
    assert decision["actions"] == ["Created label `auto-merge-ok`.", "Created label `human-review-required`."]
    created_payloads = [payload for method, _, payload in calls if method == "POST"]
    assert created_payloads == [
        {
            "name": "auto-merge-ok",
            "color": "0e8a16",
            "description": "Guarded Codex remediation auto-merge may proceed after source CI and merge guard pass.",
        },
        {
            "name": "human-review-required",
            "color": "d93f0b",
            "description": "Codex remediation PR requires human review before merge.",
        },
    ]


def test_sync_does_not_update_existing_labels(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    write_policy(policy_path)
    methods: list[str] = []

    def fake_github_request(method: str, url: str, token: str, payload=None, *, timeout=30):
        methods.append(method)
        if method == "GET" and url.endswith(("/labels/auto-merge-ok", "/labels/human-review-required")):
            return {"name": url.rsplit("/", 1)[-1]}
        raise AssertionError(f"unexpected {method} {url}")

    monkeypatch.setattr(label_sync, "github_request", fake_github_request)

    decision = sync_codex_auto_merge_labels(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        token="token",
        policy_path=policy_path,
    )

    assert decision["ready"]
    assert methods == ["GET", "GET"]
    assert decision["actions"] == ["Label `auto-merge-ok` already exists.", "Label `human-review-required` already exists."]


def test_sync_reports_missing_token_after_policy_load(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    write_policy(policy_path)

    def fail_if_called(*args, **kwargs):
        raise AssertionError("label sync should not call GitHub without a token")

    monkeypatch.setattr(label_sync, "github_request", fail_if_called)

    decision = sync_codex_auto_merge_labels(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        token="",
        policy_path=policy_path,
    )

    assert not decision["ready"]
    assert decision["auto_merge_label"] == "auto-merge-ok"
    assert decision["human_review_label"] == "human-review-required"
    assert decision["errors"] == ["GITHUB_TOKEN is required to sync guarded auto-merge labels"]


def test_sync_fails_closed_when_policy_labels_match(tmp_path: Path, monkeypatch) -> None:
    policy_path = tmp_path / "policy.json"
    write_policy(policy_path)
    payload = json.loads(policy_path.read_text(encoding="utf-8"))
    payload["human_review_label"] = payload["auto_merge_label"]
    policy_path.write_text(json.dumps(payload), encoding="utf-8")

    def fail_if_called(*args, **kwargs):
        raise AssertionError("label sync should not call GitHub when policy labels collide")

    monkeypatch.setattr(label_sync, "github_request", fail_if_called)

    decision = sync_codex_auto_merge_labels(
        auto_merge=True,
        repo="QuantStrategyLab/UsEquitySnapshotPipelines",
        token="token",
        policy_path=policy_path,
    )

    assert not decision["ready"]
    assert decision["errors"] == [
        "auto-merge and human-review labels must be distinct before enabling auto-merge"
    ]


def test_render_summary_includes_actions_and_errors() -> None:
    summary = render_summary(
        {
            "ready": False,
            "skipped": False,
            "auto_merge_label": "auto-merge-ok",
            "human_review_label": "human-review-required",
            "actions": ["Label `auto-merge-ok` already exists."],
            "errors": ["label `human-review-required` sync failed: boom"],
        }
    )

    assert "## Codex Auto-Merge Label Sync" in summary
    assert "- Ready: `no`" in summary
    assert "- Auto-merge label: `auto-merge-ok`" in summary
    assert "### Actions" in summary
    assert "### Errors" in summary


def test_cli_writes_summary_when_skipped(tmp_path: Path) -> None:
    output = tmp_path / "label_sync.md"
    env = dict(os.environ)
    env.pop("GITHUB_TOKEN", None)

    result = subprocess.run(
        [
            sys.executable,
            "scripts/sync_codex_auto_merge_labels.py",
            "--repo",
            "QuantStrategyLab/UsEquitySnapshotPipelines",
            "--auto-merge",
            "false",
            "--summary-file",
            str(output),
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "ready=true" in result.stdout
    assert output.read_text(encoding="utf-8").startswith("## Codex Auto-Merge Label Sync")
