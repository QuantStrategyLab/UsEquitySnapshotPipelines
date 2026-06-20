from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from scripts.evaluate_codex_pr_merge import (
    AUTO_MERGE_LABEL,
    DEFAULT_POLICY,
    HUMAN_REVIEW_LABEL,
    evaluate_changed_files,
    evaluate_pr,
    load_policy,
)


def _load_bridge_default_policy(bridge_script: Path) -> dict[str, object]:
    tree = ast.parse(bridge_script.read_text(encoding="utf-8"))
    constants: dict[str, object] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            names = [target.id for target in node.targets if isinstance(target, ast.Name)]
            if any(name in names for name in {
                "GUARDED_AUTO_MERGE_LABEL",
                "HUMAN_REVIEW_LABEL",
                "GUARDED_AUTO_MERGE_LOW_RISK_PREFIXES",
                "GUARDED_AUTO_MERGE_LOW_RISK_EXACT",
                "GUARDED_AUTO_MERGE_MEDIUM_RISK_EXACT",
                "DEFAULT_GUARDED_AUTO_MERGE_MAX_CHANGED_FILES",
                "DEFAULT_GUARDED_AUTO_MERGE_MAX_CHANGED_LINES",
            }):
                constants.update({name: ast.literal_eval(node.value) for name in names})
    required = {
        "GUARDED_AUTO_MERGE_LABEL",
        "HUMAN_REVIEW_LABEL",
        "GUARDED_AUTO_MERGE_LOW_RISK_PREFIXES",
        "GUARDED_AUTO_MERGE_LOW_RISK_EXACT",
        "GUARDED_AUTO_MERGE_MEDIUM_RISK_EXACT",
        "DEFAULT_GUARDED_AUTO_MERGE_MAX_CHANGED_FILES",
        "DEFAULT_GUARDED_AUTO_MERGE_MAX_CHANGED_LINES",
    }
    missing = required - constants.keys()
    if missing:
        raise AssertionError(f"Bridge policy constants not found: {sorted(missing)}")
    return {
        "version": 1,
        "auto_merge_label": constants["GUARDED_AUTO_MERGE_LABEL"],
        "human_review_label": constants["HUMAN_REVIEW_LABEL"],
        "monthly_marker_prefix": "<!-- codex-monthly-remediation:issue-",
        "max_changed_files": constants["DEFAULT_GUARDED_AUTO_MERGE_MAX_CHANGED_FILES"],
        "max_changed_lines": constants["DEFAULT_GUARDED_AUTO_MERGE_MAX_CHANGED_LINES"],
        "blocked_path_patterns": [
            r"(^|/)(\.env|.*secret.*|.*credential.*|.*token.*|.*private.*|.*\.pem|.*\.key)$",
        ],
        "risk_policy": {
            "low": {
                "prefixes": list(constants["GUARDED_AUTO_MERGE_LOW_RISK_PREFIXES"]),
                "exact": sorted(constants["GUARDED_AUTO_MERGE_LOW_RISK_EXACT"]),
                "reason": "docs/tests/readme-only monthly-review surface",
            },
            "medium": {
                "exact": sorted(constants["GUARDED_AUTO_MERGE_MEDIUM_RISK_EXACT"]),
                "reason": "monthly-review evidence/reporting helper changed",
            },
            "high": {"reason": "blocked/high-risk files require human review"},
        },
    }


def _normalized_policy(policy: dict[str, object]) -> dict[str, object]:
    normalized = dict(policy)
    normalized["blocked_path_patterns"] = sorted(policy.get("blocked_path_patterns", []))
    risk_policy = dict(policy["risk_policy"])
    low = dict(risk_policy["low"])
    medium = dict(risk_policy["medium"])
    low["prefixes"] = sorted(low.get("prefixes", []))
    low["exact"] = sorted(low.get("exact", []))
    medium["exact"] = sorted(medium.get("exact", []))
    risk_policy["low"] = low
    risk_policy["medium"] = medium
    normalized["risk_policy"] = risk_policy
    return normalized


def test_auto_merge_policy_file_is_machine_readable() -> None:
    policy = load_policy(Path(".github/codex_auto_merge_policy.json"))
    risk_policy = policy["risk_policy"]

    assert policy["version"] == 1
    assert risk_policy["low"]["prefixes"] == ["docs/", "tests/"]
    assert "README.zh-CN.md" in risk_policy["low"]["exact"]
    assert "scripts/build_monthly_live_strategy_health_reports.py" in risk_policy["medium"]["exact"]
    assert "scripts/run_monthly_report_bundle.py" in risk_policy["medium"]["exact"]
    assert "scripts/plan_codex_auto_merge_enablement.py" in risk_policy["medium"]["exact"]
    assert "scripts/post_codex_auto_merge_preflight_comment.py" in risk_policy["medium"]["exact"]
    assert "scripts/evaluate_codex_pr_merge.py" not in risk_policy["medium"]["exact"]
    assert "scripts/post_codex_auto_merge_decision_comment.py" not in risk_policy["medium"]["exact"]
    assert "scripts/sync_codex_auto_merge_labels.py" not in risk_policy["medium"]["exact"]
    assert ".github/workflows/auto_merge_codex_pr.yml" not in risk_policy["medium"]["exact"]
    assert policy["auto_merge_label"] == "auto-merge-ok"
    assert policy["human_review_label"] == "human-review-required"
    assert policy["monthly_marker_prefix"] == "<!-- codex-monthly-remediation:issue-"
    assert policy["max_changed_files"] == 20
    assert policy["max_changed_lines"] == 1200
    assert policy["blocked_path_patterns"]
    assert risk_policy["high"]["reason"] == "blocked/high-risk files require human review"
    assert risk_policy["medium"]["exact"] == DEFAULT_POLICY["risk_policy"]["medium"]["exact"]


def test_auto_merge_policy_matches_local_bridge_default_when_available() -> None:
    bridge_script = Path(__file__).resolve().parents[2] / "CodexAuditBridge" / "scripts/run_monthly_codex_audit.py"
    if not bridge_script.exists():
        pytest.skip("local CodexAuditBridge checkout is not available")

    policy = load_policy(Path(".github/codex_auto_merge_policy.json"))
    bridge_policy = _load_bridge_default_policy(bridge_script)

    assert _normalized_policy(bridge_policy) == _normalized_policy(policy)


def test_evaluate_changed_files_allows_only_monthly_review_surface() -> None:
    allowed = evaluate_changed_files(
        [
            "README.md",
            "README.zh-CN.md",
            "docs/operator_runbook.md",
            "tests/test_monthly_report_bundle.py",
            "scripts/build_monthly_live_strategy_health_reports.py",
            "scripts/run_monthly_report_bundle.py",
            "scripts/plan_codex_auto_merge_enablement.py",
            "scripts/post_codex_auto_merge_preflight_comment.py",
        ]
    )
    blocked = evaluate_changed_files(
        [
            "src/us_equity_snapshot_pipelines/contracts.py",
            "pyproject.toml",
            ".github/workflows/auto_merge_codex_pr.yml",
            "scripts/evaluate_codex_pr_merge.py",
            "scripts/check_codex_auto_merge_readiness.py",
            "scripts/post_codex_auto_merge_decision_comment.py",
            "scripts/sync_codex_auto_merge_labels.py",
        ]
    )

    assert allowed["allowed"]
    assert allowed["risk_level"] == "medium"
    assert allowed["medium_risk_files"] == [
        "scripts/build_monthly_live_strategy_health_reports.py",
        "scripts/run_monthly_report_bundle.py",
        "scripts/plan_codex_auto_merge_enablement.py",
        "scripts/post_codex_auto_merge_preflight_comment.py",
    ]
    assert not blocked["allowed"]
    assert blocked["risk_level"] == "high"
    assert blocked["blocked_files"] == [
        "src/us_equity_snapshot_pipelines/contracts.py",
        "pyproject.toml",
        ".github/workflows/auto_merge_codex_pr.yml",
        "scripts/evaluate_codex_pr_merge.py",
        "scripts/check_codex_auto_merge_readiness.py",
        "scripts/post_codex_auto_merge_decision_comment.py",
        "scripts/sync_codex_auto_merge_labels.py",
    ]


def test_evaluate_changed_files_classifies_docs_tests_as_low_risk() -> None:
    decision = evaluate_changed_files(
        [
            "./README.zh-CN.md",
            "docs/operator_runbook.md",
            "tests/test_monthly_report_bundle.py",
        ]
    )

    assert decision["allowed"]
    assert decision["risk_level"] == "low"
    assert decision["blocked_files"] == []
    assert decision["risk_reasons"] == ["docs/tests/readme-only monthly-review surface"]


def test_evaluate_changed_files_blocks_secret_like_paths_before_low_risk_prefixes() -> None:
    decision = evaluate_changed_files(
        [
            "docs/operator-token.md",
            "tests/private.key",
            "README.md",
        ]
    )

    assert not decision["allowed"]
    assert decision["risk_level"] == "high"
    assert decision["blocked_files"] == ["docs/operator-token.md", "tests/private.key"]


def test_evaluate_changed_files_blocks_oversized_low_risk_surface() -> None:
    paths = [f"docs/review-{index}.md" for index in range(21)]

    decision = evaluate_changed_files(paths)

    assert not decision["allowed"]
    assert decision["risk_level"] == "high"
    assert decision["blocked_files"] == paths
    assert decision["risk_reasons"] == [
        "changed file count exceeds auto-merge limit requires human review: 21 > 20"
    ]


def test_evaluate_changed_files_blocks_large_low_risk_diff() -> None:
    decision = evaluate_changed_files(["docs/review.md"], additions=1000, deletions=201)

    assert not decision["allowed"]
    assert decision["risk_level"] == "high"
    assert decision["blocked_files"] == ["docs/review.md"]
    assert decision["changed_lines"] == 1201
    assert decision["risk_reasons"] == [
        "changed line count exceeds auto-merge limit requires human review: 1201 > 1200"
    ]


def test_evaluate_changed_files_fails_closed_on_invalid_policy_file(tmp_path: Path) -> None:
    policy_path = tmp_path / "codex_auto_merge_policy.json"
    policy_path.write_text("{not-json", encoding="utf-8")

    policy = load_policy(policy_path)
    decision = evaluate_changed_files(["README.md"], policy=policy)

    assert not decision["allowed"]
    assert decision["risk_level"] == "high"
    assert decision["blocked_files"] == ["README.md"]
    assert decision["risk_reasons"] == ["invalid auto-merge policy requires human review"]


def test_evaluate_pr_invalid_policy_reports_primary_policy_error_only(tmp_path: Path) -> None:
    policy_path = tmp_path / "codex_auto_merge_policy.json"
    policy_path.write_text("{not-json", encoding="utf-8")
    policy = load_policy(policy_path)

    decision = evaluate_pr(
        {
            "isDraft": False,
            "body": "<!-- codex-monthly-remediation:issue-12 -->",
            "labels": [{"name": AUTO_MERGE_LABEL}],
            "files": [{"path": "README.md"}],
        },
        policy=policy,
    )

    assert not decision["should_merge"]
    assert decision["reason"] == "policy_errors"
    assert decision["policy_errors"] == ["invalid auto-merge policy requires human review"]
    assert decision["risk_reasons"] == ["invalid auto-merge policy requires human review"]


def test_evaluate_changed_files_fails_closed_when_existing_policy_schema_is_incomplete(tmp_path: Path) -> None:
    policy_path = tmp_path / "codex_auto_merge_policy.json"
    policy_path.write_text("{}", encoding="utf-8")

    policy = load_policy(policy_path)
    decision = evaluate_changed_files(["README.md"], policy=policy)

    assert not decision["allowed"]
    assert decision["blocked_files"] == ["README.md"]
    assert decision["risk_reasons"] == ["invalid auto-merge policy schema requires human review"]


def test_evaluate_changed_files_fails_closed_when_policy_labels_match(tmp_path: Path) -> None:
    policy_path = tmp_path / "codex_auto_merge_policy.json"
    payload = json.loads(json.dumps(DEFAULT_POLICY))
    payload["human_review_label"] = payload["auto_merge_label"]
    policy_path.write_text(json.dumps(payload), encoding="utf-8")

    policy = load_policy(policy_path)
    decision = evaluate_changed_files(["README.md"], policy=policy)

    assert not decision["allowed"]
    assert decision["blocked_files"] == ["README.md"]
    assert decision["risk_reasons"] == [
        "auto-merge and human-review labels must be distinct requires human review"
    ]


def test_evaluate_changed_files_fails_closed_on_unsupported_policy_version(tmp_path: Path) -> None:
    policy_path = tmp_path / "codex_auto_merge_policy.json"
    policy_path.write_text(
        """
{
  "version": 2,
  "auto_merge_label": "auto-merge-ok",
  "human_review_label": "human-review-required",
  "monthly_marker_prefix": "<!-- codex-monthly-remediation:issue-",
  "max_changed_files": 20,
  "max_changed_lines": 1200,
  "blocked_path_patterns": [".*secret.*"],
  "risk_policy": {
    "low": {"prefixes": ["docs/"], "exact": [], "reason": "low"},
    "medium": {"exact": [], "reason": "medium"},
    "high": {"reason": "high"}
  }
}
""",
        encoding="utf-8",
    )

    policy = load_policy(policy_path)
    decision = evaluate_changed_files(["docs/operator_runbook.md"], policy=policy)

    assert not decision["allowed"]
    assert decision["blocked_files"] == ["docs/operator_runbook.md"]
    assert decision["risk_reasons"] == ["unsupported auto-merge policy version requires human review"]


def test_evaluate_changed_files_fails_closed_when_policy_allows_control_plane_exact_path(tmp_path: Path) -> None:
    policy_path = tmp_path / "codex_auto_merge_policy.json"
    policy_path.write_text(
        """
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
    "medium": {"exact": ["scripts/evaluate_codex_pr_merge.py"], "reason": "medium"},
    "high": {"reason": "high"}
  }
}
""",
        encoding="utf-8",
    )

    policy = load_policy(policy_path)
    decision = evaluate_changed_files(["README.md"], policy=policy)

    assert not decision["allowed"]
    assert decision["blocked_files"] == ["README.md"]
    assert decision["risk_reasons"] == [
        "auto-merge policy must keep control-plane paths high-risk: scripts/evaluate_codex_pr_merge.py"
    ]


def test_evaluate_changed_files_fails_closed_when_policy_allows_control_plane_prefix(tmp_path: Path) -> None:
    policy_path = tmp_path / "codex_auto_merge_policy.json"
    policy_path.write_text(
        """
{
  "version": 1,
  "auto_merge_label": "auto-merge-ok",
  "human_review_label": "human-review-required",
  "monthly_marker_prefix": "<!-- codex-monthly-remediation:issue-",
  "max_changed_files": 20,
  "max_changed_lines": 1200,
  "blocked_path_patterns": [".*secret.*"],
  "risk_policy": {
    "low": {"prefixes": [".github/"], "exact": ["README.md"], "reason": "low"},
    "medium": {"exact": [], "reason": "medium"},
    "high": {"reason": "high"}
  }
}
""",
        encoding="utf-8",
    )

    policy = load_policy(policy_path)
    decision = evaluate_changed_files(["README.md"], policy=policy)

    assert not decision["allowed"]
    assert decision["blocked_files"] == ["README.md"]
    assert decision["risk_reasons"] == [
        "auto-merge policy must keep control-plane paths high-risk: "
        ".github/codex_auto_merge_policy.json, .github/workflows/*"
    ]


def test_evaluate_changed_files_fails_closed_on_invalid_blocked_regex() -> None:
    decision = evaluate_changed_files(
        ["docs/operator_runbook.md"],
        policy={
            "blocked_path_patterns": ["["],
            "risk_policy": {
                "low": {
                    "prefixes": ["docs/"],
                    "exact": [],
                    "reason": "low",
                },
                "medium": {"exact": [], "reason": "medium"},
                "high": {"reason": "high"},
            },
        },
    )

    assert not decision["allowed"]
    assert decision["blocked_files"] == ["docs/operator_runbook.md"]
    assert decision["risk_reasons"] == ["invalid blocked_path_patterns regex requires human review"]


def test_evaluate_changed_files_fails_closed_on_malformed_policy_lists() -> None:
    decision = evaluate_changed_files(
        ["data/output/report.json"],
        policy={
            "risk_policy": {
                "low": {
                    "prefixes": "docs/",
                    "exact": [],
                    "reason": "low",
                },
                "medium": {"exact": [], "reason": "medium"},
                "high": {"reason": "high"},
            },
        },
    )

    assert not decision["allowed"]
    assert decision["blocked_files"] == ["data/output/report.json"]
    assert decision["risk_reasons"] == ["invalid risk_policy.low.prefixes list requires human review"]


def test_evaluate_pr_requires_marker_label_non_draft_and_allowed_files() -> None:
    pr = {
        "isDraft": False,
        "body": "<!-- codex-monthly-remediation:issue-12 -->",
        "url": "https://github.com/example/repo/pull/1",
        "labels": [{"name": AUTO_MERGE_LABEL}],
        "files": [{"path": "docs/operator_runbook.md", "status": "modified"}],
        "changedFiles": 1,
        "additions": 12,
        "deletions": 3,
        "reviewDecision": None,
        "baseRefName": "main",
        "headRefName": "codex/monthly-review-issue-12",
        "headRepositoryOwner": {"login": "QuantStrategyLab"},
        "headRepository": {"nameWithOwner": "QuantStrategyLab/UsEquitySnapshotPipelines"},
        "isCrossRepository": False,
    }

    decision = evaluate_pr(
        pr,
        expected_base_ref="main",
        expected_head_ref="codex/monthly-review-issue-12",
        expected_head_owner="QuantStrategyLab",
        expected_head_repository="QuantStrategyLab/UsEquitySnapshotPipelines",
        require_same_repository=True,
    )

    assert decision["should_merge"]
    assert decision["reason"] == "ready"
    assert decision["risk_level"] == "low"
    assert not decision["has_human_review_label"]
    assert decision["changed_lines"] == 15
    assert decision["review_decision"] == ""


def test_evaluate_pr_human_review_label_vetoes_auto_merge() -> None:
    pr = {
        "isDraft": False,
        "body": "<!-- codex-monthly-remediation:issue-12 -->",
        "url": "https://github.com/example/repo/pull/1",
        "labels": [{"name": AUTO_MERGE_LABEL}, {"name": HUMAN_REVIEW_LABEL}],
        "files": [{"path": "docs/operator_runbook.md"}],
        "changedFiles": 1,
        "baseRefName": "main",
        "headRefName": "codex/monthly-review-issue-12",
        "headRepositoryOwner": {"login": "QuantStrategyLab"},
        "headRepository": {"nameWithOwner": "QuantStrategyLab/UsEquitySnapshotPipelines"},
        "isCrossRepository": False,
    }

    decision = evaluate_pr(
        pr,
        expected_base_ref="main",
        expected_head_ref="codex/monthly-review-issue-12",
        expected_head_owner="QuantStrategyLab",
        expected_head_repository="QuantStrategyLab/UsEquitySnapshotPipelines",
        require_same_repository=True,
    )

    assert not decision["should_merge"]
    assert decision["reason"] == "human_review_required"
    assert decision["risk_level"] == "high"
    assert decision["has_human_review_label"]
    assert decision["human_review_label"] == HUMAN_REVIEW_LABEL
    assert decision["risk_reasons"] == [f"human-review label `{HUMAN_REVIEW_LABEL}` requires manual review"]
    assert decision["base_ref"] == "main"
    assert decision["head_ref"] == "codex/monthly-review-issue-12"
    assert decision["head_owner"] == "QuantStrategyLab"
    assert decision["head_repository"] == "QuantStrategyLab/UsEquitySnapshotPipelines"
    assert decision["is_cross_repository"] is False
    assert decision["reported_changed_file_count"] == 1


def test_evaluate_pr_blocks_large_low_risk_diff() -> None:
    decision = evaluate_pr(
        {
            "isDraft": False,
            "body": "<!-- codex-monthly-remediation:issue-12 -->",
            "labels": [{"name": AUTO_MERGE_LABEL}],
            "files": [{"path": "docs/operator_runbook.md"}],
            "changedFiles": 1,
            "additions": 1201,
            "deletions": 0,
            "baseRefName": "main",
            "headRefName": "codex/monthly-review-issue-12",
            "headRepositoryOwner": {"login": "QuantStrategyLab"},
            "headRepository": {"nameWithOwner": "QuantStrategyLab/UsEquitySnapshotPipelines"},
            "isCrossRepository": False,
        },
        expected_base_ref="main",
        expected_head_ref="codex/monthly-review-issue-12",
        expected_head_owner="QuantStrategyLab",
        expected_head_repository="QuantStrategyLab/UsEquitySnapshotPipelines",
        require_same_repository=True,
    )

    assert not decision["should_merge"]
    assert decision["reason"] == "policy_errors"
    assert decision["risk_level"] == "high"
    assert decision["changed_lines"] == 1201
    assert decision["risk_reasons"] == [
        "changed line count exceeds auto-merge limit requires human review: 1201 > 1200"
    ]


def test_evaluate_pr_requires_changed_line_counts_for_auto_merge_label() -> None:
    decision = evaluate_pr(
        {
            "isDraft": False,
            "body": "<!-- codex-monthly-remediation:issue-12 -->",
            "labels": [{"name": AUTO_MERGE_LABEL}],
            "files": [{"path": "docs/operator_runbook.md"}],
            "changedFiles": 1,
            "baseRefName": "main",
            "headRefName": "codex/monthly-review-issue-12",
            "headRepositoryOwner": {"login": "QuantStrategyLab"},
            "headRepository": {"nameWithOwner": "QuantStrategyLab/UsEquitySnapshotPipelines"},
            "isCrossRepository": False,
        },
        expected_base_ref="main",
        expected_head_ref="codex/monthly-review-issue-12",
        expected_head_owner="QuantStrategyLab",
        expected_head_repository="QuantStrategyLab/UsEquitySnapshotPipelines",
        require_same_repository=True,
    )

    assert not decision["should_merge"]
    assert decision["reason"] == "policy_errors"
    assert decision["risk_level"] == "high"
    assert decision["risk_reasons"] == ["missing changed line counts require human review"]


def test_evaluate_pr_requires_review_decision_for_auto_merge_label() -> None:
    decision = evaluate_pr(
        {
            "isDraft": False,
            "body": "<!-- codex-monthly-remediation:issue-12 -->",
            "labels": [{"name": AUTO_MERGE_LABEL}],
            "files": [{"path": "docs/operator_runbook.md"}],
            "changedFiles": 1,
            "additions": 4,
            "deletions": 1,
            "baseRefName": "main",
            "headRefName": "codex/monthly-review-issue-12",
            "headRepositoryOwner": {"login": "QuantStrategyLab"},
            "headRepository": {"nameWithOwner": "QuantStrategyLab/UsEquitySnapshotPipelines"},
            "isCrossRepository": False,
        },
        expected_base_ref="main",
        expected_head_ref="codex/monthly-review-issue-12",
        expected_head_owner="QuantStrategyLab",
        expected_head_repository="QuantStrategyLab/UsEquitySnapshotPipelines",
        require_same_repository=True,
    )

    assert not decision["should_merge"]
    assert decision["reason"] == "pr_metadata_mismatch"
    assert decision["risk_level"] == "high"
    assert decision["metadata_errors"] == ["missing review decision requires human review"]


@pytest.mark.parametrize("review_decision", ["CHANGES_REQUESTED", "REVIEW_REQUIRED"])
def test_evaluate_pr_blocks_requested_changes_review_decision(review_decision: str) -> None:
    decision = evaluate_pr(
        {
            "isDraft": False,
            "body": "<!-- codex-monthly-remediation:issue-12 -->",
            "labels": [{"name": AUTO_MERGE_LABEL}],
            "files": [{"path": "docs/operator_runbook.md"}],
            "changedFiles": 1,
            "additions": 4,
            "deletions": 1,
            "reviewDecision": review_decision,
            "baseRefName": "main",
            "headRefName": "codex/monthly-review-issue-12",
            "headRepositoryOwner": {"login": "QuantStrategyLab"},
            "headRepository": {"nameWithOwner": "QuantStrategyLab/UsEquitySnapshotPipelines"},
            "isCrossRepository": False,
        },
        expected_base_ref="main",
        expected_head_ref="codex/monthly-review-issue-12",
        expected_head_owner="QuantStrategyLab",
        expected_head_repository="QuantStrategyLab/UsEquitySnapshotPipelines",
        require_same_repository=True,
    )

    assert not decision["should_merge"]
    assert decision["reason"] == "pr_metadata_mismatch"
    assert decision["risk_level"] == "high"
    assert decision["review_decision"] == review_decision
    assert decision["metadata_errors"] == [f"review decision `{review_decision}` requires human review"]


def test_evaluate_pr_blocks_unexpected_pr_metadata() -> None:
    pr = {
        "isDraft": False,
        "body": "<!-- codex-monthly-remediation:issue-12 -->",
        "labels": [{"name": AUTO_MERGE_LABEL}],
        "files": [{"path": "docs/operator_runbook.md"}],
        "baseRefName": "release",
        "headRefName": "codex/monthly-review-issue-99",
        "headRepositoryOwner": {"login": "external-user"},
        "headRepository": {"nameWithOwner": "external-user/UsEquitySnapshotPipelines"},
        "isCrossRepository": True,
    }

    decision = evaluate_pr(
        pr,
        expected_base_ref="main",
        expected_head_ref="codex/monthly-review-issue-12",
        expected_head_owner="QuantStrategyLab",
        expected_head_repository="QuantStrategyLab/UsEquitySnapshotPipelines",
        require_same_repository=True,
    )

    assert not decision["should_merge"]
    assert decision["reason"] == "pr_metadata_mismatch"
    assert decision["risk_level"] == "high"
    assert decision["metadata_errors"] == [
        "unexpected PR base ref requires human review",
        "unexpected PR head ref requires human review",
        "unexpected PR head owner requires human review",
        "unexpected PR head repository requires human review",
        "cross-repository PR requires human review",
    ]




def test_evaluate_pr_requires_file_status_metadata_for_auto_merge_label() -> None:
    decision = evaluate_pr(
        {
            "isDraft": False,
            "body": "<!-- codex-monthly-remediation:issue-12 -->",
            "labels": [{"name": AUTO_MERGE_LABEL}],
            "files": [{"path": "docs/operator_runbook.md"}],
            "changedFiles": 1,
            "additions": 4,
            "deletions": 1,
            "reviewDecision": None,
            "baseRefName": "main",
            "headRefName": "codex/monthly-review-issue-12",
            "headRepositoryOwner": {"login": "QuantStrategyLab"},
            "headRepository": {"nameWithOwner": "QuantStrategyLab/UsEquitySnapshotPipelines"},
            "isCrossRepository": False,
        },
        expected_base_ref="main",
        expected_head_ref="codex/monthly-review-issue-12",
        expected_head_owner="QuantStrategyLab",
        expected_head_repository="QuantStrategyLab/UsEquitySnapshotPipelines",
        require_same_repository=True,
    )

    assert not decision["should_merge"]
    assert decision["reason"] == "pr_metadata_mismatch"
    assert decision["risk_level"] == "high"
    assert decision["metadata_errors"] == ["missing changed file status metadata requires human review"]

def test_evaluate_pr_blocks_removed_or_renamed_files_when_status_is_available() -> None:
    decision = evaluate_pr(
        {
            "isDraft": False,
            "body": "<!-- codex-monthly-remediation:issue-12 -->",
            "labels": [{"name": AUTO_MERGE_LABEL}],
            "files": [
                {"path": "docs/old.md", "status": "removed"},
                {"path": "tests/test_old.py", "status": "renamed"},
            ],
            "changedFiles": 2,
            "additions": 1,
            "deletions": 10,
            "reviewDecision": None,
            "baseRefName": "main",
            "headRefName": "codex/monthly-review-issue-12",
            "headRepositoryOwner": {"login": "QuantStrategyLab"},
            "headRepository": {"nameWithOwner": "QuantStrategyLab/UsEquitySnapshotPipelines"},
            "isCrossRepository": False,
        },
        expected_base_ref="main",
        expected_head_ref="codex/monthly-review-issue-12",
        expected_head_owner="QuantStrategyLab",
        expected_head_repository="QuantStrategyLab/UsEquitySnapshotPipelines",
        require_same_repository=True,
    )

    assert not decision["should_merge"]
    assert decision["reason"] == "pr_metadata_mismatch"
    assert decision["risk_level"] == "high"
    assert decision["metadata_errors"] == [
        "file status `removed` requires human review",
        "file status `renamed` requires human review",
    ]

def test_evaluate_pr_blocks_changed_file_list_mismatch() -> None:
    decision = evaluate_pr(
        {
            "isDraft": False,
            "body": "<!-- codex-monthly-remediation:issue-12 -->",
            "labels": [{"name": AUTO_MERGE_LABEL}],
            "files": [{"path": "docs/operator_runbook.md"}],
            "changedFiles": 2,
            "baseRefName": "main",
            "headRefName": "codex/monthly-review-issue-12",
            "headRepositoryOwner": {"login": "QuantStrategyLab"},
            "headRepository": {"nameWithOwner": "QuantStrategyLab/UsEquitySnapshotPipelines"},
            "isCrossRepository": False,
        },
        expected_base_ref="main",
        expected_head_ref="codex/monthly-review-issue-12",
        expected_head_owner="QuantStrategyLab",
        expected_head_repository="QuantStrategyLab/UsEquitySnapshotPipelines",
        require_same_repository=True,
    )

    assert not decision["should_merge"]
    assert decision["reason"] == "pr_metadata_mismatch"
    assert decision["risk_level"] == "high"
    assert decision["metadata_errors"] == ["changed file list mismatch requires human review"]


def test_evaluate_pr_requires_marker_issue_to_match_monthly_head_ref() -> None:
    decision = evaluate_pr(
        {
            "isDraft": False,
            "body": "<!-- codex-monthly-remediation:issue-99 -->",
            "labels": [{"name": AUTO_MERGE_LABEL}],
            "files": [{"path": "docs/operator_runbook.md"}],
            "changedFiles": 1,
            "baseRefName": "main",
            "headRefName": "codex/monthly-review-issue-12-20260620",
            "headRepositoryOwner": {"login": "QuantStrategyLab"},
            "headRepository": {"nameWithOwner": "QuantStrategyLab/UsEquitySnapshotPipelines"},
            "isCrossRepository": False,
        },
        expected_base_ref="main",
        expected_head_ref="codex/monthly-review-issue-12-20260620",
        expected_head_owner="QuantStrategyLab",
        expected_head_repository="QuantStrategyLab/UsEquitySnapshotPipelines",
        require_same_repository=True,
    )

    assert not decision["should_merge"]
    assert decision["reason"] == "missing_marker"
    assert decision["metadata_errors"] == []


def test_evaluate_pr_blocks_monthly_head_ref_without_issue_number() -> None:
    decision = evaluate_pr(
        {
            "isDraft": False,
            "body": "<!-- codex-monthly-remediation:issue-12 -->",
            "labels": [{"name": AUTO_MERGE_LABEL}],
            "files": [{"path": "docs/operator_runbook.md"}],
            "changedFiles": 1,
            "baseRefName": "main",
            "headRefName": "codex/monthly-review-issue-latest",
            "headRepositoryOwner": {"login": "QuantStrategyLab"},
            "headRepository": {"nameWithOwner": "QuantStrategyLab/UsEquitySnapshotPipelines"},
            "isCrossRepository": False,
        },
        expected_base_ref="main",
        expected_head_ref="codex/monthly-review-issue-latest",
        expected_head_owner="QuantStrategyLab",
        expected_head_repository="QuantStrategyLab/UsEquitySnapshotPipelines",
        require_same_repository=True,
    )

    assert not decision["should_merge"]
    assert decision["reason"] == "pr_metadata_mismatch"
    assert decision["metadata_errors"] == ["monthly PR head ref issue number missing requires human review"]


def test_evaluate_pr_blocks_empty_changed_file_paths() -> None:
    decision = evaluate_pr(
        {
            "isDraft": False,
            "body": "<!-- codex-monthly-remediation:issue-12 -->",
            "labels": [{"name": AUTO_MERGE_LABEL}],
            "files": [{"path": ""}],
            "changedFiles": 1,
        }
    )

    assert not decision["should_merge"]
    assert decision["reason"] == "pr_metadata_mismatch"
    assert decision["metadata_errors"] == ["invalid changed file path requires human review"]


def test_evaluate_pr_blocks_malformed_changed_file_list() -> None:
    decision = evaluate_pr(
        {
            "isDraft": False,
            "body": "<!-- codex-monthly-remediation:issue-12 -->",
            "labels": [{"name": AUTO_MERGE_LABEL}],
            "files": "docs/operator_runbook.md",
        }
    )

    assert not decision["should_merge"]
    assert decision["reason"] == "pr_metadata_mismatch"
    assert decision["metadata_errors"] == ["invalid changed file list requires human review"]


def test_evaluate_pr_treats_malformed_label_list_as_missing_label() -> None:
    decision = evaluate_pr(
        {
            "isDraft": False,
            "body": "<!-- codex-monthly-remediation:issue-12 -->",
            "labels": "auto-merge-ok",
            "files": [{"path": "docs/operator_runbook.md"}],
        }
    )

    assert not decision["should_merge"]
    assert decision["reason"] == "missing_auto_merge_label"


def test_evaluate_pr_blocks_missing_cross_repository_field_when_required() -> None:
    pr = {
        "isDraft": False,
        "body": "<!-- codex-monthly-remediation:issue-12 -->",
        "labels": [{"name": AUTO_MERGE_LABEL}],
        "files": [{"path": "docs/operator_runbook.md"}],
        "baseRefName": "main",
        "headRefName": "codex/monthly-review-issue-12",
        "headRepositoryOwner": {"login": "QuantStrategyLab"},
        "headRepository": {"nameWithOwner": "QuantStrategyLab/UsEquitySnapshotPipelines"},
    }

    decision = evaluate_pr(
        pr,
        expected_base_ref="main",
        expected_head_ref="codex/monthly-review-issue-12",
        expected_head_owner="QuantStrategyLab",
        expected_head_repository="QuantStrategyLab/UsEquitySnapshotPipelines",
        require_same_repository=True,
    )

    assert not decision["should_merge"]
    assert decision["reason"] == "pr_metadata_mismatch"
    assert decision["is_cross_repository"] is None
    assert decision["metadata_errors"] == ["cross-repository PR requires human review"]


def test_evaluate_pr_uses_policy_marker_and_label() -> None:
    policy = {
        "auto_merge_label": "custom-auto-ok",
        "human_review_label": "custom-review-required",
        "monthly_marker_prefix": "<!-- custom-remediation:issue-",
        "risk_policy": {
            "low": {"prefixes": ["docs/"], "exact": [], "reason": "low"},
            "medium": {"exact": [], "reason": "medium"},
            "high": {"reason": "high"},
        },
    }
    pr = {
        "isDraft": False,
        "body": "<!-- custom-remediation:issue-12 -->",
        "labels": [{"name": "custom-auto-ok"}],
        "files": [{"path": "docs/operator_runbook.md", "status": "modified"}],
        "additions": 3,
        "deletions": 1,
        "reviewDecision": "APPROVED",
    }

    decision = evaluate_pr(pr, policy=policy)

    assert decision["should_merge"]
    assert decision["auto_merge_label"] == "custom-auto-ok"
    assert decision["human_review_label"] == "custom-review-required"
    assert decision["monthly_marker_prefix"] == "<!-- custom-remediation:issue-"


def test_evaluate_pr_fails_closed_on_invalid_policy_marker_or_label() -> None:
    policy = {
        "auto_merge_label": ["not-a-string"],
        "human_review_label": "human-review-required",
        "monthly_marker_prefix": "",
        "risk_policy": {
            "low": {"prefixes": ["docs/"], "exact": [], "reason": "low"},
            "medium": {"exact": [], "reason": "medium"},
            "high": {"reason": "high"},
        },
    }
    pr = {
        "isDraft": False,
        "body": "<!-- codex-monthly-remediation:issue-12 -->",
        "labels": [{"name": AUTO_MERGE_LABEL}],
        "files": [{"path": "docs/operator_runbook.md"}],
    }

    decision = evaluate_pr(pr, policy=policy)

    assert not decision["should_merge"]
    assert decision["reason"] == "policy_errors"
    assert decision["risk_level"] == "high"
    assert decision["policy_errors"] == [
        "invalid auto_merge_label string requires human review",
        "invalid monthly_marker_prefix string requires human review",
    ]


def test_evaluate_pr_blocks_draft_or_sensitive_files() -> None:
    draft = evaluate_pr(
        {
            "isDraft": True,
            "body": "<!-- codex-monthly-remediation:issue-12 -->",
            "labels": [{"name": AUTO_MERGE_LABEL}],
            "files": [{"path": "docs/operator_runbook.md"}],
        }
    )
    sensitive = evaluate_pr(
        {
            "isDraft": False,
            "body": "<!-- codex-monthly-remediation:issue-12 -->",
            "labels": [{"name": AUTO_MERGE_LABEL}],
            "files": [{"path": "src/us_equity_snapshot_pipelines/contracts.py"}],
        }
    )

    assert not draft["should_merge"]
    assert draft["reason"] == "draft_pr"
    assert not sensitive["should_merge"]
    assert sensitive["reason"] == "blocked_files"
