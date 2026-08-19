from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "docs" / "tqqq-validation-and-shadow-policy.md"
MANDATE_README = ROOT / "config" / "tqqq_p1_p3_mandates" / "README.md"


def test_policy_records_p3_runner_coverage_gaps_and_p4_boundary() -> None:
    policy = POLICY.read_text(encoding="utf-8")
    normalized_policy = " ".join(policy.split())

    for required in (
        "## Existing runner coverage",
        "BacktestOrchestrator",
        "2025-08-01 through 2026-07-31",
        "252-session post-training purge and zero embargo",
        "5/10/15 bp per-side",
        "executes every deterministic 3/6/12/24-month",
        "TQQQ signal, allocation, RiskEngine, and runtime logic remain in",
        "## Gaps and next gated slice",
        "still research-only",
        "trial ledger",
        "PBO",
        "Deflated Sharpe",
        "must not inherit provider, credential, promotion, paper, shadow, live, order, or capital authority",
    ):
        assert required in normalized_policy


def test_removed_forward_collector_has_no_remaining_entrypoint_or_source() -> None:
    assert "useq-collect-tqqq-forward-observation" not in (
        ROOT / "pyproject.toml"
    ).read_text(encoding="utf-8")
    for removed in (
        "scripts/install_tqqq_forward_observation_launchagent.py",
        "src/us_equity_snapshot_pipelines/lifecycle/tqqq_forward_observation.py",
        "src/us_equity_snapshot_pipelines/tqqq_forward_observation_cli.py",
    ):
        assert not (ROOT / removed).exists()


def test_p1_p3_reliability_semantics_separate_current_v5_from_legacy_v1() -> None:
    policy = POLICY.read_text(encoding="utf-8")
    mandate_readme = MANDATE_README.read_text(encoding="utf-8")

    for required in (
        "`INPUT_UNAVAILABLE`, therefore\n  `INCONCLUSIVE` and `PARKED`",
        "must not alter the\n  frozen data identity",
        "or reset the\n  locked historical OOS span",
        "### Current P2 v5 scheduled research path",
        "There is no\nper-run mandate or reviewer",
        "`DEFERRED` or `QUARANTINED` and skips P3",
        "P4 paper, P5 shadow, broker orders, capital, and\nP6 live remain unavailable",
        "### Legacy manual v1 compatibility path",
        "no-order technical scope record",
        "does\nnot govern the P2 v5 scheduled controller",
    ):
        assert required in policy
    for required in (
        "no-order technical scope\nrecord",
        "separately defined, externally verified, non-execution data-acquisition\nauthorization",
        "That authorization is not active",
        "do not read, verify, or\ninject it today",
    ):
        assert required in mandate_readme
    assert "mandatory\nhuman approval" not in policy
    assert "required\nhuman review" not in mandate_readme
