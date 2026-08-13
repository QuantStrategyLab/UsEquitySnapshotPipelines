from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "docs" / "tqqq-validation-and-shadow-policy.md"


def test_policy_records_p3_runner_coverage_gaps_and_p4_boundary() -> None:
    policy = POLICY.read_text(encoding="utf-8")
    normalized_policy = " ".join(policy.split())

    for required in (
        "## Existing runner coverage",
        "BacktestOrchestrator",
        "2025-07-02 through 2026-07-31",
        "20-calendar-day purge and 20-calendar-day embargo",
        "not 20 XNYS sessions",
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
