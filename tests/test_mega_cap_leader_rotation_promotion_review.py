from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_promotion_review import (
    build_promotion_review,
    main,
)


def _summary() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Run": "blend_top2_50_top4_50", "CAGR": 0.45, "Max Drawdown": -0.31, "Sharpe": 1.2, "Turnover/Year": 3.5},
            {"Run": "blend_top2_25_top4_75", "CAGR": 0.42, "Max Drawdown": -0.28, "Sharpe": 1.2, "Turnover/Year": 3.5},
            {"Run": "base_top4_cap25", "CAGR": 0.39, "Max Drawdown": -0.27, "Sharpe": 1.1, "Turnover/Year": 3.5},
            {"Run": "panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50", "CAGR": 0.46, "Max Drawdown": -0.31, "Sharpe": 1.3, "Turnover/Year": 3.8},
        ]
    )


def _live() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Run": "blend_top2_50_top4_50", "Candidate Role": "balanced_offensive_live_design", "Gate Profile": "balanced_offensive", "live_gate_passed": True, "live_gate_reason": "pass"},
            {"Run": "blend_top2_25_top4_75", "Candidate Role": "conservative_live_design", "Gate Profile": "conservative", "live_gate_passed": True, "live_gate_reason": "pass"},
            {"Run": "base_top4_cap25", "Candidate Role": "robust_baseline", "Gate Profile": "fallback", "live_gate_passed": True, "live_gate_reason": "pass"},
            {"Run": "panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50", "Candidate Role": "panic_rebound_guard_research", "Gate Profile": "research_only", "live_gate_passed": False, "live_gate_reason": "research_only_role"},
        ]
    )


def _stress() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Run": "blend_top2_50_top4_50", "all_stress_gates_passed": True, "stress_gate_reason": "pass"},
            {"Run": "blend_top2_25_top4_75", "all_stress_gates_passed": True, "stress_gate_reason": "pass"},
            {"Run": "base_top4_cap25", "all_stress_gates_passed": True, "stress_gate_reason": "pass"},
            {"Run": "panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50", "all_stress_gates_passed": False, "stress_gate_reason": "research_only_role"},
        ]
    )


def _overfit() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Run": "blend_top2_50_top4_50", "live_promotion_gate_passed": True, "live_promotion_gate_reason": "pass"},
            {"Run": "blend_top2_25_top4_75", "live_promotion_gate_passed": True, "live_promotion_gate_reason": "pass"},
            {"Run": "base_top4_cap25", "live_promotion_gate_passed": True, "live_promotion_gate_reason": "pass"},
            {"Run": "panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50", "live_promotion_gate_passed": False, "live_promotion_gate_reason": "walk_forward_gate_failed"},
        ]
    )


def _liquidity() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Run": "blend_top2_50_top4_50", "Portfolio NAV": 5_000_000, "Max Participation Rate": 0.005, "liquidity_gate_passed": True, "liquidity_gate_reason": "pass"},
            {"Run": "blend_top2_25_top4_75", "Portfolio NAV": 5_000_000, "Max Participation Rate": 0.004, "liquidity_gate_passed": True, "liquidity_gate_reason": "pass"},
            {"Run": "base_top4_cap25", "Portfolio NAV": 5_000_000, "Max Participation Rate": 0.004, "liquidity_gate_passed": True, "liquidity_gate_reason": "pass"},
            {"Run": "panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50", "Portfolio NAV": 5_000_000, "Max Participation Rate": 0.006, "liquidity_gate_passed": True, "liquidity_gate_reason": "pass"},
        ]
    )


def _reality_qqq() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Run": "blend_top2_50_top4_50", "Reality Check Passed": True, "Reality Check P Value": 0.006},
            {"Run": "blend_top2_25_top4_75", "Reality Check Passed": False, "Reality Check P Value": 0.006},
            {"Run": "base_top4_cap25", "Reality Check Passed": False, "Reality Check P Value": 0.006},
        ]
    )


def _reality_spy() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Run": "blend_top2_50_top4_50", "Reality Check Passed": True, "Reality Check P Value": 0.003},
            {"Run": "blend_top2_25_top4_75", "Reality Check Passed": False, "Reality Check P Value": 0.003},
            {"Run": "base_top4_cap25", "Reality Check Passed": False, "Reality Check P Value": 0.003},
        ]
    )


def test_build_promotion_review_prefers_balanced_when_all_required_gates_pass() -> None:
    review = build_promotion_review(
        _summary(),
        live_readiness=_live(),
        stress_summary=_stress(),
        overfit_promotion=_overfit(),
        liquidity_summary=_liquidity(),
        reality_check_qqq=_reality_qqq(),
        reality_check_spy=_reality_spy(),
        portfolio_nav=5_000_000,
    )
    indexed = review.set_index("Run")

    assert review.iloc[0]["Run"] == "blend_top2_50_top4_50"
    assert bool(indexed.loc["blend_top2_50_top4_50", "required_gates_passed"]) is True
    assert indexed.loc["blend_top2_50_top4_50", "promotion_decision"] == "live_design_review_balanced_offensive"
    assert indexed.loc["blend_top2_50_top4_50", "recommended_action"] == "preferred_aggressive_live_design_review"
    assert indexed.loc["blend_top2_50_top4_50", "statistical_support_level"] == "qqq_and_spy_reality_check"
    assert indexed.loc["panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50", "promotion_decision"] == "research_only"
    assert "live_gate" in indexed.loc["panicdd10_ret3_vol25_stock50_blend_top2_50_top4_50", "required_gate_reason"]


def test_promotion_review_cli_writes_output(tmp_path) -> None:
    paths = {}
    for name, frame in {
        "summary": _summary(),
        "live": _live(),
        "stress": _stress(),
        "overfit": _overfit(),
        "liquidity": _liquidity(),
        "qqq": _reality_qqq(),
        "spy": _reality_spy(),
    }.items():
        path = tmp_path / f"{name}.csv"
        frame.to_csv(path, index=False)
        paths[name] = path
    output_dir = tmp_path / "out"

    exit_code = main(
        [
            "--summary",
            str(paths["summary"]),
            "--live-readiness",
            str(paths["live"]),
            "--stress-summary",
            str(paths["stress"]),
            "--overfit-promotion",
            str(paths["overfit"]),
            "--liquidity-summary",
            str(paths["liquidity"]),
            "--reality-check-qqq",
            str(paths["qqq"]),
            "--reality-check-spy",
            str(paths["spy"]),
            "--portfolio-nav",
            "5000000",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    assert (output_dir / "live_promotion_review.csv").exists()
