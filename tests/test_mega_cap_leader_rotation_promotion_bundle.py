from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_promotion_bundle import build_promotion_bundle, main


def _summary() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Run": "blend_top2_50_top4_50", "CAGR": 0.45, "Max Drawdown": -0.31, "Sharpe": 1.2, "Turnover/Year": 3.5},
            {"Run": "blend_top2_25_top4_75", "CAGR": 0.42, "Max Drawdown": -0.28, "Sharpe": 1.2, "Turnover/Year": 3.5},
            {"Run": "base_top4_cap25", "CAGR": 0.39, "Max Drawdown": -0.27, "Sharpe": 1.1, "Turnover/Year": 3.5},
        ]
    )


def _daily_returns() -> pd.DataFrame:
    rows = []
    for idx, as_of in enumerate(pd.bdate_range("2024-01-02", periods=120)):
        qqq = 0.0002 if idx % 5 else -0.0001
        spy = qqq - 0.0001
        noise = 0.0001 * np.sin(idx / 4.0)
        rows.extend(
            [
                {"Date": as_of.date().isoformat(), "Run": "blend_top2_50_top4_50", "Variant Type": "fixed_blend", "Strategy Return": qqq + 0.0012 + noise, "QQQ Return": qqq, "SPY Return": spy},
                {"Date": as_of.date().isoformat(), "Run": "blend_top2_25_top4_75", "Variant Type": "fixed_blend", "Strategy Return": qqq + 0.0005 - noise, "QQQ Return": qqq, "SPY Return": spy},
                {"Date": as_of.date().isoformat(), "Run": "base_top4_cap25", "Variant Type": "base_top4", "Strategy Return": qqq + 0.0002, "QQQ Return": qqq, "SPY Return": spy},
            ]
        )
    return pd.DataFrame(rows)


def _live() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Run": "blend_top2_50_top4_50", "Candidate Role": "balanced_offensive_live_design", "Gate Profile": "balanced_offensive", "live_gate_passed": True, "live_gate_reason": "pass"},
            {"Run": "blend_top2_25_top4_75", "Candidate Role": "conservative_live_design", "Gate Profile": "conservative", "live_gate_passed": True, "live_gate_reason": "pass"},
            {"Run": "base_top4_cap25", "Candidate Role": "robust_baseline", "Gate Profile": "fallback", "live_gate_passed": True, "live_gate_reason": "pass"},
        ]
    )


def _stress() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Run": "blend_top2_50_top4_50", "all_stress_gates_passed": True, "stress_gate_reason": "pass"},
            {"Run": "blend_top2_25_top4_75", "all_stress_gates_passed": True, "stress_gate_reason": "pass"},
            {"Run": "base_top4_cap25", "all_stress_gates_passed": True, "stress_gate_reason": "pass"},
        ]
    )


def _overfit() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Run": "blend_top2_50_top4_50", "live_promotion_gate_passed": True, "live_promotion_gate_reason": "pass"},
            {"Run": "blend_top2_25_top4_75", "live_promotion_gate_passed": True, "live_promotion_gate_reason": "pass"},
            {"Run": "base_top4_cap25", "live_promotion_gate_passed": True, "live_promotion_gate_reason": "pass"},
        ]
    )


def _liquidity() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Run": "blend_top2_50_top4_50", "Portfolio NAV": 5_000_000, "Max Participation Rate": 0.005, "liquidity_gate_passed": True, "liquidity_gate_reason": "pass"},
            {"Run": "blend_top2_25_top4_75", "Portfolio NAV": 5_000_000, "Max Participation Rate": 0.004, "liquidity_gate_passed": True, "liquidity_gate_reason": "pass"},
            {"Run": "base_top4_cap25", "Portfolio NAV": 5_000_000, "Max Participation Rate": 0.003, "liquidity_gate_passed": True, "liquidity_gate_reason": "pass"},
        ]
    )


def test_build_promotion_bundle_writes_expected_frames(tmp_path) -> None:
    result = build_promotion_bundle(
        concentration_summary=_summary(),
        daily_returns=_daily_returns(),
        live_readiness=_live(),
        stress_summary=_stress(),
        overfit_promotion=_overfit(),
        liquidity_summary=_liquidity(),
        output_dir=tmp_path,
        candidate_runs=("base_top4_cap25", "blend_top2_25_top4_75", "blend_top2_50_top4_50"),
        portfolio_nav=5_000_000,
        eras="test_era:2024-01-01:2024-12-31",
        bootstrap_iterations=99,
        block_size=5,
        random_seed=7,
        alpha=0.10,
    )

    review = result["live_promotion_review"].set_index("Run")
    assert bool(review.loc["blend_top2_50_top4_50", "required_gates_passed"]) is True
    assert bool(review.loc["blend_top2_50_top4_50", "mcs_style_in_confidence_set"]) is True
    assert (tmp_path / "reality_check_qqq" / "reality_check_candidate_summary.csv").exists()
    assert (tmp_path / "spa_spy" / "spa_candidate_summary.csv").exists()
    assert (tmp_path / "era_split" / "era_split_promotion_summary.csv").exists()
    assert (tmp_path / "mcs_style" / "mcs_style_candidate_summary.csv").exists()
    assert (tmp_path / "dsr_pbo_qqq" / "dsr_pbo_candidate_summary.csv").exists()
    assert (tmp_path / "dsr_pbo_spy" / "dsr_pbo_global_summary.csv").exists()
    assert (tmp_path / "live_promotion_review.csv").exists()
    manifest = json.loads((tmp_path / "promotion_bundle_manifest.json").read_text())
    assert manifest["manifest_type"] == "russell_top50_promotion_bundle"
    assert manifest["candidate_runs"] == [
        "base_top4_cap25",
        "blend_top2_25_top4_75",
        "blend_top2_50_top4_50",
    ]
    assert "sha256" in manifest["artifacts"]["live_promotion_review"]
    assert "sha256" in manifest["artifacts"]["dsr_pbo_qqq_candidate"]
    assert manifest["dsr_pbo"]["cscv_groups"] == 8
    review_rows = {row["run"]: row for row in manifest["review_rows"]}
    assert review_rows["blend_top2_50_top4_50"]["required_gates_passed"] is True


def test_promotion_bundle_cli_writes_outputs(tmp_path) -> None:
    inputs = {
        "summary": _summary(),
        "daily": _daily_returns(),
        "live": _live(),
        "stress": _stress(),
        "overfit": _overfit(),
        "liquidity": _liquidity(),
    }
    paths: dict[str, Path] = {}
    for name, frame in inputs.items():
        path = tmp_path / f"{name}.csv"
        frame.to_csv(path, index=False)
        paths[name] = path
    output_dir = tmp_path / "bundle"

    exit_code = main(
        [
            "--summary",
            str(paths["summary"]),
            "--daily-returns",
            str(paths["daily"]),
            "--live-readiness",
            str(paths["live"]),
            "--stress-summary",
            str(paths["stress"]),
            "--overfit-promotion",
            str(paths["overfit"]),
            "--liquidity-summary",
            str(paths["liquidity"]),
            "--portfolio-nav",
            "5000000",
            "--eras",
            "test_era:2024-01-01:2024-12-31",
            "--bootstrap-iterations",
            "99",
            "--block-size",
            "5",
            "--random-seed",
            "7",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert exit_code == 0
    assert (output_dir / "live_promotion_review.csv").exists()
    assert (output_dir / "promotion_bundle_manifest.json").exists()
    assert (output_dir / "dsr_pbo_qqq" / "dsr_pbo_cscv_splits.csv").exists()
