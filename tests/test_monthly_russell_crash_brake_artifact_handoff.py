from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.build_monthly_live_replacement_reviews import main as live_replacement_main
from scripts.build_monthly_russell_crash_brake_research import main as crash_brake_research_main
from scripts.build_monthly_russell_crash_brake_review_chain import main as crash_brake_review_chain_main
from scripts.run_monthly_report_bundle import build_bundle
from scripts.stage_snapshot_source_inputs import main as stage_inputs_main
from us_equity_snapshot_pipelines.contracts import get_profile_contract


def _sample_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2020-01-02", periods=900)
    trends = {
        "SPY": 0.00035,
        "BOXX": 0.0001,
        "AAPL": 0.0009,
        "MSFT": 0.0010,
        "NVDA": 0.0012,
        "AMZN": 0.0008,
        "LLY": 0.0011,
        "XOM": 0.0007,
    }
    rows: list[dict[str, object]] = []
    for idx, as_of in enumerate(dates):
        if idx < 500:
            qqq_multiplier = (1.0006) ** idx
        elif idx < 540:
            qqq_multiplier = (1.0006**500) * (1.0 - 0.18 * ((idx - 500) / 40.0))
        else:
            qqq_multiplier = (1.0006**500) * 0.82 * (1.003 ** (idx - 540))
        rows.append(
            {
                "symbol": "QQQ",
                "as_of": as_of.date().isoformat(),
                "close": 120.0 * qqq_multiplier,
                "volume": 3_000_000,
            }
        )
        for offset, (symbol, trend) in enumerate(trends.items()):
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": as_of.date().isoformat(),
                    "close": (85.0 + offset * 5.0) * ((1.0 + trend) ** idx),
                    "volume": 2_500_000,
                }
            )
    return pd.DataFrame(rows)


def _sample_universe_history() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "sector": sector,
                "start_date": "2021-01-29",
                "end_date": None,
                "mega_rank": rank,
            }
            for rank, (symbol, sector) in enumerate(
                [
                    ("NVDA", "Information Technology"),
                    ("MSFT", "Information Technology"),
                    ("AAPL", "Information Technology"),
                    ("LLY", "Health Care"),
                    ("AMZN", "Consumer Discretionary"),
                    ("XOM", "Energy"),
                ],
                start=1,
            )
        ]
    )


def _sample_latest_holdings() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol": "NVDA", "sector": "Information Technology", "mega_rank": 1},
            {"symbol": "MSFT", "sector": "Information Technology", "mega_rank": 2},
            {"symbol": "AAPL", "sector": "Information Technology", "mega_rank": 3},
            {"symbol": "LLY", "sector": "Health Care", "mega_rank": 4},
        ]
    )


def _write_global_etf_artifacts(snapshot_dir: Path) -> None:
    contract = get_profile_contract("global_etf_rotation")
    paths = contract.artifact_paths(snapshot_dir)
    pd.DataFrame(
        [
            {"as_of": "2023-05-31", "symbol": "QQQ", "close": 300.0},
            {"as_of": "2023-05-31", "symbol": "SPY", "close": 400.0},
        ]
    ).to_csv(paths["snapshot"], index=False)
    pd.DataFrame(
        [
            {"current_rank": 1, "symbol": "QQQ", "final_score": 0.95, "selected_flag": "true"},
            {"current_rank": 2, "symbol": "SPY", "final_score": 0.90, "selected_flag": "true"},
        ]
    ).to_csv(paths["ranking"], index=False)
    paths["manifest"].write_text(
        json.dumps(
            {
                "manifest_type": "feature_snapshot",
                "contract_version": contract.contract_version,
                "strategy_profile": contract.profile,
                "snapshot_as_of": "2023-05-31",
                "row_count": 2,
            }
        ),
        encoding="utf-8",
    )


def _write_release_summary(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "source_project": "UsEquitySnapshotPipelines",
                "strategy_profile": "russell_top50_leader_rotation",
                "release_status": "ready",
                "snapshot_as_of": "2023-05-31",
                "signal_description": "russell snapshot ready",
                "status_description": "ready for monthly review",
                "row_count": 4,
                "diagnostics": {"selected_symbols": ["NVDA", "MSFT", "AAPL", "LLY"]},
            }
        ),
        encoding="utf-8",
    )


def _write_snapshot_artifacts(snapshot_dir: Path) -> None:
    contract = get_profile_contract("russell_top50_leader_rotation")
    paths = contract.artifact_paths(snapshot_dir)
    pd.DataFrame(
        [
            {"as_of": "2023-05-31", "symbol": "NVDA", "close": 100.0},
            {"as_of": "2023-05-31", "symbol": "MSFT", "close": 90.0},
        ]
    ).to_csv(paths["snapshot"], index=False)
    pd.DataFrame(
        [
            {"current_rank": 1, "symbol": "NVDA", "final_score": 0.95, "selected_flag": "true"},
            {"current_rank": 2, "symbol": "MSFT", "final_score": 0.90, "selected_flag": "true"},
        ]
    ).to_csv(paths["ranking"], index=False)
    paths["manifest"].write_text(
        json.dumps(
            {
                "manifest_type": "feature_snapshot",
                "contract_version": contract.contract_version,
                "strategy_profile": contract.profile,
                "snapshot_as_of": "2023-05-31",
                "row_count": 2,
            }
        ),
        encoding="utf-8",
    )


def test_monthly_russell_crash_brake_handoff_from_snapshot_artifact(tmp_path: Path, monkeypatch) -> None:
    artifact_root = tmp_path / "monthly_review_inputs"
    snapshot_dir = artifact_root / "us-equity-snapshot-russell_top50_leader_rotation-123"
    global_etf_dir = artifact_root / "us-equity-snapshot-global_etf_rotation-456"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    global_etf_dir.mkdir(parents=True, exist_ok=True)

    prices_path = tmp_path / "prices.csv"
    latest_holdings_path = tmp_path / "latest_holdings.csv"
    universe_history_path = tmp_path / "universe_history.csv"
    _sample_prices().to_csv(prices_path, index=False)
    _sample_latest_holdings().to_csv(latest_holdings_path, index=False)
    _sample_universe_history().to_csv(universe_history_path, index=False)
    _write_snapshot_artifacts(snapshot_dir)
    _write_release_summary(snapshot_dir / "release_status_summary.json")
    _write_global_etf_artifacts(global_etf_dir)
    (global_etf_dir / "release_status_summary.json").write_text(
        json.dumps(
            {
                "source_project": "UsEquitySnapshotPipelines",
                "strategy_profile": "global_etf_rotation",
                "release_status": "ready",
                "snapshot_as_of": "2023-05-31",
                "signal_description": "global etf snapshot ready",
                "status_description": "ready for monthly review",
                "row_count": 2,
                "diagnostics": {"selected_symbols": ["QQQ", "SPY"]},
            }
        ),
        encoding="utf-8",
    )

    assert (
        stage_inputs_main(
            [
                "--artifact-dir",
                str(snapshot_dir),
                "--prices",
                str(prices_path),
                "--universe",
                str(latest_holdings_path),
                "--research-universe",
                str(universe_history_path),
            ]
        )
        == 0
    )

    monkeypatch.setattr(
        "sys.argv",
        [
            "build_monthly_russell_crash_brake_research.py",
            "--artifact-root",
            str(artifact_root),
            "--output-root",
            str(artifact_root),
        ],
    )
    assert crash_brake_research_main() == 0

    monkeypatch.setattr(
        "sys.argv",
        [
            "build_monthly_russell_crash_brake_review_chain.py",
            "--artifact-root",
            str(artifact_root),
            "--output-root",
            str(artifact_root),
            "--snapshot-as-of",
            "2023-05-31",
        ],
    )
    assert crash_brake_review_chain_main() == 0

    monkeypatch.setattr(
        "sys.argv",
        [
            "build_monthly_live_replacement_reviews.py",
            "--artifact-root",
            str(artifact_root),
            "--output-root",
            str(artifact_root),
        ],
    )
    assert live_replacement_main() == 0

    bundle = build_bundle(artifact_root, report_month="2026-06", ranking_preview_size=2)

    assert bundle["status"] == "ok"
    assert bundle["missing_profile_count"] == 0
    assert bundle["non_ready_profile_count"] == 0
    assert bundle["crash_brake_research_count"] == 1
    assert bundle["crash_brake_overfit_count"] == 1
    assert bundle["crash_brake_stress_count"] == 1
    assert bundle["crash_brake_liquidity_count"] == 1
    assert bundle["live_replacement_review_count"] == 1
    assert bundle["live_replacement_reviews"][0]["replace_live_now_count"] == 0
