from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.build_monthly_russell_crash_brake_research import (
    build_crash_brake_research_from_snapshot_run,
    discover_russell_snapshot_runs,
    main,
)
from scripts.stage_snapshot_source_inputs import main as stage_inputs_main


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


def _write_release_summary(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "strategy_profile": "russell_top50_leader_rotation",
                "release_status": "ready",
                "snapshot_as_of": "2023-05-31",
            }
        ),
        encoding="utf-8",
    )


def test_discover_russell_snapshot_runs_finds_staged_research_inputs(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "us-equity-snapshot-russell_top50_leader_rotation-123"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    prices_path = tmp_path / "prices.csv"
    universe_path = tmp_path / "latest_holdings.csv"
    research_universe_path = tmp_path / "universe_history.csv"
    _sample_prices().to_csv(prices_path, index=False)
    _sample_latest_holdings().to_csv(universe_path, index=False)
    _sample_universe_history().to_csv(research_universe_path, index=False)
    _write_release_summary(artifact_dir / "release_status_summary.json")

    assert (
        stage_inputs_main(
            [
                "--artifact-dir",
                str(artifact_dir),
                "--prices",
                str(prices_path),
                "--universe",
                str(universe_path),
                "--research-universe",
                str(research_universe_path),
            ]
        )
        == 0
    )

    discovered = discover_russell_snapshot_runs(tmp_path)
    assert len(discovered) == 1
    assert discovered[0]["prices_path"].name == "prices.csv"
    assert discovered[0]["universe_path"].name == "research_universe.csv"
    assert discovered[0]["snapshot_as_of"] == "2023-05-31"


def test_build_crash_brake_research_from_snapshot_run_writes_manifest(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "us-equity-snapshot-russell_top50_leader_rotation-123"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    prices_path = tmp_path / "prices.csv"
    universe_path = tmp_path / "latest_holdings.csv"
    research_universe_path = tmp_path / "universe_history.csv"
    _sample_prices().to_csv(prices_path, index=False)
    _sample_latest_holdings().to_csv(universe_path, index=False)
    _sample_universe_history().to_csv(research_universe_path, index=False)
    _write_release_summary(artifact_dir / "release_status_summary.json")
    assert (
        stage_inputs_main(
            [
                "--artifact-dir",
                str(artifact_dir),
                "--prices",
                str(prices_path),
                "--universe",
                str(universe_path),
                "--research-universe",
                str(research_universe_path),
            ]
        )
        == 0
    )

    run = discover_russell_snapshot_runs(tmp_path)[0]
    output_dir = build_crash_brake_research_from_snapshot_run(run, output_root=tmp_path / "research")
    manifest = json.loads((output_dir / "crash_brake_research_manifest.json").read_text(encoding="utf-8"))
    assert manifest["manifest_type"] == "russell_top50_crash_brake_research"
    assert (output_dir / "crash_brake_summary.csv").exists()


def test_main_builds_russell_crash_brake_research(tmp_path: Path, monkeypatch, capsys) -> None:
    artifact_dir = tmp_path / "us-equity-snapshot-russell_top50_leader_rotation-123"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    prices_path = tmp_path / "prices.csv"
    universe_path = tmp_path / "latest_holdings.csv"
    research_universe_path = tmp_path / "universe_history.csv"
    _sample_prices().to_csv(prices_path, index=False)
    _sample_latest_holdings().to_csv(universe_path, index=False)
    _sample_universe_history().to_csv(research_universe_path, index=False)
    _write_release_summary(artifact_dir / "release_status_summary.json")
    assert (
        stage_inputs_main(
            [
                "--artifact-dir",
                str(artifact_dir),
                "--prices",
                str(prices_path),
                "--universe",
                str(universe_path),
                "--research-universe",
                str(research_universe_path),
            ]
        )
        == 0
    )

    output_root = tmp_path / "monthly_research"
    monkeypatch.setattr(
        "sys.argv",
        [
            "build_monthly_russell_crash_brake_research.py",
            "--artifact-root",
            str(tmp_path),
            "--output-root",
            str(output_root),
        ],
    )

    assert main() == 0
    captured = capsys.readouterr()
    assert "crash_brake_research_count=1" in captured.out
    manifest = output_root / "us-equity-snapshot-russell_top50_leader_rotation-123__russell_top50_crash_brake_research" / "crash_brake_research_manifest.json"
    assert manifest.exists()
