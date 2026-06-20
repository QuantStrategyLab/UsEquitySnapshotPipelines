from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.build_monthly_live_strategy_health_reports import (
    build_health_error_report,
    build_health_report_for_returns,
    discover_return_matrices,
    infer_strategy_columns,
    main,
    resolve_date_column,
)


def _returns() -> pd.DataFrame:
    dates = pd.bdate_range("2026-01-02", periods=90)
    return pd.DataFrame(
        {
            "as_of": dates,
            "global_etf_rotation": [0.0010] * len(dates),
            "monthly_variant": [0.0001] * len(dates),
            "buy_hold_SPY": [0.0005] * len(dates),
            "buy_hold_QQQ": [0.0007] * len(dates),
        }
    )


def test_build_health_report_for_discovered_return_matrix(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "global_etf_research"
    artifact_dir.mkdir()
    returns_path = artifact_dir / "portfolio_and_tracker_returns.csv"
    _returns().to_csv(returns_path, index=False)

    assert discover_return_matrices(tmp_path) == [returns_path]
    assert infer_strategy_columns(_returns(), primary_benchmark="buy_hold_SPY") == (
        "global_etf_rotation",
        "monthly_variant",
    )

    output_dir = build_health_report_for_returns(
        returns_path,
        output_root=tmp_path,
        primary_benchmark="buy_hold_SPY",
    )

    assert output_dir == tmp_path / "live_strategy_health_global_etf_research"
    assert (output_dir / "strategy_health_summary.csv").exists()
    assert (output_dir / "strategy_health_windows.csv").exists()
    manifest = json.loads((output_dir / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["artifact_type"] == "live_strategy_health_report"
    assert manifest["date_column"] == "as_of"
    assert manifest["strategies"] == ["global_etf_rotation", "monthly_variant"]


def test_build_health_report_supports_date_column(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "date_column_research"
    artifact_dir.mkdir()
    returns_path = artifact_dir / "portfolio_and_tracker_returns.csv"
    returns = _returns().rename(columns={"as_of": "date"})
    returns.to_csv(returns_path, index=False)

    output_dir = build_health_report_for_returns(
        returns_path,
        output_root=tmp_path,
        primary_benchmark="buy_hold_SPY",
    )

    assert output_dir == tmp_path / "live_strategy_health_date_column_research"
    manifest = json.loads((output_dir / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["date_column"] == "date"


def test_build_health_report_skips_when_primary_benchmark_is_missing(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "missing_benchmark"
    artifact_dir.mkdir()
    returns_path = artifact_dir / "portfolio_and_tracker_returns.csv"
    _returns().drop(columns=["buy_hold_SPY"]).to_csv(returns_path, index=False)

    output_dir = build_health_report_for_returns(
        returns_path,
        output_root=tmp_path,
        primary_benchmark="buy_hold_SPY",
    )

    assert output_dir is None
    assert not (tmp_path / "live_strategy_health_missing_benchmark").exists()


def test_resolve_date_column_requires_as_of_or_date() -> None:
    assert resolve_date_column(_returns()) == "as_of"
    assert resolve_date_column(_returns().rename(columns={"as_of": "date"})) == "date"
    try:
        resolve_date_column(_returns().drop(columns=["as_of"]))
    except ValueError as exc:
        assert "as_of or date" in str(exc)
    else:
        raise AssertionError("missing date column should fail")


def test_main_writes_error_when_return_matrix_has_no_date_column(tmp_path: Path, monkeypatch, capsys) -> None:
    bad_dir = tmp_path / "bad_research"
    bad_dir.mkdir()
    _returns().drop(columns=["as_of"]).to_csv(bad_dir / "portfolio_and_tracker_returns.csv", index=False)
    output_root = tmp_path / "health"
    monkeypatch.setattr(
        "sys.argv",
        [
            "build_monthly_live_strategy_health_reports.py",
            "--artifact-root",
            str(tmp_path),
            "--output-root",
            str(output_root),
        ],
    )

    assert main() == 0

    captured = capsys.readouterr()
    error_json_path = output_root / "live_strategy_health_error_bad_research" / "strategy_health_error.json"
    error_json = json.loads(error_json_path.read_text(encoding="utf-8"))
    assert "health_report_count=0" in captured.out
    assert "health_report_error_count=1" in captured.out
    assert error_json["error_type"] == "ValueError"
    assert "as_of or date" in error_json["error_message"]


def test_main_prints_zero_when_no_return_matrices(tmp_path: Path, monkeypatch, capsys) -> None:
    output_root = tmp_path / "health"
    monkeypatch.setattr(
        "sys.argv",
        [
            "build_monthly_live_strategy_health_reports.py",
            "--artifact-root",
            str(tmp_path),
            "--output-root",
            str(output_root),
        ],
    )

    assert main() == 0

    captured = capsys.readouterr()
    assert "health_report_count=0" in captured.out
    assert output_root.exists()


def test_build_health_error_report_writes_error_artifacts(tmp_path: Path) -> None:
    returns_path = tmp_path / "bad_research" / "portfolio_and_tracker_returns.csv"
    returns_path.parent.mkdir()

    output_dir = build_health_error_report(
        returns_path,
        output_root=tmp_path,
        error=ValueError("bad matrix"),
    )

    error_json = json.loads((output_dir / "strategy_health_error.json").read_text(encoding="utf-8"))
    error_md = (output_dir / "strategy_health_error.md").read_text(encoding="utf-8")
    assert output_dir == tmp_path / "live_strategy_health_error_bad_research"
    assert error_json["artifact_type"] == "live_strategy_health_error"
    assert error_json["error_type"] == "ValueError"
    assert error_json["error_message"] == "bad matrix"
    assert "bad matrix" in error_md


def test_main_continues_when_one_return_matrix_fails(tmp_path: Path, monkeypatch, capsys) -> None:
    good_dir = tmp_path / "good_research"
    bad_dir = tmp_path / "bad_research"
    good_dir.mkdir()
    bad_dir.mkdir()
    _returns().to_csv(good_dir / "portfolio_and_tracker_returns.csv", index=False)
    _returns().to_csv(bad_dir / "portfolio_and_tracker_returns.csv", index=False)
    output_root = tmp_path / "health"

    def fake_build_health_report_for_returns(returns_path: Path, *, output_root: Path, primary_benchmark: str):
        if returns_path.parent.name == "bad_research":
            raise ValueError("bad matrix")
        output_dir = output_root / "live_strategy_health_good_research"
        output_dir.mkdir(parents=True)
        return output_dir

    monkeypatch.setattr(
        "scripts.build_monthly_live_strategy_health_reports.build_health_report_for_returns",
        fake_build_health_report_for_returns,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "build_monthly_live_strategy_health_reports.py",
            "--artifact-root",
            str(tmp_path),
            "--output-root",
            str(output_root),
        ],
    )

    assert main() == 0

    captured = capsys.readouterr()
    assert "health_report_count=1" in captured.out
    assert "health_report_error_count=1" in captured.out
    assert (output_root / "live_strategy_health_error_bad_research" / "strategy_health_error.json").exists()
