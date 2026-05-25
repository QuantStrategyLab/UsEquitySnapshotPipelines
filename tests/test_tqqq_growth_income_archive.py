from __future__ import annotations

import json

import pandas as pd

from us_equity_snapshot_pipelines.tqqq_growth_income_archive import MANAGED_SYMBOLS, archive_backtest, run_backtest


def _sample_tqqq_prices(*, include_income: bool = False) -> pd.DataFrame:
    dates = pd.bdate_range("2009-01-01", periods=520)
    rows = []
    for idx, as_of in enumerate(dates):
        qqq = 40.0 + idx * 0.08
        spy = 35.0 + idx * 0.05
        tqqq = 10.0 + idx * 0.06
        boxx = 80.0 + idx * 0.005
        symbol_prices = [("TQQQ", tqqq), ("QQQ", qqq), ("BOXX", boxx), ("SPY", spy)]
        if include_income:
            symbol_prices.extend(
                [
                    ("SCHD", 25.0 + idx * 0.015),
                    ("DGRO", 22.0 + idx * 0.012),
                    ("SGOV", 100.0 + idx * 0.002),
                    ("SPYI", 45.0 + idx * 0.010),
                    ("QQQI", 48.0 + idx * 0.018),
                ]
            )
        for symbol, close in symbol_prices:
            rows.append({"symbol": symbol, "as_of": as_of, "close": close})
    return pd.DataFrame(rows)


def test_tqqq_growth_income_run_backtest_produces_summary() -> None:
    result = run_backtest(
        _sample_tqqq_prices(),
        start_date="2010-01-04",
        end_date="2010-12-31",
    )

    assert result["summary"]["Start"] >= "2010-01-04"
    assert result["summary"]["Max Drawdown"] <= 0
    assert not result["signal_history"].empty
    assert not result["trades"].empty


def test_tqqq_growth_income_run_backtest_can_open_income_layer() -> None:
    result = run_backtest(
        _sample_tqqq_prices(include_income=True),
        initial_equity=225000.0,
        start_date="2010-01-04",
        end_date="2010-12-31",
    )

    income_weights = result["weights_history"].reindex(columns=MANAGED_SYMBOLS, fill_value=0.0)[
        ["SCHD", "DGRO", "SGOV", "SPYI", "QQQI"]
    ]
    assert income_weights.sum(axis=1).max() > 0.0
    assert "income_layer_ratio" in result["signal_history"].columns
    assert "income_layer_loss_budget_cap_ratio" in result["signal_history"].columns


def test_tqqq_growth_income_archive_writes_replayable_outputs(tmp_path) -> None:
    archive_dir = archive_backtest(
        mode="real-core",
        output_dir=tmp_path,
        prices=_sample_tqqq_prices(),
        start_date="2010-01-04",
        end_date="2010-12-31",
    )

    assert (archive_dir / "summary.csv").exists()
    assert (archive_dir / "window_summary.csv").exists()
    assert (archive_dir / "signal_history.csv").exists()
    assert (archive_dir / "source_manifest.json").exists()

    window_summary = pd.read_csv(archive_dir / "window_summary.csv")
    assert "long_15y_to_date" in set(window_summary["Window"])
    assert {"QQQ Max Drawdown", "SPY Max Drawdown", "Within Worst Benchmark Drawdown"} <= set(
        window_summary.columns
    )

    manifest = json.loads((archive_dir / "source_manifest.json").read_text(encoding="utf-8"))
    assert manifest["strategy_profile"] == "tqqq_growth_income"
    assert manifest["backtest"]["disable_income_layer"] is True
    assert "window_summary" in manifest["artifacts"]


def test_tqqq_growth_income_archive_supports_full_income_mode(tmp_path) -> None:
    archive_dir = archive_backtest(
        mode="real-full",
        output_dir=tmp_path,
        prices=_sample_tqqq_prices(include_income=True),
        initial_equity=225000.0,
        start_date="2010-01-04",
        end_date="2010-12-31",
    )

    manifest = json.loads((archive_dir / "source_manifest.json").read_text(encoding="utf-8"))
    assert manifest["backtest"]["disable_income_layer"] is False
    assert manifest["price_source"]["requested_symbols"] == [*MANAGED_SYMBOLS, "SPY"]

    weights = pd.read_csv(archive_dir / "weights_history.csv")
    assert weights[["SCHD", "DGRO", "SGOV", "SPYI", "QQQI"]].sum(axis=1).max() > 0.0
