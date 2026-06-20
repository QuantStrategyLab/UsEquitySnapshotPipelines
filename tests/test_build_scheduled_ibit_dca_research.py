from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.build_scheduled_ibit_dca_research import build_scheduled_ibit_dca_research, main


def _zscore_history() -> pd.DataFrame:
    rows = []
    for idx, as_of in enumerate(pd.bdate_range("2024-01-02", periods=80)):
        rows.append({"as_of": as_of.date().isoformat(), "mvrv_zscore": 2.0 + idx * 0.01})
    return pd.DataFrame(rows)


def _prices() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for idx, as_of in enumerate(pd.bdate_range("2024-01-02", periods=80)):
        for symbol, base, step in (
            ("IBIT", 25.0, 0.10),
            ("BOXX", 100.0, 0.01),
            ("QQQ", 400.0, 0.20),
            ("SPY", 450.0, 0.15),
            ("BTC", 40_000.0, 50.0),
        ):
            rows.append({"as_of": as_of, "symbol": symbol, "close": base + idx * step})
    return pd.DataFrame(rows)


def test_build_scheduled_ibit_dca_research_downloads_prices_and_writes_manifest(tmp_path: Path, monkeypatch) -> None:
    from scripts import build_scheduled_ibit_dca_research as module

    zscore_path = tmp_path / "ibit_zscore_metrics.csv"
    _zscore_history().to_csv(zscore_path, index=False)
    calls: list[dict[str, object]] = []

    def fake_download(**kwargs):
        calls.append(kwargs)
        return _prices()

    monkeypatch.setattr(module, "download_ibit_smart_dca_price_history", fake_download)

    manifest_path = build_scheduled_ibit_dca_research(
        zscore_metrics_path=zscore_path,
        output_dir=tmp_path / "research",
        price_start="2024-01-01",
        price_end="2024-05-01",
        initial_parking_value=10_000.0,
        contribution_amount=500.0,
        plugin_config={"dynamic_min_periods": 5},
    )

    assert calls == [
        {
            "start": "2024-01-01",
            "end": "2024-05-01",
            "ibit_symbol": "IBIT",
            "parking_symbol": "BOXX",
            "primary_benchmark": "QQQ",
            "secondary_benchmark": "SPY",
            "btc_proxy_symbol": "BTC",
            "proxy": None,
        }
    ]
    assert manifest_path == tmp_path / "research" / "ibit_dca_research_manifest.json"
    assert (tmp_path / "research" / "downloaded_price_history.csv").exists()
    assert (tmp_path / "research" / "ibit_dca_research_report.md").exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["manifest_type"] == "ibit_smart_dca_research"
    assert manifest["inputs"]["variants"] == ["parking_only", "buy_only_dca", "plugin_on"]
    assert "ibit_dca_research_report" in manifest["artifacts"]
    assert manifest["inputs"]["config"]["parking_symbol"] == "BOXX"
    assert manifest["inputs"]["config"]["btc_proxy_symbol"] == "BTC"


def test_main_prints_manifest_path(tmp_path: Path, monkeypatch, capsys) -> None:
    from scripts import build_scheduled_ibit_dca_research as module

    zscore_path = tmp_path / "ibit_zscore_metrics.csv"
    _zscore_history().to_csv(zscore_path, index=False)
    monkeypatch.setattr(module, "download_ibit_smart_dca_price_history", lambda **_kwargs: _prices())
    monkeypatch.setattr(
        "sys.argv",
        [
            "build_scheduled_ibit_dca_research.py",
            "--zscore-metrics",
            str(zscore_path),
            "--output-dir",
            str(tmp_path / "research"),
            "--price-start",
            "2024-01-01",
            "--price-end",
            "2024-05-01",
            "--plugin-config-json",
            '{"dynamic_min_periods": 5}',
        ],
    )

    assert main() == 0

    assert "ibit_dca_research_manifest=" in capsys.readouterr().out
    assert (tmp_path / "research" / "ibit_dca_research_manifest.json").exists()
    assert (tmp_path / "research" / "ibit_dca_research_report.md").exists()
