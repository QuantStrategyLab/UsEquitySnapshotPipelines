from __future__ import annotations

import json

import pandas as pd

from us_equity_snapshot_pipelines.soxl_soxx_trend_income_archive import archive_backtest


def _fake_yfinance_download(symbols, *, start, end, auto_adjust, progress, threads):
    del end, progress, threads
    assert auto_adjust is True
    dates = pd.bdate_range(start, periods=520)
    columns = pd.MultiIndex.from_product([["Open", "High", "Low", "Close", "Volume"], symbols])
    frame = pd.DataFrame(index=dates, columns=columns, dtype=float)
    for idx, symbol in enumerate(symbols):
        base = 50.0 + idx * 25.0
        step = 0.15 + idx * 0.03
        closes = [base + row_idx * step for row_idx in range(len(dates))]
        frame[("Open", symbol)] = [close - 0.05 for close in closes]
        frame[("High", symbol)] = [close + 0.25 for close in closes]
        frame[("Low", symbol)] = [close - 0.25 for close in closes]
        frame[("Close", symbol)] = closes
        frame[("Volume", symbol)] = 1_000_000.0 + idx
    return frame


def _sample_archive_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2020-01-01", periods=520)
    rows = []
    for idx, as_of in enumerate(dates):
        for symbol, base, step in (
            ("SOXL", 50.0, 0.35),
            ("SOXX", 100.0, 0.20),
            ("BOXX", 80.0, 0.01),
        ):
            rows.append({"symbol": symbol, "as_of": as_of, "close": base + idx * step})
    return pd.DataFrame(rows)


def test_archive_core_long_download_writes_replayable_manifest(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("HTTPS_PROXY", "http://user:secret@example.test:8080")
    archive_dir = archive_backtest(
        mode="core-long",
        output_dir=tmp_path,
        price_start="2020-01-01",
        start_date="2020-01-01",
        download_fn=_fake_yfinance_download,
        sanitized_argv=["--mode", "core-long", "--download", "--proxy", "<redacted>"],
    )

    assert (archive_dir / "price_history.csv").exists()
    assert (archive_dir / "summary.csv").exists()
    assert (archive_dir / "backtest_config.json").exists()
    assert (archive_dir / "data_quality_report.csv").exists()
    assert (archive_dir / "source_manifest.json").exists()

    prices = pd.read_csv(archive_dir / "price_history.csv")
    assert {"SOXL", "SOXX", "BOXX"} <= set(prices["symbol"])
    assert {"open", "high", "low", "close", "volume"} <= set(prices.columns)
    assert prices["high"].notna().any()

    quality = pd.read_csv(archive_dir / "data_quality_report.csv")
    boxx_quality = quality.loc[quality["symbol"].eq("BOXX")].iloc[0]
    assert boxx_quality["download_symbol"] == "BIL"

    manifest_text = (archive_dir / "source_manifest.json").read_text(encoding="utf-8")
    manifest = json.loads(manifest_text)
    assert manifest["source_kind"] == "yfinance"
    assert manifest["price_source"]["auto_adjust"] is True
    assert manifest["price_source"]["proxy_used"] is True
    assert manifest["price_source"]["symbol_aliases"] == {"BOXX": ["BIL"]}
    assert "secret" not in manifest_text


def test_archive_local_prices_do_not_report_proxy_usage(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("YFINANCE_PROXY", "http://user:secret@example.test:8080")
    archive_dir = archive_backtest(
        mode="core-long",
        output_dir=tmp_path,
        prices=_sample_archive_prices(),
        start_date="2020-01-01",
    )

    manifest_text = (archive_dir / "source_manifest.json").read_text(encoding="utf-8")
    manifest = json.loads(manifest_text)
    assert manifest["source_kind"] == "local_prices"
    assert manifest["price_source"]["proxy_used"] is False
    assert "secret" not in manifest_text
