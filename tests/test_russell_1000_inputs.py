from __future__ import annotations

import json

import pandas as pd

from us_equity_snapshot_pipelines.russell_1000_history import (
    parse_ishares_holdings_json_snapshot,
    resolve_ishares_holdings_snapshot,
)
from us_equity_snapshot_pipelines import russell_1000_inputs
from us_equity_snapshot_pipelines.russell_1000_inputs import (
    collect_download_symbols,
    incremental_start_date,
    load_symbol_alias_table,
    merge_price_history,
    normalize_price_history,
    split_symbols,
)


def test_split_symbols_normalizes_and_dedupes() -> None:
    assert split_symbols(" qqq, SPY,qqq,,boxx ") == ("QQQ", "SPY", "BOXX")


def test_collect_download_symbols_adds_strategy_extras() -> None:
    universe_history = pd.DataFrame(
        [
            {"symbol": "AAPL", "sector": "Information Technology", "start_date": "2026-01-01", "end_date": None},
            {"symbol": "MSFT", "sector": "Information Technology", "start_date": "2026-01-01", "end_date": None},
        ]
    )

    symbols = collect_download_symbols(
        universe_history,
        benchmark_symbol="QQQ",
        safe_haven="BOXX",
        extra_symbols=("SPY", "QQQ"),
    )

    assert symbols[:2] == ["AAPL", "MSFT"]
    assert {"QQQ", "SPY", "BOXX"}.issubset(symbols)
    assert symbols.count("QQQ") == 1


def test_incremental_start_respects_overlap_and_floor() -> None:
    existing = pd.DataFrame(
        [
            {"symbol": "QQQ", "as_of": "2026-04-01", "close": 100.0, "volume": 1},
            {"symbol": "QQQ", "as_of": "2026-04-05", "close": 101.0, "volume": 1},
        ]
    )

    assert incremental_start_date(existing, requested_start="2026-01-01", overlap_days=3) == "2026-04-02"
    assert incremental_start_date(existing, requested_start="2026-04-04", overlap_days=10) == "2026-04-04"


def test_merge_price_history_keeps_latest_duplicate() -> None:
    old = pd.DataFrame(
        [
            {"symbol": "qqq", "as_of": "2026-04-01", "close": 100.0, "volume": 10},
            {"symbol": "QQQ", "as_of": "2026-04-02", "close": 101.0, "volume": 11},
        ]
    )
    update = pd.DataFrame(
        [
            {"symbol": "QQQ", "as_of": "2026-04-02", "close": 102.0, "volume": 12},
            {"symbol": "SPY", "as_of": "2026-04-02", "close": 200.0, "volume": 20},
        ]
    )

    merged = merge_price_history(old, update)

    assert list(merged["symbol"]) == ["QQQ", "QQQ", "SPY"]
    refreshed_qqq = merged.loc[
        (merged["symbol"] == "QQQ") & (merged["as_of"] == pd.Timestamp("2026-04-02")),
        "close",
    ].iloc[0]
    assert float(refreshed_qqq) == 102.0


def test_normalize_price_history_rejects_missing_columns() -> None:
    try:
        normalize_price_history(pd.DataFrame([{"symbol": "QQQ"}]))
    except ValueError as exc:
        assert "as_of" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_ishares_json_parser_preserves_rank_metrics() -> None:
    payload = {
        "aaData": [
            [
                "AAPL",
                "APPLE INC",
                "Information Technology",
                "Equity",
                {"display": "$100.00", "raw": 100.0},
                {"display": "5.00", "raw": 5.0},
                {"display": "$100.00", "raw": 100.0},
                {"display": "10.00", "raw": 10.0},
                "037833100",
                "US0378331005",
                "2046251",
                {"display": "10.00", "raw": 10.0},
                "United States",
                "NASDAQ",
                "USD",
                "1.00",
                "-",
            ]
        ]
    }

    as_of, snapshot = parse_ishares_holdings_json_snapshot(json.dumps(payload), as_of_date="2024-01-31")

    assert as_of == pd.Timestamp("2024-01-31")
    row = snapshot.iloc[0]
    assert row["symbol"] == "AAPL"
    assert row["weight"] == 5.0
    assert row["market_value"] == 100.0
    assert row["shares"] == 10.0
    assert row["price"] == 10.0


def test_ishares_json_parser_rejects_html_response() -> None:
    try:
        parse_ishares_holdings_json_snapshot("<!DOCTYPE html><html></html>", as_of_date="2026-05-31")
    except ValueError as exc:
        assert "returned HTML" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_resolve_ishares_holdings_snapshot_skips_failed_candidate() -> None:
    calls: list[pd.Timestamp] = []

    def fake_download(as_of_date, *, holdings_url_template):
        snapshot_date = pd.Timestamp(as_of_date).normalize()
        calls.append(snapshot_date)
        if len(calls) == 1:
            raise ValueError("upstream returned HTML")
        return snapshot_date, pd.DataFrame([{"symbol": "AAPL", "sector": "Information Technology"}])

    record = resolve_ishares_holdings_snapshot(
        "2026-05-31",
        max_lookback_days=2,
        holdings_url_template="https://example.test/{as_of_date}.json",
        download_fn=fake_download,
    )

    assert record["lookback_days"] == 1
    assert record["as_of_date"] == pd.Timestamp("2026-05-30")
    assert [f"{date:%Y-%m-%d}" for date in calls] == ["2026-05-31", "2026-05-30"]



def test_prepare_russell_1000_input_data_writes_latest_weighted_snapshot(tmp_path, monkeypatch) -> None:
    snapshots = [
        (
            pd.Timestamp("2026-03-31"),
            pd.DataFrame(
                [
                    {"symbol": "AAPL", "sector": "Information Technology", "weight": 5.0, "market_value": 100.0},
                ]
            ),
        ),
        (
            pd.Timestamp("2026-04-30"),
            pd.DataFrame(
                [
                    {"symbol": "MSFT", "sector": "Information Technology", "weight": 6.0, "market_value": 120.0},
                ]
            ),
        ),
    ]

    def fake_download_snapshots(**_kwargs):
        return snapshots, pd.DataFrame([{"snapshot_date": "2026-04-30", "status": "ok"}])

    def fake_download_prices(symbols, *, start, **_kwargs):
        return pd.DataFrame(
            [
                {"symbol": symbol, "as_of": start, "close": 100.0, "volume": 1_000}
                for symbol in symbols
            ]
        )

    monkeypatch.setattr(
        russell_1000_inputs,
        "download_ishares_historical_universe_snapshots",
        fake_download_snapshots,
    )
    monkeypatch.setattr(russell_1000_inputs, "download_price_history", fake_download_prices)

    result = russell_1000_inputs.prepare_russell_1000_input_data(
        output_dir=tmp_path,
        universe_start="2026-03-01",
        price_start="2026-01-01",
        extra_symbols=("QQQ",),
    )

    latest = pd.read_csv(result.latest_snapshot_output_path)

    assert result.latest_snapshot_output_path.name == "r1000_latest_holdings_snapshot.csv"
    assert latest.loc[0, "symbol"] == "MSFT"
    assert float(latest.loc[0, "weight"]) == 6.0
    assert float(latest.loc[0, "market_value"]) == 120.0


def test_load_symbol_alias_table_groups_candidates_by_priority(tmp_path) -> None:
    path = tmp_path / "aliases.csv"
    pd.DataFrame(
        [
            {"symbol": "abc", "download_candidate": "ABC", "priority": 2},
            {"symbol": "ABC", "download_candidate": "COR", "priority": 1},
            {"symbol": "XYZ", "download_candidate": "XYZ", "priority": 1},
        ]
    ).to_csv(path, index=False)

    aliases = load_symbol_alias_table(path)

    assert aliases["ABC"] == ["COR", "ABC"]
    assert aliases["XYZ"] == ["XYZ"]


def test_prepare_russell_1000_input_data_reuses_existing_universe_when_refresh_fails(
    tmp_path,
    monkeypatch,
) -> None:
    existing_dir = tmp_path / "current"
    existing_dir.mkdir()
    pd.DataFrame(
        [
            {"symbol": "AAPL", "sector": "Information Technology", "start_date": "2026-04-30", "end_date": ""},
            {"symbol": "MSFT", "sector": "Information Technology", "start_date": "2026-04-30", "end_date": ""},
        ]
    ).to_csv(existing_dir / "r1000_universe_history.csv", index=False)
    pd.DataFrame([{"requested_date": "2026-05-01", "as_of_date": "2026-04-29"}]).to_csv(
        existing_dir / "r1000_universe_snapshot_metadata.csv",
        index=False,
    )
    pd.DataFrame([{"symbol": "AAPL", "sector": "Information Technology", "weight": 5.0}]).to_csv(
        existing_dir / "r1000_latest_holdings_snapshot.csv",
        index=False,
    )
    pd.DataFrame(
        [
            {"symbol": "AAPL", "download_candidate": "AAPL", "priority": 1},
            {"symbol": "MSFT", "download_candidate": "MSFT", "priority": 1},
        ]
    ).to_csv(existing_dir / "r1000_symbol_aliases.csv", index=False)
    existing_prices = tmp_path / "prices.csv"
    pd.DataFrame([{"symbol": "AAPL", "as_of": "2026-04-30", "close": 100.0, "volume": 1_000}]).to_csv(
        existing_prices,
        index=False,
    )
    observed: dict[str, object] = {"starts": [], "symbol_calls": []}

    def fail_download_snapshots(**_kwargs):
        raise RuntimeError("upstream returned HTML")

    def fake_download_prices(symbols, *, start, symbol_aliases, **_kwargs):
        observed["symbol_calls"].append(tuple(symbols))
        observed["starts"].append(start)
        observed["symbol_aliases"] = dict(symbol_aliases)
        return pd.DataFrame(
            [
                {"symbol": "AAPL", "as_of": "2026-05-30", "close": 110.0, "volume": 2_000},
                {"symbol": "MSFT", "as_of": "2026-05-30", "close": 210.0, "volume": 3_000},
            ]
        )

    monkeypatch.setattr(
        russell_1000_inputs,
        "download_ishares_historical_universe_snapshots",
        fail_download_snapshots,
    )
    monkeypatch.setattr(russell_1000_inputs, "download_price_history", fake_download_prices)

    result = russell_1000_inputs.prepare_russell_1000_input_data(
        output_dir=tmp_path / "out",
        universe_start="2026-05-01",
        existing_input_dir=existing_dir,
        existing_prices_path=existing_prices,
        price_start="2018-01-01",
        extra_symbols=("QQQ",),
    )

    refreshed_prices = pd.read_csv(result.price_history_path)

    assert result.universe_fallback_used is True
    assert observed["starts"][0] == "2026-04-23"
    assert "2018-01-01" in observed["starts"]
    assert set(observed["symbol_calls"][0]) >= {"AAPL", "MSFT", "QQQ", "BOXX"}
    assert observed["symbol_aliases"]["AAPL"] == ["AAPL"]
    assert refreshed_prices["as_of"].max() == "2026-05-30"
    assert (tmp_path / "out" / "r1000_universe_history.csv").exists()
    manifest = json.loads(result.source_manifest_output_path.read_text(encoding="utf-8"))
    assert manifest["source_input_status"] == "universe_fallback"
    assert manifest["universe_fallback_used"] is True
    assert manifest["fallback_streak"] == 1
    assert manifest["price_as_of"] == "2026-05-30"
    assert manifest["universe_as_of"] == "2026-04-29"
    assert "upstream returned HTML" in manifest["fallback_reason"]


def test_prepare_russell_1000_input_data_blocks_stale_repeated_universe_fallback(
    tmp_path,
    monkeypatch,
) -> None:
    existing_dir = tmp_path / "current"
    existing_dir.mkdir()
    pd.DataFrame(
        [{"symbol": "AAPL", "sector": "Information Technology", "start_date": "2026-04-30", "end_date": ""}]
    ).to_csv(existing_dir / "r1000_universe_history.csv", index=False)
    pd.DataFrame([{"requested_date": "2026-05-01", "as_of_date": "2026-04-29"}]).to_csv(
        existing_dir / "r1000_universe_snapshot_metadata.csv",
        index=False,
    )
    pd.DataFrame([{"symbol": "AAPL", "sector": "Information Technology", "weight": 5.0}]).to_csv(
        existing_dir / "r1000_latest_holdings_snapshot.csv",
        index=False,
    )
    pd.DataFrame([{"symbol": "AAPL", "download_candidate": "AAPL", "priority": 1}]).to_csv(
        existing_dir / "r1000_symbol_aliases.csv",
        index=False,
    )
    previous_manifest = tmp_path / "r1000_source_input_manifest.json"
    previous_manifest.write_text(
        json.dumps({"universe_fallback_used": True, "fallback_streak": 1}),
        encoding="utf-8",
    )

    def fail_download_snapshots(**_kwargs):
        raise RuntimeError("upstream returned HTML again")

    monkeypatch.setattr(
        russell_1000_inputs,
        "download_ishares_historical_universe_snapshots",
        fail_download_snapshots,
    )

    try:
        russell_1000_inputs.prepare_russell_1000_input_data(
            output_dir=tmp_path / "out",
            universe_start="2026-05-01",
            existing_input_dir=existing_dir,
            existing_source_manifest_path=previous_manifest,
            max_universe_fallback_streak=1,
            price_start="2018-01-01",
        )
    except RuntimeError as exc:
        assert "universe_fallback_streak_exceeded" in str(exc)
    else:
        raise AssertionError("expected repeated universe fallback to fail closed")
