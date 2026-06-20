from __future__ import annotations

import json
import tomllib
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines.ibit_smart_dca_research import (
    build_ibit_dca_review_summary,
    build_ibit_smart_dca_research,
    render_ibit_dca_research_report,
    write_ibit_smart_dca_research_outputs,
)


def _price_rows() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=80)
    rows: list[dict[str, object]] = []
    for idx, as_of in enumerate(dates):
        rows.append({"as_of": as_of, "symbol": "IBIT", "close": 100.0 + idx * 0.5})
        rows.append({"as_of": as_of, "symbol": "BOXX", "close": 100.0 + idx * 0.02})
        rows.append({"as_of": as_of, "symbol": "QQQ", "close": 100.0 + idx * 0.3})
        rows.append({"as_of": as_of, "symbol": "SPY", "close": 100.0 + idx * 0.2})
    return pd.DataFrame(rows)


def _zscore_history(*, high_last: bool = True) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=80)
    rows = []
    for idx, as_of in enumerate(dates):
        zscore = 2.0 + (idx % 10) * 0.05
        if high_last and idx >= 60:
            zscore = 9.5
        rows.append({"as_of": as_of, "mvrv_zscore": zscore})
    return pd.DataFrame(rows)


def test_buy_only_dca_sells_parking_asset_to_fund_ibit_buys() -> None:
    result = build_ibit_smart_dca_research(
        _price_rows(),
        initial_parking_value=1_000.0,
        contribution_amount=0.0,
        rebalance_frequency="MS",
        plugin_enabled=False,
    )

    trade_ledger = result["ibit_dca_trade_ledger"]
    signal_consumption = result["ibit_dca_signal_consumption"]
    summary = result["ibit_dca_live_readiness_summary"]

    buy_only_signals = signal_consumption.loc[signal_consumption["variant"].eq("buy_only_dca")]
    assert "plugin_on" not in set(signal_consumption["variant"])
    assert set(buy_only_signals["canonical_route"]) == {"plugin_disabled"}
    assert not trade_ledger.empty
    assert {
        (row["action"], row["symbol"])
        for _, row in trade_ledger.iterrows()
    } >= {("sell", "BOXX"), ("buy", "IBIT")}
    assert bool(summary.loc[summary["variant"].eq("buy_only_dca"), "plugin_enabled"].iloc[0]) is False


def test_plugin_enabled_consumes_zscore_signal_and_keeps_defensive_parking() -> None:
    result = build_ibit_smart_dca_research(
        _price_rows(),
        zscore_history=_zscore_history(high_last=True),
        initial_parking_value=10_000.0,
        contribution_amount=0.0,
        rebalance_frequency="MS",
        plugin_enabled=True,
        plugin_config={
            "dynamic_lookback_days": 90,
            "dynamic_min_periods": 5,
            "soft_exit_percentile": 0.80,
            "hard_exit_percentile": 0.90,
            "soft_exit_zscore_floor": 2.0,
            "hard_exit_zscore_floor": 2.5,
            "risk_off_ibit_exposure": 0.25,
            "parking_symbol": "BOXX",
        },
    )

    signal_consumption = result["ibit_dca_signal_consumption"]
    latest_plugin_signal = signal_consumption.loc[signal_consumption["variant"].eq("plugin_on")].iloc[-1]
    assert latest_plugin_signal["canonical_route"] == "risk_off"
    assert latest_plugin_signal["target_ibit_exposure"] == 0.25
    assert latest_plugin_signal["target_parking_exposure"] == 0.75

    latest_holdings = result["ibit_dca_holdings_ledger"].loc[
        result["ibit_dca_holdings_ledger"]["variant"].eq("plugin_on")
    ].iloc[-1]
    assert latest_holdings["parking_weight"] > 0.70
    assert latest_holdings["ibit_weight"] < 0.30


def test_write_ibit_smart_dca_research_outputs_writes_manifest(tmp_path) -> None:
    result = build_ibit_smart_dca_research(
        _price_rows(),
        zscore_history=_zscore_history(high_last=True),
        initial_parking_value=1_000.0,
        contribution_amount=100.0,
        plugin_enabled=True,
        plugin_config={"dynamic_min_periods": 5},
    )

    outputs = write_ibit_smart_dca_research_outputs(result, tmp_path)

    assert (tmp_path / "ibit_dca_period_summary.csv").exists()
    assert (tmp_path / "ibit_dca_trade_ledger.csv").exists()
    assert (tmp_path / "ibit_dca_signal_consumption.csv").exists()
    assert (tmp_path / "ibit_dca_live_readiness_summary.csv").exists()
    assert (tmp_path / "ibit_dca_research_report.md").exists()
    manifest = json.loads((tmp_path / "ibit_dca_research_manifest.json").read_text(encoding="utf-8"))
    assert manifest["manifest_type"] == "ibit_smart_dca_research"
    assert manifest["artifact_schema_version"] == "ibit_smart_dca_research.v1"
    assert set(manifest["artifacts"]) >= {
        "ibit_dca_period_summary",
        "ibit_dca_trade_ledger",
        "ibit_dca_signal_consumption",
        "ibit_dca_live_readiness_summary",
        "ibit_dca_research_report",
    }
    assert manifest["review_summary"]["plugin_gate"] in {"pass", "fail"}
    assert manifest["review_summary"]["runtime_impact"] == "none"
    assert outputs["manifest"].name == "ibit_dca_research_manifest.json"


def test_render_ibit_dca_research_report_summarizes_plugin_gate() -> None:
    result = build_ibit_smart_dca_research(
        _price_rows(),
        zscore_history=_zscore_history(high_last=True),
        initial_parking_value=1_000.0,
        contribution_amount=100.0,
        plugin_enabled=True,
        plugin_config={"dynamic_min_periods": 5},
    )

    markdown = render_ibit_dca_research_report(result)

    assert "# IBIT Smart DCA Research Report" in markdown
    assert "Runtime impact: `none`" in markdown
    assert "plugin_on" in markdown
    assert "buy_only_dca" in markdown
    assert "Promotion checklist" in markdown


def test_build_ibit_dca_review_summary_is_machine_readable() -> None:
    result = build_ibit_smart_dca_research(
        _price_rows(),
        zscore_history=_zscore_history(high_last=True),
        initial_parking_value=1_000.0,
        contribution_amount=100.0,
        plugin_enabled=True,
        plugin_config={"dynamic_min_periods": 5},
    )

    summary = build_ibit_dca_review_summary(result)

    assert summary["runtime_impact"] == "none"
    assert summary["plugin_gate"] in {"pass", "fail"}
    assert summary["review_status"] in {"candidate_for_live_promotion_review", "research_reject_or_continue"}
    assert summary["plugin_signal_count"] > 0
    assert isinstance(summary["plugin_route_counts"], dict)
    assert summary["plugin_non_normal_signal_count"] >= 0
    assert summary["zscore_history_rows"] == 80
    assert summary["zscore_history_start"] == "2024-01-02"
    assert summary["zscore_history_end"] == "2024-04-22"
    assert "cagr_delta_vs_buy_only" in summary


def test_contributions_do_not_create_fake_cagr_when_prices_are_flat() -> None:
    dates = pd.bdate_range("2024-01-02", periods=80)
    rows = []
    for as_of in dates:
        for symbol in ("IBIT", "BOXX", "QQQ", "SPY"):
            rows.append({"as_of": as_of, "symbol": symbol, "close": 100.0})

    result = build_ibit_smart_dca_research(
        pd.DataFrame(rows),
        initial_parking_value=1_000.0,
        contribution_amount=500.0,
        rebalance_frequency="MS",
        turnover_cost_bps=0.0,
        plugin_enabled=False,
    )

    period_summary = result["ibit_dca_period_summary"]
    buy_only = period_summary.loc[period_summary["variant"].eq("buy_only_dca")].iloc[0]
    assert abs(buy_only["cagr"]) < 1e-9
    assert buy_only["ending_nav"] > 1_000.0


def test_period_summary_includes_benchmark_relative_metrics() -> None:
    result = build_ibit_smart_dca_research(
        _price_rows(),
        initial_parking_value=1_000.0,
        contribution_amount=100.0,
        rebalance_frequency="MS",
        plugin_enabled=False,
    )

    period_summary = result["ibit_dca_period_summary"]
    readiness = result["ibit_dca_live_readiness_summary"]
    for column in (
        "primary_benchmark_cagr",
        "secondary_benchmark_cagr",
        "excess_cagr_vs_primary",
        "excess_cagr_vs_secondary",
    ):
        assert column in period_summary.columns
        assert column in readiness.columns


def test_btc_proxy_backfills_ibit_before_fund_inception() -> None:
    dates = pd.bdate_range("2023-12-01", periods=50)
    rows: list[dict[str, object]] = []
    for idx, as_of in enumerate(dates):
        rows.append({"as_of": as_of, "symbol": "BTC", "close": 40_000.0 + idx * 100.0})
        rows.append({"as_of": as_of, "symbol": "BOXX", "close": 100.0 + idx * 0.01})
        if idx >= 25:
            rows.append({"as_of": as_of, "symbol": "IBIT", "close": 25.0 + (idx - 25) * 0.2})

    result = build_ibit_smart_dca_research(
        pd.DataFrame(rows),
        btc_proxy_symbol="BTC",
        initial_parking_value=1_000.0,
        contribution_amount=0.0,
        rebalance_frequency="MS",
        plugin_enabled=False,
    )

    holdings = result["ibit_dca_holdings_ledger"]
    first_buy = result["ibit_dca_trade_ledger"].loc[
        lambda frame: frame["symbol"].eq("IBIT") & frame["action"].eq("buy")
    ].iloc[0]
    manifest_inputs = result["manifest_inputs"]

    assert first_buy["as_of"] < str(dates[25].date())
    assert holdings["nav"].notna().all()
    assert manifest_inputs["proxy"]["btc_proxy_symbol"] == "BTC"
    assert manifest_inputs["proxy"]["proxy_rows_filled"] > 0


def test_initial_parking_value_waits_for_first_valid_parking_price() -> None:
    dates = pd.bdate_range("2022-06-01", periods=160)
    rows: list[dict[str, object]] = []
    for idx, as_of in enumerate(dates):
        rows.append({"as_of": as_of, "symbol": "BTC", "close": 30_000.0 + idx * 50.0})
        rows.append({"as_of": as_of, "symbol": "QQQ", "close": 300.0 + idx * 0.2})
        rows.append({"as_of": as_of, "symbol": "SPY", "close": 400.0 + idx * 0.1})
        if idx >= 60:
            rows.append({"as_of": as_of, "symbol": "BOXX", "close": 100.0 + idx * 0.01})
        if idx >= 120:
            rows.append({"as_of": as_of, "symbol": "IBIT", "close": 25.0 + (idx - 120) * 0.2})

    result = build_ibit_smart_dca_research(
        pd.DataFrame(rows),
        btc_proxy_symbol="BTC",
        initial_parking_value=10_000.0,
        contribution_amount=0.0,
        rebalance_frequency="MS",
        plugin_enabled=False,
    )

    holdings = result["ibit_dca_holdings_ledger"]
    period_summary = result["ibit_dca_period_summary"]

    assert holdings["nav"].notna().all()
    assert holdings["parking_value"].notna().all()
    assert period_summary.loc[period_summary["variant"].eq("buy_only_dca"), "observations"].iloc[0] > 0


def test_plugin_live_gate_requires_beating_parking_only_baseline() -> None:
    prices = _price_rows()
    result = build_ibit_smart_dca_research(
        prices,
        zscore_history=_zscore_history(high_last=True),
        initial_parking_value=10_000.0,
        contribution_amount=0.0,
        rebalance_frequency="MS",
        plugin_enabled=True,
        plugin_config={
            "dynamic_lookback_days": 90,
            "dynamic_min_periods": 5,
            "soft_exit_percentile": 0.80,
            "hard_exit_percentile": 0.90,
            "soft_exit_zscore_floor": 2.0,
            "hard_exit_zscore_floor": 2.5,
            "risk_off_ibit_exposure": 0.0,
            "parking_symbol": "BOXX",
        },
    )

    readiness = result["ibit_dca_live_readiness_summary"]
    assert "parking_only" in set(readiness["variant"])
    plugin_row = readiness.loc[readiness["variant"].eq("plugin_on")].iloc[0]
    assert "cagr_delta_vs_parking_only" in readiness.columns
    if plugin_row["cagr_delta_vs_parking_only"] < 0:
        assert plugin_row["gate"] == "fail"


def test_plugin_live_gate_rejects_no_value_overlay() -> None:
    result = build_ibit_smart_dca_research(
        _price_rows(),
        zscore_history=_zscore_history(high_last=False),
        initial_parking_value=10_000.0,
        contribution_amount=0.0,
        rebalance_frequency="MS",
        plugin_enabled=True,
        plugin_config={"dynamic_min_periods": 5},
    )

    readiness = result["ibit_dca_live_readiness_summary"]
    plugin_row = readiness.loc[readiness["variant"].eq("plugin_on")].iloc[0]

    assert plugin_row["cagr_delta_vs_buy_only"] == 0.0
    assert plugin_row["drawdown_delta_vs_buy_only"] == 0.0
    assert plugin_row["gate"] == "fail"


def test_download_ibit_smart_dca_price_history_uses_btc_alias(monkeypatch) -> None:
    from us_equity_snapshot_pipelines import ibit_smart_dca_research as module

    calls: list[dict[str, object]] = []

    def fake_download(symbols, *, start, end, symbol_aliases=None, **_kwargs):
        calls.append({"symbols": tuple(symbols), "start": start, "end": end, "symbol_aliases": symbol_aliases})
        dates = pd.bdate_range("2024-01-02", periods=3)
        rows = []
        for idx, as_of in enumerate(dates):
            for symbol in symbols:
                rows.append({"as_of": as_of, "symbol": symbol, "close": 100.0 + idx})
        return pd.DataFrame(rows)

    monkeypatch.setattr(module, "download_price_history", fake_download)

    prices = module.download_ibit_smart_dca_price_history(
        start="2024-01-01",
        end="2024-02-01",
        ibit_symbol="IBIT",
        parking_symbol="BOXX",
        primary_benchmark="QQQ",
        secondary_benchmark="SPY",
        btc_proxy_symbol="BTC",
    )

    assert calls == [
        {
            "symbols": ("IBIT", "BOXX", "QQQ", "SPY", "BTC"),
            "start": "2024-01-01",
            "end": "2024-02-01",
            "symbol_aliases": {"BTC": ("BTC-USD",)},
        }
    ]
    assert set(prices["symbol"]) == {"IBIT", "BOXX", "QQQ", "SPY", "BTC"}


def test_ibit_smart_dca_research_entrypoint_is_registered() -> None:
    scripts = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))["project"]["scripts"]

    assert (
        scripts["useq-research-ibit-smart-dca"]
        == "us_equity_snapshot_pipelines.ibit_smart_dca_research:main"
    )
