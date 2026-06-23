from __future__ import annotations

import json
import tempfile
import tomllib
import unittest
from datetime import date, timedelta
from pathlib import Path

from us_equity_snapshot_pipelines.leaps_growth_overlay_research import (
    LeapsProxyConfig,
    run_leaps_growth_overlay_option_chain_backtest,
    run_leaps_growth_overlay_proxy,
    write_research_outputs,
)


def _rows(
    *,
    symbol: str = "QQQ",
    periods: int = 140,
    start_price: float = 100.0,
    step: float = 1.0,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    start = date(2024, 1, 2)
    for index in range(periods):
        rows.append(
            {
                "as_of": (start + timedelta(days=index)).isoformat(),
                "symbol": symbol,
                "close": start_price + index * step,
            }
        )
    return rows


def _config(**overrides: object) -> LeapsProxyConfig:
    values = {
        "underlier": "QQQ",
        "initial_equity": 100_000.0,
        "premium_budget_ratio": 0.08,
        "dte_days": 90,
        "roll_dte_days": 30,
        "contract_multiplier": 1,
        "ma_window": 3,
        "momentum_window": 1,
        "realized_vol_window": 20,
        "vol_floor": 0.20,
        "vol_cap": 0.40,
        "risk_free_rate": 0.02,
        "min_dte_days": 60,
        "max_dte_days": 120,
        "max_bid_ask_spread_ratio": 0.20,
    }
    values.update(overrides)
    return LeapsProxyConfig(**values)


def _option_chain_rows(*, wide_spread: bool = False) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    start = date(2024, 1, 2)
    expiration = (start + timedelta(days=90)).isoformat()
    for index in range(90):
        bid = 10.0 + index * 0.2
        ask = bid + (10.0 if wide_spread else 0.5)
        rows.append(
            {
                "as_of": (start + timedelta(days=index)).isoformat(),
                "underlier": "QQQ",
                "expiration": expiration,
                "right": "C",
                "strike": 100.0,
                "bid": bid,
                "ask": ask,
                "delta": 0.75,
                "volume": 25,
                "open_interest": 500,
                "option_symbol": "QQQ240401C00100000",
            }
        )
    return rows


def _option_chain_rows_with_missing_marks() -> list[dict[str, object]]:
    rows = _option_chain_rows()
    return [
        row
        for row in rows
        if row["as_of"] in {"2024-01-02", "2024-01-03", "2024-01-04"}
        or row["as_of"] >= "2024-03-31"
    ]


class LeapsGrowthOverlayResearchTests(unittest.TestCase):
    def test_proxy_run_opens_leaps_intent_on_uptrend(self) -> None:
        result = run_leaps_growth_overlay_proxy(_rows(), _config())

        summary = result["summary"]
        daily_equity = result["daily_equity"]
        trades = result["trades"]

        self.assertEqual(summary["Underlier"], "QQQ")
        self.assertGreater(summary["Option Trade Count"], 0)
        self.assertIn("Black-Scholes proxy", summary["Proxy Warning"])
        self.assertGreater(summary["Final Equity"], 0.0)
        self.assertEqual(len(daily_equity), 140)
        self.assertIn("open_leaps", {trade["action"] for trade in trades})

    def test_downtrend_gate_skips_entries(self) -> None:
        result = run_leaps_growth_overlay_proxy(_rows(start_price=200.0, step=-1.0), _config())
        summary = result["summary"]

        self.assertEqual(summary["Option Trade Count"], 0)
        self.assertGreater(summary["Entry Skip Count"], 0)

    def test_write_outputs_marks_proxy_as_not_promotion_evidence(self) -> None:
        result = run_leaps_growth_overlay_proxy(_rows(), _config())

        with tempfile.TemporaryDirectory() as tmp_dir:
            write_research_outputs(result, tmp_dir)
            manifest = json.loads((Path(tmp_dir) / "manifest.json").read_text(encoding="utf-8"))

            self.assertEqual(manifest["schema_version"], "index_leaps_growth_overlay_research.v1")
            self.assertFalse(manifest["promotion_evidence"])
            self.assertIn("summary.csv", manifest["outputs"].values())
            self.assertIn("daily_equity.csv", manifest["outputs"].values())
            self.assertIn("trades.csv", manifest["outputs"].values())

    def test_option_chain_backtest_uses_real_bid_ask_quotes_as_promotion_evidence(self) -> None:
        result = run_leaps_growth_overlay_option_chain_backtest(
            _rows(periods=90),
            _option_chain_rows(),
            _config(),
        )
        summary = result["summary"]
        trades = result["trades"]

        self.assertEqual(summary["Data Mode"], "historical_option_chain")
        self.assertTrue(summary["Promotion Evidence"])
        self.assertEqual(summary["Missing Quote Count"], 0)
        self.assertIn("open_leaps", {trade["action"] for trade in trades})

    def test_option_chain_backtest_rejects_wide_spread_contracts(self) -> None:
        result = run_leaps_growth_overlay_option_chain_backtest(
            _rows(periods=90),
            _option_chain_rows(wide_spread=True),
            _config(max_bid_ask_spread_ratio=0.05),
        )
        summary = result["summary"]

        self.assertEqual(summary["Option Trade Count"], 0)
        self.assertGreater(summary["Entry Skip Count"], 0)
        self.assertFalse(summary["Promotion Evidence"])

    def test_option_chain_backtest_marks_missing_quotes_as_not_promotion_evidence(self) -> None:
        result = run_leaps_growth_overlay_option_chain_backtest(
            _rows(periods=30),
            _option_chain_rows_with_missing_marks(),
            _config(premium_budget_ratio=0.10),
        )
        summary = result["summary"]
        trades = result["trades"]
        daily_events = {row["event"] for row in result["daily_equity"]}

        self.assertFalse(summary["Promotion Evidence"])
        self.assertGreater(summary["Missing Quote Count"], 0)
        self.assertIn("missing_quote", daily_events)
        self.assertNotIn("recover_principal", {trade["action"] for trade in trades})

    def test_leaps_growth_overlay_entrypoint_is_registered(self) -> None:
        scripts = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))["project"]["scripts"]

        self.assertEqual(
            scripts["useq-research-index-leaps-growth-overlay"],
            "us_equity_snapshot_pipelines.leaps_growth_overlay_research:main",
        )


if __name__ == "__main__":
    unittest.main()
