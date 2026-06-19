from __future__ import annotations

import json
import unittest
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines import memory_semiconductor_momentum_research as research


def _price_history() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    long_dates = pd.bdate_range("2025-01-02", periods=280)
    young_dates = pd.bdate_range("2026-04-02", periods=45)
    for offset, date in enumerate(long_dates):
        for symbol, slope in (("SMH", 0.002), ("SOXX", 0.001), ("SPY", 0.0005)):
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": date,
                    "open": 100.0,
                    "high": 100.0,
                    "low": 100.0,
                    "close": 100.0 * (1.0 + slope * offset),
                    "volume": 1000,
                }
            )
    for offset, date in enumerate(young_dates):
        rows.append(
            {
                "symbol": "DRAM",
                "as_of": date,
                "open": 50.0,
                "high": 50.0,
                "low": 50.0,
                "close": 50.0 * (1.0 + 0.01 * offset),
                "volume": 1000,
            }
        )
    return pd.DataFrame(rows)


class MemorySemiconductorMomentumResearchTests(unittest.TestCase):
    def test_young_memory_etf_is_observation_only_until_seasoned(self) -> None:
        snapshot = research.build_memory_semiconductor_snapshot(
            _price_history(),
            specs=(
                research.SymbolSpec("DRAM", "memory_etf", True),
                research.SymbolSpec("SMH", "semiconductor_proxy_etf", True),
            ),
            seasoning_rule=research.SeasoningRule(min_trading_days=252, min_month_end_closes=13),
        )

        dram = snapshot.loc[snapshot["symbol"] == "DRAM"].iloc[0]
        smh = snapshot.loc[snapshot["symbol"] == "SMH"].iloc[0]

        self.assertFalse(bool(dram["seasoning_eligible"]))
        self.assertEqual(dram["research_action"], "observe_until_seasoned")
        self.assertTrue(bool(smh["seasoning_eligible"]))
        self.assertEqual(smh["research_action"], "eligible_for_research_ranking")

    def test_tradeable_ranking_excludes_unseasoned_etf_and_trackers(self) -> None:
        snapshot = research.build_memory_semiconductor_snapshot(
            _price_history(),
            specs=(
                research.SymbolSpec("DRAM", "memory_etf", True),
                research.SymbolSpec("SMH", "semiconductor_proxy_etf", True),
                research.SymbolSpec("SPY", "benchmark", False),
            ),
            seasoning_rule=research.SeasoningRule(min_trading_days=252, min_month_end_closes=13),
        )

        ranking = research.build_tradeable_ranking(snapshot)

        self.assertEqual(tuple(ranking["symbol"]), ("SMH",))

    def test_cli_writes_standard_transparent_audit_artifacts(self) -> None:
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmp:
            prices_path = pd.io.common.stringify_path(f"{tmp}/prices.csv")
            output_dir = pd.io.common.stringify_path(f"{tmp}/output")
            _price_history().to_csv(prices_path, index=False)

            exit_code = research.main(["--prices", prices_path, "--output-dir", output_dir])

            self.assertEqual(exit_code, 0)
            expected = {
                "downloaded_price_history.csv",
                "candidate_snapshot.csv",
                "gate_results.csv",
                "ranking.csv",
                "promotion_decision.json",
                "run_manifest.json",
                "audit_report.md",
            }
            for name in expected:
                self.assertTrue((Path(output_dir) / name).exists())
            manifest = json.loads((Path(output_dir) / "run_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["artifact_type"], "transparent_universe_audit")
            self.assertEqual(manifest["rule_spec"]["rule_id"], "memory_semiconductor_momentum")


if __name__ == "__main__":
    unittest.main()
