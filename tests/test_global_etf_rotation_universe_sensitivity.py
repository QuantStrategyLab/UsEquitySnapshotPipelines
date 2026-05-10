from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from us_equity_snapshot_pipelines import global_etf_rotation_universe_sensitivity as research


def _price_history() -> pd.DataFrame:
    dates = pd.bdate_range(end="2026-03-31", periods=12)
    rows: list[dict[str, object]] = []
    symbols = ["AAA", "BBB", "SPY", "EFA", "EEM", "AGG", "BIL", "XLC", "XLY", "XLI", "XLB", "XBI"]
    for index, symbol in enumerate(symbols):
        base = 100.0 + index
        for day, date in enumerate(dates):
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": date,
                    "close": base * (1.0 + 0.01 * day),
                    "volume": 1000 + day,
                }
            )
    return pd.DataFrame(rows)


class GlobalEtfRotationUniverseSensitivityTests(unittest.TestCase):
    def test_build_download_symbols_adds_canary_and_safe_haven(self) -> None:
        symbols = research._build_download_symbols(("VOO", "XLK", "VOO"), safe_haven="BIL")

        self.assertEqual(symbols[:2], ("VOO", "XLK"))
        self.assertIn("SPY", symbols)
        self.assertIn("EFA", symbols)
        self.assertIn("EEM", symbols)
        self.assertIn("AGG", symbols)
        self.assertIn("BIL", symbols)

    def test_build_pool_variants_adds_sector_expansion_without_duplicates(self) -> None:
        variants = research.build_pool_variants(
            base_pool=("VOO", "XLK", "VOO"),
            extra_sector_symbols=("XLC", "XLY", "XLK", "XLC"),
        )

        self.assertEqual(variants[0].name, "base")
        self.assertEqual(variants[0].ranking_pool, ("VOO", "XLK"))
        self.assertEqual(variants[1].name, "sector_expanded")
        self.assertEqual(variants[1].ranking_pool, ("VOO", "XLK", "XLC", "XLY"))

    def test_run_universe_sensitivity_returns_rows_for_base_and_sector_expanded(self) -> None:
        price_history = _price_history()

        def fake_compute_signals(
            _ib,
            _current_holdings,
            *,
            ranking_pool,
            **kwargs,
        ):
            if len(ranking_pool) == len(research.DEFAULT_BASE_POOL):
                return {"AAA": 0.5, "BBB": 0.5}, "base", False, "canary"
            return {"AAA": 0.75, "BBB": 0.25}, "expanded", False, "canary"

        with patch.object(research.strategy, "compute_signals", side_effect=fake_compute_signals):
            result = research.run_universe_sensitivity(price_history)

        self.assertEqual(set(result["Variant"]), {"base", "sector_expanded"})
        self.assertEqual(result.shape[0], 2)
        self.assertTrue((result["Ranking Pool Size"] > 0).all())
        self.assertEqual(
            result.loc[result["Variant"] == "base", "Confidence Weighting"].iloc[0],
            False,
        )


if __name__ == "__main__":
    unittest.main()
