from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import pandas as pd

from us_equity_snapshot_pipelines.contracts import (
    DYNAMIC_MEGA_LEVERAGED_PULLBACK_PROFILE,
    get_profile_contract,
)

from us_equity_snapshot_pipelines.dynamic_mega_leveraged_pullback import build_feature_snapshot


def _sample_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-02", periods=260)
    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "as_of": as_of.date().isoformat(),
                "close": 100.0 + idx * 0.1,
                "volume": 1_000_000,
            }
            for idx, as_of in enumerate(dates)
            for symbol in ("QQQ", "BOXX", "NVDA", "MSFT", "AAPL")
        ]
    )


def _sample_universe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol": "NVDA", "sector": "Information Technology", "mega_rank": 1},
            {"symbol": "MSFT", "sector": "Information Technology", "mega_rank": 2},
            {"symbol": "AAPL", "sector": "Information Technology", "mega_rank": 3},
        ]
    )


def _product_map() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"underlying_symbol": "NVDA", "trade_symbol": "NVDL", "product_leverage": 2.0, "product_available": True},
            {"underlying_symbol": "MSFT", "trade_symbol": "MSFU", "product_leverage": 2.0, "product_available": True},
            {"underlying_symbol": "AAPL", "trade_symbol": "AAPU", "product_leverage": 2.0, "product_available": True},
        ]
    )


def _load_sibling_dynamic_mega_strategy():
    strategy_path = (
        Path(__file__).resolve().parents[2]
        / "UsEquityStrategies"
        / "src"
        / "us_equity_strategies"
        / "strategies"
        / "dynamic_mega_leveraged_pullback.py"
    )
    if not strategy_path.exists():
        pytest.skip("UsEquityStrategies sibling checkout not available")
    spec = importlib.util.spec_from_file_location("sibling_dynamic_mega_leveraged_pullback", strategy_path)
    if spec is None or spec.loader is None:
        pytest.skip("could not load sibling UsEquityStrategies strategy module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_dynamic_mega_leveraged_pullback_snapshot_matches_runtime_contract() -> None:
    strategy = _load_sibling_dynamic_mega_strategy()
    contract = get_profile_contract(DYNAMIC_MEGA_LEVERAGED_PULLBACK_PROFILE)
    snapshot = build_feature_snapshot(
        price_history=_sample_prices(),
        universe_snapshot=_sample_universe(),
        product_map=_product_map(),
        as_of_date="2025-12-31",
        candidate_universe_size=3,
    )

    assert contract.contract_version == strategy.SNAPSHOT_CONTRACT_VERSION
    assert strategy.REQUIRED_FEATURE_COLUMNS <= set(snapshot.columns)
