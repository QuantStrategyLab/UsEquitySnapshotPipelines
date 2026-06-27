from __future__ import annotations

import pandas as pd
import pytest

from us_equity_snapshot_pipelines.tecl_xlk_trend_income_research_inputs import (
    apply_parking_proxy,
    normalize_price_history,
    synthesize_levered_symbol,
)


def _sample_prices() -> pd.DataFrame:
    dates = pd.date_range("2020-01-02", periods=260, freq="B")
    xlk = 100.0 * (1.0 + pd.Series(range(len(dates)), index=dates) * 0.001)
    tecl = xlk * 2.8
    bil = 90.0 + pd.Series(range(len(dates)), index=dates) * 0.01
    rows = []
    for as_of, close in xlk.items():
        rows.append({"as_of": as_of, "symbol": "XLK", "close": float(close)})
    for as_of, close in tecl.items():
        rows.append({"as_of": as_of, "symbol": "TECL", "close": float(close)})
    for as_of, close in bil.items():
        rows.append({"as_of": as_of, "symbol": "BIL", "close": float(close)})
    boxx_dates = dates[dates >= "2022-12-28"]
    for as_of in boxx_dates:
        rows.append({"as_of": as_of, "symbol": "BOXX", "close": 100.0})
    return pd.DataFrame(rows)


def test_parking_proxy_backfills_boxx_before_inception() -> None:
    prices, metadata = apply_parking_proxy(_sample_prices(), parking_symbol="BOXX", parking_proxy_symbol="BIL")
    boxx = prices.loc[prices["symbol"].eq("BOXX")].sort_values("as_of")
    assert not boxx.empty
    assert metadata["parking_proxy_rows_filled"] > 0
    assert boxx["as_of"].min() < pd.Timestamp("2022-12-28")


def test_parking_proxy_uses_flat_cash_when_proxy_starts_late() -> None:
    dates = pd.date_range("2000-01-03", periods=120, freq="B")
    rows = [{"as_of": d, "symbol": "XLK", "close": 100.0 + i} for i, d in enumerate(dates)]
    rows.extend({"as_of": d, "symbol": "BIL", "close": 90.0} for d in dates[60:])
    rows.append({"as_of": dates[-1], "symbol": "BOXX", "close": 100.0})
    prices, metadata = apply_parking_proxy(pd.DataFrame(rows), parking_symbol="BOXX", parking_proxy_symbol="BIL")
    boxx = prices.loc[prices["symbol"].eq("BOXX")].sort_values("as_of")
    assert boxx["as_of"].min() == dates[0]
    assert metadata.get("cash_proxy_rows_filled", 0) > 0


def test_synthesize_tecl_from_xlk_replaces_tecl_series() -> None:
    base = normalize_price_history(_sample_prices())
    synthesized, metadata = synthesize_levered_symbol(base, source_symbol="XLK", target_symbol="TECL")
    tecl = synthesized.loc[synthesized["symbol"].eq("TECL"), "close"]
    assert metadata["synthetic_rows"] > 0
    assert len(tecl) > 0
    assert tecl.iloc[0] == pytest.approx(100.0, rel=1e-4)
