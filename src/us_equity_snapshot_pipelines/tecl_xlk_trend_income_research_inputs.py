"""Research-only price preparation for TECL/XLK backtests."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd

from .yfinance_prices import download_price_history_with_proxy_candidates, load_proxy_candidates

DEFAULT_LONG_HISTORY_START = "2018-01-01"
DEFAULT_SYNTHETIC_HISTORY_START = "1999-12-01"
DEFAULT_DOWNLOAD_SYMBOLS = ("TECL", "XLK", "BOXX", "BIL", "SCHD", "DGRO", "SGOV", "SPYI", "QQQI")
TECL_SYNTHETIC_LEVERAGE = 3.0
TECL_SYNTHETIC_EXPENSE_RATIO = 0.0095


def _normalize_symbol(value: object) -> str:
    return str(value or "").strip().upper()


def normalize_price_history(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"as_of", "symbol", "close"}
    missing = required - set(frame.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"price_history missing required columns: {missing_text}")
    output = frame.copy()
    output["as_of"] = pd.to_datetime(output["as_of"], errors="coerce").dt.tz_localize(None).dt.normalize()
    output["symbol"] = output["symbol"].map(_normalize_symbol)
    output["close"] = pd.to_numeric(output["close"], errors="coerce")
    for column in ("open", "high", "low", "volume"):
        if column in output.columns:
            output[column] = pd.to_numeric(output[column], errors="coerce")
    output = output.loc[output["symbol"].ne("")].dropna(subset=["as_of", "close"])
    columns = ["as_of", "symbol", "close"]
    columns.extend(column for column in ("open", "high", "low", "volume") if column in output.columns)
    return (
        output.loc[:, columns]
        .drop_duplicates(subset=["as_of", "symbol"], keep="last")
        .sort_values(["as_of", "symbol"])
        .reset_index(drop=True)
    )


def synthesize_levered_symbol(
    frame: pd.DataFrame,
    *,
    source_symbol: str,
    target_symbol: str,
    leverage: float = TECL_SYNTHETIC_LEVERAGE,
    annual_expense_ratio: float = TECL_SYNTHETIC_EXPENSE_RATIO,
) -> tuple[pd.DataFrame, dict[str, object]]:
    source = _normalize_symbol(source_symbol)
    target = _normalize_symbol(target_symbol)
    normalized = normalize_price_history(frame)
    source_frame = normalized.loc[normalized["symbol"].eq(source), ["as_of", "close"]].sort_values("as_of").copy()
    if source_frame.empty:
        raise ValueError(f"cannot synthesize {target}: missing source symbol {source}")
    source_frame["source_return"] = source_frame["close"].pct_change(fill_method=None)
    daily_expense = float(annual_expense_ratio) / 252.0
    synthetic_close: list[float] = []
    last_close = 100.0
    for value in source_frame["source_return"].fillna(0.0):
        daily_return = max(-0.99, float(leverage) * float(value) - daily_expense)
        last_close *= 1.0 + daily_return
        synthetic_close.append(last_close)
    synthetic = pd.DataFrame(
        {
            "as_of": source_frame["as_of"].to_numpy(),
            "symbol": target,
            "close": synthetic_close,
        }
    )
    kept = normalized.loc[~normalized["symbol"].eq(target), ["as_of", "symbol", "close"]].copy()
    merged = (
        pd.concat([kept, synthetic], ignore_index=True)
        .sort_values(["as_of", "symbol"])
        .reset_index(drop=True)
    )
    metadata = {
        "synthetic_symbol": target,
        "synthetic_source_symbol": source,
        "synthetic_leverage": float(leverage),
        "synthetic_annual_expense_ratio": float(annual_expense_ratio),
        "synthetic_rows": int(len(synthetic)),
    }
    return merged, metadata


def apply_parking_proxy(
    frame: pd.DataFrame,
    *,
    parking_symbol: str = "BOXX",
    parking_proxy_symbol: str = "BIL",
) -> tuple[pd.DataFrame, dict[str, object]]:
    parking = _normalize_symbol(parking_symbol)
    proxy = _normalize_symbol(parking_proxy_symbol)
    normalized = normalize_price_history(frame)
    metadata: dict[str, object] = {
        "parking_symbol": parking,
        "parking_proxy_symbol": proxy,
        "parking_proxy_rows_filled": 0,
        "parking_proxy_scale": float("nan"),
        "parking_proxy_scale_source": "none",
        "first_actual_parking_date": "",
    }
    if not proxy or proxy == parking:
        return normalized, metadata

    matrix = normalized.pivot_table(index="as_of", columns="symbol", values="close", aggfunc="last").sort_index()
    if parking not in matrix.columns:
        matrix[parking] = np.nan
    if proxy not in matrix.columns:
        metadata["parking_proxy_scale_source"] = "proxy_missing"
        return normalized, metadata

    parking_series = pd.to_numeric(matrix[parking], errors="coerce")
    proxy_series = pd.to_numeric(matrix[proxy], errors="coerce")
    actual = parking_series.dropna()
    valid_proxy = proxy_series.dropna()
    if valid_proxy.empty:
        raise ValueError(f"parking_proxy_symbol {proxy} has no valid prices")

    if actual.empty:
        scale = 100.0 / float(valid_proxy.iloc[0])
        fill_mask = parking_series.isna() & proxy_series.notna()
        metadata["parking_proxy_scale_source"] = "normalized_100"
    else:
        first_actual_date = pd.Timestamp(actual.index[0])
        proxy_at_inception = proxy_series.loc[:first_actual_date].dropna()
        if proxy_at_inception.empty:
            proxy_at_inception = proxy_series.loc[first_actual_date:].dropna()
        if proxy_at_inception.empty:
            raise ValueError(f"parking_proxy_symbol {proxy} has no price around first parking date")
        scale = float(actual.iloc[0]) / float(proxy_at_inception.iloc[-1])
        fill_mask = parking_series.isna() & proxy_series.notna() & (matrix.index < first_actual_date)
        metadata["parking_proxy_scale_source"] = "first_actual_parking_close"
        metadata["first_actual_parking_date"] = str(first_actual_date.date())

    filled = parking_series.copy()
    filled.loc[fill_mask] = proxy_series.loc[fill_mask] * scale
    cash_proxy_rows = 0
    if actual.empty:
        reference_price = 100.0
    else:
        reference_price = float(actual.iloc[0])
    still_missing = filled.isna()
    if still_missing.any():
        filled.loc[still_missing] = reference_price
        cash_proxy_rows = int(still_missing.sum())
        metadata["cash_proxy_scale_source"] = "flat_cash_before_proxy_coverage"
    matrix[parking] = filled
    metadata["parking_proxy_rows_filled"] = int(fill_mask.sum())
    metadata["parking_proxy_scale"] = float(scale)
    metadata["cash_proxy_rows_filled"] = cash_proxy_rows

    melted = (
        matrix.reset_index()
        .melt(id_vars="as_of", var_name="symbol", value_name="close")
        .dropna(subset=["close"])
        .sort_values(["as_of", "symbol"])
        .reset_index(drop=True)
    )
    return melted, metadata


def prepare_tecl_research_prices(
    frame: pd.DataFrame,
    *,
    synthesize_tecl_from_xlk: bool = False,
    boxx_proxy_symbol: str = "BIL",
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Normalize prices and optionally apply research-only proxies."""
    normalized = normalize_price_history(frame)
    metadata: dict[str, object] = {"inputs_mode": "normalized"}
    if synthesize_tecl_from_xlk:
        normalized, synth_meta = synthesize_levered_symbol(
            normalized,
            source_symbol="XLK",
            target_symbol="TECL",
        )
        metadata["inputs_mode"] = "synthetic_tecl_from_xlk"
        metadata["synthetic"] = synth_meta
    if boxx_proxy_symbol:
        normalized, proxy_meta = apply_parking_proxy(
            normalized,
            parking_symbol="BOXX",
            parking_proxy_symbol=boxx_proxy_symbol,
        )
        metadata["boxx_proxy"] = proxy_meta
    return normalized, metadata


def download_tecl_research_prices(
    *,
    start: str = DEFAULT_LONG_HISTORY_START,
    end: str | None = None,
    symbols: tuple[str, ...] = DEFAULT_DOWNLOAD_SYMBOLS,
    proxy: str | None = None,
    proxy_candidates: list[str] | None = None,
) -> pd.DataFrame:
    return download_price_history_with_proxy_candidates(
        list(symbols),
        start=start,
        end=end,
        proxy=proxy,
        proxy_candidates=proxy_candidates or [],
    )


def build_tecl_long_history_inputs(
    *,
    start: str = DEFAULT_LONG_HISTORY_START,
    end: str | None = None,
    synthesize_tecl_from_xlk: bool = False,
    boxx_proxy_symbol: str = "BIL",
    proxy: str | None = None,
    proxy_candidates: list[str] | None = None,
) -> tuple[pd.DataFrame, dict[str, object]]:
    downloaded = download_tecl_research_prices(
        start=start,
        end=end,
        proxy=proxy,
        proxy_candidates=proxy_candidates,
    )
    prices, metadata = prepare_tecl_research_prices(
        downloaded,
        synthesize_tecl_from_xlk=synthesize_tecl_from_xlk,
        boxx_proxy_symbol=boxx_proxy_symbol,
    )
    metadata["download_start"] = start
    metadata["download_end"] = end or ""
    metadata["symbols"] = list(DEFAULT_DOWNLOAD_SYMBOLS)
    return prices, metadata


__all__ = [
    "DEFAULT_DOWNLOAD_SYMBOLS",
    "DEFAULT_LONG_HISTORY_START",
    "DEFAULT_SYNTHETIC_HISTORY_START",
    "apply_parking_proxy",
    "build_tecl_long_history_inputs",
    "download_tecl_research_prices",
    "normalize_price_history",
    "prepare_tecl_research_prices",
    "synthesize_levered_symbol",
]
