from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from .russell_1000_multi_factor_defensive_snapshot import read_table


def _price_history_to_close_matrix(price_history) -> pd.DataFrame:
    frame = read_table(price_history) if isinstance(price_history, (str, Path)) else pd.DataFrame(price_history).copy()
    required = {"symbol", "as_of", "close"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"price history missing required columns: {', '.join(sorted(missing))}")
    frame["symbol"] = frame["symbol"].astype(str).str.upper().str.strip()
    frame["as_of"] = pd.to_datetime(frame["as_of"], errors="coerce").dt.tz_localize(None).dt.normalize()
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = frame.dropna(subset=["symbol", "as_of", "close"])
    if frame.empty:
        raise RuntimeError("No usable price history rows")
    close = frame.pivot_table(index="as_of", columns="symbol", values="close", aggfunc="last").sort_index()
    close.columns = close.columns.astype(str).str.upper()
    return close


def normalize_close(price_history) -> pd.DataFrame:
    if isinstance(price_history, (str, Path)):
        price_history = read_table(price_history)
    if {"symbol", "as_of", "close"}.issubset(set(pd.DataFrame(price_history).columns)):
        close = _price_history_to_close_matrix(price_history)
    else:
        close = pd.DataFrame(price_history).copy()
        if close.empty:
            raise RuntimeError("No usable price history rows")
        close.index = pd.to_datetime(close.index, errors="coerce").tz_localize(None).normalize()
        close = close.loc[close.index.notna()]
        close.columns = close.columns.astype(str).str.upper().str.strip()
    close = close.sort_index()
    close.index = pd.to_datetime(close.index, errors="coerce").tz_localize(None).normalize()
    close = close.loc[close.index.notna()]
    close.columns = close.columns.astype(str).str.upper().str.strip()
    if close.empty:
        raise RuntimeError("No usable price history rows")
    return close


def resolve_signal_date(close: pd.DataFrame, as_of: str | None) -> tuple[pd.Timestamp, pd.Timestamp]:
    requested = (
        pd.Timestamp(as_of).tz_localize(None).normalize()
        if as_of
        else pd.Timestamp(close.index.max()).normalize()
    )
    candidates = close.index[close.index <= requested]
    if candidates.empty:
        raise RuntimeError(f"No price history on or before requested as_of={requested.date().isoformat()}")
    return requested, pd.Timestamp(candidates[-1]).normalize()


def bool_at(signal: pd.Series, date: pd.Timestamp) -> bool:
    if signal.empty:
        return False
    series = pd.Series(signal).fillna(False).astype(bool).copy()
    series.index = pd.to_datetime(series.index, errors="coerce").tz_localize(None).normalize()
    series = series.loc[series.index.notna()].sort_index()
    if series.empty:
        return False
    aligned = series.reindex(series.index.union(pd.DatetimeIndex([date]))).sort_index().ffill().fillna(False)
    return bool(aligned.loc[date])


def json_scalar(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): json_scalar(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_scalar(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def flatten_for_csv(payload: Mapping[str, Any]) -> dict[str, Any]:
    rows: dict[str, Any] = {}

    def visit(prefix: str, value: Any) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                visit(f"{prefix}.{key}" if prefix else str(key), item)
            return
        if isinstance(value, (list, tuple)):
            rows[prefix] = ";".join(str(item) for item in value)
            return
        rows[prefix] = value

    visit("", payload)
    return rows
