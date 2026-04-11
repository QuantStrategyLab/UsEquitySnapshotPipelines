from __future__ import annotations

import math
from collections.abc import Mapping
from pathlib import Path

import numpy as np
import pandas as pd

from us_equity_strategies.strategies.qqq_tech_enhancement import (
    BENCHMARK_SYMBOL,
    DEFAULT_MIN_ADV20_USD,
    DEFAULT_SECTOR_WHITELIST,
    PROFILE_NAME,
    SAFE_HAVEN,
)

PRICE_HISTORY_REQUIRED_COLUMNS = frozenset({"symbol", "as_of", "close", "volume"})
UNIVERSE_REQUIRED_COLUMNS = frozenset({"symbol", "sector"})


def resolve_active_universe(universe_snapshot: pd.DataFrame, as_of_date) -> pd.DataFrame:
    as_of = pd.Timestamp(as_of_date).tz_localize(None).normalize()
    frame = universe_snapshot.copy()

    if "start_date" in frame.columns:
        frame = frame.loc[frame["start_date"].isna() | (frame["start_date"] <= as_of)]
    if "end_date" in frame.columns:
        frame = frame.loc[frame["end_date"].isna() | (frame["end_date"] >= as_of)]

    return frame.loc[:, ["symbol", "sector"]].drop_duplicates(subset=["symbol"], keep="last").reset_index(drop=True)


def read_table(path: str | Path) -> pd.DataFrame:
    raw_path = str(path or "").strip()
    if not raw_path:
        raise EnvironmentError("path is required")
    table_path = Path(raw_path)
    if not table_path.exists():
        raise FileNotFoundError(f"file not found: {table_path}")

    suffix = table_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(table_path)
    if suffix in {".json", ".jsonl"}:
        return pd.read_json(table_path, orient="records", lines=suffix == ".jsonl")
    if suffix == ".parquet":
        return pd.read_parquet(table_path)
    raise ValueError("Unsupported table format; expected .csv, .json, .jsonl, or .parquet")


def write_table(frame: pd.DataFrame, path: str | Path) -> None:
    raw_path = str(path or "").strip()
    if not raw_path:
        raise EnvironmentError("path is required")
    table_path = Path(raw_path)
    table_path.parent.mkdir(parents=True, exist_ok=True)

    suffix = table_path.suffix.lower()
    if suffix == ".csv":
        frame.to_csv(table_path, index=False)
        return
    if suffix == ".json":
        frame.to_json(table_path, orient="records", indent=2, date_format="iso")
        return
    if suffix == ".jsonl":
        frame.to_json(table_path, orient="records", lines=True, date_format="iso")
        return
    if suffix == ".parquet":
        frame.to_parquet(table_path, index=False)
        return
    raise ValueError("Unsupported table format; expected .csv, .json, .jsonl, or .parquet")


FEATURE_SNAPSHOT_COLUMNS = (
    "as_of",
    "symbol",
    "sector",
    "close",
    "volume",
    "adv20_usd",
    "history_days",
    "mom_6_1",
    "mom_12_1",
    "sma20_gap",
    "sma50_gap",
    "sma200_gap",
    "ma50_over_ma200",
    "vol_63",
    "maxdd_126",
    "breakout_252",
    "dist_63_high",
    "dist_126_high",
    "rebound_20",
    "base_eligible",
)


def _require_columns(frame: pd.DataFrame, required: frozenset[str], *, name: str) -> None:
    missing = required - set(frame.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"{name} missing required columns: {missing_text}")


def _normalize_symbol_series(values: pd.Series) -> pd.Series:
    return values.astype(str).str.upper().str.strip()


def _normalize_date(value) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        return pd.NaT
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert(None)
    return timestamp.normalize()


def _normalize_price_groups(
    price_history,
    *,
    as_of: pd.Timestamp,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    if isinstance(price_history, Mapping):
        price_groups: dict[str, pd.DataFrame] = {}
        empty_history = pd.DataFrame(columns=["symbol", "as_of", "close", "volume"])
        for raw_symbol, raw_history in price_history.items():
            history = pd.DataFrame(raw_history).copy()
            if history.empty:
                continue
            _require_columns(history, PRICE_HISTORY_REQUIRED_COLUMNS, name=f"price_history[{raw_symbol!r}]")
            history["symbol"] = _normalize_symbol_series(history["symbol"])
            history["as_of"] = pd.to_datetime(history["as_of"], utc=False).map(_normalize_date)
            history["close"] = pd.to_numeric(history["close"], errors="coerce")
            history["volume"] = pd.to_numeric(history["volume"], errors="coerce")
            history = history.dropna(subset=["symbol", "as_of", "close"])
            history = history.loc[history["as_of"] <= as_of].sort_values("as_of").reset_index(drop=True)
            if history.empty:
                continue
            price_groups[str(raw_symbol).strip().upper()] = history
            if empty_history.empty:
                empty_history = history.iloc[0:0].copy()
        return price_groups, empty_history

    prices = pd.DataFrame(price_history).copy()
    if prices.empty:
        raise ValueError("price_history must contain at least one row")
    _require_columns(prices, PRICE_HISTORY_REQUIRED_COLUMNS, name="price_history")

    prices["symbol"] = _normalize_symbol_series(prices["symbol"])
    prices["as_of"] = pd.to_datetime(prices["as_of"], utc=False).map(_normalize_date)
    prices["close"] = pd.to_numeric(prices["close"], errors="coerce")
    prices["volume"] = pd.to_numeric(prices["volume"], errors="coerce")
    prices = prices.dropna(subset=["symbol", "as_of", "close"])
    prices = prices.loc[prices["as_of"] <= as_of].copy()
    price_groups = {
        symbol: group.sort_values("as_of").reset_index(drop=True)
        for symbol, group in prices.groupby("symbol", sort=False)
    }
    return price_groups, prices.iloc[0:0].copy()


def _precompute_feature_history(price_groups: Mapping[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    feature_history: dict[str, pd.DataFrame] = {}
    for symbol, history in price_groups.items():
        closes = pd.to_numeric(history["close"], errors="coerce")
        volumes = pd.to_numeric(history["volume"], errors="coerce")
        returns = closes.pct_change()
        ma20 = closes.rolling(20).mean()
        ma50 = closes.rolling(50).mean()
        ma200 = closes.rolling(200).mean()
        rolling63max = closes.rolling(63).max()
        rolling126max = closes.rolling(126).max()
        rolling252max = closes.rolling(252).max()
        drawdown_126 = closes / closes.rolling(126).max() - 1.0
        drawdown_126 = drawdown_126.replace([np.inf, -np.inf], np.nan)
        maxdd_126 = drawdown_126.rolling(126).min()
        feature_history[str(symbol).upper()] = pd.DataFrame(
            {
                "as_of": history["as_of"],
                "close": closes,
                "volume": volumes,
                "adv20_usd": (closes * volumes).rolling(20).mean(),
                "history_days": np.arange(1, len(history) + 1, dtype=int),
                "mom_6_1": closes.shift(21) / closes.shift(147) - 1.0,
                "mom_12_1": closes.shift(21) / closes.shift(273) - 1.0,
                "sma20_gap": closes / ma20 - 1.0,
                "sma50_gap": closes / ma50 - 1.0,
                "sma200_gap": closes / ma200 - 1.0,
                "ma50_over_ma200": ma50 / ma200 - 1.0,
                "vol_63": returns.rolling(63).std(ddof=0) * math.sqrt(252),
                "maxdd_126": maxdd_126,
                "breakout_252": closes / rolling252max - 1.0,
                "dist_63_high": closes / rolling63max - 1.0,
                "dist_126_high": closes / rolling126max - 1.0,
                "rebound_20": closes / closes.shift(20) - 1.0,
            }
        )
    return feature_history


def _lookup_features(
    symbol: str,
    as_of: pd.Timestamp,
    feature_history_by_symbol: Mapping[str, pd.DataFrame],
    *,
    sector: str,
) -> dict[str, object]:
    history = feature_history_by_symbol.get(str(symbol).upper())
    row = {"as_of": as_of, "symbol": symbol, "sector": sector}
    if history is None or history.empty:
        for column in FEATURE_SNAPSHOT_COLUMNS:
            if column in row:
                continue
            row[column] = 0 if column == "history_days" else False if column == "base_eligible" else float("nan")
        return row

    cutoff = int(history["as_of"].searchsorted(as_of, side="right"))
    if cutoff <= 0:
        for column in FEATURE_SNAPSHOT_COLUMNS:
            if column in row:
                continue
            row[column] = 0 if column == "history_days" else False if column == "base_eligible" else float("nan")
        return row

    current = history.iloc[cutoff - 1]
    for column in FEATURE_SNAPSHOT_COLUMNS:
        if column in row:
            continue
        if column == "base_eligible":
            row[column] = False
            continue
        value = current[column]
        if column == "history_days":
            row[column] = int(value) if pd.notna(value) else 0
        else:
            row[column] = float(value) if pd.notna(value) else float("nan")
    return row


def build_feature_snapshot(
    price_history,
    universe_snapshot,
    *,
    as_of_date=None,
    benchmark_symbol: str = BENCHMARK_SYMBOL,
    benchmark_sector: str = "benchmark",
    safe_haven: str = SAFE_HAVEN,
    sector_whitelist: tuple[str, ...] = DEFAULT_SECTOR_WHITELIST,
    min_price_usd: float = 10.0,
    min_adv20_usd: float = DEFAULT_MIN_ADV20_USD,
    min_history_days: int = 252,
) -> pd.DataFrame:
    universe = pd.DataFrame(universe_snapshot).copy()
    if universe.empty:
        raise ValueError("universe_snapshot must contain at least one row")
    _require_columns(universe, UNIVERSE_REQUIRED_COLUMNS, name="universe_snapshot")

    universe["symbol"] = _normalize_symbol_series(universe["symbol"])
    universe["sector"] = universe["sector"].fillna("unknown").astype(str).str.strip().replace("", "unknown")
    if {"start_date", "end_date"} & set(universe.columns):
        for column in ("start_date", "end_date"):
            if column in universe.columns:
                universe[column] = pd.to_datetime(universe[column], utc=False).map(_normalize_date)
        if as_of_date is None:
            raise ValueError(f"{PROFILE_NAME} requires as_of_date when universe history has start/end dates")
        universe = resolve_active_universe(universe, as_of_date)
        universe["symbol"] = _normalize_symbol_series(universe["symbol"])
        universe["sector"] = universe["sector"].fillna("unknown").astype(str).str.strip().replace("", "unknown")

    if sector_whitelist:
        universe = universe.loc[universe["sector"].isin(tuple(sector_whitelist))].copy()
    universe = universe.drop_duplicates(subset=["symbol"], keep="last")

    if as_of_date is None:
        prices = pd.DataFrame(price_history)
        if prices.empty or "as_of" not in prices.columns:
            raise ValueError("price_history must contain at least one usable row")
        as_of = pd.to_datetime(prices["as_of"], utc=False).map(_normalize_date).max()
    else:
        as_of = _normalize_date(as_of_date)

    price_groups, _ = _normalize_price_groups(price_history, as_of=as_of)
    feature_history = _precompute_feature_history(price_groups)

    benchmark_symbol = str(benchmark_symbol or "").strip().upper()
    safe_haven = str(safe_haven or "").strip().upper()
    extra_symbols = [benchmark_symbol, "SPY", safe_haven]
    sector_map = dict(zip(universe["symbol"], universe["sector"]))
    symbols = universe["symbol"].tolist()
    for extra in extra_symbols:
        if extra and extra not in symbols:
            symbols.append(extra)

    rows = [
        _lookup_features(
            symbol,
            as_of,
            feature_history,
            sector=sector_map.get(symbol, benchmark_sector if symbol in {benchmark_symbol, "SPY"} else "defense" if symbol == safe_haven else "unknown"),
        )
        for symbol in symbols
    ]
    frame = pd.DataFrame(rows).sort_values("symbol").reset_index(drop=True)

    frame["base_eligible"] = (
        ~frame["symbol"].isin([benchmark_symbol, "SPY", safe_haven])
        & frame["history_days"].ge(min_history_days)
        & frame["close"].gt(min_price_usd)
        & frame["adv20_usd"].ge(min_adv20_usd)
        & frame[
            [
                "mom_6_1",
                "mom_12_1",
                "sma20_gap",
                "sma50_gap",
                "sma200_gap",
                "ma50_over_ma200",
                "vol_63",
                "maxdd_126",
                "breakout_252",
                "dist_63_high",
                "dist_126_high",
                "rebound_20",
            ]
        ].notna().all(axis=1)
    )
    return frame.loc[:, FEATURE_SNAPSHOT_COLUMNS].reset_index(drop=True)
