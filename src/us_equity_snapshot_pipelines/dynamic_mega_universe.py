from __future__ import annotations

import pandas as pd

from .mega_cap_leader_rotation_backtest import (
    _dynamic_mega_issuer_key,
    _normalize_price_history,
    _normalize_universe,
    resolve_active_universe,
)


def normalize_price_history(price_history) -> pd.DataFrame:
    return _normalize_price_history(price_history)


def resolve_effective_as_of_date(price_history: pd.DataFrame, as_of_date: str | None) -> pd.Timestamp:
    if as_of_date:
        return pd.Timestamp(as_of_date).normalize()
    if price_history.empty or "as_of" not in price_history.columns:
        raise ValueError("price_history must contain as_of when --as-of is omitted")
    latest = pd.to_datetime(price_history["as_of"], utc=False).dt.tz_localize(None).dt.normalize().max()
    if pd.isna(latest):
        raise ValueError("price_history as_of has no usable dates")
    return pd.Timestamp(latest).normalize()


def ranked_active_dynamic_universe(
    universe_snapshot: pd.DataFrame,
    *,
    as_of_date: pd.Timestamp,
    universe_size: int,
) -> pd.DataFrame:
    universe = _normalize_universe(universe_snapshot)
    active = resolve_active_universe(universe, as_of_date)
    if active.empty:
        raise ValueError(f"universe has no active rows for as_of_date={as_of_date:%Y-%m-%d}")
    active = active.copy()

    if "mega_rank" in universe.columns:
        raw_active = universe.copy()
        as_of = pd.Timestamp(as_of_date).normalize()
        if "start_date" in raw_active.columns:
            raw_active = raw_active.loc[raw_active["start_date"].isna() | (raw_active["start_date"] <= as_of)]
        if "end_date" in raw_active.columns:
            raw_active = raw_active.loc[raw_active["end_date"].isna() | (raw_active["end_date"] >= as_of)]
        raw_active["mega_rank"] = pd.to_numeric(raw_active["mega_rank"], errors="coerce")
        ranked = raw_active.dropna(subset=["mega_rank"]).sort_values(["mega_rank", "symbol"], ascending=[True, True])
        if not ranked.empty:
            return ranked.head(int(universe_size)).loc[:, ["symbol", "sector"]].reset_index(drop=True)

    ranking_columns = ("source_weight", "weight", "source_market_value", "market_value")
    usable_column = next((column for column in ranking_columns if column in universe.columns), None)
    if usable_column is not None:
        raw_active = universe.copy()
        as_of = pd.Timestamp(as_of_date).normalize()
        if "start_date" in raw_active.columns:
            raw_active = raw_active.loc[raw_active["start_date"].isna() | (raw_active["start_date"] <= as_of)]
        if "end_date" in raw_active.columns:
            raw_active = raw_active.loc[raw_active["end_date"].isna() | (raw_active["end_date"] >= as_of)]
        raw_active[usable_column] = pd.to_numeric(raw_active[usable_column], errors="coerce")
        ranked = raw_active.dropna(subset=[usable_column]).copy()
        if not ranked.empty:
            ranked["_issuer_key"] = ranked["symbol"].map(_dynamic_mega_issuer_key)
            ranked = (
                ranked.sort_values([usable_column, "symbol"], ascending=[False, True])
                .drop_duplicates(subset=["_issuer_key"], keep="first")
                .head(int(universe_size))
            )
            return ranked.loc[:, ["symbol", "sector"]].reset_index(drop=True)

    if len(active) <= int(universe_size):
        return active.loc[:, ["symbol", "sector"]].reset_index(drop=True)

    raise ValueError(
        "mega dynamic universe requires mega_rank, source_weight, weight, "
        "source_market_value, or market_value when the active universe has more rows than the requested universe size"
    )
