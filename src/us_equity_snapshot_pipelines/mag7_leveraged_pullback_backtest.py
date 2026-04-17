from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from .mega_cap_leader_rotation_backtest import (
    BROAD_BENCHMARK_SYMBOL,
    MAG7_POOL,
    SAFE_HAVEN,
    _build_close_and_returns,
    _compute_turnover,
    _normalize_price_history,
    _normalize_symbol,
    split_symbols,
)
from .russell_1000_multi_factor_defensive_snapshot import read_table

PROFILE = "mag7_leveraged_pullback"
DYNAMIC_PROFILE = "dynamic_mega_leveraged_pullback"
MAGS_OFFICIAL_PROFILE = "mags_official_leveraged_pullback"
BENCHMARK_SYMBOL = "QQQ"
DEFAULT_DYNAMIC_CANDIDATE_UNIVERSE_SIZE = 10
RECOMMENDED_DYNAMIC_CANDIDATE_UNIVERSE_SIZE = 15
RECOMMENDED_DYNAMIC_TOP_N = 3
RECOMMENDED_DYNAMIC_MAX_PRODUCT_EXPOSURE = 0.8
RECOMMENDED_DYNAMIC_SINGLE_NAME_CAP = 0.25
RECOMMENDED_DYNAMIC_SOFT_PRODUCT_EXPOSURE = 0.0
RECOMMENDED_DYNAMIC_HARD_PRODUCT_EXPOSURE = 0.0
RECOMMENDED_DYNAMIC_MARKET_TREND_SYMBOL = "QQQ"
DEFAULT_REBOUND_BUDGET_SIGNAL_ACTIVE_DAYS = 10
DEFAULT_REBOUND_BUDGET_CAP = 0.10
DEFAULT_BEAR_CANDIDATE_MAX_SIZE_MULTIPLIER = 0.35
DEFAULT_ATR_PERIOD = 14
DEFAULT_ATR_ENTRY_SCALE = 2.5
DEFAULT_ENTRY_LINE_FLOOR = 1.04
DEFAULT_ENTRY_LINE_CAP = 1.08
DEFAULT_ATR_EXIT_SCALE = 0.0
DEFAULT_EXIT_LINE_FLOOR = 1.02
DEFAULT_EXIT_LINE_CAP = 1.02
RETURN_MODE_LEVERAGED_PRODUCT = "leveraged_product"
RETURN_MODE_MARGIN_STOCK = "margin_stock"
RETURN_MODES = (RETURN_MODE_LEVERAGED_PRODUCT, RETURN_MODE_MARGIN_STOCK)
DEFAULT_MARGIN_BORROW_RATE = 0.055
REBOUND_BUDGET_STRATEGY_SUFFIX = "_rebound_budget"
BEAR_CANDIDATE_MODE_OFF = "off"
BEAR_CANDIDATE_MODE_MARKET_SAFE = "market_safe"
BEAR_CANDIDATE_MODE_MARKET_BEAR = "market_bear"
BEAR_CANDIDATE_MODE_ALWAYS = "always"
BEAR_CANDIDATE_MODES = (
    BEAR_CANDIDATE_MODE_OFF,
    BEAR_CANDIDATE_MODE_MARKET_SAFE,
    BEAR_CANDIDATE_MODE_MARKET_BEAR,
    BEAR_CANDIDATE_MODE_ALWAYS,
)
MARKET_BEAR_REGIMES = {"hard_defense", "soft_defense"}
MAGS_HOLDING_NAME_MAP = {
    "ALPHABET": "GOOGL",
    "AMAZON": "AMZN",
    "APPLE": "AAPL",
    "META": "META",
    "MICROSOFT": "MSFT",
    "NVIDIA": "NVDA",
    "TESLA": "TSLA",
}
SUMMARY_COLUMNS = (
    "Strategy",
    "Return Mode",
    "Start",
    "End",
    "Total Return",
    "CAGR",
    "Max Drawdown",
    "Volatility",
    "Sharpe",
    "Calmar",
    "Leverage Multiple",
    "Avg Product Exposure",
    "Avg Underlying Exposure",
    "Rebalances/Year",
    "Turnover/Year",
    "Final Equity",
)


def _zscore(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    std = numeric.std(ddof=0)
    if pd.isna(std) or float(std) == 0.0:
        return pd.Series(0.0, index=values.index)
    return (numeric - numeric.mean()) / std


def _last_trading_day_by_period(index: pd.DatetimeIndex, frequency: str) -> set[pd.Timestamp]:
    if index.empty:
        return set()
    normalized = str(frequency or "W").strip().upper()
    period = "M" if normalized.startswith("M") else "W-FRI"
    series = pd.Series(index, index=index)
    grouped = series.groupby(index.to_period(period)).max()
    return set(pd.to_datetime(grouped.values))


def _mags_holding_symbol(stock_ticker: object, security_name: object) -> str | None:
    ticker = _normalize_symbol(str(stock_ticker or ""))
    name = str(security_name or "").strip().upper()
    if ticker in {"", "CASH&OTHER", "FGXXX", "XBOX"}:
        return None
    if any(fragment in name for fragment in ("TREASURY", "GOVERNMENT OBLIGATIONS", "ULTRA SHORT DURATION")):
        return None
    if " TRS " not in ticker and ticker.replace(".", "").isalpha() and len(ticker) <= 6:
        return ticker
    if ticker in MAG7_POOL:
        return ticker
    for key, symbol in MAGS_HOLDING_NAME_MAP.items():
        if key in name:
            return symbol
    return None


def _roundhill_mags_holdings_to_universe(frame: pd.DataFrame) -> pd.DataFrame:
    mags = frame.loc[frame["Account"].astype(str).str.upper().eq("MAGS")].copy()
    if mags.empty:
        return pd.DataFrame(columns=["symbol", "sector", "start_date", "end_date", "mega_rank", "source_weight"])

    rows: list[dict[str, object]] = []
    for raw_date, date_frame in mags.groupby("Date", sort=True):
        file_date = pd.to_datetime(raw_date, errors="coerce")
        if pd.isna(file_date):
            continue
        as_of = (pd.Timestamp(file_date).tz_localize(None).normalize() - pd.Timedelta(days=1))
        weights: dict[str, float] = {}
        for row in date_frame.itertuples(index=False):
            symbol = _mags_holding_symbol(getattr(row, "StockTicker", ""), getattr(row, "SecurityName", ""))
            if symbol is None:
                continue
            weight_text = str(getattr(row, "Weightings", "") or "").replace("%", "").strip()
            try:
                weight = float(weight_text)
            except ValueError:
                continue
            weights[symbol] = weights.get(symbol, 0.0) + weight
        for rank, (symbol, weight) in enumerate(sorted(weights.items(), key=lambda item: (-item[1], item[0])), start=1):
            rows.append(
                {
                    "symbol": symbol,
                    "sector": "mags_official",
                    "start_date": as_of,
                    "mega_rank": rank,
                    "source_weight": weight,
                }
            )

    history = pd.DataFrame(rows)
    if history.empty:
        return pd.DataFrame(columns=["symbol", "sector", "start_date", "end_date", "mega_rank", "source_weight"])
    history = history.drop_duplicates(subset=["start_date", "symbol"], keep="last")
    snapshots = sorted(history["start_date"].dropna().unique())
    frames: list[pd.DataFrame] = []
    for index, start_date in enumerate(snapshots):
        active = history.loc[history["start_date"].eq(start_date)].copy()
        next_date = snapshots[index + 1] if index + 1 < len(snapshots) else pd.NaT
        active["end_date"] = pd.Timestamp(next_date) - pd.Timedelta(days=1) if pd.notna(next_date) else pd.NaT
        frames.append(active)
    return pd.concat(frames, ignore_index=True).sort_values(["start_date", "mega_rank", "symbol"]).reset_index(drop=True)


def _normalize_universe_history(universe_snapshot) -> pd.DataFrame:
    frame = pd.DataFrame(universe_snapshot).copy()
    if frame.empty:
        return pd.DataFrame(columns=["symbol", "start_date", "end_date", "mega_rank"])
    if {"Account", "StockTicker", "SecurityName", "Weightings", "Date"} <= set(frame.columns):
        frame = _roundhill_mags_holdings_to_universe(frame)
        if frame.empty:
            return frame
    if "symbol" not in frame.columns:
        raise ValueError("universe_snapshot missing required column: symbol")

    frame["symbol"] = frame["symbol"].map(_normalize_symbol)
    for column in ("start_date", "end_date"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce").dt.tz_localize(None).dt.normalize()
        else:
            frame[column] = pd.NaT
    if "mega_rank" in frame.columns:
        frame["mega_rank"] = pd.to_numeric(frame["mega_rank"], errors="coerce")
    else:
        frame["mega_rank"] = np.nan
    for column in ("source_weight", "weight", "source_market_value", "market_value"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.loc[frame["symbol"].ne("")].reset_index(drop=True)


def resolve_active_candidate_symbols(
    universe: pd.DataFrame | None,
    as_of: pd.Timestamp,
    *,
    fallback_symbols: Sequence[str],
    candidate_universe_size: int | None,
    available_symbols: Iterable[str],
) -> tuple[str, ...]:
    available = set(available_symbols)
    if universe is None or universe.empty:
        return tuple(symbol for symbol in fallback_symbols if symbol in available)

    as_of_date = pd.Timestamp(as_of).tz_localize(None).normalize()
    frame = universe.copy()
    frame["symbol"] = frame["symbol"].map(_normalize_symbol)
    for column in ("start_date", "end_date"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce").dt.tz_localize(None).dt.normalize()
    if "mega_rank" in frame.columns:
        frame["mega_rank"] = pd.to_numeric(frame["mega_rank"], errors="coerce")
    if "start_date" in frame.columns:
        frame = frame.loc[frame["start_date"].isna() | (frame["start_date"] <= as_of_date)]
    if "end_date" in frame.columns:
        frame = frame.loc[frame["end_date"].isna() | (frame["end_date"] >= as_of_date)]
    frame = frame.loc[frame["symbol"].isin(available)].copy()
    if frame.empty:
        return ()

    sort_columns: list[str] = []
    ascending: list[bool] = []
    if "mega_rank" in frame.columns and frame["mega_rank"].notna().any():
        sort_columns.append("mega_rank")
        ascending.append(True)
    for column in ("source_weight", "weight", "source_market_value", "market_value"):
        if column in frame.columns and frame[column].notna().any():
            sort_columns.append(column)
            ascending.append(False)
            break
    sort_columns.append("symbol")
    ascending.append(True)
    frame = frame.sort_values(sort_columns, ascending=ascending)
    frame = frame.drop_duplicates(subset=["symbol"], keep="first")
    if candidate_universe_size is not None and int(candidate_universe_size) > 0:
        frame = frame.head(int(candidate_universe_size))
    return tuple(frame["symbol"].astype(str).tolist())


def _feature_frame_for_date(
    close_matrix: pd.DataFrame,
    returns_matrix: pd.DataFrame,
    as_of: pd.Timestamp,
    symbols: Sequence[str],
    *,
    benchmark_symbol: str,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if as_of not in close_matrix.index:
        return pd.DataFrame(rows)
    cutoff = close_matrix.index.get_loc(as_of)
    if not isinstance(cutoff, int):
        cutoff = int(np.asarray(cutoff).max())
    benchmark_close = close_matrix[benchmark_symbol] if benchmark_symbol in close_matrix else pd.Series(dtype=float)
    benchmark_mom_126 = (
        float(benchmark_close.iloc[cutoff] / benchmark_close.iloc[cutoff - 126] - 1.0)
        if len(benchmark_close) > cutoff >= 126
        and pd.notna(benchmark_close.iloc[cutoff])
        and pd.notna(benchmark_close.iloc[cutoff - 126])
        else float("nan")
    )

    for symbol in symbols:
        if symbol not in close_matrix.columns:
            continue
        close = close_matrix[symbol]
        returns = returns_matrix[symbol]
        if cutoff < 252 or pd.isna(close.iloc[cutoff]):
            continue
        current = float(close.iloc[cutoff])
        sma_50 = float(close.iloc[cutoff - 49 : cutoff + 1].mean())
        sma_200 = float(close.iloc[cutoff - 199 : cutoff + 1].mean())
        high_63 = float(close.iloc[cutoff - 62 : cutoff + 1].max())
        high_252 = float(close.iloc[cutoff - 251 : cutoff + 1].max())
        low_20 = float(close.iloc[cutoff - 19 : cutoff + 1].min())
        mom_20 = float(close.iloc[cutoff] / close.iloc[cutoff - 20] - 1.0)
        mom_63 = float(close.iloc[cutoff] / close.iloc[cutoff - 63] - 1.0)
        mom_126 = float(close.iloc[cutoff] / close.iloc[cutoff - 126] - 1.0)
        mom_252 = float(close.iloc[cutoff] / close.iloc[cutoff - 252] - 1.0)
        vol_63 = float(returns.iloc[cutoff - 62 : cutoff + 1].std(ddof=0) * np.sqrt(252))
        rows.append(
            {
                "as_of": as_of,
                "symbol": symbol,
                "close": current,
                "mom_20": mom_20,
                "mom_63": mom_63,
                "mom_126": mom_126,
                "mom_252": mom_252,
                "rel_mom_126_vs_benchmark": mom_126 - benchmark_mom_126,
                "sma_50_gap": current / sma_50 - 1.0 if sma_50 else float("nan"),
                "sma_200_gap": current / sma_200 - 1.0 if sma_200 else float("nan"),
                "high_63_gap": current / high_63 - 1.0 if high_63 else float("nan"),
                "high_252_gap": current / high_252 - 1.0 if high_252 else float("nan"),
                "low_20_gap": current / low_20 - 1.0 if low_20 else float("nan"),
                "vol_63": vol_63,
            }
        )
    return pd.DataFrame(rows)


def _benchmark_regime(
    close_matrix: pd.DataFrame,
    as_of: pd.Timestamp,
    *,
    benchmark_symbol: str,
    current_risk_active: bool,
    max_product_exposure: float,
    soft_product_exposure: float,
    hard_product_exposure: float,
    atr_period: int,
    atr_entry_scale: float,
    entry_line_floor: float,
    entry_line_cap: float,
    atr_exit_scale: float,
    exit_line_floor: float,
    exit_line_cap: float,
) -> tuple[float, str, dict[str, float]]:
    if benchmark_symbol not in close_matrix.columns or as_of not in close_matrix.index:
        return 0.0, "no_benchmark", {}
    close = close_matrix[benchmark_symbol]
    cutoff = close.index.get_loc(as_of)
    if not isinstance(cutoff, int):
        cutoff = int(np.asarray(cutoff).max())
    if cutoff < max(200, int(atr_period) + 1) or pd.isna(close.iloc[cutoff]):
        return 0.0, "warmup", {}

    current = float(close.iloc[cutoff])
    sma_50 = float(close.iloc[cutoff - 49 : cutoff + 1].mean())
    sma_200 = float(close.iloc[cutoff - 199 : cutoff + 1].mean())
    high_63 = float(close.iloc[cutoff - 62 : cutoff + 1].max())
    close_change = close.diff().abs()
    atr = float(close_change.iloc[cutoff - int(atr_period) + 1 : cutoff + 1].mean())
    atr_pct = atr / current if current > 0 else float("nan")
    entry_multiplier = max(float(entry_line_floor), min(float(entry_line_cap), 1.0 + atr_pct * float(atr_entry_scale)))
    exit_multiplier = max(float(exit_line_floor), min(float(exit_line_cap), 1.0 - atr_pct * float(atr_exit_scale)))
    entry_line = sma_200 * entry_multiplier
    exit_line = sma_200 * exit_multiplier
    mom_20 = float(close.iloc[cutoff] / close.iloc[cutoff - 20] - 1.0) if cutoff >= 20 else float("nan")
    mom_63 = float(close.iloc[cutoff] / close.iloc[cutoff - 63] - 1.0) if cutoff >= 63 else float("nan")
    sma_200_gap = current / sma_200 - 1.0 if sma_200 else float("nan")
    high_63_gap = current / high_63 - 1.0 if high_63 else float("nan")

    diagnostics = {
        "benchmark_sma_50_gap": current / sma_50 - 1.0 if sma_50 else float("nan"),
        "benchmark_sma_200_gap": sma_200_gap,
        "benchmark_high_63_gap": high_63_gap,
        "benchmark_mom_20": mom_20,
        "benchmark_mom_63": mom_63,
        "benchmark_atr": atr,
        "benchmark_atr_pct": atr_pct,
        "benchmark_entry_line": entry_line,
        "benchmark_exit_line": exit_line,
        "benchmark_entry_multiplier": entry_multiplier,
        "benchmark_exit_multiplier": exit_multiplier,
    }

    if current < exit_line:
        return float(hard_product_exposure), "hard_defense", diagnostics
    if current_risk_active:
        return float(max_product_exposure), "risk_on_hold_band", diagnostics
    if current > entry_line:
        return float(max_product_exposure), "risk_on_entry", diagnostics
    if sma_200_gap < 0.0 or mom_63 < -0.05:
        return float(soft_product_exposure), "soft_defense", diagnostics
    return float(soft_product_exposure), "entry_wait", diagnostics


def _bear_pullback_multiplier(row: Mapping[str, object], *, max_size_multiplier: float) -> float:
    sma_200_gap = float(row.get("sma_200_gap", float("nan")))
    high_252_gap = float(row.get("high_252_gap", float("nan")))
    low_20_gap = float(row.get("low_20_gap", float("nan")))
    mom_20 = float(row.get("mom_20", float("nan")))
    rel_mom_126 = float(row.get("rel_mom_126_vs_benchmark", float("nan")))
    if any(pd.isna(value) for value in (sma_200_gap, high_252_gap, low_20_gap, mom_20, rel_mom_126)):
        return 0.0
    if sma_200_gap >= 0.0 or sma_200_gap < -0.35:
        return 0.0
    if high_252_gap > -0.20 or low_20_gap < 0.02 or mom_20 < -0.12:
        return 0.0

    drawdown = abs(min(high_252_gap, 0.0))
    if drawdown < 0.25:
        multiplier = 0.20
    elif drawdown < 0.45:
        multiplier = 0.35
    elif drawdown < 0.60:
        multiplier = 0.25
    else:
        return 0.0
    if rel_mom_126 < -0.25:
        multiplier *= 0.50
    return float(max(0.0, min(float(max_size_multiplier), multiplier)))


def _pullback_multiplier(
    row: Mapping[str, object],
    *,
    allow_bear_candidate_pullbacks: bool = False,
    bear_candidate_max_size_multiplier: float = DEFAULT_BEAR_CANDIDATE_MAX_SIZE_MULTIPLIER,
) -> float:
    sma_200_gap = float(row.get("sma_200_gap", float("nan")))
    mom_126 = float(row.get("mom_126", float("nan")))
    pullback = abs(min(float(row.get("high_63_gap", 0.0)), 0.0))
    if pd.isna(sma_200_gap) or pd.isna(mom_126) or sma_200_gap <= 0.0 or mom_126 <= 0.0:
        if allow_bear_candidate_pullbacks:
            return _bear_pullback_multiplier(
                row,
                max_size_multiplier=float(bear_candidate_max_size_multiplier),
            )
        return 0.0
    if pullback < 0.02:
        multiplier = 0.45
    elif pullback < 0.06:
        multiplier = 0.85
    elif pullback < 0.16:
        multiplier = 1.25
    elif pullback < 0.28:
        multiplier = 0.75
    else:
        return 0.0

    if float(row.get("mom_20", 0.0)) > 0.18 or float(row.get("sma_50_gap", 0.0)) > 0.16:
        multiplier *= 0.50
    if sma_200_gap < 0.03:
        multiplier *= 0.60
    if float(row.get("rel_mom_126_vs_benchmark", 0.0)) < -0.08:
        multiplier *= 0.50
    return float(max(0.0, min(multiplier, 1.30)))


def rank_candidates(
    feature_frame: pd.DataFrame,
    current_holdings: Iterable[str] | None = None,
    *,
    allow_bear_candidate_pullbacks: bool = False,
    bear_candidate_max_size_multiplier: float = DEFAULT_BEAR_CANDIDATE_MAX_SIZE_MULTIPLIER,
) -> pd.DataFrame:
    if feature_frame.empty:
        return pd.DataFrame()
    frame = feature_frame.copy()
    required = ["mom_63", "mom_126", "mom_252", "rel_mom_126_vs_benchmark", "sma_200_gap", "vol_63", "high_63_gap"]
    frame = frame.loc[frame[required].notna().all(axis=1)].copy()
    if frame.empty:
        return pd.DataFrame()

    frame["pullback_depth"] = frame["high_63_gap"].clip(upper=0.0).abs()
    frame["pullback_quality"] = (1.0 - (frame["pullback_depth"] - 0.10).abs() / 0.10).clip(lower=-1.0, upper=1.0)
    frame["size_multiplier"] = frame.apply(
        lambda row: _pullback_multiplier(
            row,
            allow_bear_candidate_pullbacks=bool(allow_bear_candidate_pullbacks),
            bear_candidate_max_size_multiplier=float(bear_candidate_max_size_multiplier),
        ),
        axis=1,
    )
    frame["eligible"] = frame["size_multiplier"] > 0.0
    frame["bear_candidate"] = pd.to_numeric(frame["sma_200_gap"], errors="coerce") < 0.0
    frame = frame.loc[frame["eligible"]].copy()
    if frame.empty:
        return pd.DataFrame()

    current = set(split_symbols(current_holdings))
    frame["score"] = (
        _zscore(frame["mom_126"]) * 0.35
        + _zscore(frame["mom_252"]) * 0.20
        + _zscore(frame["rel_mom_126_vs_benchmark"]) * 0.20
        + _zscore(frame["pullback_quality"]) * 0.20
        - _zscore(frame["vol_63"]) * 0.05
    )
    if current:
        frame.loc[frame["symbol"].isin(current), "score"] += 0.05
    ranked = frame.sort_values(
        ["score", "pullback_quality", "mom_126", "symbol"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    ranked.insert(0, "rank", range(1, len(ranked) + 1))
    return ranked


def build_target_weights(
    feature_frame: pd.DataFrame,
    current_holdings: Iterable[str] | None,
    *,
    target_product_exposure: float,
    top_n: int,
    hold_buffer: int,
    single_name_cap: float,
    safe_haven: str,
    leverage_multiple: float,
    allow_bear_candidate_pullbacks: bool = False,
    bear_candidate_max_size_multiplier: float = DEFAULT_BEAR_CANDIDATE_MAX_SIZE_MULTIPLIER,
) -> tuple[dict[str, float], pd.DataFrame, dict[str, object]]:
    safe_haven = _normalize_symbol(safe_haven)
    ranked = rank_candidates(
        feature_frame,
        current_holdings,
        allow_bear_candidate_pullbacks=allow_bear_candidate_pullbacks,
        bear_candidate_max_size_multiplier=float(bear_candidate_max_size_multiplier),
    )
    metadata: dict[str, object] = {
        "target_product_exposure": float(target_product_exposure),
        "selected_symbols": (),
        "product_exposure": 0.0,
        "underlying_exposure": 0.0,
        "avg_pullback_depth": float("nan"),
        "bear_selected_count": 0,
    }
    if ranked.empty or target_product_exposure <= 0.0:
        return {safe_haven: 1.0}, ranked, metadata

    top_n = max(1, int(top_n))
    current = set(split_symbols(current_holdings))
    rank_map = dict(zip(ranked["symbol"].astype(str), ranked["rank"].astype(int)))
    max_hold_rank = top_n + max(0, int(hold_buffer))
    selected = [
        symbol
        for symbol in ranked["symbol"].astype(str)
        if symbol in current and rank_map[symbol] <= max_hold_rank
    ]
    for symbol in ranked["symbol"].astype(str):
        if len(selected) >= top_n:
            break
        if symbol not in selected:
            selected.append(symbol)
    selected = selected[:top_n]
    if not selected:
        return {safe_haven: 1.0}, ranked, metadata

    selected_frame = ranked.loc[ranked["symbol"].isin(selected)].copy()
    target_product_exposure = min(1.0, max(0.0, float(target_product_exposure)))
    base_weight = target_product_exposure / len(selected)
    raw_weights = {
        str(row.symbol): min(float(single_name_cap), base_weight * float(row.size_multiplier))
        for row in selected_frame.itertuples(index=False)
    }
    product_exposure = sum(raw_weights.values())
    if product_exposure > target_product_exposure and product_exposure > 0.0:
        scale = target_product_exposure / product_exposure
        raw_weights = {symbol: weight * scale for symbol, weight in raw_weights.items()}
        product_exposure = sum(raw_weights.values())
    cash_weight = max(0.0, 1.0 - product_exposure)
    weights = {symbol: weight for symbol, weight in raw_weights.items() if weight > 1e-12}
    if cash_weight > 1e-12:
        weights[safe_haven] = cash_weight

    metadata["selected_symbols"] = tuple(symbol for symbol in selected if symbol in weights)
    metadata["product_exposure"] = float(product_exposure)
    metadata["underlying_exposure"] = float(product_exposure * float(leverage_multiple))
    metadata["avg_pullback_depth"] = float(selected_frame["pullback_depth"].mean()) if not selected_frame.empty else float("nan")
    metadata["bear_selected_count"] = int(selected_frame["bear_candidate"].fillna(False).sum())
    return weights, ranked, metadata


def summarize_returns(
    returns: pd.Series,
    *,
    strategy_name: str,
    return_mode: str | None = None,
    leverage_multiple: float | None = None,
    weights_history: pd.DataFrame | None = None,
    exposure_history: pd.DataFrame | None = None,
) -> dict[str, float | str]:
    clean_returns = returns.dropna()
    if clean_returns.empty:
        raise RuntimeError("No returns to summarize")
    equity_curve = (1.0 + clean_returns).cumprod()
    total_return = float(equity_curve.iloc[-1] - 1.0)
    years = max((clean_returns.index[-1] - clean_returns.index[0]).days / 365.25, 1 / 365.25)
    cagr = float(equity_curve.iloc[-1] ** (1.0 / years) - 1.0)
    drawdown = equity_curve / equity_curve.cummax() - 1.0
    max_drawdown = float(drawdown.min())
    volatility = float(clean_returns.std(ddof=0) * np.sqrt(252))
    std = float(clean_returns.std(ddof=0))
    sharpe = float(clean_returns.mean() / std * np.sqrt(252)) if std else float("nan")
    calmar = float(cagr / abs(max_drawdown)) if max_drawdown < 0 else float("nan")

    rebalances_per_year = float("nan")
    turnover_per_year = float("nan")
    if weights_history is not None and not weights_history.empty:
        changes = weights_history.fillna(0.0).diff().fillna(0.0)
        if not changes.empty:
            changes.iloc[0] = 0.0
        daily_turnover = 0.5 * changes.abs().sum(axis=1)
        rebalances_per_year = float((daily_turnover > 1e-12).sum() / years)
        turnover_per_year = float(daily_turnover.sum() / years)

    avg_product = float("nan")
    avg_underlying = float("nan")
    if exposure_history is not None and not exposure_history.empty:
        avg_product = float(pd.to_numeric(exposure_history["product_exposure"], errors="coerce").mean())
        avg_underlying = float(pd.to_numeric(exposure_history["underlying_exposure"], errors="coerce").mean())

    return {
        "Strategy": strategy_name,
        "Return Mode": return_mode or "",
        "Start": str(clean_returns.index[0].date()),
        "End": str(clean_returns.index[-1].date()),
        "Total Return": total_return,
        "CAGR": cagr,
        "Max Drawdown": max_drawdown,
        "Volatility": volatility,
        "Sharpe": sharpe,
        "Calmar": calmar,
        "Leverage Multiple": float(leverage_multiple) if leverage_multiple is not None else float("nan"),
        "Avg Product Exposure": avg_product,
        "Avg Underlying Exposure": avg_underlying,
        "Rebalances/Year": rebalances_per_year,
        "Turnover/Year": turnover_per_year,
        "Final Equity": float(equity_curve.iloc[-1]),
    }


def _reference_summary(
    name: str,
    returns: pd.Series,
    *,
    return_mode: str | None = None,
    leverage_multiple: float | None = None,
) -> dict[str, float | str]:
    return summarize_returns(
        returns,
        strategy_name=name,
        return_mode=return_mode,
        leverage_multiple=leverage_multiple,
    )


def _risk_sleeve_daily_returns(
    returns: pd.Series,
    *,
    leverage_multiple: float,
    return_mode: str,
    expense_rate: float,
    margin_borrow_rate: float,
) -> pd.Series:
    leverage = float(leverage_multiple)
    if return_mode == RETURN_MODE_LEVERAGED_PRODUCT:
        daily_drag = float(expense_rate) / 252.0
    elif return_mode == RETURN_MODE_MARGIN_STOCK:
        daily_drag = max(0.0, leverage - 1.0) * float(margin_borrow_rate) / 252.0
    else:
        raise ValueError(f"Unsupported return_mode: {return_mode}")
    leveraged = returns.fillna(0.0) * leverage - daily_drag
    return leveraged.clip(lower=-1.0)


def _normalize_rebound_budget_signals(
    rebound_budget_signals,
    *,
    column: str,
) -> pd.DataFrame:
    if rebound_budget_signals is None:
        return pd.DataFrame(columns=["as_of", "active_until", "budget", "allow_hard_defense"])
    if isinstance(rebound_budget_signals, (str, Path)):
        rebound_budget_signals = read_table(rebound_budget_signals)
    frame = pd.DataFrame(rebound_budget_signals).copy()
    if frame.empty:
        return pd.DataFrame(columns=["as_of", "active_until", "budget", "allow_hard_defense"])

    normalized_columns = {str(raw).strip().lower(): raw for raw in frame.columns}
    date_column = next(
        (normalized_columns[name] for name in ("as_of", "signal_date", "date") if name in normalized_columns),
        None,
    )
    if date_column is None:
        raise ValueError("rebound budget signals require an as_of, signal_date, or date column")

    requested_column = str(column or "").strip()
    budget_column = requested_column if requested_column in frame.columns else None
    if budget_column is None:
        budget_column = next(
            (
                normalized_columns[name]
                for name in ("sleeve_suggestion", "rebound_budget", "product_exposure_boost")
                if name in normalized_columns
            ),
            None,
        )
    if budget_column is None:
        raise ValueError(
            "rebound budget signals require a sleeve_suggestion, rebound_budget, or product_exposure_boost column"
        )

    active_until_column = next(
        (
            normalized_columns[name]
            for name in ("active_until", "valid_until", "expires_at", "expiry_date")
            if name in normalized_columns
        ),
        None,
    )
    allow_hard_defense_column = next(
        (
            normalized_columns[name]
            for name in (
                "allow_hard_defense",
                "allow_rebound_budget_in_hard_defense",
                "hard_defense_allowed",
                "break_bear_allowed",
                "event_rebound_break_bear",
            )
            if name in normalized_columns
        ),
        None,
    )
    output = pd.DataFrame(
        {
            "as_of": pd.to_datetime(frame[date_column], errors="coerce").dt.tz_localize(None).dt.normalize(),
            "budget": pd.to_numeric(frame[budget_column], errors="coerce").fillna(0.0),
        }
    )
    if active_until_column is not None:
        output["active_until"] = pd.to_datetime(frame[active_until_column], errors="coerce").dt.tz_localize(
            None
        ).dt.normalize()
    else:
        output["active_until"] = pd.NaT
    if allow_hard_defense_column is not None:
        output["allow_hard_defense"] = frame[allow_hard_defense_column].map(_coerce_bool)
    else:
        output["allow_hard_defense"] = False
    output = output.dropna(subset=["as_of"]).sort_values("as_of")
    return output.reset_index(drop=True)


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on", "allow", "allowed"}


def build_rebound_budget_schedule(
    rebound_budget_signals,
    index: pd.DatetimeIndex,
    *,
    column: str = "sleeve_suggestion",
    active_days: int = DEFAULT_REBOUND_BUDGET_SIGNAL_ACTIVE_DAYS,
    cap: float = DEFAULT_REBOUND_BUDGET_CAP,
) -> pd.DataFrame:
    schedule = pd.DataFrame(
        {
            "rebound_budget_suggestion": 0.0,
            "rebound_budget_hard_defense_allowed": False,
        },
        index=index,
    )
    signals = _normalize_rebound_budget_signals(rebound_budget_signals, column=column)
    if signals.empty or schedule.empty:
        return schedule

    active_days = max(0, int(active_days))
    cap = max(0.0, float(cap))
    for row in signals.itertuples(index=False):
        signal_date = pd.Timestamp(row.as_of).normalize()
        active_until = (
            pd.Timestamp(row.active_until).normalize()
            if pd.notna(row.active_until)
            else signal_date + pd.Timedelta(days=active_days)
        )
        value = max(0.0, min(cap, float(row.budget)))
        mask = (schedule.index >= signal_date) & (schedule.index <= active_until)
        schedule.loc[mask, "rebound_budget_suggestion"] = value
        schedule.loc[mask, "rebound_budget_hard_defense_allowed"] = bool(row.allow_hard_defense)
    return schedule


def build_rebound_budget_series(
    rebound_budget_signals,
    index: pd.DatetimeIndex,
    *,
    column: str = "sleeve_suggestion",
    active_days: int = DEFAULT_REBOUND_BUDGET_SIGNAL_ACTIVE_DAYS,
    cap: float = DEFAULT_REBOUND_BUDGET_CAP,
) -> pd.Series:
    return build_rebound_budget_schedule(
        rebound_budget_signals,
        index,
        column=column,
        active_days=active_days,
        cap=cap,
    )["rebound_budget_suggestion"].rename("rebound_budget_suggestion")


def _resolve_strategy_name(dynamic_universe: pd.DataFrame | None) -> str:
    if dynamic_universe is None or dynamic_universe.empty:
        return PROFILE
    if "sector" in dynamic_universe.columns and dynamic_universe["sector"].astype(str).eq("mags_official").all():
        return MAGS_OFFICIAL_PROFILE
    return DYNAMIC_PROFILE


def run_backtest(
    price_history,
    universe_snapshot=None,
    *,
    start_date=None,
    end_date=None,
    symbols: Sequence[str] | str | None = None,
    candidate_universe_size: int | None = None,
    benchmark_symbol: str = BENCHMARK_SYMBOL,
    broad_benchmark_symbol: str = BROAD_BENCHMARK_SYMBOL,
    market_trend_symbol: str | None = None,
    safe_haven: str = SAFE_HAVEN,
    frequency: str = "weekly",
    top_n: int = 3,
    hold_buffer: int = 1,
    leverage_multiple: float = 2.0,
    max_product_exposure: float = RECOMMENDED_DYNAMIC_MAX_PRODUCT_EXPOSURE,
    soft_product_exposure: float = RECOMMENDED_DYNAMIC_SOFT_PRODUCT_EXPOSURE,
    hard_product_exposure: float = RECOMMENDED_DYNAMIC_HARD_PRODUCT_EXPOSURE,
    single_name_cap: float = RECOMMENDED_DYNAMIC_SINGLE_NAME_CAP,
    turnover_cost_bps: float = 5.0,
    return_mode: str = RETURN_MODE_LEVERAGED_PRODUCT,
    leveraged_expense_rate: float = 0.01,
    margin_borrow_rate: float = DEFAULT_MARGIN_BORROW_RATE,
    atr_period: int = DEFAULT_ATR_PERIOD,
    atr_entry_scale: float = DEFAULT_ATR_ENTRY_SCALE,
    entry_line_floor: float = DEFAULT_ENTRY_LINE_FLOOR,
    entry_line_cap: float = DEFAULT_ENTRY_LINE_CAP,
    atr_exit_scale: float = DEFAULT_ATR_EXIT_SCALE,
    exit_line_floor: float = DEFAULT_EXIT_LINE_FLOOR,
    exit_line_cap: float = DEFAULT_EXIT_LINE_CAP,
    rebound_budget_signals=None,
    rebound_budget_column: str = "sleeve_suggestion",
    rebound_budget_signal_active_days: int = DEFAULT_REBOUND_BUDGET_SIGNAL_ACTIVE_DAYS,
    rebound_budget_cap: float = DEFAULT_REBOUND_BUDGET_CAP,
    allow_rebound_budget_in_hard_defense: bool = False,
    bear_candidate_mode: str = BEAR_CANDIDATE_MODE_OFF,
    bear_candidate_max_size_multiplier: float = DEFAULT_BEAR_CANDIDATE_MAX_SIZE_MULTIPLIER,
):
    prices = _normalize_price_history(price_history)
    if end_date is not None:
        prices = prices.loc[prices["as_of"] <= pd.Timestamp(end_date).normalize()].copy()
    if prices.empty:
        raise RuntimeError("No usable price history remains inside the selected date range")

    symbols_tuple = split_symbols(symbols) or MAG7_POOL
    dynamic_universe = _normalize_universe_history(universe_snapshot) if universe_snapshot is not None else None
    if dynamic_universe is not None and not dynamic_universe.empty:
        symbols_tuple = tuple(dict.fromkeys(dynamic_universe["symbol"].astype(str).tolist()))
        if candidate_universe_size is None:
            candidate_universe_size = RECOMMENDED_DYNAMIC_CANDIDATE_UNIVERSE_SIZE
    strategy_name = _resolve_strategy_name(dynamic_universe)
    return_mode = str(return_mode or RETURN_MODE_LEVERAGED_PRODUCT).strip().lower()
    if return_mode not in RETURN_MODES:
        raise ValueError(f"return_mode must be one of: {', '.join(RETURN_MODES)}")
    bear_candidate_mode = str(bear_candidate_mode or BEAR_CANDIDATE_MODE_OFF).strip().lower()
    if bear_candidate_mode not in BEAR_CANDIDATE_MODES:
        raise ValueError(f"bear_candidate_mode must be one of: {', '.join(BEAR_CANDIDATE_MODES)}")
    benchmark_symbol = _normalize_symbol(benchmark_symbol)
    broad_benchmark_symbol = _normalize_symbol(broad_benchmark_symbol)
    market_trend_symbol = _normalize_symbol(market_trend_symbol or benchmark_symbol)
    safe_haven = _normalize_symbol(safe_haven)
    close_matrix, returns_matrix = _build_close_and_returns(prices)
    if safe_haven not in close_matrix.columns:
        close_matrix[safe_haven] = 1.0
        returns_matrix[safe_haven] = 0.0
    if safe_haven not in returns_matrix.columns:
        returns_matrix[safe_haven] = 0.0

    index = close_matrix.index
    if start_date is not None:
        index = index[index >= pd.Timestamp(start_date).normalize()]
    if len(index) < 2:
        raise RuntimeError("Not enough price history remains inside the selected date range")
    rebound_budget_schedule = build_rebound_budget_schedule(
        rebound_budget_signals,
        index,
        column=rebound_budget_column,
        active_days=int(rebound_budget_signal_active_days),
        cap=float(rebound_budget_cap),
    )
    has_rebound_budget_signals = rebound_budget_signals is not None and not _normalize_rebound_budget_signals(
        rebound_budget_signals,
        column=rebound_budget_column,
    ).empty

    rebalance_dates = _last_trading_day_by_period(index, frequency)
    weight_columns = sorted(set(symbols_tuple) | {safe_haven})
    weights_history = pd.DataFrame(0.0, index=index, columns=weight_columns)
    portfolio_returns = pd.Series(0.0, index=index, name="portfolio_return")
    turnover_history = pd.Series(0.0, index=index, name="turnover")
    exposure_rows: list[dict[str, object]] = []
    score_frames: list[pd.DataFrame] = []
    trade_rows: list[dict[str, object]] = []

    current_weights: dict[str, float] = {safe_haven: 1.0}
    current_holdings: set[str] = set()
    current_base_risk_active = False
    risk_sleeve_returns = {
        symbol: _risk_sleeve_daily_returns(
            returns_matrix[symbol].reindex(index),
            leverage_multiple=float(leverage_multiple),
            return_mode=return_mode,
            expense_rate=float(leveraged_expense_rate),
            margin_borrow_rate=float(margin_borrow_rate),
        )
        for symbol in symbols_tuple
        if symbol in returns_matrix.columns
    }
    active_candidate_cache: dict[pd.Timestamp, tuple[str, ...]] = {}

    def active_symbols_for(date: pd.Timestamp) -> tuple[str, ...]:
        if date not in active_candidate_cache:
            active_candidate_cache[date] = resolve_active_candidate_symbols(
                dynamic_universe,
                date,
                fallback_symbols=symbols_tuple,
                candidate_universe_size=candidate_universe_size,
                available_symbols=returns_matrix.columns,
            )
        return active_candidate_cache[date]

    for idx in range(len(index) - 1):
        date = index[idx]
        next_date = index[idx + 1]
        if date in rebalance_dates:
            regime_gross, regime, regime_diagnostics = _benchmark_regime(
                close_matrix,
                date,
                benchmark_symbol=market_trend_symbol,
                current_risk_active=current_base_risk_active,
                max_product_exposure=float(max_product_exposure),
                soft_product_exposure=float(soft_product_exposure),
                hard_product_exposure=float(hard_product_exposure),
                atr_period=int(atr_period),
                atr_entry_scale=float(atr_entry_scale),
                entry_line_floor=float(entry_line_floor),
                entry_line_cap=float(entry_line_cap),
                atr_exit_scale=float(atr_exit_scale),
                exit_line_floor=float(exit_line_floor),
                exit_line_cap=float(exit_line_cap),
            )
            feature_frame = _feature_frame_for_date(
                close_matrix,
                returns_matrix,
                date,
                active_symbols_for(date),
                benchmark_symbol=benchmark_symbol,
            )
            rebound_budget_suggestion = float(
                rebound_budget_schedule.at[date, "rebound_budget_suggestion"]
                if date in rebound_budget_schedule.index
                else 0.0
            )
            rebound_budget_hard_defense_allowed = bool(
                rebound_budget_schedule.at[date, "rebound_budget_hard_defense_allowed"]
                if date in rebound_budget_schedule.index
                else False
            )
            rebound_budget_applied = rebound_budget_suggestion
            if regime == "hard_defense" and not (
                bool(allow_rebound_budget_in_hard_defense) or rebound_budget_hard_defense_allowed
            ):
                rebound_budget_applied = 0.0
            market_bear_regime = str(regime) in MARKET_BEAR_REGIMES
            bear_candidates_allowed = (
                bear_candidate_mode == BEAR_CANDIDATE_MODE_ALWAYS
                or (bear_candidate_mode == BEAR_CANDIDATE_MODE_MARKET_SAFE and not market_bear_regime)
                or (bear_candidate_mode == BEAR_CANDIDATE_MODE_MARKET_BEAR and market_bear_regime)
            )
            target_product_exposure = min(
                float(max_product_exposure),
                max(0.0, float(regime_gross)) + max(0.0, rebound_budget_applied),
            )
            target_weights, ranked, metadata = build_target_weights(
                feature_frame,
                current_holdings,
                target_product_exposure=target_product_exposure,
                top_n=int(top_n),
                hold_buffer=int(hold_buffer),
                single_name_cap=float(single_name_cap),
                safe_haven=safe_haven,
                leverage_multiple=float(leverage_multiple),
                allow_bear_candidate_pullbacks=bear_candidates_allowed,
                bear_candidate_max_size_multiplier=float(bear_candidate_max_size_multiplier),
            )
            turnover = _compute_turnover(current_weights, target_weights)
            turnover_history.at[next_date] = turnover
            for symbol in sorted(set(current_weights) | set(target_weights)):
                old_weight = float(current_weights.get(symbol, 0.0))
                new_weight = float(target_weights.get(symbol, 0.0))
                if abs(new_weight - old_weight) > 1e-12:
                    trade_rows.append(
                        {
                            "signal_date": date,
                            "effective_date": next_date,
                            "symbol": symbol,
                            "old_weight": old_weight,
                            "new_weight": new_weight,
                            "delta_weight": new_weight - old_weight,
                        }
                    )
            if not ranked.empty:
                ranked = ranked.copy()
                if "as_of" in ranked.columns:
                    ranked["as_of"] = date
                else:
                    ranked.insert(0, "as_of", date)
                ranked["selected"] = ranked["symbol"].isin(metadata.get("selected_symbols", ()))
                score_frames.append(ranked)
            current_weights = target_weights
            current_base_risk_active = (
                float(regime_gross) > 1e-12 and float(metadata.get("product_exposure", 0.0)) > 1e-12
            )
            current_holdings = {
                symbol for symbol, weight in current_weights.items() if weight > 0.0 and symbol != safe_haven
            }
            exposure_rows.append(
                {
                    "signal_date": date,
                    "effective_date": next_date,
                    "regime": regime,
                    "base_target_product_exposure": regime_gross,
                    "rebound_budget_suggestion": rebound_budget_suggestion,
                    "rebound_budget_hard_defense_allowed": rebound_budget_hard_defense_allowed,
                    "rebound_budget_applied": rebound_budget_applied,
                    "target_product_exposure": target_product_exposure,
                    "product_exposure": float(metadata.get("product_exposure", 0.0)),
                    "underlying_exposure": float(metadata.get("underlying_exposure", 0.0)),
                    "leverage_multiple": float(leverage_multiple),
                    "bear_candidate_mode": bear_candidate_mode,
                    "bear_candidates_allowed": bool(bear_candidates_allowed),
                    "bear_selected_count": int(metadata.get("bear_selected_count", 0)),
                    "safe_haven_weight": float(target_weights.get(safe_haven, 0.0)),
                    "avg_pullback_depth": metadata.get("avg_pullback_depth"),
                    "candidate_symbols": ",".join(active_symbols_for(date)),
                    "selected_symbols": ",".join(metadata.get("selected_symbols", ())),
                    "turnover": turnover,
                    "market_trend_symbol": market_trend_symbol,
                    "return_mode": return_mode,
                    "leveraged_expense_rate": float(leveraged_expense_rate),
                    "margin_borrow_rate": float(margin_borrow_rate),
                    **regime_diagnostics,
                }
            )

        for symbol, weight in current_weights.items():
            if symbol in weights_history.columns:
                weights_history.at[date, symbol] = weight

        next_returns = returns_matrix.loc[next_date]
        gross_return = 0.0
        for symbol, weight in current_weights.items():
            if symbol == safe_haven:
                gross_return += weight * float(next_returns.get(symbol, 0.0))
            else:
                gross_return += weight * float(
                    risk_sleeve_returns.get(symbol, pd.Series(dtype=float)).get(next_date, 0.0)
                )
        trading_cost = turnover_history.at[next_date] * (float(turnover_cost_bps) / 10_000.0)
        portfolio_returns.at[next_date] = gross_return - trading_cost

    for symbol, weight in current_weights.items():
        if symbol in weights_history.columns:
            weights_history.at[index[-1], symbol] = weight

    used_weights = weights_history.loc[:, (weights_history != 0.0).any(axis=0)]
    exposure_history = pd.DataFrame(exposure_rows)
    reference_returns = pd.DataFrame(index=index)
    for symbol in (benchmark_symbol, broad_benchmark_symbol, market_trend_symbol):
        if symbol in returns_matrix.columns and symbol not in reference_returns.columns:
            reference_returns[symbol] = returns_matrix[symbol].reindex(index)
    available_pool_symbols = [symbol for symbol in symbols_tuple if symbol in returns_matrix.columns]
    if available_pool_symbols:
        pool_reference_name = "equal_weight_dynamic_pool" if dynamic_universe is not None else "equal_weight_mag7"
        leveraged_pool_reference_name = f"{pool_reference_name}_2x"
        if dynamic_universe is None:
            reference_returns[pool_reference_name] = returns_matrix.loc[index, available_pool_symbols].mean(axis=1)
            reference_returns[leveraged_pool_reference_name] = pd.DataFrame(
                {
                    symbol: risk_sleeve_returns[symbol].reindex(index)
                    for symbol in available_pool_symbols
                    if symbol in risk_sleeve_returns
                },
                index=index,
            ).mean(axis=1)
            for symbol in available_pool_symbols:
                if symbol in risk_sleeve_returns:
                    reference_returns[f"{symbol}_2x"] = risk_sleeve_returns[symbol].reindex(index)
        else:
            reference_returns[pool_reference_name] = pd.Series(0.0, index=index)
            reference_returns[leveraged_pool_reference_name] = pd.Series(0.0, index=index)
            for date in index:
                active_symbols = [symbol for symbol in active_symbols_for(date) if symbol in returns_matrix.columns]
                if active_symbols:
                    reference_returns.at[date, pool_reference_name] = float(returns_matrix.loc[date, active_symbols].mean())
                    active_leveraged = [symbol for symbol in active_symbols if symbol in risk_sleeve_returns]
                    if active_leveraged:
                        reference_returns.at[date, leveraged_pool_reference_name] = float(
                            pd.Series(
                                {symbol: risk_sleeve_returns[symbol].get(date, 0.0) for symbol in active_leveraged}
                            ).mean()
                        )

    strategy_summary_name = strategy_name
    if has_rebound_budget_signals:
        strategy_summary_name = f"{strategy_summary_name}{REBOUND_BUDGET_STRATEGY_SUFFIX}"
    if bear_candidate_mode != BEAR_CANDIDATE_MODE_OFF:
        strategy_summary_name = f"{strategy_summary_name}_bear_{bear_candidate_mode}"

    summary_rows = [
        summarize_returns(
            portfolio_returns,
            strategy_name=strategy_summary_name,
            return_mode=return_mode,
            leverage_multiple=float(leverage_multiple),
            weights_history=used_weights,
            exposure_history=exposure_history,
        )
    ]
    for name in [benchmark_symbol, broad_benchmark_symbol, "equal_weight_mag7", "equal_weight_dynamic_pool"]:
        if name in reference_returns.columns:
            summary_rows.append(_reference_summary(name, reference_returns[name]))
    leveraged_pool_name = "equal_weight_dynamic_pool_2x" if dynamic_universe is not None else "equal_weight_mag7_2x"
    if leveraged_pool_name in reference_returns.columns:
        summary_rows.append(
            _reference_summary(
                leveraged_pool_name,
                reference_returns[leveraged_pool_name],
                return_mode=return_mode,
                leverage_multiple=float(leverage_multiple),
            )
        )
    if dynamic_universe is None:
        for symbol in symbols_tuple:
            name = f"{symbol}_2x"
            if name in reference_returns.columns:
                summary_rows.append(
                    _reference_summary(
                        name,
                        reference_returns[name],
                        return_mode=return_mode,
                        leverage_multiple=float(leverage_multiple),
                    )
                )
    summary = pd.DataFrame(summary_rows).loc[:, list(SUMMARY_COLUMNS)]

    return {
        "portfolio_returns": portfolio_returns,
        "weights_history": used_weights,
        "turnover_history": turnover_history,
        "candidate_scores": pd.concat(score_frames, ignore_index=True) if score_frames else pd.DataFrame(),
        "trades": pd.DataFrame(trade_rows),
        "exposure_history": exposure_history,
        "reference_returns": reference_returns,
        "summary": summary,
    }


def _format_percent_columns(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    for column in (
        "Total Return",
        "CAGR",
        "Max Drawdown",
        "Volatility",
        "Avg Product Exposure",
        "Avg Underlying Exposure",
        "Turnover/Year",
    ):
        if column in output:
            output[column] = output[column].map(lambda value: f"{float(value):.2%}" if pd.notna(value) else "")
    for column in ("Sharpe", "Calmar", "Leverage Multiple", "Rebalances/Year", "Final Equity"):
        if column in output:
            output[column] = output[column].map(lambda value: f"{float(value):.2f}" if pd.notna(value) else "")
    return output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backtest leveraged leader pullback/high-trim research strategy.")
    parser.add_argument("--prices", required=True, help="Input price history file (.csv/.json/.jsonl/.parquet)")
    parser.add_argument(
        "--universe",
        help="Optional point-in-time universe history. When omitted, the strategy uses the static MAG7 pool.",
    )
    parser.add_argument("--start", dest="start_date", default="2016-01-01", help="Backtest start date")
    parser.add_argument("--end", dest="end_date", help="Backtest end date")
    parser.add_argument("--symbols", help="Comma-separated custom pool; defaults to MAG7")
    parser.add_argument(
        "--candidate-universe-size",
        type=int,
        help=f"Top-N active universe names considered at each rebalance; defaults to {RECOMMENDED_DYNAMIC_CANDIDATE_UNIVERSE_SIZE} with --universe",
    )
    parser.add_argument("--benchmark-symbol", default=BENCHMARK_SYMBOL)
    parser.add_argument("--broad-benchmark-symbol", default=BROAD_BENCHMARK_SYMBOL)
    parser.add_argument(
        "--market-trend-symbol",
        help="Optional symbol used for the market 200SMA risk filter; defaults to --benchmark-symbol.",
    )
    parser.add_argument("--safe-haven", default=SAFE_HAVEN)
    parser.add_argument("--frequency", choices=("weekly", "monthly"), default="weekly")
    parser.add_argument("--top-n", type=int, default=RECOMMENDED_DYNAMIC_TOP_N)
    parser.add_argument("--hold-buffer", type=int, default=1)
    parser.add_argument("--leverage-multiple", type=float, default=2.0)
    parser.add_argument("--max-product-exposure", type=float, default=RECOMMENDED_DYNAMIC_MAX_PRODUCT_EXPOSURE)
    parser.add_argument("--soft-product-exposure", type=float, default=RECOMMENDED_DYNAMIC_SOFT_PRODUCT_EXPOSURE)
    parser.add_argument("--hard-product-exposure", type=float, default=RECOMMENDED_DYNAMIC_HARD_PRODUCT_EXPOSURE)
    parser.add_argument("--single-name-cap", type=float, default=RECOMMENDED_DYNAMIC_SINGLE_NAME_CAP)
    parser.add_argument("--turnover-cost-bps", type=float, default=5.0)
    parser.add_argument("--return-mode", choices=RETURN_MODES, default=RETURN_MODE_LEVERAGED_PRODUCT)
    parser.add_argument("--leveraged-expense-rate", type=float, default=0.01)
    parser.add_argument("--margin-borrow-rate", type=float, default=DEFAULT_MARGIN_BORROW_RATE)
    parser.add_argument("--atr-period", type=int, default=DEFAULT_ATR_PERIOD)
    parser.add_argument("--atr-entry-scale", type=float, default=DEFAULT_ATR_ENTRY_SCALE)
    parser.add_argument("--entry-line-floor", type=float, default=DEFAULT_ENTRY_LINE_FLOOR)
    parser.add_argument("--entry-line-cap", type=float, default=DEFAULT_ENTRY_LINE_CAP)
    parser.add_argument("--atr-exit-scale", type=float, default=DEFAULT_ATR_EXIT_SCALE)
    parser.add_argument("--exit-line-floor", type=float, default=DEFAULT_EXIT_LINE_FLOOR)
    parser.add_argument("--exit-line-cap", type=float, default=DEFAULT_EXIT_LINE_CAP)
    parser.add_argument(
        "--rebound-budget-signals",
        help=(
            "Optional TACO rebound budget signal CSV/JSON. Expected columns: "
            "as_of and sleeve_suggestion; active_until and allow_hard_defense are optional."
        ),
    )
    parser.add_argument("--rebound-budget-column", default="sleeve_suggestion")
    parser.add_argument(
        "--rebound-budget-signal-active-days",
        type=int,
        default=DEFAULT_REBOUND_BUDGET_SIGNAL_ACTIVE_DAYS,
    )
    parser.add_argument("--rebound-budget-cap", type=float, default=DEFAULT_REBOUND_BUDGET_CAP)
    parser.add_argument(
        "--allow-rebound-budget-in-hard-defense",
        action="store_true",
        help="Research-only override. Default keeps hard-defense regimes fully defensive.",
    )
    parser.add_argument(
        "--bear-candidate-mode",
        choices=BEAR_CANDIDATE_MODES,
        default=BEAR_CANDIDATE_MODE_OFF,
        help=(
            "Research-only switch for below-200SMA single-name rebound candidates. "
            "market_safe allows them only when the broad market is not defensive; "
            "market_bear allows them only during soft/hard defense."
        ),
    )
    parser.add_argument(
        "--bear-candidate-max-size-multiplier",
        type=float,
        default=DEFAULT_BEAR_CANDIDATE_MAX_SIZE_MULTIPLIER,
        help="Cap for bear-candidate position sizing before normal exposure and single-name caps.",
    )
    parser.add_argument("--output-dir", help="Optional output directory for research artifacts")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_backtest(
        read_table(args.prices),
        read_table(args.universe) if args.universe else None,
        start_date=args.start_date,
        end_date=args.end_date,
        symbols=args.symbols,
        candidate_universe_size=args.candidate_universe_size,
        benchmark_symbol=args.benchmark_symbol,
        broad_benchmark_symbol=args.broad_benchmark_symbol,
        market_trend_symbol=args.market_trend_symbol,
        safe_haven=args.safe_haven,
        frequency=args.frequency,
        top_n=args.top_n,
        hold_buffer=args.hold_buffer,
        leverage_multiple=args.leverage_multiple,
        max_product_exposure=args.max_product_exposure,
        soft_product_exposure=args.soft_product_exposure,
        hard_product_exposure=args.hard_product_exposure,
        single_name_cap=args.single_name_cap,
        turnover_cost_bps=args.turnover_cost_bps,
        return_mode=args.return_mode,
        leveraged_expense_rate=args.leveraged_expense_rate,
        margin_borrow_rate=args.margin_borrow_rate,
        atr_period=args.atr_period,
        atr_entry_scale=args.atr_entry_scale,
        entry_line_floor=args.entry_line_floor,
        entry_line_cap=args.entry_line_cap,
        atr_exit_scale=args.atr_exit_scale,
        exit_line_floor=args.exit_line_floor,
        exit_line_cap=args.exit_line_cap,
        rebound_budget_signals=read_table(args.rebound_budget_signals) if args.rebound_budget_signals else None,
        rebound_budget_column=args.rebound_budget_column,
        rebound_budget_signal_active_days=args.rebound_budget_signal_active_days,
        rebound_budget_cap=args.rebound_budget_cap,
        allow_rebound_budget_in_hard_defense=args.allow_rebound_budget_in_hard_defense,
        bear_candidate_mode=args.bear_candidate_mode,
        bear_candidate_max_size_multiplier=args.bear_candidate_max_size_multiplier,
    )

    summary = result["summary"]
    print(_format_percent_columns(summary).to_string(index=False))

    if args.output_dir:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        summary.to_csv(output_dir / "summary.csv", index=False)
        result["portfolio_returns"].rename("portfolio_return").to_csv(output_dir / "portfolio_returns.csv")
        result["weights_history"].to_csv(output_dir / "weights_history.csv")
        result["turnover_history"].rename("turnover").to_csv(output_dir / "turnover_history.csv")
        result["candidate_scores"].to_csv(output_dir / "candidate_scores.csv", index=False)
        result["trades"].to_csv(output_dir / "trades.csv", index=False)
        result["exposure_history"].to_csv(output_dir / "exposure_history.csv", index=False)
        result["reference_returns"].to_csv(output_dir / "reference_returns.csv")
        print(f"wrote leveraged pullback outputs -> {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
