from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from .russell_1000_multi_factor_defensive_snapshot import read_table, write_table
from .yfinance_prices import download_price_history

PROFILE = "mega_cap_leader_rotation"
BENCHMARK_SYMBOL = "QQQ"
BROAD_BENCHMARK_SYMBOL = "SPY"
SAFE_HAVEN = "BOXX"
MAG7_POOL = ("AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA")
EXPANDED_POOL = (
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "GOOGL",
    "META",
    "TSLA",
    "AVGO",
    "NFLX",
    "AMD",
    "COST",
    "JPM",
    "BRK.B",
    "LLY",
)
DEFAULT_SECTORS = {
    "AAPL": "Information Technology",
    "MSFT": "Information Technology",
    "NVDA": "Information Technology",
    "AMZN": "Consumer Discretionary",
    "GOOGL": "Communication Services",
    "META": "Communication Services",
    "TSLA": "Consumer Discretionary",
    "AVGO": "Information Technology",
    "NFLX": "Communication Services",
    "AMD": "Information Technology",
    "COST": "Consumer Staples",
    "JPM": "Financials",
    "BRK.B": "Financials",
    "LLY": "Health Care",
}
POOL_SYMBOLS = {
    "mag7": MAG7_POOL,
    "expanded": EXPANDED_POOL,
}
BACKTEST_SUMMARY_COLUMNS = (
    "Strategy",
    "Pool",
    "Start",
    "End",
    "Total Return",
    "CAGR",
    "Max Drawdown",
    "Volatility",
    "Sharpe",
    "Calmar",
    "Rebalances/Year",
    "Turnover/Year",
    "Avg Stock Exposure",
    "Final Equity",
    "Benchmark Symbol",
    "Benchmark Total Return",
    "Benchmark Corr",
    "Broad Benchmark Symbol",
    "Broad Benchmark Total Return",
    "Equal Weight Pool Total Return",
)


@dataclass(frozen=True)
class MegaCapResearchDataResult:
    output_dir: Path
    prices_path: Path
    universe_path: Path
    price_rows: int
    symbols: tuple[str, ...]


def split_symbols(raw_symbols: str | Iterable[str] | None) -> tuple[str, ...]:
    if raw_symbols is None:
        return ()
    values = raw_symbols.split(",") if isinstance(raw_symbols, str) else list(raw_symbols)
    return tuple(dict.fromkeys(str(value).strip().upper() for value in values if str(value).strip()))


def resolve_pool_symbols(pool: str, *, symbols: Sequence[str] | str | None = None) -> tuple[str, ...]:
    explicit = split_symbols(symbols)
    if explicit:
        return explicit
    normalized_pool = str(pool or "").strip().lower().replace("-", "_")
    if normalized_pool not in POOL_SYMBOLS:
        known = ", ".join(sorted(POOL_SYMBOLS))
        raise ValueError(f"Unknown mega-cap pool {pool!r}; known pools: {known}")
    return POOL_SYMBOLS[normalized_pool]


def build_static_universe(pool: str = "expanded", *, symbols: Sequence[str] | str | None = None) -> pd.DataFrame:
    pool_symbols = resolve_pool_symbols(pool, symbols=symbols)
    rows = [
        {
            "symbol": symbol,
            "sector": DEFAULT_SECTORS.get(symbol, "unknown"),
        }
        for symbol in pool_symbols
    ]
    return pd.DataFrame(rows, columns=["symbol", "sector"])


def prepare_research_input_data(
    *,
    output_dir: str | Path,
    pool: str = "expanded",
    symbols: Sequence[str] | str | None = None,
    price_start: str = "2015-01-01",
    price_end: str | None = None,
    benchmark_symbol: str = BENCHMARK_SYMBOL,
    broad_benchmark_symbol: str = BROAD_BENCHMARK_SYMBOL,
    safe_haven: str = SAFE_HAVEN,
) -> MegaCapResearchDataResult:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    universe = build_static_universe(pool, symbols=symbols)
    benchmark_symbol = _normalize_symbol(benchmark_symbol)
    broad_benchmark_symbol = _normalize_symbol(broad_benchmark_symbol)
    safe_haven = _normalize_symbol(safe_haven)
    download_symbols = tuple(
        dict.fromkeys(
            [
                *universe["symbol"].astype(str).tolist(),
                benchmark_symbol,
                broad_benchmark_symbol,
                safe_haven,
            ]
        )
    )
    prices = download_price_history(list(download_symbols), start=price_start, end=price_end, chunk_size=25)

    universe_path = root / f"{PROFILE}_{pool}_universe.csv"
    prices_path = root / f"{PROFILE}_{pool}_price_history.csv"
    write_table(universe, universe_path)
    write_table(prices, prices_path)
    return MegaCapResearchDataResult(
        output_dir=root,
        prices_path=prices_path,
        universe_path=universe_path,
        price_rows=int(len(prices)),
        symbols=download_symbols,
    )


def _normalize_symbol(value: str) -> str:
    return str(value or "").strip().upper()


def _normalize_price_history(price_history) -> pd.DataFrame:
    frame = pd.DataFrame(price_history).copy()
    required = {"symbol", "as_of", "close"}
    missing = required - set(frame.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"price_history missing required columns: {missing_text}")

    frame["symbol"] = frame["symbol"].map(_normalize_symbol)
    frame["as_of"] = pd.to_datetime(frame["as_of"], utc=False).dt.tz_localize(None).dt.normalize()
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    if "volume" not in frame.columns:
        frame["volume"] = pd.NA
    frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce")
    frame = frame.loc[frame["symbol"].ne("")].dropna(subset=["as_of", "close"])
    return (
        frame.loc[:, ["symbol", "as_of", "close", "volume"]]
        .drop_duplicates(subset=["symbol", "as_of"], keep="last")
        .sort_values(["as_of", "symbol"])
        .reset_index(drop=True)
    )


def _normalize_universe(universe_snapshot) -> pd.DataFrame:
    frame = pd.DataFrame(universe_snapshot).copy()
    required = {"symbol", "sector"}
    missing = required - set(frame.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"universe_snapshot missing required columns: {missing_text}")

    frame["symbol"] = frame["symbol"].map(_normalize_symbol)
    frame["sector"] = frame["sector"].fillna("unknown").astype(str).str.strip().replace("", "unknown")
    for column in ("start_date", "end_date"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce").dt.tz_localize(None).dt.normalize()
    return frame.loc[frame["symbol"].ne("")].reset_index(drop=True)


def resolve_active_universe(universe_snapshot: pd.DataFrame, as_of_date) -> pd.DataFrame:
    as_of = pd.Timestamp(as_of_date).tz_localize(None).normalize()
    frame = universe_snapshot.copy()

    if "start_date" in frame.columns:
        frame = frame.loc[frame["start_date"].isna() | (frame["start_date"] <= as_of)]
    if "end_date" in frame.columns:
        frame = frame.loc[frame["end_date"].isna() | (frame["end_date"] >= as_of)]

    return frame.loc[:, ["symbol", "sector"]].drop_duplicates(subset=["symbol"], keep="last").reset_index(drop=True)


def build_monthly_rebalance_dates(index: pd.DatetimeIndex) -> set[pd.Timestamp]:
    if index.empty:
        return set()
    series = pd.Series(index, index=index)
    grouped = series.groupby(index.to_period("M")).max()
    return set(pd.to_datetime(grouped.values))


def _compute_turnover(previous_weights: Mapping[str, float], new_weights: Mapping[str, float]) -> float:
    symbols = set(previous_weights) | set(new_weights)
    return 0.5 * sum(
        abs(float(new_weights.get(symbol, 0.0)) - float(previous_weights.get(symbol, 0.0)))
        for symbol in symbols
    )


def _compute_window_drawdown(closes: pd.Series) -> float:
    if closes.empty:
        return float("nan")
    running_peak = closes.cummax()
    drawdown = closes / running_peak - 1.0
    return float(drawdown.min())


def _precompute_symbol_feature_history(price_history: pd.DataFrame) -> dict[str, pd.DataFrame]:
    feature_history: dict[str, pd.DataFrame] = {}
    for symbol, group in price_history.groupby("symbol", sort=False):
        history = group.sort_values("as_of").reset_index(drop=True).copy()
        closes = pd.to_numeric(history["close"], errors="coerce")
        volumes = pd.to_numeric(history["volume"], errors="coerce")
        returns = closes.pct_change(fill_method=None)
        dollar_volume = closes * volumes
        rolling_252_high = closes.rolling(252).max()

        feature_history[str(symbol)] = pd.DataFrame(
            {
                "as_of": history["as_of"],
                "close": closes,
                "volume": volumes,
                "adv20_usd": dollar_volume.rolling(20).mean(),
                "history_days": np.arange(1, len(history) + 1, dtype=int),
                "mom_3m": closes / closes.shift(63) - 1.0,
                "mom_6m": closes / closes.shift(126) - 1.0,
                "mom_12_1": closes.shift(21) / closes.shift(273) - 1.0,
                "sma200_gap": closes / closes.rolling(200).mean() - 1.0,
                "high_252_gap": closes / rolling_252_high - 1.0,
                "vol_63": returns.rolling(63).std(ddof=0) * np.sqrt(252),
            }
        )
    return feature_history


def _feature_row_at(
    *,
    symbol: str,
    sector: str,
    as_of: pd.Timestamp,
    history: pd.DataFrame | None,
    min_price_usd: float,
    min_adv20_usd: float,
    min_history_days: int,
    drawdown_window: int,
    force_ineligible: bool = False,
) -> dict[str, object]:
    empty_row = {
        "as_of": as_of,
        "symbol": symbol,
        "sector": sector,
        "close": float("nan"),
        "volume": float("nan"),
        "adv20_usd": float("nan"),
        "history_days": 0,
        "mom_3m": float("nan"),
        "mom_6m": float("nan"),
        "mom_12_1": float("nan"),
        "sma200_gap": float("nan"),
        "high_252_gap": float("nan"),
        "vol_63": float("nan"),
        "maxdd_126": float("nan"),
        "eligible": False,
    }
    if history is None or history.empty:
        return empty_row

    cutoff = int(history["as_of"].searchsorted(as_of, side="right"))
    if cutoff <= 0:
        return empty_row

    current = history.iloc[cutoff - 1]
    closes_window = history["close"].iloc[max(0, cutoff - drawdown_window) : cutoff]
    maxdd_126 = _compute_window_drawdown(closes_window) if len(closes_window) >= drawdown_window else float("nan")
    feature_values = (
        current["mom_3m"],
        current["mom_6m"],
        current["mom_12_1"],
        current["sma200_gap"],
        current["high_252_gap"],
        current["vol_63"],
        maxdd_126,
    )
    adv20_usd = current["adv20_usd"]
    eligible = (
        not force_ineligible
        and int(current["history_days"]) >= min_history_days
        and pd.notna(current["close"])
        and float(current["close"]) > min_price_usd
        and pd.notna(adv20_usd)
        and float(adv20_usd) >= min_adv20_usd
        and all(pd.notna(value) for value in feature_values)
    )
    return {
        "as_of": as_of,
        "symbol": symbol,
        "sector": sector,
        "close": float(current["close"]) if pd.notna(current["close"]) else float("nan"),
        "volume": float(current["volume"]) if pd.notna(current["volume"]) else float("nan"),
        "adv20_usd": float(adv20_usd) if pd.notna(adv20_usd) else float("nan"),
        "history_days": int(current["history_days"]),
        "mom_3m": float(current["mom_3m"]) if pd.notna(current["mom_3m"]) else float("nan"),
        "mom_6m": float(current["mom_6m"]) if pd.notna(current["mom_6m"]) else float("nan"),
        "mom_12_1": float(current["mom_12_1"]) if pd.notna(current["mom_12_1"]) else float("nan"),
        "sma200_gap": float(current["sma200_gap"]) if pd.notna(current["sma200_gap"]) else float("nan"),
        "high_252_gap": float(current["high_252_gap"]) if pd.notna(current["high_252_gap"]) else float("nan"),
        "vol_63": float(current["vol_63"]) if pd.notna(current["vol_63"]) else float("nan"),
        "maxdd_126": maxdd_126,
        "eligible": bool(eligible),
    }


def build_feature_snapshot_for_backtest(
    as_of_date: pd.Timestamp,
    active_universe: pd.DataFrame,
    feature_history_by_symbol: Mapping[str, pd.DataFrame],
    *,
    benchmark_symbol: str = BENCHMARK_SYMBOL,
    broad_benchmark_symbol: str = BROAD_BENCHMARK_SYMBOL,
    safe_haven: str = SAFE_HAVEN,
    min_price_usd: float = 10.0,
    min_adv20_usd: float = 20_000_000.0,
    min_history_days: int = 273,
    drawdown_window: int = 126,
) -> pd.DataFrame:
    as_of = pd.Timestamp(as_of_date).tz_localize(None).normalize()
    universe = active_universe.copy()
    universe["symbol"] = universe["symbol"].map(_normalize_symbol)
    universe["sector"] = universe["sector"].fillna("unknown").astype(str).str.strip().replace("", "unknown")
    universe = universe.drop_duplicates(subset=["symbol"], keep="last")

    benchmark_symbol = _normalize_symbol(benchmark_symbol)
    broad_benchmark_symbol = _normalize_symbol(broad_benchmark_symbol)
    safe_haven = _normalize_symbol(safe_haven)
    symbols = universe["symbol"].tolist()
    for extra_symbol in (benchmark_symbol, broad_benchmark_symbol, safe_haven):
        if extra_symbol and extra_symbol not in symbols:
            symbols.append(extra_symbol)
    sector_map = dict(zip(universe["symbol"], universe["sector"]))

    rows = []
    for symbol in symbols:
        force_ineligible = symbol in {benchmark_symbol, broad_benchmark_symbol, safe_haven}
        sector = sector_map.get(symbol, "benchmark" if symbol in {benchmark_symbol, broad_benchmark_symbol} else "cash")
        rows.append(
            _feature_row_at(
                symbol=symbol,
                sector=sector,
                as_of=as_of,
                history=feature_history_by_symbol.get(symbol),
                min_price_usd=min_price_usd,
                min_adv20_usd=min_adv20_usd,
                min_history_days=min_history_days,
                drawdown_window=drawdown_window,
                force_ineligible=force_ineligible,
            )
        )
    snapshot = pd.DataFrame(rows).sort_values("symbol").reset_index(drop=True)

    benchmark_row = snapshot.loc[snapshot["symbol"] == benchmark_symbol]
    broad_row = snapshot.loc[snapshot["symbol"] == broad_benchmark_symbol]
    benchmark_mom_6m = float(benchmark_row["mom_6m"].iloc[0]) if not benchmark_row.empty else float("nan")
    broad_mom_6m = float(broad_row["mom_6m"].iloc[0]) if not broad_row.empty else float("nan")
    snapshot["rel_mom_6m_vs_benchmark"] = snapshot["mom_6m"] - benchmark_mom_6m
    snapshot["rel_mom_6m_vs_broad_benchmark"] = snapshot["mom_6m"] - broad_mom_6m
    return snapshot


def _zscore(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    std = numeric.std(ddof=0)
    if pd.isna(std) or float(std) == 0.0:
        return pd.Series(0.0, index=values.index)
    return (numeric - numeric.mean()) / std


def score_candidates(
    snapshot: pd.DataFrame,
    current_holdings: Iterable[str] | None = None,
    *,
    benchmark_symbol: str = BENCHMARK_SYMBOL,
    broad_benchmark_symbol: str = BROAD_BENCHMARK_SYMBOL,
    safe_haven: str = SAFE_HAVEN,
    hold_bonus: float = 0.10,
) -> pd.DataFrame:
    frame = pd.DataFrame(snapshot).copy()
    if frame.empty:
        return pd.DataFrame()
    excluded = {
        _normalize_symbol(benchmark_symbol),
        _normalize_symbol(broad_benchmark_symbol),
        _normalize_symbol(safe_haven),
    }
    current_holdings_set = set(split_symbols(current_holdings))
    feature_columns = [
        "mom_3m",
        "mom_6m",
        "rel_mom_6m_vs_benchmark",
        "rel_mom_6m_vs_broad_benchmark",
        "high_252_gap",
        "sma200_gap",
        "vol_63",
        "maxdd_126",
    ]
    eligible = frame.loc[
        ~frame["symbol"].isin(excluded)
        & frame["eligible"].astype(bool)
        & frame[feature_columns].notna().all(axis=1)
    ].copy()
    if eligible.empty:
        return pd.DataFrame(columns=["rank", "symbol", "score", "eligible"])

    for column in feature_columns:
        eligible[f"z_{column}"] = _zscore(eligible[column])
    eligible["drawdown_abs"] = eligible["maxdd_126"].abs()
    eligible["z_drawdown_abs"] = _zscore(eligible["drawdown_abs"])
    eligible["score"] = (
        eligible["z_mom_6m"] * 0.25
        + eligible["z_mom_3m"] * 0.20
        + eligible["z_rel_mom_6m_vs_benchmark"] * 0.20
        + eligible["z_rel_mom_6m_vs_broad_benchmark"] * 0.10
        + eligible["z_high_252_gap"] * 0.10
        + eligible["z_sma200_gap"] * 0.10
        - eligible["z_vol_63"] * 0.025
        - eligible["z_drawdown_abs"] * 0.025
    )
    if current_holdings_set:
        eligible.loc[eligible["symbol"].isin(current_holdings_set), "score"] += float(hold_bonus)
    ranked = eligible.sort_values(
        by=["score", "rel_mom_6m_vs_benchmark", "mom_6m", "symbol"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    ranked.insert(0, "rank", range(1, len(ranked) + 1))
    output_columns = [
        "rank",
        "symbol",
        "sector",
        "score",
        "eligible",
        "close",
        "adv20_usd",
        "mom_3m",
        "mom_6m",
        "mom_12_1",
        "rel_mom_6m_vs_benchmark",
        "rel_mom_6m_vs_broad_benchmark",
        "high_252_gap",
        "sma200_gap",
        "vol_63",
        "maxdd_126",
    ]
    return ranked.loc[:, [column for column in output_columns if column in ranked.columns]]


def _resolve_stock_exposure(
    snapshot: pd.DataFrame,
    *,
    benchmark_symbol: str,
    broad_benchmark_symbol: str,
    safe_haven: str,
    soft_breadth_threshold: float,
    hard_breadth_threshold: float,
    risk_on_exposure: float,
    soft_defense_exposure: float,
    hard_defense_exposure: float,
) -> tuple[float, str, float, bool]:
    excluded = {
        _normalize_symbol(benchmark_symbol),
        _normalize_symbol(broad_benchmark_symbol),
        _normalize_symbol(safe_haven),
    }
    candidates = snapshot.loc[~snapshot["symbol"].isin(excluded) & snapshot["eligible"].astype(bool)]
    breadth_ratio = float((candidates["sma200_gap"] > 0).mean()) if not candidates.empty else 0.0
    benchmark_rows = snapshot.loc[snapshot["symbol"] == _normalize_symbol(benchmark_symbol)]
    benchmark_trend_positive = bool(
        not benchmark_rows.empty
        and pd.notna(benchmark_rows["sma200_gap"].iloc[0])
        and float(benchmark_rows["sma200_gap"].iloc[0]) > 0
    )
    if (not benchmark_trend_positive) and breadth_ratio < hard_breadth_threshold:
        return float(hard_defense_exposure), "hard_defense", breadth_ratio, benchmark_trend_positive
    if (not benchmark_trend_positive) or breadth_ratio < soft_breadth_threshold:
        return float(soft_defense_exposure), "soft_defense", breadth_ratio, benchmark_trend_positive
    return float(risk_on_exposure), "risk_on", breadth_ratio, benchmark_trend_positive


def build_target_weights(
    snapshot: pd.DataFrame,
    current_holdings: Iterable[str] | None = None,
    *,
    benchmark_symbol: str = BENCHMARK_SYMBOL,
    broad_benchmark_symbol: str = BROAD_BENCHMARK_SYMBOL,
    safe_haven: str = SAFE_HAVEN,
    top_n: int = 3,
    hold_buffer: int = 2,
    single_name_cap: float = 0.35,
    hold_bonus: float = 0.10,
    risk_on_exposure: float = 1.0,
    soft_defense_exposure: float = 0.50,
    hard_defense_exposure: float = 0.20,
    soft_breadth_threshold: float = 0.50,
    hard_breadth_threshold: float = 0.30,
) -> tuple[dict[str, float], pd.DataFrame, dict[str, object]]:
    if top_n <= 0:
        raise ValueError("top_n must be positive")
    safe_haven = _normalize_symbol(safe_haven)
    stock_exposure, regime, breadth_ratio, benchmark_trend_positive = _resolve_stock_exposure(
        snapshot,
        benchmark_symbol=benchmark_symbol,
        broad_benchmark_symbol=broad_benchmark_symbol,
        safe_haven=safe_haven,
        soft_breadth_threshold=float(soft_breadth_threshold),
        hard_breadth_threshold=float(hard_breadth_threshold),
        risk_on_exposure=float(risk_on_exposure),
        soft_defense_exposure=float(soft_defense_exposure),
        hard_defense_exposure=float(hard_defense_exposure),
    )
    ranked = score_candidates(
        snapshot,
        current_holdings,
        benchmark_symbol=benchmark_symbol,
        broad_benchmark_symbol=broad_benchmark_symbol,
        safe_haven=safe_haven,
        hold_bonus=hold_bonus,
    )
    metadata: dict[str, object] = {
        "regime": regime,
        "breadth_ratio": breadth_ratio,
        "benchmark_trend_positive": benchmark_trend_positive,
        "stock_exposure": stock_exposure,
        "selected_symbols": (),
    }
    if ranked.empty or stock_exposure <= 0:
        return {safe_haven: 1.0}, ranked, metadata

    current_holdings_set = set(split_symbols(current_holdings))
    ranked_symbols = ranked["symbol"].astype(str).tolist()
    rank_map = dict(zip(ranked["symbol"].astype(str), ranked["rank"].astype(int)))
    max_hold_rank = int(top_n) + max(int(hold_buffer), 0)
    selected = [
        symbol
        for symbol in ranked_symbols
        if symbol in current_holdings_set and rank_map[symbol] <= max_hold_rank
    ]
    for symbol in ranked_symbols:
        if len(selected) >= int(top_n):
            break
        if symbol not in selected:
            selected.append(symbol)

    if not selected:
        return {safe_haven: 1.0}, ranked, metadata
    selected = selected[: int(top_n)]
    per_name_weight = min(float(single_name_cap), stock_exposure / len(selected))
    weights = {symbol: per_name_weight for symbol in selected}
    safe_weight = max(0.0, 1.0 - sum(weights.values()))
    if safe_weight > 1e-12:
        weights[safe_haven] = safe_weight
    metadata["selected_symbols"] = tuple(selected)
    return weights, ranked, metadata


def summarize_returns(
    portfolio_returns: pd.Series,
    *,
    weights_history: pd.DataFrame | None = None,
    benchmark_returns: pd.Series | None = None,
    broad_benchmark_returns: pd.Series | None = None,
    equal_weight_pool_returns: pd.Series | None = None,
    strategy_name: str = PROFILE,
    pool_name: str = "expanded",
    benchmark_symbol: str = BENCHMARK_SYMBOL,
    broad_benchmark_symbol: str = BROAD_BENCHMARK_SYMBOL,
    safe_haven: str = SAFE_HAVEN,
) -> dict[str, float | str]:
    returns = portfolio_returns.dropna()
    if returns.empty:
        raise RuntimeError("No portfolio returns to summarize")

    equity_curve = (1.0 + returns).cumprod()
    total_return = float(equity_curve.iloc[-1] - 1.0)
    years = max((returns.index[-1] - returns.index[0]).days / 365.25, 1 / 365.25)
    cagr = float(equity_curve.iloc[-1] ** (1.0 / years) - 1.0)
    drawdown = equity_curve / equity_curve.cummax() - 1.0
    max_drawdown = float(drawdown.min())
    volatility = float(returns.std(ddof=0) * np.sqrt(252))
    std = float(returns.std(ddof=0))
    sharpe = float(returns.mean() / std * np.sqrt(252)) if std else float("nan")
    calmar = float(cagr / abs(max_drawdown)) if max_drawdown < 0 else float("nan")

    rebalances_per_year = float("nan")
    turnover_per_year = float("nan")
    avg_stock_exposure = float("nan")
    if weights_history is not None and not weights_history.empty:
        changes = weights_history.fillna(0.0).diff().fillna(0.0)
        if not changes.empty:
            changes.iloc[0] = 0.0
        daily_turnover = 0.5 * changes.abs().sum(axis=1)
        rebalances_per_year = float((daily_turnover > 1e-12).sum() / years)
        turnover_per_year = float(daily_turnover.sum() / years)
        stock_columns = [column for column in weights_history.columns if column != _normalize_symbol(safe_haven)]
        avg_stock_exposure = (
            float(weights_history[stock_columns].fillna(0.0).sum(axis=1).mean())
            if stock_columns
            else 0.0
        )

    benchmark_total_return, benchmark_corr = _reference_total_return_and_corr(returns, benchmark_returns)
    broad_benchmark_total_return, _broad_corr = _reference_total_return_and_corr(returns, broad_benchmark_returns)
    equal_weight_pool_total_return, _equal_corr = _reference_total_return_and_corr(returns, equal_weight_pool_returns)

    return {
        "Strategy": strategy_name,
        "Pool": pool_name,
        "Start": str(returns.index[0].date()),
        "End": str(returns.index[-1].date()),
        "Total Return": total_return,
        "CAGR": cagr,
        "Max Drawdown": max_drawdown,
        "Volatility": volatility,
        "Sharpe": sharpe,
        "Calmar": calmar,
        "Rebalances/Year": rebalances_per_year,
        "Turnover/Year": turnover_per_year,
        "Avg Stock Exposure": avg_stock_exposure,
        "Final Equity": float(equity_curve.iloc[-1]),
        "Benchmark Symbol": _normalize_symbol(benchmark_symbol),
        "Benchmark Total Return": benchmark_total_return,
        "Benchmark Corr": benchmark_corr,
        "Broad Benchmark Symbol": _normalize_symbol(broad_benchmark_symbol),
        "Broad Benchmark Total Return": broad_benchmark_total_return,
        "Equal Weight Pool Total Return": equal_weight_pool_total_return,
    }


def _reference_total_return_and_corr(
    portfolio_returns: pd.Series,
    reference_returns: pd.Series | None,
) -> tuple[float, float]:
    if reference_returns is None or reference_returns.dropna().empty:
        return float("nan"), float("nan")
    aligned = pd.concat([portfolio_returns.rename("portfolio"), reference_returns.rename("reference")], axis=1).dropna()
    if aligned.empty:
        return float("nan"), float("nan")
    total_return = float((1.0 + aligned["reference"]).cumprod().iloc[-1] - 1.0)
    corr = float(aligned["portfolio"].corr(aligned["reference"]))
    return total_return, corr


def _build_close_and_returns(price_history: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    close_matrix = (
        price_history.pivot_table(index="as_of", columns="symbol", values="close", aggfunc="last")
        .sort_index()
        .ffill()
    )
    returns_matrix = close_matrix.pct_change(fill_method=None).fillna(0.0)
    return close_matrix, returns_matrix


def run_backtest(
    price_history,
    universe_snapshot,
    *,
    start_date=None,
    end_date=None,
    pool_name: str = "custom",
    benchmark_symbol: str = BENCHMARK_SYMBOL,
    broad_benchmark_symbol: str = BROAD_BENCHMARK_SYMBOL,
    safe_haven: str = SAFE_HAVEN,
    top_n: int = 3,
    hold_buffer: int = 2,
    single_name_cap: float = 0.35,
    hold_bonus: float = 0.10,
    risk_on_exposure: float = 1.0,
    soft_defense_exposure: float = 0.50,
    hard_defense_exposure: float = 0.20,
    soft_breadth_threshold: float = 0.50,
    hard_breadth_threshold: float = 0.30,
    min_price_usd: float = 10.0,
    min_adv20_usd: float = 20_000_000.0,
    min_history_days: int = 273,
    turnover_cost_bps: float = 5.0,
):
    prices = _normalize_price_history(price_history)
    universe = _normalize_universe(universe_snapshot)
    benchmark_symbol = _normalize_symbol(benchmark_symbol)
    broad_benchmark_symbol = _normalize_symbol(broad_benchmark_symbol)
    safe_haven = _normalize_symbol(safe_haven)

    if end_date is not None:
        prices = prices.loc[prices["as_of"] <= pd.Timestamp(end_date).normalize()].copy()
    if prices.empty:
        raise RuntimeError("No usable price history remains inside the selected date range")

    feature_history_by_symbol = _precompute_symbol_feature_history(prices)
    close_matrix, returns_matrix = _build_close_and_returns(prices)
    if safe_haven not in close_matrix.columns:
        close_matrix[safe_haven] = 1.0
        returns_matrix[safe_haven] = 0.0
    index = close_matrix.index
    if start_date is not None:
        index = index[index >= pd.Timestamp(start_date).normalize()]
    if len(index) < 2:
        raise RuntimeError("Not enough price history remains inside the selected date range")

    rebalance_dates = build_monthly_rebalance_dates(index)
    symbols = sorted(set(close_matrix.columns) | {safe_haven})
    weights_history = pd.DataFrame(0.0, index=index, columns=symbols)
    portfolio_returns = pd.Series(0.0, index=index, name="portfolio_return")
    turnover_history = pd.Series(0.0, index=index, name="turnover")
    exposure_rows: list[dict[str, object]] = []
    score_frames: list[pd.DataFrame] = []
    trade_rows: list[dict[str, object]] = []

    current_weights: dict[str, float] = {safe_haven: 1.0}
    current_holdings: set[str] = set()

    for idx in range(len(index) - 1):
        date = index[idx]
        next_date = index[idx + 1]

        if date in rebalance_dates:
            active_universe = resolve_active_universe(universe, date)
            snapshot = build_feature_snapshot_for_backtest(
                date,
                active_universe,
                feature_history_by_symbol,
                benchmark_symbol=benchmark_symbol,
                broad_benchmark_symbol=broad_benchmark_symbol,
                safe_haven=safe_haven,
                min_price_usd=float(min_price_usd),
                min_adv20_usd=float(min_adv20_usd),
                min_history_days=int(min_history_days),
            )
            target_weights, ranked, metadata = build_target_weights(
                snapshot,
                current_holdings,
                benchmark_symbol=benchmark_symbol,
                broad_benchmark_symbol=broad_benchmark_symbol,
                safe_haven=safe_haven,
                top_n=int(top_n),
                hold_buffer=int(hold_buffer),
                single_name_cap=float(single_name_cap),
                hold_bonus=float(hold_bonus),
                risk_on_exposure=float(risk_on_exposure),
                soft_defense_exposure=float(soft_defense_exposure),
                hard_defense_exposure=float(hard_defense_exposure),
                soft_breadth_threshold=float(soft_breadth_threshold),
                hard_breadth_threshold=float(hard_breadth_threshold),
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
                ranked.insert(0, "as_of", date)
                ranked["selected"] = ranked["symbol"].isin(metadata.get("selected_symbols", ()))
                score_frames.append(ranked)
            exposure_rows.append(
                {
                    "signal_date": date,
                    "effective_date": next_date,
                    "regime": metadata.get("regime"),
                    "stock_exposure": metadata.get("stock_exposure"),
                    "safe_haven_weight": float(target_weights.get(safe_haven, 0.0)),
                    "breadth_ratio": metadata.get("breadth_ratio"),
                    "benchmark_trend_positive": metadata.get("benchmark_trend_positive"),
                    "selected_symbols": ",".join(metadata.get("selected_symbols", ())),
                    "turnover": turnover,
                }
            )
            current_weights = target_weights
            current_holdings = {
                symbol for symbol, weight in current_weights.items() if weight > 0 and symbol != safe_haven
            }

        for symbol, weight in current_weights.items():
            weights_history.at[date, symbol] = weight

        next_returns = returns_matrix.loc[next_date]
        gross_return = sum(weight * float(next_returns.get(symbol, 0.0)) for symbol, weight in current_weights.items())
        cost = turnover_history.at[next_date] * (float(turnover_cost_bps) / 10_000.0)
        portfolio_returns.at[next_date] = gross_return - cost

    for symbol, weight in current_weights.items():
        weights_history.at[index[-1], symbol] = weight

    pool_symbols = tuple(dict.fromkeys(universe["symbol"].astype(str)))
    available_pool_symbols = [symbol for symbol in pool_symbols if symbol in returns_matrix.columns]
    equal_weight_pool_returns = (
        returns_matrix.loc[index, available_pool_symbols].mean(axis=1)
        if available_pool_symbols
        else pd.Series(index=index, dtype=float)
    )
    reference_returns = pd.DataFrame(
        {
            benchmark_symbol: (
                returns_matrix[benchmark_symbol].reindex(index)
                if benchmark_symbol in returns_matrix
                else np.nan
            ),
            broad_benchmark_symbol: returns_matrix[broad_benchmark_symbol].reindex(index)
            if broad_benchmark_symbol in returns_matrix
            else np.nan,
            f"equal_weight_{pool_name}": equal_weight_pool_returns.reindex(index),
        },
        index=index,
    )
    used_weights = weights_history.loc[:, (weights_history != 0.0).any(axis=0)]
    summary = summarize_returns(
        portfolio_returns,
        weights_history=used_weights,
        benchmark_returns=reference_returns[benchmark_symbol] if benchmark_symbol in reference_returns else None,
        broad_benchmark_returns=(
            reference_returns[broad_benchmark_symbol] if broad_benchmark_symbol in reference_returns else None
        ),
        equal_weight_pool_returns=reference_returns[f"equal_weight_{pool_name}"],
        pool_name=pool_name,
        benchmark_symbol=benchmark_symbol,
        broad_benchmark_symbol=broad_benchmark_symbol,
        safe_haven=safe_haven,
    )
    return {
        "portfolio_returns": portfolio_returns,
        "weights_history": used_weights,
        "turnover_history": turnover_history,
        "candidate_scores": pd.concat(score_frames, ignore_index=True) if score_frames else pd.DataFrame(),
        "trades": pd.DataFrame(trade_rows),
        "exposure_history": pd.DataFrame(exposure_rows),
        "reference_returns": reference_returns,
        "summary": summary,
    }


def _format_summary(summary: Mapping[str, float | str]) -> pd.DataFrame:
    return pd.DataFrame([{column: summary.get(column) for column in BACKTEST_SUMMARY_COLUMNS}])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research backtest for mega_cap_leader_rotation.")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--prices", help="Input price history file (.csv/.json/.jsonl/.parquet)")
    input_group.add_argument("--download", action="store_true", help="Download research prices with yfinance")
    parser.add_argument("--universe", help="Input universe file; optional when --download is used")
    parser.add_argument("--pool", choices=sorted(POOL_SYMBOLS), default="expanded")
    parser.add_argument("--symbols", help="Comma-separated custom pool; overrides --pool")
    parser.add_argument("--price-start", default="2015-01-01", help="Download start date used with --download")
    parser.add_argument("--price-end", help="Download end date used with --download")
    parser.add_argument("--start", dest="start_date", default="2016-01-01", help="Backtest start date")
    parser.add_argument("--end", dest="end_date", help="Backtest end date")
    parser.add_argument("--output-dir", help="Optional output directory for research artifacts")
    parser.add_argument("--benchmark-symbol", default=BENCHMARK_SYMBOL)
    parser.add_argument("--broad-benchmark-symbol", default=BROAD_BENCHMARK_SYMBOL)
    parser.add_argument("--safe-haven", default=SAFE_HAVEN)
    parser.add_argument("--top-n", type=int, default=3)
    parser.add_argument("--hold-buffer", type=int, default=2)
    parser.add_argument("--single-name-cap", type=float, default=0.35)
    parser.add_argument("--hold-bonus", type=float, default=0.10)
    parser.add_argument("--risk-on-exposure", type=float, default=1.0)
    parser.add_argument("--soft-defense-exposure", type=float, default=0.50)
    parser.add_argument("--hard-defense-exposure", type=float, default=0.20)
    parser.add_argument("--soft-breadth-threshold", type=float, default=0.50)
    parser.add_argument("--hard-breadth-threshold", type=float, default=0.30)
    parser.add_argument("--min-price-usd", type=float, default=10.0)
    parser.add_argument("--min-adv20-usd", type=float, default=20_000_000.0)
    parser.add_argument("--min-history-days", type=int, default=273)
    parser.add_argument("--turnover-cost-bps", type=float, default=5.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir) if args.output_dir else None
    universe = read_table(args.universe) if args.universe else build_static_universe(args.pool, symbols=args.symbols)
    if args.download:
        if output_dir is None:
            raise EnvironmentError("--output-dir is required when --download is used")
        prepared = prepare_research_input_data(
            output_dir=output_dir / "input",
            pool=args.pool,
            symbols=args.symbols,
            price_start=args.price_start,
            price_end=args.price_end,
            benchmark_symbol=args.benchmark_symbol,
            broad_benchmark_symbol=args.broad_benchmark_symbol,
            safe_haven=args.safe_haven,
        )
        prices = read_table(prepared.prices_path)
        universe = read_table(args.universe) if args.universe else read_table(prepared.universe_path)
        print(f"downloaded {prepared.price_rows} price rows -> {prepared.prices_path}")
        print(f"wrote universe -> {prepared.universe_path}")
    else:
        prices = read_table(args.prices)

    result = run_backtest(
        prices,
        universe,
        start_date=args.start_date,
        end_date=args.end_date,
        pool_name=args.pool if not args.symbols else "custom",
        benchmark_symbol=args.benchmark_symbol,
        broad_benchmark_symbol=args.broad_benchmark_symbol,
        safe_haven=args.safe_haven,
        top_n=args.top_n,
        hold_buffer=args.hold_buffer,
        single_name_cap=args.single_name_cap,
        hold_bonus=args.hold_bonus,
        risk_on_exposure=args.risk_on_exposure,
        soft_defense_exposure=args.soft_defense_exposure,
        hard_defense_exposure=args.hard_defense_exposure,
        soft_breadth_threshold=args.soft_breadth_threshold,
        hard_breadth_threshold=args.hard_breadth_threshold,
        min_price_usd=args.min_price_usd,
        min_adv20_usd=args.min_adv20_usd,
        min_history_days=args.min_history_days,
        turnover_cost_bps=args.turnover_cost_bps,
    )

    summary_frame = _format_summary(result["summary"])
    print(summary_frame.to_string(index=False))

    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        summary_frame.to_csv(output_dir / "summary.csv", index=False)
        result["portfolio_returns"].rename("portfolio_return").to_csv(output_dir / "portfolio_returns.csv")
        result["weights_history"].to_csv(output_dir / "weights_history.csv")
        result["turnover_history"].rename("turnover").to_csv(output_dir / "turnover_history.csv")
        result["candidate_scores"].to_csv(output_dir / "candidate_scores.csv", index=False)
        result["trades"].to_csv(output_dir / "trades.csv", index=False)
        result["exposure_history"].to_csv(output_dir / "exposure_history.csv", index=False)
        result["reference_returns"].to_csv(output_dir / "reference_returns.csv")
        print(f"wrote research backtest outputs -> {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
