from __future__ import annotations


import numpy as np
import pandas as pd

from us_equity_strategies.strategies.russell_1000_multi_factor_defensive import (
    BENCHMARK_SYMBOL,
    DEFAULT_HARD_BREADTH_THRESHOLD,
    DEFAULT_HARD_DEFENSE_EXPOSURE,
    DEFAULT_HOLD_BONUS,
    DEFAULT_HOLDINGS_COUNT,
    DEFAULT_SECTOR_CAP,
    DEFAULT_SINGLE_NAME_CAP,
    DEFAULT_SOFT_BREADTH_THRESHOLD,
    DEFAULT_SOFT_DEFENSE_EXPOSURE,
    SAFE_HAVEN,
    build_target_weights,
)

BACKTEST_SUMMARY_COLUMNS = (
    "Start",
    "End",
    "Total Return",
    "CAGR",
    "Max Drawdown",
    "Volatility",
    "Sharpe",
    "Rebalances/Year",
    "Turnover/Year",
    "Avg Stock Exposure",
    "Final Equity",
    "Benchmark Total Return",
    "Benchmark Corr",
)


def _normalize_price_history(price_history) -> pd.DataFrame:
    frame = pd.DataFrame(price_history).copy()
    required = {"symbol", "as_of", "close", "volume"}
    missing = required - set(frame.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"price_history missing required columns: {missing_text}")

    frame["symbol"] = frame["symbol"].astype(str).str.upper().str.strip()
    frame["as_of"] = pd.to_datetime(frame["as_of"]).dt.tz_localize(None).dt.normalize()
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce")
    frame = frame.dropna(subset=["symbol", "as_of", "close"])
    return frame.sort_values(["as_of", "symbol"]).reset_index(drop=True)


def _normalize_universe_snapshot(universe_snapshot) -> pd.DataFrame:
    frame = pd.DataFrame(universe_snapshot).copy()
    required = {"symbol", "sector"}
    missing = required - set(frame.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"universe_snapshot missing required columns: {missing_text}")

    frame["symbol"] = frame["symbol"].astype(str).str.upper().str.strip()
    frame["sector"] = frame["sector"].fillna("unknown").astype(str).str.strip().replace("", "unknown")
    for column in ("start_date", "end_date"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column]).dt.tz_localize(None).dt.normalize()
    return frame.drop_duplicates(subset=[column for column in frame.columns if column != "sector"]).reset_index(drop=True)


def resolve_active_universe(universe_snapshot: pd.DataFrame, as_of_date) -> pd.DataFrame:
    as_of = pd.Timestamp(as_of_date).tz_localize(None).normalize()
    frame = universe_snapshot.copy()

    if "start_date" in frame.columns:
        frame = frame.loc[frame["start_date"].isna() | (frame["start_date"] <= as_of)]
    if "end_date" in frame.columns:
        frame = frame.loc[frame["end_date"].isna() | (frame["end_date"] >= as_of)]

    return frame.loc[:, ["symbol", "sector"]].drop_duplicates(subset=["symbol"], keep="last").reset_index(drop=True)


def build_monthly_rebalance_dates(index: pd.DatetimeIndex) -> set[pd.Timestamp]:
    series = pd.Series(index, index=index)
    grouped = series.groupby(index.to_period("M")).max()
    return set(pd.to_datetime(grouped.values))


def _compute_turnover(previous_weights: dict[str, float], new_weights: dict[str, float]) -> float:
    symbols = set(previous_weights) | set(new_weights)
    return 0.5 * sum(abs(new_weights.get(symbol, 0.0) - previous_weights.get(symbol, 0.0)) for symbol in symbols)

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
        returns = closes.pct_change()
        dollar_volume = closes * volumes

        feature_history[str(symbol)] = pd.DataFrame(
            {
                "as_of": history["as_of"],
                "close": closes,
                "volume": volumes,
                "adv20_usd": dollar_volume.rolling(20).mean(),
                "history_days": np.arange(1, len(history) + 1, dtype=int),
                "mom_6_1": closes.shift(21) / closes.shift(147) - 1.0,
                "mom_12_1": closes.shift(21) / closes.shift(273) - 1.0,
                "sma200_gap": closes / closes.rolling(200).mean() - 1.0,
                "vol_63": returns.rolling(63).std(ddof=0) * np.sqrt(252),
            }
        )
    return feature_history


def _build_feature_snapshot_for_backtest(
    as_of_date: pd.Timestamp,
    active_universe: pd.DataFrame,
    feature_history_by_symbol: dict[str, pd.DataFrame],
    *,
    benchmark_symbol: str,
    benchmark_sector: str = "benchmark",
    min_price_usd: float = 10.0,
    min_adv20_usd: float = 20_000_000.0,
    min_history_days: int = 252,
    drawdown_window: int = 126,
) -> pd.DataFrame:
    as_of = pd.Timestamp(as_of_date).tz_localize(None).normalize()
    universe = active_universe.copy()
    universe["symbol"] = universe["symbol"].astype(str).str.upper().str.strip()
    universe["sector"] = universe["sector"].fillna("unknown").astype(str).str.strip().replace("", "unknown")
    universe = universe.drop_duplicates(subset=["symbol"], keep="last")

    symbols = universe["symbol"].tolist()
    benchmark_symbol = str(benchmark_symbol or "").strip().upper()
    if benchmark_symbol and benchmark_symbol not in symbols:
        symbols.append(benchmark_symbol)
    sector_map = dict(zip(universe["symbol"], universe["sector"]))

    rows: list[dict[str, object]] = []
    for symbol in symbols:
        history = feature_history_by_symbol.get(symbol)
        if history is None or history.empty:
            rows.append(
                {
                    "as_of": as_of,
                    "symbol": symbol,
                    "sector": sector_map.get(symbol, benchmark_sector if symbol == benchmark_symbol else "unknown"),
                    "close": float("nan"),
                    "volume": float("nan"),
                    "adv20_usd": float("nan"),
                    "history_days": 0,
                    "mom_6_1": float("nan"),
                    "mom_12_1": float("nan"),
                    "sma200_gap": float("nan"),
                    "vol_63": float("nan"),
                    "maxdd_126": float("nan"),
                    "eligible": False,
                }
            )
            continue

        cutoff = int(history["as_of"].searchsorted(as_of, side="right"))
        if cutoff <= 0:
            rows.append(
                {
                    "as_of": as_of,
                    "symbol": symbol,
                    "sector": sector_map.get(symbol, benchmark_sector if symbol == benchmark_symbol else "unknown"),
                    "close": float("nan"),
                    "volume": float("nan"),
                    "adv20_usd": float("nan"),
                    "history_days": 0,
                    "mom_6_1": float("nan"),
                    "mom_12_1": float("nan"),
                    "sma200_gap": float("nan"),
                    "vol_63": float("nan"),
                    "maxdd_126": float("nan"),
                    "eligible": False,
                }
            )
            continue

        current = history.iloc[cutoff - 1]
        closes_window = history["close"].iloc[max(0, cutoff - drawdown_window) : cutoff]
        maxdd_126 = _compute_window_drawdown(closes_window) if len(closes_window) >= drawdown_window else float("nan")
        feature_values = (
            current["mom_6_1"],
            current["mom_12_1"],
            current["sma200_gap"],
            current["vol_63"],
            maxdd_126,
        )
        eligible = (
            symbol != benchmark_symbol
            and int(current["history_days"]) >= min_history_days
            and float(current["close"]) > min_price_usd
            and pd.notna(current["adv20_usd"])
            and float(current["adv20_usd"]) >= min_adv20_usd
            and all(pd.notna(value) for value in feature_values)
        )
        rows.append(
            {
                "as_of": as_of,
                "symbol": symbol,
                "sector": sector_map.get(symbol, benchmark_sector if symbol == benchmark_symbol else "unknown"),
                "close": float(current["close"]) if pd.notna(current["close"]) else float("nan"),
                "volume": float(current["volume"]) if pd.notna(current["volume"]) else float("nan"),
                "adv20_usd": float(current["adv20_usd"]) if pd.notna(current["adv20_usd"]) else float("nan"),
                "history_days": int(current["history_days"]),
                "mom_6_1": float(current["mom_6_1"]) if pd.notna(current["mom_6_1"]) else float("nan"),
                "mom_12_1": float(current["mom_12_1"]) if pd.notna(current["mom_12_1"]) else float("nan"),
                "sma200_gap": float(current["sma200_gap"]) if pd.notna(current["sma200_gap"]) else float("nan"),
                "vol_63": float(current["vol_63"]) if pd.notna(current["vol_63"]) else float("nan"),
                "maxdd_126": maxdd_126,
                "eligible": bool(eligible),
            }
        )

    return pd.DataFrame(rows).sort_values("symbol").reset_index(drop=True)


def summarize_backtest(
    portfolio_returns: pd.Series,
    weights_history: pd.DataFrame,
    benchmark_returns: pd.Series | None = None,
) -> dict[str, float | str]:
    returns = portfolio_returns.dropna()
    if returns.empty:
        raise RuntimeError("No portfolio returns to summarize")

    equity_curve = (1.0 + returns).cumprod()
    total_return = float(equity_curve.iloc[-1] - 1.0)
    years = max((returns.index[-1] - returns.index[0]).days / 365.25, 1 / 365.25)
    cagr = float(equity_curve.iloc[-1] ** (1.0 / years) - 1.0)
    drawdown = equity_curve / equity_curve.cummax() - 1.0
    volatility = float(returns.std(ddof=0) * np.sqrt(252))
    std = float(returns.std(ddof=0))
    sharpe = float(returns.mean() / std * np.sqrt(252)) if std else float("nan")

    changes = weights_history.fillna(0.0).diff().fillna(0.0)
    if not changes.empty:
        changes.iloc[0] = 0.0
    daily_turnover = 0.5 * changes.abs().sum(axis=1)
    rebalances_per_year = float((daily_turnover > 1e-12).sum() / years)
    turnover_per_year = float(daily_turnover.sum() / years)

    stock_columns = [column for column in weights_history.columns if column != SAFE_HAVEN]
    avg_stock_exposure = float(weights_history[stock_columns].fillna(0.0).sum(axis=1).mean()) if stock_columns else 0.0

    benchmark_total_return = float("nan")
    benchmark_corr = float("nan")
    if benchmark_returns is not None and not benchmark_returns.dropna().empty:
        aligned = pd.concat([returns, benchmark_returns.rename("benchmark")], axis=1).dropna()
        if not aligned.empty:
            benchmark_total_return = float((1.0 + aligned["benchmark"]).cumprod().iloc[-1] - 1.0)
            benchmark_corr = float(aligned.iloc[:, 0].corr(aligned["benchmark"]))

    return {
        "Start": str(returns.index[0].date()),
        "End": str(returns.index[-1].date()),
        "Total Return": total_return,
        "CAGR": cagr,
        "Max Drawdown": float(drawdown.min()),
        "Volatility": volatility,
        "Sharpe": sharpe,
        "Rebalances/Year": rebalances_per_year,
        "Turnover/Year": turnover_per_year,
        "Avg Stock Exposure": avg_stock_exposure,
        "Final Equity": float(equity_curve.iloc[-1]),
        "Benchmark Total Return": benchmark_total_return,
        "Benchmark Corr": benchmark_corr,
    }


def run_backtest(
    price_history,
    universe_snapshot,
    *,
    start_date=None,
    end_date=None,
    benchmark_symbol: str = BENCHMARK_SYMBOL,
    safe_haven: str = SAFE_HAVEN,
    holdings_count: int = DEFAULT_HOLDINGS_COUNT,
    single_name_cap: float = DEFAULT_SINGLE_NAME_CAP,
    sector_cap: float = DEFAULT_SECTOR_CAP,
    hold_bonus: float = DEFAULT_HOLD_BONUS,
    soft_defense_exposure: float = DEFAULT_SOFT_DEFENSE_EXPOSURE,
    hard_defense_exposure: float = DEFAULT_HARD_DEFENSE_EXPOSURE,
    soft_breadth_threshold: float = DEFAULT_SOFT_BREADTH_THRESHOLD,
    hard_breadth_threshold: float = DEFAULT_HARD_BREADTH_THRESHOLD,
    turnover_cost_bps: float = 0.0,
):
    prices = _normalize_price_history(price_history)
    universe = _normalize_universe_snapshot(universe_snapshot)

    if start_date is not None:
        prices = prices.loc[prices["as_of"] >= pd.Timestamp(start_date).normalize()].copy()
    if end_date is not None:
        prices = prices.loc[prices["as_of"] <= pd.Timestamp(end_date).normalize()].copy()
    if prices.empty:
        raise RuntimeError("No usable price history remains inside the selected date range")
    feature_history_by_symbol = _precompute_symbol_feature_history(prices)

    close_matrix = (
        prices.pivot_table(index="as_of", columns="symbol", values="close", aggfunc="last")
        .sort_index()
        .ffill()
    )
    returns_matrix = close_matrix.pct_change().fillna(0.0)
    if safe_haven not in returns_matrix.columns:
        returns_matrix[safe_haven] = 0.0
        close_matrix[safe_haven] = 1.0

    index = close_matrix.index
    rebalance_dates = build_monthly_rebalance_dates(index)
    symbols = sorted(set(close_matrix.columns) | {safe_haven})
    weights_history = pd.DataFrame(0.0, index=index, columns=symbols)
    portfolio_returns = pd.Series(0.0, index=index, name="portfolio")
    turnover_history = pd.Series(0.0, index=index, name="turnover")

    current_weights: dict[str, float] = {safe_haven: 1.0}
    current_holdings: set[str] = set()

    for idx in range(len(index) - 1):
        date = index[idx]
        next_date = index[idx + 1]

        if date in rebalance_dates:
            active_universe = resolve_active_universe(universe, date)
            snapshot = _build_feature_snapshot_for_backtest(
                date,
                active_universe,
                feature_history_by_symbol,
                benchmark_symbol=benchmark_symbol,
            )
            target_weights, _signal, _metadata = build_target_weights(
                snapshot,
                current_holdings,
                benchmark_symbol=benchmark_symbol,
                safe_haven=safe_haven,
                holdings_count=holdings_count,
                single_name_cap=single_name_cap,
                sector_cap=sector_cap,
                hold_bonus=hold_bonus,
                soft_defense_exposure=soft_defense_exposure,
                hard_defense_exposure=hard_defense_exposure,
                soft_breadth_threshold=soft_breadth_threshold,
                hard_breadth_threshold=hard_breadth_threshold,
            )
            turnover = _compute_turnover(current_weights, target_weights)
            turnover_history.at[next_date] = turnover
            current_weights = target_weights
            current_holdings = {
                symbol for symbol, weight in current_weights.items() if weight > 0 and symbol != safe_haven
            }

        for symbol, weight in current_weights.items():
            weights_history.at[date, symbol] = weight

        next_returns = returns_matrix.loc[next_date]
        gross_return = sum(weight * float(next_returns.get(symbol, 0.0)) for symbol, weight in current_weights.items())
        cost = turnover_history.at[next_date] * (turnover_cost_bps / 10_000.0)
        portfolio_returns.at[next_date] = gross_return - cost

    for symbol, weight in current_weights.items():
        weights_history.at[index[-1], symbol] = weight

    benchmark_returns = returns_matrix.get(benchmark_symbol)
    summary = summarize_backtest(
        portfolio_returns,
        weights_history.loc[:, (weights_history != 0.0).any(axis=0)],
        benchmark_returns=benchmark_returns,
    )
    return {
        "portfolio_returns": portfolio_returns,
        "weights_history": weights_history.loc[:, (weights_history != 0.0).any(axis=0)],
        "turnover_history": turnover_history,
        "summary": summary,
    }
