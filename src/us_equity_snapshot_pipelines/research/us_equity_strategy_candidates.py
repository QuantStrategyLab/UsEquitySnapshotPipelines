from __future__ import annotations

import argparse
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ..pipelines.mega_cap_leader_rotation_backtest import (
    _normalize_price_history as _normalize_r1000_price_history,
    _normalize_universe as _normalize_universe_snapshot,
    _precompute_symbol_feature_history,
    build_feature_snapshot_for_backtest as _build_feature_snapshot_for_backtest,
    build_monthly_rebalance_dates,
    resolve_active_universe,
)
from ..pipelines.russell_1000_multi_factor_defensive_snapshot import read_table
from ..yfinance_prices import download_price_history

DEFAULT_PERIODS = (
    ("short", "2025-06-01", None),
    ("medium", "2023-06-01", None),
    ("long", "2018-01-01", None),
)
DEFAULT_TURNOVER_COST_BPS = 5.0
DEFAULT_MAX_LIVE_CANDIDATES = 5
MAX_ALLOWED_DRAWDOWN = -0.30


@dataclass(frozen=True)
class EtfCandidateSpec:
    candidate_id: str
    display_name: str
    rule: str
    universe_symbols: tuple[str, ...]
    benchmark_symbol: str = "SPY"
    safe_symbol: str = "BIL"
    top_n: int = 1
    notes: str = ""


@dataclass(frozen=True)
class SnapshotCandidateSpec:
    candidate_id: str
    display_name: str
    rule: str
    candidate_group: str
    benchmark_symbol: str = "SPY"
    safe_symbol: str = "BOXX"
    holdings_count: int = 24
    single_name_cap: float = 0.06
    sector_cap: float = 0.20
    hold_bonus: float = 0.10
    soft_defense_exposure: float = 0.50
    hard_defense_exposure: float = 0.10
    soft_breadth_threshold: float = 0.55
    hard_breadth_threshold: float = 0.35
    top_sectors: int = 6
    notes: str = ""


@dataclass(frozen=True)
class SnapshotBacktestContext:
    prices: pd.DataFrame
    universe: pd.DataFrame
    feature_history_by_symbol: dict[str, pd.DataFrame]
    close_matrix: pd.DataFrame
    returns_matrix: pd.DataFrame


ETF_CANDIDATES: tuple[EtfCandidateSpec, ...] = ()

SNAPSHOT_BASELINE_CANDIDATES: tuple[SnapshotCandidateSpec, ...] = ()

SNAPSHOT_OPTIMIZATION_CANDIDATES: tuple[SnapshotCandidateSpec, ...] = ()

SNAPSHOT_NEW_CANDIDATES: tuple[SnapshotCandidateSpec, ...] = (
    SnapshotCandidateSpec(
        candidate_id="new_r1000_residual_strength_20",
        display_name="R1000 Residual Strength 20",
        rule="sector_balanced_relative_strength",
        candidate_group="new_snapshot_candidate",
        benchmark_symbol="SPY",
        safe_symbol="BOXX",
        holdings_count=24,
        single_name_cap=0.06,
        sector_cap=0.20,
        hold_bonus=0.10,
        soft_defense_exposure=0.50,
        hard_defense_exposure=0.10,
        soft_breadth_threshold=0.55,
        hard_breadth_threshold=0.35,
        top_sectors=6,
        notes="Sector-balanced relative-strength snapshot candidate with 20% sector cap.",
    ),
)

SNAPSHOT_CANDIDATES: tuple[SnapshotCandidateSpec, ...] = (
    *SNAPSHOT_BASELINE_CANDIDATES,
    *SNAPSHOT_OPTIMIZATION_CANDIDATES,
    *SNAPSHOT_NEW_CANDIDATES,
)


def _normalize_price_history(price_history: pd.DataFrame) -> pd.DataFrame:
    frame = pd.DataFrame(price_history).copy()
    required = {"symbol", "as_of", "close"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"price_history missing required columns: {sorted(missing)}")
    frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
    frame["as_of"] = pd.to_datetime(frame["as_of"]).dt.tz_localize(None).dt.normalize()
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = frame.dropna(subset=["symbol", "as_of", "close"])
    if frame.empty:
        raise ValueError("price_history has no usable rows")
    close = frame.pivot_table(index="as_of", columns="symbol", values="close", aggfunc="last").sort_index()
    close.columns = close.columns.map(str).str.upper()
    return close.ffill()


def _parse_periods(raw_periods: str | Sequence[tuple[str, str, str | None]] | None) -> tuple[tuple[str, str, str | None], ...]:
    if raw_periods is None:
        return DEFAULT_PERIODS
    if not isinstance(raw_periods, str):
        return tuple((str(name), str(start), None if end is None else str(end)) for name, start, end in raw_periods)

    periods: list[tuple[str, str, str | None]] = []
    for item in raw_periods.split(","):
        item = item.strip()
        if not item:
            continue
        parts = item.split(":")
        if len(parts) not in {2, 3}:
            raise ValueError("periods must use name:start[:end] entries")
        name, start = parts[0].strip(), parts[1].strip()
        end = parts[2].strip() if len(parts) == 3 and parts[2].strip() else None
        if not name or not start:
            raise ValueError("period name and start are required")
        periods.append((name, start, end))
    if not periods:
        raise ValueError("at least one period is required")
    return tuple(periods)


def collect_required_etf_symbols(candidates: Sequence[EtfCandidateSpec] = ETF_CANDIDATES) -> tuple[str, ...]:
    symbols: list[str] = ["SPY", "QQQ"]
    for candidate in candidates:
        for symbol in (*candidate.universe_symbols, candidate.benchmark_symbol, candidate.safe_symbol, "SPY", "QQQ"):
            normalized = str(symbol).strip().upper()
            if normalized and normalized not in symbols:
                symbols.append(normalized)
    return tuple(symbols)


def _zscore(values: pd.Series) -> pd.Series:
    clean = pd.to_numeric(values, errors="coerce")
    std = clean.std(ddof=0)
    if not std or pd.isna(std):
        return pd.Series(0.0, index=clean.index)
    return (clean - clean.mean()) / std


def _asof_value(series: pd.Series, as_of: pd.Timestamp, *, lookback: int = 0) -> float:
    index = series.index[series.index <= as_of]
    if len(index) <= lookback:
        return float("nan")
    return float(series.loc[index[-1 - lookback]])


def _return_between(series: pd.Series, as_of: pd.Timestamp, *, lookback: int, skip: int = 0) -> float:
    current = _asof_value(series, as_of, lookback=skip)
    previous = _asof_value(series, as_of, lookback=lookback + skip)
    if not previous or pd.isna(current) or pd.isna(previous):
        return float("nan")
    return current / previous - 1.0


def _sma_gap(series: pd.Series, as_of: pd.Timestamp, *, window: int = 200) -> float:
    history = series.loc[series.index <= as_of].dropna()
    if len(history) < window:
        return float("nan")
    sma = float(history.tail(window).mean())
    close = float(history.iloc[-1])
    if not sma:
        return float("nan")
    return close / sma - 1.0


def _realized_vol(series: pd.Series, as_of: pd.Timestamp, *, window: int = 63) -> float:
    history = series.loc[series.index <= as_of].dropna().pct_change().dropna()
    if len(history) < window:
        return float("nan")
    return float(history.tail(window).std(ddof=0) * math.sqrt(252.0))


def _weighted_momentum(series: pd.Series, as_of: pd.Timestamp) -> float:
    r1 = _return_between(series, as_of, lookback=21)
    r3 = _return_between(series, as_of, lookback=63)
    r6 = _return_between(series, as_of, lookback=126)
    r12 = _return_between(series, as_of, lookback=252)
    values = [r1, r3, r6, r12]
    if any(pd.isna(value) for value in values):
        return float("nan")
    return float((12.0 * r1 + 4.0 * r3 + 2.0 * r6 + r12) / 19.0)


def _monthly_rebalance_dates(index: pd.DatetimeIndex) -> set[pd.Timestamp]:
    dates = pd.Series(index=index, data=index)
    return set(pd.Timestamp(value).normalize() for value in dates.groupby(index.to_period("M")).tail(1).tolist())


def _safe_weights(symbol: str) -> dict[str, float]:
    return {str(symbol).strip().upper(): 1.0}


def _resolve_etf_target_weights(close: pd.DataFrame, as_of: pd.Timestamp, spec: EtfCandidateSpec) -> tuple[dict[str, float], pd.DataFrame]:
    safe_symbol = spec.safe_symbol.upper()
    rows: list[dict[str, object]] = []
    for symbol in spec.universe_symbols:
        symbol = symbol.upper()
        if symbol not in close.columns:
            continue
        series = close[symbol]
        mom_12_1 = _return_between(series, as_of, lookback=252, skip=21)
        mom_6_1 = _return_between(series, as_of, lookback=126, skip=21)
        weighted_momentum = _weighted_momentum(series, as_of)
        sma200_gap = _sma_gap(series, as_of)
        vol_63 = _realized_vol(series, as_of)
        rows.append(
            {
                "symbol": symbol,
                "mom_12_1": mom_12_1,
                "mom_6_1": mom_6_1,
                "weighted_momentum": weighted_momentum,
                "sma200_gap": sma200_gap,
                "vol_63": vol_63,
            }
        )
    ranking = pd.DataFrame(rows)
    if ranking.empty:
        return _safe_weights(safe_symbol), ranking

    if spec.rule == "dual_momentum":
        ranking["score"] = pd.to_numeric(ranking["mom_12_1"], errors="coerce")
        ranking["eligible"] = ranking["score"].gt(0) & pd.to_numeric(ranking["sma200_gap"], errors="coerce").gt(0)
        ranking = ranking.sort_values(["score", "sma200_gap"], ascending=False).reset_index(drop=True)
        if bool(ranking["eligible"].iloc[0]):
            return {str(ranking["symbol"].iloc[0]): 1.0}, ranking
        return _safe_weights(safe_symbol), ranking

    if spec.rule == "momentum_low_vol":
        ranking["score"] = (
            0.55 * _zscore(ranking["mom_12_1"])
            + 0.30 * _zscore(ranking["mom_6_1"])
            + 0.15 * _zscore(ranking["weighted_momentum"])
            - 0.35 * _zscore(ranking["vol_63"])
        )
        ranking["eligible"] = (
            pd.to_numeric(ranking["mom_12_1"], errors="coerce").gt(0)
            & pd.to_numeric(ranking["sma200_gap"], errors="coerce").gt(0)
        )
    elif spec.rule == "relative_momentum":
        ranking["score"] = pd.to_numeric(ranking["weighted_momentum"], errors="coerce")
        ranking["eligible"] = ranking["score"].gt(0) & pd.to_numeric(ranking["sma200_gap"], errors="coerce").gt(0)
    else:
        raise ValueError(f"unknown ETF candidate rule: {spec.rule}")

    ranking = ranking.sort_values(["eligible", "score", "sma200_gap"], ascending=False).reset_index(drop=True)
    selected = ranking.loc[ranking["eligible"]].head(max(1, int(spec.top_n)))
    if selected.empty:
        return _safe_weights(safe_symbol), ranking
    selected_weight = 1.0 / float(len(selected))
    weights = {str(symbol): selected_weight for symbol in selected["symbol"].tolist()}
    if len(selected) < int(spec.top_n):
        weights[safe_symbol] = weights.get(safe_symbol, 0.0) + (int(spec.top_n) - len(selected)) / float(int(spec.top_n))
    return weights, ranking


def _compute_turnover(current: Mapping[str, float], target: Mapping[str, float]) -> float:
    symbols = set(current) | set(target)
    return 0.5 * sum(abs(float(target.get(symbol, 0.0)) - float(current.get(symbol, 0.0))) for symbol in symbols)


def summarize_returns(
    returns: pd.Series,
    *,
    weights_history: pd.DataFrame | None = None,
    benchmark_returns: pd.Series | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, float | str | int]:
    clean = pd.to_numeric(returns, errors="coerce").dropna()
    if clean.empty:
        return {
            "Start": start_date or "",
            "End": end_date or "",
            "Trading Days": 0,
            "Total Return": float("nan"),
            "CAGR": float("nan"),
            "Volatility": float("nan"),
            "Max Drawdown": float("nan"),
            "Sharpe": float("nan"),
            "Calmar": float("nan"),
            "Benchmark Total Return": float("nan"),
            "Benchmark CAGR": float("nan"),
            "Excess CAGR vs Benchmark": float("nan"),
            "Turnover/Year": float("nan"),
        }
    equity = (1.0 + clean).cumprod()
    years = max(float(len(clean)) / 252.0, 1.0 / 252.0)
    total_return = float(equity.iloc[-1] - 1.0)
    cagr = float(equity.iloc[-1] ** (1.0 / years) - 1.0)
    volatility = float(clean.std(ddof=0) * math.sqrt(252.0))
    sharpe = float(clean.mean() / clean.std(ddof=0) * math.sqrt(252.0)) if clean.std(ddof=0) else float("nan")
    drawdown = equity / equity.cummax() - 1.0
    max_drawdown = float(drawdown.min())
    calmar = float(cagr / abs(max_drawdown)) if max_drawdown < 0 else float("nan")

    benchmark_total = float("nan")
    benchmark_cagr = float("nan")
    if benchmark_returns is not None:
        bench = pd.to_numeric(benchmark_returns.reindex(clean.index), errors="coerce").dropna()
        if not bench.empty:
            bench_equity = (1.0 + bench).cumprod()
            bench_years = max(float(len(bench)) / 252.0, 1.0 / 252.0)
            benchmark_total = float(bench_equity.iloc[-1] - 1.0)
            benchmark_cagr = float(bench_equity.iloc[-1] ** (1.0 / bench_years) - 1.0)

    turnover_per_year = float("nan")
    if weights_history is not None and not weights_history.empty:
        symbols = [column for column in weights_history.columns if column != "as_of"]
        if symbols:
            if "as_of" in weights_history.columns:
                weight_frame = weights_history.set_index("as_of")[symbols].fillna(0.0)
            else:
                weight_frame = weights_history.loc[:, symbols].fillna(0.0)
            daily_turnover = 0.5 * weight_frame.diff().abs().sum(axis=1).fillna(0.0)
            turnover_per_year = float(daily_turnover.sum() / years)

    return {
        "Start": clean.index.min().date().isoformat(),
        "End": clean.index.max().date().isoformat(),
        "Trading Days": int(len(clean)),
        "Total Return": total_return,
        "CAGR": cagr,
        "Volatility": volatility,
        "Max Drawdown": max_drawdown,
        "Sharpe": sharpe,
        "Calmar": calmar,
        "Benchmark Total Return": benchmark_total,
        "Benchmark CAGR": benchmark_cagr,
        "Excess CAGR vs Benchmark": cagr - benchmark_cagr if not pd.isna(benchmark_cagr) else float("nan"),
        "Turnover/Year": turnover_per_year,
    }


def run_etf_candidate_backtest(
    price_history: pd.DataFrame,
    spec: EtfCandidateSpec,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
) -> dict[str, object]:
    close = _normalize_price_history(price_history)
    start_ts = pd.Timestamp(start_date).normalize() if start_date else close.index.min()
    end_ts = pd.Timestamp(end_date).normalize() if end_date else close.index.max()
    close = close.loc[close.index <= end_ts].copy()
    active_index = close.index[close.index >= start_ts]
    if len(active_index) < 2:
        raise ValueError(f"not enough price rows for {spec.candidate_id} in requested period")

    daily_returns = close.pct_change(fill_method=None).fillna(0.0)
    rebalance_dates = _monthly_rebalance_dates(active_index)
    current_weights: dict[str, float] = {}
    strategy_returns = pd.Series(0.0, index=active_index[1:], name="strategy_return")
    turnover_history = pd.Series(0.0, index=active_index[1:], name="turnover")
    weight_rows: list[dict[str, object]] = []
    ranking_rows: list[pd.DataFrame] = []

    for idx, as_of in enumerate(active_index[:-1]):
        next_date = active_index[idx + 1]
        if as_of in rebalance_dates or not current_weights:
            target_weights, ranking = _resolve_etf_target_weights(close, as_of, spec)
            turnover = _compute_turnover(current_weights, target_weights)
            current_weights = target_weights
            turnover_history.at[next_date] = turnover
            row = {"as_of": next_date, **current_weights}
            weight_rows.append(row)
            if not ranking.empty:
                ranking = ranking.copy()
                ranking.insert(0, "candidate_id", spec.candidate_id)
                ranking.insert(1, "rebalance_as_of", as_of)
                ranking_rows.append(ranking)
        day_return = 0.0
        for symbol, weight in current_weights.items():
            if symbol in daily_returns.columns:
                value = daily_returns.at[next_date, symbol]
                if pd.notna(value):
                    day_return += float(weight) * float(value)
        cost = float(turnover_history.at[next_date]) * (float(turnover_cost_bps) / 10_000.0)
        strategy_returns.at[next_date] = day_return - cost

    weights_history = pd.DataFrame(weight_rows).fillna(0.0)
    benchmark_returns = daily_returns.get(spec.benchmark_symbol.upper(), pd.Series(index=strategy_returns.index, dtype=float))
    summary = summarize_returns(
        strategy_returns,
        weights_history=weights_history,
        benchmark_returns=benchmark_returns,
        start_date=start_date,
        end_date=end_date,
    )
    summary.update(
        {
            "Candidate": spec.candidate_id,
            "Display Name": spec.display_name,
            "Candidate Type": "ordinary_etf",
            "Candidate Group": "new_ordinary_strategy",
            "Rule": spec.rule,
            "Benchmark Symbol": spec.benchmark_symbol.upper(),
            "Safe Symbol": spec.safe_symbol.upper(),
            "Notes": spec.notes,
        }
    )
    return {
        "summary": summary,
        "portfolio_returns": strategy_returns,
        "benchmark_returns": benchmark_returns,
        "weights_history": weights_history,
        "turnover_history": turnover_history,
        "candidate_scores": pd.concat(ranking_rows, ignore_index=True) if ranking_rows else pd.DataFrame(),
    }


def prepare_snapshot_backtest_context(
    price_history: pd.DataFrame,
    universe: pd.DataFrame,
    *,
    end_date: str | None = None,
) -> SnapshotBacktestContext:
    prices = _normalize_r1000_price_history(price_history)
    if end_date is not None:
        prices = prices.loc[prices["as_of"] <= pd.Timestamp(end_date).normalize()].copy()
    if prices.empty:
        raise RuntimeError("No usable Russell 1000 price history remains inside the selected date range")
    normalized_universe = _normalize_universe_snapshot(universe)
    feature_history = _precompute_symbol_feature_history(prices)
    close_matrix = (
        prices.pivot_table(index="as_of", columns="symbol", values="close", aggfunc="last")
        .sort_index()
        .ffill()
    )
    returns_matrix = close_matrix.pct_change(fill_method=None).fillna(0.0)
    return SnapshotBacktestContext(
        prices=prices,
        universe=normalized_universe,
        feature_history_by_symbol=feature_history,
        close_matrix=close_matrix,
        returns_matrix=returns_matrix,
    )


def _snapshot_regime(
    frame: pd.DataFrame,
    *,
    benchmark_symbol: str,
    soft_breadth_threshold: float,
    hard_breadth_threshold: float,
) -> tuple[str, bool, float, float]:
    benchmark_rows = frame.loc[frame["symbol"].eq(benchmark_symbol)]
    benchmark_trend_positive = True
    if not benchmark_rows.empty:
        benchmark_trend_positive = bool(float(benchmark_rows.iloc[-1]["sma200_gap"]) > 0)
    universe = frame.loc[frame["symbol"].ne(benchmark_symbol)].copy()
    eligible = universe.loc[universe["eligible"].astype(bool)].copy()
    breadth_ratio = float(pd.to_numeric(eligible["sma200_gap"], errors="coerce").gt(0).mean()) if not eligible.empty else 0.0
    if (not benchmark_trend_positive) and breadth_ratio < hard_breadth_threshold:
        regime = "hard_defense"
    elif (not benchmark_trend_positive) or breadth_ratio < soft_breadth_threshold:
        regime = "soft_defense"
    else:
        regime = "risk_on"
    stock_exposure = 1.0
    if regime == "soft_defense":
        stock_exposure = float(frame.attrs.get("soft_defense_exposure", 0.50))
    if regime == "hard_defense":
        stock_exposure = float(frame.attrs.get("hard_defense_exposure", 0.10))
    return regime, benchmark_trend_positive, breadth_ratio, stock_exposure


def _sector_slot_cap(*, holdings_count: int, sector_cap: float, stock_exposure: float) -> int:
    per_name_target = stock_exposure / max(1, int(holdings_count))
    if per_name_target <= 0:
        return max(1, int(holdings_count))
    return max(1, int(math.floor(float(sector_cap) / per_name_target)))


def _select_ranked_with_sector_cap(
    ranked: pd.DataFrame,
    *,
    holdings_count: int,
    sector_slot_cap: int,
) -> pd.DataFrame:
    selected_rows: list[dict[str, object]] = []
    sector_counts: dict[str, int] = {}
    for row in ranked.itertuples(index=False):
        sector = str(getattr(row, "sector", "unknown") or "unknown")
        if sector_counts.get(sector, 0) >= sector_slot_cap:
            continue
        selected_rows.append(row._asdict())
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        if len(selected_rows) >= int(holdings_count):
            break
    return pd.DataFrame(selected_rows)


def _build_new_snapshot_target_weights(
    feature_snapshot: pd.DataFrame,
    current_holdings: set[str],
    spec: SnapshotCandidateSpec,
) -> tuple[dict[str, float], pd.DataFrame, dict[str, object]]:
    frame = pd.DataFrame(feature_snapshot).copy()
    benchmark_symbol = spec.benchmark_symbol.upper()
    safe_symbol = spec.safe_symbol.upper()
    frame["symbol"] = frame["symbol"].astype(str).str.upper().str.strip()
    frame["sector"] = frame["sector"].fillna("unknown").astype(str).str.strip().replace("", "unknown")
    if "mom_6_1" not in frame.columns and "mom_6m" in frame.columns:
        frame["mom_6_1"] = pd.to_numeric(frame["mom_6m"], errors="coerce")
    for column in ("mom_6_1", "mom_12_1", "sma200_gap", "vol_63", "maxdd_126"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if "eligible" in frame.columns:
        frame["eligible"] = frame["eligible"].astype(bool)
    else:
        frame["eligible"] = True
    frame.attrs["soft_defense_exposure"] = float(spec.soft_defense_exposure)
    frame.attrs["hard_defense_exposure"] = float(spec.hard_defense_exposure)
    regime, benchmark_trend_positive, breadth_ratio, stock_exposure = _snapshot_regime(
        frame,
        benchmark_symbol=benchmark_symbol,
        soft_breadth_threshold=spec.soft_breadth_threshold,
        hard_breadth_threshold=spec.hard_breadth_threshold,
    )

    eligible = frame.loc[
        frame["eligible"]
        & frame["symbol"].ne(benchmark_symbol)
        & frame["symbol"].ne(safe_symbol)
        & frame["mom_6_1"].notna()
        & frame["mom_12_1"].notna()
        & frame["sma200_gap"].notna()
        & frame["vol_63"].notna()
        & frame["maxdd_126"].notna()
    ].copy()
    if eligible.empty or stock_exposure <= 0:
        metadata = {
            "regime": regime,
            "benchmark_trend_positive": benchmark_trend_positive,
            "breadth_ratio": breadth_ratio,
            "stock_exposure": 0.0,
            "selected_symbols": (),
            "candidate_count": int(len(eligible)),
        }
        return {safe_symbol: 1.0}, eligible, metadata

    eligible["drawdown_abs"] = eligible["maxdd_126"].abs()
    eligible["z_mom_6_1"] = eligible.groupby("sector")["mom_6_1"].transform(_zscore)
    eligible["z_mom_12_1"] = eligible.groupby("sector")["mom_12_1"].transform(_zscore)
    eligible["z_sma200_gap"] = eligible.groupby("sector")["sma200_gap"].transform(_zscore)
    eligible["z_vol_63"] = eligible.groupby("sector")["vol_63"].transform(_zscore)
    eligible["z_drawdown_abs"] = eligible.groupby("sector")["drawdown_abs"].transform(_zscore)

    if spec.rule == "low_vol_momentum":
        eligible = eligible.loc[
            eligible["mom_12_1"].gt(0)
            & eligible["mom_6_1"].gt(0)
            & eligible["sma200_gap"].gt(0)
        ].copy()
        eligible["score"] = (
            0.35 * eligible["z_mom_12_1"]
            + 0.25 * eligible["z_mom_6_1"]
            + 0.10 * eligible["z_sma200_gap"]
            - 0.20 * eligible["z_vol_63"]
            - 0.10 * eligible["z_drawdown_abs"]
        )
    elif spec.rule == "sector_balanced_relative_strength":
        eligible["raw_strength"] = (
            0.45 * eligible["mom_12_1"]
            + 0.35 * eligible["mom_6_1"]
            + 0.20 * eligible["sma200_gap"]
        )
        sector_strength = eligible.groupby("sector")["raw_strength"].median().sort_values(ascending=False)
        selected_sectors = tuple(sector_strength.loc[sector_strength.gt(0)].head(max(1, int(spec.top_sectors))).index)
        if not selected_sectors:
            selected_sectors = tuple(sector_strength.head(max(1, int(spec.top_sectors))).index)
        eligible = eligible.loc[eligible["sector"].isin(selected_sectors)].copy()
        eligible["sector_strength"] = eligible["sector"].map(sector_strength)
        eligible["score"] = (
            0.30 * _zscore(eligible["sector_strength"])
            + 0.35 * eligible["z_mom_12_1"]
            + 0.20 * eligible["z_mom_6_1"]
            + 0.10 * eligible["z_sma200_gap"]
            - 0.05 * eligible["z_drawdown_abs"]
        )
    else:
        raise ValueError(f"unsupported snapshot candidate rule: {spec.rule}")

    if eligible.empty:
        metadata = {
            "regime": regime,
            "benchmark_trend_positive": benchmark_trend_positive,
            "breadth_ratio": breadth_ratio,
            "stock_exposure": 0.0,
            "selected_symbols": (),
            "candidate_count": 0,
        }
        return {safe_symbol: 1.0}, eligible, metadata

    eligible.loc[eligible["symbol"].isin(current_holdings), "score"] += float(spec.hold_bonus)
    ranked = eligible.sort_values(["score", "mom_12_1", "mom_6_1", "symbol"], ascending=[False, False, False, True])
    selected = _select_ranked_with_sector_cap(
        ranked,
        holdings_count=spec.holdings_count,
        sector_slot_cap=_sector_slot_cap(
            holdings_count=spec.holdings_count,
            sector_cap=spec.sector_cap,
            stock_exposure=stock_exposure,
        ),
    )
    if selected.empty:
        metadata = {
            "regime": regime,
            "benchmark_trend_positive": benchmark_trend_positive,
            "breadth_ratio": breadth_ratio,
            "stock_exposure": 0.0,
            "selected_symbols": (),
            "candidate_count": int(len(eligible)),
        }
        return {safe_symbol: 1.0}, ranked, metadata

    per_name_weight = min(float(spec.single_name_cap), stock_exposure / float(len(selected)))
    weights = {str(row.symbol): per_name_weight for row in selected.itertuples(index=False)}
    invested_weight = per_name_weight * len(selected)
    if invested_weight < 1.0:
        weights[safe_symbol] = 1.0 - invested_weight
    metadata = {
        "regime": regime,
        "benchmark_trend_positive": benchmark_trend_positive,
        "breadth_ratio": breadth_ratio,
        "stock_exposure": stock_exposure,
        "selected_symbols": tuple(selected["symbol"].tolist()),
        "candidate_count": int(len(eligible)),
    }
    return weights, ranked, metadata


def run_snapshot_candidate_backtest(
    context: SnapshotBacktestContext,
    spec: SnapshotCandidateSpec,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
) -> dict[str, object]:
    start_ts = pd.Timestamp(start_date).normalize() if start_date else context.close_matrix.index.min()
    end_ts = pd.Timestamp(end_date).normalize() if end_date else context.close_matrix.index.max()
    close_matrix = context.close_matrix.loc[(context.close_matrix.index >= start_ts) & (context.close_matrix.index <= end_ts)].copy()
    if len(close_matrix) < 2:
        raise ValueError(f"not enough Russell 1000 price rows for {spec.candidate_id} in requested period")

    safe_symbol = spec.safe_symbol.upper()
    benchmark_symbol = spec.benchmark_symbol.upper()
    returns_matrix = context.returns_matrix.reindex(close_matrix.index).fillna(0.0).copy()
    if safe_symbol not in returns_matrix.columns:
        returns_matrix[safe_symbol] = 0.0
    if safe_symbol not in close_matrix.columns:
        close_matrix[safe_symbol] = 1.0

    symbols = sorted(set(close_matrix.columns) | {safe_symbol})
    weights_history = pd.DataFrame(0.0, index=close_matrix.index, columns=symbols)
    portfolio_returns = pd.Series(0.0, index=close_matrix.index, name="portfolio_return")
    turnover_history = pd.Series(0.0, index=close_matrix.index, name="turnover")
    rebalance_dates = build_monthly_rebalance_dates(close_matrix.index)
    current_weights: dict[str, float] = {safe_symbol: 1.0}
    current_holdings: set[str] = set()
    score_rows: list[pd.DataFrame] = []

    for idx in range(len(close_matrix.index) - 1):
        as_of = close_matrix.index[idx]
        next_date = close_matrix.index[idx + 1]
        if as_of in rebalance_dates:
            active_universe = resolve_active_universe(context.universe, as_of)
            snapshot = _build_feature_snapshot_for_backtest(
                as_of,
                active_universe,
                context.feature_history_by_symbol,
                benchmark_symbol=benchmark_symbol,
            )
            if spec.rule == "default_factor_stack":
                raise ValueError(
                    "default_factor_stack belonged to the retired Russell 1000 Multi-Factor Defensive profile"
                )
            target_weights, scores, _metadata = _build_new_snapshot_target_weights(snapshot, current_holdings, spec)
            turnover = _compute_turnover(current_weights, target_weights)
            turnover_history.at[next_date] = turnover
            current_weights = target_weights
            current_holdings = {symbol for symbol, weight in current_weights.items() if weight > 0 and symbol != safe_symbol}
            if not scores.empty:
                scored = scores.copy()
                scored.insert(0, "candidate_id", spec.candidate_id)
                scored.insert(1, "rebalance_as_of", as_of)
                score_rows.append(scored)

        for symbol, weight in current_weights.items():
            weights_history.at[as_of, symbol] = weight
        next_returns = returns_matrix.loc[next_date]
        gross_return = sum(float(weight) * float(next_returns.get(symbol, 0.0)) for symbol, weight in current_weights.items())
        cost = float(turnover_history.at[next_date]) * (float(turnover_cost_bps) / 10_000.0)
        portfolio_returns.at[next_date] = gross_return - cost

    for symbol, weight in current_weights.items():
        weights_history.at[close_matrix.index[-1], symbol] = weight
    used_weights = weights_history.loc[:, (weights_history != 0.0).any(axis=0)]
    benchmark_returns = returns_matrix.get(benchmark_symbol, pd.Series(index=portfolio_returns.index, dtype=float))
    summary = summarize_returns(
        portfolio_returns,
        weights_history=used_weights,
        benchmark_returns=benchmark_returns,
        start_date=start_date,
        end_date=end_date,
    )
    summary.update(
        {
            "Candidate": spec.candidate_id,
            "Display Name": spec.display_name,
            "Candidate Type": "snapshot_r1000",
            "Candidate Group": spec.candidate_group,
            "Rule": spec.rule,
            "Benchmark Symbol": spec.benchmark_symbol.upper(),
            "Safe Symbol": spec.safe_symbol.upper(),
            "Notes": spec.notes,
        }
    )
    return {
        "summary": summary,
        "portfolio_returns": portfolio_returns,
        "benchmark_returns": benchmark_returns,
        "weights_history": used_weights,
        "turnover_history": turnover_history,
        "candidate_scores": pd.concat(score_rows, ignore_index=True) if score_rows else pd.DataFrame(),
    }


def _period_start_index(periods: Sequence[tuple[str, str, str | None]]) -> str:
    starts = [pd.Timestamp(start).date().isoformat() for _name, start, _end in periods]
    return min(starts)


def _period_end_index(periods: Sequence[tuple[str, str, str | None]]) -> str | None:
    ends = [pd.Timestamp(end).date().isoformat() for _name, _start, end in periods if end]
    return max(ends) if ends else None


def _slice_weights_history(weights_history: pd.DataFrame, *, start_date: str, end_date: str | None) -> pd.DataFrame:
    if weights_history is None or weights_history.empty:
        return pd.DataFrame()
    frame = weights_history.copy()
    if "as_of" in frame.columns:
        frame["as_of"] = pd.to_datetime(frame["as_of"]).dt.tz_localize(None).dt.normalize()
        frame = frame.set_index("as_of")
    else:
        frame.index = pd.to_datetime(frame.index).tz_localize(None).normalize()
    start_ts = pd.Timestamp(start_date).normalize()
    end_ts = pd.Timestamp(end_date).normalize() if end_date else frame.index.max()
    return frame.loc[(frame.index >= start_ts) & (frame.index <= end_ts)].copy()


def _period_summary_from_result(
    result: Mapping[str, object],
    *,
    period_name: str,
    start_date: str,
    end_date: str | None,
) -> dict[str, object]:
    returns = pd.Series(result["portfolio_returns"]).copy()
    returns.index = pd.to_datetime(returns.index).tz_localize(None).normalize()
    start_ts = pd.Timestamp(start_date).normalize()
    end_ts = pd.Timestamp(end_date).normalize() if end_date else returns.index.max()
    period_returns = returns.loc[(returns.index >= start_ts) & (returns.index <= end_ts)]
    benchmark_returns = pd.Series(result.get("benchmark_returns", pd.Series(dtype=float))).copy()
    if not benchmark_returns.empty:
        benchmark_returns.index = pd.to_datetime(benchmark_returns.index).tz_localize(None).normalize()
    weights_history = _slice_weights_history(
        pd.DataFrame(result.get("weights_history", pd.DataFrame())),
        start_date=start_date,
        end_date=end_date,
    )
    summary = summarize_returns(
        period_returns,
        weights_history=weights_history,
        benchmark_returns=benchmark_returns,
        start_date=start_date,
        end_date=end_date,
    )
    base_summary = dict(result.get("summary", {}))
    for key in (
        "Candidate",
        "Display Name",
        "Candidate Type",
        "Candidate Group",
        "Rule",
        "Benchmark Symbol",
        "Safe Symbol",
        "Notes",
    ):
        if key in base_summary:
            summary[key] = base_summary[key]
    return {"Period": period_name, **summary}


def _period_summary_rows_by_candidate(period_summary: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return {
        str(candidate): frame.sort_values("Period")
        for candidate, frame in period_summary.groupby("Candidate", sort=False)
    }


def build_ranking(period_summary: pd.DataFrame, *, max_live_candidates: int = DEFAULT_MAX_LIVE_CANDIDATES) -> pd.DataFrame:
    if period_summary.empty:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []
    for candidate, frame in _period_summary_rows_by_candidate(period_summary).items():
        numeric = frame.copy()
        for column in ("CAGR", "Sharpe", "Max Drawdown", "Excess CAGR vs Benchmark", "Turnover/Year"):
            numeric[column] = pd.to_numeric(numeric.get(column), errors="coerce")
        all_periods_available = len(frame) >= 3 and numeric["Trading Days"].fillna(0).ge(60).all()
        positive_return_all_periods = numeric["CAGR"].gt(0).all()
        positive_sharpe_all_periods = numeric["Sharpe"].gt(0).all()
        drawdown_gate = numeric["Max Drawdown"].ge(MAX_ALLOWED_DRAWDOWN).all()
        long_rows = frame.loc[frame["Period"].eq("long")]
        long_excess = float(long_rows["Excess CAGR vs Benchmark"].iloc[0]) if not long_rows.empty else float("nan")
        min_sharpe = float(numeric["Sharpe"].min())
        median_sharpe = float(numeric["Sharpe"].median())
        median_excess_cagr = float(numeric["Excess CAGR vs Benchmark"].median())
        worst_drawdown = float(numeric["Max Drawdown"].min())
        median_turnover = float(numeric["Turnover/Year"].median())
        robustness_score = (
            min_sharpe
            + 0.50 * median_sharpe
            + 4.0 * median_excess_cagr
            + 0.75 * worst_drawdown
            - 0.05 * median_turnover
        )
        live_gate_passed = bool(
            all_periods_available
            and positive_return_all_periods
            and positive_sharpe_all_periods
            and drawdown_gate
            and (pd.isna(long_excess) or long_excess > -0.03)
        )
        first_row = frame.iloc[0]
        rows.append(
            {
                "Candidate": candidate,
                "Display Name": first_row.get("Display Name"),
                "Candidate Type": first_row.get("Candidate Type"),
                "Candidate Group": first_row.get("Candidate Group"),
                "Rule": first_row.get("Rule"),
                "Benchmark Symbol": first_row.get("Benchmark Symbol"),
                "min_sharpe": min_sharpe,
                "median_sharpe": median_sharpe,
                "median_excess_cagr": median_excess_cagr,
                "long_excess_cagr": long_excess,
                "worst_drawdown": worst_drawdown,
                "median_turnover_per_year": median_turnover,
                "robustness_score": robustness_score,
                "live_gate_passed": live_gate_passed,
                "gate_reason": _gate_reason(
                    all_periods_available=all_periods_available,
                    positive_return_all_periods=positive_return_all_periods,
                    positive_sharpe_all_periods=positive_sharpe_all_periods,
                    drawdown_gate=drawdown_gate,
                    long_excess=long_excess,
                ),
                "Notes": first_row.get("Notes"),
            }
        )
    ranking = pd.DataFrame(rows).sort_values(
        ["live_gate_passed", "robustness_score", "median_sharpe"],
        ascending=[False, False, False],
    ).reset_index(drop=True)
    ranking.insert(0, "rank", range(1, len(ranking) + 1))
    baseline_rows = ranking.loc[ranking["Candidate Group"].eq("current_live_baseline")]
    ranking["beats_live_baseline"] = False
    if not baseline_rows.empty:
        baseline = baseline_rows.iloc[0]
        comparable = ~ranking["Candidate Group"].eq("current_live_baseline")
        ranking.loc[comparable, "beats_live_baseline"] = (
            pd.to_numeric(ranking.loc[comparable, "robustness_score"], errors="coerce").gt(float(baseline["robustness_score"]))
            & pd.to_numeric(ranking.loc[comparable, "long_excess_cagr"], errors="coerce").gt(float(baseline["long_excess_cagr"]))
            & pd.to_numeric(ranking.loc[comparable, "worst_drawdown"], errors="coerce").ge(float(baseline["worst_drawdown"]))
            & pd.to_numeric(ranking.loc[comparable, "min_sharpe"], errors="coerce").ge(float(baseline["min_sharpe"]))
        )
    ranking["replacement_review_candidate"] = (
        ranking["live_gate_passed"]
        & ranking["beats_live_baseline"]
        & ranking["Candidate Group"].eq("optimization_variant")
    )
    ranking["supplemental_review_candidate"] = False
    supplemental_selectable = ranking.index[
        ranking["live_gate_passed"]
        & ranking["beats_live_baseline"]
        & ranking["Candidate Group"].isin({"new_ordinary_strategy", "new_snapshot_strategy"})
    ].tolist()
    for index in supplemental_selectable[: int(max_live_candidates)]:
        ranking.at[index, "supplemental_review_candidate"] = True
    ranking["live_enabled_candidate"] = ranking["replacement_review_candidate"] | ranking["supplemental_review_candidate"]
    ranking["review_action"] = "reject"
    ranking.loc[ranking["Candidate Group"].eq("current_live_baseline"), "review_action"] = "current_live_baseline"
    ranking.loc[ranking["Candidate Group"].eq("optimization_variant"), "review_action"] = "no_replacement"
    ranking.loc[
        ranking["replacement_review_candidate"],
        "review_action",
    ] = "replacement_review_candidate"
    ranking.loc[
        ranking["supplemental_review_candidate"],
        "review_action",
    ] = "supplemental_review_candidate"
    return ranking


def _gate_reason(
    *,
    all_periods_available: bool,
    positive_return_all_periods: bool,
    positive_sharpe_all_periods: bool,
    drawdown_gate: bool,
    long_excess: float,
) -> str:
    reasons = []
    if not all_periods_available:
        reasons.append("missing_or_too_short_period")
    if not positive_return_all_periods:
        reasons.append("non_positive_cagr_period")
    if not positive_sharpe_all_periods:
        reasons.append("non_positive_sharpe_period")
    if not drawdown_gate:
        reasons.append("drawdown_above_30pct")
    if not pd.isna(long_excess) and long_excess <= -0.03:
        reasons.append("long_excess_cagr_below_minus_3pct")
    return "pass" if not reasons else ";".join(reasons)


def run_candidate_research(
    *,
    etf_price_history: pd.DataFrame,
    periods: Sequence[tuple[str, str, str | None]] = DEFAULT_PERIODS,
    r1000_price_history: pd.DataFrame | None = None,
    r1000_universe: pd.DataFrame | None = None,
    etf_candidates: Sequence[EtfCandidateSpec] = ETF_CANDIDATES,
    snapshot_candidates: Sequence[SnapshotCandidateSpec] = SNAPSHOT_CANDIDATES,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
    max_live_candidates: int = DEFAULT_MAX_LIVE_CANDIDATES,
) -> dict[str, pd.DataFrame]:
    summary_rows: list[dict[str, object]] = []
    full_start = _period_start_index(periods)
    full_end = _period_end_index(periods)
    for spec in etf_candidates:
        result = run_etf_candidate_backtest(
            etf_price_history,
            spec,
            start_date=full_start,
            end_date=full_end,
            turnover_cost_bps=turnover_cost_bps,
        )
        for period_name, start_date, end_date in periods:
            summary_rows.append(
                _period_summary_from_result(
                    result,
                    period_name=period_name,
                    start_date=start_date,
                    end_date=end_date,
                )
            )
    if r1000_price_history is not None and r1000_universe is not None:
        snapshot_context = prepare_snapshot_backtest_context(
            r1000_price_history,
            r1000_universe,
            end_date=full_end,
        )
        for spec in snapshot_candidates:
            result = run_snapshot_candidate_backtest(
                snapshot_context,
                spec,
                start_date=full_start,
                end_date=full_end,
                turnover_cost_bps=turnover_cost_bps,
            )
            for period_name, start_date, end_date in periods:
                summary_rows.append(
                    _period_summary_from_result(
                        result,
                        period_name=period_name,
                        start_date=start_date,
                        end_date=end_date,
                    )
                )

    period_summary = pd.DataFrame(summary_rows)
    ranking = build_ranking(period_summary, max_live_candidates=max_live_candidates)
    return {"period_summary": period_summary, "ranking": ranking}


def _format_percent_columns(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    for column in (
        "Total Return",
        "CAGR",
        "Volatility",
        "Max Drawdown",
        "Benchmark Total Return",
        "Benchmark CAGR",
        "Excess CAGR vs Benchmark",
        "min_sharpe",
        "median_sharpe",
        "median_excess_cagr",
        "long_excess_cagr",
        "worst_drawdown",
        "robustness_score",
    ):
        if column in output.columns:
            output[column] = pd.to_numeric(output[column], errors="coerce")
    return output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backtest ordinary ETF and snapshot US equity strategy candidates.")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--etf-prices", help="Existing ETF price-history CSV with symbol/as_of/close columns")
    input_group.add_argument("--download", action="store_true", help="Download ETF price history through yfinance")
    parser.add_argument("--price-start", help="ETF download start; defaults to earliest requested period start minus lookback buffer")
    parser.add_argument("--price-end", help="ETF download end date")
    parser.add_argument("--periods", default=",".join(f"{name}:{start}:{end or ''}" for name, start, end in DEFAULT_PERIODS))
    parser.add_argument("--r1000-prices", help="Optional Russell 1000 price-history CSV for snapshot candidates")
    parser.add_argument("--r1000-universe", help="Optional Russell 1000 universe-history CSV for snapshot candidates")
    parser.add_argument("--turnover-cost-bps", type=float, default=DEFAULT_TURNOVER_COST_BPS)
    parser.add_argument("--max-live-candidates", type=int, default=DEFAULT_MAX_LIVE_CANDIDATES)
    parser.add_argument("--output-dir", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    periods = _parse_periods(args.periods)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.download:
        price_start = args.price_start or (pd.Timestamp(_period_start_index(periods)) - pd.Timedelta(days=420)).date().isoformat()
        etf_prices = download_price_history(
            list(collect_required_etf_symbols()),
            start=price_start,
            end=args.price_end,
        )
        etf_prices.to_csv(output_dir / "downloaded_etf_price_history.csv", index=False)
    else:
        etf_prices = read_table(args.etf_prices)

    r1000_prices = read_table(args.r1000_prices) if args.r1000_prices else None
    r1000_universe = read_table(args.r1000_universe) if args.r1000_universe else None
    if (r1000_prices is None) != (r1000_universe is None):
        raise ValueError("--r1000-prices and --r1000-universe must be supplied together")

    result = run_candidate_research(
        etf_price_history=etf_prices,
        periods=periods,
        r1000_price_history=r1000_prices,
        r1000_universe=r1000_universe,
        turnover_cost_bps=float(args.turnover_cost_bps),
        max_live_candidates=int(args.max_live_candidates),
    )
    period_summary = _format_percent_columns(result["period_summary"])
    ranking = _format_percent_columns(result["ranking"])

    period_summary.to_csv(output_dir / "period_summary.csv", index=False)
    ranking.to_csv(output_dir / "ranking.csv", index=False)
    print(ranking.to_string(index=False))
    print(f"wrote candidate research outputs -> {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
