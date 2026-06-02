from __future__ import annotations

import argparse
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .russell_1000_multi_factor_defensive_snapshot import read_table
from .yfinance_prices import download_price_history

DEFAULT_PERIODS = (
    ("short", "2025-06-01", None),
    ("medium", "2023-06-01", None),
    ("long", "2011-01-01", None),
)
DEFAULT_TURNOVER_COST_BPS = 5.0
DEFAULT_SAFE_SYMBOL = "BIL"
MARKET_BENCHMARK_SYMBOL = "SPY"
DRAWDOWN_MARKET_BEAT_THRESHOLD = -0.25


@dataclass(frozen=True)
class LeveragedCandidateSpec:
    candidate_id: str
    display_name: str
    candidate_group: str
    rule: str
    signal_symbol: str
    risk_on_weights: Mapping[str, float]
    risk_off_weights: Mapping[str, float]
    benchmark_symbol: str = MARKET_BENCHMARK_SYMBOL
    strategy_benchmark_symbol: str | None = None
    safe_symbol: str = DEFAULT_SAFE_SYMBOL
    trend_window: int = 200
    require_ma20_slope: bool = False
    allow_pullback: bool = False
    notes: str = ""


LEVERAGED_CANDIDATES: tuple[LeveragedCandidateSpec, ...] = (
    LeveragedCandidateSpec(
        candidate_id="live_tqqq_dual_drive_45_45_proxy",
        display_name="Live TQQQ Dual-Drive 45/45 Proxy",
        candidate_group="current_live_proxy",
        rule="ma200_pullback",
        signal_symbol="QQQ",
        risk_on_weights={"QQQ": 0.45, "TQQQ": 0.45, DEFAULT_SAFE_SYMBOL: 0.10},
        risk_off_weights={DEFAULT_SAFE_SYMBOL: 1.0},
        strategy_benchmark_symbol="QQQ",
        trend_window=200,
        require_ma20_slope=True,
        allow_pullback=True,
        notes="Proxy for current live TQQQ fixed QQQ/TQQQ dual-drive profile.",
    ),
    LeveragedCandidateSpec(
        candidate_id="opt_tqqq_dual_drive_40_40",
        display_name="Optimized TQQQ Dual-Drive 40/40",
        candidate_group="optimization_variant",
        rule="ma200_pullback",
        signal_symbol="QQQ",
        risk_on_weights={"QQQ": 0.40, "TQQQ": 0.40, DEFAULT_SAFE_SYMBOL: 0.20},
        risk_off_weights={DEFAULT_SAFE_SYMBOL: 1.0},
        strategy_benchmark_symbol="QQQ",
        trend_window=200,
        require_ma20_slope=True,
        allow_pullback=True,
        notes="Lower-risk parameter variant of the current TQQQ dual-drive profile.",
    ),
    LeveragedCandidateSpec(
        candidate_id="opt_tqqq_qld_tqqq_60_20",
        display_name="Optimized QLD/TQQQ 60/20 Nasdaq Sleeve",
        candidate_group="optimization_variant",
        rule="ma200_pullback",
        signal_symbol="QQQ",
        risk_on_weights={"QLD": 0.60, "TQQQ": 0.20, DEFAULT_SAFE_SYMBOL: 0.20},
        risk_off_weights={DEFAULT_SAFE_SYMBOL: 1.0},
        strategy_benchmark_symbol="QQQ",
        trend_window=200,
        require_ma20_slope=True,
        allow_pullback=True,
        notes="Nasdaq leveraged sleeve using more 2x QLD and less 3x TQQQ exposure.",
    ),
    LeveragedCandidateSpec(
        candidate_id="live_soxl_soxx_trend_57_proxy",
        display_name="Live SOXL/SOXX Trend 57 Proxy",
        candidate_group="current_live_proxy",
        rule="trend_switch",
        signal_symbol="SOXL",
        risk_on_weights={"SOXL": 0.57, DEFAULT_SAFE_SYMBOL: 0.43},
        risk_off_weights={"SOXX": 0.57, DEFAULT_SAFE_SYMBOL: 0.43},
        strategy_benchmark_symbol="SOXX",
        trend_window=150,
        notes="Proxy for current live SOXL/SOXX strategy: SOXL above 150d MA holds SOXL, otherwise SOXX.",
    ),
    LeveragedCandidateSpec(
        candidate_id="opt_soxl_soxx_signal_soxx_50",
        display_name="Optimized SOXL/SOXX SOXX-Signal 50",
        candidate_group="optimization_variant",
        rule="trend_switch",
        signal_symbol="SOXX",
        risk_on_weights={"SOXL": 0.50, DEFAULT_SAFE_SYMBOL: 0.50},
        risk_off_weights={"SOXX": 0.50, DEFAULT_SAFE_SYMBOL: 0.50},
        strategy_benchmark_symbol="SOXX",
        trend_window=150,
        notes="Lower deploy SOXL/SOXX variant using the unlevered SOXX trend as signal source.",
    ),
    LeveragedCandidateSpec(
        candidate_id="new_qld_qqq_trend_70_20",
        display_name="New QLD/QQQ Trend 70/20",
        candidate_group="leveraged_supplement",
        rule="ma200_trend",
        signal_symbol="QQQ",
        risk_on_weights={"QLD": 0.70, "QQQ": 0.20, DEFAULT_SAFE_SYMBOL: 0.10},
        risk_off_weights={DEFAULT_SAFE_SYMBOL: 1.0},
        strategy_benchmark_symbol="QQQ",
        trend_window=200,
        require_ma20_slope=True,
        notes="New 2x Nasdaq trend sleeve; similar growth role to TQQQ with less 3x path dependency.",
    ),
    LeveragedCandidateSpec(
        candidate_id="new_rom_xlk_trend_70_20",
        display_name="New ROM/XLK Trend 70/20",
        candidate_group="leveraged_supplement",
        rule="ma200_trend",
        signal_symbol="XLK",
        risk_on_weights={"ROM": 0.70, "XLK": 0.20, DEFAULT_SAFE_SYMBOL: 0.10},
        risk_off_weights={DEFAULT_SAFE_SYMBOL: 1.0},
        strategy_benchmark_symbol="XLK",
        trend_window=200,
        require_ma20_slope=True,
        notes="New 2x technology-sector trend sleeve; lower leverage than TECL and not a TQQQ parameter variant.",
    ),
    LeveragedCandidateSpec(
        candidate_id="new_tecl_xlk_trend_50_30",
        display_name="New TECL/XLK Trend 50/30",
        candidate_group="leveraged_supplement",
        rule="ma200_trend",
        signal_symbol="XLK",
        risk_on_weights={"TECL": 0.50, "XLK": 0.30, DEFAULT_SAFE_SYMBOL: 0.20},
        risk_off_weights={DEFAULT_SAFE_SYMBOL: 1.0},
        strategy_benchmark_symbol="XLK",
        trend_window=200,
        require_ma20_slope=True,
        notes="New technology-sector 3x/1x blended trend sleeve.",
    ),
    LeveragedCandidateSpec(
        candidate_id="new_upro_spy_trend_50_30",
        display_name="New UPRO/SPY Trend 50/30",
        candidate_group="leveraged_supplement",
        rule="ma200_trend",
        signal_symbol="SPY",
        risk_on_weights={"UPRO": 0.50, "SPY": 0.30, DEFAULT_SAFE_SYMBOL: 0.20},
        risk_off_weights={DEFAULT_SAFE_SYMBOL: 1.0},
        strategy_benchmark_symbol="SPY",
        trend_window=200,
        require_ma20_slope=True,
        notes="New S&P 500 3x/1x blended trend sleeve for broad-market leveraged exposure.",
    ),
    LeveragedCandidateSpec(
        candidate_id="new_usd_smh_trend_50_30",
        display_name="New USD/SMH Trend 50/30",
        candidate_group="leveraged_supplement",
        rule="ma200_trend",
        signal_symbol="SMH",
        risk_on_weights={"USD": 0.50, "SMH": 0.30, DEFAULT_SAFE_SYMBOL: 0.20},
        risk_off_weights={DEFAULT_SAFE_SYMBOL: 1.0},
        strategy_benchmark_symbol="SMH",
        trend_window=200,
        require_ma20_slope=True,
        notes="New 2x semiconductor/1x semiconductor blended trend sleeve; related to SOXL domain but independently gated.",
    ),
)


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


def collect_required_symbols(candidates: Sequence[LeveragedCandidateSpec] = LEVERAGED_CANDIDATES) -> tuple[str, ...]:
    symbols: list[str] = [MARKET_BENCHMARK_SYMBOL]
    for candidate in candidates:
        for symbol in (
            candidate.signal_symbol,
            candidate.benchmark_symbol,
            candidate.strategy_benchmark_symbol,
            candidate.safe_symbol,
            *candidate.risk_on_weights.keys(),
            *candidate.risk_off_weights.keys(),
        ):
            text = str(symbol or "").strip().upper()
            if text and text not in symbols:
                symbols.append(text)
    return tuple(symbols)


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


def _period_start(periods: Sequence[tuple[str, str, str | None]]) -> str:
    return min(pd.Timestamp(start).date().isoformat() for _name, start, _end in periods)


def _period_end(periods: Sequence[tuple[str, str, str | None]]) -> str | None:
    ends = [pd.Timestamp(end).date().isoformat() for _name, _start, end in periods if end]
    return max(ends) if ends else None


def _normalize_weights(weights: Mapping[str, float]) -> dict[str, float]:
    cleaned = {str(symbol).strip().upper(): float(weight) for symbol, weight in weights.items() if float(weight) > 0.0}
    total = sum(cleaned.values())
    if total <= 0:
        raise ValueError("weights must sum to a positive value")
    if total > 1.000001:
        return {symbol: weight / total for symbol, weight in cleaned.items()}
    return cleaned


def _is_risk_on(close: pd.DataFrame, as_of: pd.Timestamp, spec: LeveragedCandidateSpec) -> bool:
    symbol = spec.signal_symbol.upper()
    if symbol not in close.columns:
        return False
    series = close[symbol].loc[close.index <= as_of].dropna()
    if len(series) < max(int(spec.trend_window), 20):
        return False
    latest = float(series.iloc[-1])
    trend = float(series.rolling(int(spec.trend_window)).mean().iloc[-1])
    ma20 = series.rolling(20).mean()
    ma20_slope = float(ma20.diff().iloc[-1]) if pd.notna(ma20.diff().iloc[-1]) else float("nan")
    above_trend = latest > trend
    slope_ok = (ma20_slope > 0.0) if spec.require_ma20_slope else True
    if spec.rule == "ma200_pullback" and spec.allow_pullback:
        latest_ma20 = float(ma20.iloc[-1]) if pd.notna(ma20.iloc[-1]) else float("nan")
        pullback_on = pd.notna(latest_ma20) and latest > latest_ma20 and ma20_slope > 0.0
        return bool((above_trend and slope_ok) or pullback_on)
    return bool(above_trend and slope_ok)


def summarize_returns(
    returns: pd.Series,
    *,
    weights_history: pd.DataFrame | None = None,
    benchmark_returns: pd.Series | None = None,
    market_returns: pd.Series | None = None,
) -> dict[str, float | str | int]:
    clean = pd.to_numeric(returns, errors="coerce").dropna()
    if clean.empty:
        raise RuntimeError("No returns to summarize")
    equity = (1.0 + clean).cumprod()
    years = max((clean.index[-1] - clean.index[0]).days / 365.25, 1 / 365.25)
    cagr = float(equity.iloc[-1] ** (1.0 / years) - 1.0)
    total_return = float(equity.iloc[-1] - 1.0)
    drawdown = equity / equity.cummax() - 1.0
    max_drawdown = float(drawdown.min())
    volatility = float(clean.std(ddof=0) * math.sqrt(252))
    std = float(clean.std(ddof=0))
    sharpe = float(clean.mean() / std * math.sqrt(252)) if std else float("nan")
    calmar = float(cagr / abs(max_drawdown)) if max_drawdown < 0 else float("nan")

    benchmark_cagr = float("nan")
    benchmark_total_return = float("nan")
    if benchmark_returns is not None:
        bench = pd.to_numeric(benchmark_returns.reindex(clean.index), errors="coerce").dropna()
        if not bench.empty:
            bench_equity = (1.0 + bench).cumprod()
            bench_years = max((bench.index[-1] - bench.index[0]).days / 365.25, 1 / 365.25)
            benchmark_cagr = float(bench_equity.iloc[-1] ** (1.0 / bench_years) - 1.0)
            benchmark_total_return = float(bench_equity.iloc[-1] - 1.0)

    market_cagr = float("nan")
    if market_returns is not None:
        market = pd.to_numeric(market_returns.reindex(clean.index), errors="coerce").dropna()
        if not market.empty:
            market_equity = (1.0 + market).cumprod()
            market_years = max((market.index[-1] - market.index[0]).days / 365.25, 1 / 365.25)
            market_cagr = float(market_equity.iloc[-1] ** (1.0 / market_years) - 1.0)

    turnover_per_year = float("nan")
    avg_risk_exposure = float("nan")
    if weights_history is not None and not weights_history.empty:
        weight_frame = weights_history.fillna(0.0)
        changes = weight_frame.diff().fillna(0.0)
        if not changes.empty:
            changes.iloc[0] = 0.0
        turnover_per_year = float((0.5 * changes.abs().sum(axis=1)).sum() / years)
        risk_columns = [column for column in weight_frame.columns if column not in {DEFAULT_SAFE_SYMBOL, "CASH"}]
        avg_risk_exposure = float(weight_frame[risk_columns].sum(axis=1).mean()) if risk_columns else 0.0

    return {
        "Start": clean.index[0].date().isoformat(),
        "End": clean.index[-1].date().isoformat(),
        "Trading Days": int(len(clean)),
        "Total Return": total_return,
        "CAGR": cagr,
        "Max Drawdown": max_drawdown,
        "Volatility": volatility,
        "Sharpe": sharpe,
        "Calmar": calmar,
        "Benchmark Total Return": benchmark_total_return,
        "Benchmark CAGR": benchmark_cagr,
        "Excess CAGR vs Benchmark": cagr - benchmark_cagr if not pd.isna(benchmark_cagr) else float("nan"),
        "Market CAGR": market_cagr,
        "Excess CAGR vs Market": cagr - market_cagr if not pd.isna(market_cagr) else float("nan"),
        "Turnover/Year": turnover_per_year,
        "Avg Risk Exposure": avg_risk_exposure,
    }


def run_candidate_backtest(
    price_history: pd.DataFrame,
    spec: LeveragedCandidateSpec,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
) -> dict[str, object]:
    close = _normalize_price_history(price_history)
    full_start = pd.Timestamp(start_date).normalize() if start_date else close.index.min()
    full_end = pd.Timestamp(end_date).normalize() if end_date else close.index.max()
    close = close.loc[close.index <= full_end].copy()
    index = close.index[close.index >= full_start]
    if len(index) < 2:
        raise ValueError(f"not enough price history for {spec.candidate_id}")
    returns = close.pct_change(fill_method=None).fillna(0.0)
    strategy_returns = pd.Series(0.0, index=index[1:], name="strategy_return")
    turnover_history = pd.Series(0.0, index=index[1:], name="turnover")
    all_symbols = sorted(set(spec.risk_on_weights) | set(spec.risk_off_weights) | {spec.safe_symbol.upper()})
    weights_history = pd.DataFrame(0.0, index=index[1:], columns=all_symbols)
    current_weights: dict[str, float] = {}
    signal_rows: list[dict[str, object]] = []
    for idx, as_of in enumerate(index[:-1]):
        next_date = index[idx + 1]
        target = _normalize_weights(spec.risk_on_weights if _is_risk_on(close, as_of, spec) else spec.risk_off_weights)
        turnover = 0.5 * sum(abs(target.get(symbol, 0.0) - current_weights.get(symbol, 0.0)) for symbol in set(target) | set(current_weights))
        turnover_history.at[next_date] = turnover
        current_weights = target
        for symbol, weight in current_weights.items():
            weights_history.at[next_date, symbol] = weight
        gross_return = 0.0
        for symbol, weight in current_weights.items():
            if symbol in returns.columns:
                value = returns.at[next_date, symbol]
                if pd.notna(value):
                    gross_return += float(weight) * float(value)
        strategy_returns.at[next_date] = gross_return - turnover * (float(turnover_cost_bps) / 10_000.0)
        signal_rows.append(
            {
                "as_of": as_of,
                "next_date": next_date,
                "risk_on": bool(spec.risk_on_weights == current_weights or set(current_weights) == set(_normalize_weights(spec.risk_on_weights))),
                **{f"weight_{symbol}": weight for symbol, weight in current_weights.items()},
            }
        )

    strategy_benchmark = (spec.strategy_benchmark_symbol or spec.benchmark_symbol).upper()
    summary = summarize_returns(
        strategy_returns,
        weights_history=weights_history,
        benchmark_returns=returns.get(strategy_benchmark, pd.Series(index=strategy_returns.index, dtype=float)),
        market_returns=returns.get(MARKET_BENCHMARK_SYMBOL, pd.Series(index=strategy_returns.index, dtype=float)),
    )
    summary.update(
        {
            "Candidate": spec.candidate_id,
            "Display Name": spec.display_name,
            "Candidate Group": spec.candidate_group,
            "Rule": spec.rule,
            "Signal Symbol": spec.signal_symbol.upper(),
            "Benchmark Symbol": strategy_benchmark,
            "Market Benchmark Symbol": MARKET_BENCHMARK_SYMBOL,
            "Notes": spec.notes,
        }
    )
    return {
        "summary": summary,
        "portfolio_returns": strategy_returns,
        "weights_history": weights_history,
        "turnover_history": turnover_history,
        "signal_history": pd.DataFrame(signal_rows),
        "benchmark_returns": returns.get(strategy_benchmark, pd.Series(index=strategy_returns.index, dtype=float)),
        "market_returns": returns.get(MARKET_BENCHMARK_SYMBOL, pd.Series(index=strategy_returns.index, dtype=float)),
    }


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
    weights = pd.DataFrame(result.get("weights_history", pd.DataFrame())).copy()
    if not weights.empty:
        weights.index = pd.to_datetime(weights.index).tz_localize(None).normalize()
        weights = weights.loc[(weights.index >= start_ts) & (weights.index <= end_ts)]
    benchmark_returns = pd.Series(result.get("benchmark_returns", pd.Series(dtype=float))).copy()
    if not benchmark_returns.empty:
        benchmark_returns.index = pd.to_datetime(benchmark_returns.index).tz_localize(None).normalize()
    market_returns = pd.Series(result.get("market_returns", pd.Series(dtype=float))).copy()
    if not market_returns.empty:
        market_returns.index = pd.to_datetime(market_returns.index).tz_localize(None).normalize()
    summary = summarize_returns(
        period_returns,
        weights_history=weights,
        benchmark_returns=benchmark_returns,
        market_returns=market_returns,
    )
    base = dict(result["summary"])
    for key in ("Candidate", "Display Name", "Candidate Group", "Rule", "Signal Symbol", "Benchmark Symbol", "Market Benchmark Symbol", "Notes"):
        summary[key] = base.get(key)
    return {"Period": period_name, **summary}


def build_ranking(period_summary: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for candidate, frame in period_summary.groupby("Candidate", sort=False):
        numeric = frame.copy()
        for column in ("CAGR", "Sharpe", "Max Drawdown", "Excess CAGR vs Market", "Turnover/Year"):
            numeric[column] = pd.to_numeric(numeric.get(column), errors="coerce")
        long_rows = numeric.loc[numeric["Period"].eq("long")]
        long_excess_market = float(long_rows["Excess CAGR vs Market"].iloc[0]) if not long_rows.empty else float("nan")
        min_sharpe = float(numeric["Sharpe"].min())
        median_sharpe = float(numeric["Sharpe"].median())
        worst_drawdown = float(numeric["Max Drawdown"].min())
        median_excess_market = float(numeric["Excess CAGR vs Market"].median())
        median_turnover = float(numeric["Turnover/Year"].median())
        all_periods_available = len(frame) >= 3 and numeric["Trading Days"].fillna(0).ge(60).all()
        positive_return_all_periods = numeric["CAGR"].gt(0).all()
        positive_sharpe_all_periods = numeric["Sharpe"].gt(0).all()
        drawdown_gate = worst_drawdown > -0.45
        market_beat_gate = not (worst_drawdown <= DRAWDOWN_MARKET_BEAT_THRESHOLD and long_excess_market <= 0.0)
        live_gate_passed = bool(all_periods_available and positive_return_all_periods and positive_sharpe_all_periods and drawdown_gate and market_beat_gate)
        score = min_sharpe + 0.50 * median_sharpe + 4.0 * median_excess_market + 0.50 * worst_drawdown - 0.03 * median_turnover
        first = frame.iloc[0]
        rows.append(
            {
                "Candidate": candidate,
                "Display Name": first.get("Display Name"),
                "Candidate Group": first.get("Candidate Group"),
                "Rule": first.get("Rule"),
                "Benchmark Symbol": first.get("Benchmark Symbol"),
                "min_sharpe": min_sharpe,
                "median_sharpe": median_sharpe,
                "median_excess_cagr_vs_market": median_excess_market,
                "long_excess_cagr_vs_market": long_excess_market,
                "worst_drawdown": worst_drawdown,
                "median_turnover_per_year": median_turnover,
                "robustness_score": score,
                "live_gate_passed": live_gate_passed,
                "gate_reason": _gate_reason(
                    all_periods_available=all_periods_available,
                    positive_return_all_periods=positive_return_all_periods,
                    positive_sharpe_all_periods=positive_sharpe_all_periods,
                    drawdown_gate=drawdown_gate,
                    market_beat_gate=market_beat_gate,
                ),
                "Notes": first.get("Notes"),
            }
        )
    ranking = pd.DataFrame(rows).sort_values(["live_gate_passed", "robustness_score"], ascending=[False, False]).reset_index(drop=True)
    ranking.insert(0, "rank", range(1, len(ranking) + 1))
    ranking["new_strategy_rank"] = pd.NA
    new_strategy_mask = ranking["Candidate Group"].eq("leveraged_supplement")
    ranking.loc[new_strategy_mask, "new_strategy_rank"] = range(1, int(new_strategy_mask.sum()) + 1)
    ranking["replacement_review_candidate"] = ranking["live_gate_passed"] & ranking["Candidate Group"].eq("optimization_variant")
    ranking["supplemental_review_candidate"] = ranking["live_gate_passed"] & ranking["Candidate Group"].eq("leveraged_supplement")
    ranking["review_action"] = "reject"
    ranking.loc[ranking["Candidate Group"].eq("current_live_proxy"), "review_action"] = "current_live_proxy"
    ranking.loc[ranking["replacement_review_candidate"], "review_action"] = "replacement_review_candidate"
    ranking.loc[ranking["supplemental_review_candidate"], "review_action"] = "supplemental_review_candidate"
    return ranking


def _gate_reason(
    *,
    all_periods_available: bool,
    positive_return_all_periods: bool,
    positive_sharpe_all_periods: bool,
    drawdown_gate: bool,
    market_beat_gate: bool,
) -> str:
    reasons: list[str] = []
    if not all_periods_available:
        reasons.append("missing_or_too_short_period")
    if not positive_return_all_periods:
        reasons.append("non_positive_cagr_period")
    if not positive_sharpe_all_periods:
        reasons.append("non_positive_sharpe_period")
    if not drawdown_gate:
        reasons.append("drawdown_below_minus_45pct")
    if not market_beat_gate:
        reasons.append("drawdown_near_30_without_market_outperformance")
    return "pass" if not reasons else ";".join(reasons)


def run_candidate_research(
    *,
    price_history: pd.DataFrame,
    periods: Sequence[tuple[str, str, str | None]] = DEFAULT_PERIODS,
    candidates: Sequence[LeveragedCandidateSpec] = LEVERAGED_CANDIDATES,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
) -> dict[str, pd.DataFrame]:
    full_start = _period_start(periods)
    full_end = _period_end(periods)
    rows: list[dict[str, object]] = []
    returns_by_candidate: dict[str, pd.Series] = {}
    for spec in candidates:
        result = run_candidate_backtest(
            price_history,
            spec,
            start_date=full_start,
            end_date=full_end,
            turnover_cost_bps=turnover_cost_bps,
        )
        returns_by_candidate[spec.candidate_id] = pd.Series(result["portfolio_returns"], name=spec.candidate_id)
        for period_name, start_date, end_date in periods:
            rows.append(
                _period_summary_from_result(
                    result,
                    period_name=period_name,
                    start_date=start_date,
                    end_date=end_date,
                )
            )
    period_summary = pd.DataFrame(rows)
    ranking = build_ranking(period_summary)
    portfolio_returns = pd.concat(returns_by_candidate.values(), axis=1) if returns_by_candidate else pd.DataFrame()
    return {"period_summary": period_summary, "ranking": ranking, "portfolio_returns": portfolio_returns}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backtest current TQQQ/SOXL proxies, their optimization variants, and separately ranked new leveraged supplements."
    )
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--prices", help="Existing long price-history CSV with symbol/as_of/close columns")
    input_group.add_argument("--download", action="store_true", help="Download adjusted price history through yfinance")
    parser.add_argument("--price-start", default="2010-01-01")
    parser.add_argument("--price-end")
    parser.add_argument("--periods", default=",".join(f"{name}:{start}:{end or ''}" for name, start, end in DEFAULT_PERIODS))
    parser.add_argument("--turnover-cost-bps", type=float, default=DEFAULT_TURNOVER_COST_BPS)
    parser.add_argument("--output-dir", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    periods = _parse_periods(args.periods)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    if args.download:
        prices = download_price_history(list(collect_required_symbols()), start=args.price_start, end=args.price_end)
        prices.to_csv(output_dir / "downloaded_leveraged_price_history.csv", index=False)
    else:
        prices = read_table(args.prices)
    result = run_candidate_research(
        price_history=prices,
        periods=periods,
        turnover_cost_bps=float(args.turnover_cost_bps),
    )
    result["period_summary"].to_csv(output_dir / "period_summary.csv", index=False)
    result["ranking"].to_csv(output_dir / "ranking.csv", index=False)
    result["portfolio_returns"].to_csv(output_dir / "portfolio_returns.csv")
    print(result["ranking"].to_string(index=False))
    print(f"wrote leveraged strategy research outputs -> {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
