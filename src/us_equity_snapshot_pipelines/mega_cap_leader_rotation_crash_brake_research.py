from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Iterable

import pandas as pd

from .mega_cap_leader_rotation_backtest import (
    BENCHMARK_SYMBOL,
    BROAD_BENCHMARK_SYMBOL,
    SAFE_HAVEN,
    _build_close_and_returns,
    _normalize_price_history,
    run_backtest,
)
from .mega_cap_leader_rotation_concentration_variants import (
    DAILY_RETURN_COLUMNS,
    TRADE_COLUMNS,
    _align_weights,
    _build_daily_return_rows,
    _build_rebalance_trade_rows,
    _build_rolling_rows,
    _build_yearly_rows,
    _returns_from_weights,
    _summary_for_variant,
)
from .mega_cap_leader_rotation_dynamic_validation import (
    DEFAULT_ROLLING_WINDOW_YEARS,
    lag_universe_history,
    parse_csv_ints,
)
from .russell_1000_multi_factor_defensive_snapshot import read_table

DEFAULT_BASELINE_TOP2_WEIGHT = 0.50
DEFAULT_FLOOR_TOP2_WEIGHT = 0.25
DEFAULT_DRAWDOWN_THRESHOLD = 0.08
DEFAULT_SMA_WINDOW = 200
DEFAULT_DRAWDOWN_WINDOW = 63
DEFAULT_REBOUND_WINDOW = 21
DEFAULT_VOL_WINDOW = 63
DEFAULT_VOL_MEDIAN_WINDOW = 252
CRASH_BRAKE_RESEARCH_SCHEMA_VERSION = "russell_top50_crash_brake_research.v1"
DEFAULT_CRASH_BRAKE_EXPERIMENT_PROFILE = "panic_rebound_top2_sleeve_floor_v1"
DEFAULT_CRASH_BRAKE_CANDIDATE_RUNS = ("crash_brake_top2_50_floor25",)
SUMMARY_COLUMNS = (
    "Run",
    "Variant Type",
    "Universe Lag Trading Days",
    "Baseline Top2 Weight",
    "Top2 Floor Weight",
    "Panic Brake Mode Share",
    "Start",
    "End",
    "CAGR",
    "Max Drawdown",
    "Volatility",
    "Sharpe",
    "Calmar",
    "Total Return",
    "Final Equity",
    "Rebalances/Year",
    "Turnover/Year",
    "Avg Stock Exposure",
    "Benchmark Symbol",
    "Benchmark Total Return",
    "Benchmark Corr",
    "Broad Benchmark Symbol",
    "Broad Benchmark Total Return",
    "Equal Weight Pool Total Return",
)
MODE_COLUMNS = (
    "Signal Date",
    "Effective Date",
    "Mode",
    "Top2 Weight",
    "QQQ SMA200 Gap",
    "QQQ 63D Drawdown",
    "QQQ 21D Return",
    "QQQ 63D Vol",
    "QQQ 63D Vol Median 252D",
)


def parse_csv_floats_no_percent(raw_value: str | Iterable[float] | None, *, default: tuple[float, ...]) -> tuple[float, ...]:
    if raw_value is None:
        return default
    values = raw_value.split(",") if isinstance(raw_value, str) else list(raw_value)
    parsed: list[float] = []
    seen: set[float] = set()
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        number = float(text)
        if number in seen:
            continue
        seen.add(number)
        parsed.append(number)
    return tuple(parsed) or default


def _asof(series: pd.Series, date) -> float:
    value = series.asof(pd.Timestamp(date).tz_localize(None).normalize())
    return float(value) if pd.notna(value) else float("nan")


def _is_panic_rebound_state(
    *,
    sma_gap: float,
    drawdown: float,
    rebound_return: float,
    realized_vol: float,
    realized_vol_median: float,
    drawdown_threshold: float,
) -> bool:
    if any(pd.isna(value) for value in (sma_gap, drawdown, rebound_return, realized_vol, realized_vol_median)):
        return False
    return bool(
        sma_gap < 0.0
        and drawdown <= -float(drawdown_threshold)
        and (rebound_return > 0.0 or realized_vol > realized_vol_median)
    )


def build_panic_brake_mode_history(
    index: pd.DatetimeIndex,
    *,
    benchmark_close: pd.Series,
    benchmark_returns: pd.Series,
    exposure_history: pd.DataFrame,
    baseline_top2_weight: float = DEFAULT_BASELINE_TOP2_WEIGHT,
    floor_top2_weight: float = DEFAULT_FLOOR_TOP2_WEIGHT,
    drawdown_threshold: float = DEFAULT_DRAWDOWN_THRESHOLD,
    sma_window: int = DEFAULT_SMA_WINDOW,
    drawdown_window: int = DEFAULT_DRAWDOWN_WINDOW,
    rebound_window: int = DEFAULT_REBOUND_WINDOW,
    vol_window: int = DEFAULT_VOL_WINDOW,
    vol_median_window: int = DEFAULT_VOL_MEDIAN_WINDOW,
) -> tuple[pd.Series, pd.DataFrame]:
    trading_index = pd.DatetimeIndex(index).tz_localize(None)
    close = pd.Series(benchmark_close, dtype=float).reindex(trading_index).ffill()
    returns = pd.Series(benchmark_returns, dtype=float).reindex(trading_index).fillna(0.0)
    sma_gap = close / close.rolling(int(sma_window)).mean() - 1.0
    rolling_high = close.rolling(int(drawdown_window)).max()
    drawdown = close / rolling_high - 1.0
    rebound_return = close / close.shift(int(rebound_window)) - 1.0
    realized_vol = returns.rolling(int(vol_window)).std(ddof=0) * math.sqrt(252.0)
    realized_vol_median = realized_vol.rolling(int(vol_median_window), min_periods=int(vol_window)).median()

    top2_weights = pd.Series(float(baseline_top2_weight), index=trading_index, name="top2_weight")
    rows: list[dict[str, object]] = []
    exposure = pd.DataFrame(exposure_history).copy()
    if exposure.empty:
        return top2_weights, pd.DataFrame(columns=MODE_COLUMNS)
    exposure["signal_date"] = pd.to_datetime(exposure["signal_date"], errors="coerce").dt.tz_localize(None)
    exposure["effective_date"] = pd.to_datetime(exposure["effective_date"], errors="coerce").dt.tz_localize(None)
    exposure = exposure.dropna(subset=["signal_date", "effective_date"]).sort_values("effective_date")

    for row in exposure.itertuples(index=False):
        signal_date = pd.Timestamp(getattr(row, "signal_date")).normalize()
        effective_date = pd.Timestamp(getattr(row, "effective_date")).normalize()
        signal_sma_gap = _asof(sma_gap, signal_date)
        signal_drawdown = _asof(drawdown, signal_date)
        signal_rebound = _asof(rebound_return, signal_date)
        signal_vol = _asof(realized_vol, signal_date)
        signal_vol_median = _asof(realized_vol_median, signal_date)
        use_floor = _is_panic_rebound_state(
            sma_gap=signal_sma_gap,
            drawdown=signal_drawdown,
            rebound_return=signal_rebound,
            realized_vol=signal_vol,
            realized_vol_median=signal_vol_median,
            drawdown_threshold=float(drawdown_threshold),
        )
        top2_weight = float(floor_top2_weight if use_floor else baseline_top2_weight)
        top2_weights.loc[top2_weights.index >= effective_date] = top2_weight
        rows.append(
            {
                "Signal Date": signal_date.date().isoformat(),
                "Effective Date": effective_date.date().isoformat(),
                "Mode": "floor" if use_floor else "baseline",
                "Top2 Weight": top2_weight,
                "QQQ SMA200 Gap": signal_sma_gap,
                "QQQ 63D Drawdown": signal_drawdown,
                "QQQ 21D Return": signal_rebound,
                "QQQ 63D Vol": signal_vol,
                "QQQ 63D Vol Median 252D": signal_vol_median,
            }
        )
    return top2_weights, pd.DataFrame(rows, columns=MODE_COLUMNS)


def _ordered_columns(frame: pd.DataFrame, columns: tuple[str, ...]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=columns)
    known = [column for column in columns if column in frame.columns]
    extra = [column for column in frame.columns if column not in known]
    return frame.loc[:, known + extra]


def _blend_weights(top2_weights: pd.DataFrame, top4_weights: pd.DataFrame, top2_weight) -> pd.DataFrame:
    if isinstance(top2_weight, pd.Series):
        sleeve = pd.Series(top2_weight, dtype=float).reindex(top2_weights.index).ffill().fillna(0.0)
        return top2_weights.mul(sleeve, axis=0) + top4_weights.mul(1.0 - sleeve, axis=0)
    value = float(top2_weight)
    return value * top2_weights + (1.0 - value) * top4_weights


def run_crash_brake_research(
    price_history,
    universe_history,
    *,
    start_date: str | None = "2017-10-02",
    end_date: str | None = None,
    universe_lag_trading_days: int = 21,
    rolling_window_years: Iterable[int] = DEFAULT_ROLLING_WINDOW_YEARS,
    benchmark_symbol: str = BENCHMARK_SYMBOL,
    broad_benchmark_symbol: str = BROAD_BENCHMARK_SYMBOL,
    safe_haven: str = SAFE_HAVEN,
    baseline_top2_weight: float = DEFAULT_BASELINE_TOP2_WEIGHT,
    floor_top2_weight: float = DEFAULT_FLOOR_TOP2_WEIGHT,
    drawdown_threshold: float = DEFAULT_DRAWDOWN_THRESHOLD,
    turnover_cost_bps: float = 5.0,
    min_price_usd: float = 10.0,
    min_adv20_usd: float = 20_000_000.0,
    min_history_days: int = 273,
) -> dict[str, pd.DataFrame]:
    prices = _normalize_price_history(price_history)
    if end_date is not None:
        prices = prices.loc[prices["as_of"] <= pd.Timestamp(end_date).normalize()].copy()
    close_matrix, returns_matrix = _build_close_and_returns(prices)
    trading_index = pd.DatetimeIndex(close_matrix.index)
    lagged_universe = lag_universe_history(
        universe_history,
        lag_trading_days=int(universe_lag_trading_days),
        trading_index=trading_index,
    )
    base_kwargs = {
        "start_date": start_date,
        "end_date": end_date,
        "pool_name": f"dynamic_top50_lag{int(universe_lag_trading_days)}",
        "benchmark_symbol": benchmark_symbol,
        "broad_benchmark_symbol": broad_benchmark_symbol,
        "safe_haven": safe_haven,
        "risk_on_exposure": 1.0,
        "soft_defense_exposure": 1.0,
        "hard_defense_exposure": 1.0,
        "turnover_cost_bps": float(turnover_cost_bps),
        "min_price_usd": float(min_price_usd),
        "min_adv20_usd": float(min_adv20_usd),
        "min_history_days": int(min_history_days),
    }
    top2 = run_backtest(prices, lagged_universe, top_n=2, single_name_cap=0.50, max_names_per_sector=0, **base_kwargs)
    top4 = run_backtest(prices, lagged_universe, top_n=4, single_name_cap=0.25, max_names_per_sector=0, **base_kwargs)

    index = pd.DatetimeIndex(top2["portfolio_returns"].index)
    reference_returns = top2["reference_returns"].reindex(index)
    weight_columns = sorted(
        set(top2["weights_history"].columns) | set(top4["weights_history"].columns) | set(returns_matrix.columns)
    )
    aligned_returns = returns_matrix.reindex(index).reindex(columns=weight_columns, fill_value=0.0).fillna(0.0)
    top2_weights = _align_weights(top2["weights_history"], index=index, columns=weight_columns)
    top4_weights = _align_weights(top4["weights_history"], index=index, columns=weight_columns)
    panic_top2_weight, mode_history = build_panic_brake_mode_history(
        index,
        benchmark_close=close_matrix[benchmark_symbol],
        benchmark_returns=reference_returns[benchmark_symbol],
        exposure_history=top2["exposure_history"],
        baseline_top2_weight=float(baseline_top2_weight),
        floor_top2_weight=float(floor_top2_weight),
        drawdown_threshold=float(drawdown_threshold),
    )

    variants: list[tuple[str, str, pd.DataFrame, float | None, float | None]] = [
        (
            "blend_top2_50_top4_50_no_brake",
            "fixed_blend_no_brake",
            _blend_weights(top2_weights, top4_weights, float(baseline_top2_weight)),
            float(baseline_top2_weight),
            None,
        ),
        (
            "crash_brake_top2_50_floor25",
            "panic_rebound_top2_sleeve_floor",
            _blend_weights(top2_weights, top4_weights, panic_top2_weight),
            float(baseline_top2_weight),
            float(floor_top2_weight),
        ),
        (
            "blend_top2_25_top4_75_no_brake",
            "fixed_blend_no_brake",
            _blend_weights(top2_weights, top4_weights, float(floor_top2_weight)),
            float(floor_top2_weight),
            None,
        ),
    ]

    summary_rows: list[dict[str, object]] = []
    yearly_rows: list[dict[str, object]] = []
    rolling_rows: list[dict[str, object]] = []
    daily_return_rows: list[dict[str, object]] = []
    trade_rows: list[dict[str, object]] = []
    rolling_values = parse_csv_ints(tuple(rolling_window_years), default=DEFAULT_ROLLING_WINDOW_YEARS)
    mode_share = float(mode_history["Mode"].eq("floor").mean()) if not mode_history.empty else 0.0
    for run_name, variant_type, weights, top2_weight, floor_weight in variants:
        returns = _returns_from_weights(weights, aligned_returns, turnover_cost_bps=float(turnover_cost_bps))
        summary = _summary_for_variant(
            run_name=run_name,
            variant_type=variant_type,
            weights=weights,
            portfolio_returns=returns,
            reference_returns=reference_returns,
            benchmark_symbol=benchmark_symbol,
            broad_benchmark_symbol=broad_benchmark_symbol,
            safe_haven=safe_haven,
            universe_lag_trading_days=int(universe_lag_trading_days),
            top2_blend_weight=top2_weight,
            top4_mode_share=1.0 - float(top2_weight or 0.0),
        )
        summary.update(
            {
                "Baseline Top2 Weight": float(top2_weight or 0.0),
                "Top2 Floor Weight": floor_weight,
                "Panic Brake Mode Share": mode_share if floor_weight is not None else 0.0,
            }
        )
        summary_rows.append(summary)
        trade_rows.extend(
            _build_rebalance_trade_rows(
                run_name=run_name,
                variant_type=variant_type,
                weights=weights,
            )
        )
        daily_return_rows.extend(
            _build_daily_return_rows(
                run_name=run_name,
                variant_type=variant_type,
                portfolio_returns=returns,
                reference_returns=reference_returns,
                benchmark_symbol=benchmark_symbol,
                broad_benchmark_symbol=broad_benchmark_symbol,
            )
        )
        yearly_rows.extend(
            _build_yearly_rows(
                run_name=run_name,
                variant_type=variant_type,
                portfolio_returns=returns,
                reference_returns=reference_returns,
                benchmark_symbol=benchmark_symbol,
                broad_benchmark_symbol=broad_benchmark_symbol,
            )
        )
        rolling_rows.extend(
            _build_rolling_rows(
                run_name=run_name,
                variant_type=variant_type,
                portfolio_returns=returns,
                reference_returns=reference_returns,
                benchmark_symbol=benchmark_symbol,
                broad_benchmark_symbol=broad_benchmark_symbol,
                rolling_window_years=rolling_values,
            )
        )

    return {
        "crash_brake_summary": _ordered_columns(pd.DataFrame(summary_rows), SUMMARY_COLUMNS),
        "crash_brake_yearly_summary": pd.DataFrame(yearly_rows),
        "crash_brake_rolling_summary": pd.DataFrame(rolling_rows),
        "crash_brake_mode_history": mode_history,
        "crash_brake_daily_returns": pd.DataFrame(daily_return_rows, columns=DAILY_RETURN_COLUMNS),
        "crash_brake_rebalance_trades": pd.DataFrame(trade_rows, columns=TRADE_COLUMNS),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Research a narrow panic/rebound brake for the Russell Top50 Top2 sleeve."
    )
    parser.add_argument("--prices", required=True, help="Input price history file")
    parser.add_argument("--universe", required=True, help="Input dynamic universe history file")
    parser.add_argument("--output-dir", required=True, help="Directory for crash-brake research outputs")
    parser.add_argument("--start", dest="start_date", default="2017-10-02")
    parser.add_argument("--end", dest="end_date")
    parser.add_argument("--universe-lag-days", type=int, default=21)
    parser.add_argument("--rolling-window-years", default="", help="Comma-separated complete-calendar rolling windows")
    parser.add_argument("--baseline-top2-weight", type=float, default=DEFAULT_BASELINE_TOP2_WEIGHT)
    parser.add_argument("--floor-top2-weight", type=float, default=DEFAULT_FLOOR_TOP2_WEIGHT)
    parser.add_argument("--drawdown-threshold", type=float, default=DEFAULT_DRAWDOWN_THRESHOLD)
    parser.add_argument("--benchmark-symbol", default=BENCHMARK_SYMBOL)
    parser.add_argument("--broad-benchmark-symbol", default=BROAD_BENCHMARK_SYMBOL)
    parser.add_argument("--safe-haven", default=SAFE_HAVEN)
    parser.add_argument("--turnover-cost-bps", type=float, default=5.0)
    parser.add_argument("--min-price-usd", type=float, default=10.0)
    parser.add_argument("--min-adv20-usd", type=float, default=20_000_000.0)
    parser.add_argument("--min-history-days", type=int, default=273)
    parser.add_argument("--print-top", type=int, default=10)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    result = run_crash_brake_research(
        read_table(args.prices),
        read_table(args.universe),
        start_date=args.start_date,
        end_date=args.end_date,
        universe_lag_trading_days=int(args.universe_lag_days),
        rolling_window_years=parse_csv_ints(args.rolling_window_years, default=DEFAULT_ROLLING_WINDOW_YEARS),
        benchmark_symbol=args.benchmark_symbol,
        broad_benchmark_symbol=args.broad_benchmark_symbol,
        safe_haven=args.safe_haven,
        baseline_top2_weight=args.baseline_top2_weight,
        floor_top2_weight=args.floor_top2_weight,
        drawdown_threshold=args.drawdown_threshold,
        turnover_cost_bps=args.turnover_cost_bps,
        min_price_usd=args.min_price_usd,
        min_adv20_usd=args.min_adv20_usd,
        min_history_days=args.min_history_days,
    )
    summary_path = output_dir / "crash_brake_summary.csv"
    yearly_path = output_dir / "crash_brake_yearly_summary.csv"
    rolling_path = output_dir / "crash_brake_rolling_summary.csv"
    mode_path = output_dir / "crash_brake_mode_history.csv"
    trades_path = output_dir / "crash_brake_rebalance_trades.csv"
    returns_path = output_dir / "crash_brake_daily_returns.csv"
    result["crash_brake_summary"].to_csv(summary_path, index=False)
    result["crash_brake_yearly_summary"].to_csv(yearly_path, index=False)
    result["crash_brake_rolling_summary"].to_csv(rolling_path, index=False)
    result["crash_brake_mode_history"].to_csv(mode_path, index=False)
    result["crash_brake_rebalance_trades"].to_csv(trades_path, index=False)
    result["crash_brake_daily_returns"].to_csv(returns_path, index=False)
    candidate_runs = tuple(
        str(row.get("Run", "")).strip()
        for row in result["crash_brake_summary"].to_dict(orient="records")
        if str(row.get("Run", "")).strip()
    )
    manifest = {
        "manifest_type": "russell_top50_crash_brake_research",
        "artifact_schema_version": CRASH_BRAKE_RESEARCH_SCHEMA_VERSION,
        "experiment_profile": DEFAULT_CRASH_BRAKE_EXPERIMENT_PROFILE,
        "candidate_runs": list(candidate_runs),
        "inputs": {
            "prices": str(args.prices),
            "universe": str(args.universe),
        },
        "artifacts": {
            "crash_brake_summary": {"path": summary_path.name},
            "crash_brake_yearly_summary": {"path": yearly_path.name},
            "crash_brake_rolling_summary": {"path": rolling_path.name},
            "crash_brake_mode_history": {"path": mode_path.name},
            "crash_brake_rebalance_trades": {"path": trades_path.name},
            "crash_brake_daily_returns": {"path": returns_path.name},
        },
        "outputs": [
            summary_path.name,
            yearly_path.name,
            rolling_path.name,
            mode_path.name,
            trades_path.name,
            returns_path.name,
            "crash_brake_research_manifest.json",
        ],
    }
    manifest_path = output_dir / "crash_brake_research_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(result["crash_brake_summary"].head(max(int(args.print_top), 0)).to_string(index=False))
    print(f"wrote crash-brake summary -> {summary_path}")
    print(f"wrote crash-brake yearly summary -> {yearly_path}")
    print(f"wrote crash-brake rolling summary -> {rolling_path}")
    print(f"wrote crash-brake mode history -> {mode_path}")
    print(f"wrote crash-brake rebalance trades -> {trades_path}")
    print(f"wrote crash-brake daily returns -> {returns_path}")
    print(f"wrote crash-brake research manifest -> {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
