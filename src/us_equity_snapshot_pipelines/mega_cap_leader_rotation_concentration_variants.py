from __future__ import annotations

import argparse
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
    summarize_returns,
)
from .mega_cap_leader_rotation_dynamic_validation import (
    DEFAULT_ROLLING_WINDOW_YEARS,
    _complete_calendar_years,
    _period_cagr,
    _period_max_drawdown,
    _period_return,
    lag_universe_history,
    parse_csv_ints,
)
from .russell_1000_multi_factor_defensive_snapshot import read_table

DEFAULT_BLEND_TOP2_WEIGHTS = (0.25, 0.50, 0.75)
DEFAULT_DYNAMIC_DRAWDOWN_THRESHOLDS = (0.08, 0.10, 0.12)
SUMMARY_COLUMNS = (
    "Run",
    "Variant Type",
    "Universe Lag Trading Days",
    "Top2 Blend Weight",
    "Top4 Blend Weight",
    "Dynamic Drawdown Threshold",
    "Top4 Mode Share",
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
YEARLY_COLUMNS = (
    "Run",
    "Variant Type",
    "Year",
    "Strategy Return",
    "Strategy Max Drawdown",
    "QQQ Return",
    "QQQ Max Drawdown",
    "SPY Return",
    "SPY Max Drawdown",
)
ROLLING_COLUMNS = (
    "Run",
    "Variant Type",
    "Window Years",
    "Window Start Year",
    "Window End Year",
    "Strategy Return",
    "Strategy CAGR",
    "Strategy Max Drawdown",
    "QQQ Return",
    "QQQ CAGR",
    "QQQ Max Drawdown",
    "SPY Return",
    "SPY CAGR",
    "SPY Max Drawdown",
)
MODE_COLUMNS = (
    "Run",
    "Variant Type",
    "Signal Date",
    "Effective Date",
    "Top2 Shadow Drawdown",
    "Dynamic Drawdown Threshold",
    "Mode",
)


def parse_csv_floats(raw_value: str | Iterable[float] | None, *, default: tuple[float, ...]) -> tuple[float, ...]:
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
        if number > 1.0:
            number = number / 100.0
        if number in seen:
            continue
        seen.add(number)
        parsed.append(number)
    return tuple(parsed) or default


def _trading_index(price_history: pd.DataFrame) -> pd.DatetimeIndex:
    prices = _normalize_price_history(price_history)
    index = pd.DatetimeIndex(sorted(prices["as_of"].dropna().unique()))
    if index.empty:
        raise ValueError("price_history has no usable trading dates")
    return index


def _variant_name_for_blend(top2_weight: float) -> str:
    top2_label = int(round(float(top2_weight) * 100))
    top4_label = int(round((1.0 - float(top2_weight)) * 100))
    return f"blend_top2_{top2_label}_top4_{top4_label}"


def _variant_name_for_drawdown(threshold: float) -> str:
    return f"dynamic_top2_dd{int(round(float(threshold) * 100))}_to_top4"


def _align_weights(
    weights_history: pd.DataFrame,
    *,
    index: pd.DatetimeIndex,
    columns: Iterable[str],
) -> pd.DataFrame:
    return weights_history.reindex(index).fillna(0.0).reindex(columns=columns, fill_value=0.0)


def _returns_from_weights(
    weights_history: pd.DataFrame,
    returns_matrix: pd.DataFrame,
    *,
    turnover_cost_bps: float,
) -> pd.Series:
    weights = weights_history.fillna(0.0)
    returns = returns_matrix.reindex(weights.index).reindex(columns=weights.columns, fill_value=0.0).fillna(0.0)
    gross_returns = (weights.shift(1).fillna(0.0) * returns).sum(axis=1)
    turnover = 0.5 * weights.diff().abs().sum(axis=1).shift(1).fillna(0.0)
    return gross_returns - turnover * (float(turnover_cost_bps) / 10_000.0)


def _build_yearly_rows(
    *,
    run_name: str,
    variant_type: str,
    portfolio_returns: pd.Series,
    reference_returns: pd.DataFrame,
    benchmark_symbol: str,
    broad_benchmark_symbol: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for year in sorted(int(year) for year in pd.DatetimeIndex(portfolio_returns.index).year.unique()):
        mask = pd.DatetimeIndex(portfolio_returns.index).year == year
        strategy_returns = portfolio_returns.loc[mask]
        reference_slice = reference_returns.reindex(strategy_returns.index)
        benchmark_returns = (
            reference_slice[benchmark_symbol]
            if benchmark_symbol in reference_slice.columns
            else pd.Series(index=strategy_returns.index, dtype=float)
        )
        broad_returns = (
            reference_slice[broad_benchmark_symbol]
            if broad_benchmark_symbol in reference_slice.columns
            else pd.Series(index=strategy_returns.index, dtype=float)
        )
        rows.append(
            {
                "Run": run_name,
                "Variant Type": variant_type,
                "Year": int(year),
                "Strategy Return": _period_return(strategy_returns),
                "Strategy Max Drawdown": _period_max_drawdown(strategy_returns),
                "QQQ Return": _period_return(benchmark_returns),
                "QQQ Max Drawdown": _period_max_drawdown(benchmark_returns),
                "SPY Return": _period_return(broad_returns),
                "SPY Max Drawdown": _period_max_drawdown(broad_returns),
            }
        )
    return rows


def _build_rolling_rows(
    *,
    run_name: str,
    variant_type: str,
    portfolio_returns: pd.Series,
    reference_returns: pd.DataFrame,
    benchmark_symbol: str,
    broad_benchmark_symbol: str,
    rolling_window_years: Iterable[int],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    years = _complete_calendar_years(pd.DatetimeIndex(portfolio_returns.index))
    if not years:
        return rows
    reference = reference_returns.reindex(portfolio_returns.index)
    for window_years in rolling_window_years:
        window = int(window_years)
        if window <= 0:
            continue
        for start_year in years:
            end_year = start_year + window - 1
            if end_year not in years:
                continue
            mask = (pd.DatetimeIndex(portfolio_returns.index).year >= start_year) & (
                pd.DatetimeIndex(portfolio_returns.index).year <= end_year
            )
            strategy_returns = portfolio_returns.loc[mask]
            reference_slice = reference.loc[strategy_returns.index]
            benchmark_returns = (
                reference_slice[benchmark_symbol]
                if benchmark_symbol in reference_slice.columns
                else pd.Series(index=strategy_returns.index, dtype=float)
            )
            broad_returns = (
                reference_slice[broad_benchmark_symbol]
                if broad_benchmark_symbol in reference_slice.columns
                else pd.Series(index=strategy_returns.index, dtype=float)
            )
            rows.append(
                {
                    "Run": run_name,
                    "Variant Type": variant_type,
                    "Window Years": int(window),
                    "Window Start Year": int(start_year),
                    "Window End Year": int(end_year),
                    "Strategy Return": _period_return(strategy_returns),
                    "Strategy CAGR": _period_cagr(strategy_returns),
                    "Strategy Max Drawdown": _period_max_drawdown(strategy_returns),
                    "QQQ Return": _period_return(benchmark_returns),
                    "QQQ CAGR": _period_cagr(benchmark_returns),
                    "QQQ Max Drawdown": _period_max_drawdown(benchmark_returns),
                    "SPY Return": _period_return(broad_returns),
                    "SPY CAGR": _period_cagr(broad_returns),
                    "SPY Max Drawdown": _period_max_drawdown(broad_returns),
                }
            )
    return rows


def _build_dynamic_weights(
    *,
    run_name: str,
    threshold: float,
    index: pd.DatetimeIndex,
    top2_weights: pd.DataFrame,
    top4_weights: pd.DataFrame,
    top2_returns: pd.Series,
    top2_exposure_history: pd.DataFrame,
) -> tuple[pd.DataFrame, list[dict[str, object]], float]:
    top2_equity = (1.0 + top2_returns.reindex(index).fillna(0.0)).cumprod()
    top2_drawdown = top2_equity / top2_equity.cummax() - 1.0
    mode = pd.Series("top2", index=index, dtype=object)
    mode_rows: list[dict[str, object]] = []
    exposure = top2_exposure_history.copy()
    if exposure.empty:
        return top2_weights.copy(), mode_rows, 0.0
    exposure["signal_date"] = pd.to_datetime(exposure["signal_date"], errors="coerce").dt.tz_localize(None)
    exposure["effective_date"] = pd.to_datetime(exposure["effective_date"], errors="coerce").dt.tz_localize(None)
    exposure = exposure.dropna(subset=["signal_date", "effective_date"]).sort_values("effective_date")

    for row in exposure.itertuples(index=False):
        signal_date = pd.Timestamp(getattr(row, "signal_date")).normalize()
        effective_date = pd.Timestamp(getattr(row, "effective_date")).normalize()
        drawdown_value = top2_drawdown.asof(signal_date)
        use_top4 = bool(pd.notna(drawdown_value) and float(drawdown_value) <= -float(threshold))
        selected_mode = "top4" if use_top4 else "top2"
        mode.loc[mode.index >= effective_date] = selected_mode
        mode_rows.append(
            {
                "Run": run_name,
                "Variant Type": "dynamic_top2_drawdown_to_top4",
                "Signal Date": signal_date.date().isoformat(),
                "Effective Date": effective_date.date().isoformat(),
                "Top2 Shadow Drawdown": float(drawdown_value) if pd.notna(drawdown_value) else float("nan"),
                "Dynamic Drawdown Threshold": float(threshold),
                "Mode": selected_mode,
            }
        )
    top4_share = float(mode.eq("top4").mean())
    return top2_weights.where(mode.eq("top2"), top4_weights), mode_rows, top4_share


def _summary_for_variant(
    *,
    run_name: str,
    variant_type: str,
    weights: pd.DataFrame,
    portfolio_returns: pd.Series,
    reference_returns: pd.DataFrame,
    benchmark_symbol: str,
    broad_benchmark_symbol: str,
    safe_haven: str,
    universe_lag_trading_days: int,
    top2_blend_weight: float | None = None,
    dynamic_drawdown_threshold: float | None = None,
    top4_mode_share: float | None = None,
) -> dict[str, object]:
    equal_weight_column = next(
        (column for column in reference_returns.columns if str(column).startswith("equal_weight_")),
        None,
    )
    summary = dict(
        summarize_returns(
            portfolio_returns,
            weights_history=weights,
            benchmark_returns=(
                reference_returns[benchmark_symbol]
                if benchmark_symbol in reference_returns.columns
                else None
            ),
            broad_benchmark_returns=(
                reference_returns[broad_benchmark_symbol]
                if broad_benchmark_symbol in reference_returns.columns
                else None
            ),
            equal_weight_pool_returns=reference_returns[equal_weight_column] if equal_weight_column else None,
            pool_name="dynamic_top50_concentration",
            benchmark_symbol=benchmark_symbol,
            broad_benchmark_symbol=broad_benchmark_symbol,
            safe_haven=safe_haven,
        )
    )
    summary.update(
        {
            "Run": run_name,
            "Variant Type": variant_type,
            "Universe Lag Trading Days": int(universe_lag_trading_days),
            "Top2 Blend Weight": top2_blend_weight,
            "Top4 Blend Weight": (1.0 - top2_blend_weight) if top2_blend_weight is not None else None,
            "Dynamic Drawdown Threshold": dynamic_drawdown_threshold,
            "Top4 Mode Share": top4_mode_share,
        }
    )
    return summary


def run_concentration_variant_research(
    price_history,
    universe_history,
    *,
    start_date: str | None = "2017-10-02",
    end_date: str | None = None,
    universe_lag_trading_days: int = 21,
    blend_top2_weights: Iterable[float] = DEFAULT_BLEND_TOP2_WEIGHTS,
    dynamic_drawdown_thresholds: Iterable[float] = DEFAULT_DYNAMIC_DRAWDOWN_THRESHOLDS,
    rolling_window_years: Iterable[int] = DEFAULT_ROLLING_WINDOW_YEARS,
    benchmark_symbol: str = BENCHMARK_SYMBOL,
    broad_benchmark_symbol: str = BROAD_BENCHMARK_SYMBOL,
    safe_haven: str = SAFE_HAVEN,
    turnover_cost_bps: float = 5.0,
    min_price_usd: float = 10.0,
    min_adv20_usd: float = 20_000_000.0,
    min_history_days: int = 273,
) -> dict[str, pd.DataFrame]:
    prices = _normalize_price_history(price_history)
    if end_date is not None:
        prices = prices.loc[prices["as_of"] <= pd.Timestamp(end_date).normalize()].copy()
    trading_index = _trading_index(prices)
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
    top2 = run_backtest(
        prices,
        lagged_universe,
        top_n=2,
        single_name_cap=0.50,
        max_names_per_sector=0,
        **base_kwargs,
    )
    top4 = run_backtest(
        prices,
        lagged_universe,
        top_n=4,
        single_name_cap=0.25,
        max_names_per_sector=0,
        **base_kwargs,
    )

    _close_matrix, returns_matrix = _build_close_and_returns(prices)
    index = pd.DatetimeIndex(top2["portfolio_returns"].index)
    reference_returns = top2["reference_returns"].reindex(index)
    weight_columns = sorted(
        set(top2["weights_history"].columns)
        | set(top4["weights_history"].columns)
        | set(returns_matrix.columns)
    )
    returns_matrix = returns_matrix.reindex(index).reindex(columns=weight_columns, fill_value=0.0).fillna(0.0)
    top2_weights = _align_weights(top2["weights_history"], index=index, columns=weight_columns)
    top4_weights = _align_weights(top4["weights_history"], index=index, columns=weight_columns)

    variants: list[tuple[str, str, pd.DataFrame, float | None, float | None, float | None]] = [
        ("base_top2_cap50", "base_top2", top2_weights, 1.0, None, 0.0),
        ("base_top4_cap25", "base_top4", top4_weights, 0.0, None, 1.0),
    ]
    for top2_weight in parse_csv_floats(tuple(blend_top2_weights), default=DEFAULT_BLEND_TOP2_WEIGHTS):
        if not 0.0 < top2_weight < 1.0:
            continue
        run_name = _variant_name_for_blend(top2_weight)
        weights = float(top2_weight) * top2_weights + (1.0 - float(top2_weight)) * top4_weights
        variants.append((run_name, "fixed_blend", weights, float(top2_weight), None, 1.0 - float(top2_weight)))

    mode_rows: list[dict[str, object]] = []
    for threshold in parse_csv_floats(
        tuple(dynamic_drawdown_thresholds),
        default=DEFAULT_DYNAMIC_DRAWDOWN_THRESHOLDS,
    ):
        if not 0.0 < threshold < 1.0:
            continue
        run_name = _variant_name_for_drawdown(threshold)
        weights, rows, top4_share = _build_dynamic_weights(
            run_name=run_name,
            threshold=float(threshold),
            index=index,
            top2_weights=top2_weights,
            top4_weights=top4_weights,
            top2_returns=top2["portfolio_returns"],
            top2_exposure_history=top2["exposure_history"],
        )
        mode_rows.extend(rows)
        variants.append(
            (
                run_name,
                "dynamic_top2_drawdown_to_top4",
                weights,
                None,
                float(threshold),
                top4_share,
            )
        )

    summary_rows: list[dict[str, object]] = []
    yearly_rows: list[dict[str, object]] = []
    rolling_rows: list[dict[str, object]] = []
    rolling_values = parse_csv_ints(tuple(rolling_window_years), default=DEFAULT_ROLLING_WINDOW_YEARS)
    for run_name, variant_type, weights, blend_weight, threshold, top4_share in variants:
        returns = _returns_from_weights(weights, returns_matrix, turnover_cost_bps=float(turnover_cost_bps))
        summary_rows.append(
            _summary_for_variant(
                run_name=run_name,
                variant_type=variant_type,
                weights=weights,
                portfolio_returns=returns,
                reference_returns=reference_returns,
                benchmark_symbol=benchmark_symbol,
                broad_benchmark_symbol=broad_benchmark_symbol,
                safe_haven=safe_haven,
                universe_lag_trading_days=int(universe_lag_trading_days),
                top2_blend_weight=blend_weight,
                dynamic_drawdown_threshold=threshold,
                top4_mode_share=top4_share,
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

    summary = pd.DataFrame(summary_rows, columns=SUMMARY_COLUMNS)
    yearly = pd.DataFrame(yearly_rows, columns=YEARLY_COLUMNS)
    rolling = pd.DataFrame(rolling_rows, columns=ROLLING_COLUMNS)
    mode_history = pd.DataFrame(mode_rows, columns=MODE_COLUMNS)
    return {
        "concentration_variant_summary": summary,
        "concentration_variant_yearly_summary": yearly,
        "concentration_variant_rolling_summary": rolling,
        "concentration_variant_mode_history": mode_history,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Research derived Top2/Top4 concentration variants for dynamic Russell Top50 leader rotation."
    )
    parser.add_argument("--prices", required=True, help="Input price history file")
    parser.add_argument("--universe", required=True, help="Input dynamic universe history file")
    parser.add_argument("--output-dir", required=True, help="Directory for research outputs")
    parser.add_argument("--start", dest="start_date", default="2017-10-02")
    parser.add_argument("--end", dest="end_date")
    parser.add_argument("--universe-lag-days", type=int, default=21)
    parser.add_argument(
        "--blend-top2-weights",
        default=",".join(str(value) for value in DEFAULT_BLEND_TOP2_WEIGHTS),
        help="Comma-separated Top2 sleeve weights for fixed Top2/Top4 blends",
    )
    parser.add_argument(
        "--dynamic-drawdown-thresholds",
        default=",".join(str(value) for value in DEFAULT_DYNAMIC_DRAWDOWN_THRESHOLDS),
        help="Comma-separated Top2 shadow drawdown thresholds that switch to Top4",
    )
    parser.add_argument(
        "--rolling-window-years",
        default="",
        help="Comma-separated complete-calendar-year rolling windows to summarize, for example 3,5",
    )
    parser.add_argument("--benchmark-symbol", default=BENCHMARK_SYMBOL)
    parser.add_argument("--broad-benchmark-symbol", default=BROAD_BENCHMARK_SYMBOL)
    parser.add_argument("--safe-haven", default=SAFE_HAVEN)
    parser.add_argument("--turnover-cost-bps", type=float, default=5.0)
    parser.add_argument("--min-price-usd", type=float, default=10.0)
    parser.add_argument("--min-adv20-usd", type=float, default=20_000_000.0)
    parser.add_argument("--min-history-days", type=int, default=273)
    parser.add_argument("--print-top", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    result = run_concentration_variant_research(
        read_table(args.prices),
        read_table(args.universe),
        start_date=args.start_date,
        end_date=args.end_date,
        universe_lag_trading_days=int(args.universe_lag_days),
        blend_top2_weights=parse_csv_floats(args.blend_top2_weights, default=DEFAULT_BLEND_TOP2_WEIGHTS),
        dynamic_drawdown_thresholds=parse_csv_floats(
            args.dynamic_drawdown_thresholds,
            default=DEFAULT_DYNAMIC_DRAWDOWN_THRESHOLDS,
        ),
        rolling_window_years=parse_csv_ints(args.rolling_window_years, default=DEFAULT_ROLLING_WINDOW_YEARS),
        benchmark_symbol=args.benchmark_symbol,
        broad_benchmark_symbol=args.broad_benchmark_symbol,
        safe_haven=args.safe_haven,
        turnover_cost_bps=args.turnover_cost_bps,
        min_price_usd=args.min_price_usd,
        min_adv20_usd=args.min_adv20_usd,
        min_history_days=args.min_history_days,
    )
    summary_path = output_dir / "concentration_variant_summary.csv"
    yearly_path = output_dir / "concentration_variant_yearly_summary.csv"
    rolling_path = output_dir / "concentration_variant_rolling_summary.csv"
    mode_path = output_dir / "concentration_variant_mode_history.csv"
    result["concentration_variant_summary"].to_csv(summary_path, index=False)
    result["concentration_variant_yearly_summary"].to_csv(yearly_path, index=False)
    result["concentration_variant_rolling_summary"].to_csv(rolling_path, index=False)
    result["concentration_variant_mode_history"].to_csv(mode_path, index=False)
    print(result["concentration_variant_summary"].head(max(int(args.print_top), 0)).to_string(index=False))
    print(f"wrote concentration variant summary -> {summary_path}")
    print(f"wrote concentration variant yearly summary -> {yearly_path}")
    print(f"wrote concentration variant rolling summary -> {rolling_path}")
    print(f"wrote concentration variant mode history -> {mode_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
