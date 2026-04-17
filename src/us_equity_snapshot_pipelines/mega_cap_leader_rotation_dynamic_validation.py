from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from .mega_cap_leader_rotation_backtest import (
    BENCHMARK_SYMBOL,
    BROAD_BENCHMARK_SYMBOL,
    SAFE_HAVEN,
    _normalize_price_history,
    run_backtest,
)
from .russell_1000_multi_factor_defensive_snapshot import read_table

DEFAULT_LAG_TRADING_DAYS = (0, 1, 5, 21)
DEFAULT_VALIDATION_CONFIGS = "top2_cap50:2:0.50,top3_cap35:3:0.35"
DEFAULT_MAX_NAMES_PER_SECTOR_VALUES = (0,)
DEFAULT_ROLLING_WINDOW_YEARS: tuple[int, ...] = ()
VALIDATION_SUMMARY_COLUMNS = (
    "Run",
    "Config",
    "Risk Mode",
    "Universe Lag Trading Days",
    "Max Names Per Sector",
    "Top N",
    "Single Name Cap",
    "Risk On Exposure",
    "Soft Defense Exposure",
    "Hard Defense Exposure",
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
YEARLY_SUMMARY_COLUMNS = (
    "Run",
    "Config",
    "Risk Mode",
    "Universe Lag Trading Days",
    "Max Names Per Sector",
    "Year",
    "Strategy Return",
    "Strategy Max Drawdown",
    "QQQ Return",
    "QQQ Max Drawdown",
    "SPY Return",
    "SPY Max Drawdown",
)
ROLLING_WINDOW_SUMMARY_COLUMNS = (
    "Run",
    "Config",
    "Risk Mode",
    "Universe Lag Trading Days",
    "Max Names Per Sector",
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


@dataclass(frozen=True)
class ValidationConfig:
    name: str
    top_n: int
    single_name_cap: float


@dataclass(frozen=True)
class RiskMode:
    name: str
    risk_on_exposure: float
    soft_defense_exposure: float
    hard_defense_exposure: float


def parse_csv_ints(raw_value: str | Iterable[int] | None, *, default: tuple[int, ...]) -> tuple[int, ...]:
    if raw_value is None:
        return default
    values = raw_value.split(",") if isinstance(raw_value, str) else list(raw_value)
    parsed = tuple(dict.fromkeys(int(str(value).strip()) for value in values if str(value).strip()))
    return parsed or default


def parse_validation_configs(raw_value: str | Iterable[str] | None) -> tuple[ValidationConfig, ...]:
    values = DEFAULT_VALIDATION_CONFIGS.split(",") if raw_value is None else (
        raw_value.split(",") if isinstance(raw_value, str) else list(raw_value)
    )
    configs: list[ValidationConfig] = []
    seen: set[str] = set()
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        parts = text.split(":")
        if len(parts) != 3:
            raise ValueError("strategy configs must use name:top_n:single_name_cap entries")
        name = parts[0].strip()
        if not name:
            raise ValueError("strategy config name must not be empty")
        top_n = int(parts[1])
        single_name_cap = float(parts[2])
        if top_n <= 0:
            raise ValueError("strategy config top_n must be positive")
        if not 0.0 < single_name_cap <= 1.0:
            raise ValueError("strategy config single_name_cap must be in (0, 1]")
        if name in seen:
            continue
        seen.add(name)
        configs.append(ValidationConfig(name=name, top_n=top_n, single_name_cap=single_name_cap))
    if not configs:
        raise ValueError("at least one strategy config is required")
    return tuple(configs)


def parse_risk_modes(raw_value: str | Iterable[str] | None) -> tuple[RiskMode, ...]:
    if raw_value is None:
        return ()
    values = raw_value.split(",") if isinstance(raw_value, str) else list(raw_value)
    modes: list[RiskMode] = []
    seen: set[str] = set()
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        parts = text.split(":")
        if len(parts) != 4:
            raise ValueError("risk modes must use name:risk_on:soft_defense:hard_defense entries")
        name = parts[0].strip()
        if not name:
            raise ValueError("risk mode name must not be empty")
        exposures = tuple(float(part) for part in parts[1:])
        if any(exposure < 0.0 or exposure > 1.0 for exposure in exposures):
            raise ValueError("risk mode exposures must be in [0, 1]")
        if name in seen:
            continue
        seen.add(name)
        modes.append(
            RiskMode(
                name=name,
                risk_on_exposure=exposures[0],
                soft_defense_exposure=exposures[1],
                hard_defense_exposure=exposures[2],
            )
        )
    return tuple(modes)


def _normalize_trading_index(price_history: pd.DataFrame) -> pd.DatetimeIndex:
    prices = _normalize_price_history(price_history)
    index = pd.DatetimeIndex(sorted(prices["as_of"].dropna().unique()))
    if index.empty:
        raise ValueError("price_history has no usable trading dates")
    return index


def _shift_on_trading_index(
    value,
    *,
    trading_index: pd.DatetimeIndex,
    lag_trading_days: int,
    finite_end: bool,
) -> pd.Timestamp:
    if pd.isna(value):
        return pd.NaT
    date = pd.Timestamp(value).tz_localize(None).normalize()
    position = int(trading_index.searchsorted(date, side="left")) + int(lag_trading_days)
    if position >= len(trading_index):
        return pd.Timestamp(trading_index[-1]).normalize() if finite_end else pd.NaT
    return pd.Timestamp(trading_index[position]).normalize()


def lag_universe_history(
    universe_history,
    *,
    lag_trading_days: int,
    trading_index: pd.DatetimeIndex,
) -> pd.DataFrame:
    frame = pd.DataFrame(universe_history).copy()
    if lag_trading_days < 0:
        raise ValueError("lag_trading_days must be non-negative")
    if frame.empty or int(lag_trading_days) == 0:
        return frame
    if "start_date" not in frame.columns:
        raise ValueError("lagged dynamic universe validation requires start_date")

    frame["start_date"] = pd.to_datetime(frame["start_date"], errors="coerce").dt.tz_localize(None).dt.normalize()
    if "end_date" in frame.columns:
        frame["end_date"] = pd.to_datetime(frame["end_date"], errors="coerce").dt.tz_localize(None).dt.normalize()
    else:
        frame["end_date"] = pd.NaT

    frame["start_date"] = frame["start_date"].map(
        lambda value: _shift_on_trading_index(
            value,
            trading_index=trading_index,
            lag_trading_days=int(lag_trading_days),
            finite_end=False,
        )
    )
    frame["end_date"] = frame["end_date"].map(
        lambda value: _shift_on_trading_index(
            value,
            trading_index=trading_index,
            lag_trading_days=int(lag_trading_days),
            finite_end=True,
        )
    )
    frame = frame.dropna(subset=["start_date"]).copy()
    frame = frame.loc[frame["end_date"].isna() | (frame["end_date"] >= frame["start_date"])].copy()
    return frame.reset_index(drop=True)


def _period_return(returns: pd.Series) -> float:
    values = pd.to_numeric(returns, errors="coerce").dropna()
    if values.empty:
        return float("nan")
    return float((1.0 + values).prod() - 1.0)


def _period_max_drawdown(returns: pd.Series) -> float:
    values = pd.to_numeric(returns, errors="coerce").dropna()
    if values.empty:
        return float("nan")
    equity = (1.0 + values).cumprod()
    return float((equity / equity.cummax() - 1.0).min())


def _period_cagr(returns: pd.Series) -> float:
    values = pd.to_numeric(returns, errors="coerce").dropna()
    if values.empty:
        return float("nan")
    total_return = float((1.0 + values).prod())
    years = max((values.index[-1] - values.index[0]).days / 365.25, 1 / 365.25)
    return float(total_return ** (1.0 / years) - 1.0)


def _build_yearly_summary(
    *,
    run_name: str,
    config: ValidationConfig,
    risk_mode: RiskMode,
    lag_trading_days: int,
    max_names_per_sector: int,
    portfolio_returns: pd.Series,
    reference_returns: pd.DataFrame,
    benchmark_symbol: str,
    broad_benchmark_symbol: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    years = sorted(int(year) for year in pd.DatetimeIndex(portfolio_returns.index).year.unique())
    for year in years:
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
                "Config": config.name,
                "Risk Mode": risk_mode.name,
                "Universe Lag Trading Days": int(lag_trading_days),
                "Max Names Per Sector": int(max_names_per_sector),
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


def _complete_calendar_years(index: pd.DatetimeIndex) -> list[int]:
    years: list[int] = []
    normalized = pd.DatetimeIndex(index).tz_localize(None).normalize()
    for year, values in pd.Series(normalized, index=normalized).groupby(normalized.year):
        first = pd.Timestamp(values.min()).normalize()
        last = pd.Timestamp(values.max()).normalize()
        if first <= pd.Timestamp(year=int(year), month=1, day=7) and last >= pd.Timestamp(
            year=int(year),
            month=12,
            day=24,
        ):
            years.append(int(year))
    return years


def _build_rolling_window_summary(
    *,
    run_name: str,
    config: ValidationConfig,
    risk_mode: RiskMode,
    lag_trading_days: int,
    max_names_per_sector: int,
    portfolio_returns: pd.Series,
    reference_returns: pd.DataFrame,
    benchmark_symbol: str,
    broad_benchmark_symbol: str,
    rolling_window_years: Iterable[int],
) -> list[dict[str, object]]:
    years = _complete_calendar_years(pd.DatetimeIndex(portfolio_returns.index))
    rows: list[dict[str, object]] = []
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
                    "Config": config.name,
                    "Risk Mode": risk_mode.name,
                    "Universe Lag Trading Days": int(lag_trading_days),
                    "Max Names Per Sector": int(max_names_per_sector),
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


def run_dynamic_universe_validation(
    price_history,
    universe_history,
    *,
    start_date: str | None = "2018-01-31",
    end_date: str | None = None,
    universe_lag_trading_days: Iterable[int] = DEFAULT_LAG_TRADING_DAYS,
    validation_configs: Iterable[ValidationConfig] | str | None = None,
    risk_modes: Iterable[RiskMode] | str | None = None,
    max_names_per_sector_values: Iterable[int] = DEFAULT_MAX_NAMES_PER_SECTOR_VALUES,
    rolling_window_years: Iterable[int] = DEFAULT_ROLLING_WINDOW_YEARS,
    benchmark_symbol: str = BENCHMARK_SYMBOL,
    broad_benchmark_symbol: str = BROAD_BENCHMARK_SYMBOL,
    safe_haven: str = SAFE_HAVEN,
    hold_buffer: int = 2,
    hold_bonus: float = 0.10,
    risk_on_exposure: float = 1.0,
    soft_defense_exposure: float = 1.0,
    hard_defense_exposure: float = 1.0,
    soft_breadth_threshold: float = 0.50,
    hard_breadth_threshold: float = 0.30,
    min_price_usd: float = 10.0,
    min_adv20_usd: float = 20_000_000.0,
    min_history_days: int = 273,
    turnover_cost_bps: float = 5.0,
) -> dict[str, pd.DataFrame]:
    configs = (
        parse_validation_configs(validation_configs)
        if validation_configs is None or isinstance(validation_configs, str)
        else tuple(validation_configs)
    )
    risk_mode_values = (
        parse_risk_modes(risk_modes)
        if risk_modes is None or isinstance(risk_modes, str)
        else tuple(risk_modes)
    )
    if not risk_mode_values:
        risk_mode_values = (
            RiskMode(
                name="custom",
                risk_on_exposure=float(risk_on_exposure),
                soft_defense_exposure=float(soft_defense_exposure),
                hard_defense_exposure=float(hard_defense_exposure),
            ),
        )
    lag_values = parse_csv_ints(tuple(universe_lag_trading_days), default=DEFAULT_LAG_TRADING_DAYS)
    sector_cap_values = parse_csv_ints(
        tuple(max_names_per_sector_values),
        default=DEFAULT_MAX_NAMES_PER_SECTOR_VALUES,
    )
    rolling_values = parse_csv_ints(tuple(rolling_window_years), default=DEFAULT_ROLLING_WINDOW_YEARS)
    trading_index = _normalize_trading_index(pd.DataFrame(price_history))
    summary_rows: list[dict[str, object]] = []
    yearly_rows: list[dict[str, object]] = []
    rolling_rows: list[dict[str, object]] = []

    for lag_days in lag_values:
        lagged_universe = lag_universe_history(
            universe_history,
            lag_trading_days=int(lag_days),
            trading_index=trading_index,
        )
        for config in configs:
            for risk_mode in risk_mode_values:
                for sector_cap in sector_cap_values:
                    sector_label = "all" if int(sector_cap) <= 0 else str(int(sector_cap))
                    run_name = f"{config.name}_{risk_mode.name}_sector{sector_label}_lag{int(lag_days)}"
                    result = run_backtest(
                        price_history,
                        lagged_universe,
                        start_date=start_date,
                        end_date=end_date,
                        pool_name=f"dynamic_lag{int(lag_days)}",
                        benchmark_symbol=benchmark_symbol,
                        broad_benchmark_symbol=broad_benchmark_symbol,
                        safe_haven=safe_haven,
                        top_n=config.top_n,
                        hold_buffer=hold_buffer,
                        single_name_cap=config.single_name_cap,
                        max_names_per_sector=int(sector_cap),
                        hold_bonus=hold_bonus,
                        risk_on_exposure=risk_mode.risk_on_exposure,
                        soft_defense_exposure=risk_mode.soft_defense_exposure,
                        hard_defense_exposure=risk_mode.hard_defense_exposure,
                        soft_breadth_threshold=soft_breadth_threshold,
                        hard_breadth_threshold=hard_breadth_threshold,
                        min_price_usd=min_price_usd,
                        min_adv20_usd=min_adv20_usd,
                        min_history_days=min_history_days,
                        turnover_cost_bps=turnover_cost_bps,
                    )
                    summary = dict(result["summary"])
                    summary.update(
                        {
                            "Run": run_name,
                            "Config": config.name,
                            "Risk Mode": risk_mode.name,
                            "Universe Lag Trading Days": int(lag_days),
                            "Max Names Per Sector": int(sector_cap),
                            "Top N": int(config.top_n),
                            "Single Name Cap": float(config.single_name_cap),
                            "Risk On Exposure": float(risk_mode.risk_on_exposure),
                            "Soft Defense Exposure": float(risk_mode.soft_defense_exposure),
                            "Hard Defense Exposure": float(risk_mode.hard_defense_exposure),
                        }
                    )
                    summary_rows.append(summary)
                    yearly_rows.extend(
                        _build_yearly_summary(
                            run_name=run_name,
                            config=config,
                            risk_mode=risk_mode,
                            lag_trading_days=int(lag_days),
                            max_names_per_sector=int(sector_cap),
                            portfolio_returns=result["portfolio_returns"],
                            reference_returns=result["reference_returns"],
                            benchmark_symbol=benchmark_symbol,
                            broad_benchmark_symbol=broad_benchmark_symbol,
                        )
                    )
                    rolling_rows.extend(
                        _build_rolling_window_summary(
                            run_name=run_name,
                            config=config,
                            risk_mode=risk_mode,
                            lag_trading_days=int(lag_days),
                            max_names_per_sector=int(sector_cap),
                            portfolio_returns=result["portfolio_returns"],
                            reference_returns=result["reference_returns"],
                            benchmark_symbol=benchmark_symbol,
                            broad_benchmark_symbol=broad_benchmark_symbol,
                            rolling_window_years=rolling_values,
                        )
                    )

    summary_frame = pd.DataFrame(summary_rows)
    yearly_frame = pd.DataFrame(yearly_rows)
    rolling_frame = pd.DataFrame(rolling_rows, columns=ROLLING_WINDOW_SUMMARY_COLUMNS)
    summary_columns = [column for column in VALIDATION_SUMMARY_COLUMNS if column in summary_frame.columns]
    yearly_columns = [column for column in YEARLY_SUMMARY_COLUMNS if column in yearly_frame.columns]
    rolling_columns = [column for column in ROLLING_WINDOW_SUMMARY_COLUMNS if column in rolling_frame.columns]
    return {
        "validation_summary": summary_frame.loc[:, summary_columns],
        "yearly_validation_summary": yearly_frame.loc[:, yearly_columns],
        "rolling_window_summary": rolling_frame.loc[:, rolling_columns],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate dynamic mega-cap leader rotation against universe availability lags and yearly slices."
    )
    parser.add_argument("--prices", required=True, help="Input price history file (.csv/.json/.jsonl/.parquet)")
    parser.add_argument("--universe", required=True, help="Input dynamic universe history file")
    parser.add_argument("--output-dir", required=True, help="Directory for validation outputs")
    parser.add_argument("--start", dest="start_date", default="2018-01-31")
    parser.add_argument("--end", dest="end_date")
    parser.add_argument("--universe-lag-days", default=",".join(str(value) for value in DEFAULT_LAG_TRADING_DAYS))
    parser.add_argument("--strategy-configs", default=DEFAULT_VALIDATION_CONFIGS)
    parser.add_argument(
        "--risk-modes",
        help="Comma-separated name:risk_on:soft_defense:hard_defense entries; defaults to exposure flags",
    )
    parser.add_argument(
        "--max-names-per-sector-values",
        default=",".join(str(value) for value in DEFAULT_MAX_NAMES_PER_SECTOR_VALUES),
        help="Comma-separated per-sector selected-name caps; 0 disables the cap",
    )
    parser.add_argument(
        "--rolling-window-years",
        default="",
        help="Comma-separated calendar-year rolling windows to summarize, for example 3,5",
    )
    parser.add_argument("--benchmark-symbol", default=BENCHMARK_SYMBOL)
    parser.add_argument("--broad-benchmark-symbol", default=BROAD_BENCHMARK_SYMBOL)
    parser.add_argument("--safe-haven", default=SAFE_HAVEN)
    parser.add_argument("--hold-buffer", type=int, default=2)
    parser.add_argument("--hold-bonus", type=float, default=0.10)
    parser.add_argument("--risk-on-exposure", type=float, default=1.0)
    parser.add_argument("--soft-defense-exposure", type=float, default=1.0)
    parser.add_argument("--hard-defense-exposure", type=float, default=1.0)
    parser.add_argument("--soft-breadth-threshold", type=float, default=0.50)
    parser.add_argument("--hard-breadth-threshold", type=float, default=0.30)
    parser.add_argument("--min-price-usd", type=float, default=10.0)
    parser.add_argument("--min-adv20-usd", type=float, default=20_000_000.0)
    parser.add_argument("--min-history-days", type=int, default=273)
    parser.add_argument("--turnover-cost-bps", type=float, default=5.0)
    parser.add_argument("--print-top", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    result = run_dynamic_universe_validation(
        read_table(args.prices),
        read_table(args.universe),
        start_date=args.start_date,
        end_date=args.end_date,
        universe_lag_trading_days=parse_csv_ints(args.universe_lag_days, default=DEFAULT_LAG_TRADING_DAYS),
        validation_configs=parse_validation_configs(args.strategy_configs),
        risk_modes=parse_risk_modes(args.risk_modes),
        max_names_per_sector_values=parse_csv_ints(
            args.max_names_per_sector_values,
            default=DEFAULT_MAX_NAMES_PER_SECTOR_VALUES,
        ),
        rolling_window_years=parse_csv_ints(args.rolling_window_years, default=DEFAULT_ROLLING_WINDOW_YEARS),
        benchmark_symbol=args.benchmark_symbol,
        broad_benchmark_symbol=args.broad_benchmark_symbol,
        safe_haven=args.safe_haven,
        hold_buffer=args.hold_buffer,
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
    summary_path = output_dir / "validation_summary.csv"
    yearly_path = output_dir / "yearly_validation_summary.csv"
    rolling_path = output_dir / "rolling_window_summary.csv"
    result["validation_summary"].to_csv(summary_path, index=False)
    result["yearly_validation_summary"].to_csv(yearly_path, index=False)
    result["rolling_window_summary"].to_csv(rolling_path, index=False)
    print(result["validation_summary"].head(max(int(args.print_top), 0)).to_string(index=False))
    print(f"wrote validation summary -> {summary_path}")
    print(f"wrote yearly validation summary -> {yearly_path}")
    print(f"wrote rolling window summary -> {rolling_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
