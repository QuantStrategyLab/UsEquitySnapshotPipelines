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
)
from .mega_cap_leader_rotation_dynamic_validation import (
    DEFAULT_LAG_TRADING_DAYS,
    DEFAULT_MAX_NAMES_PER_SECTOR_VALUES,
    DEFAULT_ROLLING_WINDOW_YEARS,
    DEFAULT_VALIDATION_CONFIGS,
    parse_csv_ints,
    parse_risk_modes,
    parse_validation_configs,
    run_dynamic_universe_validation,
)
from .russell_1000_multi_factor_defensive_snapshot import read_table

DEFAULT_PROXY_UNIVERSE_SIZE = 1000
DEFAULT_START_DELAY_TRADING_DAYS = 1
DEFAULT_EXCLUDED_SYMBOLS = (BENCHMARK_SYMBOL, BROAD_BENCHMARK_SYMBOL, SAFE_HAVEN)
MARKET_VALUE_COLUMNS = ("source_market_value", "market_value")
SHARES_OUTSTANDING_COLUMNS = ("shares_outstanding", "shares")
PROXY_UNIVERSE_COLUMNS = (
    "symbol",
    "sector",
    "start_date",
    "end_date",
    "mega_rank",
    "proxy_rank",
    "ranking_column",
    "proxy_method",
    "rank_as_of",
    "source_as_of",
    "source_market_value",
    "adv20_usd",
    "close",
    "volume",
    "history_days",
)


@dataclass(frozen=True)
class ProxyUniverseBuildResult:
    universe_history: pd.DataFrame
    metadata: dict[str, object]


def _normalize_symbol(value: object) -> str:
    return str(value or "").strip().upper()


def _normalize_date(value) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert(None)
    return timestamp.normalize()


def _normalize_price_history(price_history) -> pd.DataFrame:
    frame = pd.DataFrame(price_history).copy()
    required = {"symbol", "as_of", "close", "volume"}
    missing = required - set(frame.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"price_history missing required columns: {missing_text}")

    frame["symbol"] = frame["symbol"].map(_normalize_symbol)
    frame["as_of"] = pd.to_datetime(frame["as_of"], utc=False, errors="coerce").map(_normalize_date)
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce")
    if "sector" not in frame.columns:
        frame["sector"] = "unknown"
    frame["sector"] = frame["sector"].fillna("unknown").astype(str).str.strip().replace("", "unknown")

    for column in (*MARKET_VALUE_COLUMNS, *SHARES_OUTSTANDING_COLUMNS):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    return (
        frame.loc[frame["symbol"].ne("")]
        .dropna(subset=["as_of", "close"])
        .drop_duplicates(subset=["symbol", "as_of"], keep="last")
        .sort_values(["symbol", "as_of"])
        .reset_index(drop=True)
    )


def _coalesce_numeric(frame: pd.DataFrame, columns: Iterable[str]) -> pd.Series:
    values = pd.Series(pd.NA, index=frame.index, dtype="Float64")
    for column in columns:
        if column not in frame.columns:
            continue
        values = values.fillna(pd.to_numeric(frame[column], errors="coerce"))
    return values.astype(float)


def _prepare_proxy_features(price_history) -> tuple[pd.DataFrame, str, str]:
    frame = _normalize_price_history(price_history)
    frame["dollar_volume"] = frame["close"] * frame["volume"]
    frame["adv20_usd"] = frame.groupby("symbol", sort=False)["dollar_volume"].transform(
        lambda values: values.rolling(20, min_periods=1).mean()
    )
    frame["history_days"] = frame.groupby("symbol", sort=False).cumcount() + 1

    market_value = _coalesce_numeric(frame, MARKET_VALUE_COLUMNS)
    ranking_column = next((column for column in MARKET_VALUE_COLUMNS if column in frame.columns), "")
    if market_value.notna().any():
        frame["source_market_value"] = market_value
        frame["rank_metric"] = frame["source_market_value"]
        return frame, ranking_column or "market_value", "point_in_time_market_value"

    shares = _coalesce_numeric(frame, SHARES_OUTSTANDING_COLUMNS)
    if shares.notna().any():
        frame["source_market_value"] = frame["close"] * shares
        frame["rank_metric"] = frame["source_market_value"]
        share_column = next((column for column in SHARES_OUTSTANDING_COLUMNS if column in frame.columns), "shares")
        return frame, f"close_x_{share_column}", "point_in_time_shares_outstanding"

    frame["source_market_value"] = pd.NA
    frame["rank_metric"] = frame["adv20_usd"]
    return frame, "adv20_usd", "adv20_liquidity_proxy"


def _month_end_rebalance_dates(trading_index: pd.DatetimeIndex) -> pd.DatetimeIndex:
    if trading_index.empty:
        return pd.DatetimeIndex([])
    series = pd.Series(trading_index, index=trading_index)
    return pd.DatetimeIndex(pd.to_datetime(series.groupby(trading_index.to_period("M")).max().values))


def _start_date_for_rank(
    rank_as_of: pd.Timestamp,
    *,
    trading_index: pd.DatetimeIndex,
    start_delay_trading_days: int,
) -> pd.Timestamp | None:
    delay = int(start_delay_trading_days)
    position = int(trading_index.searchsorted(rank_as_of, side="left"))
    if delay > 0:
        position = int(trading_index.searchsorted(rank_as_of, side="right")) + delay - 1
    if position >= len(trading_index):
        return None
    return pd.Timestamp(trading_index[position]).normalize()


def build_proxy_universe_history(
    price_history,
    *,
    universe_size: int = DEFAULT_PROXY_UNIVERSE_SIZE,
    start_delay_trading_days: int = DEFAULT_START_DELAY_TRADING_DAYS,
    min_price_usd: float = 5.0,
    min_adv20_usd: float = 5_000_000.0,
    min_history_days: int = 252,
    max_stale_days: int = 7,
    excluded_symbols: Iterable[str] = DEFAULT_EXCLUDED_SYMBOLS,
) -> ProxyUniverseBuildResult:
    if int(universe_size) <= 0:
        raise ValueError("universe_size must be positive")
    if int(start_delay_trading_days) < 0:
        raise ValueError("start_delay_trading_days must be non-negative")

    features, ranking_column, proxy_method = _prepare_proxy_features(price_history)
    excluded = {_normalize_symbol(symbol) for symbol in excluded_symbols}
    trading_index = pd.DatetimeIndex(sorted(features["as_of"].dropna().unique()))
    rank_dates = _month_end_rebalance_dates(trading_index)
    rows: list[dict[str, object]] = []

    for rank_as_of in rank_dates:
        start_date = _start_date_for_rank(
            pd.Timestamp(rank_as_of),
            trading_index=trading_index,
            start_delay_trading_days=int(start_delay_trading_days),
        )
        if start_date is None:
            continue
        latest = features.loc[features["as_of"] <= rank_as_of].groupby("symbol", sort=False).tail(1).copy()
        latest = latest.loc[~latest["symbol"].isin(excluded)]
        latest = latest.loc[
            (pd.Timestamp(rank_as_of) - latest["as_of"]).dt.days <= int(max_stale_days)
        ]
        latest = latest.loc[
            (latest["close"] >= float(min_price_usd))
            & (latest["adv20_usd"] >= float(min_adv20_usd))
            & (latest["history_days"] >= int(min_history_days))
            & latest["rank_metric"].notna()
        ].copy()
        if latest.empty:
            continue
        latest = latest.sort_values(["rank_metric", "adv20_usd", "symbol"], ascending=[False, False, True]).head(
            int(universe_size)
        )
        for rank, item in enumerate(latest.itertuples(index=False), start=1):
            rows.append(
                {
                    "symbol": item.symbol,
                    "sector": item.sector,
                    "start_date": start_date,
                    "end_date": pd.NaT,
                    "mega_rank": int(rank),
                    "proxy_rank": int(rank),
                    "ranking_column": ranking_column,
                    "proxy_method": proxy_method,
                    "rank_as_of": pd.Timestamp(rank_as_of).normalize(),
                    "source_as_of": item.as_of,
                    "source_market_value": item.source_market_value,
                    "adv20_usd": float(item.adv20_usd),
                    "close": float(item.close),
                    "volume": float(item.volume) if pd.notna(item.volume) else float("nan"),
                    "history_days": int(item.history_days),
                }
            )

    universe = pd.DataFrame(rows, columns=PROXY_UNIVERSE_COLUMNS)
    if not universe.empty:
        start_dates = sorted(pd.Timestamp(value).normalize() for value in universe["start_date"].dropna().unique())
        end_by_start = {
            start_dates[index]: start_dates[index + 1] - pd.Timedelta(days=1)
            for index in range(len(start_dates) - 1)
        }
        universe["end_date"] = universe["start_date"].map(end_by_start).fillna(pd.NaT)
        universe = universe.sort_values(["start_date", "proxy_rank", "symbol"]).reset_index(drop=True)

    metadata = {
        "proxy_method": proxy_method,
        "ranking_column": ranking_column,
        "universe_size": int(universe_size),
        "start_delay_trading_days": int(start_delay_trading_days),
        "min_price_usd": float(min_price_usd),
        "min_adv20_usd": float(min_adv20_usd),
        "min_history_days": int(min_history_days),
        "max_stale_days": int(max_stale_days),
        "rank_dates": int(len(rank_dates)),
        "universe_rows": int(len(universe)),
        "first_start_date": str(universe["start_date"].min().date()) if not universe.empty else "",
        "last_start_date": str(universe["start_date"].max().date()) if not universe.empty else "",
    }
    return ProxyUniverseBuildResult(universe_history=universe, metadata=metadata)


def run_proxy_long_history_research(
    price_history,
    *,
    output_dir: str | Path,
    run_validation: bool = True,
    start_date: str | None = None,
    end_date: str | None = None,
    universe_size: int = DEFAULT_PROXY_UNIVERSE_SIZE,
    start_delay_trading_days: int = DEFAULT_START_DELAY_TRADING_DAYS,
    validation_configs=DEFAULT_VALIDATION_CONFIGS,
    risk_modes: str | Iterable[str] | None = None,
    universe_lag_trading_days: Iterable[int] = (0,),
    max_names_per_sector_values: Iterable[int] = DEFAULT_MAX_NAMES_PER_SECTOR_VALUES,
    rolling_window_years: Iterable[int] = DEFAULT_ROLLING_WINDOW_YEARS,
    benchmark_symbol: str = BENCHMARK_SYMBOL,
    broad_benchmark_symbol: str = BROAD_BENCHMARK_SYMBOL,
    safe_haven: str = SAFE_HAVEN,
    min_price_usd: float = 5.0,
    min_adv20_usd: float = 5_000_000.0,
    min_history_days: int = 252,
    turnover_cost_bps: float = 5.0,
) -> dict[str, pd.DataFrame | dict[str, object]]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    proxy_result = build_proxy_universe_history(
        price_history,
        universe_size=int(universe_size),
        start_delay_trading_days=int(start_delay_trading_days),
        min_price_usd=float(min_price_usd),
        min_adv20_usd=float(min_adv20_usd),
        min_history_days=int(min_history_days),
        excluded_symbols=(benchmark_symbol, broad_benchmark_symbol, safe_haven),
    )
    universe_path = root / "russell_1000_proxy_universe_history.csv"
    metadata_path = root / "russell_1000_proxy_metadata.csv"
    proxy_result.universe_history.to_csv(universe_path, index=False)
    pd.DataFrame([proxy_result.metadata]).to_csv(metadata_path, index=False)

    outputs: dict[str, pd.DataFrame | dict[str, object]] = {
        "proxy_universe_history": proxy_result.universe_history,
        "proxy_metadata": proxy_result.metadata,
    }
    if run_validation:
        validation = run_dynamic_universe_validation(
            price_history,
            proxy_result.universe_history,
            start_date=start_date or proxy_result.metadata["first_start_date"] or None,
            end_date=end_date,
            universe_lag_trading_days=tuple(universe_lag_trading_days),
            validation_configs=validation_configs,
            risk_modes=parse_risk_modes(risk_modes),
            max_names_per_sector_values=tuple(max_names_per_sector_values),
            rolling_window_years=tuple(rolling_window_years),
            benchmark_symbol=benchmark_symbol,
            broad_benchmark_symbol=broad_benchmark_symbol,
            safe_haven=safe_haven,
            min_price_usd=float(min_price_usd),
            min_adv20_usd=float(min_adv20_usd),
            min_history_days=int(min_history_days),
            turnover_cost_bps=float(turnover_cost_bps),
        )
        for name, frame in validation.items():
            frame.to_csv(root / f"{name}.csv", index=False)
        outputs.update(validation)
    return outputs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a point-in-time Russell 1000 proxy universe from long-history prices and optionally validate "
            "the Top50 leader-rotation strategy on it."
        )
    )
    parser.add_argument("--prices", required=True, help="Long-history price file with symbol/as_of/close/volume")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--start", dest="start_date")
    parser.add_argument("--end", dest="end_date")
    parser.add_argument("--universe-size", type=int, default=DEFAULT_PROXY_UNIVERSE_SIZE)
    parser.add_argument("--start-delay-trading-days", type=int, default=DEFAULT_START_DELAY_TRADING_DAYS)
    parser.add_argument("--skip-validation", action="store_true")
    parser.add_argument("--strategy-configs", default=DEFAULT_VALIDATION_CONFIGS)
    parser.add_argument("--risk-modes")
    parser.add_argument("--universe-lag-days", default="0")
    parser.add_argument(
        "--max-names-per-sector-values",
        default=",".join(str(value) for value in DEFAULT_MAX_NAMES_PER_SECTOR_VALUES),
    )
    parser.add_argument("--rolling-window-years", default="")
    parser.add_argument("--benchmark-symbol", default=BENCHMARK_SYMBOL)
    parser.add_argument("--broad-benchmark-symbol", default=BROAD_BENCHMARK_SYMBOL)
    parser.add_argument("--safe-haven", default=SAFE_HAVEN)
    parser.add_argument("--min-price-usd", type=float, default=5.0)
    parser.add_argument("--min-adv20-usd", type=float, default=5_000_000.0)
    parser.add_argument("--min-history-days", type=int, default=252)
    parser.add_argument("--turnover-cost-bps", type=float, default=5.0)
    parser.add_argument("--print-top", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_proxy_long_history_research(
        read_table(args.prices),
        output_dir=args.output_dir,
        run_validation=not args.skip_validation,
        start_date=args.start_date,
        end_date=args.end_date,
        universe_size=args.universe_size,
        start_delay_trading_days=args.start_delay_trading_days,
        validation_configs=parse_validation_configs(args.strategy_configs),
        risk_modes=args.risk_modes,
        universe_lag_trading_days=parse_csv_ints(args.universe_lag_days, default=DEFAULT_LAG_TRADING_DAYS),
        max_names_per_sector_values=parse_csv_ints(
            args.max_names_per_sector_values,
            default=DEFAULT_MAX_NAMES_PER_SECTOR_VALUES,
        ),
        rolling_window_years=parse_csv_ints(args.rolling_window_years, default=DEFAULT_ROLLING_WINDOW_YEARS),
        benchmark_symbol=args.benchmark_symbol,
        broad_benchmark_symbol=args.broad_benchmark_symbol,
        safe_haven=args.safe_haven,
        min_price_usd=args.min_price_usd,
        min_adv20_usd=args.min_adv20_usd,
        min_history_days=args.min_history_days,
        turnover_cost_bps=args.turnover_cost_bps,
    )
    metadata = result["proxy_metadata"]
    print(pd.DataFrame([metadata]).to_string(index=False))
    validation = result.get("validation_summary")
    if isinstance(validation, pd.DataFrame) and not validation.empty:
        print(validation.head(max(int(args.print_top), 0)).to_string(index=False))
    print(f"wrote proxy research outputs -> {Path(args.output_dir)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
