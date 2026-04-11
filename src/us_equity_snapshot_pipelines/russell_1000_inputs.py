from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd

from .russell_1000_history import (
    backfill_universe_history_start,
    build_interval_universe_history,
    build_symbol_alias_candidates,
    build_symbol_alias_table,
    collect_symbol_universe,
    download_ishares_historical_universe_snapshots,
)
from .yfinance_prices import download_price_history
from .russell_1000_multi_factor_defensive_snapshot import read_table, write_table

DEFAULT_EXTRA_SYMBOLS = ("QQQ", "SPY", "BOXX")


@dataclass(frozen=True)
class Russell1000InputDataResult:
    output_dir: Path
    universe_history_path: Path
    price_history_path: Path
    alias_output_path: Path
    metadata_output_path: Path
    snapshot_dir: Path
    universe_rows: int
    price_rows: int
    symbol_count: int
    download_start: str
    missing_symbol_count: int


def split_symbols(raw_symbols: str | Iterable[str] | None) -> tuple[str, ...]:
    if raw_symbols is None:
        return ()
    values = raw_symbols.split(",") if isinstance(raw_symbols, str) else list(raw_symbols)
    return tuple(dict.fromkeys(str(value).strip().upper() for value in values if str(value).strip()))


def normalize_price_history(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["symbol", "as_of", "close", "volume"])
    required = {"symbol", "as_of", "close"}
    missing = required - set(frame.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"price history missing required columns: {missing_text}")
    normalized = frame.copy()
    normalized["symbol"] = normalized["symbol"].astype(str).str.upper().str.strip()
    normalized["as_of"] = pd.to_datetime(normalized["as_of"], utc=False).dt.tz_localize(None).dt.normalize()
    normalized["close"] = pd.to_numeric(normalized["close"], errors="coerce")
    if "volume" not in normalized.columns:
        normalized["volume"] = pd.NA
    normalized["volume"] = pd.to_numeric(normalized["volume"], errors="coerce")
    valid_rows = normalized["symbol"].ne("") & normalized["as_of"].notna() & normalized["close"].notna()
    return (
        normalized.loc[valid_rows, ["symbol", "as_of", "close", "volume"]]
        .drop_duplicates(subset=["symbol", "as_of"], keep="last")
        .sort_values(["as_of", "symbol"])
        .reset_index(drop=True)
    )


def merge_price_history(*frames: pd.DataFrame) -> pd.DataFrame:
    normalized_frames = [normalize_price_history(frame) for frame in frames if frame is not None and not frame.empty]
    if not normalized_frames:
        return pd.DataFrame(columns=["symbol", "as_of", "close", "volume"])
    return normalize_price_history(pd.concat(normalized_frames, ignore_index=True))


def incremental_start_date(existing_prices: pd.DataFrame, *, requested_start: str, overlap_days: int) -> str:
    requested = pd.Timestamp(requested_start).tz_localize(None).normalize()
    if existing_prices.empty or "as_of" not in existing_prices.columns:
        return f"{requested:%Y-%m-%d}"
    existing = normalize_price_history(existing_prices)
    if existing.empty:
        return f"{requested:%Y-%m-%d}"
    latest = pd.to_datetime(existing["as_of"], utc=False).max().tz_localize(None).normalize()
    refresh_start = latest - pd.Timedelta(days=max(int(overlap_days), 0))
    return f"{max(requested, refresh_start):%Y-%m-%d}"


def collect_download_symbols(
    universe_history: pd.DataFrame,
    *,
    benchmark_symbol: str,
    safe_haven: str,
    extra_symbols: Sequence[str],
) -> list[str]:
    symbols = collect_symbol_universe(
        universe_history,
        benchmark_symbol=benchmark_symbol,
        safe_haven=safe_haven,
    )
    for symbol in extra_symbols:
        normalized = str(symbol or "").strip().upper()
        if normalized and normalized not in symbols:
            symbols.append(normalized)
    return symbols


def prepare_russell_1000_input_data(
    *,
    output_dir: str | Path,
    universe_start: str,
    universe_end: str | None = None,
    price_start: str = "2018-01-01",
    price_end: str | None = None,
    universe_backfill_start: str | None = None,
    max_lookback_days: int = 7,
    existing_prices_path: str | Path | None = None,
    price_overlap_days: int = 7,
    benchmark_symbol: str = "QQQ",
    safe_haven: str = "BOXX",
    extra_symbols: Sequence[str] = DEFAULT_EXTRA_SYMBOLS,
    chunk_size: int = 100,
) -> Russell1000InputDataResult:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    snapshot_dir = root / "universe_snapshots"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    universe_history_path = root / "r1000_universe_history.csv"
    price_history_path = root / "r1000_price_history.csv"
    alias_output_path = root / "r1000_symbol_aliases.csv"
    metadata_output_path = root / "r1000_universe_snapshot_metadata.csv"

    snapshot_tables, metadata = download_ishares_historical_universe_snapshots(
        start_date=universe_start,
        end_date=universe_end,
        max_lookback_days=max_lookback_days,
    )
    for as_of_date, snapshot in snapshot_tables:
        write_table(snapshot, snapshot_dir / f"r1000_{pd.Timestamp(as_of_date):%Y-%m-%d}.csv")
    write_table(metadata, metadata_output_path)

    universe_history = build_interval_universe_history(snapshot_tables)
    if universe_backfill_start:
        universe_history = backfill_universe_history_start(universe_history, universe_backfill_start)
    write_table(universe_history, universe_history_path)

    symbol_aliases = build_symbol_alias_candidates(snapshot_tables)
    write_table(build_symbol_alias_table(symbol_aliases), alias_output_path)

    symbols = collect_download_symbols(
        universe_history,
        benchmark_symbol=benchmark_symbol,
        safe_haven=safe_haven,
        extra_symbols=extra_symbols,
    )

    existing_prices = pd.DataFrame(columns=["symbol", "as_of", "close", "volume"])
    if existing_prices_path and Path(existing_prices_path).exists():
        existing_prices = normalize_price_history(read_table(existing_prices_path))
    update_start = incremental_start_date(
        existing_prices,
        requested_start=price_start,
        overlap_days=price_overlap_days,
    )
    missing_symbols = (
        sorted(set(symbols) - set(existing_prices["symbol"].unique())) if not existing_prices.empty else []
    )

    update_prices = download_price_history(
        symbols,
        start=update_start,
        end=price_end,
        chunk_size=chunk_size,
        symbol_aliases=symbol_aliases,
    )
    price_frames = [existing_prices, update_prices]
    if missing_symbols and update_start != pd.Timestamp(price_start).strftime("%Y-%m-%d"):
        missing_prices = download_price_history(
            missing_symbols,
            start=price_start,
            end=price_end,
            chunk_size=chunk_size,
            symbol_aliases=symbol_aliases,
        )
        price_frames.append(missing_prices)
    merged_prices = merge_price_history(*price_frames)
    write_table(merged_prices, price_history_path)

    return Russell1000InputDataResult(
        output_dir=root,
        universe_history_path=universe_history_path,
        price_history_path=price_history_path,
        alias_output_path=alias_output_path,
        metadata_output_path=metadata_output_path,
        snapshot_dir=snapshot_dir,
        universe_rows=int(len(universe_history)),
        price_rows=int(len(merged_prices)),
        symbol_count=int(len(symbols)),
        download_start=update_start,
        missing_symbol_count=int(len(missing_symbols)),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare Russell 1000 source input data for snapshot pipelines.")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--universe-start", default="2018-01-01")
    parser.add_argument("--universe-end")
    parser.add_argument("--price-start", default="2018-01-01")
    parser.add_argument("--price-end")
    parser.add_argument("--universe-backfill-start")
    parser.add_argument("--max-lookback-days", type=int, default=7)
    parser.add_argument("--existing-prices")
    parser.add_argument("--price-overlap-days", type=int, default=7)
    parser.add_argument("--benchmark-symbol", default="QQQ")
    parser.add_argument("--safe-haven", default="BOXX")
    parser.add_argument("--extra-symbols", default=",".join(DEFAULT_EXTRA_SYMBOLS))
    parser.add_argument("--chunk-size", type=int, default=100)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = prepare_russell_1000_input_data(
        output_dir=args.output_dir,
        universe_start=args.universe_start,
        universe_end=args.universe_end,
        price_start=args.price_start,
        price_end=args.price_end,
        universe_backfill_start=args.universe_backfill_start,
        max_lookback_days=args.max_lookback_days,
        existing_prices_path=args.existing_prices,
        price_overlap_days=args.price_overlap_days,
        benchmark_symbol=args.benchmark_symbol,
        safe_haven=args.safe_haven,
        extra_symbols=split_symbols(args.extra_symbols),
        chunk_size=args.chunk_size,
    )
    print(f"wrote universe history: {result.universe_rows} rows -> {result.universe_history_path}")
    print(f"wrote price history: {result.price_rows} rows -> {result.price_history_path}")
    print(f"wrote aliases -> {result.alias_output_path}")
    print(f"wrote metadata -> {result.metadata_output_path}")
    print(
        f"downloaded/updated {result.symbol_count} symbols from {result.download_start}; "
        f"missing_full_history={result.missing_symbol_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
