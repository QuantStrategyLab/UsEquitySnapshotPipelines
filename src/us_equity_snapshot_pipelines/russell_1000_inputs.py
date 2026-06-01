from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from .artifacts import sha256_file, write_json
from .russell_1000_history import (
    COMPANIES_MARKETCAP_IWB_HOLDINGS_URL,
    backfill_universe_history_start,
    build_interval_universe_history,
    build_monthly_snapshot_request_dates,
    build_symbol_alias_candidates,
    build_symbol_alias_table,
    collect_symbol_universe,
    download_companies_marketcap_iwb_holdings_snapshot,
    download_ishares_historical_universe_snapshots,
    resolve_ishares_holdings_snapshot,
)
from .yfinance_prices import download_price_history
from .russell_1000_multi_factor_defensive_snapshot import read_table, write_table

DEFAULT_EXTRA_SYMBOLS = ("QQQ", "SPY", "BOXX")
SOURCE_INPUT_MANIFEST_FILENAME = "r1000_source_input_manifest.json"


@dataclass(frozen=True)
class Russell1000InputDataResult:
    output_dir: Path
    universe_history_path: Path
    price_history_path: Path
    alias_output_path: Path
    metadata_output_path: Path
    latest_snapshot_output_path: Path
    source_manifest_output_path: Path
    snapshot_dir: Path
    universe_rows: int
    price_rows: int
    symbol_count: int
    download_start: str
    missing_symbol_count: int
    universe_fallback_used: bool = False
    fallback_reason: str | None = None
    fallback_streak: int = 0
    price_as_of: str | None = None
    universe_as_of: str | None = None
    missing_price_backfill_warning: str | None = None
    historical_universe_refresh_warning: str | None = None
    latest_universe_refresh_warning: str | None = None


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


def _latest_date(frame: pd.DataFrame, columns: Sequence[str]) -> str | None:
    for column in columns:
        if column not in frame.columns:
            continue
        values = pd.to_datetime(frame[column], errors="coerce")
        if values.notna().any():
            return pd.Timestamp(values.max()).date().isoformat()
    return None


def _load_json_mapping(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    resolved = Path(path)
    if not resolved.exists():
        return {}
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"source input manifest must contain a JSON object: {resolved}")
    return dict(payload)


def _source_fallback_streak(
    *,
    existing_source_manifest_path: str | Path | None,
    fallback_used: bool,
) -> int:
    if not fallback_used:
        return 0
    previous = _load_json_mapping(existing_source_manifest_path)
    previous_streak = previous.get("fallback_streak")
    if previous_streak is None and isinstance(previous.get("fallback"), Mapping):
        previous_streak = previous["fallback"].get("streak")
    try:
        previous_streak_int = int(previous_streak or 0)
    except (TypeError, ValueError):
        previous_streak_int = 0
    previous_used = bool(previous.get("universe_fallback_used") or previous_streak_int > 0)
    return previous_streak_int + 1 if previous_used else 1


def _file_artifact(path: Path, *, row_count: int, as_of: str | None = None) -> dict[str, Any]:
    return {
        "path": str(path),
        "row_count": int(row_count),
        "as_of": as_of,
        "sha256": sha256_file(path) if path.exists() else None,
    }


def _write_source_input_manifest(
    *,
    result: "Russell1000InputDataResult",
    universe_metadata: pd.DataFrame,
) -> Path:
    if result.universe_fallback_used:
        source_input_status = "universe_fallback"
    elif result.historical_universe_refresh_warning:
        source_input_status = "partial_history_refresh"
    else:
        source_input_status = "fresh"
    payload = {
        "manifest_type": "source_input_data",
        "contract_version": "r1000_official_monthly_v2_alias.source_input.v1",
        "source_input_status": source_input_status,
        "universe_fallback_used": bool(result.universe_fallback_used),
        "fallback_reason": result.fallback_reason,
        "fallback_streak": int(result.fallback_streak),
        "fallback": {
            "used": bool(result.universe_fallback_used),
            "reason": result.fallback_reason,
            "streak": int(result.fallback_streak),
        },
        "price_as_of": result.price_as_of,
        "universe_as_of": result.universe_as_of,
        "universe_rows": int(result.universe_rows),
        "price_rows": int(result.price_rows),
        "symbol_count": int(result.symbol_count),
        "download_start": result.download_start,
        "missing_symbol_count": int(result.missing_symbol_count),
        "missing_price_backfill_warning": result.missing_price_backfill_warning,
        "historical_universe_refresh_warning": result.historical_universe_refresh_warning,
        "latest_universe_refresh_warning": result.latest_universe_refresh_warning,
        "artifacts": {
            "universe_history": _file_artifact(
                result.universe_history_path,
                row_count=result.universe_rows,
                as_of=result.universe_as_of,
            ),
            "price_history": _file_artifact(
                result.price_history_path,
                row_count=result.price_rows,
                as_of=result.price_as_of,
            ),
            "symbol_aliases": _file_artifact(
                result.alias_output_path,
                row_count=len(read_table(result.alias_output_path)) if result.alias_output_path.exists() else 0,
            ),
            "universe_snapshot_metadata": _file_artifact(
                result.metadata_output_path,
                row_count=len(universe_metadata),
                as_of=_latest_date(universe_metadata, ("snapshot_date", "as_of_date", "requested_date")),
            ),
            "latest_holdings_snapshot": _file_artifact(
                result.latest_snapshot_output_path,
                row_count=len(read_table(result.latest_snapshot_output_path))
                if result.latest_snapshot_output_path.exists()
                else 0,
                as_of=result.universe_as_of,
            ),
        },
        "producer": {
            "repository": os.environ.get("GITHUB_REPOSITORY", ""),
            "git_sha": os.environ.get("GITHUB_SHA", ""),
            "github_run_id": os.environ.get("GITHUB_RUN_ID", ""),
            "github_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", ""),
            "workflow": os.environ.get("GITHUB_WORKFLOW", ""),
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    return write_json(result.source_manifest_output_path, payload)


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


def load_symbol_alias_table(path: str | Path) -> dict[str, list[str]]:
    alias_path = Path(path)
    if not alias_path.exists():
        return {}
    frame = read_table(alias_path)
    if frame.empty:
        return {}
    required = {"symbol", "download_candidate"}
    missing = required - set(frame.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"symbol alias table missing required columns: {missing_text}")
    normalized = frame.copy()
    normalized["symbol"] = normalized["symbol"].astype(str).str.upper().str.strip()
    normalized["download_candidate"] = normalized["download_candidate"].astype(str).str.upper().str.strip()
    if "priority" in normalized.columns:
        normalized["priority"] = pd.to_numeric(normalized["priority"], errors="coerce").fillna(999999)
    else:
        normalized["priority"] = 999999
    aliases: dict[str, list[str]] = {}
    for symbol, group in normalized.sort_values(["symbol", "priority", "download_candidate"]).groupby("symbol"):
        candidates = [
            candidate
            for candidate in group["download_candidate"].tolist()
            if candidate and candidate.lower() != "nan"
        ]
        if candidates:
            aliases[str(symbol)] = list(dict.fromkeys(candidates))
    return aliases


def _load_existing_universe_inputs(existing_input_dir: str | Path):
    root = Path(existing_input_dir)
    required_paths = {
        "universe_history": root / "r1000_universe_history.csv",
        "metadata": root / "r1000_universe_snapshot_metadata.csv",
        "latest_snapshot": root / "r1000_latest_holdings_snapshot.csv",
        "aliases": root / "r1000_symbol_aliases.csv",
    }
    missing = [str(path) for path in required_paths.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"missing existing Russell 1000 input files: {', '.join(missing)}")
    universe_history = read_table(required_paths["universe_history"])
    if universe_history.empty:
        raise ValueError("existing universe history is empty")
    return {
        "universe_history": universe_history,
        "metadata": read_table(required_paths["metadata"]),
        "latest_snapshot": read_table(required_paths["latest_snapshot"]),
        "alias_table": read_table(required_paths["aliases"]),
        "symbol_aliases": load_symbol_alias_table(required_paths["aliases"]),
    }


def _advance_existing_universe_history(
    existing_history: pd.DataFrame,
    *,
    snapshot_date,
    latest_snapshot: pd.DataFrame,
) -> pd.DataFrame:
    history = pd.DataFrame(existing_history).copy()
    required = {"symbol", "sector", "start_date", "end_date"}
    missing = required - set(history.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"existing universe history missing required columns: {missing_text}")
    if history.empty:
        raise ValueError("existing universe history is empty")

    latest_date = pd.Timestamp(snapshot_date).tz_localize(None).normalize()
    history["symbol"] = history["symbol"].astype(str).str.upper().str.strip()
    history["sector"] = history["sector"].fillna("unknown").astype(str).str.strip().replace("", "unknown")
    history["start_date"] = pd.to_datetime(history["start_date"], errors="coerce").dt.tz_localize(None).dt.normalize()
    history["end_date"] = pd.to_datetime(history["end_date"], errors="coerce").dt.tz_localize(None).dt.normalize()
    if history["start_date"].isna().any():
        raise ValueError("existing universe history contains invalid start_date values")

    open_mask = history["end_date"].isna()
    latest_open_start = history.loc[open_mask, "start_date"].max() if open_mask.any() else pd.NaT
    if pd.notna(latest_open_start) and latest_date <= pd.Timestamp(latest_open_start).normalize():
        raise RuntimeError(
            "latest iShares holdings snapshot did not advance existing universe: "
            f"latest={latest_date.date()} existing={pd.Timestamp(latest_open_start).date()}"
        )

    history.loc[open_mask, "end_date"] = latest_date - pd.Timedelta(days=1)
    latest_history = build_interval_universe_history([(latest_date, latest_snapshot)])
    combined = pd.concat([history, latest_history], ignore_index=True)
    return (
        combined.loc[:, ["symbol", "sector", "start_date", "end_date"]]
        .sort_values(["symbol", "start_date"])
        .reset_index(drop=True)
    )


def _merge_symbol_alias_tables(*tables: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for table in tables:
        if table is None or table.empty:
            continue
        frame = pd.DataFrame(table).copy()
        required = {"symbol", "download_candidate"}
        missing = required - set(frame.columns)
        if missing:
            missing_text = ", ".join(sorted(missing))
            raise ValueError(f"symbol alias table missing required columns: {missing_text}")
        frame["symbol"] = frame["symbol"].astype(str).str.upper().str.strip()
        frame["download_candidate"] = frame["download_candidate"].astype(str).str.upper().str.strip()
        if "priority" in frame.columns:
            frame["priority"] = pd.to_numeric(frame["priority"], errors="coerce").fillna(999999).astype(int)
        else:
            frame["priority"] = 999999
        frames.append(frame.loc[:, ["symbol", "download_candidate", "priority"]])

    if not frames:
        return pd.DataFrame(columns=["symbol", "download_candidate", "priority"])

    merged = pd.concat(frames, ignore_index=True)
    valid = merged["symbol"].ne("") & merged["download_candidate"].ne("") & merged["download_candidate"].ne("NAN")
    return (
        merged.loc[valid]
        .sort_values(["symbol", "priority", "download_candidate"])
        .drop_duplicates(subset=["symbol", "download_candidate"], keep="first")
        .reset_index(drop=True)
    )


def _append_latest_refresh_metadata(
    metadata: pd.DataFrame,
    *,
    record: Mapping[str, object],
    history_refresh_error: str,
) -> pd.DataFrame:
    requested_date = pd.Timestamp(record["requested_date"]).normalize()
    as_of_date = pd.Timestamp(record["as_of_date"]).normalize()
    latest_row = {
        "requested_date": requested_date.date().isoformat(),
        "as_of_date": as_of_date.date().isoformat(),
        "source_kind": str(record.get("source_kind") or "official_json_latest_after_history_failure"),
        "lookback_days": record.get("lookback_days"),
        "source_url": str(record["source_url"]),
        "row_count": int(len(record["snapshot"])),
        "refresh_note": "historical_refresh_failed_latest_refreshed",
        "history_refresh_error": history_refresh_error,
        "latest_refresh_warning": record.get("warning"),
    }
    return pd.concat([pd.DataFrame(metadata).copy(), pd.DataFrame([latest_row])], ignore_index=True)


def _resolve_latest_holdings_snapshot(
    *,
    universe_start: str,
    universe_end: str | None,
    max_lookback_days: int,
) -> dict[str, object]:
    requested_dates = build_monthly_snapshot_request_dates(universe_start, universe_end)
    requested_date = max(requested_dates)
    try:
        record = resolve_ishares_holdings_snapshot(
            requested_date,
            max_lookback_days=max_lookback_days,
        )
    except Exception as exc:
        as_of_date, snapshot = download_companies_marketcap_iwb_holdings_snapshot()
        return {
            "requested_date": requested_date,
            "as_of_date": as_of_date,
            "lookback_days": pd.NA,
            "source_kind": "companies_marketcap_html",
            "source_url": COMPANIES_MARKETCAP_IWB_HOLDINGS_URL,
            "snapshot": snapshot,
            "warning": f"official_latest_failed={type(exc).__name__}: {exc}",
        }
    record["source_kind"] = "official_json_latest_after_history_failure"
    return record


def prepare_russell_1000_input_data(
    *,
    output_dir: str | Path,
    universe_start: str,
    universe_end: str | None = None,
    price_start: str = "2018-01-01",
    price_end: str | None = None,
    universe_backfill_start: str | None = None,
    max_lookback_days: int = 7,
    existing_input_dir: str | Path | None = None,
    existing_prices_path: str | Path | None = None,
    existing_source_manifest_path: str | Path | None = None,
    max_universe_fallback_streak: int = 1,
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
    latest_snapshot_output_path = root / "r1000_latest_holdings_snapshot.csv"
    source_manifest_output_path = root / SOURCE_INPUT_MANIFEST_FILENAME

    universe_fallback_used = False
    fallback_reason = None
    historical_universe_refresh_warning = None
    latest_universe_refresh_warning = None
    try:
        snapshot_tables, metadata = download_ishares_historical_universe_snapshots(
            start_date=universe_start,
            end_date=universe_end,
            max_lookback_days=max_lookback_days,
        )
        for as_of_date, snapshot in snapshot_tables:
            write_table(snapshot, snapshot_dir / f"r1000_{pd.Timestamp(as_of_date):%Y-%m-%d}.csv")
        write_table(metadata, metadata_output_path)
        _latest_snapshot_date, latest_snapshot = max(
            snapshot_tables,
            key=lambda item: pd.Timestamp(item[0]).normalize(),
        )
        write_table(latest_snapshot, latest_snapshot_output_path)

        universe_history = build_interval_universe_history(snapshot_tables)
        if universe_backfill_start:
            universe_history = backfill_universe_history_start(universe_history, universe_backfill_start)
        write_table(universe_history, universe_history_path)

        symbol_aliases = build_symbol_alias_candidates(snapshot_tables)
        write_table(build_symbol_alias_table(symbol_aliases), alias_output_path)
    except Exception as exc:
        if not existing_input_dir:
            raise
        existing_inputs = _load_existing_universe_inputs(existing_input_dir)
        history_refresh_error = f"{type(exc).__name__}: {exc}"
        try:
            latest_record = _resolve_latest_holdings_snapshot(
                universe_start=universe_start,
                universe_end=universe_end,
                max_lookback_days=max_lookback_days,
            )
            latest_snapshot_date = pd.Timestamp(latest_record["as_of_date"]).normalize()
            latest_snapshot = pd.DataFrame(latest_record["snapshot"]).copy()
            universe_history = _advance_existing_universe_history(
                existing_inputs["universe_history"],
                snapshot_date=latest_snapshot_date,
                latest_snapshot=latest_snapshot,
            )
            metadata = _append_latest_refresh_metadata(
                existing_inputs["metadata"],
                record=latest_record,
                history_refresh_error=history_refresh_error,
            )
            fresh_alias_table = build_symbol_alias_table(
                build_symbol_alias_candidates([(latest_snapshot_date, latest_snapshot)])
            )
            alias_table = _merge_symbol_alias_tables(existing_inputs["alias_table"], fresh_alias_table)

            write_table(latest_snapshot, snapshot_dir / f"r1000_{latest_snapshot_date:%Y-%m-%d}.csv")
            write_table(universe_history, universe_history_path)
            write_table(metadata, metadata_output_path)
            write_table(latest_snapshot, latest_snapshot_output_path)
            write_table(alias_table, alias_output_path)
            symbol_aliases = load_symbol_alias_table(alias_output_path)
            historical_universe_refresh_warning = history_refresh_error
            latest_universe_refresh_warning = (
                str(latest_record.get("warning")) if latest_record.get("warning") else None
            )
        except Exception as latest_exc:
            universe_fallback_used = True
            fallback_reason = (
                f"{history_refresh_error}; latest_refresh_failed={type(latest_exc).__name__}: {latest_exc}"
            )
            fallback_streak = _source_fallback_streak(
                existing_source_manifest_path=existing_source_manifest_path,
                fallback_used=True,
            )
            if fallback_streak > int(max_universe_fallback_streak):
                raise RuntimeError(
                    "universe_fallback_streak_exceeded:"
                    f"streak={fallback_streak} max={int(max_universe_fallback_streak)} "
                    f"reason={fallback_reason}"
                ) from exc
            universe_history = existing_inputs["universe_history"]
            metadata = existing_inputs["metadata"]
            latest_snapshot = existing_inputs["latest_snapshot"]
            write_table(universe_history, universe_history_path)
            write_table(metadata, metadata_output_path)
            write_table(latest_snapshot, latest_snapshot_output_path)
            write_table(existing_inputs["alias_table"], alias_output_path)
            symbol_aliases = existing_inputs["symbol_aliases"]

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
    missing_price_backfill_warning = None
    if missing_symbols and update_start != pd.Timestamp(price_start).strftime("%Y-%m-%d"):
        try:
            missing_prices = download_price_history(
                missing_symbols,
                start=price_start,
                end=price_end,
                chunk_size=chunk_size,
                symbol_aliases=symbol_aliases,
            )
        except RuntimeError as exc:
            if "No price history downloaded" not in str(exc):
                raise
            missing_price_backfill_warning = f"{type(exc).__name__}: {exc}"
        else:
            price_frames.append(missing_prices)
    merged_prices = merge_price_history(*price_frames)
    write_table(merged_prices, price_history_path)

    fallback_streak = _source_fallback_streak(
        existing_source_manifest_path=existing_source_manifest_path,
        fallback_used=universe_fallback_used,
    )
    price_as_of = _latest_date(merged_prices, ("as_of",))
    universe_as_of = _latest_date(metadata, ("snapshot_date", "as_of_date"))
    if universe_as_of is None:
        universe_as_of = _latest_date(latest_snapshot, ("snapshot_date", "as_of_date", "as_of"))
    if universe_as_of is None:
        universe_as_of = _latest_date(universe_history, ("start_date",))

    result = Russell1000InputDataResult(
        output_dir=root,
        universe_history_path=universe_history_path,
        price_history_path=price_history_path,
        alias_output_path=alias_output_path,
        metadata_output_path=metadata_output_path,
        latest_snapshot_output_path=latest_snapshot_output_path,
        source_manifest_output_path=source_manifest_output_path,
        snapshot_dir=snapshot_dir,
        universe_rows=int(len(universe_history)),
        price_rows=int(len(merged_prices)),
        symbol_count=int(len(symbols)),
        download_start=update_start,
        missing_symbol_count=int(len(missing_symbols)),
        universe_fallback_used=universe_fallback_used,
        fallback_reason=fallback_reason,
        fallback_streak=fallback_streak,
        price_as_of=price_as_of,
        universe_as_of=universe_as_of,
        missing_price_backfill_warning=missing_price_backfill_warning,
        historical_universe_refresh_warning=historical_universe_refresh_warning,
        latest_universe_refresh_warning=latest_universe_refresh_warning,
    )
    _write_source_input_manifest(result=result, universe_metadata=metadata)
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare Russell 1000 source input data for snapshot pipelines.")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--universe-start", default="2018-01-01")
    parser.add_argument("--universe-end")
    parser.add_argument("--price-start", default="2018-01-01")
    parser.add_argument("--price-end")
    parser.add_argument("--universe-backfill-start")
    parser.add_argument("--max-lookback-days", type=int, default=7)
    parser.add_argument("--existing-input-dir")
    parser.add_argument("--existing-prices")
    parser.add_argument("--existing-source-manifest")
    parser.add_argument("--max-universe-fallback-streak", type=int, default=1)
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
        existing_input_dir=args.existing_input_dir,
        existing_prices_path=args.existing_prices,
        existing_source_manifest_path=args.existing_source_manifest,
        max_universe_fallback_streak=args.max_universe_fallback_streak,
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
    print(f"wrote latest weighted holdings snapshot -> {result.latest_snapshot_output_path}")
    print(f"wrote source input manifest -> {result.source_manifest_output_path}")
    print(
        f"downloaded/updated {result.symbol_count} symbols from {result.download_start}; "
        f"missing_full_history={result.missing_symbol_count}"
    )
    if result.universe_fallback_used:
        print("reused existing universe inputs after upstream holdings refresh failed")
    if result.historical_universe_refresh_warning:
        print(f"warning: historical universe refresh incomplete: {result.historical_universe_refresh_warning}")
    if result.latest_universe_refresh_warning:
        print(f"warning: latest universe refresh used secondary source: {result.latest_universe_refresh_warning}")
    if result.missing_price_backfill_warning:
        print(f"warning: missing price backfill skipped: {result.missing_price_backfill_warning}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
