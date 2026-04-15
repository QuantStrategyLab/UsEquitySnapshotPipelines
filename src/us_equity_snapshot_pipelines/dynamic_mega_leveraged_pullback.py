from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import pandas as pd

from .artifacts import write_json, write_release_status_summary, write_snapshot_manifest
from .contracts import DYNAMIC_MEGA_LEVERAGED_PULLBACK_PROFILE, SnapshotProfileContract, get_profile_contract
from .dynamic_mega_universe import (
    normalize_price_history,
    ranked_active_dynamic_universe,
    resolve_effective_as_of_date,
)
from .russell_1000_multi_factor_defensive_snapshot import read_table, write_table

DEFAULT_CANDIDATE_UNIVERSE_SIZE = 15
DEFAULT_PRODUCT_LEVERAGE = 2.0
DEFAULT_PRODUCT_LEVERAGE_TOLERANCE = 0.05
DEFAULT_PRODUCT_EXPENSE_RATIO = 0.01
DEFAULT_BENCHMARK_SYMBOL = "QQQ"
DEFAULT_SAFE_HAVEN = "BOXX"


@dataclass(frozen=True)
class DynamicMegaLeveragedPullbackBuildResult:
    snapshot_path: Path
    manifest_path: Path
    ranking_path: Path
    release_summary_path: Path
    row_count: int
    product_mapped_count: int


def _normalize_symbol(value: object) -> str:
    return str(value or "").strip().upper()


def _coerce_bool(value: object, *, default: bool) -> bool:
    if value is None or pd.isna(value):
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    return default if not normalized else True


def _normalize_product_map(product_map) -> pd.DataFrame:
    if product_map is None:
        raise ValueError("product_map is required for dynamic_mega_leveraged_pullback")
    frame = pd.DataFrame(product_map).copy()
    if frame.empty:
        raise ValueError("product_map must contain at least one row")

    rename: dict[str, str] = {}
    normalized_columns = {str(column).strip().lower(): column for column in frame.columns}
    for canonical, candidates in {
        "underlying_symbol": ("underlying_symbol", "underlying", "source_symbol", "stock_symbol", "stock", "symbol"),
        "trade_symbol": ("trade_symbol", "product_symbol", "leveraged_symbol", "etf_symbol", "target_symbol"),
        "product_leverage": ("product_leverage", "leverage", "leverage_multiple"),
        "product_expense_ratio": ("product_expense_ratio", "expense_ratio", "expense_rate"),
        "product_available": ("product_available", "available", "enabled", "tradable"),
    }.items():
        match = next((normalized_columns[name] for name in candidates if name in normalized_columns), None)
        if match is not None:
            rename[match] = canonical
    frame = frame.rename(columns=rename)
    if "underlying_symbol" not in frame.columns:
        raise ValueError("product_map missing required column: underlying_symbol")
    if "trade_symbol" not in frame.columns:
        raise ValueError("product_map missing required column: trade_symbol")
    if "product_leverage" not in frame.columns:
        frame["product_leverage"] = DEFAULT_PRODUCT_LEVERAGE
    if "product_expense_ratio" not in frame.columns:
        frame["product_expense_ratio"] = DEFAULT_PRODUCT_EXPENSE_RATIO
    if "product_available" not in frame.columns:
        frame["product_available"] = True

    frame["underlying_symbol"] = frame["underlying_symbol"].map(_normalize_symbol)
    frame["trade_symbol"] = frame["trade_symbol"].map(_normalize_symbol)
    frame["product_leverage"] = pd.to_numeric(frame["product_leverage"], errors="coerce").fillna(DEFAULT_PRODUCT_LEVERAGE)
    frame["product_expense_ratio"] = pd.to_numeric(frame["product_expense_ratio"], errors="coerce").fillna(
        DEFAULT_PRODUCT_EXPENSE_RATIO
    )
    frame["product_available"] = frame["product_available"].map(lambda value: _coerce_bool(value, default=True))
    frame = frame.loc[frame["underlying_symbol"].ne("")].copy()
    frame = frame.drop_duplicates(subset=["underlying_symbol"], keep="first").reset_index(drop=True)

    available = frame.loc[frame["product_available"]].copy()
    missing_symbols = available.loc[available["trade_symbol"].eq(""), "underlying_symbol"].astype(str).tolist()
    if missing_symbols:
        raise ValueError(f"product_map available rows missing trade_symbol: {', '.join(missing_symbols)}")
    invalid_leverage = available.loc[
        (available["product_leverage"] - DEFAULT_PRODUCT_LEVERAGE).abs() > DEFAULT_PRODUCT_LEVERAGE_TOLERANCE,
        "underlying_symbol",
    ].astype(str).tolist()
    if invalid_leverage:
        raise ValueError(
            "product_map available rows must be 2x long products; invalid leverage for: "
            + ", ".join(invalid_leverage)
        )
    return frame


def build_feature_snapshot(
    *,
    price_history: pd.DataFrame,
    universe_snapshot: pd.DataFrame,
    product_map: pd.DataFrame | None,
    as_of_date: str | None = None,
    candidate_universe_size: int = DEFAULT_CANDIDATE_UNIVERSE_SIZE,
) -> pd.DataFrame:
    prices = normalize_price_history(price_history)
    effective_as_of_date = resolve_effective_as_of_date(prices, as_of_date)
    active = ranked_active_dynamic_universe(
        universe_snapshot,
        as_of_date=effective_as_of_date,
        universe_size=int(candidate_universe_size),
    )
    product_frame = _normalize_product_map(product_map)
    product_by_underlying = {
        str(row.underlying_symbol): row
        for row in product_frame.itertuples(index=False)
    }

    rows: list[dict[str, object]] = []
    for rank, row in enumerate(active.itertuples(index=False), start=1):
        underlying = _normalize_symbol(getattr(row, "symbol"))
        product = product_by_underlying.get(underlying)
        trade_symbol = _normalize_symbol(getattr(product, "trade_symbol", "")) if product is not None else ""
        product_available = bool(getattr(product, "product_available", False)) if product is not None else False
        rows.append(
            {
                "as_of": effective_as_of_date.date().isoformat(),
                "symbol": trade_symbol,
                "trade_symbol": trade_symbol,
                "underlying_symbol": underlying,
                "sector": str(getattr(row, "sector", "unknown") or "unknown"),
                "candidate_rank": rank,
                "mega_rank": rank,
                "product_leverage": float(getattr(product, "product_leverage", DEFAULT_PRODUCT_LEVERAGE))
                if product is not None
                else DEFAULT_PRODUCT_LEVERAGE,
                "product_expense_ratio": float(
                    getattr(product, "product_expense_ratio", DEFAULT_PRODUCT_EXPENSE_RATIO)
                )
                if product is not None
                else DEFAULT_PRODUCT_EXPENSE_RATIO,
                "product_available": product_available and bool(trade_symbol),
                "eligible": product_available and bool(trade_symbol),
            }
        )
    return pd.DataFrame(rows)


def _build_signal_description(snapshot: pd.DataFrame) -> str:
    mapped = snapshot.loc[snapshot["product_available"]]
    preview = ", ".join(
        f"{row.underlying_symbol}->{row.symbol}"
        for row in mapped.head(5).itertuples(index=False)
    )
    return f"candidate_pool={len(snapshot)} product_mapped={len(mapped)} top={preview}"


def _write_manifest_config(output_dir: Path, *, snapshot: pd.DataFrame, product_map_path: str | Path | None) -> Path:
    config_path = output_dir / "dynamic_mega_leveraged_pullback_snapshot_config.json"
    payload: Mapping[str, object] = {
        "profile": DYNAMIC_MEGA_LEVERAGED_PULLBACK_PROFILE,
        "candidate_universe_size": int(len(snapshot)),
        "benchmark_symbol": DEFAULT_BENCHMARK_SYMBOL,
        "safe_haven": DEFAULT_SAFE_HAVEN,
        "product_map_path": str(product_map_path) if product_map_path is not None else None,
        "product_mapped_count": int(snapshot["product_available"].sum()) if "product_available" in snapshot.columns else 0,
    }
    return write_json(config_path, payload)


def build_artifacts(
    *,
    prices_path: str | Path,
    universe_path: str | Path,
    output_dir: str | Path,
    product_map_path: str | Path | None = None,
    as_of_date: str | None = None,
    snapshot_output: str | Path | None = None,
    manifest_output: str | Path | None = None,
    ranking_output: str | Path | None = None,
    release_summary_output: str | Path | None = None,
    candidate_universe_size: int = DEFAULT_CANDIDATE_UNIVERSE_SIZE,
) -> DynamicMegaLeveragedPullbackBuildResult:
    contract = get_profile_contract(DYNAMIC_MEGA_LEVERAGED_PULLBACK_PROFILE)
    paths = contract.artifact_paths(output_dir)
    output_root = Path(output_dir)
    snapshot_path = Path(snapshot_output) if snapshot_output else paths["snapshot"]
    manifest_path = Path(manifest_output) if manifest_output else paths["manifest"]
    ranking_path = Path(ranking_output) if ranking_output else paths["ranking"]
    release_summary_path = Path(release_summary_output) if release_summary_output else paths["release_summary"]

    if product_map_path is None or not str(product_map_path).strip():
        raise ValueError("product_map_path is required for dynamic_mega_leveraged_pullback")
    product_map = read_table(product_map_path) if product_map_path else None
    snapshot = build_feature_snapshot(
        price_history=read_table(prices_path),
        universe_snapshot=read_table(universe_path),
        product_map=product_map,
        as_of_date=as_of_date,
        candidate_universe_size=int(candidate_universe_size),
    )
    write_table(snapshot, snapshot_path)
    ranking = snapshot.sort_values(["candidate_rank", "underlying_symbol"]).reset_index(drop=True)
    write_table(ranking, ranking_path)
    config_path = _write_manifest_config(output_root, snapshot=snapshot, product_map_path=product_map_path)
    write_snapshot_manifest(
        contract=contract,
        snapshot_path=snapshot_path,
        snapshot=snapshot,
        config_path=config_path,
        manifest_path=manifest_path,
        config_name=contract.profile,
    )
    product_mapped_count = int(snapshot["product_available"].sum()) if "product_available" in snapshot.columns else 0
    signal_description = _build_signal_description(snapshot)
    status_description = f"candidate_pool={len(snapshot)} | product_mapped={product_mapped_count}"
    write_release_status_summary(
        contract=contract,
        snapshot_path=snapshot_path,
        manifest_path=manifest_path,
        ranking_path=ranking_path,
        summary_path=release_summary_path,
        snapshot=snapshot,
        signal_description=signal_description,
        status_description=status_description,
        diagnostics={
            "candidate_universe_size": int(candidate_universe_size),
            "product_mapped_count": product_mapped_count,
            "candidate_underlyings": tuple(snapshot["underlying_symbol"].astype(str).tolist()),
            "trade_symbols": tuple(snapshot["symbol"].astype(str).tolist()),
        },
    )
    return DynamicMegaLeveragedPullbackBuildResult(
        snapshot_path=snapshot_path,
        manifest_path=manifest_path,
        ranking_path=ranking_path,
        release_summary_path=release_summary_path,
        row_count=int(len(snapshot)),
        product_mapped_count=product_mapped_count,
    )


def build_parser() -> argparse.ArgumentParser:
    contract: SnapshotProfileContract = get_profile_contract(DYNAMIC_MEGA_LEVERAGED_PULLBACK_PROFILE)
    parser = argparse.ArgumentParser(description="Build dynamic_mega_leveraged_pullback monthly candidate snapshot.")
    parser.add_argument("--prices", required=True, help="Input price history file (.csv/.json/.jsonl/.parquet)")
    parser.add_argument("--universe", required=True, help="Ranked mega-cap/Russell holdings universe file")
    parser.add_argument("--product-map", dest="product_map_path", required=True, help="Required underlying-to-2x-product map")
    parser.add_argument("--output-dir", required=True, help="Directory for standard artifact filenames")
    parser.add_argument("--snapshot-output", help=f"Snapshot output path; default: <output-dir>/{contract.snapshot_filename}")
    parser.add_argument("--manifest-output", help=f"Manifest output path; default: <output-dir>/{contract.manifest_filename}")
    parser.add_argument("--ranking-output", help=f"Ranking output path; default: <output-dir>/{contract.ranking_filename}")
    parser.add_argument("--release-summary-output", help="Release summary output path; default: <output-dir>/release_status_summary.json")
    parser.add_argument("--as-of", dest="as_of_date", help="Snapshot date; defaults to latest price date")
    parser.add_argument("--candidate-universe-size", type=int, default=DEFAULT_CANDIDATE_UNIVERSE_SIZE)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_artifacts(
        prices_path=args.prices,
        universe_path=args.universe,
        output_dir=args.output_dir,
        product_map_path=args.product_map_path,
        as_of_date=args.as_of_date,
        snapshot_output=args.snapshot_output,
        manifest_output=args.manifest_output,
        ranking_output=args.ranking_output,
        release_summary_output=args.release_summary_output,
        candidate_universe_size=args.candidate_universe_size,
    )
    print(f"wrote {result.row_count} rows -> {result.snapshot_path}")
    print(f"wrote manifest -> {result.manifest_path}")
    print(f"wrote ranking -> {result.ranking_path}")
    print(f"wrote release summary -> {result.release_summary_path}")
    print(f"product mapped -> {result.product_mapped_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
