from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from ..artifacts import build_snapshot_input_metadata, write_release_status_summary, write_snapshot_manifest
from ..contracts import NEW_R1000_RESIDUAL_STRENGTH_20_PROFILE, get_profile_contract
from .mega_cap_leader_rotation_backtest import (
    _normalize_price_history as _normalize_r1000_price_history,
    _normalize_universe as _normalize_universe_snapshot,
    _precompute_symbol_feature_history,
    build_feature_snapshot_for_backtest as _build_feature_snapshot_for_backtest,
    resolve_active_universe,
)
from .russell_1000_multi_factor_defensive_snapshot import read_table, write_table
from ..research.us_equity_strategy_candidates import SNAPSHOT_CANDIDATES, SnapshotCandidateSpec, _build_new_snapshot_target_weights

DEFAULT_MIN_PRICE_USD = 10.0
DEFAULT_MIN_ADV20_USD = 20_000_000.0
DEFAULT_MIN_HISTORY_DAYS = 273


def _resolve_target_spec() -> SnapshotCandidateSpec:
    for spec in SNAPSHOT_CANDIDATES:
        if spec.candidate_id == NEW_R1000_RESIDUAL_STRENGTH_20_PROFILE:
            return spec
    raise ValueError(f"target snapshot candidate spec not found: {NEW_R1000_RESIDUAL_STRENGTH_20_PROFILE}")


TARGET_SPEC = _resolve_target_spec()


@dataclass(frozen=True)
class ResidualStrength20BuildResult:
    snapshot_path: Path
    manifest_path: Path
    ranking_path: Path
    release_summary_path: Path
    row_count: int
    selected_symbols: tuple[str, ...]


def _resolve_effective_as_of_date(price_history: pd.DataFrame, as_of_date: str | None) -> pd.Timestamp:
    if price_history.empty or "as_of" not in price_history.columns:
        raise ValueError("price_history must contain as_of")
    latest = pd.to_datetime(price_history["as_of"], utc=False).dt.tz_localize(None).dt.normalize().max()
    if pd.isna(latest):
        raise ValueError("price_history as_of has no usable dates")
    latest = pd.Timestamp(latest).normalize()
    if as_of_date:
        resolved = pd.Timestamp(as_of_date).normalize()
        if resolved > latest:
            raise ValueError(
                f"as_of_date cannot be later than latest price history row: as_of_date={resolved:%Y-%m-%d} latest={latest:%Y-%m-%d}"
            )
        return resolved
    return latest


def _normalize_holdings(current_holdings: Iterable[str] | None) -> set[str]:
    normalized: set[str] = set()
    for value in current_holdings or ():
        symbol = str(value or "").strip().upper()
        if symbol:
            normalized.add(symbol)
    return normalized


def _build_signal_description(metadata: dict[str, object], ranking: pd.DataFrame) -> str:
    top_preview = ", ".join(
        f"{row.symbol}({float(row.score):.2f})"
        for row in ranking.head(5).itertuples(index=False)
        if not pd.isna(row.score)
    )
    return (
        f"regime={metadata['regime']} breadth={float(metadata['breadth_ratio']):.1%} "
        f"target_stock={float(metadata['stock_exposure']):.1%} "
        f"selected={len(metadata['selected_symbols'])} top={top_preview or 'none'}"
    )


def build_artifacts(
    *,
    prices_path: str | Path,
    universe_path: str | Path,
    output_dir: str | Path,
    as_of_date: str | None = None,
    snapshot_output: str | Path | None = None,
    manifest_output: str | Path | None = None,
    ranking_output: str | Path | None = None,
    release_summary_output: str | Path | None = None,
    source_input_manifest_path: str | Path | None = None,
    current_holdings: Iterable[str] | None = None,
    min_price_usd: float = DEFAULT_MIN_PRICE_USD,
    min_adv20_usd: float = DEFAULT_MIN_ADV20_USD,
    min_history_days: int = DEFAULT_MIN_HISTORY_DAYS,
) -> ResidualStrength20BuildResult:
    contract = get_profile_contract(NEW_R1000_RESIDUAL_STRENGTH_20_PROFILE)
    paths = contract.artifact_paths(output_dir)
    snapshot_path = Path(snapshot_output) if snapshot_output else paths["snapshot"]
    manifest_path = Path(manifest_output) if manifest_output else paths["manifest"]
    ranking_path = Path(ranking_output) if ranking_output else paths["ranking"]
    release_summary_path = Path(release_summary_output) if release_summary_output else paths["release_summary"]

    price_history = _normalize_r1000_price_history(read_table(prices_path))
    effective_as_of_date = _resolve_effective_as_of_date(price_history, as_of_date)
    universe_snapshot = _normalize_universe_snapshot(read_table(universe_path))
    active_universe = resolve_active_universe(universe_snapshot, effective_as_of_date)
    if active_universe.empty:
        raise ValueError(f"universe has no active rows for as_of_date={effective_as_of_date:%Y-%m-%d}")

    feature_history_by_symbol = _precompute_symbol_feature_history(
        price_history,
        benchmark_symbol=TARGET_SPEC.benchmark_symbol,
        broad_benchmark_symbol=TARGET_SPEC.benchmark_symbol,
    )
    snapshot = _build_feature_snapshot_for_backtest(
        effective_as_of_date,
        active_universe,
        feature_history_by_symbol,
        benchmark_symbol=TARGET_SPEC.benchmark_symbol,
        broad_benchmark_symbol=TARGET_SPEC.benchmark_symbol,
        safe_haven=TARGET_SPEC.safe_symbol,
        min_price_usd=float(min_price_usd),
        min_adv20_usd=float(min_adv20_usd),
        min_history_days=int(min_history_days),
    )
    write_table(snapshot, snapshot_path)

    target_weights, ranked, metadata = _build_new_snapshot_target_weights(
        snapshot,
        _normalize_holdings(current_holdings),
        TARGET_SPEC,
    )
    ranking = ranked.copy()
    if not ranking.empty:
        ranking.insert(0, "rank", range(1, len(ranking) + 1))
        selected_symbols = set(str(symbol) for symbol in metadata.get("selected_symbols", ()))
        ranking["selected"] = ranking["symbol"].astype(str).isin(selected_symbols)
        ranking["target_weight"] = ranking["symbol"].astype(str).map(target_weights).fillna(0.0)
    write_table(ranking, ranking_path)

    signal_description = _build_signal_description(metadata, ranking)
    status_description = (
        f"regime={metadata['regime']} | selected={len(metadata['selected_symbols'])} | "
        f"stock_exposure={float(metadata['stock_exposure']):.1%}"
    )
    diagnostics = {
        **metadata,
        "candidate_id": TARGET_SPEC.candidate_id,
        "candidate_group": TARGET_SPEC.candidate_group,
        "rule": TARGET_SPEC.rule,
        "selected_count": int(len(metadata.get("selected_symbols", ()))),
        "target_weights": target_weights,
        "managed_symbols": tuple(dict.fromkeys([*metadata.get("selected_symbols", ()), TARGET_SPEC.safe_symbol.upper()])),
    }
    write_snapshot_manifest(
        contract=contract,
        snapshot_path=snapshot_path,
        snapshot=snapshot,
        config_path=None,
        manifest_path=manifest_path,
        config_name=contract.profile,
        input_metadata=build_snapshot_input_metadata(
            prices_path=prices_path,
            universe_path=universe_path,
            price_history=price_history,
            universe=universe_snapshot,
            source_input_manifest_path=source_input_manifest_path,
        ),
    )
    write_release_status_summary(
        contract=contract,
        snapshot_path=snapshot_path,
        manifest_path=manifest_path,
        ranking_path=ranking_path,
        summary_path=release_summary_path,
        snapshot=snapshot,
        signal_description=signal_description,
        status_description=status_description,
        diagnostics=diagnostics,
    )
    return ResidualStrength20BuildResult(
        snapshot_path=snapshot_path,
        manifest_path=manifest_path,
        ranking_path=ranking_path,
        release_summary_path=release_summary_path,
        row_count=int(len(snapshot)),
        selected_symbols=tuple(str(symbol) for symbol in metadata.get("selected_symbols", ())),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build runtime-facing snapshot artifacts for new_r1000_residual_strength_20.")
    parser.add_argument("--prices", required=True)
    parser.add_argument("--universe", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--as-of")
    parser.add_argument("--source-input-manifest")
    parser.add_argument("--current-holdings", default="")
    parser.add_argument("--min-price-usd", type=float, default=DEFAULT_MIN_PRICE_USD)
    parser.add_argument("--min-adv20-usd", type=float, default=DEFAULT_MIN_ADV20_USD)
    parser.add_argument("--min-history-days", type=int, default=DEFAULT_MIN_HISTORY_DAYS)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_artifacts(
        prices_path=args.prices,
        universe_path=args.universe,
        output_dir=args.output_dir,
        as_of_date=args.as_of,
        source_input_manifest_path=args.source_input_manifest,
        current_holdings=[item.strip() for item in str(args.current_holdings or "").split(",") if item.strip()],
        min_price_usd=float(args.min_price_usd),
        min_adv20_usd=float(args.min_adv20_usd),
        min_history_days=int(args.min_history_days),
    )
    print(
        json.dumps(
            {
                "strategy_profile": NEW_R1000_RESIDUAL_STRENGTH_20_PROFILE,
                "snapshot_path": str(result.snapshot_path),
                "manifest_path": str(result.manifest_path),
                "ranking_path": str(result.ranking_path),
                "release_summary_path": str(result.release_summary_path),
                "row_count": result.row_count,
                "selected_symbols": list(result.selected_symbols),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
