from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd

from .artifacts import write_release_status_summary, write_snapshot_manifest
from .contracts import MEGA_CAP_LEADER_ROTATION_DYNAMIC_TOP20_PROFILE, SnapshotProfileContract, get_profile_contract
from .dynamic_mega_universe import (
    normalize_price_history,
    ranked_active_dynamic_universe,
    resolve_effective_as_of_date,
)
from .mega_cap_leader_rotation_backtest import (
    BENCHMARK_SYMBOL,
    BROAD_BENCHMARK_SYMBOL,
    DEFAULT_DYNAMIC_MEGA_UNIVERSE_SIZE,
    SAFE_HAVEN,
    _dynamic_mega_issuer_key,
    _normalize_universe,
    _precompute_symbol_feature_history,
    build_feature_snapshot_for_backtest,
    build_target_weights,
    resolve_active_universe,
    score_candidates,
    split_symbols,
)
from .russell_1000_multi_factor_defensive_snapshot import read_table, write_table

DEFAULT_HOLDINGS_COUNT = 4
DEFAULT_SINGLE_NAME_CAP = 0.25
DEFAULT_MIN_POSITION_VALUE_USD = 3_000.0
DEFAULT_HOLD_BUFFER = 2
DEFAULT_HOLD_BONUS = 0.10
DEFAULT_RISK_ON_EXPOSURE = 1.0
DEFAULT_SOFT_DEFENSE_EXPOSURE = 0.50
DEFAULT_HARD_DEFENSE_EXPOSURE = 0.50
DEFAULT_SOFT_BREADTH_THRESHOLD = 0.0
DEFAULT_HARD_BREADTH_THRESHOLD = 0.0
DEFAULT_MIN_ADV20_USD = 20_000_000.0


@dataclass(frozen=True)
class MegaCapDynamicTop20BuildResult:
    snapshot_path: Path
    manifest_path: Path
    ranking_path: Path
    release_summary_path: Path
    row_count: int
    selected_symbols: tuple[str, ...]


def _resolve_effective_as_of_date(price_history: pd.DataFrame, as_of_date: str | None) -> pd.Timestamp:
    if as_of_date:
        return pd.Timestamp(as_of_date).normalize()
    if price_history.empty or "as_of" not in price_history.columns:
        raise ValueError("price_history must contain as_of when --as-of is omitted")
    latest = pd.to_datetime(price_history["as_of"], utc=False).dt.tz_localize(None).dt.normalize().max()
    if pd.isna(latest):
        raise ValueError("price_history as_of has no usable dates")
    return pd.Timestamp(latest).normalize()


def _ranked_active_dynamic_universe(
    universe_snapshot: pd.DataFrame,
    *,
    as_of_date: pd.Timestamp,
    universe_size: int,
) -> pd.DataFrame:
    universe = _normalize_universe(universe_snapshot)
    active = resolve_active_universe(universe, as_of_date)
    if active.empty:
        raise ValueError(f"universe has no active rows for as_of_date={as_of_date:%Y-%m-%d}")
    active = active.copy()

    if "mega_rank" in universe.columns:
        raw_active = universe.copy()
        as_of = pd.Timestamp(as_of_date).normalize()
        if "start_date" in raw_active.columns:
            raw_active = raw_active.loc[raw_active["start_date"].isna() | (raw_active["start_date"] <= as_of)]
        if "end_date" in raw_active.columns:
            raw_active = raw_active.loc[raw_active["end_date"].isna() | (raw_active["end_date"] >= as_of)]
        raw_active["mega_rank"] = pd.to_numeric(raw_active["mega_rank"], errors="coerce")
        ranked = raw_active.dropna(subset=["mega_rank"]).sort_values(["mega_rank", "symbol"], ascending=[True, True])
        if not ranked.empty:
            return ranked.head(int(universe_size)).loc[:, ["symbol", "sector"]].reset_index(drop=True)

    ranking_columns = ("source_weight", "weight", "source_market_value", "market_value")
    usable_column = next((column for column in ranking_columns if column in universe.columns), None)
    if usable_column is not None:
        raw_active = universe.copy()
        as_of = pd.Timestamp(as_of_date).normalize()
        if "start_date" in raw_active.columns:
            raw_active = raw_active.loc[raw_active["start_date"].isna() | (raw_active["start_date"] <= as_of)]
        if "end_date" in raw_active.columns:
            raw_active = raw_active.loc[raw_active["end_date"].isna() | (raw_active["end_date"] >= as_of)]
        raw_active[usable_column] = pd.to_numeric(raw_active[usable_column], errors="coerce")
        ranked = raw_active.dropna(subset=[usable_column]).copy()
        if not ranked.empty:
            ranked["_issuer_key"] = ranked["symbol"].map(_dynamic_mega_issuer_key)
            ranked = (
                ranked.sort_values([usable_column, "symbol"], ascending=[False, True])
                .drop_duplicates(subset=["_issuer_key"], keep="first")
                .head(int(universe_size))
            )
            return ranked.loc[:, ["symbol", "sector"]].reset_index(drop=True)

    if len(active) <= int(universe_size):
        return active.loc[:, ["symbol", "sector"]].reset_index(drop=True)

    raise ValueError(
        "mega dynamic top20 universe requires mega_rank, source_weight, weight, "
        "source_market_value, or market_value when the active universe has more than 20 rows"
    )


def _build_signal_description(metadata: Mapping[str, object], ranking: pd.DataFrame) -> str:
    top_preview = ", ".join(
        f"{row.symbol}({row.score:.2f})"
        for row in ranking.head(5).itertuples(index=False)
    )
    return (
        f"regime={metadata['regime']} breadth={float(metadata['breadth_ratio']):.1%} "
        f"benchmark_trend={'up' if metadata['benchmark_trend_positive'] else 'down'} "
        f"target_stock={float(metadata['stock_exposure']):.1%} selected={len(metadata['selected_symbols'])} top={top_preview}"
    )


def build_artifacts(
    *,
    profile: str = MEGA_CAP_LEADER_ROTATION_DYNAMIC_TOP20_PROFILE,
    prices_path: str | Path,
    universe_path: str | Path,
    output_dir: str | Path,
    as_of_date: str | None = None,
    snapshot_output: str | Path | None = None,
    manifest_output: str | Path | None = None,
    ranking_output: str | Path | None = None,
    release_summary_output: str | Path | None = None,
    current_holdings: Iterable[str] | None = None,
    benchmark_symbol: str = BENCHMARK_SYMBOL,
    broad_benchmark_symbol: str = BROAD_BENCHMARK_SYMBOL,
    safe_haven: str = SAFE_HAVEN,
    dynamic_universe_size: int = DEFAULT_DYNAMIC_MEGA_UNIVERSE_SIZE,
    holdings_count: int = DEFAULT_HOLDINGS_COUNT,
    single_name_cap: float = DEFAULT_SINGLE_NAME_CAP,
    hold_buffer: int = DEFAULT_HOLD_BUFFER,
    hold_bonus: float = DEFAULT_HOLD_BONUS,
    risk_on_exposure: float = DEFAULT_RISK_ON_EXPOSURE,
    soft_defense_exposure: float = DEFAULT_SOFT_DEFENSE_EXPOSURE,
    hard_defense_exposure: float = DEFAULT_HARD_DEFENSE_EXPOSURE,
    soft_breadth_threshold: float = DEFAULT_SOFT_BREADTH_THRESHOLD,
    hard_breadth_threshold: float = DEFAULT_HARD_BREADTH_THRESHOLD,
    portfolio_total_equity: float | None = None,
    min_position_value_usd: float = DEFAULT_MIN_POSITION_VALUE_USD,
    min_price_usd: float = 10.0,
    min_adv20_usd: float = DEFAULT_MIN_ADV20_USD,
    min_history_days: int = 273,
) -> MegaCapDynamicTop20BuildResult:
    contract = get_profile_contract(profile)
    paths = contract.artifact_paths(output_dir)
    snapshot_path = Path(snapshot_output) if snapshot_output else paths["snapshot"]
    manifest_path = Path(manifest_output) if manifest_output else paths["manifest"]
    ranking_path = Path(ranking_output) if ranking_output else paths["ranking"]
    release_summary_path = Path(release_summary_output) if release_summary_output else paths["release_summary"]

    price_history = normalize_price_history(read_table(prices_path))
    effective_as_of_date = resolve_effective_as_of_date(price_history, as_of_date)
    active_universe = ranked_active_dynamic_universe(
        read_table(universe_path),
        as_of_date=effective_as_of_date,
        universe_size=int(dynamic_universe_size),
    )
    feature_history_by_symbol = _precompute_symbol_feature_history(price_history)
    snapshot = build_feature_snapshot_for_backtest(
        effective_as_of_date,
        active_universe,
        feature_history_by_symbol,
        benchmark_symbol=benchmark_symbol,
        broad_benchmark_symbol=broad_benchmark_symbol,
        safe_haven=safe_haven,
        min_price_usd=float(min_price_usd),
        min_adv20_usd=float(min_adv20_usd),
        min_history_days=int(min_history_days),
    )
    write_table(snapshot, snapshot_path)

    holdings = split_symbols(current_holdings)
    ranking = score_candidates(
        snapshot,
        holdings,
        benchmark_symbol=benchmark_symbol,
        broad_benchmark_symbol=broad_benchmark_symbol,
        safe_haven=safe_haven,
        hold_bonus=float(hold_bonus),
    )
    write_table(ranking, ranking_path)

    weights, _ranked, metadata = build_target_weights(
        snapshot,
        holdings,
        benchmark_symbol=benchmark_symbol,
        broad_benchmark_symbol=broad_benchmark_symbol,
        safe_haven=safe_haven,
        top_n=int(holdings_count),
        hold_buffer=int(hold_buffer),
        single_name_cap=float(single_name_cap),
        hold_bonus=float(hold_bonus),
        risk_on_exposure=float(risk_on_exposure),
        soft_defense_exposure=float(soft_defense_exposure),
        hard_defense_exposure=float(hard_defense_exposure),
        soft_breadth_threshold=float(soft_breadth_threshold),
        hard_breadth_threshold=float(hard_breadth_threshold),
        portfolio_total_equity=portfolio_total_equity,
        min_position_value_usd=float(min_position_value_usd),
    )
    selected_symbols = tuple(str(symbol) for symbol in metadata.get("selected_symbols", ()))
    signal_description = _build_signal_description(metadata, ranking)
    status_description = (
        f"regime={metadata['regime']} | breadth={float(metadata['breadth_ratio']):.1%} | "
        f"target_stock={float(metadata['stock_exposure']):.1%}"
    )
    diagnostics = {
        **metadata,
        "target_weights": weights,
        "ranking_path": str(ranking_path),
        "active_universe_symbols": tuple(active_universe["symbol"].astype(str).tolist()),
        "dynamic_universe_size": int(dynamic_universe_size),
    }
    write_snapshot_manifest(
        contract=contract,
        snapshot_path=snapshot_path,
        snapshot=snapshot,
        config_path=None,
        manifest_path=manifest_path,
        config_name=contract.profile,
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
    return MegaCapDynamicTop20BuildResult(
        snapshot_path=snapshot_path,
        manifest_path=manifest_path,
        ranking_path=ranking_path,
        release_summary_path=release_summary_path,
        row_count=int(len(snapshot)),
        selected_symbols=selected_symbols,
    )


def build_parser(
    *,
    profile: str = MEGA_CAP_LEADER_ROTATION_DYNAMIC_TOP20_PROFILE,
    dynamic_universe_size_default: int = DEFAULT_DYNAMIC_MEGA_UNIVERSE_SIZE,
    holdings_count_default: int = DEFAULT_HOLDINGS_COUNT,
    single_name_cap_default: float = DEFAULT_SINGLE_NAME_CAP,
    soft_defense_exposure_default: float = DEFAULT_SOFT_DEFENSE_EXPOSURE,
    hard_defense_exposure_default: float = DEFAULT_HARD_DEFENSE_EXPOSURE,
    soft_breadth_threshold_default: float = DEFAULT_SOFT_BREADTH_THRESHOLD,
    hard_breadth_threshold_default: float = DEFAULT_HARD_BREADTH_THRESHOLD,
) -> argparse.ArgumentParser:
    contract: SnapshotProfileContract = get_profile_contract(profile)
    parser = argparse.ArgumentParser(description=f"Build {contract.profile} snapshot artifacts.")
    parser.add_argument("--prices", required=True, help="Input price history file (.csv/.json/.jsonl/.parquet)")
    parser.add_argument("--universe", required=True, help="Ranked mega-cap universe history or latest holdings file")
    parser.add_argument("--output-dir", required=True, help="Directory for the standard artifact filenames")
    parser.add_argument("--snapshot-output", help=f"Snapshot output path; default: <output-dir>/{contract.snapshot_filename}")
    parser.add_argument("--manifest-output", help=f"Manifest output path; default: <output-dir>/{contract.manifest_filename}")
    parser.add_argument("--ranking-output", help=f"Ranking output path; default: <output-dir>/{contract.ranking_filename}")
    parser.add_argument("--release-summary-output", help="Release summary output path; default: <output-dir>/release_status_summary.json")
    parser.add_argument("--as-of", dest="as_of_date", help="Snapshot date; defaults to latest price date")
    parser.add_argument("--current-holdings", help="Comma-separated current holdings used only for hold-bonus preview")
    parser.add_argument("--benchmark-symbol", default=BENCHMARK_SYMBOL)
    parser.add_argument("--broad-benchmark-symbol", default=BROAD_BENCHMARK_SYMBOL)
    parser.add_argument("--safe-haven", default=SAFE_HAVEN)
    parser.add_argument("--dynamic-universe-size", type=int, default=dynamic_universe_size_default)
    parser.add_argument("--holdings-count", type=int, default=holdings_count_default)
    parser.add_argument("--single-name-cap", type=float, default=single_name_cap_default)
    parser.add_argument("--soft-defense-exposure", type=float, default=soft_defense_exposure_default)
    parser.add_argument("--hard-defense-exposure", type=float, default=hard_defense_exposure_default)
    parser.add_argument("--soft-breadth-threshold", type=float, default=soft_breadth_threshold_default)
    parser.add_argument("--hard-breadth-threshold", type=float, default=hard_breadth_threshold_default)
    parser.add_argument("--portfolio-total-equity", type=float)
    parser.add_argument("--min-position-value-usd", type=float, default=DEFAULT_MIN_POSITION_VALUE_USD)
    parser.add_argument("--min-price-usd", type=float, default=10.0)
    parser.add_argument("--min-adv20-usd", type=float, default=DEFAULT_MIN_ADV20_USD)
    parser.add_argument("--min-history-days", type=int, default=273)
    return parser


def main(
    argv: list[str] | None = None,
    *,
    profile: str = MEGA_CAP_LEADER_ROTATION_DYNAMIC_TOP20_PROFILE,
    dynamic_universe_size_default: int = DEFAULT_DYNAMIC_MEGA_UNIVERSE_SIZE,
    holdings_count_default: int = DEFAULT_HOLDINGS_COUNT,
    single_name_cap_default: float = DEFAULT_SINGLE_NAME_CAP,
    soft_defense_exposure_default: float = DEFAULT_SOFT_DEFENSE_EXPOSURE,
    hard_defense_exposure_default: float = DEFAULT_HARD_DEFENSE_EXPOSURE,
    soft_breadth_threshold_default: float = DEFAULT_SOFT_BREADTH_THRESHOLD,
    hard_breadth_threshold_default: float = DEFAULT_HARD_BREADTH_THRESHOLD,
) -> int:
    args = build_parser(
        profile=profile,
        dynamic_universe_size_default=dynamic_universe_size_default,
        holdings_count_default=holdings_count_default,
        single_name_cap_default=single_name_cap_default,
        soft_defense_exposure_default=soft_defense_exposure_default,
        hard_defense_exposure_default=hard_defense_exposure_default,
        soft_breadth_threshold_default=soft_breadth_threshold_default,
        hard_breadth_threshold_default=hard_breadth_threshold_default,
    ).parse_args(argv)
    result = build_artifacts(
        profile=profile,
        prices_path=args.prices,
        universe_path=args.universe,
        output_dir=args.output_dir,
        as_of_date=args.as_of_date,
        snapshot_output=args.snapshot_output,
        manifest_output=args.manifest_output,
        ranking_output=args.ranking_output,
        release_summary_output=args.release_summary_output,
        current_holdings=split_symbols(args.current_holdings),
        benchmark_symbol=args.benchmark_symbol,
        broad_benchmark_symbol=args.broad_benchmark_symbol,
        safe_haven=args.safe_haven,
        dynamic_universe_size=args.dynamic_universe_size,
        holdings_count=args.holdings_count,
        single_name_cap=args.single_name_cap,
        soft_defense_exposure=args.soft_defense_exposure,
        hard_defense_exposure=args.hard_defense_exposure,
        soft_breadth_threshold=args.soft_breadth_threshold,
        hard_breadth_threshold=args.hard_breadth_threshold,
        portfolio_total_equity=args.portfolio_total_equity,
        min_position_value_usd=args.min_position_value_usd,
        min_price_usd=args.min_price_usd,
        min_adv20_usd=args.min_adv20_usd,
        min_history_days=args.min_history_days,
    )
    print(f"wrote {result.row_count} rows -> {result.snapshot_path}")
    print(f"wrote manifest -> {result.manifest_path}")
    print(f"wrote ranking -> {result.ranking_path}")
    print(f"wrote release summary -> {result.release_summary_path}")
    if result.selected_symbols:
        print("selected preview -> " + ", ".join(result.selected_symbols))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
