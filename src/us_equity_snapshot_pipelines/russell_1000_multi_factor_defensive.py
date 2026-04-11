from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from .russell_1000_multi_factor_defensive_snapshot import (
    build_feature_snapshot,
    read_table,
    write_table,
)
from us_equity_strategies.strategies import russell_1000_multi_factor_defensive as strategy

from .artifacts import write_release_status_summary, write_snapshot_manifest
from .contracts import RUSSELL_1000_MULTI_FACTOR_DEFENSIVE_PROFILE, SnapshotProfileContract, get_profile_contract


DEFAULT_RUNTIME_CONFIG: dict[str, Any] = {
    "benchmark_symbol": strategy.BENCHMARK_SYMBOL,
    "safe_haven": strategy.SAFE_HAVEN,
    "holdings_count": strategy.DEFAULT_HOLDINGS_COUNT,
    "single_name_cap": strategy.DEFAULT_SINGLE_NAME_CAP,
    "sector_cap": strategy.DEFAULT_SECTOR_CAP,
    "hold_bonus": strategy.DEFAULT_HOLD_BONUS,
    "soft_defense_exposure": strategy.DEFAULT_SOFT_DEFENSE_EXPOSURE,
    "hard_defense_exposure": strategy.DEFAULT_HARD_DEFENSE_EXPOSURE,
    "soft_breadth_threshold": strategy.DEFAULT_SOFT_BREADTH_THRESHOLD,
    "hard_breadth_threshold": strategy.DEFAULT_HARD_BREADTH_THRESHOLD,
}


@dataclass(frozen=True)
class Russell1000BuildResult:
    snapshot_path: Path
    manifest_path: Path
    ranking_path: Path
    release_summary_path: Path
    row_count: int
    selected_symbols: tuple[str, ...]


def _split_symbols(raw_symbols: str | Iterable[str] | None) -> tuple[str, ...]:
    if raw_symbols is None:
        return ()
    if isinstance(raw_symbols, str):
        values = raw_symbols.split(",")
    else:
        values = list(raw_symbols)
    return tuple(dict.fromkeys(str(value).strip().upper() for value in values if str(value).strip()))


def _runtime_config(**overrides: Any) -> dict[str, Any]:
    config = dict(DEFAULT_RUNTIME_CONFIG)
    for key, value in overrides.items():
        if value is not None:
            config[key] = value
    return config


def _resolve_effective_as_of_date(
    price_history: pd.DataFrame,
    universe_snapshot: pd.DataFrame,
    as_of_date: str | None,
) -> str | None:
    if as_of_date:
        return as_of_date
    if not ({"start_date", "end_date"} & set(universe_snapshot.columns)):
        return None
    if price_history.empty or "as_of" not in price_history.columns:
        return None
    latest = (
        pd.to_datetime(price_history["as_of"], utc=False)
        .dt.tz_localize(None)
        .dt.normalize()
        .max()
    )
    if pd.isna(latest):
        return None
    return f"{latest:%Y-%m-%d}"


def _filter_universe_for_as_of(
    universe_snapshot: pd.DataFrame, as_of_date: str | None
) -> pd.DataFrame:
    has_interval_columns = {"start_date", "end_date"} & set(universe_snapshot.columns)
    if as_of_date is None or not has_interval_columns:
        return universe_snapshot

    as_of = pd.Timestamp(as_of_date).normalize()
    universe = universe_snapshot.copy()
    start_dates = (
        pd.to_datetime(universe["start_date"], utc=False, errors="coerce")
        if "start_date" in universe.columns
        else pd.Series(pd.NaT, index=universe.index)
    )
    end_dates = (
        pd.to_datetime(universe["end_date"], utc=False, errors="coerce")
        if "end_date" in universe.columns
        else pd.Series(pd.NaT, index=universe.index)
    )
    active = (start_dates.isna() | (start_dates <= as_of)) & (
        end_dates.isna() | (end_dates >= as_of)
    )
    filtered = universe.loc[active].copy()
    if filtered.empty:
        raise ValueError(
            f"universe_snapshot has no active rows for as_of_date={as_of:%Y-%m-%d}"
        )
    return filtered


def build_candidate_ranking(
    snapshot: pd.DataFrame,
    current_holdings: Iterable[str] | None,
    *,
    runtime_params: Mapping[str, Any],
) -> pd.DataFrame:
    frame = strategy._to_frame(snapshot)  # noqa: SLF001 - upstream pipeline intentionally mirrors strategy scoring.
    benchmark_symbol = str(runtime_params.get("benchmark_symbol") or strategy.BENCHMARK_SYMBOL).strip().upper()
    safe_haven = str(runtime_params.get("safe_haven") or strategy.SAFE_HAVEN).strip().upper()
    current_holdings_set = strategy._normalize_holdings(current_holdings or ())  # noqa: SLF001
    eligible = frame.loc[
        (frame["symbol"] != benchmark_symbol)
        & (frame["symbol"] != safe_haven)
        & frame["eligible"]
        & frame["mom_6_1"].notna()
        & frame["mom_12_1"].notna()
        & frame["sma200_gap"].notna()
        & frame["vol_63"].notna()
        & frame["maxdd_126"].notna()
    ].copy()
    if eligible.empty:
        return pd.DataFrame(columns=["rank", "symbol", "sector", "score", "eligible"])

    eligible["z_mom_6_1"] = eligible.groupby("sector")["mom_6_1"].transform(strategy._zscore)  # noqa: SLF001
    eligible["z_mom_12_1"] = eligible.groupby("sector")["mom_12_1"].transform(strategy._zscore)  # noqa: SLF001
    eligible["z_sma200_gap"] = eligible.groupby("sector")["sma200_gap"].transform(strategy._zscore)  # noqa: SLF001
    eligible["z_vol_63"] = eligible.groupby("sector")["vol_63"].transform(strategy._zscore)  # noqa: SLF001
    eligible["drawdown_abs"] = eligible["maxdd_126"].abs()
    eligible["z_drawdown_abs"] = eligible.groupby("sector")["drawdown_abs"].transform(strategy._zscore)  # noqa: SLF001
    eligible["score"] = (
        (eligible["z_mom_6_1"] * 0.35)
        + (eligible["z_mom_12_1"] * 0.30)
        + (eligible["z_sma200_gap"] * 0.15)
        - (eligible["z_vol_63"] * 0.10)
        - (eligible["z_drawdown_abs"] * 0.10)
    )
    eligible.loc[eligible["symbol"].isin(current_holdings_set), "score"] += float(
        runtime_params.get("hold_bonus", strategy.DEFAULT_HOLD_BONUS)
    )
    ranked = eligible.sort_values(
        by=["score", "mom_12_1", "mom_6_1", "symbol"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    ranked.insert(0, "rank", range(1, len(ranked) + 1))
    output_columns = [
        "rank",
        "symbol",
        "sector",
        "score",
        "eligible",
        "close",
        "adv20_usd",
        "mom_12_1",
        "mom_6_1",
        "sma200_gap",
        "vol_63",
        "maxdd_126",
    ]
    return ranked.loc[:, [column for column in output_columns if column in ranked.columns]]


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
    current_holdings: Iterable[str] | None = None,
    benchmark_symbol: str = strategy.BENCHMARK_SYMBOL,
    safe_haven: str = strategy.SAFE_HAVEN,
    min_price_usd: float = 10.0,
    min_adv20_usd: float = 20_000_000.0,
    min_history_days: int = 252,
    holdings_count: int = strategy.DEFAULT_HOLDINGS_COUNT,
    single_name_cap: float = strategy.DEFAULT_SINGLE_NAME_CAP,
    sector_cap: float = strategy.DEFAULT_SECTOR_CAP,
) -> Russell1000BuildResult:
    contract = get_profile_contract(RUSSELL_1000_MULTI_FACTOR_DEFENSIVE_PROFILE)
    paths = contract.artifact_paths(output_dir)
    snapshot_path = Path(snapshot_output) if snapshot_output else paths["snapshot"]
    manifest_path = Path(manifest_output) if manifest_output else paths["manifest"]
    ranking_path = Path(ranking_output) if ranking_output else paths["ranking"]
    release_summary_path = Path(release_summary_output) if release_summary_output else paths["release_summary"]

    runtime_params = _runtime_config(
        benchmark_symbol=str(benchmark_symbol).strip().upper(),
        safe_haven=str(safe_haven).strip().upper(),
        holdings_count=int(holdings_count),
        single_name_cap=float(single_name_cap),
        sector_cap=float(sector_cap),
    )
    price_history = read_table(prices_path)
    universe_snapshot = read_table(universe_path)
    effective_as_of_date = _resolve_effective_as_of_date(
        price_history, universe_snapshot, as_of_date
    )
    effective_universe = _filter_universe_for_as_of(
        universe_snapshot, effective_as_of_date
    )
    snapshot = build_feature_snapshot(
        price_history,
        effective_universe,
        as_of_date=effective_as_of_date,
        benchmark_symbol=str(runtime_params["benchmark_symbol"]),
        min_price_usd=float(min_price_usd),
        min_adv20_usd=float(min_adv20_usd),
        min_history_days=int(min_history_days),
    )
    write_table(snapshot, snapshot_path)

    ranking = build_candidate_ranking(snapshot, current_holdings, runtime_params=runtime_params)
    write_table(ranking, ranking_path)

    weights, signal_description, _is_emergency, status_description, diagnostics = strategy.compute_signals(
        snapshot,
        _split_symbols(current_holdings),
        **runtime_params,
    )
    selected_symbols = tuple(str(symbol) for symbol in diagnostics.get("selected_symbols", ()))
    diagnostics = {
        **diagnostics,
        "target_weights": weights,
        "ranking_path": str(ranking_path),
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
    return Russell1000BuildResult(
        snapshot_path=snapshot_path,
        manifest_path=manifest_path,
        ranking_path=ranking_path,
        release_summary_path=release_summary_path,
        row_count=int(len(snapshot)),
        selected_symbols=selected_symbols,
    )


def build_parser() -> argparse.ArgumentParser:
    contract: SnapshotProfileContract = get_profile_contract(RUSSELL_1000_MULTI_FACTOR_DEFENSIVE_PROFILE)
    parser = argparse.ArgumentParser(description="Build russell_1000_multi_factor_defensive snapshot artifacts.")
    parser.add_argument("--prices", required=True, help="Input price history file (.csv/.json/.jsonl/.parquet)")
    parser.add_argument("--universe", required=True, help="Input universe file (.csv/.json/.jsonl/.parquet)")
    parser.add_argument("--output-dir", required=True, help="Directory for the standard artifact filenames")
    parser.add_argument("--snapshot-output", help=f"Snapshot output path; default: <output-dir>/{contract.snapshot_filename}")
    parser.add_argument("--manifest-output", help=f"Manifest output path; default: <output-dir>/{contract.manifest_filename}")
    parser.add_argument("--ranking-output", help=f"Ranking output path; default: <output-dir>/{contract.ranking_filename}")
    parser.add_argument("--release-summary-output", help="Release summary output path; default: <output-dir>/release_status_summary.json")
    parser.add_argument("--as-of", dest="as_of_date", help="Snapshot date; defaults to latest price date")
    parser.add_argument("--current-holdings", help="Comma-separated current holdings used only for hold-bonus preview")
    parser.add_argument("--benchmark-symbol", default=strategy.BENCHMARK_SYMBOL)
    parser.add_argument("--safe-haven", default=strategy.SAFE_HAVEN)
    parser.add_argument("--min-price-usd", type=float, default=10.0)
    parser.add_argument("--min-adv20-usd", type=float, default=20_000_000.0)
    parser.add_argument("--min-history-days", type=int, default=252)
    parser.add_argument("--holdings-count", type=int, default=strategy.DEFAULT_HOLDINGS_COUNT)
    parser.add_argument("--single-name-cap", type=float, default=strategy.DEFAULT_SINGLE_NAME_CAP)
    parser.add_argument("--sector-cap", type=float, default=strategy.DEFAULT_SECTOR_CAP)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_artifacts(
        prices_path=args.prices,
        universe_path=args.universe,
        output_dir=args.output_dir,
        as_of_date=args.as_of_date,
        snapshot_output=args.snapshot_output,
        manifest_output=args.manifest_output,
        ranking_output=args.ranking_output,
        release_summary_output=args.release_summary_output,
        current_holdings=_split_symbols(args.current_holdings),
        benchmark_symbol=args.benchmark_symbol,
        safe_haven=args.safe_haven,
        min_price_usd=args.min_price_usd,
        min_adv20_usd=args.min_adv20_usd,
        min_history_days=args.min_history_days,
        holdings_count=args.holdings_count,
        single_name_cap=args.single_name_cap,
        sector_cap=args.sector_cap,
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
