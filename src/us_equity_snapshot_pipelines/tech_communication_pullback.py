from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from .qqq_tech_enhancement_snapshot import (
    build_feature_snapshot,
    read_table,
    write_table,
)
from us_equity_strategies.strategies import qqq_tech_enhancement as strategy

from .artifacts import write_release_status_summary, write_snapshot_manifest
from .contracts import TECH_COMMUNICATION_PULLBACK_PROFILE, SnapshotProfileContract, get_profile_contract


@dataclass(frozen=True)
class TechCommunicationPullbackBuildResult:
    snapshot_path: Path
    manifest_path: Path
    ranking_path: Path
    release_summary_path: Path
    row_count: int
    selected_symbols: tuple[str, ...]


def _default_config_path() -> Path | None:
    candidates = (
        Path(__file__).resolve().parents[3]
        / "LongBridgePlatform"
        / "research"
        / "configs"
        / "growth_pullback_tech_communication_pullback_enhancement.json",
        Path(__file__).resolve().parents[3]
        / "InteractiveBrokersPlatform"
        / "research"
        / "configs"
        / "growth_pullback_tech_communication_pullback_enhancement.json",
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _resolve_config_path(raw_path: str | Path | None, *, use_default_config: bool) -> Path | None:
    if raw_path:
        return Path(raw_path)
    if use_default_config:
        return _default_config_path()
    return None


def _split_symbols(raw_symbols: str | Iterable[str] | None) -> tuple[str, ...]:
    if raw_symbols is None:
        return ()
    if isinstance(raw_symbols, str):
        values = raw_symbols.split(",")
    else:
        values = list(raw_symbols)
    return tuple(dict.fromkeys(str(value).strip().upper() for value in values if str(value).strip()))


def _config_for_signal(runtime_params: Mapping[str, Any], *, portfolio_total_equity: float | None) -> dict[str, Any]:
    config = dict(runtime_params)
    config.pop("execution_cash_reserve_ratio", None)
    if portfolio_total_equity is not None:
        config["portfolio_total_equity"] = float(portfolio_total_equity)
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


def build_candidate_ranking(
    snapshot: pd.DataFrame,
    current_holdings: Iterable[str] | None,
    *,
    runtime_params: Mapping[str, Any],
) -> pd.DataFrame:
    frame = strategy._to_frame(snapshot)  # noqa: SLF001 - upstream pipeline intentionally mirrors strategy scoring.
    current_holdings_set = strategy._normalize_holdings(current_holdings or ())  # noqa: SLF001
    scored = strategy._score_candidates(  # noqa: SLF001
        frame,
        current_holdings_set,
        benchmark_symbol=str(runtime_params.get("benchmark_symbol") or strategy.BENCHMARK_SYMBOL),
        safe_haven=str(runtime_params.get("safe_haven") or strategy.SAFE_HAVEN),
        sector_whitelist=tuple(runtime_params.get("sector_whitelist") or strategy.DEFAULT_SECTOR_WHITELIST),
        min_adv20_usd=float(runtime_params.get("min_adv20_usd", strategy.DEFAULT_MIN_ADV20_USD)),
        normalization=str(runtime_params.get("normalization") or strategy.DEFAULT_NORMALIZATION),
        score_template=str(runtime_params.get("score_template") or strategy.DEFAULT_SCORE_TEMPLATE),
        hold_bonus=float(runtime_params.get("hold_bonus", strategy.DEFAULT_HOLD_BONUS)),
    )
    if scored.empty:
        return pd.DataFrame(columns=["rank", "symbol", "sector", "score", "base_eligible"])
    ranked = scored.sort_values(
        by=["score", "excess_mom_12_1", "trend_strength", "symbol"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    ranked.insert(0, "rank", range(1, len(ranked) + 1))
    output_columns = [
        "rank",
        "symbol",
        "sector",
        "score",
        "base_eligible",
        "close",
        "adv20_usd",
        "excess_mom_12_1",
        "excess_mom_6_1",
        "trend_strength",
        "controlled_pullback_score",
        "recovery_confirmation",
        "rel_strength_vs_group",
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
    config_path: str | Path | None = None,
    use_default_config: bool = True,
    snapshot_output: str | Path | None = None,
    manifest_output: str | Path | None = None,
    ranking_output: str | Path | None = None,
    release_summary_output: str | Path | None = None,
    current_holdings: Iterable[str] | None = None,
    portfolio_total_equity: float | None = None,
    min_price_usd: float = 10.0,
    min_history_days: int = 252,
) -> TechCommunicationPullbackBuildResult:
    contract = get_profile_contract(TECH_COMMUNICATION_PULLBACK_PROFILE)
    paths = contract.artifact_paths(output_dir)
    snapshot_path = Path(snapshot_output) if snapshot_output else paths["snapshot"]
    manifest_path = Path(manifest_output) if manifest_output else paths["manifest"]
    ranking_path = Path(ranking_output) if ranking_output else paths["ranking"]
    release_summary_path = Path(release_summary_output) if release_summary_output else paths["release_summary"]

    resolved_config_path = _resolve_config_path(config_path, use_default_config=use_default_config)
    runtime_params = strategy.load_runtime_parameters(config_path=resolved_config_path)

    price_history = read_table(prices_path)
    universe_snapshot = read_table(universe_path)
    effective_as_of_date = _resolve_effective_as_of_date(
        price_history, universe_snapshot, as_of_date
    )
    snapshot = build_feature_snapshot(
        price_history,
        universe_snapshot,
        as_of_date=effective_as_of_date,
        benchmark_symbol=str(runtime_params.get("benchmark_symbol") or strategy.BENCHMARK_SYMBOL),
        safe_haven=str(runtime_params.get("safe_haven") or strategy.SAFE_HAVEN),
        sector_whitelist=tuple(runtime_params.get("sector_whitelist") or strategy.DEFAULT_SECTOR_WHITELIST),
        min_price_usd=float(min_price_usd),
        min_adv20_usd=float(runtime_params.get("min_adv20_usd", strategy.DEFAULT_MIN_ADV20_USD)),
        min_history_days=int(min_history_days),
    )
    write_table(snapshot, snapshot_path)

    ranking = build_candidate_ranking(snapshot, current_holdings, runtime_params=runtime_params)
    write_table(ranking, ranking_path)

    signal_config = _config_for_signal(runtime_params, portfolio_total_equity=portfolio_total_equity)
    weights, signal_description, _is_emergency, status_description, diagnostics = strategy.compute_signals(
        snapshot,
        _split_symbols(current_holdings),
        **signal_config,
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
        config_path=resolved_config_path,
        manifest_path=manifest_path,
        config_name=str(runtime_params.get("runtime_config_name") or contract.profile),
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
    return TechCommunicationPullbackBuildResult(
        snapshot_path=snapshot_path,
        manifest_path=manifest_path,
        ranking_path=ranking_path,
        release_summary_path=release_summary_path,
        row_count=int(len(snapshot)),
        selected_symbols=selected_symbols,
    )


def build_parser() -> argparse.ArgumentParser:
    contract: SnapshotProfileContract = get_profile_contract(TECH_COMMUNICATION_PULLBACK_PROFILE)
    parser = argparse.ArgumentParser(description="Build tech_communication_pullback_enhancement snapshot artifacts.")
    parser.add_argument("--prices", required=True, help="Input price history file (.csv/.json/.jsonl/.parquet)")
    parser.add_argument("--universe", required=True, help="Input universe file (.csv/.json/.jsonl/.parquet)")
    parser.add_argument("--output-dir", required=True, help="Directory for the standard artifact filenames")
    parser.add_argument("--snapshot-output", help=f"Snapshot output path; default: <output-dir>/{contract.snapshot_filename}")
    parser.add_argument("--manifest-output", help=f"Manifest output path; default: <output-dir>/{contract.manifest_filename}")
    parser.add_argument("--ranking-output", help=f"Ranking output path; default: <output-dir>/{contract.ranking_filename}")
    parser.add_argument("--release-summary-output", help="Release summary output path; default: <output-dir>/release_status_summary.json")
    parser.add_argument("--config-path", help="Strategy config path. Defaults to the sibling platform config when present.")
    parser.add_argument("--no-default-config", action="store_true", help="Use module defaults instead of sibling platform config.")
    parser.add_argument("--as-of", dest="as_of_date", help="Snapshot date; defaults to latest price date")
    parser.add_argument("--current-holdings", help="Comma-separated current holdings used only for hold-bonus preview")
    parser.add_argument("--portfolio-total-equity", type=float, help="Optional account equity used for dynamic position-count preview")
    parser.add_argument("--min-price-usd", type=float, default=10.0)
    parser.add_argument("--min-history-days", type=int, default=252)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = build_artifacts(
        prices_path=args.prices,
        universe_path=args.universe,
        output_dir=args.output_dir,
        as_of_date=args.as_of_date,
        config_path=args.config_path,
        use_default_config=not args.no_default_config,
        snapshot_output=args.snapshot_output,
        manifest_output=args.manifest_output,
        ranking_output=args.ranking_output,
        release_summary_output=args.release_summary_output,
        current_holdings=_split_symbols(args.current_holdings),
        portfolio_total_equity=args.portfolio_total_equity,
        min_price_usd=args.min_price_usd,
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
