from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Mapping, Sequence

import numpy as np
import pandas as pd

from us_equity_strategies.manifests import global_etf_rotation_manifest
from us_equity_strategies.strategies import global_etf_rotation as strategy

from .artifacts import write_release_status_summary, write_snapshot_manifest
from .contracts import GLOBAL_ETF_ROTATION_PROFILE, get_profile_contract
from .russell_1000_multi_factor_defensive_snapshot import read_table, write_table
from .universe_audit_contracts import ScoreTermSpec, SeasoningRule, SelectionRuleSpec, SymbolSpec, WATCHLIST
from .universe_audit_engine import normalize_symbols as _normalize_symbols
from .universe_audit_engine import run_universe_audit
from .yfinance_prices import download_yahoo_chart_price_history

DEFAULT_PRICE_START_DATE = "2023-01-01"
DEFAULT_MIN_TRADING_DAYS = 252
DEFAULT_MIN_MONTH_END_CLOSES = 13
DEFAULT_RULE_ID = "global_etf_rotation_universe_audit"
DEFAULT_RULE_VERSION = "v1"
DEFAULT_UNIVERSE_ID = "global_etf_rotation_ranking_pool"
DEFAULT_VOLATILITY_WINDOW = 126
RUNTIME_FEATURE_COLUMNS = (
    "as_of",
    "symbol",
    "role",
    "close",
    "momentum_13612w",
    "score",
    "sma_pass",
    "eligible",
    "vol_126",
    "history_days",
    "selection_rule_id",
    "selection_rule_version",
)


@dataclass(frozen=True)
class GlobalEtfRotationRuntimeArtifactResult:
    snapshot_path: Path
    manifest_path: Path
    ranking_path: Path
    release_summary_path: Path
    row_count: int
    eligible_symbols: tuple[str, ...]


def _config_tuple(config: Mapping[str, object], key: str) -> tuple[str, ...]:
    value = config.get(key)
    if isinstance(value, str):
        return _normalize_symbols(value)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return _normalize_symbols(tuple(str(item) for item in value))
    return ()


def build_default_symbol_specs(config: Mapping[str, object] | None = None) -> tuple[SymbolSpec, ...]:
    resolved = dict(config or global_etf_rotation_manifest.default_config)
    ranking_pool = _config_tuple(resolved, "ranking_pool")
    canary_assets = _config_tuple(resolved, "canary_assets")
    safe_haven = str(resolved.get("safe_haven") or "BIL").strip().upper()
    specs: list[SymbolSpec] = []
    for symbol in ranking_pool:
        specs.append(SymbolSpec(symbol=symbol, role="ranking_pool_etf", eligible_for_trading=True))
    for symbol in canary_assets:
        specs.append(SymbolSpec(symbol=symbol, role="canary_asset", eligible_for_trading=False))
    if safe_haven:
        specs.append(SymbolSpec(symbol=safe_haven, role="safe_haven", eligible_for_trading=False))
    deduped: dict[str, SymbolSpec] = {}
    for spec in specs:
        normalized = spec.normalized()
        deduped.setdefault(normalized.symbol, normalized)
    return tuple(deduped.values())


def build_global_etf_rotation_rule_spec(
    *,
    seasoning_rule: SeasoningRule = SeasoningRule(DEFAULT_MIN_TRADING_DAYS, DEFAULT_MIN_MONTH_END_CLOSES),
    rule_id: str = DEFAULT_RULE_ID,
    rule_version: str = DEFAULT_RULE_VERSION,
    universe_id: str = DEFAULT_UNIVERSE_ID,
) -> SelectionRuleSpec:
    return SelectionRuleSpec(
        rule_id=rule_id,
        rule_version=rule_version,
        universe_id=universe_id,
        hard_gates=seasoning_rule.to_hard_gates(failure_action=WATCHLIST),
        score_terms=(
            ScoreTermSpec("momentum_score", 0.50, higher_is_better=True),
            ScoreTermSpec("ret_126d", 0.30, higher_is_better=True),
            ScoreTermSpec("vol_63", 0.20, higher_is_better=False),
        ),
        benchmark_symbols=("SPY", "QQQ"),
        notes=(
            "Snapshot-managed universe audit for Global ETF Rotation. Runtime ranking-pool membership and future "
            "extensions are governed here by transparent gates."
        ),
    )


def build_global_etf_rotation_audit(
    price_history: pd.DataFrame,
    *,
    config: Mapping[str, object] | None = None,
    seasoning_rule: SeasoningRule = SeasoningRule(DEFAULT_MIN_TRADING_DAYS, DEFAULT_MIN_MONTH_END_CLOSES),
    as_of_date: str | None = None,
):
    return run_universe_audit(
        price_history,
        specs=build_default_symbol_specs(config),
        rule_spec=build_global_etf_rotation_rule_spec(seasoning_rule=seasoning_rule),
        as_of_date=as_of_date,
    )


def _normalize_price_history(
    price_history: pd.DataFrame,
    *,
    as_of_date: str | None = None,
) -> tuple[pd.DataFrame, pd.Timestamp]:
    required = {"symbol", "as_of", "close"}
    missing = required - set(price_history.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"price_history missing required columns: {missing_text}")
    history = price_history.copy()
    history["symbol"] = history["symbol"].astype(str).str.strip().str.upper()
    history["as_of"] = pd.to_datetime(history["as_of"], utc=True, errors="coerce").dt.tz_convert(None).dt.normalize()
    history["close"] = pd.to_numeric(history["close"], errors="coerce")
    history = history.dropna(subset=["symbol", "as_of", "close"])
    if history.empty:
        raise ValueError("price_history has no usable rows")
    effective_as_of = (
        pd.Timestamp(as_of_date).tz_localize(None).normalize()
        if as_of_date
        else pd.Timestamp(history["as_of"].max()).normalize()
    )
    history = history.loc[history["as_of"] <= effective_as_of].sort_values(["symbol", "as_of"]).reset_index(drop=True)
    return history, effective_as_of


def _close_series(history: pd.DataFrame) -> pd.Series:
    if history.empty:
        return pd.Series(dtype=float)
    series = (
        history.sort_values("as_of")
        .drop_duplicates(subset=["as_of"], keep="last")
        .set_index("as_of")["close"]
        .astype(float)
    )
    series.index = pd.DatetimeIndex(series.index)
    return series


def _runtime_feature_row(
    spec: SymbolSpec,
    symbol_history: pd.DataFrame,
    *,
    as_of: pd.Timestamp,
    sma_period: int,
    volatility_window: int,
    min_history_days: int,
) -> dict[str, object]:
    closes = _close_series(symbol_history)
    close = float(closes.iloc[-1]) if not closes.empty else float("nan")
    momentum = strategy.compute_13612w_momentum(closes, as_of_date=as_of) if not closes.empty else float("nan")
    sma_pass = strategy.check_sma(closes, period=int(sma_period)) if not closes.empty else False
    volatility = (
        strategy._annualized_volatility(closes, window=int(volatility_window))
        if not closes.empty
        else float("nan")
    )
    eligible = (
        bool(spec.eligible_for_trading)
        and len(closes) >= int(min_history_days)
        and close > 0.0
        and not np.isnan(momentum)
        and bool(sma_pass)
    )
    return {
        "as_of": as_of,
        "symbol": spec.symbol,
        "role": spec.role,
        "close": close,
        "momentum_13612w": momentum,
        "score": momentum,
        "sma_pass": bool(sma_pass),
        "eligible": bool(eligible),
        "vol_126": volatility,
        "history_days": int(len(closes)),
        "selection_rule_id": DEFAULT_RULE_ID,
        "selection_rule_version": DEFAULT_RULE_VERSION,
    }


def build_global_etf_rotation_feature_snapshot(
    price_history: pd.DataFrame,
    *,
    config: Mapping[str, object] | None = None,
    as_of_date: str | None = None,
    min_history_days: int = DEFAULT_MIN_TRADING_DAYS,
    volatility_window: int = DEFAULT_VOLATILITY_WINDOW,
) -> pd.DataFrame:
    resolved_config = dict(config or global_etf_rotation_manifest.default_config)
    history, effective_as_of = _normalize_price_history(price_history, as_of_date=as_of_date)
    grouped = {symbol: group for symbol, group in history.groupby("symbol", sort=False)}
    rows = [
        _runtime_feature_row(
            spec.normalized(),
            grouped.get(spec.normalized().symbol, pd.DataFrame(columns=history.columns)),
            as_of=effective_as_of,
            sma_period=int(resolved_config.get("sma_period", strategy.SMA_PERIOD)),
            volatility_window=int(volatility_window),
            min_history_days=int(min_history_days),
        )
        for spec in build_default_symbol_specs(resolved_config)
    ]
    return pd.DataFrame(rows, columns=RUNTIME_FEATURE_COLUMNS)


def build_runtime_ranking(feature_snapshot: pd.DataFrame) -> pd.DataFrame:
    ranking = feature_snapshot.loc[feature_snapshot["role"].eq("ranking_pool_etf")].copy()
    if ranking.empty:
        return ranking
    ranking["eligible"] = ranking["eligible"].astype(bool)
    ranking["score"] = pd.to_numeric(ranking["score"], errors="coerce")
    ranking["momentum_13612w"] = pd.to_numeric(ranking["momentum_13612w"], errors="coerce")
    return ranking.sort_values(
        ["eligible", "score", "momentum_13612w", "symbol"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)


def write_runtime_artifacts(
    price_history: pd.DataFrame,
    *,
    output_dir: str | Path,
    config: Mapping[str, object] | None = None,
    as_of_date: str | None = None,
    min_history_days: int = DEFAULT_MIN_TRADING_DAYS,
    snapshot_output: str | Path | None = None,
    manifest_output: str | Path | None = None,
    ranking_output: str | Path | None = None,
    release_summary_output: str | Path | None = None,
) -> GlobalEtfRotationRuntimeArtifactResult:
    contract = get_profile_contract(GLOBAL_ETF_ROTATION_PROFILE)
    paths = contract.artifact_paths(output_dir)
    snapshot_path = Path(snapshot_output) if snapshot_output else paths["snapshot"]
    manifest_path = Path(manifest_output) if manifest_output else paths["manifest"]
    ranking_path = Path(ranking_output) if ranking_output else paths["ranking"]
    release_summary_path = Path(release_summary_output) if release_summary_output else paths["release_summary"]

    feature_snapshot = build_global_etf_rotation_feature_snapshot(
        price_history,
        config=config,
        as_of_date=as_of_date,
        min_history_days=int(min_history_days),
    )
    write_table(feature_snapshot, snapshot_path)
    ranking = build_runtime_ranking(feature_snapshot)
    write_table(ranking, ranking_path)
    write_snapshot_manifest(
        contract=contract,
        snapshot_path=snapshot_path,
        snapshot=feature_snapshot,
        config_path=None,
        manifest_path=manifest_path,
        config_name=contract.profile,
    )
    eligible_symbols = tuple(ranking.loc[ranking["eligible"].astype(bool), "symbol"].astype(str).tolist())
    top_preview = ", ".join(
        f"{row.symbol}({float(row.score):.3f})"
        for row in ranking.head(5).itertuples(index=False)
        if not pd.isna(row.score)
    )
    signal_description = f"Global ETF Rotation snapshot universe ready; top={top_preview or 'none'}"
    status_description = f"rows={len(feature_snapshot)} | eligible={len(eligible_symbols)}"
    write_release_status_summary(
        contract=contract,
        snapshot_path=snapshot_path,
        manifest_path=manifest_path,
        ranking_path=ranking_path,
        summary_path=release_summary_path,
        snapshot=feature_snapshot,
        signal_description=signal_description,
        status_description=status_description,
        diagnostics={
            "eligible_symbols": eligible_symbols,
            "ranking_pool": tuple(
                feature_snapshot.loc[feature_snapshot["role"].eq("ranking_pool_etf"), "symbol"].astype(str).tolist()
            ),
            "canary_assets": tuple(
                feature_snapshot.loc[feature_snapshot["role"].eq("canary_asset"), "symbol"].astype(str).tolist()
            ),
            "safe_haven": tuple(
                feature_snapshot.loc[feature_snapshot["role"].eq("safe_haven"), "symbol"].astype(str).tolist()
            ),
        },
    )
    return GlobalEtfRotationRuntimeArtifactResult(
        snapshot_path=snapshot_path,
        manifest_path=manifest_path,
        ranking_path=ranking_path,
        release_summary_path=release_summary_path,
        row_count=int(len(feature_snapshot)),
        eligible_symbols=eligible_symbols,
    )


def _json_safe(value):
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_json_safe(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_audit_report(path: Path, *, result) -> None:
    ranking_preview = (
        ", ".join(result.ranking["symbol"].astype(str).head(10).tolist())
        if not result.ranking.empty
        else "none"
    )
    path.write_text(
        "\n".join(
            [
                "# Global ETF Rotation Universe Audit",
                "",
                (
                    "Global ETF Rotation runtime now consumes a feature snapshot. Ranking-pool governance and "
                    "signal inputs are handled as transparent snapshot-side evidence."
                ),
                "",
                f"- Rule: `{result.rule_spec.rule_id}` `{result.rule_spec.rule_version}`",
                f"- Universe: `{result.rule_spec.universe_id}`",
                f"- Candidate count: {result.diagnostics['candidate_count']}",
                f"- Research-ranking count: {result.diagnostics['ranking_count']}",
                f"- Ranking preview: {ranking_preview}",
                "",
                "Artifacts:",
                "",
                "- `global_etf_rotation_feature_snapshot_latest.csv`",
                "- `global_etf_rotation_feature_snapshot_latest.csv.manifest.json`",
                "- `global_etf_rotation_ranking_latest.csv`",
                "- `release_status_summary.json`",
                "- `candidate_snapshot.csv`",
                "- `gate_results.csv`",
                "- `ranking.csv`",
                "- `promotion_decision.json`",
                "- `run_manifest.json`",
                "",
            ]
        ),
        encoding="utf-8",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build snapshot-side Global ETF Rotation universe audit artifacts.")
    parser.add_argument("--prices", help="Input price history file (.csv/.json/.jsonl/.parquet).")
    parser.add_argument("--price-start", default=DEFAULT_PRICE_START_DATE)
    parser.add_argument("--price-end", default=None)
    parser.add_argument("--as-of-date", "--as-of", dest="as_of_date", default=None)
    parser.add_argument("--min-trading-days", type=int, default=DEFAULT_MIN_TRADING_DAYS)
    parser.add_argument("--min-month-end-closes", type=int, default=DEFAULT_MIN_MONTH_END_CLOSES)
    parser.add_argument("--output-dir", default="data/output/global_etf_rotation_universe_audit")
    parser.add_argument(
        "--snapshot-output",
        help="Feature snapshot output path; default: <output-dir>/global_etf_rotation_feature_snapshot_latest.csv",
    )
    parser.add_argument(
        "--manifest-output",
        help=(
            "Feature snapshot manifest output path; default: "
            "<output-dir>/global_etf_rotation_feature_snapshot_latest.csv.manifest.json"
        ),
    )
    parser.add_argument(
        "--ranking-output",
        help="Runtime ranking output path; default: <output-dir>/global_etf_rotation_ranking_latest.csv",
    )
    parser.add_argument(
        "--release-summary-output",
        help="Release summary output path; default: <output-dir>/release_status_summary.json",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    specs = build_default_symbol_specs()
    symbols = [spec.symbol for spec in specs]
    if args.prices:
        price_history = read_table(Path(args.prices))
    else:
        price_history = download_yahoo_chart_price_history(symbols, start=str(args.price_start), end=args.price_end)
    seasoning_rule = SeasoningRule(
        min_trading_days=int(args.min_trading_days),
        min_month_end_closes=int(args.min_month_end_closes),
    )
    result = build_global_etf_rotation_audit(
        price_history,
        seasoning_rule=seasoning_rule,
        as_of_date=args.as_of_date,
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    runtime_result = write_runtime_artifacts(
        price_history,
        output_dir=output_dir,
        as_of_date=args.as_of_date,
        min_history_days=int(args.min_trading_days),
        snapshot_output=args.snapshot_output,
        manifest_output=args.manifest_output,
        ranking_output=args.ranking_output,
        release_summary_output=args.release_summary_output,
    )
    write_table(price_history, output_dir / "downloaded_price_history.csv")
    write_table(result.candidate_snapshot, output_dir / "candidate_snapshot.csv")
    write_table(result.gate_results, output_dir / "gate_results.csv")
    write_table(result.ranking, output_dir / "ranking.csv")
    _write_json(
        output_dir / "promotion_decision.json",
        {
            "rule_spec": result.rule_spec.to_dict(),
            "decisions": [decision.to_row() for decision in result.promotion_decisions],
        },
    )
    _write_json(
        output_dir / "run_manifest.json",
        {
            "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
            "profile": "global_etf_rotation",
            "artifact_type": "feature_snapshot_with_transparent_universe_audit",
            "price_start": str(args.price_start),
            "price_end": args.price_end,
            "as_of_date": args.as_of_date,
            "seasoning_rule": asdict(seasoning_rule),
            "rule_spec": result.rule_spec.to_dict(),
            "symbols": [asdict(spec) for spec in specs],
            "diagnostics": dict(result.diagnostics),
            "runtime_note": (
                "Runtime consumes the feature snapshot; universe governance remains transparent and auditable."
            ),
            "outputs": [
                runtime_result.snapshot_path.name,
                runtime_result.manifest_path.name,
                runtime_result.ranking_path.name,
                runtime_result.release_summary_path.name,
                "downloaded_price_history.csv",
                "candidate_snapshot.csv",
                "gate_results.csv",
                "ranking.csv",
                "promotion_decision.json",
                "audit_report.md",
            ],
        },
    )
    _write_audit_report(output_dir / "audit_report.md", result=result)
    print(f"wrote feature snapshot -> {runtime_result.snapshot_path}")
    print(f"wrote manifest -> {runtime_result.manifest_path}")
    print(f"wrote runtime ranking -> {runtime_result.ranking_path}")
    print(f"wrote release summary -> {runtime_result.release_summary_path}")
    print(result.ranking.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
