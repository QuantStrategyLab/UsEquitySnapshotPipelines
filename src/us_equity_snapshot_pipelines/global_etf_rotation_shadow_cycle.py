from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from .contracts import GLOBAL_ETF_ROTATION_PROFILE

SHADOW_VARIANTS: dict[str, dict[str, object]] = {
    "active": {},
    "equal_weight": {"confidence_weighting_enabled": False, "confidence_top1_weight": 1.0},
    "no_vol_gate": {"confidence_volatility_gate_enabled": False},
    "top_1": {"top_n": 1},
}
DEFAULT_ACTIVE_VARIANT = "active"


@dataclass(frozen=True)
class ShadowCycleOutputs:
    diagnostics_json: Path
    variant_comparison_json: Path


def _load_feature_snapshot(path: Path) -> pd.DataFrame:
    """Load a feature snapshot CSV and validate the as_of column."""
    frame = pd.read_csv(path)
    if "as_of" not in frame.columns:
        raise ValueError(f"feature snapshot missing as_of column: {path}")
    frame["as_of"] = pd.to_datetime(frame["as_of"], errors="coerce").dt.tz_localize(None)
    if frame["as_of"].isna().any():
        raise ValueError(f"feature snapshot contains invalid as_of values: {path}")
    return frame


def _resolve_as_of(feature_snapshot: pd.DataFrame, snapshot_as_of: str) -> str:
    """Resolve the snapshot as-of date from CLI override or the CSV contents."""
    if snapshot_as_of:
        return str(snapshot_as_of).strip()
    values = feature_snapshot["as_of"].dropna().dt.strftime("%Y-%m-%d").unique()
    if len(values) != 1:
        raise ValueError(
            "feature snapshot must contain exactly one as_of value when --snapshot-as-of is omitted; "
            f"found={sorted(values)!r}"
        )
    return str(values[0])


def _json_default(value: Any) -> Any:
    """JSON serializer fallback for non-standard types (Timestamp, numpy scalars)."""
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    """Write a JSON payload to disk with consistent formatting."""
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=_json_default),
        encoding="utf-8",
    )


def _positions_to_weights(decision: Any) -> dict[str, float]:
    """Extract target weights from a StrategyDecision's positions sequence."""
    weights: dict[str, float] = {}
    for position in getattr(decision, "positions", ()) or ():
        symbol = str(getattr(position, "symbol", "") or "").strip()
        if not symbol:
            continue
        target_weight = getattr(position, "target_weight", None)
        if target_weight is None:
            continue
        weights[symbol] = float(target_weight)
    return weights


def _evaluate_variant(
    *,
    entrypoint: Any,
    as_of: str,
    feature_snapshot: pd.DataFrame,
    portfolio_total_equity: float,
    runtime_overrides: dict[str, object],
) -> Any:
    """Evaluate the strategy with a given set of runtime config overrides."""
    from quant_platform_kit.common.models import PortfolioSnapshot
    from quant_platform_kit.strategy_contracts import StrategyContext

    return entrypoint.evaluate(
        StrategyContext(
            as_of=as_of,
            market_data={"feature_snapshot": feature_snapshot},
            portfolio=PortfolioSnapshot(
                as_of=as_of,
                total_equity=float(portfolio_total_equity),
                buying_power=float(portfolio_total_equity),
                cash_balance=float(portfolio_total_equity),
                positions=(),
            ),
            runtime_config=runtime_overrides,
        )
    )


def _compute_turnover_delta(active_weights: dict[str, float], variant_weights: dict[str, float]) -> float:
    """Compute the absolute weight difference / 2 (turnover) between two weight dicts."""
    all_symbols = set(active_weights) | set(variant_weights)
    total_diff = sum(
        abs(active_weights.get(sym, 0.0) - variant_weights.get(sym, 0.0))
        for sym in all_symbols
    )
    return total_diff / 2.0


def _classify_weight(weights: dict[str, float], safe_haven: str, offensive_symbols: set[str]) -> tuple[float, float]:
    """Split a weight dict into offensive and safe-haven buckets."""
    offensive = 0.0
    safe_haven_w = 0.0
    for symbol, weight in weights.items():
        if symbol == safe_haven:
            safe_haven_w += weight
        elif symbol in offensive_symbols:
            offensive += weight
        else:
            # Symbols not in the offensive pool default to offensive
            offensive += weight
    return offensive, safe_haven_w


def build_variant_comparison(
    *,
    entrypoint: Any,
    as_of: str,
    feature_snapshot: pd.DataFrame,
    portfolio_total_equity: float,
    active_variant: str = DEFAULT_ACTIVE_VARIANT,
    safe_haven: str = "BIL",
) -> dict[str, Any]:
    """Evaluate all shadow variants and build a side-by-side comparison."""
    # Determine the set of offensive symbols from the active variant's weights
    active_decision = _evaluate_variant(
        entrypoint=entrypoint,
        as_of=as_of,
        feature_snapshot=feature_snapshot,
        portfolio_total_equity=portfolio_total_equity,
        runtime_overrides=SHADOW_VARIANTS.get(active_variant, {}),
    )
    active_weights = _positions_to_weights(active_decision)
    offensive_symbols = set(active_weights.keys()) - {safe_haven}

    rows: list[dict[str, Any]] = []
    for variant_name, overrides in SHADOW_VARIANTS.items():
        decision = _evaluate_variant(
            entrypoint=entrypoint,
            as_of=as_of,
            feature_snapshot=feature_snapshot,
            portfolio_total_equity=portfolio_total_equity,
            runtime_overrides=overrides,
        )
        weights = _positions_to_weights(decision)
        diagnostics = dict(decision.diagnostics) if hasattr(decision, "diagnostics") else {}
        selected_count = int(diagnostics.get("selected_count") or 0)
        offensive_weight, safe_haven_weight = _classify_weight(weights, safe_haven, offensive_symbols)

        row: dict[str, Any] = {
            "variant": variant_name,
            "is_active": variant_name == active_variant,
            "selected_count": selected_count,
            "offensive_weight": offensive_weight,
            "safe_haven_weight": safe_haven_weight,
            "target_weights": weights,
        }

        if variant_name == active_variant:
            row["turnover_delta_vs_active"] = 0.0
        else:
            row["turnover_delta_vs_active"] = _compute_turnover_delta(active_weights, weights)

        rows.append(row)

    return {
        "as_of": as_of,
        "active_variant": active_variant,
        "variants": rows,
    }


def run_global_etf_rotation_shadow_cycle(
    *,
    feature_snapshot_path: str | Path,
    output_dir: str | Path,
    snapshot_as_of: str = "",
    active_variant: str = DEFAULT_ACTIVE_VARIANT,
    portfolio_total_equity: float = 100_000.0,
) -> ShadowCycleOutputs:
    """Run the Global ETF Rotation shadow cycle against a feature snapshot.

    Evaluates the strategy with each shadow variant configuration, records
    diagnostics for the active configuration, and writes both diagnostics and
    variant comparison output to the specified output directory.

    Parameters
    ----------
    feature_snapshot_path
        Path to the feature snapshot CSV file.
    output_dir
        Directory for output JSON artifacts.
    snapshot_as_of
        Optional snapshot as-of override (YYYY-MM-DD). If omitted, inferred
        from the CSV contents.
    active_variant
        Which variant to treat as the active (production) configuration.
    portfolio_total_equity
        Portfolio total equity used for deterministic runtime evaluation.

    Returns
    -------
    ShadowCycleOutputs
        Paths to the written diagnostics and variant comparison JSON files.
    """
    feature_snapshot_path = Path(feature_snapshot_path).expanduser().resolve()
    output_dir = Path(output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    from us_equity_strategies.catalog import get_strategy_entrypoint

    feature_snapshot = _load_feature_snapshot(feature_snapshot_path)
    snapshot_as_of_resolved = _resolve_as_of(feature_snapshot, snapshot_as_of)
    entrypoint = get_strategy_entrypoint(GLOBAL_ETF_ROTATION_PROFILE)

    active_overrides = SHADOW_VARIANTS.get(active_variant, {})
    active_decision = _evaluate_variant(
        entrypoint=entrypoint,
        as_of=snapshot_as_of_resolved,
        feature_snapshot=feature_snapshot,
        portfolio_total_equity=portfolio_total_equity,
        runtime_overrides=active_overrides,
    )

    variant_comparison = build_variant_comparison(
        entrypoint=entrypoint,
        as_of=snapshot_as_of_resolved,
        feature_snapshot=feature_snapshot,
        portfolio_total_equity=portfolio_total_equity,
        active_variant=active_variant,
    )

    diagnostics_payload: dict[str, Any] = {
        "strategy_profile": GLOBAL_ETF_ROTATION_PROFILE,
        "snapshot_as_of": snapshot_as_of_resolved,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "diagnostics": dict(active_decision.diagnostics) if hasattr(active_decision, "diagnostics") else {},
    }
    diagnostics_json = output_dir / "global_etf_rotation_runtime_diagnostics.json"
    _write_json(diagnostics_json, diagnostics_payload)

    comparison_json = output_dir / "global_etf_rotation_variant_comparison.json"
    _write_json(comparison_json, variant_comparison)

    return ShadowCycleOutputs(
        diagnostics_json=diagnostics_json,
        variant_comparison_json=comparison_json,
    )


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Run Global ETF Rotation shadow cycle: evaluate named runtime variants, "
            "emit variant comparison, and archive operator artifacts."
        )
    )
    parser.add_argument("--feature-snapshot", required=True, help="Global ETF feature snapshot CSV path")
    parser.add_argument("--output-dir", required=True, help="Directory for diagnostics and variant comparison artifacts")
    parser.add_argument("--snapshot-as-of", default="", help="Optional snapshot as-of override (YYYY-MM-DD)")
    parser.add_argument(
        "--active-variant",
        default=DEFAULT_ACTIVE_VARIANT,
        choices=tuple(SHADOW_VARIANTS),
        help="Active runtime variant used for returned positions and shadow deltas",
    )
    parser.add_argument(
        "--portfolio-total-equity",
        type=float,
        default=100_000.0,
        help="Portfolio equity used for deterministic runtime evaluation",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for the Global ETF Rotation shadow cycle."""
    args = build_parser().parse_args(argv)
    outputs = run_global_etf_rotation_shadow_cycle(
        feature_snapshot_path=args.feature_snapshot,
        output_dir=args.output_dir,
        snapshot_as_of=str(args.snapshot_as_of or ""),
        active_variant=str(args.active_variant),
        portfolio_total_equity=float(args.portfolio_total_equity),
    )
    print(f"runtime_diagnostics_json={outputs.diagnostics_json}")
    print(f"variant_comparison_json={outputs.variant_comparison_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
