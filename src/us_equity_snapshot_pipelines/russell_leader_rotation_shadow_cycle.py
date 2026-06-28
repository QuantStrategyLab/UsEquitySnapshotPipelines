from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from .contracts import RUSSELL_TOP50_LEADER_ROTATION_PROFILE
from .mega_cap_leader_rotation_shadow_review import build_shadow_review_artifacts

DEFAULT_ACTIVE_VARIANT = "blend_top2_50_top4_50"
NAMED_VARIANTS = (
    "top4_baseline",
    "blend_top2_25_top4_75",
    "blend_top2_50_top4_50",
)


@dataclass(frozen=True)
class ShadowCycleOutputs:
    diagnostics_json: Path
    variant_comparison_json: Path
    shadow_review_csv: Path
    shadow_review_json: Path
    shadow_review_manifest: Path


def _load_feature_snapshot(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    if "as_of" not in frame.columns:
        raise ValueError(f"feature snapshot missing as_of column: {path}")
    frame["as_of"] = pd.to_datetime(frame["as_of"], errors="coerce").dt.tz_localize(None)
    if frame["as_of"].isna().any():
        raise ValueError(f"feature snapshot contains invalid as_of values: {path}")
    return frame


def _resolve_run_as_of(feature_snapshot: pd.DataFrame, *, snapshot_as_of: str, run_as_of: str) -> str:
    if run_as_of:
        return str(run_as_of).strip()
    from us_equity_strategies.strategies.mega_cap_leader_rotation import evaluate_execution_window

    execution_window = evaluate_execution_window(
        feature_snapshot,
        run_as_of=pd.Timestamp(snapshot_as_of),
    )
    allowed_days = tuple(str(day) for day in execution_window.get("execution_window") or ())
    if not allowed_days:
        reason = str(execution_window.get("no_op_reason") or "unknown")
        raise ValueError(
            "no runtime execution window available for snapshot; "
            f"snapshot_as_of={snapshot_as_of} reason={reason}"
        )
    return allowed_days[0]


def _resolve_as_of(feature_snapshot: pd.DataFrame, snapshot_as_of: str) -> str:
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
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=_json_default),
        encoding="utf-8",
    )


def _positions_to_weights(decision: Any) -> dict[str, float]:
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
    active_variant: str,
    shadow_variants: bool,
) -> Any:
    from quant_platform_kit.common.models import PortfolioSnapshot
    from quant_platform_kit.strategy_contracts import StrategyContext

    runtime_config: dict[str, Any] = {"leader_rotation_profile_variant": active_variant}
    if shadow_variants:
        runtime_config["leader_rotation_shadow_variants"] = True
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
            runtime_config=runtime_config,
        )
    )


def build_variant_comparison(
    *,
    entrypoint: Any,
    as_of: str,
    feature_snapshot: pd.DataFrame,
    portfolio_total_equity: float,
    active_variant: str = DEFAULT_ACTIVE_VARIANT,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for variant in NAMED_VARIANTS:
        decision = _evaluate_variant(
            entrypoint=entrypoint,
            as_of=as_of,
            feature_snapshot=feature_snapshot,
            portfolio_total_equity=portfolio_total_equity,
            active_variant=variant,
            shadow_variants=False,
        )
        weights = _positions_to_weights(decision)
        rows.append(
            {
                "variant": variant,
                "is_active": variant == active_variant,
                "selected_count": int(decision.diagnostics.get("selected_count") or 0),
                "target_weights": weights,
                "realized_stock_weight": float(decision.diagnostics.get("realized_stock_weight") or 0.0),
                "safe_haven_weight": float(decision.diagnostics.get("safe_haven_weight") or 0.0),
            }
        )
    return {
        "as_of": as_of,
        "active_variant": active_variant,
        "variants": rows,
    }


def run_russell_leader_rotation_shadow_cycle(
    *,
    feature_snapshot_path: str | Path,
    output_dir: str | Path,
    snapshot_as_of: str = "",
    run_as_of: str = "",
    active_variant: str = DEFAULT_ACTIVE_VARIANT,
    portfolio_total_equity: float = 100_000.0,
) -> ShadowCycleOutputs:
    feature_snapshot_path = Path(feature_snapshot_path).expanduser().resolve()
    output_dir = Path(output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    from us_equity_strategies.catalog import get_strategy_entrypoint

    feature_snapshot = _load_feature_snapshot(feature_snapshot_path)
    snapshot_as_of_resolved = _resolve_as_of(feature_snapshot, snapshot_as_of)
    run_as_of_resolved = _resolve_run_as_of(
        feature_snapshot,
        snapshot_as_of=snapshot_as_of_resolved,
        run_as_of=run_as_of,
    )
    entrypoint = get_strategy_entrypoint(RUSSELL_TOP50_LEADER_ROTATION_PROFILE)

    active_decision = _evaluate_variant(
        entrypoint=entrypoint,
        as_of=run_as_of_resolved,
        feature_snapshot=feature_snapshot,
        portfolio_total_equity=portfolio_total_equity,
        active_variant=active_variant,
        shadow_variants=True,
    )
    variant_comparison = build_variant_comparison(
        entrypoint=entrypoint,
        as_of=run_as_of_resolved,
        feature_snapshot=feature_snapshot,
        portfolio_total_equity=portfolio_total_equity,
        active_variant=active_variant,
    )

    diagnostics_payload = {
        "strategy_profile": RUSSELL_TOP50_LEADER_ROTATION_PROFILE,
        "snapshot_as_of": snapshot_as_of_resolved,
        "run_as_of": run_as_of_resolved,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "diagnostics": dict(active_decision.diagnostics),
    }
    diagnostics_json = output_dir / "russell_leader_rotation_runtime_diagnostics.json"
    _write_json(diagnostics_json, diagnostics_payload)

    comparison_json = output_dir / "russell_leader_rotation_variant_comparison.json"
    _write_json(comparison_json, variant_comparison)

    shadow_outputs = build_shadow_review_artifacts(
        diagnostics_json,
        output_dir=output_dir,
        profile=RUSSELL_TOP50_LEADER_ROTATION_PROFILE,
        snapshot_as_of=snapshot_as_of_resolved,
    )
    return ShadowCycleOutputs(
        diagnostics_json=diagnostics_json,
        variant_comparison_json=comparison_json,
        shadow_review_csv=shadow_outputs.csv_path,
        shadow_review_json=shadow_outputs.json_path,
        shadow_review_manifest=shadow_outputs.manifest_path,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run Russell Top50 Phase-1 shadow cycle: evaluate named runtime variants, "
            "emit shadow review rows, and archive operator artifacts."
        )
    )
    parser.add_argument("--feature-snapshot", required=True, help="Russell feature snapshot CSV path")
    parser.add_argument("--output-dir", required=True, help="Directory for diagnostics and shadow review artifacts")
    parser.add_argument("--snapshot-as-of", default="", help="Optional snapshot as-of override (YYYY-MM-DD)")
    parser.add_argument(
        "--run-as-of",
        default="",
        help="Optional runtime evaluation date inside the monthly execution window (defaults to first allowed day)",
    )
    parser.add_argument(
        "--active-variant",
        default=DEFAULT_ACTIVE_VARIANT,
        choices=NAMED_VARIANTS,
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
    args = build_parser().parse_args(argv)
    outputs = run_russell_leader_rotation_shadow_cycle(
        feature_snapshot_path=args.feature_snapshot,
        output_dir=args.output_dir,
        snapshot_as_of=str(args.snapshot_as_of or ""),
        run_as_of=str(args.run_as_of or ""),
        active_variant=str(args.active_variant),
        portfolio_total_equity=float(args.portfolio_total_equity),
    )
    print(f"runtime_diagnostics_json={outputs.diagnostics_json}")
    print(f"variant_comparison_json={outputs.variant_comparison_json}")
    print(f"shadow_review_csv={outputs.shadow_review_csv}")
    print(f"shadow_review_manifest={outputs.shadow_review_manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
