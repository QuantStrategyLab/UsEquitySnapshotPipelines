from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from .mega_cap_leader_rotation_shadow_review import (
    RUNTIME_SHADOW_REVIEW_SCHEMA_VERSION,
    SHADOW_REVIEW_ROW_FIELDS,
    build_shadow_review_artifacts,
)
from .mega_cap_leader_rotation_stress_readiness import parse_csv_strings
from .pipelines.russell_1000_multi_factor_defensive_snapshot import read_table


def _final_target_weights(trades: pd.DataFrame, *, safe_haven: str) -> tuple[int, float, float]:
    frame = pd.DataFrame(trades).copy()
    if frame.empty:
        return 0, 0.0, 0.0
    frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce")
    frame = frame.dropna(subset=["Date", "Symbol"]).sort_values(["Date", "Symbol"])
    latest = frame.drop_duplicates(subset=["Symbol"], keep="last")
    latest["Target Weight"] = pd.to_numeric(latest["Target Weight"], errors="coerce").fillna(0.0)
    safe_symbol = str(safe_haven or "").upper()
    stock_weights = latest.loc[latest["Symbol"].astype(str).str.upper().ne(safe_symbol), "Target Weight"]
    safe_weight_series = latest.loc[latest["Symbol"].astype(str).str.upper().eq(safe_symbol), "Target Weight"]
    selected_count = int(stock_weights.gt(0.0).sum())
    realized_stock_weight = float(stock_weights.clip(lower=0.0).sum())
    safe_weight = float(safe_weight_series.iloc[-1]) if not safe_weight_series.empty else 0.0
    return selected_count, realized_stock_weight, safe_weight


def _turnover_delta(summary: pd.DataFrame, *, candidate_run: str, reference_run: str) -> float:
    frame = pd.DataFrame(summary).copy()
    if frame.empty or "Run" not in frame.columns:
        return 0.0
    indexed = frame.set_index("Run")
    candidate_turnover = pd.to_numeric(pd.Series([indexed.get("Turnover/Year", pd.Series()).get(candidate_run)]), errors="coerce").iloc[0]
    reference_turnover = pd.to_numeric(pd.Series([indexed.get("Turnover/Year", pd.Series()).get(reference_run)]), errors="coerce").iloc[0]
    candidate_turnover = float(candidate_turnover) if pd.notna(candidate_turnover) else 0.0
    reference_turnover = float(reference_turnover) if pd.notna(reference_turnover) else 0.0
    return candidate_turnover - reference_turnover


def _largest_moves(trades: pd.DataFrame) -> tuple[str, float, str, float]:
    frame = pd.DataFrame(trades).copy()
    if frame.empty:
        return "", 0.0, "", 0.0
    frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce")
    frame["Trade Weight Delta"] = pd.to_numeric(frame["Trade Weight Delta"], errors="coerce")
    latest_date = frame["Date"].max()
    latest = frame.loc[frame["Date"].eq(latest_date)].copy()
    if latest.empty:
        return "", 0.0, "", 0.0
    increases = latest.loc[latest["Trade Weight Delta"].gt(0.0)].sort_values("Trade Weight Delta", ascending=False)
    decreases = latest.loc[latest["Trade Weight Delta"].lt(0.0)].sort_values("Trade Weight Delta", ascending=True)
    inc_symbol = str(increases.iloc[0]["Symbol"]) if not increases.empty else ""
    inc_delta = float(increases.iloc[0]["Trade Weight Delta"]) if not increases.empty else 0.0
    dec_symbol = str(decreases.iloc[0]["Symbol"]) if not decreases.empty else ""
    dec_delta = float(decreases.iloc[0]["Trade Weight Delta"]) if not decreases.empty else 0.0
    return inc_symbol, inc_delta, dec_symbol, dec_delta


def build_shadow_review_input_payload(
    *,
    summary: pd.DataFrame,
    trades: pd.DataFrame,
    candidate_runs: tuple[str, ...],
    reference_run: str,
    safe_haven: str = "BOXX",
) -> dict[str, object]:
    rows: list[dict[str, object]] = []
    for candidate_run in candidate_runs:
        candidate_trades = pd.DataFrame(trades).loc[pd.DataFrame(trades)["Run"].astype(str).eq(candidate_run)].copy()
        selected_count, realized_stock_weight, safe_weight = _final_target_weights(
            candidate_trades,
            safe_haven=safe_haven,
        )
        inc_symbol, inc_delta, dec_symbol, dec_delta = _largest_moves(candidate_trades)
        turnover_delta = _turnover_delta(summary, candidate_run=candidate_run, reference_run=reference_run)
        rows.append(
            {
                "schema_version": RUNTIME_SHADOW_REVIEW_SCHEMA_VERSION,
                "active_variant": reference_run,
                "shadow_variant": candidate_run,
                "selected_count": selected_count,
                "realized_stock_weight": realized_stock_weight,
                "safe_haven_weight": safe_weight,
                "turnover_delta_vs_active": turnover_delta,
                "largest_increase_symbol": inc_symbol,
                "largest_increase_delta": inc_delta,
                "largest_decrease_symbol": dec_symbol,
                "largest_decrease_delta": dec_delta,
                "review_note": (
                    f"research-only shadow comparison active={reference_run} shadow={candidate_run} "
                    f"turnover_delta={turnover_delta:.4f}"
                ),
            }
        )
    return {
        "strategy_profile": "russell_top50_leader_rotation",
        "diagnostics": {
            "leader_rotation_shadow_review_schema_version": RUNTIME_SHADOW_REVIEW_SCHEMA_VERSION,
            "leader_rotation_shadow_review_row_fields": list(SHADOW_REVIEW_ROW_FIELDS),
            "leader_rotation_shadow_review_rows": rows,
        },
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a crash-brake shadow-review artifact compatible with the Russell shadow-review contract."
    )
    parser.add_argument("--summary", required=True, help="Input crash_brake_summary.csv")
    parser.add_argument("--trades", required=True, help="Input crash_brake_rebalance_trades.csv")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--candidate-runs", default="crash_brake_top2_50_floor25")
    parser.add_argument("--reference-run", default="blend_top2_50_top4_50_no_brake")
    parser.add_argument("--safe-haven", default="BOXX")
    parser.add_argument("--snapshot-as-of", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    candidate_runs = parse_csv_strings(args.candidate_runs, default=("crash_brake_top2_50_floor25",))
    payload = build_shadow_review_input_payload(
        summary=read_table(args.summary),
        trades=read_table(args.trades),
        candidate_runs=candidate_runs,
        reference_run=str(args.reference_run),
        safe_haven=str(args.safe_haven),
    )
    input_path = output_dir / "crash_brake_shadow_review_input.json"
    input_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    outputs = build_shadow_review_artifacts(
        input_path,
        output_dir=output_dir,
        profile="russell_top50_leader_rotation",
        snapshot_as_of=str(args.snapshot_as_of),
    )
    print(f"shadow_review_csv={outputs.csv_path}")
    print(f"shadow_review_json={outputs.json_path}")
    print(f"shadow_review_manifest={outputs.manifest_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
