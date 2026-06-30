from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from .global_etf_offensive_rotation_research import resolve_experiment_profile
from .global_etf_oos_window_diagnostics import build_global_etf_oos_window_diagnostics
from .live_replacement_review import _render_markdown, build_live_replacement_review
from .pipelines.russell_1000_multi_factor_defensive_snapshot import read_table

PROMOTION_BUNDLE_SCHEMA_VERSION = "global_etf_promotion_bundle.v1"


def _input_entry(path: str | Path | None) -> dict[str, object]:
    if path is None:
        return {}
    resolved = Path(path)
    payload: dict[str, object] = {"path": str(resolved)}
    if resolved.exists():
        payload["exists"] = True
    return payload


def _read_optional_table(path: str | Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    resolved = Path(path)
    if not resolved.exists():
        return pd.DataFrame()
    try:
        return read_table(resolved)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _normalize_candidate_ids(values: str | Iterable[str] | None) -> tuple[str, ...]:
    if values is None:
        return ()
    raw_values = values.split(",") if isinstance(values, str) else values
    cleaned: list[str] = []
    for value in raw_values:
        candidate_id = str(value or "").strip()
        if candidate_id and candidate_id not in cleaned:
            cleaned.append(candidate_id)
    return tuple(cleaned)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _filter_frame(frame: pd.DataFrame, column: str, candidate_ids: tuple[str, ...]) -> pd.DataFrame:
    if frame.empty or not candidate_ids or column not in frame.columns:
        return frame
    return frame.loc[frame[column].astype(str).isin(candidate_ids)].copy()


def _stress_gate_snapshot(frame: pd.DataFrame, *, axis_column: str) -> dict[str, object]:
    if frame.empty or axis_column not in frame.columns:
        return {}
    working = frame.copy()
    working[axis_column] = pd.to_numeric(working[axis_column], errors="coerce")
    working = working.dropna(subset=[axis_column])
    if working.empty:
        return {}
    max_value = float(working[axis_column].max())
    focus = working.loc[working[axis_column].eq(max_value)].copy()
    if focus.empty:
        return {}
    rows: list[dict[str, object]] = []
    for row in focus.to_dict(orient="records"):
        rows.append(
            {
                "candidate": str(row.get("Candidate") or ""),
                "live_gate_passed": bool(row.get("live_gate_passed", False)),
                "live_gate_reason": str(row.get("live_gate_reason") or ""),
            }
        )
    return {
        "axis_column": axis_column,
        "max_value": max_value,
        "candidate_rows": rows,
        "passed_candidates": [row["candidate"] for row in rows if row["live_gate_passed"]],
        "failed_candidates": [row["candidate"] for row in rows if not row["live_gate_passed"]],
    }


def _render_oos_report(
    *,
    summary: dict[str, object],
    monthly: pd.DataFrame,
    comparison: pd.DataFrame,
    signals: pd.DataFrame,
) -> str:
    if summary.get("status") == "unavailable":
        return "\n".join(
            [
                "# Global ETF Promotion Bundle Worst OOS Diagnostics",
                "",
                f"- Status: `{summary.get('status')}`",
                f"- Reason: `{summary.get('reason') or 'n/a'}`",
                "",
                "## Monthly excess returns",
                "",
                "No monthly rows.",
                "",
                "## Candidate comparison",
                "",
                "No candidate comparison rows.",
                "",
                "## Selected candidate overlay changes",
                "",
                "No overlay-change rows.",
                "",
            ]
        )
    lines = [
        f"# Global ETF Promotion Bundle Worst OOS Diagnostics ({summary['test_year']})",
        "",
        f"- Selected candidate: `{summary['selected_candidate']}`",
        f"- Baseline: `{summary['baseline_candidate']}`",
        f"- Test excess CAGR vs baseline: `{float(summary['test_excess_cagr_vs_baseline']):.6f}`",
        f"- Test drawdown delta vs baseline: `{float(summary['test_drawdown_delta_vs_baseline']):.6f}`",
        f"- Window return: `{float(summary['window_return']):.6f}`",
        f"- Baseline window return: `{float(summary['baseline_window_return']):.6f}`",
        f"- Window excess return: `{float(summary['window_excess_return']):.6f}`",
    ]
    if summary.get("worst_month") is not None:
        lines.append(
            f"- Worst month: `{summary['worst_month']}` ({float(summary['worst_month_excess_return']):.6f})"
        )
    lines.extend(
        [
            "",
            "## Monthly excess returns",
            "",
            monthly.to_csv(index=False).strip() if not monthly.empty else "No monthly rows.",
            "",
            "## Candidate comparison",
            "",
            comparison.to_csv(index=False).strip() if not comparison.empty else "No candidate comparison rows.",
            "",
            "## Selected candidate overlay changes",
            "",
            signals.to_csv(index=False).strip() if not signals.empty else "No overlay-change rows.",
            "",
        ]
    )
    return "\n".join(lines)


def build_global_etf_promotion_bundle(
    *,
    artifact_dir: Path,
    output_dir: Path,
    candidate_ids: tuple[str, ...] = (),
    global_etf_shadow_review: pd.DataFrame | None = None,
    global_etf_live_decay_summary: pd.DataFrame | None = None,
    global_etf_shadow_review_path: str | Path | None = None,
    global_etf_live_decay_path: str | Path | None = None,
    top_n_candidates: int = 5,
    experiment_profile_id: str | None = None,
) -> dict[str, object]:
    artifact_root = Path(artifact_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    ranking = read_table(artifact_root / "ranking.csv")
    live_readiness = read_table(artifact_root / "live_readiness_summary.csv")
    walk_forward_summary = _read_optional_table(artifact_root / "walk_forward_selection_summary.csv")
    cost_stress = _read_optional_table(artifact_root / "cost_stress_live_readiness_summary.csv")
    dynamic_cost_stress = _read_optional_table(artifact_root / "dynamic_cost_nav_stress_live_readiness_summary.csv")

    filtered_ranking = _filter_frame(ranking, "Candidate", candidate_ids)
    filtered_live_readiness = _filter_frame(live_readiness, "Candidate", candidate_ids)
    filtered_cost_stress = _filter_frame(cost_stress, "Candidate", candidate_ids)
    filtered_dynamic_cost_stress = _filter_frame(dynamic_cost_stress, "Candidate", candidate_ids)
    review = build_live_replacement_review(
        global_etf_ranking=filtered_ranking,
        global_etf_live_readiness=filtered_live_readiness,
        global_etf_walk_forward_summary=walk_forward_summary,
        global_etf_shadow_review=global_etf_shadow_review,
        global_etf_live_decay_summary=global_etf_live_decay_summary,
    )
    if candidate_ids:
        review = review.loc[review["candidate"].astype(str).isin(candidate_ids)].reset_index(drop=True)

    review_csv = output_dir / "live_replacement_review.csv"
    review_md = output_dir / "live_replacement_review.md"
    review.to_csv(review_csv, index=False)
    review_md.write_text(_render_markdown(review), encoding="utf-8")

    try:
        diagnostics = build_global_etf_oos_window_diagnostics(
            artifact_dir=artifact_root,
            top_n_candidates=int(top_n_candidates),
        )
    except (FileNotFoundError, RuntimeError, pd.errors.EmptyDataError) as exc:
        diagnostics = {
            "summary": {
                "status": "unavailable",
                "reason": f"{exc.__class__.__name__}: {exc}",
            },
            "monthly_returns": pd.DataFrame(),
            "candidate_comparison": pd.DataFrame(),
            "selected_candidate_signals": pd.DataFrame(),
        }
    oos_dir = output_dir / "worst_oos_window_diagnostics"
    oos_dir.mkdir(parents=True, exist_ok=True)
    monthly = pd.DataFrame(diagnostics["monthly_returns"])
    comparison = pd.DataFrame(diagnostics["candidate_comparison"])
    signals = pd.DataFrame(diagnostics["selected_candidate_signals"])
    summary = dict(diagnostics["summary"])
    _write_json(oos_dir / "worst_oos_window_summary.json", summary)
    monthly.to_csv(oos_dir / "worst_oos_window_monthly_returns.csv", index=False)
    comparison.to_csv(oos_dir / "worst_oos_window_candidate_comparison.csv", index=False)
    signals.to_csv(oos_dir / "worst_oos_window_selected_candidate_signals.csv", index=False)
    (oos_dir / "worst_oos_window_report.md").write_text(
        _render_oos_report(summary=summary, monthly=monthly, comparison=comparison, signals=signals),
        encoding="utf-8",
    )
    cost_stress_snapshot = _stress_gate_snapshot(filtered_cost_stress, axis_column="turnover_cost_bps")
    dynamic_cost_snapshot = _stress_gate_snapshot(
        filtered_dynamic_cost_stress, axis_column="Estimated Portfolio NAV"
    )

    bundle_summary = {
        "experiment_profile": experiment_profile_id,
        "candidate_ids": list(candidate_ids),
        "review_row_count": int(len(review)),
        "replace_live_now_count": int(review["replace_live_now"].sum()) if not review.empty else 0,
        "required_gates_passed_count": int(review["required_gates_passed"].sum()) if not review.empty else 0,
        "review_rows": review.to_dict(orient="records"),
        "cost_stress_snapshot": cost_stress_snapshot,
        "dynamic_cost_stress_snapshot": dynamic_cost_snapshot,
        "worst_oos_summary": summary,
    }
    _write_json(output_dir / "promotion_bundle_summary.json", bundle_summary)

    manifest = {
        "manifest_type": "global_etf_promotion_bundle",
        "artifact_schema_version": PROMOTION_BUNDLE_SCHEMA_VERSION,
        "source_artifact_dir": str(artifact_root),
        "experiment_profile": experiment_profile_id,
        "candidate_ids": list(candidate_ids),
        "inputs": {
            "ranking": _input_entry(artifact_root / "ranking.csv"),
            "live_readiness_summary": _input_entry(artifact_root / "live_readiness_summary.csv"),
            "walk_forward_selection_summary": _input_entry(artifact_root / "walk_forward_selection_summary.csv"),
            "walk_forward_selection_windows": _input_entry(artifact_root / "walk_forward_selection_windows.csv"),
            "portfolio_returns_with_benchmarks": _input_entry(artifact_root / "portfolio_returns_with_benchmarks.csv"),
            "rebalance_events": _input_entry(artifact_root / "rebalance_events.csv"),
            "cost_stress_live_readiness_summary": _input_entry(artifact_root / "cost_stress_live_readiness_summary.csv"),
            "dynamic_cost_nav_stress_live_readiness_summary": _input_entry(
                artifact_root / "dynamic_cost_nav_stress_live_readiness_summary.csv"
            ),
            "global_etf_shadow_review": _input_entry(global_etf_shadow_review_path),
            "global_etf_live_decay_summary": _input_entry(global_etf_live_decay_path),
        },
        "outputs": {
            "live_replacement_review_csv": str(review_csv),
            "live_replacement_review_md": str(review_md),
            "promotion_bundle_summary_json": str(output_dir / "promotion_bundle_summary.json"),
            "worst_oos_window_summary_json": str(oos_dir / "worst_oos_window_summary.json"),
            "worst_oos_window_report_md": str(oos_dir / "worst_oos_window_report.md"),
        },
        "replace_live_now_count": bundle_summary["replace_live_now_count"],
        "required_gates_passed_count": bundle_summary["required_gates_passed_count"],
        "cost_stress_snapshot": cost_stress_snapshot,
        "dynamic_cost_stress_snapshot": dynamic_cost_snapshot,
        "worst_oos_summary": summary,
    }
    _write_json(output_dir / "promotion_bundle_manifest.json", manifest)
    return {
        "review": review,
        "worst_oos_summary": summary,
        "manifest": manifest,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a narrow Global ETF promotion bundle from an existing research artifact directory."
    )
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--candidate-ids", help="Optional comma-separated candidate IDs to keep in the review bundle.")
    parser.add_argument(
        "--experiment-profile",
        help="Optional pre-registered experiment profile; when set, its liveable candidate set becomes the default review scope.",
    )
    parser.add_argument("--global-etf-shadow-review", help="Optional Global ETF shadow review CSV.")
    parser.add_argument("--global-etf-live-decay", help="Optional Global ETF live decay CSV.")
    parser.add_argument("--top-n-candidates", type=int, default=5)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    experiment_profile = resolve_experiment_profile(args.experiment_profile)
    candidate_ids = _normalize_candidate_ids(args.candidate_ids) or (
        experiment_profile.liveable_composite_ids if experiment_profile is not None else ()
    )
    artifact_dir = Path(args.artifact_dir).expanduser().resolve()
    output_dir = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else artifact_dir / "promotion_bundle"
    )
    build_global_etf_promotion_bundle(
        artifact_dir=artifact_dir,
        output_dir=output_dir,
        candidate_ids=candidate_ids,
        global_etf_shadow_review=read_table(args.global_etf_shadow_review) if args.global_etf_shadow_review else None,
        global_etf_live_decay_summary=read_table(args.global_etf_live_decay) if args.global_etf_live_decay else None,
        global_etf_shadow_review_path=args.global_etf_shadow_review,
        global_etf_live_decay_path=args.global_etf_live_decay,
        top_n_candidates=int(args.top_n_candidates),
        experiment_profile_id=experiment_profile.profile_id if experiment_profile is not None else None,
    )
    print(f"wrote Global ETF promotion bundle -> {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
