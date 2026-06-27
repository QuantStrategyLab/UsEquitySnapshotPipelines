from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def _read_csv(path: Path, **kwargs) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"missing required artifact: {path}")
    return pd.read_csv(path, **kwargs)


def _normalize_dates(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    normalized = frame.copy()
    for column in columns:
        if column in normalized.columns:
            normalized[column] = pd.to_datetime(normalized[column], errors="coerce").dt.tz_localize(None).dt.normalize()
    return normalized


def build_global_etf_oos_window_diagnostics(
    *,
    artifact_dir: Path,
    top_n_candidates: int = 5,
) -> dict[str, pd.DataFrame | dict[str, object]]:
    artifact_root = Path(artifact_dir)
    windows = _read_csv(artifact_root / "walk_forward_selection_windows.csv")
    windows = _normalize_dates(windows, [])
    promoted = windows.loc[windows["Selection Action"].eq("promote_candidate")].copy()
    if promoted.empty:
        raise RuntimeError("no promote_candidate rows found in walk_forward_selection_windows.csv")
    promoted["Test Excess CAGR vs Baseline"] = pd.to_numeric(promoted["Test Excess CAGR vs Baseline"], errors="coerce")
    promoted = promoted.dropna(subset=["Test Excess CAGR vs Baseline"])
    if promoted.empty:
        raise RuntimeError("no numeric Test Excess CAGR vs Baseline values found for promoted rows")
    worst_row = promoted.sort_values("Test Excess CAGR vs Baseline", ascending=True).iloc[0]

    test_year = int(worst_row["Test Window"])
    selected_candidate = str(worst_row["Selected Candidate"])
    baseline_candidate = "live_global_etf_rotation_defensive_baseline"

    returns = _read_csv(artifact_root / "portfolio_returns_with_benchmarks.csv")
    returns = _normalize_dates(returns, ["as_of"])
    returns = returns.dropna(subset=["as_of"]).sort_values("as_of").reset_index(drop=True)
    if selected_candidate not in returns.columns or baseline_candidate not in returns.columns:
        raise RuntimeError("portfolio_returns_with_benchmarks.csv missing selected or baseline candidate columns")

    target_rows = returns.loc[returns["as_of"].dt.year.eq(test_year)].copy()
    target_rows["candidate_return"] = pd.to_numeric(target_rows[selected_candidate], errors="coerce").fillna(0.0)
    target_rows["baseline_return"] = pd.to_numeric(target_rows[baseline_candidate], errors="coerce").fillna(0.0)
    target_rows["daily_excess_return"] = target_rows["candidate_return"] - target_rows["baseline_return"]
    target_rows["month"] = target_rows["as_of"].dt.to_period("M").astype(str)

    monthly = (
        target_rows.groupby("month", dropna=False)
        .agg(
            candidate_return=("candidate_return", lambda series: float((1.0 + series).prod() - 1.0)),
            baseline_return=("baseline_return", lambda series: float((1.0 + series).prod() - 1.0)),
            trading_days=("candidate_return", "size"),
        )
        .reset_index()
    )
    monthly["excess_return"] = monthly["candidate_return"] - monthly["baseline_return"]

    ranking = _read_csv(artifact_root / "ranking.csv")
    ranking["rank"] = pd.to_numeric(ranking["rank"], errors="coerce")
    top_candidates = (
        ranking.loc[ranking["Candidate Group"].eq("liveable_candidate"), ["Candidate", "Display Name", "rank"]]
        .dropna(subset=["Candidate"])
        .sort_values("rank", ascending=True)
        .head(int(top_n_candidates))
    )
    comparison_rows: list[dict[str, object]] = []
    for _, row in top_candidates.iterrows():
        candidate = str(row["Candidate"])
        if candidate not in target_rows.columns:
            continue
        candidate_return = pd.to_numeric(target_rows[candidate], errors="coerce").fillna(0.0)
        comparison_rows.append(
            {
                "Candidate": candidate,
                "Display Name": row.get("Display Name"),
                "rank": int(row["rank"]) if pd.notna(row["rank"]) else None,
                "window_return": float((1.0 + candidate_return).prod() - 1.0),
                "window_excess_return_vs_baseline": float(
                    (1.0 + candidate_return).prod() - (1.0 + target_rows["baseline_return"]).prod()
                ),
            }
        )
    candidate_comparison = pd.DataFrame(comparison_rows).sort_values(
        "window_excess_return_vs_baseline", ascending=False
    )

    rebalance = _read_csv(artifact_root / "rebalance_events.csv")
    rebalance = _normalize_dates(rebalance, ["as_of", "next_date"])
    selected_signals = (
        rebalance.loc[
            rebalance["candidate_id"].astype(str).eq(selected_candidate)
            & rebalance["next_date"].dt.year.eq(test_year)
        ]
        .copy()
        .sort_values("next_date")
    )
    selected_signals = selected_signals[
        [
            column
            for column in [
                "candidate_id",
                "as_of",
                "next_date",
                "signal_description",
                "overlay_weight",
                "base_candidate_id",
                "overlay_candidate_id",
            ]
            if column in selected_signals.columns
        ]
    ]

    summary = {
        "test_year": test_year,
        "selected_candidate": selected_candidate,
        "baseline_candidate": baseline_candidate,
        "test_excess_cagr_vs_baseline": float(worst_row["Test Excess CAGR vs Baseline"]),
        "test_drawdown_delta_vs_baseline": float(
            pd.to_numeric(pd.Series([worst_row.get("Test Drawdown Delta vs Baseline")]), errors="coerce").iloc[0]
        ),
        "window_return": float((1.0 + target_rows["candidate_return"]).prod() - 1.0),
        "baseline_window_return": float((1.0 + target_rows["baseline_return"]).prod() - 1.0),
        "window_excess_return": float(
            (1.0 + target_rows["candidate_return"]).prod() - (1.0 + target_rows["baseline_return"]).prod()
        ),
        "worst_month": (
            monthly.sort_values("excess_return", ascending=True).iloc[0]["month"] if not monthly.empty else None
        ),
        "worst_month_excess_return": (
            float(monthly.sort_values("excess_return", ascending=True).iloc[0]["excess_return"])
            if not monthly.empty
            else None
        ),
    }
    return {
        "summary": summary,
        "monthly_returns": monthly,
        "candidate_comparison": candidate_comparison,
        "selected_candidate_signals": selected_signals,
    }


def _render_report(summary: dict[str, object], monthly: pd.DataFrame, comparison: pd.DataFrame, signals: pd.DataFrame) -> str:
    lines = [
        f"# Global ETF Worst OOS Window Diagnostics ({summary['test_year']})",
        "",
        f"- Selected candidate: `{summary['selected_candidate']}`",
        f"- Baseline: `{summary['baseline_candidate']}`",
        f"- Test excess CAGR vs baseline: `{summary['test_excess_cagr_vs_baseline']:.6f}`",
        f"- Test drawdown delta vs baseline: `{summary['test_drawdown_delta_vs_baseline']:.6f}`",
        f"- Window return: `{summary['window_return']:.6f}`",
        f"- Baseline window return: `{summary['baseline_window_return']:.6f}`",
        f"- Window excess return: `{summary['window_excess_return']:.6f}`",
        f"- Worst month: `{summary['worst_month']}` ({summary['worst_month_excess_return']:.6f})"
        if summary.get("worst_month") is not None
        else "- Worst month: `n/a`",
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
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build Global ETF worst-OOS-window diagnostics from research artifacts.")
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--top-n-candidates", type=int, default=5)
    args = parser.parse_args(argv)

    artifact_dir = Path(args.artifact_dir).expanduser().resolve()
    output_dir = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else artifact_dir / "worst_oos_window_diagnostics"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    diagnostics = build_global_etf_oos_window_diagnostics(
        artifact_dir=artifact_dir,
        top_n_candidates=int(args.top_n_candidates),
    )
    summary = diagnostics["summary"]
    monthly = pd.DataFrame(diagnostics["monthly_returns"])
    comparison = pd.DataFrame(diagnostics["candidate_comparison"])
    signals = pd.DataFrame(diagnostics["selected_candidate_signals"])

    (output_dir / "worst_oos_window_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    monthly.to_csv(output_dir / "worst_oos_window_monthly_returns.csv", index=False)
    comparison.to_csv(output_dir / "worst_oos_window_candidate_comparison.csv", index=False)
    signals.to_csv(output_dir / "worst_oos_window_selected_candidate_signals.csv", index=False)
    (output_dir / "worst_oos_window_report.md").write_text(_render_report(summary, monthly, comparison, signals))
    print(f"wrote global ETF OOS window diagnostics -> {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
