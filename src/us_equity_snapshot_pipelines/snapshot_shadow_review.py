from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .russell_1000_multi_factor_defensive_snapshot import read_table

WINDOWS = (63, 126, 252)
MAX_UNDERPERFORMANCE = -0.02
MAX_DRAWDOWN_DELTA = -0.03


def _normalize_returns(frame: pd.DataFrame) -> pd.DataFrame:
    returns = pd.DataFrame(frame).copy()
    if "as_of" in returns.columns:
        returns["as_of"] = pd.to_datetime(returns["as_of"], errors="coerce").dt.tz_localize(None).dt.normalize()
        returns = returns.dropna(subset=["as_of"]).set_index("as_of")
    else:
        returns.index = pd.to_datetime(returns.index, errors="coerce").tz_localize(None).normalize()
        returns = returns.loc[returns.index.notna()]
    for column in returns.columns:
        returns[column] = pd.to_numeric(returns[column], errors="coerce")
    return returns.sort_index()


def _drawdown(series: pd.Series) -> float:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return float("nan")
    equity = (1.0 + clean).cumprod()
    return float((equity / equity.cummax() - 1.0).min())


def _window_review(candidate_returns: pd.Series, active_returns: pd.Series, window: int) -> dict[str, float | int | bool | str]:
    aligned = pd.concat([candidate_returns.rename("candidate"), active_returns.rename("active")], axis=1).tail(int(window)).dropna()
    if aligned.empty:
        return {
            "window": f"trailing_{int(window)}d",
            "observations": 0,
            "candidate_total_return": float("nan"),
            "active_total_return": float("nan"),
            "candidate_minus_active_total_return": float("nan"),
            "candidate_drawdown": float("nan"),
            "active_drawdown": float("nan"),
            "candidate_minus_active_drawdown": float("nan"),
            "window_passed": False,
        }
    candidate_total = float((1.0 + aligned["candidate"]).prod() - 1.0)
    active_total = float((1.0 + aligned["active"]).prod() - 1.0)
    candidate_drawdown = _drawdown(aligned["candidate"])
    active_drawdown = _drawdown(aligned["active"])
    excess_total = candidate_total - active_total
    drawdown_delta = active_drawdown - candidate_drawdown
    passed = bool(excess_total >= MAX_UNDERPERFORMANCE and drawdown_delta >= MAX_DRAWDOWN_DELTA)
    return {
        "window": f"trailing_{int(window)}d",
        "observations": int(len(aligned)),
        "candidate_total_return": candidate_total,
        "active_total_return": active_total,
        "candidate_minus_active_total_return": excess_total,
        "candidate_drawdown": candidate_drawdown,
        "active_drawdown": active_drawdown,
        "candidate_minus_active_drawdown": drawdown_delta,
        "window_passed": passed,
    }


def build_snapshot_shadow_review_rows(
    ranking: pd.DataFrame,
    candidate_daily_returns: pd.DataFrame,
    *,
    candidate_ids: list[str] | None = None,
    active_candidate: str | None = None,
) -> pd.DataFrame:
    ranking_frame = pd.DataFrame(ranking).copy()
    returns = _normalize_returns(candidate_daily_returns)
    if ranking_frame.empty or returns.empty:
        return pd.DataFrame()
    if active_candidate is None:
        baseline_rows = ranking_frame.loc[ranking_frame["Candidate Group"].astype(str).eq("current_live_baseline")]
        if baseline_rows.empty:
            raise ValueError("snapshot ranking missing current_live_baseline candidate")
        active_candidate = str(baseline_rows.iloc[0]["Candidate"])
    candidate_list = candidate_ids or ranking_frame.loc[
        ranking_frame.get("replacement_review_candidate", False).astype(bool), "Candidate"
    ].astype(str).tolist()
    rows: list[dict[str, object]] = []
    for candidate in candidate_list:
        if candidate == active_candidate or candidate not in returns.columns or active_candidate not in returns.columns:
            continue
        window_reviews = [_window_review(returns[candidate], returns[active_candidate], window) for window in WINDOWS]
        review_frame = pd.DataFrame(window_reviews)
        passed = bool(review_frame["window_passed"].all()) if not review_frame.empty else False
        note = "; ".join(
            f"{row['window']}: excess={float(row['candidate_minus_active_total_return']):.2%}, drawdown_delta={float(row['candidate_minus_active_drawdown']):.2%}"
            for row in window_reviews
            if int(row["observations"]) > 0
        )
        rows.append(
            {
                "candidate": candidate,
                "active_candidate": active_candidate,
                "shadow_review_passed": passed,
                "review_note": note,
            }
        )
    return pd.DataFrame(rows)


def build_snapshot_shadow_review_artifacts(
    *,
    ranking_path: str | Path,
    candidate_returns_path: str | Path,
    output_dir: str | Path,
    candidate_ids: list[str] | None = None,
) -> tuple[Path, Path]:
    ranking = read_table(ranking_path)
    candidate_returns = read_table(candidate_returns_path)
    rows = build_snapshot_shadow_review_rows(ranking, candidate_returns, candidate_ids=candidate_ids)
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    csv_path = root / "snapshot_us_equity_shadow_review_rows.csv"
    manifest_path = root / "snapshot_us_equity_shadow_review_manifest.json"
    rows.to_csv(csv_path, index=False)
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_type": "shadow_review_artifact",
                "artifact_schema_version": "snapshot_shadow_review_artifact.v1",
                "generated_at": datetime.now(UTC).isoformat(),
                "artifacts": {"csv": {"path": csv_path.name}},
                "candidate_ids": candidate_ids or rows.get("candidate", pd.Series(dtype=str)).astype(str).tolist(),
                "inputs": {
                    "ranking": str(ranking_path),
                    "candidate_daily_returns": str(candidate_returns_path),
                },
            },
            ensure_ascii=False,
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )
    return csv_path, manifest_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build snapshot shadow review artifacts from candidate daily returns.")
    parser.add_argument("--ranking", required=True)
    parser.add_argument("--candidate-daily-returns", required=True)
    parser.add_argument("--candidate-ids", default="")
    parser.add_argument("--output-dir", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    candidate_ids = [item.strip() for item in str(args.candidate_ids).split(",") if item.strip()]
    csv_path, manifest_path = build_snapshot_shadow_review_artifacts(
        ranking_path=args.ranking,
        candidate_returns_path=args.candidate_daily_returns,
        output_dir=args.output_dir,
        candidate_ids=candidate_ids or None,
    )
    print(f"snapshot_shadow_review_csv={csv_path}")
    print(f"snapshot_shadow_review_manifest={manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
