from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd

from .artifacts import sha256_file, write_json
from .mega_cap_leader_rotation_era_split_diagnostics import build_era_split_diagnostics, parse_era_specs
from .mega_cap_leader_rotation_mcs_diagnostics import build_mcs_style_diagnostics
from .mega_cap_leader_rotation_promotion_review import build_promotion_review
from .mega_cap_leader_rotation_reality_check import build_reality_check_diagnostics
from .mega_cap_leader_rotation_spa_check import build_spa_diagnostics
from .mega_cap_leader_rotation_stress_readiness import parse_csv_strings
from .russell_1000_multi_factor_defensive_snapshot import read_table

DEFAULT_CANDIDATE_RUNS = "base_top4_cap25,blend_top2_25_top4_75,blend_top2_50_top4_50"
DEFAULT_BOOTSTRAP_ITERATIONS = 1000
DEFAULT_BLOCK_SIZE = 21
DEFAULT_RANDOM_SEED = 42
DEFAULT_ALPHA = 0.10
PROMOTION_BUNDLE_MANIFEST_SCHEMA_VERSION = "russell_top50_promotion_bundle.v1"


def _artifact_entry(path: Path) -> dict[str, object]:
    return {"path": str(path), "sha256": sha256_file(path)}


def _input_entry(path: str | Path | None) -> dict[str, object]:
    if path is None:
        return {}
    resolved = Path(path)
    payload: dict[str, object] = {"path": str(resolved)}
    if resolved.exists():
        payload["sha256"] = sha256_file(resolved)
    return payload


def _manifest_payload(
    *,
    output_dir: Path,
    input_paths: Mapping[str, str | Path] | None,
    candidate_runs: tuple[str, ...],
    portfolio_nav: float | None,
    eras: str | Iterable[str] | None,
    bootstrap_iterations: int,
    block_size: int,
    random_seed: int,
    alpha: float,
    review: pd.DataFrame,
) -> dict[str, object]:
    artifacts = {
        "live_promotion_review": _artifact_entry(output_dir / "live_promotion_review.csv"),
        "reality_check_qqq_candidate": _artifact_entry(
            output_dir / "reality_check_qqq" / "reality_check_candidate_summary.csv"
        ),
        "reality_check_qqq_global": _artifact_entry(output_dir / "reality_check_qqq" / "reality_check_global_summary.csv"),
        "reality_check_spy_candidate": _artifact_entry(
            output_dir / "reality_check_spy" / "reality_check_candidate_summary.csv"
        ),
        "reality_check_spy_global": _artifact_entry(output_dir / "reality_check_spy" / "reality_check_global_summary.csv"),
        "spa_qqq_candidate": _artifact_entry(output_dir / "spa_qqq" / "spa_candidate_summary.csv"),
        "spa_qqq_global": _artifact_entry(output_dir / "spa_qqq" / "spa_global_summary.csv"),
        "spa_spy_candidate": _artifact_entry(output_dir / "spa_spy" / "spa_candidate_summary.csv"),
        "spa_spy_global": _artifact_entry(output_dir / "spa_spy" / "spa_global_summary.csv"),
        "era_split_candidate": _artifact_entry(output_dir / "era_split" / "era_split_candidate_summary.csv"),
        "era_split_promotion": _artifact_entry(output_dir / "era_split" / "era_split_promotion_summary.csv"),
        "mcs_style_candidate": _artifact_entry(output_dir / "mcs_style" / "mcs_style_candidate_summary.csv"),
        "mcs_style_pairwise": _artifact_entry(output_dir / "mcs_style" / "mcs_style_pairwise_summary.csv"),
        "mcs_style_global": _artifact_entry(output_dir / "mcs_style" / "mcs_style_global_summary.csv"),
    }
    review_rows = []
    for row in review.to_dict(orient="records"):
        review_rows.append(
            {
                "run": str(row.get("Run", "")),
                "required_gates_passed": bool(row.get("required_gates_passed", False)),
                "statistical_support_level": str(row.get("statistical_support_level", "")),
                "promotion_decision": str(row.get("promotion_decision", "")),
                "recommended_action": str(row.get("recommended_action", "")),
            }
        )
    return {
        "manifest_type": "russell_top50_promotion_bundle",
        "artifact_schema_version": PROMOTION_BUNDLE_MANIFEST_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "candidate_runs": list(candidate_runs),
        "portfolio_nav": portfolio_nav,
        "eras": list(eras) if not isinstance(eras, str) and eras is not None else eras,
        "bootstrap": {
            "iterations": int(bootstrap_iterations),
            "block_size": int(block_size),
            "random_seed": int(random_seed),
            "alpha": float(alpha),
        },
        "inputs": {name: _input_entry(path) for name, path in (input_paths or {}).items()},
        "artifacts": artifacts,
        "review_rows": review_rows,
    }


def _write_frame(frame: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return path


def _run_reality_and_spa(
    daily_returns: pd.DataFrame,
    *,
    candidate_runs: Iterable[str],
    output_dir: Path,
    bootstrap_iterations: int,
    block_size: int,
    random_seed: int,
    alpha: float,
) -> dict[str, pd.DataFrame]:
    outputs: dict[str, pd.DataFrame] = {}
    for label, benchmark_column in (("qqq", "QQQ Return"), ("spy", "SPY Return")):
        reality = build_reality_check_diagnostics(
            daily_returns,
            benchmark_column=benchmark_column,
            candidate_runs=candidate_runs,
            bootstrap_iterations=bootstrap_iterations,
            block_size=block_size,
            random_seed=random_seed,
            alpha=alpha,
        )
        spa = build_spa_diagnostics(
            daily_returns,
            benchmark_column=benchmark_column,
            candidate_runs=candidate_runs,
            bootstrap_iterations=bootstrap_iterations,
            block_size=block_size,
            random_seed=random_seed,
            alpha=alpha,
        )
        reality_dir = output_dir / f"reality_check_{label}"
        spa_dir = output_dir / f"spa_{label}"
        _write_frame(reality["reality_check_candidate_summary"], reality_dir / "reality_check_candidate_summary.csv")
        _write_frame(reality["reality_check_global_summary"], reality_dir / "reality_check_global_summary.csv")
        _write_frame(spa["spa_candidate_summary"], spa_dir / "spa_candidate_summary.csv")
        _write_frame(spa["spa_global_summary"], spa_dir / "spa_global_summary.csv")
        outputs[f"reality_check_{label}"] = reality["reality_check_candidate_summary"]
        outputs[f"spa_{label}"] = spa["spa_candidate_summary"]
    return outputs


def build_promotion_bundle(
    *,
    concentration_summary: pd.DataFrame,
    daily_returns: pd.DataFrame,
    live_readiness: pd.DataFrame,
    stress_summary: pd.DataFrame,
    overfit_promotion: pd.DataFrame,
    liquidity_summary: pd.DataFrame,
    output_dir: Path,
    candidate_runs: Iterable[str],
    portfolio_nav: float | None,
    eras: str | Iterable[str] | None,
    bootstrap_iterations: int = DEFAULT_BOOTSTRAP_ITERATIONS,
    block_size: int = DEFAULT_BLOCK_SIZE,
    random_seed: int = DEFAULT_RANDOM_SEED,
    alpha: float = DEFAULT_ALPHA,
    input_paths: Mapping[str, str | Path] | None = None,
) -> dict[str, pd.DataFrame]:
    runs = tuple(str(run) for run in candidate_runs)
    output_dir.mkdir(parents=True, exist_ok=True)
    statistical = _run_reality_and_spa(
        daily_returns,
        candidate_runs=runs,
        output_dir=output_dir,
        bootstrap_iterations=int(bootstrap_iterations),
        block_size=int(block_size),
        random_seed=int(random_seed),
        alpha=float(alpha),
    )
    era = build_era_split_diagnostics(
        daily_returns,
        eras=parse_era_specs(eras),
        candidate_runs=runs,
    )
    mcs = build_mcs_style_diagnostics(
        daily_returns,
        candidate_runs=runs,
        bootstrap_iterations=int(bootstrap_iterations),
        block_size=int(block_size),
        random_seed=int(random_seed),
        alpha=float(alpha),
    )
    era_dir = output_dir / "era_split"
    mcs_dir = output_dir / "mcs_style"
    _write_frame(era["era_split_candidate_summary"], era_dir / "era_split_candidate_summary.csv")
    _write_frame(era["era_split_promotion_summary"], era_dir / "era_split_promotion_summary.csv")
    _write_frame(mcs["mcs_style_candidate_summary"], mcs_dir / "mcs_style_candidate_summary.csv")
    _write_frame(mcs["mcs_style_pairwise_summary"], mcs_dir / "mcs_style_pairwise_summary.csv")
    _write_frame(mcs["mcs_style_global_summary"], mcs_dir / "mcs_style_global_summary.csv")
    review = build_promotion_review(
        concentration_summary,
        live_readiness=live_readiness,
        stress_summary=stress_summary,
        overfit_promotion=overfit_promotion,
        liquidity_summary=liquidity_summary,
        reality_check_qqq=statistical["reality_check_qqq"],
        reality_check_spy=statistical["reality_check_spy"],
        spa_qqq=statistical["spa_qqq"],
        spa_spy=statistical["spa_spy"],
        era_split_promotion=era["era_split_promotion_summary"],
        mcs_style_summary=mcs["mcs_style_candidate_summary"],
        candidate_runs=runs,
        portfolio_nav=portfolio_nav,
    )
    _write_frame(review, output_dir / "live_promotion_review.csv")
    write_json(
        output_dir / "promotion_bundle_manifest.json",
        _manifest_payload(
            output_dir=output_dir,
            input_paths=input_paths,
            candidate_runs=runs,
            portfolio_nav=portfolio_nav,
            eras=eras,
            bootstrap_iterations=int(bootstrap_iterations),
            block_size=int(block_size),
            random_seed=int(random_seed),
            alpha=float(alpha),
            review=review,
        ),
    )
    return {
        **statistical,
        "era_split_candidate_summary": era["era_split_candidate_summary"],
        "era_split_promotion_summary": era["era_split_promotion_summary"],
        "mcs_style_candidate_summary": mcs["mcs_style_candidate_summary"],
        "mcs_style_pairwise_summary": mcs["mcs_style_pairwise_summary"],
        "mcs_style_global_summary": mcs["mcs_style_global_summary"],
        "live_promotion_review": review,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the Russell fixed-candidate promotion review bundle.")
    parser.add_argument("--summary", required=True, help="Input concentration_variant_summary.csv")
    parser.add_argument("--daily-returns", required=True, help="Input concentration_variant_daily_returns.csv")
    parser.add_argument("--live-readiness", required=True, help="Input live_readiness_summary.csv")
    parser.add_argument("--stress-summary", required=True, help="Input stress_live_readiness_summary.csv")
    parser.add_argument("--overfit-promotion", required=True, help="Input overfit_promotion_gate_summary.csv")
    parser.add_argument("--liquidity-summary", required=True, help="Input liquidity_summary.csv")
    parser.add_argument("--candidate-runs", default=DEFAULT_CANDIDATE_RUNS)
    parser.add_argument("--portfolio-nav", type=float)
    parser.add_argument("--eras", default=None, help="Optional comma-separated name:start:end era specs")
    parser.add_argument("--bootstrap-iterations", type=int, default=DEFAULT_BOOTSTRAP_ITERATIONS)
    parser.add_argument("--block-size", type=int, default=DEFAULT_BLOCK_SIZE)
    parser.add_argument("--random-seed", type=int, default=DEFAULT_RANDOM_SEED)
    parser.add_argument("--alpha", type=float, default=DEFAULT_ALPHA)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--print-top", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_promotion_bundle(
        concentration_summary=read_table(args.summary),
        daily_returns=read_table(args.daily_returns),
        live_readiness=read_table(args.live_readiness),
        stress_summary=read_table(args.stress_summary),
        overfit_promotion=read_table(args.overfit_promotion),
        liquidity_summary=read_table(args.liquidity_summary),
        output_dir=Path(args.output_dir),
        candidate_runs=parse_csv_strings(args.candidate_runs, default=()),
        portfolio_nav=args.portfolio_nav,
        eras=args.eras,
        bootstrap_iterations=int(args.bootstrap_iterations),
        block_size=int(args.block_size),
        random_seed=int(args.random_seed),
        alpha=float(args.alpha),
        input_paths={
            "summary": args.summary,
            "daily_returns": args.daily_returns,
            "live_readiness": args.live_readiness,
            "stress_summary": args.stress_summary,
            "overfit_promotion": args.overfit_promotion,
            "liquidity_summary": args.liquidity_summary,
        },
    )
    print(result["live_promotion_review"].head(max(int(args.print_top), 0)).to_string(index=False))
    print(f"wrote promotion bundle -> {Path(args.output_dir)}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
