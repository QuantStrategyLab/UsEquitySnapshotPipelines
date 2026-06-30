from __future__ import annotations

import argparse
from math import ceil
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from .mega_cap_leader_rotation_stress_readiness import parse_csv_strings
from .pipelines.russell_1000_multi_factor_defensive_snapshot import read_table

DEFAULT_BENCHMARK_COLUMN = "QQQ Return"
DEFAULT_BOOTSTRAP_ITERATIONS = 1000
DEFAULT_BLOCK_SIZE = 21
DEFAULT_RANDOM_SEED = 42
DEFAULT_ALPHA = 0.10
CANDIDATE_SUMMARY_COLUMNS = (
    "Run",
    "Benchmark Column",
    "Observations",
    "Mean Daily Excess Return",
    "Annualized Mean Excess Return",
    "Daily Excess Volatility",
    "Excess Return T Statistic",
    "Observed Best Candidate",
    "Reality Check P Value",
    "Reality Check Alpha",
    "Reality Check Passed",
    "diagnostic_scope",
    "recommended_action",
)
GLOBAL_SUMMARY_COLUMNS = (
    "Benchmark Column",
    "Candidate Count",
    "Observations",
    "Bootstrap Iterations",
    "Block Size",
    "Random Seed",
    "Best Run",
    "Best Annualized Mean Excess Return",
    "Reality Check P Value",
    "Reality Check Alpha",
    "Reality Check Passed",
    "diagnostic_scope",
)


def _prepare_excess_returns(
    daily_returns: pd.DataFrame,
    *,
    benchmark_column: str,
    candidate_runs: Iterable[str] | None = None,
) -> pd.DataFrame:
    frame = pd.DataFrame(daily_returns).copy()
    required = {"Date", "Run", "Strategy Return", benchmark_column}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"daily_returns must include columns: {', '.join(missing)}")
    frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce").dt.tz_localize(None)
    frame["Strategy Return"] = pd.to_numeric(frame["Strategy Return"], errors="coerce")
    frame[benchmark_column] = pd.to_numeric(frame[benchmark_column], errors="coerce")
    frame = frame.dropna(subset=["Date", "Run", "Strategy Return", benchmark_column])
    runs = tuple(str(run) for run in candidate_runs or ())
    if runs:
        frame = frame.loc[frame["Run"].astype(str).isin(runs)].copy()
    if frame.empty:
        raise ValueError("daily_returns has no rows after filtering")
    strategy = frame.pivot_table(index="Date", columns="Run", values="Strategy Return", aggfunc="first").sort_index()
    benchmark = frame.groupby("Date", sort=True)[benchmark_column].first().reindex(strategy.index)
    excess = strategy.sub(benchmark, axis=0).dropna(axis=0, how="any")
    if excess.empty:
        raise ValueError("daily_returns has no complete candidate/benchmark rows")
    return excess


def _circular_block_indices(*, observations: int, block_size: int, rng: np.random.Generator) -> np.ndarray:
    n = int(observations)
    block = max(int(block_size), 1)
    starts = rng.integers(0, n, size=int(ceil(n / block)))
    offsets = np.arange(block)
    return np.concatenate([(start + offsets) % n for start in starts])[:n]


def _bootstrap_reality_check_p_value(
    excess_returns: pd.DataFrame,
    *,
    bootstrap_iterations: int,
    block_size: int,
    random_seed: int,
) -> tuple[float, str, float]:
    excess = excess_returns.astype(float)
    observed_means = excess.mean(axis=0)
    best_run = str(observed_means.idxmax())
    observed_best = float(observed_means.max())
    centered = excess - observed_means
    rng = np.random.default_rng(int(random_seed))
    exceedances = 0
    values = centered.to_numpy(copy=True)
    n = len(centered)
    for _ in range(int(bootstrap_iterations)):
        sampled = values[_circular_block_indices(observations=n, block_size=int(block_size), rng=rng)]
        bootstrap_best = float(np.nanmax(sampled.mean(axis=0)))
        if bootstrap_best >= observed_best:
            exceedances += 1
    p_value = (1.0 + float(exceedances)) / (1.0 + float(bootstrap_iterations))
    return p_value, best_run, observed_best


def build_reality_check_diagnostics(
    daily_returns: pd.DataFrame,
    *,
    benchmark_column: str = DEFAULT_BENCHMARK_COLUMN,
    candidate_runs: Iterable[str] | None = None,
    bootstrap_iterations: int = DEFAULT_BOOTSTRAP_ITERATIONS,
    block_size: int = DEFAULT_BLOCK_SIZE,
    random_seed: int = DEFAULT_RANDOM_SEED,
    alpha: float = DEFAULT_ALPHA,
) -> dict[str, pd.DataFrame]:
    iterations = int(bootstrap_iterations)
    if iterations <= 0:
        raise ValueError("bootstrap_iterations must be positive")
    excess = _prepare_excess_returns(daily_returns, benchmark_column=benchmark_column, candidate_runs=candidate_runs)
    p_value, best_run, observed_best = _bootstrap_reality_check_p_value(
        excess,
        bootstrap_iterations=iterations,
        block_size=int(block_size),
        random_seed=int(random_seed),
    )
    passed = bool(observed_best > 0.0 and p_value <= float(alpha))
    rows: list[dict[str, object]] = []
    observations = int(len(excess))
    for run in excess.columns:
        values = pd.to_numeric(excess[run], errors="coerce").dropna()
        mean = float(values.mean())
        std = float(values.std(ddof=1)) if len(values) > 1 else float("nan")
        t_stat = mean / (std / np.sqrt(len(values))) if std and not np.isnan(std) else float("nan")
        is_best = str(run) == best_run
        rows.append(
            {
                "Run": str(run),
                "Benchmark Column": benchmark_column,
                "Observations": int(len(values)),
                "Mean Daily Excess Return": mean,
                "Annualized Mean Excess Return": mean * 252.0,
                "Daily Excess Volatility": std,
                "Excess Return T Statistic": t_stat,
                "Observed Best Candidate": bool(is_best),
                "Reality Check P Value": p_value,
                "Reality Check Alpha": float(alpha),
                "Reality Check Passed": bool(is_best and passed),
                "diagnostic_scope": "return_panel_bootstrap_not_live_gate",
                "recommended_action": "statistical_review_candidate" if is_best and passed else "statistical_research_only",
            }
        )
    candidate_summary = pd.DataFrame(rows).loc[:, list(CANDIDATE_SUMMARY_COLUMNS)]
    candidate_summary = candidate_summary.sort_values(
        ["Observed Best Candidate", "Annualized Mean Excess Return"],
        ascending=[False, False],
        kind="stable",
    ).reset_index(drop=True)
    global_summary = pd.DataFrame(
        [
            {
                "Benchmark Column": benchmark_column,
                "Candidate Count": int(excess.shape[1]),
                "Observations": observations,
                "Bootstrap Iterations": iterations,
                "Block Size": int(block_size),
                "Random Seed": int(random_seed),
                "Best Run": best_run,
                "Best Annualized Mean Excess Return": observed_best * 252.0,
                "Reality Check P Value": p_value,
                "Reality Check Alpha": float(alpha),
                "Reality Check Passed": passed,
                "diagnostic_scope": "return_panel_bootstrap_not_live_gate",
            }
        ],
        columns=GLOBAL_SUMMARY_COLUMNS,
    )
    return {
        "reality_check_candidate_summary": candidate_summary,
        "reality_check_global_summary": global_summary,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a bootstrap Reality Check diagnostic from Russell candidate daily return panels."
    )
    parser.add_argument("--daily-returns", required=True, help="Input concentration_variant_daily_returns.csv")
    parser.add_argument("--output-dir", required=True, help="Directory for Reality Check outputs")
    parser.add_argument("--benchmark-column", default=DEFAULT_BENCHMARK_COLUMN)
    parser.add_argument(
        "--candidate-runs",
        default="",
        help="Optional comma-separated candidate run filter. Empty means all runs in the daily-return panel.",
    )
    parser.add_argument("--bootstrap-iterations", type=int, default=DEFAULT_BOOTSTRAP_ITERATIONS)
    parser.add_argument("--block-size", type=int, default=DEFAULT_BLOCK_SIZE)
    parser.add_argument("--random-seed", type=int, default=DEFAULT_RANDOM_SEED)
    parser.add_argument("--alpha", type=float, default=DEFAULT_ALPHA)
    parser.add_argument("--print-top", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    candidates = parse_csv_strings(args.candidate_runs, default=())
    result = build_reality_check_diagnostics(
        read_table(args.daily_returns),
        benchmark_column=args.benchmark_column,
        candidate_runs=candidates,
        bootstrap_iterations=int(args.bootstrap_iterations),
        block_size=int(args.block_size),
        random_seed=int(args.random_seed),
        alpha=float(args.alpha),
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    candidate_path = output_dir / "reality_check_candidate_summary.csv"
    global_path = output_dir / "reality_check_global_summary.csv"
    result["reality_check_candidate_summary"].to_csv(candidate_path, index=False)
    result["reality_check_global_summary"].to_csv(global_path, index=False)
    print(result["reality_check_candidate_summary"].head(max(int(args.print_top), 0)).to_string(index=False))
    print(result["reality_check_global_summary"].to_string(index=False))
    print(f"wrote Reality Check candidate summary -> {candidate_path}")
    print(f"wrote Reality Check global summary -> {global_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
