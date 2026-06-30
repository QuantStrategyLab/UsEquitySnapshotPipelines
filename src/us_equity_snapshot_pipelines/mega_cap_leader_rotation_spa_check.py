"""SPA-style return-panel diagnostic for frozen Russell candidate sets.

The implementation deliberately avoids adding a new production dependency. It
uses the same circular block-bootstrap machinery as the local Reality Check
diagnostic, but studentizes candidate excess returns and reports lower,
consistent, and upper re-centering p-values in the spirit of Hansen's SPA test.

This is a research diagnostic, not a formal replacement for a validated
econometrics package such as ``arch.bootstrap.SPA``.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from .mega_cap_leader_rotation_reality_check import (
    DEFAULT_ALPHA,
    DEFAULT_BENCHMARK_COLUMN,
    DEFAULT_BLOCK_SIZE,
    DEFAULT_BOOTSTRAP_ITERATIONS,
    DEFAULT_RANDOM_SEED,
    _circular_block_indices,
    _prepare_excess_returns,
)
from .mega_cap_leader_rotation_stress_readiness import parse_csv_strings
from .pipelines.russell_1000_multi_factor_defensive_snapshot import read_table

SPA_CANDIDATE_SUMMARY_COLUMNS = (
    "Run",
    "Benchmark Column",
    "Observations",
    "Mean Daily Excess Return",
    "Annualized Mean Excess Return",
    "Daily Excess Volatility",
    "SPA T Statistic",
    "Observed Best Candidate",
    "SPA Lower P Value",
    "SPA Consistent P Value",
    "SPA Upper P Value",
    "SPA Alpha",
    "SPA Passed",
    "SPA Recenter Bound",
    "diagnostic_scope",
    "recommended_action",
)
SPA_GLOBAL_SUMMARY_COLUMNS = (
    "Benchmark Column",
    "Candidate Count",
    "Observations",
    "Bootstrap Iterations",
    "Block Size",
    "Random Seed",
    "Best Run",
    "Best Annualized Mean Excess Return",
    "Best SPA T Statistic",
    "SPA Lower P Value",
    "SPA Consistent P Value",
    "SPA Upper P Value",
    "SPA Alpha",
    "SPA Passed",
    "SPA Recenter Bound",
    "diagnostic_scope",
)
DIAGNOSTIC_SCOPE = "studentized_spa_style_bootstrap_not_live_gate"


def _positive_int(value: int, *, name: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{name} must be positive")
    return parsed


def _studentized_inputs(excess_returns: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, pd.Series, pd.Series, float]:
    excess = excess_returns.astype(float).dropna(axis=0, how="any")
    if excess.empty:
        raise ValueError("excess_returns has no complete rows")
    observations = len(excess)
    means = excess.mean(axis=0)
    vol = excess.std(axis=0, ddof=1)
    finite_vol = vol.replace([np.inf, -np.inf], np.nan)
    usable = finite_vol > 0.0
    if not bool(usable.any()):
        raise ValueError("SPA diagnostic requires at least one candidate with non-zero excess-return volatility")
    excess = excess.loc[:, usable]
    means = means.loc[usable]
    vol = finite_vol.loc[usable]
    t_stats = np.sqrt(float(observations)) * means / vol
    recenter_bound = -np.sqrt(2.0 * np.log(np.log(max(float(observations), np.e + 1.0))))
    return excess, means, vol, t_stats, float(recenter_bound)


def _bootstrap_spa_p_values(
    excess_returns: pd.DataFrame,
    *,
    bootstrap_iterations: int,
    block_size: int,
    random_seed: int,
) -> tuple[dict[str, float], str, float, float, float, pd.Series, pd.Series, pd.Series]:
    iterations = _positive_int(bootstrap_iterations, name="bootstrap_iterations")
    block = _positive_int(block_size, name="block_size")
    excess, means, vol, t_stats, recenter_bound = _studentized_inputs(excess_returns)
    observed_stat = float(max(0.0, t_stats.max()))
    best_run = str(t_stats.idxmax())
    observed_best_mean = float(means.loc[best_run])

    lower_adjustment = np.minimum(t_stats.to_numpy(dtype=float), 0.0)
    consistent_adjustment = np.where(t_stats.to_numpy(dtype=float) <= recenter_bound, t_stats.to_numpy(dtype=float), 0.0)
    upper_adjustment = np.zeros_like(lower_adjustment)
    adjustments = {
        "lower": lower_adjustment,
        "consistent": consistent_adjustment,
        "upper": upper_adjustment,
    }
    exceedances = {name: 0 for name in adjustments}

    centered = excess - means
    values = centered.to_numpy(copy=True)
    vol_values = vol.to_numpy(dtype=float)
    n = len(centered)
    rng = np.random.default_rng(int(random_seed))
    for _ in range(iterations):
        sampled = values[_circular_block_indices(observations=n, block_size=block, rng=rng)]
        bootstrap_t = np.sqrt(float(n)) * np.nanmean(sampled, axis=0) / vol_values
        for name, adjustment in adjustments.items():
            bootstrap_stat = float(max(0.0, np.nanmax(bootstrap_t + adjustment)))
            if bootstrap_stat >= observed_stat:
                exceedances[name] += 1
    p_values = {name: (1.0 + float(count)) / (1.0 + float(iterations)) for name, count in exceedances.items()}
    return p_values, best_run, observed_best_mean, observed_stat, recenter_bound, means, vol, t_stats


def build_spa_diagnostics(
    daily_returns: pd.DataFrame,
    *,
    benchmark_column: str = DEFAULT_BENCHMARK_COLUMN,
    candidate_runs: Iterable[str] | None = None,
    bootstrap_iterations: int = DEFAULT_BOOTSTRAP_ITERATIONS,
    block_size: int = DEFAULT_BLOCK_SIZE,
    random_seed: int = DEFAULT_RANDOM_SEED,
    alpha: float = DEFAULT_ALPHA,
) -> dict[str, pd.DataFrame]:
    excess = _prepare_excess_returns(daily_returns, benchmark_column=benchmark_column, candidate_runs=candidate_runs)
    p_values, best_run, observed_best, observed_stat, recenter_bound, means, vol, t_stats = _bootstrap_spa_p_values(
        excess,
        bootstrap_iterations=int(bootstrap_iterations),
        block_size=int(block_size),
        random_seed=int(random_seed),
    )
    passed = bool(observed_best > 0.0 and p_values["consistent"] <= float(alpha))
    observations = int(len(excess.dropna(axis=0, how="any")))
    rows: list[dict[str, object]] = []
    for run in means.index:
        is_best = str(run) == best_run
        rows.append(
            {
                "Run": str(run),
                "Benchmark Column": benchmark_column,
                "Observations": observations,
                "Mean Daily Excess Return": float(means.loc[run]),
                "Annualized Mean Excess Return": float(means.loc[run]) * 252.0,
                "Daily Excess Volatility": float(vol.loc[run]),
                "SPA T Statistic": float(t_stats.loc[run]),
                "Observed Best Candidate": bool(is_best),
                "SPA Lower P Value": p_values["lower"],
                "SPA Consistent P Value": p_values["consistent"],
                "SPA Upper P Value": p_values["upper"],
                "SPA Alpha": float(alpha),
                "SPA Passed": bool(is_best and passed),
                "SPA Recenter Bound": recenter_bound,
                "diagnostic_scope": DIAGNOSTIC_SCOPE,
                "recommended_action": "spa_statistical_review_candidate" if is_best and passed else "spa_statistical_research_only",
            }
        )
    candidate_summary = pd.DataFrame(rows).loc[:, list(SPA_CANDIDATE_SUMMARY_COLUMNS)]
    candidate_summary = candidate_summary.sort_values(
        ["Observed Best Candidate", "Annualized Mean Excess Return"],
        ascending=[False, False],
        kind="stable",
    ).reset_index(drop=True)
    global_summary = pd.DataFrame(
        [
            {
                "Benchmark Column": benchmark_column,
                "Candidate Count": int(len(means)),
                "Observations": observations,
                "Bootstrap Iterations": int(bootstrap_iterations),
                "Block Size": int(block_size),
                "Random Seed": int(random_seed),
                "Best Run": best_run,
                "Best Annualized Mean Excess Return": observed_best * 252.0,
                "Best SPA T Statistic": observed_stat,
                "SPA Lower P Value": p_values["lower"],
                "SPA Consistent P Value": p_values["consistent"],
                "SPA Upper P Value": p_values["upper"],
                "SPA Alpha": float(alpha),
                "SPA Passed": passed,
                "SPA Recenter Bound": recenter_bound,
                "diagnostic_scope": DIAGNOSTIC_SCOPE,
            }
        ],
        columns=SPA_GLOBAL_SUMMARY_COLUMNS,
    )
    return {
        "spa_candidate_summary": candidate_summary,
        "spa_global_summary": global_summary,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a studentized SPA-style bootstrap diagnostic from Russell candidate daily return panels."
    )
    parser.add_argument("--daily-returns", required=True, help="Input concentration_variant_daily_returns.csv")
    parser.add_argument("--output-dir", required=True, help="Directory for SPA outputs")
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
    result = build_spa_diagnostics(
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
    candidate_path = output_dir / "spa_candidate_summary.csv"
    global_path = output_dir / "spa_global_summary.csv"
    result["spa_candidate_summary"].to_csv(candidate_path, index=False)
    result["spa_global_summary"].to_csv(global_path, index=False)
    print(result["spa_candidate_summary"].head(max(int(args.print_top), 0)).to_string(index=False))
    print(result["spa_global_summary"].to_string(index=False))
    print(f"wrote SPA candidate summary -> {candidate_path}")
    print(f"wrote SPA global summary -> {global_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
