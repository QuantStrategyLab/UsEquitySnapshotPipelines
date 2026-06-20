from __future__ import annotations

import argparse
from math import ceil
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from .mega_cap_leader_rotation_stress_readiness import parse_csv_strings
from .russell_1000_multi_factor_defensive_snapshot import read_table

DEFAULT_BOOTSTRAP_ITERATIONS = 1000
DEFAULT_BLOCK_SIZE = 21
DEFAULT_RANDOM_SEED = 42
DEFAULT_ALPHA = 0.10
MCS_STYLE_SCOPE = "mcs_style_pairwise_return_confidence_set_not_live_gate"
PAIRWISE_COLUMNS = (
    "Best Run",
    "Compared Run",
    "Observations",
    "Mean Daily Advantage Best vs Compared",
    "Annualized Advantage Best vs Compared",
    "Paired Bootstrap P Value",
    "Alpha",
    "Compared Excluded From MCS Style Set",
    "diagnostic_scope",
)
SUMMARY_COLUMNS = (
    "Run",
    "Observations",
    "Mean Daily Return",
    "Annualized Mean Return",
    "Daily Return Volatility",
    "Return T Statistic",
    "Mean Return Rank",
    "Observed Best Candidate",
    "In MCS Style Confidence Set",
    "Dominated By Best Candidate",
    "Pairwise P Value vs Best",
    "Annualized Gap vs Best",
    "Alpha",
    "diagnostic_scope",
    "recommended_action",
)
GLOBAL_COLUMNS = (
    "Candidate Count",
    "Observations",
    "Bootstrap Iterations",
    "Block Size",
    "Random Seed",
    "Alpha",
    "Best Run",
    "MCS Style Confidence Set Size",
    "MCS Style Confidence Set",
    "Excluded Candidate Count",
    "diagnostic_scope",
)


def _prepare_return_panel(daily_returns: pd.DataFrame, *, candidate_runs: Iterable[str] | None) -> pd.DataFrame:
    frame = pd.DataFrame(daily_returns).copy()
    required = {"Date", "Run", "Strategy Return"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"daily_returns must include columns: {', '.join(missing)}")
    frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce").dt.tz_localize(None)
    frame["Strategy Return"] = pd.to_numeric(frame["Strategy Return"], errors="coerce")
    frame = frame.dropna(subset=["Date", "Run", "Strategy Return"])
    runs = tuple(str(run) for run in candidate_runs or ())
    if runs:
        frame = frame.loc[frame["Run"].astype(str).isin(runs)].copy()
    if frame.empty:
        raise ValueError("daily_returns has no rows after filtering")
    panel = frame.pivot_table(index="Date", columns="Run", values="Strategy Return", aggfunc="first").sort_index()
    panel = panel.dropna(axis=0, how="any")
    if panel.empty:
        raise ValueError("daily_returns has no complete candidate rows")
    if panel.shape[1] < 2:
        raise ValueError("MCS-style diagnostic requires at least two candidate runs")
    return panel


def _circular_block_indices(*, observations: int, block_size: int, rng: np.random.Generator) -> np.ndarray:
    n = int(observations)
    block = max(int(block_size), 1)
    starts = rng.integers(0, n, size=int(ceil(n / block)))
    offsets = np.arange(block)
    return np.concatenate([(start + offsets) % n for start in starts])[:n]


def _one_sided_advantage_p_value(
    advantage: np.ndarray,
    *,
    bootstrap_iterations: int,
    block_size: int,
    random_seed: int,
) -> float:
    values = np.asarray(advantage, dtype=float)
    values = values[np.isfinite(values)]
    if values.size == 0:
        return float("nan")
    observed = float(values.mean())
    centered = values - observed
    rng = np.random.default_rng(int(random_seed))
    exceedances = 0
    for _ in range(int(bootstrap_iterations)):
        idx = _circular_block_indices(observations=len(centered), block_size=int(block_size), rng=rng)
        bootstrap_mean = float(centered[idx].mean())
        if bootstrap_mean >= observed:
            exceedances += 1
    return (1.0 + float(exceedances)) / (1.0 + float(bootstrap_iterations))


def _return_t_stat(values: pd.Series) -> float:
    returns = pd.to_numeric(values, errors="coerce").dropna()
    if len(returns) < 2:
        return float("nan")
    std = returns.std(ddof=1)
    if not std or pd.isna(std):
        return float("nan")
    return float(returns.mean() / (std / np.sqrt(len(returns))))


def build_mcs_style_diagnostics(
    daily_returns: pd.DataFrame,
    *,
    candidate_runs: Iterable[str] | None = None,
    bootstrap_iterations: int = DEFAULT_BOOTSTRAP_ITERATIONS,
    block_size: int = DEFAULT_BLOCK_SIZE,
    random_seed: int = DEFAULT_RANDOM_SEED,
    alpha: float = DEFAULT_ALPHA,
) -> dict[str, pd.DataFrame]:
    iterations = int(bootstrap_iterations)
    if iterations <= 0:
        raise ValueError("bootstrap_iterations must be positive")
    if int(block_size) <= 0:
        raise ValueError("block_size must be positive")
    panel = _prepare_return_panel(daily_returns, candidate_runs=candidate_runs)
    observations = int(len(panel))
    means = panel.mean(axis=0)
    best_run = str(means.idxmax())
    best_values = panel[best_run].to_numpy(dtype=float)

    pairwise_rows: list[dict[str, object]] = []
    p_values_by_run: dict[str, float] = {best_run: float("nan")}
    gap_by_run: dict[str, float] = {best_run: 0.0}
    excluded_by_run: dict[str, bool] = {best_run: False}
    for run in panel.columns:
        if str(run) == best_run:
            continue
        compared_values = panel[run].to_numpy(dtype=float)
        advantage = best_values - compared_values
        mean_advantage = float(np.nanmean(advantage))
        p_value = _one_sided_advantage_p_value(
            advantage,
            bootstrap_iterations=iterations,
            block_size=int(block_size),
            random_seed=int(random_seed),
        )
        excluded = bool(mean_advantage > 0.0 and p_value <= float(alpha))
        p_values_by_run[str(run)] = p_value
        gap_by_run[str(run)] = -mean_advantage * 252.0
        excluded_by_run[str(run)] = excluded
        pairwise_rows.append(
            {
                "Best Run": best_run,
                "Compared Run": str(run),
                "Observations": observations,
                "Mean Daily Advantage Best vs Compared": mean_advantage,
                "Annualized Advantage Best vs Compared": mean_advantage * 252.0,
                "Paired Bootstrap P Value": p_value,
                "Alpha": float(alpha),
                "Compared Excluded From MCS Style Set": excluded,
                "diagnostic_scope": MCS_STYLE_SCOPE,
            }
        )

    ranks = means.rank(method="min", ascending=False)
    summary_rows: list[dict[str, object]] = []
    for run in panel.columns:
        run_text = str(run)
        values = pd.to_numeric(panel[run], errors="coerce").dropna()
        excluded = bool(excluded_by_run.get(run_text, False))
        in_set = not excluded
        is_best = run_text == best_run
        if is_best:
            action = "mcs_style_best_candidate"
        elif in_set:
            action = "mcs_style_statistically_indistinguishable_from_best"
        else:
            action = "mcs_style_excluded_by_best"
        summary_rows.append(
            {
                "Run": run_text,
                "Observations": int(len(values)),
                "Mean Daily Return": float(values.mean()),
                "Annualized Mean Return": float(values.mean()) * 252.0,
                "Daily Return Volatility": float(values.std(ddof=1)) if len(values) > 1 else float("nan"),
                "Return T Statistic": _return_t_stat(values),
                "Mean Return Rank": float(ranks.loc[run]),
                "Observed Best Candidate": bool(is_best),
                "In MCS Style Confidence Set": bool(in_set),
                "Dominated By Best Candidate": bool(excluded),
                "Pairwise P Value vs Best": p_values_by_run.get(run_text, float("nan")),
                "Annualized Gap vs Best": gap_by_run.get(run_text, float("nan")),
                "Alpha": float(alpha),
                "diagnostic_scope": MCS_STYLE_SCOPE,
                "recommended_action": action,
            }
        )
    summary = pd.DataFrame(summary_rows).loc[:, list(SUMMARY_COLUMNS)]
    summary = summary.sort_values(
        ["In MCS Style Confidence Set", "Mean Return Rank"], ascending=[False, True], kind="stable"
    ).reset_index(drop=True)
    pairwise = pd.DataFrame(pairwise_rows, columns=PAIRWISE_COLUMNS).sort_values(
        "Annualized Advantage Best vs Compared", ascending=False, kind="stable"
    ).reset_index(drop=True)
    confidence_set = summary.loc[summary["In MCS Style Confidence Set"].astype(bool), "Run"].astype(str).tolist()
    global_summary = pd.DataFrame(
        [
            {
                "Candidate Count": int(panel.shape[1]),
                "Observations": observations,
                "Bootstrap Iterations": iterations,
                "Block Size": int(block_size),
                "Random Seed": int(random_seed),
                "Alpha": float(alpha),
                "Best Run": best_run,
                "MCS Style Confidence Set Size": int(len(confidence_set)),
                "MCS Style Confidence Set": ",".join(confidence_set),
                "Excluded Candidate Count": int(panel.shape[1] - len(confidence_set)),
                "diagnostic_scope": MCS_STYLE_SCOPE,
            }
        ],
        columns=GLOBAL_COLUMNS,
    )
    return {
        "mcs_style_candidate_summary": summary,
        "mcs_style_pairwise_summary": pairwise,
        "mcs_style_global_summary": global_summary,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run an MCS-style pairwise confidence-set diagnostic for Russell candidate daily returns."
    )
    parser.add_argument("--daily-returns", required=True, help="Input concentration_variant_daily_returns.csv")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--candidate-runs", default="")
    parser.add_argument("--bootstrap-iterations", type=int, default=DEFAULT_BOOTSTRAP_ITERATIONS)
    parser.add_argument("--block-size", type=int, default=DEFAULT_BLOCK_SIZE)
    parser.add_argument("--random-seed", type=int, default=DEFAULT_RANDOM_SEED)
    parser.add_argument("--alpha", type=float, default=DEFAULT_ALPHA)
    parser.add_argument("--print-top", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_mcs_style_diagnostics(
        read_table(args.daily_returns),
        candidate_runs=parse_csv_strings(args.candidate_runs, default=()),
        bootstrap_iterations=int(args.bootstrap_iterations),
        block_size=int(args.block_size),
        random_seed=int(args.random_seed),
        alpha=float(args.alpha),
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    candidate_path = output_dir / "mcs_style_candidate_summary.csv"
    pairwise_path = output_dir / "mcs_style_pairwise_summary.csv"
    global_path = output_dir / "mcs_style_global_summary.csv"
    result["mcs_style_candidate_summary"].to_csv(candidate_path, index=False)
    result["mcs_style_pairwise_summary"].to_csv(pairwise_path, index=False)
    result["mcs_style_global_summary"].to_csv(global_path, index=False)
    print(result["mcs_style_candidate_summary"].head(max(int(args.print_top), 0)).to_string(index=False))
    print(result["mcs_style_global_summary"].to_string(index=False))
    print(f"wrote MCS-style candidate summary -> {candidate_path}")
    print(f"wrote MCS-style pairwise summary -> {pairwise_path}")
    print(f"wrote MCS-style global summary -> {global_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
