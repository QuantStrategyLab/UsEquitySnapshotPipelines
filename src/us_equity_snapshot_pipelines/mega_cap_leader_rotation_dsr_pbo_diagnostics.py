"""DSR/PBO-style diagnostics for frozen Russell candidate return panels.

This module is intentionally dependency-free and research-only. It implements
small, transparent approximations that are useful for monthly promotion review:

- a Deflated-Sharpe-style probability using the Bailey/Lopez de Prado PSR/DSR
  normal approximation with skew/kurtosis adjustment;
- a CSCV/PBO-style split diagnostic that chooses the best in-sample candidate
  and measures whether it lands in the bottom half out of sample.

It is not a replacement for a validated econometrics package and must not be
used as an automatic live gate by itself.
"""

from __future__ import annotations

import argparse
import math
from itertools import combinations
from pathlib import Path
from statistics import NormalDist
from typing import Iterable

import numpy as np
import pandas as pd

from .mega_cap_leader_rotation_reality_check import DEFAULT_ALPHA, DEFAULT_BENCHMARK_COLUMN, _prepare_excess_returns
from .mega_cap_leader_rotation_stress_readiness import parse_csv_strings
from .russell_1000_multi_factor_defensive_snapshot import read_table

DEFAULT_CSCV_GROUPS = 8
DEFAULT_TRADING_DAYS = 252.0
DIAGNOSTIC_SCOPE = "deflated_sharpe_and_cscv_pbo_style_not_live_gate"
DSR_PBO_CANDIDATE_COLUMNS = (
    "Run",
    "Benchmark Column",
    "Observations",
    "Mean Daily Excess Return",
    "Annualized Mean Excess Return",
    "Daily Excess Volatility",
    "Daily Sharpe",
    "Annualized Sharpe",
    "Skew",
    "Kurtosis",
    "Effective Trials",
    "Expected Max Null Daily Sharpe",
    "Probabilistic Sharpe Ratio",
    "Deflated Sharpe Probability",
    "Deflated Sharpe Alpha",
    "Deflated Sharpe Passed",
    "CSCV Train Winner Count",
    "CSCV Train Winner Rate",
    "CSCV Selected OOS Loss Rate",
    "CSCV Median Selected Test Rank Percentile",
    "Observed Best Candidate",
    "diagnostic_scope",
    "recommended_action",
)
DSR_PBO_SPLIT_COLUMNS = (
    "Benchmark Column",
    "Split Index",
    "CSCV Groups",
    "Train Groups",
    "Test Groups",
    "Train Winner Run",
    "Train Winner Daily Sharpe",
    "Test Winner Run",
    "Train Winner Test Daily Sharpe",
    "Train Winner Test Rank",
    "Train Winner Test Rank Percentile",
    "Train Winner Test Logit Rank",
    "OOS Loss",
)
DSR_PBO_GLOBAL_COLUMNS = (
    "Benchmark Column",
    "Candidate Count",
    "Observations",
    "Effective Trials",
    "CSCV Groups",
    "CSCV Split Count",
    "Best Run",
    "Best Annualized Mean Excess Return",
    "Best Annualized Sharpe",
    "Best Deflated Sharpe Probability",
    "Best Deflated Sharpe Passed",
    "CSCV PBO Loss Rate",
    "CSCV Median Test Rank Percentile",
    "Deflated Sharpe Alpha",
    "diagnostic_scope",
    "recommended_action",
)


def _positive_int(value: int, *, name: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{name} must be positive")
    return parsed


def _daily_sharpe(values: pd.Series) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if len(clean) < 2:
        return float("nan")
    std = float(clean.std(ddof=1))
    if not std or math.isnan(std):
        return float("nan")
    return float(clean.mean()) / std


def _expected_max_null_daily_sharpe(effective_trials: float, *, sharpe_std: float) -> float:
    trials = max(float(effective_trials), 1.0)
    if trials <= 1.0 or sharpe_std <= 0.0 or math.isnan(sharpe_std):
        return 0.0
    normal = NormalDist()
    euler_gamma = 0.5772156649015329
    p_one = min(max(1.0 - 1.0 / trials, 1e-12), 1.0 - 1e-12)
    p_two = min(max(1.0 - 1.0 / (trials * math.e), 1e-12), 1.0 - 1e-12)
    return float(sharpe_std) * ((1.0 - euler_gamma) * normal.inv_cdf(p_one) + euler_gamma * normal.inv_cdf(p_two))


def _sharpe_probability(*, daily_sharpe: float, threshold: float, observations: int, skew: float, kurtosis: float) -> float:
    if observations < 2 or math.isnan(daily_sharpe) or math.isnan(threshold):
        return float("nan")
    safe_skew = 0.0 if math.isnan(skew) else float(skew)
    safe_kurtosis = 3.0 if math.isnan(kurtosis) else max(float(kurtosis), 1.0)
    denominator = 1.0 - safe_skew * daily_sharpe + ((safe_kurtosis - 1.0) / 4.0) * daily_sharpe**2
    denominator = math.sqrt(max(denominator, 1e-12))
    z_score = (daily_sharpe - float(threshold)) * math.sqrt(float(observations) - 1.0) / denominator
    return float(NormalDist().cdf(z_score))


def _candidate_statistics(
    excess: pd.DataFrame,
    *,
    benchmark_column: str,
    effective_trials: float,
    alpha: float,
) -> pd.DataFrame:
    observations = int(len(excess))
    sharpe_std = 1.0 / math.sqrt(float(observations) - 1.0) if observations > 1 else float("nan")
    expected_max = _expected_max_null_daily_sharpe(effective_trials, sharpe_std=sharpe_std)
    rows: list[dict[str, object]] = []
    means = excess.mean(axis=0)
    best_run = str(means.idxmax())
    for run in excess.columns:
        values = pd.to_numeric(excess[run], errors="coerce").dropna()
        daily_sharpe = _daily_sharpe(values)
        skew = float(values.skew()) if len(values) >= 3 else float("nan")
        kurtosis = float(values.kurt()) + 3.0 if len(values) >= 4 else float("nan")
        psr = _sharpe_probability(
            daily_sharpe=daily_sharpe,
            threshold=0.0,
            observations=len(values),
            skew=skew,
            kurtosis=kurtosis,
        )
        dsr = _sharpe_probability(
            daily_sharpe=daily_sharpe,
            threshold=expected_max,
            observations=len(values),
            skew=skew,
            kurtosis=kurtosis,
        )
        rows.append(
            {
                "Run": str(run),
                "Benchmark Column": benchmark_column,
                "Observations": int(len(values)),
                "Mean Daily Excess Return": float(values.mean()),
                "Annualized Mean Excess Return": float(values.mean()) * DEFAULT_TRADING_DAYS,
                "Daily Excess Volatility": float(values.std(ddof=1)) if len(values) > 1 else float("nan"),
                "Daily Sharpe": daily_sharpe,
                "Annualized Sharpe": daily_sharpe * math.sqrt(DEFAULT_TRADING_DAYS) if not math.isnan(daily_sharpe) else float("nan"),
                "Skew": skew,
                "Kurtosis": kurtosis,
                "Effective Trials": float(effective_trials),
                "Expected Max Null Daily Sharpe": expected_max,
                "Probabilistic Sharpe Ratio": psr,
                "Deflated Sharpe Probability": dsr,
                "Deflated Sharpe Alpha": float(alpha),
                "Deflated Sharpe Passed": bool(dsr >= 1.0 - float(alpha)) if not math.isnan(dsr) else False,
                "Observed Best Candidate": str(run) == best_run,
                "diagnostic_scope": DIAGNOSTIC_SCOPE,
                "recommended_action": "pending_cscv_context",
            }
        )
    return pd.DataFrame(rows)


def _group_labels(observations: int, groups: int) -> np.ndarray:
    group_count = min(_positive_int(groups, name="cscv_groups"), int(observations))
    if group_count < 4:
        raise ValueError("CSCV/PBO-style diagnostic requires at least 4 groups")
    if group_count % 2:
        group_count -= 1
    labels = np.floor(np.arange(int(observations)) * group_count / int(observations)).astype(int)
    return np.minimum(labels, group_count - 1)


def _rank_percentile_from_rank(rank: float, candidate_count: int) -> float:
    if candidate_count <= 1:
        return 1.0
    return 1.0 - ((float(rank) - 1.0) / (float(candidate_count) - 1.0))


def _logit(value: float) -> float:
    clipped = min(max(float(value), 1e-6), 1.0 - 1e-6)
    return float(math.log(clipped / (1.0 - clipped)))


def _cscv_pbo_splits(excess: pd.DataFrame, *, benchmark_column: str, cscv_groups: int) -> pd.DataFrame:
    values = excess.reset_index(drop=True)
    labels = _group_labels(len(values), int(cscv_groups))
    groups = tuple(sorted(int(label) for label in np.unique(labels)))
    train_size = len(groups) // 2
    rows: list[dict[str, object]] = []
    for split_index, train_groups in enumerate(combinations(groups, train_size), start=1):
        train_set = set(train_groups)
        test_groups = tuple(group for group in groups if group not in train_set)
        train_mask = np.isin(labels, list(train_set))
        test_mask = ~train_mask
        train = values.loc[train_mask]
        test = values.loc[test_mask]
        train_scores = train.apply(_daily_sharpe, axis=0)
        test_scores = test.apply(_daily_sharpe, axis=0)
        if train_scores.dropna().empty or test_scores.dropna().empty:
            continue
        train_winner = str(train_scores.idxmax())
        test_winner = str(test_scores.idxmax())
        test_ranks = test_scores.rank(method="min", ascending=False, na_option="bottom")
        winner_rank = float(test_ranks.loc[train_winner])
        percentile = _rank_percentile_from_rank(winner_rank, len(test_scores))
        rows.append(
            {
                "Benchmark Column": benchmark_column,
                "Split Index": int(split_index),
                "CSCV Groups": int(len(groups)),
                "Train Groups": ",".join(str(group) for group in train_groups),
                "Test Groups": ",".join(str(group) for group in test_groups),
                "Train Winner Run": train_winner,
                "Train Winner Daily Sharpe": float(train_scores.loc[train_winner]),
                "Test Winner Run": test_winner,
                "Train Winner Test Daily Sharpe": float(test_scores.loc[train_winner]),
                "Train Winner Test Rank": winner_rank,
                "Train Winner Test Rank Percentile": percentile,
                "Train Winner Test Logit Rank": _logit(percentile),
                "OOS Loss": bool(percentile < 0.5),
            }
        )
    return pd.DataFrame(rows, columns=DSR_PBO_SPLIT_COLUMNS)


def _attach_cscv_context(candidates: pd.DataFrame, splits: pd.DataFrame) -> pd.DataFrame:
    frame = candidates.copy()
    if splits.empty:
        frame["CSCV Train Winner Count"] = 0
        frame["CSCV Train Winner Rate"] = 0.0
        frame["CSCV Selected OOS Loss Rate"] = np.nan
        frame["CSCV Median Selected Test Rank Percentile"] = np.nan
    else:
        grouped = splits.groupby("Train Winner Run", dropna=False)
        counts = grouped.size()
        loss = grouped["OOS Loss"].mean()
        percentile = grouped["Train Winner Test Rank Percentile"].median()
        split_count = float(len(splits))
        frame["CSCV Train Winner Count"] = frame["Run"].map(counts).fillna(0).astype(int)
        frame["CSCV Train Winner Rate"] = frame["CSCV Train Winner Count"] / split_count
        frame["CSCV Selected OOS Loss Rate"] = frame["Run"].map(loss)
        frame["CSCV Median Selected Test Rank Percentile"] = frame["Run"].map(percentile)
    frame["recommended_action"] = np.where(
        frame["Observed Best Candidate"] & frame["Deflated Sharpe Passed"],
        "dsr_pbo_statistical_review_candidate",
        "dsr_pbo_research_only",
    )
    return frame.loc[:, list(DSR_PBO_CANDIDATE_COLUMNS)]


def build_dsr_pbo_diagnostics(
    daily_returns: pd.DataFrame,
    *,
    benchmark_column: str = DEFAULT_BENCHMARK_COLUMN,
    candidate_runs: Iterable[str] | None = None,
    cscv_groups: int = DEFAULT_CSCV_GROUPS,
    effective_trials: float | None = None,
    alpha: float = DEFAULT_ALPHA,
) -> dict[str, pd.DataFrame]:
    excess = _prepare_excess_returns(daily_returns, benchmark_column=benchmark_column, candidate_runs=candidate_runs)
    if excess.shape[1] < 2:
        raise ValueError("DSR/PBO-style diagnostic requires at least two candidate runs")
    trials = float(effective_trials) if effective_trials is not None else float(excess.shape[1])
    trials = max(trials, 1.0)
    candidates = _candidate_statistics(
        excess,
        benchmark_column=benchmark_column,
        effective_trials=trials,
        alpha=float(alpha),
    )
    splits = _cscv_pbo_splits(excess, benchmark_column=benchmark_column, cscv_groups=int(cscv_groups))
    candidate_summary = _attach_cscv_context(candidates, splits).sort_values(
        ["Observed Best Candidate", "Annualized Mean Excess Return"],
        ascending=[False, False],
        kind="stable",
    ).reset_index(drop=True)
    best = candidate_summary.iloc[0]
    pbo_loss_rate = float(splits["OOS Loss"].mean()) if not splits.empty else float("nan")
    median_percentile = float(splits["Train Winner Test Rank Percentile"].median()) if not splits.empty else float("nan")
    recommended_action = (
        "dsr_pbo_review_supports_frozen_best"
        if bool(best["Deflated Sharpe Passed"]) and (math.isnan(pbo_loss_rate) or pbo_loss_rate < 0.50)
        else "dsr_pbo_needs_human_review"
    )
    global_summary = pd.DataFrame(
        [
            {
                "Benchmark Column": benchmark_column,
                "Candidate Count": int(excess.shape[1]),
                "Observations": int(len(excess)),
                "Effective Trials": trials,
                "CSCV Groups": int(splits["CSCV Groups"].iloc[0]) if not splits.empty else int(cscv_groups),
                "CSCV Split Count": int(len(splits)),
                "Best Run": str(best["Run"]),
                "Best Annualized Mean Excess Return": float(best["Annualized Mean Excess Return"]),
                "Best Annualized Sharpe": float(best["Annualized Sharpe"]),
                "Best Deflated Sharpe Probability": float(best["Deflated Sharpe Probability"]),
                "Best Deflated Sharpe Passed": bool(best["Deflated Sharpe Passed"]),
                "CSCV PBO Loss Rate": pbo_loss_rate,
                "CSCV Median Test Rank Percentile": median_percentile,
                "Deflated Sharpe Alpha": float(alpha),
                "diagnostic_scope": DIAGNOSTIC_SCOPE,
                "recommended_action": recommended_action,
            }
        ],
        columns=DSR_PBO_GLOBAL_COLUMNS,
    )
    return {
        "dsr_pbo_candidate_summary": candidate_summary,
        "dsr_pbo_cscv_splits": splits,
        "dsr_pbo_global_summary": global_summary,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run DSR/PBO-style diagnostics from Russell candidate daily return panels."
    )
    parser.add_argument("--daily-returns", required=True, help="Input concentration_variant_daily_returns.csv")
    parser.add_argument("--output-dir", required=True, help="Directory for DSR/PBO-style outputs")
    parser.add_argument("--benchmark-column", default=DEFAULT_BENCHMARK_COLUMN)
    parser.add_argument(
        "--candidate-runs",
        default="",
        help="Optional comma-separated candidate run filter. Empty means all runs in the daily-return panel.",
    )
    parser.add_argument("--cscv-groups", type=int, default=DEFAULT_CSCV_GROUPS)
    parser.add_argument("--effective-trials", type=float, default=None)
    parser.add_argument("--alpha", type=float, default=DEFAULT_ALPHA)
    parser.add_argument("--print-top", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    candidates = parse_csv_strings(args.candidate_runs, default=())
    result = build_dsr_pbo_diagnostics(
        read_table(args.daily_returns),
        benchmark_column=args.benchmark_column,
        candidate_runs=candidates,
        cscv_groups=int(args.cscv_groups),
        effective_trials=args.effective_trials,
        alpha=float(args.alpha),
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    candidate_path = output_dir / "dsr_pbo_candidate_summary.csv"
    splits_path = output_dir / "dsr_pbo_cscv_splits.csv"
    global_path = output_dir / "dsr_pbo_global_summary.csv"
    result["dsr_pbo_candidate_summary"].to_csv(candidate_path, index=False)
    result["dsr_pbo_cscv_splits"].to_csv(splits_path, index=False)
    result["dsr_pbo_global_summary"].to_csv(global_path, index=False)
    print(result["dsr_pbo_candidate_summary"].head(max(int(args.print_top), 0)).to_string(index=False))
    print(result["dsr_pbo_global_summary"].to_string(index=False))
    print(f"wrote DSR/PBO candidate summary -> {candidate_path}")
    print(f"wrote DSR/PBO CSCV splits -> {splits_path}")
    print(f"wrote DSR/PBO global summary -> {global_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
