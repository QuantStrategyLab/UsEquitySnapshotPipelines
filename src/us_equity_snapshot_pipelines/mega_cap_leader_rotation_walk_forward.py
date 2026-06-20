from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from .mega_cap_leader_rotation_backtest import (
    BENCHMARK_SYMBOL,
    BROAD_BENCHMARK_SYMBOL,
    SAFE_HAVEN,
    _build_close_and_returns,
    _normalize_price_history,
    run_backtest,
)
from .mega_cap_leader_rotation_concentration_variants import (
    DEFAULT_BLEND_TOP2_WEIGHTS,
    _align_weights,
    _apply_panic_rebound_guard,
    _panic_guard_variant_name,
    _returns_from_weights,
    _variant_name_for_blend,
)
from .mega_cap_leader_rotation_dynamic_validation import (
    _complete_calendar_years,
    _period_cagr,
    _period_max_drawdown,
    _period_return,
    lag_universe_history,
)
from .mega_cap_leader_rotation_stress_readiness import (
    DEFAULT_PANIC_GUARD_DRAWDOWN_THRESHOLD,
    DEFAULT_PANIC_GUARD_REBOUND_THRESHOLD,
    DEFAULT_PANIC_GUARD_STOCK_EXPOSURE,
    DEFAULT_PANIC_GUARD_VOL_THRESHOLD,
    parse_csv_floats_no_percent,
)
from .russell_1000_multi_factor_defensive_snapshot import read_table

DEFAULT_WALK_FORWARD_TRAIN_YEARS = 3
DEFAULT_WALK_FORWARD_MIN_TRAIN_EXCESS_CAGR = 0.0
DEFAULT_WALK_FORWARD_MAX_TRAIN_DRAWDOWN_DEGRADATION = 0.03
DEFAULT_WALK_FORWARD_MIN_OOS_WINDOWS = 3
DEFAULT_WALK_FORWARD_MIN_OOS_WIN_RATE = 0.50
DEFAULT_WALK_FORWARD_MIN_MEDIAN_OOS_EXCESS_CAGR = 0.0
DEFAULT_WALK_FORWARD_MIN_WORST_OOS_EXCESS_CAGR = -0.03
DEFAULT_WALK_FORWARD_MAX_OOS_DRAWDOWN_DEGRADATION = 0.03
DEFAULT_UNIVERSE_LAG_DAYS = 21
DEFAULT_TURNOVER_COST_BPS = 5.0
DEFAULT_MIN_ADV20_USD = 20_000_000.0

WALK_FORWARD_WINDOW_COLUMNS = (
    "Pair",
    "Baseline Run",
    "Candidate Run",
    "Train Window",
    "Test Year",
    "Train Baseline CAGR",
    "Train Candidate CAGR",
    "Train Excess CAGR vs Baseline",
    "Train Drawdown Delta vs Baseline",
    "train_gate_passed",
    "train_gate_reason",
    "Test Baseline CAGR",
    "Test Candidate CAGR",
    "Test Excess CAGR vs Baseline",
    "Test Baseline Max Drawdown",
    "Test Candidate Max Drawdown",
    "Test Drawdown Delta vs Baseline",
    "Test Baseline Sharpe",
    "Test Candidate Sharpe",
    "Test Sharpe Delta vs Baseline",
    "Test Baseline Turnover/Year",
    "Test Candidate Turnover/Year",
    "Test Turnover Delta vs Baseline",
    "oos_win_vs_baseline",
)
WALK_FORWARD_SUMMARY_COLUMNS = (
    "Pair",
    "Baseline Run",
    "Candidate Run",
    "Train Years",
    "Total Windows",
    "Promotion OOS Windows",
    "OOS Baseline CAGR Win Rate",
    "Median OOS Excess CAGR vs Baseline",
    "Worst OOS Excess CAGR vs Baseline",
    "Worst OOS Drawdown Delta vs Baseline",
    "Median OOS Sharpe Delta vs Baseline",
    "Median OOS Turnover Delta vs Baseline",
    "walk_forward_gate_passed",
    "walk_forward_gate_reason",
    "recommended_action",
)


def _ordered_columns(frame: pd.DataFrame, columns: tuple[str, ...]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=columns)
    known = [column for column in columns if column in frame.columns]
    extra = [column for column in frame.columns if column not in known]
    return frame.loc[:, known + extra]


def _sharpe(returns: pd.Series) -> float:
    values = pd.to_numeric(returns, errors="coerce").dropna()
    if values.empty:
        return float("nan")
    std = float(values.std(ddof=0))
    return float(values.mean() / std * np.sqrt(252)) if std else float("nan")


def _turnover_per_year(weights: pd.DataFrame, *, start: pd.Timestamp, end: pd.Timestamp) -> float:
    window = weights.loc[(weights.index >= start) & (weights.index <= end)].fillna(0.0)
    if window.empty:
        return float("nan")
    changes = window.diff().fillna(0.0)
    if not changes.empty:
        changes.iloc[0] = 0.0
    turnover = 0.5 * changes.abs().sum(axis=1)
    years = max((window.index[-1] - window.index[0]).days / 365.25, 1 / 365.25)
    return float(turnover.sum() / years)


def _period_metrics(
    returns: pd.Series,
    weights: pd.DataFrame,
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> dict[str, float]:
    window_returns = returns.loc[(returns.index >= start) & (returns.index <= end)]
    return {
        "return": _period_return(window_returns),
        "cagr": _period_cagr(window_returns),
        "max_drawdown": _period_max_drawdown(window_returns),
        "sharpe": _sharpe(window_returns),
        "turnover_per_year": _turnover_per_year(weights, start=start, end=end),
    }


def _train_gate_reason(
    *,
    train_excess_cagr: float,
    train_drawdown_delta: float,
    min_train_excess_cagr: float,
    max_train_drawdown_degradation: float,
) -> str:
    reasons: list[str] = []
    if pd.isna(train_excess_cagr) or train_excess_cagr <= float(min_train_excess_cagr):
        reasons.append("train_excess_cagr_not_positive")
    if pd.isna(train_drawdown_delta) or train_drawdown_delta < -float(max_train_drawdown_degradation):
        reasons.append("train_drawdown_delta_too_negative")
    return ";".join(reasons) if reasons else "pass"


def _walk_forward_gate_reason(
    *,
    promotion_windows: int,
    min_oos_windows: int,
    win_rate: float,
    median_excess: float,
    worst_excess: float,
    worst_drawdown_delta: float,
    min_win_rate: float,
    min_median_excess: float,
    min_worst_excess: float,
    max_drawdown_degradation: float,
) -> str:
    reasons: list[str] = []
    if int(promotion_windows) < int(min_oos_windows):
        reasons.append(f"promotion_windows_below_{int(min_oos_windows)}")
    if pd.isna(win_rate) or win_rate < float(min_win_rate):
        reasons.append("oos_win_rate_below_threshold")
    if pd.isna(median_excess) or median_excess <= float(min_median_excess):
        reasons.append("median_oos_excess_not_positive")
    if pd.isna(worst_excess) or worst_excess < float(min_worst_excess):
        reasons.append("worst_oos_excess_too_negative")
    if pd.isna(worst_drawdown_delta) or worst_drawdown_delta < -float(max_drawdown_degradation):
        reasons.append("worst_oos_drawdown_delta_too_negative")
    return ";".join(reasons) if reasons else "pass"


def _build_pair_walk_forward_windows(
    *,
    pair_name: str,
    baseline_run: str,
    candidate_run: str,
    baseline_returns: pd.Series,
    candidate_returns: pd.Series,
    baseline_weights: pd.DataFrame,
    candidate_weights: pd.DataFrame,
    train_years: int,
    min_train_excess_cagr: float,
    max_train_drawdown_degradation: float,
) -> pd.DataFrame:
    years = _complete_calendar_years(pd.DatetimeIndex(baseline_returns.index))
    rows: list[dict[str, object]] = []
    for test_year in years:
        train_start_year = int(test_year) - int(train_years)
        train_year_values = list(range(train_start_year, int(test_year)))
        if any(year not in years for year in train_year_values):
            continue
        train_start = pd.Timestamp(year=train_start_year, month=1, day=1)
        train_end = pd.Timestamp(year=int(test_year) - 1, month=12, day=31)
        test_start = pd.Timestamp(year=int(test_year), month=1, day=1)
        test_end = pd.Timestamp(year=int(test_year), month=12, day=31)

        train_baseline = _period_metrics(baseline_returns, baseline_weights, start=train_start, end=train_end)
        train_candidate = _period_metrics(candidate_returns, candidate_weights, start=train_start, end=train_end)
        test_baseline = _period_metrics(baseline_returns, baseline_weights, start=test_start, end=test_end)
        test_candidate = _period_metrics(candidate_returns, candidate_weights, start=test_start, end=test_end)

        train_excess = train_candidate["cagr"] - train_baseline["cagr"]
        train_drawdown_delta = train_candidate["max_drawdown"] - train_baseline["max_drawdown"]
        train_reason = _train_gate_reason(
            train_excess_cagr=train_excess,
            train_drawdown_delta=train_drawdown_delta,
            min_train_excess_cagr=min_train_excess_cagr,
            max_train_drawdown_degradation=max_train_drawdown_degradation,
        )
        test_excess = test_candidate["cagr"] - test_baseline["cagr"]
        test_drawdown_delta = test_candidate["max_drawdown"] - test_baseline["max_drawdown"]
        rows.append(
            {
                "Pair": pair_name,
                "Baseline Run": baseline_run,
                "Candidate Run": candidate_run,
                "Train Window": f"{train_start.date().isoformat()}_{train_end.date().isoformat()}",
                "Test Year": int(test_year),
                "Train Baseline CAGR": train_baseline["cagr"],
                "Train Candidate CAGR": train_candidate["cagr"],
                "Train Excess CAGR vs Baseline": train_excess,
                "Train Drawdown Delta vs Baseline": train_drawdown_delta,
                "train_gate_passed": train_reason == "pass",
                "train_gate_reason": train_reason,
                "Test Baseline CAGR": test_baseline["cagr"],
                "Test Candidate CAGR": test_candidate["cagr"],
                "Test Excess CAGR vs Baseline": test_excess,
                "Test Baseline Max Drawdown": test_baseline["max_drawdown"],
                "Test Candidate Max Drawdown": test_candidate["max_drawdown"],
                "Test Drawdown Delta vs Baseline": test_drawdown_delta,
                "Test Baseline Sharpe": test_baseline["sharpe"],
                "Test Candidate Sharpe": test_candidate["sharpe"],
                "Test Sharpe Delta vs Baseline": test_candidate["sharpe"] - test_baseline["sharpe"],
                "Test Baseline Turnover/Year": test_baseline["turnover_per_year"],
                "Test Candidate Turnover/Year": test_candidate["turnover_per_year"],
                "Test Turnover Delta vs Baseline": (
                    test_candidate["turnover_per_year"] - test_baseline["turnover_per_year"]
                ),
                "oos_win_vs_baseline": bool(test_excess > 0.0),
            }
        )
    return _ordered_columns(pd.DataFrame(rows), WALK_FORWARD_WINDOW_COLUMNS)


def summarize_walk_forward_oos(
    windows: pd.DataFrame,
    *,
    train_years: int = DEFAULT_WALK_FORWARD_TRAIN_YEARS,
    min_oos_windows: int = DEFAULT_WALK_FORWARD_MIN_OOS_WINDOWS,
    min_oos_win_rate: float = DEFAULT_WALK_FORWARD_MIN_OOS_WIN_RATE,
    min_median_oos_excess_cagr: float = DEFAULT_WALK_FORWARD_MIN_MEDIAN_OOS_EXCESS_CAGR,
    min_worst_oos_excess_cagr: float = DEFAULT_WALK_FORWARD_MIN_WORST_OOS_EXCESS_CAGR,
    max_oos_drawdown_degradation: float = DEFAULT_WALK_FORWARD_MAX_OOS_DRAWDOWN_DEGRADATION,
) -> pd.DataFrame:
    frame = pd.DataFrame(windows).copy()
    if frame.empty:
        return pd.DataFrame(columns=WALK_FORWARD_SUMMARY_COLUMNS)
    rows: list[dict[str, object]] = []
    for pair, group in frame.groupby("Pair", sort=False):
        promoted = group.loc[group["train_gate_passed"].astype(bool)].copy()
        win_rate = float(promoted["oos_win_vs_baseline"].astype(bool).mean()) if not promoted.empty else float("nan")
        median_excess = float(promoted["Test Excess CAGR vs Baseline"].median()) if not promoted.empty else float("nan")
        worst_excess = float(promoted["Test Excess CAGR vs Baseline"].min()) if not promoted.empty else float("nan")
        worst_drawdown_delta = (
            float(promoted["Test Drawdown Delta vs Baseline"].min()) if not promoted.empty else float("nan")
        )
        median_sharpe_delta = (
            float(promoted["Test Sharpe Delta vs Baseline"].median()) if not promoted.empty else float("nan")
        )
        median_turnover_delta = (
            float(promoted["Test Turnover Delta vs Baseline"].median()) if not promoted.empty else float("nan")
        )
        reason = _walk_forward_gate_reason(
            promotion_windows=len(promoted),
            min_oos_windows=min_oos_windows,
            win_rate=win_rate,
            median_excess=median_excess,
            worst_excess=worst_excess,
            worst_drawdown_delta=worst_drawdown_delta,
            min_win_rate=min_oos_win_rate,
            min_median_excess=min_median_oos_excess_cagr,
            min_worst_excess=min_worst_oos_excess_cagr,
            max_drawdown_degradation=max_oos_drawdown_degradation,
        )
        first = group.iloc[0]
        rows.append(
            {
                "Pair": pair,
                "Baseline Run": first["Baseline Run"],
                "Candidate Run": first["Candidate Run"],
                "Train Years": int(train_years),
                "Total Windows": int(len(group)),
                "Promotion OOS Windows": int(len(promoted)),
                "OOS Baseline CAGR Win Rate": win_rate,
                "Median OOS Excess CAGR vs Baseline": median_excess,
                "Worst OOS Excess CAGR vs Baseline": worst_excess,
                "Worst OOS Drawdown Delta vs Baseline": worst_drawdown_delta,
                "Median OOS Sharpe Delta vs Baseline": median_sharpe_delta,
                "Median OOS Turnover Delta vs Baseline": median_turnover_delta,
                "walk_forward_gate_passed": reason == "pass",
                "walk_forward_gate_reason": reason,
                "recommended_action": "walk_forward_live_design_review" if reason == "pass" else "research_only",
            }
        )
    return _ordered_columns(pd.DataFrame(rows), WALK_FORWARD_SUMMARY_COLUMNS)


def build_panic_guard_walk_forward_oos(
    price_history,
    universe_history,
    *,
    start_date: str | None = "2017-10-02",
    end_date: str | None = None,
    universe_lag_trading_days: int = DEFAULT_UNIVERSE_LAG_DAYS,
    turnover_cost_bps: float = DEFAULT_TURNOVER_COST_BPS,
    min_adv20_usd: float = DEFAULT_MIN_ADV20_USD,
    min_price_usd: float = 10.0,
    min_history_days: int = 273,
    blend_top2_weights: Iterable[float] = (0.25, 0.50),
    benchmark_symbol: str = BENCHMARK_SYMBOL,
    broad_benchmark_symbol: str = BROAD_BENCHMARK_SYMBOL,
    safe_haven: str = SAFE_HAVEN,
    train_years: int = DEFAULT_WALK_FORWARD_TRAIN_YEARS,
    min_train_excess_cagr: float = DEFAULT_WALK_FORWARD_MIN_TRAIN_EXCESS_CAGR,
    max_train_drawdown_degradation: float = DEFAULT_WALK_FORWARD_MAX_TRAIN_DRAWDOWN_DEGRADATION,
    min_oos_windows: int = DEFAULT_WALK_FORWARD_MIN_OOS_WINDOWS,
    min_oos_win_rate: float = DEFAULT_WALK_FORWARD_MIN_OOS_WIN_RATE,
    min_median_oos_excess_cagr: float = DEFAULT_WALK_FORWARD_MIN_MEDIAN_OOS_EXCESS_CAGR,
    min_worst_oos_excess_cagr: float = DEFAULT_WALK_FORWARD_MIN_WORST_OOS_EXCESS_CAGR,
    max_oos_drawdown_degradation: float = DEFAULT_WALK_FORWARD_MAX_OOS_DRAWDOWN_DEGRADATION,
    panic_guard_drawdown_threshold: float = DEFAULT_PANIC_GUARD_DRAWDOWN_THRESHOLD,
    panic_guard_rebound_threshold: float = DEFAULT_PANIC_GUARD_REBOUND_THRESHOLD,
    panic_guard_vol_threshold: float = DEFAULT_PANIC_GUARD_VOL_THRESHOLD,
    panic_guard_stock_exposure: float = DEFAULT_PANIC_GUARD_STOCK_EXPOSURE,
) -> dict[str, pd.DataFrame]:
    prices = _normalize_price_history(price_history)
    if end_date is not None:
        prices = prices.loc[prices["as_of"] <= pd.Timestamp(end_date).normalize()].copy()
    trading_index = pd.DatetimeIndex(sorted(prices["as_of"].dropna().unique()))
    lagged_universe = lag_universe_history(
        universe_history,
        lag_trading_days=int(universe_lag_trading_days),
        trading_index=trading_index,
    )
    base_kwargs = {
        "start_date": start_date,
        "end_date": end_date,
        "pool_name": f"dynamic_top50_lag{int(universe_lag_trading_days)}",
        "benchmark_symbol": benchmark_symbol,
        "broad_benchmark_symbol": broad_benchmark_symbol,
        "safe_haven": safe_haven,
        "risk_on_exposure": 1.0,
        "soft_defense_exposure": 1.0,
        "hard_defense_exposure": 1.0,
        "turnover_cost_bps": float(turnover_cost_bps),
        "min_price_usd": float(min_price_usd),
        "min_adv20_usd": float(min_adv20_usd),
        "min_history_days": int(min_history_days),
    }
    top2 = run_backtest(prices, lagged_universe, top_n=2, single_name_cap=0.50, max_names_per_sector=0, **base_kwargs)
    top4 = run_backtest(prices, lagged_universe, top_n=4, single_name_cap=0.25, max_names_per_sector=0, **base_kwargs)

    _close_matrix, returns_matrix = _build_close_and_returns(prices)
    index = pd.DatetimeIndex(top2["portfolio_returns"].index)
    reference_returns = top2["reference_returns"].reindex(index)
    weight_columns = sorted(
        set(top2["weights_history"].columns) | set(top4["weights_history"].columns) | set(returns_matrix.columns)
    )
    returns_matrix = returns_matrix.reindex(index).reindex(columns=weight_columns, fill_value=0.0).fillna(0.0)
    top2_weights = _align_weights(top2["weights_history"], index=index, columns=weight_columns)
    top4_weights = _align_weights(top4["weights_history"], index=index, columns=weight_columns)
    benchmark_returns = (
        reference_returns[benchmark_symbol] if benchmark_symbol in reference_returns.columns else pd.Series(index=index)
    )

    window_frames: list[pd.DataFrame] = []
    for top2_weight in blend_top2_weights:
        top2_weight = float(top2_weight)
        if not 0.0 < top2_weight < 1.0:
            continue
        baseline_run = _variant_name_for_blend(top2_weight)
        baseline_weights = top2_weight * top2_weights + (1.0 - top2_weight) * top4_weights
        candidate_run = _panic_guard_variant_name(
            baseline_run,
            drawdown_threshold=float(panic_guard_drawdown_threshold),
            rebound_threshold=float(panic_guard_rebound_threshold),
            vol_threshold=float(panic_guard_vol_threshold),
            stock_exposure=float(panic_guard_stock_exposure),
        )
        candidate_weights = _apply_panic_rebound_guard(
            baseline_weights,
            benchmark_returns,
            safe_haven=safe_haven,
            drawdown_threshold=float(panic_guard_drawdown_threshold),
            rebound_threshold=float(panic_guard_rebound_threshold),
            vol_threshold=float(panic_guard_vol_threshold),
            stock_exposure=float(panic_guard_stock_exposure),
        )
        baseline_returns = _returns_from_weights(
            baseline_weights,
            returns_matrix,
            turnover_cost_bps=float(turnover_cost_bps),
        )
        candidate_returns = _returns_from_weights(
            candidate_weights,
            returns_matrix,
            turnover_cost_bps=float(turnover_cost_bps),
        )
        window_frames.append(
            _build_pair_walk_forward_windows(
                pair_name=f"{candidate_run}_vs_{baseline_run}",
                baseline_run=baseline_run,
                candidate_run=candidate_run,
                baseline_returns=baseline_returns,
                candidate_returns=candidate_returns,
                baseline_weights=baseline_weights,
                candidate_weights=candidate_weights,
                train_years=int(train_years),
                min_train_excess_cagr=float(min_train_excess_cagr),
                max_train_drawdown_degradation=float(max_train_drawdown_degradation),
            )
        )

    windows = pd.concat(window_frames, ignore_index=True) if window_frames else pd.DataFrame(columns=WALK_FORWARD_WINDOW_COLUMNS)
    summary = summarize_walk_forward_oos(
        windows,
        train_years=int(train_years),
        min_oos_windows=int(min_oos_windows),
        min_oos_win_rate=float(min_oos_win_rate),
        min_median_oos_excess_cagr=float(min_median_oos_excess_cagr),
        min_worst_oos_excess_cagr=float(min_worst_oos_excess_cagr),
        max_oos_drawdown_degradation=float(max_oos_drawdown_degradation),
    )
    return {
        "walk_forward_oos_windows": _ordered_columns(windows, WALK_FORWARD_WINDOW_COLUMNS),
        "walk_forward_oos_summary": _ordered_columns(summary, WALK_FORWARD_SUMMARY_COLUMNS),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Walk-forward/OOS diagnostics for Russell panic-rebound guard variants.")
    parser.add_argument("--prices", required=True, help="Input price history file")
    parser.add_argument("--universe", required=True, help="Input dynamic universe history file")
    parser.add_argument("--output-dir", required=True, help="Directory for walk-forward outputs")
    parser.add_argument("--start", dest="start_date", default="2017-10-02")
    parser.add_argument("--end", dest="end_date")
    parser.add_argument("--universe-lag-days", type=int, default=DEFAULT_UNIVERSE_LAG_DAYS)
    parser.add_argument("--turnover-cost-bps", type=float, default=DEFAULT_TURNOVER_COST_BPS)
    parser.add_argument("--min-adv20-usd", type=float, default=DEFAULT_MIN_ADV20_USD)
    parser.add_argument("--min-price-usd", type=float, default=10.0)
    parser.add_argument("--min-history-days", type=int, default=273)
    parser.add_argument(
        "--blend-top2-weights",
        default="0.25,0.5",
        help="Comma-separated fixed Top2 sleeve weights to compare against panic-guard variants.",
    )
    parser.add_argument("--benchmark-symbol", default=BENCHMARK_SYMBOL)
    parser.add_argument("--broad-benchmark-symbol", default=BROAD_BENCHMARK_SYMBOL)
    parser.add_argument("--safe-haven", default=SAFE_HAVEN)
    parser.add_argument("--train-years", type=int, default=DEFAULT_WALK_FORWARD_TRAIN_YEARS)
    parser.add_argument("--min-train-excess-cagr", type=float, default=DEFAULT_WALK_FORWARD_MIN_TRAIN_EXCESS_CAGR)
    parser.add_argument(
        "--max-train-drawdown-degradation",
        type=float,
        default=DEFAULT_WALK_FORWARD_MAX_TRAIN_DRAWDOWN_DEGRADATION,
    )
    parser.add_argument("--min-oos-windows", type=int, default=DEFAULT_WALK_FORWARD_MIN_OOS_WINDOWS)
    parser.add_argument("--min-oos-win-rate", type=float, default=DEFAULT_WALK_FORWARD_MIN_OOS_WIN_RATE)
    parser.add_argument(
        "--min-median-oos-excess-cagr",
        type=float,
        default=DEFAULT_WALK_FORWARD_MIN_MEDIAN_OOS_EXCESS_CAGR,
    )
    parser.add_argument(
        "--min-worst-oos-excess-cagr",
        type=float,
        default=DEFAULT_WALK_FORWARD_MIN_WORST_OOS_EXCESS_CAGR,
    )
    parser.add_argument(
        "--max-oos-drawdown-degradation",
        type=float,
        default=DEFAULT_WALK_FORWARD_MAX_OOS_DRAWDOWN_DEGRADATION,
    )
    parser.add_argument("--panic-guard-drawdown-threshold", type=float, default=DEFAULT_PANIC_GUARD_DRAWDOWN_THRESHOLD)
    parser.add_argument("--panic-guard-rebound-threshold", type=float, default=DEFAULT_PANIC_GUARD_REBOUND_THRESHOLD)
    parser.add_argument("--panic-guard-vol-threshold", type=float, default=DEFAULT_PANIC_GUARD_VOL_THRESHOLD)
    parser.add_argument("--panic-guard-stock-exposure", type=float, default=DEFAULT_PANIC_GUARD_STOCK_EXPOSURE)
    parser.add_argument("--print-top", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    result = build_panic_guard_walk_forward_oos(
        read_table(args.prices),
        read_table(args.universe),
        start_date=args.start_date,
        end_date=args.end_date,
        universe_lag_trading_days=int(args.universe_lag_days),
        turnover_cost_bps=float(args.turnover_cost_bps),
        min_adv20_usd=float(args.min_adv20_usd),
        min_price_usd=float(args.min_price_usd),
        min_history_days=int(args.min_history_days),
        blend_top2_weights=parse_csv_floats_no_percent(
            args.blend_top2_weights,
            default=DEFAULT_BLEND_TOP2_WEIGHTS,
        ),
        benchmark_symbol=args.benchmark_symbol,
        broad_benchmark_symbol=args.broad_benchmark_symbol,
        safe_haven=args.safe_haven,
        train_years=int(args.train_years),
        min_train_excess_cagr=float(args.min_train_excess_cagr),
        max_train_drawdown_degradation=float(args.max_train_drawdown_degradation),
        min_oos_windows=int(args.min_oos_windows),
        min_oos_win_rate=float(args.min_oos_win_rate),
        min_median_oos_excess_cagr=float(args.min_median_oos_excess_cagr),
        min_worst_oos_excess_cagr=float(args.min_worst_oos_excess_cagr),
        max_oos_drawdown_degradation=float(args.max_oos_drawdown_degradation),
        panic_guard_drawdown_threshold=float(args.panic_guard_drawdown_threshold),
        panic_guard_rebound_threshold=float(args.panic_guard_rebound_threshold),
        panic_guard_vol_threshold=float(args.panic_guard_vol_threshold),
        panic_guard_stock_exposure=float(args.panic_guard_stock_exposure),
    )
    windows_path = output_dir / "walk_forward_oos_windows.csv"
    summary_path = output_dir / "walk_forward_oos_summary.csv"
    result["walk_forward_oos_windows"].to_csv(windows_path, index=False)
    result["walk_forward_oos_summary"].to_csv(summary_path, index=False)
    print(result["walk_forward_oos_summary"].head(max(int(args.print_top), 0)).to_string(index=False))
    print(f"wrote walk-forward/OOS windows -> {windows_path}")
    print(f"wrote walk-forward/OOS summary -> {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
