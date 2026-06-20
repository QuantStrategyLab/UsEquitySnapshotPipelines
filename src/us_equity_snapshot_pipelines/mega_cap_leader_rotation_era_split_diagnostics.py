from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from .mega_cap_leader_rotation_stress_readiness import parse_csv_strings
from .russell_1000_multi_factor_defensive_snapshot import read_table

DEFAULT_ERA_SPECS = (
    "2017_2019_early_live_window:2017-10-02:2019-12-31,"
    "2020_2021_covid_liquidity:2020-01-01:2021-12-31,"
    "2022_bear_rate_shock:2022-01-01:2022-12-31,"
    "2023_2026_ai_recovery:2023-01-01:2026-12-31"
)
DEFAULT_MIN_OBSERVATIONS = 60
DEFAULT_MIN_BEST_ERA_COUNT = 2
DEFAULT_MIN_POSITIVE_SPY_EXCESS_RATE = 0.75
DEFAULT_MIN_POSITIVE_QQQ_EXCESS_RATE = 0.75
DEFAULT_MIN_WORST_SPY_EXCESS_CAGR = -0.03
DEFAULT_MIN_WORST_QQQ_EXCESS_CAGR = -0.10
DEFAULT_MIN_WORST_MAX_DRAWDOWN = -0.35
ERA_DETAIL_COLUMNS = (
    "Run",
    "Variant Type",
    "Era",
    "Era Start",
    "Era End",
    "Observations",
    "CAGR",
    "Total Return",
    "Max Drawdown",
    "Volatility",
    "Sharpe",
    "QQQ CAGR",
    "SPY CAGR",
    "QQQ Total Return",
    "SPY Total Return",
    "QQQ Excess CAGR",
    "SPY Excess CAGR",
    "Best CAGR Candidate In Era",
    "CAGR Rank In Era",
)
ERA_PROMOTION_COLUMNS = (
    "Run",
    "Variant Type",
    "Era Count",
    "Best CAGR Era Count",
    "Positive QQQ Excess Era Count",
    "Positive QQQ Excess Era Rate",
    "Positive SPY Excess Era Count",
    "Positive SPY Excess Era Rate",
    "Worst QQQ Excess CAGR",
    "Worst QQQ Excess Era",
    "Worst SPY Excess CAGR",
    "Worst SPY Excess Era",
    "Worst Max Drawdown",
    "Worst Max Drawdown Era",
    "Median Era CAGR",
    "Median Era Sharpe",
    "era_robustness_passed",
    "era_robustness_reason",
    "diagnostic_scope",
    "recommended_action",
)
DIAGNOSTIC_SCOPE = "pre_registered_era_split_not_live_gate"


@dataclass(frozen=True)
class EraSpec:
    name: str
    start: pd.Timestamp
    end: pd.Timestamp


def parse_era_specs(raw_value: str | Iterable[str] | None) -> tuple[EraSpec, ...]:
    values = DEFAULT_ERA_SPECS.split(",") if raw_value is None else (
        raw_value.split(",") if isinstance(raw_value, str) else list(raw_value)
    )
    eras: list[EraSpec] = []
    seen: set[str] = set()
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        parts = text.split(":")
        if len(parts) != 3:
            raise ValueError("era specs must use name:start_date:end_date entries")
        name = parts[0].strip()
        if not name:
            raise ValueError("era name must not be empty")
        if name in seen:
            continue
        start = pd.Timestamp(parts[1]).tz_localize(None).normalize()
        end = pd.Timestamp(parts[2]).tz_localize(None).normalize()
        if end < start:
            raise ValueError(f"era {name} has end before start")
        seen.add(name)
        eras.append(EraSpec(name=name, start=start, end=end))
    if not eras:
        raise ValueError("at least one era spec is required")
    return tuple(eras)


def _period_return(values: pd.Series) -> float:
    returns = pd.to_numeric(values, errors="coerce").dropna()
    if returns.empty:
        return float("nan")
    return float((1.0 + returns).prod() - 1.0)


def _period_cagr(values: pd.Series, dates: pd.Series) -> float:
    returns = pd.to_numeric(values, errors="coerce").dropna()
    if returns.empty:
        return float("nan")
    clean_dates = pd.to_datetime(dates.loc[returns.index], errors="coerce").dropna()
    if clean_dates.empty:
        return float("nan")
    total = float((1.0 + returns).prod())
    years = max((clean_dates.max() - clean_dates.min()).days / 365.25, 1.0 / 365.25)
    return float(total ** (1.0 / years) - 1.0)


def _period_max_drawdown(values: pd.Series) -> float:
    returns = pd.to_numeric(values, errors="coerce").dropna()
    if returns.empty:
        return float("nan")
    equity = (1.0 + returns).cumprod()
    return float((equity / equity.cummax() - 1.0).min())


def _annualized_vol(values: pd.Series) -> float:
    returns = pd.to_numeric(values, errors="coerce").dropna()
    if len(returns) < 2:
        return float("nan")
    return float(returns.std(ddof=1) * np.sqrt(252.0))


def _annualized_sharpe(values: pd.Series) -> float:
    returns = pd.to_numeric(values, errors="coerce").dropna()
    if len(returns) < 2:
        return float("nan")
    std = returns.std(ddof=1)
    if not std or pd.isna(std):
        return float("nan")
    return float(returns.mean() / std * np.sqrt(252.0))


def _prepare_daily_returns(daily_returns: pd.DataFrame, *, candidate_runs: Iterable[str] | None) -> pd.DataFrame:
    frame = pd.DataFrame(daily_returns).copy()
    required = {"Date", "Run", "Strategy Return", "QQQ Return", "SPY Return"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"daily_returns must include columns: {', '.join(missing)}")
    frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce").dt.tz_localize(None)
    for column in ("Strategy Return", "QQQ Return", "SPY Return"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["Date", "Run", "Strategy Return", "QQQ Return", "SPY Return"])
    runs = tuple(str(run) for run in candidate_runs or ())
    if runs:
        frame = frame.loc[frame["Run"].astype(str).isin(runs)].copy()
    if frame.empty:
        raise ValueError("daily_returns has no rows after filtering")
    if "Variant Type" not in frame.columns:
        frame["Variant Type"] = ""
    return frame.sort_values(["Date", "Run"]).reset_index(drop=True)


def _build_era_detail(
    daily_returns: pd.DataFrame,
    *,
    eras: Iterable[EraSpec],
    min_observations: int,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for era in eras:
        era_frame = daily_returns.loc[(daily_returns["Date"] >= era.start) & (daily_returns["Date"] <= era.end)].copy()
        if era_frame.empty:
            continue
        for run, group in era_frame.groupby("Run", sort=True):
            group = group.sort_values("Date").reset_index(drop=True)
            if len(group) < int(min_observations):
                continue
            strategy = group["Strategy Return"]
            qqq = group["QQQ Return"]
            spy = group["SPY Return"]
            cagr = _period_cagr(strategy, group["Date"])
            qqq_cagr = _period_cagr(qqq, group["Date"])
            spy_cagr = _period_cagr(spy, group["Date"])
            rows.append(
                {
                    "Run": str(run),
                    "Variant Type": str(group.get("Variant Type", pd.Series([""])).iloc[0]),
                    "Era": era.name,
                    "Era Start": group["Date"].min().date().isoformat(),
                    "Era End": group["Date"].max().date().isoformat(),
                    "Observations": int(len(group)),
                    "CAGR": cagr,
                    "Total Return": _period_return(strategy),
                    "Max Drawdown": _period_max_drawdown(strategy),
                    "Volatility": _annualized_vol(strategy),
                    "Sharpe": _annualized_sharpe(strategy),
                    "QQQ CAGR": qqq_cagr,
                    "SPY CAGR": spy_cagr,
                    "QQQ Total Return": _period_return(qqq),
                    "SPY Total Return": _period_return(spy),
                    "QQQ Excess CAGR": cagr - qqq_cagr,
                    "SPY Excess CAGR": cagr - spy_cagr,
                }
            )
    detail = pd.DataFrame(rows)
    if detail.empty:
        return pd.DataFrame(columns=ERA_DETAIL_COLUMNS)
    detail["CAGR Rank In Era"] = detail.groupby("Era", sort=False)["CAGR"].rank(method="min", ascending=False)
    best = detail.loc[detail["CAGR Rank In Era"].eq(1.0), ["Era", "Run"]].rename(
        columns={"Run": "Best CAGR Candidate In Era"}
    )
    detail = detail.merge(best, on="Era", how="left")
    return detail.loc[:, list(ERA_DETAIL_COLUMNS)].sort_values(["Era", "CAGR Rank In Era", "Run"]).reset_index(drop=True)


def _idxmin_label(frame: pd.DataFrame, *, value_column: str, label_column: str) -> str:
    values = pd.to_numeric(frame[value_column], errors="coerce")
    if values.dropna().empty:
        return ""
    return str(frame.loc[values.idxmin(), label_column])


def _recommended_action(
    run: str,
    passed: bool,
    best_era_count: int,
    max_best_era_count: int,
    *,
    min_best_era_count: int,
) -> str:
    if not passed:
        return "era_split_caveat_review"
    if (
        run == "blend_top2_50_top4_50"
        and best_era_count == max_best_era_count
        and best_era_count >= int(min_best_era_count)
    ):
        return "era_supported_preferred_offensive_review"
    if run == "blend_top2_25_top4_75":
        return "era_supported_conservative_review"
    if run == "base_top4_cap25":
        return "era_supported_fallback_review"
    return "era_supported_research_review"


def _promotion_summary(
    detail: pd.DataFrame,
    *,
    min_best_era_count: int,
    min_positive_qqq_excess_rate: float,
    min_positive_spy_excess_rate: float,
    min_worst_qqq_excess_cagr: float,
    min_worst_spy_excess_cagr: float,
    min_worst_max_drawdown: float,
) -> pd.DataFrame:
    if detail.empty:
        return pd.DataFrame(columns=ERA_PROMOTION_COLUMNS)
    max_best_era_count = int(detail.groupby("Run")["CAGR Rank In Era"].apply(lambda s: int((s == 1.0).sum())).max())
    rows: list[dict[str, object]] = []
    for run, group in detail.groupby("Run", sort=False):
        era_count = int(len(group))
        best_era_count = int((pd.to_numeric(group["CAGR Rank In Era"], errors="coerce") == 1.0).sum())
        positive_qqq = int((pd.to_numeric(group["QQQ Excess CAGR"], errors="coerce") > 0.0).sum())
        positive_spy = int((pd.to_numeric(group["SPY Excess CAGR"], errors="coerce") > 0.0).sum())
        qqq_rate = positive_qqq / era_count if era_count else float("nan")
        spy_rate = positive_spy / era_count if era_count else float("nan")
        worst_qqq = float(pd.to_numeric(group["QQQ Excess CAGR"], errors="coerce").min())
        worst_spy = float(pd.to_numeric(group["SPY Excess CAGR"], errors="coerce").min())
        worst_dd = float(pd.to_numeric(group["Max Drawdown"], errors="coerce").min())
        reasons = []
        if qqq_rate < float(min_positive_qqq_excess_rate):
            reasons.append("positive_qqq_excess_rate_below_min")
        if spy_rate < float(min_positive_spy_excess_rate):
            reasons.append("positive_spy_excess_rate_below_min")
        if worst_qqq < float(min_worst_qqq_excess_cagr):
            reasons.append("worst_qqq_excess_below_min")
        if worst_spy < float(min_worst_spy_excess_cagr):
            reasons.append("worst_spy_excess_below_min")
        if worst_dd < float(min_worst_max_drawdown):
            reasons.append("worst_drawdown_below_min")
        passed = not reasons
        rows.append(
            {
                "Run": str(run),
                "Variant Type": str(group["Variant Type"].iloc[0]),
                "Era Count": era_count,
                "Best CAGR Era Count": best_era_count,
                "Positive QQQ Excess Era Count": positive_qqq,
                "Positive QQQ Excess Era Rate": qqq_rate,
                "Positive SPY Excess Era Count": positive_spy,
                "Positive SPY Excess Era Rate": spy_rate,
                "Worst QQQ Excess CAGR": worst_qqq,
                "Worst QQQ Excess Era": _idxmin_label(group, value_column="QQQ Excess CAGR", label_column="Era"),
                "Worst SPY Excess CAGR": worst_spy,
                "Worst SPY Excess Era": _idxmin_label(group, value_column="SPY Excess CAGR", label_column="Era"),
                "Worst Max Drawdown": worst_dd,
                "Worst Max Drawdown Era": _idxmin_label(group, value_column="Max Drawdown", label_column="Era"),
                "Median Era CAGR": float(pd.to_numeric(group["CAGR"], errors="coerce").median()),
                "Median Era Sharpe": float(pd.to_numeric(group["Sharpe"], errors="coerce").median()),
                "era_robustness_passed": bool(passed),
                "era_robustness_reason": "pass" if passed else ";".join(reasons),
                "diagnostic_scope": DIAGNOSTIC_SCOPE,
                "recommended_action": _recommended_action(
                    str(run),
                    passed,
                    best_era_count,
                    max_best_era_count,
                    min_best_era_count=int(min_best_era_count),
                ),
            }
        )
    summary = pd.DataFrame(rows).loc[:, list(ERA_PROMOTION_COLUMNS)]
    return summary.sort_values(
        ["era_robustness_passed", "Best CAGR Era Count", "Median Era CAGR"],
        ascending=[False, False, False],
        kind="stable",
    ).reset_index(drop=True)


def build_era_split_diagnostics(
    daily_returns: pd.DataFrame,
    *,
    eras: Iterable[EraSpec] | None = None,
    candidate_runs: Iterable[str] | None = None,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    min_best_era_count: int = DEFAULT_MIN_BEST_ERA_COUNT,
    min_positive_qqq_excess_rate: float = DEFAULT_MIN_POSITIVE_QQQ_EXCESS_RATE,
    min_positive_spy_excess_rate: float = DEFAULT_MIN_POSITIVE_SPY_EXCESS_RATE,
    min_worst_qqq_excess_cagr: float = DEFAULT_MIN_WORST_QQQ_EXCESS_CAGR,
    min_worst_spy_excess_cagr: float = DEFAULT_MIN_WORST_SPY_EXCESS_CAGR,
    min_worst_max_drawdown: float = DEFAULT_MIN_WORST_MAX_DRAWDOWN,
) -> dict[str, pd.DataFrame]:
    prepared = _prepare_daily_returns(daily_returns, candidate_runs=candidate_runs)
    era_specs = tuple(eras or parse_era_specs(None))
    detail = _build_era_detail(prepared, eras=era_specs, min_observations=int(min_observations))
    summary = _promotion_summary(
        detail,
        min_best_era_count=int(min_best_era_count),
        min_positive_qqq_excess_rate=float(min_positive_qqq_excess_rate),
        min_positive_spy_excess_rate=float(min_positive_spy_excess_rate),
        min_worst_qqq_excess_cagr=float(min_worst_qqq_excess_cagr),
        min_worst_spy_excess_cagr=float(min_worst_spy_excess_cagr),
        min_worst_max_drawdown=float(min_worst_max_drawdown),
    )
    return {
        "era_split_candidate_summary": detail,
        "era_split_promotion_summary": summary,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build pre-registered era-split diagnostics for Russell candidates.")
    parser.add_argument("--daily-returns", required=True, help="Input concentration_variant_daily_returns.csv")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--eras", default=DEFAULT_ERA_SPECS, help="Comma-separated name:start:end era specs")
    parser.add_argument("--candidate-runs", default="")
    parser.add_argument("--min-observations", type=int, default=DEFAULT_MIN_OBSERVATIONS)
    parser.add_argument("--min-best-era-count", type=int, default=DEFAULT_MIN_BEST_ERA_COUNT)
    parser.add_argument("--min-positive-qqq-excess-rate", type=float, default=DEFAULT_MIN_POSITIVE_QQQ_EXCESS_RATE)
    parser.add_argument("--min-positive-spy-excess-rate", type=float, default=DEFAULT_MIN_POSITIVE_SPY_EXCESS_RATE)
    parser.add_argument("--min-worst-qqq-excess-cagr", type=float, default=DEFAULT_MIN_WORST_QQQ_EXCESS_CAGR)
    parser.add_argument("--min-worst-spy-excess-cagr", type=float, default=DEFAULT_MIN_WORST_SPY_EXCESS_CAGR)
    parser.add_argument("--min-worst-max-drawdown", type=float, default=DEFAULT_MIN_WORST_MAX_DRAWDOWN)
    parser.add_argument("--print-top", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_era_split_diagnostics(
        read_table(args.daily_returns),
        eras=parse_era_specs(args.eras),
        candidate_runs=parse_csv_strings(args.candidate_runs, default=()),
        min_observations=int(args.min_observations),
        min_best_era_count=int(args.min_best_era_count),
        min_positive_qqq_excess_rate=float(args.min_positive_qqq_excess_rate),
        min_positive_spy_excess_rate=float(args.min_positive_spy_excess_rate),
        min_worst_qqq_excess_cagr=float(args.min_worst_qqq_excess_cagr),
        min_worst_spy_excess_cagr=float(args.min_worst_spy_excess_cagr),
        min_worst_max_drawdown=float(args.min_worst_max_drawdown),
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    detail_path = output_dir / "era_split_candidate_summary.csv"
    summary_path = output_dir / "era_split_promotion_summary.csv"
    result["era_split_candidate_summary"].to_csv(detail_path, index=False)
    result["era_split_promotion_summary"].to_csv(summary_path, index=False)
    print(result["era_split_promotion_summary"].head(max(int(args.print_top), 0)).to_string(index=False))
    print(f"wrote era split candidate summary -> {detail_path}")
    print(f"wrote era split promotion summary -> {summary_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
