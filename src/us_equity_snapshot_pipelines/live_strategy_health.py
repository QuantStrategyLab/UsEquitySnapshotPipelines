from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Sequence

import numpy as np
import pandas as pd

KEEP = "keep"
WATCH = "watch"
REVIEW_FOR_RETIREMENT = "review_for_retirement"
INSUFFICIENT_DATA = "insufficient_data"

DEFAULT_PRIMARY_BENCHMARK = "buy_hold_SPY"
DEFAULT_MIN_OBSERVATIONS = 60
DEFAULT_MIN_EXCESS_CAGR = 0.0
DEFAULT_MIN_DRAWDOWN_ADVANTAGE = 0.03
DEFAULT_MAX_DRAWDOWN_LAG = 0.05


@dataclass(frozen=True)
class HealthPolicy:
    min_observations: int = DEFAULT_MIN_OBSERVATIONS
    min_excess_cagr: float = DEFAULT_MIN_EXCESS_CAGR
    min_drawdown_advantage: float = DEFAULT_MIN_DRAWDOWN_ADVANTAGE
    max_drawdown_lag: float = DEFAULT_MAX_DRAWDOWN_LAG

    def to_dict(self) -> dict[str, float | int]:
        return asdict(self)


@dataclass(frozen=True)
class HealthWindow:
    name: str
    start: pd.Timestamp | None
    end: pd.Timestamp | None
    description: str = ""


def _normalize_columns(columns: Sequence[str] | str | None) -> tuple[str, ...]:
    if columns is None:
        return ()
    raw = columns.split(",") if isinstance(columns, str) else list(columns)
    return tuple(dict.fromkeys(str(column or "").strip() for column in raw if str(column or "").strip()))


def _normalize_return_matrix(return_matrix: pd.DataFrame, *, date_column: str = "as_of") -> pd.DataFrame:
    frame = pd.DataFrame(return_matrix).copy()
    if date_column in frame.columns:
        frame[date_column] = pd.to_datetime(frame[date_column], errors="coerce").dt.tz_localize(None).dt.normalize()
        frame = frame.dropna(subset=[date_column]).set_index(date_column)
    else:
        frame.index = pd.to_datetime(frame.index, errors="coerce").tz_localize(None).normalize()
        frame = frame.loc[frame.index.notna()]
    for column in frame.columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.sort_index()


def _normalize_returns(returns: pd.Series) -> pd.Series:
    series = pd.Series(returns).copy()
    series.index = pd.to_datetime(series.index, errors="coerce").tz_localize(None).normalize()
    series = pd.to_numeric(series, errors="coerce")
    series = series.loc[series.index.notna()].dropna().sort_index()
    return series


def _trailing_start(index: pd.DatetimeIndex, observations: int) -> pd.Timestamp:
    observations = max(1, int(observations))
    return pd.Timestamp(index[max(0, len(index) - observations)]).normalize()


def build_health_windows(returns: pd.Series) -> tuple[HealthWindow, ...]:
    series = _normalize_returns(returns)
    if series.empty:
        return ()
    index = pd.DatetimeIndex(series.index)
    end = pd.Timestamp(index[-1]).normalize()
    year_start = pd.Timestamp(year=end.year, month=1, day=1)
    return (
        HealthWindow("full", None, None, "all overlapping observations"),
        HealthWindow("ytd", year_start, end, "calendar YTD"),
        HealthWindow("trailing_3m", _trailing_start(index, 63), end, "latest 63 observations"),
        HealthWindow("trailing_6m", _trailing_start(index, 126), end, "latest 126 observations"),
        HealthWindow("trailing_1y", _trailing_start(index, 252), end, "latest 252 observations"),
        HealthWindow("trailing_3y", _trailing_start(index, 756), end, "latest 756 observations"),
    )


def summarize_returns(returns: pd.Series) -> dict[str, float | int | str]:
    series = _normalize_returns(returns)
    if series.empty:
        return {
            "start": "",
            "end": "",
            "observations": 0,
            "total_return": float("nan"),
            "cagr": float("nan"),
            "max_drawdown": float("nan"),
            "volatility": float("nan"),
            "sharpe": float("nan"),
            "sortino": float("nan"),
            "calmar": float("nan"),
            "final_equity": float("nan"),
        }
    equity = (1.0 + series).cumprod()
    years = max((series.index[-1] - series.index[0]).days / 365.25, 1.0 / 365.25)
    total_return = float(equity.iloc[-1] - 1.0)
    cagr = float(equity.iloc[-1] ** (1.0 / years) - 1.0)
    drawdown = equity / equity.cummax() - 1.0
    max_drawdown = float(drawdown.min())
    volatility = float(series.std(ddof=0) * np.sqrt(252.0))
    std = float(series.std(ddof=0))
    sharpe = float(series.mean() / std * np.sqrt(252.0)) if std else float("nan")
    downside = series.loc[series < 0.0]
    downside_std = float(downside.std(ddof=0)) if not downside.empty else 0.0
    sortino = float(series.mean() / downside_std * np.sqrt(252.0)) if downside_std else float("nan")
    calmar = float(cagr / abs(max_drawdown)) if max_drawdown < 0.0 else float("nan")
    return {
        "start": str(series.index[0].date()),
        "end": str(series.index[-1].date()),
        "observations": int(len(series)),
        "total_return": total_return,
        "cagr": cagr,
        "max_drawdown": max_drawdown,
        "volatility": volatility,
        "sharpe": sharpe,
        "sortino": sortino,
        "calmar": calmar,
        "final_equity": float(equity.iloc[-1]),
    }


def _slice_window(series: pd.Series, window: HealthWindow) -> pd.Series:
    result = _normalize_returns(series)
    if window.start is not None:
        result = result.loc[result.index >= pd.Timestamp(window.start).normalize()]
    if window.end is not None:
        result = result.loc[result.index <= pd.Timestamp(window.end).normalize()]
    return result


def _align_strategy_and_benchmark(strategy_returns: pd.Series, benchmark_returns: pd.Series) -> pd.DataFrame:
    frame = pd.concat(
        [
            _normalize_returns(strategy_returns).rename("strategy_return"),
            _normalize_returns(benchmark_returns).rename("benchmark_return"),
        ],
        axis=1,
        join="inner",
    )
    return frame.dropna(subset=["strategy_return", "benchmark_return"])


def classify_window_health(
    *,
    observations: int,
    excess_cagr: float,
    drawdown_advantage: float,
    strategy_max_drawdown: float,
    benchmark_max_drawdown: float,
    policy: HealthPolicy = HealthPolicy(),
) -> tuple[str, str]:
    if observations < int(policy.min_observations):
        return INSUFFICIENT_DATA, "not enough overlapping observations"

    underperforms = bool(pd.notna(excess_cagr) and excess_cagr < float(policy.min_excess_cagr))
    lacks_drawdown_advantage = bool(
        pd.notna(drawdown_advantage) and drawdown_advantage < float(policy.min_drawdown_advantage)
    )
    materially_worse_drawdown = bool(
        pd.notna(strategy_max_drawdown)
        and pd.notna(benchmark_max_drawdown)
        and strategy_max_drawdown < benchmark_max_drawdown - float(policy.max_drawdown_lag)
    )

    if underperforms and lacks_drawdown_advantage:
        return (
            REVIEW_FOR_RETIREMENT,
            "underperforms primary benchmark without enough drawdown advantage",
        )
    if underperforms:
        return WATCH, "underperforms primary benchmark but has some drawdown advantage"
    if materially_worse_drawdown:
        return WATCH, "drawdown is materially worse than primary benchmark"
    return KEEP, "meets benchmark return/drawdown health gate"


def build_strategy_window_health(
    return_matrix: pd.DataFrame,
    *,
    strategies: Sequence[str],
    primary_benchmark: str = DEFAULT_PRIMARY_BENCHMARK,
    windows: Sequence[HealthWindow] | None = None,
    policy: HealthPolicy = HealthPolicy(),
    date_column: str = "as_of",
) -> pd.DataFrame:
    frame = _normalize_return_matrix(return_matrix, date_column=date_column)
    strategy_columns = _normalize_columns(strategies)
    if not strategy_columns:
        raise ValueError("at least one strategy column is required")
    benchmark_column = str(primary_benchmark or "").strip()
    if not benchmark_column:
        raise ValueError("primary_benchmark is required")

    missing = [column for column in (*strategy_columns, benchmark_column) if column not in frame.columns]
    if missing:
        raise ValueError(f"return matrix missing columns: {missing}")

    resolved_windows = tuple(windows) if windows is not None else build_health_windows(frame[benchmark_column])
    rows: list[dict[str, object]] = []
    for strategy in strategy_columns:
        for window in resolved_windows:
            aligned = _align_strategy_and_benchmark(
                _slice_window(frame[strategy], window),
                _slice_window(frame[benchmark_column], window),
            )
            strategy_summary = summarize_returns(aligned["strategy_return"])
            benchmark_summary = summarize_returns(aligned["benchmark_return"])
            observations = int(strategy_summary["observations"])
            excess_cagr = float(strategy_summary["cagr"]) - float(benchmark_summary["cagr"])
            drawdown_advantage = float(strategy_summary["max_drawdown"]) - float(benchmark_summary["max_drawdown"])
            health_state, reason = classify_window_health(
                observations=observations,
                excess_cagr=excess_cagr,
                drawdown_advantage=drawdown_advantage,
                strategy_max_drawdown=float(strategy_summary["max_drawdown"]),
                benchmark_max_drawdown=float(benchmark_summary["max_drawdown"]),
                policy=policy,
            )
            rows.append(
                {
                    "strategy": strategy,
                    "window": window.name,
                    "description": window.description,
                    "start": strategy_summary["start"],
                    "end": strategy_summary["end"],
                    "observations": observations,
                    "strategy_total_return": strategy_summary["total_return"],
                    "strategy_cagr": strategy_summary["cagr"],
                    "strategy_max_drawdown": strategy_summary["max_drawdown"],
                    "strategy_volatility": strategy_summary["volatility"],
                    "strategy_sharpe": strategy_summary["sharpe"],
                    "strategy_sortino": strategy_summary["sortino"],
                    "strategy_calmar": strategy_summary["calmar"],
                    "benchmark": benchmark_column,
                    "benchmark_total_return": benchmark_summary["total_return"],
                    "benchmark_cagr": benchmark_summary["cagr"],
                    "benchmark_max_drawdown": benchmark_summary["max_drawdown"],
                    "excess_cagr": excess_cagr,
                    "drawdown_advantage": drawdown_advantage,
                    "health_state": health_state,
                    "health_reason": reason,
                }
            )
    return pd.DataFrame(rows)


def build_strategy_health_summary(window_health: pd.DataFrame) -> pd.DataFrame:
    if window_health.empty:
        return pd.DataFrame()
    rows: list[dict[str, object]] = []
    state_priority = {
        REVIEW_FOR_RETIREMENT: 0,
        WATCH: 1,
        INSUFFICIENT_DATA: 2,
        KEEP: 3,
    }
    for strategy, group in window_health.groupby("strategy", sort=False):
        full = group.loc[group["window"].eq("full")]
        if full.empty:
            decisive = group.iloc[0]
        else:
            decisive = full.iloc[0]
        worst_state = min(group["health_state"].astype(str), key=lambda state: state_priority.get(state, 99))
        if str(decisive["health_state"]) == REVIEW_FOR_RETIREMENT:
            overall = REVIEW_FOR_RETIREMENT
            reason = str(decisive["health_reason"])
        elif worst_state in {WATCH, REVIEW_FOR_RETIREMENT}:
            overall = WATCH
            reason = "one or more windows require monitoring"
        elif worst_state == INSUFFICIENT_DATA and group["health_state"].nunique() == 1:
            overall = INSUFFICIENT_DATA
            reason = "all windows lack enough overlapping observations"
        else:
            overall = KEEP
            reason = "no retirement or watch gate triggered"
        rows.append(
            {
                "strategy": strategy,
                "overall_health_state": overall,
                "overall_reason": reason,
                "full_window_excess_cagr": decisive.get("excess_cagr"),
                "full_window_drawdown_advantage": decisive.get("drawdown_advantage"),
                "full_window_strategy_cagr": decisive.get("strategy_cagr"),
                "full_window_benchmark_cagr": decisive.get("benchmark_cagr"),
                "full_window_strategy_max_drawdown": decisive.get("strategy_max_drawdown"),
                "full_window_benchmark_max_drawdown": decisive.get("benchmark_max_drawdown"),
                "watch_windows": ",".join(
                    group.loc[group["health_state"].isin([WATCH, REVIEW_FOR_RETIREMENT]), "window"].astype(str)
                ),
            }
        )
    return pd.DataFrame(rows)


def _format_pct(value: object) -> str:
    try:
        number = float(value)
    except Exception:
        return ""
    if pd.isna(number):
        return ""
    return f"{number:.2%}"


def build_markdown_report(summary: pd.DataFrame, window_health: pd.DataFrame, *, policy: HealthPolicy) -> str:
    lines = [
        "# Live Strategy Health Report",
        "",
        "This report is an evidence layer only. It does not change live allocations, strategy manifests, or broker settings.",
        "",
        "Policy:",
        "",
        f"- Minimum overlapping observations per window: {policy.min_observations}",
        f"- Minimum excess CAGR vs primary benchmark: {_format_pct(policy.min_excess_cagr)}",
        f"- Required drawdown advantage when underperforming: {_format_pct(policy.min_drawdown_advantage)}",
        f"- Watch threshold for worse drawdown: {_format_pct(policy.max_drawdown_lag)}",
        "",
        "## Summary",
        "",
        "| Strategy | Health | Full Excess CAGR | Full Drawdown Advantage | Reason |",
        "| --- | --- | ---: | ---: | --- |",
    ]
    for row in summary.to_dict(orient="records"):
        lines.append(
            "| {strategy} | {state} | {excess} | {drawdown} | {reason} |".format(
                strategy=row.get("strategy", ""),
                state=row.get("overall_health_state", ""),
                excess=_format_pct(row.get("full_window_excess_cagr")),
                drawdown=_format_pct(row.get("full_window_drawdown_advantage")),
                reason=str(row.get("overall_reason", "")).replace("|", "\\|"),
            )
        )
    lines.extend(
        [
            "",
            "## Window Details",
            "",
            "| Strategy | Window | State | Strategy CAGR | Benchmark CAGR | Strategy Max DD | Benchmark Max DD | Reason |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in window_health.to_dict(orient="records"):
        lines.append(
            "| {strategy} | {window} | {state} | {strategy_cagr} | {benchmark_cagr} | {strategy_dd} | {benchmark_dd} | {reason} |".format(
                strategy=row.get("strategy", ""),
                window=row.get("window", ""),
                state=row.get("health_state", ""),
                strategy_cagr=_format_pct(row.get("strategy_cagr")),
                benchmark_cagr=_format_pct(row.get("benchmark_cagr")),
                strategy_dd=_format_pct(row.get("strategy_max_drawdown")),
                benchmark_dd=_format_pct(row.get("benchmark_max_drawdown")),
                reason=str(row.get("health_reason", "")).replace("|", "\\|"),
            )
        )
    lines.append("")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build an evidence-only live strategy health report.")
    parser.add_argument("--returns", required=True, help="CSV containing a date column and daily return columns.")
    parser.add_argument("--date-column", default="as_of")
    parser.add_argument("--strategies", required=True, help="Comma-separated strategy return columns to audit.")
    parser.add_argument("--primary-benchmark", default=DEFAULT_PRIMARY_BENCHMARK)
    parser.add_argument("--min-observations", type=int, default=DEFAULT_MIN_OBSERVATIONS)
    parser.add_argument("--min-excess-cagr", type=float, default=DEFAULT_MIN_EXCESS_CAGR)
    parser.add_argument("--min-drawdown-advantage", type=float, default=DEFAULT_MIN_DRAWDOWN_ADVANTAGE)
    parser.add_argument("--max-drawdown-lag", type=float, default=DEFAULT_MAX_DRAWDOWN_LAG)
    parser.add_argument("--output-dir", default="data/output/live_strategy_health_report")
    return parser


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return_matrix = pd.read_csv(args.returns)
    strategies = _normalize_columns(args.strategies)
    policy = HealthPolicy(
        min_observations=int(args.min_observations),
        min_excess_cagr=float(args.min_excess_cagr),
        min_drawdown_advantage=float(args.min_drawdown_advantage),
        max_drawdown_lag=float(args.max_drawdown_lag),
    )
    window_health = build_strategy_window_health(
        return_matrix,
        strategies=strategies,
        primary_benchmark=args.primary_benchmark,
        policy=policy,
        date_column=args.date_column,
    )
    summary = build_strategy_health_summary(window_health)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    summary.to_csv(output_dir / "strategy_health_summary.csv", index=False)
    window_health.to_csv(output_dir / "strategy_health_windows.csv", index=False)
    (output_dir / "strategy_health_report.md").write_text(
        build_markdown_report(summary, window_health, policy=policy),
        encoding="utf-8",
    )
    _write_json(
        output_dir / "run_manifest.json",
        {
            "artifact_type": "live_strategy_health_report",
            "source_returns": str(args.returns),
            "date_column": str(args.date_column),
            "strategies": list(strategies),
            "primary_benchmark": str(args.primary_benchmark),
            "policy": policy.to_dict(),
            "outputs": [
                "strategy_health_summary.csv",
                "strategy_health_windows.csv",
                "strategy_health_report.md",
                "run_manifest.json",
            ],
        },
    )
    print(summary.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
