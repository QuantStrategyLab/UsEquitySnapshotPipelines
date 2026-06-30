from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Sequence

import numpy as np
import pandas as pd

KEEP = "keep"
WATCH = "watch"
REVIEW = "review"
INSUFFICIENT_DATA = "insufficient_data"

DEFAULT_WINDOWS = (63, 126, 252)
DEFAULT_PRIMARY_BENCHMARK = "QQQ"
DEFAULT_SECONDARY_BENCHMARK = "SPY"
DEFAULT_MIN_OBSERVATIONS = 60
DEFAULT_MIN_EXCESS_CAGR = 0.0
DEFAULT_MIN_REALIZED_EXPECTED_RATIO = 0.50

RUSSELL_DAILY_COLUMNS = {
    "Date",
    "Run",
    "Strategy Return",
    "QQQ Return",
    "SPY Return",
}


@dataclass(frozen=True)
class DecayPolicy:
    min_observations: int = DEFAULT_MIN_OBSERVATIONS
    min_excess_cagr_vs_primary: float = DEFAULT_MIN_EXCESS_CAGR
    min_excess_cagr_vs_secondary: float = DEFAULT_MIN_EXCESS_CAGR
    min_realized_expected_ratio: float = DEFAULT_MIN_REALIZED_EXPECTED_RATIO

    def to_dict(self) -> dict[str, float | int]:
        return asdict(self)


def _normalize_columns(columns: Sequence[str] | str | None) -> tuple[str, ...]:
    if columns is None:
        return ()
    raw = columns.split(",") if isinstance(columns, str) else list(columns)
    return tuple(dict.fromkeys(str(column or "").strip() for column in raw if str(column or "").strip()))


def _parse_int_list(value: str | Sequence[int] | None) -> tuple[int, ...]:
    if value is None:
        return DEFAULT_WINDOWS
    if isinstance(value, str):
        raw = [item.strip() for item in value.split(",") if item.strip()]
    else:
        raw = [str(item).strip() for item in value if str(item).strip()]
    windows = tuple(int(item) for item in raw)
    if not windows or any(window <= 0 for window in windows):
        raise ValueError("windows must contain positive integers")
    return tuple(dict.fromkeys(windows))


def _normalize_date_series(values: pd.Series) -> pd.Series:
    return pd.to_datetime(values, errors="coerce").dt.tz_localize(None).dt.normalize()


def _looks_like_russell_daily(frame: pd.DataFrame) -> bool:
    return RUSSELL_DAILY_COLUMNS.issubset(set(frame.columns))


def _normalize_russell_daily_returns(
    returns: pd.DataFrame,
    *,
    candidate_runs: Sequence[str] | str | None = None,
) -> tuple[pd.DataFrame, tuple[str, ...], str]:
    frame = pd.DataFrame(returns).copy()
    frame["Date"] = _normalize_date_series(frame["Date"])
    frame = frame.dropna(subset=["Date", "Run"])
    runs = _normalize_columns(candidate_runs) or tuple(dict.fromkeys(frame["Run"].astype(str)))
    if runs:
        frame = frame.loc[frame["Run"].astype(str).isin(runs)].copy()
    if frame.empty:
        raise ValueError("no Russell daily return rows matched candidate_runs")
    for column in ("Strategy Return", "QQQ Return", "SPY Return"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    strategy_returns = frame.pivot_table(index="Date", columns="Run", values="Strategy Return", aggfunc="first")
    benchmark_returns = frame.groupby("Date", sort=True)[["QQQ Return", "SPY Return"]].first()
    benchmark_returns = benchmark_returns.rename(columns={"QQQ Return": "QQQ", "SPY Return": "SPY"})
    matrix = pd.concat([strategy_returns, benchmark_returns], axis=1).sort_index()
    matrix.index.name = "as_of"
    return matrix, tuple(str(run) for run in strategy_returns.columns), "russell_daily"


def _normalize_wide_returns(
    returns: pd.DataFrame,
    *,
    strategies: Sequence[str] | str | None = None,
    candidate_runs: Sequence[str] | str | None = None,
    date_column: str = "as_of",
) -> tuple[pd.DataFrame, tuple[str, ...], str]:
    frame = pd.DataFrame(returns).copy()
    if date_column in frame.columns:
        frame[date_column] = _normalize_date_series(frame[date_column])
        frame = frame.dropna(subset=[date_column]).set_index(date_column)
    else:
        index = pd.to_datetime(frame.index, errors="coerce")
        if getattr(index, "tz", None) is not None:
            index = index.tz_localize(None)
        frame.index = pd.DatetimeIndex(index).normalize()
        frame = frame.loc[frame.index.notna()]
    for column in frame.columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    strategy_columns = _normalize_columns(strategies) or _normalize_columns(candidate_runs)
    if not strategy_columns:
        raise ValueError("at least one strategy column or candidate run is required")
    frame = frame.sort_index()
    frame.index.name = "as_of"
    return frame, strategy_columns, "wide"


def normalize_return_matrix(
    returns: pd.DataFrame,
    *,
    strategies: Sequence[str] | str | None = None,
    candidate_runs: Sequence[str] | str | None = None,
    date_column: str = "as_of",
    input_format: str = "auto",
) -> tuple[pd.DataFrame, tuple[str, ...], str]:
    frame = pd.DataFrame(returns).copy()
    normalized_format = str(input_format or "auto").strip().lower()
    if normalized_format not in {"auto", "wide", "russell_daily"}:
        raise ValueError("input_format must be one of: auto, wide, russell_daily")
    if normalized_format == "russell_daily" or (normalized_format == "auto" and _looks_like_russell_daily(frame)):
        return _normalize_russell_daily_returns(frame, candidate_runs=candidate_runs or strategies)
    return _normalize_wide_returns(
        frame,
        strategies=strategies,
        candidate_runs=candidate_runs,
        date_column=date_column,
    )


def _summarize_returns(returns: pd.Series) -> dict[str, float | int | str]:
    series = pd.to_numeric(pd.Series(returns), errors="coerce").dropna().sort_index()
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
        }
    equity = (1.0 + series).cumprod()
    years = max((series.index[-1] - series.index[0]).days / 365.25, 1.0 / 365.25)
    total_return = float(equity.iloc[-1] - 1.0)
    cagr = float(equity.iloc[-1] ** (1.0 / years) - 1.0)
    drawdown = equity / equity.cummax() - 1.0
    std = float(series.std(ddof=0))
    return {
        "start": str(pd.Timestamp(series.index[0]).date()),
        "end": str(pd.Timestamp(series.index[-1]).date()),
        "observations": int(len(series)),
        "total_return": total_return,
        "cagr": cagr,
        "max_drawdown": float(drawdown.min()),
        "volatility": float(series.std(ddof=0) * np.sqrt(252.0)),
        "sharpe": float(series.mean() / std * np.sqrt(252.0)) if std else float("nan"),
    }


def classify_decay_window(
    *,
    observations: int,
    excess_cagr_vs_primary: float,
    excess_cagr_vs_secondary: float | None,
    realized_expected_ratio: float | None,
    policy: DecayPolicy = DecayPolicy(),
) -> tuple[str, str]:
    if observations < int(policy.min_observations):
        return INSUFFICIENT_DATA, "not enough overlapping observations"

    primary_fail = bool(
        pd.notna(excess_cagr_vs_primary) and excess_cagr_vs_primary < float(policy.min_excess_cagr_vs_primary)
    )
    secondary_fail = bool(
        excess_cagr_vs_secondary is not None
        and pd.notna(excess_cagr_vs_secondary)
        and excess_cagr_vs_secondary < float(policy.min_excess_cagr_vs_secondary)
    )
    expected_fail = bool(
        realized_expected_ratio is not None
        and pd.notna(realized_expected_ratio)
        and realized_expected_ratio < float(policy.min_realized_expected_ratio)
    )

    reasons: list[str] = []
    if primary_fail:
        reasons.append("underperforms primary benchmark")
    if secondary_fail:
        reasons.append("underperforms secondary benchmark")
    if expected_fail:
        reasons.append("realized edge is below expected edge")

    if (primary_fail and secondary_fail) or (primary_fail and expected_fail):
        return REVIEW, "; ".join(reasons)
    if primary_fail or secondary_fail or expected_fail:
        return WATCH, "; ".join(reasons)
    return KEEP, "meets benchmark and expected-edge decay gates"


def _expected_for_strategy(expected: dict[str, float], strategy: str) -> float | None:
    if strategy in expected:
        return float(expected[strategy])
    if "*" in expected:
        return float(expected["*"])
    return None


def build_live_decay_monitor(
    returns: pd.DataFrame,
    *,
    strategies: Sequence[str] | str | None = None,
    candidate_runs: Sequence[str] | str | None = None,
    primary_benchmark: str = DEFAULT_PRIMARY_BENCHMARK,
    secondary_benchmark: str = DEFAULT_SECONDARY_BENCHMARK,
    windows: Sequence[int] = DEFAULT_WINDOWS,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    min_excess_cagr_vs_primary: float = DEFAULT_MIN_EXCESS_CAGR,
    min_excess_cagr_vs_secondary: float = DEFAULT_MIN_EXCESS_CAGR,
    expected_excess_cagr_by_strategy: dict[str, float] | None = None,
    min_realized_expected_ratio: float = DEFAULT_MIN_REALIZED_EXPECTED_RATIO,
    date_column: str = "as_of",
    input_format: str = "auto",
) -> dict[str, pd.DataFrame | dict[str, object]]:
    matrix, strategy_columns, resolved_input_format = normalize_return_matrix(
        returns,
        strategies=strategies,
        candidate_runs=candidate_runs,
        date_column=date_column,
        input_format=input_format,
    )
    primary = str(primary_benchmark or "").strip()
    secondary = str(secondary_benchmark or "").strip()
    if not primary:
        raise ValueError("primary_benchmark is required")
    required = [*strategy_columns, primary]
    if secondary:
        required.append(secondary)
    missing = [column for column in required if column not in matrix.columns]
    if missing:
        raise ValueError(f"return matrix missing columns: {missing}")

    policy = DecayPolicy(
        min_observations=int(min_observations),
        min_excess_cagr_vs_primary=float(min_excess_cagr_vs_primary),
        min_excess_cagr_vs_secondary=float(min_excess_cagr_vs_secondary),
        min_realized_expected_ratio=float(min_realized_expected_ratio),
    )
    expected = {str(key): float(value) for key, value in (expected_excess_cagr_by_strategy or {}).items()}
    window_sizes = _parse_int_list(tuple(windows))
    rows: list[dict[str, object]] = []
    for strategy in strategy_columns:
        expected_edge = _expected_for_strategy(expected, strategy)
        for window in window_sizes:
            columns = [strategy, primary] + ([secondary] if secondary else [])
            aligned = matrix.loc[:, columns].tail(int(window)).dropna()
            strategy_summary = _summarize_returns(aligned[strategy])
            primary_summary = _summarize_returns(aligned[primary])
            secondary_summary = _summarize_returns(aligned[secondary]) if secondary else None
            excess_primary = float(strategy_summary["cagr"]) - float(primary_summary["cagr"])
            excess_secondary = (
                float(strategy_summary["cagr"]) - float(secondary_summary["cagr"])
                if secondary_summary is not None
                else None
            )
            realized_expected_ratio = (
                float(excess_primary) / float(expected_edge)
                if expected_edge is not None and float(expected_edge) > 0.0 and pd.notna(excess_primary)
                else None
            )
            state, reason = classify_decay_window(
                observations=int(strategy_summary["observations"]),
                excess_cagr_vs_primary=excess_primary,
                excess_cagr_vs_secondary=excess_secondary,
                realized_expected_ratio=realized_expected_ratio,
                policy=policy,
            )
            rows.append(
                {
                    "strategy": strategy,
                    "window": f"trailing_{int(window)}d",
                    "window_observations_requested": int(window),
                    "start": strategy_summary["start"],
                    "end": strategy_summary["end"],
                    "observations": int(strategy_summary["observations"]),
                    "primary_benchmark": primary,
                    "secondary_benchmark": secondary,
                    "strategy_total_return": strategy_summary["total_return"],
                    "strategy_cagr": strategy_summary["cagr"],
                    "strategy_max_drawdown": strategy_summary["max_drawdown"],
                    "primary_benchmark_cagr": primary_summary["cagr"],
                    "primary_benchmark_max_drawdown": primary_summary["max_drawdown"],
                    "secondary_benchmark_cagr": secondary_summary["cagr"] if secondary_summary is not None else None,
                    "secondary_benchmark_max_drawdown": secondary_summary["max_drawdown"]
                    if secondary_summary is not None
                    else None,
                    "excess_cagr_vs_primary": excess_primary,
                    "excess_cagr_vs_secondary": excess_secondary,
                    "expected_excess_cagr_vs_primary": expected_edge,
                    "realized_expected_ratio": realized_expected_ratio,
                    "decay_state": state,
                    "decay_reason": reason,
                }
            )
    window_summary = pd.DataFrame(rows)
    strategy_summary = build_strategy_decay_summary(window_summary)
    return {
        "live_decay_window_summary": window_summary,
        "live_decay_strategy_summary": strategy_summary,
        "manifest_inputs": {
            "input_format": resolved_input_format,
            "strategies": list(strategy_columns),
            "primary_benchmark": primary,
            "secondary_benchmark": secondary,
            "windows": list(window_sizes),
            "policy": policy.to_dict(),
            "expected_excess_cagr_by_strategy": expected,
        },
    }


def build_strategy_decay_summary(window_summary: pd.DataFrame) -> pd.DataFrame:
    if window_summary.empty:
        return pd.DataFrame()
    priority = {REVIEW: 0, WATCH: 1, INSUFFICIENT_DATA: 2, KEEP: 3}
    rows: list[dict[str, object]] = []
    for strategy, group in window_summary.groupby("strategy", sort=False):
        states = group["decay_state"].astype(str)
        worst_state = min(states, key=lambda state: priority.get(state, 99))
        review_count = int(states.eq(REVIEW).sum())
        watch_count = int(states.eq(WATCH).sum())
        insufficient_count = int(states.eq(INSUFFICIENT_DATA).sum())
        if worst_state == REVIEW:
            reason = "one or more windows show material live decay"
            action = "human_review_keep_runtime_unchanged"
        elif worst_state == WATCH:
            reason = "one or more windows require monitoring"
            action = "monitor_next_cycle"
        elif insufficient_count == len(group):
            reason = "all windows lack enough observations"
            action = "collect_more_live_data"
        else:
            reason = "no decay gate triggered"
            action = "continue_shadow_or_live_monitoring"
        rows.append(
            {
                "strategy": strategy,
                "overall_decay_state": worst_state,
                "overall_reason": reason,
                "recommended_action": action,
                "window_count": int(len(group)),
                "review_window_count": review_count,
                "watch_window_count": watch_count,
                "insufficient_window_count": insufficient_count,
                "worst_excess_cagr_vs_primary": group["excess_cagr_vs_primary"].min(),
                "worst_excess_cagr_vs_secondary": group["excess_cagr_vs_secondary"].min(),
                "worst_realized_expected_ratio": group["realized_expected_ratio"].min(),
                "watch_windows": ",".join(
                    group.loc[group["decay_state"].isin([WATCH, REVIEW]), "window"].astype(str)
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


def build_markdown_report(strategy_summary: pd.DataFrame, window_summary: pd.DataFrame, *, policy: DecayPolicy) -> str:
    lines = [
        "# Live Decay Monitor",
        "",
        "This is a research/observability artifact only. It does not change live allocations, strategy manifests, or broker settings.",
        "",
        "Policy:",
        "",
        f"- Minimum observations per trailing window: {policy.min_observations}",
        f"- Minimum excess CAGR vs primary benchmark: {_format_pct(policy.min_excess_cagr_vs_primary)}",
        f"- Minimum excess CAGR vs secondary benchmark: {_format_pct(policy.min_excess_cagr_vs_secondary)}",
        f"- Minimum realized/expected edge ratio when an expectation is supplied: {policy.min_realized_expected_ratio:.2f}",
        "",
        "## Strategy Summary",
        "",
        "| Strategy | State | Worst primary excess CAGR | Worst secondary excess CAGR | Watch windows | Action |",
        "| --- | --- | ---: | ---: | --- | --- |",
    ]
    for row in strategy_summary.to_dict(orient="records"):
        lines.append(
            "| {strategy} | {state} | {primary} | {secondary} | {windows} | {action} |".format(
                strategy=row.get("strategy", ""),
                state=row.get("overall_decay_state", ""),
                primary=_format_pct(row.get("worst_excess_cagr_vs_primary")),
                secondary=_format_pct(row.get("worst_excess_cagr_vs_secondary")),
                windows=row.get("watch_windows") or "none",
                action=row.get("recommended_action", ""),
            )
        )
    lines.extend(
        [
            "",
            "## Window Details",
            "",
            "| Strategy | Window | State | Strategy CAGR | Primary CAGR | Secondary CAGR | Expected edge | Realized/expected | Reason |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for row in window_summary.to_dict(orient="records"):
        ratio = row.get("realized_expected_ratio")
        ratio_text = "" if ratio is None or pd.isna(ratio) else f"{float(ratio):.2f}"
        lines.append(
            "| {strategy} | {window} | {state} | {strategy_cagr} | {primary_cagr} | {secondary_cagr} | {expected} | {ratio} | {reason} |".format(
                strategy=row.get("strategy", ""),
                window=row.get("window", ""),
                state=row.get("decay_state", ""),
                strategy_cagr=_format_pct(row.get("strategy_cagr")),
                primary_cagr=_format_pct(row.get("primary_benchmark_cagr")),
                secondary_cagr=_format_pct(row.get("secondary_benchmark_cagr")),
                expected=_format_pct(row.get("expected_excess_cagr_vs_primary")),
                ratio=ratio_text or "n/a",
                reason=str(row.get("decay_reason", "")).replace("|", "\\|"),
            )
        )
    lines.append("")
    return "\n".join(lines)


def _read_expected_csv(path: str | None) -> dict[str, float]:
    if not path:
        return {}
    frame = pd.read_csv(path)
    required = {"strategy", "expected_excess_cagr_vs_primary"}
    if not required.issubset(frame.columns):
        raise ValueError("expected CSV must contain strategy,expected_excess_cagr_vs_primary columns")
    result: dict[str, float] = {}
    for row in frame.to_dict(orient="records"):
        strategy = str(row.get("strategy", "") or "").strip()
        if strategy:
            result[strategy] = float(row.get("expected_excess_cagr_vs_primary"))
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build an evidence-only live decay monitor for strategy returns.")
    parser.add_argument("--returns", required=True, help="CSV containing daily returns in wide or Russell daily-return format.")
    parser.add_argument("--input-format", default="auto", choices=("auto", "wide", "russell_daily"))
    parser.add_argument("--date-column", default="as_of")
    parser.add_argument("--strategies", help="Comma-separated strategy columns for wide return matrices.")
    parser.add_argument("--candidate-runs", help="Comma-separated Run values for Russell daily-return matrices.")
    parser.add_argument("--primary-benchmark", default=DEFAULT_PRIMARY_BENCHMARK)
    parser.add_argument("--secondary-benchmark", default=DEFAULT_SECONDARY_BENCHMARK)
    parser.add_argument("--windows", default=",".join(str(window) for window in DEFAULT_WINDOWS))
    parser.add_argument("--min-observations", type=int, default=DEFAULT_MIN_OBSERVATIONS)
    parser.add_argument("--min-excess-cagr-vs-primary", type=float, default=DEFAULT_MIN_EXCESS_CAGR)
    parser.add_argument("--min-excess-cagr-vs-secondary", type=float, default=DEFAULT_MIN_EXCESS_CAGR)
    parser.add_argument(
        "--expected-excess-cagr",
        type=float,
        default=None,
        help="Optional expected annual excess CAGR vs primary benchmark applied to all strategies.",
    )
    parser.add_argument(
        "--expected-excess-cagr-csv",
        help="Optional CSV with strategy,expected_excess_cagr_vs_primary columns.",
    )
    parser.add_argument("--min-realized-expected-ratio", type=float, default=DEFAULT_MIN_REALIZED_EXPECTED_RATIO)
    parser.add_argument("--output-dir", default="data/output/live_decay_monitor")
    return parser


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    returns = pd.read_csv(args.returns)
    expected = _read_expected_csv(args.expected_excess_cagr_csv)
    if args.expected_excess_cagr is not None:
        expected.setdefault("*", float(args.expected_excess_cagr))
    windows = _parse_int_list(args.windows)
    result = build_live_decay_monitor(
        returns,
        strategies=args.strategies,
        candidate_runs=args.candidate_runs,
        primary_benchmark=args.primary_benchmark,
        secondary_benchmark=args.secondary_benchmark,
        windows=windows,
        min_observations=int(args.min_observations),
        min_excess_cagr_vs_primary=float(args.min_excess_cagr_vs_primary),
        min_excess_cagr_vs_secondary=float(args.min_excess_cagr_vs_secondary),
        expected_excess_cagr_by_strategy=expected,
        min_realized_expected_ratio=float(args.min_realized_expected_ratio),
        date_column=str(args.date_column),
        input_format=str(args.input_format),
    )
    window_summary = pd.DataFrame(result["live_decay_window_summary"])
    strategy_summary = pd.DataFrame(result["live_decay_strategy_summary"])
    manifest_inputs = dict(result["manifest_inputs"])
    policy = DecayPolicy(**manifest_inputs["policy"])

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    window_summary.to_csv(output_dir / "live_decay_window_summary.csv", index=False)
    strategy_summary.to_csv(output_dir / "live_decay_strategy_summary.csv", index=False)
    (output_dir / "live_decay_report.md").write_text(
        build_markdown_report(strategy_summary, window_summary, policy=policy),
        encoding="utf-8",
    )
    outputs = [
        "live_decay_window_summary.csv",
        "live_decay_strategy_summary.csv",
        "live_decay_report.md",
        "live_decay_monitor_manifest.json",
    ]
    _write_json(
        output_dir / "live_decay_monitor_manifest.json",
        {
            "manifest_type": "live_decay_monitor",
            "artifact_schema_version": "live_decay_monitor.v1",
            "generated_at": datetime.now(UTC).isoformat(),
            "source_returns": str(args.returns),
            "input_format": manifest_inputs["input_format"],
            "strategies": manifest_inputs["strategies"],
            "primary_benchmark": manifest_inputs["primary_benchmark"],
            "secondary_benchmark": manifest_inputs["secondary_benchmark"],
            "windows": manifest_inputs["windows"],
            "policy": manifest_inputs["policy"],
            "expected_excess_cagr_by_strategy": manifest_inputs["expected_excess_cagr_by_strategy"],
            "row_counts": {
                "live_decay_window_summary": int(len(window_summary)),
                "live_decay_strategy_summary": int(len(strategy_summary)),
            },
            "artifacts": {name.removesuffix(".csv").removesuffix(".md").removesuffix(".json"): {"path": name} for name in outputs},
            "outputs": outputs,
        },
    )
    print(strategy_summary.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
