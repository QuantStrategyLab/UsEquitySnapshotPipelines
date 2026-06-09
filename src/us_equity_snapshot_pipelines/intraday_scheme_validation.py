from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path
from typing import Mapping, Sequence

import numpy as np
import pandas as pd

from .intraday_crash_circuit_breaker_research import (
    DEFAULT_SOXL_PRICES,
    DEFAULT_TQQQ_PRICES,
    STRATEGY_SPECS,
    StrategySpec,
    _align_hourly_close_to_daily_prices,
    _load_hourly_close,
    _load_prices,
    _normalize_returns,
    _run_core_backtest,
    _summarize_returns,
)
from .intraday_scheme_research import (
    HourlyOverlayRule,
    _period_detail,
    _summaries_with_deltas,
    _window_scorecard,
    apply_execution_timing_overlay,
    apply_hourly_overlay,
)

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = ROOT / "data/output/intraday_scheme_validation"


def _load_raw_intraday(path: str | Path, *, label: str) -> pd.DataFrame:
    frame = pd.read_csv(path)
    time_column = next((column for column in ("time", "timestamp", "datetime", "as_of") if column in frame.columns), None)
    required = {"symbol", "close"}
    if time_column is None or not required.issubset(frame.columns):
        raise ValueError(f"{label} CSV must include symbol, close, and one of time/timestamp/datetime/as_of")
    normalized = pd.DataFrame(
        {
            "source": label,
            "symbol": frame["symbol"].astype(str).str.upper().str.strip(),
            "time": pd.to_datetime(frame[time_column], errors="coerce").dt.tz_localize(None),
            "close": pd.to_numeric(frame["close"], errors="coerce"),
        }
    ).dropna(subset=["symbol", "time", "close"])
    return normalized


def audit_intraday_data(*, hourly_prices: str | Path, intraday_15m_prices: str | Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    frames = [
        _load_raw_intraday(hourly_prices, label="1h"),
        _load_raw_intraday(intraday_15m_prices, label="15m"),
    ]
    data = pd.concat(frames, ignore_index=True)
    data["date"] = data["time"].dt.normalize()

    summary_rows: list[dict[str, object]] = []
    session_rows: list[dict[str, object]] = []
    for (source, symbol), group in data.groupby(["source", "symbol"]):
        duplicates = int(group.duplicated(["time"]).sum())
        by_day = group.groupby("date").agg(
            bars=("close", "size"),
            first_time=("time", "min"),
            last_time=("time", "max"),
            min_close=("close", "min"),
            max_close=("close", "max"),
        )
        expected_bars = 7 if source == "1h" else 26
        short_days = int((by_day["bars"] < expected_bars).sum())
        long_days = int((by_day["bars"] > expected_bars).sum())
        summary_rows.append(
            {
                "Source": source,
                "Symbol": symbol,
                "Rows": int(len(group)),
                "First Time": str(group["time"].min()),
                "Last Time": str(group["time"].max()),
                "Trading Days": int(len(by_day)),
                "Median Bars Per Day": float(by_day["bars"].median()),
                "Min Bars Per Day": int(by_day["bars"].min()),
                "Max Bars Per Day": int(by_day["bars"].max()),
                "Expected Bars Per Full Day": expected_bars,
                "Short Days": short_days,
                "Long Days": long_days,
                "Duplicate Symbol-Time Rows": duplicates,
                "Nonpositive Closes": int((group["close"] <= 0.0).sum()),
            }
        )
        for date, row in by_day.loc[(by_day["bars"] != expected_bars)].iterrows():
            session_rows.append(
                {
                    "Source": source,
                    "Symbol": symbol,
                    "Date": str(pd.Timestamp(date).date()),
                    "Bars": int(row["bars"]),
                    "Expected Bars": expected_bars,
                    "First Time": str(row["first_time"]),
                    "Last Time": str(row["last_time"]),
                }
            )
    return pd.DataFrame(summary_rows).sort_values(["Source", "Symbol"]), pd.DataFrame(session_rows)


def _prepare_strategy_inputs(
    *,
    hourly_close: pd.DataFrame | None,
    intraday_15m_close: pd.DataFrame | None,
    tqqq_prices: str | Path,
    soxl_prices: str | Path,
    research_start: str | None,
) -> dict[str, dict[str, object]]:
    inputs: dict[str, dict[str, object]] = {}
    price_paths = {"tqqq": Path(tqqq_prices), "soxl": Path(soxl_prices)}
    effective_start = research_start
    if effective_start is None and hourly_close is not None and not hourly_close.empty:
        effective_start = str(pd.Timestamp(hourly_close.index.min()).normalize().date())

    for key, spec in STRATEGY_SPECS.items():
        prices = _load_prices(price_paths[key])
        result = _run_core_backtest(spec, prices)
        returns = _normalize_returns(result["portfolio_returns"])
        weights = result["weights_history"].copy()
        weights.index = pd.to_datetime(weights.index, errors="coerce").tz_localize(None).normalize()
        if effective_start:
            start_ts = pd.Timestamp(effective_start).normalize()
            returns = returns.loc[returns.index >= start_ts]
            weights = weights.loc[weights.index >= start_ts]
        inputs[spec.profile] = {
            "spec": spec,
            "prices": prices,
            "returns": returns,
            "weights": weights,
            "hourly_close": _align_hourly_close_to_daily_prices(hourly_close, prices),
            "intraday_15m_close": _align_hourly_close_to_daily_prices(intraday_15m_close, prices),
        }
    return inputs


def _summarize_variant(
    *,
    profile: str,
    display_name: str,
    variant: str,
    layer: str,
    events: int,
    returns: pd.Series,
) -> dict[str, object]:
    return {
        "Profile": profile,
        "Display Name": display_name,
        "Variant": variant,
        "Layer": layer,
        "Events": int(events),
        **_summarize_returns(returns),
    }


def threshold_sensitivity(
    strategy_inputs: Mapping[str, Mapping[str, object]],
    *,
    thresholds: Sequence[float],
    circuit_cost_bps: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    summary_rows: list[dict[str, object]] = []
    period_frames: list[pd.DataFrame] = []
    for profile, inputs in strategy_inputs.items():
        spec = inputs["spec"]
        assert isinstance(spec, StrategySpec)
        baseline = _normalize_returns(inputs["returns"])
        return_streams: dict[str, pd.Series] = {"baseline": baseline}
        summary_rows.append(
            _summarize_variant(
                profile=profile,
                display_name=spec.display_name,
                variant="baseline",
                layer="threshold_sensitivity",
                events=0,
                returns=baseline,
            )
        )
        for threshold in thresholds:
            rule = HourlyOverlayRule(
                name=f"fixed_{abs(float(threshold)):.0%}_exit",
                mode="exit_rest_day",
                threshold=float(threshold),
            )
            adjusted, events = apply_hourly_overlay(
                portfolio_returns=baseline,
                weights_history=inputs["weights"],
                prices=inputs["prices"],
                risk_symbols=spec.risk_symbols,
                hourly_close=inputs["hourly_close"],
                rule=rule,
                circuit_cost_bps=circuit_cost_bps,
            )
            return_streams[rule.name] = adjusted
            summary_rows.append(
                _summarize_variant(
                    profile=profile,
                    display_name=spec.display_name,
                    variant=rule.name,
                    layer="threshold_sensitivity",
                    events=len(events),
                    returns=adjusted,
                )
            )
        period = _period_detail(profile, return_streams)
        if not period.empty:
            period_frames.append(period)
    summary = _summaries_with_deltas(pd.DataFrame(summary_rows))
    period_detail = pd.concat(period_frames, ignore_index=True) if period_frames else pd.DataFrame()
    return summary, period_detail


def cost_sensitivity(
    strategy_inputs: Mapping[str, Mapping[str, object]],
    *,
    rules_by_profile: Mapping[str, Sequence[HourlyOverlayRule]],
    costs_bps: Sequence[float],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for profile, inputs in strategy_inputs.items():
        spec = inputs["spec"]
        assert isinstance(spec, StrategySpec)
        baseline = _normalize_returns(inputs["returns"])
        baseline_summary = _summarize_returns(baseline)
        for rule in rules_by_profile.get(profile, ()):
            for cost_bps in costs_bps:
                adjusted, events = apply_hourly_overlay(
                    portfolio_returns=baseline,
                    weights_history=inputs["weights"],
                    prices=inputs["prices"],
                    risk_symbols=spec.risk_symbols,
                    hourly_close=inputs["hourly_close"],
                    rule=rule,
                    circuit_cost_bps=float(cost_bps),
                )
                summary = _summarize_returns(adjusted)
                rows.append(
                    {
                        "Profile": profile,
                        "Display Name": spec.display_name,
                        "Variant": rule.name,
                        "Cost Bps": float(cost_bps),
                        "Events": int(len(events)),
                        **summary,
                        "CAGR Delta": float(summary["CAGR"]) - float(baseline_summary["CAGR"]),
                        "Max Drawdown Delta": float(summary["Max Drawdown"]) - float(baseline_summary["Max Drawdown"]),
                        "Sharpe Delta": float(summary["Sharpe"]) - float(baseline_summary["Sharpe"]),
                    }
                )
    return pd.DataFrame(rows)


def combined_overlay_validation(
    strategy_inputs: Mapping[str, Mapping[str, object]],
    *,
    rules_by_profile: Mapping[str, Sequence[HourlyOverlayRule]],
    execution_slots: Sequence[str],
    circuit_cost_bps: float,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    summary_rows: list[dict[str, object]] = []
    event_rows: list[pd.DataFrame] = []
    period_frames: list[pd.DataFrame] = []
    for profile, inputs in strategy_inputs.items():
        spec = inputs["spec"]
        assert isinstance(spec, StrategySpec)
        baseline = _normalize_returns(inputs["returns"])
        return_streams: dict[str, pd.Series] = {"baseline": baseline}
        summary_rows.append(
            _summarize_variant(
                profile=profile,
                display_name=spec.display_name,
                variant="baseline",
                layer="combined_overlay",
                events=0,
                returns=baseline,
            )
        )
        for rule in rules_by_profile.get(profile, ()):
            hourly_adjusted, hourly_events = apply_hourly_overlay(
                portfolio_returns=baseline,
                weights_history=inputs["weights"],
                prices=inputs["prices"],
                risk_symbols=spec.risk_symbols,
                hourly_close=inputs["hourly_close"],
                rule=rule,
                circuit_cost_bps=circuit_cost_bps,
            )
            for slot in execution_slots:
                combined, execution_events = apply_execution_timing_overlay(
                    portfolio_returns=hourly_adjusted,
                    weights_history=inputs["weights"],
                    prices=inputs["prices"],
                    intraday_close=inputs["intraday_15m_close"],
                    slot=slot,
                )
                variant = f"{rule.name}+execution_{slot}"
                return_streams[variant] = combined
                summary_rows.append(
                    _summarize_variant(
                        profile=profile,
                        display_name=spec.display_name,
                        variant=variant,
                        layer="combined_overlay",
                        events=int(len(hourly_events) + len(execution_events)),
                        returns=combined,
                    )
                )
                if not execution_events.empty:
                    events = execution_events.copy()
                    events.insert(0, "variant", variant)
                    events.insert(0, "profile", profile)
                    event_rows.append(events)
        period = _period_detail(profile, return_streams)
        if not period.empty:
            period_frames.append(period)
    summary = _summaries_with_deltas(pd.DataFrame(summary_rows))
    events = pd.concat(event_rows, ignore_index=True) if event_rows else pd.DataFrame()
    period_detail = pd.concat(period_frames, ignore_index=True) if period_frames else pd.DataFrame()
    return summary, events, period_detail


def run_validation(
    *,
    output_dir: str | Path,
    tqqq_prices: str | Path = DEFAULT_TQQQ_PRICES,
    soxl_prices: str | Path = DEFAULT_SOXL_PRICES,
    hourly_prices: str | Path,
    intraday_15m_prices: str | Path,
    research_start: str | None = None,
) -> Path:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    data_quality, session_anomalies = audit_intraday_data(
        hourly_prices=hourly_prices,
        intraday_15m_prices=intraday_15m_prices,
    )
    data_quality.to_csv(output_path / "data_quality_summary.csv", index=False)
    session_anomalies.to_csv(output_path / "data_quality_session_anomalies.csv", index=False)

    hourly_close = _load_hourly_close(hourly_prices)
    intraday_15m_close = _load_hourly_close(intraday_15m_prices)
    strategy_inputs = _prepare_strategy_inputs(
        hourly_close=hourly_close,
        intraday_15m_close=intraday_15m_close,
        tqqq_prices=tqqq_prices,
        soxl_prices=soxl_prices,
        research_start=research_start,
    )

    thresholds = tuple(-value / 100.0 for value in range(3, 13))
    threshold_summary, threshold_period = threshold_sensitivity(
        strategy_inputs,
        thresholds=thresholds,
        circuit_cost_bps=5.0,
    )
    threshold_summary.to_csv(output_path / "threshold_sensitivity.csv", index=False)
    threshold_period.to_csv(output_path / "threshold_period_detail.csv", index=False)
    _window_scorecard(threshold_period).to_csv(output_path / "threshold_window_scorecard.csv", index=False)

    rules_by_profile = {
        "tqqq_growth_income": (
            HourlyOverlayRule(
                name="hourly_dynamic_1_5_5_9_reentry_half",
                mode="same_day_reentry",
                dynamic_k=1.5,
                dynamic_floor=0.05,
                dynamic_cap=0.09,
            ),
            HourlyOverlayRule(
                name="hourly_two_step_5_8",
                mode="two_step",
                warning_threshold=-0.05,
                stop_threshold=-0.08,
            ),
        ),
        "soxl_soxx_trend_income": (
            HourlyOverlayRule(name="hourly_fixed_8_exit", mode="exit_rest_day", threshold=-0.08),
            HourlyOverlayRule(
                name="hourly_two_step_5_8",
                mode="two_step",
                warning_threshold=-0.05,
                stop_threshold=-0.08,
            ),
        ),
    }
    costs = (0.0, 5.0, 10.0, 20.0, 50.0)
    cost_sensitivity(strategy_inputs, rules_by_profile=rules_by_profile, costs_bps=costs).to_csv(
        output_path / "cost_sensitivity.csv",
        index=False,
    )

    combined_summary, combined_events, combined_period = combined_overlay_validation(
        strategy_inputs,
        rules_by_profile=rules_by_profile,
        execution_slots=("first_15m", "last_15m"),
        circuit_cost_bps=5.0,
    )
    combined_summary.to_csv(output_path / "combined_overlay_summary.csv", index=False)
    combined_events.to_csv(output_path / "combined_overlay_execution_events.csv", index=False)
    combined_period.to_csv(output_path / "combined_overlay_period_detail.csv", index=False)
    _window_scorecard(combined_period).to_csv(output_path / "combined_overlay_window_scorecard.csv", index=False)
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run additional validation for intraday scheme research.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--tqqq-prices", default=str(DEFAULT_TQQQ_PRICES))
    parser.add_argument("--soxl-prices", default=str(DEFAULT_SOXL_PRICES))
    parser.add_argument("--hourly-prices", required=True)
    parser.add_argument("--intraday-15m-prices", required=True)
    parser.add_argument("--research-start")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = run_validation(
        output_dir=args.output_dir,
        tqqq_prices=args.tqqq_prices,
        soxl_prices=args.soxl_prices,
        hourly_prices=args.hourly_prices,
        intraday_15m_prices=args.intraday_15m_prices,
        research_start=args.research_start,
    )
    print(f"wrote intraday scheme validation -> {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
