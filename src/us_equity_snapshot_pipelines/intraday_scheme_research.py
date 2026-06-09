from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Mapping, Sequence

import numpy as np
import pandas as pd

from .intraday_crash_circuit_breaker_research import (
    DEFAULT_SOXL_PRICES,
    DEFAULT_TQQQ_PRICES,
    STRATEGY_SPECS,
    StrategySpec,
    _align_hourly_close_to_daily_prices,
    _daily_symbol_returns,
    _load_hourly_close,
    _load_prices,
    _normalize_returns,
    _prepare_weights,
    _run_core_backtest,
    _summarize_returns,
)
from .soxl_soxx_trend_income_backtest import _build_close_matrix

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = ROOT / "data/output/intraday_scheme_research"
THRESHOLD_EPSILON = 1e-12


@dataclass(frozen=True)
class HourlyOverlayRule:
    name: str
    mode: Literal["exit_rest_day", "same_day_reentry", "two_step"]
    threshold: float | None = None
    warning_threshold: float | None = None
    stop_threshold: float | None = None
    dynamic_k: float | None = None
    dynamic_floor: float = 0.05
    dynamic_cap: float = 0.09
    reentry_fraction: float = 0.5


HOURLY_RULES: tuple[HourlyOverlayRule, ...] = (
    HourlyOverlayRule(name="hourly_fixed_5_exit", mode="exit_rest_day", threshold=-0.05),
    HourlyOverlayRule(name="hourly_fixed_8_exit", mode="exit_rest_day", threshold=-0.08),
    HourlyOverlayRule(name="hourly_fixed_8_reentry_half", mode="same_day_reentry", threshold=-0.08),
    HourlyOverlayRule(
        name="hourly_two_step_5_8",
        mode="two_step",
        warning_threshold=-0.05,
        stop_threshold=-0.08,
    ),
    HourlyOverlayRule(
        name="hourly_dynamic_1_5_5_9_exit",
        mode="exit_rest_day",
        dynamic_k=1.5,
        dynamic_floor=0.05,
        dynamic_cap=0.09,
    ),
    HourlyOverlayRule(
        name="hourly_dynamic_1_5_5_9_reentry_half",
        mode="same_day_reentry",
        dynamic_k=1.5,
        dynamic_floor=0.05,
        dynamic_cap=0.09,
    ),
)

EXECUTION_SLOTS = ("first_15m", "first_hour", "mid_day", "twap", "last_15m")


def _risk_return_path_for_day(
    intraday_close: pd.DataFrame | None,
    *,
    date: pd.Timestamp,
    previous_daily_close: pd.Series,
    risk_weights: pd.Series,
) -> pd.Series:
    if intraday_close is None or intraday_close.empty:
        return pd.Series(dtype=float)
    day = pd.Timestamp(date).normalize()
    intraday = intraday_close.loc[intraday_close.index.normalize() == day]
    if intraday.empty:
        return pd.Series(dtype=float)

    weighted_paths: list[pd.Series] = []
    for symbol, weight in risk_weights.items():
        numeric_weight = float(weight)
        if numeric_weight <= 0.0 or symbol not in intraday.columns:
            continue
        base = float(previous_daily_close.get(symbol, np.nan))
        if not np.isfinite(base) or base <= 0.0:
            continue
        path = pd.to_numeric(intraday[symbol], errors="coerce") / base - 1.0
        weighted_paths.append(numeric_weight * path)
    if not weighted_paths:
        return pd.Series(dtype=float)
    risk_path = sum(weighted_paths).dropna()
    risk_path.name = "risk_return"
    return risk_path


def _risk_sleeve_daily_returns(
    weights: pd.DataFrame,
    symbol_returns: pd.DataFrame,
    risk_columns: Sequence[str],
) -> pd.Series:
    risk_weights = weights.loc[:, [column for column in risk_columns if column in weights.columns]].fillna(0.0)
    risk_weight = risk_weights.sum(axis=1)
    returns = symbol_returns.reindex(weights.index).loc[:, risk_weights.columns].fillna(0.0)
    sleeve = (risk_weights * returns).sum(axis=1)
    sleeve = sleeve.where(risk_weight <= 1e-12, sleeve / risk_weight.replace(0.0, np.nan))
    return sleeve.dropna()


def _threshold_series_for_rule(
    rule: HourlyOverlayRule,
    *,
    weights: pd.DataFrame,
    symbol_returns: pd.DataFrame,
    risk_columns: Sequence[str],
) -> pd.Series:
    if rule.dynamic_k is None:
        threshold = rule.threshold
        if threshold is None:
            threshold = rule.warning_threshold
        if threshold is None:
            raise ValueError(f"rule {rule.name} does not define a threshold")
        return pd.Series(float(threshold), index=weights.index)

    sleeve_returns = _risk_sleeve_daily_returns(weights, symbol_returns, risk_columns)
    realized_vol = sleeve_returns.rolling(20, min_periods=10).std(ddof=0).shift(1)
    dynamic_threshold = -(float(rule.dynamic_k) * realized_vol).clip(
        lower=float(rule.dynamic_floor),
        upper=float(rule.dynamic_cap),
    )
    return dynamic_threshold.reindex(weights.index).fillna(-float(rule.dynamic_floor))


def apply_hourly_overlay(
    *,
    portfolio_returns: pd.Series,
    weights_history: pd.DataFrame,
    prices: pd.DataFrame,
    risk_symbols: Sequence[str],
    hourly_close: pd.DataFrame | None,
    rule: HourlyOverlayRule,
    circuit_cost_bps: float = 5.0,
) -> tuple[pd.Series, pd.DataFrame]:
    returns = _normalize_returns(portfolio_returns)
    weights = _prepare_weights(weights_history, pd.DatetimeIndex(returns.index))
    symbol_returns = _daily_symbol_returns(prices, pd.DatetimeIndex(returns.index)).fillna(0.0)
    close = _build_close_matrix(prices).ffill()
    close.index = pd.to_datetime(close.index).tz_localize(None).normalize()

    risk_columns = [symbol for symbol in risk_symbols if symbol in weights.columns]
    threshold_series = _threshold_series_for_rule(
        rule,
        weights=weights,
        symbol_returns=symbol_returns,
        risk_columns=risk_columns,
    )
    cost_rate = float(circuit_cost_bps) / 10_000.0

    adjusted = returns.copy()
    event_rows: list[dict[str, object]] = []
    for as_of, base_return in returns.items():
        risk_weights = weights.loc[as_of, risk_columns].fillna(0.0)
        risk_weight = float(risk_weights.sum())
        if risk_weight <= 1e-12:
            continue
        normalized_risk_weights = risk_weights / risk_weight
        daily_risk_returns = symbol_returns.loc[as_of, risk_columns]
        risk_return = float((normalized_risk_weights * daily_risk_returns.fillna(0.0)).sum())
        previous_closes = close.shift(1).reindex([as_of]).iloc[0] if as_of in close.index else pd.Series(dtype=float)
        risk_path = _risk_return_path_for_day(
            hourly_close,
            date=as_of,
            previous_daily_close=previous_closes,
            risk_weights=normalized_risk_weights,
        )
        if risk_path.empty:
            continue

        threshold = float(threshold_series.loc[as_of])
        replacement_risk_return: float | None = None
        event_type = ""
        trigger_time: pd.Timestamp | None = None
        reentry_time: pd.Timestamp | None = None
        cost_turnover = 0.0

        if rule.mode in {"exit_rest_day", "same_day_reentry"}:
            breaches = risk_path.loc[risk_path <= threshold + THRESHOLD_EPSILON]
            if breaches.empty:
                continue
            trigger_time = pd.Timestamp(breaches.index[0])
            trigger_return = float(breaches.iloc[0])
            replacement_risk_return = trigger_return
            cost_turnover = 1.0
            event_type = "exit_rest_day"

            if rule.mode == "same_day_reentry":
                reentry_threshold = threshold * float(rule.reentry_fraction)
                after_trigger = risk_path.loc[risk_path.index > trigger_time]
                reentries = after_trigger.loc[after_trigger >= reentry_threshold - THRESHOLD_EPSILON]
                if not reentries.empty:
                    reentry_time = pd.Timestamp(reentries.index[0])
                    reentry_return = float(reentries.iloc[0])
                    replacement_risk_return = (1.0 + trigger_return) * (
                        (1.0 + risk_return) / (1.0 + reentry_return)
                    ) - 1.0
                    cost_turnover = 2.0
                    event_type = "same_day_reentry"

        elif rule.mode == "two_step":
            warning_threshold = float(rule.warning_threshold if rule.warning_threshold is not None else threshold)
            stop_threshold = float(rule.stop_threshold if rule.stop_threshold is not None else threshold)
            warnings = risk_path.loc[risk_path <= warning_threshold + THRESHOLD_EPSILON]
            if warnings.empty:
                continue
            trigger_time = pd.Timestamp(warnings.index[0])
            warning_return = float(warnings.iloc[0])
            after_warning = risk_path.loc[risk_path.index >= trigger_time]
            stops = after_warning.loc[after_warning <= stop_threshold + THRESHOLD_EPSILON]
            cost_turnover = 0.5
            if stops.empty:
                replacement_risk_return = 0.5 * warning_return + 0.5 * risk_return
                event_type = "warning_half_out"
            else:
                stop_time = pd.Timestamp(stops.index[0])
                stop_return = float(stops.iloc[0])
                reentry_time = stop_time
                replacement_risk_return = 0.5 * warning_return + 0.5 * stop_return
                cost_turnover = 1.0
                event_type = "warning_then_full_exit"
        else:
            raise ValueError(f"unsupported hourly rule mode: {rule.mode}")

        if replacement_risk_return is None:
            continue
        adjusted_return = float(base_return) - (risk_weight * risk_return) + (risk_weight * replacement_risk_return)
        adjusted_return -= risk_weight * cost_rate * cost_turnover
        adjusted.at[as_of] = adjusted_return
        event_rows.append(
            {
                "as_of": str(pd.Timestamp(as_of).date()),
                "rule": rule.name,
                "event_type": event_type,
                "trigger_time": "" if trigger_time is None else str(trigger_time),
                "reentry_or_stop_time": "" if reentry_time is None else str(reentry_time),
                "threshold": threshold,
                "risk_weight": risk_weight,
                "daily_risk_return": risk_return,
                "replacement_risk_return": replacement_risk_return,
                "base_portfolio_return": float(base_return),
                "adjusted_portfolio_return": float(adjusted_return),
                "cost_turnover": cost_turnover,
            }
        )

    return adjusted, pd.DataFrame(event_rows)


def _slot_pre_execution_returns(
    intraday_close: pd.DataFrame,
    prices: pd.DataFrame,
    slot: str,
) -> pd.DataFrame:
    aligned = _align_hourly_close_to_daily_prices(intraday_close, prices)
    if aligned is None or aligned.empty:
        return pd.DataFrame()
    close = _build_close_matrix(prices).ffill()
    close.index = pd.to_datetime(close.index).tz_localize(None).normalize()
    previous_close = close.shift(1)

    rows: list[pd.Series] = []
    row_index: list[pd.Timestamp] = []
    for day, group in aligned.groupby(aligned.index.normalize()):
        clean_group = group.dropna(how="all")
        if clean_group.empty:
            continue
        if slot == "first_15m":
            selected = clean_group.iloc[0]
        elif slot == "first_hour":
            selected = clean_group.iloc[min(3, len(clean_group) - 1)]
        elif slot == "mid_day":
            selected = clean_group.iloc[len(clean_group) // 2]
        elif slot == "last_15m":
            selected = clean_group.iloc[-1]
        elif slot == "twap":
            selected = clean_group.mean(axis=0, numeric_only=True)
        else:
            raise ValueError(f"unsupported execution slot: {slot}")
        previous = previous_close.reindex([pd.Timestamp(day)]).iloc[0] if pd.Timestamp(day) in previous_close.index else None
        if previous is None:
            continue
        pre_execution_return = pd.to_numeric(selected, errors="coerce") / pd.to_numeric(previous, errors="coerce") - 1.0
        rows.append(pre_execution_return)
        row_index.append(pd.Timestamp(day).normalize())
    if not rows:
        return pd.DataFrame()
    result = pd.DataFrame(rows, index=pd.DatetimeIndex(row_index)).sort_index()
    result.index.name = "as_of"
    return result


def apply_execution_timing_overlay(
    *,
    portfolio_returns: pd.Series,
    weights_history: pd.DataFrame,
    prices: pd.DataFrame,
    intraday_close: pd.DataFrame | None,
    slot: str,
) -> tuple[pd.Series, pd.DataFrame]:
    returns = _normalize_returns(portfolio_returns)
    weights = _prepare_weights(weights_history, pd.DatetimeIndex(returns.index)).fillna(0.0)
    if intraday_close is None or intraday_close.empty:
        return returns.copy(), pd.DataFrame()

    pre_execution = _slot_pre_execution_returns(intraday_close, prices, slot).reindex(returns.index)
    if pre_execution.empty:
        return returns.copy(), pd.DataFrame()

    previous_weights = weights.shift(1).reindex(returns.index).fillna(0.0)
    target_weights = weights.reindex(returns.index).fillna(0.0)
    columns = [column for column in pre_execution.columns if column in target_weights.columns]
    adjusted = returns.copy()
    event_rows: list[dict[str, object]] = []
    for as_of, base_return in returns.items():
        if as_of not in pre_execution.index:
            continue
        symbol_pre_returns = pre_execution.loc[as_of, columns].fillna(0.0)
        previous = previous_weights.loc[as_of, columns].fillna(0.0)
        target = target_weights.loc[as_of, columns].fillna(0.0)
        turnover = float((target - previous).abs().sum())
        if turnover <= 1e-12:
            continue
        adjustment = float(((previous - target) * symbol_pre_returns).sum())
        adjusted.at[as_of] = float(base_return) + adjustment
        event_rows.append(
            {
                "as_of": str(pd.Timestamp(as_of).date()),
                "slot": slot,
                "tracked_turnover": turnover,
                "pre_execution_adjustment": adjustment,
                "base_portfolio_return": float(base_return),
                "adjusted_portfolio_return": float(adjusted.at[as_of]),
            }
        )
    return adjusted, pd.DataFrame(event_rows)


def _summaries_with_deltas(summary: pd.DataFrame) -> pd.DataFrame:
    baseline_by_profile = summary.loc[summary["Variant"].eq("baseline")].set_index("Profile")
    summary["CAGR Delta"] = [
        float(row["CAGR"]) - float(baseline_by_profile.loc[row["Profile"], "CAGR"])
        for _, row in summary.iterrows()
    ]
    summary["Max Drawdown Delta"] = [
        float(row["Max Drawdown"]) - float(baseline_by_profile.loc[row["Profile"], "Max Drawdown"])
        for _, row in summary.iterrows()
    ]
    summary["Sharpe Delta"] = [
        float(row["Sharpe"]) - float(baseline_by_profile.loc[row["Profile"], "Sharpe"])
        for _, row in summary.iterrows()
    ]
    return summary


def _period_windows(returns: Mapping[str, pd.Series]) -> list[tuple[str, pd.Timestamp, pd.Timestamp]]:
    non_empty = [series for series in returns.values() if not series.empty]
    if not non_empty:
        return []
    start = min(series.index.min() for series in non_empty)
    end = max(series.index.max() for series in non_empty)
    windows: list[tuple[str, pd.Timestamp, pd.Timestamp]] = [("full", start, end)]

    for years in (1, 3, 5, 10, 15):
        window_start = end - pd.DateOffset(years=years)
        windows.append((f"trailing_{years}y", pd.Timestamp(window_start).normalize(), end))

    for year in range(int(start.year), int(end.year) + 1):
        year_start = pd.Timestamp(f"{year}-01-01")
        year_end = pd.Timestamp(f"{year}-12-31")
        windows.append((f"calendar_{year}", year_start, min(year_end, end)))

    regimes = (
        ("regime_2011_2014", "2011-01-01", "2014-12-31"),
        ("regime_2015_2019", "2015-01-01", "2019-12-31"),
        ("regime_2020_2021", "2020-01-01", "2021-12-31"),
        ("regime_2022", "2022-01-01", "2022-12-31"),
        ("regime_2023_2024", "2023-01-01", "2024-12-31"),
        ("regime_2025_2026ytd", "2025-01-01", str(end.date())),
    )
    for name, window_start, window_end in regimes:
        windows.append((name, pd.Timestamp(window_start), pd.Timestamp(window_end)))
    return windows


def _period_detail(profile: str, return_streams: Mapping[str, pd.Series]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    windows = _period_windows(return_streams)
    for variant, series in return_streams.items():
        clean = _normalize_returns(series)
        if clean.empty:
            continue
        for window_name, window_start, window_end in windows:
            window_returns = clean.loc[(clean.index >= window_start) & (clean.index <= window_end)]
            if len(window_returns) < 20:
                continue
            summary = _summarize_returns(window_returns)
            rows.append({"Profile": profile, "Variant": variant, "Window": window_name, **summary})
    detail = pd.DataFrame(rows)
    if detail.empty:
        return detail
    baseline = detail.loc[detail["Variant"].eq("baseline")].set_index("Window")
    detail["CAGR Delta"] = [
        float(row["CAGR"]) - float(baseline.loc[row["Window"], "CAGR"]) if row["Window"] in baseline.index else np.nan
        for _, row in detail.iterrows()
    ]
    detail["Max Drawdown Delta"] = [
        float(row["Max Drawdown"]) - float(baseline.loc[row["Window"], "Max Drawdown"])
        if row["Window"] in baseline.index
        else np.nan
        for _, row in detail.iterrows()
    ]
    return detail


def _window_scorecard(period_detail: pd.DataFrame) -> pd.DataFrame:
    if period_detail.empty:
        return pd.DataFrame()
    detail = period_detail.loc[period_detail["Variant"].ne("baseline")].copy()
    if detail.empty:
        return pd.DataFrame()
    detail["Window Type"] = detail["Window"].astype(str).str.extract(r"^(calendar|trailing|regime|full)")[0].fillna(
        "other"
    )
    rows: list[dict[str, object]] = []
    for (profile, variant, window_type), group in detail.groupby(["Profile", "Variant", "Window Type"]):
        cagr_delta = pd.to_numeric(group["CAGR Delta"], errors="coerce")
        maxdd_delta = pd.to_numeric(group["Max Drawdown Delta"], errors="coerce")
        both_win = (cagr_delta > 0.0) & (maxdd_delta > 0.0)
        rows.append(
            {
                "Profile": profile,
                "Variant": variant,
                "Window Type": window_type,
                "Windows": int(len(group)),
                "CAGR Win Rate": float((cagr_delta > 0.0).mean()),
                "MaxDD Win Rate": float((maxdd_delta > 0.0).mean()),
                "Both Win Rate": float(both_win.mean()),
                "Median CAGR Delta": float(cagr_delta.median()),
                "Median MaxDD Delta": float(maxdd_delta.median()),
                "Worst CAGR Delta": float(cagr_delta.min()),
                "Worst MaxDD Delta": float(maxdd_delta.min()),
            }
        )
    return pd.DataFrame(rows).sort_values(["Profile", "Window Type", "Variant"])


def _run_strategy_variants(
    *,
    spec: StrategySpec,
    prices: pd.DataFrame,
    portfolio_returns: pd.Series,
    weights_history: pd.DataFrame,
    hourly_close: pd.DataFrame | None,
    intraday_15m_close: pd.DataFrame | None,
    circuit_cost_bps: float,
) -> tuple[list[dict[str, object]], dict[str, pd.Series], list[pd.DataFrame], list[pd.DataFrame]]:
    summary_rows: list[dict[str, object]] = []
    return_streams: dict[str, pd.Series] = {}
    hourly_events: list[pd.DataFrame] = []
    execution_events: list[pd.DataFrame] = []

    baseline = _normalize_returns(portfolio_returns)
    return_streams["baseline"] = baseline
    summary_rows.append(
        {
            "Profile": spec.profile,
            "Display Name": spec.display_name,
            "Variant": "baseline",
            "Layer": "daily_core",
            "Events": 0,
            **_summarize_returns(baseline),
        }
    )

    for rule in HOURLY_RULES:
        adjusted, events = apply_hourly_overlay(
            portfolio_returns=baseline,
            weights_history=weights_history,
            prices=prices,
            risk_symbols=spec.risk_symbols,
            hourly_close=hourly_close,
            rule=rule,
            circuit_cost_bps=circuit_cost_bps,
        )
        return_streams[rule.name] = adjusted
        summary_rows.append(
            {
                "Profile": spec.profile,
                "Display Name": spec.display_name,
                "Variant": rule.name,
                "Layer": "hourly_risk",
                "Events": int(len(events)),
                **_summarize_returns(adjusted),
            }
        )
        if not events.empty:
            events.insert(0, "profile", spec.profile)
            hourly_events.append(events)

    for slot in EXECUTION_SLOTS:
        adjusted, events = apply_execution_timing_overlay(
            portfolio_returns=baseline,
            weights_history=weights_history,
            prices=prices,
            intraday_close=intraday_15m_close,
            slot=slot,
        )
        variant = f"execution_{slot}"
        return_streams[variant] = adjusted
        summary_rows.append(
            {
                "Profile": spec.profile,
                "Display Name": spec.display_name,
                "Variant": variant,
                "Layer": "execution_15m",
                "Events": int(len(events)),
                **_summarize_returns(adjusted),
            }
        )
        if not events.empty:
            events.insert(0, "profile", spec.profile)
            execution_events.append(events)

    return summary_rows, return_streams, hourly_events, execution_events


def run_research(
    *,
    output_dir: str | Path,
    tqqq_prices: str | Path = DEFAULT_TQQQ_PRICES,
    soxl_prices: str | Path = DEFAULT_SOXL_PRICES,
    hourly_prices: str | Path | None = None,
    intraday_15m_prices: str | Path | None = None,
    circuit_cost_bps: float = 5.0,
    research_start: str | None = None,
) -> Path:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    hourly_close = _load_hourly_close(hourly_prices)
    intraday_15m_close = _load_hourly_close(intraday_15m_prices)
    effective_research_start = research_start
    if effective_research_start is None and hourly_close is not None and not hourly_close.empty:
        effective_research_start = str(pd.Timestamp(hourly_close.index.min()).normalize().date())

    all_summary_rows: list[dict[str, object]] = []
    all_hourly_events: list[pd.DataFrame] = []
    all_execution_events: list[pd.DataFrame] = []
    all_period_detail: list[pd.DataFrame] = []

    price_paths = {"tqqq": Path(tqqq_prices), "soxl": Path(soxl_prices)}
    for key, spec in STRATEGY_SPECS.items():
        prices = _load_prices(price_paths[key])
        strategy_hourly_close = _align_hourly_close_to_daily_prices(hourly_close, prices)
        strategy_15m_close = _align_hourly_close_to_daily_prices(intraday_15m_close, prices)
        result = _run_core_backtest(spec, prices)
        portfolio_returns = _normalize_returns(result["portfolio_returns"])
        weights_history = result["weights_history"]

        if effective_research_start:
            start_ts = pd.Timestamp(effective_research_start).normalize()
            portfolio_returns = portfolio_returns.loc[portfolio_returns.index >= start_ts]
            weights = weights_history.copy()
            weights.index = pd.to_datetime(weights.index, errors="coerce").tz_localize(None).normalize()
            weights_history = weights.loc[weights.index >= start_ts]

        summary_rows, return_streams, hourly_events, execution_events = _run_strategy_variants(
            spec=spec,
            prices=prices,
            portfolio_returns=portfolio_returns,
            weights_history=weights_history,
            hourly_close=strategy_hourly_close,
            intraday_15m_close=strategy_15m_close,
            circuit_cost_bps=circuit_cost_bps,
        )
        all_summary_rows.extend(summary_rows)
        all_hourly_events.extend(hourly_events)
        all_execution_events.extend(execution_events)
        period_detail = _period_detail(spec.profile, return_streams)
        if not period_detail.empty:
            all_period_detail.append(period_detail)

    summary = _summaries_with_deltas(pd.DataFrame(all_summary_rows))
    summary.to_csv(output_path / "summary.csv", index=False)
    hourly_events = pd.concat(all_hourly_events, ignore_index=True) if all_hourly_events else pd.DataFrame()
    hourly_events.to_csv(output_path / "hourly_events.csv", index=False)
    execution_events = pd.concat(all_execution_events, ignore_index=True) if all_execution_events else pd.DataFrame()
    execution_events.to_csv(output_path / "execution_events.csv", index=False)
    period_detail = pd.concat(all_period_detail, ignore_index=True) if all_period_detail else pd.DataFrame()
    period_detail.to_csv(output_path / "period_detail.csv", index=False)
    _window_scorecard(period_detail).to_csv(output_path / "window_scorecard.csv", index=False)
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research daily/hourly/15-minute intraday scheme overlays.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--tqqq-prices", default=str(DEFAULT_TQQQ_PRICES))
    parser.add_argument("--soxl-prices", default=str(DEFAULT_SOXL_PRICES))
    parser.add_argument("--hourly-prices", help="Optional hourly CSV with symbol,time,close columns.")
    parser.add_argument("--intraday-15m-prices", help="Optional 15-minute CSV with symbol,time,close columns.")
    parser.add_argument("--research-start", help="Optional start date for the comparison window.")
    parser.add_argument("--circuit-cost-bps", type=float, default=5.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = run_research(
        output_dir=args.output_dir,
        tqqq_prices=args.tqqq_prices,
        soxl_prices=args.soxl_prices,
        hourly_prices=args.hourly_prices,
        intraday_15m_prices=args.intraday_15m_prices,
        circuit_cost_bps=float(args.circuit_cost_bps),
        research_start=args.research_start,
    )
    print(f"wrote intraday scheme research -> {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
