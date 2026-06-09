from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

import numpy as np
import pandas as pd

from .soxl_soxx_trend_income_backtest import _build_close_matrix, _build_price_frame
from .soxl_soxx_trend_income_backtest import run_backtest as run_soxl_backtest
from .tqqq_growth_income_archive import _income_disabled_overrides
from .tqqq_growth_income_archive import run_backtest as run_tqqq_backtest

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TQQQ_PRICES = ROOT / "data/output/tqqq_volatility_delever_threshold_research_20260609/normalized_price_history.csv"
DEFAULT_SOXL_PRICES = (
    ROOT / "data/output/soxl_dynamic_volatility_delever_threshold_research_20260609/normalized_price_history.csv"
)
DEFAULT_OUTPUT_DIR = ROOT / "data/output/intraday_crash_circuit_breaker_research"
DEFAULT_THRESHOLDS = (-0.03, -0.05, -0.07, -0.10)


@dataclass(frozen=True)
class StrategySpec:
    profile: str
    display_name: str
    risk_symbols: tuple[str, ...]
    safe_symbol: str
    prices_path: Path
    start_date: str


STRATEGY_SPECS: Mapping[str, StrategySpec] = {
    "tqqq": StrategySpec(
        profile="tqqq_growth_income",
        display_name="TQQQ Growth Income core",
        risk_symbols=("TQQQ", "QQQM"),
        safe_symbol="BOXX",
        prices_path=DEFAULT_TQQQ_PRICES,
        start_date="2010-02-11",
    ),
    "soxl": StrategySpec(
        profile="soxl_soxx_trend_income",
        display_name="SOXL/SOXX Trend Income core",
        risk_symbols=("SOXL", "SOXX"),
        safe_symbol="BOXX",
        prices_path=DEFAULT_SOXL_PRICES,
        start_date="2016-06-06",
    ),
}


def _load_prices(path: str | Path) -> pd.DataFrame:
    return _build_price_frame(pd.read_csv(path))


def _normalize_returns(returns: pd.Series) -> pd.Series:
    normalized = pd.Series(returns).copy()
    normalized.index = pd.to_datetime(normalized.index, errors="coerce").tz_localize(None).normalize()
    normalized = pd.to_numeric(normalized, errors="coerce")
    return normalized.loc[normalized.index.notna()].dropna().sort_index()


def _summarize_returns(returns: pd.Series) -> dict[str, object]:
    clean = _normalize_returns(returns)
    if clean.empty:
        raise RuntimeError("No returns to summarize")
    equity = (1.0 + clean).cumprod()
    total_return = float(equity.iloc[-1] - 1.0)
    years = max((clean.index[-1] - clean.index[0]).days / 365.25, 1 / 365.25)
    cagr = float(equity.iloc[-1] ** (1.0 / years) - 1.0)
    drawdown = equity / equity.cummax() - 1.0
    volatility = float(clean.std(ddof=0) * np.sqrt(252))
    std = float(clean.std(ddof=0))
    return {
        "Start": str(clean.index[0].date()),
        "End": str(clean.index[-1].date()),
        "Observations": int(len(clean)),
        "Total Return": total_return,
        "CAGR": cagr,
        "Max Drawdown": float(drawdown.min()),
        "Volatility": volatility,
        "Sharpe": float(clean.mean() / std * np.sqrt(252)) if std else float("nan"),
        "Final Equity": float(equity.iloc[-1]),
    }


def _run_core_backtest(spec: StrategySpec, prices: pd.DataFrame) -> dict[str, object]:
    if spec.profile == "tqqq_growth_income":
        overrides = {
            **_income_disabled_overrides(),
            "dual_drive_unlevered_symbol": "QQQM",
            "market_regime_control_enabled": False,
            "dual_drive_macro_risk_governor_enabled": False,
            "dual_drive_crisis_defense_enabled": False,
        }
        return run_tqqq_backtest(prices, start_date=spec.start_date, strategy_overrides=overrides)
    if spec.profile == "soxl_soxx_trend_income":
        return run_soxl_backtest(prices, start_date=spec.start_date, disable_income_layer=True)
    raise ValueError(f"unsupported profile: {spec.profile}")


def _prepare_weights(weights_history: pd.DataFrame, returns_index: pd.DatetimeIndex) -> pd.DataFrame:
    weights = weights_history.copy()
    weights.index = pd.to_datetime(weights.index, errors="coerce").tz_localize(None).normalize()
    weights = weights.loc[weights.index.notna()].sort_index()
    return weights.reindex(returns_index).fillna(0.0)


def _daily_symbol_returns(prices: pd.DataFrame, returns_index: pd.DatetimeIndex) -> pd.DataFrame:
    close = _build_close_matrix(prices).ffill()
    symbol_returns = close.pct_change(fill_method=None)
    symbol_returns.index = pd.to_datetime(symbol_returns.index).tz_localize(None).normalize()
    return symbol_returns.reindex(returns_index)


def _load_hourly_close(path: str | Path | None) -> pd.DataFrame | None:
    if not path:
        return None
    frame = pd.read_csv(path)
    symbol_column = "symbol"
    time_column = next((column for column in ("time", "timestamp", "datetime", "as_of") if column in frame.columns), None)
    if time_column is None or symbol_column not in frame.columns or "close" not in frame.columns:
        raise ValueError("hourly price CSV must include symbol, close, and one of time/timestamp/datetime/as_of")
    normalized = pd.DataFrame(
        {
            "symbol": frame[symbol_column].astype(str).str.upper().str.strip(),
            "time": pd.to_datetime(frame[time_column], errors="coerce").dt.tz_localize(None),
            "close": pd.to_numeric(frame["close"], errors="coerce"),
        }
    ).dropna(subset=["symbol", "time", "close"])
    if normalized.empty:
        return pd.DataFrame()
    return normalized.pivot_table(index="time", columns="symbol", values="close", aggfunc="last").sort_index().ffill()


def _align_hourly_close_to_daily_prices(hourly_close: pd.DataFrame | None, prices: pd.DataFrame) -> pd.DataFrame | None:
    if hourly_close is None or hourly_close.empty:
        return hourly_close
    daily_close = _build_close_matrix(prices).ffill()
    daily_close.index = pd.to_datetime(daily_close.index).tz_localize(None).normalize()
    hourly_dates = pd.Series(pd.DatetimeIndex(hourly_close.index).normalize(), index=hourly_close.index)
    aligned = hourly_close.copy()
    for symbol in [column for column in hourly_close.columns if column in daily_close.columns]:
        raw_last_by_day = pd.to_numeric(hourly_close[symbol], errors="coerce").groupby(hourly_dates).last()
        local_daily_close = pd.to_numeric(daily_close[symbol], errors="coerce").reindex(raw_last_by_day.index)
        scale_by_day = (local_daily_close / raw_last_by_day).replace([np.inf, -np.inf], np.nan).dropna()
        if scale_by_day.empty:
            continue
        scale = hourly_dates.map(scale_by_day).astype(float)
        aligned[symbol] = pd.to_numeric(hourly_close[symbol], errors="coerce") * scale
    return aligned


def _hourly_risk_return_for_day(
    hourly_close: pd.DataFrame | None,
    *,
    date: pd.Timestamp,
    previous_daily_close: pd.Series,
    risk_weights: pd.Series,
    threshold: float,
) -> tuple[float | None, pd.Timestamp | None, bool]:
    if hourly_close is None or hourly_close.empty:
        return None, None, False
    day = pd.Timestamp(date).normalize()
    intraday = hourly_close.loc[(hourly_close.index.normalize() == day)]
    if intraday.empty:
        return None, None, False

    weighted_returns = []
    symbols = [symbol for symbol in risk_weights.index if float(risk_weights.get(symbol, 0.0)) > 0.0]
    for symbol in symbols:
        base = float(previous_daily_close.get(symbol, np.nan))
        if not np.isfinite(base) or base <= 0.0 or symbol not in intraday.columns:
            continue
        weighted_returns.append(float(risk_weights[symbol]) * (pd.to_numeric(intraday[symbol], errors="coerce") / base - 1.0))
    if not weighted_returns:
        return None, None, True
    risk_return_path = sum(weighted_returns)
    risk_return_path = risk_return_path.dropna()
    if risk_return_path.empty:
        return None, None, True
    breaches = risk_return_path.loc[risk_return_path <= float(threshold)]
    if breaches.empty:
        return None, None, True
    trigger_time = breaches.index[0]
    return float(breaches.iloc[0]), pd.Timestamp(trigger_time), True


def apply_crash_circuit_breaker(
    *,
    portfolio_returns: pd.Series,
    weights_history: pd.DataFrame,
    prices: pd.DataFrame,
    risk_symbols: Sequence[str],
    threshold: float,
    circuit_cost_bps: float = 5.0,
    hourly_close: pd.DataFrame | None = None,
) -> tuple[pd.Series, pd.DataFrame]:
    returns = _normalize_returns(portfolio_returns)
    weights = _prepare_weights(weights_history, pd.DatetimeIndex(returns.index))
    symbol_returns = _daily_symbol_returns(prices, pd.DatetimeIndex(returns.index))
    close = _build_close_matrix(prices).ffill()
    close.index = pd.to_datetime(close.index).tz_localize(None).normalize()
    risk_columns = [symbol for symbol in risk_symbols if symbol in weights.columns]
    cost_rate = float(circuit_cost_bps) / 10_000.0

    adjusted = returns.copy()
    event_rows: list[dict[str, object]] = []
    for as_of, base_return in returns.items():
        risk_weights = weights.loc[as_of, risk_columns].fillna(0.0)
        risk_weight = float(risk_weights.sum())
        if risk_weight <= 1e-12:
            continue

        daily_risk_returns = symbol_returns.loc[as_of, risk_columns]
        risk_return = float((risk_weights * daily_risk_returns.fillna(0.0)).sum() / risk_weight)
        trigger_return = risk_return
        trigger_time = None
        previous_closes = close.shift(1).reindex([as_of]).iloc[0] if as_of in close.index else pd.Series(dtype=float)
        hourly_risk_return, hourly_time, has_hourly_data = _hourly_risk_return_for_day(
            hourly_close,
            date=as_of,
            previous_daily_close=previous_closes,
            risk_weights=risk_weights / risk_weight,
            threshold=float(threshold),
        )
        if has_hourly_data and hourly_risk_return is None:
            continue
        if hourly_risk_return is not None:
            trigger_return = hourly_risk_return
            trigger_time = hourly_time

        if trigger_return > threshold:
            continue

        # Daily-only runs do not know the exact hour; cap the risk sleeve at the threshold as a threshold-fill proxy.
        replacement_risk_return = trigger_return if hourly_risk_return is not None else float(threshold)
        adjusted_return = float(base_return) - (risk_weight * risk_return) + (risk_weight * replacement_risk_return)
        adjusted_return -= risk_weight * cost_rate
        adjusted.at[as_of] = adjusted_return
        event_rows.append(
            {
                "as_of": str(pd.Timestamp(as_of).date()),
                "trigger_time": "" if trigger_time is None else str(trigger_time),
                "threshold": float(threshold),
                "risk_weight": risk_weight,
                "daily_risk_return": risk_return,
                "trigger_return": float(trigger_return),
                "replacement_risk_return": float(replacement_risk_return),
                "base_portfolio_return": float(base_return),
                "adjusted_portfolio_return": float(adjusted_return),
                "method": "hourly_close" if hourly_risk_return is not None else "daily_threshold_fill_proxy",
            }
        )

    return adjusted, pd.DataFrame(event_rows)


def run_research(
    *,
    output_dir: str | Path,
    thresholds: Sequence[float] = DEFAULT_THRESHOLDS,
    tqqq_prices: str | Path = DEFAULT_TQQQ_PRICES,
    soxl_prices: str | Path = DEFAULT_SOXL_PRICES,
    hourly_prices: str | Path | None = None,
    circuit_cost_bps: float = 5.0,
    research_start: str | None = None,
) -> Path:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    hourly_close = _load_hourly_close(hourly_prices)
    effective_research_start = research_start
    if effective_research_start is None and hourly_close is not None and not hourly_close.empty:
        effective_research_start = str(pd.Timestamp(hourly_close.index.min()).normalize().date())
    summary_rows: list[dict[str, object]] = []
    event_frames: list[pd.DataFrame] = []

    price_paths = {"tqqq": Path(tqqq_prices), "soxl": Path(soxl_prices)}
    for key, spec in STRATEGY_SPECS.items():
        prices = _load_prices(price_paths[key])
        strategy_hourly_close = _align_hourly_close_to_daily_prices(hourly_close, prices)
        result = _run_core_backtest(spec, prices)
        portfolio_returns = _normalize_returns(result["portfolio_returns"])
        weights_history = result["weights_history"]
        if effective_research_start:
            start_ts = pd.Timestamp(effective_research_start).normalize()
            portfolio_returns = portfolio_returns.loc[portfolio_returns.index >= start_ts]
            weights = weights_history.copy()
            weights.index = pd.to_datetime(weights.index, errors="coerce").tz_localize(None).normalize()
            weights_history = weights.loc[weights.index >= start_ts]
        baseline_summary = _summarize_returns(portfolio_returns)
        summary_rows.append(
            {
                "Profile": spec.profile,
                "Display Name": spec.display_name,
                "Variant": "baseline",
                "Threshold": np.nan,
                "Circuit Events": 0,
                "Method": "baseline",
                **baseline_summary,
            }
        )
        for threshold in thresholds:
            adjusted_returns, events = apply_crash_circuit_breaker(
                portfolio_returns=portfolio_returns,
                weights_history=weights_history,
                prices=prices,
                risk_symbols=spec.risk_symbols,
                threshold=float(threshold),
                circuit_cost_bps=float(circuit_cost_bps),
                hourly_close=strategy_hourly_close,
            )
            adjusted_summary = _summarize_returns(adjusted_returns)
            summary_rows.append(
                {
                    "Profile": spec.profile,
                    "Display Name": spec.display_name,
                    "Variant": f"circuit_{abs(float(threshold)):.0%}",
                    "Threshold": float(threshold),
                    "Circuit Events": int(len(events)),
                    "Method": "hourly_close" if hourly_close is not None else "daily_threshold_fill_proxy",
                    **adjusted_summary,
                }
            )
            if not events.empty:
                events.insert(0, "profile", spec.profile)
                event_frames.append(events)

    summary = pd.DataFrame(summary_rows)
    baseline_by_profile = summary.loc[summary["Variant"].eq("baseline")].set_index("Profile")
    summary["CAGR Delta"] = [
        float(row["CAGR"]) - float(baseline_by_profile.loc[row["Profile"], "CAGR"])
        for _, row in summary.iterrows()
    ]
    summary["Max Drawdown Delta"] = [
        float(row["Max Drawdown"]) - float(baseline_by_profile.loc[row["Profile"], "Max Drawdown"])
        for _, row in summary.iterrows()
    ]
    summary.to_csv(output_path / "summary.csv", index=False)
    events = pd.concat(event_frames, ignore_index=True) if event_frames else pd.DataFrame()
    events.to_csv(output_path / "circuit_events.csv", index=False)
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research crypto-style intraday crash circuit breakers for TQQQ/SOXL.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--tqqq-prices", default=str(DEFAULT_TQQQ_PRICES))
    parser.add_argument("--soxl-prices", default=str(DEFAULT_SOXL_PRICES))
    parser.add_argument("--hourly-prices", help="Optional hourly CSV with symbol,time,close columns.")
    parser.add_argument("--research-start", help="Optional start date for the comparison window.")
    parser.add_argument("--thresholds", default=",".join(str(item) for item in DEFAULT_THRESHOLDS))
    parser.add_argument("--circuit-cost-bps", type=float, default=5.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    thresholds = tuple(float(item.strip()) for item in str(args.thresholds).split(",") if item.strip())
    output_dir = run_research(
        output_dir=args.output_dir,
        thresholds=thresholds,
        tqqq_prices=args.tqqq_prices,
        soxl_prices=args.soxl_prices,
        hourly_prices=args.hourly_prices,
        circuit_cost_bps=float(args.circuit_cost_bps),
        research_start=args.research_start,
    )
    print(f"wrote intraday crash circuit breaker research -> {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
