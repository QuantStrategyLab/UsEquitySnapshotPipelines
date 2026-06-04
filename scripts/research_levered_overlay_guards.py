from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import numpy as np
import pandas as pd

from us_equity_snapshot_pipelines.soxl_soxx_trend_income_backtest import (
    _build_chandelier_stop_history,
    _build_close_matrix,
    _build_price_frame,
    _summarize_returns,
    run_backtest as run_soxl_backtest,
)
from us_equity_snapshot_pipelines.tqqq_growth_income_archive import (
    MANAGED_SYMBOLS as TQQQ_MANAGED_SYMBOLS,
    _build_account_snapshot,
    _execute_rebalance as _execute_tqqq_rebalance,
    _income_disabled_overrides,
    _strategy_kwargs as _tqqq_strategy_kwargs,
)
from us_equity_strategies.strategies.tqqq_growth_income import build_rebalance_plan as build_tqqq_plan


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "data" / "output" / "codex_levered_overlay_guard_research_20260604"
TQQQ_PRICE_PATH = ROOT / "data" / "output" / "codex_tqqq_9sig_recheck_20260603" / "price_history.csv"
SOXL_PRICE_PATH = ROOT / "data" / "output" / "codex_soxl_rsi_recheck_20260603" / "price_history.csv"

TQQQ_PERIODS = {
    "pre2020": ("2011-01-04", "2019-12-31"),
    "covid_hike": ("2020-01-02", "2022-12-30"),
    "recent_bull": ("2023-01-03", None),
    "short": ("2025-06-02", None),
    "long": ("2011-01-04", None),
}
SOXL_PERIODS = {
    "pre2020": ("2017-01-04", "2019-12-31"),
    "covid_hike": ("2020-01-03", "2022-12-30"),
    "recent_bull": ("2023-01-04", None),
    "short": ("2025-06-03", None),
    "long": ("2017-01-04", None),
}


@dataclass(frozen=True)
class OverlaySpec:
    candidate: str
    kind: str = "none"
    symbol: str = ""
    window: int = 20
    fast_window: int | None = None
    slow_window: int | None = None
    threshold: float | None = None
    atr_multiple: float = 3.0
    redirect_symbol: str = ""
    retention_ratio: float = 0.0


TQQQ_CANDIDATES = (
    OverlaySpec("current_default", redirect_symbol="QQQ"),
    OverlaySpec("dual_ma_20_60_qqq", kind="dual_ma", symbol="QQQ", fast_window=20, slow_window=60, redirect_symbol="QQQ"),
    OverlaySpec("dual_ma_50_200_qqq", kind="dual_ma", symbol="QQQ", fast_window=50, slow_window=200, redirect_symbol="QQQ"),
    OverlaySpec("rolling_stop_20_5pct_qqq", kind="drawdown", symbol="QQQ", window=20, threshold=-0.05, redirect_symbol="QQQ"),
    OverlaySpec("rolling_stop_30_8pct_qqq", kind="drawdown", symbol="QQQ", window=30, threshold=-0.08, redirect_symbol="QQQ"),
    OverlaySpec("rolling_stop_30_8pct_qqq_ret50", kind="drawdown", symbol="QQQ", window=30, threshold=-0.08, redirect_symbol="QQQ", retention_ratio=0.50),
    OverlaySpec("rolling_stop_30_8pct_qqq_ret75", kind="drawdown", symbol="QQQ", window=30, threshold=-0.08, redirect_symbol="QQQ", retention_ratio=0.75),
    OverlaySpec("chandelier_22_3_qqq", kind="chandelier", symbol="QQQ", window=22, atr_multiple=3.0, redirect_symbol="QQQ"),
    OverlaySpec("chandelier_22_4_qqq", kind="chandelier", symbol="QQQ", window=22, atr_multiple=4.0, redirect_symbol="QQQ"),
    OverlaySpec("chandelier_22_4_qqq_ret50", kind="chandelier", symbol="QQQ", window=22, atr_multiple=4.0, redirect_symbol="QQQ", retention_ratio=0.50),
    OverlaySpec("chandelier_22_4_qqq_ret75", kind="chandelier", symbol="QQQ", window=22, atr_multiple=4.0, redirect_symbol="QQQ", retention_ratio=0.75),
)

SOXL_CANDIDATES = (
    OverlaySpec("current_default", redirect_symbol="SOXX"),
    OverlaySpec("dual_ma_10_30_soxx", kind="dual_ma", symbol="SOXX", fast_window=10, slow_window=30, redirect_symbol="SOXX"),
    OverlaySpec("dual_ma_20_60_soxx", kind="dual_ma", symbol="SOXX", fast_window=20, slow_window=60, redirect_symbol="SOXX"),
    OverlaySpec("rolling_stop_20_8pct_soxx", kind="drawdown", symbol="SOXX", window=20, threshold=-0.08, redirect_symbol="SOXX"),
    OverlaySpec("rolling_stop_30_12pct_soxx", kind="drawdown", symbol="SOXX", window=30, threshold=-0.12, redirect_symbol="SOXX"),
    OverlaySpec("rolling_stop_30_12pct_soxx_ret50", kind="drawdown", symbol="SOXX", window=30, threshold=-0.12, redirect_symbol="SOXX", retention_ratio=0.50),
    OverlaySpec("rolling_stop_30_12pct_soxx_ret75", kind="drawdown", symbol="SOXX", window=30, threshold=-0.12, redirect_symbol="SOXX", retention_ratio=0.75),
    OverlaySpec("chandelier_22_3_soxx", kind="chandelier", symbol="SOXX", window=22, atr_multiple=3.0, redirect_symbol="SOXX"),
    OverlaySpec("chandelier_22_4_soxx", kind="chandelier", symbol="SOXX", window=22, atr_multiple=4.0, redirect_symbol="SOXX"),
    OverlaySpec("chandelier_22_4_soxx_ret50", kind="chandelier", symbol="SOXX", window=22, atr_multiple=4.0, redirect_symbol="SOXX", retention_ratio=0.50),
    OverlaySpec("chandelier_22_4_soxx_ret75", kind="chandelier", symbol="SOXX", window=22, atr_multiple=4.0, redirect_symbol="SOXX", retention_ratio=0.75),
)


def _load_prices(path: Path, *, aliases: Mapping[str, str] | None = None) -> pd.DataFrame:
    frame = pd.read_csv(path)
    aliases = dict(aliases or {})
    additions = []
    for source, target in aliases.items():
        if target in set(frame["symbol"].astype(str).str.upper()):
            continue
        source_rows = frame.loc[frame["symbol"].astype(str).str.upper().eq(source)].copy()
        if not source_rows.empty:
            source_rows["symbol"] = target
            additions.append(source_rows)
    if additions:
        frame = pd.concat([frame, *additions], ignore_index=True)
    return frame


def _overlay_history(price_frame: pd.DataFrame, spec: OverlaySpec) -> pd.DataFrame:
    close_matrix = _build_close_matrix(price_frame)
    if spec.kind == "none":
        return pd.DataFrame({"triggered": False, "metric": np.nan, "threshold": np.nan}, index=close_matrix.index)
    symbol = spec.symbol.upper()
    if symbol not in close_matrix.columns:
        return pd.DataFrame({"triggered": False, "metric": np.nan, "threshold": np.nan}, index=close_matrix.index)
    if spec.kind == "chandelier":
        history = _build_chandelier_stop_history(
            price_frame,
            symbol=symbol,
            window=int(spec.window),
            atr_multiple=float(spec.atr_multiple),
        )
        history["metric"] = history["close"] - history["stop_line"]
        history["threshold"] = 0.0
        return history

    close = close_matrix[symbol].astype(float)
    if spec.kind == "dual_ma":
        fast_window = int(spec.fast_window or 20)
        slow_window = max(fast_window + 1, int(spec.slow_window or 60))
        fast_ma = close.rolling(fast_window, min_periods=fast_window).mean()
        slow_ma = close.rolling(slow_window, min_periods=slow_window).mean()
        metric = fast_ma / slow_ma - 1.0
        threshold = 0.0 if spec.threshold is None else float(spec.threshold)
        triggered = metric <= threshold
        return pd.DataFrame(
            {
                "triggered": triggered,
                "metric": metric,
                "threshold": threshold,
                "fast_ma": fast_ma,
                "slow_ma": slow_ma,
            },
            index=close_matrix.index,
        )
    if spec.kind == "drawdown":
        window = int(spec.window)
        threshold = float(spec.threshold if spec.threshold is not None else -0.05)
        rolling_high = close.rolling(window, min_periods=window).max()
        metric = close / rolling_high - 1.0
        return pd.DataFrame(
            {
                "triggered": metric <= threshold,
                "metric": metric,
                "threshold": threshold,
                "rolling_high": rolling_high,
            },
            index=close_matrix.index,
        )
    raise ValueError(f"unsupported overlay kind: {spec.kind}")


def _run_tqqq_overlay_backtest(
    price_history: pd.DataFrame,
    *,
    spec: OverlaySpec,
    start_date: str,
    end_date: str | None,
    turnover_cost_bps: float = 5.0,
    initial_equity: float = 100_000.0,
) -> dict[str, object]:
    prices = _build_price_frame(price_history)
    if end_date is not None:
        prices = prices.loc[prices["as_of"] <= pd.Timestamp(end_date).normalize()].copy()
    close_matrix = _build_close_matrix(prices).ffill()
    missing = sorted(symbol for symbol in ("TQQQ", "QQQ", "BOXX") if symbol not in close_matrix.columns)
    if missing:
        raise RuntimeError(f"TQQQ price history missing required symbols: {', '.join(missing)}")

    index = close_matrix.dropna(subset=["TQQQ", "QQQ", "BOXX"]).index
    index = index[index >= pd.Timestamp(start_date).normalize()]
    if len(index) < 2:
        raise RuntimeError("Not enough TQQQ price history remains inside the selected date range")

    qqq_history = prices.loc[prices["symbol"].eq("QQQ")].sort_values("as_of")
    overlay = _overlay_history(prices, spec)
    weights_history = pd.DataFrame(0.0, index=index, columns=[*TQQQ_MANAGED_SYMBOLS, "__cash__"])
    portfolio_returns = pd.Series(index=index, dtype=float, name="portfolio_return")
    turnover_history = pd.Series(index=index, dtype=float, name="turnover")
    current_weights = {symbol: 0.0 for symbol in TQQQ_MANAGED_SYMBOLS}
    current_weights["BOXX"] = 1.0
    current_weights["__cash__"] = 0.0
    current_equity = float(initial_equity)
    signal_rows: list[dict[str, object]] = []
    overlay_stops = 0
    overrides = _income_disabled_overrides()

    for pos, as_of in enumerate(index[:-1]):
        next_as_of = index[pos + 1]
        close_row = close_matrix.loc[as_of]
        next_close_row = close_matrix.loc[next_as_of]
        history = qqq_history.loc[qqq_history["as_of"] <= as_of, ["as_of", "close"]]
        if len(history) < 220:
            continue

        snapshot = _build_account_snapshot(
            weights=current_weights,
            equity=current_equity,
            close_prices={symbol: float(close_row.get(symbol, np.nan)) for symbol in TQQQ_MANAGED_SYMBOLS},
        )
        plan = build_tqqq_plan(
            history,
            snapshot,
            signal_text_fn=str,
            translator=lambda key, **kwargs: key,
            **_tqqq_strategy_kwargs(overrides),
        )
        target_values = dict(plan["target_values"])
        overlay_row = overlay.loc[as_of] if as_of in overlay.index else pd.Series(dtype=object)
        triggered = bool(overlay_row.get("triggered", False)) if not overlay_row.empty else False
        if triggered and float(target_values.get("TQQQ", 0.0) or 0.0) > 0.0:
            tqqq_value = float(target_values.get("TQQQ", 0.0))
            retained_value = tqqq_value * float(spec.retention_ratio)
            redirected_value = tqqq_value - retained_value
            target_values["TQQQ"] = retained_value
            target_values[spec.redirect_symbol or "QQQ"] = (
                float(target_values.get(spec.redirect_symbol or "QQQ", 0.0) or 0.0) + redirected_value
            )
            overlay_stops += 1

        next_weights, turnover, next_equity = _execute_tqqq_rebalance(
            current_weights=current_weights,
            target_values=target_values,
            equity=current_equity,
            threshold_value=float(plan["threshold"]),
            turnover_cost_bps=float(turnover_cost_bps),
            sell_order=plan["sell_order_symbols"],
            buy_order=plan["buy_order_symbols"],
        )
        signal_rows.append(
            {
                "signal_date": as_of,
                "effective_date": next_as_of,
                "candidate": spec.candidate,
                "overlay_kind": spec.kind,
                "overlay_symbol": spec.symbol,
                "overlay_triggered": triggered,
                "overlay_metric": overlay_row.get("metric") if not overlay_row.empty else None,
                "overlay_threshold": overlay_row.get("threshold") if not overlay_row.empty else None,
                "target_tqqq": target_values.get("TQQQ", 0.0),
                "target_qqq": target_values.get("QQQ", 0.0),
                "target_boxx": target_values.get("BOXX", 0.0),
                "total_equity": current_equity,
            }
        )
        turnover_history.at[next_as_of] = turnover
        current_weights = {symbol: float(next_weights.get(symbol, 0.0)) for symbol in TQQQ_MANAGED_SYMBOLS}
        current_weights["__cash__"] = float(next_weights.get("__cash__", 0.0))
        current_equity = float(next_equity)

        next_market_values = {
            symbol: float(current_equity) * float(current_weights.get(symbol, 0.0))
            for symbol in TQQQ_MANAGED_SYMBOLS
        }
        next_cash = float(current_equity) * float(current_weights.get("__cash__", 0.0))
        equity_after_return = next_cash
        for symbol in TQQQ_MANAGED_SYMBOLS:
            current_price = float(close_row.get(symbol, np.nan))
            next_price = float(next_close_row.get(symbol, np.nan))
            symbol_value = float(next_market_values.get(symbol, 0.0))
            if np.isfinite(current_price) and np.isfinite(next_price) and current_price > 0.0:
                equity_after_return += symbol_value * (next_price / current_price)
            else:
                equity_after_return += symbol_value
        portfolio_returns.at[next_as_of] = equity_after_return / current_equity - 1.0 if current_equity > 0.0 else np.nan
        for symbol in TQQQ_MANAGED_SYMBOLS:
            weights_history.at[next_as_of, symbol] = float(current_weights.get(symbol, 0.0))
        weights_history.at[next_as_of, "__cash__"] = float(current_weights.get("__cash__", 0.0))
        current_equity = float(equity_after_return)

    used_weights = weights_history.loc[:, (weights_history != 0.0).any(axis=0)]
    summary = _summarize_returns(portfolio_returns, used_weights)
    summary["Overlay Stops"] = float(overlay_stops)
    return {
        "summary": summary,
        "portfolio_returns": portfolio_returns,
        "weights_history": weights_history,
        "turnover_history": turnover_history.fillna(0.0),
        "signal_history": pd.DataFrame(signal_rows),
    }


def _summary_row(strategy: str, candidate: str, period: str, result: Mapping[str, object]) -> dict[str, object]:
    summary = dict(result["summary"])
    return {
        "Strategy": strategy,
        "Candidate": candidate,
        "Period": period,
        "Start": summary.get("Start"),
        "End": summary.get("End"),
        "CAGR": float(summary.get("CAGR", np.nan)),
        "Max Drawdown": float(summary.get("Max Drawdown", np.nan)),
        "Volatility": float(summary.get("Volatility", np.nan)),
        "Sharpe": float(summary.get("Sharpe", np.nan)),
        "Calmar": float(summary.get("Calmar", np.nan)),
        "Total Return": float(summary.get("Total Return", np.nan)),
        "Overlay Stops": float(summary.get("Overlay Stops", summary.get("SOXL Delever Stops", 0.0)) or 0.0),
    }


def _build_screen(summary: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for strategy, strategy_rows in summary.groupby("Strategy", sort=False):
        baseline = strategy_rows.loc[strategy_rows["Candidate"].eq("current_default")].set_index("Period")
        for candidate, candidate_rows in strategy_rows.groupby("Candidate", sort=False):
            candidate_by_period = candidate_rows.set_index("Period")
            joined = candidate_by_period[["CAGR", "Max Drawdown"]].join(
                baseline[["CAGR", "Max Drawdown"]],
                lsuffix="",
                rsuffix=" Baseline",
                how="inner",
            )
            cagr_delta = joined["CAGR"] - joined["CAGR Baseline"]
            mdd_delta = joined["Max Drawdown"] - joined["Max Drawdown Baseline"]
            no_regression = bool((cagr_delta >= -1e-9).all() and (mdd_delta >= -1e-9).all())
            improves_drawdown = bool((mdd_delta > 1e-9).any())
            rows.append(
                {
                    "Strategy": strategy,
                    "Candidate": candidate,
                    "Baseline": "current_default",
                    "Min CAGR Delta": float(cagr_delta.min()),
                    "Min MDD Delta": float(mdd_delta.min()),
                    "Median CAGR Delta": float(cagr_delta.median()),
                    "Median MDD Delta": float(mdd_delta.median()),
                    "No Regression Pass": no_regression,
                    "Drawdown Improved Somewhere": improves_drawdown,
                    "Decision": "review" if no_regression and improves_drawdown and candidate != "current_default" else (
                        "baseline" if candidate == "current_default" else "reject"
                    ),
                }
            )
    return pd.DataFrame(rows)


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    tqqq_prices = _load_prices(TQQQ_PRICE_PATH, aliases={"BIL": "BOXX"})
    soxl_prices = _load_prices(SOXL_PRICE_PATH)
    summary_rows = []
    event_rows = []

    for spec in TQQQ_CANDIDATES:
        for period, (start, end) in TQQQ_PERIODS.items():
            result = _run_tqqq_overlay_backtest(tqqq_prices, spec=spec, start_date=start, end_date=end)
            summary_rows.append(_summary_row("tqqq_growth_income_core", spec.candidate, period, result))
        event_rows.append(
            {
                "Strategy": "tqqq_growth_income_core",
                "Candidate": spec.candidate,
                "Kind": spec.kind,
                "Symbol": spec.symbol,
                "Window": spec.window,
                "Fast Window": spec.fast_window,
                "Slow Window": spec.slow_window,
                "Threshold": spec.threshold,
                "ATR Multiple": spec.atr_multiple,
                "Redirect Symbol": spec.redirect_symbol,
                "Long Overlay Stops": summary_rows[-1]["Overlay Stops"],
            }
        )

    for spec in SOXL_CANDIDATES:
        for period, (start, end) in SOXL_PERIODS.items():
            if spec.kind == "none":
                result = run_soxl_backtest(
                    soxl_prices,
                    start_date=start,
                    end_date=end,
                    disable_income_layer=True,
                    dynamic_rsi_quantile_window=252,
                    dynamic_rsi_quantile=0.90,
                    dynamic_rsi_floor=70.0,
                )
            else:
                result = run_soxl_backtest(
                    soxl_prices,
                    start_date=start,
                    end_date=end,
                    disable_income_layer=True,
                    dynamic_rsi_quantile_window=252,
                    dynamic_rsi_quantile=0.90,
                    dynamic_rsi_floor=70.0,
                    soxl_delever_overlay_kind=spec.kind,
                    soxl_delever_overlay_symbol=spec.symbol,
                    soxl_delever_overlay_window=spec.window,
                    soxl_delever_overlay_fast_window=spec.fast_window,
                    soxl_delever_overlay_slow_window=spec.slow_window,
                    soxl_delever_overlay_threshold=spec.threshold,
                    soxl_delever_overlay_atr_multiple=spec.atr_multiple,
                    soxl_delever_overlay_retention_ratio=spec.retention_ratio,
                    soxl_delever_overlay_redirect_symbol=spec.redirect_symbol,
                    soxl_delever_overlay_combine_with_core=True,
                )
            summary_rows.append(_summary_row("soxl_soxx_trend_income_core", spec.candidate, period, result))
        event_rows.append(
            {
                "Strategy": "soxl_soxx_trend_income_core",
                "Candidate": spec.candidate,
                "Kind": spec.kind,
                "Symbol": spec.symbol,
                "Window": spec.window,
                "Fast Window": spec.fast_window,
                "Slow Window": spec.slow_window,
                "Threshold": spec.threshold,
                "ATR Multiple": spec.atr_multiple,
                "Redirect Symbol": spec.redirect_symbol,
                "Long Overlay Stops": summary_rows[-1]["Overlay Stops"],
            }
        )

    summary = pd.DataFrame(summary_rows)
    screen = _build_screen(summary)
    events = pd.DataFrame(event_rows)
    summary.to_csv(OUTPUT_DIR / "period_summary.csv", index=False)
    screen.to_csv(OUTPUT_DIR / "strict_no_regression_screen.csv", index=False)
    events.to_csv(OUTPUT_DIR / "candidate_event_summary.csv", index=False)

    print(f"wrote {OUTPUT_DIR.relative_to(ROOT)}")
    print("\nSTRICT SCREEN")
    print(screen.to_string(index=False))
    print("\nLONG WINDOW")
    long_rows = summary.loc[summary["Period"].eq("long"), ["Strategy", "Candidate", "CAGR", "Max Drawdown", "Sharpe", "Calmar", "Overlay Stops"]]
    print(long_rows.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
