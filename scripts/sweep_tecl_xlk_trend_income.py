from __future__ import annotations

import argparse
import json
from itertools import product
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines.backtest_windows import build_benchmark_returns, build_window_summary
from us_equity_snapshot_pipelines.tecl_xlk_trend_income_backtest import run_backtest
from us_equity_snapshot_pipelines.tecl_xlk_trend_income_research_inputs import (
    DEFAULT_LONG_HISTORY_START,
    DEFAULT_SYNTHETIC_HISTORY_START,
    build_tecl_long_history_inputs,
    normalize_price_history,
    prepare_tecl_research_prices,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "data" / "output" / "tecl_xlk_trend_income_sweep_20260628"
DEFAULT_PRICES = ROOT / "data" / "output" / "tecl_xlk_trend_income_research_20260628" / "price_history.csv"

RESEARCH_WINDOWS = (
    ("full_2024", "2024-01-30", None),
    ("boxx_available", "2023-07-26", None),
    ("post_2022", "2023-01-03", None),
    ("recent_1y", "2025-06-01", None),
    ("recent_3m", "2026-03-01", None),
)

LONG_HISTORY_WINDOWS = (
    ("covid_2020", "2020-02-18", "2020-04-30"),
    ("rate_bear_2022", "2022-01-03", "2022-12-30"),
    ("post_2022", "2023-01-03", None),
    ("full_long", "2018-02-01", None),
)

SYNTHETIC_LONG_HISTORY_WINDOWS = (
    ("dotcom_2000_2002", "2000-03-24", "2002-10-09"),
    ("gfc_2007_2009", "2007-10-09", "2009-03-09"),
    ("covid_2020", "2020-02-18", "2020-04-30"),
    ("rate_bear_2022", "2022-01-03", "2022-12-30"),
    ("post_2022", "2023-01-03", None),
    ("full_synthetic", "2000-02-01", None),
)


def _load_prices(path: Path) -> pd.DataFrame:
    return normalize_price_history(pd.read_csv(path))


def _as_float(value: object, default: float = float("nan")) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _core_candidates() -> list[dict[str, object]]:
    candidates: list[dict[str, object]] = [
        {"variant": "manifest_default", "overrides": _baseline_overrides()},
        {"variant": "vol_off", "overrides": {"blend_gate_volatility_delever_enabled": False}},
    ]
    trend_windows = (100, 140, 180)
    buffer_sets = (
        (0.08, 0.06, 0.02),
        (0.10, 0.07, 0.03),
        (0.06, 0.04, 0.02),
    )
    tecl_weights = (0.65, 0.70, 0.75)
    redirects = ("XLK", "BOXX")
    percentiles = (0.90, 0.95)
    for trend_ma, buffers, tecl_weight, redirect, percentile in product(
        trend_windows,
        buffer_sets,
        tecl_weights,
        redirects,
        percentiles,
    ):
        entry, mid, exit_ = buffers
        label = (
            f"ma{trend_ma}_b{entry:.2f}_{mid:.2f}_{exit_:.2f}"
            f"_tw{tecl_weight:.2f}_{redirect.lower()}_p{int(percentile * 100)}"
        )
        candidates.append(
            {
                "variant": label,
                "overrides": {
                    "trend_ma_window": int(trend_ma),
                    "trend_entry_buffer": float(entry),
                    "trend_mid_buffer": float(mid),
                    "trend_exit_buffer": float(exit_),
                    "blend_gate_tecl_weight": float(tecl_weight),
                    "blend_gate_mid_tecl_weight": float(max(0.0, tecl_weight - 0.05)),
                    "blend_gate_volatility_delever_redirect_symbol": redirect,
                    "blend_gate_volatility_delever_dynamic_percentile": float(percentile),
                },
            }
        )
    return candidates


def _baseline_overrides() -> dict[str, object]:
    return {}


def _narrow_candidates() -> list[dict[str, object]]:
    """Lower TECL offensive weight with manifest-like vol delever (research round 2)."""
    candidates: list[dict[str, object]] = [
        {"variant": "manifest_default", "overrides": _baseline_overrides()},
        {"variant": "vol_off", "overrides": {"blend_gate_volatility_delever_enabled": False}},
    ]
    trend_windows = (100, 140)
    buffer_sets = (
        (0.08, 0.06, 0.02),
        (0.06, 0.04, 0.02),
    )
    tecl_weights = (0.55, 0.60, 0.65)
    for trend_ma, buffers, tecl_weight in product(trend_windows, buffer_sets, tecl_weights):
        entry, mid, exit_ = buffers
        label = f"ma{trend_ma}_b{entry:.2f}_{mid:.2f}_{exit_:.2f}_tw{tecl_weight:.2f}_xlk_p95"
        candidates.append(
            {
                "variant": label,
                "overrides": {
                    "trend_ma_window": int(trend_ma),
                    "trend_entry_buffer": float(entry),
                    "trend_mid_buffer": float(mid),
                    "trend_exit_buffer": float(exit_),
                    "blend_gate_tecl_weight": float(tecl_weight),
                    "blend_gate_mid_tecl_weight": float(max(0.0, tecl_weight - 0.05)),
                    "blend_gate_volatility_delever_redirect_symbol": "XLK",
                    "blend_gate_volatility_delever_dynamic_percentile": 0.95,
                },
            }
        )
    return candidates


def _candidate_set(mode: str) -> list[dict[str, object]]:
    if mode == "narrow":
        return _narrow_candidates()
    return _core_candidates()


def _slice_metrics(portfolio_returns: pd.Series, windows: tuple[tuple[str, str, str | None], ...]) -> dict[str, object]:
    returns = pd.Series(portfolio_returns).dropna()
    if returns.empty:
        return {}
    metrics: dict[str, object] = {}
    for name, start, end in windows:
        slice_returns = returns.loc[returns.index >= pd.Timestamp(start)]
        if end:
            slice_returns = slice_returns.loc[slice_returns.index <= pd.Timestamp(end)]
        if len(slice_returns) < 2:
            metrics[f"{name}_cagr"] = float("nan")
            metrics[f"{name}_max_drawdown"] = float("nan")
            continue
        equity = (1.0 + slice_returns).cumprod()
        years = max((slice_returns.index[-1] - slice_returns.index[0]).days / 365.25, 1 / 365.25)
        drawdown = equity / equity.cummax() - 1.0
        metrics[f"{name}_cagr"] = float(equity.iloc[-1] ** (1.0 / years) - 1.0)
        metrics[f"{name}_max_drawdown"] = float(drawdown.min())
    return metrics


def _window_gate_metrics(portfolio_returns: pd.Series, prices: pd.DataFrame) -> dict[str, object]:
    benchmark_returns = build_benchmark_returns(prices, symbols=("XLK", "QQQ", "SPY"))
    primary = "XLK" if "XLK" in benchmark_returns else ("QQQ" if "QQQ" in benchmark_returns else "SPY")
    if primary not in benchmark_returns:
        return {
            "passes_spy_windows": False,
            "worst_spy_drawdown_margin": float("nan"),
            "failed_spy_windows": "",
            "window_count": 0,
            "primary_benchmark": "",
        }
    window_summary = build_window_summary(
        portfolio_returns,
        benchmark_returns=benchmark_returns,
        primary_benchmark_symbol=primary,
    )
    if window_summary.empty or "Within Primary Benchmark Drawdown" not in window_summary.columns:
        return {
            "passes_spy_windows": False,
            "worst_spy_drawdown_margin": float("nan"),
            "failed_spy_windows": "",
            "window_count": 0,
            "primary_benchmark": primary,
        }
    comparable = window_summary.loc[window_summary["Primary Benchmark"].astype(str).eq(primary)].copy()
    if comparable.empty:
        return {
            "passes_spy_windows": False,
            "worst_spy_drawdown_margin": float("nan"),
            "failed_spy_windows": "",
            "window_count": 0,
            "primary_benchmark": primary,
        }
    within = comparable["Within Primary Benchmark Drawdown"].astype(bool)
    failed = comparable.loc[~within, "Window"].astype(str).tolist()
    margin = pd.to_numeric(comparable["Drawdown Difference vs Primary Benchmark"], errors="coerce")
    return {
        "passes_spy_windows": bool(within.all()),
        "worst_spy_drawdown_margin": float(margin.min()),
        "failed_spy_windows": ",".join(failed),
        "window_count": int(len(comparable)),
        "primary_benchmark": primary,
    }


def _result_row(
    *,
    result: dict[str, object],
    prices: pd.DataFrame,
    variant: str,
    overrides: dict[str, object],
    windows: tuple[tuple[str, str, str | None], ...],
) -> dict[str, object]:
    summary = dict(result["summary"])
    row: dict[str, object] = {
        "variant": variant,
        "start": summary.get("Start"),
        "end": summary.get("End"),
        "cagr": _as_float(summary.get("CAGR")),
        "max_drawdown": _as_float(summary.get("Max Drawdown")),
        "volatility": _as_float(summary.get("Volatility")),
        "sharpe": _as_float(summary.get("Sharpe")),
        "calmar": _as_float(summary.get("Calmar")),
        "total_return": _as_float(summary.get("Total Return")),
        "final_equity": _as_float(summary.get("Final Equity")),
        "tecl_delever_stops": _as_float(summary.get("TECL Delever Stops"), 0.0),
        "turnover_per_year": _as_float(summary.get("Turnover/Year")),
        "overrides_json": json.dumps(overrides, sort_keys=True),
    }
    row.update(_slice_metrics(result["portfolio_returns"], windows))
    row.update(_window_gate_metrics(result["portfolio_returns"], prices))
    return row


def _rank_candidates(frame: pd.DataFrame, *, baseline_cagr: float, baseline_mdd: float) -> pd.DataFrame:
    if frame.empty:
        return frame
    ranked = frame.copy()
    ranked["beats_baseline_cagr"] = ranked["cagr"].ge(float(baseline_cagr))
    ranked["beats_baseline_mdd"] = ranked["max_drawdown"].ge(float(baseline_mdd))
    ranked["passes_dual_gate"] = ranked["beats_baseline_cagr"] & ranked["beats_baseline_mdd"]
    ranked = ranked.sort_values(
        ["passes_dual_gate", "cagr", "max_drawdown", "calmar"],
        ascending=[False, False, False, False],
    )
    return ranked


def run_sweep(
    *,
    prices: pd.DataFrame,
    output_dir: Path,
    start_date: str,
    windows: tuple[tuple[str, str, str | None], ...],
    mode: str = "full",
) -> pd.DataFrame:
    output_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, object]] = []
    candidates = _candidate_set(mode)
    baseline = run_backtest(
        prices,
        start_date=start_date,
        disable_income_layer=True,
        strategy_overrides=_baseline_overrides(),
    )
    baseline_summary = dict(baseline["summary"])
    baseline_cagr = _as_float(baseline_summary.get("CAGR"))
    baseline_mdd = _as_float(baseline_summary.get("Max Drawdown"))
    rows.append(
        _result_row(
            result=baseline,
            prices=prices,
            variant="manifest_default",
            overrides=_baseline_overrides(),
            windows=windows,
        )
    )
    for index, candidate in enumerate(candidates[1:], start=2):
        if index == 2 or index % 25 == 0 or index == len(candidates):
            print(f"[tecl-sweep] {index}/{len(candidates)} {candidate['variant']}", flush=True)
        result = run_backtest(
            prices,
            start_date=start_date,
            disable_income_layer=True,
            strategy_overrides=dict(candidate["overrides"]),
        )
        rows.append(
            _result_row(
                result=result,
                prices=prices,
                variant=str(candidate["variant"]),
                overrides=dict(candidate["overrides"]),
                windows=windows,
            )
        )
    frame = pd.DataFrame(rows)
    frame.to_csv(output_dir / "tecl_core_sweep.csv", index=False)
    ranked = _rank_candidates(frame, baseline_cagr=baseline_cagr, baseline_mdd=baseline_mdd)
    ranked.to_csv(output_dir / "tecl_core_sweep_ranked.csv", index=False)
    summary = {
        "mode": mode,
        "candidate_count": len(candidates),
        "baseline_cagr": baseline_cagr,
        "baseline_max_drawdown": baseline_mdd,
        "dual_gate_pass_count": int(ranked["passes_dual_gate"].sum()) if not ranked.empty else 0,
        "best_variant": str(ranked.iloc[0]["variant"]) if not ranked.empty else "",
    }
    (output_dir / "tecl_core_sweep_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return ranked


def main() -> int:
    parser = argparse.ArgumentParser(description="Bounded TECL/XLK core-parameter sweep.")
    parser.add_argument("--prices", type=Path, default=DEFAULT_PRICES)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--start", default="2024-01-30")
    parser.add_argument("--mode", choices=("full", "narrow"), default="full")
    parser.add_argument("--synthetic-start", default=DEFAULT_SYNTHETIC_HISTORY_START)
    parser.add_argument("--download-long-history", action="store_true")
    parser.add_argument("--long-history-start", default=DEFAULT_LONG_HISTORY_START)
    parser.add_argument("--synthesize-tecl-from-xlk", action="store_true")
    parser.add_argument("--boxx-proxy-symbol", default="BIL")
    args = parser.parse_args()

    synthetic = bool(args.synthesize_tecl_from_xlk)
    if args.download_long_history:
        history_start = str(args.synthetic_start if synthetic else args.long_history_start)
        prices, metadata = build_tecl_long_history_inputs(
            start=history_start,
            synthesize_tecl_from_xlk=synthetic,
            boxx_proxy_symbol=str(args.boxx_proxy_symbol or "BIL"),
        )
        args.output_dir.mkdir(parents=True, exist_ok=True)
        prices.to_csv(args.output_dir / "price_history.csv", index=False)
        (args.output_dir / "inputs_manifest.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        windows = SYNTHETIC_LONG_HISTORY_WINDOWS if synthetic else LONG_HISTORY_WINDOWS
        start_date = "2000-02-01" if synthetic else "2018-02-01"
    else:
        if not args.prices.exists():
            raise FileNotFoundError(args.prices)
        prices = _load_prices(args.prices)
        prices, metadata = prepare_tecl_research_prices(
            prices,
            synthesize_tecl_from_xlk=False,
            boxx_proxy_symbol=str(args.boxx_proxy_symbol or "BIL"),
        )
        windows = RESEARCH_WINDOWS
        start_date = str(args.start)

    ranked = run_sweep(
        prices=prices,
        output_dir=args.output_dir,
        start_date=start_date,
        windows=windows,
        mode=str(args.mode),
    )
    if not ranked.empty:
        print(ranked.head(10).to_string(index=False))
    print(f"wrote sweep outputs -> {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
