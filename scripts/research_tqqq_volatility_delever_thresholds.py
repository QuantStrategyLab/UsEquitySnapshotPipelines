from __future__ import annotations

import argparse
from pathlib import Path
from typing import Mapping

import pandas as pd

from us_equity_snapshot_pipelines.backtest_windows import build_benchmark_returns, build_window_summary
from us_equity_snapshot_pipelines.soxl_soxx_trend_income_backtest import _build_price_frame
from us_equity_snapshot_pipelines.tqqq_growth_income_archive import (
    DEFAULT_BACKTEST_START,
    DEFAULT_INITIAL_EQUITY_USD,
    DEFAULT_TURNOVER_COST_BPS,
    run_backtest,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PRICES = ROOT / "data" / "output" / "codex_tqqq_9sig_recheck_20260603" / "price_history.csv"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "output" / "tqqq_volatility_delever_threshold_research"


def _normalize_prices(path: Path) -> pd.DataFrame:
    prices = _build_price_frame(pd.read_csv(path))
    symbols = set(prices["symbol"].unique())
    additions = []
    if "QQQM" not in symbols and "QQQ" in symbols:
        additions.append(prices.loc[prices["symbol"].eq("QQQ")].assign(symbol="QQQM"))
    if "BOXX" not in symbols and "BIL" in symbols:
        additions.append(prices.loc[prices["symbol"].eq("BIL")].assign(symbol="BOXX"))
    if additions:
        prices = pd.concat([prices, *additions], ignore_index=True)
    return _build_price_frame(prices)


def _base_overrides() -> dict[str, object]:
    return {
        "income_threshold_usd": 1e18,
        "income_layer_enabled": False,
        "income_layer_start_usd": 1e18,
        "income_layer_max_ratio": 0.0,
        "dual_drive_unlevered_symbol": "QQQM",
        "market_regime_control_enabled": False,
        "dual_drive_macro_risk_governor_enabled": False,
        "dual_drive_crisis_defense_enabled": False,
    }


def _variants() -> tuple[tuple[str, dict[str, object]], ...]:
    base = _base_overrides()
    return (
        (
            "dynamic_p90_floor24_cap36",
            {
                **base,
                "dual_drive_volatility_delever_enabled": True,
                "dual_drive_volatility_delever_threshold_mode": "rolling_percentile",
                "dual_drive_volatility_delever_threshold": 0.28,
                "dual_drive_volatility_delever_exit_threshold": 0.28,
                "dual_drive_volatility_delever_dynamic_lookback": 252,
                "dual_drive_volatility_delever_dynamic_percentile": 0.90,
                "dual_drive_volatility_delever_dynamic_min_periods": 126,
                "dual_drive_volatility_delever_dynamic_floor": 0.24,
                "dual_drive_volatility_delever_dynamic_cap": 0.36,
            },
        ),
        (
            "fixed_28",
            {
                **base,
                "dual_drive_volatility_delever_enabled": True,
                "dual_drive_volatility_delever_threshold_mode": "fixed",
                "dual_drive_volatility_delever_threshold": 0.28,
                "dual_drive_volatility_delever_exit_threshold": 0.28,
            },
        ),
        (
            "fixed_32",
            {
                **base,
                "dual_drive_volatility_delever_enabled": True,
                "dual_drive_volatility_delever_threshold_mode": "fixed",
                "dual_drive_volatility_delever_threshold": 0.32,
                "dual_drive_volatility_delever_exit_threshold": 0.32,
            },
        ),
        (
            "no_vol_delever",
            {
                **base,
                "dual_drive_volatility_delever_enabled": False,
            },
        ),
        *(
            (
                f"dynamic_p{int(percentile * 100)}",
                {
                    **base,
                    "dual_drive_volatility_delever_enabled": True,
                    "dual_drive_volatility_delever_threshold_mode": "rolling_percentile",
                    "dual_drive_volatility_delever_threshold": 0.28,
                    "dual_drive_volatility_delever_exit_threshold": 0.28,
                    "dual_drive_volatility_delever_dynamic_lookback": 252,
                    "dual_drive_volatility_delever_dynamic_percentile": percentile,
                    "dual_drive_volatility_delever_dynamic_min_periods": 126,
                    "dual_drive_volatility_delever_dynamic_floor": 0.0,
                    "dual_drive_volatility_delever_dynamic_cap": 0.50,
                },
            )
            for percentile in (0.80, 0.85, 0.90, 0.95)
        ),
    )


def _variant_row(name: str, result: Mapping[str, object]) -> dict[str, object]:
    summary = dict(result["summary"])
    signal_history = pd.DataFrame(result["signal_history"])
    applied = signal_history.get("dual_drive_volatility_delever_applied", pd.Series(dtype=bool)).fillna(False).astype(bool)
    triggered = signal_history.get("dual_drive_volatility_delever_triggered", pd.Series(dtype=bool)).fillna(False).astype(bool)
    return {
        "Variant": name,
        **summary,
        "Vol Delever Trigger Days": int(triggered.sum()),
        "Vol Delever Applied Days": int(applied.sum()),
        "Median Effective Threshold": float(
            pd.to_numeric(signal_history.get("dual_drive_volatility_delever_threshold"), errors="coerce").median()
        ),
        "Min Effective Threshold": float(
            pd.to_numeric(signal_history.get("dual_drive_volatility_delever_threshold"), errors="coerce").min()
        ),
        "Max Effective Threshold": float(
            pd.to_numeric(signal_history.get("dual_drive_volatility_delever_threshold"), errors="coerce").max()
        ),
    }


def run_research(
    *,
    prices_path: Path,
    output_dir: Path,
    start_date: str,
    end_date: str | None,
    initial_equity: float,
    turnover_cost_bps: float,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    prices = _normalize_prices(prices_path)
    prices.to_csv(output_dir / "normalized_price_history.csv", index=False)
    summary_rows = []
    window_frames = []
    benchmark_returns = build_benchmark_returns(prices)

    for name, overrides in _variants():
        result = run_backtest(
            prices,
            initial_equity=float(initial_equity),
            start_date=start_date,
            end_date=end_date,
            turnover_cost_bps=float(turnover_cost_bps),
            strategy_overrides=overrides,
        )
        summary_rows.append(_variant_row(name, result))
        window_summary = build_window_summary(result["portfolio_returns"], benchmark_returns=benchmark_returns)
        window_summary.insert(0, "Variant", name)
        window_frames.append(window_summary)
        result["signal_history"].to_csv(output_dir / f"{name}_signal_history.csv", index=False)
        result["turnover_history"].rename("turnover").to_csv(output_dir / f"{name}_turnover_history.csv")

    pd.DataFrame(summary_rows).to_csv(output_dir / "variant_summary.csv", index=False)
    pd.concat(window_frames, ignore_index=True).to_csv(output_dir / "variant_window_summary.csv", index=False)
    return output_dir


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research TQQQ volatility-delever threshold variants.")
    parser.add_argument("--prices", default=str(DEFAULT_PRICES))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--start-date", default=DEFAULT_BACKTEST_START)
    parser.add_argument("--end-date")
    parser.add_argument("--initial-equity", type=float, default=DEFAULT_INITIAL_EQUITY_USD)
    parser.add_argument("--turnover-cost-bps", type=float, default=DEFAULT_TURNOVER_COST_BPS)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = run_research(
        prices_path=Path(args.prices),
        output_dir=Path(args.output_dir),
        start_date=args.start_date,
        end_date=args.end_date,
        initial_equity=float(args.initial_equity),
        turnover_cost_bps=float(args.turnover_cost_bps),
    )
    print(f"wrote TQQQ volatility-delever threshold research -> {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
