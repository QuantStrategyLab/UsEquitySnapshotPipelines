from __future__ import annotations

import argparse
from pathlib import Path
from typing import Mapping

import pandas as pd

from us_equity_snapshot_pipelines.backtest_windows import build_benchmark_returns, build_window_summary
from us_equity_snapshot_pipelines.artifacts import write_json
from us_equity_snapshot_pipelines.pipelines.soxl_soxx_trend_income_backtest import (
    DEFAULT_INITIAL_EQUITY_USD,
    DEFAULT_TURNOVER_COST_BPS,
    _build_price_frame,
    run_backtest,
)
from us_equity_snapshot_pipelines.yfinance_prices import (
    download_price_history_with_proxy_candidates,
    load_proxy_candidates,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PRICES = ROOT / "data" / "output" / "codex_soxl_rsi_recheck_20260603" / "price_history.csv"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "output" / "soxl_dynamic_volatility_delever_threshold_research"
DEFAULT_BACKTEST_START = "2016-06-06"
DEFAULT_DOWNLOAD_SYMBOLS = ("SOXL", "SOXX", "BIL")


def _normalize_prices(path: Path, *, allow_constant_cash_proxy: bool = False) -> pd.DataFrame:
    prices = _build_price_frame(pd.read_csv(path))
    symbols = set(prices["symbol"].unique())
    additions = []
    if "BOXX" not in symbols and "BIL" in symbols:
        additions.append(prices.loc[prices["symbol"].eq("BIL")].assign(symbol="BOXX"))
    elif "BOXX" not in symbols and allow_constant_cash_proxy:
        calendar = prices.loc[prices["symbol"].eq("SOXX"), ["as_of"]].drop_duplicates()
        if calendar.empty:
            raise ValueError("constant cash proxy requires SOXX price dates")
        additions.append(calendar.assign(symbol="BOXX", close=100.0))
    elif "BOXX" not in symbols:
        raise ValueError("price history must include BOXX or BIL; use --constant-cash-proxy only for relative research")
    if additions:
        prices = pd.concat([prices, *additions], ignore_index=True)
    return _build_price_frame(prices)


def _external_vol_overlay(
    *,
    threshold: float,
    threshold_mode: str = "fixed",
    percentile: float | None = None,
    floor: float | None = None,
    cap: float | None = None,
    lookback: int = 252,
    min_periods: int = 126,
    reentry_hysteresis: float = 0.0,
    reentry_cooldown_days: int = 0,
) -> dict[str, object]:
    return {
        "soxl_delever_overlay_kind": "volatility",
        "soxl_delever_overlay_symbol": "SOXX",
        "soxl_delever_overlay_window": 10,
        "soxl_delever_overlay_threshold": float(threshold),
        "soxl_delever_overlay_threshold_mode": threshold_mode,
        "soxl_delever_overlay_threshold_lookback": int(lookback),
        "soxl_delever_overlay_threshold_percentile": percentile,
        "soxl_delever_overlay_threshold_min_periods": int(min_periods),
        "soxl_delever_overlay_threshold_floor": floor,
        "soxl_delever_overlay_threshold_cap": cap,
        "soxl_delever_overlay_reentry_hysteresis": float(reentry_hysteresis),
        "soxl_delever_overlay_reentry_cooldown_days": int(reentry_cooldown_days),
        "soxl_delever_overlay_retention_ratio": 0.0,
        "soxl_delever_overlay_redirect_symbol": "SOXX",
    }


def _risk_budget_overrides(
    *,
    full_soxl_weight: float,
    mid_soxl_weight: float,
) -> dict[str, object]:
    """Return a research-only cap on the leveraged SOXL sleeve.

    The removed allocation remains in BOXX through the strategy's normal tier
    allocator.  It is deliberately not reallocated to SOXX: this is a genuine
    reduction of the portfolio's semiconductor beta and leverage budget.
    """

    return {
        "strategy_overrides": {
            "blend_gate_soxl_weight": float(full_soxl_weight),
            "blend_gate_mid_soxl_weight": float(mid_soxl_weight),
        }
    }


def _with_risk_budget(
    variant: Mapping[str, object],
    *,
    full_soxl_weight: float,
    mid_soxl_weight: float,
) -> dict[str, object]:
    """Add a sleeve cap without mutating the base external-overlay variant."""

    overrides = dict(variant.get("strategy_overrides", {}))
    overrides.update(
        {
            "blend_gate_soxl_weight": float(full_soxl_weight),
            "blend_gate_mid_soxl_weight": float(mid_soxl_weight),
        }
    )
    return {**variant, "strategy_overrides": overrides}


def _variants() -> tuple[tuple[str, dict[str, object]], ...]:
    core_fixed55_overrides = {
        "strategy_overrides": {
            "blend_gate_volatility_delever_threshold": 0.55,
            "blend_gate_volatility_delever_threshold_mode": "fixed",
        }
    }
    hysteresis_candidate = _external_vol_overlay(
        threshold=0.55,
        threshold_mode="rolling_percentile",
        percentile=0.95,
        floor=0.50,
        cap=0.75,
        reentry_hysteresis=0.075,
        reentry_cooldown_days=2,
    )
    return (
        ("current_core_dynamic_p95", {}),
        ("core_cap60_55", _risk_budget_overrides(full_soxl_weight=0.60, mid_soxl_weight=0.55)),
        ("core_cap50_45", _risk_budget_overrides(full_soxl_weight=0.50, mid_soxl_weight=0.45)),
        ("core_cap40_35", _risk_budget_overrides(full_soxl_weight=0.40, mid_soxl_weight=0.35)),
        ("current_core_fixed55", core_fixed55_overrides),
        ("overlay_fixed55_replay", _external_vol_overlay(threshold=0.55)),
        (
            "no_vol_delever",
            {"strategy_overrides": {"blend_gate_volatility_delever_enabled": False}},
        ),
        ("fixed50", _external_vol_overlay(threshold=0.50)),
        ("fixed60", _external_vol_overlay(threshold=0.60)),
        (
            "dynamic_p80_floor45_cap70",
            _external_vol_overlay(
                threshold=0.55,
                threshold_mode="rolling_percentile",
                percentile=0.80,
                floor=0.45,
                cap=0.70,
            ),
        ),
        (
            "dynamic_p85_floor45_cap70",
            _external_vol_overlay(
                threshold=0.55,
                threshold_mode="rolling_percentile",
                percentile=0.85,
                floor=0.45,
                cap=0.70,
            ),
        ),
        (
            "dynamic_p90_floor45_cap70",
            _external_vol_overlay(
                threshold=0.55,
                threshold_mode="rolling_percentile",
                percentile=0.90,
                floor=0.45,
                cap=0.70,
            ),
        ),
        (
            "dynamic_p90_floor50_cap70",
            _external_vol_overlay(
                threshold=0.55,
                threshold_mode="rolling_percentile",
                percentile=0.90,
                floor=0.50,
                cap=0.70,
            ),
        ),
        (
            "dynamic_p90_floor55_cap75",
            _external_vol_overlay(
                threshold=0.55,
                threshold_mode="rolling_percentile",
                percentile=0.90,
                floor=0.55,
                cap=0.75,
            ),
        ),
        (
            "dynamic_p95_floor45_cap75",
            _external_vol_overlay(
                threshold=0.55,
                threshold_mode="rolling_percentile",
                percentile=0.95,
                floor=0.45,
                cap=0.75,
            ),
        ),
        (
            "dynamic_p95_floor45_cap70",
            _external_vol_overlay(
                threshold=0.55,
                threshold_mode="rolling_percentile",
                percentile=0.95,
                floor=0.45,
                cap=0.70,
            ),
        ),
        (
            "dynamic_p95_floor50_cap75_replay",
            _external_vol_overlay(
                threshold=0.55,
                threshold_mode="rolling_percentile",
                percentile=0.95,
                floor=0.50,
                cap=0.75,
            ),
        ),
        (
            "dynamic_p95_hysteresis5pp_cooldown1d",
            _external_vol_overlay(
                threshold=0.55,
                threshold_mode="rolling_percentile",
                percentile=0.95,
                floor=0.50,
                cap=0.75,
                reentry_hysteresis=0.05,
                reentry_cooldown_days=1,
            ),
        ),
        (
            "dynamic_p95_hysteresis7_5pp_cooldown2d",
            hysteresis_candidate,
        ),
        (
            "risk_budget60_55_p95_hysteresis7_5pp_cooldown2d",
            _with_risk_budget(hysteresis_candidate, full_soxl_weight=0.60, mid_soxl_weight=0.55),
        ),
        (
            "risk_budget50_45_p95_hysteresis7_5pp_cooldown2d",
            _with_risk_budget(hysteresis_candidate, full_soxl_weight=0.50, mid_soxl_weight=0.45),
        ),
        (
            "risk_budget40_35_p95_hysteresis7_5pp_cooldown2d",
            _with_risk_budget(hysteresis_candidate, full_soxl_weight=0.40, mid_soxl_weight=0.35),
        ),
        (
            "dynamic_p95_floor55_cap75",
            _external_vol_overlay(
                threshold=0.55,
                threshold_mode="rolling_percentile",
                percentile=0.95,
                floor=0.55,
                cap=0.75,
            ),
        ),
        (
            "dynamic_p95_cap75",
            _external_vol_overlay(
                threshold=0.55,
                threshold_mode="rolling_percentile",
                percentile=0.95,
                floor=None,
                cap=0.75,
            ),
        ),
        (
            "dynamic_p90_cap75",
            _external_vol_overlay(
                threshold=0.55,
                threshold_mode="rolling_percentile",
                percentile=0.90,
                floor=None,
                cap=0.75,
            ),
        ),
    )


def _first_existing_series(frame: pd.DataFrame, *columns: str) -> pd.Series:
    for column in columns:
        if column in frame.columns:
            series = pd.to_numeric(frame[column], errors="coerce")
            if not series.dropna().empty:
                return series
    return pd.Series(dtype=float)


def _variant_row(
    name: str,
    result: Mapping[str, object],
    *,
    strategy_overrides: Mapping[str, object] | None = None,
) -> dict[str, object]:
    summary = dict(result["summary"])
    signal_history = pd.DataFrame(result["signal_history"])
    core_triggered = (
        signal_history.get("blend_gate_volatility_delever_triggered", pd.Series(dtype=bool)).fillna(False).astype(bool)
    )
    overlay_triggered = (
        signal_history.get("soxl_delever_overlay_triggered", pd.Series(dtype=bool)).fillna(False).astype(bool)
    )
    raw_overlay_triggered = (
        signal_history.get("soxl_delever_overlay_raw_triggered", pd.Series(dtype=bool)).fillna(False).astype(bool)
    )
    threshold = _first_existing_series(
        signal_history,
        "soxl_delever_overlay_threshold",
        "blend_gate_volatility_delever_threshold",
    )
    dynamic_threshold = _first_existing_series(
        signal_history,
        "soxl_delever_overlay_dynamic_threshold",
        "blend_gate_volatility_delever_dynamic_threshold",
    )
    dynamic_sample_count = _first_existing_series(
        signal_history,
        "soxl_delever_overlay_dynamic_sample_count",
        "blend_gate_volatility_delever_dynamic_sample_count",
    )
    threshold_mode = ""
    if "soxl_delever_overlay_threshold_mode" in signal_history.columns:
        modes = tuple(
            str(item) for item in signal_history["soxl_delever_overlay_threshold_mode"].dropna().unique() if str(item)
        )
        threshold_mode = ",".join(modes)
    if not threshold_mode and "blend_gate_volatility_delever_threshold_mode" in signal_history.columns:
        modes = tuple(
            str(item)
            for item in signal_history["blend_gate_volatility_delever_threshold_mode"].dropna().unique()
            if str(item)
        )
        threshold_mode = ",".join(modes)
    if not threshold_mode:
        threshold_mode = "fixed_core"
    configured_weights = dict(strategy_overrides or {})
    return {
        "Variant": name,
        **summary,
        "Full SOXL Sleeve Cap": float(configured_weights.get("blend_gate_soxl_weight", 0.70)),
        "Mid SOXL Sleeve Cap": float(configured_weights.get("blend_gate_mid_soxl_weight", 0.65)),
        "Core Vol Trigger Days": int(core_triggered.sum()),
        "Overlay Vol Trigger Days": int(overlay_triggered.sum()),
        "Raw Overlay Vol Trigger Days": int(raw_overlay_triggered.sum()),
        "Stateful Overlay Hold Days": int((overlay_triggered & ~raw_overlay_triggered).sum()),
        "Total Vol Delever Days": int(core_triggered.sum() + overlay_triggered.sum()),
        "Threshold Mode": threshold_mode,
        "Median Effective Threshold": float(threshold.median()) if not threshold.dropna().empty else float("nan"),
        "Min Effective Threshold": float(threshold.min()) if not threshold.dropna().empty else float("nan"),
        "Max Effective Threshold": float(threshold.max()) if not threshold.dropna().empty else float("nan"),
        "Median Dynamic Threshold": float(dynamic_threshold.median())
        if not dynamic_threshold.dropna().empty
        else float("nan"),
        "Median Dynamic Sample Count": float(dynamic_sample_count.median())
        if not dynamic_sample_count.dropna().empty
        else float("nan"),
        "Re-entry Hysteresis": float(
            _first_existing_series(signal_history, "soxl_delever_overlay_reentry_hysteresis").dropna().iloc[0]
        )
        if not _first_existing_series(signal_history, "soxl_delever_overlay_reentry_hysteresis").dropna().empty
        else 0.0,
        "Re-entry Cooldown Days": int(
            _first_existing_series(signal_history, "soxl_delever_overlay_reentry_cooldown_days").dropna().iloc[0]
        )
        if not _first_existing_series(signal_history, "soxl_delever_overlay_reentry_cooldown_days").dropna().empty
        else 0,
    }


def run_research(
    *,
    prices_path: Path,
    output_dir: Path,
    start_date: str,
    end_date: str | None,
    initial_equity: float,
    turnover_cost_bps: float,
    allow_constant_cash_proxy: bool = False,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    prices = _normalize_prices(prices_path, allow_constant_cash_proxy=allow_constant_cash_proxy)
    prices.to_csv(output_dir / "normalized_price_history.csv", index=False)
    write_json(
        output_dir / "research_metadata.json",
        {
            "source_price_history": str(prices_path),
            "cash_proxy": "constant_0pct" if allow_constant_cash_proxy else "source_boxx_or_bil",
            "research_only": True,
            "strategy_parameters_changed": False,
        },
    )
    benchmark_returns = build_benchmark_returns(prices, symbols=("SOXX", "SOXL"))
    summary_rows = []
    window_frames = []

    for name, kwargs in _variants():
        result = run_backtest(
            prices,
            initial_equity=float(initial_equity),
            start_date=start_date,
            end_date=end_date,
            turnover_cost_bps=float(turnover_cost_bps),
            disable_income_layer=True,
            **kwargs,
        )
        summary_rows.append(
            _variant_row(
                name,
                result,
                strategy_overrides=kwargs.get("strategy_overrides"),
            )
        )
        window_summary = build_window_summary(
            result["portfolio_returns"],
            benchmark_returns=benchmark_returns,
            primary_benchmark_symbol="SOXX",
        )
        window_summary.insert(0, "Variant", name)
        window_frames.append(window_summary)
        result["signal_history"].to_csv(output_dir / f"{name}_signal_history.csv", index=False)
        result["turnover_history"].rename("turnover").to_csv(output_dir / f"{name}_turnover_history.csv")

    pd.DataFrame(summary_rows).to_csv(output_dir / "variant_summary.csv", index=False)
    pd.concat(window_frames, ignore_index=True).to_csv(output_dir / "variant_window_summary.csv", index=False)
    return output_dir


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research SOXL dynamic volatility-delever threshold variants.")
    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument("--prices", default=str(DEFAULT_PRICES))
    input_group.add_argument("--download", action="store_true", help="Download the compact SOXL/SOXX/BIL history")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--price-start", default="2010-03-11")
    parser.add_argument("--price-end")
    parser.add_argument("--proxy", help="Proxy URL for the Yahoo Finance download")
    parser.add_argument("--proxy-list", help="Path or URL with one HTTP(S) proxy per line")
    parser.add_argument("--proxy-list-max", type=int, default=12, help="Maximum proxy candidates to try")
    parser.add_argument(
        "--constant-cash-proxy",
        action="store_true",
        help="Use a 0% return BOXX proxy when the source lacks both BOXX and BIL; relative research only",
    )
    parser.add_argument("--start-date", default=DEFAULT_BACKTEST_START)
    parser.add_argument("--end-date")
    parser.add_argument("--initial-equity", type=float, default=DEFAULT_INITIAL_EQUITY_USD)
    parser.add_argument("--turnover-cost-bps", type=float, default=DEFAULT_TURNOVER_COST_BPS)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    prices_path = Path(args.prices)
    if args.download:
        output_dir.mkdir(parents=True, exist_ok=True)
        downloaded_prices = download_price_history_with_proxy_candidates(
            DEFAULT_DOWNLOAD_SYMBOLS,
            start=args.price_start,
            end=args.price_end,
            chunk_size=len(DEFAULT_DOWNLOAD_SYMBOLS),
            proxy=args.proxy,
            proxy_candidates=load_proxy_candidates(args.proxy_list, max_candidates=args.proxy_list_max)
            if args.proxy_list
            else None,
        )
        prices_path = output_dir / "downloaded_price_history.csv"
        downloaded_prices.to_csv(prices_path, index=False)
    output_dir = run_research(
        prices_path=prices_path,
        output_dir=output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        initial_equity=float(args.initial_equity),
        turnover_cost_bps=float(args.turnover_cost_bps),
        allow_constant_cash_proxy=bool(args.constant_cash_proxy),
    )
    print(f"wrote SOXL dynamic volatility-delever threshold research -> {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
