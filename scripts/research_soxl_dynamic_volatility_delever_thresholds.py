from __future__ import annotations

import argparse
from collections.abc import Mapping
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines.artifacts import write_json
from us_equity_snapshot_pipelines.backtest_windows import (
    BacktestWindow,
    build_benchmark_returns,
    build_window_summary,
)
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
DEFAULT_ANNUAL_STABILITY_BASELINE = "current_core_dynamic_p95"
DEFAULT_ANNUAL_STABILITY_CANDIDATE = "risk_budget60_55_p95_hysteresis7_5pp_cooldown2d"
DEFAULT_ANNUAL_STABILITY_START_YEAR = 2016
MIN_COMPLETE_YEAR_OBSERVATIONS = 200


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


def _complete_calendar_year_windows(
    returns: pd.Series,
    *,
    start_year: int,
    end_year: int | None,
) -> tuple[BacktestWindow, ...]:
    """Return only complete, sufficiently populated annual comparison windows."""

    normalized = pd.to_numeric(pd.Series(returns), errors="coerce").dropna().copy()
    normalized.index = pd.to_datetime(normalized.index, errors="coerce").tz_localize(None).normalize()
    normalized = normalized.loc[normalized.index.notna()].sort_index()
    if normalized.empty:
        return ()

    last_observation = pd.Timestamp(normalized.index[-1]).normalize()
    last_complete_year = last_observation.year if last_observation >= pd.Timestamp(last_observation.year, 12, 31) else last_observation.year - 1
    selected_end_year = min(int(end_year), last_complete_year) if end_year is not None else last_complete_year
    windows = []
    for year in range(int(start_year), selected_end_year + 1):
        start = pd.Timestamp(year, 1, 1)
        end = pd.Timestamp(year, 12, 31)
        if int(normalized.loc[start:end].size) < MIN_COMPLETE_YEAR_OBSERVATIONS:
            continue
        windows.append(BacktestWindow(f"calendar_{year}", start, end, "fixed-spec annual stability window"))
    return tuple(windows)


def build_fixed_spec_annual_stability(
    results_by_variant: Mapping[str, Mapping[str, object]],
    *,
    baseline_variant: str = DEFAULT_ANNUAL_STABILITY_BASELINE,
    candidate_variant: str = DEFAULT_ANNUAL_STABILITY_CANDIDATE,
    start_year: int = DEFAULT_ANNUAL_STABILITY_START_YEAR,
    end_year: int | None = None,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Compare a pre-named candidate to baseline on complete annual windows.

    This is a stability diagnostic, not a promotion-grade OOS claim: the named
    candidate was specified after historical data already existed.  A true OOS
    begins only after its immutable configuration and data contract are frozen.
    """

    if baseline_variant not in results_by_variant or candidate_variant not in results_by_variant:
        raise ValueError("annual stability variants must be present in the research results")

    baseline_returns = pd.Series(results_by_variant[baseline_variant]["portfolio_returns"])
    candidate_returns = pd.Series(results_by_variant[candidate_variant]["portfolio_returns"])
    windows = _complete_calendar_year_windows(
        baseline_returns,
        start_year=int(start_year),
        end_year=end_year,
    )
    baseline_summary = build_window_summary(baseline_returns, windows=windows).set_index("Window")
    candidate_summary = build_window_summary(candidate_returns, windows=windows).set_index("Window")
    shared_windows = baseline_summary.index.intersection(candidate_summary.index)
    if shared_windows.empty:
        return pd.DataFrame(), {
            "status": "research_only_insufficient_complete_years",
            "baseline_variant": baseline_variant,
            "candidate_variant": candidate_variant,
            "complete_years": 0,
            "promotion_eligible": False,
        }

    baseline_summary = baseline_summary.loc[shared_windows]
    candidate_summary = candidate_summary.loc[shared_windows]
    annual = pd.DataFrame(
        {
            "Window": shared_windows,
            "Start": baseline_summary["Start"].to_numpy(),
            "End": baseline_summary["End"].to_numpy(),
            "Observations": baseline_summary["Observations"].astype(int).to_numpy(),
            "Baseline CAGR": baseline_summary["CAGR"].astype(float).to_numpy(),
            "Candidate CAGR": candidate_summary["CAGR"].astype(float).to_numpy(),
            "Baseline Max Drawdown": baseline_summary["Max Drawdown"].astype(float).to_numpy(),
            "Candidate Max Drawdown": candidate_summary["Max Drawdown"].astype(float).to_numpy(),
            "Baseline Calmar": baseline_summary["Calmar"].astype(float).to_numpy(),
            "Candidate Calmar": candidate_summary["Calmar"].astype(float).to_numpy(),
        }
    )
    annual["Candidate Excess CAGR"] = annual["Candidate CAGR"] - annual["Baseline CAGR"]
    annual["Candidate Drawdown Difference"] = (
        annual["Candidate Max Drawdown"] - annual["Baseline Max Drawdown"]
    )
    annual["Candidate Excess Calmar"] = annual["Candidate Calmar"] - annual["Baseline Calmar"]
    annual["Risk-adjusted Win"] = (
        annual["Candidate Excess Calmar"].gt(0.0) & annual["Candidate Drawdown Difference"].ge(0.0)
    )

    complete_years = len(annual)
    risk_adjusted_win_rate = float(annual["Risk-adjusted Win"].mean())
    median_excess_calmar = float(annual["Candidate Excess Calmar"].median())
    worst_excess_cagr = float(annual["Candidate Excess CAGR"].min())
    worst_drawdown_difference = float(annual["Candidate Drawdown Difference"].min())
    diagnostic_gate_passed = bool(
        complete_years >= 5
        and risk_adjusted_win_rate >= 0.50
        and median_excess_calmar >= 0.0
        and worst_excess_cagr >= -0.03
        and worst_drawdown_difference >= -0.03
    )
    return annual, {
        "status": "research_only_historical_stability",
        "baseline_variant": baseline_variant,
        "candidate_variant": candidate_variant,
        "complete_years": complete_years,
        "risk_adjusted_win_rate": risk_adjusted_win_rate,
        "median_excess_calmar": median_excess_calmar,
        "worst_excess_cagr": worst_excess_cagr,
        "worst_drawdown_difference": worst_drawdown_difference,
        "diagnostic_gate_passed": diagnostic_gate_passed,
        "promotion_eligible": False,
        "promotion_blocker": "candidate_not_selected_blind_to_historical_data; require immutable forward OOS",
        "gate_thresholds": {
            "minimum_complete_years": 5,
            "minimum_risk_adjusted_win_rate": 0.50,
            "minimum_median_excess_calmar": 0.0,
            "minimum_worst_excess_cagr": -0.03,
            "minimum_worst_drawdown_difference": -0.03,
        },
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
    annual_stability_baseline: str = DEFAULT_ANNUAL_STABILITY_BASELINE,
    annual_stability_candidate: str = DEFAULT_ANNUAL_STABILITY_CANDIDATE,
    annual_stability_start_year: int = DEFAULT_ANNUAL_STABILITY_START_YEAR,
    annual_stability_end_year: int | None = None,
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
    results_by_variant: dict[str, Mapping[str, object]] = {}
    annual_stability_variants = {annual_stability_baseline, annual_stability_candidate}

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
        if name in annual_stability_variants:
            results_by_variant[name] = result
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
    annual_stability, annual_stability_summary = build_fixed_spec_annual_stability(
        results_by_variant,
        baseline_variant=annual_stability_baseline,
        candidate_variant=annual_stability_candidate,
        start_year=int(annual_stability_start_year),
        end_year=annual_stability_end_year,
    )
    annual_stability.to_csv(output_dir / "fixed_spec_annual_stability.csv", index=False)
    write_json(output_dir / "fixed_spec_annual_stability_summary.json", annual_stability_summary)
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
    parser.add_argument("--annual-stability-baseline", default=DEFAULT_ANNUAL_STABILITY_BASELINE)
    parser.add_argument("--annual-stability-candidate", default=DEFAULT_ANNUAL_STABILITY_CANDIDATE)
    parser.add_argument("--annual-stability-start-year", type=int, default=DEFAULT_ANNUAL_STABILITY_START_YEAR)
    parser.add_argument("--annual-stability-end-year", type=int)
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
        annual_stability_baseline=str(args.annual_stability_baseline),
        annual_stability_candidate=str(args.annual_stability_candidate),
        annual_stability_start_year=int(args.annual_stability_start_year),
        annual_stability_end_year=args.annual_stability_end_year,
    )
    print(f"wrote SOXL dynamic volatility-delever threshold research -> {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
