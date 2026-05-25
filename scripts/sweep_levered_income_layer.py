from __future__ import annotations

import argparse
import json
from itertools import product
from pathlib import Path
from typing import Mapping

import pandas as pd

from us_equity_snapshot_pipelines.backtest_windows import build_benchmark_returns, build_window_summary
from us_equity_snapshot_pipelines.soxl_soxx_trend_income_archive import (
    DEFAULT_DYNAMIC_RSI_FLOOR,
    DEFAULT_DYNAMIC_RSI_QUANTILE,
    DEFAULT_DYNAMIC_RSI_QUANTILE_WINDOW,
)
from us_equity_snapshot_pipelines.soxl_soxx_trend_income_backtest import run_backtest as run_soxl_backtest
from us_equity_snapshot_pipelines.tqqq_growth_income_archive import (
    DEFAULT_FULL_BACKTEST_START as TQQQ_FULL_BACKTEST_START,
)
from us_equity_snapshot_pipelines.tqqq_growth_income_archive import run_backtest as run_tqqq_backtest


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "data" / "output" / "levered_income_layer_sweep"
DEFAULT_TQQQ_PRICES = (
    ROOT / "data" / "output" / "tqqq_growth_income_real_full_archive_2026-05-26" / "price_history.csv"
)
DEFAULT_SOXL_INCOME_PRICES = (
    ROOT / "data" / "output" / "soxl_soxx_trend_income_live_full_archive_2026-05-26" / "price_history.csv"
)
DEFAULT_SOXL_CORE_PRICES = (
    ROOT / "data" / "output" / "soxl_soxx_trend_income_core_long_archive_2026-05-26" / "price_history.csv"
)

INCOME_SYMBOLS = ("SCHD", "DGRO", "SGOV", "SPYI", "QQQI")

INCOME_BASKETS: dict[str, dict[str, float]] = {
    "current_tqqq": {"SCHD": 0.30, "DGRO": 0.20, "SGOV": 0.40, "SPYI": 0.08, "QQQI": 0.02},
    "current_soxl": {"SCHD": 0.20, "DGRO": 0.10, "SGOV": 0.65, "SPYI": 0.04, "QQQI": 0.01},
    "cash_70": {"SCHD": 0.15, "DGRO": 0.10, "SGOV": 0.70, "SPYI": 0.04, "QQQI": 0.01},
    "balanced_income": {"SCHD": 0.25, "DGRO": 0.15, "SGOV": 0.55, "SPYI": 0.04, "QQQI": 0.01},
    "dividend_income": {"SCHD": 0.30, "DGRO": 0.20, "SGOV": 0.45, "SPYI": 0.04, "QQQI": 0.01},
}

TQQQ_STARTS = (100_000.0, 150_000.0, 200_000.0, 300_000.0)
TQQQ_MAX_RATIOS = (0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65)
TQQQ_LOG_FACTORS = (0.50, 0.60, 0.70, 0.80, 0.90, 1.00)

SOXL_STARTS = (100_000.0, 150_000.0, 200_000.0, 300_000.0)
SOXL_MAX_RATIOS = (0.55, 0.65, 0.75, 0.85, 0.90, 0.95)
SOXL_LOG_FACTORS = (0.50, 0.60, 0.70, 0.80, 0.90, 1.00)

SOXL_CORE_WINDOWS = (5, 7, 10, 12, 15, 20, 30, 40)
SOXL_CORE_THRESHOLDS = (0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65)
SOXL_CORE_REDIRECTS = ("SOXX", "BOXX")
SOXL_CORE_RETENTIONS = (0.0, 0.25)


def _load_prices(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_csv(path)


def _as_float(value: object, default: float = float("nan")) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _income_overrides(
    *,
    start_usd: float,
    max_ratio: float,
    log_factor: float,
    allocations: Mapping[str, float],
) -> dict[str, object]:
    return {
        "income_layer_enabled": True,
        "income_layer_start_usd": float(start_usd),
        "income_layer_max_ratio": float(max_ratio),
        "income_layer_ratio_mode": "log_cap",
        "income_layer_log_growth_factor": float(log_factor),
        "income_layer_stress_drawdown_ratio": 0.30,
        "income_layer_base_loss_budget_ratio": 0.08,
        "income_layer_min_loss_budget_ratio": 0.06,
        "income_layer_loss_budget_decay_per_double": 0.01,
        "income_layer_qqqi_weight": float(allocations.get("QQQI", 0.0)),
        "income_layer_spyi_weight": float(allocations.get("SPYI", 0.0)),
        "income_layer_allocations": dict(allocations),
    }


def _soxl_dynamic_rsi_kwargs() -> dict[str, object]:
    return {
        "dynamic_rsi_quantile_window": DEFAULT_DYNAMIC_RSI_QUANTILE_WINDOW,
        "dynamic_rsi_quantile": DEFAULT_DYNAMIC_RSI_QUANTILE,
        "dynamic_rsi_floor": DEFAULT_DYNAMIC_RSI_FLOOR,
    }


def _soxl_core_candidates() -> list[dict[str, object]]:
    candidates = [
        {"core_variant": "manifest_default", "overrides": {}},
        {
            "core_variant": "vol_off",
            "overrides": {"blend_gate_volatility_delever_enabled": False},
        },
    ]
    for window, threshold, redirect, retention in product(
        SOXL_CORE_WINDOWS,
        SOXL_CORE_THRESHOLDS,
        SOXL_CORE_REDIRECTS,
        SOXL_CORE_RETENTIONS,
    ):
        label = f"vol_w{window}_t{threshold:.2f}_{redirect.lower()}_r{retention:.2f}"
        candidates.append(
            {
                "core_variant": label,
                "overrides": {
                    "blend_gate_volatility_delever_enabled": True,
                    "blend_gate_volatility_delever_symbol": "SOXX",
                    "blend_gate_volatility_delever_window": int(window),
                    "blend_gate_volatility_delever_threshold": float(threshold),
                    "blend_gate_volatility_delever_retention_ratio": float(retention),
                    "blend_gate_volatility_delever_redirect_symbol": redirect,
                },
            }
        )
    return candidates


def _window_metrics(portfolio_returns: pd.Series, prices: pd.DataFrame) -> dict[str, object]:
    window_summary = build_window_summary(
        portfolio_returns,
        benchmark_returns=build_benchmark_returns(prices),
    )
    if window_summary.empty or "Within Primary Benchmark Drawdown" not in window_summary:
        return {
            "passes_spy_windows": False,
            "worst_spy_drawdown_margin": float("nan"),
            "failed_spy_windows": "",
            "window_count": 0,
        }

    comparable = window_summary.loc[window_summary["Primary Benchmark"].astype(str).eq("SPY")].copy()
    if comparable.empty:
        return {
            "passes_spy_windows": False,
            "worst_spy_drawdown_margin": float("nan"),
            "failed_spy_windows": "",
            "window_count": 0,
        }

    within = comparable["Within Primary Benchmark Drawdown"].astype(bool)
    failed = comparable.loc[~within, "Window"].astype(str).tolist()
    margin = pd.to_numeric(comparable["Drawdown Difference vs Primary Benchmark"], errors="coerce")
    return {
        "passes_spy_windows": bool(within.all()),
        "worst_spy_drawdown_margin": float(margin.min()),
        "failed_spy_windows": ",".join(failed),
        "window_count": int(len(comparable)),
    }


def _income_ratio_metrics(weights_history: pd.DataFrame) -> dict[str, float]:
    weights = pd.DataFrame(weights_history)
    present = [symbol for symbol in INCOME_SYMBOLS if symbol in weights.columns]
    if not present:
        return {"avg_income_ratio": 0.0, "end_income_ratio": 0.0}
    income_ratio = weights[present].sum(axis=1).fillna(0.0)
    return {
        "avg_income_ratio": float(income_ratio.mean()),
        "end_income_ratio": float(income_ratio.iloc[-1]) if len(income_ratio) else 0.0,
    }


def _result_row(
    *,
    strategy: str,
    result: Mapping[str, object],
    prices: pd.DataFrame,
    extra: Mapping[str, object],
) -> dict[str, object]:
    summary = dict(result["summary"])
    row = {
        "strategy": strategy,
        "start": summary.get("Start"),
        "end": summary.get("End"),
        "cagr": _as_float(summary.get("CAGR")),
        "max_drawdown": _as_float(summary.get("Max Drawdown")),
        "volatility": _as_float(summary.get("Volatility")),
        "sharpe": _as_float(summary.get("Sharpe")),
        "calmar": _as_float(summary.get("Calmar")),
        "total_return": _as_float(summary.get("Total Return")),
        "final_equity": _as_float(summary.get("Final Equity")),
        "soxl_delever_stops": _as_float(summary.get("SOXL Delever Stops"), 0.0),
    }
    row.update(_window_metrics(result["portfolio_returns"], prices))
    row.update(_income_ratio_metrics(result["weights_history"]))
    row.update(extra)
    return row


def _write_rows(path: Path, rows: list[dict[str, object]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    frame.to_csv(path, index=False)
    return frame


def _rank_combo(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    ranked = frame.copy()
    ranked["_passes_sort"] = ranked["passes_spy_windows"].astype(bool).astype(int)
    ranked = ranked.sort_values(
        ["_passes_sort", "cagr", "worst_spy_drawdown_margin", "max_drawdown"],
        ascending=[False, False, False, False],
    )
    return ranked.drop(columns=["_passes_sort"])


def _rank_core(frame: pd.DataFrame, *, mdd_floor: float) -> pd.DataFrame:
    if frame.empty:
        return frame
    ranked = frame.copy()
    ranked["_core_passes_sort"] = ranked["max_drawdown"].ge(float(mdd_floor)).astype(int)
    ranked = ranked.sort_values(
        ["_core_passes_sort", "cagr", "max_drawdown", "calmar"],
        ascending=[False, False, False, False],
    )
    return ranked.drop(columns=["_core_passes_sort"])


def run_soxl_core_sweep(*, prices: pd.DataFrame, output_dir: Path, core_mdd_floor: float) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    candidates = _soxl_core_candidates()
    for index, candidate in enumerate(candidates, start=1):
        if index == 1 or index % 25 == 0 or index == len(candidates):
            print(f"[soxl-core] {index}/{len(candidates)}", flush=True)
        result = run_soxl_backtest(
            prices,
            initial_equity=100_000.0,
            start_date="2010-01-01",
            disable_income_layer=True,
            strategy_overrides=dict(candidate["overrides"]),
            **_soxl_dynamic_rsi_kwargs(),
        )
        rows.append(
            _result_row(
                strategy="soxl_core",
                result=result,
                prices=prices,
                extra={
                    "core_variant": candidate["core_variant"],
                    "core_overrides_json": json.dumps(candidate["overrides"], sort_keys=True),
                },
            )
        )
    frame = _write_rows(output_dir / "soxl_core_overlay_sweep.csv", rows)
    ranked = _rank_core(frame, mdd_floor=core_mdd_floor)
    ranked.to_csv(output_dir / "soxl_core_overlay_ranked.csv", index=False)
    return ranked


def _income_grid(strategy: str) -> list[dict[str, object]]:
    if strategy == "tqqq":
        starts = TQQQ_STARTS
        max_ratios = TQQQ_MAX_RATIOS
        log_factors = TQQQ_LOG_FACTORS
    elif strategy == "soxl":
        starts = SOXL_STARTS
        max_ratios = SOXL_MAX_RATIOS
        log_factors = SOXL_LOG_FACTORS
    else:
        raise ValueError(f"unsupported strategy {strategy!r}")

    candidates: list[dict[str, object]] = []
    for start_usd, max_ratio, log_factor, basket_name in product(
        starts,
        max_ratios,
        log_factors,
        tuple(INCOME_BASKETS),
    ):
        allocations = INCOME_BASKETS[basket_name]
        candidates.append(
            {
                "income_layer_start_usd": float(start_usd),
                "income_layer_max_ratio": float(max_ratio),
                "income_layer_log_growth_factor": float(log_factor),
                "income_basket": basket_name,
                "income_allocations_json": json.dumps(allocations, sort_keys=True),
                "overrides": _income_overrides(
                    start_usd=float(start_usd),
                    max_ratio=float(max_ratio),
                    log_factor=float(log_factor),
                    allocations=allocations,
                ),
            }
        )
    return candidates


def run_tqqq_income_sweep(*, prices: pd.DataFrame, initial_equity: float, output_dir: Path) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    candidates = _income_grid("tqqq")
    for index, candidate in enumerate(candidates, start=1):
        if index == 1 or index % 50 == 0 or index == len(candidates):
            print(f"[tqqq-income] {index}/{len(candidates)}", flush=True)
        result = run_tqqq_backtest(
            prices,
            initial_equity=float(initial_equity),
            start_date=TQQQ_FULL_BACKTEST_START,
            strategy_overrides=dict(candidate["overrides"]),
        )
        rows.append(
            _result_row(
                strategy="tqqq_income",
                result=result,
                prices=prices,
                extra={key: value for key, value in candidate.items() if key != "overrides"},
            )
        )
    frame = _write_rows(output_dir / "tqqq_income_layer_sweep.csv", rows)
    ranked = _rank_combo(frame)
    ranked.to_csv(output_dir / "tqqq_income_layer_ranked.csv", index=False)
    return ranked


def _select_soxl_core_overlays_for_income(core_ranked: pd.DataFrame, *, top_n: int) -> list[dict[str, object]]:
    if core_ranked.empty:
        return [{"core_variant": "manifest_default", "overrides": {}}]

    selected_rows = core_ranked.head(max(1, int(top_n))).copy()
    manifest_default = core_ranked.loc[core_ranked["core_variant"].astype(str).eq("manifest_default")]
    if not manifest_default.empty and "manifest_default" not in selected_rows["core_variant"].astype(str).tolist():
        selected_rows = pd.concat([manifest_default.head(1), selected_rows], ignore_index=True)

    overlays: list[dict[str, object]] = []
    seen: set[str] = set()
    for row in selected_rows.to_dict(orient="records"):
        name = str(row.get("core_variant"))
        if name in seen:
            continue
        seen.add(name)
        raw_overrides = str(row.get("core_overrides_json") or "{}")
        overlays.append({"core_variant": name, "overrides": json.loads(raw_overrides)})
    return overlays


def run_soxl_income_sweep(
    *,
    prices: pd.DataFrame,
    initial_equity: float,
    output_dir: Path,
    core_overlays: list[dict[str, object]],
) -> pd.DataFrame:
    income_candidates = _income_grid("soxl")
    total = len(core_overlays) * len(income_candidates)
    rows: list[dict[str, object]] = []
    index = 0
    for overlay in core_overlays:
        for candidate in income_candidates:
            index += 1
            if index == 1 or index % 100 == 0 or index == total:
                print(f"[soxl-income] {index}/{total}", flush=True)
            overrides = dict(overlay["overrides"])
            overrides.update(dict(candidate["overrides"]))
            result = run_soxl_backtest(
                prices,
                initial_equity=float(initial_equity),
                strategy_overrides=overrides,
                **_soxl_dynamic_rsi_kwargs(),
            )
            rows.append(
                _result_row(
                    strategy="soxl_income",
                    result=result,
                    prices=prices,
                    extra={
                        "core_variant": overlay["core_variant"],
                        "core_overrides_json": json.dumps(overlay["overrides"], sort_keys=True),
                        **{key: value for key, value in candidate.items() if key != "overrides"},
                    },
                )
            )
    frame = _write_rows(output_dir / "soxl_income_layer_sweep.csv", rows)
    ranked = _rank_combo(frame)
    ranked.to_csv(output_dir / "soxl_income_layer_ranked.csv", index=False)
    return ranked


def _best_record(frame: pd.DataFrame) -> dict[str, object] | None:
    if frame.empty:
        return None
    return frame.head(1).to_dict(orient="records")[0]


def main() -> int:
    parser = argparse.ArgumentParser(description="Sweep levered strategy core overlays and income layer defaults.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--initial-equity", type=float, default=1_000_000.0)
    parser.add_argument("--core-mdd-floor", type=float, default=-0.50)
    parser.add_argument("--soxl-income-core-top-n", type=int, default=8)
    parser.add_argument("--tqqq-price-history", type=Path, default=DEFAULT_TQQQ_PRICES)
    parser.add_argument("--soxl-income-price-history", type=Path, default=DEFAULT_SOXL_INCOME_PRICES)
    parser.add_argument("--soxl-core-price-history", type=Path, default=DEFAULT_SOXL_CORE_PRICES)
    parser.add_argument("--skip-tqqq-income", action="store_true")
    parser.add_argument("--skip-soxl-core", action="store_true")
    parser.add_argument("--skip-soxl-income", action="store_true")
    args = parser.parse_args()

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    best: dict[str, object] = {
        "initial_equity": float(args.initial_equity),
        "core_mdd_floor": float(args.core_mdd_floor),
        "soxl_income_core_top_n": int(args.soxl_income_core_top_n),
    }

    core_ranked = pd.DataFrame()
    if not args.skip_soxl_core:
        soxl_core_prices = _load_prices(args.soxl_core_price_history)
        core_ranked = run_soxl_core_sweep(
            prices=soxl_core_prices,
            output_dir=output_dir,
            core_mdd_floor=float(args.core_mdd_floor),
        )
        best["soxl_core"] = _best_record(core_ranked)
    else:
        core_ranked_path = output_dir / "soxl_core_overlay_ranked.csv"
        if core_ranked_path.exists():
            core_ranked = pd.read_csv(core_ranked_path)
            best["soxl_core"] = _best_record(core_ranked)

    if not args.skip_tqqq_income:
        tqqq_prices = _load_prices(args.tqqq_price_history)
        tqqq_ranked = run_tqqq_income_sweep(
            prices=tqqq_prices,
            initial_equity=float(args.initial_equity),
            output_dir=output_dir,
        )
        best["tqqq_income"] = _best_record(tqqq_ranked)

    if not args.skip_soxl_income:
        soxl_income_prices = _load_prices(args.soxl_income_price_history)
        core_overlays = _select_soxl_core_overlays_for_income(
            core_ranked,
            top_n=int(args.soxl_income_core_top_n),
        )
        best["soxl_income_core_variants_tested"] = [item["core_variant"] for item in core_overlays]
        soxl_ranked = run_soxl_income_sweep(
            prices=soxl_income_prices,
            initial_equity=float(args.initial_equity),
            output_dir=output_dir,
            core_overlays=core_overlays,
        )
        best["soxl_income"] = _best_record(soxl_ranked)

    (output_dir / "best_configs.json").write_text(
        json.dumps(best, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(f"wrote {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
