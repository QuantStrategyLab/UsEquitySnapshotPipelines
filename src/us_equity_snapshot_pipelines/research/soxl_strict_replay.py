"""Pre-registered strict replay of the SOXL core-only strategy (research only).

Question (pre-registered 2026-11-26): how much of the historical SOXL core-only result
(SOXX trend + RSI cap + volatility delever across SOXL / SOXX / cash, no plugins) survives
a strict replay, and where does the gap come from?

Two methods on the same frozen strategy parameters and the same data:
- ``original``: the strategy's own simulator convention, a signal from session T's close
  traded at that same close, 5 bp turnover cost, no tax, no commission.
- ``strict``: the same targets executed at the next session's close, each trade paying its
  own spread (3 bp 3x, 1 bp 1x; stress x2, x3) and a USD 0.35 minimum commission on a fresh
  USD 1,500 account, and a configurable yearly tax upper bound (default 0%, example 20% stress profile; see small_account_cost_comparison).

The strategy and its parameters are not changed. BIL stands in for BOXX, which only trades
from 2022-12. Constant: live_ready=False, no_order=True.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Callable

import pandas as pd

from . import small_account_cost_comparison as costs

PREREGISTRATION_DATE = "2026-11-26"
AUTHORIZATION_ID = "soxl-strict-replay-2026-11-26"
STUDY_ID = "us_equity_snapshot_pipelines.research.soxl_strict_replay"
FULL_START = costs.FULL_START
OOS_START = costs.OOS_START
DOWNLOAD_START = "2009-01-01"
SYMBOLS = ("BIL", "SOXL", "SOXX", "SPY")
ORIGINAL_COST_BPS = 5.0
REBALANCE_THRESHOLD = 0.01
WARM_UP_SESSIONS = 260  # the simulator's DEFAULT_MIN_HISTORY_DAYS  # the strategy's own rebalance_threshold_ratio
MANDATE_PATH = Path("config/research_mandates") / f"{AUTHORIZATION_ID}.json"
MANDATE_SHA256 = "85246db3dc588fadbad8ccc91e199d1a5ce65c153435eb8dfdfc6a6766003d60"


def _close_matrix(price_history: pd.DataFrame) -> pd.DataFrame:
    return costs._normalize_price_history(price_history)


def original_replay(targets: pd.DataFrame, close: pd.DataFrame, *, cost_bps: float = ORIGINAL_COST_BPS) -> pd.Series:
    """Cross-check of the same-close convention with half-turnover cost on targets.

    The unchanged simulator (``original_simulator``) is the reported baseline; it charges
    both legs against drifted holdings, so this recomputation is slightly cheaper.
    """
    returns = close.pct_change(fill_method=None)
    out, weights = [], None
    for session, row in targets.iterrows():
        target = row.fillna(0.0)
        turnover = 0.0 if weights is None else 0.5 * float((target - weights).abs().sum())
        if weights is None:
            turnover = 0.5 * float(target.abs().sum())
        gross = float(sum(target[s] * returns.at[session, s] for s in target.index if target[s]))
        out.append(gross - turnover * cost_bps / 10_000.0)
        weights = target
    return pd.Series(out, index=targets.index, name="original_return")


def run(price_history: pd.DataFrame, target_builder: Callable[[pd.DataFrame], pd.DataFrame], *,
        end_date: str | None = None,
        reference: Callable[[pd.DataFrame], pd.Series] | None = None,
        tax_rate: float | None = None) -> dict:
    """target_builder(close) returns weights per session already lagged one session
    (row d holds the target chosen from the close of the prior session)."""
    effective_tax_rate = costs.default_tax_rate() if tax_rate is None else costs.validate_tax_rate(tax_rate)
    # Every symbol must trade on every XNYS session before prices are forward-filled.
    costs.validate_coverage(price_history, FULL_START, end_date, symbols=list(SYMBOLS),
                            signal_windows={"SOXX": WARM_UP_SESSIONS, "SOXL": WARM_UP_SESSIONS})
    close = _close_matrix(price_history)
    if end_date:
        close = close.loc[close.index <= pd.Timestamp(end_date)]
    signal_targets = target_builder(close).fillna(0.0)
    # Execute at the next session's close: shift on the full history, then slice each period,
    # so the first session of a period uses the target chosen inside the warm-up history.
    executed_targets = signal_targets.shift(1).dropna()
    returns = close.pct_change(fill_method=None)
    rows = []
    for period, start in (("full", FULL_START), ("oos", OOS_START)):
        window = signal_targets.loc[signal_targets.index >= pd.Timestamp(start)]
        if len(window) < 3:
            raise ValueError(f"price history cannot produce the registered {period} period")
        rows.append({"method": "original", "stress": 1.0, "period": period,
                     **costs.metrics(original_replay(window, close))})
        strict_targets = executed_targets.loc[executed_targets.index >= pd.Timestamp(start)]
        for multiplier in costs.STRESS_MULTIPLIERS:
            net = costs.simulate_account(strict_targets, returns, spread_multiplier=multiplier,
                                         tax_rate=effective_tax_rate, min_change=REBALANCE_THRESHOLD)
            rows.append({"method": "strict", "stress": multiplier, "period": period, **costs.metrics(net)})
    if reference is not None:
        # The strategy's own simulator, unchanged, as an exact reference for the original method.
        ref = reference(close)
        for period, start in (("full", FULL_START), ("oos", OOS_START)):
            rows.append({"method": "original_simulator", "stress": 1.0, "period": period,
                         **costs.metrics(ref.loc[ref.index >= pd.Timestamp(start)])})
    frame = pd.DataFrame(rows)
    full = frame.loc[frame["period"].eq("full")]
    # The unchanged simulator is the baseline when available; the recomputation is a cross-check.
    baseline = "original_simulator" if (full["method"] == "original_simulator").any() else "original"
    original = full.loc[full["method"].eq(baseline)].iloc[0]
    strict = full.loc[full["method"].eq("strict") & full["stress"].eq(1.0)].iloc[0]
    summary = {
        "baseline": baseline,
        "original_cagr": float(original["cagr"]), "strict_cagr": float(strict["cagr"]),
        "cagr_gap": float(original["cagr"] - strict["cagr"]),
        "strict_max_drawdown": float(strict["max_drawdown"]),
        "excluded_on_balanced_tier": bool(abs(strict["max_drawdown"]) > costs.BALANCED_MAX_DRAWDOWN),
    }
    return {"rows": frame, "summary": summary, "tax_rate": effective_tax_rate}


def render_markdown(output: dict) -> str:
    tax_rate = float(output.get("tax_rate", costs.DEFAULT_TAX_RATE))
    lines = ["# SOXL core-only strict replay (pre-registered 2026-11-26, research only)", "",
             "live_ready=False · no_order=True · original = same-close execution, 5 bp, no tax;"
             " strict = next-close execution, per-trade spread + USD 0.35 commission on USD 1,500,"
             f" configurable yearly tax ({tax_rate:.0%}) on positive yearly gains. Strategy parameters unchanged; BIL stands in for BOXX.", "",
             "| Method | Period | Spread stress | CAGR | Max DD | Calmar | Sharpe |",
             "| --- | --- | ---: | ---: | ---: | ---: | ---: |"]
    for r in output["rows"].itertuples():
        lines.append(f"| {r.method} | {r.period} | x{r.stress:.0f} | {r.cagr:.1%} | {r.max_drawdown:.1%}"
                     f" | {r.calmar:.2f} | {r.sharpe:.2f} |")
    s = output["summary"]
    lines += ["", f"Full-period CAGR: {s['baseline']} {s['original_cagr']:.1%} → strict {s['strict_cagr']:.1%}"
              f" (gap {s['cagr_gap']:.1%}); strict max drawdown {s['strict_max_drawdown']:.1%};"
              f" excluded on the balanced tier: {s['excluded_on_balanced_tier']}.",
              "The 2023+ period is a later historical subperiod, not an untouched holdout."]
    return "\n".join(lines) + "\n"


def verify_mandate(path: Path, end_date: str, *, now: pd.Timestamp | None = None) -> dict:
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != MANDATE_SHA256:
        raise SystemExit("research data mandate rejected: not the reviewed record")
    record = json.loads(raw.decode("utf-8"))
    scope, attestation = record.get("scope") or {}, record.get("attestation") or {}
    now = now or pd.Timestamp.now(tz="UTC")
    problems = []
    if record.get("mandate_id") != AUTHORIZATION_ID or record.get("study") != STUDY_ID:
        problems.append("identity")
    if scope.get("provider") != "YAHOO_FINANCE_PUBLIC_ADJUSTED_CLOSE" or scope.get("download_start") != DOWNLOAD_START:
        problems.append("source")
    if sorted(scope.get("symbols") or []) != sorted(SYMBOLS):
        problems.append("symbols")
    if not all(scope.get(flag) is True for flag in ("no_order", "no_paper", "no_live", "no_capital")):
        problems.append("authority")
    if pd.Timestamp(end_date) > pd.Timestamp(scope.get("latest_end_date", "1900-01-01")):
        problems.append("end_date")
    if pd.Timestamp(attestation.get("expires_at", "1900-01-01T00:00:00Z")) <= now:
        problems.append("expired")
    if problems:
        raise SystemExit(f"research data mandate rejected: {', '.join(problems)}")
    return record


def soxl_core_only_targets(close: pd.DataFrame) -> pd.DataFrame:
    """Targets from the frozen SOXL core-only simulator, with BIL standing in for BOXX."""
    from us_equity_strategies.backtest import soxl_trend_simulator as simulator

    frame = close.rename(columns={"BIL": "BOXX"})[list(simulator.CORE_ASSETS)]
    kwargs = simulator._strategy_kwargs()
    indicators = simulator.build_indicator_history(
        frame,
        trend_ma_window=int(kwargs["trend_ma_window"]),
        volatility_window=int(kwargs["blend_gate_volatility_delever_window"]),
        volatility_lookback=int(kwargs["blend_gate_volatility_delever_dynamic_lookback"]),
        volatility_percentile=float(kwargs["blend_gate_volatility_delever_dynamic_percentile"]),
        volatility_min_periods=int(kwargs["blend_gate_volatility_delever_dynamic_min_periods"]),
        volatility_floor=float(kwargs["blend_gate_volatility_delever_dynamic_floor"]),
        volatility_cap=float(kwargs["blend_gate_volatility_delever_dynamic_cap"]),
    )
    targets = simulator._target_weight_frame(frame, indicators, min_history_days=simulator.DEFAULT_MIN_HISTORY_DAYS,
                                             strategy_kwargs=kwargs)
    return targets.rename(columns={"BOXX": "BIL"}).ffill()


def soxl_core_only_reference(close: pd.DataFrame) -> pd.Series:
    """Daily returns of the unchanged SOXL core-only simulator (same-close, 5 bp)."""
    from us_equity_strategies.backtest import soxl_trend_simulator as simulator

    frame = close.rename(columns={"BIL": "BOXX"})[list(simulator.CORE_ASSETS)]
    history = frame.stack().rename("close").reset_index()
    history.columns = ["date", "symbol", "close"]
    return simulator.run_soxl_core_only_backtest(history, cost_bps=ORIGINAL_COST_BPS).daily_returns


def main(argv=None) -> int:
    from ..yfinance_prices import download_price_history

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--tax-rate", type=float, default=None,
                        help=f"annual gain tax rate in [0, 1] (default env {costs.TAX_RATE_ENV_VAR} or 0.0)")
    args = parser.parse_args(argv)
    verify_mandate(MANDATE_PATH, args.end_date)  # before any provider access
    import os
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("GITHUB_REF") != "refs/heads/main":
        raise SystemExit("research data access runs only in the reviewed workflow on main")
    end_exclusive = (pd.Timestamp(args.end_date) + pd.Timedelta(days=1)).date().isoformat()
    prices = download_price_history(list(SYMBOLS), start=DOWNLOAD_START, end=end_exclusive)
    output = run(prices, soxl_core_only_targets, end_date=args.end_date, reference=soxl_core_only_reference,
                 tax_rate=args.tax_rate)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "report.md").write_text(render_markdown(output), encoding="utf-8")
    payload = {"rows": output["rows"].to_dict("records"), "summary": output["summary"],
               "tax_rate": output["tax_rate"], "live_ready": False, "no_order": True}
    safe = costs._json_safe(payload)
    (args.output_dir / "results.json").write_text(json.dumps(safe, indent=2, allow_nan=False), encoding="utf-8")
    print(render_markdown(output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
