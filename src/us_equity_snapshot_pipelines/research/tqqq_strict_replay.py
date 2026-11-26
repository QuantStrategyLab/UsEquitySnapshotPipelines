"""Pre-registered strict replay of the complete tqqq_growth_income strategy (research only).

Question (pre-registered 2026-11-27): how much of the historical tqqq_growth_income result
(45% TQQQ / 45% QQQM [QQQ pre-listing] / 2% cash / BOXX [BIL proxy] with MA200+MA20 slope,
20d pullback rebound, and 5d/252d rolling volatility delever) survives a strict replay, and
where does the gap come from?

Two methods on the same frozen strategy parameters and the same data:
- ``original_simulator`` / ``original``: signal from session d's close traded at that same close,
  5 bp turnover cost, no tax, no minimum commission.
- ``strict``: the same targets executed at the next session's close, each trade paying its own
  spread (3 bp 3x, 1 bp 1x; stress x2, x3) and a USD 0.35 minimum commission on a fresh
  USD 1,500 account, and a 20% yearly tax upper bound on positive gains.

Income layer (income_layer_start_usd=250000) is inactive under USD 1,500 and is disabled
consistently across both baseline and strict replays. Constant: live_ready=False, no_order=True.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Callable
from pathlib import Path

import pandas as pd

from . import small_account_cost_comparison as costs

PREREGISTRATION_DATE = "2026-11-27"
AUTHORIZATION_ID = "tqqq-strict-replay-2026-11-27"
STUDY_ID = "us_equity_snapshot_pipelines.research.tqqq_strict_replay"
FULL_START = costs.FULL_START
OOS_START = costs.OOS_START
QQQM_START = "2020-10-14"
DOWNLOAD_START = "2009-01-01"
BASE_SYMBOLS = ("BIL", "QQQ", "SPY", "TQQQ")
SYMBOLS = ("BIL", "QQQ", "QQQM", "SPY", "TQQQ")
ORIGINAL_COST_BPS = 5.0
REBALANCE_THRESHOLD = 0.01
WARM_UP_SESSIONS = 260
MANDATE_PATH = Path("config/research_mandates") / f"{AUTHORIZATION_ID}.json"
MANDATE_SHA256 = "325bb80f1266d479b77bafb59a737719aca55084c3d09596913b3f216db6712a"


def _close_matrix(price_history: pd.DataFrame) -> pd.DataFrame:
    return costs._normalize_price_history(price_history)


def original_replay(targets: pd.DataFrame, close: pd.DataFrame, *, cost_bps: float = ORIGINAL_COST_BPS) -> pd.Series:
    """Cross-check of the same-close convention with half-turnover cost on targets."""
    returns = close.pct_change(fill_method=None)
    out, weights = [], None
    for session, row in targets.iterrows():
        target = row.fillna(0.0)
        turnover = 0.5 * float(target.abs().sum()) if weights is None else 0.5 * float((target - weights).abs().sum())
        gross = float(sum(target[s] * returns.at[session, s] for s in target.index if target[s]))
        out.append(gross - turnover * cost_bps / 10_000.0)
        weights = target
    return pd.Series(out, index=targets.index, name="original_return")


def _effective_end_timestamp(price_history: pd.DataFrame, end_date: str | None) -> pd.Timestamp:
    if end_date:
        return pd.Timestamp(end_date)
    dates = pd.to_datetime(price_history["as_of"]).dt.tz_localize(None).dt.normalize()
    return pd.Timestamp(dates.max())


def run(price_history: pd.DataFrame, target_builder: Callable[[pd.DataFrame], pd.DataFrame], *,
        end_date: str | None = None,
        reference: Callable[[pd.DataFrame], pd.Series] | None = None) -> dict:
    """target_builder(close) returns weights per session already lagged one session
    (row d holds the target chosen from the close of the prior session)."""
    # Every base symbol must trade on every XNYS session before prices are forward-filled.
    costs.validate_coverage(price_history, FULL_START, end_date, symbols=list(BASE_SYMBOLS),
                            signal_windows={"QQQ": WARM_UP_SESSIONS, "TQQQ": WARM_UP_SESSIONS})
    end_ts = _effective_end_timestamp(price_history, end_date)
    if end_ts >= pd.Timestamp(QQQM_START):
        costs.validate_coverage(price_history, QQQM_START, end_date, symbols=["QQQM"], signal_windows={})

    close = _close_matrix(price_history)
    if end_date:
        close = close.loc[close.index <= pd.Timestamp(end_date)]
    signal_targets = target_builder(close).fillna(0.0)
    # Execute at the next session's close: shift on the full history, then slice each period.
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
                                         min_change=REBALANCE_THRESHOLD)
            rows.append({"method": "strict", "stress": multiplier, "period": period, **costs.metrics(net)})
    if reference is not None:
        ref = reference(close)
        for period, start in (("full", FULL_START), ("oos", OOS_START)):
            rows.append({"method": "original_simulator", "stress": 1.0, "period": period,
                         **costs.metrics(ref.loc[ref.index >= pd.Timestamp(start)])})
    frame = pd.DataFrame(rows)
    full = frame.loc[frame["period"].eq("full")]
    baseline = "original_simulator" if (full["method"] == "original_simulator").any() else "original"
    original = full.loc[full["method"].eq(baseline)].iloc[0]
    strict = full.loc[full["method"].eq("strict") & full["stress"].eq(1.0)].iloc[0]
    summary = {
        "baseline": baseline,
        "original_cagr": float(original["cagr"]),
        "strict_cagr": float(strict["cagr"]),
        "cagr_gap": float(original["cagr"] - strict["cagr"]),
        "strict_max_drawdown": float(strict["max_drawdown"]),
        "excluded_on_balanced_tier": bool(abs(strict["max_drawdown"]) > costs.BALANCED_MAX_DRAWDOWN),
    }
    return {"rows": frame, "summary": summary}


def render_markdown(output: dict) -> str:
    lines = [
        "# `tqqq_growth_income` strict replay (pre-registered 2026-11-27, research only)",
        "",
        (
            "live_ready=False · no_order=True · original = same-close execution, 5 bp, no tax;"
            " strict = next-close execution, per-trade spread + USD 0.35 commission on USD 1,500,"
            " 20% CN tax on positive yearly gains. Parameters unchanged; BIL proxy for BOXX, QQQ proxy"
            " for pre-2020-10-14 QQQM, income layer inactive under USD 1,500."
        ),
        "",
        "| Method | Period | Spread stress | CAGR | Max DD | Calmar | Sharpe |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for r in output["rows"].itertuples():
        lines.append(
            f"| {r.method} | {r.period} | x{r.stress:.0f} | {r.cagr:.1%} | {r.max_drawdown:.1%}"
            f" | {r.calmar:.2f} | {r.sharpe:.2f} |"
        )
    s = output["summary"]
    lines += [
        "",
        (
            f"Full-period CAGR: {s['baseline']} {s['original_cagr']:.1%} → strict {s['strict_cagr']:.1%}"
            f" (gap {s['cagr_gap']:.1%}); strict max drawdown {s['strict_max_drawdown']:.1%};"
            f" excluded on the balanced tier: {s['excluded_on_balanced_tier']}."
        ),
        "The 2023+ period is a later historical subperiod, not an untouched holdout.",
    ]
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


def tqqq_growth_income_targets(close: pd.DataFrame) -> pd.DataFrame:
    """Targets from frozen tqqq_growth_income rules (QQQ proxy pre-QQQM listing, BIL proxy for BOXX)."""
    from types import SimpleNamespace

    from us_equity_strategies.strategies.tqqq_growth_income import build_rebalance_plan

    from .. import tqqq_growth_income_archive as archive

    qqq_series = close["QQQ"].dropna()
    qqq_history = qqq_series.rename("close").reset_index()
    qqq_history.columns = ["as_of", "close"]
    qqq_dates = pd.DatetimeIndex(qqq_history["as_of"])
    returns = close.pct_change(fill_method=None)
    qqqm_first_return = (
        returns["QQQM"].first_valid_index()
        if "QQQM" in returns.columns
        else None
    )

    strategy_kwargs = archive._strategy_kwargs(archive._income_disabled_overrides())
    volatility_window = max(1, int(strategy_kwargs.get("dual_drive_volatility_delever_window") or 5))
    dynamic_lookback = max(1, int(strategy_kwargs.get("dual_drive_volatility_delever_dynamic_lookback") or 252))
    pullback_window = max(1, int(strategy_kwargs.get("dual_drive_pullback_rebound_window") or 20))
    history_window = max(WARM_UP_SESSIONS, dynamic_lookback + volatility_window + 10, pullback_window + 220)

    weights: dict[str, float] = {"TQQQ": 0.0, "QQQ": 0.0, "QQQM": 0.0, "BIL": 1.0}
    equity = 1.0
    rows: list[dict[str, float | pd.Timestamp]] = []

    for as_of in pd.DatetimeIndex(close.index):
        history_end = int(qqq_dates.searchsorted(as_of, side="right"))
        if history_end < 220:
            continue
        close_row = close.loc[as_of]
        if any(pd.isna(close_row.get(sym)) for sym in ("TQQQ", "QQQ", "BIL")):
            continue
        use_qqqm = bool(
            qqqm_first_return is not None
            and as_of >= qqqm_first_return
            and pd.notna(close_row.get("QQQM"))
        )
        active_unlevered = "QQQM" if use_qqqm else "QQQ"
        active_kwargs = {**strategy_kwargs, "dual_drive_unlevered_symbol": active_unlevered}
        positions = [
            SimpleNamespace(
                symbol=symbol,
                market_value=equity * float(weights.get(symbol, 0.0)),
                quantity=(
                    (equity * float(weights.get(symbol, 0.0)) / float(close_row[symbol]))
                    if pd.notna(close_row.get(symbol)) and float(close_row[symbol]) > 0.0
                    else 0.0
                ),
            )
            for symbol in ("TQQQ", active_unlevered)
        ]
        positions.append(
            SimpleNamespace(
                symbol="BOXX",
                market_value=equity * float(weights["BIL"]),
                quantity=equity * float(weights["BIL"]) / float(close_row["BIL"]),
            )
        )
        snapshot = SimpleNamespace(
            positions=tuple(positions),
            total_equity=equity,
            buying_power=equity,
            metadata={},
        )
        history = qqq_history.iloc[max(0, history_end - history_window):history_end]
        plan = build_rebalance_plan(
            history,
            snapshot,
            signal_text_fn=str,
            translator=lambda key, **_kwargs: key,
            **active_kwargs,
        )
        target_values = dict(plan["target_values"])
        selected = {
            "TQQQ": max(0.0, float(target_values.get("TQQQ", 0.0)) / equity),
            "QQQ": max(0.0, float(target_values.get("QQQ", 0.0)) / equity) if active_unlevered == "QQQ" else 0.0,
            "QQQM": max(0.0, float(target_values.get("QQQM", 0.0)) / equity) if active_unlevered == "QQQM" else 0.0,
            "BIL": max(0.0, float(target_values.get("BOXX", 0.0)) / equity),
        }
        weight_sum = sum(selected.values())
        if weight_sum > 1.0 + 1e-9:
            selected = {symbol: value / weight_sum for symbol, value in selected.items()}
        rows.append({"date": as_of, **selected})
        weights = selected

    if not rows:
        raise ValueError("tqqq_growth_income replay produced no target weight events")
    targets_frame = pd.DataFrame(rows).set_index("date")
    # Lag-one close convention: signal at d close applies to d -> d+1 (same-close baseline);
    # strict mode shifts one further session inside run().
    return targets_frame.reindex(close.index).shift(1)


def tqqq_growth_income_reference(close: pd.DataFrame) -> pd.Series:
    """Daily returns of unchanged tqqq_growth_income_archive.run_backtest (same-close, 5 bp)."""
    from .. import tqqq_growth_income_archive as archive

    frame = pd.DataFrame(index=close.index)
    frame["TQQQ"] = close["TQQQ"]
    frame["QQQ"] = close["QQQ"]
    frame["BOXX"] = close["BIL"]
    frame["QQQM"] = close["QQQM"].combine_first(close["QQQ"]) if "QQQM" in close.columns else close["QQQ"]
    history = frame.stack().rename("close").reset_index()
    history.columns = ["as_of", "symbol", "close"]
    result = archive.run_backtest(
        history,
        initial_equity=costs.STARTING_EQUITY_USD,
        start_date=FULL_START,
        turnover_cost_bps=ORIGINAL_COST_BPS,
        strategy_overrides=archive._income_disabled_overrides(),
    )
    return result["portfolio_returns"]


def main(argv=None) -> int:
    from ..yfinance_prices import download_price_history_with_proxy_candidates

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    verify_mandate(MANDATE_PATH, args.end_date)  # before any provider access
    import os
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("GITHUB_REF") != "refs/heads/main":
        raise SystemExit("research data access runs only in the reviewed workflow on main")
    end_exclusive = (pd.Timestamp(args.end_date) + pd.Timedelta(days=1)).date().isoformat()
    # Download observed ETF histories cleanly; QQQM starts on 2020-10-13 without fake back-filling.
    prices = download_price_history_with_proxy_candidates(list(SYMBOLS), start=DOWNLOAD_START, end=end_exclusive)
    output = run(prices, tqqq_growth_income_targets, end_date=args.end_date, reference=tqqq_growth_income_reference)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "report.md").write_text(render_markdown(output), encoding="utf-8")
    payload = {
        "rows": output["rows"].to_dict("records"),
        "summary": output["summary"],
        "live_ready": False,
        "no_order": True,
        "income_layer_active_on_starting_equity": False,
        "income_layer_start_usd": 250000.0,
    }
    safe = costs._json_safe(payload)
    (args.output_dir / "results.json").write_text(json.dumps(safe, indent=2, allow_nan=False), encoding="utf-8")
    print(render_markdown(output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
