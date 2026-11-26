"""Pre-registered small-account cost comparison: 3x strategies vs their 1x twins (research only).

Question (pre-registered 2026-11-26): for a USD 1,000-2,000 IBKR account of a China tax
resident on the balanced tier (max drawdown target about 30%), does each current 3x ETF
strategy beat its 1x twin (same signal, same weights, 1x instruments) after every cost?

Rules fixed before running:
- No tuning. Each 3x proxy keeps its frozen weights and signal; the twin only swaps the
  traded instruments (TQQQ/QLD -> QQQ, SOXL -> SOXX, SOXX -> BIL in the SOXL risk-off leg).
- Costs, on a fresh USD 1,500 account per period: holdings drift between signal changes;
  every trade to a new target pays its own symbol's bid-ask spread (1 bp 1x ETFs, 3 bp
  2x/3x ETFs; stress x2, x3) and one IBKR minimum commission (USD 0.35); 20% China tax on
  each calendar year's positive gain (an upper bound treating gains as realized yearly).
  Fund expense ratios and 3x financing are already inside adjusted prices.
- Periods: full 2011-04-01 onward, and 2023-01-01 onward reported separately (a later
  historical subperiod, not an untouched holdout). Prices download from 2009-01-01 to warm
  up the 200-session trend filter; missing coverage of any required symbol fails the run.
- Decision: the 1x twin is preferred when its max drawdown is within 30% and its net Calmar
  is not lower than the 3x proxy's; a 3x proxy whose drawdown exceeds 30% is ruled out on
  the balanced tier regardless of return. Anything else is reported to the Owner, and no
  strategy is changed here.

Constant: live_ready=False, no_order=True. Every variant run is listed in the trial registry.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import dataclass, replace
from pathlib import Path

import pandas as pd

from .leveraged_strategy_candidates import (
    DEFAULT_SAFE_SYMBOL,
    LEVERAGED_CANDIDATES,
    LeveragedCandidateSpec,
    _normalize_price_history,
    run_candidate_backtest,
)

PREREGISTRATION_DATE = "2026-11-26"
FULL_START = "2011-04-01"  # SOXL trades from 2010-03; a year of history warms up every signal
OOS_START = "2023-01-01"
STARTING_EQUITY_USD = 1500.0
MIN_COMMISSION_USD = 0.35
CN_TAX_RATE = 0.20
SPREAD_BPS_1X = 1.0
SPREAD_BPS_LEVERED = 3.0
STRESS_MULTIPLIERS = (1.0, 2.0, 3.0)
BALANCED_MAX_DRAWDOWN = 0.30
LEVERED_SYMBOLS = frozenset({"TQQQ", "QLD", "SOXL"})
TWIN_SWAP = {"TQQQ": "QQQ", "QLD": "QQQ", "SOXL": "SOXX"}
PROXY_IDS = ("live_tqqq_dual_drive_45_45_proxy", "live_soxl_soxx_trend_57_proxy")


@dataclass(frozen=True)
class Variant:
    spec: LeveragedCandidateSpec
    label: str
    pair: str
    leverage: str  # "3x" or "1x"


def _merge(pairs) -> dict[str, float]:
    merged: dict[str, float] = {}
    for symbol, weight in pairs:
        merged[symbol] = merged.get(symbol, 0.0) + float(weight)
    return merged


def one_x_twin(spec: LeveragedCandidateSpec) -> LeveragedCandidateSpec:
    """Same signal and weights, levered holdings swapped for 1x.

    A risk-off leg that holds the 1x underlying of a levered risk-on holding (the SOXL proxy
    parks in SOXX) moves one notch lower, to cash, so the twin stays one notch less levered.
    """
    risk_on_symbols = {s.upper() for s in spec.risk_on_weights}
    levered_underlyings = {TWIN_SWAP[s] for s in risk_on_symbols if s in TWIN_SWAP}
    risk_on = _merge((TWIN_SWAP.get(s.upper(), s.upper()), w) for s, w in spec.risk_on_weights.items())
    risk_off = _merge(
        (DEFAULT_SAFE_SYMBOL if s.upper() in levered_underlyings and s.upper() not in risk_on_symbols
         else TWIN_SWAP.get(s.upper(), s.upper()), w)
        for s, w in spec.risk_off_weights.items())
    return replace(spec, candidate_id=f"{spec.candidate_id}__1x_twin", display_name=f"{spec.display_name} (1x twin)",
                   risk_on_weights=risk_on, risk_off_weights=risk_off,
                   notes=f"1x twin of {spec.candidate_id}: same signal, 1x instruments.")


def build_variants() -> list[Variant]:
    by_id = {spec.candidate_id: spec for spec in LEVERAGED_CANDIDATES}
    variants = []
    for proxy_id in PROXY_IDS:
        spec = by_id[proxy_id]
        variants.append(Variant(spec, f"{proxy_id} (3x)", proxy_id, "3x"))
        twin = one_x_twin(spec)
        variants.append(Variant(twin, f"{proxy_id} (1x twin)", proxy_id, "1x"))
    return variants


AUTHORIZATION_ID = "small-account-cost-comparison-2026-11-26"
DOWNLOAD_START = "2009-01-01"  # warm-up for the 200-session trend filter before FULL_START
MIN_COVERAGE = 0.99


def symbol_spread_bps(symbol: str) -> float:
    return SPREAD_BPS_LEVERED if symbol.upper() in LEVERED_SYMBOLS else SPREAD_BPS_1X


def required_symbols() -> list[str]:
    return sorted({s.upper() for v in build_variants()
                   for s in [*v.spec.risk_on_weights, *v.spec.risk_off_weights, v.spec.signal_symbol]} | {"SPY"})


def validate_coverage(price_history: pd.DataFrame, start: str, end: str | None = None,
                      *, max_gap_days: int = 5) -> None:
    """Fail unless every required symbol trades on every session of the window.

    Sessions are the union of dates any symbol traded. Each required symbol must cover all
    of them, start before the window (signal warm-up), reach the window end, and never skip
    more than ``max_gap_days`` calendar days, so a stale forward-filled close cannot be traded.
    """
    frame = pd.DataFrame(price_history).copy()
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    frame["as_of"] = pd.to_datetime(frame["as_of"]).dt.tz_localize(None).dt.normalize()
    frame = frame.dropna(subset=["close"])
    in_window = frame["as_of"] >= pd.Timestamp(start)
    if end:
        in_window &= frame["as_of"] <= pd.Timestamp(end)
    observed = pd.DatetimeIndex(sorted(frame.loc[in_window, "as_of"].unique()))
    if len(observed) == 0:
        raise ValueError("price history has no sessions in the evaluation window")
    import exchange_calendars as xcals  # independent completeness reference, not the download itself

    last = pd.Timestamp(end) if end else observed[-1]
    sessions = pd.DatetimeIndex(xcals.get_calendar("XNYS").sessions_in_range(
        max(pd.Timestamp(start), pd.Timestamp("2006-01-03")), last)).tz_localize(None).normalize()
    if end and (pd.Timestamp(end) - observed[-1]).days > max_gap_days:
        raise ValueError(f"price history ends {observed[-1].date()}, before {end}")
    signal_windows: dict[str, int] = {}
    for variant in build_variants():
        symbol = variant.spec.signal_symbol.upper()
        signal_windows[symbol] = max(signal_windows.get(symbol, 0), int(variant.spec.trend_window))
    for symbol in required_symbols():
        dates = pd.DatetimeIndex(sorted(frame.loc[frame["symbol"].eq(symbol), "as_of"].unique()))
        if len(dates) == 0 or dates.min() > pd.Timestamp(start):
            raise ValueError(f"price history does not cover {symbol} from {start}")
        if symbol in signal_windows and (dates < pd.Timestamp(start)).sum() < signal_windows[symbol]:
            raise ValueError(f"price history for signal {symbol} is too short to warm up before {start}")
        missing = sessions.difference(dates)
        if len(missing):
            raise ValueError(f"price history for {symbol} misses {len(missing)} sessions, first {missing[0].date()}")
        in_range = dates[(dates >= sessions[0]) & (dates <= sessions[-1])]
        if len(in_range) > 1 and in_range.to_series().diff().dt.days.max() > max_gap_days:
            raise ValueError(f"price history for {symbol} has a gap longer than {max_gap_days} days")


def simulate_account(targets: pd.DataFrame, returns: pd.DataFrame, *, spread_multiplier: float = 1.0,
                     starting_equity: float = STARTING_EQUITY_USD, min_commission: float = MIN_COMMISSION_USD,
                     tax_rate: float = CN_TAX_RATE, min_change: float = 1e-12) -> pd.Series:
    """Small-account path: holdings drift between signal changes; every trade pays costs.

    targets: target weights applied to each session's returns (index = session).
    The account opens with a trade at its first session's close and earns returns from the
    next session. Whenever the target changes later, it trades to the new target at the
    prior close: each traded symbol pays its own spread on the traded
    notional and one minimum commission. Between changes the holdings drift. At each new
    calendar year, tax_rate of the prior year's positive gain is paid pro rata.
    """
    symbols = list(targets.columns)
    holdings = {s: 0.0 for s in symbols}
    cash = starting_equity
    previous_target = None
    first_session = True
    year, year_start = None, starting_equity
    out = []
    for session, target_row in targets.iterrows():
        value = cash + sum(holdings.values())
        opening = value
        if year is not None and session.year != year:
            gain = value - year_start
            if gain > 0 and value > 0:
                scale = 1.0 - tax_rate * gain / value
                # Paying the tax sells part of every holding; each sale pays its costs.
                sale_costs = sum(h * (1.0 - scale) * symbol_spread_bps(s) * spread_multiplier / 10_000.0
                                 + min_commission for s, h in holdings.items() if h * (1.0 - scale) > 0.01)
                holdings = {s: h * scale for s, h in holdings.items()}
                cash = cash * scale - sale_costs
                value = cash + sum(holdings.values())
            year_start = value
        year = session.year
        target = {s: float(target_row[s]) for s in symbols}
        if previous_target is None or any(abs(target[s] - previous_target[s]) > min_change for s in symbols):
            costs = 0.0
            for s in symbols:
                trade = target[s] * value - holdings[s]
                if abs(trade) > 0.01:
                    costs += abs(trade) * symbol_spread_bps(s) * spread_multiplier / 10_000.0 + min_commission
            value = max(value - costs, 0.0)
            holdings = {s: target[s] * value for s in symbols}
            cash = value - sum(holdings.values())
            previous_target = target
        if first_session:
            # A fresh account opens at its first session's close: no return before that trade.
            first_session = False
            closing = cash + sum(holdings.values())
            out.append(closing / opening - 1.0 if opening > 0 else 0.0)
            continue
        for s in symbols:
            r = returns.at[session, s] if s in returns.columns else float("nan")
            if pd.isna(r):
                if holdings[s]:
                    raise ValueError(f"missing return for held {s} on {session.date()}")
                continue
            holdings[s] *= 1.0 + float(r)
        closing = cash + sum(holdings.values())
        out.append(closing / opening - 1.0 if opening > 0 else 0.0)
    return pd.Series(out, index=targets.index, name="net_return")


def metrics(returns: pd.Series) -> dict[str, float]:
    clean = returns.dropna()
    equity = (1.0 + clean).cumprod()
    peak = equity.cummax().clip(lower=1.0)  # the opening capital is the first high-water mark
    years = max((clean.index[-1] - clean.index[0]).days / 365.25, 1 / 365.25)
    cagr = float(equity.iloc[-1] ** (1.0 / years) - 1.0)
    max_dd = float((equity / peak - 1.0).min())
    std = float(clean.std(ddof=0))
    sharpe = float(clean.mean() / std * math.sqrt(252)) if std else float("nan")
    return {"cagr": cagr, "max_drawdown": min(max_dd, 0.0),
            "calmar": cagr / abs(max_dd) if max_dd < 0 else float("nan"),
            "sharpe": sharpe, "sessions": int(len(clean)), "skew": float(clean.skew()),
            "kurtosis": float(clean.kurt() + 3.0)}


def deflated_sharpe_probability(sharpe_annual: float, sessions: int, skew: float, kurtosis: float,
                                trial_sharpes: list[float]) -> float:
    """Bailey & Lopez de Prado DSR: probability the true Sharpe exceeds the best-of-N expectation."""
    from statistics import NormalDist, pvariance
    norm = NormalDist()
    n = len(trial_sharpes)
    sr = sharpe_annual / math.sqrt(252)
    if n < 2 or sessions < 3 or math.isnan(sr):
        return float("nan")
    var = pvariance([s / math.sqrt(252) for s in trial_sharpes]) or 1e-12
    gamma = 0.5772156649
    sr0 = math.sqrt(var) * ((1 - gamma) * norm.inv_cdf(1 - 1 / n) + gamma * norm.inv_cdf(1 - 1 / (n * math.e)))
    denom = math.sqrt(max(1 - skew * sr + (kurtosis - 1) / 4 * sr * sr, 1e-12))
    return float(norm.cdf((sr - sr0) * math.sqrt(sessions - 1) / denom))


def run(price_history: pd.DataFrame, *, end_date: str | None = None) -> dict:
    validate_coverage(price_history, FULL_START, end_date)
    close = _normalize_price_history(price_history)
    if end_date:
        close = close.loc[close.index <= pd.Timestamp(end_date)]
    returns = close.pct_change(fill_method=None)
    registry, rows = [], []
    for variant in build_variants():
        result = run_candidate_backtest(price_history, variant.spec, start_date=FULL_START, end_date=end_date,
                                        turnover_cost_bps=0.0)
        # A target chosen from close T is executed at close T+1, so it earns returns from T+2.
        targets = pd.DataFrame(result["weights_history"]).fillna(0.0).shift(1).dropna()
        for multiplier in STRESS_MULTIPLIERS:
            for period, start in (("full", FULL_START), ("oos", OOS_START)):
                window = targets.loc[targets.index >= pd.Timestamp(start)]
                if len(window) < 2:
                    raise ValueError(f"price history cannot produce the registered {period} period")
                # Each period is a fresh USD 1,500 account opened at the period start.
                net = simulate_account(window, returns, spread_multiplier=multiplier)
                rows.append({"pair": variant.pair, "variant": variant.label, "leverage": variant.leverage,
                             "stress": multiplier, "period": period, **metrics(net)})
            registry.append({"trial": variant.label, "spread_stress": multiplier,
                             "registered_on": PREREGISTRATION_DATE})
    frame = pd.DataFrame(rows)
    frame["dsr"] = float("nan")
    for period, group in frame.groupby("period"):
        trial_sharpes = group["sharpe"].tolist()
        frame.loc[group.index, "dsr"] = [
            deflated_sharpe_probability(r.sharpe, r.sessions, r.skew, r.kurtosis, trial_sharpes)
            for r in group.itertuples()]
    return {"rows": frame, "registry": registry, "decisions": decide(frame)}


def decide(frame: pd.DataFrame) -> list[dict]:
    """Apply the pre-registered rules at base costs on the full period; rules are independent."""
    decisions = []
    base = frame.loc[frame["stress"].eq(1.0) & frame["period"].eq("full")]
    for pair, group in base.groupby("pair", sort=False):
        three = group.loc[group["leverage"].eq("3x")].iloc[0]
        one = group.loc[group["leverage"].eq("1x")].iloc[0]
        three_out = abs(three["max_drawdown"]) > BALANCED_MAX_DRAWDOWN
        one_within = abs(one["max_drawdown"]) <= BALANCED_MAX_DRAWDOWN
        prefer_twin = one_within and one["calmar"] >= three["calmar"]
        verdicts = []
        if three_out:
            verdicts.append("rule_out_3x_on_balanced_tier")
        if prefer_twin:
            verdicts.append("prefer_1x_twin")
        if not verdicts or (three_out and not one_within):
            verdicts.append("report_to_owner")
        decisions.append({"pair": pair, "verdicts": verdicts, "three_x_out_on_balanced": bool(three_out),
                          "one_x_within_cap": bool(one_within),
                          "one_x_drawdown": float(one["max_drawdown"]), "three_x_drawdown": float(three["max_drawdown"]),
                          "one_x_calmar": float(one["calmar"]), "three_x_calmar": float(three["calmar"])})
    return decisions


def render_markdown(output: dict) -> str:
    rows = output["rows"]
    lines = ["# Small-account cost comparison (pre-registered 2026-11-26, research only)", "",
             "live_ready=False · no_order=True · each period is a fresh USD 1,500 account; holdings drift between"
             " signal changes; each trade pays its own spread and a USD 0.35 minimum commission; 20% CN tax on"
             " positive yearly gains; expense ratios already in adjusted prices.", "",
             "| Pair | Variant | Period | Spread stress | CAGR | Max DD | Calmar | Sharpe | DSR |",
             "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for r in rows.itertuples():
        lines.append(f"| {r.pair} | {r.leverage} | {r.period} | x{r.stress:.0f} | {r.cagr:.1%} | {r.max_drawdown:.1%}"
                     f" | {r.calmar:.2f} | {r.sharpe:.2f} | {r.dsr:.2f} |")
    lines += ["", "## Decisions (base costs, full period)", ""]
    for d in output["decisions"]:
        lines.append(f"- {d['pair']}: **{', '.join(d['verdicts'])}** (1x DD {d['one_x_drawdown']:.1%},"
                     f" Calmar {d['one_x_calmar']:.2f}; 3x DD {d['three_x_drawdown']:.1%}, Calmar {d['three_x_calmar']:.2f})")
    lines += ["", f"Trial registry: {len(output['registry'])} trials (every variant x cost tier run).",
              "The 2023+ period is a later historical subperiod, not an untouched holdout."]
    return "\n".join(lines) + "\n"


def _json_safe(value):
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


STUDY_ID = "us_equity_snapshot_pipelines.research.small_account_cost_comparison"
MANDATE_PATH = Path("config/research_mandates") / f"{AUTHORIZATION_ID}.json"
# The reviewed mandate is pinned here: changing either needs a reviewed, merged PR.
MANDATE_SHA256 = "3e65f961b4d3ad00efbaf4607ebc45aef3cef9e7413e83bda88931f8b8fcf785"


def verify_mandate(path: Path, end_date: str, *, now: pd.Timestamp | None = None) -> dict:
    """Accept only the merged, unexpired mandate whose scope covers this exact run."""
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != MANDATE_SHA256:
        raise SystemExit("research data mandate rejected: not the reviewed record")
    record = json.loads(raw.decode("utf-8"))
    scope = record.get("scope") or {}
    attestation = record.get("attestation") or {}
    now = now or pd.Timestamp.now(tz="UTC")
    problems = []
    if record.get("mandate_id") != AUTHORIZATION_ID or record.get("study") != STUDY_ID:
        problems.append("identity")
    if scope.get("provider") != "YAHOO_FINANCE_PUBLIC_ADJUSTED_CLOSE" or scope.get("download_start") != DOWNLOAD_START:
        problems.append("source")
    if sorted(scope.get("symbols") or []) != required_symbols():
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


def main(argv=None) -> int:
    from ..yfinance_prices import download_price_history

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--end-date", required=True, help="last price date (YYYY-MM-DD), within the mandate")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    verify_mandate(MANDATE_PATH, args.end_date)  # before any provider access
    end_exclusive = (pd.Timestamp(args.end_date) + pd.Timedelta(days=1)).date().isoformat()
    prices = download_price_history(required_symbols(), start=DOWNLOAD_START, end=end_exclusive)
    output = run(prices, end_date=args.end_date)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "report.md").write_text(render_markdown(output), encoding="utf-8")
    payload = {"rows": output["rows"].to_dict("records"), "decisions": output["decisions"],
               "registry": output["registry"], "live_ready": False, "no_order": True}
    (args.output_dir / "results.json").write_text(json.dumps(_json_safe(payload), indent=2, default=str,
                                                             allow_nan=False), encoding="utf-8")
    print(render_markdown(output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
