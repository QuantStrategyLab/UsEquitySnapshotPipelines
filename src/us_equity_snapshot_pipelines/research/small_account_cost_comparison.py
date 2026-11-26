"""Pre-registered small-account cost comparison: 3x strategies vs their 1x twins (research only).

Question (pre-registered 2026-11-26): for a USD 1,000-2,000 IBKR account of a China tax
resident on the balanced tier (max drawdown target about 30%), does each current 3x ETF
strategy beat its 1x twin (same signal, same weights, 1x instruments) after every cost?

Rules fixed before running:
- No tuning. Each 3x proxy keeps its frozen weights and signal; the twin only swaps the
  traded instruments (TQQQ/QLD -> QQQ, SOXL -> SOXX, SOXX -> BIL in the SOXL risk-off leg).
- Costs: bid-ask spread on turnover (1 bp for 1x ETFs, 3 bp for 2x/3x ETFs; stress x2, x3);
  IBKR minimum commission per order (USD 0.35) on the simulated small-account equity path;
  20% China tax on each calendar year's positive return (a conservative upper bound that
  treats all gains as realized yearly; the 10% US dividend withholding is creditable
  against it). Fund expense ratios and 3x financing are already inside adjusted prices
  and are not deducted again.
- Periods: full 2010-04-01 onward, and out-of-sample 2023-01-01 onward reported separately.
- Decision: the 1x twin is preferred when its max drawdown is within 30% and its net Calmar
  is not lower than the 3x proxy's; a 3x proxy whose drawdown exceeds 30% is ruled out on
  the balanced tier regardless of return. Anything else is reported to the Owner, and no
  strategy is changed here.

Constant: live_ready=False, no_order=True. Every variant run is listed in the trial registry.
"""
from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass, replace
from pathlib import Path

import pandas as pd

from .leveraged_strategy_candidates import (
    DEFAULT_SAFE_SYMBOL,
    LEVERAGED_CANDIDATES,
    LeveragedCandidateSpec,
    run_candidate_backtest,
)

PREREGISTRATION_DATE = "2026-11-26"
FULL_START = "2010-04-01"
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


def spread_bps(spec: LeveragedCandidateSpec) -> float:
    symbols = {s.upper() for s in spec.risk_on_weights} | {s.upper() for s in spec.risk_off_weights}
    return SPREAD_BPS_LEVERED if symbols & LEVERED_SYMBOLS else SPREAD_BPS_1X


def apply_small_account_costs(result: dict, *, starting_equity: float = STARTING_EQUITY_USD,
                              min_commission: float = MIN_COMMISSION_USD, tax_rate: float = CN_TAX_RATE) -> pd.Series:
    """Deduct per-order minimum commissions on the equity path, then yearly tax on positive returns."""
    returns = pd.Series(result["portfolio_returns"], dtype=float)
    weights = pd.DataFrame(result["weights_history"]).reindex(returns.index).fillna(0.0)
    orders = weights.diff().abs().gt(1e-12).sum(axis=1)
    orders.iloc[0] = int(weights.iloc[0].gt(1e-12).sum())
    net = []
    equity = starting_equity
    year, year_start = None, starting_equity
    for as_of, gross in returns.items():
        before = equity  # opening equity, before any tax paid today
        if year is not None and as_of.year != year:
            gain = equity - year_start
            if gain > 0:
                equity -= tax_rate * gain  # tax paid at the first session of the next year
            year_start = equity
        year = as_of.year
        equity = equity * (1.0 + float(gross)) - min_commission * int(orders.at[as_of])
        equity = max(equity, 0.0)
        net.append(equity / before - 1.0 if before > 0 else 0.0)
    return pd.Series(net, index=returns.index, name="net_return")


def metrics(returns: pd.Series) -> dict[str, float]:
    clean = returns.dropna()
    equity = (1.0 + clean).cumprod()
    years = max((clean.index[-1] - clean.index[0]).days / 365.25, 1 / 365.25)
    cagr = float(equity.iloc[-1] ** (1.0 / years) - 1.0)
    max_dd = float((equity / equity.cummax() - 1.0).min())
    std = float(clean.std(ddof=0))
    sharpe = float(clean.mean() / std * math.sqrt(252)) if std else float("nan")
    return {"cagr": cagr, "max_drawdown": max_dd, "calmar": cagr / abs(max_dd) if max_dd < 0 else float("nan"),
            "sharpe": sharpe, "sessions": int(len(clean)), "skew": float(clean.skew()),
            "kurtosis": float(clean.kurt() + 3.0)}


def deflated_sharpe_probability(sharpe_annual: float, sessions: int, skew: float, kurtosis: float,
                                trial_sharpes: list[float]) -> float:
    """Bailey & Lopez de Prado DSR: probability the true Sharpe exceeds the best-of-N expectation."""
    from statistics import NormalDist, pvariance
    norm = NormalDist()
    n = len(trial_sharpes)
    sr = sharpe_annual / math.sqrt(252)
    if n < 2 or sessions < 3:
        return float("nan")
    var = pvariance([s / math.sqrt(252) for s in trial_sharpes]) or 1e-12
    gamma = 0.5772156649
    sr0 = math.sqrt(var) * ((1 - gamma) * norm.inv_cdf(1 - 1 / n) + gamma * norm.inv_cdf(1 - 1 / (n * math.e)))
    denom = math.sqrt(max(1 - skew * sr + (kurtosis - 1) / 4 * sr * sr, 1e-12))
    return float(norm.cdf((sr - sr0) * math.sqrt(sessions - 1) / denom))


def run(price_history: pd.DataFrame, *, end_date: str | None = None) -> dict:
    registry, rows = [], []
    for variant in build_variants():
        base_bps = spread_bps(variant.spec)
        for multiplier in STRESS_MULTIPLIERS:
            result = run_candidate_backtest(price_history, variant.spec, start_date=FULL_START, end_date=end_date,
                                            turnover_cost_bps=base_bps * multiplier)
            net = apply_small_account_costs(result)
            for period, start in (("full", FULL_START), ("oos", OOS_START)):
                window = net.loc[net.index >= pd.Timestamp(start)]
                if len(window) < 2:
                    continue  # period not covered by the available history
                row = {"pair": variant.pair, "variant": variant.label, "leverage": variant.leverage,
                       "spread_bps": base_bps * multiplier, "stress": multiplier, "period": period,
                       **metrics(window)}
                rows.append(row)
            registry.append({"trial": variant.label, "spread_bps": base_bps * multiplier,
                             "registered_on": PREREGISTRATION_DATE})
    frame = pd.DataFrame(rows)
    trial_sharpes = frame.loc[frame["period"].eq("full"), "sharpe"].tolist()
    frame["dsr"] = [deflated_sharpe_probability(r.sharpe, r.sessions, r.skew, r.kurtosis, trial_sharpes)
                    for r in frame.itertuples()]
    return {"rows": frame, "registry": registry, "decisions": decide(frame)}


def decide(frame: pd.DataFrame) -> list[dict]:
    """Apply the pre-registered rule at base costs (stress 1x) on the full period."""
    decisions = []
    base = frame.loc[frame["stress"].eq(1.0) & frame["period"].eq("full")]
    for pair, group in base.groupby("pair", sort=False):
        three = group.loc[group["leverage"].eq("3x")].iloc[0]
        one = group.loc[group["leverage"].eq("1x")].iloc[0]
        three_out = abs(three["max_drawdown"]) > BALANCED_MAX_DRAWDOWN
        one_ok = abs(one["max_drawdown"]) <= BALANCED_MAX_DRAWDOWN and one["calmar"] >= three["calmar"]
        if one_ok:
            verdict = "prefer_1x_twin"
        elif three_out and abs(one["max_drawdown"]) <= BALANCED_MAX_DRAWDOWN:
            verdict = "rule_out_3x_on_balanced_tier"
        else:
            verdict = "report_to_owner"
        decisions.append({"pair": pair, "verdict": verdict, "three_x_out_on_balanced": bool(three_out),
                          "one_x_drawdown": float(one["max_drawdown"]), "three_x_drawdown": float(three["max_drawdown"]),
                          "one_x_calmar": float(one["calmar"]), "three_x_calmar": float(three["calmar"])})
    return decisions


def render_markdown(output: dict) -> str:
    rows = output["rows"]
    lines = ["# Small-account cost comparison (pre-registered 2026-11-26, research only)", "",
             "live_ready=False · no_order=True · costs: spread + USD 0.35 min commission on a USD 1,500 path"
             " + 20% CN tax on positive yearly returns; expense ratios already in adjusted prices.", "",
             "| Pair | Variant | Period | Spread bps | CAGR | Max DD | Calmar | Sharpe | DSR |",
             "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for r in rows.itertuples():
        lines.append(f"| {r.pair} | {r.leverage} | {r.period} | {r.spread_bps:.0f} | {r.cagr:.1%} | {r.max_drawdown:.1%}"
                     f" | {r.calmar:.2f} | {r.sharpe:.2f} | {r.dsr:.2f} |")
    lines += ["", "## Decisions (base costs, full period)", ""]
    for d in output["decisions"]:
        lines.append(f"- {d['pair']}: **{d['verdict']}** (1x DD {d['one_x_drawdown']:.1%}, Calmar {d['one_x_calmar']:.2f};"
                     f" 3x DD {d['three_x_drawdown']:.1%}, Calmar {d['three_x_calmar']:.2f})")
    lines += ["", f"Trial registry: {len(output['registry'])} trials (every variant x cost tier run)."]
    return "\n".join(lines) + "\n"


def main(argv=None) -> int:
    from ..yfinance_prices import download_price_history

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    symbols = sorted({s.upper() for v in build_variants()
                      for s in [*v.spec.risk_on_weights, *v.spec.risk_off_weights, v.spec.signal_symbol, "SPY"]})
    prices = download_price_history(symbols, start="2009-01-01", end=args.end_date)
    output = run(prices, end_date=args.end_date)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "report.md").write_text(render_markdown(output), encoding="utf-8")
    (args.output_dir / "results.json").write_text(json.dumps(
        {"rows": output["rows"].to_dict("records"), "decisions": output["decisions"], "registry": output["registry"],
         "live_ready": False, "no_order": True}, indent=2, default=str), encoding="utf-8")
    print(render_markdown(output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
