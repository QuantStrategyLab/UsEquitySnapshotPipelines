"""Bounded, research-only Russell signal replay.

This adapter deliberately consumes the strategy's signal calculator rather than
the production entrypoint.  The latter applies account/risk and telemetry
side-effects that are unavailable to an offline research replay.
"""
from __future__ import annotations

import argparse
import json
import math
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from quant_platform_kit.strategy_lifecycle.backtest_orchestrator import BacktestOrchestrator
from quant_platform_kit.strategy_lifecycle.contracts import BacktestResult
from us_equity_strategies.catalog import get_strategy_definition
from us_equity_strategies.strategies import mega_cap_leader_rotation as strategy

PROFILE = "russell_top50_leader_rotation"
RUNNER_KIND = "research_only"
DATA_KINDS = frozenset({"synthetic", "research"})
RESEARCH_FLAGS = {
    "runner_kind": RUNNER_KIND,
    "learning_only": True,
    "promotion_eligible": False,
    "live_ready": False,
    "size_zero_required": True,
    "no_order": True,
    "runtime_parity_verified": False,
    "research_scope": "core_signal_only",
    "excluded_layers": ("income", "option_overlay", "account_risk", "live_execution"),
}
_PARAM_KEYS = frozenset({"variant", "data_kind", "equity", "cost_bps", *RESEARCH_FLAGS})


class _MemoryStore:
    def __init__(self) -> None:
        self.saved = []

    def save_backtest_result(self, result):
        self.saved.append(result)


def _require_frame(params: Mapping[str, Any], key: str, columns: tuple[str, ...]) -> pd.DataFrame:
    frame = params.get(key)
    if not isinstance(frame, pd.DataFrame):
        raise TypeError(f"{key} must be a pandas DataFrame")
    missing = [col for col in columns if col not in frame.columns]
    if missing:
        raise ValueError(f"{key} missing columns: {missing}")
    return frame.copy()


def _clean_inputs(params: Mapping[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, float, float]:
    features = _require_frame(
        params,
        "feature_snapshots",
        ("as_of", "available_at", *tuple(strategy.REQUIRED_FEATURE_COLUMNS - {"as_of"})),
    )
    prices = _require_frame(params, "prices", ("session", "symbol", "open", "close"))
    features["as_of"] = pd.to_datetime(features["as_of"], errors="coerce")
    prices["session"] = pd.to_datetime(prices["session"], errors="coerce")
    available = pd.to_datetime(features["available_at"], errors="coerce")
    if available.isna().any() or available.dt.tz is None:
        raise ValueError("available_at must be timezone-aware")
    features["available_at"] = available.dt.tz_convert("UTC")
    if features["as_of"].isna().any() or prices["session"].isna().any():
        raise ValueError("invalid session or as_of")
    if (features["as_of"] != features["as_of"].dt.normalize()).any() or (prices["session"] != prices["session"].dt.normalize()).any():
        raise ValueError("session and as_of must be date-only")
    for frame in (features, prices):
        if frame["symbol"].isna().any():
            raise ValueError("invalid symbol null")
        raw = frame["symbol"].astype(str)
        if raw.ne(raw.str.strip()).any() or raw.str.strip().eq("").any():
            raise ValueError("invalid symbol whitespace")
        frame["symbol"] = raw.str.strip()
    if features.duplicated(["as_of", "symbol"]).any() or prices.duplicated(["session", "symbol"]).any():
        raise ValueError("duplicate snapshot or price rows")
    numeric_features = tuple(strategy.REQUIRED_FEATURE_COLUMNS - {"symbol", "sector"})
    for col in numeric_features:
        values = pd.to_numeric(features[col], errors="coerce")
        if (~np.isfinite(values)).any():
            raise ValueError(f"invalid feature: {col}")
        features[col] = values
    for as_of, group in features.groupby("as_of"):
        if not {"QQQ", "SPY", "BOXX"}.issubset(set(group.symbol)):
            raise ValueError(f"feature snapshot {as_of.date()} missing benchmark")
    for col in ("open", "close"):
        values = pd.to_numeric(prices[col], errors="coerce")
        if (~np.isfinite(values)).any() or (values <= 0).any():
            raise ValueError(f"invalid price: {col}")
        prices[col] = values.astype(float)
    equity = float(params.get("equity", 100_000.0))
    bps = float(params.get("cost_bps", 0.0))
    if not math.isfinite(equity) or equity <= 0 or not math.isfinite(bps) or not 0 <= bps < 10_000:
        raise ValueError("equity must be positive and 0 <= cost_bps < 10000")
    if params.get("data_kind") not in DATA_KINDS:
        raise ValueError("data_kind must be synthetic or research")
    if prices.empty:
        raise ValueError("empty price range")
    try:
        import exchange_calendars as xcals
        sessions = xcals.get_calendar("XNYS").sessions_in_range(prices.session.min(), prices.session.max())
        expected = {pd.Timestamp(x).date() for x in sessions}
        actual = {x.date() for x in prices.session.unique()}
        if actual != expected:
            raise ValueError("prices do not contain the complete XNYS session range")
    except ImportError:
        raise ValueError("exchange_calendars is required for session validation") from None
    return features, prices, equity, bps


def _latest_snapshot(features: pd.DataFrame, session: pd.Timestamp, used: set[pd.Timestamp]):
    import exchange_calendars as xcals
    calendar = xcals.get_calendar("XNYS")
    cutoff = calendar.schedule.loc[pd.Timestamp(session.date()), "open"]
    eligible = features[features["as_of"] < session.normalize()]
    if eligible.empty:
        return None, "no_snapshot_available_before_open"
    usable = []
    for as_of, group in eligible.groupby("as_of"):
        if (group["available_at"] < cutoff).all():
            usable.append((pd.Timestamp(as_of), group))
    if not usable:
        return None, "no_snapshot_available_before_open"
    newest, group = max(usable, key=lambda item: item[0])
    if newest in used:
        return None, "snapshot_already_used"
    return group.copy(), None


def _target_weights(snapshot: pd.DataFrame, session: pd.Timestamp, current: Mapping[str, float], equity: float, variant: str):
    config = {
        key: value
        for key, value in get_strategy_definition(PROFILE).default_config.items()
        if key in strategy.FEATURE_SIGNAL_KWARG_KEYS
    }
    config["leader_rotation_profile_variant"] = variant
    config["portfolio_total_equity"] = equity
    config["run_as_of"] = session.date().isoformat()
    config.pop("translator", None)
    weights, _desc, _defense, _status, diagnostics = strategy.compute_signals(
        snapshot, tuple(current), **config
    )
    return weights, diagnostics


def _rebalance(shares, cash, targets, prices, equity, bps):
    symbols = set(shares) | set(targets)
    open_px = {str(r.symbol): float(r.open) for r in prices.itertuples()}
    for symbol in symbols:
        if symbol not in open_px:
            raise ValueError(f"missing held or target price: {symbol}")
    if not math.isfinite(cash):
        raise ValueError("invalid cash")
    open_equity = cash + sum(shares.get(s, 0.0) * open_px[s] for s in shares)
    if not math.isfinite(open_equity) or open_equity <= 0:
        raise ValueError("non-positive open equity")
    target_weights = {str(symbol): float(weight) for symbol, weight in targets.items()}
    target_values = {symbol: weight for symbol, weight in target_weights.items()}
    if any(not math.isfinite(v) or v < 0 for v in target_values.values()):
        raise ValueError("invalid target weights")
    if sum(target_values.values()) > 1 + 1e-8:
        raise ValueError("target weights exceed 100%")

    def plan(net_equity):
        new = {s: net_equity * target_values.get(s, 0.0) / open_px[s] for s in symbols}
        fee = sum(abs(new[s] - shares.get(s, 0.0)) * open_px[s] for s in symbols) * bps / 10_000
        return new, open_equity - fee, fee

    lo, hi = 0.0, open_equity
    for _ in range(80):
        mid = (lo + hi) / 2
        _candidate, net_equity, fee = plan(mid)
        if mid + fee <= open_equity:
            lo = mid
        else:
            hi = mid
    new, net_equity, fee = plan(lo)
    cash_after = net_equity - sum(new[s] * open_px[s] for s in symbols)
    if cash_after < -1e-8:
        raise ValueError("transaction costs cannot be self-financed")
    trades = []
    for s in sorted(symbols):
        delta = new[s] - shares.get(s, 0.0)
        if abs(delta) > 1e-12:
            notional = abs(delta) * open_px[s]
            trades.append({"symbol": s, "shares": delta, "open": open_px[s], "notional": notional, "cost": notional * bps / 10_000})
    return {s: q for s, q in new.items() if abs(q) > 1e-12}, float(cash_after), trades


class RussellResearchRunner:
    runner_kind = RUNNER_KIND

    def __init__(self, *, feature_snapshots=None, prices=None, data_kind=None, initial_equity=100_000.0, cost_bps=0.0):
        if get_strategy_definition(PROFILE).profile != PROFILE:
            raise ValueError("unexpected strategy definition")
        self.last_artifacts: dict[str, pd.DataFrame] = {}
        self._data = {
            "feature_snapshots": feature_snapshots.copy() if isinstance(feature_snapshots, pd.DataFrame) else feature_snapshots,
            "prices": prices.copy() if isinstance(prices, pd.DataFrame) else prices,
            "data_kind": data_kind,
            "equity": initial_equity,
            "cost_bps": cost_bps,
        }

    def run(self, strategy_profile, params, start_date=None, end_date=None):
        self.last_artifacts = {}
        if strategy_profile != PROFILE:
            raise ValueError(f"unsupported profile: {strategy_profile}")
        unknown = set(params) - _PARAM_KEYS
        if unknown:
            raise ValueError(f"unsupported parameters: {sorted(unknown)}")
        for key, expected in RESEARCH_FLAGS.items():
            if key in params and params[key] != expected:
                raise ValueError(f"research flag cannot be changed: {key}")
        if "data_kind" in params and params["data_kind"] != self._data["data_kind"]:
            raise ValueError("data_kind cannot be changed after construction")
        for key in ("equity", "cost_bps"):
            if key in params and params[key] != self._data[key]:
                raise ValueError(f"{key} cannot be changed after construction")
        effective_params = dict(self._data)
        effective_params["variant"] = params.get("variant", strategy.PROFILE_VARIANT_BALANCED_BLEND)
        features, prices, initial_equity, bps = _clean_inputs(effective_params)
        variant = str(effective_params["variant"])
        if variant not in strategy.PROFILE_VARIANT_ALIASES:
            raise ValueError(f"unsupported variant: {variant}")
        sessions = sorted(prices.session.unique())
        if start_date is not None:
            sessions = [s for s in sessions if s.date() >= start_date]
        if end_date is not None:
            sessions = [s for s in sessions if s.date() <= end_date]
        if not sessions:
            raise ValueError("empty requested session range")
        shares: dict[str, float] = {}
        cash = initial_equity
        used: set[pd.Timestamp] = set()
        daily, trades = [], []
        previous_equity = initial_equity
        for session in sessions:
            session_prices = prices[prices.session == session]
            held = [s for s, q in shares.items() if abs(q) > 1e-12]
            close_px = {str(r.symbol): float(r.close) for r in session_prices.itertuples()}
            if any(s not in close_px for s in held):
                raise ValueError("missing held close price")
            snapshot, reason = _latest_snapshot(features, session, used)
            targets = None
            if snapshot is not None:
                as_of = pd.Timestamp(snapshot.as_of.iloc[0])
                weights, diagnostics = _target_weights(snapshot, session, shares, previous_equity, variant)
                used.add(as_of)
                reason = diagnostics.get("no_op_reason")
                if weights is not None:
                    targets = {str(k): float(v) for k, v in weights.items()}
            if targets is not None:
                shares, cash, day_trades = _rebalance(shares, cash, targets, session_prices, previous_equity, bps)
                for trade in day_trades:
                    trades.append({"session": session.date().isoformat(), **trade})
            equity = cash + sum(q * close_px[s] for s, q in shares.items())
            if not math.isfinite(equity) or equity <= 0:
                raise ValueError("non-positive or non-finite portfolio equity")
            row = {"session": session.date().isoformat(), "equity": equity, "return": equity / previous_equity - 1.0, "cash": cash, "snapshot_as_of": (snapshot.as_of.iloc[0].date().isoformat() if snapshot is not None else None), "no_op_reason": reason}
            row.update({f"shares_{s}": q for s, q in shares.items()})
            daily.append(row)
            previous_equity = equity
        daily_df, trades_df = pd.DataFrame(daily), pd.DataFrame(trades)
        self.last_artifacts = {"daily": daily_df, "trades": trades_df}
        equity_series = daily_df["equity"]
        returns = daily_df["return"]
        total_return = float(equity_series.iloc[-1] / initial_equity - 1.0)
        drawdown = equity_series / pd.concat([pd.Series([initial_equity]), equity_series], ignore_index=True).cummax().iloc[1:].to_numpy() - 1.0
        max_dd = float(drawdown.min())
        volatility = float(returns.std(ddof=1) * math.sqrt(252)) if len(returns) > 1 else None
        sharpe = float(returns.mean() / returns.std(ddof=1) * math.sqrt(252)) if len(returns) > 1 and returns.std(ddof=1) > 0 else None
        enriched = dict(effective_params)
        enriched.pop("feature_snapshots", None)
        enriched.pop("prices", None)
        enriched.update(RESEARCH_FLAGS, data_kind=effective_params["data_kind"], variant=variant)
        return BacktestResult(strategy_profile=PROFILE, domain="us_equity", param_set_id="russell_research", params=enriched, total_return=total_return, max_drawdown=max_dd, volatility=volatility, sharpe_ratio=sharpe, start_date=sessions[0].date(), end_date=sessions[-1].date(), observation_count=len(daily_df), source_script=__file__, cost_model="two_sided_notional_bps", cost_inputs={"cost_bps": bps})


def run_via_orchestrator(runner, params, *, store=None, start_date=None, end_date=None):
    store = store or _MemoryStore()
    orchestrator = BacktestOrchestrator(store=store)
    forbidden = set(params) & {"feature_snapshots", "prices", "equity", "cost_bps"}
    if forbidden:
        raise ValueError(f"constructor-only parameters: {sorted(forbidden)}")
    if "data_kind" in params and params["data_kind"] != runner._data["data_kind"]:
        raise ValueError("data_kind cannot be changed after construction")
    orchestrator.register_runner("us_equity", runner)
    persisted = dict(params)
    for key, expected in RESEARCH_FLAGS.items():
        if key in persisted and persisted[key] != expected:
            raise ValueError(f"research flag cannot be changed: {key}")
    persisted.update(RESEARCH_FLAGS, data_kind=runner._data["data_kind"], variant=params.get("variant", strategy.PROFILE_VARIANT_BALANCED_BLEND))
    persisted.update(equity=runner._data["equity"], cost_bps=runner._data["cost_bps"])
    return orchestrator.run(PROFILE, domain="us_equity", params=persisted, start_date=start_date, end_date=end_date)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--features", required=True)
    parser.add_argument("--prices", required=True)
    parser.add_argument("--data-kind", choices=sorted(DATA_KINDS), required=True)
    parser.add_argument("--equity", type=float, default=100000.0)
    parser.add_argument("--bps", type=float, default=0.0)
    parser.add_argument("--variant", default=strategy.PROFILE_VARIANT_BALANCED_BLEND)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    params = {
        "feature_snapshots": pd.read_csv(args.features),
        "prices": pd.read_csv(args.prices),
        "data_kind": args.data_kind,
        "equity": args.equity,
        "cost_bps": args.bps,
        "variant": args.variant,
    }
    runner = RussellResearchRunner(
        feature_snapshots=params["feature_snapshots"], prices=params["prices"],
        data_kind=args.data_kind, initial_equity=args.equity, cost_bps=args.bps,
    )
    result = run_via_orchestrator(runner, {"variant": args.variant})
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    runner.last_artifacts["daily"].to_csv(output / "daily.csv", index=False)
    runner.last_artifacts["trades"].to_csv(output / "trades.csv", index=False)
    (output / "result.json").write_text(json.dumps(result.to_dict(), allow_nan=False), encoding="utf-8")


if __name__ == "__main__":
    main()
