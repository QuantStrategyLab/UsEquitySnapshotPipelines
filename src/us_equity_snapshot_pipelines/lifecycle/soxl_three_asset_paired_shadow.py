"""One-session financial ledger for the frozen SOXL three-asset shadow pair."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable, Mapping
from typing import Any


SYMBOLS = ("SOXL", "SOXX", "BOXX")


class SoxlThreeAssetPairedShadowError(ValueError):
    """Raised when a paired observation cannot preserve financial causality."""


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SoxlThreeAssetPairedShadowError("invalid paired shadow value") from exc


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise SoxlThreeAssetPairedShadowError(f"invalid {label}")
    return json.loads(_canonical(value))


def _finite(value: object, label: str, *, nonnegative: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SoxlThreeAssetPairedShadowError(f"invalid {label}")
    number = float(value)
    if not math.isfinite(number) or (nonnegative and number < 0.0):
        raise SoxlThreeAssetPairedShadowError(f"invalid {label}")
    return number


def input_snapshot_sha256(value: Mapping[str, object]) -> str:
    """Digest a closed observation snapshot before either strategy leg sees it."""
    snapshot = _mapping(value, "input snapshot")
    snapshot.pop("input_snapshot_sha256", None)
    return hashlib.sha256(_canonical(snapshot)).hexdigest()


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _weights(value: object, label: str) -> dict[str, float]:
    raw = _mapping(value, label)
    if set(raw) != set(SYMBOLS):
        raise SoxlThreeAssetPairedShadowError(f"invalid {label}")
    weights = {
        symbol: _finite(raw[symbol], label, nonnegative=True)
        for symbol in SYMBOLS
    }
    if sum(weights.values()) > 1.0 + 1e-12:
        raise SoxlThreeAssetPairedShadowError(f"invalid {label}")
    return weights


def _state(value: object, label: str) -> dict[str, object]:
    raw = _mapping(value, label)
    if set(raw) != {
        "cash",
        "quantities",
        "pending_target_weights",
        "pending_cash_weight",
        "previous_equity",
    }:
        raise SoxlThreeAssetPairedShadowError(f"invalid {label}")
    quantities = _mapping(raw["quantities"], f"{label} quantities")
    if set(quantities) != set(SYMBOLS):
        raise SoxlThreeAssetPairedShadowError(f"invalid {label}")
    pending = raw["pending_target_weights"]
    pending_cash = raw["pending_cash_weight"]
    if (pending is None) != (pending_cash is None):
        raise SoxlThreeAssetPairedShadowError(f"invalid {label}")
    normalized_pending = None if pending is None else _weights(pending, f"{label} pending weights")
    normalized_pending_cash = (
        None
        if pending_cash is None
        else _finite(pending_cash, f"{label} pending cash", nonnegative=True)
    )
    if normalized_pending is not None and abs(
        sum(normalized_pending.values()) + float(normalized_pending_cash) - 1.0
    ) > 1e-9:
        raise SoxlThreeAssetPairedShadowError(f"invalid {label}")
    return {
        "cash": _finite(raw["cash"], f"{label} cash", nonnegative=True),
        "quantities": {
            symbol: _finite(quantities[symbol], f"{label} quantities", nonnegative=True)
            for symbol in SYMBOLS
        },
        "pending_target_weights": normalized_pending,
        "pending_cash_weight": normalized_pending_cash,
        "previous_equity": _finite(
            raw["previous_equity"], f"{label} previous equity", nonnegative=True
        ),
    }


def _advance_leg(
    *,
    state: Mapping[str, object],
    prices: Mapping[str, float],
    market_data: Mapping[str, object],
    as_of: str,
    cost_bps: float,
    mid_soxl_weight: float,
    decide: Callable[..., Mapping[str, object]],
) -> tuple[dict[str, object], dict[str, object]]:
    input_state_sha256 = _sha256(state)
    cash = float(state["cash"])
    quantities = dict(state["quantities"])
    market_values = {symbol: quantities[symbol] * prices[symbol] for symbol in SYMBOLS}
    equity_before_trade = cash + sum(market_values.values())
    if equity_before_trade <= 0.0:
        raise SoxlThreeAssetPairedShadowError("invalid paired shadow equity")

    turnover = 0.0
    executed_cost = 0.0
    pending = state["pending_target_weights"]
    pending_cash = state["pending_cash_weight"]
    if pending is not None:
        current_weights = {
            symbol: market_values[symbol] / equity_before_trade for symbol in SYMBOLS
        }
        turnover = 0.5 * (
            sum(abs(pending[symbol] - current_weights[symbol]) for symbol in SYMBOLS)
            + abs(float(pending_cash) - cash / equity_before_trade)
        )
        executed_cost = equity_before_trade * turnover * cost_bps / 10_000.0
        equity_after_trade = equity_before_trade - executed_cost
        if equity_after_trade <= 0.0:
            raise SoxlThreeAssetPairedShadowError("invalid paired shadow equity")
        quantities = {
            symbol: pending[symbol] * equity_after_trade / prices[symbol]
            for symbol in SYMBOLS
        }
        cash = float(pending_cash) * equity_after_trade
        market_values = {symbol: quantities[symbol] * prices[symbol] for symbol in SYMBOLS}

    equity = cash + sum(market_values.values())
    previous_equity = float(state["previous_equity"])
    if previous_equity <= 0.0:
        raise SoxlThreeAssetPairedShadowError("invalid paired shadow equity")
    portfolio = {
        "as_of": as_of,
        "total_equity": equity,
        "buying_power": cash,
        "cash_balance": cash,
        "positions": [
            {
                "symbol": symbol,
                "quantity": quantities[symbol],
                "market_value": market_values[symbol],
                "currency": "USD",
            }
            for symbol in SYMBOLS
            if quantities[symbol] > 0.0
        ],
        "metadata": {"observed_effective_exposure": 0.0},
    }
    decision = _mapping(
        decide(
            portfolio=json.loads(_canonical(portfolio)),
            market_data=json.loads(_canonical(market_data)),
            as_of=as_of,
            mid_soxl_weight=mid_soxl_weight,
        ),
        "paired shadow decision",
    )
    if str(decision.get("as_of") or "") != as_of:
        raise SoxlThreeAssetPairedShadowError("invalid paired shadow decision")
    targets = _mapping(decision.get("target_values"), "paired shadow targets")
    if set(targets) != set(SYMBOLS):
        raise SoxlThreeAssetPairedShadowError("invalid paired shadow targets")
    target_values = {
        symbol: _finite(targets[symbol], "paired shadow targets", nonnegative=True)
        for symbol in SYMBOLS
    }
    target_weights = {symbol: target_values[symbol] / equity for symbol in SYMBOLS}
    target_cash_weight = max(0.0, 1.0 - sum(target_weights.values()))
    if sum(target_weights.values()) > 1.0 + 1e-9:
        raise SoxlThreeAssetPairedShadowError("invalid paired shadow targets")

    next_state = {
        "cash": cash,
        "quantities": quantities,
        "pending_target_weights": target_weights,
        "pending_cash_weight": target_cash_weight,
        "previous_equity": equity,
    }
    leg = {
        "signal": decision,
        "hypothetical_order": {
            "execution_timing": "next_complete_trading_session",
            "target_values": target_values,
            "pending_target_weights": target_weights,
            "pending_cash_weight": target_cash_weight,
            "next_state_sha256": _sha256(next_state),
        },
        "position": {
            "input_state_sha256": input_state_sha256,
            "cash": cash,
            "quantities": quantities,
            "market_values": market_values,
            "equity": equity,
        },
        "cost": {
            "model": "one_way_turnover_all_in_bps",
            "cost_bps": cost_bps,
            "executed_one_way_turnover": turnover,
            "executed_cost": executed_cost,
        },
        "return": {
            "previous_equity": previous_equity,
            "equity": equity,
            "period_return": equity / previous_equity - 1.0,
        },
    }
    return leg, next_state


def advance_paired_shadow_session(
    *,
    session: Mapping[str, object],
    baseline_state: Mapping[str, object],
    candidate_state: Mapping[str, object],
    cost_bps: float,
    decide: Callable[..., Mapping[str, object]],
) -> dict[str, object]:
    """Advance two isolated ledgers from one pre-frozen, no-order session."""
    snapshot = _mapping(session, "paired shadow session")
    if set(snapshot) != {"as_of", "prices", "market_data", "input_snapshot_sha256"}:
        raise SoxlThreeAssetPairedShadowError("invalid paired shadow session")
    claimed_digest = str(snapshot.get("input_snapshot_sha256") or "")
    if claimed_digest != input_snapshot_sha256(snapshot):
        raise SoxlThreeAssetPairedShadowError("input snapshot changed before decision")
    as_of = str(snapshot.get("as_of") or "")
    if not as_of:
        raise SoxlThreeAssetPairedShadowError("invalid paired shadow session")
    prices = _mapping(snapshot["prices"], "paired shadow session")
    if set(prices) != set(SYMBOLS):
        raise SoxlThreeAssetPairedShadowError("invalid paired shadow session")
    normalized_prices = {
        symbol: _finite(prices[symbol], "paired shadow session", nonnegative=True)
        for symbol in SYMBOLS
    }
    if any(price <= 0.0 for price in normalized_prices.values()):
        raise SoxlThreeAssetPairedShadowError("invalid paired shadow session")
    normalized_cost = _finite(cost_bps, "paired shadow cost", nonnegative=True)
    market_data = _mapping(snapshot["market_data"], "paired shadow session")

    baseline, next_baseline = _advance_leg(
        state=_state(baseline_state, "baseline state"),
        prices=normalized_prices,
        market_data=market_data,
        as_of=as_of,
        cost_bps=normalized_cost,
        mid_soxl_weight=0.65,
        decide=decide,
    )
    candidate, next_candidate = _advance_leg(
        state=_state(candidate_state, "candidate state"),
        prices=normalized_prices,
        market_data=market_data,
        as_of=as_of,
        cost_bps=normalized_cost,
        mid_soxl_weight=0.55,
        decide=decide,
    )
    return {
        "input_snapshot_sha256": claimed_digest,
        "observed_at": as_of,
        "baseline": baseline,
        "candidate": candidate,
        "baseline_state": next_baseline,
        "candidate_state": next_candidate,
        "no_order": True,
        "live_authority_granted": False,
    }


__all__ = [
    "SoxlThreeAssetPairedShadowError",
    "advance_paired_shadow_session",
    "input_snapshot_sha256",
]
