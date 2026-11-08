"""Synthetic-only B1/B2 bridge. Input construction is separate from execution."""

from dataclasses import replace
from datetime import date
from math import fsum, isclose, isfinite, sqrt
from statistics import variance

from quant_platform_kit.research_stats.execution_models import FillModel
from us_equity_strategies.research.b1_b2_rs04_preparation import (
    compose_exposure, continuous_drawdown_factor, ewma_variance_update, original_ladder_factor,
)
from us_equity_strategies.research.fractional_kelly_walk_forward import fit_fractional_kelly
from us_equity_strategies.research.rs04_b3 import cap_targets
from us_equity_strategies.research.rs04_b3_config import CONFIG
from us_equity_strategies.research.rs04_b3_runner import Ledger


TRAIN_SESSIONS = 252
PURGE_SESSIONS = 5
FOLD_SESSIONS = 63

ARMS = ("B1.1", "B1.2", "B1.3", "B1.4", "B2.1", "B2.2")


def fit_schedule(core_returns, cash_returns, first, count, observation_start):
    """At each fold's first decision, use 252 observations and a five-session purge."""
    if len(core_returns) != len(cash_returns):
        raise ValueError("SESSIONS_NOT_ALIGNED")
    if first + count > len(core_returns):
        raise ValueError("TRAINING_WINDOW_INCOMPLETE")
    estimates = []
    for start in range(first, first + count, FOLD_SESSIONS):
        train_end = start - PURGE_SESSIONS
        train_start = train_end - TRAIN_SESSIONS
        if train_start < observation_start:
            raise ValueError("TRAINING_PREFIX_INCOMPLETE")
        fit = fit_fractional_kelly(
            core_returns[train_start:train_end], cash_returns[train_start:train_end], 0.25,
        )
        estimates.extend([fit.get("kelly_fraction", 0.0)] * FOLD_SESSIONS)
    return estimates[:count]


def ewma_path(returns, decay):
    warmup = 63
    if len(returns) < warmup:
        return [None] * len(returns)
    value = variance(returns[:warmup])
    forecasts = [None] * (warmup - 1) + [sqrt(value)]
    for observation in returns[warmup:]:
        value = ewma_variance_update(value, observation, decay)
        forecasts.append(sqrt(value))
    return forecasts


def target_weights(core, profile, arm, full_kelly, forecast, nav, peak, target_vol):
    weights = cap_targets(core, profile)
    nominal = fsum(weights.values())
    if not nominal:
        return weights
    ceiling = CONFIG.soxl_total if profile == "SOXL" else CONFIG.tqqq_total
    kind = ("fixed_50" if arm == "B1.1"
            else "vol_target_and_kelly" if arm == "B1.4"
            else "fractional_kelly")
    budget = compose_exposure(
        kind=kind, existing_cap=ceiling, c=0.5 if arm == "B1.3" else 0.25,
        f_star=full_kelly, forecast_vol=forecast, target_vol=target_vol / sqrt(252),
    )["exposure"]
    factor = min(1.0, budget / nominal)
    if arm == "B2.1":
        factor *= original_ladder_factor(1 - nav / peak, full_until=0.10, half_until=0.20)
    if arm == "B2.2":
        factor *= continuous_drawdown_factor(nav, peak, 0.20)
    return {symbol: weight * factor for symbol, weight in weights.items()}


class ReplayCheckpointError(ValueError):
    """The saved scenario is inconsistent with the requested replay."""


def _restore_checkpoint(checkpoint, scenario, sessions, prices, first, count):
    if checkpoint.get("scenario") != scenario:
        raise ReplayCheckpointError("REPLAY_CHECKPOINT_BINDING_MISMATCH")
    try:
        completed = checkpoint["completed"]
        if type(completed) is not int or not 1 <= completed <= count:
            raise ValueError()
        index = first + completed - 1
        if checkpoint["last_session"] != sessions[index].isoformat():
            raise ValueError()
        cash, nav, peak = (checkpoint[field] for field in ("cash", "nav", "peak"))
        if not all(isfinite(value) for value in (cash, nav, peak)) or not (
            cash >= 0 and 0 < nav <= peak and peak >= CONFIG.initial_usd
        ):
            raise ValueError()
        quantity = dict(checkpoint["quantity"])
        if any(type(value) is not int or value <= 0 for value in quantity.values()):
            raise ValueError()
        receivables = {date.fromisoformat(day): value for day, value in checkpoint["receivables"].items()}
        if any(day <= sessions[index] or not isfinite(value) or value < 0
               for day, value in receivables.items()):
            raise ValueError()
        book = Ledger(cash, quantity, receivables)
        if not isclose(book.nav(prices[index]), nav, rel_tol=0, abs_tol=1e-7):
            raise ValueError()
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        raise ReplayCheckpointError("REPLAY_CHECKPOINT_INVALID") from exc
    return index, book, nav, peak


def replay_synthetic(profile, arm, sessions, prices, signals, core_returns, cash_returns,
                     first, count, *, observation_start, fees, decay=0.94, target_vol=0.20, actions=None,
                     observe_fill=None, opening_prices=None, input_binding=None,
                     observe_checkpoint=None, resume=None):
    """Replay on one frozen session axis; optional checkpoints are post-fill."""
    if arm not in ARMS or first < 1 or count < 1 or first + count > len(sessions):
        raise ValueError("REPLAY_SCOPE_INVALID")
    schedule = fit_schedule(core_returns, cash_returns, first, count, observation_start)
    forecast = [None] * observation_start + ewma_path(core_returns[observation_start:], decay)
    fee_schedule = replace(
        CONFIG, commission_bps=fees[0], slippage_including_spread_bps=fees[1],
        minimum_commission_usd=0.0, commission_per_share_usd=0.0,
    )
    start_index = first - 1
    book = Ledger(CONFIG.initial_usd, {}, {})
    nav = peak = CONFIG.initial_usd
    if observe_checkpoint is not None or resume is not None:
        if not isinstance(input_binding, str) or not input_binding:
            raise ReplayCheckpointError("REPLAY_CHECKPOINT_INPUT_BINDING_REQUIRED")
        scenario = {
            "input_binding": input_binding, "profile": profile, "arm": arm,
            "commission_bps": fees[0], "slippage_bps": fees[1], "decay": decay, "target_vol": target_vol,
            "first_session": sessions[first].isoformat(), "last_session": sessions[first + count - 1].isoformat(),
            "observation_start": observation_start, "sessions": count,
        }
        if resume is not None:
            if not isinstance(resume, dict):
                raise ReplayCheckpointError("REPLAY_CHECKPOINT_INVALID")
            start_index, book, nav, peak = _restore_checkpoint(resume, scenario, sessions, prices, first, count)
    points = []
    pending = None
    fill_model = FillModel("next_close")
    events = actions or {}
    for i in range(start_index, first + count):
        if pending is not None:
            decision, weights = pending
            point = fill_model.resolve(signal_index=decision, available_index=decision, bar_count=len(sessions))
            assert point is not None and point.index == i
            splits, dividends = events.get(sessions[i], ({}, ()))
            for symbol, ratio in splits.items():
                book.split(symbol, ratio)
            for symbol, amount, pay_day in dividends:
                book.accrue(symbol, amount, pay_day)
            book.pay(sessions[i])
            opening_nav = None
            if opening_prices is not None:
                marks = opening_prices.get(sessions[i], {})
                holdings = {symbol: quantity for symbol, quantity in book.quantity.items() if quantity}
                if all(symbol in marks and isfinite(marks[symbol]) and marks[symbol] > 0 for symbol in holdings):
                    opening_nav = book.cash + fsum(book.receivables.values()) + fsum(
                        quantity * marks[symbol] for symbol, quantity in holdings.items()
                    )
            prefill = book.nav(prices[i])
            cost = book.rebalance(weights, prices[i], profile=profile,
                                  cost_multiplier=1.0, fee_schedule=fee_schedule)
            after = book.nav(prices[i])
            prior_peak, peak = peak, max(peak, after)
            points.append({
                "signal": sessions[decision], "fill": sessions[i], "targets": weights,
                "start": nav, "open_nav": opening_nav, "prefill": prefill, "nav": after,
                "prior_peak": prior_peak, "peak": peak,
                "cash": book.cash, "receivables": fsum(book.receivables.values()),
                "quantity": dict(book.quantity), "cost": fsum(cost.values()),
                "orders": tuple(book.last_fills),
            })
            nav = after
            if observe_fill is not None:
                observe_fill(points[-1])
            if observe_checkpoint is not None:
                observe_checkpoint(points[-1], {
                    "scenario": dict(scenario), "completed": i - first + 1, "last_session": sessions[i].isoformat(),
                    "cash": book.cash, "nav": nav, "peak": peak, "quantity": dict(book.quantity),
                    "receivables": {day.isoformat(): value for day, value in book.receivables.items()},
                })
        if i + 1 == first + count:
            break
        core = signals[sessions[i]]
        pending = (i, target_weights(core, profile, arm, schedule[i + 1 - first],
                                     forecast[i], nav, peak, target_vol))
    return points
