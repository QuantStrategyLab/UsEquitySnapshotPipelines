"""Execute the registered B1/B2 scenarios on already-bound replay inputs."""
from dataclasses import dataclass
from datetime import date
from math import fsum
from typing import Mapping, Sequence

from archive_bridge import NormalizedInput
from input_qualification import PROFILES, select_profiles
from paired import PRIMARY_FEES, net_path_report, primary_pair_summary, validate_net_path
from replay import ARMS, FOLD_SESSIONS, PURGE_SESSIONS, TRAIN_SESSIONS, ReplayCheckpointError, replay_synthetic
from risk_observations import drawdown_observations
from us_equity_strategies.research.kelly_core_return_generator import (
    INDICATORS_QPK_LIVE_BUILDER, SIGNAL_SPLIT_ADJUSTED, generate_soxl_core_path, generate_tqqq_core_path,
)
from us_equity_strategies.research.rs04_b3_config import CONFIG


FEE_SCHEDULES = ((0.0, 0.0), PRIMARY_FEES, (5.0, 10.0))


@dataclass(frozen=True)
class ReplayInputs:
    sessions: Sequence[date]
    prices: Sequence[Mapping[str, float]]
    signals: Mapping[date, Mapping[str, float]]
    core_returns: Sequence[float]
    cash_returns: Sequence[float]
    observation_start: int
    actions: Mapping
    cash_source: str
    opening_prices: Mapping[date, Mapping[str, float]] | None = None
    input_binding: str | None = None


def prepare_replay_inputs(profile: str, source: NormalizedInput, *,
                          effective_core_config: Mapping[str, object], trade_start: date,
                          observation_start: date, cash_source: str, input_binding: str) -> ReplayInputs:
    """Build dated research signals with explicitly bound settings; no authorization is inferred."""
    if profile not in PROFILES or not effective_core_config or not cash_source or not input_binding:
        raise ValueError("CORE_INPUT_BINDING_REQUIRED")
    sessions = source.core_sessions[profile]
    prices = source.core_prices[profile]
    try:
        observation_index = sessions.index(observation_start)
    except ValueError as exc:
        raise ValueError("CORE_OBSERVATION_START_NOT_COVERED") from exc
    if trade_start not in sessions or "BOXX" not in prices[observation_index]:
        raise ValueError("CORE_OBSERVATION_START_NOT_COVERED")
    signals = {}
    options = {
        "trade_start": trade_start, "signal_price_basis": SIGNAL_SPLIT_ADJUSTED,
        "observe_decision": signals.__setitem__, "effective_core_config": effective_core_config,
    }
    if profile == "SOXL":
        core = generate_soxl_core_path(
            source.bars, source.actions, sessions, indicator_source=INDICATORS_QPK_LIVE_BUILDER,
            indicator_history_bars=504, **options,
        )
    else:
        core = generate_tqqq_core_path(source.bars, source.actions, sessions, **options)
    return ReplayInputs(
        sessions, prices, signals, core.gross_returns, core.asset_returns["BOXX"],
        observation_index, source.core_events, cash_source,
        opening_prices=source.opening_prices, input_binding=input_binding,
    )


def _inputs_window(inputs, requested):
    days = tuple(inputs.sessions)
    if any(right <= left for left, right in zip(days, days[1:])):
        raise ValueError("REPLAY_SESSIONS_NOT_ORDERED")
    if any(len(values) != len(days) for values in (inputs.prices, inputs.core_returns, inputs.cash_returns)):
        raise ValueError("REPLAY_INPUTS_NOT_ALIGNED")
    try:
        first = days.index(requested[0])
    except ValueError as exc:
        raise ValueError("REPLAY_WINDOW_NOT_ALIGNED") from exc
    if (first < 1 or not 0 <= inputs.observation_start <= first - TRAIN_SESSIONS - PURGE_SESSIONS
            or days[first:first + len(requested)] != requested):
        raise ValueError("REPLAY_WINDOW_NOT_ALIGNED")
    return first


def _period_report(points, days, cash, cash_source):
    return {
        "from": days[0], "to": days[-1], "sessions": len(days),
        "report": net_path_report(
            points, days, cash_by_session={day: cash[day] for day in days}, cash_source=cash_source,
        ),
        "max_drawdown_with_path_peak": min(point["nav"] / point["peak"] - 1 for point in points),
        "drawdown_observations": drawdown_observations(points),
    }


def _resume_prefix(saved, requested, preceding_session):
    try:
        checkpoint = saved["checkpoint"]
        points = [{**point, "fill": date.fromisoformat(point["fill"]), "signal": date.fromisoformat(point["signal"])}
                  for point in saved["points"]]
        completed = checkpoint["completed"]
        if type(completed) is not int or not 1 <= completed <= len(requested) or completed != len(points):
            raise ValueError()
        validate_net_path(points, requested[:completed])
        if tuple(point["signal"] for point in points) != (preceding_session,) + requested[:completed - 1]:
            raise ValueError()
        prior_peak = CONFIG.initial_usd
        if points[0]["start"] != prior_peak:
            raise ValueError()
        for point in points:
            peak = max(prior_peak, point["nav"])
            if point["prior_peak"] != prior_peak or point["peak"] != peak:
                raise ValueError()
            prior_peak = peak
        last = points[-1]
        if any(last[field] != checkpoint[field] for field in ("nav", "cash", "peak", "quantity")):
            raise ValueError()
        if last["receivables"] != fsum(checkpoint["receivables"].values()):
            raise ValueError()
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        raise ValueError("REPLAY_CHECKPOINT_PREFIX_INVALID") from exc
    return points, checkpoint


def _checkpoint_observer(observer, scenario_id):
    def save(point, checkpoint):
        record = {
            **point, "fill": point["fill"].isoformat(), "signal": point["signal"].isoformat(),
            "targets": dict(point["targets"]), "quantity": dict(point["quantity"]),
            "orders": [dict(order) for order in point["orders"]],
        }
        try:
            observer(scenario_id, record, checkpoint)
        except (KeyError, ValueError) as exc:
            raise RuntimeError("REPLAY_CHECKPOINT_OBSERVER_FAILED") from exc
    return save


def evaluate_study_matrix(profiles: Mapping[str, ReplayInputs], *, main_sessions, tail_sessions=(),
                          observe_checkpoint=None, resume=None, selected_profiles=PROFILES):
    """Retain full-window reports when bound scenarios resume from saved settlements."""
    main, tail = tuple(main_sessions), tuple(tail_sessions)
    if not main or len(main) % FOLD_SESSIONS or len(tail) >= FOLD_SESSIONS:
        raise ValueError("REGISTERED_FOLD_WINDOW_INVALID")
    selected = select_profiles(selected_profiles)
    if set(profiles) != set(selected):
        raise ValueError("REGISTERED_PROFILES_REQUIRED")
    requested = main + tail
    first_by_profile = {name: _inputs_window(profiles[name], requested) for name in selected}
    rows, main_paths, tail_paths = [], {}, {}
    parameters = tuple((arm, "registered", 0.94, 0.20) for arm in ARMS)
    parameters += (("B1.4", "sensitivity", 0.97, 0.30),)
    registered = {
        profile: [(f"{profile}/{arm}/{variant}/{sum(fees):g}bps", arm, variant, decay, target, fees)
                  for arm, variant, decay, target in parameters for fees in FEE_SCHEDULES]
        for profile in selected
    }
    saved_scenarios = {} if resume is None else dict(resume)
    if saved_scenarios.keys() - {row[0] for entries in registered.values() for row in entries}:
        raise ValueError("REPLAY_CHECKPOINT_SCENARIO_UNKNOWN")
    if observe_checkpoint is not None or saved_scenarios:
        if any(not isinstance(data.input_binding, str) or not data.input_binding for data in profiles.values()):
            raise ValueError("REPLAY_CHECKPOINT_INPUT_BINDING_REQUIRED")
    for profile in selected:
        data = profiles[profile]
        first = first_by_profile[profile]
        cash = dict(zip(requested, data.cash_returns[first:first + len(requested)], strict=True))
        for scenario_id, arm, variant, decay, target, fees in registered[profile]:
            if scenario_id in saved_scenarios:
                points, checkpoint = _resume_prefix(saved_scenarios[scenario_id], requested, data.sessions[first - 1])
            else:
                points, checkpoint = [], None
            observer = _checkpoint_observer(observe_checkpoint, scenario_id) if observe_checkpoint is not None else None
            failure = None
            try:
                replay_synthetic(
                    profile, arm, data.sessions, data.prices, data.signals, data.core_returns,
                    data.cash_returns, first, len(requested), observation_start=data.observation_start,
                    fees=fees, decay=decay, target_vol=target, actions=data.actions,
                    observe_fill=points.append, opening_prices=data.opening_prices,
                    input_binding=data.input_binding, observe_checkpoint=observer, resume=checkpoint,
                )
            except KeyError:
                failure = "REPLAY_REQUIRED_FIELD_MISSING"
            except ReplayCheckpointError:
                raise
            except ValueError as exc:
                failure = str(exc)
            row = {
                "scenario_id": scenario_id, "profile": profile, "arm": arm, "variant": variant, "fees": fees,
                "ewma_decay": decay if arm == "B1.4" else None,
                "target_annual_vol": target if arm == "B1.4" else None,
                "status": "FAILED" if failure is not None else "COMPUTED",
                "reason_code": failure, "failed_session": requested[len(points)] if failure is not None else None,
                "completed_sessions": len(points), "folds": [], "main": None, "tail": None,
            }
            rows.append(row)
            for start in range(0, min(len(main), len(points)) - FOLD_SESSIONS + 1, FOLD_SESSIONS):
                end = start + FOLD_SESSIONS
                row["folds"].append(_period_report(points[start:end], main[start:end], cash, data.cash_source))
            if len(points) >= len(main):
                row["main"] = _period_report(points[:len(main)], main, cash, data.cash_source)
                if variant == "registered":
                    main_paths[profile, arm, fees] = points[:len(main)]
            if tail and len(points) == len(requested):
                row["tail"] = _period_report(points[len(main):], tail, cash, data.cash_source)
                if variant == "registered":
                    tail_paths[profile, arm, fees] = points[len(main):]
    return {
        "scenarios": rows,
        "primary": primary_pair_summary(main_paths, main, profiles=selected),
        "descriptive_tail": primary_pair_summary(
            tail_paths, tail, inherited_opening_capital=True, profiles=selected,
        ) if tail else [],
    }
