"""Build one private long-horizon risk observation from frozen SOXL v7 P3.

This adapter is intentionally offline and non-executing.  It accepts only the
already-frozen P3 materialization, plan, metrics summary, and the same isolated
replay results used to derive that summary.  It emits paired, net-of-cost return
paths for the generic control-plane risk Composer.  The result contains raw
return paths and is therefore a private ingress artifact: callers must not
publish it to Actions summaries, repository files, consoles, AI prompts, or
unrestricted storage.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from decimal import Decimal, InvalidOperation, ROUND_FLOOR, localcontext

from .soxl_core_only_p2_v7_longterm_compounding_cash_reserve_contract import (
    P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT,
)
from .soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence import (
    SoxlCoreOnlyV7LongtermCompoundingCashReserveP3EvidenceError,
    build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_plan,
    build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_summary,
)

RISK_OBSERVATION_SCHEMA = "qsl.long_horizon_risk_observation.v1"
_ISOLATED_REPLAY_SCHEMA = "qsl.soxl-core-only-p3-isolated-replay-result.v1"
_STATEFUL_REPLAY_SCHEMA = "qsl.soxl-core-only-p3-stateful-replay-result.v1"
_REPLAY_INPUT_SCHEMA = "qsl.soxl-core-only-p3-stateful-replay-input.v1"
_OBSERVED_BENCHMARK_ID = "soxx"
_OBSERVED_SESSIONS_PER_YEAR = 252
_ROLLING_OOS_WINDOW_ID = "trailing_252_xnys_session_oos"
_CONTINUOUS_LONG_WINDOW_ID = "continuous_756_xnys_session_long_horizon"
_ROLLING_OOS_COST_BPS = 10
_STRESS_COST_BPS = 15
_BOOTSTRAP_COST_BPS = 10
_BOOTSTRAP_BLOCK_SESSIONS = 21
_BOOTSTRAP_PATH_COUNT = 8
_MAX_RETURN_BPS = 100_000


class SoxlCoreOnlyV7LongHorizonRiskObservationError(ValueError):
    """Fail closed without emitting source rows, account data, or return paths."""


def _fail() -> None:
    raise SoxlCoreOnlyV7LongHorizonRiskObservationError("invalid private SOXL v7 long-horizon risk observation")


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SoxlCoreOnlyV7LongHorizonRiskObservationError(
            "invalid private SOXL v7 long-horizon risk observation"
        ) from exc


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def calculate_soxl_core_only_v7_long_horizon_risk_observation_sha256(value: Mapping[str, object]) -> str:
    """Return the digest compatible with QRS's private observation contract."""
    payload = _mapping(value)
    payload.pop("observation_sha256", None)
    return _sha256(payload)


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        _fail()
    return dict(value)


def _digest(value: object) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        _fail()
    return value


def _positive_decimal(value: object) -> Decimal:
    if isinstance(value, bool):
        _fail()
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise SoxlCoreOnlyV7LongHorizonRiskObservationError(
            "invalid private SOXL v7 long-horizon risk observation"
        ) from exc
    if not result.is_finite() or result <= 0:
        _fail()
    return result


def _integer_return_bps(*, previous: Decimal, current: Decimal) -> int:
    with localcontext() as context:
        context.prec = 48
        raw_bps = ((current / previous) - Decimal(1)) * Decimal(10_000)
        result = int(raw_bps.to_integral_value(rounding=ROUND_FLOOR))
    if result <= -10_000 or result > _MAX_RETURN_BPS:
        _fail()
    return result


def _sessions_by_date(materialized: Mapping[str, object]) -> dict[str, dict[str, object]]:
    payload = _mapping(materialized)
    sessions = payload.get("sessions")
    if not isinstance(sessions, list):
        _fail()
    result: dict[str, dict[str, object]] = {}
    for raw_session in sessions:
        session = _mapping(raw_session)
        as_of = session.get("as_of")
        if not isinstance(as_of, str) or not as_of.endswith("T00:00:00+00:00"):
            _fail()
        session_date = as_of.removesuffix("T00:00:00+00:00")
        if session_date in result:
            _fail()
        result[session_date] = session
    return result


def _replay_input(*, request: Mapping[str, object], sessions_by_date: Mapping[str, Mapping[str, object]]) -> dict[str, object]:
    dates = request.get("session_dates")
    if not isinstance(dates, list) or len(dates) < 2:
        _fail()
    try:
        sessions = [sessions_by_date[date] for date in dates]
    except (KeyError, TypeError):
        _fail()
    cost_bps = request.get("cost_bps")
    if type(cost_bps) is not int:
        _fail()
    return {
        "schema_version": _REPLAY_INPUT_SCHEMA,
        "initial_equity": 100_000.0,
        "cost_bps": cost_bps,
        "sessions": sessions,
    }


def _validated_replay(
    value: Mapping[str, object],
    *,
    expected_cost_bps: int,
    expected_replay_sha256: str,
    expected_session_count: int,
) -> list[Decimal]:
    outer = _mapping(value)
    claimed_outer_digest = outer.pop("result_sha256", None)
    if (
        outer.get("schema_version") != _ISOLATED_REPLAY_SCHEMA
        or outer.get("status") != "SUCCESS"
        or _digest(claimed_outer_digest) != _sha256(outer)
    ):
        _fail()
    if _mapping(outer.get("p2_identity")) != {
        "candidate_id": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.candidate_id,
        "config_sha256": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.config_sha256,
    }:
        _fail()
    execution_identity = _mapping(outer.get("execution_identity"))
    if execution_identity != {
        "repository": "QuantStrategyLab/UsEquityStrategies",
        "revision": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.ues_revision,
        "quant_platform_kit_revision": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.qpk_revision,
        "uv_lock_sha256": "3ab6974ae8c2cece2fcff527828612eab6d4ab1baf5ab3b4a6f648c057ecc301",
    }:
        _fail()
    replay = _mapping(outer.get("replay"))
    claimed_replay_digest = replay.pop("output_sha256", None)
    if (
        replay.get("schema_version") != _STATEFUL_REPLAY_SCHEMA
        or replay.get("cost_bps") != expected_cost_bps
        or _digest(claimed_replay_digest) != expected_replay_sha256
        or claimed_replay_digest != _sha256(replay)
    ):
        _fail()
    decisions = replay.get("decisions")
    if not isinstance(decisions, list) or len(decisions) != expected_session_count:
        _fail()
    return [_positive_decimal(_mapping(item).get("equity_before_signal")) for item in decisions]


def _paired_returns(
    *,
    request: Mapping[str, object],
    run: Mapping[str, object],
    sessions_by_date: Mapping[str, Mapping[str, object]],
    replay_executor: Callable[[Mapping[str, object]], Mapping[str, object]],
) -> tuple[list[int], list[int], int]:
    replay_input = _replay_input(request=request, sessions_by_date=sessions_by_date)
    if run.get("replay_input_sha256") != _sha256(replay_input):
        _fail()
    metrics = _mapping(run.get("metrics"))
    replay_sha256 = _digest(metrics.get("replay_result_sha256"))
    dates = request.get("session_dates")
    if not isinstance(dates, list):
        _fail()
    replay_result = replay_executor(replay_input)
    if not isinstance(replay_result, Mapping):
        _fail()
    equity = _validated_replay(
        replay_result,
        expected_cost_bps=int(request["cost_bps"]),
        expected_replay_sha256=replay_sha256,
        expected_session_count=len(dates),
    )
    benchmark = []
    for session_date in dates:
        try:
            benchmark.append(_positive_decimal(_mapping(sessions_by_date[session_date]["prices"])["SOXX"]))
        except (KeyError, TypeError):
            _fail()
    strategy_returns = [
        _integer_return_bps(previous=equity[index - 1], current=equity[index])
        for index in range(1, len(equity))
    ]
    benchmark_returns = [
        _integer_return_bps(previous=benchmark[index - 1], current=benchmark[index])
        for index in range(1, len(benchmark))
    ]
    if len(strategy_returns) != len(benchmark_returns) or not strategy_returns:
        _fail()
    return strategy_returns, benchmark_returns, len(dates)


def _bootstrap_paths(
    *, strategy_returns: Sequence[int], benchmark_returns: Sequence[int], evidence_digest: str
) -> list[dict[str, object]]:
    if len(strategy_returns) != len(benchmark_returns) or len(strategy_returns) < _BOOTSTRAP_BLOCK_SESSIONS:
        _fail()
    result: list[dict[str, object]] = []
    path_length = len(strategy_returns)
    start_count = path_length - _BOOTSTRAP_BLOCK_SESSIONS + 1
    for path_index in range(1, _BOOTSTRAP_PATH_COUNT + 1):
        strategy_path: list[int] = []
        benchmark_path: list[int] = []
        block_index = 0
        while len(strategy_path) < path_length:
            seed = f"{evidence_digest}:moving-block-bootstrap-v1:{path_index}:{block_index}".encode("ascii")
            start = int.from_bytes(hashlib.sha256(seed).digest(), "big") % start_count
            remaining = path_length - len(strategy_path)
            take = min(_BOOTSTRAP_BLOCK_SESSIONS, remaining)
            strategy_path.extend(strategy_returns[start : start + take])
            benchmark_path.extend(benchmark_returns[start : start + take])
            block_index += 1
        result.append(
            {
                "scenario_id": f"soxl_soxx_v7_bootstrap_{path_index:02d}",
                "scenario_kind": "BOOTSTRAP",
                "session_count": path_length + 1,
                "strategy_returns_bps": strategy_path,
                "benchmark_returns_bps": benchmark_path,
            }
        )
    return result


def _select_run(
    *, runs_by_key: Mapping[tuple[str, int], Mapping[str, object]], window_id: str, cost_bps: int
) -> Mapping[str, object]:
    try:
        return runs_by_key[(window_id, cost_bps)]
    except KeyError:
        _fail()


def build_soxl_core_only_v7_long_horizon_risk_observation(
    *,
    materialized: Mapping[str, object],
    evidence_plan: Mapping[str, object],
    evidence_summary: Mapping[str, object],
    replay_executor: Callable[[Mapping[str, object]], Mapping[str, object]],
) -> dict[str, object]:
    """Build a hash-bound private observation from one validated SOXL v7 P3 run.

    The rolling locked OOS replay supplies `WALK_FORWARD`; continuous 15 bps
    net-cost replay supplies `STRESS`; and eight paired 21-session moving-block
    resamples of the continuous 10 bps replay supply `BOOTSTRAP`.  None of
    these paths creates promotion or execution authority.
    """
    if not callable(replay_executor):
        _fail()
    try:
        expected_plan = build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_plan(materialized)
        if _mapping(evidence_plan) != expected_plan:
            _fail()
        verified_summary = build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_summary(
            materialized=materialized,
            evidence_plan=expected_plan,
            replay_executor=replay_executor,
        )
    except SoxlCoreOnlyV7LongtermCompoundingCashReserveP3EvidenceError as exc:
        raise SoxlCoreOnlyV7LongHorizonRiskObservationError(
            "invalid private SOXL v7 long-horizon risk observation"
        ) from exc
    if _mapping(evidence_summary) != verified_summary:
        _fail()
    summary = _mapping(verified_summary)
    evidence_digest = _digest(summary.get("evidence_summary_sha256"))
    p1_identity = _mapping(summary.get("p1_identity"))
    execution_identity = _mapping(summary.get("execution_identity"))
    if p1_identity.get("input_manifest_sha256") is None:
        _fail()
    p1_digest = _digest(p1_identity["input_manifest_sha256"])
    plugin_digest = _digest(execution_identity.get("uv_lock_sha256"))
    if execution_identity.get("repository") != "QuantStrategyLab/UsEquityStrategies" or execution_identity.get(
        "revision"
    ) != P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.ues_revision:
        _fail()
    requests = expected_plan.get("requests")
    raw_runs = summary.get("runs")
    if not isinstance(requests, list) or not isinstance(raw_runs, list) or len(requests) != len(raw_runs):
        _fail()
    runs_by_key: dict[tuple[str, int], Mapping[str, object]] = {}
    for request, raw_run in zip(requests, raw_runs, strict=True):
        item = _mapping(request)
        run = _mapping(raw_run)
        window_id = item.get("window_id")
        cost_bps = item.get("cost_bps")
        if (
            not isinstance(window_id, str)
            or type(cost_bps) is not int
            or run.get("window_id") != window_id
            or run.get("cost_bps") != cost_bps
            or (window_id, cost_bps) in runs_by_key
        ):
            _fail()
        runs_by_key[(window_id, cost_bps)] = run
    sessions = _sessions_by_date(materialized)

    def paired(window_id: str, cost_bps: int) -> tuple[list[int], list[int], int]:
        matching_requests = [
            _mapping(item)
            for item in requests
            if _mapping(item).get("window_id") == window_id and _mapping(item).get("cost_bps") == cost_bps
        ]
        if len(matching_requests) != 1:
            _fail()
        return _paired_returns(
            request=matching_requests[0],
            run=_select_run(runs_by_key=runs_by_key, window_id=window_id, cost_bps=cost_bps),
            sessions_by_date=sessions,
            replay_executor=replay_executor,
        )

    walk_strategy, walk_benchmark, walk_sessions = paired(_ROLLING_OOS_WINDOW_ID, _ROLLING_OOS_COST_BPS)
    stress_strategy, stress_benchmark, stress_sessions = paired(_CONTINUOUS_LONG_WINDOW_ID, _STRESS_COST_BPS)
    bootstrap_strategy, bootstrap_benchmark, _ = paired(_CONTINUOUS_LONG_WINDOW_ID, _BOOTSTRAP_COST_BPS)
    scenarios: list[dict[str, object]] = [
        {
            "scenario_id": "soxl_soxx_v7_rolling_oos_10bps",
            "scenario_kind": "WALK_FORWARD",
            "session_count": walk_sessions,
            "strategy_returns_bps": walk_strategy,
            "benchmark_returns_bps": walk_benchmark,
        },
        {
            "scenario_id": "soxl_soxx_v7_cost_stress_15bps",
            "scenario_kind": "STRESS",
            "session_count": stress_sessions,
            "strategy_returns_bps": stress_strategy,
            "benchmark_returns_bps": stress_benchmark,
        },
        *_bootstrap_paths(
            strategy_returns=bootstrap_strategy,
            benchmark_returns=bootstrap_benchmark,
            evidence_digest=evidence_digest,
        ),
    ]
    result: dict[str, object] = {
        "schema": RISK_OBSERVATION_SCHEMA,
        "candidate": {
            "candidate_id": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.candidate_id,
            "candidate_kind": "individual",
            "strategy_repository": "QuantStrategyLab/UsEquityStrategies",
            "strategy_revision": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.ues_revision,
        },
        "source_evidence": {
            "p1_input_digest": p1_digest,
            "p2_config_digest": P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.config_sha256,
            "p3_evidence_sha256": evidence_digest,
            "plugin_bundle_sha256": plugin_digest,
        },
        "benchmark": {
            "benchmark_id": _OBSERVED_BENCHMARK_ID,
            "benchmark_kind": "unlevered_reference",
            "sessions_per_year": _OBSERVED_SESSIONS_PER_YEAR,
        },
        "scenario_paths": scenarios,
        "observation_sha256": "",
    }
    result["observation_sha256"] = calculate_soxl_core_only_v7_long_horizon_risk_observation_sha256(result)
    return result


__all__ = [
    "RISK_OBSERVATION_SCHEMA",
    "SoxlCoreOnlyV7LongHorizonRiskObservationError",
    "build_soxl_core_only_v7_long_horizon_risk_observation",
    "calculate_soxl_core_only_v7_long_horizon_risk_observation_sha256",
]
