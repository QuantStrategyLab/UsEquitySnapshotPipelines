"""Build one bounded P5-ready observation from an immutable TQQQ P1 root.

This module is deliberately upstream of the P5 shadow ledger.  It verifies an
immutable P1 input and its same-root P3 terminal metadata, then calls the
frozen public P2 v5 research adapter to produce a small, reproducible target
allocation.  It does not submit orders, read credentials, use a broker client,
or write to a network service.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from quant_platform_kit.common.models import PortfolioSnapshot
from quant_platform_kit.common.strategy_contracts import StrategyContext
from us_equity_strategies.entrypoints import build_tqqq_core_only_p2_v2_research_decision

from .tqqq_core_only_p1_binding import (
    P2_V5_CONTRACT,
    next_tqqq_core_only_xnys_session_after,
    verify_tqqq_core_only_input_root,
)
from .tqqq_p3_evidence_index import P3_STATUS, validate_tqqq_p3_result

FORWARD_OBSERVATION_SCHEMA = "qsl.tqqq-forward-observation.v1"
_CANDIDATE_ID = P2_V5_CONTRACT.candidate_id
_STRATEGY_REPOSITORY = "QuantStrategyLab/UsEquityStrategies"
_ENTRYPOINT = "us_equity_strategies.entrypoints.build_tqqq_core_only_p2_v2_research_decision"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_REVISION = re.compile(r"^[0-9a-f]{40}$")
_TIMESTAMP = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")
_ROOT_FIELDS = frozenset(
    {
        "schema",
        "produced_at",
        "candidate",
        "source_evidence",
        "forward_decision",
        "forward_observation_sha256",
    }
)
_CANDIDATE_FIELDS = frozenset(
    {"candidate_id", "config_sha256", "strategy_repository", "strategy_revision"}
)
_SOURCE_EVIDENCE_FIELDS = frozenset(
    {"p1_manifest_sha256", "p2_config_sha256", "p3_evidence_sha256", "producer_revision"}
)
_DECISION_FIELDS = frozenset(
    {"decision_id", "effective_session", "producer_revision", "allocation_bps", "decision_sha256"}
)
_STATUS_FIELDS = frozenset(
    {
        "schema_version",
        "candidate",
        "date_cutoff",
        "input_manifest_sha256",
        "p1_health_sha256",
        "p3_terminal",
    }
)
_STATUS_CANDIDATE_FIELDS = frozenset({"candidate_id", "config_sha256"})
_ALLOCATION_SYMBOLS = ("TQQQ", "QQQM", "BOXX", "CASH")
_P3_RESULT_FIELDS = frozenset({"evidence_sha256", "status", "verdict"})


class TqqqP5ForwardObservationError(ValueError):
    """Raised when the P1/P2/P3 provenance cannot safely produce a P5 input."""


def _fail(message: str) -> None:
    raise TqqqP5ForwardObservationError(message)


def _exact_mapping(value: Any, expected: frozenset[str], path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != expected:
        _fail(f"invalid {path}")
    return value


def _digest(value: Any, path: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        _fail(f"invalid {path}")
    return value


def _revision(value: Any, path: str) -> str:
    if not isinstance(value, str) or not _REVISION.fullmatch(value):
        _fail(f"invalid {path}")
    return value


def _timestamp(value: Any, path: str) -> str:
    if not isinstance(value, str) or not _TIMESTAMP.fullmatch(value):
        _fail(f"invalid {path}")
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError as exc:
        raise TqqqP5ForwardObservationError(f"invalid {path}") from exc
    return value


def _canonical_json(value: Mapping[str, Any], *, omitted: str, path: str) -> str:
    material = dict(value)
    material.pop(omitted, None)
    try:
        return json.dumps(material, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise TqqqP5ForwardObservationError(f"invalid {path}") from exc


def calculate_forward_decision_sha256(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        _canonical_json(value, omitted="decision_sha256", path="forward decision").encode("utf-8")
    ).hexdigest()


def calculate_forward_observation_sha256(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        _canonical_json(value, omitted="forward_observation_sha256", path="forward observation").encode("utf-8")
    ).hexdigest()


def _read_json(path: Path, label: str) -> Any:
    def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for key, item in pairs:
            if key in output:
                _fail(f"invalid {label}")
            output[key] = item
        return output

    try:
        return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=no_duplicates)
    except (OSError, TypeError, json.JSONDecodeError) as exc:
        raise TqqqP5ForwardObservationError(f"invalid {label}") from exc


def _canonical_config_sha256(config: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(config, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    ).hexdigest()


def _validate_frozen_config(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or _canonical_config_sha256(value) != P2_V5_CONTRACT.config_sha256:
        _fail("invalid frozen P2 v5 config")
    source = value.get("source")
    runtime_config = value.get("runtime_config")
    if (
        not isinstance(source, Mapping)
        or source.get("repository") != _STRATEGY_REPOSITORY
        or source.get("revision") != P2_V5_CONTRACT.ues_revision
        or source.get("entrypoint") != _ENTRYPOINT
        or not isinstance(runtime_config, Mapping)
    ):
        _fail("invalid frozen P2 v5 config")
    return value


def _validated_daily_status(
    value: Any,
    *,
    input_manifest_sha256: str,
    date_cutoff: str,
) -> Mapping[str, str]:
    status = _exact_mapping(value, _STATUS_FIELDS, "daily research status")
    candidate = _exact_mapping(status["candidate"], _STATUS_CANDIDATE_FIELDS, "daily research candidate")
    if candidate != {"candidate_id": _CANDIDATE_ID, "config_sha256": P2_V5_CONTRACT.config_sha256}:
        _fail("invalid daily research candidate")
    if status["schema_version"] != "qsl.tqqq-daily-research-status.v1":
        _fail("invalid daily research status")
    if status["date_cutoff"] != date_cutoff or status["input_manifest_sha256"] != input_manifest_sha256:
        _fail("daily research status does not bind this P1 root")
    _digest(status["p1_health_sha256"], "daily P1 health digest")
    terminal = _exact_mapping(status["p3_terminal"], _P3_RESULT_FIELDS, "daily P3 terminal")
    try:
        validated = validate_tqqq_p3_result(terminal)
    except ValueError as exc:
        raise TqqqP5ForwardObservationError("invalid daily P3 terminal") from exc
    if validated["status"] != P3_STATUS:
        _fail("daily P3 is not complete")
    return validated


def _qqq_history(snapshot_root: Path) -> tuple[str, list[dict[str, Any]]]:
    try:
        manifest_sha256 = verify_tqqq_core_only_input_root(snapshot_root, contract=P2_V5_CONTRACT)
    except (OSError, ValueError) as exc:
        raise TqqqP5ForwardObservationError("invalid P1 root") from exc
    binding = _read_json(snapshot_root / "binding.json", "P1 binding")
    bars = _read_json(snapshot_root / "bars.json", "P1 bars")
    if not isinstance(binding, Mapping) or not isinstance(bars, Mapping):
        _fail("invalid P1 root")
    identity = binding.get("data_identity")
    symbols = bars.get("symbols")
    if not isinstance(identity, Mapping) or not isinstance(identity.get("date_cutoff"), str) or not isinstance(symbols, Mapping):
        _fail("invalid P1 root")
    qqq = symbols.get("QQQ")
    if not isinstance(qqq, Mapping) or not isinstance(qqq.get("bars"), list):
        _fail("invalid P1 root")
    history: list[dict[str, Any]] = []
    for row in qqq["bars"]:
        if not isinstance(row, Mapping) or set(row) != {"date", "open", "high", "low", "close", "volume"}:
            _fail("invalid P1 root")
        history.append(dict(row))
    if len(history) < 252:
        _fail("insufficient frozen QQQ history")
    return manifest_sha256, history


def _basis_points_from_decision(decision: Any) -> dict[str, int]:
    positions = getattr(decision, "positions", None)
    if not isinstance(positions, tuple):
        _fail("invalid frozen strategy decision")
    values = {symbol: 0.0 for symbol in _ALLOCATION_SYMBOLS[:-1]}
    for position in positions:
        symbol = getattr(position, "symbol", None)
        target_value = getattr(position, "target_value", None)
        if not isinstance(symbol, str) or isinstance(target_value, bool) or not isinstance(target_value, (int, float)):
            _fail("invalid frozen strategy decision")
        numeric = float(target_value)
        if not math.isfinite(numeric) or numeric < 0.0:
            _fail("invalid frozen strategy decision")
        if symbol not in values:
            if numeric > 0.0:
                _fail("frozen strategy produced an excluded nonzero target")
            continue
        values[symbol] += numeric
    total_equity = 100_000.0
    allocation: dict[str, int] = {}
    for symbol, value in values.items():
        exact_bps = value * 10_000.0 / total_equity
        rounded = round(exact_bps)
        if abs(exact_bps - rounded) > 1e-9 or rounded < 0 or rounded > 10_000:
            _fail("frozen strategy target cannot be represented in basis points")
        allocation[symbol] = int(rounded)
    allocation["CASH"] = 10_000 - sum(allocation.values())
    if allocation["CASH"] < 0:
        _fail("frozen strategy exceeds virtual equity")
    return allocation


def _decision_from_verified_inputs(
    *,
    date_cutoff: str,
    qqq_history: list[dict[str, Any]],
    runtime_config: Mapping[str, Any],
) -> dict[str, Any]:
    try:
        signal_session = date.fromisoformat(date_cutoff)
        as_of = datetime.combine(signal_session, time(16, 0), tzinfo=ZoneInfo("America/New_York"))
        portfolio = PortfolioSnapshot(
            as_of=as_of,
            total_equity=100_000.0,
            buying_power=100_000.0,
            cash_balance=100_000.0,
            positions=(),
            metadata={"observed_effective_exposure": 0.0},
        )
        decision = build_tqqq_core_only_p2_v2_research_decision(
            StrategyContext(
                as_of=as_of,
                portfolio=portfolio,
                market_data={"benchmark_history": tuple(qqq_history)},
                runtime_config=dict(runtime_config),
            )
        )
        effective_session = next_tqqq_core_only_xnys_session_after(date_cutoff)
    except Exception as exc:
        raise TqqqP5ForwardObservationError("frozen forward decision failed") from exc
    result: dict[str, Any] = {
        "decision_id": f"{_CANDIDATE_ID}_forward_{signal_session.strftime('%Y%m%d')}",
        "effective_session": effective_session.isoformat(),
        "producer_revision": P2_V5_CONTRACT.ues_revision,
        "allocation_bps": _basis_points_from_decision(decision),
        "decision_sha256": "",
    }
    result["decision_sha256"] = calculate_forward_decision_sha256(result)
    return result


def build_tqqq_p5_forward_observation(
    *,
    snapshot_root: str | Path,
    config_payload: Any,
    daily_research_status: Any,
    producer_revision: str,
    produced_at: str,
) -> dict[str, Any]:
    """Build a pure P5 input candidate without enabling P5 or any broker lane."""
    config = _validate_frozen_config(config_payload)
    producer = _revision(producer_revision, "forward-observation producer revision")
    timestamp = _timestamp(produced_at, "forward-observation timestamp")
    root = Path(snapshot_root)
    manifest_sha256, qqq_history = _qqq_history(root)
    binding = _read_json(root / "binding.json", "P1 binding")
    if not isinstance(binding, Mapping) or not isinstance(binding.get("data_identity"), Mapping):
        _fail("invalid P1 root")
    date_cutoff = binding["data_identity"].get("date_cutoff")
    if not isinstance(date_cutoff, str):
        _fail("invalid P1 root")
    p3_terminal = _validated_daily_status(
        daily_research_status,
        input_manifest_sha256=manifest_sha256,
        date_cutoff=date_cutoff,
    )
    runtime = config.get("runtime_config")
    assert isinstance(runtime, Mapping)
    forward_decision = _decision_from_verified_inputs(
        date_cutoff=date_cutoff,
        qqq_history=qqq_history,
        runtime_config=runtime,
    )
    observation: dict[str, Any] = {
        "schema": FORWARD_OBSERVATION_SCHEMA,
        "produced_at": timestamp,
        "candidate": {
            "candidate_id": _CANDIDATE_ID,
            "config_sha256": P2_V5_CONTRACT.config_sha256,
            "strategy_repository": _STRATEGY_REPOSITORY,
            "strategy_revision": P2_V5_CONTRACT.ues_revision,
        },
        "source_evidence": {
            "p1_manifest_sha256": manifest_sha256,
            "p2_config_sha256": P2_V5_CONTRACT.config_sha256,
            "p3_evidence_sha256": p3_terminal["evidence_sha256"],
            "producer_revision": producer,
        },
        "forward_decision": forward_decision,
        "forward_observation_sha256": "",
    }
    observation["forward_observation_sha256"] = calculate_forward_observation_sha256(observation)
    return validate_tqqq_p5_forward_observation(observation)


def validate_tqqq_p5_forward_observation(value: Any) -> dict[str, Any]:
    """Validate the sanitized artifact passed from P1/P2/P3 into P5."""
    observation = _exact_mapping(value, _ROOT_FIELDS, "forward observation")
    if observation["schema"] != FORWARD_OBSERVATION_SCHEMA:
        _fail("invalid forward observation schema")
    candidate = _exact_mapping(observation["candidate"], _CANDIDATE_FIELDS, "forward candidate")
    expected_candidate = {
        "candidate_id": _CANDIDATE_ID,
        "config_sha256": P2_V5_CONTRACT.config_sha256,
        "strategy_repository": _STRATEGY_REPOSITORY,
        "strategy_revision": P2_V5_CONTRACT.ues_revision,
    }
    if dict(candidate) != expected_candidate:
        _fail("invalid forward candidate")
    source = _exact_mapping(observation["source_evidence"], _SOURCE_EVIDENCE_FIELDS, "forward source evidence")
    for field in ("p1_manifest_sha256", "p2_config_sha256", "p3_evidence_sha256"):
        _digest(source[field], f"forward source evidence {field}")
    if source["p2_config_sha256"] != P2_V5_CONTRACT.config_sha256:
        _fail("invalid forward source evidence")
    _revision(source["producer_revision"], "forward source producer revision")
    decision = _exact_mapping(observation["forward_decision"], _DECISION_FIELDS, "forward decision")
    if (
        not isinstance(decision["decision_id"], str)
        or not decision["decision_id"].startswith(f"{_CANDIDATE_ID}_forward_")
        or decision["producer_revision"] != P2_V5_CONTRACT.ues_revision
    ):
        _fail("invalid forward decision")
    _revision(decision["producer_revision"], "forward decision producer revision")
    try:
        effective_session = date.fromisoformat(str(decision["effective_session"]))
    except ValueError as exc:
        raise TqqqP5ForwardObservationError("invalid forward effective session") from exc
    if effective_session.weekday() >= 5:
        _fail("invalid forward effective session")
    allocation = _exact_mapping(decision["allocation_bps"], frozenset(_ALLOCATION_SYMBOLS), "forward allocation")
    normalized_allocation: dict[str, int] = {}
    for symbol in _ALLOCATION_SYMBOLS:
        amount = allocation[symbol]
        if isinstance(amount, bool) or not isinstance(amount, int) or not 0 <= amount <= 10_000:
            _fail("invalid forward allocation")
        normalized_allocation[symbol] = amount
    if sum(normalized_allocation.values()) != 10_000:
        _fail("invalid forward allocation")
    _digest(decision["decision_sha256"], "forward decision digest")
    normalized_decision = {
        "decision_id": decision["decision_id"],
        "effective_session": effective_session.isoformat(),
        "producer_revision": decision["producer_revision"],
        "allocation_bps": normalized_allocation,
        "decision_sha256": decision["decision_sha256"],
    }
    if normalized_decision["decision_sha256"] != calculate_forward_decision_sha256(normalized_decision):
        _fail("invalid forward decision digest")
    timestamp = _timestamp(observation["produced_at"], "forward observation timestamp")
    normalized: dict[str, Any] = {
        "schema": FORWARD_OBSERVATION_SCHEMA,
        "produced_at": timestamp,
        "candidate": expected_candidate,
        "source_evidence": {
            "p1_manifest_sha256": source["p1_manifest_sha256"],
            "p2_config_sha256": source["p2_config_sha256"],
            "p3_evidence_sha256": source["p3_evidence_sha256"],
            "producer_revision": source["producer_revision"],
        },
        "forward_decision": normalized_decision,
        "forward_observation_sha256": _digest(
            observation["forward_observation_sha256"], "forward observation digest"
        ),
    }
    if normalized["forward_observation_sha256"] != calculate_forward_observation_sha256(normalized):
        _fail("invalid forward observation digest")
    return normalized


__all__ = [
    "FORWARD_OBSERVATION_SCHEMA",
    "TqqqP5ForwardObservationError",
    "build_tqqq_p5_forward_observation",
    "calculate_forward_decision_sha256",
    "calculate_forward_observation_sha256",
    "validate_tqqq_p5_forward_observation",
]
