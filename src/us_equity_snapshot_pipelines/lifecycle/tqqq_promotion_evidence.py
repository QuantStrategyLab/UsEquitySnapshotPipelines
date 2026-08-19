"""Private-input, offline-only TQQQ promotion evidence producer."""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from quant_platform_kit.common.models import PortfolioSnapshot, Position
from quant_platform_kit.data.research_input import (
    InvalidResearchInputEvidence,
    canonical_research_input_manifest_bytes,
    validate_research_input_manifest,
)
from quant_platform_kit.risk.contracts import CandidateRiskIdentity
from quant_platform_kit.risk.engine import build_risk_engine
from quant_platform_kit.strategy_contracts import PositionTarget, StrategyContext, StrategyDecision
from quant_platform_kit.strategy_lifecycle.contracts import PurgedWalkForwardFold
from quant_platform_kit.strategy_lifecycle.evidence_package_v2 import (
    canonical_evidence_package_v2_bytes,
    validate_evidence_package_v2,
)
from us_equity_strategies.entrypoints import (
    _build_tqqq_growth_income_decision,
    build_tqqq_core_only_p2_v2_research_decision,
)

from .tqqq_core_only_p1_binding import (
    CANDIDATE_CONFIG_SHA256,
    P2_V2_CONTRACT,
    P2_V2_UES_REVISION,
    P2_V4_CONTRACT,
    P2_V4_UES_REVISION,
    P2_V5_CONTRACT,
    P2_V5_UES_REVISION,
    TqqqCoreOnlyCandidateContract,
    _expected_xnys_sessions,
    resolve_tqqq_core_only_candidate_contract,
    tqqq_core_only_p1_binding_sha256_for_contract,
    validate_tqqq_core_only_input_manifest,
    validate_tqqq_core_only_p1_binding_for_contract,
)
from .tqqq_promotion_runner import (
    _EXACT_COMMON_ELIGIBILITY,
    TqqqEpisodeSummary,
    TqqqPromotionIdentity,
    TqqqPromotionPlan,
    TqqqPromotionResearchResult,
    TqqqSwitchingTrace,
    TqqqWindowReplay,
    _resolve_runner_revision,
    build_tqqq_switching_characterization_contract,
    evaluate_tqqq_pre_result_acceptance,
    run_tqqq_promotion_research,
)

_PROFILE = "tqqq_core_only_p2_v1"
_DOMAIN = "us_equity"
_INPUT_SCHEMA = "tqqq_core_only_private_bars.v1"
_CONFIG_SCHEMA = "qsl.tqqq-core-only-p2-candidate.v1"
_P2_V2_CONFIG_SCHEMA = "qsl.tqqq-core-only-p2-candidate.v2"
_P2_V4_CONFIG_SCHEMA = "qsl.tqqq-core-only-p2-candidate.v4"
_P2_V5_CONFIG_SCHEMA = "qsl.tqqq-core-only-p2-candidate.v5"
_ALLOWED_COST_SCENARIOS = frozenset({5, 10, 15, 25})
_ORDERABLE_ASSETS = ("TQQQ", "QQQM", "BOXX")
_ASSET_FACTORS = {"TQQQ": 3, "QQQM": 1, "BOXX": 1}
_BOXX_FIRST_ELIGIBLE_SESSION = date(2022, 12, 28)
_TQQQ_REPLAY_CALLABLE = (
    "us_equity_strategies.entrypoints._build_tqqq_growth_income_decision"
)
_P2_V2_REPLAY_CALLABLE = (
    "us_equity_strategies.entrypoints.build_tqqq_core_only_p2_v2_research_decision"
)

_CORE_FIELDS = (
    "schema_version",
    "evidence_package_id",
    "generated_at",
    "requested_stage",
    "strategy",
    "input_provenance",
    "backtest",
    "artifacts",
    "metrics",
    "cost_stress",
    "risk_assessment",
)

class TqqqPromotionEvidenceError(ValueError):
    """Fail-closed error without provider bars or account material."""


@dataclass(frozen=True)
class _Bar:
    session: date
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class _ReplayState:
    cash: float = 100_000.0
    quantities: dict[str, float] = field(
        default_factory=lambda: {symbol: 0.0 for symbol in _ORDERABLE_ASSETS}
    )
    tqqq_entry_price: float | None = None
    tqqq_stop_price: float | None = None
    tqqq_entry_identity_sha256: str | None = None
    pending_weights: dict[str, float] = field(
        default_factory=lambda: {symbol: 0.0 for symbol in _ORDERABLE_ASSETS}
    )
    high_water_equity: float = 100_000.0
    last_equity: float = 100_000.0
    consecutive_losing_exits: int = 0
    cooldown_remaining_execution_sessions: int = 0
    parked: bool = False
    breaker_reason: str | None = None
    first_park_session: date | None = None
    last_session: date | None = None
    turnover: float = 0.0
    trade_count: int = 0
    decision_count: int = 0
    assessment_count: int = 0
    tqqq_entry_count: int = 0
    tqqq_stop_armed_count: int = 0
    tqqq_stop_crossing_count: int = 0
    tqqq_stop_fill_count: int = 0
    tqqq_unprotected_holding_session_count: int = 0
    market_regime_control_sha256: str = field(
        default_factory=lambda: _digest({"state": "ABSENT"})
    )
    volatility_hysteresis_state_sha256: str = field(
        default_factory=lambda: _digest({"state": "UNINITIALIZED"})
    )
    retention_state_sha256: str = field(
        default_factory=lambda: _digest({"state": "UNINITIALIZED"})
    )


def _canonical(value: Any) -> bytes:
    return json.dumps(
        _wire(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _wire(value: Any) -> Any:
    if value is None or type(value) in {str, int, float, bool}:
        return value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, tuple):
        return [_wire(item) for item in value]
    if isinstance(value, list):
        return [_wire(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _wire(item) for key, item in value.items()}
    if hasattr(value, "to_dict"):
        return _wire(value.to_dict())
    if hasattr(value, "__dataclass_fields__"):
        return _wire(asdict(value))
    raise TqqqPromotionEvidenceError("unsupported evidence material")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _digest(value: Any) -> str:
    return _sha256(_canonical(value))


def _exact_mapping(value: object, fields: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise TqqqPromotionEvidenceError(f"invalid {label}")
    return copy.deepcopy(dict(value))


def _finite(value: object, label: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TqqqPromotionEvidenceError(f"invalid {label}")
    number = float(value)
    if not math.isfinite(number) or (positive and number <= 0.0):
        raise TqqqPromotionEvidenceError(f"invalid {label}")
    return number


def _digest_text(value: object, length: int, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != length
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise TqqqPromotionEvidenceError(f"invalid {label}")
    return value


def _timestamp(value: str | None) -> str:
    if value is None:
        return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    try:
        parsed = datetime.fromisoformat(value)
    except (AttributeError, ValueError) as exc:
        raise TqqqPromotionEvidenceError("invalid generated_at") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise TqqqPromotionEvidenceError("invalid generated_at")
    return value


def _validate_config(value: Mapping[str, Any]) -> dict[str, Any]:
    """Bind the complete injected P2 candidate, never a copied field subset."""
    if not isinstance(value, Mapping):
        raise TqqqPromotionEvidenceError("invalid config payload")
    if value.get("schema_version") in {
        _CONFIG_SCHEMA,
        _P2_V2_CONFIG_SCHEMA,
        _P2_V4_CONFIG_SCHEMA,
        _P2_V5_CONFIG_SCHEMA,
    }:
        candidate = copy.deepcopy(dict(value))
        risk_standard_id = "P2_CANDIDATE_SEMANTIC_BINDING"
        risk_standard_sha256 = _candidate_contract(candidate).config_sha256
    else:
        config = _exact_mapping(
            value,
            {"candidate", "risk_standard_id", "risk_standard_sha256"},
            "config envelope",
        )
        candidate = config["candidate"]
        risk_standard_id = config["risk_standard_id"]
        risk_standard_sha256 = config["risk_standard_sha256"]
        if not isinstance(candidate, Mapping):
            raise TqqqPromotionEvidenceError("invalid frozen P2 candidate")
        candidate = copy.deepcopy(dict(candidate))
    contract = _candidate_contract(candidate)
    expected_config_sha256 = (
        CANDIDATE_CONFIG_SHA256
        if contract.candidate_id == _PROFILE
        else contract.config_sha256
    )
    if _digest(candidate) != expected_config_sha256:
        raise TqqqPromotionEvidenceError("candidate config digest mismatch")
    if not isinstance(candidate.get("runtime_config"), Mapping):
        raise TqqqPromotionEvidenceError("invalid frozen P2 candidate")
    _digest_text(risk_standard_sha256, 64, "risk standard digest")
    if not isinstance(risk_standard_id, str) or not risk_standard_id:
        raise TqqqPromotionEvidenceError("invalid risk standard")
    return {
        "candidate": candidate,
        "risk_standard_id": risk_standard_id,
        "risk_standard_sha256": risk_standard_sha256,
    }


def _candidate_contract(candidate: Mapping[str, Any]) -> TqqqCoreOnlyCandidateContract:
    candidate_id = candidate.get("candidate_id")
    try:
        contract = resolve_tqqq_core_only_candidate_contract(candidate_id)
    except ValueError as exc:
        raise TqqqPromotionEvidenceError("invalid frozen P2 candidate") from exc
    expected_schema = {
        _PROFILE: _CONFIG_SCHEMA,
        P2_V2_CONTRACT.candidate_id: _P2_V2_CONFIG_SCHEMA,
        P2_V4_CONTRACT.candidate_id: _P2_V4_CONFIG_SCHEMA,
        P2_V5_CONTRACT.candidate_id: _P2_V5_CONFIG_SCHEMA,
    }[contract.candidate_id]
    if candidate.get("schema_version") != expected_schema:
        raise TqqqPromotionEvidenceError("invalid frozen P2 candidate")
    if contract in {P2_V2_CONTRACT, P2_V4_CONTRACT, P2_V5_CONTRACT}:
        source = candidate.get("source")
        if (
            not isinstance(source, Mapping)
            or source.get("repository") != "QuantStrategyLab/UsEquityStrategies"
            or source.get("revision")
            != (
                P2_V2_UES_REVISION
                if contract == P2_V2_CONTRACT
                else P2_V4_UES_REVISION
                if contract == P2_V4_CONTRACT
                else P2_V5_UES_REVISION
            )
            or source.get("entrypoint") != _P2_V2_REPLAY_CALLABLE
        ):
            raise TqqqPromotionEvidenceError("invalid public research adapter")
    return contract


def _runtime_config(candidate: Mapping[str, Any]) -> dict[str, Any]:
    runtime = candidate.get("runtime_config")
    if not isinstance(runtime, Mapping):
        raise TqqqPromotionEvidenceError("missing frozen runtime config")
    return copy.deepcopy(dict(runtime))


def _tqqq_replay_callable_and_identity(
    contract: TqqqCoreOnlyCandidateContract,
) -> tuple[Callable[[StrategyContext], StrategyDecision], dict[str, str]]:
    """Return the exact, contract-selected UES callable used by the P3 replay."""
    try:
        frozen_contract = resolve_tqqq_core_only_candidate_contract(contract.candidate_id)
    except ValueError as exc:
        raise TqqqPromotionEvidenceError("unexpected TQQQ replay callable") from exc
    if frozen_contract != contract:
        raise TqqqPromotionEvidenceError("unexpected TQQQ replay callable")
    callable_ = (
        _build_tqqq_growth_income_decision
        if frozen_contract.candidate_id == _PROFILE
        else build_tqqq_core_only_p2_v2_research_decision
    )
    expected_callable = (
        _TQQQ_REPLAY_CALLABLE
        if frozen_contract.candidate_id == _PROFILE
        else _P2_V2_REPLAY_CALLABLE
    )
    observed = f"{callable_.__module__}.{callable_.__qualname__}"
    if observed != expected_callable:
        raise TqqqPromotionEvidenceError("unexpected TQQQ replay callable")
    return callable_, {"callable": observed, "ues_revision": frozen_contract.ues_revision}


def _tqqq_replay_callable_identity() -> dict[str, str]:
    """Return the actual v1 callable for its preserved compatibility artifact."""
    return _tqqq_replay_callable_and_identity(
        resolve_tqqq_core_only_candidate_contract(_PROFILE)
    )[1]


def _parse_bar(value: object) -> _Bar:
    row = _exact_mapping(
        value,
        {"date", "open", "high", "low", "close", "volume"},
        "bar",
    )
    try:
        session = date.fromisoformat(row["date"])
    except (TypeError, ValueError) as exc:
        raise TqqqPromotionEvidenceError("invalid bar session") from exc
    open_price = _finite(row["open"], "bar open", positive=True)
    high = _finite(row["high"], "bar high", positive=True)
    low = _finite(row["low"], "bar low", positive=True)
    close = _finite(row["close"], "bar close", positive=True)
    volume = _finite(row["volume"], "bar volume")
    if volume < 0.0 or high < max(open_price, low, close) or low > min(open_price, high, close):
        raise TqqqPromotionEvidenceError("invalid OHLCV bar")
    return _Bar(session, open_price, high, low, close, volume)


def _plan_from_candidate(
    candidate: Mapping[str, Any], *, data_cutoff: str | None = None
) -> TqqqPromotionPlan:
    """Build a candidate plan without giving evidence outcomes any tuning path.

    P2 v5 is the only rolling candidate.  Its frozen configuration fixes the
    folds, cost model, and trailing OOS session count; the verified P1 binding
    contributes only the completed data cutoff that anchors that OOS window.
    """
    contract = _candidate_contract(candidate)
    plan = candidate.get("evaluation_plan")
    if not isinstance(plan, Mapping):
        raise TqqqPromotionEvidenceError("missing candidate evaluation plan")
    try:
        purge_days = plan["purge_sessions"]
        if type(purge_days) is not int:
            raise TypeError("invalid purge days")
        folds = tuple(
            PurgedWalkForwardFold(
                train_start=date.fromisoformat(fold["train"][0]),
                train_end=date.fromisoformat(fold["train"][1]),
                test_start=date.fromisoformat(fold["evaluation"][0]),
                test_end=date.fromisoformat(fold["evaluation"][1]),
            )
            for fold in plan["purged_folds"]
            if isinstance(fold, Mapping)
            and fold.get("purge_sessions_after_train") == purge_days
        )
        if contract == P2_V5_CONTRACT:
            rolling = plan["rolling_locked_oos"]
            if (
                not isinstance(rolling, Mapping)
                or set(rolling)
                != {
                    "anchor",
                    "minimum_date_cutoff",
                    "rule",
                    "trailing_xnys_sessions",
                }
                or rolling["anchor"] != "VERIFIED_P1_DATE_CUTOFF"
                or rolling["minimum_date_cutoff"] != "2026-08-04"
                or rolling["trailing_xnys_sessions"] != 252
                or not isinstance(rolling["rule"], str)
                or not isinstance(data_cutoff, str)
            ):
                raise ValueError
            cutoff = date.fromisoformat(data_cutoff)
            sessions = _expected_xnys_sessions(data_cutoff)
            if (
                cutoff < date(2026, 8, 4)
                or not sessions
                or sessions[-1] != cutoff
                or len(sessions) < 252
            ):
                raise ValueError
            locked_start = sessions[-252]
            locked_end = cutoff
        else:
            if data_cutoff is not None:
                raise ValueError
            locked = plan["locked_oos"]
            locked_start = date.fromisoformat(locked["start"])
            locked_end = date.fromisoformat(locked["end"])
        result = TqqqPromotionPlan(
            folds=folds,
            locked_oos_start=locked_start,
            locked_oos_end=locked_end,
            purge_days=purge_days,
            embargo_days=plan.get("embargo_days", 0),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise TqqqPromotionEvidenceError("invalid candidate evaluation plan") from exc
    from .tqqq_promotion_runner import _validate_plan
    try:
        _validate_plan(result, candidate_profile=contract.candidate_id)
    except ValueError as exc:
        raise TqqqPromotionEvidenceError("invalid candidate evaluation plan") from exc
    return result


def _validate_input(
    value: Mapping[str, Any], config: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, tuple[_Bar, ...]], str]:
    candidate = config.get("candidate")
    if not isinstance(candidate, Mapping):
        raise TqqqPromotionEvidenceError("missing frozen candidate")
    contract = _candidate_contract(candidate)
    payload = _exact_mapping(value, {"binding", "input_manifest", "bars"}, "input payload")
    try:
        binding = validate_tqqq_core_only_p1_binding_for_contract(
            payload["binding"], contract
        )
        manifest_sha256 = validate_tqqq_core_only_input_manifest(
            payload["input_manifest"], binding, contract=contract
        )
        manifest = validate_research_input_manifest(payload["input_manifest"])
    except (InvalidResearchInputEvidence, ValueError):
        raise TqqqPromotionEvidenceError("invalid TQQQ core-only input binding") from None
    identity = binding["data_identity"]
    assert isinstance(identity, dict)
    try:
        plan = _plan_from_candidate(
            candidate, data_cutoff=str(identity["date_cutoff"])
            if contract == P2_V5_CONTRACT
            else None,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise TqqqPromotionEvidenceError("invalid candidate evaluation plan") from exc
    retention = identity["retention"]
    assert isinstance(retention, dict)
    provenance = {
        "source": identity["provider"],
        "source_revision": tqqq_core_only_p1_binding_sha256_for_contract(
            binding, contract
        ),
        "license": "P1_FROZEN_BINDING_RETENTION_ONLY_NO_LICENSE_CLAIM",
        "usage_scope": retention["policy"],
    }
    bars_payload = _exact_mapping(payload["bars"], {"schema_version", "symbols"}, "bars payload")
    symbols = _exact_mapping(
        bars_payload["symbols"], {"BOXX", "QQQ", "QQQM", "TQQQ"}, "bar symbols"
    )
    if bars_payload["schema_version"] != _INPUT_SCHEMA:
        raise TqqqPromotionEvidenceError("invalid bars schema")
    bars_bytes = _canonical(bars_payload)
    members = manifest["members"]
    if (
        len(members) != 1
        or members[0]["path"] != "bars.json"
        or members[0]["media_type"] != "application/json"
        or members[0]["size_bytes"] != len(bars_bytes)
        or members[0]["sha256"] != _sha256(bars_bytes)
    ):
        raise TqqqPromotionEvidenceError("input identity mismatch")
    parsed: dict[str, tuple[_Bar, ...]] = {}
    sources = manifest["sources"]
    expected_source_ids = {
        "alpaca_sip_1day_adjustment_all:BOXX",
        "alpaca_sip_1day_adjustment_all:QQQ",
        "alpaca_sip_1day_adjustment_all:QQQM",
        "alpaca_sip_1day_adjustment_all:TQQQ",
    }
    source_digests = {item["source_id"]: item["content_sha256"] for item in sources}
    if len(sources) != len(expected_source_ids) or set(source_digests) != expected_source_ids:
        raise TqqqPromotionEvidenceError("invalid provider source identities")
    for symbol in ("BOXX", "QQQ", "QQQM", "TQQQ"):
        symbol_payload = _exact_mapping(symbols[symbol], {"bars"}, "Alpaca bars")
        rows = symbol_payload["bars"]
        if not isinstance(rows, list) or not rows:
            raise TqqqPromotionEvidenceError("missing immutable bars")
        if source_digests[f"alpaca_sip_1day_adjustment_all:{symbol}"] != _sha256(
            _canonical(symbol_payload)
        ):
            raise TqqqPromotionEvidenceError("input identity mismatch")
        values = tuple(_parse_bar(row) for row in rows)
        sessions = tuple(row.session for row in values)
        if sessions != tuple(sorted(set(sessions))):
            raise TqqqPromotionEvidenceError("invalid bar ordering")
        parsed[symbol] = values
    qqq_sessions = tuple(row.session for row in parsed["QQQ"])
    tqqq_sessions = tuple(row.session for row in parsed["TQQQ"])
    qqqm_sessions = tuple(row.session for row in parsed["QQQM"])
    boxx_sessions = tuple(row.session for row in parsed["BOXX"])
    if qqq_sessions != tqqq_sessions:
        raise TqqqPromotionEvidenceError("QQQ/TQQQ session mismatch")
    if (
        qqqm_sessions
        != tuple(session for session in qqq_sessions if session >= qqqm_sessions[0])
        or not set(qqqm_sessions) <= set(qqq_sessions)
    ):
        raise TqqqPromotionEvidenceError("QQQM eligibility violation")
    if (
        boxx_sessions[0] != _BOXX_FIRST_ELIGIBLE_SESSION
        or any(session < _BOXX_FIRST_ELIGIBLE_SESSION for session in boxx_sessions)
        or boxx_sessions
        != tuple(session for session in qqq_sessions if session >= _BOXX_FIRST_ELIGIBLE_SESSION)
    ):
        raise TqqqPromotionEvidenceError("BOXX eligibility violation")
    if (
        qqq_sessions[0] > plan.folds[0].train_start
        or qqq_sessions[-1] != plan.locked_oos_end
        or manifest["calendar"]["session_date"] != qqq_sessions[-1].isoformat()
        or sum(session < plan.folds[0].test_start for session in qqq_sessions) < 257
    ):
        raise TqqqPromotionEvidenceError("immutable input coverage mismatch")
    expected_locked_sessions = tuple(
        session
        for session in _expected_xnys_sessions(plan.locked_oos_end.isoformat())
        if plan.locked_oos_start <= session <= plan.locked_oos_end
    )
    observed_locked_sessions = tuple(
        session
        for session in qqq_sessions
        if plan.locked_oos_start <= session <= plan.locked_oos_end
    )
    if observed_locked_sessions != expected_locked_sessions:
        raise TqqqPromotionEvidenceError("locked OOS calendar identity mismatch")
    return provenance, parsed, manifest_sha256


def _bound_data_cutoff(
    input_payload: Mapping[str, Any], contract: TqqqCoreOnlyCandidateContract
) -> str | None:
    """Read the cutoff only after exact candidate-bound binding validation."""
    try:
        binding = validate_tqqq_core_only_p1_binding_for_contract(
            input_payload["binding"], contract
        )
        identity = binding["data_identity"]
        if not isinstance(identity, Mapping):
            raise TypeError
        cutoff = identity["date_cutoff"]
    except (KeyError, TypeError, ValueError):
        raise TqqqPromotionEvidenceError("invalid TQQQ core-only input binding") from None
    if contract == P2_V5_CONTRACT:
        if not isinstance(cutoff, str):
            raise TqqqPromotionEvidenceError("invalid TQQQ core-only input binding")
        return cutoff
    return None


def _initial_state_projection() -> dict[str, Any]:
    return {
        "cash": 100_000.0,
        "quantities": {symbol: 0.0 for symbol in _ORDERABLE_ASSETS},
        "tqqq_entry_price": None,
        "tqqq_stop_price": None,
        "tqqq_entry_identity_sha256": None,
        "pending_weights": {symbol: 0.0 for symbol in _ORDERABLE_ASSETS},
        "high_water_equity": 100_000.0,
        "last_equity": 100_000.0,
        "consecutive_losing_exits": 0,
        "cooldown_remaining_execution_sessions": 0,
        "parked": False,
        "breaker_reason": None,
        "first_park_session": None,
        "last_session": None,
        "tqqq_entry_count": 0,
        "tqqq_stop_armed_count": 0,
        "tqqq_stop_crossing_count": 0,
        "tqqq_stop_fill_count": 0,
        "tqqq_unprotected_holding_session_count": 0,
        "market_regime_control_sha256": _digest({"state": "ABSENT"}),
        "volatility_hysteresis_state_sha256": _digest({"state": "UNINITIALIZED"}),
        "retention_state_sha256": _digest({"state": "UNINITIALIZED"}),
    }


def _state_projection(state: _ReplayState) -> dict[str, Any]:
    return {
        "cash": state.cash,
        "quantities": state.quantities,
        "tqqq_entry_price": state.tqqq_entry_price,
        "tqqq_stop_price": state.tqqq_stop_price,
        "tqqq_entry_identity_sha256": state.tqqq_entry_identity_sha256,
        "pending_weights": state.pending_weights,
        "high_water_equity": state.high_water_equity,
        "last_equity": state.last_equity,
        "consecutive_losing_exits": state.consecutive_losing_exits,
        "cooldown_remaining_execution_sessions": (
            state.cooldown_remaining_execution_sessions
        ),
        "parked": state.parked,
        "breaker_reason": state.breaker_reason,
        "first_park_session": state.first_park_session,
        "last_session": state.last_session,
        "tqqq_entry_count": state.tqqq_entry_count,
        "tqqq_stop_armed_count": state.tqqq_stop_armed_count,
        "tqqq_stop_crossing_count": state.tqqq_stop_crossing_count,
        "tqqq_stop_fill_count": state.tqqq_stop_fill_count,
        "tqqq_unprotected_holding_session_count": (
            state.tqqq_unprotected_holding_session_count
        ),
        "market_regime_control_sha256": state.market_regime_control_sha256,
        "volatility_hysteresis_state_sha256": state.volatility_hysteresis_state_sha256,
        "retention_state_sha256": state.retention_state_sha256,
    }


class _ImmutableReplayProducer:
    def __init__(
        self,
        bars: Mapping[str, tuple[_Bar, ...]],
        config: Mapping[str, Any],
        candidate: CandidateRiskIdentity,
        identity: TqqqPromotionIdentity,
        replay_callable: Callable[[StrategyContext], StrategyDecision],
    ) -> None:
        self.config = config
        self.candidate = candidate
        self.identity = identity
        self._replay_callable = replay_callable
        self.qqq = bars["QQQ"]
        self.prices = {
            symbol: {row.session: row for row in bars[symbol]}
            for symbol in _ORDERABLE_ASSETS
        }
        self._index = {row.session: index for index, row in enumerate(self.qqq)}
        self._scenario: int | None = None
        self._state = _ReplayState()
        self._state_sha256 = identity.initial_state_sha256
        self._scenario_counts: dict[int, dict[str, int]] = {}
        self._switching_traces: list[TqqqSwitchingTrace] = []

    @property
    def scenario_counts(self) -> dict[int, dict[str, int]]:
        return copy.deepcopy(self._scenario_counts)

    @property
    def switching_traces(self) -> tuple[TqqqSwitchingTrace, ...]:
        return tuple(self._switching_traces)

    def _reset(self, scenario: int, prior_state_sha256: str) -> None:
        if scenario not in _ALLOWED_COST_SCENARIOS:
            raise TqqqPromotionEvidenceError("invalid cost scenario")
        if prior_state_sha256 != self.identity.initial_state_sha256:
            raise TqqqPromotionEvidenceError("initial state identity mismatch")
        self._scenario = scenario
        self._state = _ReplayState()
        self._state_sha256 = self.identity.initial_state_sha256
        self._scenario_counts.setdefault(scenario, {"decisions": 0, "assessments": 0})
        self._switching_traces = []

    def _price(self, symbol: str, session: date) -> _Bar:
        try:
            return self.prices[symbol][session]
        except KeyError as exc:
            raise TqqqPromotionEvidenceError("eligible asset data unavailable") from exc

    def _equity(self, session: date, field: str) -> float:
        return self._state.cash + math.fsum(
            quantity * getattr(self._price(symbol, session), field)
            for symbol, quantity in self._state.quantities.items()
            if quantity > 0.0
        )

    def _allocation(self, session: date, field: str) -> tuple[tuple[str, float], ...]:
        equity = self._equity(session, field)
        if equity <= 0.0:
            raise TqqqPromotionEvidenceError("nonpositive replay equity")
        allocation = {
            symbol: self._state.quantities[symbol]
            * getattr(self._price(symbol, session), field)
            / equity
            for symbol in _ORDERABLE_ASSETS
        }
        allocation["cash"] = self._state.cash / equity
        return tuple(sorted(allocation.items()))

    def _record_executed_allocation(self, session: date) -> None:
        if not getattr(self, "_switching_traces", None):
            return
        if self._switching_traces[-1].execution_session != session:
            raise TqqqPromotionEvidenceError("switching trace execution mismatch")

    def _record_parked_allocation(self, session: date) -> None:
        state = self._state
        cash = tuple(
            sorted({**{symbol: 0.0 for symbol in _ORDERABLE_ASSETS}, "cash": 1.0}.items())
        )
        if (
            self._switching_traces
            and self._switching_traces[-1].execution_session == session
            and self._switching_traces[-1].risk_disposition == "PARK"
            and not self._switching_traces[-1].executed_allocation
        ):
            self._switching_traces[-1] = replace(
                self._switching_traces[-1],
                executed_allocation=self._allocation(session, "open"),
            )
            return
        if (
            not state.parked
            or state.last_session is None
            or state.last_session >= session
            or state.breaker_reason
            not in {"ACCOUNT_DRAWDOWN", "RISK_ENGINE_NON_APPROVE"}
        ):
            raise TqqqPromotionEvidenceError("invalid parked session state")
        self._switching_traces.append(
            TqqqSwitchingTrace(
                signal_session=state.last_session,
                execution_session=session,
                signal_state="parked",
                signal_regime="DEFENSIVE",
                intended_allocation=cash,
                risk_disposition="PARK",
                risk_reason_codes=(state.breaker_reason,),
                replay_target_allocation=cash,
                executed_allocation=self._allocation(session, "open"),
            )
        )

    def _park(self, reason: str, session: date) -> None:
        state = self._state
        if state.parked:
            return
        state.parked = True
        state.breaker_reason = reason
        state.first_park_session = session
        state.cooldown_remaining_execution_sessions = 0
        state.pending_weights = {symbol: 0.0 for symbol in _ORDERABLE_ASSETS}

    def _record_completed_exit(self, fill: float, session: date) -> None:
        """No-op: P2 does not admit an endogenous exit/cooldown mechanism."""
        del fill, session

    def _complete_cooldown_execution_session(self, session: date) -> None:
        """No-op retained only for legacy private method compatibility."""
        del session

    def _apply_drawdown_breaker(self, session: date, equity: float) -> None:
        drawdown = max(0.0, 1.0 - equity / self._state.high_water_equity)
        if drawdown > 0.10:
            was_parked = self._state.parked
            self._park("ACCOUNT_DRAWDOWN", session)
            if (
                not was_parked
                and self._switching_traces
                and self._switching_traces[-1].execution_session == session
            ):
                self._switching_traces[-1] = replace(
                    self._switching_traces[-1],
                    executed_allocation=self._allocation({}),
                )

    def _trade_to_target(self, session: date, cost_bps: int) -> None:
        state = self._state
        rate = cost_bps / 10_000.0
        opening_equity = self._equity(session, "open")
        target_weights = dict(state.pending_weights)
        deltas = {
            symbol: opening_equity * target_weights[symbol]
            - state.quantities[symbol] * self._price(symbol, session).open
            for symbol in _ORDERABLE_ASSETS
        }
        tolerance = opening_equity * 1e-12
        for symbol in _ORDERABLE_ASSETS:
            value_delta = deltas.get(symbol, 0.0)
            if value_delta >= -tolerance:
                continue
            bar = self._price(symbol, session)
            prior_quantity = state.quantities[symbol]
            sold_quantity = min(prior_quantity, -value_delta / bar.open)
            fill = bar.open * (1.0 - rate)
            state.cash += sold_quantity * fill
            state.quantities[symbol] = max(0.0, prior_quantity - sold_quantity)
            state.turnover += sold_quantity * bar.open / opening_equity
            state.trade_count += 1
        for symbol in _ORDERABLE_ASSETS:
            value_delta = deltas.get(symbol, 0.0)
            if value_delta <= tolerance:
                continue
            bar = self._price(symbol, session)
            fill = bar.open * (1.0 + rate)
            added_quantity = value_delta / fill
            if added_quantity * fill > state.cash + tolerance:
                raise TqqqPromotionEvidenceError("target portfolio exceeds available cash")
            prior_quantity = state.quantities[symbol]
            state.cash -= added_quantity * fill
            state.quantities[symbol] += added_quantity
            state.turnover += value_delta / opening_equity
            state.trade_count += 1
        self._record_executed_allocation(session)

    def _apply_stop(self, session: date, cost_bps: int) -> None:
        state = self._state
        quantity = state.quantities["TQQQ"]
        if quantity <= 0.0:
            return
        if state.tqqq_stop_price is None:
            state.tqqq_unprotected_holding_session_count += 1
            return
        bar = self._price("TQQQ", session)
        if bar.low > state.tqqq_stop_price:
            return
        state.tqqq_stop_crossing_count += 1
        exit_reference = min(bar.open, state.tqqq_stop_price)
        fill = exit_reference * (1.0 - cost_bps / 10_000.0)
        opening_equity = max(self._equity(session, "open"), 1e-12)
        state.cash += quantity * fill
        state.turnover += quantity * exit_reference / opening_equity
        state.trade_count += 1
        state.tqqq_stop_fill_count += 1
        self._record_completed_exit(fill, session)
        state.quantities["TQQQ"] = 0.0
        state.tqqq_entry_price = None
        state.tqqq_stop_price = None
        state.tqqq_entry_identity_sha256 = None
    def _current_weights(self, session: date, equity: float) -> dict[str, float]:
        if equity <= 0.0:
            return {symbol: 0.0 for symbol in _ORDERABLE_ASSETS}
        return {
            symbol: state_quantity * self._price(symbol, session).close / equity
            if state_quantity > 0.0
            else 0.0
            for symbol, state_quantity in self._state.quantities.items()
        }

    @staticmethod
    def _allocation(weights: Mapping[str, float]) -> tuple[tuple[str, float], ...]:
        targets = {
            symbol: _finite(weights.get(symbol, 0.0), "target weight")
            for symbol in _ORDERABLE_ASSETS
        }
        cash = 1.0 - math.fsum(targets.values())
        if cash < -1e-12:
            raise TqqqPromotionEvidenceError("invalid TQQQ switching allocation")
        return tuple(sorted({**targets, "cash": max(0.0, cash)}.items()))

    @staticmethod
    def _decision_weights(decision: object, equity: float) -> dict[str, float]:
        if not isinstance(decision, StrategyDecision) or equity <= 0.0:
            raise TqqqPromotionEvidenceError("invalid UES core-parity target")
        targets = {symbol: 0.0 for symbol in _ORDERABLE_ASSETS}
        for position in decision.positions:
            symbol = getattr(position, "symbol", None)
            if symbol in {"cash", "CASH"}:
                continue
            if symbol not in targets:
                continue
            weight = getattr(position, "target_weight", None)
            if weight is None:
                value = getattr(position, "target_value", None)
                if value is None:
                    raise TqqqPromotionEvidenceError("invalid UES core-parity target")
                weight = _finite(value, "target value") / equity
            targets[symbol] = _finite(weight, "target weight")
        _ImmutableReplayProducer._allocation(targets)
        return targets


    def _assessment(self, signal_index: int, execution_session: date, equity: float) -> dict[str, float]:
        """Build one candidate decision, then assess it exactly once."""
        state = self._state
        signal_session = self.qqq[signal_index].session
        if signal_index + 1 < 252:
            raise TqqqPromotionEvidenceError("insufficient candidate warmup")
        positions = tuple(
            Position(symbol=symbol, quantity=quantity, market_value=quantity * self._price(symbol, signal_session).close)
            for symbol, quantity in state.quantities.items() if quantity > 0.0
        )
        portfolio = PortfolioSnapshot(as_of=datetime.now(UTC), total_equity=equity, buying_power=state.cash, cash_balance=state.cash, positions=positions)
        context = StrategyContext(
            as_of=datetime.combine(signal_session, time(16, 0), tzinfo=ZoneInfo("America/New_York")),
            portfolio=portfolio,
            market_data={"benchmark_history": tuple({"date": row.session.isoformat(), "open": row.open, "high": row.high, "low": row.low, "close": row.close, "volume": row.volume} for row in self.qqq[:signal_index + 1]), "signal_session": signal_session.isoformat(), "next_execution_session": execution_session.isoformat()},
            runtime_config=_runtime_config(self.config),
        )
        decision = self._replay_callable(context)
        targets = self._decision_weights(decision, equity)
        assessment = build_risk_engine().assess(StrategyDecision(positions=tuple(PositionTarget(symbol=symbol, target_weight=weight) for symbol, weight in sorted(targets.items()) if weight > 0.0)), portfolio, market_data=context.market_data)
        state.decision_count += 1
        state.assessment_count += 1
        self._scenario_counts[self._scenario]["decisions"] += 1
        self._scenario_counts[self._scenario]["assessments"] += 1
        intended = self._allocation(targets)
        approved = assessment.action == "approve"
        executed = intended if approved else self._allocation({})
        if approved:
            signal_state = "entry" if targets["TQQQ"] or targets["QQQM"] else "idle"
            signal_regime = "RISK_ON" if signal_state == "entry" else "DEFENSIVE"
        else:
            signal_state = "risk_engine_non_approve"
            signal_regime = "DEFENSIVE"
        self._switching_traces.append(TqqqSwitchingTrace(
            signal_session=signal_session, execution_session=execution_session,
            signal_state=signal_state, signal_regime=signal_regime,
            intended_allocation=intended, risk_disposition="APPROVE" if approved else "REJECT",
            risk_reason_codes=() if approved else (assessment.reason,), replay_target_allocation=executed, executed_allocation=executed,
        ))
        return targets if approved else {symbol: 0.0 for symbol in _ORDERABLE_ASSETS}

    def __call__(
        self,
        start_date: date,
        end_date: date,
        total_cost_bps: int,
        prior_state_sha256: str,
    ) -> TqqqWindowReplay:
        self._reset(total_cost_bps, prior_state_sha256)
        if start_date < _EXACT_COMMON_ELIGIBILITY:
            raise TqqqPromotionEvidenceError("replay window precedes exact common eligibility")
        try:
            start_index = self._index[start_date]
            end_index = self._index[end_date]
        except KeyError as exc:
            raise TqqqPromotionEvidenceError("replay window data unavailable") from exc
        if start_index > end_index or start_index < 257:
            raise TqqqPromotionEvidenceError("replay window data unavailable")
        state = self._state
        for index in range(start_index, end_index + 1):
            for symbol in _ORDERABLE_ASSETS:
                self._price(symbol, self.qqq[index].session)
        state.pending_weights = self._assessment(
            start_index - 1,
            start_date,
            state.last_equity,
        )
        benchmark_origin = self.qqq[start_index].open
        defensive_benchmark_origin = self._price("BOXX", start_date).open
        strategy_origin = state.last_equity
        window_strategy: list[float] = [100.0]
        window_benchmark: list[float] = [100.0]
        window_defensive_benchmark: list[float] = [100.0]
        exposure_counts = {symbol: 0 for symbol in _ORDERABLE_ASSETS}
        cash_only_session_count = 0
        parked_session_count = 0
        for index in range(start_index, end_index + 1):
            qqq = self.qqq[index]
            self._trade_to_target(qqq.session, total_cost_bps)
            equity = self._equity(qqq.session, "close")
            if not math.isfinite(equity) or equity <= 0.0:
                raise TqqqPromotionEvidenceError("nonpositive replay equity")
            state.last_equity = equity
            state.high_water_equity = max(state.high_water_equity, equity)
            state.last_session = qqq.session
            if index < end_index:
                next_session = self.qqq[index + 1].session
                state.pending_weights = self._assessment(index, next_session, equity)
            window_strategy.append(equity / strategy_origin * 100.0)
            window_benchmark.append(qqq.close / benchmark_origin * 100.0)
            window_defensive_benchmark.append(
                self._price("BOXX", qqq.session).close
                / defensive_benchmark_origin
                * 100.0
            )
            has_exposure = False
            for symbol in _ORDERABLE_ASSETS:
                if state.quantities[symbol] > 1e-12:
                    exposure_counts[symbol] += 1
                    has_exposure = True
            if (
                state.parked
                and state.first_park_session is not None
                and qqq.session >= state.first_park_session
            ):
                parked_session_count += 1
            elif not has_exposure:
                cash_only_session_count += 1
            self._state_sha256 = _digest(_state_projection(state))
        final_weights = self._current_weights(self.qqq[end_index].session, state.last_equity)
        weights = tuple((symbol, final_weights[symbol]) for symbol in _ORDERABLE_ASSETS)
        return TqqqWindowReplay(
            start_date=start_date,
            end_date=end_date,
            prior_state_sha256=prior_state_sha256,
            final_state_sha256=self._state_sha256,
            strategy_equity=tuple(window_strategy),
            qqq_total_return_equity=tuple(window_benchmark),
            boxx_total_return_equity=tuple(window_defensive_benchmark),
            asset_weights=weights,
            turnover=state.turnover,
            trade_count=state.trade_count,
            decision_count=state.decision_count,
            risk_assessment_count=state.assessment_count,
            warmup_sessions=start_index,
            sessions=tuple(
                row.session for row in self.qqq[start_index : end_index + 1]
            ),
            episode_summary=TqqqEpisodeSummary(
                episode_session_count=end_index - start_index + 1,
                tqqq_exposure_session_count=exposure_counts["TQQQ"],
                qqqm_exposure_session_count=exposure_counts["QQQM"],
                boxx_exposure_session_count=exposure_counts["BOXX"],
                cash_only_session_count=cash_only_session_count,
                parked_session_count=parked_session_count,
                tqqq_entry_count=state.tqqq_entry_count,
                tqqq_stop_armed_count=state.tqqq_stop_armed_count,
                tqqq_stop_crossing_count=state.tqqq_stop_crossing_count,
                tqqq_stop_fill_count=state.tqqq_stop_fill_count,
                tqqq_unprotected_holding_session_count=(
                    state.tqqq_unprotected_holding_session_count
                ),
                breaker_reason=state.breaker_reason,
                first_park_session=state.first_park_session,
            ),
            market_regime_control_sha256=state.market_regime_control_sha256,
            risk_active_state_sha256=_digest({"weights": final_weights}),
            volatility_hysteresis_state_sha256=state.volatility_hysteresis_state_sha256,
            retention_state_sha256=state.retention_state_sha256,
            switching_traces=self.switching_traces,
        )


def _private_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path, 0o700)


def _write_private(path: Path, payload: bytes) -> dict[str, str]:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, 0o700)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)
    return {"path": path.as_posix(), "sha256": _sha256(payload)}


def _relative_record(root: Path, record: dict[str, str]) -> dict[str, str]:
    return {
        "path": Path(record["path"]).relative_to(root).as_posix(),
        "sha256": record["sha256"],
    }


def _refresh_digests(payload: dict[str, Any]) -> None:
    core = {field: payload[field] for field in _CORE_FIELDS}
    payload["digests"]["evidence_core_sha256"] = _sha256(canonical_evidence_package_v2_bytes(core))
    projection = copy.deepcopy(payload)
    projection["digests"].pop("package_sha256", None)
    payload["digests"]["package_sha256"] = _sha256(canonical_evidence_package_v2_bytes(projection))


def _result_artifacts(
    result: TqqqPromotionResearchResult,
    replay: _ImmutableReplayProducer,
    *,
    root: Path,
    config: Mapping[str, Any],
    manifest: Mapping[str, Any],
    strategy_execution: Mapping[str, str],
) -> dict[str, dict[str, str]]:
    artifacts = root / "artifacts"
    _private_directory(artifacts)
    scenarios = {scenario.total_cost_bps: scenario for scenario in result.scenarios}
    base = scenarios[5]
    locked = base.windows[-1]
    records = {
        "config": _write_private(artifacts / "config.json", _canonical(config)),
        "data_manifest": _write_private(
            artifacts / "data-manifest.json",
            canonical_research_input_manifest_bytes(manifest),
        ),
        "backtest": _write_private(
            artifacts / "backtest.json",
            _canonical(
                {
                    "schema_version": "tqqq_etf_only_promotion_backtest.v1",
                    "strategy_execution": strategy_execution,
                    "switching_characterization": build_tqqq_switching_characterization_contract(
                        result.identity
                    ),
                    "development_robustness_plan": result.systematic_reporting.plan,
                    "frozen_trial_ledger": result.frozen_trial_ledger,
                    "systematic_reporting": result.systematic_reporting,
                    "scenarios": {
                        str(cost): {
                            "promotion_run": scenario.promotion_run,
                            "windows": scenario.windows,
                        }
                        for cost, scenario in scenarios.items()
                    },
                }
            ),
        ),
        "risk": _write_private(
            artifacts / "risk.json",
            _canonical(
                {
                    "schema_version": "tqqq_etf_only_risk_evidence.v1",
                    "status": "PASS",
                    "risk_engine_exactly_once": True,
                    "scenario_counts": replay.scenario_counts,
                    "candidate_controls": "RiskEngine only; no endogenous stop or cooldown",
                    "authority_scope": "RESEARCH_ONLY",
                    "no_order": True,
                    "size_zero_required": True,
                }
            ),
        ),
        "information_coefficient": _write_private(
            artifacts / "information-coefficient.json",
            _canonical(
                {
                    "schema_version": "tqqq_information_coefficient.v1",
                    "information_coefficient": locked.relative_metrics.information_coefficient,
                    "information_ratio": locked.relative_metrics.information_ratio,
                    "alpha": locked.relative_metrics.alpha,
                    "beta": locked.relative_metrics.beta,
                    "up_market_capture": locked.relative_metrics.up_market_capture,
                    "down_market_capture": locked.relative_metrics.down_market_capture,
                }
            ),
        ),
        "cost_model": _write_private(
            artifacts / "cost-model.json",
            _canonical(
                {
                    "schema_version": "tqqq_all_in_per_side_cost.v1",
                    "method": "adverse_open_fill",
                    "scenarios_bps": [
                        scenario.total_cost_bps for scenario in result.scenarios
                    ],
                }
            ),
        ),
    }
    return {name: _relative_record(root, record) for name, record in records.items()}


def _run_tqqq_promotion_replay(
    *,
    input_payload: Mapping[str, Any],
    config_payload: Mapping[str, Any],
    mandate_receipt_sha256: str,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, tuple[_Bar, ...]],
    dict[str, Any],
    str,
    CandidateRiskIdentity,
    _ImmutableReplayProducer,
    TqqqPromotionResearchResult,
    dict[str, str],
]:
    mandate_receipt_sha256 = _digest_text(
        mandate_receipt_sha256, 64, "mandate receipt"
    )
    config = _validate_config(config_payload)
    candidate_config = config["candidate"]
    assert isinstance(candidate_config, Mapping)
    contract = _candidate_contract(candidate_config)
    provenance, bars, manifest_sha256 = _validate_input(input_payload, config)
    plan = _plan_from_candidate(
        candidate_config,
        data_cutoff=_bound_data_cutoff(input_payload, contract),
    )
    manifest = validate_research_input_manifest(input_payload["input_manifest"])
    runner_revision = _resolve_runner_revision()
    replay_callable, strategy_execution = _tqqq_replay_callable_and_identity(contract)
    config_sha256 = contract.config_sha256
    initial_state_sha256 = _digest(_initial_state_projection())
    candidate = CandidateRiskIdentity(
        strategy_profile=contract.candidate_id,
        account_mode="single_strategy_account_v1",
        strategy_revision=contract.ues_revision,
        runner_revision=runner_revision,
        config_sha256=config_sha256,
        input_manifest_sha256=manifest_sha256,
        authority_receipt_sha256=mandate_receipt_sha256,
    )
    identity = TqqqPromotionIdentity(
        qpk_revision=contract.qpk_revision,
        ues_revision=contract.ues_revision,
        runner_revision=runner_revision,
        config_sha256=config_sha256,
        input_manifest_sha256=manifest_sha256,
        mandate_receipt_sha256=mandate_receipt_sha256,
        initial_state_sha256=initial_state_sha256,
        candidate_profile=contract.candidate_id,
        candidate_variant=contract.candidate_id,
    )
    replay = _ImmutableReplayProducer(
        bars, candidate_config, candidate, identity, replay_callable
    )
    result = run_tqqq_promotion_research(identity, plan, replay, candidate_config["cost_assumptions"])
    return (
        config,
        provenance,
        bars,
        manifest,
        manifest_sha256,
        candidate,
        replay,
        result,
        strategy_execution,
    )


def run_tqqq_promotion_diagnostic(
    *,
    input_payload: Mapping[str, Any],
    config_payload: Mapping[str, Any],
    mandate_receipt_sha256: str,
) -> None:
    """Execute the frozen replay without writing promotion evidence."""
    _run_tqqq_promotion_replay(
        input_payload=input_payload,
        config_payload=config_payload,
        mandate_receipt_sha256=mandate_receipt_sha256,
    )


def run_tqqq_promotion_evidence(
    *,
    input_payload: Mapping[str, Any],
    config_payload: Mapping[str, Any],
    output_dir: str | Path,
    mandate_receipt_sha256: str,
    generated_at: str | None = None,
) -> dict[str, str]:
    """Execute the frozen replay once and write no provider bars to evidence."""

    output_root = Path(output_dir)
    if output_root.exists() and any(output_root.iterdir()):
        raise TqqqPromotionEvidenceError("output directory must be empty")
    (
        config,
        provenance,
        bars,
        manifest,
        manifest_sha256,
        candidate,
        replay,
        result,
        strategy_execution,
    ) = (
        _run_tqqq_promotion_replay(
            input_payload=input_payload,
            config_payload=config_payload,
            mandate_receipt_sha256=mandate_receipt_sha256,
        )
    )
    _private_directory(output_root)
    records = _result_artifacts(
        result,
        replay,
        root=output_root,
        config=config,
        manifest=manifest,
        strategy_execution=strategy_execution,
    )
    base = result.scenarios[0]
    locked = base.windows[-1]
    metrics = locked.relative_metrics
    verdict = evaluate_tqqq_pre_result_acceptance(result, "NOT_COMPARABLE")
    generated = _timestamp(generated_at)
    evidence: dict[str, Any] = {
        "schema_version": "strategy_evidence_package.v2",
        "evidence_package_id": f"tqqq_p2_{candidate.candidate_sha256[:12]}",
        "generated_at": generated,
        "requested_stage": "research_backtest_only",
        "strategy": {
            "profile": candidate.strategy_profile,
            "domain": _DOMAIN,
            "source_revision": candidate.strategy_revision,
        },
        "input_provenance": {
            "source": provenance["source"],
            "source_revision": provenance["source_revision"],
            "license": provenance["license"],
            "usage_scope": provenance["usage_scope"],
            "range": {
                "start": bars["QQQ"][0].session.isoformat(),
                "end": bars["QQQ"][-1].session.isoformat(),
            },
            "timestamp": manifest["observed_at"],
            "manifest_sha256": manifest_sha256,
        },
        "backtest": {
            "orchestrator": "BacktestOrchestrator",
            "protocol": "purged_walk_forward.v1",
            "calendar": "XNYS",
            "timezone": "America/New_York",
            "signal_timing": "close_t",
            "execution_timing": "open_t_plus_1",
            "locked_independent_oos": {
                "locked": True,
                "independent": True,
                "reused_for_selection": False,
            },
            "promotion_run": base.promotion_run.to_dict(),
        },
        "artifacts": records,
        "metrics": {
            "sharpe_ratio": metrics.sharpe_ratio,
            "sortino_ratio": metrics.sortino_ratio,
            "max_drawdown": metrics.strategy_max_drawdown,
            "annualized_return": metrics.strategy_cagr,
            "annualized_volatility": metrics.annualized_volatility,
            "calmar_ratio": metrics.calmar_ratio,
            "information_ratio": metrics.information_ratio,
            "information_coefficient": metrics.information_coefficient,
            "var_95": metrics.var_95,
            "cvar_95": metrics.cvar_95,
            "turnover": metrics.turnover,
            "trade_count": metrics.trade_count,
            "win_rate": metrics.win_rate,
            "profit_factor": metrics.profit_factor,
        },
        "cost_stress": {
            "scenarios": [
                {
                    "multiplier": index,
                    "total_cost_bps": float(scenario.total_cost_bps),
                }
                for index, scenario in enumerate(result.scenarios, start=1)
            ],
            "status": "PASS",
        },
        "risk_assessment": {
            "status": "PASS",
            "standard_id": config["risk_standard_id"],
            "standard_sha256": config["risk_standard_sha256"],
        },
        "digests": {
            "config_sha256": records["config"]["sha256"],
            "data_manifest_sha256": records["data_manifest"]["sha256"],
            "backtest_sha256": records["backtest"]["sha256"],
            "risk_sha256": records["risk"]["sha256"],
            "information_coefficient_sha256": records["information_coefficient"]["sha256"],
            "cost_model_sha256": records["cost_model"]["sha256"],
            "evidence_core_sha256": "0" * 64,
            "package_sha256": "0" * 64,
        },
        "human_acceptance": None,
        "lifecycle_claims": {
            "learning_only": True,
            "promotion_eligible": False,
            "live_ready": False,
            "size_zero_required": True,
            "no_order": True,
        },
    }
    _refresh_digests(evidence)
    issues = validate_evidence_package_v2(evidence, base_dir=output_root)
    if issues:
        raise TqqqPromotionEvidenceError("evidence package validation failed:" + ";".join(issues))
    evidence_bytes = canonical_evidence_package_v2_bytes(evidence)
    evidence_record = _write_private(output_root / "strategy-evidence-package.v2.json", evidence_bytes)
    terminal_record = _write_private(
        output_root / "promotion-research-result.v1.json",
        _canonical(
            {
                "schema_version": "tqqq_promotion_research_result.v1",
                "generated_at": generated,
                "status": "EVIDENCE_V2_COMPLETE",
                "verdict": verdict,
                "candidate_identity_sha256": candidate.candidate_sha256,
                "input_manifest_sha256": manifest_sha256,
                "evidence_sha256": evidence_record["sha256"],
                "human_acceptance": None,
                "authority_scope": "RESEARCH_ONLY",
                "learning_only": True,
                "promotion_eligible": False,
                "live_ready": False,
                "size_zero_required": True,
                "no_order": True,
            }
        ),
    )
    return {
        "evidence_sha256": evidence_record["sha256"],
        "promotion_result_sha256": terminal_record["sha256"],
        "candidate_identity_sha256": candidate.candidate_sha256,
        "input_manifest_sha256": manifest_sha256,
        "verdict": verdict,
    }


__all__ = [
    "TqqqPromotionEvidenceError",
    "run_tqqq_promotion_diagnostic",
    "run_tqqq_promotion_evidence",
]
