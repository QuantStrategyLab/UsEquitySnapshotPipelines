"""Private-input, offline-only TQQQ promotion evidence producer."""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from quant_platform_kit.common.models import PortfolioSnapshot, Position
from quant_platform_kit.data.research_input import (
    InvalidResearchInputEvidence,
    canonical_research_input_manifest_bytes,
    research_input_manifest_sha256,
    validate_research_input_manifest,
)
from quant_platform_kit.risk.contracts import CandidateRiskIdentity
from quant_platform_kit.strategy_contracts import StrategyContext
from quant_platform_kit.strategy_lifecycle.contracts import PurgedWalkForwardFold
from quant_platform_kit.strategy_lifecycle.evidence_package_v2 import (
    canonical_evidence_package_v2_bytes,
    validate_evidence_package_v2,
)
from us_equity_strategies.entrypoints import (
    evaluate_tqqq_growth_income_promotion_research,
)

from .tqqq_promotion_runner import (
    _QPK_REVISION,
    _UES_REVISION,
    _EXACT_COMMON_ELIGIBILITY,
    _FROZEN_CALENDAR_SOURCE_REVISION,
    _LOCKED_OOS_END,
    _LOCKED_OOS_SESSION_COUNT,
    _LOCKED_OOS_SESSIONS_SHA256,
    _LOCKED_OOS_START,
    TqqqEpisodeSummary,
    TqqqPromotionIdentity,
    TqqqPromotionPlan,
    TqqqPromotionResearchResult,
    TqqqWindowReplay,
    build_tqqq_development_robustness_plan,
    _resolve_runner_revision,
    run_tqqq_promotion_research,
)

_PROFILE = "tqqq_core_parity_v1"
_DOMAIN = "us_equity"
_INPUT_CONTRACT_ID = "tqqq_etf_only_ibkr_adjusted_last.v1"
_INPUT_SCHEMA = "tqqq_etf_only_private_bars.v1"
_CONFIG_SCHEMA = "tqqq_etf_only_replay_config.v1"
_MANDATE_ID = "tqqq_core_parity_v1"
_LICENSE = "GFIS_API_NON_COMMERCIAL_PERSONAL_RESTRICTED_2026-02-04"
_USAGE_SCOPE = "PRIVATE_LOCAL_NONCOMMERCIAL_RESEARCH_NO_REDISTRIBUTION"
_SESSION_PROVIDER = {
    "paper": "IBKR Paper Gateway TWS API",
    "live-data-only": "IBKR Live Gateway Data Only TWS API",
}
_SESSION_TOOL = {
    "paper": "tqqq_ibkr_paper_single_acquisition",
    "live-data-only": "tqqq_ibkr_live_data_only_single_acquisition",
}
_BOXX_FIRST_ELIGIBLE_SESSION = _EXACT_COMMON_ELIGIBILITY
_COST_SCENARIOS = (5, 10, 15)
_ORDERABLE_ASSETS = ("TQQQ", "QQQM", "BOXX")
_ASSET_FACTORS = {"TQQQ": 3, "QQQM": 1, "BOXX": 1}
_RUNTIME_OVERRIDES = {
    "benchmark_symbol": "QQQ",
    "managed_symbols": _ORDERABLE_ASSETS,
    "signal_effective_after_trading_days": 1,
    "dual_drive_unlevered_symbol": "QQQM",
    "income_layer_enabled": False,
    "option_overlay_enabled": False,
    "option_growth_overlay_enabled": False,
    "option_income_overlay_enabled": False,
    "ai_extensions": {"enabled": False},
}
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
_PLAN = TqqqPromotionPlan(
    folds=(
        PurgedWalkForwardFold(
            train_start=date(2018, 1, 2),
            train_end=date(2022, 11, 25),
            test_start=date(2022, 12, 28),
            test_end=date(2023, 1, 31),
        ),
        PurgedWalkForwardFold(
            train_start=date(2023, 2, 21),
            train_end=date(2023, 4, 28),
            test_start=date(2023, 5, 22),
            test_end=date(2023, 6, 30),
        ),
        PurgedWalkForwardFold(
            train_start=date(2023, 7, 24),
            train_end=date(2023, 10, 31),
            test_start=date(2023, 12, 1),
            test_end=date(2024, 5, 31),
        ),
    ),
    locked_oos_start=_LOCKED_OOS_START,
    locked_oos_end=_LOCKED_OOS_END,
    purge_days=20,
    embargo_days=20,
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
    fields = {
        "schema_version",
        "strategy_profile",
        "signal_model",
        "signal_window_sessions",
        "tqqq_nominal_cap",
        "qqqm_nominal_cap",
        "boxx_nominal_cap",
        "risk_mandate_id",
        "risk_standard_id",
        "risk_standard_sha256",
        "authority_receipt_sha256",
        "platform_execution_revision",
        "input_license",
        "input_usage_scope",
        "session_class",
    }
    config = _exact_mapping(value, fields, "config")
    if (
        config["schema_version"] != _CONFIG_SCHEMA
        or config["strategy_profile"] != _PROFILE
        or config["signal_model"] != "ues_tqqq_growth_income_core_parity"
        or config["signal_window_sessions"] != 257
        or _finite(config["tqqq_nominal_cap"], "TQQQ cap") != 0.15
        or _finite(config["qqqm_nominal_cap"], "QQQM cap") != 0.50
        or _finite(config["boxx_nominal_cap"], "BOXX cap") != 0.50
        or config["risk_mandate_id"] != _MANDATE_ID
        or not isinstance(config["risk_standard_id"], str)
        or not config["risk_standard_id"]
        or config["input_license"] != _LICENSE
        or config["input_usage_scope"] != _USAGE_SCOPE
        or not isinstance(config["session_class"], str)
        or config["session_class"] not in _SESSION_PROVIDER
    ):
        raise TqqqPromotionEvidenceError("invalid frozen config")
    _digest_text(config["risk_standard_sha256"], 64, "risk standard digest")
    _digest_text(config["authority_receipt_sha256"], 64, "authority digest")
    _digest_text(config["platform_execution_revision"], 40, "platform revision")
    return config


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


def _validate_input(
    value: Mapping[str, Any], config: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, tuple[_Bar, ...]], str]:
    payload = _exact_mapping(value, {"provenance", "input_manifest", "bars"}, "input payload")
    provenance = _exact_mapping(
        payload["provenance"],
        {
            "evidence_class",
            "real_producer",
            "provider",
            "provider_revision",
            "license",
            "usage_scope",
            "session_class",
        },
        "input provenance",
    )
    if (
        provenance["evidence_class"] != "provider_observed"
        or provenance["real_producer"] is not True
        or provenance["session_class"] != config["session_class"]
        or provenance["provider"] != _SESSION_PROVIDER[config["session_class"]]
        or not isinstance(provenance["provider_revision"], str)
        or not provenance["provider_revision"]
        or provenance["license"] != config["input_license"]
        or provenance["usage_scope"] != config["input_usage_scope"]
    ):
        raise TqqqPromotionEvidenceError("invalid provider provenance")
    try:
        manifest = validate_research_input_manifest(payload["input_manifest"])
    except InvalidResearchInputEvidence as exc:
        raise TqqqPromotionEvidenceError("invalid input manifest") from exc
    if (
        manifest["research_input_contract_id"] != _INPUT_CONTRACT_ID
        or manifest["domain"] != _DOMAIN
        or manifest["profile"] != _PROFILE
        or manifest["artifact_type"] != "immutable_adjusted_ohlcv_etf_only"
        or manifest["calendar"]["calendar_id"] != "XNYS"
        or manifest["calendar"]["timezone"] != "America/New_York"
        or manifest["calendar"]["source"] != "exchange_calendars"
        or manifest["calendar"]["source_revision"] != _FROZEN_CALENDAR_SOURCE_REVISION
        or manifest["adjustment"]["policy"] != "total_return_adjusted"
        or manifest["adjustment"]["source"] != "IBKR_ADJUSTED_LAST"
    ):
        raise TqqqPromotionEvidenceError("invalid immutable input contract")
    session_class = config["session_class"]
    manifest_prefix = f"tqqq-ibkr-{session_class}-single-acquisition-"
    manifest_suffix = manifest["manifest_id"].removeprefix(manifest_prefix)
    producer = manifest["producer"]
    if (
        not manifest["manifest_id"].startswith(manifest_prefix)
        or len(manifest_suffix) != 24
        or any(character not in "0123456789abcdef" for character in manifest_suffix)
        or producer["repository"] != "QuantStrategyLab/UsEquitySnapshotPipelines"
        or producer["tool"] != _SESSION_TOOL[session_class]
        or producer["tool_version"] != "v1"
    ):
        raise TqqqPromotionEvidenceError("invalid provider session identity")
    bars_payload = _exact_mapping(payload["bars"], {"schema_version", "symbols"}, "bars payload")
    symbols = _exact_mapping(
        bars_payload["symbols"], {"BOXX", "QQQ", "QQQM", "TQQQ"}, "bar symbols"
    )
    if bars_payload["schema_version"] != _INPUT_SCHEMA:
        raise TqqqPromotionEvidenceError("invalid bars schema")
    bars_bytes = _canonical(bars_payload)
    if manifest_suffix != _sha256(bars_bytes)[:24]:
        raise TqqqPromotionEvidenceError("input identity mismatch")
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
    source_digests = {item["source_id"]: item["content_sha256"] for item in manifest["sources"]}
    source_revisions = {item["revision"] for item in manifest["sources"]}
    if (
        set(source_digests) != {"ibkr:BOXX", "ibkr:QQQ", "ibkr:QQQM", "ibkr:TQQQ"}
        or source_revisions != {provenance["provider_revision"]}
    ):
        raise TqqqPromotionEvidenceError("invalid provider source identities")
    for symbol in ("BOXX", "QQQ", "QQQM", "TQQQ"):
        rows = symbols[symbol]
        if not isinstance(rows, list) or not rows:
            raise TqqqPromotionEvidenceError("missing immutable bars")
        if source_digests[f"ibkr:{symbol}"] != _sha256(_canonical(rows)):
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
        qqq_sessions[0] > _PLAN.folds[0].train_start
        or qqq_sessions[-1] != _PLAN.locked_oos_end
        or manifest["calendar"]["session_date"] != qqq_sessions[-1].isoformat()
        or sum(session < _PLAN.folds[0].test_start for session in qqq_sessions) < 257
    ):
        raise TqqqPromotionEvidenceError("immutable input coverage mismatch")
    locked_sessions = tuple(
        session.isoformat()
        for session in qqq_sessions
        if _LOCKED_OOS_START <= session <= _LOCKED_OOS_END
    )
    if (
        len(locked_sessions) != _LOCKED_OOS_SESSION_COUNT
        or _sha256(_canonical(list(locked_sessions))) != _LOCKED_OOS_SESSIONS_SHA256
    ):
        raise TqqqPromotionEvidenceError("locked OOS calendar identity mismatch")
    return provenance, parsed, research_input_manifest_sha256(manifest)


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
    ) -> None:
        self.config = config
        self.candidate = candidate
        self.identity = identity
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

    @property
    def scenario_counts(self) -> dict[int, dict[str, int]]:
        return copy.deepcopy(self._scenario_counts)

    def _reset(self, scenario: int, prior_state_sha256: str) -> None:
        if scenario not in _COST_SCENARIOS:
            raise TqqqPromotionEvidenceError("invalid cost scenario")
        if prior_state_sha256 != self.identity.initial_state_sha256:
            raise TqqqPromotionEvidenceError("initial state identity mismatch")
        self._scenario = scenario
        self._state = _ReplayState()
        self._state_sha256 = self.identity.initial_state_sha256
        self._scenario_counts.setdefault(scenario, {"decisions": 0, "assessments": 0})

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

    def _park(self, reason: str, session: date) -> None:
        state = self._state
        if state.parked:
            return
        state.parked = True
        state.breaker_reason = reason
        state.first_park_session = session
        state.pending_weights = {symbol: 0.0 for symbol in _ORDERABLE_ASSETS}

    def _record_completed_exit(self, fill: float, session: date) -> None:
        state = self._state
        losing = state.tqqq_entry_price is not None and fill < state.tqqq_entry_price
        state.consecutive_losing_exits = state.consecutive_losing_exits + 1 if losing else 0
        if state.consecutive_losing_exits >= 5:
            self._park("CONSECUTIVE_TQQQ_LOSING_EXITS", session)

    def _apply_drawdown_breaker(self, session: date, equity: float) -> None:
        drawdown = max(0.0, 1.0 - equity / self._state.high_water_equity)
        if drawdown > 0.10:
            self._park("ACCOUNT_DRAWDOWN", session)

    def _trade_to_target(self, session: date, cost_bps: int) -> None:
        state = self._state
        rate = cost_bps / 10_000.0
        opening_equity = self._equity(session, "open")
        target_weights = {
            symbol: 0.0 if state.parked else state.pending_weights[symbol]
            for symbol in _ORDERABLE_ASSETS
        }
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
            if symbol == "TQQQ" and state.quantities[symbol] <= 1e-12:
                self._record_completed_exit(fill, session)
                state.quantities[symbol] = 0.0
                state.tqqq_entry_price = None
                state.tqqq_stop_price = None
                state.tqqq_entry_identity_sha256 = None
        if state.parked:
            return
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
            if symbol == "TQQQ":
                state.tqqq_entry_count += 1
                state.tqqq_entry_price = (
                    (state.tqqq_entry_price or bar.open) * prior_quantity
                    + fill * added_quantity
                ) / state.quantities[symbol]
                state.tqqq_stop_price = state.tqqq_entry_price * 0.95
                state.tqqq_stop_armed_count += 1
                state.tqqq_entry_identity_sha256 = _digest(
                    {
                        "candidate": self.candidate.candidate_sha256,
                        "session": session,
                        "symbol": symbol,
                        "fill": fill,
                        "quantity": state.quantities[symbol],
                    }
                )

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

    def _assessment(
        self, signal_index: int, execution_session: date, equity: float
    ) -> dict[str, float]:
        state = self._state
        signal_session = self.qqq[signal_index].session
        if state.parked:
            raise TqqqPromotionEvidenceError("parked episode cannot create a decision")
        if signal_index + 1 < 257:
            raise TqqqPromotionEvidenceError("insufficient core-parity warmup")
        drawdown = max(0.0, 1.0 - equity / state.high_water_equity)
        scalar = 1.0 if drawdown <= 0.05 else 0.5 if drawdown <= 0.10 else 0.0
        if drawdown > 0.10:
            self._park("ACCOUNT_DRAWDOWN", signal_session)
            return {symbol: 0.0 for symbol in _ORDERABLE_ASSETS}
        now = datetime.now(UTC)
        effective_at = (now - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
        expires_at = (now + timedelta(days=30)).isoformat().replace("+00:00", "Z")
        evaluated_at = now.isoformat().replace("+00:00", "Z")
        entry_identity = state.tqqq_entry_identity_sha256 or _digest(
            {
                "candidate": self.candidate.candidate_sha256,
                "signal_session": signal_session,
                "execution_session": execution_session,
                "precommitted_stop": True,
            }
        )
        mandate = {
            "mandate_id": _MANDATE_ID,
            "mandate_version": "v1",
            "authority_receipt_sha256": self.candidate.authority_receipt_sha256,
            "authority_scope": "RESEARCH_ONLY",
            "strategy_profile": self.candidate.strategy_profile,
            "account_mode": self.candidate.account_mode,
            "strategy_revision": self.candidate.strategy_revision,
            "runner_revision": self.candidate.runner_revision,
            "config_sha256": self.candidate.config_sha256,
            "input_manifest_sha256": self.candidate.input_manifest_sha256,
            "candidate_identity_sha256": self.candidate.candidate_sha256,
            "effective_at": effective_at,
            "expires_at": expires_at,
            "max_snapshot_age_seconds": 300,
            "effective_exposure_cap": 0.50,
            "loss_budget": 0.01,
            "loss_budget_equity_reference": "completed_session_equity",
            "product_caps": {"TQQQ": 0.15, "QQQM": 0.50, "BOXX": 0.50},
            "nominal_caps": {"TQQQ": 0.15, "QQQM": 0.50, "BOXX": 0.50},
            "product_effective_caps": {"TQQQ": 0.45, "QQQM": 0.50, "BOXX": 0.50},
            "product_leverage_factors": _ASSET_FACTORS,
            "allowed_nonzero_assets": list(_ORDERABLE_ASSETS),
            "max_nonzero_assets": 3,
            "broker_margin_factor": 1,
            "margin_stacking": False,
            "borrowing": False,
            "shorting": False,
            "income_sleeve_enabled": False,
            "option_overlay_enabled": False,
            "precommitted_executable_stop_distance": 0.05,
            "max_consecutive_completed_losing_exits": 5,
            "source_revision": _QPK_REVISION,
        }
        risk_state = {
            "as_of": evaluated_at,
            "mandate_id": _MANDATE_ID,
            "candidate_identity_sha256": self.candidate.candidate_sha256,
            "stop_loss_distance": 0.05,
            "stop_intent_ready": True,
            "tqqq_entry_fill_identity_sha256": entry_identity,
            "stop_entry_fill_identity_sha256": entry_identity,
            "consecutive_completed_losing_exits": state.consecutive_losing_exits,
            "account_drawdown_fraction": drawdown,
            "drawdown_scalar": scalar,
        }
        positions = tuple(
            Position(
                symbol=symbol,
                quantity=quantity,
                market_value=quantity * self._price(symbol, signal_session).close,
                average_cost=(state.tqqq_entry_price if symbol == "TQQQ" else None),
            )
            for symbol, quantity in state.quantities.items()
            if quantity > 0.0
        )
        observed_weights = self._current_weights(signal_session, equity)
        portfolio = PortfolioSnapshot(
                as_of=now,
                total_equity=equity,
                buying_power=state.cash,
                cash_balance=state.cash,
                positions=positions,
                metadata={
                    "observed_effective_exposure": math.fsum(
                        observed_weights[symbol] * _ASSET_FACTORS[symbol]
                        for symbol in _ORDERABLE_ASSETS
                    )
                },
            )
        completed_session = datetime.combine(
            signal_session,
            time(16, 0),
            tzinfo=ZoneInfo("America/New_York"),
        )
        benchmark_history = tuple(
            {
                "date": row.session.isoformat(),
                "open": row.open,
                "high": row.high,
                "low": row.low,
                "close": row.close,
                "volume": row.volume,
            }
            for row in self.qqq[: signal_index + 1]
        )
        result = evaluate_tqqq_growth_income_promotion_research(
            StrategyContext(
                as_of=completed_session,
                portfolio=portfolio,
                market_data={
                    "benchmark_history": benchmark_history,
                    "signal_session": signal_session.isoformat(),
                    "next_execution_session": execution_session.isoformat(),
                },
                runtime_config=_RUNTIME_OVERRIDES,
            ),
            mandate_provenance=mandate,
            candidate_identity=self.candidate,
            stop_loss_distances={symbol: 0.05 for symbol in _ORDERABLE_ASSETS},
            drawdown_scalar=scalar if not state.parked else 0.0,
            inputs_fresh=not state.parked,
            risk_control_state=risk_state,
        )
        state.decision_count += 1
        state.assessment_count += 1
        self._scenario_counts[self._scenario]["decisions"] += 1
        self._scenario_counts[self._scenario]["assessments"] += 1
        assessment = result.assessment
        if assessment.execution_authorized is not False:
            raise TqqqPromotionEvidenceError("execution authority is forbidden")
        if assessment.outcome != "APPROVE":
            if state.parked:
                return {symbol: 0.0 for symbol in _ORDERABLE_ASSETS}
            raise TqqqPromotionEvidenceError(
                "RiskEngine rejected immutable replay decision:"
                + ",".join(assessment.reason_codes)
            )
        targets = {symbol: 0.0 for symbol in _ORDERABLE_ASSETS}
        for target in result.decision.positions:
            if target.symbol not in targets or target.target_weight is None:
                raise TqqqPromotionEvidenceError("invalid UES core-parity target")
            targets[target.symbol] = _finite(target.target_weight, "target weight")
        diagnostics = result.decision.diagnostics
        state.market_regime_control_sha256 = _digest(
            {
                "enabled": diagnostics.get("market_regime_control_enabled"),
                "found": diagnostics.get("market_regime_control_found"),
                "route": diagnostics.get("market_regime_control_route"),
                "active": diagnostics.get("market_regime_control_active"),
            }
        )
        state.volatility_hysteresis_state_sha256 = _digest(
            {
                "triggered": diagnostics.get("dual_drive_volatility_delever_triggered"),
                "entry_triggered": diagnostics.get(
                    "dual_drive_volatility_delever_entry_triggered"
                ),
                "hysteresis_triggered": diagnostics.get(
                    "dual_drive_volatility_delever_hysteresis_triggered"
                ),
            }
        )
        state.retention_state_sha256 = _digest(
            {
                "mode": diagnostics.get("dual_drive_volatility_delever_retention_mode"),
                "policy": diagnostics.get("dual_drive_volatility_delever_retention_policy"),
                "ratio": diagnostics.get("dual_drive_volatility_delever_retention_ratio"),
                "source": diagnostics.get("dual_drive_volatility_delever_retention_source"),
            }
        )
        return targets

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
            self._apply_stop(qqq.session, total_cost_bps)
            equity = self._equity(qqq.session, "close")
            if not math.isfinite(equity) or equity <= 0.0:
                raise TqqqPromotionEvidenceError("nonpositive replay equity")
            state.last_equity = equity
            state.high_water_equity = max(state.high_water_equity, equity)
            state.last_session = qqq.session
            self._apply_drawdown_breaker(qqq.session, equity)
            if not state.parked and index < end_index:
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
            if state.parked:
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
                    "development_robustness_plan": build_tqqq_development_robustness_plan(
                        tuple(row.session for row in replay.qqq)
                    ),
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
                    "hard_stop_distance": 0.05,
                    "account_drawdown_breaker": 0.10,
                    "strategy_losing_exit_breaker": 5,
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
                    "scenarios_bps": list(_COST_SCENARIOS),
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
]:
    mandate_receipt_sha256 = _digest_text(
        mandate_receipt_sha256, 64, "mandate receipt"
    )
    config = _validate_config(config_payload)
    provenance, bars, manifest_sha256 = _validate_input(input_payload, config)
    manifest = validate_research_input_manifest(input_payload["input_manifest"])
    runner_revision = _resolve_runner_revision()
    config_sha256 = _sha256(_canonical(config))
    initial_state_sha256 = _digest(_initial_state_projection())
    candidate = CandidateRiskIdentity(
        strategy_profile=_PROFILE,
        account_mode="single_strategy_account_v1",
        strategy_revision=_UES_REVISION,
        runner_revision=runner_revision,
        config_sha256=config_sha256,
        input_manifest_sha256=manifest_sha256,
        authority_receipt_sha256=config["authority_receipt_sha256"],
    )
    identity = TqqqPromotionIdentity(
        qpk_revision=_QPK_REVISION,
        ues_revision=_UES_REVISION,
        runner_revision=runner_revision,
        platform_execution_revision=config["platform_execution_revision"],
        config_sha256=config_sha256,
        input_manifest_sha256=manifest_sha256,
        mandate_receipt_sha256=mandate_receipt_sha256,
        initial_state_sha256=initial_state_sha256,
    )
    replay = _ImmutableReplayProducer(bars, config, candidate, identity)
    result = run_tqqq_promotion_research(identity, _PLAN, replay)
    return config, provenance, bars, manifest, manifest_sha256, candidate, replay, result


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
    config, provenance, bars, manifest, manifest_sha256, candidate, replay, result = (
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
    )
    base = result.scenarios[0]
    locked = base.windows[-1]
    metrics = locked.relative_metrics
    generated = _timestamp(generated_at)
    evidence: dict[str, Any] = {
        "schema_version": "strategy_evidence_package.v2",
        "evidence_package_id": f"tqqq_p2_{candidate.candidate_sha256[:12]}",
        "generated_at": generated,
        "requested_stage": "research_backtest_only",
        "strategy": {
            "profile": _PROFILE,
            "domain": _DOMAIN,
            "source_revision": _UES_REVISION,
        },
        "input_provenance": {
            "source": provenance["provider"],
            "source_revision": provenance["provider_revision"],
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
                {"multiplier": index, "total_cost_bps": float(cost)}
                for index, cost in enumerate(_COST_SCENARIOS, start=1)
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
            "learning_only": False,
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
                "candidate_identity_sha256": candidate.candidate_sha256,
                "input_manifest_sha256": manifest_sha256,
                "evidence_sha256": evidence_record["sha256"],
                "human_acceptance": None,
                "authority_scope": "RESEARCH_ONLY",
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
    }


__all__ = [
    "TqqqPromotionEvidenceError",
    "run_tqqq_promotion_diagnostic",
    "run_tqqq_promotion_evidence",
]
