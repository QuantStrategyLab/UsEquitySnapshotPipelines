"""Offline, candidate-bound SOXL promotion-research backtest runner."""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.metadata
import json
import math
import statistics
import subprocess
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from quant_platform_kit import build_semiconductor_rotation_indicators_from_history
from quant_platform_kit.common.models import PortfolioSnapshot, Position
from quant_platform_kit.data.research_input import (
    canonical_research_input_manifest_bytes,
    research_input_manifest_sha256,
    validate_research_input_manifest,
)
from quant_platform_kit.risk.contracts import CandidateRiskIdentity
from quant_platform_kit.risk.gate import assess_with_evidence
from quant_platform_kit.common.strategy_contracts import PositionTarget, StrategyContext, StrategyDecision
from quant_platform_kit.strategy_lifecycle.backtest_orchestrator import BacktestOrchestrator
from quant_platform_kit.strategy_lifecycle.contracts import (
    BacktestResult,
    PromotionBacktestRun,
    PromotionCostModel,
    PurgedWalkForwardFold,
)
from quant_platform_kit.strategy_lifecycle.evidence_package_v2 import (
    canonical_evidence_package_v2_bytes,
    validate_evidence_package_v2,
)
from quant_platform_kit.strategy_lifecycle.performance_store import PerformanceStore
from us_equity_strategies.entrypoints import evaluate_soxl_soxx_trend_income_promotion_research
from us_equity_strategies.manifests import soxl_soxx_trend_income_manifest

from .soxl_pit_input_packager import FIRST_ELIGIBLE_SESSION, INPUT_CONTRACT_ID, MANDATE_ID
from .soxl_pit_regime_component_producer import (
    CANDIDATE_ID,
    CORE_ONLY_CONFIG_SHA256,
    MARKET_REGIME_SCHEMA,
    SOURCE_CONTRACT_SCHEMA,
    UNAVAILABLE_COMPONENTS,
)


SOXL_PROMOTION_ASSETS = (
    "SOXL",
    "SOXX",
    "BOXX",
    "SCHD",
    "DGRO",
    "SGOV",
    "SPYI",
    "QQQI",
    "QQQ",
)
_QPK_REVISION = "730ad9f3983bd90cd75adecb67fcf483ffb96736"
_UES_REVISION = "15df2a42df5d230cfb03a7cb655fd4b226956681"
_PROFILE = "soxl_soxx_trend_income"
_DOMAIN = "us_equity"
_MIN_INDICATOR_SESSIONS = 420
_ORDERED_VARIANTS = ("explicit_qqq_fallback", "cash_origin")
_FROZEN_AVAILABILITY_CONTRACT = {
    "schema_version": "soxl_asset_availability.v1",
    "universe": list(SOXL_PROMOTION_ASSETS),
    "always_eligible": ["SOXL", "SOXX", "SCHD", "DGRO", "QQQ"],
    "first_eligible_session": FIRST_ELIGIBLE_SESSION,
    "ordered_variants": list(_ORDERED_VARIANTS),
    "primary_variant": "explicit_qqq_fallback",
    "transition_rule": "qqq_to_qqqi_close_t_open_t_plus_1",
    "unavailable_target_policy": "cash_without_renormalization",
    "price_identity_policy": "actual_symbol_only_no_proxy_backfill_forward_fill_substitution",
    "initial_state": "100_percent_cash",
}
_FROZEN_FOLDS = (
    ("2018-08-03", "2020-04-03", "2020-05-05", "2020-10-30"),
    ("2020-12-01", "2022-08-02", "2022-08-31", "2023-03-02"),
    ("2023-03-31", "2024-11-29", "2024-12-31", "2025-07-03"),
)
_FROZEN_LOCKED_OOS = ("2025-08-04", "2026-08-04")
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


class SoxlPromotionContractError(ValueError):
    """Fail-closed error for invalid inputs, authority, state, or evidence."""


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_json(value: Any) -> str:
    return _sha256_bytes(canonical_json_bytes(value))


def _strict_json(path: Path) -> dict[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise SoxlPromotionContractError("invalid JSON")
            result[key] = value
        return result

    def reject_constant(_: str) -> None:
        raise SoxlPromotionContractError("invalid JSON")

    try:
        payload = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
        )
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise SoxlPromotionContractError("invalid JSON input") from exc
    if not isinstance(payload, dict):
        raise SoxlPromotionContractError("JSON input must be an object")
    return payload


def _resolve_runner_revision() -> str:
    try:
        distribution = importlib.metadata.distribution("us-equity-snapshot-pipelines")
        direct_url_text = distribution.read_text("direct_url.json")
        direct_url = json.loads(direct_url_text) if direct_url_text else {}
        commit_id = direct_url.get("vcs_info", {}).get("commit_id")
        if isinstance(commit_id, str):
            return _git_revision(commit_id, "installed runner revision")
    except (importlib.metadata.PackageNotFoundError, TypeError, ValueError, json.JSONDecodeError):
        pass
    repository = Path(__file__).resolve().parents[3]
    try:
        status = subprocess.run(
            ["git", "-C", str(repository), "status", "--porcelain", "--untracked-files=normal"],
            check=True,
            capture_output=True,
            text=True,
        )
        if status.stdout:
            raise SoxlPromotionContractError("runner implementation checkout is not immutable")
        completed = subprocess.run(
            ["git", "-C", str(repository), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise SoxlPromotionContractError("runner implementation identity is unavailable") from exc
    return _git_revision(completed.stdout.strip(), "runner implementation revision")


def _exact_keys(value: object, expected: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise SoxlPromotionContractError(f"invalid {label} fields")
    return dict(value)


def _finite(value: object, *, positive: bool = False, nonnegative: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SoxlPromotionContractError("non-finite numeric input")
    number = float(value)
    if not math.isfinite(number) or (positive and number <= 0.0) or (nonnegative and number < 0.0):
        raise SoxlPromotionContractError("non-finite numeric input")
    return number


def _git_revision(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 40
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise SoxlPromotionContractError(f"invalid {label}")
    return value


def _sha256_digest(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise SoxlPromotionContractError(f"invalid {label}")
    return value


def _calendar_date(value: object, label: str) -> date:
    if not isinstance(value, str):
        raise SoxlPromotionContractError(f"invalid {label}")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise SoxlPromotionContractError(f"invalid {label}") from exc


def _eligible_assets_on(session_date: date) -> tuple[str, ...]:
    return tuple(
        symbol
        for symbol in SOXL_PROMOTION_ASSETS
        if symbol not in FIRST_ELIGIBLE_SESSION
        or session_date >= date.fromisoformat(FIRST_ELIGIBLE_SESSION[symbol])
    )


@dataclass
class _Lot:
    quantity: float
    entry_price: float
    stop_price: float


@dataclass
class _PortfolioState:
    cash: float
    quantities: dict[str, float]
    lots: dict[str, list[_Lot]]
    pending_target: dict[str, float] | None = None
    stopped_today: set[str] = field(default_factory=set)
    normalized: bool = False
    account_parked: bool = False
    strategy_parked: bool = False
    stop_count: int = 0
    high_water_equity: float = 0.0
    last_equity: float = 0.0
    turnover: float = 0.0
    costs_paid: float = 0.0
    trade_count: int = 0
    assessment_count: int = 0
    strategy_state: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class WindowEvidence:
    result: BacktestResult
    recovery_sessions: int | None
    recovery_censored: bool
    benchmark_recovery_sessions: int | None
    benchmark_recovery_censored: bool
    benchmark_total_return: float
    upside_capture: float
    upside_participation: float
    turnover: float
    trade_count: int
    profit_factor: float
    var_95: float
    cvar_95: float
    information_ratio: float
    information_coefficient: float
    costs_paid: float
    assessment_count: int
    state_digest_sha256: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "result": self.result.to_dict(),
            "recovery_sessions": self.recovery_sessions,
            "recovery_censored": self.recovery_censored,
            "benchmark_recovery_sessions": self.benchmark_recovery_sessions,
            "benchmark_recovery_censored": self.benchmark_recovery_censored,
            "benchmark_total_return": self.benchmark_total_return,
            "upside_capture": self.upside_capture,
            "upside_participation": self.upside_participation,
            "turnover": self.turnover,
            "trade_count": self.trade_count,
            "profit_factor": self.profit_factor,
            "var_95": self.var_95,
            "cvar_95": self.cvar_95,
            "information_ratio": self.information_ratio,
            "information_coefficient": self.information_coefficient,
            "costs_paid": self.costs_paid,
            "assessment_count": self.assessment_count,
            "state_digest_sha256": self.state_digest_sha256,
        }


class SoxlPromotionRunner:
    """A real, offline runner with explicit immutable-input and risk contracts."""

    runner_kind = "real"

    def __init__(
        self,
        input_payload: Mapping[str, Any],
        config_payload: Mapping[str, Any],
        *,
        variant_id: str,
        assessment_clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.variant_id = variant_id
        self._assessment_clock = assessment_clock or (lambda: datetime.now(timezone.utc))
        self.implementation_revision = _resolve_runner_revision()
        self.input_payload = copy.deepcopy(dict(input_payload))
        self.config = copy.deepcopy(dict(config_payload))
        self._validate_input()
        self._validate_config()
        self._date_to_index = {
            _calendar_date(session["date"], "session date"): index
            for index, session in enumerate(self.sessions)
        }
        self.folds, self.locked_oos_start, self.locked_oos_end = self._validate_windows()
        self._window_evidence: dict[tuple[date, date, float], WindowEvidence] = {}

    def _validate_market_regime(
        self,
        value: object,
        *,
        session_date: date,
        session_index: int,
        source_contract_sha256: str | None,
    ) -> str:
        regime = _exact_keys(
            value,
            {
                "schema_version",
                "profile",
                "candidate_id",
                "as_of",
                "market_regime_control_enabled",
                "component_signals",
                "execution_controls",
                "pit_provenance",
            },
            "market regime",
        )
        if (
            regime["schema_version"] != MARKET_REGIME_SCHEMA
            or regime["profile"] != "market_regime_control"
            or regime["candidate_id"] != CANDIDATE_ID
            or _calendar_date(regime["as_of"], "market regime as_of") != session_date
            or regime["market_regime_control_enabled"] is not False
        ):
            raise SoxlPromotionContractError("point-in-time market regime mismatch")
        unavailable = {
            component: {"enabled": False, "available": False}
            for component in UNAVAILABLE_COMPONENTS
        }
        if regime["component_signals"] != unavailable:
            raise SoxlPromotionContractError("unavailable component identity mismatch")
        execution_controls = _exact_keys(
            regime["execution_controls"],
            {
                "broker_order_allowed",
                "live_allocation_mutation_allowed",
                "repository_broker_write_allowed",
                "repository_allocation_mutation_allowed",
                "position_control_allowed",
                "consumption_evidence_status",
            },
            "market regime execution controls",
        )
        if execution_controls != {
            "broker_order_allowed": False,
            "live_allocation_mutation_allowed": False,
            "repository_broker_write_allowed": False,
            "repository_allocation_mutation_allowed": False,
            "position_control_allowed": False,
            "consumption_evidence_status": "static_research_only",
        }:
            raise SoxlPromotionContractError("market regime position control is forbidden")
        provenance = _exact_keys(
            regime["pit_provenance"],
            {
                "source_contract_sha256",
                "candidate_contract_sha256",
                "producer_receipt_sha256",
                "prefix_input_manifest_sha256",
                "logical_input_ids",
                "evidence_class",
                "real_producer",
                "prefix_session_count",
                "prefix_end",
                "future_sessions_exposed",
                "raw_series_persisted",
            },
            "market regime provenance",
        )
        current_source_sha256 = _sha256_digest(
            provenance["source_contract_sha256"], "source contract digest"
        )
        if source_contract_sha256 is not None and current_source_sha256 != source_contract_sha256:
            raise SoxlPromotionContractError("source contract identity mismatch")
        evidence_class = provenance["evidence_class"]
        if (
            provenance["candidate_contract_sha256"] != CORE_ONLY_CONFIG_SHA256
            or provenance["logical_input_ids"] != list(SOXL_PROMOTION_ASSETS)
            or evidence_class not in {"synthetic_fixture", "provider_observed"}
            or provenance["real_producer"] is not (evidence_class == "provider_observed")
            or provenance["prefix_session_count"] != session_index + 1
            or provenance["prefix_end"] != session_date.isoformat()
            or provenance["future_sessions_exposed"] is not False
            or provenance["raw_series_persisted"] is not False
        ):
            raise SoxlPromotionContractError("market regime provenance identity mismatch")
        _sha256_digest(provenance["candidate_contract_sha256"], "candidate contract digest")
        _sha256_digest(provenance["producer_receipt_sha256"], "producer receipt digest")
        _sha256_digest(
            provenance["prefix_input_manifest_sha256"], "prefix input manifest digest"
        )
        return current_source_sha256

    def _validate_input(self) -> None:
        payload = _exact_keys(
            self.input_payload,
            {"schema_version", "input_manifest", "sessions"},
            "input package",
        )
        if payload["schema_version"] != INPUT_CONTRACT_ID:
            raise SoxlPromotionContractError("invalid input schema")
        try:
            self.input_manifest = validate_research_input_manifest(payload["input_manifest"])
        except ValueError as exc:
            raise SoxlPromotionContractError("invalid input manifest") from exc
        if (
            self.input_manifest["domain"] != _DOMAIN
            or self.input_manifest["profile"] != _PROFILE
            or self.input_manifest["calendar"]["calendar_id"] != "XNYS"
            or self.input_manifest["calendar"]["timezone"] != "America/New_York"
            or self.input_manifest["adjustment"]["policy"] != "total_return_adjusted"
            or self.input_manifest["research_input_contract_id"] != INPUT_CONTRACT_ID
            or self.input_manifest["artifact_type"] != "immutable_adjusted_ohlcv_core_only"
        ):
            raise SoxlPromotionContractError("input manifest is not production-parity")
        source_ids = [source["source_id"] for source in self.input_manifest["sources"]]
        if source_ids != sorted(SOXL_PROMOTION_ASSETS):
            raise SoxlPromotionContractError("exact 9 input sources are required")
        raw_sessions = payload["sessions"]
        if not isinstance(raw_sessions, list) or len(raw_sessions) != 2_010:
            raise SoxlPromotionContractError("exact frozen input sessions are required")
        self.sessions = copy.deepcopy(raw_sessions)
        previous_date: date | None = None
        source_contract_sha256: str | None = None
        for session_index, raw_session in enumerate(self.sessions):
            session = _exact_keys(
                raw_session,
                {"date", "bars", "eligible_assets", "market_regime"},
                "session",
            )
            session_date = _calendar_date(session["date"], "session date")
            if previous_date is not None and session_date <= previous_date:
                raise SoxlPromotionContractError("sessions must be strictly ordered")
            previous_date = session_date
            eligible_assets = _eligible_assets_on(session_date)
            if session["eligible_assets"] != list(eligible_assets):
                raise SoxlPromotionContractError("session eligibility calendar mismatch")
            bars = session["bars"]
            if not isinstance(bars, Mapping) or set(bars) != set(eligible_assets):
                raise SoxlPromotionContractError("exact eligible bar set is required")
            for symbol in eligible_assets:
                bar = _exact_keys(bars[symbol], {"open", "high", "low", "close", "volume"}, "bar")
                open_price = _finite(bar["open"], positive=True)
                high = _finite(bar["high"], positive=True)
                low = _finite(bar["low"], positive=True)
                close = _finite(bar["close"], positive=True)
                _finite(bar["volume"], nonnegative=True)
                if low > min(open_price, close) or high < max(open_price, close) or high < low:
                    raise SoxlPromotionContractError("invalid OHLC relationship")
            source_contract_sha256 = self._validate_market_regime(
                session["market_regime"],
                session_date=session_date,
                session_index=session_index,
                source_contract_sha256=source_contract_sha256,
            )
        if (
            self.sessions[0]["date"] != "2018-08-03"
            or self.sessions[-1]["date"] != "2026-08-04"
        ):
            raise SoxlPromotionContractError("exact frozen input range is required")
        sessions_by_date = {session["date"]: session for session in self.sessions}
        if any(
            start not in sessions_by_date
            or symbol not in sessions_by_date[start]["bars"]
            for symbol, start in FIRST_ELIGIBLE_SESSION.items()
        ):
            raise SoxlPromotionContractError("first eligible actual session is missing")
        sessions_bytes = canonical_json_bytes(self.sessions)
        members = self.input_manifest["members"]
        if len(members) != 1 or members[0]["path"] != "sessions.json":
            raise SoxlPromotionContractError("input manifest must bind sessions.json")
        if (
            members[0]["sha256"] != _sha256_bytes(sessions_bytes)
            or members[0]["size_bytes"] != len(sessions_bytes)
        ):
            raise SoxlPromotionContractError("sessions.json digest mismatch")
        self.input_manifest_sha256 = research_input_manifest_sha256(self.input_manifest)
        if source_contract_sha256 is None:
            raise SoxlPromotionContractError("source contract identity is required")
        self.source_contract_sha256 = source_contract_sha256

    def _validate_config(self) -> None:
        expected = {
            "schema_version",
            "candidate_id",
            "input_contract_id",
            "source_contract_schema",
            "source_contract_sha256",
            "candidate_contract_sha256",
            "market_regime_control_enabled",
            "benchmark_symbol",
            "substitution_policy",
            "position_control_allowed",
            "strategy_profile",
            "domain",
            "account_mode",
            "strategy_revision",
            "runner_revision",
            "qpk_revision",
            "frozen_strategy_config",
            "availability_contract",
            "ordered_variants",
            "candidate_identity",
            "mandate_provenance",
            "initial_equity",
            "initial_weights",
            "stop_loss_distance",
            "purge_sessions",
            "embargo_sessions",
            "folds",
            "locked_oos",
            "risk_standard_id",
            "risk_standard_sha256",
            "input_license",
            "input_usage_scope",
            "learning_only",
            "promotion_eligible",
            "live_ready",
            "size_zero_required",
            "no_order",
        }
        config = _exact_keys(self.config, expected, "promotion config")
        if (
            config["schema_version"] != "soxl_p3_core_only_9_input_config.v1"
            or config["strategy_profile"] != _PROFILE
            or config["domain"] != _DOMAIN
            or config["account_mode"] != "single_strategy"
            or config["qpk_revision"] != _QPK_REVISION
            or config["strategy_revision"] != _UES_REVISION
            or config["runner_revision"] != self.implementation_revision
        ):
            raise SoxlPromotionContractError("candidate revision or profile mismatch")
        if (
            config["candidate_id"] != CANDIDATE_ID
            or config["input_contract_id"] != INPUT_CONTRACT_ID
            or config["source_contract_schema"] != SOURCE_CONTRACT_SCHEMA
            or config["candidate_contract_sha256"] != CORE_ONLY_CONFIG_SHA256
            or config["market_regime_control_enabled"] is not False
            or config["substitution_policy"] != "none_no_proxy_no_alias"
        ):
            raise SoxlPromotionContractError("core-only candidate contract mismatch")
        if config["benchmark_symbol"] != "SOXX":
            raise SoxlPromotionContractError("SOXX benchmark is required")
        if config["source_contract_sha256"] != self.source_contract_sha256:
            raise SoxlPromotionContractError("source contract identity mismatch")
        _sha256_digest(config["source_contract_sha256"], "source contract digest")
        _sha256_digest(config["candidate_contract_sha256"], "candidate contract digest")
        _git_revision(config["runner_revision"], "runner revision")
        current_config = json.loads(canonical_json_bytes(soxl_soxx_trend_income_manifest.default_config))
        if config["frozen_strategy_config"] != current_config:
            raise SoxlPromotionContractError("frozen strategy config mismatch")
        if config["availability_contract"] != _FROZEN_AVAILABILITY_CONTRACT:
            raise SoxlPromotionContractError("frozen availability contract mismatch")
        if config["ordered_variants"] != list(_ORDERED_VARIANTS):
            raise SoxlPromotionContractError("ordered variant contract mismatch")
        if self.variant_id not in _ORDERED_VARIANTS:
            raise SoxlPromotionContractError("invalid promotion variant")
        if config["initial_weights"] != {}:
            raise SoxlPromotionContractError("initial state must be 100% cash")
        self.initial_equity = _finite(config["initial_equity"], positive=True)
        self.stop_loss_distance = _finite(config["stop_loss_distance"], positive=True)
        if not math.isclose(self.stop_loss_distance, 0.05, abs_tol=1e-12):
            raise SoxlPromotionContractError("5% executable stop is required")
        if config["purge_sessions"] != 20:
            raise SoxlPromotionContractError("20-session purge is required")
        if config["embargo_sessions"] != 20:
            raise SoxlPromotionContractError("20-session embargo is required")
        if not isinstance(config["input_license"], str) or not config["input_license"].strip():
            raise SoxlPromotionContractError("input license is required")
        if not isinstance(config["input_usage_scope"], str) or not config["input_usage_scope"].strip():
            raise SoxlPromotionContractError("input usage scope is required")
        if (
            config["learning_only"] is not False
            or config["promotion_eligible"] is not False
            or config["live_ready"] is not False
            or config["position_control_allowed"] is not False
            or config["size_zero_required"] is not True
            or config["no_order"] is not True
        ):
            raise SoxlPromotionContractError("promotion-research lifecycle claims are invalid")
        config_sha256 = _sha256_json(
            {
                key: value
                for key, value in config.items()
                if key not in {"candidate_identity", "mandate_provenance"}
            }
        )
        try:
            self.candidate_identity = CandidateRiskIdentity(**config["candidate_identity"])
        except (TypeError, ValueError) as exc:
            raise SoxlPromotionContractError("invalid candidate identity") from exc
        if (
            self.candidate_identity.strategy_profile != _PROFILE
            or self.candidate_identity.account_mode != "single_strategy"
            or self.candidate_identity.strategy_revision != _UES_REVISION
            or self.candidate_identity.runner_revision != config["runner_revision"]
            or self.candidate_identity.config_sha256 != config_sha256
            or self.candidate_identity.input_manifest_sha256 != self.input_manifest_sha256
        ):
            raise SoxlPromotionContractError("candidate identity mismatch")
        if not isinstance(config["mandate_provenance"], Mapping):
            raise SoxlPromotionContractError("candidate-bound mandate is required")
        self.mandate = copy.deepcopy(dict(config["mandate_provenance"]))
        expected_candidate_fields = {
            "strategy_profile": self.candidate_identity.strategy_profile,
            "account_mode": self.candidate_identity.account_mode,
            "strategy_revision": self.candidate_identity.strategy_revision,
            "runner_revision": self.candidate_identity.runner_revision,
            "config_sha256": self.candidate_identity.config_sha256,
            "input_manifest_sha256": self.candidate_identity.input_manifest_sha256,
            "authority_receipt_sha256": self.candidate_identity.authority_receipt_sha256,
            "candidate_identity_sha256": self.candidate_identity.candidate_sha256,
        }
        if any(self.mandate.get(key) != value for key, value in expected_candidate_fields.items()):
            raise SoxlPromotionContractError("candidate-bound mandate mismatch")
        if (
            self.mandate.get("mandate_id") != MANDATE_ID
            or self.mandate.get("authority_scope") != "RESEARCH_ONLY"
        ):
            raise SoxlPromotionContractError("core-only mandate mismatch")
        factors = self.mandate.get("product_leverage_factors")
        if (
            not isinstance(factors, Mapping)
            or dict(factors) != {symbol: 3 if symbol == "SOXL" else 1 for symbol in SOXL_PROMOTION_ASSETS}
            or self.mandate.get("allowed_nonzero_assets") != list(SOXL_PROMOTION_ASSETS)
        ):
            raise SoxlPromotionContractError("candidate mandate asset vector mismatch")

    def _validate_windows(self) -> tuple[tuple[PurgedWalkForwardFold, ...], date, date]:
        raw_folds = self.config["folds"]
        if not isinstance(raw_folds, list) or len(raw_folds) != 3:
            raise SoxlPromotionContractError("exactly three ordered folds are required")
        dates = [_calendar_date(session["date"], "session date") for session in self.sessions]
        index = {value: offset for offset, value in enumerate(dates)}
        folds: list[PurgedWalkForwardFold] = []
        previous_test_end_index: int | None = None
        for fold_index, raw_fold in enumerate(raw_folds):
            fold = _exact_keys(raw_fold, {"train_start", "train_end", "test_start", "test_end"}, "fold")
            boundaries = tuple(
                _calendar_date(fold[name], f"fold {name}")
                for name in ("train_start", "train_end", "test_start", "test_end")
            )
            if any(boundary not in index for boundary in boundaries):
                raise SoxlPromotionContractError("fold boundary is not an input session")
            train_start, train_end, test_start, test_end = boundaries
            if tuple(value.isoformat() for value in boundaries) != _FROZEN_FOLDS[fold_index]:
                raise SoxlPromotionContractError("fold boundary is not frozen")
            train_start_i, train_end_i, test_start_i, test_end_i = (index[value] for value in boundaries)
            if (
                train_end_i - train_start_i + 1 != _MIN_INDICATOR_SESSIONS
                or test_end_i - test_start_i + 1 != 126
                or test_start_i - train_end_i - 1 != 20
            ):
                raise SoxlPromotionContractError("fold violates 20-session purge or minimum window")
            if previous_test_end_index is not None and train_start_i - previous_test_end_index - 1 != 20:
                raise SoxlPromotionContractError("fold violates 20-session embargo")
            folds.append(PurgedWalkForwardFold(train_start, train_end, test_start, test_end))
            previous_test_end_index = test_end_i
        locked = _exact_keys(self.config["locked_oos"], {"start", "end"}, "locked OOS")
        locked_start = _calendar_date(locked["start"], "locked OOS start")
        locked_end = _calendar_date(locked["end"], "locked OOS end")
        if (locked_start.isoformat(), locked_end.isoformat()) != _FROZEN_LOCKED_OOS:
            raise SoxlPromotionContractError("locked OOS boundary is not frozen")
        if locked_start not in index or locked_end not in index:
            raise SoxlPromotionContractError("locked OOS boundary is not an input session")
        if (
            previous_test_end_index is None
            or index[locked_start] - previous_test_end_index - 1 != 20
            or index[locked_end] - index[locked_start] + 1 != 252
            or locked_end < _add_calendar_months(locked_start, 12)
        ):
            raise SoxlPromotionContractError("locked OOS must be untouched, >=252 sessions, and >=12 months")
        if any(
            tuple(self.sessions[offset]["eligible_assets"]) != SOXL_PROMOTION_ASSETS
            for offset in range(index[locked_start], index[locked_end] + 1)
        ):
            raise SoxlPromotionContractError("locked OOS must contain actual-only nine-asset sessions")
        return tuple(folds), locked_start, locked_end

    def _lot(self, quantity: float, entry_price: float) -> _Lot:
        return _Lot(quantity, entry_price, entry_price * (1.0 - self.stop_loss_distance))

    def _initial_state(self) -> _PortfolioState:
        quantities = {symbol: 0.0 for symbol in SOXL_PROMOTION_ASSETS}
        lots = {symbol: [] for symbol in SOXL_PROMOTION_ASSETS}
        return _PortfolioState(
            cash=self.initial_equity,
            quantities=quantities,
            lots=lots,
            normalized=True,
            high_water_equity=self.initial_equity,
            last_equity=self.initial_equity,
        )

    def _prices(self, index: int, field_name: str) -> dict[str, float]:
        return {
            symbol: float(bar[field_name])
            for symbol, bar in self.sessions[index]["bars"].items()
        }

    @staticmethod
    def _equity(state: _PortfolioState, prices: Mapping[str, float]) -> float:
        equity = state.cash
        for symbol in SOXL_PROMOTION_ASSETS:
            quantity = state.quantities[symbol]
            if quantity <= 1e-12:
                continue
            if symbol not in prices:
                raise SoxlPromotionContractError("held asset is not point-in-time eligible")
            equity += quantity * prices[symbol]
        return equity

    def _weights(self, state: _PortfolioState, prices: Mapping[str, float]) -> dict[str, float]:
        equity = self._equity(state, prices)
        if equity <= 0.0 or not math.isfinite(equity):
            raise SoxlPromotionContractError("portfolio equity is invalid")
        return {
            symbol: state.quantities[symbol] * prices[symbol] / equity
            for symbol in SOXL_PROMOTION_ASSETS
            if state.quantities[symbol] > 1e-12 and symbol in prices
        }

    def _portfolio_snapshot(
        self,
        index: int,
        state: _PortfolioState,
        *,
        market_regime: Mapping[str, Any],
    ) -> PortfolioSnapshot:
        close_prices = self._prices(index, "close")
        equity = self._equity(state, close_prices)
        weights = self._weights(state, close_prices)
        factors = self.mandate["product_leverage_factors"]
        positions = tuple(
            Position(
                symbol=symbol,
                quantity=state.quantities[symbol],
                market_value=state.quantities[symbol] * close_prices[symbol],
                average_cost=(
                    sum(lot.quantity * lot.entry_price for lot in state.lots[symbol])
                    / sum(lot.quantity for lot in state.lots[symbol])
                    if state.lots[symbol]
                    else None
                ),
            )
            for symbol in SOXL_PROMOTION_ASSETS
            if state.quantities[symbol] > 1e-12 and symbol in close_prices
        )
        return PortfolioSnapshot(
            as_of=self._assessment_clock(),
            total_equity=equity,
            buying_power=state.cash,
            cash_balance=state.cash,
            positions=positions,
            metadata={
                "raw_cash": state.cash,
                "sellable_quantities": dict(state.quantities),
                "observed_effective_exposure": sum(weights[symbol] * factors[symbol] for symbol in weights),
                "market_regime": dict(market_regime),
                "simulated_session": self.sessions[index]["date"],
                "high_water_equity": state.high_water_equity,
                "drawdown": 1.0 - state.last_equity / state.high_water_equity,
                "strategy_state": copy.deepcopy(state.strategy_state),
            },
        )

    def _assess_control(
        self,
        index: int,
        state: _PortfolioState,
        target_weights: Mapping[str, float],
        *,
        reason: str,
        normalization_origin_weights: Mapping[str, float] | None = None,
    ) -> dict[str, float]:
        decision = StrategyDecision(
            positions=tuple(
                PositionTarget(symbol=symbol, target_weight=float(weight), role="risk_control")
                for symbol, weight in sorted(target_weights.items())
                if weight > 0.0
            ),
            diagnostics={"promotion_research_control": reason},
        )
        market_regime = self.sessions[index]["market_regime"]
        snapshot = self._portfolio_snapshot(index, state, market_regime=market_regime)
        try:
            result = assess_with_evidence(
                decision,
                snapshot,
                scope="ACCOUNT",
                mandate_provenance=self.mandate,
                market_data={"market_regime": market_regime},
                candidate_identity=self.candidate_identity,
                normalization_origin_weights=normalization_origin_weights,
            )
        except Exception as exc:
            raise SoxlPromotionContractError("risk control assessment failed") from exc
        state.assessment_count += 1
        if result.assessment.outcome != "APPROVE" or result.decision.budgets:
            raise SoxlPromotionContractError("risk control assessment rejected")
        return {
            position.symbol: float(position.target_weight)
            for position in result.decision.positions
            if position.target_weight is not None
        }

    def _evaluate_close(self, index: int, state: _PortfolioState) -> dict[str, float]:
        drawdown = max(0.0, 1.0 - state.last_equity / state.high_water_equity)
        if drawdown > 0.10:
            state.account_parked = True
        if state.stop_count >= 3:
            state.strategy_parked = True
        if state.account_parked or state.strategy_parked:
            return self._assess_control(index, state, {}, reason="breaker_flatten")
        if index + 1 < _MIN_INDICATOR_SESSIONS:
            raise SoxlPromotionContractError("insufficient point-in-time indicator history")
        try:
            indicators = build_semiconductor_rotation_indicators_from_history(
                soxl_history=[
                    float(self.sessions[offset]["bars"]["SOXL"]["close"])
                    for offset in range(index + 1)
                ],
                soxx_history=[
                    float(self.sessions[offset]["bars"]["SOXX"]["close"])
                    for offset in range(index + 1)
                ],
            )
        except Exception as exc:
            raise SoxlPromotionContractError("candidate indicator evaluation failed") from exc
        regime = self.sessions[index]["market_regime"]
        snapshot = self._portfolio_snapshot(index, state, market_regime=regime)
        ctx = StrategyContext(
            as_of=self.sessions[index]["date"],
            market_data={"derived_indicators": indicators},
            portfolio=snapshot,
            state=copy.deepcopy(state.strategy_state),
            runtime_config=copy.deepcopy(self.config["frozen_strategy_config"]),
        )
        eligible_assets = frozenset(self.sessions[index]["eligible_assets"])
        fallback_symbol = (
            "QQQ"
            if self.variant_id == "explicit_qqq_fallback" and "QQQI" not in eligible_assets
            else None
        )
        try:
            result = evaluate_soxl_soxx_trend_income_promotion_research(
                ctx,
                candidate_identity=self.candidate_identity,
                mandate_provenance=self.mandate,
                stop_loss_distances={symbol: self.stop_loss_distance for symbol in eligible_assets},
                drawdown_scalar=0.5 if drawdown > 0.05 else 1.0,
                inputs_fresh=True,
                point_in_time_eligible_assets=eligible_assets,
                qqqi_preinception_fallback_symbol=fallback_symbol,
            )
        except Exception as exc:
            raise SoxlPromotionContractError("candidate decision evaluation failed") from exc
        state.assessment_count += 1
        if result.assessment.outcome != "APPROVE" or result.decision.budgets:
            raise SoxlPromotionContractError("candidate decision assessment rejected")
        targets: dict[str, float] = {}
        for position in result.decision.positions:
            if position.symbol not in eligible_assets or position.target_weight is None:
                raise SoxlPromotionContractError("candidate returned an invalid asset vector")
            weight = _finite(position.target_weight, nonnegative=True)
            if weight > 0.0:
                targets[position.symbol] = weight
        if "QQQI" in eligible_assets and targets.get("QQQ", 0.0) > 0.0:
            raise SoxlPromotionContractError("QQQ is forbidden after the QQQI transition")
        factors = self.mandate["product_leverage_factors"]
        if sum(targets[symbol] * factors[symbol] for symbol in targets) > 0.50 + 1e-9:
            raise SoxlPromotionContractError("candidate exceeded effective exposure")
        state.strategy_state = {
            "last_signal_session": self.sessions[index]["date"],
            "last_assessment_sha256": result.assessment.assessment_sha256,
            "last_active_risk_asset": result.decision.diagnostics.get("active_risk_asset"),
            "last_blend_tier": result.decision.diagnostics.get("blend_tier"),
            "last_income_ratio": result.decision.diagnostics.get("income_ratio_text"),
        }
        return targets

    def _sell_lots(self, state: _PortfolioState, symbol: str, quantity: float) -> None:
        remaining = quantity
        retained: list[_Lot] = []
        for lot in state.lots[symbol]:
            sold = min(lot.quantity, remaining)
            remaining -= sold
            if lot.quantity - sold > 1e-12:
                retained.append(_Lot(lot.quantity - sold, lot.entry_price, lot.stop_price))
        if remaining > 1e-8:
            raise SoxlPromotionContractError("lot state is inconsistent")
        state.lots[symbol] = retained

    def _rebalance(
        self,
        state: _PortfolioState,
        prices: Mapping[str, float],
        target_weights: Mapping[str, float],
        total_cost_bps: float,
    ) -> None:
        if not set(target_weights).issubset(prices):
            raise SoxlPromotionContractError("target asset is not point-in-time eligible")
        equity = self._equity(state, prices)
        current = self._weights(state, prices)
        current_cash_weight = state.cash / equity
        target_cash_weight = 1.0 - sum(target_weights.values())
        if target_cash_weight < -1e-9:
            raise SoxlPromotionContractError("target weights exceed equity")
        half_l1 = 0.5 * (
            sum(
                abs(target_weights.get(symbol, 0.0) - current.get(symbol, 0.0))
                for symbol in SOXL_PROMOTION_ASSETS
            )
            + abs(target_cash_weight - current_cash_weight)
        )
        cost = equity * half_l1 * total_cost_bps / 10_000.0
        equity_after_cost = equity - cost
        if equity_after_cost <= 0.0:
            raise SoxlPromotionContractError("transaction costs exhausted equity")
        desired_quantities = {}
        for symbol in SOXL_PROMOTION_ASSETS:
            target_weight = target_weights.get(symbol, 0.0)
            if target_weight > 0.0:
                desired_quantities[symbol] = equity_after_cost * target_weight / prices[symbol]
            else:
                desired_quantities[symbol] = 0.0
        for symbol in SOXL_PROMOTION_ASSETS:
            delta = desired_quantities[symbol] - state.quantities[symbol]
            if delta < -1e-12:
                self._sell_lots(state, symbol, -delta)
            elif delta > 1e-12:
                state.lots[symbol].append(self._lot(delta, prices[symbol]))
            state.quantities[symbol] = desired_quantities[symbol]
        state.cash = equity_after_cost * max(0.0, target_cash_weight)
        state.turnover += half_l1
        state.costs_paid += cost
        if half_l1 > 1e-12:
            state.trade_count += 1

    def _execute_open(self, index: int, state: _PortfolioState, *, total_cost_bps: float) -> None:
        state.stopped_today.clear()
        opens = self._prices(index, "open")
        lows = self._prices(index, "low")
        triggered: dict[str, tuple[float, float]] = {}
        for symbol in SOXL_PROMOTION_ASSETS:
            if not state.lots[symbol]:
                continue
            if symbol not in opens or symbol not in lows:
                raise SoxlPromotionContractError("held asset is not point-in-time eligible")
            executable_stop = max(lot.stop_price for lot in state.lots[symbol])
            execution_price = (
                opens[symbol]
                if opens[symbol] <= executable_stop
                else executable_stop
                if lows[symbol] <= executable_stop
                else None
            )
            if execution_price is not None:
                quantity = state.quantities[symbol]
                triggered[symbol] = (quantity, quantity * execution_price)
        if triggered:
            current_weights = self._weights(state, opens)
            target_weights = dict(current_weights)
            for symbol in triggered:
                target_weights.pop(symbol, None)
            self._assess_control(index, state, target_weights, reason="executable_5pct_stop")
            for symbol, (quantity, notional) in triggered.items():
                self._sell_lots(state, symbol, quantity)
                state.quantities[symbol] -= quantity
                stop_cost = notional * total_cost_bps / 10_000.0
                state.cash += notional - stop_cost
                state.costs_paid += stop_cost
                state.trade_count += 1
                state.stop_count += 1
                state.stopped_today.add(symbol)
        if state.pending_target is not None:
            executable = {
                symbol: weight
                for symbol, weight in state.pending_target.items()
                if symbol not in state.stopped_today
            }
            self._rebalance(state, opens, executable, total_cost_bps)
            state.pending_target = None

    def _state_digest(self, state: _PortfolioState) -> str:
        return _sha256_json(
            {
                "variant_id": self.variant_id,
                "cash": state.cash,
                "quantities": state.quantities,
                "lots": {
                    symbol: [lot.__dict__ for lot in state.lots[symbol]]
                    for symbol in SOXL_PROMOTION_ASSETS
                },
                "pending_target": state.pending_target,
                "normalized": state.normalized,
                "account_parked": state.account_parked,
                "strategy_parked": state.strategy_parked,
                "stop_count": state.stop_count,
                "high_water_equity": state.high_water_equity,
                "strategy_state": state.strategy_state,
            }
        )

    def _replay_window(self, start: date, end: date, total_cost_bps: float) -> WindowEvidence:
        if start not in self._date_to_index or end not in self._date_to_index:
            raise SoxlPromotionContractError("requested window is not covered")
        start_index = self._date_to_index[start]
        end_index = self._date_to_index[end]
        state = self._initial_state()
        equities: list[float] = []
        benchmark_equities: list[float] = []
        turnovers: list[float] = []
        costs: list[float] = []
        trades: list[int] = []
        assessments: list[int] = []
        benchmark_quantity = self.initial_equity / float(self.sessions[0]["bars"]["SOXX"]["close"])
        for index in range(end_index + 1):
            before_turnover = state.turnover
            before_cost = state.costs_paid
            before_trades = state.trade_count
            before_assessments = state.assessment_count
            self._execute_open(index, state, total_cost_bps=total_cost_bps)
            closes = self._prices(index, "close")
            equity = self._equity(state, closes)
            if not math.isfinite(equity) or equity <= 0.0:
                raise SoxlPromotionContractError("non-finite backtest state")
            state.last_equity = equity
            state.high_water_equity = max(state.high_water_equity, equity)
            equities.append(equity)
            benchmark_equities.append(benchmark_quantity * closes["SOXX"])
            if index >= _MIN_INDICATOR_SESSIONS - 1 and index < end_index:
                state.pending_target = self._evaluate_close(index, state)
            turnovers.append(state.turnover - before_turnover)
            costs.append(state.costs_paid - before_cost)
            trades.append(state.trade_count - before_trades)
            assessments.append(state.assessment_count - before_assessments)
        return self._window_metrics(
            start,
            end,
            equities[start_index : end_index + 1],
            benchmark_equities[start_index : end_index + 1],
            turnovers[start_index : end_index + 1],
            costs[start_index : end_index + 1],
            trades[start_index : end_index + 1],
            assessments[start_index : end_index + 1],
            state,
        )

    def _window_metrics(
        self,
        start: date,
        end: date,
        equities: Sequence[float],
        benchmark_equities: Sequence[float],
        turnovers: Sequence[float],
        costs: Sequence[float],
        trades: Sequence[int],
        assessments: Sequence[int],
        state: _PortfolioState,
    ) -> WindowEvidence:
        returns = _returns(equities)
        benchmark_returns = _returns(benchmark_equities)
        total_return = equities[-1] / equities[0] - 1.0
        benchmark_total_return = benchmark_equities[-1] / benchmark_equities[0] - 1.0
        cagr = _cagr(equities[0], equities[-1], start, end)
        benchmark_cagr = _cagr(benchmark_equities[0], benchmark_equities[-1], start, end)
        max_drawdown, recovery_sessions, recovery_censored = _drawdown_recovery(equities)
        benchmark_mdd, benchmark_recovery_sessions, benchmark_recovery_censored = _drawdown_recovery(
            benchmark_equities
        )
        sharpe = _annualized_ratio(returns)
        downside = [min(value, 0.0) for value in returns]
        sortino = _annualized_ratio(returns, denominator_values=downside)
        volatility = statistics.pstdev(returns) * math.sqrt(252.0) if len(returns) > 1 else 0.0
        excess = [left - right for left, right in zip(returns, benchmark_returns)]
        information_ratio = _annualized_ratio(excess)
        information_coefficient = _correlation(returns, benchmark_returns)
        positive_benchmark = [index for index, value in enumerate(benchmark_returns) if value > 0.0]
        benchmark_up = sum(benchmark_returns[index] for index in positive_benchmark)
        strategy_up = sum(returns[index] for index in positive_benchmark)
        upside_capture = strategy_up / benchmark_up if benchmark_up > 0.0 else 0.0
        upside_participation = (
            sum(1 for index in positive_benchmark if returns[index] > 0.0) / len(positive_benchmark)
            if positive_benchmark
            else 0.0
        )
        losses = abs(sum(value for value in returns if value < 0.0))
        profit_factor = sum(value for value in returns if value > 0.0) / losses if losses > 0.0 else 0.0
        var_95 = _quantile(returns, 0.05)
        tail = [value for value in returns if value <= var_95]
        cvar_95 = statistics.fmean(tail) if tail else var_95
        calmar = cagr / abs(max_drawdown) if max_drawdown < 0.0 else 0.0
        result = BacktestResult(
            strategy_profile=_PROFILE,
            domain=_DOMAIN,
            param_set_id="soxl_p3_candidate",
            params={},
            sharpe_ratio=sharpe,
            calmar_ratio=calmar,
            sortino_ratio=sortino,
            max_drawdown=max_drawdown,
            cagr=cagr,
            volatility=volatility,
            win_rate=sum(1 for value in returns if value > 0.0) / len(returns) if returns else 0.0,
            total_return=total_return,
            start_date=start,
            end_date=end,
            observation_count=len(equities),
            benchmark_symbol="SOXX",
            benchmark_cagr=benchmark_cagr,
            benchmark_max_drawdown=benchmark_mdd,
            excess_cagr=cagr - benchmark_cagr,
            oos_sharpe=sharpe,
            oos_calmar=calmar,
            oos_max_drawdown=max_drawdown,
            walk_forward_stability=1.0,
            run_duration_seconds=0.0,
            source_script="soxl_promotion_runner",
        )
        return WindowEvidence(
            result=result,
            recovery_sessions=recovery_sessions,
            recovery_censored=recovery_censored,
            benchmark_recovery_sessions=benchmark_recovery_sessions,
            benchmark_recovery_censored=benchmark_recovery_censored,
            benchmark_total_return=benchmark_total_return,
            upside_capture=upside_capture,
            upside_participation=upside_participation,
            turnover=sum(turnovers),
            trade_count=sum(trades),
            profit_factor=profit_factor,
            var_95=var_95,
            cvar_95=cvar_95,
            information_ratio=information_ratio,
            information_coefficient=information_coefficient,
            costs_paid=sum(costs),
            assessment_count=sum(assessments),
            state_digest_sha256=self._state_digest(state),
        )

    def run_purged_fold(
        self,
        strategy_profile: str,
        params: Mapping[str, Any],
        *,
        fold: PurgedWalkForwardFold,
        purge_days: int,
        embargo_days: int,
        cost_model: PromotionCostModel,
    ) -> BacktestResult:
        self._require_run_contract(strategy_profile, params, purge_days, embargo_days)
        total_cost_bps = _total_cost_bps(cost_model)
        evidence = self._replay_window(fold.test_start, fold.test_end, total_cost_bps)
        self._window_evidence[(fold.test_start, fold.test_end, total_cost_bps)] = evidence
        return evidence.result

    def run_locked_oos(
        self,
        strategy_profile: str,
        params: Mapping[str, Any],
        *,
        start_date: date,
        end_date: date,
        cost_model: PromotionCostModel,
    ) -> BacktestResult:
        self._require_run_contract(strategy_profile, params, 20, 20)
        total_cost_bps = _total_cost_bps(cost_model)
        evidence = self._replay_window(start_date, end_date, total_cost_bps)
        self._window_evidence[(start_date, end_date, total_cost_bps)] = evidence
        return evidence.result

    def _require_run_contract(
        self,
        strategy_profile: str,
        params: Mapping[str, Any],
        purge_days: int,
        embargo_days: int,
    ) -> None:
        if strategy_profile != _PROFILE or purge_days != 20 or embargo_days != 20:
            raise SoxlPromotionContractError("promotion run contract mismatch")
        if dict(params) != {
            "candidate_identity_sha256": self.candidate_identity.candidate_sha256,
            "config_sha256": self.candidate_identity.config_sha256,
            "input_manifest_sha256": self.candidate_identity.input_manifest_sha256,
            "variant_id": self.variant_id,
        }:
            raise SoxlPromotionContractError("promotion params are not candidate-bound")

    def window_evidence(self, start: date, end: date, total_cost_bps: float) -> WindowEvidence:
        try:
            return self._window_evidence[(start, end, float(total_cost_bps))]
        except KeyError as exc:
            raise SoxlPromotionContractError("missing window evidence") from exc


def _total_cost_bps(cost_model: PromotionCostModel) -> float:
    return float(cost_model.commission_bps + cost_model.slippage_bps + cost_model.market_impact_bps)


def _add_calendar_months(value: date, months: int) -> date:
    import calendar

    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    return date(year, month, min(value.day, calendar.monthrange(year, month)[1]))


def _returns(values: Sequence[float]) -> list[float]:
    return [values[index] / values[index - 1] - 1.0 for index in range(1, len(values))]


def _cagr(start_value: float, end_value: float, start: date, end: date) -> float:
    years = max((end - start).days / 365.25, 1.0 / 365.25)
    return (end_value / start_value) ** (1.0 / years) - 1.0


def _annualized_ratio(values: Sequence[float], *, denominator_values: Sequence[float] | None = None) -> float:
    if not values:
        return 0.0
    denominator = denominator_values if denominator_values is not None else values
    deviation = statistics.pstdev(denominator) if len(denominator) > 1 else 0.0
    return statistics.fmean(values) / deviation * math.sqrt(252.0) if deviation > 0.0 else 0.0


def _correlation(left: Sequence[float], right: Sequence[float]) -> float:
    if len(left) < 2 or len(left) != len(right):
        return 0.0
    left_mean = statistics.fmean(left)
    right_mean = statistics.fmean(right)
    numerator = sum((x - left_mean) * (y - right_mean) for x, y in zip(left, right))
    denominator = math.sqrt(
        sum((x - left_mean) ** 2 for x in left) * sum((y - right_mean) ** 2 for y in right)
    )
    return numerator / denominator if denominator > 0.0 else 0.0


def _quantile(values: Sequence[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(fraction * len(ordered)) - 1))
    return ordered[index]


def _drawdown_recovery(values: Sequence[float]) -> tuple[float, int | None, bool]:
    peak = values[0]
    peak_index = 0
    trough_index = 0
    max_drawdown = 0.0
    max_peak = peak
    max_peak_index = 0
    for index, value in enumerate(values):
        if value > peak:
            peak = value
            peak_index = index
        drawdown = value / peak - 1.0
        if drawdown < max_drawdown:
            max_drawdown = drawdown
            trough_index = index
            max_peak = peak
            max_peak_index = peak_index
    if max_drawdown == 0.0:
        return 0.0, 0, False
    for index in range(trough_index + 1, len(values)):
        if values[index] >= max_peak:
            return max_drawdown, index - max_peak_index, False
    return max_drawdown, None, True


def _write_canonical(path: Path, value: Any) -> dict[str, str]:
    payload = canonical_json_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return {"path": path.as_posix(), "sha256": _sha256_bytes(payload)}


def _refresh_evidence_digests(payload: dict[str, Any]) -> None:
    core = {field: payload[field] for field in _CORE_FIELDS}
    payload["digests"]["evidence_core_sha256"] = _sha256_bytes(
        canonical_evidence_package_v2_bytes(core)
    )
    projection = copy.deepcopy(payload)
    projection["digests"].pop("package_sha256", None)
    payload["digests"]["package_sha256"] = _sha256_bytes(
        canonical_evidence_package_v2_bytes(projection)
    )


def _recovery_passes(candidate: WindowEvidence) -> bool:
    if candidate.recovery_censored:
        return False
    if candidate.benchmark_recovery_censored:
        return True
    return (
        candidate.recovery_sessions is not None
        and candidate.benchmark_recovery_sessions is not None
        and candidate.recovery_sessions <= candidate.benchmark_recovery_sessions
    )


def _window_acceptance(candidate: WindowEvidence) -> bool:
    result = candidate.result
    return (
        result.total_return is not None
        and result.benchmark_cagr is not None
        and result.benchmark_max_drawdown is not None
        and result.cagr is not None
        and result.max_drawdown is not None
        and result.total_return > candidate.benchmark_total_return
        and result.cagr > result.benchmark_cagr
        and abs(result.max_drawdown) <= abs(result.benchmark_max_drawdown)
        and _recovery_passes(candidate)
    )


def run_soxl_promotion_research(
    *,
    input_payload: Mapping[str, Any],
    config_payload: Mapping[str, Any],
    output_dir: str | Path,
    generated_at: str | None = None,
) -> dict[str, str]:
    """Run the frozen protocol and write only explicit local immutable evidence."""
    output_root = Path(output_dir)
    if output_root.exists() and any(output_root.iterdir()):
        raise SoxlPromotionContractError("output directory must be empty")
    output_root.mkdir(parents=True, exist_ok=True)
    artifacts_root = output_root / "artifacts"
    artifacts_root.mkdir()
    runners: dict[str, SoxlPromotionRunner] = {}
    runs: dict[str, dict[float, PromotionBacktestRun]] = {}
    details: dict[str, dict[float, dict[str, Any]]] = {}
    for variant_id in _ORDERED_VARIANTS:
        runner = SoxlPromotionRunner(input_payload, config_payload, variant_id=variant_id)
        runners[variant_id] = runner
        orchestrator = BacktestOrchestrator(
            store=PerformanceStore(
                cloud_bucket="",
                local_root=output_root / "orchestrator" / variant_id,
            )
        )
        orchestrator.register_runner(_DOMAIN, runner)
        params = {
            "candidate_identity_sha256": runner.candidate_identity.candidate_sha256,
            "config_sha256": runner.candidate_identity.config_sha256,
            "input_manifest_sha256": runner.candidate_identity.input_manifest_sha256,
            "variant_id": variant_id,
        }
        variant_runs: dict[float, PromotionBacktestRun] = {}
        for total_cost_bps in (5.0, 10.0, 15.0, 25.0):
            cost_model = PromotionCostModel(
                model_id=f"half_l1_{int(total_cost_bps)}bp_v1",
                commission_bps=0.0,
                slippage_bps=total_cost_bps,
                market_impact_bps=0.0,
            )
            variant_runs[total_cost_bps] = orchestrator.run_promotion(
                _PROFILE,
                domain=_DOMAIN,
                params=params,
                folds=runner.folds,
                locked_oos_start=runner.locked_oos_start,
                locked_oos_end=runner.locked_oos_end,
                purge_days=20,
                embargo_days=20,
                source_revision=runner.candidate_identity.strategy_revision,
                cost_model=cost_model,
                param_set_id=f"soxl_p3_{variant_id}_{int(total_cost_bps)}bp",
            )
        runs[variant_id] = variant_runs
        details[variant_id] = {
            cost: {
                "folds": [
                    runner.window_evidence(fold.test_start, fold.test_end, cost).to_dict()
                    for fold in runner.folds
                ],
                "locked_oos": runner.window_evidence(
                    runner.locked_oos_start, runner.locked_oos_end, cost
                ).to_dict(),
            }
            for cost in variant_runs
        }
    primary_runner = runners[_ORDERED_VARIANTS[0]]
    availability_contract_sha256 = _sha256_json(_FROZEN_AVAILABILITY_CONTRACT)
    qqqi_start_index = primary_runner._date_to_index[
        date.fromisoformat(FIRST_ELIGIBLE_SESSION["QQQI"])
    ]
    availability_segments = {
        "pre_qqqi": {
            "start": primary_runner.sessions[0]["date"],
            "end": primary_runner.sessions[qqqi_start_index - 1]["date"],
            "primary_policy": "independent_qqq_fallback",
            "sensitivity_policy": "cash_origin",
            "observed_qqqi": False,
        },
        "actual_qqqi": {
            "start": FIRST_ELIGIBLE_SESSION["QQQI"],
            "end": primary_runner.sessions[-1]["date"],
            "primary_policy": "actual_qqqi",
            "sensitivity_policy": "actual_qqqi",
            "observed_qqqi": True,
        },
        "locked_oos": {
            "start": primary_runner.locked_oos_start.isoformat(),
            "end": primary_runner.locked_oos_end.isoformat(),
            "session_count": 252,
            "actual_only": True,
        },
    }
    stress_25_payload = {
        "schema_version": "soxl_cost_stress.v2",
        "total_cost_bps": 25.0,
        "ordered_variants": list(_ORDERED_VARIANTS),
        "variants": {
            variant_id: {
                "promotion_run": runs[variant_id][25.0].to_dict(),
                "locked_oos_result": details[variant_id][25.0]["locked_oos"]["result"],
                "window_evidence": details[variant_id][25.0]["locked_oos"],
            }
            for variant_id in _ORDERED_VARIANTS
        },
    }
    stress_25_record = _write_canonical(artifacts_root / "cost-stress-25bp.json", stress_25_payload)
    config_record = _write_canonical(artifacts_root / "config.json", config_payload)
    manifest_path = artifacts_root / "data-manifest.json"
    manifest_path.write_bytes(canonical_research_input_manifest_bytes(primary_runner.input_manifest))
    manifest_record = {"path": manifest_path.as_posix(), "sha256": _sha256_bytes(manifest_path.read_bytes())}
    backtest_record = _write_canonical(
        artifacts_root / "backtest.json",
        {
            "schema_version": "soxl_promotion_backtest.v2",
            "availability_contract_sha256": availability_contract_sha256,
            "availability_segments": availability_segments,
            "ordered_variants": list(_ORDERED_VARIANTS),
            "variants": {
                variant_id: {
                    "runs": {
                        str(int(cost)): run.to_dict()
                        for cost, run in runs[variant_id].items()
                    },
                    "window_evidence": {
                        str(int(cost)): value
                        for cost, value in details[variant_id].items()
                    },
                }
                for variant_id in _ORDERED_VARIANTS
            },
        },
    )
    variant_risk: dict[str, dict[str, Any]] = {}
    for variant_id in _ORDERED_VARIANTS:
        runner = runners[variant_id]
        locked_oos = runner.window_evidence(
            runner.locked_oos_start, runner.locked_oos_end, 5.0
        )
        fold_passes = sum(
            _window_acceptance(runner.window_evidence(fold.test_start, fold.test_end, 5.0))
            for fold in runner.folds
        )
        cost_positive = all(
            runner.window_evidence(
                runner.locked_oos_start, runner.locked_oos_end, cost
            ).result.total_return
            > 0.0
            for cost in (5.0, 10.0, 25.0)
        )
        variant_pass = _window_acceptance(locked_oos) and fold_passes >= 2 and cost_positive
        variant_risk[variant_id] = {
            "status": "PASS" if variant_pass else "FAIL",
            "locked_oos_return_mdd_recovery": _window_acceptance(locked_oos),
            "ordered_fold_pass_count": fold_passes,
            "cost_5_10_25_positive": cost_positive,
            "capture_reported": True,
            "final_oos_actual_only": True,
            "candidate_identity_sha256": runner.candidate_identity.candidate_sha256,
            "state_digest_sha256": locked_oos.state_digest_sha256,
            "risk_assessment_count": locked_oos.assessment_count,
        }
    primary_pass = variant_risk[_ORDERED_VARIANTS[0]]["status"] == "PASS"
    sensitivity_pass = variant_risk[_ORDERED_VARIANTS[1]]["status"] == "PASS"
    proxy_sensitive = primary_pass != sensitivity_pass
    risk_status = "PROXY_SENSITIVE" if proxy_sensitive else "PASS" if primary_pass else "FAIL"
    risk_record = _write_canonical(
        artifacts_root / "risk.json",
        {
            "schema_version": "soxl_p3_acceptance.v2",
            "availability_contract_sha256": availability_contract_sha256,
            "availability_segments": availability_segments,
            "status": risk_status,
            "proxy_sensitive": proxy_sensitive,
            "ordered_variants": list(_ORDERED_VARIANTS),
            "variants": variant_risk,
        },
    )
    primary = primary_runner.window_evidence(
        primary_runner.locked_oos_start, primary_runner.locked_oos_end, 5.0
    )
    variant_information = {}
    for variant_id in _ORDERED_VARIANTS:
        runner = runners[variant_id]
        window = runner.window_evidence(runner.locked_oos_start, runner.locked_oos_end, 5.0)
        variant_information[variant_id] = {
            "information_coefficient": window.information_coefficient,
            "information_ratio": window.information_ratio,
            "upside_capture": window.upside_capture,
            "upside_participation": window.upside_participation,
        }
    information_record = _write_canonical(
        artifacts_root / "information-coefficient.json",
        {
            "schema_version": "soxl_information_coefficient.v1",
            "information_coefficient": primary.information_coefficient,
            "information_ratio": primary.information_ratio,
            "upside_capture": primary.upside_capture,
            "upside_participation": primary.upside_participation,
            "ordered_variants": list(_ORDERED_VARIANTS),
            "variants": variant_information,
        },
    )
    cost_record = _write_canonical(
        artifacts_root / "cost-model.json",
        {
            "schema_version": "soxl_half_l1_cost.v1",
            "method": "half_l1_turnover",
            "v2_scenarios_bps": [5.0, 10.0, 15.0],
            "required_stress_bps": [10.0, 25.0],
            "ordered_variants": list(_ORDERED_VARIANTS),
            "cost_stress_25bp_sha256": stress_25_record["sha256"],
        },
    )
    records = {
        "config": config_record,
        "data_manifest": manifest_record,
        "backtest": backtest_record,
        "risk": risk_record,
        "information_coefficient": information_record,
        "cost_model": cost_record,
    }
    for record in records.values():
        record["path"] = Path(record["path"]).relative_to(output_root).as_posix()
    generated = generated_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    terminal_payload = {
        "schema_version": "soxl_promotion_research_result.v1",
        "generated_at": generated,
        "status": risk_status,
        "candidate_id": CANDIDATE_ID,
        "candidate_identity_sha256": primary_runner.candidate_identity.candidate_sha256,
        "input_manifest_sha256": primary_runner.input_manifest_sha256,
        "source_contract_sha256": primary_runner.source_contract_sha256,
        "artifacts": records,
        "human_acceptance": None,
        "lifecycle_claims": {
            "learning_only": False,
            "promotion_eligible": False,
            "live_ready": False,
            "size_zero_required": True,
            "no_order": True,
        },
    }
    if risk_status != "PASS":
        _write_canonical(output_root / "promotion-research-result.v1.json", terminal_payload)
        raise SoxlPromotionContractError(f"promotion acceptance failed: {risk_status}")
    evidence: dict[str, Any] = {
        "schema_version": "strategy_evidence_package.v2",
        "evidence_package_id": f"soxl_p3_{primary_runner.candidate_identity.candidate_sha256[:12]}",
        "generated_at": generated,
        "requested_stage": "research_backtest_only",
        "strategy": {
            "profile": _PROFILE,
            "domain": _DOMAIN,
            "source_revision": primary_runner.candidate_identity.strategy_revision,
        },
        "input_provenance": {
            "source": CANDIDATE_ID,
            "source_revision": primary_runner.source_contract_sha256,
            "license": primary_runner.config["input_license"],
            "usage_scope": primary_runner.config["input_usage_scope"],
            "range": {
                "start": primary_runner.sessions[0]["date"],
                "end": primary_runner.sessions[-1]["date"],
            },
            "timestamp": primary_runner.input_manifest["as_of"],
            "manifest_sha256": primary_runner.input_manifest_sha256,
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
            "promotion_run": runs[_ORDERED_VARIANTS[0]][5.0].to_dict(),
        },
        "artifacts": records,
        "metrics": {
            "sharpe_ratio": primary.result.sharpe_ratio,
            "sortino_ratio": primary.result.sortino_ratio,
            "max_drawdown": primary.result.max_drawdown,
            "annualized_return": primary.result.cagr,
            "annualized_volatility": primary.result.volatility,
            "calmar_ratio": primary.result.calmar_ratio,
            "information_ratio": primary.information_ratio,
            "information_coefficient": primary.information_coefficient,
            "var_95": primary.var_95,
            "cvar_95": primary.cvar_95,
            "turnover": primary.turnover,
            "trade_count": primary.trade_count,
            "win_rate": primary.result.win_rate,
            "profit_factor": primary.profit_factor,
        },
        "cost_stress": {
            "scenarios": [
                {"multiplier": 1, "total_cost_bps": 5.0},
                {"multiplier": 2, "total_cost_bps": 10.0},
                {"multiplier": 3, "total_cost_bps": 15.0},
            ],
            "status": "PASS",
        },
        "risk_assessment": {
            "status": "PASS",
            "standard_id": primary_runner.config["risk_standard_id"],
            "standard_sha256": primary_runner.config["risk_standard_sha256"],
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
    _refresh_evidence_digests(evidence)
    if validate_evidence_package_v2(evidence, base_dir=output_root):
        terminal_payload["status"] = "EVIDENCE_INVALID"
        _write_canonical(output_root / "promotion-research-result.v1.json", terminal_payload)
        raise SoxlPromotionContractError("evidence package validation failed")
    evidence_path = output_root / "strategy-evidence-package.v2.json"
    evidence_path.write_bytes(canonical_evidence_package_v2_bytes(evidence))
    terminal_record = _write_canonical(
        output_root / "promotion-research-result.v1.json", terminal_payload
    )
    return {
        "evidence_sha256": _sha256_bytes(evidence_path.read_bytes()),
        "cost_stress_25bp_sha256": stress_25_record["sha256"],
        "promotion_result_sha256": terminal_record["sha256"],
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run candidate-bound offline SOXL promotion research")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    result = run_soxl_promotion_research(
        input_payload=_strict_json(args.input),
        config_payload=_strict_json(args.config),
        output_dir=args.output,
    )
    print(canonical_json_bytes(result).decode("utf-8"))
    return 0


__all__ = [
    "SOXL_PROMOTION_ASSETS",
    "SoxlPromotionContractError",
    "SoxlPromotionRunner",
    "WindowEvidence",
    "canonical_json_bytes",
    "run_soxl_promotion_research",
    "main",
]
