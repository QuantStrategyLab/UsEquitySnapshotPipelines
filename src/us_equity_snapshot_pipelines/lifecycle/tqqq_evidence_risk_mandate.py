"""Strict one-shot authority and evidence-risk material for TQQQ research."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import math
import os
import sqlite3
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from quant_platform_kit.common.capital_base import (
    CapitalBaseBinding,
    CapitalBaseSnapshot,
    CapitalScope,
    CapitalValuationBasis,
)
from quant_platform_kit.risk import assess_with_evidence
from quant_platform_kit.risk.contracts import CandidateRiskIdentity
from quant_platform_kit.common.strategy_contracts import StrategyDecision

AUTHORITY_RECEIPT_SHA256 = (
    "c0c5020fbe64057b735f987b3bcc490dfe708304b58f01d57cd581344afb44c8"
)
AUTHORITY_SOURCE_REVISION = "ca259ebde6967309771d61f75af33d036239678a"
QPK_SOURCE_REVISION = "7f140f07ac89f0b4b88347a903906825dde11c39"
CANONICAL_AUTHORITY_LEDGER_PATH = Path(
    "/var/lib/quantstrategylab/risk-authority-ledgers/v1"
) / f"{AUTHORITY_RECEIPT_SHA256}.sqlite3"

_MANDATE_ID = "tqqq_core_parity_v1"
_ACCOUNT_MODE = "single_strategy_account_v1"
_ALLOWED_ASSETS = ("TQQQ", "QQQM", "BOXX")
_FACTORS = {"TQQQ": 3, "QQQM": 1, "BOXX": 1}
_NOMINAL_CAPS = {"TQQQ": 0.15, "QQQM": 0.50, "BOXX": 0.50}
_EFFECTIVE_CAPS = {"TQQQ": 0.45, "QQQM": 0.50, "BOXX": 0.50}
_MAX_AGE_SECONDS = 300
_DIGEST_LENGTH = 64
_MAX_RECEIPT_BYTES = 32_768
_VERIFIED_SESSION_TOKEN = object()
_POLICY_REJECT_REASONS = frozenset(
    {
        "account_breaker_triggered",
        "effective_exposure_cap",
        "max_nonzero_assets",
        "observed_effective_exposure",
        "product_effective_exposure_cap",
        "product_exposure_cap",
        "risk_budget_exposure_cap",
        "risk_engine_non_approve",
    }
)
_AUTHORITY_FIELDS = frozenset(
    {
        "allowed_tradable_assets",
        "authority_role",
        "authority_scope",
        "authority_store",
        "benchmark_only_assets",
        "capital_scope",
        "currency",
        "decided_at",
        "decided_by",
        "decision",
        "decision_id",
        "decision_source",
        "drawdown_scalars",
        "effective_product_caps",
        "execution_constraints",
        "fx_conversion_allowed",
        "integrity",
        "leverage_factors",
        "loss_budget_equity_reference",
        "loss_budget_fraction",
        "mandate_validity_seconds",
        "max_nonzero_assets",
        "modeled_stress_is_not_stop_order",
        "modeled_stress_loss_distance",
        "nominal_product_caps",
        "policy_bundle",
        "prohibited",
        "purpose",
        "runner_is_authority",
        "schema_version",
        "signature",
        "single_consumption",
        "snapshot_max_age_seconds",
        "source_revision_binding",
        "total_effective_exposure_cap",
        "valuation_basis",
    }
)


class TqqqEvidenceRiskMandateError(ValueError):
    """Sanitized fail-closed evidence-risk authority error."""


@dataclass(frozen=True)
class TqqqEvidenceRiskAssessment:
    approved: bool
    input_digest_sha256: str
    assessment_sha256: str
    reason_codes: tuple[str, ...]


def _canonical(value: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise TqqqEvidenceRiskMandateError("invalid risk material") from exc


def _digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _digest_text(value: object, field: str) -> str:
    if (
        type(value) is not str
        or len(value) != _DIGEST_LENGTH
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise TqqqEvidenceRiskMandateError(f"invalid {field}")
    return value


def _revision(value: object, field: str) -> str:
    if (
        type(value) is not str
        or len(value) != 40
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise TqqqEvidenceRiskMandateError(f"invalid {field}")
    return value


def _number(value: object, field: str, *, positive: bool = False) -> float:
    if type(value) not in (int, float):
        raise TqqqEvidenceRiskMandateError(f"invalid {field}")
    number = float(value)
    if not math.isfinite(number) or (positive and number <= 0.0):
        raise TqqqEvidenceRiskMandateError(f"invalid {field}")
    return number


def _logical_time(value: datetime | None) -> datetime:
    logical = datetime.now(UTC).replace(microsecond=0) if value is None else value
    if (
        type(logical) is not datetime
        or logical.tzinfo is None
        or logical.utcoffset() != timedelta(0)
        or logical.microsecond != 0
    ):
        raise TqqqEvidenceRiskMandateError("invalid logical evaluation time")
    logical = logical.astimezone(UTC)
    age = (datetime.now(UTC) - logical).total_seconds()
    if age < 0.0 or age > _MAX_AGE_SECONDS:
        raise TqqqEvidenceRiskMandateError("invalid logical evaluation time")
    return logical


def _timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _strict_json(data: bytes) -> Mapping[str, Any]:
    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise TqqqEvidenceRiskMandateError("invalid authority receipt")
            result[key] = value
        return result

    try:
        value = json.loads(
            data,
            object_pairs_hook=pairs,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                TqqqEvidenceRiskMandateError("invalid authority receipt")
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TqqqEvidenceRiskMandateError("invalid authority receipt") from exc
    if not isinstance(value, Mapping):
        raise TqqqEvidenceRiskMandateError("invalid authority receipt")
    return value


def _regular_file_bytes(path: Path) -> bytes:
    try:
        details = path.lstat()
        if not stat.S_ISREG(details.st_mode) or path.is_symlink():
            raise TqqqEvidenceRiskMandateError("invalid authority receipt")
        if details.st_size <= 0 or details.st_size > _MAX_RECEIPT_BYTES:
            raise TqqqEvidenceRiskMandateError("invalid authority receipt")
        data = path.read_bytes()
    except OSError as exc:
        raise TqqqEvidenceRiskMandateError("invalid authority receipt") from exc
    if len(data) != details.st_size:
        raise TqqqEvidenceRiskMandateError("invalid authority receipt")
    return data


def _validate_authority_receipt(path: Path, source_revision: str) -> None:
    if source_revision != AUTHORITY_SOURCE_REVISION:
        raise TqqqEvidenceRiskMandateError("authority source revision mismatch")
    data = _regular_file_bytes(path)
    if hashlib.sha256(data).hexdigest() != AUTHORITY_RECEIPT_SHA256:
        raise TqqqEvidenceRiskMandateError("authority receipt digest mismatch")
    sidecar = path.with_suffix(path.suffix + ".sha256")
    expected_sidecar = f"{AUTHORITY_RECEIPT_SHA256}  {path.name}\n".encode()
    if _regular_file_bytes(sidecar) != expected_sidecar:
        raise TqqqEvidenceRiskMandateError("authority receipt sidecar mismatch")
    payload = _strict_json(data)
    if (
        set(payload) != _AUTHORITY_FIELDS
        or payload.get("schema_version") != "qsl.human-authority-receipt.v1"
        or payload.get("decision") != "APPROVE"
        or payload.get("policy_bundle") != "CONSERVATIVE_RESEARCH_V1"
        or payload.get("authority_scope") != "RESEARCH_ONLY"
        or payload.get("purpose") != "TQQQ_CANDIDATE_RESEARCH_EVIDENCE_ONLY"
        or payload.get("allowed_tradable_assets") != ["BOXX", "QQQM", "TQQQ"]
        or payload.get("benchmark_only_assets") != ["QQQ"]
        or payload.get("leverage_factors")
        != {"BOXX": "1", "QQQM": "1", "TQQQ": "3"}
        or payload.get("nominal_product_caps")
        != {"BOXX": "0.50", "QQQM": "0.50", "TQQQ": "0.15"}
        or payload.get("effective_product_caps")
        != {"BOXX": "0.50", "QQQM": "0.50", "TQQQ": "0.45"}
        or payload.get("total_effective_exposure_cap") != "0.50"
        or payload.get("loss_budget_fraction") != "0.01"
        or payload.get("loss_budget_equity_reference") != "completed_session_equity"
        or payload.get("mandate_validity_seconds") != _MAX_AGE_SECONDS
        or payload.get("snapshot_max_age_seconds") != _MAX_AGE_SECONDS
        or payload.get("single_consumption") is not True
        or payload.get("capital_scope") != "allocated_sleeve"
        or payload.get("valuation_basis") != "allocated_sleeve_ledger"
        or payload.get("currency") != "USD"
        or payload.get("fx_conversion_allowed") is not False
        or payload.get("runner_is_authority") is not False
        or payload.get("signature") is not None
        or payload.get("execution_constraints")
        != {
            "no_live": True,
            "no_order": True,
            "no_paper": True,
            "no_promotion_authority": True,
            "no_shadow": True,
        }
    ):
        raise TqqqEvidenceRiskMandateError("invalid authority receipt")


def _validate_store_path(path: Path) -> None:
    try:
        parent = path.parent
        parent_details = parent.lstat()
        if (
            not stat.S_ISDIR(parent_details.st_mode)
            or parent.is_symlink()
            or parent_details.st_uid != os.getuid()
            or stat.S_IMODE(parent_details.st_mode) != 0o700
        ):
            raise TqqqEvidenceRiskMandateError("invalid consumption store")
        if not path.exists() and not path.is_symlink():
            raise TqqqEvidenceRiskMandateError("invalid consumption store")
        details = path.lstat()
        if (
            not stat.S_ISREG(details.st_mode)
            or path.is_symlink()
            or details.st_uid != os.getuid()
            or details.st_nlink != 1
            or stat.S_IMODE(details.st_mode) != 0o600
        ):
            raise TqqqEvidenceRiskMandateError("invalid consumption store")
    except OSError as exc:
        raise TqqqEvidenceRiskMandateError("invalid consumption store") from exc


def _validate_qpk_revision() -> None:
    try:
        direct_url = importlib.metadata.distribution("quant-platform-kit").read_text(
            "direct_url.json"
        )
        payload = json.loads(direct_url or "null")
        revision = payload["vcs_info"]["commit_id"]
    except (ImportError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise TqqqEvidenceRiskMandateError("QPK source revision unavailable") from exc
    if revision != QPK_SOURCE_REVISION:
        raise TqqqEvidenceRiskMandateError("QPK source revision mismatch")


def _decision_digest(decision: StrategyDecision) -> str:
    return _digest(
        {
            "positions": [
                {
                    "symbol": position.symbol,
                    "target_weight": position.target_weight,
                    "target_value": position.target_value,
                    "role": position.role,
                    "order_preference": position.order_preference,
                }
                for position in decision.positions
            ],
            "budgets": [
                {
                    "name": budget.name,
                    "symbol": budget.symbol,
                    "amount": budget.amount,
                    "unit": budget.unit,
                    "purpose": budget.purpose,
                }
                for budget in decision.budgets
            ],
            "risk_flags": list(decision.risk_flags),
            "diagnostics_sha256": hashlib.sha256(
                _canonical(dict(decision.diagnostics))
            ).hexdigest(),
        }
    )


class TqqqEvidenceRiskMandateSession:
    """Durable at-most-once gate session for one approved evidence run."""

    def __init__(
        self,
        *,
        consumption_store_path: Path,
        logical_evaluation_time: datetime,
        verification_token: object,
    ) -> None:
        if verification_token is not _VERIFIED_SESSION_TOKEN:
            raise TqqqEvidenceRiskMandateError("unverified evidence risk session")
        self._store = consumption_store_path
        self._logical_time = logical_evaluation_time
        self._verification_token = verification_token
        self._candidate: CandidateRiskIdentity | None = None
        self._consumption_id: str | None = None
        self._assessments: list[TqqqEvidenceRiskAssessment] = []
        self._sealed: dict[str, object] | None = None

    @property
    def authority_receipt_sha256(self) -> str:
        return AUTHORITY_RECEIPT_SHA256

    @property
    def authority_source_revision(self) -> str:
        return AUTHORITY_SOURCE_REVISION

    @property
    def logical_evaluation_time(self) -> datetime:
        return self._logical_time

    @property
    def is_verified(self) -> bool:
        return (
            getattr(self, "_verification_token", None) is _VERIFIED_SESSION_TOKEN
        )

    def _connect(self) -> sqlite3.Connection:
        _validate_store_path(self._store)
        try:
            connection = sqlite3.connect(
                f"{self._store.absolute().as_uri()}?mode=rw",
                timeout=5.0,
                isolation_level=None,
                uri=True,
            )
            os.chmod(self._store, 0o600)
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute("PRAGMA journal_mode = DELETE")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS risk_consumptions (
                    authority_receipt_sha256 TEXT PRIMARY KEY,
                    consumption_id TEXT NOT NULL UNIQUE,
                    candidate_identity_sha256 TEXT NOT NULL,
                    logical_evaluation_time TEXT NOT NULL,
                    status TEXT NOT NULL,
                    assessment_count INTEGER NOT NULL,
                    consumption_receipt_sha256 TEXT,
                    failure_code TEXT
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS risk_assessments (
                    authority_receipt_sha256 TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    input_digest_sha256 TEXT NOT NULL,
                    status TEXT NOT NULL,
                    assessment_sha256 TEXT,
                    outcome TEXT,
                    failure_code TEXT,
                    PRIMARY KEY (authority_receipt_sha256, ordinal),
                    FOREIGN KEY (authority_receipt_sha256)
                        REFERENCES risk_consumptions(authority_receipt_sha256)
                )
                """
            )
            return connection
        except (OSError, sqlite3.Error) as exc:
            raise TqqqEvidenceRiskMandateError("invalid consumption store") from exc

    def start(self, candidate: CandidateRiskIdentity) -> None:
        if type(candidate) is not CandidateRiskIdentity or self._candidate is not None:
            raise TqqqEvidenceRiskMandateError("invalid risk candidate")
        if (
            candidate.strategy_profile != _MANDATE_ID
            or candidate.account_mode != _ACCOUNT_MODE
            or candidate.authority_receipt_sha256 != AUTHORITY_RECEIPT_SHA256
        ):
            raise TqqqEvidenceRiskMandateError("risk candidate binding mismatch")
        material = {
            "schema_version": "qsl.tqqq-evidence-risk-consumption-id.v1",
            "authority_receipt_sha256": AUTHORITY_RECEIPT_SHA256,
            "authority_source_revision": AUTHORITY_SOURCE_REVISION,
            "candidate_identity_sha256": candidate.candidate_sha256,
            "logical_evaluation_time": _timestamp(self._logical_time),
        }
        consumption_id = _digest(material)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT status FROM risk_consumptions WHERE authority_receipt_sha256 = ?",
                (AUTHORITY_RECEIPT_SHA256,),
            ).fetchone()
            if existing is not None:
                if existing == ("STARTED",):
                    connection.execute(
                        """
                        UPDATE risk_consumptions
                        SET status = 'PARKED', failure_code = 'RECOVERED_INDETERMINATE'
                        WHERE authority_receipt_sha256 = ? AND status = 'STARTED'
                        """,
                        (AUTHORITY_RECEIPT_SHA256,),
                    )
                    connection.execute(
                        """
                        UPDATE risk_assessments
                        SET status = 'PARKED', failure_code = 'RECOVERED_INDETERMINATE'
                        WHERE authority_receipt_sha256 = ? AND status = 'STARTED'
                        """,
                        (AUTHORITY_RECEIPT_SHA256,),
                    )
                connection.execute("COMMIT")
                raise TqqqEvidenceRiskMandateError("risk authority already consumed")
            connection.execute(
                """
                INSERT INTO risk_consumptions (
                    authority_receipt_sha256, consumption_id,
                    candidate_identity_sha256, logical_evaluation_time,
                    status, assessment_count
                ) VALUES (?, ?, ?, ?, 'STARTED', 0)
                """,
                (
                    AUTHORITY_RECEIPT_SHA256,
                    consumption_id,
                    candidate.candidate_sha256,
                    _timestamp(self._logical_time),
                ),
            )
            connection.execute("COMMIT")
        except TqqqEvidenceRiskMandateError:
            raise
        except sqlite3.Error as exc:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise TqqqEvidenceRiskMandateError("risk consumption failed") from exc
        finally:
            connection.close()
        self._candidate = candidate
        self._consumption_id = consumption_id

    def park(self, failure_code: str) -> None:
        if (
            self._candidate is None
            or self._consumption_id is None
            or type(failure_code) is not str
            or not failure_code
            or any(character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ_" for character in failure_code)
        ):
            raise TqqqEvidenceRiskMandateError("invalid risk park state")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT status, failure_code FROM risk_consumptions
                WHERE authority_receipt_sha256 = ? AND consumption_id = ?
                """,
                (AUTHORITY_RECEIPT_SHA256, self._consumption_id),
            ).fetchone()
            if row == ("STARTED", None):
                connection.execute(
                    """
                    UPDATE risk_consumptions SET status = 'PARKED', failure_code = ?
                    WHERE authority_receipt_sha256 = ? AND consumption_id = ?
                        AND status = 'STARTED'
                    """,
                    (failure_code, AUTHORITY_RECEIPT_SHA256, self._consumption_id),
                )
                connection.execute(
                    """
                    UPDATE risk_assessments SET status = 'PARKED', failure_code = ?
                    WHERE authority_receipt_sha256 = ? AND status = 'STARTED'
                    """,
                    (failure_code, AUTHORITY_RECEIPT_SHA256),
                )
            elif row != ("PARKED", failure_code):
                connection.execute("ROLLBACK")
                raise TqqqEvidenceRiskMandateError("risk consumption state mismatch")
            connection.execute("COMMIT")
        except TqqqEvidenceRiskMandateError:
            raise
        except sqlite3.Error as exc:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise TqqqEvidenceRiskMandateError("risk park persistence failed") from exc
        finally:
            connection.close()

    def _park_best_effort(self, failure_code: str) -> None:
        if self._candidate is None or self._consumption_id is None:
            return
        try:
            self.park(failure_code)
        except TqqqEvidenceRiskMandateError:
            pass

    def _materials(
        self,
        *,
        equity: float,
        current_weights: Mapping[str, float],
        account_drawdown_fraction: float,
        source_identity_sha256: str,
    ) -> tuple[
        Mapping[str, Any],
        Mapping[str, Any],
        Mapping[str, Any],
        CapitalBaseSnapshot,
        CapitalBaseBinding,
    ]:
        candidate = self._candidate
        if candidate is None:
            raise TqqqEvidenceRiskMandateError("risk consumption not started")
        equity = _number(equity, "equity", positive=True)
        drawdown = _number(account_drawdown_fraction, "drawdown")
        if not 0.0 <= drawdown <= 1.0:
            raise TqqqEvidenceRiskMandateError("invalid drawdown")
        source_identity = _digest_text(source_identity_sha256, "source identity")
        if not isinstance(current_weights, Mapping) or set(current_weights) - set(
            _ALLOWED_ASSETS
        ):
            raise TqqqEvidenceRiskMandateError("invalid current weights")
        weights = {
            symbol: _number(current_weights.get(symbol, 0.0), "current weight")
            for symbol in _ALLOWED_ASSETS
        }
        if any(weight < 0.0 for weight in weights.values()):
            raise TqqqEvidenceRiskMandateError("invalid current weights")
        observed_effective_exposure = math.fsum(
            weights[symbol] * _FACTORS[symbol] for symbol in _ALLOWED_ASSETS
        )
        logical_timestamp = _timestamp(self._logical_time)
        coverage_digest = _digest(
            {"assets": list(_ALLOWED_ASSETS), "currency": "USD"}
        )
        capital_source_digest = _digest(
            {
                "candidate_identity_sha256": candidate.candidate_sha256,
                "source_identity_sha256": source_identity,
                "valuation_basis": "allocated_sleeve_ledger",
            }
        )
        capital = CapitalBaseSnapshot(
            reported_equity=equity,
            reported_currency="USD",
            target_currency="USD",
            fx_rate_to_target=1.0,
            as_of=self._logical_time,
            account_scope=_ACCOUNT_MODE,
            runtime_scope=candidate.runner_revision,
            strategy_scope=_MANDATE_ID,
            source_digest_sha256=capital_source_digest,
            capital_scope=CapitalScope.ALLOCATED_SLEEVE,
            valuation_basis=CapitalValuationBasis.ALLOCATED_SLEEVE_LEDGER,
            allocation_scope=_MANDATE_ID,
            component_coverage_digest_sha256=coverage_digest,
        )
        capital_binding = CapitalBaseBinding(
            account_scope=_ACCOUNT_MODE,
            runtime_scope=candidate.runner_revision,
            strategy_scope=_MANDATE_ID,
            target_currency="USD",
            capital_scope=CapitalScope.ALLOCATED_SLEEVE,
            valuation_basis=CapitalValuationBasis.ALLOCATED_SLEEVE_LEDGER,
            allocation_scope=_MANDATE_ID,
            max_age_seconds=float(_MAX_AGE_SECONDS),
        )
        capital_digest = _digest(
            {**capital.to_safe_dict(), "target_equity": capital.target_equity}
        )
        portfolio = {
            "schema_version": "qsl.tqqq-evidence-portfolio-snapshot.v1",
            "as_of": logical_timestamp,
            "observed_effective_exposure": observed_effective_exposure,
            "total_equity": equity,
            "source_identity_sha256": source_identity,
        }
        portfolio_digest = _digest(portfolio)
        scalar = 1.0 if drawdown <= 0.05 else 0.5 if drawdown <= 0.10 else 0.0
        risk_state = {
            "schema_version": "qsl.tqqq-evidence-risk-state.v1",
            "as_of": logical_timestamp,
            "mandate_id": _MANDATE_ID,
            "candidate_identity_sha256": candidate.candidate_sha256,
            "modeled_stress_loss_distance": 0.05,
            "account_drawdown_fraction": drawdown,
            "drawdown_scalar": scalar,
        }
        risk_state_digest = _digest(risk_state)
        mandate = {
            "schema_version": "qsl.tqqq-evidence-risk-mandate.v1",
            "mandate_id": _MANDATE_ID,
            "mandate_version": "v1",
            "purpose": "TQQQ_CANDIDATE_RESEARCH_EVIDENCE_ONLY",
            "candidate_binding": {
                "strategy_profile": candidate.strategy_profile,
                "account_mode": candidate.account_mode,
                "strategy_revision": candidate.strategy_revision,
                "runner_revision": candidate.runner_revision,
                "config_sha256": candidate.config_sha256,
                "input_manifest_sha256": candidate.input_manifest_sha256,
                "candidate_identity_sha256": candidate.candidate_sha256,
            },
            "validity": {
                "effective_at": logical_timestamp,
                "expires_at": _timestamp(
                    self._logical_time + timedelta(seconds=_MAX_AGE_SECONDS)
                ),
                "snapshot_max_age_seconds": _MAX_AGE_SECONDS,
                "single_consumption": True,
            },
            "portfolio_policy": {
                "allowed_nonzero_assets": list(_ALLOWED_ASSETS),
                "benchmark_only_assets": ["QQQ"],
                "product_leverage_factors": dict(_FACTORS),
                "max_nonzero_assets": 3,
                "effective_exposure_cap": 0.50,
                "nominal_caps": dict(_NOMINAL_CAPS),
                "product_effective_caps": dict(_EFFECTIVE_CAPS),
                "loss_budget": 0.01,
                "loss_budget_equity_reference": "completed_session_equity",
                "modeled_stress_loss_distance": 0.05,
                "stress_loss_is_model_assumption": True,
                "drawdown_scalars": {
                    "at_or_below_0_05": 1.0,
                    "above_0_05_to_0_10": 0.5,
                    "above_0_10": 0.0,
                },
                "broker_margin_factor": 1,
                "margin_stacking": False,
                "borrowing": False,
                "shorting": False,
            },
            "capital_binding": {
                "schema_version": "qpk.capital_base.v2",
                "snapshot_digest_sha256": capital_digest,
                "as_of": logical_timestamp,
                "account_mode": _ACCOUNT_MODE,
                "capital_scope": "allocated_sleeve",
                "valuation_basis": "allocated_sleeve_ledger",
                "target_currency": "USD",
                "max_age_seconds": _MAX_AGE_SECONDS,
                "fx_conversion_allowed": False,
            },
            "portfolio_binding": {
                "schema_version": portfolio["schema_version"],
                "snapshot_digest_sha256": portfolio_digest,
                "as_of": logical_timestamp,
                "source_identity_sha256": source_identity,
                "max_age_seconds": _MAX_AGE_SECONDS,
            },
            "risk_state_binding": {
                "schema_version": risk_state["schema_version"],
                "snapshot_digest_sha256": risk_state_digest,
                "as_of": logical_timestamp,
                "max_age_seconds": _MAX_AGE_SECONDS,
            },
            "authority": {
                "authority_scope": "RESEARCH_ONLY",
                "authority_receipt_sha256": AUTHORITY_RECEIPT_SHA256,
                "source_revision": AUTHORITY_SOURCE_REVISION,
                "runner_is_authority": False,
                "no_order": True,
                "no_paper": True,
                "no_shadow": True,
                "no_live": True,
                "no_promotion_authority": True,
            },
        }
        return mandate, portfolio, risk_state, capital, capital_binding

    def assess(
        self,
        decision: StrategyDecision,
        *,
        market_data: Mapping[str, Any],
        equity: float,
        current_weights: Mapping[str, float],
        account_drawdown_fraction: float,
        source_identity_sha256: str,
    ) -> TqqqEvidenceRiskAssessment:
        candidate = self._candidate
        if (
            type(decision) is not StrategyDecision
            or candidate is None
            or self._sealed is not None
            or not isinstance(market_data, Mapping)
        ):
            self._park_best_effort("ASSESSMENT_INPUT_INVALID")
            raise TqqqEvidenceRiskMandateError("invalid risk assessment")
        try:
            for position in decision.positions:
                if getattr(position, "symbol", None) not in _ALLOWED_ASSETS:
                    raise TqqqEvidenceRiskMandateError("asset not authorized")
            mandate, portfolio, risk_state, capital, capital_binding = self._materials(
                equity=equity,
                current_weights=current_weights,
                account_drawdown_fraction=account_drawdown_fraction,
                source_identity_sha256=source_identity_sha256,
            )
        except TqqqEvidenceRiskMandateError:
            self._park_best_effort("ASSESSMENT_INPUT_INVALID")
            raise
        ordinal = len(self._assessments) + 1
        input_digest = _digest(
            {
                "ordinal": ordinal,
                "decision_sha256": _decision_digest(decision),
                "market_data_sha256": hashlib.sha256(
                    _canonical(dict(market_data))
                ).hexdigest(),
                "mandate_sha256": hashlib.sha256(_canonical(mandate)).hexdigest(),
                "portfolio_sha256": hashlib.sha256(_canonical(portfolio)).hexdigest(),
                "risk_state_sha256": hashlib.sha256(_canonical(risk_state)).hexdigest(),
                "capital_snapshot_sha256": mandate["capital_binding"][
                    "snapshot_digest_sha256"
                ],
                "capital_binding_scope_sha256": capital_binding.scope_digest_sha256,
            }
        )
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT status, assessment_count FROM risk_consumptions
                WHERE authority_receipt_sha256 = ? AND consumption_id = ?
                """,
                (AUTHORITY_RECEIPT_SHA256, self._consumption_id),
            ).fetchone()
            if row != ("STARTED", ordinal - 1):
                connection.execute("ROLLBACK")
                raise TqqqEvidenceRiskMandateError("risk consumption state mismatch")
            connection.execute(
                """
                INSERT INTO risk_assessments (
                    authority_receipt_sha256, ordinal, input_digest_sha256, status
                ) VALUES (?, ?, ?, 'STARTED')
                """,
                (AUTHORITY_RECEIPT_SHA256, ordinal, input_digest),
            )
            connection.execute("COMMIT")
        except TqqqEvidenceRiskMandateError:
            self._park_best_effort("ASSESSMENT_START_FAILED")
            raise
        except sqlite3.Error as exc:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            self._park_best_effort("ASSESSMENT_START_FAILED")
            raise TqqqEvidenceRiskMandateError("risk assessment start failed") from exc
        finally:
            connection.close()

        try:
            result = assess_with_evidence(
                decision,
                portfolio,
                scope="ACCOUNT",
                mandate_provenance=mandate,
                market_data=market_data,
                candidate_identity=candidate,
                risk_control_state=risk_state,
                capital_base=capital,
                capital_base_binding=capital_binding,
                logical_evaluation_time=self._logical_time,
            )
            assessment = result.assessment
            digest = _digest_text(assessment.assessment_sha256, "assessment digest")
            reason_codes = assessment.reason_codes
            evaluated_at = _timestamp(self._logical_time)
            if (
                assessment.outcome not in {"APPROVE", "REJECT"}
                or type(reason_codes) is not tuple
                or any(type(reason) is not str or not reason for reason in reason_codes)
                or assessment.execution_authorized is not False
                or assessment.candidate_identity_sha256 != candidate.candidate_sha256
                or assessment.mandate_authority_receipt_sha256
                != AUTHORITY_RECEIPT_SHA256
                or assessment.mandate_id != _MANDATE_ID
                or assessment.mandate_scope != "RESEARCH_ONLY"
                or assessment.qpk_source_revision != AUTHORITY_SOURCE_REVISION
                or assessment.evaluated_at != evaluated_at
                or (assessment.outcome == "APPROVE" and reason_codes)
                or (assessment.outcome == "REJECT" and not reason_codes)
            ):
                raise TqqqEvidenceRiskMandateError("invalid risk assessment receipt")
            if assessment.outcome == "REJECT" and not set(reason_codes).issubset(
                _POLICY_REJECT_REASONS
            ):
                self._park_best_effort("ASSESSMENT_INTEGRITY_REJECTED")
                raise TqqqEvidenceRiskMandateError(
                    "risk assessment integrity rejected"
                )
        except TqqqEvidenceRiskMandateError:
            self._park_best_effort("ASSESSMENT_INDETERMINATE")
            raise
        except Exception as exc:
            self._park_best_effort("ASSESSMENT_INDETERMINATE")
            raise TqqqEvidenceRiskMandateError("risk assessment failed") from exc

        normalized = TqqqEvidenceRiskAssessment(
            approved=assessment.outcome == "APPROVE",
            input_digest_sha256=input_digest,
            assessment_sha256=digest,
            reason_codes=reason_codes,
        )
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            updated = connection.execute(
                """
                UPDATE risk_assessments
                SET status = 'COMPLETED', assessment_sha256 = ?, outcome = ?
                WHERE authority_receipt_sha256 = ? AND ordinal = ?
                    AND status = 'STARTED' AND assessment_sha256 IS NULL
                """,
                (
                    digest,
                    assessment.outcome,
                    AUTHORITY_RECEIPT_SHA256,
                    ordinal,
                ),
            ).rowcount
            if updated != 1:
                connection.execute("ROLLBACK")
                raise TqqqEvidenceRiskMandateError("risk assessment state mismatch")
            consumption_updated = connection.execute(
                """
                UPDATE risk_consumptions SET assessment_count = ?
                WHERE authority_receipt_sha256 = ? AND consumption_id = ?
                    AND status = 'STARTED' AND assessment_count = ?
                """,
                (
                    ordinal,
                    AUTHORITY_RECEIPT_SHA256,
                    self._consumption_id,
                    ordinal - 1,
                ),
            ).rowcount
            if consumption_updated != 1:
                connection.execute("ROLLBACK")
                raise TqqqEvidenceRiskMandateError("risk consumption state mismatch")
            connection.execute("COMMIT")
        except TqqqEvidenceRiskMandateError:
            self._park_best_effort("ASSESSMENT_COMPLETION_FAILED")
            raise
        except sqlite3.Error as exc:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            self._park_best_effort("ASSESSMENT_COMPLETION_FAILED")
            raise TqqqEvidenceRiskMandateError("risk assessment completion failed") from exc
        finally:
            connection.close()
        self._assessments.append(normalized)
        return normalized

    def seal(self, *, expected_decision_count: int) -> dict[str, object]:
        if self._candidate is None or self._consumption_id is None:
            raise TqqqEvidenceRiskMandateError("risk consumption not started")
        if self._sealed is not None:
            if expected_decision_count != len(self._assessments):
                raise TqqqEvidenceRiskMandateError("risk assessment count mismatch")
            return dict(self._sealed)
        if (
            type(expected_decision_count) is not int
            or expected_decision_count <= 0
            or expected_decision_count != len(self._assessments)
        ):
            self._park_best_effort("SEAL_VALIDATION_FAILED")
            raise TqqqEvidenceRiskMandateError("risk assessment count mismatch")
        chain_root = _digest(
            {
                "assessment_chain": [
                    {
                        "ordinal": ordinal,
                        "input_digest_sha256": item.input_digest_sha256,
                        "assessment_sha256": item.assessment_sha256,
                    }
                    for ordinal, item in enumerate(self._assessments, start=1)
                ]
            }
        )
        receipt = {
            "schema_version": "qsl.tqqq-evidence-risk-consumption.v1",
            "status": "PASS",
            "authority_receipt_sha256": AUTHORITY_RECEIPT_SHA256,
            "authority_source_revision": AUTHORITY_SOURCE_REVISION,
            "qpk_source_revision": QPK_SOURCE_REVISION,
            "consumption_id": self._consumption_id,
            "candidate_identity_sha256": self._candidate.candidate_sha256,
            "assessment_count": len(self._assessments),
            "approved_count": sum(item.approved for item in self._assessments),
            "rejected_count": sum(not item.approved for item in self._assessments),
            "assessment_disposition": (
                "ALL_APPROVED"
                if all(item.approved for item in self._assessments)
                else "POLICY_REJECTED"
            ),
            "assessment_chain_sha256": chain_root,
            "canonical_completion_required": True,
            "consumption_semantics": "AT_MOST_ONCE_PER_CANONICAL_AUTHORITY_LEDGER",
            "consumption_ledger_id": (
                f"qsl-risk-authority-ledger-v1:{AUTHORITY_RECEIPT_SHA256}"
            ),
            "distributed_exactly_once": False,
            "execution_authorized": False,
            "no_order": True,
            "no_paper": True,
            "no_shadow": True,
            "no_live": True,
            "no_promotion_authority": True,
        }
        self._sealed = {
            **receipt,
            "consumption_receipt_sha256": _digest(receipt),
        }
        return dict(self._sealed)

    def complete(self) -> None:
        if self._sealed is None or self._consumption_id is None:
            raise TqqqEvidenceRiskMandateError("risk consumption is not sealed")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            updated = connection.execute(
                """
                UPDATE risk_consumptions
                SET status = 'COMPLETED', consumption_receipt_sha256 = ?
                WHERE authority_receipt_sha256 = ? AND consumption_id = ?
                    AND status = 'STARTED' AND assessment_count = ?
                """,
                (
                    self._sealed["consumption_receipt_sha256"],
                    AUTHORITY_RECEIPT_SHA256,
                    self._consumption_id,
                    len(self._assessments),
                ),
            ).rowcount
            if updated != 1:
                connection.execute("ROLLBACK")
                raise TqqqEvidenceRiskMandateError("risk consumption state mismatch")
            connection.execute("COMMIT")
        except TqqqEvidenceRiskMandateError:
            raise
        except sqlite3.Error as exc:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise TqqqEvidenceRiskMandateError("risk consumption completion failed") from exc
        finally:
            connection.close()


def load_tqqq_evidence_risk_mandate(
    *,
    authority_receipt_path: str | Path | None,
    authority_source_revision: str | None,
    consumption_store_path: str | Path | None,
    logical_evaluation_time: datetime | None = None,
) -> TqqqEvidenceRiskMandateSession:
    """Verify immutable authority without starting replay or consuming it."""

    if (
        not isinstance(authority_receipt_path, (str, Path))
        or not isinstance(authority_source_revision, str)
        or not isinstance(consumption_store_path, (str, Path))
    ):
        raise TqqqEvidenceRiskMandateError("missing evidence risk authority")
    receipt_path = Path(authority_receipt_path)
    source_revision = _revision(authority_source_revision, "authority source revision")
    store_path = Path(consumption_store_path)
    _validate_authority_receipt(receipt_path, source_revision)
    if store_path.absolute() != CANONICAL_AUTHORITY_LEDGER_PATH.absolute():
        raise TqqqEvidenceRiskMandateError("noncanonical consumption store")
    _validate_store_path(store_path)
    _validate_qpk_revision()
    return TqqqEvidenceRiskMandateSession(
        consumption_store_path=store_path,
        logical_evaluation_time=_logical_time(logical_evaluation_time),
        verification_token=_VERIFIED_SESSION_TOKEN,
    )


__all__ = [
    "AUTHORITY_RECEIPT_SHA256",
    "AUTHORITY_SOURCE_REVISION",
    "CANONICAL_AUTHORITY_LEDGER_PATH",
    "QPK_SOURCE_REVISION",
    "TqqqEvidenceRiskAssessment",
    "TqqqEvidenceRiskMandateError",
    "load_tqqq_evidence_risk_mandate",
]
