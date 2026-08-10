"""One-shot SOXL acquisition-to-evidence orchestration over frozen contracts."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from quant_platform_kit.data.research_mandate import ResearchMandateAuthorityGuard
from quant_platform_kit.ibkr import StrictAdjustedHistoryResult
from quant_platform_kit.risk.contracts import CandidateRiskIdentity
from quant_platform_kit.strategy_lifecycle.evidence_package_v2 import (
    validate_evidence_package_v2,
)
from us_equity_strategies.manifests import soxl_soxx_trend_income_manifest

from . import soxl_promotion_runner as promotion_runner
from .soxl_pit_input_packager import (
    FIRST_ELIGIBLE_SESSION,
    FROZEN_XNYS_SESSIONS,
    INPUT_CONTRACT_ID,
    MANDATE_ID,
    QPK_REVISION,
    SOXL_PROMOTION_ASSETS,
    canonical_json_bytes,
    prepare_soxl_pit_input,
    publish_soxl_pit_input,
)
from .soxl_pit_regime_component_producer import (
    CANDIDATE_ID,
    CORE_ONLY_CONFIG_SHA256,
    FIXED_CUTOFF,
    SOURCE_CONTRACT_SCHEMA,
    UNAVAILABLE_COMPONENTS,
    runtime_producer_source_identity,
)
from .soxl_promotion_runner import SoxlPromotionContractError, run_soxl_promotion_research

EXACT_DURATIONS = {
    "SOXL": "9 Y",
    "SOXX": "9 Y",
    "BOXX": "4 Y",
    "SCHD": "9 Y",
    "DGRO": "9 Y",
    "SGOV": "7 Y",
    "SPYI": "4 Y",
    "QQQI": "3 Y",
    "QQQ": "9 Y",
}
OFFICIAL_IBAPI_PROVENANCE_SHA256 = (
    "dcfffc68992d46081a611d100ef6e7c74fccfbb5621295147cba29ef767318f0"
)
_SESSION_PROVIDER_ID = {
    "paper": "IBKR_PAPER_GATEWAY",
    "live-data-only": "IBKR_LIVE_GATEWAY_DATA_ONLY",
}
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_REVISION = re.compile(r"^[0-9a-f]{40}$")


class SoxlOrchestrationError(ValueError):
    """Sanitized fail-closed error for the concrete one-shot orchestration."""


def _utc_timestamp(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise SoxlOrchestrationError("invalid orchestration timestamp")
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_utc_timestamp(value: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise SoxlOrchestrationError("invalid orchestration timestamp")
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise SoxlOrchestrationError("invalid orchestration timestamp") from exc
    if parsed.tzinfo != UTC:
        raise SoxlOrchestrationError("invalid orchestration timestamp")
    return parsed


def _require_digest(value: str, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise SoxlOrchestrationError(f"invalid {label}")
    return value


def _require_revision(value: str, label: str) -> str:
    if not isinstance(value, str) or not _REVISION.fullmatch(value):
        raise SoxlOrchestrationError(f"invalid {label}")
    return value


def _require_text(value: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise SoxlOrchestrationError(f"invalid {label}")
    return value


@dataclass(frozen=True)
class SoxlOrchestrationAuthority:
    """Safe external identities required by the existing source and risk contracts."""

    authority_receipt_sha256: str
    entitlement_receipt_sha256: str
    license_receipt_sha256: str
    retention_expires_at: str
    risk_standard_id: str
    risk_standard_sha256: str
    input_license: str
    input_usage_scope: str

    def __post_init__(self) -> None:
        _require_digest(self.authority_receipt_sha256, "authority receipt digest")
        _require_digest(self.entitlement_receipt_sha256, "entitlement receipt digest")
        _require_digest(self.license_receipt_sha256, "license receipt digest")
        _parse_utc_timestamp(self.retention_expires_at)
        _require_text(self.risk_standard_id, "risk standard identity")
        _require_digest(self.risk_standard_sha256, "risk standard digest")
        _require_text(self.input_license, "input license")
        _require_text(self.input_usage_scope, "input usage scope")


def resolve_soxl_runtime_identity() -> tuple[str, str]:
    """Resolve one clean current checkout before any provider contact."""
    revision = promotion_runner._resolve_runner_revision()
    repository = Path(__file__).resolve().parents[3]
    try:
        status = subprocess.run(
            ["git", "-C", str(repository), "status", "--porcelain", "--untracked-files=normal"],
            check=True,
            capture_output=True,
            text=True,
        )
        head = subprocess.run(
            ["git", "-C", str(repository), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        tree = subprocess.run(
            ["git", "-C", str(repository), "rev-parse", "HEAD^{tree}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise SoxlOrchestrationError("runner implementation identity is unavailable") from exc
    if status.stdout or head != revision:
        raise SoxlOrchestrationError("runner implementation checkout is not immutable")
    return _require_revision(revision, "runner revision"), _require_revision(tree, "runner tree")


def _strict_raw_sessions(
    results: Mapping[str, StrictAdjustedHistoryResult],
) -> list[dict[str, Any]]:
    if not isinstance(results, Mapping) or tuple(results) != SOXL_PROMOTION_ASSETS:
        raise SoxlOrchestrationError("exact nine-input result is required")
    candles_by_symbol: dict[str, dict[str, Any]] = {}
    for symbol in SOXL_PROMOTION_ASSETS:
        result = results[symbol]
        if not isinstance(result, StrictAdjustedHistoryResult):
            raise SoxlOrchestrationError("exact nine-input result is required")
        expected_sessions = tuple(
            value
            for value in FROZEN_XNYS_SESSIONS
            if value >= FIRST_ELIGIBLE_SESSION.get(symbol, FROZEN_XNYS_SESSIONS[0])
        )
        observed_sessions = tuple(candle.session.isoformat() for candle in result.candles)
        provenance = result.provenance
        diagnostic = result.diagnostic
        if (
            observed_sessions != expected_sessions
            or provenance.symbol != symbol
            or provenance.exchange != "SMART"
            or provenance.currency != "USD"
            or provenance.end_datetime != FIXED_CUTOFF
            or provenance.duration != EXACT_DURATIONS[symbol]
            or provenance.bar_size != "1 day"
            or provenance.what_to_show != "ADJUSTED_LAST"
            or provenance.use_rth is not True
            or provenance.format_date != 1
            or provenance.keep_up_to_date is not False
            or provenance.returned_row_count != len(expected_sessions)
            or diagnostic.classification != "exact_match"
            or diagnostic.completion_observed is not True
            or diagnostic.expected_count != len(expected_sessions)
            or diagnostic.observed_in_window_count != len(expected_sessions)
            or diagnostic.missing_count != 0
            or diagnostic.extra_count != 0
            or diagnostic.duplicate_count != 0
            or diagnostic.provider_error_code_counts
        ):
            raise SoxlOrchestrationError("strict history result identity mismatch")
        candles_by_symbol[symbol] = {
            candle.session.isoformat(): {
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
                "volume": candle.volume,
            }
            for candle in result.candles
        }
    return [
        {
            "date": session,
            "bars": {
                symbol: candles_by_symbol[symbol][session]
                for symbol in SOXL_PROMOTION_ASSETS
                if session in candles_by_symbol[symbol]
            },
        }
        for session in FROZEN_XNYS_SESSIONS
    ]


def _source_contract(
    raw_sessions: list[dict[str, Any]],
    results: Mapping[str, StrictAdjustedHistoryResult],
    *,
    session_class: str,
    authority: SoxlOrchestrationAuthority,
    runner_revision: str,
    runner_tree_sha: str,
    observed_at: str,
) -> dict[str, Any]:
    if session_class not in _SESSION_PROVIDER_ID:
        raise SoxlOrchestrationError("invalid IBKR session class")
    observed = _parse_utc_timestamp(observed_at)
    if _parse_utc_timestamp(authority.retention_expires_at) < observed:
        raise SoxlOrchestrationError("retention authority is expired")
    logical_inputs = []
    for symbol in SOXL_PROMOTION_ASSETS:
        result = results[symbol]
        payload = [
            {"date": session["date"], **session["bars"][symbol]}
            for session in raw_sessions
            if symbol in session["bars"]
        ]
        request_contract = {
            "session_class": session_class,
            "symbol": symbol,
            "exchange": result.provenance.exchange,
            "currency": result.provenance.currency,
            "end_datetime": "",
            "duration": result.provenance.duration,
            "bar_size": result.provenance.bar_size,
            "what_to_show": result.provenance.what_to_show,
            "use_rth": result.provenance.use_rth,
            "format_date": result.provenance.format_date,
            "keep_up_to_date": result.provenance.keep_up_to_date,
        }
        request_sha256 = hashlib.sha256(canonical_json_bytes(request_contract)).hexdigest()
        logical_inputs.append(
            {
                "logical_input_id": symbol,
                "provider_instrument_id": f"IBKR:SMART:STK:{symbol}:USD",
                "instrument_type": "etf",
                "venue": "SMART",
                "currency": "USD",
                "provider_id": _SESSION_PROVIDER_ID[session_class],
                "source_revision": OFFICIAL_IBAPI_PROVENANCE_SHA256,
                "field": "adjusted_ohlcv",
                "frequency": "1d",
                "timezone": "America/New_York",
                "calendar": "XNYS",
                "adjustment_contract": "total_return_adjusted",
                "corporate_action_basis": "provider_adjusted",
                "missing_value_policy": "reject",
                "data_origin": "provider_observed",
                "substitution_policy": "none",
                "entitlement_receipt_sha256": authority.entitlement_receipt_sha256,
                "license_or_usage_receipt_sha256": authority.license_receipt_sha256,
                "retention_scope": "filevault_local_encrypted_immutable_internal_research_only",
                "retention_expires_at": authority.retention_expires_at,
                "request_sha256": request_sha256,
                "observed_at": observed_at,
                "effective_at": observed_at,
                "fixed_cutoff": FIXED_CUTOFF,
                "content_sha256": hashlib.sha256(canonical_json_bytes(payload)).hexdigest(),
                "row_count": len(payload),
                "first_date": payload[0]["date"],
                "last_date": payload[-1]["date"],
                "no_future_rows": True,
            }
        )
    return {
        "schema_version": SOURCE_CONTRACT_SCHEMA,
        "data_class": "provider_observed",
        "observed_at": observed_at,
        "effective_at": observed_at,
        "as_of": observed_at,
        "fixed_cutoff": FIXED_CUTOFF,
        "input_content_sha256": hashlib.sha256(canonical_json_bytes(raw_sessions)).hexdigest(),
        "calendar": {
            "calendar_id": "XNYS",
            "timezone": "America/New_York",
            "source": "uesp_repo_local_xnys_holiday_rules",
            "source_revision": "soxl_pit_input_packager.v1",
            "first_session": FROZEN_XNYS_SESSIONS[0],
            "last_session": FROZEN_XNYS_SESSIONS[-1],
            "session_count": len(FROZEN_XNYS_SESSIONS),
            "sessions_sha256": hashlib.sha256(
                canonical_json_bytes(list(FROZEN_XNYS_SESSIONS))
            ).hexdigest(),
        },
        "producer": runtime_producer_source_identity(
            commit_sha=runner_revision,
            tree_sha=runner_tree_sha,
        ),
        "candidate_id": CANDIDATE_ID,
        "candidate_contract_sha256": CORE_ONLY_CONFIG_SHA256,
        "market_regime_control_enabled": False,
        "unavailable_components": {
            component: {"enabled": False, "available": False}
            for component in UNAVAILABLE_COMPONENTS
        },
        "logical_inputs": logical_inputs,
    }


def _config_without_authority(
    *,
    source_contract_sha256: str,
    runner_revision: str,
    authority: SoxlOrchestrationAuthority,
) -> dict[str, Any]:
    return {
        "schema_version": "soxl_p3_core_only_9_input_config.v1",
        "candidate_id": CANDIDATE_ID,
        "input_contract_id": INPUT_CONTRACT_ID,
        "source_contract_schema": SOURCE_CONTRACT_SCHEMA,
        "source_contract_sha256": source_contract_sha256,
        "candidate_contract_sha256": CORE_ONLY_CONFIG_SHA256,
        "market_regime_control_enabled": False,
        "benchmark_symbol": "SOXX",
        "substitution_policy": "none_no_proxy_no_alias",
        "position_control_allowed": False,
        "strategy_profile": "soxl_soxx_trend_income",
        "domain": "us_equity",
        "account_mode": "single_strategy",
        "strategy_revision": promotion_runner._UES_REVISION,
        "runner_revision": runner_revision,
        "qpk_revision": QPK_REVISION,
        "frozen_strategy_config": json.loads(
            canonical_json_bytes(soxl_soxx_trend_income_manifest.default_config)
        ),
        "availability_contract": json.loads(
            canonical_json_bytes(promotion_runner._FROZEN_AVAILABILITY_CONTRACT)
        ),
        "ordered_variants": list(promotion_runner._ORDERED_VARIANTS),
        "initial_equity": 100_000.0,
        "initial_weights": {},
        "stop_loss_distance": 0.05,
        "purge_sessions": 20,
        "embargo_sessions": 20,
        "folds": [
            {
                "train_start": train_start,
                "train_end": train_end,
                "test_start": test_start,
                "test_end": test_end,
            }
            for train_start, train_end, test_start, test_end in promotion_runner._FROZEN_FOLDS
        ],
        "locked_oos": {
            "start": promotion_runner._FROZEN_LOCKED_OOS[0],
            "end": promotion_runner._FROZEN_LOCKED_OOS[1],
        },
        "risk_standard_id": authority.risk_standard_id,
        "risk_standard_sha256": authority.risk_standard_sha256,
        "input_license": authority.input_license,
        "input_usage_scope": authority.input_usage_scope,
        "learning_only": False,
        "promotion_eligible": False,
        "live_ready": False,
        "size_zero_required": True,
        "no_order": True,
    }


def _private_run_root(output_root: Path, input_manifest_sha256: str) -> Path:
    if output_root.is_symlink():
        raise SoxlOrchestrationError("private output root is unavailable")
    output_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    os.chmod(output_root, 0o700)
    run_root = output_root / input_manifest_sha256
    try:
        run_root.mkdir(mode=0o700)
    except OSError as exc:
        raise SoxlOrchestrationError("content-addressed output already exists") from exc
    return run_root


def _seal_private_tree(root: Path) -> None:
    for path in root.rglob("*"):
        if path.is_symlink():
            raise SoxlOrchestrationError("private output symlink is forbidden")
        os.chmod(path, 0o700 if path.is_dir() else 0o600)
    os.chmod(root, 0o700)


def orchestrate_soxl_promotion(
    results: Mapping[str, StrictAdjustedHistoryResult],
    *,
    authority: SoxlOrchestrationAuthority,
    output_root: str | Path,
    runner_revision: str,
    runner_tree_sha: str,
    session_class: str = "paper",
    clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Publish one exact snapshot, consume one nonce, then run one frozen rerun."""
    if not isinstance(authority, SoxlOrchestrationAuthority):
        raise SoxlOrchestrationError("invalid orchestration authority")
    runner_revision = _require_revision(runner_revision, "runner revision")
    runner_tree_sha = _require_revision(runner_tree_sha, "runner tree")
    now = (clock or (lambda: datetime.now(UTC)))()
    observed_at = _utc_timestamp(now)
    raw_sessions = _strict_raw_sessions(results)
    source_contract = _source_contract(
        raw_sessions,
        results,
        session_class=session_class,
        authority=authority,
        runner_revision=runner_revision,
        runner_tree_sha=runner_tree_sha,
        observed_at=observed_at,
    )
    trusted_source_contract_sha256 = hashlib.sha256(
        canonical_json_bytes(source_contract)
    ).hexdigest()
    prepared = prepare_soxl_pit_input(
        raw_sessions,
        source_contract,
        trusted_regime_source_contract_sha256=trusted_source_contract_sha256,
    )
    config_without_authority = _config_without_authority(
        source_contract_sha256=prepared.source_contract_sha256,
        runner_revision=runner_revision,
        authority=authority,
    )
    config_digest = hashlib.sha256(canonical_json_bytes(config_without_authority)).hexdigest()
    candidate = CandidateRiskIdentity(
        strategy_profile="soxl_soxx_trend_income",
        account_mode="single_strategy",
        strategy_revision=promotion_runner._UES_REVISION,
        runner_revision=runner_revision,
        config_sha256=config_digest,
        input_manifest_sha256=prepared.input_manifest_sha256,
        authority_receipt_sha256=authority.authority_receipt_sha256,
    )
    run_root = _private_run_root(Path(output_root), prepared.input_manifest_sha256)
    guard = ResearchMandateAuthorityGuard(run_root / "mandate-authority.sqlite3", clock=lambda: now)
    mandate = guard.issue(
        candidate_id=candidate.candidate_sha256,
        mandate_id=MANDATE_ID,
        config_digest=config_digest,
        input_digest=prepared.input_manifest_sha256,
        authority_id=authority.authority_receipt_sha256,
    )
    factors = {symbol: 3 if symbol == "SOXL" else 1 for symbol in SOXL_PROMOTION_ASSETS}
    caps = {symbol: 0.15 if symbol == "SOXL" else 0.50 for symbol in SOXL_PROMOTION_ASSETS}
    mandate_provenance = {
        "mandate_id": MANDATE_ID,
        "mandate_version": "v1",
        "authority_receipt_sha256": authority.authority_receipt_sha256,
        "authority_scope": "RESEARCH_ONLY",
        "strategy_profile": candidate.strategy_profile,
        "account_mode": candidate.account_mode,
        "strategy_revision": candidate.strategy_revision,
        "runner_revision": candidate.runner_revision,
        "config_sha256": candidate.config_sha256,
        "input_manifest_sha256": candidate.input_manifest_sha256,
        "candidate_identity_sha256": candidate.candidate_sha256,
        "effective_at": mandate.issued_at,
        "expires_at": mandate.expires_at,
        "max_snapshot_age_seconds": 300,
        "effective_exposure_cap": 0.50,
        "loss_budget": 0.01,
        "product_caps": caps,
        "nominal_caps": caps,
        "product_leverage_factors": factors,
        "allowed_nonzero_assets": list(SOXL_PROMOTION_ASSETS),
        "source_revision": QPK_REVISION,
        "research_mandate_digest": mandate.mandate_digest,
    }
    config = {
        **config_without_authority,
        "candidate_identity": {
            "strategy_profile": candidate.strategy_profile,
            "account_mode": candidate.account_mode,
            "strategy_revision": candidate.strategy_revision,
            "runner_revision": candidate.runner_revision,
            "config_sha256": candidate.config_sha256,
            "input_manifest_sha256": candidate.input_manifest_sha256,
            "authority_receipt_sha256": candidate.authority_receipt_sha256,
        },
        "mandate_provenance": mandate_provenance,
    }
    binding = {
        "candidate_id": CANDIDATE_ID,
        "input_contract_id": INPUT_CONTRACT_ID,
        "source_contract_schema": SOURCE_CONTRACT_SCHEMA,
        "source_contract_sha256": prepared.source_contract_sha256,
        "candidate_contract_sha256": CORE_ONLY_CONFIG_SHA256,
        "strategy_profile": candidate.strategy_profile,
        "account_mode": candidate.account_mode,
        "strategy_revision": candidate.strategy_revision,
        "runner_revision": candidate.runner_revision,
        "qpk_revision": QPK_REVISION,
        "config_sha256": candidate.config_sha256,
        "input_manifest_sha256": candidate.input_manifest_sha256,
        "authority_receipt_sha256": candidate.authority_receipt_sha256,
        "candidate_identity_sha256": candidate.candidate_sha256,
        "mandate_id": MANDATE_ID,
        "mandate_digest_sha256": mandate.mandate_digest,
    }
    old_umask = os.umask(0o077)
    try:
        snapshot = publish_soxl_pit_input(prepared, binding, run_root / "snapshot")
        consumption = guard.consume(
            mandate,
            candidate_id=candidate.candidate_sha256,
            mandate_id=MANDATE_ID,
            config_digest=config_digest,
            input_digest=prepared.input_manifest_sha256,
            authority_id=authority.authority_receipt_sha256,
        )
        mandate_provenance["research_mandate_consumption_receipt_sha256"] = (
            consumption.receipt_digest
        )
        evidence_root = run_root / "evidence"
        try:
            run_result = run_soxl_promotion_research(
                input_payload=json.loads(prepared.input_bytes),
                config_payload=config,
                output_dir=evidence_root,
                generated_at=observed_at,
            )
        except SoxlPromotionContractError as exc:
            result_path = evidence_root / "promotion-research-result.v1.json"
            if result_path.is_file():
                terminal = json.loads(result_path.read_bytes())
                if terminal.get("status") in {"FAIL", "PROXY_SENSITIVE"}:
                    _seal_private_tree(run_root)
                    return {
                        "status": "IMMUTABLE_NEGATIVE_STRATEGY_EVIDENCE",
                        "asset_count": len(SOXL_PROMOTION_ASSETS),
                        "snapshot_digest": snapshot["package_manifest_sha256"],
                        "evidence_digest": None,
                        "mandate_receipt_digest": consumption.receipt_digest,
                        "rerun_count": 1,
                    }
            raise SoxlOrchestrationError("promotion rerun failed") from exc
        evidence_path = evidence_root / "strategy-evidence-package.v2.json"
        evidence_bytes = evidence_path.read_bytes()
        evidence = json.loads(evidence_bytes)
        if (
            not isinstance(evidence, dict)
            or validate_evidence_package_v2(evidence, base_dir=evidence_root)
            or hashlib.sha256(evidence_bytes).hexdigest() != run_result["evidence_sha256"]
        ):
            raise SoxlOrchestrationError("evidence package validation failed")
        _seal_private_tree(run_root)
        return {
            "status": "VALIDATED_EVIDENCE_V2_AWAITING_HUMAN_PROMOTION_ACCEPTANCE",
            "asset_count": len(SOXL_PROMOTION_ASSETS),
            "snapshot_digest": snapshot["package_manifest_sha256"],
            "evidence_digest": run_result["evidence_sha256"],
            "mandate_receipt_digest": consumption.receipt_digest,
            "rerun_count": 1,
        }
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise SoxlOrchestrationError("promotion orchestration failed") from exc
    finally:
        os.umask(old_umask)


__all__ = [
    "EXACT_DURATIONS",
    "OFFICIAL_IBAPI_PROVENANCE_SHA256",
    "SoxlOrchestrationAuthority",
    "SoxlOrchestrationError",
    "orchestrate_soxl_promotion",
    "resolve_soxl_runtime_identity",
]
