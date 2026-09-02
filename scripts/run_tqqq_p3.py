#!/usr/bin/env python3
"""Write TQQQ P3 evidence from one preserved immutable snapshot."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import tempfile
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.tqqq_core_only_free_ohlcv_p1 import (
    verify_tqqq_core_only_free_ohlcv_p1_input_root,
)
from us_equity_snapshot_pipelines.lifecycle.tqqq_core_only_p1_binding import (
    P2_V2_CANDIDATE_ID,
    P2_V7_CONTRACT,
    P2_V8_CONTRACT,
    P2_V9_CONTRACT,
    resolve_tqqq_core_only_candidate_contract,
    verify_tqqq_core_only_input_root,
)
from us_equity_snapshot_pipelines.lifecycle.tqqq_p3_evidence_index import (
    P3_STATUS,
    validate_tqqq_p3_result,
)
from us_equity_snapshot_pipelines.lifecycle.tqqq_p3_v7_evidence_index import (
    validate_tqqq_p3_v7_result,
)
from us_equity_snapshot_pipelines.lifecycle.tqqq_evidence_risk_mandate import (
    TqqqEvidenceRiskMandateError,
    TqqqEvidenceRiskMandateSession,
    load_tqqq_evidence_risk_mandate,
)
from us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence import (
    _sync_directory,
    run_tqqq_promotion_evidence,
)
from us_equity_snapshot_pipelines.tqqq_r1_snapshot import _publish_noreplace

_SOURCE_COMMIT = "6f346ac1b4fbff7b3d190b8c86d2d6701346e3a2"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_COMPLETED_EVIDENCE_FIELDS = frozenset(
    {
        "evidence_sha256",
        "promotion_result_sha256",
        "candidate_identity_sha256",
        "input_manifest_sha256",
        "verdict",
    }
)
_FAILURE_CLASSIFICATIONS = {
    "InputValidationError": ("input_validation_failure", "input_validation"),
    "InvalidResearchInputEvidence": ("input_validation_failure", "input_validation"),
    "ConfigContractError": ("config_contract_failure", "config_contract"),
    "OrchestratorContractError": ("orchestrator_contract_failure", "orchestrator_contract"),
    "TqqqPromotionContractError": ("orchestrator_contract_failure", "orchestrator_contract"),
    "RiskContractError": ("risk_contract_failure", "risk_contract"),
    "TqqqEvidenceRiskMandateError": ("risk_contract_failure", "risk_contract"),
    "EvidenceValidationError": ("evidence_validation_failure", "evidence_validation"),
    "TqqqPromotionEvidenceError": ("evidence_validation_failure", "evidence_validation"),
    "RuntimeInternalError": ("runtime_internal_failure", "runtime_internal"),
    "TqqqOfflineReplayRuntimeError": ("runtime_internal_failure", "runtime_internal"),
}


class OrchestratorContractError(ValueError):
    """The offline evidence producer did not return a bound P3 completion."""


class ConfigContractError(ValueError):
    """The frozen candidate is not eligible for this P3 replay route."""


class _SanitizedParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise ValueError("invalid arguments")


def _arguments(argv: list[str]) -> argparse.Namespace:
    parser = _SanitizedParser(add_help=False)
    parser.add_argument("--snapshot-root", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--mandate-receipt-sha256", required=True)
    parser.add_argument("--risk-authority-receipt", type=Path)
    parser.add_argument("--risk-authority-source-revision")
    parser.add_argument("--risk-consumption-store", type=Path)
    parser.add_argument("--logical-evaluation-time", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args(argv)


def _read_json(path: Path) -> object:
    try:
        return json.loads(path.read_bytes())
    except (OSError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid immutable snapshot") from exc


def _logical_evaluation_time(value: object) -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        raise TqqqEvidenceRiskMandateError("invalid logical evaluation time")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise TqqqEvidenceRiskMandateError("invalid logical evaluation time") from exc
    if (
        parsed.tzinfo is None
        or parsed.utcoffset() != timedelta(0)
        or parsed.microsecond != 0
        or parsed.astimezone(UTC).isoformat().replace("+00:00", "Z") != value
    ):
        raise TqqqEvidenceRiskMandateError("invalid logical evaluation time")
    return parsed.astimezone(UTC)


def _snapshot_payload(snapshot_root: Path) -> dict[str, object]:
    payload: dict[str, object] = {
        "binding": _read_json(snapshot_root / "binding.json"),
        "input_manifest": _read_json(snapshot_root / "manifest.json"),
        "bars": _read_json(snapshot_root / "bars.json"),
    }
    assurance = snapshot_root / "assurance.json"
    if assurance.is_file():
        payload["assurance"] = _read_json(assurance)
    return payload


def _candidate_contract(config_payload: object):
    if not isinstance(config_payload, Mapping):
        raise ValueError("invalid frozen candidate")
    return resolve_tqqq_core_only_candidate_contract(config_payload.get("candidate_id"))


def _require_replayable_candidate(contract: object) -> None:
    """Keep the historical P2 v2 candidate from entering an impossible replay.

    P2 v2 predates the common BOXX availability window.  It is retained for
    source provenance, but cannot create a truthful P3 evidence package.
    """
    if getattr(contract, "candidate_id", None) == P2_V2_CANDIDATE_ID:
        raise ConfigContractError("historical TQQQ candidate is not replayable")


def _completed_evidence_summary(
    value: object, *, expected_input_manifest_sha256: str, candidate_id: str
) -> dict[str, str]:
    """Accept a replay success only when it stays bound to the verified P1 root."""
    required_fields = (
        _COMPLETED_EVIDENCE_FIELDS
        | {"relative_benchmark_policy_sha256"}
        if candidate_id
        in {
            P2_V7_CONTRACT.candidate_id,
            P2_V8_CONTRACT.candidate_id,
            P2_V9_CONTRACT.candidate_id,
        }
        else _COMPLETED_EVIDENCE_FIELDS
    )
    if (
        not isinstance(value, Mapping)
        or set(value) != required_fields
        or not isinstance(value["input_manifest_sha256"], str)
        or value["input_manifest_sha256"] != expected_input_manifest_sha256
        or any(
            not isinstance(value[field], str) or not _DIGEST.fullmatch(value[field])
            for field in (
                "evidence_sha256",
                "promotion_result_sha256",
                "candidate_identity_sha256",
                "input_manifest_sha256",
            )
        )
    ):
        raise OrchestratorContractError("invalid bound P3 completion")
    try:
        if candidate_id in {
            P2_V7_CONTRACT.candidate_id,
            P2_V8_CONTRACT.candidate_id,
            P2_V9_CONTRACT.candidate_id,
        }:
            result = validate_tqqq_p3_v7_result(
                {
                    "evidence_sha256": value["evidence_sha256"],
                    "promotion_result_sha256": value["promotion_result_sha256"],
                    "relative_benchmark_policy_sha256": value[
                        "relative_benchmark_policy_sha256"
                    ],
                    "status": P3_STATUS,
                    "verdict": value["verdict"],
                }
            )
            return {
                "evidence_sha256": result["evidence_sha256"],
                "promotion_result_sha256": result["promotion_result_sha256"],
                "relative_benchmark_policy_sha256": result[
                    "relative_benchmark_policy_sha256"
                ],
                "verdict": result["verdict"],
            }
        return validate_tqqq_p3_result(
            {
                "evidence_sha256": value["evidence_sha256"],
                "status": P3_STATUS,
                "verdict": value["verdict"],
            }
        )
    except ValueError as exc:
        raise OrchestratorContractError("invalid bound P3 completion") from exc


def _failure_payload(error: Exception, *, stage: str, replay_started: bool) -> dict[str, object]:
    failure_class, stage = _FAILURE_CLASSIFICATIONS.get(
        type(error).__name__,
        (
            "input_validation_failure"
            if stage == "input_validation"
            else "config_contract_failure"
            if stage == "config_contract"
            else "runtime_internal_failure",
            stage,
        ),
    )
    return {
        "complete_evidence": False,
        "failure_class": failure_class,
        "replay_started": replay_started,
        "source_commit": _SOURCE_COMMIT,
        "stage": stage,
        "status": "PARKED",
    }


def main(argv: list[str] | None = None) -> int:
    stage = "input_validation"
    replay_started = False
    risk_mandate_session: TqqqEvidenceRiskMandateSession | None = None
    temporary_output: Path | None = None
    try:
        args = _arguments(list(sys.argv[1:] if argv is None else argv))
        stage = "config_contract"
        config_payload = _read_json(args.config)
        contract = _candidate_contract(config_payload)
        _require_replayable_candidate(contract)
        stage = "risk_contract"
        logical_evaluation_time = _logical_evaluation_time(
            args.logical_evaluation_time
        )
        risk_mandate_session = load_tqqq_evidence_risk_mandate(
            authority_receipt_path=args.risk_authority_receipt,
            authority_source_revision=args.risk_authority_source_revision,
            consumption_store_path=args.risk_consumption_store,
            logical_evaluation_time=logical_evaluation_time,
        )
        if (
            type(risk_mandate_session) is not TqqqEvidenceRiskMandateSession
            or risk_mandate_session.is_verified is not True
        ):
            raise TqqqEvidenceRiskMandateError("unverified evidence risk session")
        stage = "input_validation"
        manifest_sha256 = (
            verify_tqqq_core_only_free_ohlcv_p1_input_root(
                args.snapshot_root, contract=contract
            )
            if contract in {P2_V8_CONTRACT, P2_V9_CONTRACT}
            else verify_tqqq_core_only_input_root(args.snapshot_root, contract=contract)
        )
        input_payload = _snapshot_payload(args.snapshot_root)
        if args.output_dir.exists() or args.output_dir.is_symlink():
            raise OrchestratorContractError("fresh evidence output is required")
        args.output_dir.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary_output = Path(
            tempfile.mkdtemp(
                prefix=f".{args.output_dir.name}.", dir=args.output_dir.parent
            )
        )
        stage = "orchestrator_contract"
        replay_started = True
        result = _completed_evidence_summary(
            run_tqqq_promotion_evidence(
                input_payload=input_payload,
                config_payload=config_payload,
                mandate_receipt_sha256=args.mandate_receipt_sha256,
                risk_mandate_session=risk_mandate_session,
                generated_at=args.logical_evaluation_time,
                defer_risk_completion=True,
                output_dir=temporary_output,
            ),
            expected_input_manifest_sha256=manifest_sha256,
            candidate_id=contract.candidate_id,
        )
        risk_mandate_session.complete()
        _publish_noreplace(temporary_output, args.output_dir)
        _sync_directory(args.output_dir.parent)
        temporary_output = None
    except Exception as error:
        if risk_mandate_session is not None:
            try:
                risk_mandate_session.park("CLI_EVIDENCE_FAILED")
            except TqqqEvidenceRiskMandateError:
                pass
        if temporary_output is not None and temporary_output.exists():
            try:
                shutil.rmtree(temporary_output)
            except OSError:
                pass
        print(json.dumps(_failure_payload(error, stage=stage, replay_started=replay_started), sort_keys=True))
        return 2
    print(
        json.dumps(
            {
                "evidence_sha256": result["evidence_sha256"],
                "status": "EVIDENCE_V2_COMPLETE",
                "verdict": result["verdict"],
                **(
                    {
                        "promotion_result_sha256": result[
                            "promotion_result_sha256"
                        ],
                        "relative_benchmark_policy_sha256": result[
                            "relative_benchmark_policy_sha256"
                        ]
                    }
                    if contract.candidate_id
                    in {
                        P2_V7_CONTRACT.candidate_id,
                        P2_V8_CONTRACT.candidate_id,
                        P2_V9_CONTRACT.candidate_id,
                    }
                    else {}
                ),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
