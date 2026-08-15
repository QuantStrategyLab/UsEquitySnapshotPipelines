#!/usr/bin/env python3
"""Write TQQQ P3 evidence from one preserved immutable snapshot."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.tqqq_core_only_p1_binding import (
    verify_tqqq_core_only_input_root,
)
from us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence import (
    TqqqPromotionEvidenceError,
    run_tqqq_promotion_evidence,
)

_SOURCE_COMMIT = "6f346ac1b4fbff7b3d190b8c86d2d6701346e3a2"
_FAILURE_CLASSIFICATIONS = {
    "InputValidationError": ("input_validation_failure", "input_validation"),
    "InvalidResearchInputEvidence": ("input_validation_failure", "input_validation"),
    "ConfigContractError": ("config_contract_failure", "config_contract"),
    "OrchestratorContractError": ("orchestrator_contract_failure", "orchestrator_contract"),
    "TqqqPromotionContractError": ("orchestrator_contract_failure", "orchestrator_contract"),
    "RiskContractError": ("risk_contract_failure", "risk_contract"),
    "EvidenceValidationError": ("evidence_validation_failure", "evidence_validation"),
    "TqqqPromotionEvidenceError": ("evidence_validation_failure", "evidence_validation"),
    "RuntimeInternalError": ("runtime_internal_failure", "runtime_internal"),
    "TqqqOfflineReplayRuntimeError": ("runtime_internal_failure", "runtime_internal"),
}


class _SanitizedParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise ValueError("invalid arguments")


def _arguments(argv: list[str]) -> argparse.Namespace:
    parser = _SanitizedParser(add_help=False)
    parser.add_argument("--snapshot-root", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--mandate-receipt-sha256", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args(argv)


def _read_json(path: Path) -> object:
    try:
        return json.loads(path.read_bytes())
    except (OSError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid immutable snapshot") from exc


def _snapshot_payload(snapshot_root: Path) -> dict[str, object]:
    return {
        "binding": _read_json(snapshot_root / "binding.json"),
        "input_manifest": _read_json(snapshot_root / "manifest.json"),
        "bars": _read_json(snapshot_root / "bars.json"),
    }


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
    try:
        args = _arguments(list(sys.argv[1:] if argv is None else argv))
        verify_tqqq_core_only_input_root(args.snapshot_root)
        input_payload = _snapshot_payload(args.snapshot_root)
        stage = "config_contract"
        config_payload = _read_json(args.config)
        stage = "orchestrator_contract"
        replay_started = True
        result = run_tqqq_promotion_evidence(
            input_payload=input_payload,
            config_payload=config_payload,
            mandate_receipt_sha256=args.mandate_receipt_sha256,
            output_dir=args.output_dir,
        )
    except (OSError, RuntimeError, TypeError, ValueError, TqqqPromotionEvidenceError) as error:
        print(json.dumps(_failure_payload(error, stage=stage, replay_started=replay_started), sort_keys=True))
        return 2
    print(
        json.dumps(
            {
                "evidence_sha256": result["evidence_sha256"],
                "status": "EVIDENCE_V2_COMPLETE",
                "verdict": result["verdict"],
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
