#!/usr/bin/env python3
"""Run one existing immutable SOXL snapshot without any provider path."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.soxl_acquisition_orchestration import (
    SoxlOrchestrationAuthority,
    SoxlOrchestrationError,
    orchestrate_existing_soxl_snapshot,
    resolve_soxl_runtime_identity,
)

_LOCAL_RESEARCH_ROOT = Path.home() / ".local/share/qsl/soxl-promotion-snapshot-reruns"


class _SanitizedParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ValueError("invalid arguments")


def _arguments(argv: list[str]) -> tuple[Path, str, SoxlOrchestrationAuthority]:
    parser = _SanitizedParser(add_help=False)
    parser.add_argument("--snapshot", required=True, type=Path)
    parser.add_argument("--snapshot-digest", required=True)
    parser.add_argument("--authority-receipt-sha256", required=True)
    parser.add_argument("--entitlement-receipt-sha256", required=True)
    parser.add_argument("--license-receipt-sha256", required=True)
    parser.add_argument("--retention-expires-at", required=True)
    parser.add_argument("--risk-standard-id", required=True)
    parser.add_argument("--risk-standard-sha256", required=True)
    parser.add_argument("--input-license", required=True)
    parser.add_argument("--input-usage-scope", required=True)
    args = parser.parse_args(argv)
    authority = SoxlOrchestrationAuthority(
        authority_receipt_sha256=args.authority_receipt_sha256,
        entitlement_receipt_sha256=args.entitlement_receipt_sha256,
        license_receipt_sha256=args.license_receipt_sha256,
        retention_expires_at=args.retention_expires_at,
        risk_standard_id=args.risk_standard_id,
        risk_standard_sha256=args.risk_standard_sha256,
        input_license=args.input_license,
        input_usage_scope=args.input_usage_scope,
    )
    return args.snapshot, args.snapshot_digest, authority


def _require_filevault() -> None:
    try:
        status = subprocess.run(
            ["/usr/bin/fdesetup", "status"],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError("FileVault status is unavailable") from exc
    if status.stdout.strip() != "FileVault is On.":
        raise RuntimeError("FileVault is required")


def main(argv: list[str] | None = None) -> int:
    try:
        snapshot, snapshot_digest, authority = _arguments(
            list(sys.argv[1:] if argv is None else argv)
        )
    except (TypeError, ValueError):
        print(
            json.dumps(
                {
                    "asset_count": 0,
                    "evidence_digest": None,
                    "mandate_receipt_digest": None,
                    "rerun_count": 0,
                    "snapshot_digest": None,
                    "status": "INVALID_ARGUMENTS",
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 2

    terminal = {
        "asset_count": 0,
        "evidence_digest": None,
        "mandate_receipt_digest": None,
        "rerun_count": 0,
        "snapshot_digest": None,
        "status": "FAILED_MATERIAL",
    }
    try:
        _require_filevault()
        runner_revision, runner_tree_sha = resolve_soxl_runtime_identity()
        outcome = orchestrate_existing_soxl_snapshot(
            snapshot,
            expected_snapshot_digest=snapshot_digest,
            authority=authority,
            output_root=_LOCAL_RESEARCH_ROOT / authority.authority_receipt_sha256,
            runner_revision=runner_revision,
            runner_tree_sha=runner_tree_sha,
        )
        terminal.update(outcome)
    except SoxlOrchestrationError as exc:
        if exc.sanitized_failure is not None:
            failure = dict(exc.sanitized_failure)
            terminal["snapshot_digest"] = failure["snapshot_digest"]
            terminal["mandate_receipt_digest"] = failure["mandate_receipt_digest"]
            terminal["orchestration_failure"] = failure
    except Exception:  # noqa: BLE001 - terminal output must remain sanitized
        pass
    print(json.dumps(terminal, sort_keys=True, separators=(",", ":")))
    return 0 if terminal["status"] in {
        "VALIDATED_EVIDENCE_V2_AWAITING_HUMAN_PROMOTION_ACCEPTANCE",
        "IMMUTABLE_NEGATIVE_STRATEGY_EVIDENCE",
    } else 1


if __name__ == "__main__":
    raise SystemExit(main())
