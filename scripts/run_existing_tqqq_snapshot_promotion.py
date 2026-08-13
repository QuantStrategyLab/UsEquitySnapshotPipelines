#!/usr/bin/env python3
"""Run one existing immutable TQQQ snapshot without any provider path."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if __package__:
    from scripts.run_existing_tqqq_snapshot_diagnostic import (
        _RUNNER_PROJECT_ROOT,
        _current_runtime_identity,
        _load_execution_binding,
        _require_filevault,
    )
else:
    from run_existing_tqqq_snapshot_diagnostic import (  # type: ignore[import-not-found]
        _RUNNER_PROJECT_ROOT,
        _current_runtime_identity,
        _load_execution_binding,
        _require_filevault,
    )
from us_equity_snapshot_pipelines.lifecycle.tqqq_acquisition_orchestration import (
    TqqqOrchestrationError,
    orchestrate_existing_tqqq_snapshot_promotion,
)

_LOCAL_RESEARCH_ROOT = (
    Path.home() / ".local/share/qsl/tqqq-existing-snapshot-promotion-reruns"
)


class _SanitizedParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise ValueError("invalid arguments")


def _arguments(argv: list[str]) -> argparse.Namespace:
    parser = _SanitizedParser(add_help=False)
    parser.add_argument("--execution-terminal", required=True, type=Path)
    parser.add_argument("--execution-terminal-sha256", required=True)
    parser.add_argument("--risk-standard-id", required=True)
    parser.add_argument("--risk-standard-sha256", required=True)
    parser.add_argument("--platform-execution-revision", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    terminal = {
        "asset_count": 0,
        "evidence_digest": None,
        "mandate_receipt_digest": None,
        "rerun_count": 0,
        "snapshot_digest": None,
        "status": "FAILED_MATERIAL",
    }
    try:
        args = _arguments(list(sys.argv[1:] if argv is None else argv))
        _require_filevault()
        (
            run_root,
            authority,
            snapshot_digest,
            source_mandate_receipt_digest,
            execution_revision,
            execution_tree_sha,
            session_class,
        ) = _load_execution_binding(
            args.execution_terminal,
            args.execution_terminal_sha256,
            risk_standard_id=args.risk_standard_id,
            risk_standard_sha256=args.risk_standard_sha256,
            platform_execution_revision=args.platform_execution_revision,
        )
        terminal["snapshot_digest"] = snapshot_digest
        runner_revision, runner_tree_sha = _current_runtime_identity()
        terminal = orchestrate_existing_tqqq_snapshot_promotion(
            run_root,
            expected_snapshot_digest=snapshot_digest,
            expected_source_mandate_receipt_digest=source_mandate_receipt_digest,
            authority=authority,
            output_root=_LOCAL_RESEARCH_ROOT / authority.authority_receipt_sha256,
            execution_revision=execution_revision,
            execution_tree_sha=execution_tree_sha,
            runner_revision=runner_revision,
            runner_tree_sha=runner_tree_sha,
            session_class=session_class,
            source_checkout=_RUNNER_PROJECT_ROOT,
        )
    except TqqqOrchestrationError as exc:
        if exc.sanitized_failure is not None:
            failure = dict(exc.sanitized_failure)
            terminal["snapshot_digest"] = failure["snapshot_digest"]
            terminal["mandate_receipt_digest"] = failure["mandate_receipt_digest"]
            terminal["orchestration_failure"] = failure
    except Exception:  # noqa: BLE001, S110 - terminal output must remain sanitized
        pass
    print(json.dumps(terminal, sort_keys=True, separators=(",", ":")))
    return (
        0
        if terminal["status"]
        == "VALIDATED_EVIDENCE_V2_AWAITING_HUMAN_PROMOTION_ACCEPTANCE"
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
