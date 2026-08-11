#!/usr/bin/env python3
"""Replay one preserved TQQQ snapshot and persist only a sanitized diagnostic."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any

from us_equity_snapshot_pipelines.lifecycle.tqqq_acquisition_orchestration import (
    INPUT_LICENSE,
    INPUT_USAGE_SCOPE,
    TqqqOrchestrationAuthority,
    _canonical,
    _config,
    orchestrate_existing_tqqq_snapshot_diagnostic,
    resolve_tqqq_runtime_identity,
)

_LOCAL_RESEARCH_ROOT = Path.home() / ".local/share/qsl/tqqq-promotion-evidence-v2"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_REVISION = re.compile(r"^[0-9a-f]{40}$")
_EXCEPTION_CLASS = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_TERMINAL_FIELDS = {
    "config_digest",
    "exception_class",
    "function_identifiers",
    "mandate_receipt_digest",
    "runner_completion_count",
    "runner_invocation_count",
    "snapshot_digest",
    "stage",
}
_STAGES = {
    "preflight_validation_failed",
    "promotion_replay_completed",
    "promotion_replay_exception",
}


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
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args(argv)


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


def _private_json(path: Path, expected_sha256: str) -> dict[str, Any]:
    if not _DIGEST.fullmatch(expected_sha256):
        raise ValueError("invalid receipt digest")
    try:
        if (
            path.is_symlink()
            or not path.is_file()
            or stat.S_IMODE(path.stat().st_mode) != 0o600
        ):
            raise ValueError("invalid private receipt")
        payload = path.read_bytes()
        if hashlib.sha256(payload).hexdigest() != expected_sha256:
            raise ValueError("private receipt digest mismatch")
        result = json.loads(payload)
    except (OSError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("private receipt is unavailable") from exc
    if not isinstance(result, dict):
        raise TypeError("invalid private receipt")
    return result


def _load_execution_binding(
    execution_terminal: Path,
    execution_terminal_sha256: str,
    *,
    risk_standard_id: str,
    risk_standard_sha256: str,
    platform_execution_revision: str,
) -> tuple[Path, TqqqOrchestrationAuthority, str, str, str, str, str]:
    terminal = _private_json(execution_terminal, execution_terminal_sha256)
    immutable = terminal.get("immutable_result")
    authority_metadata = terminal.get("authority_metadata")
    repository = terminal.get("repository")
    execution = terminal.get("execution")
    safety = terminal.get("safety")
    if not all(
        isinstance(value, dict)
        for value in (immutable, authority_metadata, repository, execution, safety)
    ):
        raise ValueError("invalid execution binding")
    snapshot_digest = immutable.get("snapshot_digest")
    mandate_receipt_digest = immutable.get("mandate_receipt_digest")
    execution_revision = repository.get("runtime_head")
    execution_tree_sha = repository.get("runtime_tree")
    session_class = execution.get("session_class")
    retention_expires_at = authority_metadata.get("retention_expires_at")
    authority_receipt_sha256 = authority_metadata.get("human_authority_receipt_sha256")
    entitlement_receipt_sha256 = authority_metadata.get("entitlement_receipt_sha256")
    license_receipt_sha256 = authority_metadata.get("license_receipt_sha256")
    if (
        not isinstance(snapshot_digest, str)
        or not _DIGEST.fullmatch(snapshot_digest)
        or not isinstance(mandate_receipt_digest, str)
        or not _DIGEST.fullmatch(mandate_receipt_digest)
        or not isinstance(execution_revision, str)
        or not _REVISION.fullmatch(execution_revision)
        or not isinstance(execution_tree_sha, str)
        or not _REVISION.fullmatch(execution_tree_sha)
        or session_class != "live-data-only"
        or not isinstance(retention_expires_at, str)
        or not isinstance(authority_receipt_sha256, str)
        or not _DIGEST.fullmatch(authority_receipt_sha256)
        or not isinstance(entitlement_receipt_sha256, str)
        or not _DIGEST.fullmatch(entitlement_receipt_sha256)
        or not isinstance(license_receipt_sha256, str)
        or not _DIGEST.fullmatch(license_receipt_sha256)
        or authority_metadata.get("transaction_consumed") is not True
        or immutable.get("evidence_artifact_count") != 0
        or immutable.get("evidence_runner_invocation_count") != 1
        or immutable.get("evidence_runner_completion_count") != 0
        or immutable.get("provider_reacquisition") != 0
        or immutable.get("second_evidence_run") != 0
        or safety.get("no_order") is not True
        or safety.get("size_zero_required") is not True
        or safety.get("order_calls") != 0
        or safety.get("account_positions_funds_orders_executions_capital_calls") != 0
        or safety.get("raw_bars_dates_prices_volumes_provider_messages_logged") != 0
    ):
        raise ValueError("invalid execution binding")
    authority_receipt_path = authority_metadata.get("human_authority_receipt")
    if not isinstance(authority_receipt_path, str):
        raise TypeError("invalid authority receipt binding")
    _private_json(Path(authority_receipt_path), authority_receipt_sha256)
    authority = TqqqOrchestrationAuthority(
        authority_receipt_sha256=authority_receipt_sha256,
        entitlement_receipt_sha256=entitlement_receipt_sha256,
        license_receipt_sha256=license_receipt_sha256,
        retention_expires_at=retention_expires_at,
        risk_standard_id=risk_standard_id,
        risk_standard_sha256=risk_standard_sha256,
        platform_execution_revision=platform_execution_revision,
        input_license=INPUT_LICENSE,
        input_usage_scope=INPUT_USAGE_SCOPE,
    )
    return (
        _LOCAL_RESEARCH_ROOT / snapshot_digest,
        authority,
        snapshot_digest,
        mandate_receipt_digest,
        execution_revision,
        execution_tree_sha,
        session_class,
    )


def _validate_terminal(payload: dict[str, Any]) -> bytes:
    if set(payload) != _TERMINAL_FIELDS or payload["stage"] not in _STAGES:
        raise ValueError("invalid diagnostic terminal")
    for field in ("config_digest", "mandate_receipt_digest", "snapshot_digest"):
        value = payload[field]
        if value is not None and (not isinstance(value, str) or not _DIGEST.fullmatch(value)):
            raise ValueError("invalid diagnostic digest")
    exception_class = payload["exception_class"]
    if exception_class is not None and (
        not isinstance(exception_class, str)
        or not _EXCEPTION_CLASS.fullmatch(exception_class)
    ):
        raise ValueError("invalid diagnostic exception class")
    identifiers = payload["function_identifiers"]
    if (
        not isinstance(identifiers, list)
        or len(identifiers) > 3
        or any(
            not isinstance(identifier, str)
            or len(identifier) > 180
            or identifier.count(":") != 1
            or "/" in identifier
            or "\\" in identifier
            for identifier in identifiers
        )
    ):
        raise ValueError("invalid diagnostic function identifiers")
    for field in ("runner_invocation_count", "runner_completion_count"):
        if payload[field] not in {0, 1}:
            raise ValueError("invalid diagnostic count")
    if payload["runner_completion_count"] > payload["runner_invocation_count"]:
        raise ValueError("invalid diagnostic count")
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _write_terminal(destination: Path, payload: dict[str, Any]) -> None:
    encoded = _validate_terminal(payload)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(destination, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)


def main(argv: list[str] | None = None) -> int:
    try:
        args = _arguments(list(sys.argv[1:] if argv is None else argv))
    except (TypeError, ValueError):
        return 2
    terminal: dict[str, Any] = {
        "config_digest": None,
        "exception_class": None,
        "function_identifiers": [],
        "mandate_receipt_digest": None,
        "runner_completion_count": 0,
        "runner_invocation_count": 0,
        "snapshot_digest": None,
        "stage": "preflight_validation_failed",
    }
    try:
        _require_filevault()
        (
            run_root,
            authority,
            snapshot_digest,
            mandate_receipt_digest,
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
        terminal.update(
            config_digest=hashlib.sha256(
                _canonical(_config(authority, session_class=session_class))
            ).hexdigest(),
            mandate_receipt_digest=mandate_receipt_digest,
            snapshot_digest=snapshot_digest,
        )
        runner_revision, runner_tree_sha = resolve_tqqq_runtime_identity()
        terminal = orchestrate_existing_tqqq_snapshot_diagnostic(
            run_root,
            expected_snapshot_digest=snapshot_digest,
            expected_mandate_receipt_digest=mandate_receipt_digest,
            authority=authority,
            execution_revision=execution_revision,
            execution_tree_sha=execution_tree_sha,
            runner_revision=runner_revision,
            runner_tree_sha=runner_tree_sha,
            session_class=session_class,
        )
    except Exception as exc:  # noqa: BLE001 - diagnostic terminal must remain sanitized
        exception_class = type(exc).__name__
        terminal["exception_class"] = (
            exception_class if _EXCEPTION_CLASS.fullmatch(exception_class) else "Exception"
        )
    _write_terminal(args.output, terminal)
    print(_validate_terminal(terminal).decode("utf-8"))
    return 0 if terminal["stage"] == "promotion_replay_exception" else 1


if __name__ == "__main__":
    raise SystemExit(main())
