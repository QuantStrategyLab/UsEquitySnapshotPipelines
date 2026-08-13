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
from us_equity_snapshot_pipelines.tqqq_offline_replay_runtime import (
    derive_tqqq_offline_replay_runtime_manifest,
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
_RUNNER_CONSUMABLE_BINDING_SCHEMA = (
    "qsl.tqqq.execution-binding-record.runner-consumable.v3"
)
_RUNTIME_MANIFEST_SCHEMA = "qsl.tqqq.offline-replay-runtime.v1"
_RUNTIME_MANIFEST_FIELDS = {
    "schema_version",
    "uesp_revision",
    "lockfile_sha256",
    "qpk_revision",
    "ues_revision",
    "python_major_minor",
}
_PYTHON_MAJOR_MINOR = re.compile(r"^\d+\.\d+$")
_RUNNER_PROJECT_ROOT = Path(__file__).resolve().parents[1]


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


def _current_runtime_manifest() -> dict[str, str]:
    return derive_tqqq_offline_replay_runtime_manifest(_RUNNER_PROJECT_ROOT).to_dict()


def _current_runtime_identity() -> tuple[str, str]:
    return resolve_tqqq_runtime_identity()


def _valid_runtime_manifest(value: object) -> bool:
    if not isinstance(value, dict) or set(value) != _RUNTIME_MANIFEST_FIELDS:
        return False
    return (
        value["schema_version"] == _RUNTIME_MANIFEST_SCHEMA
        and isinstance(value["uesp_revision"], str)
        and _REVISION.fullmatch(value["uesp_revision"]) is not None
        and isinstance(value["lockfile_sha256"], str)
        and _DIGEST.fullmatch(value["lockfile_sha256"]) is not None
        and isinstance(value["qpk_revision"], str)
        and _REVISION.fullmatch(value["qpk_revision"]) is not None
        and isinstance(value["ues_revision"], str)
        and _REVISION.fullmatch(value["ues_revision"]) is not None
        and isinstance(value["python_major_minor"], str)
        and _PYTHON_MAJOR_MINOR.fullmatch(value["python_major_minor"]) is not None
    )


def _load_materialized_runtime_manifest(path: object, expected_sha256: object) -> dict[str, str]:
    if not isinstance(path, str) or not isinstance(expected_sha256, str) or not _DIGEST.fullmatch(expected_sha256):
        raise ValueError("invalid runtime manifest")
    candidate = Path(path)
    try:
        if candidate.is_symlink() or not candidate.is_file():
            raise ValueError("invalid runtime manifest")
        payload = candidate.read_bytes()
        if hashlib.sha256(payload).hexdigest() != expected_sha256:
            raise ValueError("runtime manifest identity mismatch")
        result = json.loads(payload)
    except (OSError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("runtime manifest is unavailable") from exc
    if not _valid_runtime_manifest(result):
        raise ValueError("invalid runtime manifest")
    return result


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
    binding = terminal.get("binding")
    execution = terminal.get("execution")
    verification = terminal.get("verification")
    if (
        terminal.get("schema_version") != _RUNNER_CONSUMABLE_BINDING_SCHEMA
        or set(terminal) != {"schema_version", "binding", "execution", "verification"}
        or not all(isinstance(value, dict) for value in (binding, execution, verification))
        or set(binding)
        != {
            "immutable_snapshot_identity",
            "source_mandate_identity",
            "authority_identity",
        }
        or set(execution)
        != {
            "snapshot_execution_identity",
            "runner_runtime_identity",
            "runtime_manifest",
            "session_identity",
        }
        or set(verification)
        != {
            "authority_transaction_consumed",
            "evidence_artifact_count",
            "evidence_runner_invocation_count",
            "evidence_runner_completion_count",
            "provider_reacquisition",
            "second_evidence_run",
            "safety",
        }
    ):
        raise ValueError("invalid execution binding")
    immutable_snapshot_identity = binding["immutable_snapshot_identity"]
    source_mandate_identity = binding["source_mandate_identity"]
    authority_identity = binding["authority_identity"]
    snapshot_execution_identity = execution["snapshot_execution_identity"]
    runner_runtime_identity = execution["runner_runtime_identity"]
    runtime_manifest = execution["runtime_manifest"]
    session_identity = execution["session_identity"]
    safety = verification["safety"]
    if (
        not all(
            isinstance(value, dict)
            for value in (
                immutable_snapshot_identity,
                source_mandate_identity,
                authority_identity,
                snapshot_execution_identity,
                runner_runtime_identity,
                runtime_manifest,
                session_identity,
                safety,
            )
        )
        or set(immutable_snapshot_identity) != {"snapshot_digest"}
        or set(source_mandate_identity) != {"mandate_receipt_digest"}
        or set(authority_identity)
        != {
            "authority_receipt",
            "authority_receipt_sha256",
            "entitlement_receipt_sha256",
            "license_receipt_sha256",
            "retention_expires_at",
            "risk_standard_id",
            "risk_standard_sha256",
            "platform_execution_revision",
        }
        or set(snapshot_execution_identity) != {"revision", "tree_sha"}
        or set(runner_runtime_identity) != {"revision", "tree_sha"}
        or set(runtime_manifest) != {"identity", "materialization"}
        or not _valid_runtime_manifest(runtime_manifest["identity"])
        or not isinstance(runtime_manifest["materialization"], dict)
        or set(runtime_manifest["materialization"])
        != {"manifest_path", "manifest_sha256", "python_executable"}
        or set(session_identity) != {"session_class"}
        or set(safety)
        != {
            "no_order",
            "size_zero_required",
            "order_calls",
            "account_positions_funds_orders_executions_capital_calls",
            "raw_bars_dates_prices_volumes_provider_messages_logged",
        }
    ):
        raise ValueError("invalid execution binding")
    snapshot_digest = immutable_snapshot_identity["snapshot_digest"]
    mandate_receipt_digest = source_mandate_identity["mandate_receipt_digest"]
    execution_revision = snapshot_execution_identity["revision"]
    execution_tree_sha = snapshot_execution_identity["tree_sha"]
    runner_runtime_revision = runner_runtime_identity["revision"]
    runner_runtime_tree_sha = runner_runtime_identity["tree_sha"]
    runtime_manifest_identity = runtime_manifest["identity"]
    runtime_manifest_materialization = runtime_manifest["materialization"]
    session_class = session_identity["session_class"]
    retention_expires_at = authority_identity["retention_expires_at"]
    authority_receipt_sha256 = authority_identity["authority_receipt_sha256"]
    entitlement_receipt_sha256 = authority_identity["entitlement_receipt_sha256"]
    license_receipt_sha256 = authority_identity["license_receipt_sha256"]
    if (
        not isinstance(snapshot_digest, str)
        or not _DIGEST.fullmatch(snapshot_digest)
        or not isinstance(mandate_receipt_digest, str)
        or not _DIGEST.fullmatch(mandate_receipt_digest)
        or not isinstance(execution_revision, str)
        or not _REVISION.fullmatch(execution_revision)
        or not isinstance(execution_tree_sha, str)
        or not _REVISION.fullmatch(execution_tree_sha)
        or not isinstance(runner_runtime_revision, str)
        or not _REVISION.fullmatch(runner_runtime_revision)
        or not isinstance(runner_runtime_tree_sha, str)
        or not _REVISION.fullmatch(runner_runtime_tree_sha)
        or session_class != "live-data-only"
        or not isinstance(retention_expires_at, str)
        or not isinstance(authority_receipt_sha256, str)
        or not _DIGEST.fullmatch(authority_receipt_sha256)
        or not isinstance(entitlement_receipt_sha256, str)
        or not _DIGEST.fullmatch(entitlement_receipt_sha256)
        or not isinstance(license_receipt_sha256, str)
        or not _DIGEST.fullmatch(license_receipt_sha256)
        or authority_identity["risk_standard_id"] != risk_standard_id
        or authority_identity["risk_standard_sha256"] != risk_standard_sha256
        or authority_identity["platform_execution_revision"] != platform_execution_revision
        or verification["authority_transaction_consumed"] is not True
        or any(
            type(value) is not int
            for value in (
                verification["evidence_artifact_count"],
                verification["evidence_runner_invocation_count"],
                verification["evidence_runner_completion_count"],
                verification["provider_reacquisition"],
                verification["second_evidence_run"],
                safety["order_calls"],
                safety["account_positions_funds_orders_executions_capital_calls"],
                safety["raw_bars_dates_prices_volumes_provider_messages_logged"],
            )
        )
        or verification["evidence_artifact_count"] != 0
        or verification["evidence_runner_invocation_count"] != 1
        or verification["evidence_runner_completion_count"] != 0
        or verification["provider_reacquisition"] != 0
        or verification["second_evidence_run"] != 0
        or safety.get("no_order") is not True
        or safety.get("size_zero_required") is not True
        or safety.get("order_calls") != 0
        or safety.get("account_positions_funds_orders_executions_capital_calls") != 0
        or safety.get("raw_bars_dates_prices_volumes_provider_messages_logged") != 0
    ):
        raise ValueError("invalid execution binding")
    materialized_runtime_manifest = _load_materialized_runtime_manifest(
        runtime_manifest_materialization["manifest_path"],
        runtime_manifest_materialization["manifest_sha256"],
    )
    runtime_python = runtime_manifest_materialization["python_executable"]
    current_runtime_manifest = _current_runtime_manifest()
    current_runtime_revision, current_runtime_tree_sha = _current_runtime_identity()
    if (
        not isinstance(runtime_python, str)
        or Path(runtime_python).absolute() != Path(sys.executable).absolute()
        or materialized_runtime_manifest != runtime_manifest_identity
        or not _valid_runtime_manifest(current_runtime_manifest)
        or runtime_manifest_identity != current_runtime_manifest
        or runner_runtime_revision != runtime_manifest_identity["uesp_revision"]
        or current_runtime_revision != runner_runtime_revision
        or current_runtime_tree_sha != runner_runtime_tree_sha
    ):
        raise ValueError("invalid execution binding")
    authority_receipt_path = authority_identity["authority_receipt"]
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
