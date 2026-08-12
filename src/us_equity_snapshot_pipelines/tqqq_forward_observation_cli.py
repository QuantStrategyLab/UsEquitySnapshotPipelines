"""Thin provider transport CLI for the portable TQQQ forward collector core."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
import plistlib
import re
import secrets
import shutil
import stat
import subprocess
import sys
import tempfile
from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, NoReturn

from us_equity_snapshot_pipelines.lifecycle.soxl_adjusted_last_acquisition import (
    acquire_strict_adjusted_last,
    build_request_bound_ibkr_app,
)
from us_equity_snapshot_pipelines.lifecycle.tqqq_acquisition_orchestration import (
    OFFICIAL_IBAPI_PROVENANCE_SHA256,
    resolve_tqqq_runtime_identity,
)
from us_equity_snapshot_pipelines.lifecycle.tqqq_forward_observation import (
    APPLICATION_CALL_CEILING,
    ORDERED_SYMBOLS,
    PLAN_SHA256,
    CollectionResult,
    ForwardObservationError,
    canonical_json,
    collect_once,
    frozen_sessions,
    validate_authority_contract,
)
from us_equity_snapshot_pipelines.tqqq_r1_snapshot import _publish_noreplace

_HOST = "127.0.0.1"
_PORT = 4001
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_ATTEMPT = re.compile(r"^(\d{4}-\d{2}-\d{2})\.(started|completed)\.json$")


def validate_local_adapter_authority(receipt: Mapping[str, Any]) -> None:
    """Validate only the concrete localhost Gateway adapter envelope."""
    provider_identity = receipt.get("provider_identity")
    if (
        not isinstance(provider_identity, Mapping)
        or provider_identity.get("deploy_target") != "local"
        or receipt.get("local_adapter")
        != {
            "gateway_authenticated": True,
            "host": _HOST,
            "listener_loopback_only": True,
            "no_other_local_api_client": True,
            "port": _PORT,
        }
    ):
        raise ForwardObservationError("local provider adapter authority invalid")


def _require_private_file(path: Path, expected_sha256: str) -> dict[str, Any]:
    if not path.is_absolute() or not _DIGEST.fullmatch(expected_sha256):
        raise ForwardObservationError("authority binding invalid")
    try:
        file_stat = path.lstat()
        payload = path.read_bytes()
    except OSError as exc:
        raise ForwardObservationError("authority binding unavailable") from exc
    if (
        stat.S_ISLNK(file_stat.st_mode)
        or not stat.S_ISREG(file_stat.st_mode)
        or stat.S_IMODE(file_stat.st_mode) != 0o600
        or hashlib.sha256(payload).hexdigest() != expected_sha256
    ):
        raise ForwardObservationError("authority binding invalid")
    try:
        parsed = json.loads(payload)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ForwardObservationError("authority binding invalid") from exc
    if not isinstance(parsed, dict) or canonical_json(parsed) != payload:
        raise ForwardObservationError("authority binding invalid")
    return parsed


def load_authority_receipts(
    authority_receipt: Path,
    authority_receipt_sha256: str,
    *,
    plan_receipt: Path,
    plan_receipt_sha256: str,
    runtime_commit: str,
    now: datetime,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Load private local receipts, then apply the portable core contract."""
    authority = _require_private_file(authority_receipt, authority_receipt_sha256)
    plan = _require_private_file(plan_receipt, plan_receipt_sha256)
    validate_authority_contract(
        authority,
        authority_receipt_sha256,
        plan_receipt=plan,
        plan_receipt_sha256=plan_receipt_sha256,
        runtime_commit=runtime_commit,
        now=now,
    )
    for field in ("entitlement_receipt", "license_source_terms_receipt"):
        binding = authority.get(field)
        if not isinstance(binding, Mapping):
            raise ForwardObservationError("authority binding invalid")
        path = binding.get("path")
        digest = binding.get("sha256")
        if not isinstance(path, str) or not isinstance(digest, str):
            raise ForwardObservationError("authority binding invalid")
        _require_private_file(Path(path), digest)
    return authority, plan


def _private_directory(path: Path) -> None:
    if not path.is_absolute():
        raise ForwardObservationError("private output root invalid")
    current = path
    while current != current.parent:
        if current.exists() and current.is_symlink():
            raise ForwardObservationError("private output root invalid")
        current = current.parent
    try:
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        if path.is_symlink() or not path.is_dir():
            raise ForwardObservationError("private output root invalid")
        os.chmod(path, 0o700)
    except OSError as exc:
        raise ForwardObservationError("private output root unavailable") from exc


def _write_private(path: Path, payload: bytes) -> None:
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


class LocalForwardObservationLedger:
    """Concrete FileVault-local persistence for the candidate-specific ledger."""

    def __init__(self, output_root: Path) -> None:
        self._root = output_root

    def invalidate(self, reason_code: str) -> NoReturn:
        marker = self._root / "PLAN_INVALID.json"
        if not marker.exists() and not marker.is_symlink():
            try:
                _write_private(
                    marker,
                    canonical_json(
                        {
                            "plan_sha256": PLAN_SHA256,
                            "reason_code": reason_code,
                            "status": "PLAN_INVALID",
                        }
                    ),
                )
            except OSError:
                pass
        raise ForwardObservationError("plan invalid")

    def completed_sessions(self) -> tuple[date, ...]:
        _private_directory(self._root)
        marker = self._root / "PLAN_INVALID.json"
        if marker.exists() or marker.is_symlink():
            raise ForwardObservationError("plan invalid")
        ledger = self._root / "attempt-ledger"
        _private_directory(ledger)
        observations = self._root / "observations"
        _private_directory(observations)
        completed: set[date] = set()
        started: set[date] = set()
        observation_digests: list[str] = []
        try:
            entries = tuple(ledger.iterdir())
        except OSError:
            self.invalidate("ATTEMPT_LEDGER_INVALID")
        frozen = frozen_sessions()
        frozen_set = set(frozen)
        for path in entries:
            match = _ATTEMPT.fullmatch(path.name)
            try:
                file_stat = path.lstat()
            except OSError:
                self.invalidate("ATTEMPT_LEDGER_INVALID")
            if (
                match is None
                or stat.S_ISLNK(file_stat.st_mode)
                or not stat.S_ISREG(file_stat.st_mode)
                or stat.S_IMODE(file_stat.st_mode) != 0o600
            ):
                self.invalidate("ATTEMPT_LEDGER_INVALID")
            session = date.fromisoformat(match.group(1))
            if session not in frozen_set:
                self.invalidate("ATTEMPT_LEDGER_EXTRA")
            try:
                encoded = path.read_bytes()
                payload = json.loads(encoded)
            except (OSError, TypeError, json.JSONDecodeError):
                self.invalidate("ATTEMPT_LEDGER_INVALID")
            if not isinstance(payload, dict) or canonical_json(payload) != encoded:
                self.invalidate("ATTEMPT_LEDGER_INVALID")
            if match.group(2) == "started":
                if payload != {
                    "plan_sha256": PLAN_SHA256,
                    "session_sha256": hashlib.sha256(session.isoformat().encode()).hexdigest(),
                    "status": "STARTED_NO_RETRY",
                }:
                    self.invalidate("ATTEMPT_LEDGER_INVALID")
            else:
                observation_sha256 = payload.get("observation_sha256")
                observation = (
                    self._root
                    / "observations"
                    / str(observation_sha256)
                    / "observation.json"
                )
                try:
                    observation_stat = observation.lstat()
                    observation_bytes = observation.read_bytes()
                    observation_payload = json.loads(observation_bytes)
                except (OSError, TypeError, json.JSONDecodeError):
                    self.invalidate("ATTEMPT_LEDGER_INVALID")
                daily_observations = (
                    observation_payload.get("observations")
                    if isinstance(observation_payload, dict)
                    else None
                )
                if (
                    set(payload)
                    != {
                        "observation_sha256",
                        "plan_sha256",
                        "provider_application_calls",
                        "status",
                    }
                    or not isinstance(observation_sha256, str)
                    or not _DIGEST.fullmatch(observation_sha256)
                    or payload.get("plan_sha256") != PLAN_SHA256
                    or payload.get("provider_application_calls") != APPLICATION_CALL_CEILING
                    or payload.get("status") != "COMPLETED"
                    or stat.S_ISLNK(observation_stat.st_mode)
                    or not stat.S_ISREG(observation_stat.st_mode)
                    or stat.S_IMODE(observation_stat.st_mode) != 0o600
                    or hashlib.sha256(observation_bytes).hexdigest() != observation_sha256
                    or canonical_json(observation_payload) != observation_bytes
                    or observation_payload.get("plan_sha256") != PLAN_SHA256
                    or not isinstance(daily_observations, list)
                    or any(
                        not isinstance(item, dict)
                        or item.get("session") != session.isoformat()
                        for item in daily_observations
                    )
                    or [item.get("symbol") for item in daily_observations]
                    != list(ORDERED_SYMBOLS)
                ):
                    self.invalidate("ATTEMPT_LEDGER_INVALID")
                observation_digests.append(observation_sha256)
            (started if match.group(2) == "started" else completed).add(session)
        if completed - started or started - completed:
            self.invalidate("ATTEMPT_LEDGER_PARTIAL")
        prefix = frozen[: len(completed)]
        if set(prefix) != completed:
            self.invalidate("ATTEMPT_LEDGER_MISSING_OR_DUPLICATE")
        try:
            observation_entries = tuple(observations.iterdir())
        except OSError:
            self.invalidate("ATTEMPT_LEDGER_INVALID")
        if (
            len(set(observation_digests)) != len(observation_digests)
            or {entry.name for entry in observation_entries} != set(observation_digests)
        ):
            self.invalidate("ATTEMPT_LEDGER_MISSING_OR_DUPLICATE")
        for entry in observation_entries:
            try:
                entry_stat = entry.lstat()
                members = tuple(entry.iterdir())
            except OSError:
                self.invalidate("ATTEMPT_LEDGER_INVALID")
            if (
                stat.S_ISLNK(entry_stat.st_mode)
                or not stat.S_ISDIR(entry_stat.st_mode)
                or stat.S_IMODE(entry_stat.st_mode) != 0o700
                or {member.name for member in members} != {"observation.json"}
            ):
                self.invalidate("ATTEMPT_LEDGER_INVALID")
        return prefix

    def start_session(self, session: date) -> None:
        _write_private(
            self._root / "attempt-ledger" / f"{session.isoformat()}.started.json",
            canonical_json(
                {
                    "plan_sha256": PLAN_SHA256,
                    "session_sha256": hashlib.sha256(session.isoformat().encode()).hexdigest(),
                    "status": "STARTED_NO_RETRY",
                }
            ),
        )

    def publish_observation(self, payload: bytes) -> str:
        digest = hashlib.sha256(payload).hexdigest()
        observations = self._root / "observations"
        _private_directory(observations)
        destination = observations / digest
        if destination.exists() or destination.is_symlink():
            self.invalidate("DUPLICATE_OBSERVATION")
        temporary: Path | None = None
        try:
            temporary = Path(tempfile.mkdtemp(prefix=f".{digest}.", dir=observations))
            os.chmod(temporary, 0o700)
            _write_private(temporary / "observation.json", payload)
            _publish_noreplace(temporary, destination)
            temporary = None
        except Exception:  # noqa: BLE001 - publication failures invalidate the plan
            self.invalidate("ATOMIC_PUBLICATION_FAILED")
        finally:
            if temporary is not None:
                try:
                    if temporary.is_symlink():
                        temporary.unlink()
                    elif temporary.exists():
                        shutil.rmtree(temporary)
                except OSError:
                    pass
        return digest

    def complete_session(self, session: date, observation_sha256: str) -> None:
        _write_private(
            self._root / "attempt-ledger" / f"{session.isoformat()}.completed.json",
            canonical_json(
                {
                    "observation_sha256": observation_sha256,
                    "plan_sha256": PLAN_SHA256,
                    "provider_application_calls": APPLICATION_CALL_CEILING,
                    "status": "COMPLETED",
                }
            ),
        )


class _SanitizedParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise ValueError("invalid arguments")


def _arguments(argv: list[str]) -> argparse.Namespace:
    parser = _SanitizedParser(add_help=False)
    parser.add_argument("--authority-receipt", required=True, type=Path)
    parser.add_argument("--authority-receipt-sha256", required=True)
    parser.add_argument("--ibapi-root", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--plan-receipt", required=True, type=Path)
    parser.add_argument("--plan-receipt-sha256", required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--runtime-commit", required=True)
    return parser.parse_args(argv)


def _require_filevault(output_root: Path) -> None:
    if not output_root.is_absolute():
        raise ForwardObservationError("FileVault output root invalid")
    existing = output_root
    while not existing.exists() and existing != existing.parent:
        existing = existing.parent
    current = existing
    while current != current.parent and not os.path.ismount(current):
        if current.is_symlink():
            raise ForwardObservationError("FileVault output root invalid")
        current = current.parent
    mountpoint = current
    try:
        status = subprocess.run(
            ["/usr/bin/fdesetup", "status"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        volume = subprocess.run(
            ["/usr/sbin/diskutil", "info", "-plist", str(mountpoint)],
            check=True,
            capture_output=True,
            timeout=5,
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise ForwardObservationError("FileVault status unavailable") from exc
    if status.stdout.strip() != "FileVault is On.":
        raise ForwardObservationError("FileVault required")
    try:
        volume_info = plistlib.loads(volume.stdout)
    except plistlib.InvalidFileException as exc:
        raise ForwardObservationError("FileVault status unavailable") from exc
    if volume_info.get("FileVault") is not True or volume_info.get("Encryption") is not True:
        raise ForwardObservationError("FileVault encrypted output volume required")


def _bound_application_calls(app: Any) -> Any:
    app.provider_application_calls = 0

    def bounded(original: Callable[..., Any]) -> Callable[..., Any]:
        def invoke(*args: Any, **kwargs: Any) -> Any:
            if app.provider_application_calls >= APPLICATION_CALL_CEILING:
                raise ForwardObservationError("provider application call ceiling reached")
            app.provider_application_calls += 1
            return original(*args, **kwargs)

        return invoke

    for name in ("reqContractDetails", "reqHistoricalData", "cancelHistoricalData"):
        original = getattr(app, name, None)
        if not callable(original):
            raise ForwardObservationError("provider application surface invalid")
        setattr(app, name, bounded(original))
    return app


def _runtime(ibapi_root: Path) -> tuple[Any, Callable[..., Any]]:
    if not ibapi_root.is_absolute() or ibapi_root.is_symlink() or not ibapi_root.is_dir():
        raise ForwardObservationError("approved provider runtime unavailable")
    provenance_path = ibapi_root / "provenance.installed.json"
    try:
        payload = provenance_path.read_bytes()
        if (
            provenance_path.is_symlink()
            or provenance_path.stat().st_mode & 0o777 != 0o600
            or hashlib.sha256(payload).hexdigest() != OFFICIAL_IBAPI_PROVENANCE_SHA256
        ):
            raise ForwardObservationError("approved provider runtime identity mismatch")
    except OSError as exc:
        raise ForwardObservationError("approved provider runtime unavailable") from exc
    try:
        import ibapi
        from ibapi.client import EClient
        from ibapi.contract import Contract
        from ibapi.wrapper import EWrapper
    except ImportError:
        raise ForwardObservationError("approved provider runtime unavailable") from None
    if (
        EClient.__module__ != "ibapi.client"
        or EWrapper.__module__ != "ibapi.wrapper"
        or Contract.__module__ != "ibapi.contract"
        or not Path(ibapi.__file__).resolve().is_relative_to(ibapi_root.resolve())
    ):
        raise ForwardObservationError("approved provider runtime identity mismatch")
    app = _bound_application_calls(
        build_request_bound_ibkr_app(
            client_type=EClient,
            wrapper_type=EWrapper,
            contract_type=Contract,
            request_id_start=secrets.randbelow(2_000_000_000),
        )
    )

    def stock_factory(symbol: str, exchange: str, currency: str) -> Any:
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = exchange
        contract.currency = currency
        return contract

    return app, stock_factory


def _provider_adapter(app: Any, contract_factory: Callable[..., Any]) -> Callable[..., Any]:
    connected = False

    def acquire(symbol: str, session: date, end_datetime: datetime) -> Any:
        nonlocal connected
        if not connected:
            app.connect(_HOST, _PORT, secrets.randbelow(2_000_000_000) + 1)
            if not app.isConnected():
                raise ForwardObservationError("provider transport unavailable")
            app.start_reader()
            if not app.wait_for_handshake():
                raise ForwardObservationError("provider handshake unavailable")
            connected = True
        provider_end = end_datetime.strftime("%Y%m%d %H:%M:%S UTC")

        def requester(contract: Any, **request_kwargs: Any) -> Any:
            return app.request_adjusted_history(
                symbol,
                contract,
                expected_session_count=1,
                expected_duration="1 D",
                expected_end_datetime=provider_end,
                **request_kwargs,
            )

        return acquire_strict_adjusted_last(
            app,
            symbol,
            end_datetime=end_datetime,
            duration="1 D",
            expected_sessions=(session,),
            provider_end_datetime=provider_end,
            stock_factory=contract_factory,
            requester=requester,
        )

    return acquire


def _collect_local_once(
    *,
    output_root: Path,
    authority_receipt: Path,
    authority_receipt_sha256: str,
    plan_receipt: Path,
    plan_receipt_sha256: str,
    runtime_commit: str,
    acquire_symbol: Callable[..., Any],
    now: datetime | None = None,
) -> CollectionResult:
    """Bind private local receipts and storage to the portable business core."""
    observed_now = datetime.now(UTC) if now is None else now
    authority, plan = load_authority_receipts(
        authority_receipt,
        authority_receipt_sha256,
        plan_receipt=plan_receipt,
        plan_receipt_sha256=plan_receipt_sha256,
        runtime_commit=runtime_commit,
        now=observed_now,
    )
    validate_local_adapter_authority(authority)
    return collect_once(
        ledger=LocalForwardObservationLedger(output_root),
        authority_receipt=authority,
        authority_receipt_sha256=authority_receipt_sha256,
        plan_receipt=plan,
        plan_receipt_sha256=plan_receipt_sha256,
        runtime_commit=runtime_commit,
        acquire_symbol=acquire_symbol,
        now=observed_now,
    )


def _terminal(result: CollectionResult) -> dict[str, Any]:
    return {
        "no_order": True,
        "observation_sha256": result.observation_sha256,
        "plan_sha256": PLAN_SHA256,
        "provider_application_call_ceiling": APPLICATION_CALL_CEILING,
        "provider_application_calls": result.provider_application_calls,
        "size_zero_required": True,
        "status": result.status,
    }


def _provider_call_count(app: Any | None) -> int:
    try:
        count = getattr(app, "provider_application_calls", 0)
    except Exception:  # noqa: BLE001 - public terminal must stay sanitized
        return 0
    return count if isinstance(count, int) and 0 <= count <= APPLICATION_CALL_CEILING else 0


def main(argv: list[str] | None = None) -> int:
    result = CollectionResult("PARK_MATERIAL", 0, None)
    app: Any | None = None
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        try:
            args = _arguments(list(sys.argv[1:] if argv is None else argv))
            if args.plan_sha256 != PLAN_SHA256:
                raise ForwardObservationError("plan identity mismatch")
            observed_now = datetime.now(UTC)
            authority, plan = load_authority_receipts(
                args.authority_receipt,
                args.authority_receipt_sha256,
                plan_receipt=args.plan_receipt,
                plan_receipt_sha256=args.plan_receipt_sha256,
                runtime_commit=args.runtime_commit,
                now=observed_now,
            )
            validate_local_adapter_authority(authority)
            _require_filevault(args.output_root)
            runtime_revision, _runtime_tree = resolve_tqqq_runtime_identity()
            if runtime_revision != args.runtime_commit:
                raise ForwardObservationError("collector runtime identity mismatch")
            app, contract_factory = _runtime(args.ibapi_root)
            result = collect_once(
                ledger=LocalForwardObservationLedger(args.output_root),
                authority_receipt=authority,
                authority_receipt_sha256=args.authority_receipt_sha256,
                plan_receipt=plan,
                plan_receipt_sha256=args.plan_receipt_sha256,
                runtime_commit=args.runtime_commit,
                acquire_symbol=_provider_adapter(app, contract_factory),
                now=observed_now,
            )
        except Exception:  # noqa: BLE001 - public terminal must stay sanitized
            result = CollectionResult("PARK_MATERIAL", _provider_call_count(app), None)
        finally:
            try:
                if (
                    app is not None
                    and callable(getattr(app, "isConnected", None))
                    and app.isConnected()
                ):
                    app.disconnect()
            except Exception:  # noqa: BLE001, S110 - teardown is also sanitized
                pass
    print(json.dumps(_terminal(result), sort_keys=True, separators=(",", ":")))
    return 0 if result.status in {"COLLECTED", "NO_FROZEN_SESSION_READY", "FROZEN_PLAN_COMPLETE"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
