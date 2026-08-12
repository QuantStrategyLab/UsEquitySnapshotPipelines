#!/usr/bin/env python3
"""Install the optional macOS-only thin scheduler for the portable collector."""

from __future__ import annotations

import argparse
import json
import os
import plistlib
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.tqqq_forward_observation import PLAN_SHA256
from us_equity_snapshot_pipelines.tqqq_forward_observation_cli import (
    _require_filevault,
    load_authority_receipts,
    validate_local_adapter_authority,
)

LAUNCH_AGENT_LABEL = "com.quantstrategylab.tqqq-forward-observation"
_FIRST_COLLECTION_DEADLINE = datetime(2026, 8, 13, 22, 0, tzinfo=UTC)


def _runtime_commit(runtime_python: Path) -> str:
    identity = subprocess.run(
        [
            str(runtime_python),
            "-c",
            (
                "from us_equity_snapshot_pipelines.lifecycle."
                "tqqq_acquisition_orchestration import resolve_tqqq_runtime_identity; "
                "print(resolve_tqqq_runtime_identity()[0])"
            ),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.strip()
    if len(identity) != 40 or any(character not in "0123456789abcdef" for character in identity):
        raise ValueError("collector runtime identity invalid")
    return identity


def build_launch_agent_plist(
    *,
    runtime_python: Path,
    ibapi_root: Path,
    authority_receipt: Path,
    authority_receipt_sha256: str,
    plan_receipt: Path,
    plan_receipt_sha256: str,
    output_root: Path,
    runtime_commit: str,
    stdout_path: Path,
    stderr_path: Path,
) -> bytes:
    """Build one fixed 06:15 Asia/Shanghai daily schedule with umask 077."""
    values = (
        runtime_python,
        ibapi_root,
        authority_receipt,
        plan_receipt,
        output_root,
        stdout_path,
        stderr_path,
    )
    if any(not path.is_absolute() for path in values):
        raise ValueError("LaunchAgent paths must be absolute")
    return plistlib.dumps(
        {
            "Label": LAUNCH_AGENT_LABEL,
            "ProgramArguments": [
                str(runtime_python),
                "-m",
                "us_equity_snapshot_pipelines.tqqq_forward_observation_cli",
                "--authority-receipt",
                str(authority_receipt),
                "--authority-receipt-sha256",
                authority_receipt_sha256,
                "--ibapi-root",
                str(ibapi_root),
                "--output-root",
                str(output_root),
                "--plan-receipt",
                str(plan_receipt),
                "--plan-receipt-sha256",
                plan_receipt_sha256,
                "--plan-sha256",
                PLAN_SHA256,
                "--runtime-commit",
                runtime_commit,
            ],
            "StartCalendarInterval": {"Hour": 6, "Minute": 15},
            "StandardOutPath": str(stdout_path),
            "StandardErrorPath": str(stderr_path),
            "Umask": 0o77,
        },
        fmt=plistlib.FMT_XML,
        sort_keys=True,
    )


def _activation_gate(
    *,
    runtime_python: Path,
    ibapi_root: Path,
    authority_receipt: Path,
    authority_receipt_sha256: str,
    plan_receipt: Path,
    plan_receipt_sha256: str,
    output_root: Path,
    runtime_commit: str,
    now: datetime,
) -> bool:
    if now.astimezone(UTC) >= _FIRST_COLLECTION_DEADLINE:
        return False
    if (
        not runtime_python.is_absolute()
        or runtime_python.is_symlink()
        or not runtime_python.is_file()
        or not ibapi_root.is_absolute()
        or ibapi_root.is_symlink()
        or not ibapi_root.is_dir()
        or not output_root.is_absolute()
    ):
        return False
    try:
        localtime = Path("/etc/localtime").resolve()
        if not localtime.as_posix().endswith("/Asia/Shanghai"):
            return False
        _require_filevault(output_root)
        if _runtime_commit(runtime_python) != runtime_commit:
            return False
        domain = f"gui/{os.getuid()}"
        collision = subprocess.run(
            ["/bin/launchctl", "print", f"{domain}/{LAUNCH_AGENT_LABEL}"],
            check=False,
            capture_output=True,
            timeout=10,
        )
        if collision.returncode == 0:
            return False
        authority, _plan = load_authority_receipts(
            authority_receipt,
            authority_receipt_sha256,
            plan_receipt=plan_receipt,
            plan_receipt_sha256=plan_receipt_sha256,
            runtime_commit=runtime_commit,
            now=now,
        )
        validate_local_adapter_authority(authority)
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired, ValueError):
        return False
    return True


def install_launch_agent(
    *,
    destination: Path,
    runtime_python: Path,
    ibapi_root: Path,
    authority_receipt: Path,
    authority_receipt_sha256: str,
    plan_receipt: Path,
    plan_receipt_sha256: str,
    output_root: Path,
    runtime_commit: str,
    now: datetime | None = None,
) -> bool:
    """Install and bootstrap only after every activation gate passes."""
    observed_now = datetime.now(UTC) if now is None else now
    if not _activation_gate(
        runtime_python=runtime_python,
        ibapi_root=ibapi_root,
        authority_receipt=authority_receipt,
        authority_receipt_sha256=authority_receipt_sha256,
        plan_receipt=plan_receipt,
        plan_receipt_sha256=plan_receipt_sha256,
        output_root=output_root,
        runtime_commit=runtime_commit,
        now=observed_now,
    ):
        return False
    if not destination.is_absolute() or destination.exists() or destination.is_symlink():
        return False
    try:
        log_root = output_root / "scheduler-logs"
        log_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(log_root, 0o700)
        payload = build_launch_agent_plist(
            runtime_python=runtime_python,
            ibapi_root=ibapi_root,
            authority_receipt=authority_receipt,
            authority_receipt_sha256=authority_receipt_sha256,
            plan_receipt=plan_receipt,
            plan_receipt_sha256=plan_receipt_sha256,
            output_root=output_root,
            runtime_commit=runtime_commit,
            stdout_path=log_root / "stdout.log",
            stderr_path=log_root / "stderr.log",
        )
        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(destination.parent, 0o700)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".tqqq-forward.", dir=destination.parent
        )
    except (OSError, ValueError):
        return False
    temporary = Path(temporary_name)
    bootstrapped = False
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, destination, follow_symlinks=False)
        temporary.unlink()
        domain = f"gui/{os.getuid()}"
        subprocess.run(["/bin/launchctl", "bootstrap", domain, str(destination)], check=True)
        bootstrapped = True
        subprocess.run(
            ["/bin/launchctl", "print", f"{domain}/{LAUNCH_AGENT_LABEL}"],
            check=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError):
        if bootstrapped:
            subprocess.run(
                ["/bin/launchctl", "bootout", f"{domain}/{LAUNCH_AGENT_LABEL}"],
                check=False,
                capture_output=True,
            )
        temporary.unlink(missing_ok=True)
        destination.unlink(missing_ok=True)
        return False
    return True


class _SanitizedParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise ValueError("invalid arguments")


def main(argv: list[str] | None = None) -> int:
    parser = _SanitizedParser(add_help=False)
    parser.add_argument("--destination", required=True, type=Path)
    parser.add_argument("--runtime-python", required=True, type=Path)
    parser.add_argument("--ibapi-root", required=True, type=Path)
    parser.add_argument("--authority-receipt", required=True, type=Path)
    parser.add_argument("--authority-receipt-sha256", required=True)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--plan-receipt", required=True, type=Path)
    parser.add_argument("--plan-receipt-sha256", required=True)
    parser.add_argument("--runtime-commit", required=True)
    installed = False
    try:
        args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
        installed = install_launch_agent(
            destination=args.destination,
            runtime_python=args.runtime_python,
            ibapi_root=args.ibapi_root,
            authority_receipt=args.authority_receipt,
            authority_receipt_sha256=args.authority_receipt_sha256,
            plan_receipt=args.plan_receipt,
            plan_receipt_sha256=args.plan_receipt_sha256,
            output_root=args.output_root,
            runtime_commit=args.runtime_commit,
        )
    except (TypeError, ValueError):
        pass
    print(
        json.dumps(
            {
                "label": LAUNCH_AGENT_LABEL,
                "plan_sha256": PLAN_SHA256,
                "status": "INSTALLED_ARMED" if installed else "ACTIVATION_PARKED",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0 if installed else 1


if __name__ == "__main__":
    raise SystemExit(main())
