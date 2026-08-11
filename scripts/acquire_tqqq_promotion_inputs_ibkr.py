#!/usr/bin/env python3
"""Exact one-transaction IBKR caller for frozen TQQQ core-parity inputs."""

from __future__ import annotations

import argparse
import hashlib
import json
import secrets
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable

from quant_platform_kit.ibkr import StrictAdjustedHistoryError

from us_equity_snapshot_pipelines.lifecycle.soxl_adjusted_last_acquisition import (
    acquire_strict_adjusted_last,
    build_request_bound_ibkr_app,
)
from us_equity_snapshot_pipelines.lifecycle.tqqq_acquisition_orchestration import (
    EXACT_DURATIONS,
    FIRST_ELIGIBLE_SESSION,
    FIXED_CUTOFF,
    FROZEN_XNYS_SESSIONS,
    OFFICIAL_IBAPI_PROVENANCE_SHA256,
    TQQQ_PROMOTION_ASSETS,
    TqqqOrchestrationAuthority,
    TqqqOrchestrationError,
    orchestrate_tqqq_promotion,
    resolve_tqqq_runtime_identity,
)

EXACT_ASSETS = TQQQ_PROMOTION_ASSETS
APPLICATION_CALL_CEILING = len(EXACT_ASSETS) * 2
_FIXED_CUTOFF = datetime.fromisoformat(FIXED_CUTOFF)
_HOST = "127.0.0.1"
_SESSION_PORT = {"paper": 4002, "live-data-only": 4001}
_LOCAL_RESEARCH_ROOT = Path.home() / ".local/share/qsl/tqqq-promotion-evidence-v2"
_OFFICIAL_IBAPI_ROOT = Path.home() / ".local/share/qsl/ibkr-tws-api-v1049.02"
_SAFETY_BOUNDARY = {
    "execution_authorized": False,
    "no_order": True,
    "research_only": True,
    "size_zero_required": True,
}


def run_exact_acquisition(
    app: Any,
    *,
    contract_factory: Any,
    on_strict_history_failure: (
        Callable[[str, int, StrictAdjustedHistoryError], None] | None
    ) = None,
) -> dict[str, Any]:
    """Acquire QQQ, TQQQ, QQQM, and BOXX sequentially with zero retry."""
    results: dict[str, Any] = {}
    frozen_sessions = tuple(date.fromisoformat(value) for value in FROZEN_XNYS_SESSIONS)
    for symbol in EXACT_ASSETS:
        first = date.fromisoformat(
            FIRST_ELIGIBLE_SESSION.get(symbol, FROZEN_XNYS_SESSIONS[0])
        )
        expected_sessions = tuple(session for session in frozen_sessions if session >= first)
        duration = EXACT_DURATIONS[symbol]

        def requester(
            contract: Any,
            _symbol: str = symbol,
            _expected_session_count: int = len(expected_sessions),
            _duration: str = duration,
            **request_kwargs: Any,
        ) -> Any:
            return app.request_adjusted_history(
                _symbol,
                contract,
                expected_session_count=_expected_session_count,
                expected_duration=_duration,
                **request_kwargs,
            )

        try:
            results[symbol] = acquire_strict_adjusted_last(
                app,
                symbol,
                end_datetime=_FIXED_CUTOFF,
                duration=duration,
                expected_sessions=expected_sessions,
                stock_factory=contract_factory,
                requester=requester,
            )
        except StrictAdjustedHistoryError as exc:
            if on_strict_history_failure is not None:
                on_strict_history_failure(symbol, len(results), exc)
            raise
    return results


def _bound_application_calls(app: Any) -> Any:
    application_calls = 0

    def bounded(original: Callable[..., Any]) -> Callable[..., Any]:
        def invoke(*args: Any, **kwargs: Any) -> Any:
            nonlocal application_calls
            if application_calls >= APPLICATION_CALL_CEILING:
                raise TqqqOrchestrationError("IBKR application call ceiling reached")
            application_calls += 1
            return original(*args, **kwargs)

        return invoke

    for name in ("reqContractDetails", "reqHistoricalData", "cancelHistoricalData"):
        original = getattr(app, name, None)
        if not callable(original):
            raise TqqqOrchestrationError("IBKR application call surface mismatch")
        setattr(app, name, bounded(original))
    return app


def _runtime() -> tuple[Any, Any]:
    """Load the approved local official IBKR API runtime without fallback."""
    provenance_path = _OFFICIAL_IBAPI_ROOT / "provenance.installed.json"
    try:
        if (
            provenance_path.is_symlink()
            or not provenance_path.is_file()
            or provenance_path.stat().st_mode & 0o777 != 0o600
        ):
            raise RuntimeError("approved local IBKR API provenance identity mismatch")
        provenance_payload = provenance_path.read_bytes()
    except OSError:
        raise RuntimeError("approved local IBKR API provenance is unavailable") from None
    if hashlib.sha256(provenance_payload).hexdigest() != OFFICIAL_IBAPI_PROVENANCE_SHA256:
        raise RuntimeError("approved local IBKR API provenance identity mismatch")
    try:
        import ibapi
        from ibapi.client import EClient
        from ibapi.contract import Contract
        from ibapi.wrapper import EWrapper
    except ImportError:
        raise RuntimeError("approved local IBKR API runtime is unavailable") from None
    if (
        EClient.__module__ != "ibapi.client"
        or EWrapper.__module__ != "ibapi.wrapper"
        or Contract.__module__ != "ibapi.contract"
        or not Path(ibapi.__file__).resolve().is_relative_to(_OFFICIAL_IBAPI_ROOT.resolve())
    ):
        raise RuntimeError("approved local IBKR API runtime identity mismatch")

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


def _require_filevault_local_root() -> None:
    home = Path.home().resolve()
    root = _LOCAL_RESEARCH_ROOT.expanduser()
    if not root.is_absolute() or not root.resolve(strict=False).is_relative_to(home):
        raise RuntimeError("approved FileVault-local output root is unavailable")
    current = root
    while current != current.parent:
        if current.exists() and current.is_symlink():
            raise RuntimeError("approved FileVault-local output root is unavailable")
        current = current.parent
    try:
        status = subprocess.run(
            ["/usr/bin/fdesetup", "status"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError("FileVault status is unavailable") from exc
    if status.stdout.strip() != "FileVault is On.":
        raise RuntimeError("FileVault is required")


class _SanitizedParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise ValueError("invalid arguments")


def _authority(raw_argv: list[str]) -> tuple[TqqqOrchestrationAuthority, str]:
    parser = _SanitizedParser(add_help=False)
    parser.add_argument("--authority-receipt-sha256", required=True)
    parser.add_argument("--entitlement-receipt-sha256", required=True)
    parser.add_argument("--license-receipt-sha256", required=True)
    parser.add_argument("--retention-expires-at", required=True)
    parser.add_argument("--risk-standard-id", required=True)
    parser.add_argument("--risk-standard-sha256", required=True)
    parser.add_argument("--platform-execution-revision", required=True)
    parser.add_argument("--input-license", required=True)
    parser.add_argument("--input-usage-scope", required=True)
    parser.add_argument(
        "--session-mode",
        choices=tuple(_SESSION_PORT),
        default="paper",
    )
    args = parser.parse_args(raw_argv)
    return (
        TqqqOrchestrationAuthority(
            authority_receipt_sha256=args.authority_receipt_sha256,
            entitlement_receipt_sha256=args.entitlement_receipt_sha256,
            license_receipt_sha256=args.license_receipt_sha256,
            retention_expires_at=args.retention_expires_at,
            risk_standard_id=args.risk_standard_id,
            risk_standard_sha256=args.risk_standard_sha256,
            platform_execution_revision=args.platform_execution_revision,
            input_license=args.input_license,
            input_usage_scope=args.input_usage_scope,
        ),
        args.session_mode,
    )


def main(argv: list[str] | None = None) -> int:
    """Run the closed acquisition envelope and emit only a sanitized terminal."""
    try:
        authority, session_class = _authority(
            list(sys.argv[1:] if argv is None else argv)
        )
    except (TypeError, ValueError):
        print(
            json.dumps(
                {
                    **_SAFETY_BOUNDARY,
                    "asset_count": 0,
                    "lifecycle": [],
                    "status": "INVALID_ARGUMENTS",
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 2

    app: Any | None = None
    status = "PARK_MATERIAL"
    asset_count = 0
    snapshot_digest = None
    evidence_digest = None
    mandate_receipt_digest = None
    rerun_count = 0
    lifecycle: list[dict[str, Any]] = []
    orchestration_failure: dict[str, Any] | None = None
    strict_history_failure: dict[str, Any] | None = None

    def retain_strict_history_failure(
        symbol: str,
        strict_complete_input_count: int,
        error: StrictAdjustedHistoryError,
    ) -> None:
        nonlocal strict_history_failure
        if error.diagnostic is not None:
            strict_history_failure = {
                **error.diagnostic.to_dict(),
                "failing_symbol": symbol,
                "strict_complete_input_count": strict_complete_input_count,
            }

    try:
        _require_filevault_local_root()
        runner_revision, runner_tree_sha = resolve_tqqq_runtime_identity()
        app, contract_factory = _runtime()
        client_id = secrets.randbelow(2_000_000_000) + 1
        app.connect(_HOST, _SESSION_PORT[session_class], client_id)
        if not app.isConnected():
            raise RuntimeError("IBKR transport unavailable")
        app.start_reader()
        if not app.wait_for_handshake():
            raise RuntimeError("IBKR handshake unavailable")
        results = run_exact_acquisition(
            app,
            contract_factory=contract_factory,
            on_strict_history_failure=retain_strict_history_failure,
        )
        asset_count = len(results)
        outcome = orchestrate_tqqq_promotion(
            results,
            authority=authority,
            output_root=_LOCAL_RESEARCH_ROOT,
            runner_revision=runner_revision,
            runner_tree_sha=runner_tree_sha,
            session_class=session_class,
        )
        if outcome.get("asset_count") != len(EXACT_ASSETS) or any(
            outcome.get(field) is not expected
            for field, expected in _SAFETY_BOUNDARY.items()
        ):
            raise TqqqOrchestrationError("invalid research safety boundary")
        status = outcome["status"]
        snapshot_digest = outcome["snapshot_digest"]
        evidence_digest = outcome["evidence_digest"]
        mandate_receipt_digest = outcome["mandate_receipt_digest"]
        rerun_count = outcome["rerun_count"]
    except TqqqOrchestrationError as exc:
        if exc.sanitized_failure is not None:
            orchestration_failure = dict(exc.sanitized_failure)
            snapshot_digest = orchestration_failure["snapshot_digest"]
            mandate_receipt_digest = orchestration_failure["mandate_receipt_digest"]
    except Exception:  # noqa: BLE001 - terminal output must remain sanitized
        pass
    finally:
        if app is not None:
            lifecycle = list(app.sanitized_lifecycle())
            if app.isConnected():
                app.disconnect()

    terminal = {
        **_SAFETY_BOUNDARY,
        "asset_count": asset_count,
        "evidence_digest": evidence_digest,
        "lifecycle": lifecycle,
        "mandate_receipt_digest": mandate_receipt_digest,
        "rerun_count": rerun_count,
        "snapshot_digest": snapshot_digest,
        "status": status,
    }
    if strict_history_failure is not None:
        if lifecycle and "terminal_trigger" in lifecycle[-1]:
            strict_history_failure["terminal_trigger"] = lifecycle[-1]["terminal_trigger"]
        terminal["strict_history_failure"] = strict_history_failure
    if orchestration_failure is not None:
        terminal["orchestration_failure"] = orchestration_failure
    print(json.dumps(terminal, sort_keys=True, separators=(",", ":")))
    return (
        0
        if status == "VALIDATED_EVIDENCE_V2_AWAITING_HUMAN_PROMOTION_ACCEPTANCE"
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
