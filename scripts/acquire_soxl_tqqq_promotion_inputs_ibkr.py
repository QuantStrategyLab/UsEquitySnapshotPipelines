#!/usr/bin/env python3
"""Exact one-transaction IBKR caller for frozen SOXL/TQQQ research inputs."""

from __future__ import annotations

import json
import secrets
import sys
from datetime import date, datetime
from typing import Any

from us_equity_snapshot_pipelines.lifecycle.soxl_adjusted_last_acquisition import (
    acquire_strict_adjusted_last,
    build_request_bound_ibkr_app,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_pit_input_packager import (
    FIRST_ELIGIBLE_SESSION,
    FROZEN_XNYS_SESSIONS,
    SOXL_PROMOTION_ASSETS,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_pit_regime_component_producer import (
    FIXED_CUTOFF,
)


EXACT_ASSETS = SOXL_PROMOTION_ASSETS
_EXACT_DURATIONS = {
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
_FIXED_CUTOFF = datetime.fromisoformat(FIXED_CUTOFF)
_HOST = "127.0.0.1"
_PAPER_GATEWAY_PORT = 4002


def run_exact_acquisition(
    app: Any,
    *,
    contract_factory: Any,
) -> dict[str, Any]:
    """Acquire the frozen nine inputs sequentially and stop on first failure."""
    results: dict[str, Any] = {}
    frozen_sessions = tuple(date.fromisoformat(item) for item in FROZEN_XNYS_SESSIONS)
    for symbol in EXACT_ASSETS:
        first_eligible = date.fromisoformat(
            FIRST_ELIGIBLE_SESSION.get(symbol, FROZEN_XNYS_SESSIONS[0])
        )
        expected_sessions = tuple(
            session for session in frozen_sessions if session >= first_eligible
        )
        duration = _EXACT_DURATIONS[symbol]

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

        results[symbol] = acquire_strict_adjusted_last(
            app,
            symbol,
            end_datetime=_FIXED_CUTOFF,
            duration=duration,
            expected_sessions=expected_sessions,
            stock_factory=contract_factory,
            requester=requester,
        )
    return results


def _runtime() -> tuple[Any, Any]:
    """Load the already-approved local official IBKR runtime without fallback."""
    try:
        from ibapi.client import EClient
        from ibapi.contract import Contract
        from ibapi.wrapper import EWrapper
    except ImportError:
        raise RuntimeError("approved local IBKR API runtime is unavailable") from None
    if (
        EClient.__module__ != "ibapi.client"
        or EWrapper.__module__ != "ibapi.wrapper"
        or Contract.__module__ != "ibapi.contract"
    ):
        raise RuntimeError("approved local IBKR API runtime identity mismatch")

    app = build_request_bound_ibkr_app(
        client_type=EClient,
        wrapper_type=EWrapper,
        contract_type=Contract,
        request_id_start=secrets.randbelow(2_000_000_000),
    )

    def stock_factory(symbol: str, exchange: str, currency: str) -> Any:
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = exchange
        contract.currency = currency
        return contract

    return app, stock_factory


def main(argv: list[str] | None = None) -> int:
    """Run the closed acquisition envelope and emit only a sanitized terminal."""
    if list(sys.argv[1:] if argv is None else argv):
        print(
            json.dumps(
                {"asset_count": 0, "lifecycle": [], "status": "INVALID_ARGUMENTS"},
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 2

    app: Any | None = None
    status = "FAILED_MATERIAL"
    asset_count = 0
    lifecycle: list[dict[str, Any]] = []
    try:
        app, contract_factory = _runtime()
        client_id = secrets.randbelow(2_000_000_000) + 1
        app.connect(_HOST, _PAPER_GATEWAY_PORT, client_id)
        if not app.isConnected():
            raise RuntimeError("IBKR transport unavailable")
        app.start_reader()
        if not app.wait_for_handshake():
            raise RuntimeError("IBKR handshake unavailable")
        results = run_exact_acquisition(app, contract_factory=contract_factory)
        asset_count = len(results)
        status = "STRICT_COMPLETE"
    except Exception:  # noqa: BLE001 - terminal output must remain sanitized
        status = "FAILED_MATERIAL"
    finally:
        if app is not None:
            lifecycle = list(app.sanitized_lifecycle())
            if app.isConnected():
                app.disconnect()

    print(
        json.dumps(
            {
                "asset_count": asset_count,
                "lifecycle": lifecycle,
                "status": status,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0 if status == "STRICT_COMPLETE" else 1


if __name__ == "__main__":
    raise SystemExit(main())
