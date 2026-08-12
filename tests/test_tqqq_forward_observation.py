from __future__ import annotations

import hashlib
import inspect
import json
import plistlib
import stat
import subprocess
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from quant_platform_kit.ibkr import (
    AdjustedHistoricalCandle,
    StrictAdjustedHistoryDiagnostic,
    StrictAdjustedHistoryProvenance,
    StrictAdjustedHistoryResult,
)

from scripts import install_tqqq_forward_observation_launchagent as installer
from us_equity_snapshot_pipelines import tqqq_forward_observation_cli as cli
from us_equity_snapshot_pipelines.lifecycle import tqqq_forward_observation as forward


RUNTIME_COMMIT = "a" * 40
EMPTY_SESSIONS_SHA256 = hashlib.sha256(b"[]").hexdigest()


def _private_json(path: Path, payload: object) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    path.write_bytes(encoded)
    path.chmod(0o600)
    return hashlib.sha256(encoded).hexdigest()


def _authority(tmp_path: Path, *, runtime_commit: str = RUNTIME_COMMIT) -> tuple[Path, str]:
    plan = tmp_path / "plan.json"
    plan_sha = _private_json(
        plan,
        {
            "plan": {
                "contract": {"plan_id": forward.PLAN_ID},
                "plan_sha256": forward.PLAN_SHA256,
            }
        },
    )
    entitlement = tmp_path / "entitlement.json"
    license_terms = tmp_path / "license.json"
    entitlement_sha = _private_json(entitlement, {"current_exact_four_input_access": True})
    license_sha = _private_json(
        license_terms,
        {"personal_internal_noncommercial_nondisplay_daily_use": True},
    )
    authority = tmp_path / "authority.json"
    authority_sha = _private_json(
        authority,
        {
            "authority_scope": "RESEARCH_ONLY",
            "candidate_contract_sha256": forward.CANDIDATE_CONTRACT_SHA256,
            "collector_commit": runtime_commit,
            "entitlement_receipt": {
                "path": str(entitlement),
                "sha256": entitlement_sha,
            },
            "license_source_terms_receipt": {
                "path": str(license_terms),
                "sha256": license_sha,
            },
            "live_ready": False,
            "no_order": True,
            "plan_id": forward.PLAN_ID,
            "plan_sha256": forward.PLAN_SHA256,
            "plan_receipt_sha256": plan_sha,
            "promotion_eligible": False,
            "provider_identity": {
                "application_call_ceiling": 8,
                "deploy_target": "local",
                "ordered_symbols": list(forward.ORDERED_SYMBOLS),
                "provider_kind": "ibkr_gateway",
                "session_class": "live-data-only",
                "source_identity_sha256": forward.SOURCE_CONTRACT_SHA256,
            },
            "local_adapter": {
                "gateway_authenticated": True,
                "host": "127.0.0.1",
                "listener_loopback_only": True,
                "no_other_local_api_client": True,
                "port": 4001,
            },
            "retention_expires_at": forward.RETENTION_EXPIRES_AT,
            "scheduling": {
                "gateway_availability_confirmed": True,
                "per_session_once_guaranteed": True,
            },
            "sessions_sha256": forward.SESSIONS_SHA256,
            "size_zero_required": True,
            "source_contract_sha256": forward.SOURCE_CONTRACT_SHA256,
        },
    )
    return authority, authority_sha


def _plan_args(authority: Path) -> dict[str, object]:
    return {
        "plan_receipt": authority.with_name("plan.json"),
        "plan_receipt_sha256": json.loads(authority.read_bytes())["plan_receipt_sha256"],
    }


def _result(symbol: str, session: date, end_datetime: datetime) -> StrictAdjustedHistoryResult:
    return StrictAdjustedHistoryResult(
        candles=(
            AdjustedHistoricalCandle(
                session=session,
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=1234.0,
            ),
        ),
        provenance=StrictAdjustedHistoryProvenance(
            symbol=symbol,
            exchange="SMART",
            currency="USD",
            end_datetime=end_datetime.isoformat().replace("+00:00", "Z"),
            duration="1 D",
            bar_size="1 day",
            what_to_show="ADJUSTED_LAST",
            use_rth=True,
            format_date=1,
            keep_up_to_date=False,
            returned_row_count=1,
        ),
        diagnostic=StrictAdjustedHistoryDiagnostic(
            classification="exact_match",
            completion_observed=True,
            expected_count=1,
            observed_in_window_count=1,
            missing_count=0,
            extra_count=0,
            duplicate_count=0,
            missing_sessions_sha256=EMPTY_SESSIONS_SHA256,
            extra_sessions_sha256=EMPTY_SESSIONS_SHA256,
            duplicate_sessions_sha256=EMPTY_SESSIONS_SHA256,
            provider_error_code_counts=(),
        ),
    )


def test_frozen_calendar_and_no_session_skip_make_zero_provider_calls(
    tmp_path: Path,
) -> None:
    authority, authority_sha = _authority(tmp_path)
    calls: list[str] = []

    assert len(forward.frozen_sessions()) == 252
    assert forward.frozen_sessions()[0] == date(2026, 8, 13)
    assert forward.frozen_sessions()[-1] == date(2027, 8, 13)
    assert forward._session_close(date(2026, 11, 27)) == datetime(
        2026, 11, 27, 18, 0, tzinfo=UTC
    )
    assert forward._session_close(date(2027, 7, 2)) == datetime(
        2027, 7, 2, 20, 0, tzinfo=UTC
    )
    result = cli._collect_local_once(
        output_root=tmp_path / "output",
        authority_receipt=authority,
        authority_receipt_sha256=authority_sha,
        **_plan_args(authority),
        runtime_commit=RUNTIME_COMMIT,
        acquire_symbol=lambda *_args: calls.append("provider"),
        now=datetime(2026, 8, 13, 19, 59, tzinfo=UTC),
    )

    assert result == forward.CollectionResult(
        status="NO_FROZEN_SESSION_READY",
        provider_application_calls=0,
        observation_sha256=None,
    )
    assert calls == []

    result_at_close = cli._collect_local_once(
        output_root=tmp_path / "output",
        authority_receipt=authority,
        authority_receipt_sha256=authority_sha,
        **_plan_args(authority),
        runtime_commit=RUNTIME_COMMIT,
        acquire_symbol=lambda *_args: calls.append("provider"),
        now=datetime(2026, 8, 13, 20, 0, 30, tzinfo=UTC),
    )
    assert result_at_close.status == "NO_FROZEN_SESSION_READY"
    assert calls == []


def test_portable_core_runs_with_injected_ledger_without_local_adapter(
    tmp_path: Path,
) -> None:
    authority, authority_sha = _authority(tmp_path)
    plan_path = Path(_plan_args(authority)["plan_receipt"])
    result = forward.collect_once(
        ledger=SimpleNamespace(completed_sessions=lambda: ()),
        authority_receipt=json.loads(authority.read_bytes()),
        authority_receipt_sha256=authority_sha,
        plan_receipt=json.loads(plan_path.read_bytes()),
        plan_receipt_sha256=str(_plan_args(authority)["plan_receipt_sha256"]),
        runtime_commit=RUNTIME_COMMIT,
        acquire_symbol=lambda *_args: pytest.fail("provider must not be called"),
        now=datetime(2026, 8, 13, 19, 59, tzinfo=UTC),
    )

    assert result.status == "NO_FROZEN_SESSION_READY"
    assert result.provider_application_calls == 0


def test_attempt_lock_precedes_ordered_eight_call_collection_and_atomic_private_publish(
    tmp_path: Path,
) -> None:
    authority, authority_sha = _authority(tmp_path)
    output = tmp_path / "output"
    calls: list[str] = []

    def acquire(symbol: str, session: date, end_datetime: datetime):
        started = output / "attempt-ledger" / f"{session.isoformat()}.started.json"
        assert started.is_file()
        assert stat.S_IMODE(started.stat().st_mode) == 0o600
        calls.append(symbol)
        return _result(symbol, session, end_datetime)

    result = cli._collect_local_once(
        output_root=output,
        authority_receipt=authority,
        authority_receipt_sha256=authority_sha,
        **_plan_args(authority),
        runtime_commit=RUNTIME_COMMIT,
        acquire_symbol=acquire,
        now=datetime(2026, 8, 13, 22, 15, tzinfo=UTC),
    )

    assert calls == ["QQQ", "TQQQ", "QQQM", "BOXX"]
    assert result.status == "COLLECTED"
    assert result.provider_application_calls == 8
    assert len(result.observation_sha256 or "") == 64
    observation = output / "observations" / result.observation_sha256
    assert {member.name for member in observation.iterdir()} == {"observation.json"}
    assert all(
        stat.S_IMODE(path.stat().st_mode) == (0o700 if path.is_dir() else 0o600)
        for path in (output, *output.rglob("*"))
    )
    payload = json.loads((observation / "observation.json").read_bytes())
    assert payload["plan_id"] == forward.PLAN_ID
    assert [item["symbol"] for item in payload["observations"]] == list(
        forward.ORDERED_SYMBOLS
    )


@pytest.mark.parametrize("failure_index", range(4))
def test_first_partial_failure_invalidates_plan_without_retry(
    tmp_path: Path, failure_index: int
) -> None:
    authority, authority_sha = _authority(tmp_path)
    output = tmp_path / "output"
    calls: list[str] = []

    def acquire(symbol: str, session: date, end_datetime: datetime):
        calls.append(symbol)
        if len(calls) - 1 == failure_index:
            raise RuntimeError("private provider message with prices and volumes")
        return _result(symbol, session, end_datetime)

    with pytest.raises(forward.ForwardObservationError, match="plan invalid"):
        cli._collect_local_once(
            output_root=output,
            authority_receipt=authority,
            authority_receipt_sha256=authority_sha,
            **_plan_args(authority),
            runtime_commit=RUNTIME_COMMIT,
            acquire_symbol=acquire,
            now=datetime(2026, 8, 13, 22, 15, tzinfo=UTC),
        )

    assert calls == list(forward.ORDERED_SYMBOLS[: failure_index + 1])
    invalid = (output / "PLAN_INVALID.json").read_text()
    assert stat.S_IMODE((output / "PLAN_INVALID.json").stat().st_mode) == 0o600
    for forbidden in ("provider message", "prices", "volumes", "2026-08-13"):
        assert forbidden not in invalid
    with pytest.raises(forward.ForwardObservationError, match="plan invalid"):
        cli._collect_local_once(
            output_root=output,
            authority_receipt=authority,
            authority_receipt_sha256=authority_sha,
            **_plan_args(authority),
            runtime_commit=RUNTIME_COMMIT,
            acquire_symbol=acquire,
            now=datetime(2026, 8, 13, 22, 20, tzinfo=UTC),
        )
    assert calls == list(forward.ORDERED_SYMBOLS[: failure_index + 1])


def test_crash_missing_duplicate_extra_and_symlink_state_fail_closed_before_provider(
    tmp_path: Path,
) -> None:
    authority, authority_sha = _authority(tmp_path)
    for condition in ("crash", "missing", "extra", "symlink", "orphan", "duplicate"):
        output = tmp_path / condition
        ledger = output / "attempt-ledger"
        ledger.mkdir(parents=True, mode=0o700)
        if condition == "crash":
            _private_json(ledger / "2026-08-13.started.json", {"plan_sha256": forward.PLAN_SHA256})
        elif condition == "missing":
            now = datetime(2026, 8, 17, 14, 0, tzinfo=UTC)
        elif condition == "extra":
            _private_json(ledger / "2026-08-12.started.json", {"plan_sha256": forward.PLAN_SHA256})
        elif condition == "symlink":
            target = tmp_path / "target"
            target.write_text("do not touch")
            (ledger / "2026-08-13.started.json").symlink_to(target)
        elif condition == "orphan":
            observation = output / "observations" / ("0" * 64)
            observation.mkdir(parents=True, mode=0o700)
            _private_json(observation / "observation.json", {"orphan": True})
        else:
            first = forward.frozen_sessions()[0]
            cli._collect_local_once(
                output_root=output,
                authority_receipt=authority,
                authority_receipt_sha256=authority_sha,
                **_plan_args(authority),
                runtime_commit=RUNTIME_COMMIT,
                acquire_symbol=lambda symbol, session, end: _result(symbol, session, end),
                now=datetime(2026, 8, 13, 22, 15, tzinfo=UTC),
            )
            second = forward.frozen_sessions()[1]
            _private_json(
                ledger / f"{second.isoformat()}.started.json",
                {
                    "plan_sha256": forward.PLAN_SHA256,
                    "session_sha256": hashlib.sha256(second.isoformat().encode()).hexdigest(),
                    "status": "STARTED_NO_RETRY",
                },
            )
            first_completed = json.loads(
                (ledger / f"{first.isoformat()}.completed.json").read_bytes()
            )
            _private_json(
                ledger / f"{second.isoformat()}.completed.json",
                first_completed,
            )
            now = datetime(2026, 8, 14, 22, 15, tzinfo=UTC)
        calls: list[str] = []
        with pytest.raises(forward.ForwardObservationError, match="plan invalid"):
            cli._collect_local_once(
                output_root=output,
                authority_receipt=authority,
                authority_receipt_sha256=authority_sha,
                **_plan_args(authority),
                runtime_commit=RUNTIME_COMMIT,
                acquire_symbol=lambda *_args, calls=calls: calls.append("provider"),
                now=(
                    now
                    if condition in {"missing", "duplicate"}
                    else datetime(2026, 8, 13, 22, 15, tzinfo=UTC)
                ),
            )
        assert calls == []


def test_authority_and_runtime_identity_fail_before_output_or_provider(
    tmp_path: Path,
) -> None:
    authority, _authority_sha = _authority(tmp_path)
    calls: list[str] = []
    payload = json.loads(authority.read_bytes())
    payload["provider_identity"]["source_identity_sha256"] = "0" * 64
    bad_sha = _private_json(authority, payload)

    with pytest.raises(forward.ForwardObservationError, match="authority"):
        cli._collect_local_once(
            output_root=tmp_path / "output",
            authority_receipt=authority,
            authority_receipt_sha256=bad_sha,
            **_plan_args(authority),
            runtime_commit=RUNTIME_COMMIT,
            acquire_symbol=lambda *_args: calls.append("provider"),
            now=datetime(2026, 8, 13, 22, 15, tzinfo=UTC),
        )
    assert calls == []
    assert not (tmp_path / "output").exists()


def test_local_adapter_gate_fails_before_runtime_or_provider(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    authority, _ = _authority(tmp_path)
    payload = json.loads(authority.read_bytes())
    payload["local_adapter"]["listener_loopback_only"] = False
    authority_sha = _private_json(authority, payload)
    runtime_calls: list[str] = []
    monkeypatch.setattr(cli, "_require_filevault", lambda: None)
    monkeypatch.setattr(cli, "_runtime", lambda _root: runtime_calls.append("runtime"))

    assert cli.main(
        [
            "--authority-receipt",
            str(authority),
            "--authority-receipt-sha256",
            authority_sha,
            "--ibapi-root",
            str(tmp_path / "ibapi"),
            "--output-root",
            str(tmp_path / "output"),
            "--plan-receipt",
            str(_plan_args(authority)["plan_receipt"]),
            "--plan-receipt-sha256",
            str(_plan_args(authority)["plan_receipt_sha256"]),
            "--plan-sha256",
            forward.PLAN_SHA256,
            "--runtime-commit",
            RUNTIME_COMMIT,
        ]
    ) == 1
    assert runtime_calls == []
    assert json.loads(capsys.readouterr().out)["status"] == "PARK_MATERIAL"


def test_atomic_publication_failure_invalidates_without_visible_observation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    authority, authority_sha = _authority(tmp_path)
    output = tmp_path / "output"
    calls: list[str] = []

    def acquire(symbol: str, session: date, end_datetime: datetime):
        calls.append(symbol)
        return _result(symbol, session, end_datetime)

    monkeypatch.setattr(
        cli,
        "_publish_noreplace",
        lambda *_args: (_ for _ in ()).throw(OSError("simulated publication failure")),
    )
    with pytest.raises(forward.ForwardObservationError, match="plan invalid"):
        cli._collect_local_once(
            output_root=output,
            authority_receipt=authority,
            authority_receipt_sha256=authority_sha,
            **_plan_args(authority),
            runtime_commit=RUNTIME_COMMIT,
            acquire_symbol=acquire,
            now=datetime(2026, 8, 13, 22, 15, tzinfo=UTC),
        )

    assert calls == list(forward.ORDERED_SYMBOLS)
    assert list((output / "observations").iterdir()) == []
    assert (output / "PLAN_INVALID.json").is_file()


def test_core_rejects_ledger_digest_that_does_not_bind_published_payload(
    tmp_path: Path,
) -> None:
    authority, authority_sha = _authority(tmp_path)
    reasons: list[str] = []

    def invalidate(reason: str):
        reasons.append(reason)
        raise forward.ForwardObservationError("plan invalid")

    ledger = SimpleNamespace(
        completed_sessions=lambda: (),
        start_session=lambda _session: None,
        publish_observation=lambda _payload: "0" * 64,
        complete_session=lambda *_args: pytest.fail("must not complete"),
        invalidate=invalidate,
    )
    plan_path = Path(_plan_args(authority)["plan_receipt"])
    with pytest.raises(forward.ForwardObservationError, match="plan invalid"):
        forward.collect_once(
            ledger=ledger,
            authority_receipt=json.loads(authority.read_bytes()),
            authority_receipt_sha256=authority_sha,
            plan_receipt=json.loads(plan_path.read_bytes()),
            plan_receipt_sha256=str(_plan_args(authority)["plan_receipt_sha256"]),
            runtime_commit=RUNTIME_COMMIT,
            acquire_symbol=lambda symbol, session, end: _result(symbol, session, end),
            now=datetime(2026, 8, 13, 22, 15, tzinfo=UTC),
        )
    assert reasons == ["MATERIAL_COLLECTION_FAILURE"]


def test_swapped_completed_observations_invalidate_before_provider(tmp_path: Path) -> None:
    authority, authority_sha = _authority(tmp_path)
    output = tmp_path / "output"
    for now in (
        datetime(2026, 8, 13, 22, 15, tzinfo=UTC),
        datetime(2026, 8, 14, 22, 15, tzinfo=UTC),
    ):
        cli._collect_local_once(
            output_root=output,
            authority_receipt=authority,
            authority_receipt_sha256=authority_sha,
            **_plan_args(authority),
            runtime_commit=RUNTIME_COMMIT,
            acquire_symbol=lambda symbol, session, end: _result(symbol, session, end),
            now=now,
        )
    first, second = forward.frozen_sessions()[:2]
    first_path = output / "attempt-ledger" / f"{first.isoformat()}.completed.json"
    second_path = output / "attempt-ledger" / f"{second.isoformat()}.completed.json"
    first_payload = json.loads(first_path.read_bytes())
    second_payload = json.loads(second_path.read_bytes())
    first_payload["observation_sha256"], second_payload["observation_sha256"] = (
        second_payload["observation_sha256"],
        first_payload["observation_sha256"],
    )
    _private_json(first_path, first_payload)
    _private_json(second_path, second_payload)
    calls: list[str] = []

    with pytest.raises(forward.ForwardObservationError, match="plan invalid"):
        cli._collect_local_once(
            output_root=output,
            authority_receipt=authority,
            authority_receipt_sha256=authority_sha,
            **_plan_args(authority),
            runtime_commit=RUNTIME_COMMIT,
            acquire_symbol=lambda *_args: calls.append("provider"),
            now=datetime(2026, 8, 17, 22, 15, tzinfo=UTC),
        )
    assert calls == []


def test_wrong_provenance_cutoff_invalidates_plan(tmp_path: Path) -> None:
    authority, authority_sha = _authority(tmp_path)

    with pytest.raises(forward.ForwardObservationError, match="plan invalid"):
        cli._collect_local_once(
            output_root=tmp_path / "output",
            authority_receipt=authority,
            authority_receipt_sha256=authority_sha,
            **_plan_args(authority),
            runtime_commit=RUNTIME_COMMIT,
            acquire_symbol=lambda symbol, session, end: _result(
                symbol, session, end.replace(minute=2)
            ),
            now=datetime(2026, 8, 13, 22, 15, tzinfo=UTC),
        )


def test_cli_terminal_is_sanitized_and_bounds_ninth_application_call(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    authority, authority_sha = _authority(tmp_path)
    seen: dict[str, object] = {}
    monkeypatch.setattr(cli, "_require_filevault", lambda _root: None)
    monkeypatch.setattr(
        cli,
        "resolve_tqqq_runtime_identity",
        lambda: (RUNTIME_COMMIT, "b" * 40),
    )
    monkeypatch.setattr(cli, "_runtime", lambda _root: (SimpleNamespace(), object()))

    def collect_once(**kwargs):
        seen.update(kwargs)
        return forward.CollectionResult("COLLECTED", 8, "b" * 64)

    monkeypatch.setattr(cli, "collect_once", collect_once)
    assert cli.main(
        [
            "--authority-receipt",
            str(authority),
            "--authority-receipt-sha256",
            authority_sha,
            "--ibapi-root",
            str(tmp_path / "ibapi"),
            "--output-root",
            str(tmp_path / "output"),
            "--plan-receipt",
            str(_plan_args(authority)["plan_receipt"]),
            "--plan-receipt-sha256",
            str(_plan_args(authority)["plan_receipt_sha256"]),
            "--plan-sha256",
            forward.PLAN_SHA256,
            "--runtime-commit",
            RUNTIME_COMMIT,
        ]
    ) == 0
    terminal = json.loads(capsys.readouterr().out)
    assert terminal == {
        "no_order": True,
        "observation_sha256": "b" * 64,
        "plan_sha256": forward.PLAN_SHA256,
        "provider_application_call_ceiling": 8,
        "provider_application_calls": 8,
        "size_zero_required": True,
        "status": "COLLECTED",
    }
    assert callable(seen["acquire_symbol"])

    app = SimpleNamespace(
        reqContractDetails=lambda *_args: None,
        reqHistoricalData=lambda *_args: None,
        cancelHistoricalData=lambda *_args: None,
    )
    bounded = cli._bound_application_calls(app)
    for _ in range(8):
        bounded.reqHistoricalData()
    with pytest.raises(forward.ForwardObservationError, match="ceiling"):
        bounded.cancelHistoricalData()


def test_cli_reports_failed_provider_calls_and_sanitizes_teardown(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    authority, authority_sha = _authority(tmp_path)
    app = SimpleNamespace(provider_application_calls=3)
    app.isConnected = lambda: (_ for _ in ()).throw(RuntimeError("private socket state"))
    monkeypatch.setattr(cli, "_require_filevault", lambda _root: None)
    monkeypatch.setattr(
        cli,
        "resolve_tqqq_runtime_identity",
        lambda: (RUNTIME_COMMIT, "b" * 40),
    )
    monkeypatch.setattr(cli, "_runtime", lambda _root: (app, object()))
    monkeypatch.setattr(
        cli,
        "collect_once",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("private provider payload")),
    )

    assert cli.main(
        [
            "--authority-receipt",
            str(authority),
            "--authority-receipt-sha256",
            authority_sha,
            "--ibapi-root",
            str(tmp_path / "ibapi"),
            "--output-root",
            str(tmp_path / "output"),
            "--plan-receipt",
            str(_plan_args(authority)["plan_receipt"]),
            "--plan-receipt-sha256",
            str(_plan_args(authority)["plan_receipt_sha256"]),
            "--plan-sha256",
            forward.PLAN_SHA256,
            "--runtime-commit",
            RUNTIME_COMMIT,
        ]
    ) == 1
    terminal = json.loads(capsys.readouterr().out)
    assert terminal["status"] == "PARK_MATERIAL"
    assert terminal["provider_application_calls"] == 3


def test_cli_actual_runtime_revision_mismatch_fails_before_provider_runtime(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    authority, authority_sha = _authority(tmp_path)
    runtime_calls: list[str] = []
    monkeypatch.setattr(cli, "_require_filevault", lambda _root: None)
    monkeypatch.setattr(
        cli,
        "resolve_tqqq_runtime_identity",
        lambda: ("f" * 40, "b" * 40),
    )
    monkeypatch.setattr(cli, "_runtime", lambda _root: runtime_calls.append("runtime"))

    assert cli.main(
        [
            "--authority-receipt",
            str(authority),
            "--authority-receipt-sha256",
            authority_sha,
            "--ibapi-root",
            str(tmp_path / "ibapi"),
            "--output-root",
            str(tmp_path / "output"),
            "--plan-receipt",
            str(_plan_args(authority)["plan_receipt"]),
            "--plan-receipt-sha256",
            str(_plan_args(authority)["plan_receipt_sha256"]),
            "--plan-sha256",
            forward.PLAN_SHA256,
            "--runtime-commit",
            RUNTIME_COMMIT,
        ]
    ) == 1
    assert runtime_calls == []
    assert json.loads(capsys.readouterr().out)["status"] == "PARK_MATERIAL"


def test_selected_output_volume_must_be_filevault_encrypted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def run(command, **_kwargs):
        if command[:2] == ["/usr/bin/fdesetup", "status"]:
            return subprocess.CompletedProcess(command, 0, stdout="FileVault is On.\n", stderr="")
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=plistlib.dumps({"Encryption": False, "FileVault": False}),
            stderr=b"",
        )

    monkeypatch.setattr(cli.subprocess, "run", run)
    with pytest.raises(forward.ForwardObservationError, match="FileVault"):
        cli._require_filevault(tmp_path / "output")


def test_launchagent_contract_is_fixed_private_and_never_run_at_load(tmp_path: Path) -> None:
    authority, authority_sha = _authority(tmp_path)
    payload = installer.build_launch_agent_plist(
        runtime_python=Path("/fixed/runtime/bin/python"),
        ibapi_root=Path("/fixed/runtime/ibapi"),
        authority_receipt=authority,
        authority_receipt_sha256=authority_sha,
        plan_receipt=Path(_plan_args(authority)["plan_receipt"]),
        plan_receipt_sha256=str(_plan_args(authority)["plan_receipt_sha256"]),
        output_root=Path("/Users/test/.local/share/qsl/tqqq-forward-observation"),
        runtime_commit=RUNTIME_COMMIT,
        stdout_path=Path("/private/log/stdout.log"),
        stderr_path=Path("/private/log/stderr.log"),
    )
    plist = plistlib.loads(payload)
    assert plist["Label"] == installer.LAUNCH_AGENT_LABEL
    assert plist["StartCalendarInterval"] == {"Hour": 6, "Minute": 15}
    assert plist["Umask"] == 0o77
    assert "RunAtLoad" not in plist
    assert "KeepAlive" not in plist
    assert plist["ProgramArguments"] == [
        "/fixed/runtime/bin/python",
        "-m",
        "us_equity_snapshot_pipelines.tqqq_forward_observation_cli",
        "--authority-receipt",
        str(authority),
        "--authority-receipt-sha256",
        authority_sha,
        "--ibapi-root",
        "/fixed/runtime/ibapi",
        "--output-root",
        "/Users/test/.local/share/qsl/tqqq-forward-observation",
        "--plan-receipt",
        str(_plan_args(authority)["plan_receipt"]),
        "--plan-receipt-sha256",
        str(_plan_args(authority)["plan_receipt_sha256"]),
        "--plan-sha256",
        forward.PLAN_SHA256,
        "--runtime-commit",
        RUNTIME_COMMIT,
    ]


def test_activation_deadline_precedes_first_non_run_at_load_trigger() -> None:
    assert installer._FIRST_COLLECTION_DEADLINE == datetime(
        2026, 8, 13, 22, 0, tzinfo=UTC
    )


def test_installer_runtime_identity_must_match_fixed_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        installer.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            [], 0, stdout=("f" * 40) + "\n", stderr=""
        ),
    )
    assert installer._runtime_commit(Path("/fixed/runtime/bin/python")) == "f" * 40


def test_installer_runtime_module_must_resolve_inside_selected_interpreter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        installer.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            [], 0, stdout="/outside/checkout/us_equity_snapshot_pipelines/__init__.py\n", stderr=""
        ),
    )
    with pytest.raises(ValueError, match="runtime identity"):
        installer._runtime_module(Path("/fixed/runtime/bin/python"))


def test_installer_filesystem_failure_returns_parked_without_launchctl(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    launch_calls: list[object] = []
    monkeypatch.setattr(installer, "_activation_gate", lambda **_kwargs: True)
    monkeypatch.setattr(
        installer.os,
        "chmod",
        lambda *_args: (_ for _ in ()).throw(OSError("private filesystem detail")),
    )
    monkeypatch.setattr(
        installer.subprocess,
        "run",
        lambda *args, **kwargs: launch_calls.append((args, kwargs)),
    )

    assert installer.install_launch_agent(
        destination=tmp_path / "LaunchAgents" / "collector.plist",
        runtime_python=tmp_path / "runtime" / "python",
        ibapi_root=tmp_path / "runtime" / "ibapi",
        authority_receipt=tmp_path / "authority.json",
        authority_receipt_sha256="0" * 64,
        plan_receipt=tmp_path / "plan.json",
        plan_receipt_sha256="0" * 64,
        output_root=tmp_path / "output",
        runtime_commit=RUNTIME_COMMIT,
        now=datetime(2026, 8, 13, 0, 0, tzinfo=UTC),
    ) is False
    assert launch_calls == []


def test_activation_fails_closed_before_plist_or_launchctl_on_missing_gate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    launch_calls: list[object] = []
    monkeypatch.setattr(installer, "_activation_gate", lambda **_kwargs: False)
    monkeypatch.setattr(
        installer.subprocess,
        "run",
        lambda *args, **kwargs: launch_calls.append((args, kwargs)),
    )
    destination = tmp_path / "LaunchAgents" / "collector.plist"

    assert installer.install_launch_agent(
        destination=destination,
        runtime_python=tmp_path / "runtime" / "python",
        ibapi_root=tmp_path / "runtime" / "ibapi",
        authority_receipt=tmp_path / "missing.json",
        authority_receipt_sha256="0" * 64,
        plan_receipt=tmp_path / "missing-plan.json",
        plan_receipt_sha256="0" * 64,
        output_root=tmp_path / "output",
        runtime_commit=RUNTIME_COMMIT,
        now=datetime(2026, 8, 13, 0, 0, tzinfo=UTC),
    ) is False
    assert not destination.exists()
    assert launch_calls == []


def test_committed_surface_has_no_account_order_or_performance_readback() -> None:
    combined = "\n".join(
        (
            inspect.getsource(forward),
            inspect.getsource(cli),
            inspect.getsource(installer),
        )
    ).casefold()
    for forbidden in (
        "reqaccount",
        "reqpositions",
        "reqopenorders",
        "placeorder",
        "cancelorder",
        "reqexecutions",
        "totalcashvalue",
        "netliquidation",
        "sharpe",
        "max_drawdown",
        "mdd",
        "pnl",
        "returns",
        "turnover",
        "allocation_outcome",
        "gcs",
    ):
        assert forbidden not in combined


def test_portable_core_has_no_scheduler_platform_cloud_or_quantconnect_imports() -> None:
    source = inspect.getsource(forward)
    assert "darwin" not in source.casefold()
    assert "launchd" not in source.casefold()
    assert "launchctl" not in source.casefold()
    assert '"deploy_target": "local"' not in source
    for forbidden_import in (
        "import os",
        "import shutil",
        "import stat",
        "import tempfile",
        "from pathlib import path",
        "from quant_platform_kit.cloud.local",
        "from quant_platform_kit.cloud.gcp",
        "from quant_platform_kit.cloud.aws",
        "from quant_platform_kit.cloud.azure",
        "from quant_platform_kit.quantconnect",
        "import google",
        "from google",
        "import quantconnect",
        "from quantconnect",
        "import clr",
        "from algorithmimports",
    ):
        assert forbidden_import not in source.casefold()
    assert "future platform adapter route must re-freeze" in forward.__doc__
    assert "cannot be joined to this encrypted forward holdout" in forward.__doc__
    assert "infrastructure portability is not evidence equivalence" in forward.__doc__.casefold()
    assert "must not read the encrypted forward payload" in forward.__doc__
    assert "QPK-first" in forward.__doc__
