from __future__ import annotations

import hashlib
import inspect
import json
from queue import Empty, Queue
from pathlib import Path
from types import SimpleNamespace

import pytest
from quant_platform_kit.data.research_input import research_input_manifest_sha256

from scripts import acquire_tqqq_core_only_p1_inputs_ibkr as acquisition_cli
from scripts import bind_tqqq_core_only_p1_input as cli
from us_equity_snapshot_pipelines.lifecycle import tqqq_core_only_p1_binding as binding
from us_equity_snapshot_pipelines.lifecycle import tqqq_promotion_runner


def test_binding_freezes_new_candidate_source_and_data_identity() -> None:
    value = binding.build_tqqq_core_only_p1_binding()

    assert value["candidate"] == {
        "candidate_id": "tqqq_core_only_p2_v1",
        "config_sha256": "969cae10850f5a2d72c17fedd77689301411f62dc24d9a530026e3f7efdc1c69",
    }
    assert value["source"] == {
        "repository": "QuantStrategyLab/UsEquityStrategies",
        "revision": "8b6b418bac74318f8054c5951521c9b62391de3e",
    }
    assert value["data_identity"] == {
        "provider": "IBKR",
        "feed": "ADJUSTED_LAST",
        "calendar": {
            "calendar_id": "XNYS",
            "timezone": "America/New_York",
            "source": "exchange_calendars",
        },
        "adjustment": {
            "policy": "total_return_adjusted",
            "source": "IBKR_ADJUSTED_LAST",
        },
        "universe": ["QQQ", "TQQQ", "QQQM", "BOXX"],
        "date_cutoff": "2026-07-31",
        "cost_assumptions": {
            "turnover_cost_bps": 5.0,
            "stress_turnover_cost_bps": [10.0, 25.0],
            "borrow_cost_bps": 0.0,
            "cash_yield_assumption": 0.0,
            "execution_timing": "next_complete_trading_session_after_signal_effective_date",
        },
        "retention": {
            "policy": "PRIVATE_LOCAL_ENCRYPTED_RESEARCH_SNAPSHOT_NO_BACKUP_NO_REDISTRIBUTION",
            "redistribution_allowed": False,
        },
    }
    assert "paper" not in json.dumps(value, sort_keys=True).lower()
    assert "live" not in json.dumps(value, sort_keys=True).lower()


def test_future_manifest_is_validated_and_bound_to_the_static_binding() -> None:
    value = binding.build_tqqq_core_only_p1_binding()
    member = b'{"schema_version":"tqqq_core_only_private_bars.v1","symbols":{}}'
    manifest = binding.build_tqqq_core_only_input_manifest(
        value,
        observed_at="2026-08-14T00:00:00Z",
        producer={
            "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
            "commit_sha": "1" * 40,
            "tree_sha": "2" * 40,
            "tool": "tqqq_core_only_data_only_acquisition",
            "tool_version": "v1",
        },
        member_bytes=member,
        source_content_sha256={symbol: hashlib.sha256(symbol.encode()).hexdigest() for symbol in value["data_identity"]["universe"]},
    )

    assert manifest["research_input_contract_id"] == binding.INPUT_CONTRACT_ID
    assert manifest["profile"] == "tqqq_core_only_p2_v1"
    assert manifest["calendar"]["session_date"] == "2026-07-31"
    assert manifest["adjustment"] == {
        "policy": "total_return_adjusted",
        "source": "IBKR_ADJUSTED_LAST",
        "source_revision": binding.binding_sha256(value),
    }
    assert {source["source_id"] for source in manifest["sources"]} == {
        "ibkr_adjusted_last:BOXX",
        "ibkr_adjusted_last:QQQ",
        "ibkr_adjusted_last:QQQM",
        "ibkr_adjusted_last:TQQQ",
    }
    assert {source["revision"] for source in manifest["sources"]} == {binding.binding_sha256(value)}
    assert research_input_manifest_sha256(manifest) == binding.validate_tqqq_core_only_input_manifest(manifest, value)


def test_cli_writes_canonical_static_binding_only(tmp_path: Path, capsys) -> None:
    output = tmp_path / "binding"

    assert cli.main(["--output", str(output)]) == 0

    raw = (output / "binding.json").read_bytes()
    value = binding.build_tqqq_core_only_p1_binding()
    assert raw == binding.canonical_binding_bytes(value)
    assert json.loads(capsys.readouterr().out) == {
        "binding_sha256": binding.binding_sha256(value),
        "candidate_id": "tqqq_core_only_p2_v1",
        "status": "P1_DATA_ONLY_BINDING_COMPLETE",
    }


def test_binding_has_no_provider_runtime_or_order_path() -> None:
    source = inspect.getsource(binding).lower()

    for forbidden in ("ibapi", "gateway", "credential", "reqhistoricaldata", "placeorder"):
        assert forbidden not in source


class _FakeHistoricalBarsProvider:
    def __init__(
        self, *, fail_on: str | None = None, full_history: bool = False, partial_history: bool = False
    ) -> None:
        self.calls: list[dict[str, str]] = []
        self.fail_on = fail_on
        self.full_history = full_history
        self.partial_history = partial_history

    def fetch_historical_bars(
        self,
        *,
        symbol: str,
        calendar_id: str,
        timezone: str,
        adjustment_policy: str,
        feed: str,
        date_cutoff: str,
    ) -> dict[str, object]:
        self.calls.append(
            {
                "symbol": symbol,
                "calendar_id": calendar_id,
                "timezone": timezone,
                "adjustment_policy": adjustment_policy,
                "feed": feed,
                "date_cutoff": date_cutoff,
            }
        )
        if symbol == self.fail_on:
            raise RuntimeError("synthetic provider failure")
        bars = _bars_for(symbol)
        if self.partial_history:
            bars = bars[:-1]
        return {"bars": bars if self.full_history else bars[-1:]}


class _OfficialCallbackShapeWrapper:
    def __init__(self) -> None:
        pass


class _UnconnectedCallbackShapeClient:
    def __init__(self, wrapper: object) -> None:
        self.wrapper = wrapper
        self.history_calls: list[tuple[object, ...]] = []

    def isConnected(self) -> bool:
        return False

    def reqHistoricalData(self, *args: object) -> None:
        self.history_calls.append(args)

    def cancelHistoricalData(self, _request_id: int) -> None:
        pass

    def reqContractDetails(self, _request_id: int, _contract: object) -> None:
        pass

    def run(self) -> None:
        pass


class _CallbackShapeContract:
    pass


class _QueuedCallbackClient:
    def __init__(self, wrapper: object) -> None:
        self.wrapper = wrapper
        self.history_calls: list[tuple[object, ...]] = []
        self.request_envelopes: list[dict[str, object]] = []
        self.cancel_calls: list[int] = []
        self.history_bars: list[object] = []
        self.behavior = "success"
        self.behavior_by_request: dict[int, str] = {}
        self.emit_frozen_window_bars = False
        self.run_calls = 0
        self.disconnect_calls = 0
        self._requests: Queue[int] = Queue()
        self._reader_released = False

    def isConnected(self) -> bool:
        return True

    def reqHistoricalData(self, *args: object) -> None:
        self.history_calls.append(args)
        envelope = self.wrapper.last_tqqq_core_only_request_envelope
        assert isinstance(envelope, dict)
        self.request_envelopes.append(dict(envelope))
        request_id = args[0]
        assert isinstance(request_id, int)
        self._requests.put(request_id)

    def cancelHistoricalData(self, request_id: int) -> None:
        self.cancel_calls.append(request_id)

    def disconnect(self) -> None:
        self.disconnect_calls += 1
        self._reader_released = True

    def run(self) -> None:
        self.run_calls += 1
        if self.behavior == "reader_exit":
            return
        if self.behavior != "no_handshake":
            self.wrapper.nextValidId(1)
        while not self._reader_released:
            try:
                request_id = self._requests.get(timeout=0.01)
            except Empty:
                continue
            behavior = self.behavior_by_request.get(request_id, self.behavior)
            bars = self.history_bars
            if self.emit_frozen_window_bars:
                envelope = self.wrapper.last_tqqq_core_only_request_envelope
                assert isinstance(envelope, dict)
                bars = _bars_for_window(str(envelope["start_date"]), str(envelope["date_cutoff"]))
            if behavior == "error":
                self.wrapper.error(request_id, 0, 321, "synthetic provider detail", "")
            elif behavior in {"success", "partial"}:
                for bar in bars[:1] if behavior == "partial" else bars:
                    self.wrapper.historicalData(request_id, bar)
                self.wrapper.historicalDataEnd(request_id, "raw start", "raw end")
            elif behavior == "foreign_end":
                self.wrapper.historicalDataEnd(request_id + 1, "raw start", "raw end")


def _producer() -> dict[str, str]:
    return {
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "commit_sha": "a" * 40,
        "tree_sha": "b" * 40,
        "tool": "tqqq_core_only_p1_data_only_acquisition",
        "tool_version": "v1",
    }


def _bars_for(symbol: str) -> list[dict[str, object]]:
    eligible_start = {"QQQM": "2020-10-13", "BOXX": "2022-12-28"}.get(symbol)
    return [
        {
            "date": session.isoformat(),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "volume": 1.0,
        }
        for session in tqqq_promotion_runner._FROZEN_XNYS_SESSIONS
        if eligible_start is None or session.isoformat() >= eligible_start
    ]


def _bars_for_window(start_date: str, date_cutoff: str) -> list[object]:
    return [
        SimpleNamespace(
            date=session.strftime("%Y%m%d"),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.0,
            volume=1.0,
        )
        for session in tqqq_promotion_runner._FROZEN_XNYS_SESSIONS
        if start_date <= session.isoformat() <= date_cutoff
    ]


def test_injected_four_input_provider_publishes_private_qpk_manifest(tmp_path: Path) -> None:
    output = tmp_path / "immutable-input"
    provider = _FakeHistoricalBarsProvider(full_history=True)

    result = acquisition_cli.publish_tqqq_core_only_p1_inputs(
        provider,
        output_root=output,
        observed_at="2026-08-14T00:00:00Z",
        producer=_producer(),
    )

    assert [call["symbol"] for call in provider.calls] == ["QQQ", "TQQQ", "QQQM", "BOXX"]
    assert all(
        {
            "calendar_id": "XNYS",
            "timezone": "America/New_York",
            "adjustment_policy": "total_return_adjusted",
            "feed": "ADJUSTED_LAST",
            "date_cutoff": "2026-07-31",
        }.items()
        <= call.items()
        for call in provider.calls
    )
    assert (output.stat().st_mode & 0o777) == 0o700
    manifest = json.loads((output / "manifest.json").read_bytes())
    assert result["manifest_sha256"] == research_input_manifest_sha256(manifest)
    assert binding.verify_tqqq_core_only_input_root(output) == result["manifest_sha256"]


@pytest.mark.parametrize("full_history,partial_history", [(False, False), (True, True)])
def test_incomplete_provider_is_rejected_without_publishing_a_root(
    tmp_path: Path, full_history: bool, partial_history: bool
) -> None:
    output = tmp_path / "immutable-input"

    with pytest.raises(binding.TqqqCoreOnlyP1BindingError, match="historical coverage"):
        acquisition_cli.publish_tqqq_core_only_p1_inputs(
            _FakeHistoricalBarsProvider(full_history=full_history, partial_history=partial_history),
            output_root=output,
            observed_at="2026-08-14T00:00:00Z",
            producer=_producer(),
        )

    assert not output.exists()


def test_published_root_rejects_tampering_and_clobbering(tmp_path: Path) -> None:
    output = tmp_path / "immutable-input"
    acquisition_cli.publish_tqqq_core_only_p1_inputs(
        _FakeHistoricalBarsProvider(full_history=True),
        output_root=output,
        observed_at="2026-08-14T00:00:00Z",
        producer=_producer(),
    )
    (output / "bars.json").write_bytes(b"{}")

    with pytest.raises(binding.TqqqCoreOnlyP1BindingError, match="invalid TQQQ core-only input root"):
        binding.verify_tqqq_core_only_input_root(output)
    with pytest.raises(binding.TqqqCoreOnlyP1BindingError, match="immutable output already exists"):
        acquisition_cli.publish_tqqq_core_only_p1_inputs(
            _FakeHistoricalBarsProvider(),
            output_root=output,
            observed_at="2026-08-14T00:00:00Z",
            producer=_producer(),
        )


def test_provider_failure_stops_without_publishing_partial_root(tmp_path: Path) -> None:
    output = tmp_path / "immutable-input"
    provider = _FakeHistoricalBarsProvider(fail_on="TQQQ")

    with pytest.raises(binding.TqqqCoreOnlyP1BindingError, match="data-only acquisition failed"):
        acquisition_cli.publish_tqqq_core_only_p1_inputs(
            provider,
            output_root=output,
            observed_at="2026-08-14T00:00:00Z",
            producer=_producer(),
        )

    assert [call["symbol"] for call in provider.calls] == ["QQQ", "TQQQ"]
    assert not output.exists()


def test_cli_without_an_injected_provider_is_parked_without_publishing(tmp_path: Path, capsys) -> None:
    output = tmp_path / "immutable-input"

    assert (
        acquisition_cli.main(
            ["--output-root", str(output), "--observed-at", "2026-08-14T00:00:00Z"]
        )
        == 2
    )

    assert capsys.readouterr().out == '{"status":"PARKED"}\n'
    assert not output.exists()


def test_cli_provider_failure_emits_only_fixed_sanitized_lifecycle(
    tmp_path: Path, capsys
) -> None:
    output = tmp_path / "immutable-input"
    provider = _FakeHistoricalBarsProvider(fail_on="TQQQ")

    assert (
        acquisition_cli.main(
            ["--output-root", str(output), "--observed-at", "2026-08-14T00:00:00Z"],
            provider=provider,
            producer=_producer(),
        )
        == 2
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "candidate_id": "tqqq_core_only_p2_v1",
        "failure_class": "data_only_acquisition_failed",
        "request_id": None,
        "event_type": "historical_bars",
        "submitted": True,
        "completed": False,
        "count": 2,
        "source_commit": "a" * 40,
        "status": "PARKED",
    }
    assert not output.exists()


def test_publisher_callback_boundary_matches_official_interface_and_stays_provider_zero() -> None:
    app = acquisition_cli.build_tqqq_core_only_ibkr_callback_app(
        client_type=_UnconnectedCallbackShapeClient,
        wrapper_type=_OfficialCallbackShapeWrapper,
        contract_type=_CallbackShapeContract,
    )

    assert tuple(inspect.signature(type(app).error).parameters) == (
        "self",
        "reqId",
        "errorTime",
        "errorCode",
        "errorString",
        "advancedOrderRejectJson",
    )
    app.error(-1, 0, 2106, "synthetic", "")
    app.historicalDataEnd(1, "", "")
    assert app.isConnected() is False
    assert app.history_calls == []
    assert app.tqqq_core_only_terminal_state() == {"active_request_id": None, "terminal": "IDLE"}


def _fetch_tqqq_core_only_bars(app: object) -> dict[str, object]:
    return app.fetch_historical_bars(
        symbol="QQQ",
        calendar_id="XNYS",
        timezone="America/New_York",
        adjustment_policy="total_return_adjusted",
        feed="ADJUSTED_LAST",
        date_cutoff="2026-07-31",
    )


@pytest.mark.parametrize("behavior", ("no_handshake", "reader_exit"))
def test_callback_port_requires_reader_and_handshake_before_request(behavior: str) -> None:
    app = acquisition_cli.build_tqqq_core_only_ibkr_callback_app(
        client_type=_QueuedCallbackClient,
        wrapper_type=_OfficialCallbackShapeWrapper,
        contract_type=_CallbackShapeContract,
        history_watchdog_seconds=0.001,
    )
    app.behavior = behavior

    with pytest.raises(binding.TqqqCoreOnlyP1BindingError, match="data-only acquisition failed"):
        _fetch_tqqq_core_only_bars(app)
    app.close()

    assert app.run_calls == 1
    assert app.history_calls == []
    assert app.disconnect_calls == 1


def test_callback_app_uses_the_fixed_single_concurrency_annual_plan(tmp_path: Path) -> None:
    app = acquisition_cli.build_tqqq_core_only_ibkr_callback_app(
        client_type=_QueuedCallbackClient,
        wrapper_type=_OfficialCallbackShapeWrapper,
        contract_type=_CallbackShapeContract,
        history_watchdog_seconds=0.1,
    )
    app.emit_frozen_window_bars = True

    result = acquisition_cli.publish_tqqq_core_only_p1_inputs(
        app,
        output_root=tmp_path / "immutable-input",
        observed_at="2026-08-14T00:00:00Z",
        producer=_producer(),
    )

    assert result["status"] == "P1_DATA_ONLY_INPUTS_PUBLISHED"
    assert app.run_calls == 1
    assert app.disconnect_calls == 1
    assert [
        (envelope["symbol"], envelope["start_date"], envelope["date_cutoff"])
        for envelope in app.request_envelopes
    ] == [
        ("QQQ", "2018-01-02", "2018-07-31"),
        *[("QQQ", f"{year}-08-01", f"{year + 1}-07-31") for year in range(2018, 2026)],
        ("TQQQ", "2018-01-02", "2018-07-31"),
        *[("TQQQ", f"{year}-08-01", f"{year + 1}-07-31") for year in range(2018, 2026)],
        ("QQQM", "2020-10-13", "2021-07-31"),
        *[("QQQM", f"{year}-08-01", f"{year + 1}-07-31") for year in range(2021, 2026)],
        ("BOXX", "2022-12-28", "2023-07-31"),
        *[("BOXX", f"{year}-08-01", f"{year + 1}-07-31") for year in range(2023, 2026)],
    ]
    assert len(app.history_calls) == 28
    assert all(call[3:] == ("1 Y", "1 day", "ADJUSTED_LAST", 1, 1, False, []) for call in app.history_calls)


@pytest.mark.parametrize("behavior", ("error", "timeout", "foreign_end"))
def test_callback_app_halts_the_annual_plan_on_first_terminal_failure(
    tmp_path: Path, behavior: str
) -> None:
    app = acquisition_cli.build_tqqq_core_only_ibkr_callback_app(
        client_type=_QueuedCallbackClient,
        wrapper_type=_OfficialCallbackShapeWrapper,
        contract_type=_CallbackShapeContract,
        history_watchdog_seconds=0.001,
    )
    app.emit_frozen_window_bars = True
    app.behavior_by_request[1_000_002] = behavior
    output = tmp_path / "immutable-input"

    with pytest.raises(binding.TqqqCoreOnlyP1BindingError, match="data-only acquisition failed"):
        acquisition_cli.publish_tqqq_core_only_p1_inputs(
            app,
            output_root=output,
            observed_at="2026-08-14T00:00:00Z",
            producer=_producer(),
        )

    assert [
        (envelope["symbol"], envelope["start_date"], envelope["date_cutoff"])
        for envelope in app.request_envelopes
    ] == [
        ("QQQ", "2018-01-02", "2018-07-31"),
        ("QQQ", "2018-08-01", "2019-07-31"),
        ("QQQ", "2019-08-01", "2020-07-31"),
    ]
    assert app.cancel_calls == [1_000_002]
    assert app.disconnect_calls == 1
    assert not output.exists()


def test_callback_app_rejects_an_incomplete_chunk_before_the_next_request(tmp_path: Path) -> None:
    app = acquisition_cli.build_tqqq_core_only_ibkr_callback_app(
        client_type=_QueuedCallbackClient,
        wrapper_type=_OfficialCallbackShapeWrapper,
        contract_type=_CallbackShapeContract,
        history_watchdog_seconds=0.1,
    )
    app.emit_frozen_window_bars = True
    app.behavior = "partial"
    output = tmp_path / "immutable-input"

    with pytest.raises(binding.TqqqCoreOnlyP1BindingError, match="data-only acquisition failed"):
        acquisition_cli.publish_tqqq_core_only_p1_inputs(
            app,
            output_root=output,
            observed_at="2026-08-14T00:00:00Z",
            producer=_producer(),
        )

    assert len(app.history_calls) == 1
    assert app.cancel_calls == []
    assert app.disconnect_calls == 1
    assert not output.exists()


@pytest.mark.parametrize("behavior", ("error", "timeout", "foreign_end"))
def test_callback_port_fails_closed_on_error_or_timeout(behavior: str) -> None:
    app = acquisition_cli.build_tqqq_core_only_ibkr_callback_app(
        client_type=_QueuedCallbackClient,
        wrapper_type=_OfficialCallbackShapeWrapper,
        contract_type=_CallbackShapeContract,
        history_watchdog_seconds=0.001,
    )
    app.behavior = behavior

    with pytest.raises(binding.TqqqCoreOnlyP1BindingError, match="data-only acquisition failed"):
        _fetch_tqqq_core_only_bars(app)
    app.close()

    assert len(app.history_calls) == 1
    assert app.disconnect_calls == 1
    assert "synthetic provider detail" not in repr(app.tqqq_core_only_terminal_state())


def test_callback_port_requires_matching_end_and_normalizes_terminal_failures() -> None:
    app = acquisition_cli.build_tqqq_core_only_ibkr_callback_app(
        client_type=_QueuedCallbackClient,
        wrapper_type=_OfficialCallbackShapeWrapper,
        contract_type=_CallbackShapeContract,
        history_watchdog_seconds=0.001,
    )
    app.behavior = "timeout"
    app.history_bars = [SimpleNamespace(date="20180102", open=1.0, high=2.0, low=0.5, close=1.5, volume=10)]

    with pytest.raises(binding.TqqqCoreOnlyP1BindingError, match="data-only acquisition failed"):
        _fetch_tqqq_core_only_bars(app)
    app.close()

    assert app.disconnect_calls == 1


def test_callback_port_partial_history_cannot_publish_a_root(tmp_path: Path) -> None:
    app = acquisition_cli.build_tqqq_core_only_ibkr_callback_app(
        client_type=_QueuedCallbackClient,
        wrapper_type=_OfficialCallbackShapeWrapper,
        contract_type=_CallbackShapeContract,
        history_watchdog_seconds=0.001,
    )
    app.history_bars = [SimpleNamespace(date="20180102", open=1.0, high=2.0, low=0.5, close=1.5, volume=10)]
    app.behavior = "partial"
    output = tmp_path / "immutable-input"

    with pytest.raises(binding.TqqqCoreOnlyP1BindingError, match="data-only acquisition failed"):
        acquisition_cli.publish_tqqq_core_only_p1_inputs(
            app,
            output_root=output,
            observed_at="2026-08-14T00:00:00Z",
            producer=_producer(),
        )

    assert not output.exists()


def test_data_only_publisher_has_no_order_or_p3_reachability() -> None:
    source = inspect.getsource(acquisition_cli).lower()
    binding_source = inspect.getsource(binding).lower()

    for forbidden in ("place_order", "placeorder", "promotion", "replay", "tqqq_r1", "p3", "v3"):
        assert forbidden not in source
        assert forbidden not in binding_source
