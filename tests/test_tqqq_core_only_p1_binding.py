from __future__ import annotations

import hashlib
import inspect
import json
from pathlib import Path

import pytest
from quant_platform_kit.data.research_input import research_input_manifest_sha256

from scripts import acquire_tqqq_core_only_p1_inputs_ibkr as acquisition_cli
from scripts import bind_tqqq_core_only_p1_input as cli
from us_equity_snapshot_pipelines.lifecycle import tqqq_core_only_p1_binding as binding


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
    cli_source = inspect.getsource(cli).lower()

    for forbidden in ("ibapi", "gateway", "credential", "reqhistoricaldata", "placeorder"):
        assert forbidden not in source
        assert forbidden not in cli_source


class _FakeHistoricalBarsProvider:
    def __init__(self, *, fail_on: str | None = None) -> None:
        self.calls: list[dict[str, str]] = []
        self.fail_on = fail_on

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
        return {"bars": [{"close": 100.0, "session": "2026-07-31"}]}


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


def _producer() -> dict[str, str]:
    return {
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "commit_sha": "a" * 40,
        "tree_sha": "b" * 40,
        "tool": "tqqq_core_only_p1_data_only_acquisition",
        "tool_version": "v1",
    }


def test_injected_four_input_provider_publishes_private_qpk_manifest(tmp_path: Path) -> None:
    output = tmp_path / "immutable-input"
    provider = _FakeHistoricalBarsProvider()

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


def test_published_root_rejects_tampering_and_clobbering(tmp_path: Path) -> None:
    output = tmp_path / "immutable-input"
    acquisition_cli.publish_tqqq_core_only_p1_inputs(
        _FakeHistoricalBarsProvider(),
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
    assert app.sanitized_lifecycle() == ()


def test_data_only_publisher_has_no_order_or_p3_reachability() -> None:
    source = inspect.getsource(acquisition_cli).lower()
    binding_source = inspect.getsource(binding).lower()

    for forbidden in ("place_order", "placeorder", "promotion", "replay", "tqqq_r1", "v3"):
        assert forbidden not in source
        assert forbidden not in binding_source
