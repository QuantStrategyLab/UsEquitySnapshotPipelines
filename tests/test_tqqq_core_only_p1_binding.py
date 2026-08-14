from __future__ import annotations

import hashlib
import inspect
import json
from pathlib import Path

from quant_platform_kit.data.research_input import research_input_manifest_sha256

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
