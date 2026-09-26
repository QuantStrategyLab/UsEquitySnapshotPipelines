"""Direct synthetic checks for the separate R6 source and replay boundary."""

from __future__ import annotations

import json
import copy
from pathlib import Path

import pytest
from quant_platform_kit.data.multisource_assurance import (
    SOURCE_OBSERVATION_READY,
    DailyBar,
    DailyBarSourceObservation,
    DailyBarSourceSnapshot,
)

from scripts import soxl_v7_r6_twelve_single_source as tool
from us_equity_snapshot_pipelines.lifecycle import soxl_v7_twelve_single_source_r6 as r6
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_free_split_close_p1 import (
    SoxlCoreOnlyFreeSplitCloseP1Error,
    validate_soxl_core_only_free_split_close_p1_binding,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p1_binding import expected_soxl_core_only_sessions
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence import (
    build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_plan,
)


def _observations(*, missing_soxl: bool = False) -> dict[str, DailyBarSourceObservation]:
    result = {}
    for symbol, days in expected_soxl_core_only_sessions(r6.DATE_CUTOFF).items():
        if missing_soxl and symbol == "SOXL":
            days = days[:-1]
        bars = tuple(
            DailyBar(
                session_date=day.isoformat(), open=float(index + 100), high=float(index + 100),
                low=float(index + 100), close=float(index + 100), volume=1000.0,
            )
            for index, day in enumerate(days)
        )
        source_sha = r6.sha(r6.canonical({
            "source_id": r6.TWELVE_DATA_DAILY_SOURCE_ID,
            "symbol": symbol,
            "start_date": expected_soxl_core_only_sessions(r6.DATE_CUTOFF)[symbol][0].isoformat(),
            "date_cutoff": r6.DATE_CUTOFF,
            "adjustment_basis": "split_adjusted",
            "bars": [bar.to_dict() for bar in bars],
        }))
        snapshot = DailyBarSourceSnapshot(
            source_id=r6.TWELVE_DATA_DAILY_SOURCE_ID, symbol=symbol, date_cutoff=r6.DATE_CUTOFF,
            adjustment_basis="split_adjusted", source_artifact_sha256=source_sha, bars=bars,
        )
        result[symbol] = DailyBarSourceObservation(
            source_id=r6.TWELVE_DATA_DAILY_SOURCE_ID,
            status=SOURCE_OBSERVATION_READY,
            snapshot=snapshot,
        )
    return result


def _producer() -> dict[str, str]:
    return {
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "commit_sha": "a" * 40, "tree_sha": "b" * 40,
        "tool": "soxl_v7_r6_twelve_single_source_test", "tool_version": "r6.v1",
    }


def test_r6_input_has_distinct_identity_and_reuses_v7_indicator_math() -> None:
    members = r6.build_input(_observations(), observed_at="2026-09-26T00:00:00Z", producer=_producer())
    manifest_sha, series = r6.verify_input(members)
    assert set(series) == set(r6.SYMBOLS)
    assert len(manifest_sha) == 64
    assert json.loads(members["assurance.json"])["cross_provider_verified"] is False
    with pytest.raises(SoxlCoreOnlyFreeSplitCloseP1Error):
        validate_soxl_core_only_free_split_close_p1_binding(json.loads(members["binding.json"]))
    materialized = r6.materialize_input(members)
    assert materialized["p1_identity"]["input_manifest_sha256"] == manifest_sha
    assert len(materialized["sessions"]) >= 756
    plan = build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_plan(materialized)
    assert len(plan["requests"]) == 15


def test_r6_rejects_missing_daily_session_and_mutated_source_hash() -> None:
    with pytest.raises(r6.R6InputError, match="coverage incomplete"):
        r6.build_input(_observations(missing_soxl=True), observed_at="2026-09-26T00:00:00Z", producer=_producer())
    members = r6.build_input(_observations(), observed_at="2026-09-26T00:00:00Z", producer=_producer())
    report = json.loads(members["assurance.json"])
    report["cross_provider_verified"] = True
    members["assurance.json"] = r6.canonical(report)
    with pytest.raises(r6.R6InputError):
        r6.verify_input(members)


def test_r6_readback_rejects_backdated_or_inconsistent_observation_time() -> None:
    members = r6.build_input(_observations(), observed_at="2026-09-26T00:00:00Z", producer=_producer())
    manifest = json.loads(members["manifest.json"])
    for field in ("observed_at", "effective_at", "as_of"):
        manifest[field] = "2020-01-01T00:00:00Z"
    for source in manifest["sources"]:
        source["observed_at"] = "2020-01-01T00:00:00Z"
    members["manifest.json"] = r6.canonical(manifest)
    with pytest.raises(r6.R6InputError):
        r6.verify_input(members)


def test_r6_report_distinguishes_numeric_component_from_original_p3() -> None:
    report = tool._r6_report(
        {"status": "SUCCESS", "runs": [{"window_id": "rolling_locked_oos"}]},
        manifest_sha="a" * 64,
        observed_at="2026-09-26T00:00:00Z",
    )
    assert report["study_id"] == r6.STUDY_ID
    assert report["original_dual_source_p1_p3_verified"] is False
    assert report["joint_account_admission"] == "NOT_ADMITTED"
    assert report["historical_point_in_time_certified"] is False
    assert "development" in report["window_classification"]


def test_r6_accounting_readback_recalculates_cash_turnover_and_fees() -> None:
    sessions = [
        {"as_of": "2023-01-03T00:00:00+00:00", "prices": {"SOXL": 10, "SOXX": 20, "BOXX": 30}},
        {"as_of": "2023-01-04T00:00:00+00:00", "prices": {"SOXL": 11, "SOXX": 20, "BOXX": 30}},
    ]
    output = {
        "cost_bps": 5,
        "initial_equity": 100.0,
        "final_equity": 99.975,
        "executed_signal_count": 1,
        "one_way_turnover": 0.5,
        "cost_total": 0.025,
        "decisions": [
            {
                "signal_as_of": sessions[0]["as_of"], "equity_before_signal": 100.0,
                "executed_one_way_turnover": 0.0, "executed_cost": 0.0,
                "pending_target_weights": {"SOXL": 0.5, "SOXX": 0.0, "BOXX": 0.0},
                "pending_cash_weight": 0.5,
            },
            {
                "signal_as_of": sessions[1]["as_of"], "equity_before_signal": 99.975,
                "executed_one_way_turnover": 0.5, "executed_cost": 0.025,
                "pending_target_weights": None, "pending_cash_weight": None,
            },
        ],
    }
    record = {"input": {"cost_bps": 5, "initial_equity": 100.0, "sessions": sessions}, "output": {"replay": output}}
    records = [copy.deepcopy(record) for _ in range(15)]
    checked = tool._verify_replay_accounting(records)
    assert checked["runs"][0]["recomputed_cost_total"] == pytest.approx(0.025)
    records[0]["output"]["replay"]["cost_total"] = 0.0
    with pytest.raises(tool.R6ArchiveError, match="accounting invalid"):
        tool._verify_replay_accounting(records)


def test_r6_raw_duplicate_and_metadata_rejected_before_sorting() -> None:
    raw = {
        "meta": {"symbol": "SOXL", "currency": "USD", "interval": "1day", "type": "ETF"},
        "values": [{"datetime": "2026-08-25"}, {"datetime": "2026-08-25"}],
    }
    with pytest.raises(tool.R6ArchiveError, match="raw session"):
        tool._validate_twelve_payload(raw)
    raw["values"] = [{"datetime": "2026-08-25"}]
    raw["meta"]["currency"] = "EUR"
    with pytest.raises(tool.R6ArchiveError, match="metadata"):
        tool._validate_twelve_payload(raw)


def test_r6_transport_never_routes_yahoo() -> None:
    from urllib.request import Request

    meter = tool.HttpMeter(allowed_hosts=frozenset({"api.twelvedata.com"}), max_requests=12)
    with pytest.raises(tool.R5ArchiveError, match="host outside"):
        meter.open(Request("https://query1.finance.yahoo.com/v8/finance/chart/SOXL"))
    assert meter.requests == 0


def test_r6_cannot_start_repeat_acquisition_after_attempt(tmp_path: Path) -> None:
    from tests.test_soxl_v7_r5_native_archive import FakeClient

    uri = "gs://synthetic-private/approved/"
    archive = tool.FixedArchive(
        FakeClient(), uri, root_sha256=tool.digest(uri.encode()),
        max_operations=160, max_bytes=512 * 1024 * 1024,
        probe_content=tool.canonical({"schema": "soxl_v7_r6_probe.v1", "study_id": r6.STUDY_ID}),
    )
    archive.create_probe()
    archive.create("attempt.json", b"{}")
    with pytest.raises(tool.R5ArchiveError, match="create-only write failed"):
        archive.create("attempt.json", b"{}")


def test_r6_raw_source_is_archived_before_normalization_and_read_back() -> None:
    from tests.test_soxl_v7_r5_native_archive import FakeClient

    uri = "gs://synthetic-private/approved/"
    archive = tool.FixedArchive(
        FakeClient(), uri, root_sha256=tool.digest(uri.encode()),
        max_operations=160, max_bytes=512 * 1024 * 1024,
    )
    observations = _observations()
    members = r6.build_input(observations, observed_at="2026-09-26T00:00:00Z", producer=_producer())
    raw_receipts = {}
    snapshot_receipts = {}
    for symbol, observation in observations.items():
        bars = observation.snapshot.bars
        raw = {
            "meta": {"symbol": symbol, "currency": "USD", "interval": "1day", "type": "ETF"},
            "values": [
                {"datetime": bar.session_date, "open": bar.open, "high": bar.high, "low": bar.low,
                 "close": bar.close, "volume": bar.volume}
                for bar in reversed(bars)
            ],
        }
        raw_receipts[symbol] = archive.create(f"source_raw/{symbol}.json", tool.canonical(raw))
        snapshot_receipts[symbol] = archive.create(
            f"source/{symbol}.json", tool.canonical(observation.snapshot.to_dict())
        )
    trace = tool._verify_archived_sources(archive, raw_receipts, snapshot_receipts, members)
    assert set(trace["symbols"]) == set(r6.SYMBOLS)
    bad_raw = {"meta": {"symbol": "SOXL", "currency": "USD", "interval": "1day", "type": "ETF"},
               "values": [{"datetime": "2026-08-25", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}]}
    raw_receipts["SOXL"] = archive.create("source_raw/SOXL-other.json", tool.canonical(bad_raw))
    with pytest.raises(tool.R6ArchiveError, match="raw-to-normalized"):
        tool._verify_archived_sources(archive, raw_receipts, snapshot_receipts, members)
