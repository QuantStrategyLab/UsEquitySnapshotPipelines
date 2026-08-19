from __future__ import annotations

from us_equity_snapshot_pipelines.lifecycle import tqqq_core_only_p1_binding as binding
from us_equity_snapshot_pipelines.lifecycle.tqqq_core_only_p1_data_health import (
    assess_tqqq_core_only_p1_input_health,
    build_tqqq_core_only_p1_input_unavailable_health,
)


def _payload() -> dict[str, object]:
    expected = binding.expected_tqqq_core_only_sessions_for_contract(binding.P2_V4_CONTRACT)
    return {
        "schema_version": "tqqq_core_only_private_bars.v1",
        "symbols": {
            symbol: {
                "bars": [
                    {"t": f"{session.isoformat()}T00:00:00Z", "c": 100.0}
                    for session in sessions
                ]
            }
            for symbol, sessions in expected.items()
        },
    }


def test_complete_v4_payload_is_ready_with_no_missing_ranges() -> None:
    result = assess_tqqq_core_only_p1_input_health(_payload(), observed_at="2026-08-19T00:00:00Z")

    assert result["status"] == "ACCEPTED"
    assert result["verdict"] == "READY"
    assert result["reason_codes"] == ["COMPLETE"]
    assert result["candidate"] == {
        "candidate_id": "tqqq_core_only_p2_v4",
        "config_sha256": "b20335a16d0c5001dc28d3a1555dc1d46e6331fc714ca489a952d779de3279f1",
    }
    assert all(value["missing_ranges"] == [] for value in result["coverage"].values())


def test_missing_sessions_defer_only_the_affected_range() -> None:
    payload = _payload()
    qqq_bars = payload["symbols"]["QQQ"]["bars"]
    del qqq_bars[3:5]

    result = assess_tqqq_core_only_p1_input_health(payload, observed_at="2026-08-19T00:00:00Z")

    assert result["status"] == "DEFERRED"
    assert result["verdict"] == "INCONCLUSIVE"
    assert result["reason_codes"] == ["MISSING_SESSIONS"]
    assert result["coverage"]["QQQ"] == {
        "expected_sessions": 2158,
        "observed_sessions": 2156,
        "missing_sessions": 2,
        "missing_ranges": [{"start": "2018-01-05", "end": "2018-01-08"}],
    }
    assert result["coverage"]["TQQQ"]["missing_sessions"] == 0


def test_duplicate_or_non_monotonic_sessions_are_quarantined() -> None:
    payload = _payload()
    qqq_bars = payload["symbols"]["QQQ"]["bars"]
    qqq_bars.insert(2, dict(qqq_bars[1]))

    result = assess_tqqq_core_only_p1_input_health(payload, observed_at="2026-08-19T00:00:00Z")

    assert result["status"] == "QUARANTINED"
    assert result["verdict"] == "REJECTED"
    assert result["reason_codes"] == ["DUPLICATE_SESSION", "NON_MONOTONIC_SESSIONS"]


def test_malformed_payload_is_quarantined_without_content() -> None:
    result = assess_tqqq_core_only_p1_input_health(
        {"schema_version": "tqqq_core_only_private_bars.v1", "symbols": {"QQQ": {}}},
        observed_at="2026-08-19T00:00:00Z",
    )

    assert result["status"] == "QUARANTINED"
    assert result["reason_codes"] == ["MALFORMED_PAYLOAD"]
    assert result["coverage"] == {}


def test_provider_unavailable_is_deferred_without_erasing_prior_snapshot_state() -> None:
    result = build_tqqq_core_only_p1_input_unavailable_health(observed_at="2026-08-19T00:00:00Z")

    assert result["status"] == "DEFERRED"
    assert result["verdict"] == "INCONCLUSIVE"
    assert result["reason_codes"] == ["INPUT_UNAVAILABLE"]
    assert result["bars_payload_sha256"] is None
    assert result["coverage"] == {}
