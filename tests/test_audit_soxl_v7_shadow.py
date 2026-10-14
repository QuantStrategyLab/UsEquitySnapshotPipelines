"""Synthetic receipt-chain and output-boundary regressions; no real data."""

import copy
import json

import pytest

from scripts import audit_soxl_v7_shadow as audit


def _records():
    dates = ["2026-08-26", "2026-08-27"]
    result = []
    for index, session in enumerate(dates, 1):
        result.append({
            "candidate_id": audit.CANDIDATE,
            "candidate_config_sha256": audit.CONFIG_SHA256,
            "observation_sessions": dates[:index],
            "last_observed_session": session,
            "observed_at": session + "T23:00:00Z",
            "controller": {"observations_completed": index, "state": "FORWARD_ACTIVE"},
            "forward_observation_receipt": {"receipt_sha256": str(index) * 64,
                                             "previous_receipt_sha256": str(index - 1) * 64 if index > 1 else None},
            "record_sha256": "a" * 64,
            "private_unknown_field": {"price_rows": [123.456], "account": "SYNTHETIC_ACCOUNT"},
        })
    return result


def _summarize(records, validator=lambda record: None):
    return audit.summarize_records(records, validate_record=validator,
                                   expected_sessions=["2026-08-26", "2026-08-27"], references=[])


def test_summary_keeps_unknown_metrics_null_and_private_fields_out():
    result = _summarize(_records())
    assert result["effective_observation_sessions"] == 2
    assert result["healthy_receipt_sessions"] == 2
    assert result["shadow_vs_backtest_tracking_error"]["value"] is None
    assert result["execution_parity_qualified_days"]["value"] is None
    assert result["promotion_eligible"] is False
    assert "SYNTHETIC_ACCOUNT" not in json.dumps(result)
    assert "price_rows" not in json.dumps(result)


@pytest.mark.parametrize("failure", ["missing", "duplicate", "wrong_config", "broken_link", "wrong_prefix"])
def test_bad_chain_is_rejected(failure):
    records = _records()
    if failure == "missing":
        records = records[1:]
    elif failure == "duplicate":
        records = [records[0], copy.deepcopy(records[0])]
    elif failure == "wrong_config":
        records[1]["candidate_config_sha256"] = "b" * 64
    elif failure == "broken_link":
        records[1]["forward_observation_receipt"]["previous_receipt_sha256"] = "b" * 64
    else:
        records[0]["observation_sessions"] = ["2026-08-25"]
    with pytest.raises(ValueError):
        _summarize(records)


def test_existing_record_validator_is_required_for_every_record():
    visited = []

    def reject(record):
        visited.append(record)
        raise ValueError("synthetic invalid identity")

    with pytest.raises(ValueError):
        _summarize(_records(), validator=reject)
    assert len(visited) == 1


def test_backfilled_receipts_do_not_become_execution_parity_days():
    records = _records()
    records[0]["observed_at"] = "2026-10-01T00:00:00Z"
    result = _summarize(records)
    assert result["receipts_observed_after_session_utc_date"] == 1
    assert result["execution_parity_qualified_days"]["status"] == "UNCOMPUTABLE"


def test_cli_refuses_the_mac_before_cloud_access(monkeypatch, capsys):
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.setattr(audit, "_read_cloud", lambda: pytest.fail("cloud access attempted"))
    assert audit.main() == 2
    assert json.loads(capsys.readouterr().out) == {"status": "CLOUD_CI_ONLY"}


def test_cloud_failure_does_not_print_private_exception(monkeypatch, capsys, tmp_path):
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("RUNNER_OS", "Linux")
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path))
    monkeypatch.setenv("GITHUB_SHA", "c" * 40)

    def fail():
        raise RuntimeError("SYNTHETIC_PRIVATE_ACCOUNT_AND_PRICE")

    monkeypatch.setattr(audit, "_read_cloud", fail)
    assert audit.main() == 2
    output = capsys.readouterr().out
    assert "SYNTHETIC_PRIVATE" not in output
    assert json.loads(output)["reason"] == "READ_OR_VALIDATION_FAILED"
    assert list((tmp_path / "soxl-v7-readonly-audit").iterdir()) == [
        tmp_path / "soxl-v7-readonly-audit/summary.json"
    ]


def test_actual_v7_builder_and_validator_chain():
    module = pytest.importorskip("us_equity_snapshot_pipelines.lifecycle.soxl_v7_nonlive_forward_observation")
    records = []
    dates = ["2026-08-26", "2026-08-27"]
    previous = None
    for index in range(1, 3):
        inputs = module.SoxlV7NonliveForwardInputs("d" * 64, tuple(dates[:index]), {}, {})
        record = module.build_soxl_v7_nonlive_forward_record(
            observed_at="2026-08-28T02:45:00Z", inputs=inputs,
            shadow_observation_sha256="e" * 64, simulated_paper_observation_sha256="f" * 64,
            previous_record=previous,
        )
        records.append(record)
        previous = record
    result = _summarize(
        records, validator=lambda record: module.build_soxl_v7_forward_control_plane_source(
            record, generated_at=record["observed_at"]
        )
    )
    assert result["effective_observation_sessions"] == 2
    assert result["signal_match_and_fill_executability"]["status"] == "UNCOMPUTABLE"
