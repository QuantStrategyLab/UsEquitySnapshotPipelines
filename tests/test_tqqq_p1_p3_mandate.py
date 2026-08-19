from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from us_equity_snapshot_pipelines.lifecycle import tqqq_p1_p3_mandate as mandate


NOW = datetime(2026, 8, 19, 12, 0, tzinfo=UTC)


def _value() -> dict[str, object]:
    return {
        "schema_version": mandate.SCHEMA_VERSION,
        "mandate_id": "tqqq-p1-p3-20260819",
        "candidate": {
            "candidate_id": "tqqq_core_only_p2_v1",
            "config_sha256": "969cae10850f5a2d72c17fedd77689301411f62dc24d9a530026e3f7efdc1c69",
        },
        "scope": {
            "authority_scope": "P1_P3_RESEARCH_ONLY",
            "provider": "ALPACA_SIP",
            "allowed_operations": [
                "p1_data_acquisition",
                "p1_private_root_create_only_upload",
                "p3_historical_replay",
                "p3_private_root_read",
                "p3_private_evidence_index_create_only_upload",
            ],
            "no_order": True,
            "no_paper": True,
            "no_shadow": True,
            "no_live": True,
            "no_capital": True,
        },
        "attestation": {
            "record_source": "github-environment:tqqq-p1-p3-nonlive",
            "recorded_by": "quant-operator",
            "recorded_at": "2026-08-19T11:00:00Z",
            "expires_at": "2026-08-20T11:00:00Z",
        },
    }


def test_valid_nonlive_mandate_has_deterministic_receipt() -> None:
    value = _value()

    validated = mandate.validate_tqqq_p1_p3_mandate(value, now_utc=NOW)
    receipt = mandate.tqqq_p1_p3_mandate_receipt_sha256(value, now_utc=NOW)

    assert validated == value
    assert len(receipt) == 64
    assert receipt == mandate.tqqq_p1_p3_mandate_receipt_sha256(validated, now_utc=NOW)


@pytest.mark.parametrize(
    "mutate",
    (
        lambda value: value["candidate"].update({"candidate_id": "other"}),  # type: ignore[index]
        lambda value: value["scope"].update({"no_order": False}),  # type: ignore[index]
        lambda value: value["scope"].update({"allowed_operations": ["p1_data_acquisition"]}),  # type: ignore[index]
        lambda value: value["attestation"].update({"expires_at": "2026-10-20T11:00:00Z"}),  # type: ignore[index]
        lambda value: value.update({"unexpected": True}),
    ),
)
def test_rejects_wider_or_unbound_mandate(mutate) -> None:
    value = _value()
    mutate(value)

    with pytest.raises(mandate.TqqqP1P3MandateError):
        mandate.validate_tqqq_p1_p3_mandate(value, now_utc=NOW)


def test_loads_only_matching_current_mandate_record(tmp_path: Path) -> None:
    value = _value()
    path = tmp_path / "tqqq-p1-p3-20260819.json"
    path.write_text(json.dumps(value), encoding="utf-8")

    loaded, receipt = mandate.load_tqqq_p1_p3_mandate(tmp_path, "tqqq-p1-p3-20260819", now_utc=NOW)

    assert loaded == value
    assert receipt == mandate.tqqq_p1_p3_mandate_receipt_sha256(value, now_utc=NOW)


def test_rejects_missing_expired_or_path_like_mandate(tmp_path: Path) -> None:
    with pytest.raises(mandate.TqqqP1P3MandateError):
        mandate.load_tqqq_p1_p3_mandate(tmp_path, "tqqq-p1-p3-20260819", now_utc=NOW)

    value = _value()
    value["attestation"]["expires_at"] = (NOW - timedelta(seconds=1)).isoformat().replace("+00:00", "Z")  # type: ignore[index]
    (tmp_path / "tqqq-p1-p3-20260819.json").write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(mandate.TqqqP1P3MandateError):
        mandate.load_tqqq_p1_p3_mandate(tmp_path, "tqqq-p1-p3-20260819", now_utc=NOW)
    with pytest.raises(mandate.TqqqP1P3MandateError):
        mandate.load_tqqq_p1_p3_mandate(tmp_path, "../not-a-mandate", now_utc=NOW)
