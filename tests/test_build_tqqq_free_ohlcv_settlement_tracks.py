from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

from us_equity_snapshot_pipelines.lifecycle.tqqq_core_only_p1_binding import (
    P2_V9_CONTRACT,
)


SCRIPT = Path("scripts/build_tqqq_free_ohlcv_settlement_tracks.py")
SPEC = importlib.util.spec_from_file_location(
    "build_tqqq_free_ohlcv_settlement_tracks", SCRIPT
)
assert SPEC is not None and SPEC.loader is not None
track_builder = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(track_builder)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _observation(*, cutoff: str, observed_at: str, suffix: str = "") -> dict[str, object]:
    reports = {}
    for symbol in ("QQQ", "TQQQ", "QQQM", "BOXX"):
        reports[symbol] = {
            "status": "VERIFIED",
            "findings": [],
            "source_statuses": {
                "twelve_data_1day_split_adjusted": "READY",
                "yahoo_finance_chart_1day_split_adjusted": "READY",
            },
            "source_snapshot_sha256": {
                "twelve_data_1day_split_adjusted": _digest(f"twelve:{symbol}:{cutoff}:{suffix}"),
                "yahoo_finance_chart_1day_split_adjusted": _digest(f"yahoo:{symbol}:{cutoff}:{suffix}"),
            },
            "price_agreement": {
                "status": "COMPARED",
                "field_delta_bps": {"open": {"max_bps": 0.0}},
            },
        }
    return {
        "schema_version": "qsl.tqqq-free-ohlcv-settlement-observation.v1",
        "candidate": {
            "candidate_id": P2_V9_CONTRACT.candidate_id,
            "config_sha256": P2_V9_CONTRACT.config_sha256,
        },
        "date_cutoff": cutoff,
        "observed_at": observed_at,
        "request_window": {"start_date": cutoff, "end_date": cutoff},
        "status": "VERIFIED",
        "reason_code": "",
        "availability_diagnostic": {
            "schema_version": "qsl.tqqq-core-only-free-ohlcv-availability.v1",
            "reports": reports,
        },
        "no_order": True,
        "automatic_promotion": False,
    }


def _write_batch(path: Path, observations: list[tuple[int, dict[str, object]]]) -> None:
    payload = {
        "schema_version": "qsl.tqqq-free-ohlcv-settlement-observation-batch.v1",
        "candidate": {
            "candidate_id": P2_V9_CONTRACT.candidate_id,
            "config_sha256": P2_V9_CONTRACT.config_sha256,
        },
        "observations": [
            {"age_sessions": age, "observation": observation}
            for age, observation in observations
        ],
        "no_order": True,
        "automatic_promotion": False,
    }
    path.write_bytes(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode())


def test_track_builder_groups_t_plus_zero_one_two_without_raw_bars(tmp_path: Path) -> None:
    current = tmp_path / "current.json"
    prior = tmp_path / "prior.json"
    _write_batch(
        current,
        [
            (0, _observation(cutoff="2026-08-26", observed_at="2026-08-27T02:00:00Z")),
            (1, _observation(cutoff="2026-08-25", observed_at="2026-08-27T02:00:00Z")),
            (2, _observation(cutoff="2026-08-24", observed_at="2026-08-27T02:00:00Z")),
        ],
    )
    _write_batch(
        prior,
        [
            (0, _observation(cutoff="2026-08-25", observed_at="2026-08-26T02:00:00Z")),
            (1, _observation(cutoff="2026-08-24", observed_at="2026-08-26T02:00:00Z")),
            (2, _observation(cutoff="2026-08-21", observed_at="2026-08-26T02:00:00Z")),
        ],
    )

    result = track_builder.build_settlement_tracks(
        current_path=current, prior_paths=[prior]
    )

    assert result["schema_version"] == "qsl.tqqq-free-ohlcv-settlement-track-batch.v1"
    assert result["tracks"]["2026-08-26"]["settlement_state"] == "PENDING_T_PLUS_2"
    assert result["tracks"]["2026-08-25"]["settlement_state"] == "PENDING_T_PLUS_2"
    settled = result["tracks"]["2026-08-24"]
    assert settled["settlement_state"] == "SETTLED_VERIFIED"
    assert settled["no_order"] is True
    assert settled["automatic_promotion"] is False
    assert "bars" not in json.dumps(result)


def test_track_builder_parks_ambiguous_repeated_age(tmp_path: Path) -> None:
    current = tmp_path / "current.json"
    prior = tmp_path / "prior.json"
    conflict = tmp_path / "conflict.json"
    repeated = _observation(cutoff="2026-08-24", observed_at="2026-08-26T02:00:00Z")
    _write_batch(
        current,
        [
            (0, _observation(cutoff="2026-08-26", observed_at="2026-08-27T02:00:00Z")),
            (1, _observation(cutoff="2026-08-25", observed_at="2026-08-27T02:00:00Z")),
            (2, {**repeated, "observed_at": "2026-08-27T02:00:00Z", "reason_code": "FREE_SOURCE_DISAGREEMENT"}),
        ],
    )
    _write_batch(
        prior,
        [
            (0, _observation(cutoff="2026-08-25", observed_at="2026-08-26T02:00:00Z")),
            (1, repeated),
            (2, _observation(cutoff="2026-08-21", observed_at="2026-08-26T02:00:00Z")),
        ],
    )
    _write_batch(
        conflict,
        [
            (0, _observation(cutoff="2026-08-25", observed_at="2026-08-26T03:00:00Z")),
            (1, {**repeated, "observed_at": "2026-08-26T03:00:00Z", "reason_code": "FREE_SOURCE_DISAGREEMENT"}),
            (2, _observation(cutoff="2026-08-21", observed_at="2026-08-26T03:00:00Z")),
        ],
    )

    result = track_builder.build_settlement_tracks(
        current_path=current, prior_paths=[prior, conflict]
    )

    assert result["ambiguous_cutoff_count"] == 1
    assert result["tracks"]["2026-08-24"]["settlement_state"] == "AMBIGUOUS_REPEATED_PROBE"
