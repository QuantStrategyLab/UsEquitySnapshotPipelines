from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from quant_platform_kit.data.multisource_assurance import (
    SOURCE_OBSERVATION_READY,
    DailyBar,
    DailyBarSourceObservation,
    DailyBarSourceSnapshot,
)

from us_equity_snapshot_pipelines.lifecycle import tqqq_core_only_free_ohlcv_p1 as p1
from us_equity_snapshot_pipelines.lifecycle.tqqq_core_only_p1_binding import (
    P2_V8_CONTRACT,
    P2_V9_CONTRACT,
    expected_tqqq_core_only_sessions_for_contract,
)

_CUTOFF = "2026-08-25"
_CANONICAL = "twelve_data_1day_split_adjusted"
_VERIFIER = "yahoo_finance_chart_1day_split_adjusted"


def _producer() -> dict[str, str]:
    return {
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "commit_sha": "a" * 40,
        "tree_sha": "b" * 40,
        "tool": "tqqq_core_only_free_ohlcv_p1_test",
        "tool_version": "v1",
    }


class _Observer:
    def __init__(self, *, divergent: bool = False) -> None:
        self._sessions = expected_tqqq_core_only_sessions_for_contract(
            P2_V8_CONTRACT, date_cutoff=_CUTOFF
        )
        self._divergent = divergent

    def observe_daily_bars(
        self, *, source_id: str, symbol: str, start_date: str, date_cutoff: str
    ) -> DailyBarSourceObservation:
        assert start_date == self._sessions[symbol][0].isoformat()
        assert date_cutoff == _CUTOFF
        bars = []
        for index, session in enumerate(self._sessions[symbol], start=1):
            price = 10.0 + index
            if self._divergent and source_id == _VERIFIER and symbol == "TQQQ" and index == 1:
                price *= 1.01
            bars.append(
                DailyBar(
                    session_date=session.isoformat(),
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=1000.0,
                )
            )
        return DailyBarSourceObservation(
            source_id=source_id,
            status=SOURCE_OBSERVATION_READY,
            snapshot=DailyBarSourceSnapshot(
                source_id=source_id,
                symbol=symbol,
                date_cutoff=date_cutoff,
                adjustment_basis="split_adjusted",
                source_artifact_sha256=hashlib.sha256(f"{source_id}:{symbol}".encode()).hexdigest(),
                bars=tuple(bars),
            ),
        )


class _SettlementObserver:
    def __init__(self, *, divergent: bool = False) -> None:
        self._divergent = divergent

    def observe_daily_bars(
        self, *, source_id: str, symbol: str, start_date: str, date_cutoff: str
    ) -> DailyBarSourceObservation:
        assert start_date == date_cutoff == _CUTOFF
        price = 10.0
        if self._divergent and source_id == _VERIFIER and symbol == "TQQQ":
            price *= 1.01
        return DailyBarSourceObservation(
            source_id=source_id,
            status=SOURCE_OBSERVATION_READY,
            snapshot=DailyBarSourceSnapshot(
                source_id=source_id,
                symbol=symbol,
                date_cutoff=date_cutoff,
                adjustment_basis="split_adjusted",
                source_artifact_sha256=hashlib.sha256(
                    f"settlement:{source_id}:{symbol}".encode()
                ).hexdigest(),
                bars=(
                    DailyBar(
                        session_date=date_cutoff,
                        open=price,
                        high=price,
                        low=price,
                        close=price,
                        volume=1000.0,
                    ),
                ),
            ),
        )


def test_v8_free_ohlcv_root_requires_two_source_agreement_and_is_candidate_bound(tmp_path: Path) -> None:
    root = tmp_path / "p1"

    published = p1.publish_tqqq_core_only_free_ohlcv_p1_inputs(
        _Observer(), output_root=root, observed_at="2026-08-26T02:00:00Z", producer=_producer(), date_cutoff=_CUTOFF
    )

    assert p1.verify_tqqq_core_only_free_ohlcv_p1_input_root(root) == published["manifest_sha256"]
    assert {path.name for path in root.iterdir()} == {"binding.json", "bars.json", "assurance.json", "manifest.json"}
    manifest = json.loads((root / "manifest.json").read_bytes())
    assert manifest["profile"] == P2_V8_CONTRACT.candidate_id
    assert {source["source_id"] for source in manifest["sources"]} == {
        f"{source_id}:{symbol}"
        for source_id in (_CANONICAL, _VERIFIER)
        for symbol in ("QQQ", "TQQQ", "QQQM", "BOXX")
    }


def test_v8_free_ohlcv_parks_when_a_mandatory_source_disagrees(tmp_path: Path) -> None:
    with pytest.raises(p1.TqqqCoreOnlyFreeOhlcvP1UnavailableError) as raised:
        p1.publish_tqqq_core_only_free_ohlcv_p1_inputs(
            _Observer(divergent=True), output_root=tmp_path / "parked", observed_at="2026-08-26T02:00:00Z", producer=_producer(), date_cutoff=_CUTOFF
        )
    diagnostic = raised.value.availability_diagnostic
    assert diagnostic is not None
    assert diagnostic["candidate"] == {
        "candidate_id": P2_V8_CONTRACT.candidate_id,
        "config_sha256": P2_V8_CONTRACT.config_sha256,
    }
    assert diagnostic["status"] == "NOT_VERIFIED"
    assert diagnostic["reports"]["TQQQ"]["findings"] == [
        "daily_bar_price_divergence"
    ]
    agreement = diagnostic["reports"]["TQQQ"]["price_agreement"]
    assert agreement["status"] == "COMPARED"
    assert agreement["first_price_divergent_session"] is not None
    assert agreement["price_divergent_fields"] == ["close", "high", "low", "open"]
    assert agreement["max_price_delta_bps"] > 90.0
    assert agreement["field_delta_bps"]["open"] == {
        "compared_session_count": len(_Observer()._sessions["TQQQ"]),
        "divergent_session_count": 1,
        "p50_nearest_rank_bps": 0.0,
        "p95_nearest_rank_bps": 0.0,
        "p99_nearest_rank_bps": 0.0,
        "max_bps": 99.009901,
    }
    assert p1.classify_tqqq_core_only_free_ohlcv_availability(diagnostic) == "FREE_SOURCE_DISAGREEMENT"
    assert "bars" not in diagnostic["reports"]["TQQQ"]
    assert "close" not in agreement


def test_unavailable_source_status_remains_distinct_from_healthy_source_disagreement() -> None:
    assert p1.classify_tqqq_core_only_free_ohlcv_availability(
        {
            "status": "NOT_VERIFIED",
            "reports": {
                "TQQQ": {
                    "source_statuses": {_CANONICAL: "UNAVAILABLE", _VERIFIER: SOURCE_OBSERVATION_READY},
                    "findings": [],
                }
            },
        }
    ) == "FREE_SOURCE_UNAVAILABLE"


def test_assurance_observation_is_redacted_and_never_grants_execution(tmp_path: Path) -> None:
    record = p1.observe_tqqq_core_only_free_ohlcv_assurance(
        _Observer(divergent=True),
        output_root=tmp_path / "parked",
        observed_at="2026-08-26T02:00:00Z",
        producer=_producer(),
        date_cutoff=_CUTOFF,
        contract=P2_V9_CONTRACT,
    )

    assert record["schema_version"] == "qsl.tqqq-free-ohlcv-assurance-observation.v1"
    assert record["candidate"] == {
        "candidate_id": P2_V9_CONTRACT.candidate_id,
        "config_sha256": P2_V9_CONTRACT.config_sha256,
    }
    assert record["status"] == "PARKED"
    assert record["reason_code"] == "FREE_SOURCE_DISAGREEMENT"
    assert record["input_manifest_sha256"] == ""
    assert record["no_order"] is True
    assert record["automatic_promotion"] is False
    diagnostic = record["availability_diagnostic"]
    assert isinstance(diagnostic, dict)
    assert diagnostic["reports"]["TQQQ"]["price_agreement"]["field_delta_bps"]["open"]["max_bps"] > 90.0
    assert "bars" not in diagnostic["reports"]["TQQQ"]


def test_settlement_observation_is_single_session_redacted_and_fail_closed() -> None:
    verified = p1.observe_tqqq_core_only_free_ohlcv_settlement(
        _SettlementObserver(),
        observed_at="2026-08-26T02:00:00Z",
        date_cutoff=_CUTOFF,
        contract=P2_V9_CONTRACT,
    )
    assert verified["schema_version"] == "qsl.tqqq-free-ohlcv-settlement-observation.v1"
    assert verified["status"] == "VERIFIED"
    assert verified["reason_code"] == ""
    assert verified["request_window"] == {
        "start_date": _CUTOFF,
        "end_date": _CUTOFF,
    }
    assert verified["no_order"] is True
    assert verified["automatic_promotion"] is False
    assert verified["availability_diagnostic"]["reports"]["TQQQ"]["price_agreement"]["field_delta_bps"]["open"] == {
        "compared_session_count": 1,
        "divergent_session_count": 0,
        "p50_nearest_rank_bps": 0.0,
        "p95_nearest_rank_bps": 0.0,
        "p99_nearest_rank_bps": 0.0,
        "max_bps": 0.0,
    }

    parked = p1.observe_tqqq_core_only_free_ohlcv_settlement(
        _SettlementObserver(divergent=True),
        observed_at="2026-08-26T02:00:00Z",
        date_cutoff=_CUTOFF,
        contract=P2_V9_CONTRACT,
    )
    assert parked["status"] == "PARKED"
    assert parked["reason_code"] == "FREE_SOURCE_DISAGREEMENT"
    assert "bars" not in parked["availability_diagnostic"]["reports"]["TQQQ"]

    settled = p1.build_tqqq_core_only_free_ohlcv_settlement_track(
        {
            0: parked,
            1: verified,
            2: {**verified, "observed_at": "2026-08-28T02:00:00Z"},
        },
        contract=P2_V9_CONTRACT,
    )
    assert settled["settlement_state"] == "SETTLED_VERIFIED"
    assert settled["revision_detected"] is True
    assert settled["no_order"] is True
    assert settled["automatic_promotion"] is False
    assert set(settled["observations"]) == {"0", "1", "2"}

    persistent = p1.build_tqqq_core_only_free_ohlcv_settlement_track(
        {
            0: parked,
            1: {**parked, "observed_at": "2026-08-27T02:00:00Z"},
            2: {**parked, "observed_at": "2026-08-28T02:00:00Z"},
        },
        contract=P2_V9_CONTRACT,
    )
    assert persistent["settlement_state"] == "PERSISTENT_CROSS_SOURCE_DISAGREEMENT"
    assert persistent["revision_detected"] is False


def test_v9_uses_the_same_two_source_p1_transport_but_a_distinct_identity(
    tmp_path: Path,
) -> None:
    root = tmp_path / "v9"

    published = p1.publish_tqqq_core_only_free_ohlcv_p1_inputs(
        _Observer(),
        output_root=root,
        observed_at="2026-08-26T02:00:00Z",
        producer=_producer(),
        date_cutoff=_CUTOFF,
        contract=P2_V9_CONTRACT,
    )

    assert (
        p1.verify_tqqq_core_only_free_ohlcv_p1_input_root(
            root, contract=P2_V9_CONTRACT
        )
        == published["manifest_sha256"]
    )
    assert json.loads((root / "manifest.json").read_bytes())["profile"] == P2_V9_CONTRACT.candidate_id
    with pytest.raises(p1.TqqqCoreOnlyFreeOhlcvP1Error):
        p1.verify_tqqq_core_only_free_ohlcv_p1_input_root(root)
