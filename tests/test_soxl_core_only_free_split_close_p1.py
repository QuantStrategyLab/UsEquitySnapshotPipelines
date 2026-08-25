from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path

import pytest
from quant_platform_kit.data.multisource_assurance import (
    SOURCE_OBSERVATION_READY,
    DailyBar,
    DailyBarSourceObservation,
    DailyBarSourceSnapshot,
)

from us_equity_snapshot_pipelines.lifecycle import soxl_core_only_free_split_close_p1 as p1
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p1_binding import (
    expected_soxl_core_only_sessions,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p2_v4_free_split_close_contract import (
    P2_V4_FREE_SPLIT_CLOSE_CONTRACT,
)

_CUTOFF = "2026-08-18"
_CANONICAL = "twelve_data_1day_split_adjusted"
_VERIFIER = "yahoo_finance_chart_1day_split_adjusted"


def _producer() -> dict[str, str]:
    return {
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "commit_sha": "a" * 40,
        "tree_sha": "b" * 40,
        "tool": "soxl_core_only_free_split_close_p1_test",
        "tool_version": "v1",
    }


class _AssuredObserver:
    def __init__(
        self,
        *,
        missing_symbol: str | None = None,
        divergent_symbol: str | None = None,
    ) -> None:
        self._expected = expected_soxl_core_only_sessions(_CUTOFF)
        self._missing_symbol = missing_symbol
        self._divergent_symbol = divergent_symbol
        self.requests: list[dict[str, str]] = []

    def observe_daily_bars(
        self,
        *,
        source_id: str,
        symbol: str,
        start_date: str,
        date_cutoff: str,
    ) -> DailyBarSourceObservation:
        self.requests.append(
            {
                "source_id": source_id,
                "symbol": symbol,
                "start_date": start_date,
                "date_cutoff": date_cutoff,
            }
        )
        sessions = self._expected[symbol]
        if source_id == _VERIFIER and symbol == self._missing_symbol:
            sessions = sessions[:-1]
        offset = {"SOXL": 10.0, "SOXX": 100.0, "BOXX": 20.0}[symbol]
        bars = []
        for index, session in enumerate(sessions, start=1):
            close = offset + index
            if source_id == _VERIFIER and symbol == self._divergent_symbol and index == len(sessions):
                close *= 1.01
            bars.append(
                DailyBar(
                    session_date=session.isoformat(),
                    open=close,
                    high=close,
                    low=close,
                    close=close,
                    volume=1000.0 + index,
                )
            )
        snapshot = DailyBarSourceSnapshot(
            source_id=source_id,
            symbol=symbol,
            date_cutoff=date_cutoff,
            adjustment_basis="split_adjusted",
            source_artifact_sha256=hashlib.sha256(f"{source_id}:{symbol}".encode()).hexdigest(),
            bars=tuple(bars),
        )
        return DailyBarSourceObservation(
            source_id=source_id,
            status=SOURCE_OBSERVATION_READY,
            snapshot=snapshot,
        )


def test_binding_freezes_v4_two_source_split_adjusted_close_identity() -> None:
    binding = p1.build_soxl_core_only_free_split_close_p1_binding(date_cutoff=_CUTOFF)

    assert binding["candidate"] == {
        "candidate_id": P2_V4_FREE_SPLIT_CLOSE_CONTRACT.candidate_id,
        "config_sha256": P2_V4_FREE_SPLIT_CLOSE_CONTRACT.config_sha256,
    }
    assert binding["data_identity"]["adjustment"]["policy"] == "split_adjusted"
    assert binding["data_identity"]["assurance"] == {
        "canonical_source_id": _CANONICAL,
        "verifier_source_id": _VERIFIER,
        "scope_id_prefix": "soxl_core_only_p2_v4_free_split_close",
        "required_price_fields": ["close"],
        "compare_volume": False,
        "price_relative_tolerance": 0.0001,
    }
    assert p1.validate_soxl_core_only_free_split_close_p1_binding(binding) == binding


def test_p1_rejects_an_in_progress_xnys_session_before_any_source_observation(tmp_path: Path) -> None:
    observer = _AssuredObserver()
    output_root = tmp_path / "in-progress-p1"

    with pytest.raises(p1.SoxlCoreOnlyFreeSplitCloseP1UnavailableError, match="not complete"):
        p1.publish_soxl_core_only_free_split_close_p1_inputs(
            observer,
            output_root=output_root,
            observed_at="2026-08-18T19:59:59Z",
            producer=_producer(),
            date_cutoff=_CUTOFF,
        )

    assert observer.requests == []
    assert not output_root.exists()
    assert p1.validate_soxl_core_only_free_split_close_completed_session(
        date_cutoff=_CUTOFF,
        observed_at="2026-08-18T20:00:00Z",
    ) == _CUTOFF


def test_publisher_requires_two_source_close_agreement_then_writes_a_private_root(tmp_path: Path) -> None:
    observer = _AssuredObserver()
    output_root = tmp_path / "free-split-close-p1"

    result = p1.publish_soxl_core_only_free_split_close_p1_inputs(
        observer,
        output_root=output_root,
        observed_at="2026-08-19T00:00:00Z",
        producer=_producer(),
        date_cutoff=_CUTOFF,
    )

    assert result["status"] == "P1_FREE_SPLIT_CLOSE_INPUTS_PUBLISHED"
    assert p1.verify_soxl_core_only_free_split_close_p1_input_root(output_root) == result["manifest_sha256"]
    assert output_root.stat().st_mode & 0o777 == 0o700
    assert {path.name for path in output_root.iterdir()} == {
        "assurance.json",
        "binding.json",
        "closes.json",
        "manifest.json",
    }
    assert [request["source_id"] for request in observer.requests] == [
        _CANONICAL,
        _VERIFIER,
    ] * 3
    closes = json.loads((output_root / "closes.json").read_bytes())
    assert closes["schema_version"] == "qsl.soxl-soxx-core-only-split-adjusted-close-series.v1"
    assert set(closes["series"]) == {"SOXL", "SOXX", "BOXX"}
    assurance = json.loads((output_root / "assurance.json").read_bytes())
    assert all(
        item["diagnostic"]["status"] == "VERIFIED"
        and item["diagnostic"]["can_publish_research_input"] is True
        for item in assurance["assurances"].values()
    )
    assert all("bars" not in item for item in assurance["assurances"].values())
    manifest = json.loads((output_root / "manifest.json").read_bytes())
    assert {source["source_id"] for source in manifest["sources"]} == {
        f"{source_id}:{symbol}"
        for symbol in ("SOXL", "SOXX", "BOXX")
        for source_id in (_CANONICAL, _VERIFIER)
    }


@pytest.mark.parametrize("kwargs", ({"missing_symbol": "SOXX"}, {"divergent_symbol": "SOXL"}))
def test_publisher_parks_incomplete_or_disagreeing_sources_without_a_root(
    tmp_path: Path, kwargs: Mapping[str, str]
) -> None:
    output_root = tmp_path / "parked-p1"

    with pytest.raises(p1.SoxlCoreOnlyFreeSplitCloseP1UnavailableError, match="not verified"):
        p1.publish_soxl_core_only_free_split_close_p1_inputs(
            _AssuredObserver(**kwargs),
            output_root=output_root,
            observed_at="2026-08-19T00:00:00Z",
            producer=_producer(),
            date_cutoff=_CUTOFF,
        )

    assert not output_root.exists()


def test_verifier_rejects_tampered_assurance_without_reading_a_provider(tmp_path: Path) -> None:
    output_root = tmp_path / "tampered-p1"
    p1.publish_soxl_core_only_free_split_close_p1_inputs(
        _AssuredObserver(),
        output_root=output_root,
        observed_at="2026-08-19T00:00:00Z",
        producer=_producer(),
        date_cutoff=_CUTOFF,
    )
    assurance_path = output_root / "assurance.json"
    assurance = json.loads(assurance_path.read_bytes())
    assurance["assurances"]["SOXL"]["diagnostic"]["status"] = "DEGRADED"
    assurance_path.write_bytes(
        json.dumps(assurance, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    )

    with pytest.raises(p1.SoxlCoreOnlyFreeSplitCloseP1Error, match="input root"):
        p1.verify_soxl_core_only_free_split_close_p1_input_root(output_root)


def test_series_rejects_noncanonical_or_nonpositive_close() -> None:
    with pytest.raises(p1.SoxlCoreOnlyFreeSplitCloseP1Error, match="close series"):
        p1.canonical_soxl_core_only_free_split_close_series_bytes(
            symbol="SOXL",
            series=[{"session_date": date(2026, 8, 18).isoformat(), "close": 0.0}],
        )
