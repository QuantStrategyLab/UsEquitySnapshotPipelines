"""R6-only Twelve Data input identity for the unchanged SOXL V7 signal.

Single-source structural assurance is deliberately separate from the original
Twelve/Yahoo P1 gate. This module has no provider, storage, or execution access.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path

from quant_platform_kit.data.multisource_assurance import SOURCE_OBSERVATION_READY, DailyBarSourceObservation
from quant_platform_kit.data.research_input import (
    canonical_research_input_manifest_bytes,
    research_input_manifest_sha256,
    validate_research_input_manifest,
)

from .soxl_core_only_free_split_close_p1 import (
    canonical_soxl_core_only_free_split_close_series_bytes,
    validate_soxl_core_only_free_split_close_completed_session,
)
from .soxl_core_only_free_split_close_p3_input_materializer import _materialize_validated_close_series
from .soxl_core_only_p1_binding import expected_soxl_core_only_sessions
from .soxl_core_only_p2_v7_longterm_compounding_cash_reserve_contract import (
    P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT as V7,
)
from ..twelve_data_daily import TWELVE_DATA_DAILY_SOURCE_ID

STUDY_ID = "soxl_v7_twelve_basic_split_close_development_v1"
INPUT_CONTRACT_ID = "qsl.soxl-v7-twelve-single-source-r6-input.v1"
DATE_CUTOFF = "2026-08-25"
SYMBOLS = ("SOXL", "SOXX", "BOXX")
FILES = ("binding.json", "manifest.json", "closes.json", "assurance.json")
ASSURANCE_LEVEL = "single_source_structural_only_no_cross_provider_verification"
CALENDAR = {
    "calendar_id": "XNYS", "timezone": "America/New_York", "source": "exchange_calendars:4.13.2:XNYS",
}


class R6InputError(ValueError):
    """Sanitized source or provenance rejection."""


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def binding() -> dict[str, object]:
    return {
        "schema_version": INPUT_CONTRACT_ID,
        "study_id": STUDY_ID,
        "signal_candidate_id": V7.candidate_id,
        "signal_config_sha256": V7.config_sha256,
        "signal_ues_revision": V7.ues_revision,
        "signal_qpk_revision": V7.qpk_revision,
        "date_cutoff": DATE_CUTOFF,
        "universe": list(SYMBOLS),
        "calendar": CALENDAR,
        "source_id": TWELVE_DATA_DAILY_SOURCE_ID,
        "interval": "1day",
        "currency": "USD",
        "price_field": "split_adjusted_close",
        "adjustment": "splits_not_dividends",
        "assurance_level": ASSURANCE_LEVEL,
    }


def _series_from_observations(observations: Mapping[str, DailyBarSourceObservation]) -> tuple[bytes, bytes]:
    if set(observations) != set(SYMBOLS):
        raise R6InputError("R6 source set incomplete")
    expected = expected_soxl_core_only_sessions(DATE_CUTOFF)
    series: dict[str, list[dict[str, object]]] = {}
    assurance: dict[str, dict[str, object]] = {}
    for symbol in SYMBOLS:
        observation = observations[symbol]
        snapshot = observation.snapshot
        if (
            observation.status != SOURCE_OBSERVATION_READY
            or observation.source_id != TWELVE_DATA_DAILY_SOURCE_ID
            or snapshot is None
            or snapshot.source_id != TWELVE_DATA_DAILY_SOURCE_ID
            or snapshot.symbol != symbol
            or snapshot.date_cutoff != DATE_CUTOFF
            or snapshot.adjustment_basis != "split_adjusted"
        ):
            raise R6InputError("R6 source identity unavailable")
        rows = list(snapshot.bars)
        if tuple(bar.session_date for bar in rows) != tuple(day.isoformat() for day in expected[symbol]):
            raise R6InputError(f"R6 {symbol} daily coverage incomplete")
        source_sha = sha(canonical({
            "source_id": TWELVE_DATA_DAILY_SOURCE_ID,
            "symbol": symbol,
            "start_date": expected[symbol][0].isoformat(),
            "date_cutoff": DATE_CUTOFF,
            "adjustment_basis": "split_adjusted",
            "bars": [bar.to_dict() for bar in rows],
        }))
        if source_sha != snapshot.source_artifact_sha256:
            raise R6InputError("R6 source digest mismatch")
        close_rows = [{"session_date": bar.session_date, "close": bar.close} for bar in rows]
        close_sha = sha(canonical_soxl_core_only_free_split_close_series_bytes(symbol=symbol, series=close_rows))
        series[symbol] = close_rows
        assurance[symbol] = {
            "source_snapshot_sha256": sha(canonical(snapshot.to_dict())),
            "canonical_close_series_sha256": close_sha,
            "first_session": expected[symbol][0].isoformat(),
            "last_session": DATE_CUTOFF,
            "session_count": len(rows),
        }
    closes = canonical({
        "schema_version": "qsl.soxl-soxx-core-only-split-adjusted-close-series.v1",
        "series": series,
    })
    report = canonical({
        "schema_version": "qsl.soxl-v7-twelve-single-source-assurance.v1",
        "assurance_level": ASSURANCE_LEVEL,
        "date_cutoff": DATE_CUTOFF,
        "source_id": TWELVE_DATA_DAILY_SOURCE_ID,
        "cross_provider_verified": False,
        "symbols": assurance,
    })
    return closes, report


def _manifest(*, observed_at: str, producer: Mapping[str, object], closes: bytes, assurance: bytes) -> dict[str, object]:
    bind_sha = sha(canonical(binding()))
    report = json.loads(assurance)
    result = {
        "schema_version": "research_input_manifest.v1",
        "manifest_id": f"soxl-v7-r6-{bind_sha[:24]}-{sha(closes)[:24]}",
        "research_input_contract_id": INPUT_CONTRACT_ID,
        "domain": "us_equity",
        "profile": STUDY_ID,
        "artifact_type": "immutable_single_source_split_adjusted_close_etf_only",
        "observed_at": observed_at,
        "effective_at": observed_at,
        "as_of": observed_at,
        "producer": dict(producer),
        "calendar": {**CALENDAR, "session_date": DATE_CUTOFF, "source_revision": bind_sha},
        "adjustment": {
            "policy": "split_adjusted", "source": "Twelve Data 1day adjust=splits; no cross-provider check",
            "source_revision": bind_sha,
        },
        "sources": sorted(({
            "source_id": f"{TWELVE_DATA_DAILY_SOURCE_ID}:{symbol}",
            "revision": bind_sha,
            "observed_at": observed_at,
            "content_sha256": report["symbols"][symbol]["source_snapshot_sha256"],
        } for symbol in SYMBOLS), key=lambda item: item["source_id"]),
        "members": [{
            "path": name, "media_type": "application/json", "size_bytes": len(content), "sha256": sha(content),
        } for name, content in (("assurance.json", assurance), ("closes.json", closes))],
    }
    return validate_research_input_manifest(result)


def build_input(
    observations: Mapping[str, DailyBarSourceObservation], *, observed_at: str, producer: Mapping[str, object]
) -> dict[str, bytes]:
    validate_soxl_core_only_free_split_close_completed_session(date_cutoff=DATE_CUTOFF, observed_at=observed_at)
    closes, assurance = _series_from_observations(observations)
    return {
        "binding.json": canonical(binding()),
        "closes.json": closes,
        "assurance.json": assurance,
        "manifest.json": canonical_research_input_manifest_bytes(
            _manifest(observed_at=observed_at, producer=producer, closes=closes, assurance=assurance)
        ),
    }


def verify_input(members: Mapping[str, bytes]) -> tuple[str, dict[str, list[dict[str, object]]]]:
    """Check every R6 member and source hash; never call the dual-source gate."""
    try:
        if set(members) != set(FILES) or members["binding.json"] != canonical(binding()):
            raise ValueError
        manifest = json.loads(members["manifest.json"])
        validated = validate_research_input_manifest(manifest)
        if members["manifest.json"] != canonical_research_input_manifest_bytes(validated):
            raise ValueError
        observed_at = validated["observed_at"]
        validate_soxl_core_only_free_split_close_completed_session(
            date_cutoff=DATE_CUTOFF, observed_at=observed_at
        )
        bind_sha = sha(members["binding.json"])
        if (
            validated["research_input_contract_id"] != INPUT_CONTRACT_ID
            or validated["profile"] != STUDY_ID
            or validated["artifact_type"] != "immutable_single_source_split_adjusted_close_etf_only"
            or validated["effective_at"] != observed_at
            or validated["as_of"] != observed_at
            or validated["calendar"] != {**CALENDAR, "session_date": DATE_CUTOFF, "source_revision": bind_sha}
            or validated["adjustment"] != {
                "policy": "split_adjusted", "source": "Twelve Data 1day adjust=splits; no cross-provider check",
                "source_revision": bind_sha,
            }
        ):
            raise ValueError
        expected_members = {name: content for name, content in members.items() if name in {"closes.json", "assurance.json"}}
        if {item["path"]: item for item in validated["members"]} != {
            name: {"path": name, "media_type": "application/json", "size_bytes": len(content), "sha256": sha(content)}
            for name, content in expected_members.items()
        }:
            raise ValueError
        closes = json.loads(members["closes.json"])
        report = json.loads(members["assurance.json"])
        if (
            members["closes.json"] != canonical(closes)
            or members["assurance.json"] != canonical(report)
            or set(closes) != {"schema_version", "series"}
            or closes["schema_version"] != "qsl.soxl-soxx-core-only-split-adjusted-close-series.v1"
            or set(closes["series"]) != set(SYMBOLS)
            or report["schema_version"] != "qsl.soxl-v7-twelve-single-source-assurance.v1"
            or report["assurance_level"] != ASSURANCE_LEVEL
            or report["cross_provider_verified"] is not False
            or report["source_id"] != TWELVE_DATA_DAILY_SOURCE_ID
            or report["date_cutoff"] != DATE_CUTOFF
            or set(report["symbols"]) != set(SYMBOLS)
        ):
            raise ValueError
        expected_dates = expected_soxl_core_only_sessions(DATE_CUTOFF)
        expected_sources: dict[str, str] = {}
        for symbol in SYMBOLS:
            rows = closes["series"][symbol]
            canonical_soxl_core_only_free_split_close_series_bytes(symbol=symbol, series=rows)
            expected = tuple(day.isoformat() for day in expected_dates[symbol])
            if tuple(row["session_date"] for row in rows) != expected:
                raise ValueError
            item = report["symbols"][symbol]
            if item != {
                "source_snapshot_sha256": item["source_snapshot_sha256"],
                "canonical_close_series_sha256": sha(
                    canonical_soxl_core_only_free_split_close_series_bytes(symbol=symbol, series=rows)
                ),
                "first_session": expected[0], "last_session": DATE_CUTOFF, "session_count": len(expected),
            }:
                raise ValueError
            expected_sources[f"{TWELVE_DATA_DAILY_SOURCE_ID}:{symbol}"] = item["source_snapshot_sha256"]
        if {item["source_id"]: item["content_sha256"] for item in validated["sources"]} != expected_sources:
            raise ValueError
        if {item["revision"] for item in validated["sources"]} != {bind_sha}:
            raise ValueError
        if {item["observed_at"] for item in validated["sources"]} != {observed_at}:
            raise ValueError
        return research_input_manifest_sha256(validated), closes["series"]
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise R6InputError("R6 single-source input invalid") from exc


def read_input(root: Path) -> dict[str, bytes]:
    if root.is_symlink() or not root.is_dir() or root.stat().st_mode & 0o777 != 0o700:
        raise R6InputError("R6 private input root invalid")
    if {path.name for path in root.iterdir()} != set(FILES):
        raise R6InputError("R6 private input members invalid")
    return {name: (root / name).read_bytes() for name in FILES}


def materialize_input(members: Mapping[str, bytes]) -> dict[str, object]:
    """Reuse V7 indicators only after independent R6 provenance validation."""
    manifest_sha, series = verify_input(members)
    result = _materialize_validated_close_series(
        series,
        p1_identity={
            "input_manifest_sha256": manifest_sha,
            "binding_sha256": sha(members["binding.json"]),
            "closes_member_sha256": sha(members["closes.json"]),
            "assurance_member_sha256": sha(members["assurance.json"]),
            "date_cutoff": DATE_CUTOFF,
        },
        p2_identity={"candidate_id": V7.candidate_id, "config_sha256": V7.config_sha256},
    )
    if len(result["sessions"]) < 756:
        raise R6InputError("R6 fixed long window unavailable")
    return result
