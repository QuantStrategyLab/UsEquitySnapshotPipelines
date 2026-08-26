"""Candidate-bound, two-source free OHLCV P1 input for free-data TQQQ candidates.

This module owns no credentials, broker, retry, scheduling, or storage policy.
It receives two source observations, admits a canonical Twelve Data OHLCV
series only after the pinned QPK assurance gate verifies it against Yahoo, and
then writes one immutable private P1 root.  It is deliberately separate from
the licensed Alpaca P1 contract used by earlier TQQQ candidates.
"""

from __future__ import annotations

import ctypes
import errno
import hashlib
import json
import math
import os
import re
import shutil
import stat
import sys
import tempfile
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Protocol

from quant_platform_kit.data.multisource_assurance import (
    DATA_ASSURANCE_STATUS_VERIFIED,
    SOURCE_OBSERVATION_READY,
    DailyBarSourceObservation,
    MultiSourceDailyBarPolicy,
    assess_multisource_daily_bars,
)
from quant_platform_kit.data.research_input import (
    canonical_research_input_manifest_bytes,
    validate_research_input_manifest,
)

from ..twelve_data_daily import TWELVE_DATA_DAILY_SOURCE_ID
from ..yahoo_finance_daily import YAHOO_FINANCE_DAILY_SOURCE_ID
from .tqqq_core_only_p1_binding import (
    FREE_OHLCV_INPUT_CONTRACT_ID,
    P2_V8_CONTRACT,
    TqqqCoreOnlyCandidateContract,
    _expected_xnys_sessions,
    build_tqqq_core_only_input_manifest,
    build_tqqq_core_only_p1_binding_for_contract,
    canonical_tqqq_core_only_p1_binding_bytes_for_contract,
    expected_tqqq_core_only_sessions_for_contract,
    tqqq_core_only_expected_source_ids,
    tqqq_core_only_input_contract_id,
    validate_tqqq_core_only_input_manifest,
    validate_tqqq_core_only_p1_binding_for_contract,
)

_BARS_SCHEMA = "tqqq_core_only_private_bars.v1"
_ASSURANCE_SCHEMA = "qsl.tqqq-core-only-free-ohlcv-assurance.v1"
_UNIVERSE = ("QQQ", "TQQQ", "QQQM", "BOXX")
_CANONICAL = TWELVE_DATA_DAILY_SOURCE_ID
_VERIFIER = YAHOO_FINANCE_DAILY_SOURCE_ID
_OUTPUT_FILENAMES = frozenset({"binding.json", "bars.json", "assurance.json", "manifest.json"})
_DIGEST = re.compile(r"^[0-9a-f]{64}$")


class TqqqCoreOnlyFreeOhlcvP1Error(ValueError):
    """Fail closed without provider detail or account material."""


class TqqqCoreOnlyFreeOhlcvP1UnavailableError(TqqqCoreOnlyFreeOhlcvP1Error):
    """A mandatory source is unavailable, incomplete, or disagrees."""


class TqqqCoreOnlyFreeOhlcvObserver(Protocol):
    """Injected observation port; P1 itself cannot read credentials."""

    def observe_daily_bars(
        self,
        *,
        source_id: str,
        symbol: str,
        start_date: str,
        date_cutoff: str,
    ) -> DailyBarSourceObservation: ...


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _assurance_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")
    ).hexdigest()


def _cutoff(value: object) -> str:
    if not isinstance(value, str):
        raise TqqqCoreOnlyFreeOhlcvP1Error("invalid free OHLCV date cutoff")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise TqqqCoreOnlyFreeOhlcvP1Error("invalid free OHLCV date cutoff") from exc
    sessions = _expected_xnys_sessions(value)
    if parsed.isoformat() != value or not sessions or sessions[-1] != parsed or parsed < date(2026, 8, 4):
        raise TqqqCoreOnlyFreeOhlcvP1Error("invalid free OHLCV date cutoff")
    return value


def _free_contract(contract: TqqqCoreOnlyCandidateContract) -> TqqqCoreOnlyCandidateContract:
    if tqqq_core_only_input_contract_id(contract) != FREE_OHLCV_INPUT_CONTRACT_ID:
        raise TqqqCoreOnlyFreeOhlcvP1Error("invalid free OHLCV candidate")
    return contract


def _policy(
    contract: TqqqCoreOnlyCandidateContract, symbol: str, cutoff: str
) -> MultiSourceDailyBarPolicy:
    frozen_contract = _free_contract(contract)
    return MultiSourceDailyBarPolicy(
        scope_id=f"{frozen_contract.candidate_id}:{symbol.lower()}",
        symbol=symbol,
        date_cutoff=cutoff,
        adjustment_basis="split_adjusted",
        required_source_ids=(_CANONICAL, _VERIFIER),
        required_price_fields=("open", "high", "low", "close"),
        compare_volume=False,
        price_relative_tolerance=0.0001,
    )


def _bar_rows(observation: DailyBarSourceObservation, expected: tuple[date, ...]) -> list[dict[str, object]]:
    if observation.status != SOURCE_OBSERVATION_READY or observation.snapshot is None:
        raise TqqqCoreOnlyFreeOhlcvP1UnavailableError("free OHLCV assurance not verified")
    bars = observation.snapshot.bars
    try:
        if tuple(date.fromisoformat(bar.session_date) for bar in bars) != expected:
            raise ValueError
        rows = [
            {
                "date": bar.session_date,
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "volume": float(bar.volume),
            }
            for bar in bars
        ]
    except (TypeError, ValueError) as exc:
        raise TqqqCoreOnlyFreeOhlcvP1UnavailableError("free OHLCV coverage not verified") from exc
    for row in rows:
        values = [row[key] for key in ("open", "high", "low", "close", "volume")]
        if not all(math.isfinite(float(value)) for value in values) or any(float(row[key]) <= 0 for key in ("open", "high", "low", "close")):
            raise TqqqCoreOnlyFreeOhlcvP1Error("invalid free OHLCV bar")
        if float(row["volume"]) < 0 or float(row["high"]) < max(float(row["open"]), float(row["low"]), float(row["close"])) or float(row["low"]) > min(float(row["open"]), float(row["high"]), float(row["close"])):
            raise TqqqCoreOnlyFreeOhlcvP1Error("invalid free OHLCV bar")
    return rows


def _assurance_member(
    *,
    contract: TqqqCoreOnlyCandidateContract,
    cutoff: str,
    reports: Mapping[str, object],
    bars: Mapping[str, object],
) -> dict[str, object]:
    frozen_contract = _free_contract(contract)
    if set(reports) != set(_UNIVERSE) or set(bars) != set(_UNIVERSE):
        raise TqqqCoreOnlyFreeOhlcvP1Error("invalid free OHLCV assurance")
    assurances: dict[str, object] = {}
    for symbol in _UNIVERSE:
        report = reports[symbol]
        diagnostic = getattr(report, "to_diagnostic", lambda: None)()
        if (
            getattr(report, "status", None) != DATA_ASSURANCE_STATUS_VERIFIED
            or getattr(report, "can_publish_research_input", False) is not True
            or not isinstance(diagnostic, Mapping)
            or getattr(report, "report_sha256", None) != _assurance_digest(diagnostic)
        ):
            raise TqqqCoreOnlyFreeOhlcvP1UnavailableError("free OHLCV assurance not verified")
        expected = {
            "schema_version": "qpk.multisource_daily_bar_assurance.v1",
            "policy_sha256": _policy(frozen_contract, symbol, cutoff).policy_sha256,
            "scope_id": f"{frozen_contract.candidate_id}:{symbol.lower()}",
            "symbol": symbol,
            "date_cutoff": cutoff,
            "status": DATA_ASSURANCE_STATUS_VERIFIED,
            "can_publish_research_input": True,
            "source_statuses": {_CANONICAL: SOURCE_OBSERVATION_READY, _VERIFIER: SOURCE_OBSERVATION_READY},
            "source_reason_codes": {},
            "findings": [],
        }
        if any(diagnostic.get(key) != value for key, value in expected.items()):
            raise TqqqCoreOnlyFreeOhlcvP1Error("invalid free OHLCV assurance")
        snapshots = diagnostic.get("source_snapshot_sha256")
        if not isinstance(snapshots, Mapping) or set(snapshots) != {_CANONICAL, _VERIFIER} or any(not isinstance(value, str) or not _DIGEST.fullmatch(value) for value in snapshots.values()):
            raise TqqqCoreOnlyFreeOhlcvP1Error("invalid free OHLCV assurance")
        assurances[symbol] = {
            "assurance_report_sha256": _assurance_digest(diagnostic),
            "canonical_bars_sha256": hashlib.sha256(_canonical({"bars": bars[symbol]})).hexdigest(),
            "diagnostic": dict(diagnostic),
        }
    return {"schema_version": _ASSURANCE_SCHEMA, "date_cutoff": cutoff, "assurances": assurances}


def validate_tqqq_core_only_free_ohlcv_assurance(
    value: object,
    *,
    bars: Mapping[str, object],
    cutoff: str,
    contract: TqqqCoreOnlyCandidateContract = P2_V8_CONTRACT,
) -> dict[str, dict[str, str]]:
    frozen_contract = _free_contract(contract)
    if not isinstance(value, Mapping) or set(value) != {"schema_version", "date_cutoff", "assurances"} or value.get("schema_version") != _ASSURANCE_SCHEMA or value.get("date_cutoff") != cutoff or not isinstance(value.get("assurances"), Mapping) or set(value["assurances"]) != set(_UNIVERSE):
        raise TqqqCoreOnlyFreeOhlcvP1Error("invalid free OHLCV assurance")
    snapshots: dict[str, dict[str, str]] = {}
    for symbol in _UNIVERSE:
        item = value["assurances"][symbol]
        if not isinstance(item, Mapping) or set(item) != {"assurance_report_sha256", "canonical_bars_sha256", "diagnostic"} or item["canonical_bars_sha256"] != hashlib.sha256(_canonical({"bars": bars[symbol]})).hexdigest():
            raise TqqqCoreOnlyFreeOhlcvP1Error("invalid free OHLCV assurance")
        diagnostic = item["diagnostic"]
        if not isinstance(diagnostic, Mapping) or item["assurance_report_sha256"] != _assurance_digest(diagnostic):
            raise TqqqCoreOnlyFreeOhlcvP1Error("invalid free OHLCV assurance")
        required = {
            "status": DATA_ASSURANCE_STATUS_VERIFIED,
            "can_publish_research_input": True,
            "policy_sha256": _policy(frozen_contract, symbol, cutoff).policy_sha256,
            "scope_id": f"{frozen_contract.candidate_id}:{symbol.lower()}",
            "symbol": symbol,
            "date_cutoff": cutoff,
        }
        if any(diagnostic.get(key) != expected for key, expected in required.items()):
            raise TqqqCoreOnlyFreeOhlcvP1Error("invalid free OHLCV assurance")
        source_snapshots = diagnostic.get("source_snapshot_sha256")
        if not isinstance(source_snapshots, Mapping) or set(source_snapshots) != {_CANONICAL, _VERIFIER} or any(not isinstance(digest, str) or not _DIGEST.fullmatch(digest) for digest in source_snapshots.values()):
            raise TqqqCoreOnlyFreeOhlcvP1Error("invalid free OHLCV assurance")
        snapshots[symbol] = {source: str(source_snapshots[source]) for source in (_CANONICAL, _VERIFIER)}
    return snapshots


def _root_payload(
    binding: object,
    manifest: object,
    bars: object,
    assurance: object,
    *,
    contract: TqqqCoreOnlyCandidateContract,
) -> tuple[dict[str, object], dict[str, object], dict[str, object], dict[str, object]]:
    if not all(isinstance(value, Mapping) for value in (binding, manifest, bars, assurance)):
        raise TqqqCoreOnlyFreeOhlcvP1Error("invalid free OHLCV input root")
    frozen_contract = _free_contract(contract)
    frozen = validate_tqqq_core_only_p1_binding_for_contract(binding, frozen_contract)
    validated_manifest = validate_tqqq_core_only_input_manifest(manifest, frozen, contract=frozen_contract)
    del validated_manifest
    if not isinstance(bars.get("symbols"), Mapping) or bars.get("schema_version") != _BARS_SCHEMA or set(bars["symbols"]) != set(_UNIVERSE):
        raise TqqqCoreOnlyFreeOhlcvP1Error("invalid free OHLCV input root")
    expected = expected_tqqq_core_only_sessions_for_contract(frozen_contract, date_cutoff=frozen["data_identity"]["date_cutoff"])
    for symbol in _UNIVERSE:
        payload = bars["symbols"][symbol]
        if not isinstance(payload, Mapping) or set(payload) != {"bars"} or not isinstance(payload["bars"], list):
            raise TqqqCoreOnlyFreeOhlcvP1Error("invalid free OHLCV input root")
        observed = tuple(date.fromisoformat(str(row.get("date"))) for row in payload["bars"] if isinstance(row, Mapping))
        if len(observed) != len(payload["bars"]) or observed != expected[symbol]:
            raise TqqqCoreOnlyFreeOhlcvP1Error("incomplete free OHLCV coverage")
    return dict(frozen), dict(manifest), dict(bars), dict(assurance)


def validate_tqqq_core_only_free_ohlcv_input_payload(
    value: object, *, contract: TqqqCoreOnlyCandidateContract = P2_V8_CONTRACT
) -> str:
    """Validate parsed P1 members before P3, including two-source assurance."""
    if not isinstance(value, Mapping) or set(value) != {"binding", "input_manifest", "bars", "assurance"}:
        raise TqqqCoreOnlyFreeOhlcvP1Error("invalid free OHLCV input root")
    frozen_contract = _free_contract(contract)
    binding, manifest, bars, assurance = _root_payload(
        value["binding"], value["input_manifest"], value["bars"], value["assurance"], contract=frozen_contract
    )
    cutoff = str(binding["data_identity"]["date_cutoff"])
    snapshots = validate_tqqq_core_only_free_ohlcv_assurance(
        assurance, bars=bars["symbols"], cutoff=cutoff, contract=frozen_contract
    )
    expected_sources = {
        f"{source}:{symbol}": snapshots[symbol][source]
        for symbol in _UNIVERSE
        for source in (_CANONICAL, _VERIFIER)
    }
    actual_sources = {source["source_id"]: source["content_sha256"] for source in manifest["sources"]}
    if actual_sources != expected_sources or set(actual_sources) != tqqq_core_only_expected_source_ids(frozen_contract):
        raise TqqqCoreOnlyFreeOhlcvP1Error("free OHLCV source identity mismatch")
    bars_bytes = _canonical(bars)
    assurance_bytes = _canonical(assurance)
    members = {member["path"]: member for member in manifest["members"]}
    expected_members = {
        "bars.json": bars_bytes,
        "assurance.json": assurance_bytes,
    }
    if set(members) != set(expected_members) or any(members[path] != {"path": path, "media_type": "application/json", "size_bytes": len(content), "sha256": hashlib.sha256(content).hexdigest()} for path, content in expected_members.items()):
        raise TqqqCoreOnlyFreeOhlcvP1Error("free OHLCV member identity mismatch")
    return hashlib.sha256(canonical_research_input_manifest_bytes(manifest)).hexdigest()


def _publish_noreplace(source: Path, destination: Path) -> None:
    if not sys.platform.startswith("linux"):
        raise TqqqCoreOnlyFreeOhlcvP1Error("unsupported immutable publish platform")
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise TqqqCoreOnlyFreeOhlcvP1Error("required no-clobber capability unavailable")
    flags = getattr(os, "O_PATH", os.O_RDONLY) | os.O_DIRECTORY | os.O_NOFOLLOW
    parent = os.open(destination.parent, flags)
    try:
        result = renameat2(parent, source.name.encode(), parent, destination.name.encode(), 1)
    finally:
        os.close(parent)
    if result != 0:
        raise TqqqCoreOnlyFreeOhlcvP1Error("immutable output already exists" if ctypes.get_errno() == errno.EEXIST else "atomic immutable publish failed")


def publish_tqqq_core_only_free_ohlcv_p1_inputs(
    observer: TqqqCoreOnlyFreeOhlcvObserver,
    *,
    output_root: str | Path,
    observed_at: str,
    producer: Mapping[str, object],
    date_cutoff: str,
    contract: TqqqCoreOnlyCandidateContract = P2_V8_CONTRACT,
) -> dict[str, object]:
    """Publish only after both free sources cover and agree on every OHLC session."""
    cutoff = _cutoff(date_cutoff)
    frozen_contract = _free_contract(contract)
    destination = Path(output_root)
    if destination.exists() or destination.is_symlink() or destination.parent.is_symlink() or not destination.parent.is_dir():
        raise TqqqCoreOnlyFreeOhlcvP1Error("immutable output unavailable")
    binding = build_tqqq_core_only_p1_binding_for_contract(frozen_contract, date_cutoff=cutoff)
    expected = expected_tqqq_core_only_sessions_for_contract(frozen_contract, date_cutoff=cutoff)
    symbols: dict[str, object] = {}
    reports: dict[str, object] = {}
    for symbol in _UNIVERSE:
        try:
            observations = tuple(observer.observe_daily_bars(source_id=source, symbol=symbol, start_date=expected[symbol][0].isoformat(), date_cutoff=cutoff) for source in (_CANONICAL, _VERIFIER))
            report = assess_multisource_daily_bars(_policy(frozen_contract, symbol, cutoff), observations)
        except Exception as exc:
            raise TqqqCoreOnlyFreeOhlcvP1Error("free OHLCV source observation failed") from exc
        if report.status != DATA_ASSURANCE_STATUS_VERIFIED or not report.can_publish_research_input:
            raise TqqqCoreOnlyFreeOhlcvP1UnavailableError("free OHLCV assurance not verified")
        symbols[symbol] = {"bars": _bar_rows(observations[0], expected[symbol])}
        reports[symbol] = report
    bars = {"schema_version": _BARS_SCHEMA, "symbols": symbols}
    assurance = _assurance_member(contract=frozen_contract, cutoff=cutoff, reports=reports, bars=symbols)
    bars_bytes, assurance_bytes = _canonical(bars), _canonical(assurance)
    snapshots = validate_tqqq_core_only_free_ohlcv_assurance(
        assurance, bars=symbols, cutoff=cutoff, contract=frozen_contract
    )
    manifest = build_tqqq_core_only_input_manifest(
        binding, observed_at=observed_at, producer=producer, member_bytes=bars_bytes,
        source_content_sha256={f"{source}:{symbol}": snapshots[symbol][source] for symbol in _UNIVERSE for source in (_CANONICAL, _VERIFIER)},
        contract=frozen_contract,
    )
    manifest["members"].append({"path": "assurance.json", "media_type": "application/json", "size_bytes": len(assurance_bytes), "sha256": hashlib.sha256(assurance_bytes).hexdigest()})
    manifest["members"].sort(key=lambda member: str(member["path"]))
    manifest = validate_research_input_manifest(manifest)
    manifest_bytes = canonical_research_input_manifest_bytes(manifest)
    temporary = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
    try:
        temporary.chmod(0o700)
        (temporary / "binding.json").write_bytes(canonical_tqqq_core_only_p1_binding_bytes_for_contract(binding, frozen_contract))
        (temporary / "bars.json").write_bytes(bars_bytes)
        (temporary / "assurance.json").write_bytes(assurance_bytes)
        (temporary / "manifest.json").write_bytes(manifest_bytes)
        result = verify_tqqq_core_only_free_ohlcv_p1_input_root(temporary, contract=frozen_contract)
        _publish_noreplace(temporary, destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return {"manifest_sha256": result, "status": "P1_FREE_OHLCV_INPUTS_PUBLISHED"}


def verify_tqqq_core_only_free_ohlcv_p1_input_root(
    output_root: str | Path, *, contract: TqqqCoreOnlyCandidateContract = P2_V8_CONTRACT
) -> str:
    """Verify one free-data P1 root without provider or credential access."""
    root = Path(output_root)
    frozen_contract = _free_contract(contract)
    try:
        if root.is_symlink() or not root.is_dir() or stat.S_IMODE(root.stat().st_mode) != 0o700 or {path.name for path in root.iterdir()} != _OUTPUT_FILENAMES:
            raise ValueError
        members = {name: (root / name).read_bytes() for name in _OUTPUT_FILENAMES}
        parsed = {name.removesuffix(".json") if name != "manifest.json" else "input_manifest": json.loads(content) for name, content in members.items()}
        if members["binding.json"] != canonical_tqqq_core_only_p1_binding_bytes_for_contract(parsed["binding"], frozen_contract) or members["bars.json"] != _canonical(parsed["bars"]) or members["assurance.json"] != _canonical(parsed["assurance"]) or members["manifest.json"] != canonical_research_input_manifest_bytes(parsed["input_manifest"]):
            raise ValueError
        return validate_tqqq_core_only_free_ohlcv_input_payload(parsed, contract=frozen_contract)
    except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError, TqqqCoreOnlyFreeOhlcvP1Error):
        raise TqqqCoreOnlyFreeOhlcvP1Error("invalid free OHLCV input root") from None


__all__ = [
    "TqqqCoreOnlyFreeOhlcvObserver",
    "TqqqCoreOnlyFreeOhlcvP1Error",
    "TqqqCoreOnlyFreeOhlcvP1UnavailableError",
    "publish_tqqq_core_only_free_ohlcv_p1_inputs",
    "validate_tqqq_core_only_free_ohlcv_assurance",
    "validate_tqqq_core_only_free_ohlcv_input_payload",
    "verify_tqqq_core_only_free_ohlcv_p1_input_root",
]
