"""Static P1 data identity binding for the frozen TQQQ core-only candidate."""

from __future__ import annotations

import ctypes
import errno
import hashlib
import json
import os
import re
import shutil
import stat
import sys
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Protocol

from quant_platform_kit.data.research_input import (
    canonical_research_input_manifest_bytes,
    research_input_manifest_sha256,
    validate_research_input_manifest,
)

CANDIDATE_ID = "tqqq_core_only_p2_v1"
CANDIDATE_CONFIG_SHA256 = "969cae10850f5a2d72c17fedd77689301411f62dc24d9a530026e3f7efdc1c69"
UES_REVISION = "8b6b418bac74318f8054c5951521c9b62391de3e"
P2_V2_CANDIDATE_ID = "tqqq_core_only_p2_v2"
P2_V2_CANDIDATE_CONFIG_SHA256 = "f1d6e4cf8aa0f7ab818768fb6a6e9c86bcd03cc567e5a5a844024a446a43bd31"
P2_V2_UES_REVISION = "5f0c30cdcaf3ee0f3f1c050acbe172580ea40c81"
P2_V4_CANDIDATE_ID = "tqqq_core_only_p2_v4"
P2_V4_CANDIDATE_CONFIG_SHA256 = "b20335a16d0c5001dc28d3a1555dc1d46e6331fc714ca489a952d779de3279f1"
P2_V4_UES_REVISION = P2_V2_UES_REVISION
P2_V5_CANDIDATE_ID = "tqqq_core_only_p2_v5"
P2_V5_CANDIDATE_CONFIG_SHA256 = "e6422cf7c3819734ec300a7bfa3d936d5273993c0ce865dfe0218d7b7f8426e2"
P2_V5_UES_REVISION = P2_V2_UES_REVISION
P2_V7_CANDIDATE_ID = "tqqq_core_only_p2_v7_relative_benchmark"
P2_V7_CANDIDATE_CONFIG_SHA256 = "455fd66ad56734a291cfcfecacb63fef7bf7bfa5857f3a2f2f92bba169a18a12"
P2_V7_UES_REVISION = P2_V2_UES_REVISION
P2_V8_CANDIDATE_ID = "tqqq_core_only_p2_v8_free_ohlcv_relative_benchmark"
P2_V8_CANDIDATE_CONFIG_SHA256 = "94cb9832bd32afc07b65922f684283c1cf55b9ffd0b0d798838ad7b96f11ee14"
P2_V8_UES_REVISION = P2_V2_UES_REVISION
INPUT_CONTRACT_ID = "tqqq_core_only_alpaca_sip_adjustment_all.v1"
FREE_OHLCV_INPUT_CONTRACT_ID = "tqqq_core_only_free_split_adjusted_ohlcv_assured.v1"
_INPUT_SCHEMA = "qsl.tqqq_core_only_p1_data_binding.v1"
_UNIVERSE = ("QQQ", "TQQQ", "QQQM", "BOXX")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_OUTPUT_FILENAMES = frozenset({"bars.json", "binding.json", "manifest.json"})
_REMOTE_COMPLETION_SCHEMA = "qsl.tqqq-p1-p3-remote-completion.v1"
REMOTE_COMPLETION_FILENAME = "p1-complete.json"
_P2_EARLIEST_TRAIN_SESSION = date(2018, 1, 2)
_FIRST_ELIGIBLE_SESSION = {"QQQM": date(2020, 10, 13), "BOXX": date(2022, 12, 28)}
_P2_V5_MINIMUM_DATE_CUTOFF = "2026-08-04"


@dataclass(frozen=True)
class TqqqCoreOnlyCandidateContract:
    """One frozen TQQQ candidate's source/config identity for private inputs."""

    candidate_id: str
    config_sha256: str
    ues_revision: str
    qpk_revision: str


_P2_V1_CONTRACT = TqqqCoreOnlyCandidateContract(
    candidate_id=CANDIDATE_ID,
    config_sha256=CANDIDATE_CONFIG_SHA256,
    ues_revision=UES_REVISION,
    qpk_revision="730ad9f3983bd90cd75adecb67fcf483ffb96736",
)
P2_V2_CONTRACT = TqqqCoreOnlyCandidateContract(
    candidate_id=P2_V2_CANDIDATE_ID,
    config_sha256=P2_V2_CANDIDATE_CONFIG_SHA256,
    ues_revision=P2_V2_UES_REVISION,
    qpk_revision="730ad9f3983bd90cd75adecb67fcf483ffb96736",
)
P2_V4_CONTRACT = TqqqCoreOnlyCandidateContract(
    candidate_id=P2_V4_CANDIDATE_ID,
    config_sha256=P2_V4_CANDIDATE_CONFIG_SHA256,
    ues_revision=P2_V4_UES_REVISION,
    qpk_revision="730ad9f3983bd90cd75adecb67fcf483ffb96736",
)
P2_V5_CONTRACT = TqqqCoreOnlyCandidateContract(
    candidate_id=P2_V5_CANDIDATE_ID,
    config_sha256=P2_V5_CANDIDATE_CONFIG_SHA256,
    ues_revision=P2_V5_UES_REVISION,
    qpk_revision="730ad9f3983bd90cd75adecb67fcf483ffb96736",
)
P2_V7_CONTRACT = TqqqCoreOnlyCandidateContract(
    candidate_id=P2_V7_CANDIDATE_ID,
    config_sha256=P2_V7_CANDIDATE_CONFIG_SHA256,
    ues_revision=P2_V7_UES_REVISION,
    qpk_revision="730ad9f3983bd90cd75adecb67fcf483ffb96736",
)
P2_V8_CONTRACT = TqqqCoreOnlyCandidateContract(
    candidate_id=P2_V8_CANDIDATE_ID,
    config_sha256=P2_V8_CANDIDATE_CONFIG_SHA256,
    ues_revision=P2_V8_UES_REVISION,
    qpk_revision="730ad9f3983bd90cd75adecb67fcf483ffb96736",
)
_SUPPORTED_CONTRACTS = {
    _P2_V1_CONTRACT.candidate_id: _P2_V1_CONTRACT,
    P2_V2_CONTRACT.candidate_id: P2_V2_CONTRACT,
    P2_V4_CONTRACT.candidate_id: P2_V4_CONTRACT,
    P2_V5_CONTRACT.candidate_id: P2_V5_CONTRACT,
    P2_V7_CONTRACT.candidate_id: P2_V7_CONTRACT,
    P2_V8_CONTRACT.candidate_id: P2_V8_CONTRACT,
}


class TqqqCoreOnlyHistoricalBarsProvider(Protocol):
    """Injected data-only port for one canonical adjusted historical-bars request per symbol."""

    def fetch_historical_bars(
        self,
        *,
        symbol: str,
        calendar_id: str,
        timezone: str,
        adjustment_policy: str,
        feed: str,
        date_cutoff: str,
    ) -> Mapping[str, object]: ...


class TqqqCoreOnlyP1BindingError(ValueError):
    """Sanitized failure for an invalid static P1 binding."""


class TqqqCoreOnlyP1InputUnavailableError(TqqqCoreOnlyP1BindingError):
    """The fixed P1 provider cannot currently supply a usable frozen input."""


def _canonical(value: Mapping[str, object]) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def build_tqqq_core_only_p1_cloud_storage_binding() -> dict[str, object]:
    """Return the bounded private-cloud retention contract for P1/P3 inputs.

    This is a storage *boundary*, not a bucket locator.  It intentionally keeps
    the concrete object-store address out of the immutable research binding.
    """
    return {
        "provider": "GOOGLE_CLOUD_STORAGE",
        "access_scope": "PRIVATE",
        "public_access_prevention": "enforced",
        "raw_snapshot_lifecycle": {
            "policy": "SHORT_TERM_PRIVATE_CLOUD_RESEARCH_SNAPSHOT_NO_REDISTRIBUTION",
            "active_lifecycle_days": 7,
            "soft_delete_lifecycle_days": 7,
            "retention_extension_authorized": False,
            "retention_decision": "PENDING_LICENSE_AND_RETENTION_REVIEW",
        },
        "evidence_metadata_boundary": {
            "logical_separation_from_raw_snapshot": True,
            "shares_raw_snapshot_lifecycle": True,
            "separate_or_long_term_retention_authorized": False,
            "write_mode": "CREATE_ONLY",
            "raw_bars_included": False,
            "content": "DIGESTS_AND_NON_SENSITIVE_RESEARCH_PROVENANCE_ONLY",
        },
    }


def resolve_tqqq_core_only_candidate_contract(candidate_id: object) -> TqqqCoreOnlyCandidateContract:
    """Return a known frozen candidate identity without authorizing acquisition."""
    if not isinstance(candidate_id, str):
        raise TqqqCoreOnlyP1BindingError("unknown TQQQ core-only candidate")
    try:
        return _SUPPORTED_CONTRACTS[candidate_id]
    except KeyError as exc:
        raise TqqqCoreOnlyP1BindingError("unknown TQQQ core-only candidate") from exc


def _require_contract(
    contract: TqqqCoreOnlyCandidateContract,
) -> TqqqCoreOnlyCandidateContract:
    if (
        type(contract) is not TqqqCoreOnlyCandidateContract
        or _SUPPORTED_CONTRACTS.get(contract.candidate_id) != contract
    ):
        raise TqqqCoreOnlyP1BindingError("unknown TQQQ core-only candidate")
    return contract


def tqqq_core_only_input_contract_id(contract: TqqqCoreOnlyCandidateContract) -> str:
    """Return the immutable input contract identifier for a known candidate."""
    return (
        FREE_OHLCV_INPUT_CONTRACT_ID
        if _require_contract(contract) == P2_V8_CONTRACT
        else INPUT_CONTRACT_ID
    )


def tqqq_core_only_expected_source_ids(contract: TqqqCoreOnlyCandidateContract) -> frozenset[str]:
    """Return the exact manifest source IDs without performing acquisition."""
    frozen_contract = _require_contract(contract)
    if frozen_contract == P2_V8_CONTRACT:
        return frozenset(
            f"{source_id}:{symbol}"
            for source_id in (
                "twelve_data_1day_split_adjusted",
                "yahoo_finance_chart_1day_split_adjusted",
            )
            for symbol in _UNIVERSE
        )
    return frozenset(f"alpaca_sip_1day_adjustment_all:{symbol}" for symbol in _UNIVERSE)


def build_tqqq_core_only_p1_binding_for_contract(
    contract: TqqqCoreOnlyCandidateContract,
    *,
    date_cutoff: str | None = None,
) -> dict[str, object]:
    """Return a data-only identity; this function performs no acquisition.

    v1--v4 retain their exact historical cutoffs.  The rolling v5 and v7
    candidates bind a caller supplied, completed XNYS date into each immutable
    daily input root.  The cutoff is an input identity, never a mutable
    strategy parameter.
    """
    frozen_contract = _require_contract(contract)
    if frozen_contract in {P2_V5_CONTRACT, P2_V7_CONTRACT, P2_V8_CONTRACT}:
        resolved_date_cutoff = _validate_p2_v5_date_cutoff(date_cutoff)
    elif date_cutoff is not None:
        raise TqqqCoreOnlyP1BindingError("unexpected TQQQ core-only date cutoff")
    else:
        resolved_date_cutoff = (
            "2026-08-04" if frozen_contract == P2_V4_CONTRACT else "2026-07-31"
        )
    data_identity: dict[str, object] = {
        "calendar": {
            "calendar_id": "XNYS",
            "timezone": "America/New_York",
            "source": "exchange_calendars",
        },
        "universe": list(_UNIVERSE),
        "date_cutoff": resolved_date_cutoff,
        "cost_assumptions": {
            "turnover_cost_bps": 5.0,
            "stress_turnover_cost_bps": [10.0, 15.0]
            if frozen_contract == P2_V8_CONTRACT
            else [10.0, 25.0],
            "borrow_cost_bps": 0.0,
            "cash_yield_assumption": 0.0,
            "execution_timing": "next_complete_trading_session_after_signal_effective_date",
        },
        "retention": {
            "policy": "PRIVATE_CLOUD_SHORT_TERM_RESEARCH_SNAPSHOT_NO_REDISTRIBUTION",
            "redistribution_allowed": False,
            "long_term_retention_authorized": False,
        },
    }
    if frozen_contract == P2_V8_CONTRACT:
        data_identity.update(
            {
                "provider": "TWELVE_DATA_AND_YAHOO_FINANCE",
                "adjustment": {
                    "policy": "split_adjusted",
                    "source": "Twelve Data canonical split-adjusted OHLCV verified by Yahoo Finance",
                },
                "assurance": {
                    "canonical_source_id": "twelve_data_1day_split_adjusted",
                    "verifier_source_id": "yahoo_finance_chart_1day_split_adjusted",
                    "required_price_fields": ["open", "high", "low", "close"],
                    "compare_volume": False,
                    "price_relative_tolerance": 0.0001,
                },
            }
        )
    else:
        data_identity.update(
            {
                "provider": "ALPACA_MARKET_DATA",
                "feed": "SIP",
                "adjustment": {
                    "policy": "total_return_adjusted",
                    "source": "ALPACA_MARKET_DATA adjustment=all(split,dividend,spin-off)",
                },
            }
        )
    return {
        "schema_version": _INPUT_SCHEMA,
        "candidate": {
            "candidate_id": frozen_contract.candidate_id,
            "config_sha256": frozen_contract.config_sha256,
        },
        "source": {
            "repository": "QuantStrategyLab/UsEquityStrategies",
            "revision": frozen_contract.ues_revision,
        },
        "cloud_storage": build_tqqq_core_only_p1_cloud_storage_binding(),
        "data_identity": data_identity,
    }


def build_tqqq_core_only_p1_binding() -> dict[str, object]:
    """Return the original frozen v1 data-only identity."""
    return build_tqqq_core_only_p1_binding_for_contract(_P2_V1_CONTRACT)


def canonical_binding_bytes(value: Mapping[str, object]) -> bytes:
    """Validate and encode one exact binding in canonical form."""
    validated = validate_tqqq_core_only_p1_binding(value)
    return _canonical(validated)


def binding_sha256(value: Mapping[str, object]) -> str:
    return hashlib.sha256(canonical_binding_bytes(value)).hexdigest()


def validate_tqqq_core_only_p1_binding(value: Mapping[str, object]) -> dict[str, object]:
    """Reject any binding other than the original P2 v1 source/config/data identity."""
    return validate_tqqq_core_only_p1_binding_for_contract(value, _P2_V1_CONTRACT)


def validate_tqqq_core_only_p1_binding_for_contract(
    value: Mapping[str, object],
    contract: TqqqCoreOnlyCandidateContract,
) -> dict[str, object]:
    """Reject a binding that is not exact for its immutable candidate identity."""
    frozen_contract = _require_contract(contract)
    date_cutoff: str | None = None
    if frozen_contract in {P2_V5_CONTRACT, P2_V7_CONTRACT, P2_V8_CONTRACT}:
        try:
            identity = value["data_identity"]
            if not isinstance(identity, Mapping):
                raise TypeError
            date_cutoff = identity["date_cutoff"]
        except (KeyError, TypeError):
            raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only P1 binding") from None
    expected = build_tqqq_core_only_p1_binding_for_contract(
        frozen_contract, date_cutoff=date_cutoff
    )
    if not isinstance(value, Mapping) or dict(value) != expected:
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only P1 binding")
    return expected


def canonical_tqqq_core_only_p1_binding_bytes_for_contract(
    value: Mapping[str, object],
    contract: TqqqCoreOnlyCandidateContract,
) -> bytes:
    return _canonical(validate_tqqq_core_only_p1_binding_for_contract(value, contract))


def tqqq_core_only_p1_binding_sha256_for_contract(
    value: Mapping[str, object],
    contract: TqqqCoreOnlyCandidateContract,
) -> str:
    return hashlib.sha256(
        canonical_tqqq_core_only_p1_binding_bytes_for_contract(value, contract)
    ).hexdigest()


def build_tqqq_core_only_input_manifest(
    binding: Mapping[str, object],
    *,
    observed_at: str,
    producer: Mapping[str, object],
    member_bytes: bytes,
    source_content_sha256: Mapping[str, str],
    contract: TqqqCoreOnlyCandidateContract = _P2_V1_CONTRACT,
) -> dict[str, object]:
    """Build the future immutable-input manifest from one already-collected member."""
    frozen_contract = _require_contract(contract)
    frozen = validate_tqqq_core_only_p1_binding_for_contract(binding, frozen_contract)
    normalized_source_content_sha256: Mapping[str, str]
    if frozen_contract == P2_V8_CONTRACT:
        normalized_source_content_sha256 = source_content_sha256
    else:
        normalized_source_content_sha256 = {
            f"alpaca_sip_1day_adjustment_all:{symbol}": digest
            for symbol, digest in source_content_sha256.items()
        }
    if (
        not isinstance(member_bytes, bytes)
        or set(normalized_source_content_sha256)
        != tqqq_core_only_expected_source_ids(frozen_contract)
        or any(
            not isinstance(digest, str) or not _DIGEST.fullmatch(digest)
            for digest in normalized_source_content_sha256.values()
        )
    ):
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only input member")
    identity = frozen["data_identity"]
    assert isinstance(identity, dict)
    binding_digest = tqqq_core_only_p1_binding_sha256_for_contract(frozen, frozen_contract)
    manifest = validate_research_input_manifest(
        {
            "schema_version": "research_input_manifest.v1",
            "manifest_id": f"tqqq-core-only-{binding_digest[:24]}-{hashlib.sha256(member_bytes).hexdigest()[:24]}",
            "research_input_contract_id": tqqq_core_only_input_contract_id(frozen_contract),
            "domain": "us_equity",
            "profile": frozen_contract.candidate_id,
            "artifact_type": (
                "immutable_assured_split_adjusted_ohlcv_etf_only"
                if frozen_contract == P2_V8_CONTRACT
                else "immutable_adjusted_ohlcv_etf_only"
            ),
            "observed_at": observed_at,
            "effective_at": observed_at,
            "as_of": observed_at,
            "producer": dict(producer),
            "calendar": {
                **identity["calendar"],
                "session_date": identity["date_cutoff"],
                "source_revision": binding_digest,
            },
            "adjustment": {
                **identity["adjustment"],
                "source_revision": binding_digest,
            },
            "sources": [
                {
                    "source_id": source_id,
                    "revision": binding_digest,
                    "observed_at": observed_at,
                    "content_sha256": normalized_source_content_sha256[source_id],
                }
                for source_id in sorted(tqqq_core_only_expected_source_ids(frozen_contract))
            ],
            "members": [
                {
                    "path": "bars.json",
                    "media_type": "application/json",
                    "size_bytes": len(member_bytes),
                    "sha256": hashlib.sha256(member_bytes).hexdigest(),
                }
            ],
        }
    )
    validate_tqqq_core_only_input_manifest(manifest, frozen, contract=frozen_contract)
    return manifest


def validate_tqqq_core_only_input_manifest(
    manifest: Mapping[str, object],
    binding: Mapping[str, object],
    *,
    contract: TqqqCoreOnlyCandidateContract = _P2_V1_CONTRACT,
) -> str:
    """Validate a QPK immutable-input manifest against the frozen static binding."""
    frozen_contract = _require_contract(contract)
    frozen = validate_tqqq_core_only_p1_binding_for_contract(binding, frozen_contract)
    try:
        validated = validate_research_input_manifest(manifest)
    except ValueError as exc:
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only input manifest") from exc
    identity = frozen["data_identity"]
    assert isinstance(identity, dict)
    binding_digest = tqqq_core_only_p1_binding_sha256_for_contract(frozen, frozen_contract)
    expected_source_ids = tqqq_core_only_expected_source_ids(frozen_contract)
    sources = validated["sources"]
    if (
        validated["research_input_contract_id"] != tqqq_core_only_input_contract_id(frozen_contract)
        or validated["domain"] != "us_equity"
        or validated["profile"] != frozen_contract.candidate_id
        or validated["artifact_type"]
        != (
            "immutable_assured_split_adjusted_ohlcv_etf_only"
            if frozen_contract == P2_V8_CONTRACT
            else "immutable_adjusted_ohlcv_etf_only"
        )
        or validated["calendar"]
        != {
            **identity["calendar"],
            "session_date": identity["date_cutoff"],
            "source_revision": binding_digest,
        }
        or validated["adjustment"]
        != {
            **identity["adjustment"],
            "source_revision": binding_digest,
        }
        or {source["source_id"] for source in sources} != expected_source_ids
        or {source["revision"] for source in sources} != {binding_digest}
    ):
        raise TqqqCoreOnlyP1BindingError("TQQQ core-only input binding mismatch")
    return research_input_manifest_sha256(validated)


def _publish_noreplace(source: Path, destination: Path) -> None:
    """Atomically publish a prepared private root without replacing an existing one."""
    if sys.platform.startswith("linux"):
        libc = ctypes.CDLL(None, use_errno=True)
        renameat2 = getattr(libc, "renameat2", None)
        if renameat2 is None or not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
            raise TqqqCoreOnlyP1BindingError("required no-clobber capability unavailable")
        renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        renameat2.restype = ctypes.c_int
        parent_flags = getattr(os, "O_PATH", os.O_RDONLY) | os.O_DIRECTORY | os.O_NOFOLLOW
        parent_fd = os.open(destination.parent, parent_flags)
        try:
            result = renameat2(parent_fd, source.name.encode(), parent_fd, destination.name.encode(), 1)
        finally:
            os.close(parent_fd)
        if result != 0:
            if ctypes.get_errno() == errno.EEXIST:
                raise TqqqCoreOnlyP1BindingError("immutable output already exists")
            raise TqqqCoreOnlyP1BindingError("atomic no-clobber publish failed")
        return
    if sys.platform == "darwin":
        libc = ctypes.CDLL(None, use_errno=True)
        renamex_np = getattr(libc, "renamex_np", None)
        if renamex_np is None or not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
            raise TqqqCoreOnlyP1BindingError("required no-clobber capability unavailable")
        renamex_np.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        renamex_np.restype = ctypes.c_int
        if renamex_np(str(source).encode(), str(destination).encode(), 4) != 0:
            if ctypes.get_errno() == errno.EEXIST:
                raise TqqqCoreOnlyP1BindingError("immutable output already exists")
            raise TqqqCoreOnlyP1BindingError("atomic no-clobber publish failed")
        return
    raise TqqqCoreOnlyP1BindingError("unsupported platform for atomic no-clobber publish")


def _require_new_private_output_root(output_root: str | Path) -> Path:
    destination = Path(output_root)
    if destination.exists() or destination.is_symlink():
        raise TqqqCoreOnlyP1BindingError("immutable output already exists")
    if destination.parent.is_symlink() or not destination.parent.is_dir():
        raise TqqqCoreOnlyP1BindingError("output parent is unavailable")
    return destination


def _observed_fixed_holiday(day: date) -> date:
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def _nth_weekday(year: int, month: int, weekday: int, occurrence: int) -> date:
    current = date(year, month, 1)
    return current + timedelta(days=(weekday - current.weekday()) % 7 + 7 * (occurrence - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    current = date(year + (month == 12), 1 if month == 12 else month + 1, 1) - timedelta(days=1)
    return current - timedelta(days=(current.weekday() - weekday) % 7)


def _easter_sunday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    length = (32 + 2 * e + 2 * i - h - k) % 7
    month = (h + length - 7 * ((a + 11 * h + 22 * length) // 451) + 114) // 31
    day = (h + length - 7 * ((a + 11 * h + 22 * length) // 451) + 114) % 31 + 1
    return date(year, month, day)


def _xnys_holidays(year: int) -> set[date]:
    new_year = date(year, 1, 1)
    if new_year.weekday() == 6:
        new_year += timedelta(days=1)
    holidays = {
        new_year,
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _easter_sunday(year) - timedelta(days=2),
        _last_weekday(year, 5, 0),
        _observed_fixed_holiday(date(year, 7, 4)),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        _observed_fixed_holiday(date(year, 12, 25)),
    }
    if year >= 2022:
        holidays.add(_observed_fixed_holiday(date(year, 6, 19)))
    holidays.update({date(2018, 12, 5), date(2025, 1, 9)})
    return holidays


def _expected_xnys_sessions(date_cutoff: str) -> tuple[date, ...]:
    try:
        cutoff = date.fromisoformat(date_cutoff)
    except (TypeError, ValueError) as exc:
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only historical coverage") from exc
    holidays = set().union(
        *(_xnys_holidays(year) for year in range(_P2_EARLIEST_TRAIN_SESSION.year, cutoff.year + 1))
    )
    sessions: list[date] = []
    current = _P2_EARLIEST_TRAIN_SESSION
    while current <= cutoff:
        if current.weekday() < 5 and current not in holidays:
            sessions.append(current)
        current += timedelta(days=1)
    return tuple(sessions)


def _validate_p2_v5_date_cutoff(value: object) -> str:
    """Accept only a completed XNYS session within v5's fixed research policy."""
    if not isinstance(value, str):
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only date cutoff")
    try:
        cutoff = date.fromisoformat(value)
        minimum = date.fromisoformat(_P2_V5_MINIMUM_DATE_CUTOFF)
    except ValueError as exc:
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only date cutoff") from exc
    sessions = _expected_xnys_sessions(value)
    if cutoff < minimum or not sessions or sessions[-1] != cutoff:
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only date cutoff")
    return value


def next_tqqq_core_only_xnys_session_after(value: object) -> date:
    """Return the first scheduled XNYS session after a verified P2 v5 cutoff.

    This is a calendar-only helper.  It neither fetches data nor grants an
    execution lane; the forward-observation producer uses it solely to label
    when a decision derived from a completed session becomes effective.
    """
    try:
        completed_session = date.fromisoformat(value) if isinstance(value, str) else None
    except ValueError as exc:
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only date cutoff") from exc
    if completed_session is None:
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only date cutoff")
    known_sessions = _expected_xnys_sessions(completed_session.isoformat())
    if not known_sessions or known_sessions[-1] != completed_session:
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only date cutoff")
    candidate = completed_session + timedelta(days=1)
    while candidate.weekday() >= 5 or candidate in _xnys_holidays(candidate.year):
        candidate += timedelta(days=1)
    return candidate


def expected_tqqq_core_only_sessions_for_contract(
    contract: TqqqCoreOnlyCandidateContract,
    *,
    date_cutoff: str | None = None,
) -> dict[str, tuple[date, ...]]:
    """Return the exact expected session sequence for one frozen TQQQ input contract.

    This is a pure calendar helper shared by acquisition validation and the
    non-network input-health contract.  It does not acquire, retain, or publish
    any market data.
    """
    frozen_contract = _require_contract(contract)
    binding = build_tqqq_core_only_p1_binding_for_contract(
        frozen_contract, date_cutoff=date_cutoff
    )
    identity = binding["data_identity"]
    assert isinstance(identity, Mapping)
    expected = _expected_xnys_sessions(str(identity["date_cutoff"]))
    return {
        symbol: tuple(
            session
            for session in expected
            if session >= _FIRST_ELIGIBLE_SESSION.get(symbol, _P2_EARLIEST_TRAIN_SESSION)
        )
        for symbol in _UNIVERSE
    }


def _response_sessions(value: object) -> tuple[date, ...]:
    if not isinstance(value, Mapping) or not isinstance(value.get("bars"), list):
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only historical coverage")
    sessions: list[date] = []
    try:
        for bar in value["bars"]:
            if not isinstance(bar, Mapping):
                raise TypeError
            sessions.append(date.fromisoformat(bar["date"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only historical coverage") from exc
    return tuple(sessions)


def _validate_frozen_historical_coverage(
    symbols: Mapping[str, object], binding: Mapping[str, object]
) -> None:
    if set(symbols) != set(_UNIVERSE):
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only historical coverage")
    candidate = binding.get("candidate")
    if not isinstance(candidate, Mapping):
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only historical coverage")
    try:
        contract = resolve_tqqq_core_only_candidate_contract(candidate.get("candidate_id"))
    except TqqqCoreOnlyP1BindingError:
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only historical coverage") from None
    sessions = {symbol: _response_sessions(symbols[symbol]) for symbol in _UNIVERSE}
    identity = binding.get("data_identity")
    if not isinstance(identity, Mapping):
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only historical coverage")
    expected_by_symbol = expected_tqqq_core_only_sessions_for_contract(
        contract,
        date_cutoff=identity.get("date_cutoff")
        if contract in {P2_V5_CONTRACT, P2_V7_CONTRACT, P2_V8_CONTRACT}
        else None,
    )
    for symbol in _UNIVERSE:
        if sessions[symbol] != expected_by_symbol[symbol]:
            raise TqqqCoreOnlyP1BindingError("incomplete TQQQ core-only historical coverage")


def _collect_frozen_four_inputs(
    provider: TqqqCoreOnlyHistoricalBarsProvider,
    binding: Mapping[str, object],
) -> tuple[dict[str, object], dict[str, str]]:
    identity = binding["data_identity"]
    assert isinstance(identity, dict)
    calendar = identity["calendar"]
    adjustment = identity["adjustment"]
    assert isinstance(calendar, dict) and isinstance(adjustment, dict)
    symbols: dict[str, object] = {}
    source_content_sha256: dict[str, str] = {}
    for symbol in _UNIVERSE:
        try:
            response = provider.fetch_historical_bars(
                symbol=symbol,
                calendar_id=str(calendar["calendar_id"]),
                timezone=str(calendar["timezone"]),
                adjustment_policy=str(adjustment["policy"]),
                feed=str(identity["feed"]),
                date_cutoff=str(identity["date_cutoff"]),
            )
        except TqqqCoreOnlyP1InputUnavailableError:
            raise
        except Exception:
            raise TqqqCoreOnlyP1BindingError("data-only acquisition failed") from None
        if not isinstance(response, Mapping):
            raise TqqqCoreOnlyP1BindingError("data-only acquisition failed")
        normalized = dict(response)
        try:
            source_content_sha256[symbol] = hashlib.sha256(_canonical(normalized)).hexdigest()
        except (TypeError, ValueError):
            raise TqqqCoreOnlyP1BindingError("data-only acquisition failed") from None
        symbols[symbol] = normalized
    return symbols, source_content_sha256


def publish_tqqq_core_only_p1_inputs(
    provider: TqqqCoreOnlyHistoricalBarsProvider,
    *,
    output_root: str | Path,
    observed_at: str,
    producer: Mapping[str, object],
) -> dict[str, object]:
    """Acquire exactly the frozen four inputs and atomically publish one private input root."""
    return publish_tqqq_core_only_p1_inputs_for_contract(
        provider,
        output_root=output_root,
        observed_at=observed_at,
        producer=producer,
        contract=_P2_V1_CONTRACT,
    )


def publish_tqqq_core_only_p1_inputs_for_contract(
    provider: TqqqCoreOnlyHistoricalBarsProvider,
    *,
    output_root: str | Path,
    observed_at: str,
    producer: Mapping[str, object],
    contract: TqqqCoreOnlyCandidateContract,
    date_cutoff: str | None = None,
) -> dict[str, object]:
    """Publish one candidate-bound immutable input root without provider fallback.

    The caller may supply a cutoff only for P2 v5.  That cutoff is validated
    and written into the binding before any provider request, so all four
    symbol responses, the manifest, and later P3 evidence share one identity.
    """
    destination = _require_new_private_output_root(output_root)
    frozen_contract = _require_contract(contract)
    binding = build_tqqq_core_only_p1_binding_for_contract(
        frozen_contract, date_cutoff=date_cutoff
    )
    symbols, source_content_sha256 = _collect_frozen_four_inputs(provider, binding)
    _validate_frozen_historical_coverage(symbols, binding)
    try:
        bars_bytes = _canonical(
            {
                "schema_version": "tqqq_core_only_private_bars.v1",
                "symbols": symbols,
            }
        )
        manifest = build_tqqq_core_only_input_manifest(
            binding,
            observed_at=observed_at,
            producer=producer,
            member_bytes=bars_bytes,
            source_content_sha256=source_content_sha256,
            contract=frozen_contract,
        )
        manifest_bytes = canonical_research_input_manifest_bytes(manifest)
    except (TypeError, ValueError):
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only input") from None
    temporary = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
    try:
        temporary.chmod(0o700)
        (temporary / "binding.json").write_bytes(
            canonical_tqqq_core_only_p1_binding_bytes_for_contract(
                binding, frozen_contract
            )
        )
        (temporary / "bars.json").write_bytes(bars_bytes)
        (temporary / "manifest.json").write_bytes(manifest_bytes)
        manifest_sha256 = verify_tqqq_core_only_input_root(
            temporary, contract=frozen_contract
        )
        _publish_noreplace(temporary, destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return {"manifest_sha256": manifest_sha256, "status": "P1_DATA_ONLY_INPUTS_PUBLISHED"}


def verify_tqqq_core_only_input_root(
    output_root: str | Path,
    *,
    contract: TqqqCoreOnlyCandidateContract = _P2_V1_CONTRACT,
) -> str:
    """Verify the complete QPK-compatible immutable root without provider access."""
    frozen_contract = _require_contract(contract)
    root = Path(output_root)
    try:
        root_stat = root.lstat()
        if stat.S_ISLNK(root_stat.st_mode) or not stat.S_ISDIR(root_stat.st_mode):
            raise ValueError
        if stat.S_IMODE(root_stat.st_mode) != 0o700:
            raise ValueError
        if {entry.name for entry in root.iterdir()} != _OUTPUT_FILENAMES:
            raise ValueError
        binding_bytes = (root / "binding.json").read_bytes()
        bars_bytes = (root / "bars.json").read_bytes()
        manifest_bytes = (root / "manifest.json").read_bytes()
        binding = json.loads(binding_bytes)
        if binding_bytes != canonical_tqqq_core_only_p1_binding_bytes_for_contract(
            binding, frozen_contract
        ):
            raise ValueError
        manifest = json.loads(manifest_bytes)
        if manifest_bytes != canonical_research_input_manifest_bytes(manifest):
            raise ValueError
        manifest_sha256 = validate_tqqq_core_only_input_manifest(
            manifest, binding, contract=frozen_contract
        )
        payload = json.loads(bars_bytes)
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version") != "tqqq_core_only_private_bars.v1"
            or not isinstance(payload.get("symbols"), dict)
            or set(payload["symbols"]) != set(_UNIVERSE)
        ):
            raise ValueError
        _validate_frozen_historical_coverage(payload["symbols"], binding)
        members = manifest["members"]
        if (
            len(members) != 1
            or members[0]["path"] != "bars.json"
            or members[0]["sha256"] != hashlib.sha256(bars_bytes).hexdigest()
            or members[0]["size_bytes"] != len(bars_bytes)
        ):
            raise ValueError
        source_hashes = {source["source_id"]: source["content_sha256"] for source in manifest["sources"]}
        if frozen_contract != P2_V8_CONTRACT and source_hashes != {
            f"alpaca_sip_1day_adjustment_all:{symbol}": hashlib.sha256(_canonical(payload["symbols"][symbol])).hexdigest()
            for symbol in _UNIVERSE
        }:
            raise ValueError
        return manifest_sha256
    except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError):
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only input root") from None


def build_tqqq_core_only_p1_remote_completion(output_root: str | Path) -> dict[str, object]:
    """Bind the three verified P1 files before publishing a remote completion marker."""
    root = Path(output_root)
    manifest_sha256 = verify_tqqq_core_only_input_root(root)
    try:
        return {
            "schema_version": _REMOTE_COMPLETION_SCHEMA,
            "manifest_sha256": manifest_sha256,
            "members": {
                filename: hashlib.sha256((root / filename).read_bytes()).hexdigest()
                for filename in sorted(_OUTPUT_FILENAMES)
            },
        }
    except OSError:
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only input root") from None


def canonical_tqqq_core_only_p1_remote_completion_bytes(value: Mapping[str, object]) -> bytes:
    """Validate and canonically encode a remote P1 completion marker."""
    if (
        not isinstance(value, Mapping)
        or set(value) != {"schema_version", "manifest_sha256", "members"}
        or value.get("schema_version") != _REMOTE_COMPLETION_SCHEMA
        or not isinstance(value.get("manifest_sha256"), str)
        or not _DIGEST.fullmatch(value["manifest_sha256"])
        or not isinstance(value.get("members"), Mapping)
        or set(value["members"]) != _OUTPUT_FILENAMES
        or any(
            not isinstance(digest, str) or not _DIGEST.fullmatch(digest)
            for digest in value["members"].values()
        )
    ):
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only completion marker")
    return _canonical(
        {
            "schema_version": _REMOTE_COMPLETION_SCHEMA,
            "manifest_sha256": value["manifest_sha256"],
            "members": {filename: value["members"][filename] for filename in sorted(_OUTPUT_FILENAMES)},
        }
    )


def verify_tqqq_core_only_p1_remote_completion(
    output_root: str | Path,
    completion_path: str | Path,
) -> str:
    """Accept a remote P1 root only when its create-only completion marker matches it."""
    expected = build_tqqq_core_only_p1_remote_completion(output_root)
    try:
        marker_bytes = Path(completion_path).read_bytes()
        marker = json.loads(marker_bytes)
        if marker_bytes != canonical_tqqq_core_only_p1_remote_completion_bytes(marker) or marker != expected:
            raise ValueError
        return str(expected["manifest_sha256"])
    except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError):
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only completion marker") from None
