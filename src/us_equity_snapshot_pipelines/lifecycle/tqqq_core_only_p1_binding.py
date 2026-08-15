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
INPUT_CONTRACT_ID = "tqqq_core_only_alpaca_sip_adjustment_all.v1"
_INPUT_SCHEMA = "qsl.tqqq_core_only_p1_data_binding.v1"
_UNIVERSE = ("QQQ", "TQQQ", "QQQM", "BOXX")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_OUTPUT_FILENAMES = frozenset({"bars.json", "binding.json", "manifest.json"})
_P2_EARLIEST_TRAIN_SESSION = date(2018, 1, 2)
_FIRST_ELIGIBLE_SESSION = {"QQQM": date(2020, 10, 13), "BOXX": date(2022, 12, 28)}


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


def _canonical(value: Mapping[str, object]) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def build_tqqq_core_only_p1_binding() -> dict[str, object]:
    """Return the frozen data-only identity; this function performs no acquisition."""
    return {
        "schema_version": _INPUT_SCHEMA,
        "candidate": {
            "candidate_id": CANDIDATE_ID,
            "config_sha256": CANDIDATE_CONFIG_SHA256,
        },
        "source": {
            "repository": "QuantStrategyLab/UsEquityStrategies",
            "revision": UES_REVISION,
        },
        "data_identity": {
            "provider": "ALPACA_MARKET_DATA",
            "feed": "SIP",
            "calendar": {
                "calendar_id": "XNYS",
                "timezone": "America/New_York",
                "source": "exchange_calendars",
            },
            "adjustment": {
                "policy": "total_return_adjusted",
                "source": "ALPACA_MARKET_DATA adjustment=all(split,dividend,spin-off)",
            },
            "universe": list(_UNIVERSE),
            "date_cutoff": "2026-07-31",
            "cost_assumptions": {
                "turnover_cost_bps": 5.0,
                "stress_turnover_cost_bps": [10.0, 25.0],
                "borrow_cost_bps": 0.0,
                "cash_yield_assumption": 0.0,
                "execution_timing": "next_complete_trading_session_after_signal_effective_date",
            },
            "retention": {
                "policy": "PRIVATE_LOCAL_ENCRYPTED_RESEARCH_SNAPSHOT_NO_BACKUP_NO_REDISTRIBUTION",
                "redistribution_allowed": False,
            },
        },
    }


def canonical_binding_bytes(value: Mapping[str, object]) -> bytes:
    """Validate and encode one exact binding in canonical form."""
    validated = validate_tqqq_core_only_p1_binding(value)
    return _canonical(validated)


def binding_sha256(value: Mapping[str, object]) -> str:
    return hashlib.sha256(canonical_binding_bytes(value)).hexdigest()


def validate_tqqq_core_only_p1_binding(value: Mapping[str, object]) -> dict[str, object]:
    """Reject any binding other than the P2-frozen source/config/data identity."""
    expected = build_tqqq_core_only_p1_binding()
    if not isinstance(value, Mapping) or dict(value) != expected:
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only P1 binding")
    return expected


def build_tqqq_core_only_input_manifest(
    binding: Mapping[str, object],
    *,
    observed_at: str,
    producer: Mapping[str, object],
    member_bytes: bytes,
    source_content_sha256: Mapping[str, str],
) -> dict[str, object]:
    """Build the future immutable-input manifest from one already-collected member."""
    frozen = validate_tqqq_core_only_p1_binding(binding)
    if (
        not isinstance(member_bytes, bytes)
        or set(source_content_sha256) != set(_UNIVERSE)
        or any(
            not isinstance(digest, str) or not _DIGEST.fullmatch(digest)
            for digest in source_content_sha256.values()
        )
    ):
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only input member")
    identity = frozen["data_identity"]
    assert isinstance(identity, dict)
    binding_digest = binding_sha256(frozen)
    manifest = validate_research_input_manifest(
        {
            "schema_version": "research_input_manifest.v1",
            "manifest_id": f"tqqq-core-only-{binding_digest[:24]}-{hashlib.sha256(member_bytes).hexdigest()[:24]}",
            "research_input_contract_id": INPUT_CONTRACT_ID,
            "domain": "us_equity",
            "profile": CANDIDATE_ID,
            "artifact_type": "immutable_adjusted_ohlcv_etf_only",
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
                    "source_id": f"alpaca_sip_1day_adjustment_all:{symbol}",
                    "revision": binding_digest,
                    "observed_at": observed_at,
                    "content_sha256": source_content_sha256[symbol],
                }
                for symbol in sorted(_UNIVERSE)
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
    validate_tqqq_core_only_input_manifest(manifest, frozen)
    return manifest


def validate_tqqq_core_only_input_manifest(
    manifest: Mapping[str, object], binding: Mapping[str, object]
) -> str:
    """Validate a QPK immutable-input manifest against the frozen static binding."""
    frozen = validate_tqqq_core_only_p1_binding(binding)
    try:
        validated = validate_research_input_manifest(manifest)
    except ValueError as exc:
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only input manifest") from exc
    identity = frozen["data_identity"]
    assert isinstance(identity, dict)
    binding_digest = binding_sha256(frozen)
    expected_source_ids = {f"alpaca_sip_1day_adjustment_all:{symbol}" for symbol in _UNIVERSE}
    sources = validated["sources"]
    if (
        validated["research_input_contract_id"] != INPUT_CONTRACT_ID
        or validated["domain"] != "us_equity"
        or validated["profile"] != CANDIDATE_ID
        or validated["artifact_type"] != "immutable_adjusted_ohlcv_etf_only"
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
    identity = binding["data_identity"]
    assert isinstance(identity, Mapping)
    expected = _expected_xnys_sessions(str(identity["date_cutoff"]))
    if set(symbols) != set(_UNIVERSE):
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only historical coverage")
    sessions = {symbol: _response_sessions(symbols[symbol]) for symbol in _UNIVERSE}
    if sessions["QQQ"] != expected or sessions["TQQQ"] != expected:
        raise TqqqCoreOnlyP1BindingError("incomplete TQQQ core-only historical coverage")
    for symbol, first_eligible in _FIRST_ELIGIBLE_SESSION.items():
        if sessions[symbol] != tuple(session for session in expected if session >= first_eligible):
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
    destination = _require_new_private_output_root(output_root)
    binding = build_tqqq_core_only_p1_binding()
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
        )
        manifest_bytes = canonical_research_input_manifest_bytes(manifest)
    except (TypeError, ValueError):
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only input") from None
    temporary = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
    try:
        temporary.chmod(0o700)
        (temporary / "binding.json").write_bytes(canonical_binding_bytes(binding))
        (temporary / "bars.json").write_bytes(bars_bytes)
        (temporary / "manifest.json").write_bytes(manifest_bytes)
        manifest_sha256 = verify_tqqq_core_only_input_root(temporary)
        _publish_noreplace(temporary, destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return {"manifest_sha256": manifest_sha256, "status": "P1_DATA_ONLY_INPUTS_PUBLISHED"}


def verify_tqqq_core_only_input_root(output_root: str | Path) -> str:
    """Verify the complete QPK-compatible immutable root without provider access."""
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
        if binding_bytes != canonical_binding_bytes(binding):
            raise ValueError
        manifest = json.loads(manifest_bytes)
        if manifest_bytes != canonical_research_input_manifest_bytes(manifest):
            raise ValueError
        manifest_sha256 = validate_tqqq_core_only_input_manifest(manifest, binding)
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
        if source_hashes != {
            f"alpaca_sip_1day_adjustment_all:{symbol}": hashlib.sha256(_canonical(payload["symbols"][symbol])).hexdigest()
            for symbol in _UNIVERSE
        }:
            raise ValueError
        return manifest_sha256
    except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError):
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only input root") from None
