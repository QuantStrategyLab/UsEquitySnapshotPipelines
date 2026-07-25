"""Pure local materializer for the TQQQ R1 QQQ/TQQQ immutable price snapshot."""

from __future__ import annotations

import hashlib
from io import BytesIO
import json
import math
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


CONTRACT_VERSION = "tqqq_r1_qqq_tqqq_immutable_snapshot.v2"
SYMBOLS = ("QQQ", "TQQQ")
REQUESTED_LOWER_BOUND = "2010-01-01"
PRICE_FIELD = "adjusted_close"
PLUGIN = "ABSENT_DISABLED"
MODE = "core_only"
OUTPUT_FILENAMES = ("prices.csv", "manifest.json", "validation.json", "sha256sums.json")


class SnapshotValidationError(ValueError):
    """Raised when a snapshot cannot satisfy the immutable local contract."""

    recommendation = None
    size_zero_required = True
    side_effects = {"provider": 0, "replay": 0, "order": 0}


@dataclass(frozen=True)
class SnapshotResult:
    output_dir: Path
    manifest_sha256: str


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _invalid(message: str) -> None:
    raise SnapshotValidationError(message)


def _normalize_session(value: object) -> pd.Timestamp:
    if pd.api.types.is_bool(value):
        _invalid("invalid session")
    try:
        session = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise SnapshotValidationError("invalid session") from exc
    if pd.isna(session):
        _invalid("invalid session")
    if session.tz is not None:
        _invalid("timezone-aware session is not allowed")
    return session.normalize()


def _is_canonical_session(value: object) -> bool:
    return (
        type(value) is str
        and len(value) == 10
        and value[4] == "-"
        and value[7] == "-"
        and value[:4].isdigit()
        and value[5:7].isdigit()
        and value[8:].isdigit()
    )


def _normalized_prices(
    prices: pd.DataFrame,
    *,
    require_exact_columns: bool = False,
    require_canonical_symbols: bool = False,
    require_canonical_sessions: bool = False,
) -> pd.DataFrame:
    if not isinstance(prices, pd.DataFrame):
        _invalid("prices must be a DataFrame")
    required = ("session", "symbol", PRICE_FIELD)
    if any(list(prices.columns).count(column) != 1 for column in required):
        _invalid("required columns must appear exactly once")
    missing = set(required).difference(prices.columns)
    if missing:
        _invalid(f"missing required columns: {', '.join(sorted(missing))}")
    if require_exact_columns and list(prices.columns) != list(required):
        _invalid("prices.csv must contain exact columns")

    normalized = prices.loc[:, list(required)].copy()
    if require_canonical_symbols:
        if not normalized["symbol"].map(lambda value: type(value) is str and value in SYMBOLS).all():
            _invalid("prices.csv contains noncanonical symbol")
    else:
        normalized["symbol"] = normalized["symbol"].astype(str).str.strip()
    received = set(normalized["symbol"])
    if received != set(SYMBOLS):
        missing_symbols = sorted(set(SYMBOLS).difference(received))
        unexpected_symbols = sorted(received.difference(SYMBOLS))
        _invalid(f"missing required symbol or unexpected symbol: missing={missing_symbols}, unexpected={unexpected_symbols}")

    raw_sessions = normalized["session"]
    normalized["session"] = raw_sessions.map(_normalize_session)
    if require_canonical_sessions and not raw_sessions.map(_is_canonical_session).all():
        _invalid("prices.csv contains noncanonical session")
    if (normalized["session"] < pd.Timestamp(REQUESTED_LOWER_BOUND)).any():
        _invalid("session precedes requested lower bound")
    if (normalized["session"].dt.dayofweek >= 5).any():
        _invalid("observed session must be a weekday")
    if normalized.duplicated(["session", "symbol"]).any():
        _invalid("duplicate session for symbol")
    if not normalized.groupby("session")["symbol"].agg(set).eq(set(SYMBOLS)).all():
        _invalid("each session must contain exactly QQQ and TQQQ")

    adjusted_close = normalized[PRICE_FIELD]
    if pd.api.types.is_complex_dtype(adjusted_close) or adjusted_close.map(pd.api.types.is_complex).any():
        _invalid("complex adjusted_close is not allowed")
    if pd.api.types.is_bool_dtype(adjusted_close) or adjusted_close.map(pd.api.types.is_bool).any():
        _invalid("boolean adjusted_close is not allowed")
    if pd.api.types.is_datetime64_any_dtype(adjusted_close) or pd.api.types.is_timedelta64_dtype(adjusted_close):
        _invalid("datetime-like adjusted_close is not allowed")
    normalized[PRICE_FIELD] = pd.to_numeric(adjusted_close, errors="coerce")
    if (
        normalized[PRICE_FIELD].isna().any()
        or not normalized[PRICE_FIELD].map(math.isfinite).all()
        or (normalized[PRICE_FIELD] <= 0).any()
    ):
        _invalid("adjusted_close must be positive finite")
    return normalized.sort_values(["session", "symbol"], kind="stable").reset_index(drop=True)


def _write_prices(path: Path, prices: pd.DataFrame) -> None:
    output = prices.copy()
    output["session"] = output["session"].dt.strftime("%Y-%m-%d")
    output.to_csv(path, index=False, lineterminator="\n", float_format="%.17g")


def _parse_json_object(raw: bytes, name: str) -> dict[str, Any]:
    def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=no_duplicates,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite")),
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise SnapshotValidationError(f"invalid {name}") from exc
    if type(value) is not dict:
        _invalid(f"invalid {name}")
    return value


def _has_exact_type(value: object, expected: object) -> bool:
    if type(value) is not type(expected):
        return False
    if type(expected) is dict:
        return set(value) == set(expected) and all(_has_exact_type(value[key], expected[key]) for key in expected)
    if type(expected) is list:
        return len(value) == len(expected) and all(_has_exact_type(actual, wanted) for actual, wanted in zip(value, expected))
    return value == expected


def verify_tqqq_r1_snapshot(
    output_dir: str | Path,
    *,
    expected_manifest_sha256: str,
) -> SnapshotResult:
    output = Path(output_dir)
    if output.is_symlink():
        _invalid("snapshot root symlink is not allowed")
    try:
        names = tuple(sorted(path.name for path in output.iterdir())) if output.is_dir() else ()
    except OSError as exc:
        raise SnapshotValidationError("unable to read snapshot members") from exc
    if names != tuple(sorted(OUTPUT_FILENAMES)):
        _invalid(f"unexpected output files: {names}")
    if any(not (output / name).is_file() or (output / name).is_symlink() for name in OUTPUT_FILENAMES):
        _invalid("snapshot members must be regular non-symlink files")
    try:
        members = {name: (output / name).read_bytes() for name in OUTPUT_FILENAMES}
    except OSError as exc:
        raise SnapshotValidationError("unable to read snapshot members") from exc

    member_hashes = {name: hashlib.sha256(content).hexdigest() for name, content in members.items()}
    if type(expected_manifest_sha256) is not str or member_hashes["manifest.json"] != expected_manifest_sha256:
        _invalid("trusted manifest hash mismatch")

    sums = _parse_json_object(members["sha256sums.json"], "sha256sums")
    manifest = _parse_json_object(members["manifest.json"], "manifest")
    validation = _parse_json_object(members["validation.json"], "validation")
    if set(sums) != {"prices.csv", "manifest.json", "validation.json"}:
        _invalid("invalid sha256sums")
    for name, expected in sums.items():
        if type(expected) is not str or member_hashes[name] != expected:
            _invalid(f"hash mismatch: {name}")

    try:
        prices = _normalized_prices(
            pd.read_csv(BytesIO(members["prices.csv"])),
            require_exact_columns=True,
            require_canonical_symbols=True,
            require_canonical_sessions=True,
        )
    except (UnicodeDecodeError, ValueError, pd.errors.EmptyDataError, pd.errors.ParserError) as exc:
        if isinstance(exc, SnapshotValidationError):
            raise
        raise SnapshotValidationError("invalid prices.csv") from exc

    expected_manifest = {
        "contract_version": CONTRACT_VERSION,
        "symbols": list(SYMBOLS),
        "requested_lower_bound": REQUESTED_LOWER_BOUND,
        "price_field": PRICE_FIELD,
        "plugin": PLUGIN,
        "mode": MODE,
        "size": 0,
        "row_count": len(prices),
        "prices_sha256": member_hashes["prices.csv"],
    }
    if not _has_exact_type(manifest, expected_manifest):
        _invalid("invalid manifest")
    expected_validation = {"valid": True, "row_count": len(prices), "symbols": list(SYMBOLS)}
    if not _has_exact_type(validation, expected_validation):
        _invalid("invalid validation")
    return SnapshotResult(output_dir=output, manifest_sha256=member_hashes["manifest.json"])


def materialize_tqqq_r1_snapshot(
    prices: pd.DataFrame,
    output_dir: str | Path,
    *,
    mode: str = MODE,
    plugin: str = PLUGIN,
    size: int = 0,
) -> SnapshotResult:
    """Validate fixture/local input and atomically write the four immutable contract files."""
    if mode != MODE:
        _invalid("mode must be core_only")
    if plugin != PLUGIN:
        _invalid("plugin must be ABSENT_DISABLED")
    if size != 0:
        _invalid("size must be zero")
    normalized = _normalized_prices(prices)
    destination = Path(output_dir)
    if destination.exists() or destination.is_symlink():
        _invalid(f"immutable output already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
    try:
        prices_path = temporary / "prices.csv"
        _write_prices(prices_path, normalized)
        validation = {"valid": True, "row_count": len(normalized), "symbols": list(SYMBOLS)}
        _write_json(temporary / "validation.json", validation)
        manifest = {
            "contract_version": CONTRACT_VERSION,
            "symbols": list(SYMBOLS),
            "requested_lower_bound": REQUESTED_LOWER_BOUND,
            "price_field": PRICE_FIELD,
            "plugin": PLUGIN,
            "mode": MODE,
            "size": 0,
            "row_count": len(normalized),
            "prices_sha256": _sha256(prices_path),
        }
        _write_json(temporary / "manifest.json", manifest)
        _write_json(
            temporary / "sha256sums.json",
            {name: _sha256(temporary / name) for name in ("prices.csv", "manifest.json", "validation.json")},
        )
        manifest_sha256 = _sha256(temporary / "manifest.json")
        verify_tqqq_r1_snapshot(temporary, expected_manifest_sha256=manifest_sha256)
        os.replace(temporary, destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return SnapshotResult(output_dir=destination, manifest_sha256=manifest_sha256)


_TRUSTED_OUTPUT_FILENAMES = ("prices.csv", "sha256sums.json", "trust.json")
_CALENDAR_SCHEMA = "qsl.r1.xnys.session.v1"
_ENDPOINT_SCHEMA = "qsl.tqqq.fixture-calendar-endpoint-packet.v1"
_RUNTIME_SCHEMA = "qsl.tqqq.fixture-runtime-source-identity.v1"
_TRUST_SCHEMA = "qsl.tqqq.fixture-trusted-snapshot.v1"
_QQQ_FIRST_SESSION = "2010-01-04"
_TQQQ_FIRST_USABLE_SESSION = "2010-02-11"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _is_sha256(value: object) -> bool:
    return type(value) is str and len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _read_trusted_bytes(path: object) -> bytes:
    if not isinstance(path, (str, Path)):
        _invalid("CALENDAR_ENDPOINT_AUTHORITY_MISSING")
    target = Path(path)
    if target.is_symlink():
        _invalid("CALENDAR_ENDPOINT_AUTHORITY_MISSING")
    try:
        return target.read_bytes()
    except OSError as exc:
        raise SnapshotValidationError("CALENDAR_ENDPOINT_AUTHORITY_MISSING") from exc


def _parse_utc(value: object, status: str) -> datetime:
    if type(value) is not str:
        _invalid(status)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SnapshotValidationError(status) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _invalid(status)
    return parsed.astimezone(timezone.utc)


def _trusted_calendar(raw: bytes) -> list[dict[str, object]]:
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise SnapshotValidationError("CALENDAR_SCHEMA_INVALID") from exc
    if not lines:
        _invalid("CALENDAR_SCHEMA_INVALID")
    sessions: list[dict[str, object]] = []
    previous = ""
    for line in lines:
        entry = _parse_json_object(line.encode("utf-8"), "calendar")
        if set(entry) != {"schema", "session", "open_utc", "close_utc"} or entry["schema"] != _CALENDAR_SCHEMA:
            _invalid("CALENDAR_SCHEMA_INVALID")
        session = entry["session"]
        if type(session) is not str or len(session) != 10:
            _invalid("CALENDAR_SCHEMA_INVALID")
        try:
            parsed_session = pd.Timestamp(session)
        except ValueError as exc:
            raise SnapshotValidationError("CALENDAR_SCHEMA_INVALID") from exc
        if parsed_session.strftime("%Y-%m-%d") != session or session <= previous:
            _invalid("CALENDAR_SCHEMA_INVALID")
        opened = _parse_utc(entry["open_utc"], "CALENDAR_SCHEMA_INVALID")
        closed = _parse_utc(entry["close_utc"], "CALENDAR_SCHEMA_INVALID")
        if opened >= closed or closed.date().isoformat() != session:
            _invalid("CALENDAR_SCHEMA_INVALID")
        sessions.append(entry)
        previous = session
    return sessions


def _trusted_prices(prices: pd.DataFrame, expected_sessions: dict[str, list[str]]) -> pd.DataFrame:
    required = ("session", "symbol", PRICE_FIELD)
    if not isinstance(prices, pd.DataFrame) or any(list(prices.columns).count(column) != 1 for column in required):
        _invalid("EXACT_SESSION_SET_MISMATCH")
    if list(prices.columns) != list(required):
        _invalid("EXACT_SESSION_SET_MISMATCH")
    normalized = prices.copy()
    if not normalized["symbol"].map(lambda value: type(value) is str and value in SYMBOLS).all():
        _invalid("EXACT_SESSION_SET_MISMATCH")
    if not normalized["session"].map(_is_canonical_session).all():
        _invalid("EXACT_SESSION_SET_MISMATCH")
    normalized["session"] = normalized["session"].map(_normalize_session)
    if normalized.duplicated(["session", "symbol"]).any():
        _invalid("EXACT_SESSION_SET_MISMATCH")
    normalized[PRICE_FIELD] = pd.to_numeric(normalized[PRICE_FIELD], errors="coerce")
    if normalized[PRICE_FIELD].isna().any() or not normalized[PRICE_FIELD].map(math.isfinite).all():
        _invalid("EXACT_SESSION_SET_MISMATCH")
    for symbol, expected in expected_sessions.items():
        actual = normalized.loc[normalized["symbol"].eq(symbol), "session"].dt.strftime("%Y-%m-%d").tolist()
        if actual != expected:
            _invalid("EXACT_SESSION_SET_MISMATCH")
    return normalized.sort_values(["session", "symbol"], kind="stable").reset_index(drop=True)


def _trusted_readback_matches(output_dir: Path, member_hashes: dict[str, str]) -> bool:
    try:
        return all(_digest((output_dir / name).read_bytes()) == digest for name, digest in member_hashes.items())
    except OSError:
        return False


def _trusted_output_digest(members: dict[str, bytes]) -> str:
    digest = hashlib.sha256(b"qsl.tqqq.fixture-trusted-output.v1\0")
    for name in _TRUSTED_OUTPUT_FILENAMES:
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(members[name])
    return digest.hexdigest()


def _trusted_result(output_dir: Path, output_digest: str) -> SnapshotResult:
    return SnapshotResult(output_dir=output_dir, manifest_sha256=output_digest)


def verify_tqqq_calendar_endpoint_trusted_snapshot(
    output_dir: str | Path,
    *,
    expected_calendar_sha256: object,
    expected_endpoint_packet_sha256: object,
    expected_runtime_source_identity_sha256: object,
    expected_output_sha256: object,
) -> SnapshotResult:
    expected_digests = {
        "calendar_sha256": expected_calendar_sha256,
        "endpoint_packet_sha256": expected_endpoint_packet_sha256,
        "runtime_source_identity_sha256": expected_runtime_source_identity_sha256,
    }
    if not all(_is_sha256(value) for value in expected_digests.values()):
        _invalid("CALENDAR_ENDPOINT_AUTHORITY_MISSING")
    if not _is_sha256(expected_output_sha256):
        _invalid("STRICT_READBACK_FAILED")
    output = Path(output_dir)
    if output.is_symlink() or not output.is_dir():
        _invalid("STRICT_READBACK_FAILED")
    try:
        names = tuple(sorted(member.name for member in output.iterdir()))
        members = {name: (output / name).read_bytes() for name in _TRUSTED_OUTPUT_FILENAMES}
    except OSError as exc:
        raise SnapshotValidationError("STRICT_READBACK_FAILED") from exc
    if names != _TRUSTED_OUTPUT_FILENAMES or any((output / name).is_symlink() for name in _TRUSTED_OUTPUT_FILENAMES):
        _invalid("STRICT_READBACK_FAILED")
    member_hashes = {name: _digest(raw) for name, raw in members.items()}
    if _trusted_output_digest(members) != expected_output_sha256:
        _invalid("STRICT_READBACK_FAILED")
    sums = _parse_json_object(members["sha256sums.json"], "trusted sha256sums")
    trust = _parse_json_object(members["trust.json"], "trusted snapshot")
    if sums != {"prices.csv": member_hashes["prices.csv"], "trust.json": member_hashes["trust.json"]}:
        _invalid("STRICT_READBACK_FAILED")
    if set(trust) != {"schema", "digests", "expected_sessions", "prices_sha256", "row_count"}:
        _invalid("STRICT_READBACK_FAILED")
    if trust["schema"] != _TRUST_SCHEMA or trust["digests"] != expected_digests:
        _invalid("STRICT_READBACK_FAILED")
    expected_sessions = trust["expected_sessions"]
    if not isinstance(expected_sessions, dict) or set(expected_sessions) != set(SYMBOLS):
        _invalid("STRICT_READBACK_FAILED")
    if any(type(values) is not list or any(type(value) is not str for value in values) for values in expected_sessions.values()):
        _invalid("STRICT_READBACK_FAILED")
    if trust["prices_sha256"] != member_hashes["prices.csv"] or type(trust["row_count"]) is not int:
        _invalid("STRICT_READBACK_FAILED")
    try:
        prices = pd.read_csv(BytesIO(members["prices.csv"]))
    except (UnicodeDecodeError, pd.errors.EmptyDataError, pd.errors.ParserError) as exc:
        raise SnapshotValidationError("STRICT_READBACK_FAILED") from exc
    if len(prices) != trust["row_count"]:
        _invalid("STRICT_READBACK_FAILED")
    _trusted_prices(prices, expected_sessions)
    if not _trusted_readback_matches(output, member_hashes):
        _invalid("STRICT_READBACK_FAILED")
    return _trusted_result(output, expected_output_sha256)


def materialize_tqqq_calendar_endpoint_trusted_snapshot(
    prices: pd.DataFrame,
    output_dir: str | Path,
    *,
    calendar_path: object,
    endpoint_packet_path: object,
    runtime_anchor_path: object,
    expected_calendar_sha256: object,
    expected_endpoint_packet_sha256: object,
    expected_runtime_source_identity_sha256: object,
) -> SnapshotResult:
    observation_time = _utc_now()
    if observation_time.tzinfo is None or observation_time.utcoffset() is None:
        _invalid("CALENDAR_ENDPOINT_STALE_AT_OBSERVATION")
    observation_time = observation_time.astimezone(timezone.utc)
    expected = {
        "calendar_sha256": expected_calendar_sha256,
        "endpoint_packet_sha256": expected_endpoint_packet_sha256,
        "runtime_source_identity_sha256": expected_runtime_source_identity_sha256,
    }
    if not all(_is_sha256(value) for value in expected.values()):
        _invalid("CALENDAR_ENDPOINT_AUTHORITY_MISSING")
    calendar_raw = _read_trusted_bytes(calendar_path)
    endpoint_raw = _read_trusted_bytes(endpoint_packet_path)
    runtime_raw = _read_trusted_bytes(runtime_anchor_path)
    if _digest(calendar_raw) != expected_calendar_sha256:
        _invalid("CALENDAR_DIGEST_MISMATCH")
    if _digest(endpoint_raw) != expected_endpoint_packet_sha256:
        _invalid("CALENDAR_ENDPOINT_PACKET_DIGEST_MISMATCH")
    if _digest(runtime_raw) != expected_runtime_source_identity_sha256:
        _invalid("RUNTIME_SOURCE_IDENTITY_MISMATCH")
    endpoint = _parse_json_object(endpoint_raw, "endpoint packet")
    runtime = _parse_json_object(runtime_raw, "runtime source identity")
    endpoint_keys = {
        "schema",
        "venue",
        "required_first_session",
        "required_last_completed_session",
        "required_last_completed_close_utc",
        "next_session",
        "next_session_close_utc",
        "endpoint_observed_at_utc",
        "expected_session_count",
        "expected_calendar_sha256",
        "expected_runtime_source_identity_sha256",
    }
    if set(endpoint) != endpoint_keys or endpoint["schema"] != _ENDPOINT_SCHEMA or endpoint["venue"] != "XNYS":
        _invalid("CALENDAR_SCHEMA_INVALID")
    if endpoint["expected_calendar_sha256"] != expected_calendar_sha256:
        _invalid("CALENDAR_DIGEST_MISMATCH")
    if endpoint["expected_runtime_source_identity_sha256"] != expected_runtime_source_identity_sha256:
        _invalid("RUNTIME_SOURCE_IDENTITY_MISMATCH")
    if set(runtime) != {"schema", "source_sha256"} or runtime["schema"] != _RUNTIME_SCHEMA:
        _invalid("RUNTIME_SOURCE_IDENTITY_MISMATCH")
    source_hashes = runtime["source_sha256"]
    if not isinstance(source_hashes, dict) or set(source_hashes) != {"tqqq_r1_snapshot.py", "yfinance_prices.py"}:
        _invalid("RUNTIME_SOURCE_IDENTITY_MISMATCH")
    source_paths = {"tqqq_r1_snapshot.py": Path(__file__), "yfinance_prices.py": Path(__file__).with_name("yfinance_prices.py")}
    if any(not _is_sha256(source_hashes[name]) or _sha256(path) != source_hashes[name] for name, path in source_paths.items()):
        _invalid("RUNTIME_SOURCE_IDENTITY_MISMATCH")
    calendar = _trusted_calendar(calendar_raw)
    sessions = [entry["session"] for entry in calendar]
    if endpoint["required_first_session"] != _QQQ_FIRST_SESSION or sessions[0] != _QQQ_FIRST_SESSION:
        _invalid("CALENDAR_START_MISMATCH")
    if (
        type(endpoint["expected_session_count"]) is not int
        or endpoint["expected_session_count"] != len(calendar)
        or endpoint["required_last_completed_session"] != sessions[-1]
        or endpoint["required_last_completed_close_utc"] != calendar[-1]["close_utc"]
    ):
        _invalid("CALENDAR_END_MISMATCH")
    endpoint_observed = _parse_utc(endpoint["endpoint_observed_at_utc"], "CALENDAR_SCHEMA_INVALID")
    last_close = _parse_utc(endpoint["required_last_completed_close_utc"], "CALENDAR_SCHEMA_INVALID")
    next_close = _parse_utc(endpoint["next_session_close_utc"], "CALENDAR_SCHEMA_INVALID")
    next_session = endpoint["next_session"]
    if (
        not _is_canonical_session(next_session)
        or next_session <= sessions[-1]
        or next_close.date().isoformat() != next_session
        or endpoint_observed < last_close
        or endpoint_observed >= next_close
        or endpoint_observed > observation_time
        or last_close > observation_time
        or observation_time >= next_close
    ):
        _invalid("CALENDAR_ENDPOINT_STALE_AT_OBSERVATION")
    expected_sessions = {
        "QQQ": sessions,
        "TQQQ": [session for session in sessions if session >= _TQQQ_FIRST_USABLE_SESSION],
    }
    if _TQQQ_FIRST_USABLE_SESSION not in expected_sessions["TQQQ"]:
        _invalid("EXACT_SESSION_SET_MISMATCH")
    normalized = _trusted_prices(prices, expected_sessions)
    destination = Path(output_dir)
    if destination.exists() or destination.is_symlink():
        _invalid("IMMUTABLE_CREATE_FAILED")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
    try:
        _write_prices(temporary / "prices.csv", normalized)
        trust = {
            "schema": _TRUST_SCHEMA,
            "digests": expected,
            "expected_sessions": expected_sessions,
            "prices_sha256": _sha256(temporary / "prices.csv"),
            "row_count": len(normalized),
        }
        _write_json(temporary / "trust.json", trust)
        _write_json(
            temporary / "sha256sums.json",
            {name: _sha256(temporary / name) for name in ("prices.csv", "trust.json")},
        )
        members = {name: (temporary / name).read_bytes() for name in _TRUSTED_OUTPUT_FILENAMES}
        output_digest = _trusted_output_digest(members)
        result = verify_tqqq_calendar_endpoint_trusted_snapshot(
            temporary,
            expected_calendar_sha256=expected_calendar_sha256,
            expected_endpoint_packet_sha256=expected_endpoint_packet_sha256,
            expected_runtime_source_identity_sha256=expected_runtime_source_identity_sha256,
            expected_output_sha256=output_digest,
        )
        os.replace(temporary, destination)
        return SnapshotResult(output_dir=destination, manifest_sha256=result.manifest_sha256)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
