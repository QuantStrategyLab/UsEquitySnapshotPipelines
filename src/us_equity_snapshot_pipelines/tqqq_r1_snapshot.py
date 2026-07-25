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
from pathlib import Path
from typing import Any

import pandas as pd


LEGACY_CONTRACT_VERSION = "tqqq_r1_qqq_tqqq_immutable_snapshot.v2"
CONTRACT_VERSION = "tqqq_r1_qqq_tqqq_immutable_snapshot.v3"
SYMBOLS = ("QQQ", "TQQQ")
REQUESTED_LOWER_BOUND = "2010-01-01"
PRICE_FIELD = "adjusted_close"
PLUGIN = "ABSENT_DISABLED"
MODE = "core_only"
OUTPUT_FILENAMES = ("prices.csv", "manifest.json", "validation.json", "sha256sums.json")
SOURCE_IDENTITY_KEYS = {
    "route",
    "provider",
    "retrieval_library",
    "source_version",
    "symbols",
    "price_field",
    "adjustment",
    "calendar_sha256",
    "calendar_sessions",
    "timezone",
    "coverage_start",
    "coverage_end",
    "as_of",
    "payload_schema",
    "sort_order",
    "missing_data_policy",
}


class SnapshotValidationError(ValueError):
    """Raised when a snapshot cannot satisfy the immutable local contract."""


@dataclass(frozen=True)
class SnapshotResult:
    output_dir: Path
    manifest_sha256: str
    snapshot_id: str | None = None


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


def _is_sha256(value: object) -> bool:
    return type(value) is str and len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _validate_source_identity(source_identity: object, prices: pd.DataFrame) -> dict[str, object]:
    if type(source_identity) is not dict or set(source_identity) != SOURCE_IDENTITY_KEYS:
        _invalid("invalid R1 source identity")
    string_fields = {
        "route",
        "provider",
        "retrieval_library",
        "source_version",
        "adjustment",
        "timezone",
        "payload_schema",
        "sort_order",
        "missing_data_policy",
    }
    if any(type(source_identity[field]) is not str or not source_identity[field] for field in string_fields):
        _invalid("invalid R1 source identity")
    if source_identity["route"] != "R1_STATIC_RESEARCH":
        _invalid("invalid R1 route")
    if source_identity["symbols"] != list(SYMBOLS) or source_identity["price_field"] != PRICE_FIELD:
        _invalid("invalid R1 source identity")
    if source_identity["adjustment"] != "split-and-dividend-adjusted":
        _invalid("invalid adjusted-price semantics")
    if source_identity["timezone"] != "America/New_York":
        _invalid("invalid R1 timezone")
    if source_identity["payload_schema"] != "prices.csv.v1" or source_identity["sort_order"] != "session,symbol":
        _invalid("invalid R1 payload identity")
    if source_identity["missing_data_policy"] != "reject-missing-or-duplicate":
        _invalid("invalid R1 missing-data policy")
    if not _is_sha256(source_identity["calendar_sha256"]):
        _invalid("invalid R1 calendar digest")
    calendar_sessions = source_identity["calendar_sessions"]
    if (
        type(calendar_sessions) is not list
        or not calendar_sessions
        or any(not _is_canonical_session(session) for session in calendar_sessions)
        or calendar_sessions != sorted(set(calendar_sessions))
    ):
        _invalid("invalid referenced calendar sessions")
    calendar_bytes = (json.dumps(calendar_sessions, separators=(",", ":")) + "\n").encode()
    if hashlib.sha256(calendar_bytes).hexdigest() != source_identity["calendar_sha256"]:
        _invalid("referenced calendar digest mismatch")
    coverage = ("coverage_start", "coverage_end", "as_of")
    if any(not _is_canonical_session(source_identity[field]) for field in coverage):
        _invalid("invalid R1 coverage identity")
    first_session = prices["session"].iloc[0].strftime("%Y-%m-%d")
    last_session = prices["session"].iloc[-1].strftime("%Y-%m-%d")
    if (
        source_identity["coverage_start"] != first_session
        or source_identity["coverage_end"] != last_session
        or source_identity["as_of"] != last_session
    ):
        _invalid("R1 coverage identity does not match prices")
    observed_sessions = [session.strftime("%Y-%m-%d") for session in prices["session"].drop_duplicates()]
    if observed_sessions != calendar_sessions:
        _invalid("observed sessions do not match referenced calendar sessions")
    return source_identity


def _normalized_prices(
    prices: pd.DataFrame,
    *,
    require_exact_columns: bool = False,
    require_canonical_symbols: bool = False,
    require_canonical_sessions: bool = False,
    require_canonical_order: bool = False,
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
    if require_canonical_order:
        observed_order = list(zip(normalized["session"], normalized["symbol"]))
        if observed_order != sorted(observed_order):
            _invalid("prices.csv violates declared sort order")

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
    allow_legacy: bool = False,
) -> SnapshotResult:
    if not _is_sha256(expected_manifest_sha256):
        _invalid("invalid external manifest receipt")
    if type(allow_legacy) is not bool:
        _invalid("invalid legacy opt-in")
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
    if member_hashes["manifest.json"] != expected_manifest_sha256:
        _invalid("trusted manifest hash mismatch")

    sums = _parse_json_object(members["sha256sums.json"], "sha256sums")
    manifest = _parse_json_object(members["manifest.json"], "manifest")
    validation = _parse_json_object(members["validation.json"], "validation")
    if set(sums) != {"prices.csv", "manifest.json", "validation.json"}:
        _invalid("invalid sha256sums")
    for name, expected in sums.items():
        if type(expected) is not str or member_hashes[name] != expected:
            _invalid(f"hash mismatch: {name}")

    contract_version = manifest.get("contract_version")
    if type(contract_version) is not str or contract_version not in {LEGACY_CONTRACT_VERSION, CONTRACT_VERSION}:
        _invalid("unsupported contract version")
    try:
        prices = _normalized_prices(
            pd.read_csv(BytesIO(members["prices.csv"])),
            require_exact_columns=True,
            require_canonical_symbols=True,
            require_canonical_sessions=True,
            require_canonical_order=contract_version == CONTRACT_VERSION,
        )
    except (UnicodeDecodeError, ValueError, pd.errors.EmptyDataError, pd.errors.ParserError) as exc:
        if isinstance(exc, SnapshotValidationError):
            raise
        raise SnapshotValidationError("invalid prices.csv") from exc

    expected_manifest = {
        "contract_version": contract_version,
        "symbols": list(SYMBOLS),
        "requested_lower_bound": REQUESTED_LOWER_BOUND,
        "price_field": PRICE_FIELD,
        "plugin": PLUGIN,
        "mode": MODE,
        "size": 0,
        "row_count": len(prices),
        "prices_sha256": member_hashes["prices.csv"],
    }
    if contract_version == LEGACY_CONTRACT_VERSION:
        if not allow_legacy:
            _invalid("legacy snapshot requires explicit opt-in")
        expected_validation = {"valid": True, "row_count": len(prices), "symbols": list(SYMBOLS)}
        if not _has_exact_type(manifest, expected_manifest) or not _has_exact_type(validation, expected_validation):
            _invalid("invalid legacy snapshot")
        return SnapshotResult(output_dir=output, manifest_sha256=expected_manifest_sha256)

    source_identity = _validate_source_identity(manifest.get("source_identity"), prices)
    expected_manifest["source_identity"] = source_identity
    if not _has_exact_type(manifest, expected_manifest):
        _invalid("invalid manifest")
    snapshot_id = f"sha256-{expected_manifest_sha256}"
    expected_validation = {
        "valid": True,
        "row_count": len(prices),
        "symbols": list(SYMBOLS),
        "snapshot_id": snapshot_id,
    }
    if validation.get("snapshot_id") != snapshot_id:
        _invalid("snapshot id does not bind external manifest receipt")
    if not _has_exact_type(validation, expected_validation):
        _invalid("invalid validation")
    return SnapshotResult(output_dir=output, manifest_sha256=expected_manifest_sha256, snapshot_id=snapshot_id)


def materialize_tqqq_r1_snapshot(
    prices: pd.DataFrame,
    output_dir: str | Path,
    *,
    expected_manifest_sha256: str | None = None,
    source_identity: dict[str, object] | None = None,
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
    receipt_bound = expected_manifest_sha256 is not None or source_identity is not None
    if receipt_bound and (not _is_sha256(expected_manifest_sha256) or source_identity is None):
        _invalid("receipt-bound snapshots require external manifest receipt and source identity")
    normalized = _normalized_prices(prices)
    validated_source_identity = _validate_source_identity(source_identity, normalized) if receipt_bound else None
    destination = Path(output_dir)
    if destination.exists() or destination.is_symlink():
        _invalid(f"immutable output already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
    try:
        prices_path = temporary / "prices.csv"
        _write_prices(prices_path, normalized)
        validation = {"valid": True, "row_count": len(normalized), "symbols": list(SYMBOLS)}
        if receipt_bound:
            validation["snapshot_id"] = f"sha256-{expected_manifest_sha256}"
        _write_json(temporary / "validation.json", validation)
        manifest = {
            "contract_version": CONTRACT_VERSION if receipt_bound else LEGACY_CONTRACT_VERSION,
            "symbols": list(SYMBOLS),
            "requested_lower_bound": REQUESTED_LOWER_BOUND,
            "price_field": PRICE_FIELD,
            "plugin": PLUGIN,
            "mode": MODE,
            "size": 0,
            "row_count": len(normalized),
            "prices_sha256": _sha256(prices_path),
        }
        if receipt_bound:
            manifest["source_identity"] = validated_source_identity
        _write_json(temporary / "manifest.json", manifest)
        _write_json(
            temporary / "sha256sums.json",
            {name: _sha256(temporary / name) for name in ("prices.csv", "manifest.json", "validation.json")},
        )
        manifest_sha256 = _sha256(temporary / "manifest.json")
        verify_tqqq_r1_snapshot(
            temporary,
            expected_manifest_sha256=expected_manifest_sha256 if receipt_bound else manifest_sha256,
            allow_legacy=not receipt_bound,
        )
        os.replace(temporary, destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return SnapshotResult(
        output_dir=destination,
        manifest_sha256=expected_manifest_sha256 if receipt_bound else manifest_sha256,
        snapshot_id=f"sha256-{expected_manifest_sha256}" if receipt_bound else None,
    )
