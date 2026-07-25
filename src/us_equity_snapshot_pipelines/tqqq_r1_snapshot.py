"""Pure local materializer for the TQQQ R1 QQQ/TQQQ immutable price snapshot."""

from __future__ import annotations

import argparse
import hashlib
from io import BytesIO
import json
import math
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from . import yfinance_prices
from .yfinance_prices import download_price_history


CONTRACT_VERSION = "tqqq_r1_qqq_tqqq_immutable_snapshot.v2"
RETRIEVAL_CONTRACT_VERSION = "tqqq_r1_qqq_tqqq_immutable_snapshot.v3"
SYMBOLS = ("QQQ", "TQQQ")
REQUESTED_LOWER_BOUND = "2010-01-01"
PRICE_FIELD = "adjusted_close"
PLUGIN = "ABSENT_DISABLED"
MODE = "core_only"
OUTPUT_FILENAMES = ("prices.csv", "manifest.json", "validation.json", "sha256sums.json")
RETRIEVAL_OUTPUT_FILENAMES = (*OUTPUT_FILENAMES, "retrieval_receipt.json")
IDENTITY_REPOSITORY = "QuantStrategyLab/UsEquitySnapshotPipelines"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA1_RE = re.compile(r"^[0-9a-f]{40}$")
_SOURCE_FILES = (
    "src/us_equity_snapshot_pipelines/tqqq_r1_snapshot.py",
    "src/us_equity_snapshot_pipelines/yfinance_prices.py",
)


class SnapshotValidationError(ValueError):
    """Raised when a snapshot cannot satisfy the immutable local contract."""


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


def _trusted_digest(value: object, *, name: str) -> str:
    if type(value) is not str or not _SHA256_RE.fullmatch(value):
        _invalid(f"invalid expected {name} digest")
    return value


def _read_external_artifact(path: str | Path, *, name: str) -> bytes:
    artifact = Path(path)
    try:
        metadata = artifact.stat()
        if artifact.is_symlink() or not artifact.is_file() or metadata.st_mode & 0o222:
            _invalid(f"invalid {name} artifact")
        return artifact.read_bytes()
    except OSError as exc:
        raise SnapshotValidationError(f"invalid {name} artifact") from exc


def _source_identity_reference(value: object) -> dict[str, object]:
    if type(value) is not dict or set(value) != {"artifact_sha256", "repository", "commit", "tree", "files"}:
        _invalid("invalid source identity")
    files = value["files"]
    if (
        type(value["artifact_sha256"]) is not str
        or not _SHA256_RE.fullmatch(value["artifact_sha256"])
        or value["repository"] != IDENTITY_REPOSITORY
        or type(value["commit"]) is not str
        or not _GIT_SHA1_RE.fullmatch(value["commit"])
        or type(value["tree"]) is not str
        or not _GIT_SHA1_RE.fullmatch(value["tree"])
        or type(files) is not dict
        or set(files) != set(_SOURCE_FILES)
        or any(type(digest) is not str or not _SHA256_RE.fullmatch(digest) for digest in files.values())
    ):
        _invalid("invalid source identity")
    return {"artifact_sha256": value["artifact_sha256"], "repository": value["repository"], "commit": value["commit"], "tree": value["tree"], "files": dict(files)}


def _load_external_source_identity(
    path: str | Path, *, expected_source_identity_sha256: str
) -> dict[str, object]:
    expected = _trusted_digest(expected_source_identity_sha256, name="source identity")
    raw = _read_external_artifact(path, name="source identity")
    if hashlib.sha256(raw).hexdigest() != expected:
        _invalid("source identity digest mismatch")
    identity = _parse_json_object(raw, "source identity")
    if (
        identity.get("schema") != "qsl.r1.uesp.source-identity-anchor.v1"
        or identity.get("repo") != IDENTITY_REPOSITORY
        or identity.get("ref") != "refs/heads/main"
        or type(identity.get("commit")) is not str
        or not _GIT_SHA1_RE.fullmatch(identity["commit"])
        or type(identity.get("tree")) is not str
        or not _GIT_SHA1_RE.fullmatch(identity["tree"])
        or type(identity.get("files")) is not dict
    ):
        _invalid("invalid source identity")
    files = identity["files"]
    try:
        recorded = {name: files[name]["content_sha256"] for name in _SOURCE_FILES}
    except (KeyError, TypeError):
        _invalid("invalid source identity")
    reference = _source_identity_reference(
        {
            "artifact_sha256": expected,
            "repository": IDENTITY_REPOSITORY,
            "commit": identity["commit"],
            "tree": identity["tree"],
            "files": recorded,
        }
    )
    try:
        actual = {
            _SOURCE_FILES[0]: _sha256(Path(__file__)),
            _SOURCE_FILES[1]: _sha256(Path(yfinance_prices.__file__)),
        }
    except (OSError, TypeError) as exc:
        raise SnapshotValidationError("invalid source identity") from exc
    if actual != reference["files"]:
        _invalid("source identity executed-file mismatch")
    return reference


def _calendar_reference(value: object) -> dict[str, object]:
    if type(value) is not dict or set(value) != {"artifact_sha256", "schema", "coverage"}:
        _invalid("invalid XNYS calendar")
    coverage = value["coverage"]
    if (
        type(value["artifact_sha256"]) is not str
        or not _SHA256_RE.fullmatch(value["artifact_sha256"])
        or value["schema"] != "qsl.r1.xnys.session.v1"
        or type(coverage) is not dict
        or set(coverage) != {"first_session", "last_session", "session_count"}
        or any(type(coverage[key]) is not str for key in ("first_session", "last_session"))
        or type(coverage["session_count"]) is not int
        or coverage["session_count"] <= 0
    ):
        _invalid("invalid XNYS calendar")
    try:
        first = _normalize_session(coverage["first_session"])
        last = _normalize_session(coverage["last_session"])
    except SnapshotValidationError:
        _invalid("invalid XNYS calendar")
    if first > last:
        _invalid("invalid XNYS calendar")
    return {"artifact_sha256": value["artifact_sha256"], "schema": value["schema"], "coverage": dict(coverage)}


def _load_external_xnys_calendar(path: str | Path, *, expected_calendar_sha256: str) -> tuple[dict[str, object], dict[pd.Timestamp, datetime]]:
    expected = _trusted_digest(expected_calendar_sha256, name="calendar")
    raw = _read_external_artifact(path, name="XNYS calendar")
    if hashlib.sha256(raw).hexdigest() != expected:
        _invalid("XNYS calendar digest mismatch")
    try:
        rows = [_parse_json_object(line, "XNYS calendar session") for line in raw.splitlines()]
    except SnapshotValidationError:
        raise
    if not rows:
        _invalid("invalid XNYS calendar")
    closes: dict[pd.Timestamp, datetime] = {}
    for row in rows:
        if set(row) != {"close_utc", "early_close", "open_utc", "schema", "session_date"} or row["schema"] != "qsl.r1.xnys.session.v1":
            _invalid("invalid XNYS calendar")
        if type(row["early_close"]) is not bool or type(row["session_date"]) is not str:
            _invalid("invalid XNYS calendar")
        try:
            session = _normalize_session(row["session_date"])
            close = datetime.fromisoformat(str(row["close_utc"]).replace("Z", "+00:00"))
            opened = datetime.fromisoformat(str(row["open_utc"]).replace("Z", "+00:00"))
        except (SnapshotValidationError, ValueError):
            _invalid("invalid XNYS calendar")
        if close.tzinfo != timezone.utc or opened.tzinfo != timezone.utc or opened >= close or session in closes:
            _invalid("invalid XNYS calendar")
        closes[session] = close
    sessions = list(closes)
    if sessions != sorted(sessions):
        _invalid("invalid XNYS calendar")
    coverage = {"first_session": sessions[0].strftime("%Y-%m-%d"), "last_session": sessions[-1].strftime("%Y-%m-%d"), "session_count": len(sessions)}
    return _calendar_reference({"artifact_sha256": expected, "schema": "qsl.r1.xnys.session.v1", "coverage": coverage}), closes


def _normalized_observed_prices(prices: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(prices, pd.DataFrame):
        _invalid("downloaded prices must be a DataFrame")
    price_column = PRICE_FIELD if PRICE_FIELD in prices.columns else "close"
    required = ("as_of", "symbol", price_column)
    if any(list(prices.columns).count(column) != 1 for column in required):
        _invalid("downloaded prices require exact columns")
    if price_column == "close" and list(prices.columns).count(PRICE_FIELD):
        _invalid("downloaded prices require one price column")
    observed = prices.loc[:, list(required)].rename(columns={"as_of": "session", price_column: PRICE_FIELD}).copy()
    if not observed["symbol"].map(lambda value: type(value) is str and value in SYMBOLS).all():
        _invalid("downloaded prices contain noncanonical symbol")
    observed["session"] = observed["session"].map(_normalize_session)
    if (observed["session"] < pd.Timestamp(REQUESTED_LOWER_BOUND)).any() or observed.duplicated(["session", "symbol"]).any():
        _invalid("downloaded prices contain invalid session")
    adjusted_close = observed[PRICE_FIELD]
    if pd.api.types.is_complex_dtype(adjusted_close) or adjusted_close.map(pd.api.types.is_complex).any():
        _invalid("complex adjusted_close is not allowed")
    if pd.api.types.is_bool_dtype(adjusted_close) or adjusted_close.map(pd.api.types.is_bool).any():
        _invalid("boolean adjusted_close is not allowed")
    observed[PRICE_FIELD] = pd.to_numeric(adjusted_close, errors="coerce")
    if (
        observed[PRICE_FIELD].isna().any()
        or not observed[PRICE_FIELD].map(math.isfinite).all()
        or (observed[PRICE_FIELD] <= 0).any()
    ):
        _invalid("adjusted_close must be positive finite")
    return observed.sort_values(["session", "symbol"], kind="stable").reset_index(drop=True)


def _require_xnys_sessions(prices: pd.DataFrame, closes: dict[pd.Timestamp, datetime], *, observed_at: datetime) -> None:
    if observed_at.tzinfo is None:
        _invalid("observed_at must be timezone-aware")
    observed_utc = observed_at.astimezone(timezone.utc)
    current_date = pd.Timestamp(observed_utc.date())
    for session in prices["session"].drop_duplicates():
        if session not in closes:
            _invalid("observed session outside XNYS calendar")
        if session > current_date:
            _invalid("future observed session")
        if session == current_date and observed_utc < closes[session]:
            _invalid("unclosed trading session")


def _observed_coverage(prices: pd.DataFrame) -> dict[str, list[str]]:
    return {
        symbol: prices.loc[prices["symbol"].eq(symbol), "session"].dt.strftime("%Y-%m-%d").tolist()
        for symbol in SYMBOLS
    }


def _common_prices(observed: pd.DataFrame) -> pd.DataFrame:
    common_sessions = set(observed.loc[observed["symbol"].eq(SYMBOLS[0]), "session"])
    common_sessions &= set(observed.loc[observed["symbol"].eq(SYMBOLS[1]), "session"])
    if not common_sessions:
        _invalid("no common observed sessions")
    return _normalized_prices(observed.loc[observed["session"].isin(common_sessions)].copy())


def _canonical_coverage(value: object, *, error: str) -> dict[str, list[str]]:
    if type(value) is not dict or set(value) != set(SYMBOLS):
        _invalid(error)
    coverage: dict[str, list[str]] = {}
    for symbol in SYMBOLS:
        sessions = value[symbol]
        if type(sessions) is not list or any(type(session) is not str for session in sessions):
            _invalid(error)
        try:
            normalized = [_normalize_session(session) for session in sessions]
        except SnapshotValidationError:
            _invalid(error)
        canonical = [session.strftime("%Y-%m-%d") for session in normalized]
        if len(canonical) != len(set(canonical)):
            _invalid(error)
        coverage[symbol] = sorted(canonical)
    return coverage


def verify_tqqq_r1_snapshot(
    output_dir: str | Path,
    *,
    expected_manifest_sha256: str,
    expected_source_identity_sha256: str | None = None,
    expected_calendar_sha256: str | None = None,
) -> SnapshotResult:
    output = Path(output_dir)
    if output.is_symlink():
        _invalid("snapshot root symlink is not allowed")
    try:
        names = tuple(sorted(path.name for path in output.iterdir())) if output.is_dir() else ()
    except OSError as exc:
        raise SnapshotValidationError("unable to read snapshot members") from exc
    if names not in {tuple(sorted(OUTPUT_FILENAMES)), tuple(sorted(RETRIEVAL_OUTPUT_FILENAMES))}:
        _invalid(f"unexpected output files: {names}")
    filenames = RETRIEVAL_OUTPUT_FILENAMES if "retrieval_receipt.json" in names else OUTPUT_FILENAMES
    if any(not (output / name).is_file() or (output / name).is_symlink() for name in filenames):
        _invalid("snapshot members must be regular non-symlink files")
    try:
        members = {name: (output / name).read_bytes() for name in filenames}
    except OSError as exc:
        raise SnapshotValidationError("unable to read snapshot members") from exc

    member_hashes = {name: hashlib.sha256(content).hexdigest() for name, content in members.items()}
    if type(expected_manifest_sha256) is not str or member_hashes["manifest.json"] != expected_manifest_sha256:
        _invalid("trusted manifest hash mismatch")

    sums = _parse_json_object(members["sha256sums.json"], "sha256sums")
    manifest = _parse_json_object(members["manifest.json"], "manifest")
    validation = _parse_json_object(members["validation.json"], "validation")
    summed_files = tuple(name for name in filenames if name != "sha256sums.json")
    if set(sums) != set(summed_files):
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

    expected_manifest: dict[str, object] = {
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
    if "retrieval_receipt.json" in members:
        if expected_source_identity_sha256 is None or expected_calendar_sha256 is None:
            _invalid("external source and calendar digests are required")
        expected_source = _trusted_digest(expected_source_identity_sha256, name="source identity")
        expected_calendar = _trusted_digest(expected_calendar_sha256, name="calendar")
        receipt = _parse_json_object(members["retrieval_receipt.json"], "retrieval receipt")
        source_identity = _source_identity_reference(receipt.get("source_identity"))
        calendar = _calendar_reference(receipt.get("calendar"))
        coverage = _canonical_coverage(receipt.get("observed_coverage"), error="invalid retrieval receipt")
        if source_identity["artifact_sha256"] != expected_source or calendar["artifact_sha256"] != expected_calendar:
            _invalid("retrieval receipt external digest mismatch")
        common_sessions = sorted(set(coverage[SYMBOLS[0]]) & set(coverage[SYMBOLS[1]]))
        if common_sessions != prices["session"].dt.strftime("%Y-%m-%d").drop_duplicates().tolist():
            _invalid("invalid retrieval receipt")
        expected_receipt = {
            "contract_version": RETRIEVAL_CONTRACT_VERSION,
            "symbols": list(SYMBOLS),
            "source_identity": source_identity,
            "calendar": calendar,
            "observed_coverage": coverage,
            "prices_sha256": member_hashes["prices.csv"],
        }
        if not _has_exact_type(receipt, expected_receipt):
            _invalid("invalid retrieval receipt")
        expected_manifest.update(
            {
                "contract_version": RETRIEVAL_CONTRACT_VERSION,
                "source_identity": source_identity,
                "calendar": calendar,
                "retrieval_receipt_sha256": member_hashes["retrieval_receipt.json"],
            }
        )
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
    source_identity: dict[str, object] | None = None,
    calendar: dict[str, object] | None = None,
    observed_coverage: dict[str, list[str]] | None = None,
) -> SnapshotResult:
    """Validate fixture/local input and atomically write the four immutable contract files."""
    if mode != MODE:
        _invalid("mode must be core_only")
    if plugin != PLUGIN:
        _invalid("plugin must be ABSENT_DISABLED")
    if size != 0:
        _invalid("size must be zero")
    normalized = _normalized_prices(prices)
    if (source_identity is None) != (calendar is None) or (source_identity is None) != (observed_coverage is None):
        _invalid("source identity, calendar, and observed coverage are required together")
    if source_identity is not None:
        source_identity = _source_identity_reference(source_identity)
        calendar = _calendar_reference(calendar)
        observed_coverage = _canonical_coverage(observed_coverage, error="invalid observed coverage")
        common_sessions = sorted(set(observed_coverage[SYMBOLS[0]]) & set(observed_coverage[SYMBOLS[1]]))
        if common_sessions != normalized["session"].dt.strftime("%Y-%m-%d").drop_duplicates().tolist():
            _invalid("invalid observed coverage")
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
        if source_identity is not None:
            manifest["contract_version"] = RETRIEVAL_CONTRACT_VERSION
            manifest["source_identity"] = source_identity
            manifest["calendar"] = calendar
            _write_json(
                temporary / "retrieval_receipt.json",
                {
                    "contract_version": RETRIEVAL_CONTRACT_VERSION,
                    "symbols": list(SYMBOLS),
                    "source_identity": source_identity,
                    "calendar": calendar,
                    "observed_coverage": observed_coverage,
                    "prices_sha256": _sha256(prices_path),
                },
            )
            manifest["retrieval_receipt_sha256"] = _sha256(temporary / "retrieval_receipt.json")
        _write_json(temporary / "manifest.json", manifest)
        _write_json(
            temporary / "sha256sums.json",
            {
                name: _sha256(temporary / name)
                for name in ("prices.csv", "manifest.json", "validation.json", *(() if source_identity is None else ("retrieval_receipt.json",)))
            },
        )
        manifest_sha256 = _sha256(temporary / "manifest.json")
        verify_tqqq_r1_snapshot(
            temporary,
            expected_manifest_sha256=manifest_sha256,
            expected_source_identity_sha256=None if source_identity is None else source_identity["artifact_sha256"],
            expected_calendar_sha256=None if calendar is None else calendar["artifact_sha256"],
        )
        os.replace(temporary, destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return SnapshotResult(output_dir=destination, manifest_sha256=manifest_sha256)


def run_tqqq_r1_snapshot(
    output_dir: str | Path,
    *,
    source_identity_path: str | Path,
    expected_source_identity_sha256: str,
    calendar_path: str | Path,
    expected_calendar_sha256: str,
    observed_at: datetime | None = None,
    download_fn: Any = None,
) -> SnapshotResult:
    """Materialize one snapshot only after external source and XNYS anchors verify."""
    destination = Path(output_dir)
    if destination.exists() or destination.is_symlink():
        _invalid(f"immutable output already exists: {destination}")
    source_identity = _load_external_source_identity(
        source_identity_path,
        expected_source_identity_sha256=expected_source_identity_sha256,
    )
    calendar, closes = _load_external_xnys_calendar(
        calendar_path,
        expected_calendar_sha256=expected_calendar_sha256,
    )
    observed = _normalized_observed_prices(
        download_price_history(
            list(SYMBOLS),
            start=REQUESTED_LOWER_BOUND,
            download_fn=download_fn,
            price_field=PRICE_FIELD,
        )
    )
    _require_xnys_sessions(observed, closes, observed_at=observed_at or datetime.now(timezone.utc))
    return materialize_tqqq_r1_snapshot(
        _common_prices(observed),
        destination,
        source_identity=source_identity,
        calendar=calendar,
        observed_coverage=_observed_coverage(observed),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Materialize one externally anchored TQQQ R1 snapshot.")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--source-identity", required=True)
    parser.add_argument("--expected-source-identity-sha256", required=True)
    parser.add_argument("--xnys-calendar", required=True)
    parser.add_argument("--expected-calendar-sha256", required=True)
    args = parser.parse_args(argv)
    run_tqqq_r1_snapshot(
        args.output_dir,
        source_identity_path=args.source_identity,
        expected_source_identity_sha256=args.expected_source_identity_sha256,
        calendar_path=args.xnys_calendar,
        expected_calendar_sha256=args.expected_calendar_sha256,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
