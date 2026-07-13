from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import date, datetime
from pathlib import Path, PurePosixPath
from typing import Any


class EvidenceValidationError(ValueError):
    pass


ALLOWED_PROFILES = frozenset({"soxl_soxx_trend_income", "tqqq_growth_income"})
REQUIRED_ARTIFACT_FILES = tuple(
    "champion_identity.json prices.parquet data_quality.json daily_returns.csv targets.csv trades.csv costs.csv metrics.json "
    "walk_forward.json trial_ledger.jsonl robustness.json risk_sleeve.json strategy_performance.v2.json checksums.sha256".split()
)
_IDENTITY_FIELDS = frozenset("schema profile code_sha config_sha256 plugin_sha256 execution_timing timezone calendar as_of".split())
_MANIFEST_FIELDS = frozenset(
    "schema profile run_id code_sha config_sha256 plugin_sha256 data_sha256 data_revision calendar timezone execution_timing "
    "cost_model_id random_seed as_of generated_at artifacts".split()
)
_FORBIDDEN_KEY_PARTS = (
    "account",
    "balance",
    "credential",
    "cookie",
    "deployment",
    "endpoint",
    "notional",
    "order",
    "password",
    "private",
    "quantity",
    "secret",
    "service",
    "token",
    "broker",
    "order_id",
    "orderid",
    "session_id",
    "session_token",
)
_SHA1_RE, _SHA256_RE = re.compile(r"^[0-9a-f]{40}$"), re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID_RE, _OTHER_TIMING_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$"), re.compile(r"^other:[a-z0-9][a-z0-9._-]{1,63}$")


def _fail(message: str) -> None:
    raise EvidenceValidationError(message)


def _require_mapping(payload: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        _fail(f"{label} must be a JSON object")
    return payload


def _reject_forbidden_keys(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if not isinstance(key, str):
                _fail("object keys must be strings")
            normalized = key.lower()
            if any(part in normalized for part in _FORBIDDEN_KEY_PARTS):
                _fail("forbidden sensitive field")
            if normalized == "artifacts" and isinstance(nested, Mapping):
                for digest in nested.values():
                    _reject_forbidden_keys(digest)
                continue
            _reject_forbidden_keys(nested)
    elif isinstance(value, list):
        for nested in value:
            _reject_forbidden_keys(nested)


def _require_exact_fields(payload: Mapping[str, Any], required: set[str], allowed: frozenset[str], label: str) -> None:
    missing = required.difference(payload)
    if missing:
        _fail(f"{label} required field missing: {sorted(missing)[0]}")
    if set(payload).difference(allowed):
        _fail(f"{label} contains unknown field")


def _require_string(payload: Mapping[str, Any], field: str, label: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        _fail(f"{label} field invalid")
    return value


def _require_hash(payload: Mapping[str, Any], field: str, pattern: re.Pattern[str], label: str) -> str:
    value = _require_string(payload, field, label)
    if pattern.fullmatch(value) is None:
        _fail(f"{label} hash field invalid")
    return value


def _validate_timestamp(value: Any, label: str, *, date_only: bool = True) -> None:
    if not isinstance(value, str):
        _fail(f"{label} timestamp invalid")
    try:
        if date_only and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            date.fromisoformat(value)
            return
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        _fail(f"{label} timestamp invalid")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _fail(f"{label} timestamp invalid")


def _validate_common_identity_fields(payload: Mapping[str, Any], *, label: str) -> None:
    profile = _require_string(payload, "profile", label)
    if profile not in ALLOWED_PROFILES:
        _fail(f"{label} profile invalid")
    _require_hash(payload, "code_sha", _SHA1_RE, label)
    _require_hash(payload, "config_sha256", _SHA256_RE, label)
    _require_hash(payload, "plugin_sha256", _SHA256_RE, label)
    execution_timing = _require_string(payload, "execution_timing", label)
    if execution_timing not in {"next_open", "next_close", "scheduled_vwap"} and _OTHER_TIMING_RE.fullmatch(execution_timing) is None:
        _fail(f"{label} timing invalid")
    if _require_string(payload, "timezone", label) != "America/New_York":
        _fail(f"{label} timezone invalid")
    if _require_string(payload, "calendar", label) not in {"XNYS", "NYSE"}:
        _fail(f"{label} calendar invalid")
    _validate_timestamp(payload.get("as_of"), f"{label} as_of")


def validate_champion_identity(payload: Mapping[str, Any]) -> dict[str, Any]:
    _reject_forbidden_keys(payload)
    payload = _require_mapping(payload, "champion identity")
    _require_exact_fields(payload, set(_IDENTITY_FIELDS), _IDENTITY_FIELDS, "champion identity")
    if _require_string(payload, "schema", "champion identity") != "champion_identity.v1":
        _fail("champion identity schema invalid")
    _validate_common_identity_fields(payload, label="champion identity")
    return dict(payload)


def load_champion_identity(path: str | Path) -> dict[str, Any]:
    resolved = Path(path)
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        _fail("champion identity file unreadable")
    return validate_champion_identity(payload)


def _validate_safe_relative_path(name: Any) -> str:
    if not isinstance(name, str) or not name or "\\" in name:
        _fail("artifact path invalid")
    path = PurePosixPath(name)
    if path.is_absolute() or name.startswith("~") or ":" in path.parts[0] or any(part in {"", ".", ".."} for part in path.parts):
        _fail("artifact path unsafe")
    return name


def _validate_metrics_file(path: Path) -> None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        _fail("metrics file unreadable")
    _reject_forbidden_keys(payload)
    payload = _require_mapping(payload, "metrics")
    metrics = payload.get("metrics")
    if not isinstance(metrics, Mapping) or not metrics:
        _fail("metrics must be machine-readable")
    for metric in metrics.values():
        if not isinstance(metric, Mapping):
            _fail("metrics must be machine-readable")
        if metric.get("status") == "insufficient_evidence":
            continue
        value = metric.get("value")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            _fail("metrics must not be prose-only")


def _validate_artifact_content(path: Path) -> None:
    if path.suffix == ".csv":
        try:
            header = path.read_text(encoding="utf-8").splitlines()[0].split(",")
        except (OSError, IndexError, UnicodeDecodeError):
            _fail("artifact content unreadable")
        _reject_forbidden_keys({field.strip(): None for field in header})
    elif path.suffix in {".json", ".jsonl"}:
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
            payloads = [json.loads(line) for line in lines] if path.suffix == ".jsonl" else [json.loads("\n".join(lines))]
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            _fail("artifact content unreadable")
        for payload in payloads:
            _reject_forbidden_keys(payload)


def _validate_artifact_files(manifest: Mapping[str, Any], bundle_root: Path) -> None:
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, Mapping):
        _fail("artifacts hashes required")
    for name in artifacts:
        _validate_safe_relative_path(name)
    if set(artifacts) != set(REQUIRED_ARTIFACT_FILES):
        _fail("artifacts must contain exactly the required files")
    for name, digest in artifacts.items():
        if not isinstance(digest, str) or _SHA256_RE.fullmatch(digest) is None:
            _fail("artifact hash invalid")
        path = bundle_root / name
        if not path.is_file():
            _fail("required file missing")
        if path.is_symlink():
            _fail("artifact path unsafe")
        if not path.is_relative_to(bundle_root):
            _fail("artifact path unsafe")
        if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            _fail("artifact hash mismatch")
        _validate_artifact_content(path)
    _validate_metrics_file(bundle_root / "metrics.json")


def validate_evidence_manifest(payload: Mapping[str, Any], bundle_root: str | Path | None = None) -> dict[str, Any]:
    _reject_forbidden_keys(payload)
    payload = _require_mapping(payload, "evidence manifest")
    required = set(_MANIFEST_FIELDS)
    _require_exact_fields(payload, required, _MANIFEST_FIELDS, "evidence manifest")
    if _require_string(payload, "schema", "evidence manifest") != "soxl_tqqq_research_evidence.v1":
        _fail("evidence manifest schema invalid")
    _validate_common_identity_fields(payload, label="evidence manifest")
    _require_hash(payload, "data_sha256", _SHA256_RE, "evidence manifest")
    for field in ("run_id", "data_revision", "cost_model_id"):
        value = _require_string(payload, field, "evidence manifest")
        if _SAFE_ID_RE.fullmatch(value) is None:
            _fail("evidence manifest identifier invalid")
    seed = payload.get("random_seed")
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        _fail("evidence manifest random_seed invalid")
    _validate_timestamp(payload.get("generated_at"), "generated_at", date_only=False)
    if bundle_root is not None:
        _validate_artifact_files(payload, Path(bundle_root).resolve())
    return dict(payload)


def validate_evidence_bundle(bundle_root: str | Path) -> dict[str, Any]:
    root = Path(bundle_root).resolve()
    manifest_path = root / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        _fail("manifest file unreadable")
    validated = validate_evidence_manifest(manifest, root)
    identity = load_champion_identity(root / "champion_identity.json")
    for field in ("profile", "code_sha", "config_sha256", "plugin_sha256", "execution_timing", "timezone", "calendar", "as_of"):
        if identity[field] != validated[field]:
            _fail("champion identity does not match manifest")
    return validated
