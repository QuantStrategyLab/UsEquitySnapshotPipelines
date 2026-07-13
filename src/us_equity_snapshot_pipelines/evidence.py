from __future__ import annotations

import csv
import hashlib
import json
import math
import re
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from pathlib import Path
from stat import S_ISDIR, S_ISREG
from typing import Any
from urllib.parse import parse_qsl, urlsplit


class EvidenceValidationError(ValueError):
    """Raised when an in-memory research evidence contract is invalid."""


REQUIRED_ARTIFACT_FILES: tuple[str, ...] = (
    "champion_identity.json",
    "prices.csv",
    "data_quality.json",
    "daily_returns.csv",
    "targets.csv",
    "trades.csv",
    "costs.csv",
    "metrics.json",
    "walk_forward.json",
    "trial_ledger.jsonl",
    "robustness.json",
    "risk_sleeve.json",
    "strategy_performance.v2.json",
    "checksums.sha256",
)

_MANIFEST_FIELDS = frozenset(
    {
        "schema", "profile", "run_id", "code_sha", "config_sha256", "plugin_sha256", "data_sha256",
        "data_revision", "calendar", "timezone", "execution_timing", "cost_model_id", "random_seed",
        "as_of", "generated_at", "artifacts",
    }
)
_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")
_SHA1 = re.compile(r"[0-9a-f]{40}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
_UTC = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\Z")
_OTHER_TIMING = re.compile(r"other:[a-z0-9][a-z0-9._-]{1,63}\Z")


def _invalid(rule: str, location: str = "") -> None:
    suffix = f" at {location}" if location else ""
    raise EvidenceValidationError(f"evidence validation violates {rule}{suffix}")


def _string_matching(payload: Mapping[str, Any], field: str, pattern: re.Pattern[str]) -> None:
    value = payload.get(field)
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        _invalid(field)


def validate_evidence_manifest(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping) or set(payload) != _MANIFEST_FIELDS:
        _invalid("exact manifest fields")
    manifest = dict(payload)
    if manifest["schema"] != "soxl_tqqq_research_evidence.v1":
        _invalid("schema")
    if not isinstance(manifest["profile"], str) or manifest["profile"] not in {"soxl_soxx_trend_income", "tqqq_growth_income"}:
        _invalid("profile")
    for field in ("run_id", "data_revision", "cost_model_id"):
        _string_matching(manifest, field, _ID)
    _string_matching(manifest, "code_sha", _SHA1)
    for field in ("config_sha256", "plugin_sha256", "data_sha256"):
        _string_matching(manifest, field, _SHA256)
    if not isinstance(manifest["calendar"], str) or manifest["calendar"] not in {"XNYS", "NYSE"}:
        _invalid("calendar")
    if manifest["timezone"] != "America/New_York":
        _invalid("timezone")
    timing = manifest["execution_timing"]
    if not isinstance(timing, str) or (timing not in {"next_open", "next_close", "scheduled_vwap"} and _OTHER_TIMING.fullmatch(timing) is None):
        _invalid("execution_timing")
    seed = manifest["random_seed"]
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        _invalid("random_seed")
    as_of = manifest["as_of"]
    if not isinstance(as_of, str) or _DATE.fullmatch(as_of) is None:
        _invalid("as_of")
    try:
        date.fromisoformat(as_of)
    except ValueError:
        _invalid("as_of")
    generated_at = manifest["generated_at"]
    if not isinstance(generated_at, str) or _UTC.fullmatch(generated_at) is None:
        _invalid("generated_at")
    try:
        parsed = datetime.fromisoformat(generated_at[:-1] + "+00:00")
    except ValueError:
        _invalid("generated_at")
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        _invalid("generated_at")
    artifacts = manifest["artifacts"]
    if not isinstance(artifacts, Mapping) or set(artifacts) != set(REQUIRED_ARTIFACT_FILES):
        _invalid("exact artifact mapping")
    for digest in artifacts.values():
        if not isinstance(digest, str) or _SHA256.fullmatch(digest) is None:
            _invalid("artifact digest")
    return manifest


def _reject_nonfinite(value: Any) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        _invalid("finite metrics")
    if isinstance(value, Mapping):
        for item in value.values():
            _reject_nonfinite(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _reject_nonfinite(item)


def _contains_numeric(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return True
    if isinstance(value, Mapping):
        return any(_contains_numeric(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_numeric(item) for item in value)
    return False


def validate_metrics_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        _invalid("metrics object")
    _reject_nonfinite(payload)
    metrics = payload.get("metrics")
    if not isinstance(metrics, Mapping) or not metrics or not _contains_numeric(metrics):
        _invalid("numeric metrics evidence")
    return dict(payload)


MAX_JSON_BYTES, MAX_TEXT_LINE_BYTES, HASH_CHUNK_BYTES, MAX_NESTING_DEPTH = 8 * 1024 * 1024, 1 * 1024 * 1024, 1 * 1024 * 1024, 32
_CHAMPION_FIELDS = frozenset({"schema", "profile", "code_sha", "config_sha256", "plugin_sha256",
                              "execution_timing", "timezone", "calendar", "as_of"})
_FORBIDDEN_KEYS = frozenset("""account account_id account_number broker_account_id account_balance
cash_balance balance_usd account_nav actual_nav portfolio_nav nav_usd order order_id client_order_id
broker_order_id quantity qty actual_quantity notional actual_notional broker_endpoint session_id
service_id deployment_id credential credentials api_key access_token refresh_token token secret
client_secret password passwd cookie set_cookie user_id email phone""".split())
_VALUE_NAMES = frozenset("""api_key apikey access_token refresh_token token secret client_secret password
passwd signature sig x-amz-credential x-amz-signature""".split())
_ASSIGNMENT = re.compile(r"(?i)(?<![a-z0-9_])(?:api_key|apikey|access_token|refresh_token|token|secret|client_secret|password|passwd|signature|sig|x-amz-credential|x-amz-signature)\s*[:=]\s*\S+")
_JWT = re.compile(r"^[A-Za-z0-9_-]{3,}\.[A-Za-z0-9_-]{3,}\.[A-Za-z0-9_-]{3,}$")
_DRIVE = re.compile(r"^[A-Za-z]:[\\/]")
_PREFIXES = (("github_pat_", 20), ("ghp_", 20), ("gho_", 20), ("ghu_", 20), ("ghs_", 20),
             ("ghr_", 20), ("sk-", 20), ("AKIA", 16), ("ASIA", 16), ("xoxb-", 10),
             ("xoxp-", 10), ("xoxa-", 10), ("xoxr-", 10), ("glpat-", 20), ("npm_", 20), ("pypi-", 20))
_JSON_ARTIFACTS = frozenset({name for name in REQUIRED_ARTIFACT_FILES if name.endswith(".json")})
_CSV_ARTIFACTS = frozenset({"prices.csv", "daily_returns.csv", "targets.csv", "trades.csv", "costs.csv"})


def _normalize_key(value: str) -> str:
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = re.sub(r"([A-Z])([A-Z][a-z])", r"\1_\2", value)
    return re.sub(r"_+", "_", re.sub(r"[^a-zA-Z0-9]+", "_", value.casefold())).strip("_")


def _inspect_string(value: str, artifact: str, location: str) -> None:
    normalized = value.replace("\\", "/")
    if (value.startswith(("/", "~")) or _DRIVE.match(value) or value.startswith(("\\\\", "//"))
            or value.casefold().startswith("file://") or ".." in normalized.split("/")):
        _invalid("private_path", f"{artifact}:{location}")
    if "://" in value:
        try:
            parsed = urlsplit(value)
            if parsed.username is not None or parsed.password is not None:
                _invalid("sensitive_value", f"{artifact}:{location}")
            parameters = dict(parse_qsl(parsed.query, keep_blank_values=True))
            parameters.update(dict(parse_qsl(parsed.fragment, keep_blank_values=True)))
            if any(name.casefold() in _VALUE_NAMES for name in parameters):
                _invalid("sensitive_value", f"{artifact}:{location}")
        except ValueError:
            _invalid("sensitive_value", f"{artifact}:{location}")
    if re.search(r"(?i)\b(?:bearer|basic)\s+\S+", value) or _ASSIGNMENT.search(value) or _JWT.fullmatch(value):
        _invalid("sensitive_value", f"{artifact}:{location}")
    for prefix, minimum in _PREFIXES:
        if value.startswith(prefix) and len(value) - len(prefix) >= minimum:
            _invalid("sensitive_value", f"{artifact}:{location}")


def _inspect(value: Any, artifact: str, location: str = "$") -> None:
    try:
        _inspect_inner(value, artifact, location)
    except RecursionError:
        _invalid("nesting_depth", f"{artifact}:{location}")


def _inspect_inner(value: Any, artifact: str, location: str = "$", depth: int = 0) -> None:
    if depth > MAX_NESTING_DEPTH:
        _invalid("nesting_depth", f"{artifact}:{location}")
    if isinstance(value, Mapping):
        for index, (key, item) in enumerate(value.items()):
            _inspect_string(str(key), artifact, f"{location}[{index}].key")
            if _normalize_key(str(key)) in _FORBIDDEN_KEYS:
                _invalid("sensitive_key", f"{artifact}:{location}[{index}]")
            _inspect_inner(item, artifact, f"{location}[{index}]", depth + 1)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _inspect_inner(item, artifact, f"{location}[{index}]", depth + 1)
    elif isinstance(value, str):
        _inspect_string(value, artifact, location)


def _regular(path: Path, rule: str, artifact: str) -> None:
    try:
        mode = path.lstat().st_mode
    except OSError:
        _invalid(rule, artifact)
    if not S_ISREG(mode):
        _invalid(rule, artifact)


def _json_file(path: Path, artifact: str) -> dict[str, Any]:
    try:
        if path.stat().st_size > MAX_JSON_BYTES:
            _invalid("json_size", artifact)
        with path.open("rb") as stream:
            raw = stream.read(MAX_JSON_BYTES + 1)
    except OSError:
        _invalid("artifact_read", artifact)
    if len(raw) > MAX_JSON_BYTES:
        _invalid("json_size", artifact)
    try:
        text = raw.decode("utf-8")
        value = json.loads(text, parse_constant=lambda _: _invalid("nonfinite_json", artifact))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
        _invalid("json_structure", artifact)
    if not isinstance(value, dict):
        _invalid("json_object", artifact)
    return value


def _text_lines(path: Path, artifact: str):
    try:
        with path.open("rb") as stream:
            while raw := stream.readline(MAX_TEXT_LINE_BYTES + 1):
                if len(raw) > MAX_TEXT_LINE_BYTES:
                    _invalid("text_line_size", artifact)
                try:
                    yield raw.decode("utf-8")
                except UnicodeDecodeError:
                    _invalid("utf8", artifact)
    except OSError:
        _invalid("artifact_read", artifact)


def _jsonl_file(path: Path, artifact: str) -> None:
    count = 0
    for line in _text_lines(path, artifact):
        if not line.strip():
            continue
        try:
            value = json.loads(line, parse_constant=lambda _: _invalid("nonfinite_json", artifact))
        except (json.JSONDecodeError, RecursionError):
            _invalid("jsonl_structure", artifact)
        if not isinstance(value, dict):
            _invalid("jsonl_object", artifact)
        _inspect(value, artifact, f"$[{count}]")
        count += 1
    if not count:
        _invalid("jsonl_empty", artifact)


def _csv_file(path: Path, artifact: str) -> None:
    try:
        reader = csv.reader(_text_lines(path, artifact))
        header = next(reader)
        if not header or any(not cell for cell in header) or len(set(header)) != len(header):
            _invalid("csv_header", artifact)
        for index, cell in enumerate(header):
            if _normalize_key(cell) in _FORBIDDEN_KEYS:
                _invalid("sensitive_key", f"{artifact}:header[{index}]")
            _inspect_string(cell, artifact, f"header[{index}]")
        rows = 0
        for row in reader:
            if len(row) != len(header):
                _invalid("csv_width", artifact)
            for index, cell in enumerate(row):
                _inspect_string(cell, artifact, f"row[{rows}][{index}]")
            rows += 1
    except (StopIteration, csv.Error):
        _invalid("csv_structure", artifact)
    if not rows:
        _invalid("csv_empty", artifact)


def _checksums(path: Path) -> dict[str, str]:
    entries: dict[str, str] = {}
    for line in _text_lines(path, "checksums.sha256"):
        match = re.fullmatch(r"([0-9a-f]{64})  ([^\r\n]+)\n", line)
        if match is None:
            _invalid("checksum_line", "checksums.sha256")
        digest, name = match.groups()
        if name in entries or name not in REQUIRED_ARTIFACT_FILES or name == "checksums.sha256":
            _invalid("checksum_set", "checksums.sha256")
        entries[name] = digest
    expected = set(REQUIRED_ARTIFACT_FILES) - {"checksums.sha256"}
    if set(entries) != expected or list(entries) != sorted(entries):
        _invalid("checksum_set", "checksums.sha256")
    return entries


def _sha256(path: Path, artifact: str) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            while chunk := stream.read(HASH_CHUNK_BYTES):
                digest.update(chunk)
    except OSError:
        _invalid("artifact_read", artifact)
    return digest.hexdigest()


def validate_evidence_bundle(bundle_root: str | Path) -> dict[str, Any]:
    root = Path(bundle_root)
    try:
        if not S_ISDIR(root.lstat().st_mode):
            _invalid("bundle_root", "bundle")
    except OSError:
        _invalid("bundle_root", "bundle")
    try:
        if (entries := list(root.iterdir())) and ({entry.name for entry in entries} != {"manifest.json", *REQUIRED_ARTIFACT_FILES} or any(not S_ISREG(entry.lstat().st_mode) for entry in entries)):
            _invalid("bundle_root_entries", "bundle")
    except OSError:
        _invalid("bundle_root_entries", "bundle")
    manifest_path = root / "manifest.json"
    _regular(manifest_path, "artifact_file", "manifest.json")
    manifest = _json_file(manifest_path, "manifest.json")
    validate_evidence_manifest(manifest)
    _inspect(manifest, "manifest.json")
    paths = {name: root / name for name in REQUIRED_ARTIFACT_FILES}
    for name, path in paths.items():
        _regular(path, "artifact_file", name)
    checksums = _checksums(paths["checksums.sha256"])
    for name in _JSON_ARTIFACTS:
        value = _json_file(paths[name], name)
        if name == "champion_identity.json":
            if set(value) != _CHAMPION_FIELDS or value.get("schema") != "champion_identity.v1":
                _invalid("champion_identity", name)
            if any(value[field] != manifest[field] for field in _CHAMPION_FIELDS - {"schema"}):
                _invalid("champion_identity", name)
        if name == "metrics.json":
            validate_metrics_payload(value)
        _inspect(value, name)
    for name in _CSV_ARTIFACTS:
        _csv_file(paths[name], name)
    _jsonl_file(paths["trial_ledger.jsonl"], "trial_ledger.jsonl")
    for name, path in paths.items():
        digest = _sha256(path, name)
        if digest != manifest["artifacts"][name] or (name != "checksums.sha256" and digest != checksums[name]):
            _invalid("artifact_digest", name)
    return {"bundle_secret_free": True, "manifest": manifest}
