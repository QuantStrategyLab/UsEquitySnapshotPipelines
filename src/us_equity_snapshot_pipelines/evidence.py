from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from pathlib import Path
import stat
from typing import Any


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


def _invalid(rule: str) -> None:
    raise EvidenceValidationError(f"evidence manifest/metrics violates {rule}")


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


_FILE_MAX_BYTES = {
    "manifest.json": 262_144, "champion_identity.json": 131_072, "prices.csv": 67_108_864,
    "data_quality.json": 8_388_608, "daily_returns.csv": 33_554_432, "targets.csv": 33_554_432,
    "trades.csv": 33_554_432, "costs.csv": 33_554_432, "metrics.json": 8_388_608,
    "walk_forward.json": 16_777_216, "trial_ledger.jsonl": 67_108_864, "robustness.json": 33_554_432,
    "risk_sleeve.json": 16_777_216, "strategy_performance.v2.json": 8_388_608, "checksums.sha256": 65_536,
}
_MAX_BUNDLE_BYTES = 361_168_896
_MAX_LINE_BYTES = 1_048_576
_HASH_CHUNK_BYTES = 1_048_576
_JSON_FILES = frozenset({
    "manifest.json", "champion_identity.json", "data_quality.json", "metrics.json", "walk_forward.json",
    "robustness.json", "risk_sleeve.json", "strategy_performance.v2.json",
})
_CHAMPION_FIELDS = frozenset({"schema", "profile", "code_sha", "config_sha256", "plugin_sha256", "execution_timing", "timezone", "calendar", "as_of"})


class _DuplicateJSONKey(ValueError):
    pass


def _json_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJSONKey
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise ValueError(value)


def _check_json_depth(value: Any, name: str) -> None:
    pending = [(value, 1)]
    while pending:
        item, depth = pending.pop()
        if not isinstance(item, (dict, list)):
            continue
        if depth > 64:
            _invalid(f"{name} JSON nesting")
        children = item.values() if isinstance(item, dict) else item
        pending.extend((child, depth + 1) for child in children)


def _consume(state: dict[str, int], name: str, cap: int, hasher: Any, chunk: bytes, used: int) -> int:
    used += len(chunk)
    state["total"] += len(chunk)
    if used > cap:
        _invalid(f"{name} size")
    if state["total"] > _MAX_BUNDLE_BYTES:
        _invalid("bundle total size")
    hasher.update(chunk)
    return used


def _parse_json_text(text: str, name: str) -> Any:
    try:
        value = json.loads(text, object_pairs_hook=_json_pairs, parse_constant=_reject_json_constant)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError):
        _invalid(f"{name} JSON")
    if not isinstance(value, dict):
        _invalid(f"{name} JSON object")
    _check_json_depth(value, name)
    return value


def _parse_json_stream(stream: Any, name: str, state: dict[str, int]) -> tuple[dict[str, Any], str]:
    hasher = hashlib.sha256()
    chunks: list[bytes] = []
    used = 0
    while chunk := stream.read(_HASH_CHUNK_BYTES):
        used = _consume(state, name, _FILE_MAX_BYTES[name], hasher, chunk, used)
        chunks.append(chunk)
    try:
        text = b"".join(chunks).decode("utf-8")
    except UnicodeDecodeError:
        _invalid(f"{name} UTF-8")
    return _parse_json_text(text, name), hasher.hexdigest()


def _physical_lines(stream: Any, name: str, state: dict[str, int], hasher: Any, encoding: str = "utf-8", require_lf: bool = False):
    used = 0
    while raw := stream.readline(_MAX_LINE_BYTES + 1):
        used = _consume(state, name, _FILE_MAX_BYTES[name], hasher, raw, used)
        if len(raw) > _MAX_LINE_BYTES:
            _invalid(f"{name} line size")
        if require_lf and not raw.endswith(b"\n"):
            _invalid(f"{name} line ending")
        body = raw[:-1] if raw.endswith(b"\n") else raw
        if not body.strip():
            _invalid(f"{name} blank line")
        try:
            yield raw.decode(encoding)
        except UnicodeDecodeError:
            _invalid(f"{name} UTF-8")


def _parse_jsonl(stream: Any, name: str, state: dict[str, int]) -> str:
    hasher = hashlib.sha256()
    count = 0
    for line in _physical_lines(stream, name, state, hasher, require_lf=True):
        _parse_json_text(line[:-1], name)
        count += 1
    if not count:
        _invalid(f"{name} records")
    return hasher.hexdigest()


def _parse_csv(stream: Any, name: str, state: dict[str, int]) -> str:
    hasher = hashlib.sha256()
    rows = 0
    header: list[str] | None = None
    try:
        for row in csv.reader(_physical_lines(stream, name, state, hasher)):
            if header is None:
                if not row or any(not column for column in row) or len(set(row)) != len(row):
                    _invalid(f"{name} header")
                header = row
            else:
                if len(row) != len(header):
                    _invalid(f"{name} row width")
            rows += 1
    except csv.Error:
        _invalid(f"{name} CSV")
    if header is None or rows < 2:
        _invalid(f"{name} rows")
    return hasher.hexdigest()


def _open_regular(root: str, name: str):
    path = os.path.join(root, name)
    try:
        info = os.lstat(path)
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            _invalid(f"{name} regular file")
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size > _FILE_MAX_BYTES[name]:
                _invalid(f"{name} regular file")
            return os.fdopen(fd, "rb", closefd=True)
        except BaseException:
            os.close(fd)
            raise
    except EvidenceValidationError:
        raise
    except OSError:
        _invalid(f"{name} regular file")


def _parse_checksums(stream: Any, state: dict[str, int]) -> tuple[dict[str, str], str]:
    hasher = hashlib.sha256()
    rows: list[tuple[str, str]] = []
    pattern = re.compile(r"([0-9a-f]{64})  ([^\n]+)\n\Z")
    for line in _physical_lines(stream, "checksums.sha256", state, hasher, encoding="ascii", require_lf=True):
        match = pattern.fullmatch(line)
        if match is None:
            _invalid("checksums.sha256 line")
        rows.append((match.group(2), match.group(1)))
    expected = sorted(set(REQUIRED_ARTIFACT_FILES) - {"checksums.sha256"})
    if [name for name, _ in rows] != expected:
        _invalid("checksums.sha256 set")
    return dict(rows), hasher.hexdigest()


def _parse_artifact(name: str, stream: Any, state: dict[str, int]) -> tuple[Any, str]:
    if name in _JSON_FILES:
        return _parse_json_stream(stream, name, state)
    if name == "trial_ledger.jsonl":
        return None, _parse_jsonl(stream, name, state)
    if name == "checksums.sha256":
        return _parse_checksums(stream, state)
    return None, _parse_csv(stream, name, state)


def validate_evidence_bundle(bundle_root: str | Path) -> dict[str, Any]:
    root = os.fspath(bundle_root)
    try:
        root_info = os.lstat(root)
    except OSError:
        _invalid("bundle root")
    if stat.S_ISLNK(root_info.st_mode) or not stat.S_ISDIR(root_info.st_mode):
        _invalid("bundle root")
    state = {"total": 0}
    with _open_regular(root, "manifest.json") as stream:
        manifest, _ = _parse_json_stream(stream, "manifest.json", state)
    manifest = validate_evidence_manifest(manifest)
    try:
        names = {entry.name for entry in os.scandir(root)}
    except OSError:
        _invalid("bundle root entries")
    if names != {"manifest.json", *REQUIRED_ARTIFACT_FILES}:
        _invalid("exact bundle root")
    parsed: dict[str, Any] = {}
    digests: dict[str, str] = {}
    for name in REQUIRED_ARTIFACT_FILES:
        with _open_regular(root, name) as stream:
            parsed[name], digests[name] = _parse_artifact(name, stream, state)
    identity = parsed["champion_identity.json"]
    if set(identity) != _CHAMPION_FIELDS or identity["schema"] != "champion_identity.v1":
        _invalid("champion identity fields")
    for field in _CHAMPION_FIELDS - {"schema"}:
        if identity[field] != manifest[field]:
            _invalid(f"champion identity {field}")
    try:
        validate_metrics_payload(parsed["metrics.json"])
    except (EvidenceValidationError, RecursionError):
        _invalid("metrics.json")
    checksums = parsed["checksums.sha256"]
    for name in REQUIRED_ARTIFACT_FILES:
        if manifest["artifacts"][name] != digests[name]:
            _invalid(f"{name} digest")
        if name != "checksums.sha256" and checksums[name] != digests[name]:
            _invalid(f"{name} checksum")
    return {"bundle_integrity_valid": True, "content_safety_status": "not_evaluated", "manifest": manifest}
