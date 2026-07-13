from __future__ import annotations

import math
import re
from collections.abc import Mapping
from datetime import date, datetime, timedelta
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
