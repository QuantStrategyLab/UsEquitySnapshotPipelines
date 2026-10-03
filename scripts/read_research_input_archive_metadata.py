"""GET-only research archive metadata / integrity / contracts / materialized reader.

metadata_only: fixed RAW metadata + bounded complete index + four P1 metadata.
integrity_only: fixed-generation whole-byte hashes for RAW/complete/P1 plus three
contract metadata observations.
contracts_only: fixed-generation whole-byte hashes for the three RAW-prefix contracts.
materialized_identity_only: fixed P1 bytes + legacy-lock worker identity check.
raw_manifest_projection_only: fixed RAW manifest bytes + closed public projection.
No provider calls, listing, uploads, body dumps, or research qualification claims.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

OVERALL_BUDGET_S = 30.0
ROOT_ENV = "SOXL_V7_R6_PRIVATE_ROOT"
ROOT_SHA256 = "274610e59a8d654b412af9155ee4c3bd59b90a53fc5f8bbc56091d346f60070c"
ALLOWED_REPOSITORY = "QuantStrategyLab/UsEquitySnapshotPipelines"
STORAGE_READONLY_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only"
# SDK 3.12+ otherwise queues a background bucket metadata GET from Blob spans.
DISABLE_OTEL_BUCKET_METADATA_ENV = "DISABLE_GCS_PYTHON_CLIENT_OTEL_BUCKET_METADATA"

RAW_BUCKET = "qsl-research-evidence-831478360303"
RAW_OBJECT = "research/v2/input/qqqm-boxx-raw-20260925-001/manifest.json"
RAW_PREFIX = "research/v2/input/qqqm-boxx-raw-20260925-001/"
RAW_GENERATION = 1790338090501279
RAW_SIZE_BYTES = 7580
RAW_EXPECTED_SHA256 = "cb14a511083c824a748d137a271c93cfe0e8adf38f648905b26e37decf4c6182"
RAW_MANIFEST_SCHEMA = "qsl.research.raw_sip_input.v1"
RAW_MANIFEST_SOURCE = "alpaca.stocks.bars.v2_and_corporate_actions.v1"
RAW_MANIFEST_FEED = "sip"
RAW_MANIFEST_PRICE_ADJUSTMENT = "raw"
RAW_MANIFEST_CALENDAR = "XNYS"
RAW_MANIFEST_TIMEZONE = "America/New_York"
RAW_MANIFEST_CURRENCY = "USD"
RAW_MANIFEST_LICENSE_RETENTION = (
    "private research only; retain in qsl-research-evidence bucket; no redistribution"
)
RAW_MANIFEST_BAR_TIMESTAMP_MEANING = (
    "left edge of daily bar, not decision availability"
)
RAW_MANIFEST_CORPORATE_ACTION_LIMITATION = (
    "provider does not guarantee creation time; historical availability not proven"
)
RAW_MANIFEST_SYMBOLS = ("QQQM", "BOXX", "SOXL", "SOXX", "TQQQ", "QQQ")
RAW_MANIFEST_KINDS = ("bars", "actions")
RAW_MANIFEST_INPUT_KEYS = frozenset(
    (symbol, kind) for symbol in RAW_MANIFEST_SYMBOLS for kind in RAW_MANIFEST_KINDS
)
RAW_MANIFEST_TOP_KEYS = frozenset(
    {
        "schema_version",
        "retrieved_at",
        "source",
        "feed",
        "price_adjustment",
        "calendar",
        "timezone",
        "currency",
        "license_retention",
        "scope",
        "write_probe",
        "inputs",
        "provider_page_requests",
        "provider_response_bytes",
        "bar_timestamp_meaning",
        "corporate_action_limitation",
        "no_order",
        "research_only",
        "execution_authorized",
    }
)
RAW_MANIFEST_INPUT_FIELD_KEYS = frozenset(
    {
        "symbol",
        "kind",
        "request",
        "count",
        "first_bar_time",
        "last_bar_time",
        "pages",
        "complete_pagination",
    }
)
RAW_MANIFEST_PAGE_KEYS = frozenset({"uri", "generation", "bytes", "sha256"})
RAW_MANIFEST_PROBE_KEYS = frozenset({"uri", "generation", "bytes", "sha256"})
RAW_DIGITS = re.compile(r"^[1-9][0-9]*$")

R6_COMPLETE_NAME = "complete.json"
R6_COMPLETE_MAX_BYTES = 64 * 1024
R6_COMPLETE_GENERATION = 1790408062686989
R6_COMPLETE_SIZE_BYTES = 3390
R6_COMPLETE_SHA256 = "15bfb9d59a884e79614f555b959d888df895377f4924c7639480b106752fee46"
R6_COMPLETE_OBSERVED_AT = "2026-09-26T07:34:09Z"
R6_LICENSE_SHA256 = "c11f174c833e93443df6dd210e4d965dfb4aaa9a86406a95f434728c4ac07cdb"

P1_MAX_BYTES = 16 * 1024 * 1024
P1_KEYS = ("binding.json", "manifest.json", "closes.json", "assurance.json")
P1_NAME_MAP = {key: f"p1/{key}" for key in P1_KEYS}
P1_MANIFEST_SHA256 = "86fa48cba3459ad9ff228671cd5b5e574e0e68c1637eaa40d7487c541ff36795"
P1_PINS: dict[str, dict[str, object]] = {
    "binding.json": {
        "generation": 1790408051987526,
        "size_bytes": 832,
        "sha256": "769ab7fd151d7576755ef1ab3a01540cd1229ae671f07faeb348a59fec484f27",
    },
    "manifest.json": {
        "generation": 1790408052165082,
        "size_bytes": 2199,
        "sha256": P1_MANIFEST_SHA256,
    },
    "closes.json": {
        "generation": 1790408052331620,
        "size_bytes": 147981,
        "sha256": "e5761ee4def3b42237f605bf04e65056b2556e3bb4be7ae1b376a925b7008072",
    },
    "assurance.json": {
        "generation": 1790408052509816,
        "size_bytes": 1097,
        "sha256": "ad2b8232ea2b4f4b22f1a8c0d4e34ddfa2170798fb2fceab52afb4fbb998048b",
    },
}
INTEGRITY_MAX_BODY_BYTES = (
    RAW_SIZE_BYTES
    + R6_COMPLETE_SIZE_BYTES
    + int(P1_PINS["binding.json"]["size_bytes"])
    + int(P1_PINS["manifest.json"]["size_bytes"])
    + int(P1_PINS["closes.json"]["size_bytes"])
    + int(P1_PINS["assurance.json"]["size_bytes"])
)

STUDY_ID = "soxl_v7_twelve_basic_split_close_development_v1"
INPUT_CONTRACT_ID = "qsl.soxl-v7-twelve-single-source-r6-input.v1"
COMPLETION_SCHEMA = "soxl-v7-r6-twelve-single-completion.v1"
SIGNAL_CANDIDATE_ID = "soxl_soxx_core_only_p2_v7_longterm_compounding_cash_reserve"
PRODUCER_REVISION = "0ae8ac4eb886431f9f9695702d9dd60982919dae"
DATE_CUTOFF = "2026-08-25"
SOURCE_ASSURANCE = "single_source_structural_only_no_cross_provider_verification"
BINDING_SHA256 = str(P1_PINS["binding.json"]["sha256"])

CONTRACT_SPECS: tuple[tuple[str, str], ...] = (
    (
        "tqqq_qqq_guard_cash_contract.v1.json",
        "7b603312762262ebb2fe90c86bef2ed0b7914ab26586b0b9db93ee39b4d1e60b",
    ),
    (
        "boxx_outer_cash_policy.v1.json",
        "cfed32767cb367ccce7ef880c3c15f82d50b99fe805d542e85839fc67270c905",
    ),
    (
        "s4_budget_policy.v1.json",
        "8c7a4410717c52222bb09c91a9c5d6774524625b8b2ad3226ffb2cd28dd31bbd",
    ),
)
# Fixed pins for contracts_only whole-byte integrity (independent of CONTRACT_SPECS
# metadata-only observations used by integrity_only).
CONTRACT_PINS: dict[str, dict[str, object]] = {
    "tqqq_qqq_guard_cash_contract.v1.json": {
        "generation": 1790498207123297,
        "size_bytes": 3620,
        "sha256": "7b603312762262ebb2fe90c86bef2ed0b7914ab26586b0b9db93ee39b4d1e60b",
    },
    "boxx_outer_cash_policy.v1.json": {
        "generation": 1790498221615335,
        "size_bytes": 2986,
        "sha256": "cfed32767cb367ccce7ef880c3c15f82d50b99fe805d542e85839fc67270c905",
    },
    "s4_budget_policy.v1.json": {
        "generation": 1790498234696597,
        "size_bytes": 1764,
        "sha256": "8c7a4410717c52222bb09c91a9c5d6774524625b8b2ad3226ffb2cd28dd31bbd",
    },
}
CONTRACTS_MAX_BODY_BYTES = (
    int(CONTRACT_PINS["tqqq_qqq_guard_cash_contract.v1.json"]["size_bytes"])
    + int(CONTRACT_PINS["boxx_outer_cash_policy.v1.json"]["size_bytes"])
    + int(CONTRACT_PINS["s4_budget_policy.v1.json"]["size_bytes"])
)

MATERIALIZED_MAX_BODY_BYTES = (
    int(P1_PINS["binding.json"]["size_bytes"])
    + int(P1_PINS["manifest.json"]["size_bytes"])
    + int(P1_PINS["closes.json"]["size_bytes"])
    + int(P1_PINS["assurance.json"]["size_bytes"])
)
LEGACY_SOURCE_COMMIT = "0ae8ac4eb886431f9f9695702d9dd60982919dae"
LEGACY_UV_LOCK_SHA256 = "56f837bdf65342ebff4e59f8dd3b5f2e351dbb064ca0e1f46204827c50f1afc8"
EXPECTED_MATERIALIZED_WHOLE_SHA256 = (
    "6477644fc07a7202cd484e0d0309ecdb871ef347de844ee31fc2e5e63e36bb38"
)
EXPECTED_V7_CONFIG_SHA256 = (
    "843ab4e93e81985c2b3becc61a2f0b971508ccf25afa59acf402e75f574514d1"
)
EXPECTED_SESSION_COUNT = 914
MATERIALIZED_ENTRY_BUDGET_S = 150.0
MATERIALIZED_WORKER_BUDGET_S = 90.0
WORKER_CLEANUP_BUDGET_S = 5.0
WORKER_PROTOCOL = "qsl.materialized_worker.v1"
WORKER_NETWORK_GUARD_SCOPE = "python_monkeypatch_audit"
WORKER_BLOCKED_REASONS = frozenset(
    {
        "WORKER_ARGV_INVALID",
        "WORKER_IMPORT_FAILED",
        "WORKER_COMPUTE_FAILED",
        "WORKER_TIMEOUT",
        "WORKER_CLEANUP_FAILED",
        "WORKER_PROTOCOL_INVALID",
        "NETWORK_ATTEMPT_REFUSED",
    }
)
WORKER_OK_KEYS = frozenset(
    {
        "protocol",
        "status",
        "session_count",
        "v7_config_sha256",
        "p1_binding_sha256",
        "p1_manifest_sha256",
        "candidate_whole_sha256",
        "internal_materialized_sha256",
        "internal_verified",
        "network_guard_attempts",
        "network_guard_scope",
        "elapsed_import_s",
        "elapsed_compute_s",
        "attempt_count",
    }
)
WORKER_BLOCKED_KEYS = frozenset(
    {
        "protocol",
        "status",
        "reason_class",
        "network_guard_attempts",
        "network_guard_scope",
        "elapsed_import_s",
        "elapsed_compute_s",
        "attempt_count",
    }
)
TASK_ROOT_ENV = "QSL_MATERIALIZED_TASK_ROOT"
SOURCE_DIR_ENV = "QSL_MATERIALIZED_SOURCE_DIR"
VENV_PYTHON_ENV = "QSL_MATERIALIZED_VENV_PYTHON"
SOURCE_COMMIT_ENV = "QSL_MATERIALIZED_SOURCE_COMMIT"
LEGACY_ORIGIN_PINS: dict[str, str] = {
    "quant-platform-kit": "5c916917626707c4ee798c6b45a5d43609019816",
    "quant-strategy-plugins": "6b76d512c8273deb804c0770a203558284778399",
    "us-equity-strategies": "33d8c09a9aa517cde94f36d2f67e526c340ea6e9",
}
LEGACY_VERSION_PINS: dict[str, str] = {
    "google-cloud-storage": "3.12.0",
    "pandas": "3.0.3",
    "numpy": "2.4.6",
    "exchange-calendars": "4.13.2",
}

_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_SHA_NAME = re.compile(r"^(?:\.\./|/|gs:|https?:)", re.IGNORECASE)

# Isolated worker body: stdlib + installed legacy package only; no current PYTHONPATH.
_MATERIALIZED_WORKER_SOURCE = r"""
import hashlib
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

_ATTEMPTS = {"n": 0}
_PROTOCOL = "qsl.materialized_worker.v1"
_SCOPE = "python_monkeypatch_audit"
_AUDIT_EVENTS = {
    "socket.connect",
    "socket.getaddrinfo",
    "socket.sendto",
    "os.system",
    "os.fork",
    "os.posix_spawn",
    "subprocess.Popen",
}


def _bump() -> None:
    _ATTEMPTS["n"] += 1


def _deny(*_a, **_k):
    _bump()
    raise OSError("blocked")


def _install_guards() -> None:
    socket.getaddrinfo = _deny  # type: ignore[assignment]
    socket.create_connection = _deny  # type: ignore[assignment]

    def _connect(self, *_a, **_k):  # noqa: ANN001
        _bump()
        raise OSError("blocked")

    def _sendto(self, *_a, **_k):  # noqa: ANN001
        _bump()
        raise OSError("blocked")

    socket.socket.connect = _connect  # type: ignore[method-assign]
    socket.socket.sendto = _sendto  # type: ignore[method-assign]

    def _popen(*_a, **_k):
        _bump()
        raise RuntimeError("blocked")

    subprocess.Popen = _popen  # type: ignore[assignment]
    subprocess.run = _popen  # type: ignore[assignment]
    subprocess.call = _popen  # type: ignore[assignment]
    subprocess.check_call = _popen  # type: ignore[assignment]
    subprocess.check_output = _popen  # type: ignore[assignment]
    os.system = _popen  # type: ignore[assignment]
    if hasattr(os, "fork"):
        def _fork():
            _bump()
            raise OSError("blocked")

        os.fork = _fork  # type: ignore[assignment]
    if hasattr(os, "posix_spawn"):
        def _spawn(*_a, **_k):
            _bump()
            raise OSError("blocked")

        os.posix_spawn = _spawn  # type: ignore[attr-defined]

    def _audit(event, _args):  # noqa: ANN001
        if event in _AUDIT_EVENTS:
            _bump()
            raise OSError("blocked")

    sys.addaudithook(_audit)


def _emit(payload: dict) -> None:
    sys.stdout.write(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    sys.stdout.write("\n")
    sys.stdout.flush()


def _blocked(reason: str, *, import_s: float = 0.0, compute_s: float = 0.0) -> int:
    _emit(
        {
            "protocol": _PROTOCOL,
            "status": "BLOCKED",
            "reason_class": reason,
            "network_guard_attempts": int(_ATTEMPTS["n"]),
            "network_guard_scope": _SCOPE,
            "elapsed_import_s": round(float(import_s), 6),
            "elapsed_compute_s": round(float(compute_s), 6),
            "attempt_count": 1,
        }
    )
    return 2


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def main() -> int:
    if len(sys.argv) != 2:
        return _blocked("WORKER_ARGV_INVALID")
    root = Path(sys.argv[1])
    _install_guards()
    t0 = time.monotonic()
    try:
        from us_equity_snapshot_pipelines.lifecycle.soxl_v7_twelve_single_source_r6 import (
            materialize_input,
            read_input,
        )
    except Exception:
        return _blocked("WORKER_IMPORT_FAILED", import_s=time.monotonic() - t0)
    t1 = time.monotonic()
    try:
        members = read_input(root)
        result = materialize_input(members)
        if int(_ATTEMPTS["n"]) != 0:
            return _blocked(
                "NETWORK_ATTEMPT_REFUSED",
                import_s=t1 - t0,
                compute_s=time.monotonic() - t1,
            )
        declared_internal = result.get("materialized_input_sha256")
        without_internal = {
            key: value for key, value in result.items() if key != "materialized_input_sha256"
        }
        recomputed_internal = hashlib.sha256(_canonical(without_internal)).hexdigest()
        if declared_internal != recomputed_internal:
            return _blocked(
                "WORKER_COMPUTE_FAILED",
                import_s=t1 - t0,
                compute_s=time.monotonic() - t1,
            )
        whole = hashlib.sha256(_canonical(result)).hexdigest()
        sessions = result.get("sessions")
        p2 = result.get("p2_identity") if isinstance(result.get("p2_identity"), dict) else {}
        p1 = result.get("p1_identity") if isinstance(result.get("p1_identity"), dict) else {}
        payload = {
            "protocol": _PROTOCOL,
            "status": "OK",
            "session_count": len(sessions) if isinstance(sessions, list) else -1,
            "v7_config_sha256": p2.get("config_sha256"),
            "p1_binding_sha256": p1.get("binding_sha256"),
            "p1_manifest_sha256": p1.get("input_manifest_sha256"),
            "candidate_whole_sha256": whole,
            "internal_materialized_sha256": recomputed_internal,
            "internal_verified": True,
            "network_guard_attempts": int(_ATTEMPTS["n"]),
            "network_guard_scope": _SCOPE,
            "elapsed_import_s": round(t1 - t0, 6),
            "elapsed_compute_s": round(time.monotonic() - t1, 6),
            "attempt_count": 1,
        }
    except Exception:
        return _blocked(
            "WORKER_COMPUTE_FAILED",
            import_s=t1 - t0,
            compute_s=time.monotonic() - t1,
        )
    if int(_ATTEMPTS["n"]) != 0:
        return _blocked(
            "NETWORK_ATTEMPT_REFUSED",
            import_s=t1 - t0,
            compute_s=time.monotonic() - t1,
        )
    _emit(payload)
    return 0


raise SystemExit(main())
"""


class MetadataError(Exception):
    """Sanitized group failure with a fixed reason class."""

    def __init__(self, reason_class: str) -> None:
        self.reason_class = reason_class
        super().__init__(reason_class)


def _digest(value: bytes | str) -> str:
    data = value if isinstance(value, bytes) else value.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def _require_actions_identity() -> None:
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise MetadataError("ENV_REFUSED")
    if os.environ.get("GITHUB_REPOSITORY") != ALLOWED_REPOSITORY:
        raise MetadataError("ENV_REFUSED")


def _parse_root(uri: str) -> tuple[str, str]:
    if not isinstance(uri, str) or _digest(uri) != ROOT_SHA256:
        raise MetadataError("ROOT_MISMATCH")
    if not uri.startswith("gs://") or not uri.endswith("/"):
        raise MetadataError("ROOT_INVALID")
    parsed = urlparse(uri)
    if parsed.scheme != "gs" or parsed.query or parsed.params or parsed.fragment:
        raise MetadataError("ROOT_INVALID")
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    if not bucket or not prefix or ".." in prefix.split("/") or "//" in prefix:
        raise MetadataError("ROOT_INVALID")
    return bucket, prefix


def _strict_positive_int(value: object, *, reason: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise MetadataError(reason)
    if value <= 0:
        raise MetadataError(reason)
    return value


def _strict_nonneg_int(value: object, *, reason: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise MetadataError(reason)
    if value < 0:
        raise MetadataError(reason)
    return value


def _require_hex64(value: object, *, reason: str) -> str:
    if not isinstance(value, str) or _HEX64.fullmatch(value) is None:
        raise MetadataError(reason)
    return value


def _remaining(deadline: float, clock: Callable[[], float]) -> float:
    left = deadline - clock()
    if left <= 0:
        raise MetadataError("DEADLINE_EXCEEDED")
    return left


def _object_pairs_hook(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise MetadataError("COMPLETE_JSON_INVALID")
        result[key] = value
    return result


def _parse_complete_json(raw: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_object_pairs_hook,
            parse_constant=lambda _c: (_ for _ in ()).throw(MetadataError("COMPLETE_JSON_INVALID")),
        )
    except MetadataError:
        raise
    except Exception as exc:  # noqa: BLE001 - redact parser/transport detail
        raise MetadataError("COMPLETE_JSON_INVALID") from exc
    if not isinstance(payload, dict):
        raise MetadataError("COMPLETE_JSON_INVALID")

    def _reject_nonfinite(node: object) -> None:
        if isinstance(node, float) and (math.isnan(node) or math.isinf(node)):
            raise MetadataError("COMPLETE_JSON_INVALID")
        if isinstance(node, dict):
            for value in node.values():
                _reject_nonfinite(value)
        elif isinstance(node, list):
            for value in node:
                _reject_nonfinite(value)

    _reject_nonfinite(payload)
    return payload


def _validate_complete_identity(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("schema") != COMPLETION_SCHEMA:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    if payload.get("study_id") != STUDY_ID:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    if payload.get("producer_revision") != PRODUCER_REVISION:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    if payload.get("date_cutoff") != DATE_CUTOFF:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    if payload.get("signal_candidate_id") != SIGNAL_CANDIDATE_ID:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    if payload.get("source_assurance") != SOURCE_ASSURANCE:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    if payload.get("historical_point_in_time_certified") is not False:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    manifest_sha = _require_hex64(payload.get("p1_manifest_sha256"), reason="COMPLETE_IDENTITY_MISMATCH")
    if manifest_sha != P1_MANIFEST_SHA256:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    license_sha = _require_hex64(payload.get("license_evidence_sha256"), reason="COMPLETE_IDENTITY_MISMATCH")
    observed_at = payload.get("observed_at")
    if not isinstance(observed_at, str):
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    try:
        parsed = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH") from exc
    if parsed.tzinfo is None:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    return {
        "schema": COMPLETION_SCHEMA,
        "study_id": STUDY_ID,
        "producer_revision": PRODUCER_REVISION,
        "date_cutoff": DATE_CUTOFF,
        "signal_candidate_id": SIGNAL_CANDIDATE_ID,
        "source_assurance": SOURCE_ASSURANCE,
        "historical_point_in_time_certified": False,
        "p1_manifest_sha256": manifest_sha,
        "license_evidence_sha256": license_sha,
        "observed_at": observed_at,
    }


def _validate_p1_receipts(payload: dict[str, Any]) -> dict[str, dict[str, object]]:
    receipts = payload.get("p1")
    if not isinstance(receipts, dict) or set(receipts) != set(P1_KEYS):
        raise MetadataError("P1_SET_INVALID")
    validated: dict[str, dict[str, object]] = {}
    for key in P1_KEYS:
        receipt = receipts[key]
        if not isinstance(receipt, dict):
            raise MetadataError("P1_RECEIPT_INVALID")
        name = receipt.get("name")
        if not isinstance(name, str) or name != P1_NAME_MAP[key]:
            raise MetadataError("P1_RECEIPT_INVALID")
        if "/" not in name or name.startswith("/") or ".." in name.split("/") or "//" in name:
            raise MetadataError("P1_RECEIPT_INVALID")
        if _SHA_NAME.search(name) is not None or "?" in name or "#" in name:
            raise MetadataError("P1_RECEIPT_INVALID")
        generation = _strict_positive_int(receipt.get("generation"), reason="P1_RECEIPT_INVALID")
        size = _strict_nonneg_int(receipt.get("size_bytes"), reason="P1_RECEIPT_INVALID")
        if size > P1_MAX_BYTES:
            raise MetadataError("P1_RECEIPT_INVALID")
        sha256 = _require_hex64(receipt.get("sha256"), reason="P1_RECEIPT_INVALID")
        if key == "manifest.json" and sha256 != P1_MANIFEST_SHA256:
            raise MetadataError("P1_RECEIPT_INVALID")
        if set(receipt) - {"name", "generation", "size_bytes", "sha256"}:
            raise MetadataError("P1_RECEIPT_INVALID")
        validated[key] = {
            "name": name,
            "generation": generation,
            "size_bytes": size,
            "sha256": sha256,
        }
    return validated


def _require_receipts_match_pins(receipts: dict[str, dict[str, object]]) -> None:
    for key in P1_KEYS:
        pin = P1_PINS[key]
        receipt = receipts[key]
        if (
            receipt["generation"] != pin["generation"]
            or receipt["size_bytes"] != pin["size_bytes"]
            or receipt["sha256"] != pin["sha256"]
        ):
            raise MetadataError("P1_RECEIPT_PIN_MISMATCH")


def _reload_metadata(
    client: Any,
    *,
    bucket_name: str,
    object_name: str,
    generation: int | None,
    expected_generation: int | None,
    expected_size: int | None,
    deadline: float,
    clock: Callable[[], float],
    mismatch_reason: str,
    unavailable_reason: str,
) -> tuple[int, int]:
    timeout = _remaining(deadline, clock)
    try:
        blob = client.bucket(bucket_name).blob(object_name, generation=generation)
        blob.reload(retry=None, timeout=timeout)
        observed_generation = blob.generation
        observed_size = blob.size
    except MetadataError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise MetadataError(unavailable_reason) from exc
    _remaining(deadline, clock)
    generation_i = _strict_positive_int(observed_generation, reason=mismatch_reason)
    size_i = _strict_nonneg_int(observed_size, reason=mismatch_reason)
    if expected_generation is not None and generation_i != expected_generation:
        raise MetadataError(mismatch_reason)
    if expected_size is not None and size_i != expected_size:
        raise MetadataError(mismatch_reason)
    return generation_i, size_i


def _download_bytes(
    client: Any,
    *,
    bucket_name: str,
    object_name: str,
    generation: int,
    size: int,
    max_size: int,
    deadline: float,
    clock: Callable[[], float],
    oversize_reason: str,
    fail_reason: str,
) -> bytes:
    if size > max_size:
        raise MetadataError(oversize_reason)
    timeout = _remaining(deadline, clock)
    try:
        blob = client.bucket(bucket_name).blob(object_name, generation=generation)
        content = blob.download_as_bytes(
            start=0,
            end=size,
            if_generation_match=generation,
            retry=None,
            timeout=timeout,
            raw_download=True,
        )
    except MetadataError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise MetadataError(fail_reason) from exc
    _remaining(deadline, clock)
    if not isinstance(content, (bytes, bytearray)) or len(content) != size:
        raise MetadataError(fail_reason)
    return bytes(content)


def _download_complete(
    client: Any,
    *,
    bucket_name: str,
    prefix: str,
    generation: int,
    size: int,
    deadline: float,
    clock: Callable[[], float],
) -> bytes:
    return _download_bytes(
        client,
        bucket_name=bucket_name,
        object_name=prefix + R6_COMPLETE_NAME,
        generation=generation,
        size=size,
        max_size=R6_COMPLETE_MAX_BYTES,
        deadline=deadline,
        clock=clock,
        oversize_reason="COMPLETE_OVERSIZE",
        fail_reason="COMPLETE_DOWNLOAD_FAILED",
    )


def read_raw_group(
    client: Any,
    *,
    deadline: float,
    clock: Callable[[], float],
) -> dict[str, object]:
    generation, size = _reload_metadata(
        client,
        bucket_name=RAW_BUCKET,
        object_name=RAW_OBJECT,
        generation=RAW_GENERATION,
        expected_generation=RAW_GENERATION,
        expected_size=RAW_SIZE_BYTES,
        deadline=deadline,
        clock=clock,
        mismatch_reason="RAW_METADATA_MISMATCH",
        unavailable_reason="RAW_METADATA_UNAVAILABLE",
    )
    _remaining(deadline, clock)
    return {
        "status": "METADATA_MATCHED",
        "content_verified": False,
        "bucket": RAW_BUCKET,
        "object": RAW_OBJECT,
        "generation": generation,
        "size_bytes": size,
        "expected_sha256": RAW_EXPECTED_SHA256,
        "note": "source_declared_sha_not_stored_in_object_metadata",
    }


def read_r6_group(
    client: Any,
    *,
    root_uri: str,
    deadline: float,
    clock: Callable[[], float],
) -> dict[str, object]:
    bucket_name, prefix = _parse_root(root_uri)
    object_name = prefix + R6_COMPLETE_NAME

    # Observe actual positive generation/size once, then pin the index download.
    generation, size = _reload_metadata(
        client,
        bucket_name=bucket_name,
        object_name=object_name,
        generation=None,
        expected_generation=None,
        expected_size=None,
        deadline=deadline,
        clock=clock,
        mismatch_reason="COMPLETE_METADATA_MISMATCH",
        unavailable_reason="COMPLETE_METADATA_UNAVAILABLE",
    )
    if size > R6_COMPLETE_MAX_BYTES:
        raise MetadataError("COMPLETE_OVERSIZE")
    saved_generation, saved_size = generation, size
    body = _download_complete(
        client,
        bucket_name=bucket_name,
        prefix=prefix,
        generation=saved_generation,
        size=saved_size,
        deadline=deadline,
        clock=clock,
    )
    _remaining(deadline, clock)
    payload = _parse_complete_json(body)
    _remaining(deadline, clock)
    identity = _validate_complete_identity(payload)
    receipts = _validate_p1_receipts(payload)
    p1_meta: dict[str, object] = {}
    for key, receipt in receipts.items():
        observed_generation, observed_size = _reload_metadata(
            client,
            bucket_name=bucket_name,
            object_name=prefix + str(receipt["name"]),
            generation=int(receipt["generation"]),
            expected_generation=int(receipt["generation"]),
            expected_size=int(receipt["size_bytes"]),
            deadline=deadline,
            clock=clock,
            mismatch_reason="P1_METADATA_MISMATCH",
            unavailable_reason="P1_METADATA_UNAVAILABLE",
        )
        p1_meta[key] = {
            "name": receipt["name"],
            "generation": observed_generation,
            "size_bytes": observed_size,
            "sha256": receipt["sha256"],
            "body_read": False,
        }
    _remaining(deadline, clock)
    return {
        "status": "METADATA_READY",
        "content_verified": False,
        "completion_identity_authenticated": False,
        "complete": {
            "name": R6_COMPLETE_NAME,
            "generation": saved_generation,
            "size_bytes": saved_size,
            "sha256": _digest(body),
            **identity,
        },
        "p1": p1_meta,
    }


def _validate_manifest_binding_links(manifest: dict[str, Any]) -> None:
    if manifest.get("schema_version") != "research_input_manifest.v1":
        raise MetadataError("MANIFEST_BINDING_MISMATCH")
    if manifest.get("profile") != STUDY_ID:
        raise MetadataError("MANIFEST_BINDING_MISMATCH")
    if manifest.get("research_input_contract_id") != INPUT_CONTRACT_ID:
        raise MetadataError("MANIFEST_BINDING_MISMATCH")
    producer = manifest.get("producer")
    if not isinstance(producer, dict) or producer.get("commit_sha") != PRODUCER_REVISION:
        raise MetadataError("MANIFEST_BINDING_MISMATCH")
    calendar = manifest.get("calendar")
    adjustment = manifest.get("adjustment")
    if not isinstance(calendar, dict) or calendar.get("source_revision") != BINDING_SHA256:
        raise MetadataError("MANIFEST_BINDING_MISMATCH")
    if not isinstance(adjustment, dict) or adjustment.get("source_revision") != BINDING_SHA256:
        raise MetadataError("MANIFEST_BINDING_MISMATCH")
    members = manifest.get("members")
    if not isinstance(members, list) or len(members) != 2:
        raise MetadataError("MANIFEST_BINDING_MISMATCH")
    by_path: dict[str, dict[str, Any]] = {}
    for item in members:
        if not isinstance(item, dict):
            raise MetadataError("MANIFEST_BINDING_MISMATCH")
        path = item.get("path")
        if not isinstance(path, str) or path in by_path:
            raise MetadataError("MANIFEST_BINDING_MISMATCH")
        by_path[path] = item
    if set(by_path) != {"closes.json", "assurance.json"}:
        raise MetadataError("MANIFEST_BINDING_MISMATCH")
    for path in ("closes.json", "assurance.json"):
        pin = P1_PINS[path]
        item = by_path[path]
        size = item.get("size_bytes")
        sha256 = item.get("sha256")
        if size != pin["size_bytes"] or sha256 != pin["sha256"]:
            raise MetadataError("MANIFEST_BINDING_MISMATCH")


def _read_verified_raw_manifest_bytes(
    client: Any,
    *,
    deadline: float,
    clock: Callable[[], float],
) -> tuple[int, int, bytes]:
    """Reload + download fixed RAW manifest; return verified whole bytes only."""
    generation, size = _reload_metadata(
        client,
        bucket_name=RAW_BUCKET,
        object_name=RAW_OBJECT,
        generation=RAW_GENERATION,
        expected_generation=RAW_GENERATION,
        expected_size=RAW_SIZE_BYTES,
        deadline=deadline,
        clock=clock,
        mismatch_reason="RAW_METADATA_MISMATCH",
        unavailable_reason="RAW_METADATA_UNAVAILABLE",
    )
    body = _download_bytes(
        client,
        bucket_name=RAW_BUCKET,
        object_name=RAW_OBJECT,
        generation=generation,
        size=size,
        max_size=RAW_SIZE_BYTES,
        deadline=deadline,
        clock=clock,
        oversize_reason="RAW_OVERSIZE",
        fail_reason="RAW_DOWNLOAD_FAILED",
    )
    digest = _digest(body)
    if digest != RAW_EXPECTED_SHA256:
        raise MetadataError("RAW_HASH_MISMATCH")
    _remaining(deadline, clock)
    return generation, size, body


def read_raw_integrity_group(
    client: Any,
    *,
    deadline: float,
    clock: Callable[[], float],
) -> dict[str, object]:
    generation, size, body = _read_verified_raw_manifest_bytes(
        client, deadline=deadline, clock=clock
    )
    return {
        "status": "INTEGRITY_MATCHED",
        "content_hash_verified": True,
        "generation": generation,
        "size_bytes": size,
        "sha256": _digest(body),
    }


def _raw_manifest_pairs_hook(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise MetadataError("RAW_MANIFEST_INVALID")
        result[key] = value
    return result


def _parse_raw_manifest_json(raw: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_raw_manifest_pairs_hook,
            parse_constant=lambda _c: (_ for _ in ()).throw(
                MetadataError("RAW_MANIFEST_INVALID")
            ),
        )
    except MetadataError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise MetadataError("RAW_MANIFEST_INVALID") from exc
    if not isinstance(payload, dict):
        raise MetadataError("RAW_MANIFEST_INVALID")

    def _reject_nonfinite(node: object) -> None:
        if isinstance(node, float) and (math.isnan(node) or math.isinf(node)):
            raise MetadataError("RAW_MANIFEST_INVALID")
        if isinstance(node, dict):
            for value in node.values():
                _reject_nonfinite(value)
        elif isinstance(node, list):
            for value in node:
                _reject_nonfinite(value)

    _reject_nonfinite(payload)
    return payload


def _require_aware_iso_present(value: object) -> bool:
    if not isinstance(value, str) or not value:
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None


def _validate_raw_page(
    page: object, *, symbol: str, kind: str, seen_uris: set[str]
) -> dict[str, object]:
    if not isinstance(page, dict) or set(page) != RAW_MANIFEST_PAGE_KEYS:
        raise MetadataError("RAW_MANIFEST_INVALID")
    uri = page.get("uri")
    generation = page.get("generation")
    size = page.get("bytes")
    sha256 = page.get("sha256")
    expected_uri = f"gs://{RAW_BUCKET}/{RAW_PREFIX}{kind}/{symbol}/page-001.json"
    if not isinstance(uri, str) or uri != expected_uri:
        raise MetadataError("RAW_INPUT_SET_INVALID")
    if uri in seen_uris:
        raise MetadataError("RAW_INPUT_SET_INVALID")
    seen_uris.add(uri)
    if not isinstance(generation, str) or RAW_DIGITS.fullmatch(generation) is None:
        raise MetadataError("RAW_MANIFEST_INVALID")
    size_i = _strict_positive_int(size, reason="RAW_MANIFEST_INVALID")
    digest = _require_hex64(sha256, reason="RAW_MANIFEST_INVALID")
    return {
        "declared_size_bytes": size_i,
        "declared_sha256": digest,
        "has_original_generation": True,
    }


def _validate_raw_write_probe(probe: object) -> None:
    if not isinstance(probe, dict) or set(probe) != RAW_MANIFEST_PROBE_KEYS:
        raise MetadataError("RAW_MANIFEST_INVALID")
    uri = probe.get("uri")
    generation = probe.get("generation")
    size = probe.get("bytes")
    sha256 = probe.get("sha256")
    expected = f"gs://{RAW_BUCKET}/{RAW_PREFIX}_write_probe.json"
    if not isinstance(uri, str) or uri != expected:
        raise MetadataError("RAW_MANIFEST_IDENTITY_MISMATCH")
    if not isinstance(generation, str) or RAW_DIGITS.fullmatch(generation) is None:
        raise MetadataError("RAW_MANIFEST_INVALID")
    _strict_positive_int(size, reason="RAW_MANIFEST_INVALID")
    _require_hex64(sha256, reason="RAW_MANIFEST_INVALID")


def _project_raw_manifest(payload: dict[str, Any]) -> dict[str, object]:
    if set(payload) != RAW_MANIFEST_TOP_KEYS:
        raise MetadataError("RAW_MANIFEST_INVALID")
    identity_ok = (
        payload.get("schema_version") == RAW_MANIFEST_SCHEMA
        and payload.get("source") == RAW_MANIFEST_SOURCE
        and payload.get("feed") == RAW_MANIFEST_FEED
        and payload.get("price_adjustment") == RAW_MANIFEST_PRICE_ADJUSTMENT
        and payload.get("calendar") == RAW_MANIFEST_CALENDAR
        and payload.get("timezone") == RAW_MANIFEST_TIMEZONE
        and payload.get("currency") == RAW_MANIFEST_CURRENCY
        and payload.get("scope") == RAW_PREFIX
        and payload.get("no_order") is True
        and payload.get("research_only") is True
        and payload.get("execution_authorized") is False
        and payload.get("bar_timestamp_meaning") == RAW_MANIFEST_BAR_TIMESTAMP_MEANING
        and payload.get("corporate_action_limitation")
        == RAW_MANIFEST_CORPORATE_ACTION_LIMITATION
    )
    if not identity_ok:
        raise MetadataError("RAW_MANIFEST_IDENTITY_MISMATCH")
    license_scope_matches = payload.get("license_retention") == RAW_MANIFEST_LICENSE_RETENTION
    if not license_scope_matches:
        raise MetadataError("RAW_MANIFEST_IDENTITY_MISMATCH")
    retrieved_at = payload.get("retrieved_at")
    retrieved_at_present = isinstance(retrieved_at, str) and bool(retrieved_at)
    retrieved_at_valid = _require_aware_iso_present(retrieved_at)
    if not retrieved_at_present or not retrieved_at_valid:
        raise MetadataError("RAW_MANIFEST_IDENTITY_MISMATCH")
    historical_ok = (
        payload.get("corporate_action_limitation") == RAW_MANIFEST_CORPORATE_ACTION_LIMITATION
    )
    if not historical_ok:
        raise MetadataError("RAW_MANIFEST_IDENTITY_MISMATCH")
    _validate_raw_write_probe(payload.get("write_probe"))
    _strict_nonneg_int(payload.get("provider_page_requests"), reason="RAW_MANIFEST_INVALID")
    _strict_nonneg_int(payload.get("provider_response_bytes"), reason="RAW_MANIFEST_INVALID")

    inputs = payload.get("inputs")
    if not isinstance(inputs, list) or len(inputs) != len(RAW_MANIFEST_INPUT_KEYS):
        raise MetadataError("RAW_INPUT_SET_INVALID")
    seen_pairs: set[tuple[str, str]] = set()
    seen_uris: set[str] = set()
    projected: dict[tuple[str, str], dict[str, object]] = {}
    total_bytes = 0
    available_at_present = False
    for item in inputs:
        if not isinstance(item, dict) or set(item) != RAW_MANIFEST_INPUT_FIELD_KEYS:
            raise MetadataError("RAW_MANIFEST_INVALID")
        if "available_at" in item:
            available_at_present = True
            raise MetadataError("RAW_MANIFEST_INVALID")
        symbol = item.get("symbol")
        kind = item.get("kind")
        if not isinstance(symbol, str) or not isinstance(kind, str):
            raise MetadataError("RAW_INPUT_SET_INVALID")
        pair = (symbol, kind)
        if pair not in RAW_MANIFEST_INPUT_KEYS or pair in seen_pairs:
            raise MetadataError("RAW_INPUT_SET_INVALID")
        seen_pairs.add(pair)
        count = _strict_nonneg_int(item.get("count"), reason="RAW_MANIFEST_INVALID")
        if item.get("complete_pagination") is not True:
            raise MetadataError("RAW_INPUT_SET_INVALID")
        pages = item.get("pages")
        if not isinstance(pages, list) or len(pages) != 1:
            raise MetadataError("RAW_INPUT_SET_INVALID")
        if not isinstance(item.get("request"), dict):
            raise MetadataError("RAW_MANIFEST_INVALID")
        first_t = item.get("first_bar_time")
        last_t = item.get("last_bar_time")
        if kind == "actions":
            if first_t is not None or last_t is not None:
                raise MetadataError("RAW_MANIFEST_INVALID")
        else:
            if count == 0:
                if first_t is not None or last_t is not None:
                    raise MetadataError("RAW_MANIFEST_INVALID")
            else:
                if not isinstance(first_t, str) or not isinstance(last_t, str):
                    raise MetadataError("RAW_MANIFEST_INVALID")
                if not _require_aware_iso_present(first_t) or not _require_aware_iso_present(
                    last_t
                ):
                    raise MetadataError("RAW_MANIFEST_INVALID")
        page_proj = _validate_raw_page(
            pages[0], symbol=symbol, kind=kind, seen_uris=seen_uris
        )
        total_bytes += int(page_proj["declared_size_bytes"])
        projected[pair] = {
            "symbol": symbol,
            "kind": kind,
            "count": count,
            "page_count": 1,
            "declared_size_bytes": page_proj["declared_size_bytes"],
            "declared_sha256": page_proj["declared_sha256"],
            "has_original_generation": page_proj["has_original_generation"],
        }
    if seen_pairs != RAW_MANIFEST_INPUT_KEYS or len(seen_uris) != len(RAW_MANIFEST_INPUT_KEYS):
        raise MetadataError("RAW_INPUT_SET_INVALID")
    ordered = [
        projected[(symbol, kind)]
        for symbol in RAW_MANIFEST_SYMBOLS
        for kind in RAW_MANIFEST_KINDS
    ]
    return {
        "manifest_identity_matched": True,
        "license_scope_matches": True,
        "retrieved_at_present": True,
        "retrieved_at_valid": True,
        "available_at_present": available_at_present,
        "historical_availability_limitation_matches": True,
        "input_count": len(ordered),
        "total_declared_member_bytes": total_bytes,
        "inputs": ordered,
        "budget": {
            "manifest_get_count": 2,
            "manifest_bytes": RAW_SIZE_BYTES,
            "timeout_s": int(OVERALL_BUDGET_S),
            "planned_member_get_count": 24,
            "planned_member_bytes": total_bytes,
            "planned_next_run_with_manifest_reverify_get_count": 26,
            "planned_next_run_with_manifest_reverify_bytes": total_bytes + RAW_SIZE_BYTES,
        },
    }


def run_raw_manifest_projection_read(
    *,
    client: Any | None = None,
    clock: Callable[[], float] | None = None,
    budget_s: float = OVERALL_BUDGET_S,
    create_client: Callable[[], Any] | None = None,
) -> tuple[int, dict[str, object]]:
    started = (clock or time.monotonic)()
    deadline = started + budget_s
    mono = clock or time.monotonic
    result: dict[str, object] = {
        **_base_result(),
        "member_body_read": False,
        "member_content_verified": False,
        "manifest_identity_matched": False,
        "license_scope_matches": False,
        "retrieved_at_present": False,
        "retrieved_at_valid": False,
        "available_at_present": False,
        "historical_availability_limitation_matches": False,
        "input_count": 0,
        "total_declared_member_bytes": 0,
        "inputs": [],
        "budget": {
            "manifest_get_count": 2,
            "manifest_bytes": RAW_SIZE_BYTES,
            "timeout_s": int(OVERALL_BUDGET_S),
            "planned_member_get_count": 24,
            "planned_member_bytes": 0,
            "planned_next_run_with_manifest_reverify_get_count": 26,
            "planned_next_run_with_manifest_reverify_bytes": RAW_SIZE_BYTES,
        },
    }
    try:
        _require_actions_identity()
        if client is None:
            factory = create_client or _lazy_storage_client
            client = factory()
        _remaining(deadline, mono)
        _generation, _size, body = _read_verified_raw_manifest_bytes(
            client, deadline=deadline, clock=mono
        )
        _remaining(deadline, mono)
        payload = _parse_raw_manifest_json(body)
        projection = _project_raw_manifest(payload)
        _remaining(deadline, mono)
    except MetadataError as exc:
        result["reason_class"] = exc.reason_class
        return 2, result
    except Exception:  # noqa: BLE001
        result["reason_class"] = "CLIENT_UNAVAILABLE"
        return 2, result
    result.update(projection)
    result["status"] = "RAW_MANIFEST_PROJECTION_READY"
    result["member_body_read"] = False
    result["member_content_verified"] = False
    result["research_qualification"] = False
    result["trading_rights"] = False
    result["license_verified"] = False
    result["historical_point_in_time_certified"] = False
    result["completion_identity_authenticated"] = False
    result["content_verified"] = False
    result["content_integrity_verified"] = False
    return 0, result


def read_r6_integrity_group(
    client: Any,
    *,
    root_uri: str,
    deadline: float,
    clock: Callable[[], float],
) -> dict[str, object]:
    bucket_name, prefix = _parse_root(root_uri)
    generation, size = _reload_metadata(
        client,
        bucket_name=bucket_name,
        object_name=prefix + R6_COMPLETE_NAME,
        generation=R6_COMPLETE_GENERATION,
        expected_generation=R6_COMPLETE_GENERATION,
        expected_size=R6_COMPLETE_SIZE_BYTES,
        deadline=deadline,
        clock=clock,
        mismatch_reason="COMPLETE_METADATA_MISMATCH",
        unavailable_reason="COMPLETE_METADATA_UNAVAILABLE",
    )
    body = _download_bytes(
        client,
        bucket_name=bucket_name,
        object_name=prefix + R6_COMPLETE_NAME,
        generation=generation,
        size=size,
        max_size=R6_COMPLETE_SIZE_BYTES,
        deadline=deadline,
        clock=clock,
        oversize_reason="COMPLETE_OVERSIZE",
        fail_reason="COMPLETE_DOWNLOAD_FAILED",
    )
    complete_digest = _digest(body)
    if complete_digest != R6_COMPLETE_SHA256:
        raise MetadataError("COMPLETE_HASH_MISMATCH")
    payload = _parse_complete_json(body)
    identity = _validate_complete_identity(payload)
    if identity["license_evidence_sha256"] != R6_LICENSE_SHA256:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    if identity["observed_at"] not in {R6_COMPLETE_OBSERVED_AT, "2026-09-26T07:34:09+00:00"}:
        raise MetadataError("COMPLETE_IDENTITY_MISMATCH")
    receipts = _validate_p1_receipts(payload)
    _require_receipts_match_pins(receipts)

    bodies: dict[str, bytes] = {}
    # Manifest whole-bytes first; binding links are extracted only after that hash.
    for key in ("manifest.json", "binding.json", "closes.json", "assurance.json"):
        pin = P1_PINS[key]
        object_name = prefix + P1_NAME_MAP[key]
        observed_generation, observed_size = _reload_metadata(
            client,
            bucket_name=bucket_name,
            object_name=object_name,
            generation=int(pin["generation"]),
            expected_generation=int(pin["generation"]),
            expected_size=int(pin["size_bytes"]),
            deadline=deadline,
            clock=clock,
            mismatch_reason="P1_METADATA_MISMATCH",
            unavailable_reason="P1_METADATA_UNAVAILABLE",
        )
        member = _download_bytes(
            client,
            bucket_name=bucket_name,
            object_name=object_name,
            generation=observed_generation,
            size=observed_size,
            max_size=int(pin["size_bytes"]),
            deadline=deadline,
            clock=clock,
            oversize_reason="P1_OVERSIZE",
            fail_reason="P1_DOWNLOAD_FAILED",
        )
        digest = _digest(member)
        if digest != pin["sha256"]:
            raise MetadataError("P1_HASH_MISMATCH")
        bodies[key] = member
        if key == "manifest.json":
            try:
                manifest = json.loads(
                    member.decode("utf-8"),
                    object_pairs_hook=_object_pairs_hook,
                    parse_constant=lambda _c: (_ for _ in ()).throw(
                        MetadataError("MANIFEST_JSON_INVALID")
                    ),
                )
            except MetadataError:
                raise
            except Exception as exc:  # noqa: BLE001
                raise MetadataError("MANIFEST_JSON_INVALID") from exc
            if not isinstance(manifest, dict):
                raise MetadataError("MANIFEST_JSON_INVALID")
            _validate_manifest_binding_links(manifest)

    if _digest(bodies["binding.json"]) != BINDING_SHA256:
        raise MetadataError("P1_HASH_MISMATCH")
    _remaining(deadline, clock)
    return {
        "status": "INTEGRITY_MATCHED",
        "content_hash_verified": True,
        "completion_identity_authenticated": False,
        "complete": {
            "generation": generation,
            "size_bytes": size,
            "sha256": complete_digest,
            "p1_manifest_sha256": P1_MANIFEST_SHA256,
            "license_evidence_sha256": R6_LICENSE_SHA256,
            "observed_at": identity["observed_at"],
            "historical_point_in_time_certified": False,
        },
        "p1": {
            key: {
                "generation": int(P1_PINS[key]["generation"]),
                "size_bytes": int(P1_PINS[key]["size_bytes"]),
                "sha256": str(P1_PINS[key]["sha256"]),
                "content_hash_verified": True,
                "body_read": True,
            }
            for key in P1_KEYS
        },
    }


def read_contracts_metadata_group(
    client: Any,
    *,
    deadline: float,
    clock: Callable[[], float],
) -> dict[str, object]:
    contracts: dict[str, object] = {}
    for name, expected_sha in CONTRACT_SPECS:
        object_name = RAW_PREFIX + name
        try:
            generation, size = _reload_metadata(
                client,
                bucket_name=RAW_BUCKET,
                object_name=object_name,
                generation=None,
                expected_generation=None,
                expected_size=None,
                deadline=deadline,
                clock=clock,
                mismatch_reason="CONTRACT_METADATA_MISMATCH",
                unavailable_reason="CONTRACT_METADATA_UNAVAILABLE",
            )
        except MetadataError:
            raise
        if size > P1_MAX_BYTES:
            raise MetadataError("CONTRACT_OVERSIZE")
        contracts[name] = {
            "location_present": True,
            "generation": generation,
            "size_bytes": size,
            "expected_sha256": expected_sha,
            "expected_sha_status": "unverified",
            "body_read": False,
            "content_hash_verified": False,
        }
    _remaining(deadline, clock)
    return {"status": "METADATA_MATCHED", "contracts": contracts}


def read_contracts_integrity_group(
    client: Any,
    *,
    deadline: float,
    clock: Callable[[], float],
) -> dict[str, object]:
    contracts: dict[str, object] = {}
    for name, pin in CONTRACT_PINS.items():
        object_name = RAW_PREFIX + name
        pinned_generation = int(pin["generation"])
        pinned_size = int(pin["size_bytes"])
        pinned_sha = str(pin["sha256"])
        generation, size = _reload_metadata(
            client,
            bucket_name=RAW_BUCKET,
            object_name=object_name,
            generation=pinned_generation,
            expected_generation=pinned_generation,
            expected_size=pinned_size,
            deadline=deadline,
            clock=clock,
            mismatch_reason="CONTRACT_METADATA_MISMATCH",
            unavailable_reason="CONTRACT_METADATA_UNAVAILABLE",
        )
        body = _download_bytes(
            client,
            bucket_name=RAW_BUCKET,
            object_name=object_name,
            generation=generation,
            size=size,
            max_size=pinned_size,
            deadline=deadline,
            clock=clock,
            oversize_reason="CONTRACT_OVERSIZE",
            fail_reason="CONTRACT_DOWNLOAD_FAILED",
        )
        digest = _digest(body)
        if digest != pinned_sha:
            raise MetadataError("CONTRACT_HASH_MISMATCH")
        _remaining(deadline, clock)
        contracts[name] = {
            "name": name,
            "generation": generation,
            "size_bytes": size,
            "expected_sha256": pinned_sha,
            "observed_sha256": digest,
            "body_read": True,
            "content_hash_verified": True,
        }
    _remaining(deadline, clock)
    return contracts


def _lazy_storage_client() -> Any:
    # Force the exact SDK knob before imports/ctor. Caller "false" must not re-enable
    # background bucket metadata GETs; getenv is consulted dynamically per span.
    os.environ[DISABLE_OTEL_BUCKET_METADATA_ENV] = "true"
    # Lazy imports keep ADC/client construction behind the Actions identity gate.
    import google.auth
    from google.auth.transport.requests import AuthorizedSession
    from google.cloud import storage

    credentials, project = google.auth.default(scopes=(STORAGE_READONLY_SCOPE,))
    session = AuthorizedSession(credentials, max_refresh_attempts=0)
    return storage.Client(project=project, credentials=credentials, _http=session)


def _base_result() -> dict[str, object]:
    return {
        "status": "BLOCKED",
        "research_qualification": False,
        "trading_rights": False,
        "license_verified": False,
        "historical_point_in_time_certified": False,
        "completion_identity_authenticated": False,
        "content_verified": False,
        "content_integrity_verified": False,
    }


def run_metadata_read(
    *,
    client: Any | None = None,
    clock: Callable[[], float] | None = None,
    root: str | None = None,
    budget_s: float = OVERALL_BUDGET_S,
    create_client: Callable[[], Any] | None = None,
) -> tuple[int, dict[str, object]]:
    started = (clock or time.monotonic)()
    deadline = started + budget_s
    mono = clock or time.monotonic
    result: dict[str, object] = {
        **_base_result(),
        "raw": None,
        "r6": None,
    }
    try:
        _require_actions_identity()
        if client is None:
            factory = create_client or _lazy_storage_client
            client = factory()
    except MetadataError as exc:
        result["reason_class"] = exc.reason_class
        return 2, result
    except Exception:  # noqa: BLE001
        result["reason_class"] = "CLIENT_UNAVAILABLE"
        return 2, result

    root_uri = root if root is not None else os.environ.get(ROOT_ENV, "")

    raw_error: str | None = None
    r6_error: str | None = None
    try:
        _remaining(deadline, mono)
        result["raw"] = read_raw_group(client, deadline=deadline, clock=mono)
        _remaining(deadline, mono)
    except MetadataError as exc:
        raw_error = exc.reason_class
        result["raw"] = {"status": "BLOCKED", "reason_class": raw_error}
    except Exception:  # noqa: BLE001
        raw_error = "RAW_METADATA_UNAVAILABLE"
        result["raw"] = {"status": "BLOCKED", "reason_class": raw_error}

    try:
        _remaining(deadline, mono)
        if not isinstance(root_uri, str) or not root_uri:
            raise MetadataError("ROOT_MISMATCH")
        # Reject malformed root before any private R6 object access.
        _parse_root(root_uri)
        result["r6"] = read_r6_group(
            client,
            root_uri=root_uri,
            deadline=deadline,
            clock=mono,
        )
        _remaining(deadline, mono)
    except MetadataError as exc:
        r6_error = exc.reason_class
        result["r6"] = {"status": "BLOCKED", "reason_class": r6_error}
    except Exception:  # noqa: BLE001
        r6_error = "COMPLETE_METADATA_UNAVAILABLE"
        result["r6"] = {"status": "BLOCKED", "reason_class": r6_error}

    if raw_error is None and r6_error is None:
        try:
            _remaining(deadline, mono)
        except MetadataError as exc:
            result["status"] = "BLOCKED"
            result["reason_class"] = exc.reason_class
            return 2, result
        result["status"] = "METADATA_READY"
        return 0, result
    result["status"] = "BLOCKED"
    if raw_error and r6_error:
        result["reason_class"] = "GROUP_MISMATCH"
    else:
        result["reason_class"] = raw_error or r6_error
    return 2, result


def run_integrity_read(
    *,
    client: Any | None = None,
    clock: Callable[[], float] | None = None,
    root: str | None = None,
    budget_s: float = OVERALL_BUDGET_S,
    create_client: Callable[[], Any] | None = None,
) -> tuple[int, dict[str, object]]:
    started = (clock or time.monotonic)()
    deadline = started + budget_s
    mono = clock or time.monotonic
    result: dict[str, object] = {
        **_base_result(),
        "raw": None,
        "r6": None,
        "contracts": None,
    }
    try:
        _require_actions_identity()
        if client is None:
            factory = create_client or _lazy_storage_client
            client = factory()
    except MetadataError as exc:
        result["reason_class"] = exc.reason_class
        return 2, result
    except Exception:  # noqa: BLE001
        result["reason_class"] = "CLIENT_UNAVAILABLE"
        return 2, result

    root_uri = root if root is not None else os.environ.get(ROOT_ENV, "")
    raw_error: str | None = None
    r6_error: str | None = None
    contracts_error: str | None = None

    try:
        _remaining(deadline, mono)
        result["raw"] = read_raw_integrity_group(client, deadline=deadline, clock=mono)
        _remaining(deadline, mono)
    except MetadataError as exc:
        raw_error = exc.reason_class
        result["raw"] = {"status": "BLOCKED", "reason_class": raw_error}
    except Exception:  # noqa: BLE001
        raw_error = "RAW_METADATA_UNAVAILABLE"
        result["raw"] = {"status": "BLOCKED", "reason_class": raw_error}

    try:
        _remaining(deadline, mono)
        if not isinstance(root_uri, str) or not root_uri:
            raise MetadataError("ROOT_MISMATCH")
        _parse_root(root_uri)
        result["r6"] = read_r6_integrity_group(
            client,
            root_uri=root_uri,
            deadline=deadline,
            clock=mono,
        )
        _remaining(deadline, mono)
    except MetadataError as exc:
        r6_error = exc.reason_class
        result["r6"] = {"status": "BLOCKED", "reason_class": r6_error}
    except Exception:  # noqa: BLE001
        r6_error = "COMPLETE_METADATA_UNAVAILABLE"
        result["r6"] = {"status": "BLOCKED", "reason_class": r6_error}

    try:
        _remaining(deadline, mono)
        result["contracts"] = read_contracts_metadata_group(
            client, deadline=deadline, clock=mono
        )
        _remaining(deadline, mono)
    except MetadataError as exc:
        contracts_error = exc.reason_class
        result["contracts"] = {"status": "BLOCKED", "reason_class": contracts_error}
    except Exception:  # noqa: BLE001
        contracts_error = "CONTRACT_METADATA_UNAVAILABLE"
        result["contracts"] = {"status": "BLOCKED", "reason_class": contracts_error}

    if raw_error is None and r6_error is None and contracts_error is None:
        try:
            _remaining(deadline, mono)
        except MetadataError as exc:
            result["status"] = "BLOCKED"
            result["reason_class"] = exc.reason_class
            return 2, result
        result["status"] = "INTEGRITY_READY"
        result["content_integrity_verified"] = True
        return 0, result

    result["status"] = "BLOCKED"
    # Incomplete contracts keep overall BLOCKED even when RAW/P1 integrity matched.
    errors = [code for code in (raw_error, r6_error, contracts_error) if code]
    if len(errors) > 1:
        result["reason_class"] = "GROUP_MISMATCH"
    else:
        result["reason_class"] = errors[0]
    if raw_error is None and r6_error is None:
        result["content_integrity_verified"] = False
    return 2, result


def run_contracts_read(
    *,
    client: Any | None = None,
    clock: Callable[[], float] | None = None,
    budget_s: float = OVERALL_BUDGET_S,
    create_client: Callable[[], Any] | None = None,
) -> tuple[int, dict[str, object]]:
    started = (clock or time.monotonic)()
    deadline = started + budget_s
    mono = clock or time.monotonic
    result: dict[str, object] = {
        **_base_result(),
        "contracts_integrity_verified": False,
        "contracts": None,
    }
    try:
        _require_actions_identity()
        if client is None:
            factory = create_client or _lazy_storage_client
            client = factory()
    except MetadataError as exc:
        result["reason_class"] = exc.reason_class
        return 2, result
    except Exception:  # noqa: BLE001
        result["reason_class"] = "CLIENT_UNAVAILABLE"
        return 2, result

    try:
        _remaining(deadline, mono)
        # Fixed RAW bucket/prefix pins only; no R6 root, RAW manifest, or P1 access.
        result["contracts"] = read_contracts_integrity_group(
            client, deadline=deadline, clock=mono
        )
        _remaining(deadline, mono)
    except MetadataError as exc:
        result["status"] = "BLOCKED"
        result["reason_class"] = exc.reason_class
        return 2, result
    except Exception:  # noqa: BLE001
        result["status"] = "BLOCKED"
        result["reason_class"] = "CONTRACT_METADATA_UNAVAILABLE"
        return 2, result

    result["status"] = "CONTRACTS_INTEGRITY_READY"
    result["contracts_integrity_verified"] = True
    return 0, result


def _direct_url_commit(dist_name: str) -> str | None:
    from importlib.metadata import PackageNotFoundError, distribution

    try:
        dist = distribution(dist_name)
    except PackageNotFoundError:
        return None
    try:
        raw = dist.read_text("direct_url.json")
    except Exception:  # noqa: BLE001
        return None
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    vcs = payload.get("vcs_info")
    if not isinstance(vcs, dict):
        return None
    commit = vcs.get("commit_id")
    return commit if isinstance(commit, str) else None


def _package_version(dist_name: str) -> str | None:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version(dist_name)
    except PackageNotFoundError:
        return None


def _uesp_noneditable_under_source(source_dir: Path) -> bool:
    from importlib.metadata import PackageNotFoundError, distribution

    try:
        dist = distribution("us-equity-snapshot-pipelines")
    except PackageNotFoundError:
        return False
    try:
        raw = dist.read_text("direct_url.json")
    except Exception:  # noqa: BLE001
        return False
    if not raw:
        return False
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return False
    if payload.get("dir_info", {}).get("editable") is True:
        return False
    url = payload.get("url")
    if not isinstance(url, str):
        return False
    try:
        source_resolved = source_dir.resolve()
    except OSError:
        return False
    # Boolean path containment only; never emit path/URL text.
    if url.startswith("file:"):
        parsed = urlparse(url)
        candidate = Path(parsed.path)
        try:
            return source_resolved in candidate.resolve().parents or candidate.resolve() == source_resolved
        except OSError:
            return False
    return False


def preflight_materialized_runtime(
    *,
    source_dir: str | None = None,
    source_commit: str | None = None,
) -> dict[str, object]:
    src = Path(source_dir or os.environ.get(SOURCE_DIR_ENV, ""))
    commit = source_commit if source_commit is not None else os.environ.get(SOURCE_COMMIT_ENV, "")
    if commit != LEGACY_SOURCE_COMMIT:
        raise MetadataError("RUNTIME_SOURCE_MISMATCH")
    lock_path = src / "uv.lock"
    try:
        lock_bytes = lock_path.read_bytes()
    except OSError as exc:
        raise MetadataError("RUNTIME_LOCK_UNAVAILABLE") from exc
    if _digest(lock_bytes) != LEGACY_UV_LOCK_SHA256:
        raise MetadataError("RUNTIME_LOCK_MISMATCH")
    if not sys.version.startswith("3.11"):
        raise MetadataError("RUNTIME_PYTHON_MISMATCH")
    version_ok = True
    for name, expected in LEGACY_VERSION_PINS.items():
        if _package_version(name) != expected:
            version_ok = False
            break
    origin_ok = True
    for name, expected in LEGACY_ORIGIN_PINS.items():
        if _direct_url_commit(name) != expected:
            origin_ok = False
            break
    uesp_ok = _uesp_noneditable_under_source(src)
    if not version_ok:
        raise MetadataError("RUNTIME_VERSION_MISMATCH")
    if not origin_ok:
        raise MetadataError("RUNTIME_ORIGIN_MISMATCH")
    if not uesp_ok:
        raise MetadataError("RUNTIME_UESP_ORIGIN_MISMATCH")
    return {
        "source_commit_matched": True,
        "lock_digest_matched": True,
        "python_version_matched": True,
        "dependency_versions_matched": True,
        "dependency_origins_matched": True,
        "uesp_noneditable_source_matched": True,
    }


def read_p1_materialize_group(
    client: Any,
    *,
    root_uri: str,
    deadline: float,
    clock: Callable[[], float],
) -> dict[str, bytes]:
    bucket_name, prefix = _parse_root(root_uri)
    bodies: dict[str, bytes] = {}
    for key in ("manifest.json", "binding.json", "closes.json", "assurance.json"):
        pin = P1_PINS[key]
        object_name = prefix + P1_NAME_MAP[key]
        generation, size = _reload_metadata(
            client,
            bucket_name=bucket_name,
            object_name=object_name,
            generation=int(pin["generation"]),
            expected_generation=int(pin["generation"]),
            expected_size=int(pin["size_bytes"]),
            deadline=deadline,
            clock=clock,
            mismatch_reason="P1_METADATA_MISMATCH",
            unavailable_reason="P1_METADATA_UNAVAILABLE",
        )
        member = _download_bytes(
            client,
            bucket_name=bucket_name,
            object_name=object_name,
            generation=generation,
            size=size,
            max_size=int(pin["size_bytes"]),
            deadline=deadline,
            clock=clock,
            oversize_reason="P1_OVERSIZE",
            fail_reason="P1_DOWNLOAD_FAILED",
        )
        digest = _digest(member)
        if digest != pin["sha256"]:
            raise MetadataError("P1_HASH_MISMATCH")
        bodies[key] = member
        if key == "manifest.json":
            try:
                manifest = json.loads(
                    member.decode("utf-8"),
                    object_pairs_hook=_object_pairs_hook,
                    parse_constant=lambda _c: (_ for _ in ()).throw(
                        MetadataError("MANIFEST_JSON_INVALID")
                    ),
                )
            except MetadataError:
                raise
            except Exception as exc:  # noqa: BLE001
                raise MetadataError("MANIFEST_JSON_INVALID") from exc
            if not isinstance(manifest, dict):
                raise MetadataError("MANIFEST_JSON_INVALID")
            _validate_manifest_binding_links(manifest)
    if _digest(bodies["binding.json"]) != BINDING_SHA256:
        raise MetadataError("P1_HASH_MISMATCH")
    _remaining(deadline, clock)
    return bodies


def _require_task_root(task_root: Path) -> None:
    if task_root.is_symlink() or not task_root.is_dir():
        raise MetadataError("TASK_ROOT_INVALID")
    if task_root.stat().st_mode & 0o777 != 0o700:
        raise MetadataError("TASK_ROOT_INVALID")


def _cleanup_dir(path: Path | None) -> bool:
    if path is None:
        return True
    if not path.exists():
        return True
    ok = True
    for child in sorted(path.rglob("*"), reverse=True):
        try:
            if child.is_symlink() or child.is_file():
                child.unlink()
            elif child.is_dir():
                child.rmdir()
        except OSError:
            ok = False
    try:
        if path.exists():
            path.rmdir()
    except OSError:
        ok = False
    return ok and not path.exists()


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise OSError("short write")
        view = view[written:]


def _write_p1_input_dir(task_root: Path, bodies: dict[str, bytes]) -> Path:
    _require_task_root(task_root)
    input_dir = task_root / "input"
    if input_dir.exists() or input_dir.is_symlink():
        raise MetadataError("INPUT_DIR_EXISTS")
    input_dir.mkdir(mode=0o700)
    try:
        for name in P1_KEYS:
            path = input_dir / name
            fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                _write_all(fd, bodies[name])
            finally:
                os.close(fd)
            if path.stat().st_mode & 0o777 != 0o600:
                raise MetadataError("INPUT_DIR_INVALID")
        if {path.name for path in input_dir.iterdir()} != set(P1_KEYS):
            raise MetadataError("INPUT_DIR_INVALID")
        if input_dir.stat().st_mode & 0o777 != 0o700:
            raise MetadataError("INPUT_DIR_INVALID")
        return input_dir
    except MetadataError:
        if not _cleanup_dir(input_dir):
            raise MetadataError("INPUT_CLEANUP_FAILED")
        raise
    except Exception as exc:  # noqa: BLE001
        if not _cleanup_dir(input_dir):
            raise MetadataError("INPUT_CLEANUP_FAILED") from exc
        raise MetadataError("INPUT_DIR_INVALID") from exc


def _worker_env(*, cache_dir: Path) -> dict[str, str]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    mode = cache_dir.stat().st_mode & 0o777
    if mode != 0o700:
        cache_dir.chmod(0o700)
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "LANG": os.environ.get("LANG", "C.UTF-8"),
        "LC_ALL": os.environ.get("LC_ALL", "C.UTF-8"),
        "TMPDIR": str(cache_dir),
        "XDG_CACHE_HOME": str(cache_dir),
        "MPLCONFIGDIR": str(cache_dir),
        "PYTHONDONTWRITEBYTECODE": "1",
    }


def process_group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Ambiguous: fail closed so cleanup must keep polling / escalate.
        return True
    return True


def _close_proc_pipes(proc: subprocess.Popen[Any]) -> None:
    for stream in (proc.stdout, proc.stderr):
        if stream is not None:
            try:
                stream.close()
            except Exception:  # noqa: BLE001
                pass


def terminate_process_group(
    proc: subprocess.Popen[Any],
    *,
    pgid: int | None = None,
    budget_s: float = WORKER_CLEANUP_BUDGET_S,
) -> bool:
    """TERM→KILL own start_new_session group; True only after bounded absence."""
    resolved = pgid
    if resolved is None:
        try:
            resolved = os.getpgid(proc.pid)
        except ProcessLookupError:
            resolved = None
    deadline = time.monotonic() + max(0.0, float(budget_s))

    def _signal_group(sig: int) -> None:
        if resolved is None:
            return
        try:
            os.killpg(resolved, sig)
        except (ProcessLookupError, PermissionError):
            pass

    _signal_group(signal.SIGTERM)
    # Leader wait alone is not group cleanup; only bound the leader reap attempt.
    try:
        proc.wait(timeout=min(0.2, max(0.0, deadline - time.monotonic())))
    except subprocess.TimeoutExpired:
        pass
    _signal_group(signal.SIGKILL)

    while True:
        leader_reaped = proc.poll() is not None
        group_absent = resolved is None or not process_group_alive(resolved)
        if leader_reaped and group_absent:
            # Re-check briefly: SIGKILL delivery can leave a PID visible for ms.
            stable = True
            for _ in range(3):
                if time.monotonic() >= deadline:
                    stable = False
                    break
                time.sleep(0.01)
                if resolved is not None and process_group_alive(resolved):
                    stable = False
                    break
                if proc.poll() is None:
                    stable = False
                    break
            if stable:
                try:
                    proc.wait(timeout=0.05)
                except subprocess.TimeoutExpired:
                    pass
                _close_proc_pipes(proc)
                return True
        if time.monotonic() >= deadline:
            break
        _signal_group(signal.SIGKILL)
        time.sleep(0.01)

    try:
        proc.wait(timeout=0.05)
    except subprocess.TimeoutExpired:
        pass
    _close_proc_pipes(proc)
    leader_reaped = proc.poll() is not None
    group_absent = resolved is None or not process_group_alive(resolved)
    return leader_reaped and group_absent


def _blocked_worker_result(reason_class: str) -> dict[str, object]:
    return {
        "protocol": WORKER_PROTOCOL,
        "status": "BLOCKED",
        "reason_class": reason_class,
        "network_guard_attempts": 0,
        "network_guard_scope": WORKER_NETWORK_GUARD_SCOPE,
        "elapsed_import_s": 0.0,
        "elapsed_compute_s": 0.0,
        "attempt_count": 1,
    }


def _protocol_invalid() -> dict[str, object]:
    return _blocked_worker_result("WORKER_PROTOCOL_INVALID")


def _worker_nonneg_number(value: object, *, max_value: float) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    if math.isnan(number) or math.isinf(number) or number < 0 or number > max_value:
        return None
    return number


def _worker_nonneg_int(value: object, *, max_value: int) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    if value < 0 or value > max_value:
        return None
    return value


def validate_worker_payload(
    payload: object, *, returncode: int
) -> tuple[int, dict[str, object]]:
    invalid = _protocol_invalid()
    if not isinstance(payload, dict):
        return 2, invalid
    status = payload.get("status")
    if payload.get("protocol") != WORKER_PROTOCOL or status not in {"OK", "BLOCKED"}:
        return 2, invalid
    if status == "OK":
        if set(payload) != WORKER_OK_KEYS:
            return 2, invalid
        if returncode != 0:
            return 2, invalid
        attempts = _worker_nonneg_int(payload.get("network_guard_attempts"), max_value=0)
        attempt_count = _worker_nonneg_int(payload.get("attempt_count"), max_value=1)
        import_s = _worker_nonneg_number(
            payload.get("elapsed_import_s"), max_value=MATERIALIZED_WORKER_BUDGET_S
        )
        compute_s = _worker_nonneg_number(
            payload.get("elapsed_compute_s"), max_value=MATERIALIZED_WORKER_BUDGET_S
        )
        session_count = _worker_nonneg_int(
            payload.get("session_count"), max_value=EXPECTED_SESSION_COUNT
        )
        digests = (
            payload.get("v7_config_sha256"),
            payload.get("p1_binding_sha256"),
            payload.get("p1_manifest_sha256"),
            payload.get("candidate_whole_sha256"),
            payload.get("internal_materialized_sha256"),
        )
        if (
            attempts != 0
            or attempt_count != 1
            or import_s is None
            or compute_s is None
            or session_count != EXPECTED_SESSION_COUNT
            or payload.get("network_guard_scope") != WORKER_NETWORK_GUARD_SCOPE
            or payload.get("internal_verified") is not True
            or any(not isinstance(item, str) or _HEX64.fullmatch(item) is None for item in digests)
            or payload.get("v7_config_sha256") != EXPECTED_V7_CONFIG_SHA256
            or payload.get("p1_binding_sha256") != BINDING_SHA256
            or payload.get("p1_manifest_sha256") != P1_MANIFEST_SHA256
        ):
            return 2, invalid
        if import_s + compute_s > MATERIALIZED_WORKER_BUDGET_S:
            return 2, invalid
        return 0, {
            "protocol": WORKER_PROTOCOL,
            "status": "OK",
            "session_count": EXPECTED_SESSION_COUNT,
            "v7_config_sha256": EXPECTED_V7_CONFIG_SHA256,
            "p1_binding_sha256": BINDING_SHA256,
            "p1_manifest_sha256": P1_MANIFEST_SHA256,
            "candidate_whole_sha256": str(payload["candidate_whole_sha256"]),
            "internal_materialized_sha256": str(payload["internal_materialized_sha256"]),
            "internal_verified": True,
            "network_guard_attempts": 0,
            "network_guard_scope": WORKER_NETWORK_GUARD_SCOPE,
            "elapsed_import_s": import_s,
            "elapsed_compute_s": compute_s,
            "attempt_count": 1,
        }

    if set(payload) - WORKER_BLOCKED_KEYS:
        return 2, invalid
    if not WORKER_BLOCKED_KEYS <= set(payload):
        return 2, invalid
    reason = payload.get("reason_class")
    if reason not in WORKER_BLOCKED_REASONS or returncode == 0:
        return 2, invalid
    attempts = _worker_nonneg_int(payload.get("network_guard_attempts"), max_value=10_000)
    attempt_count = _worker_nonneg_int(payload.get("attempt_count"), max_value=1)
    import_s = _worker_nonneg_number(
        payload.get("elapsed_import_s"), max_value=MATERIALIZED_WORKER_BUDGET_S
    )
    compute_s = _worker_nonneg_number(
        payload.get("elapsed_compute_s"), max_value=MATERIALIZED_WORKER_BUDGET_S
    )
    if (
        attempts is None
        or attempt_count != 1
        or import_s is None
        or compute_s is None
        or payload.get("network_guard_scope") != WORKER_NETWORK_GUARD_SCOPE
    ):
        return 2, invalid
    return 2, {
        "protocol": WORKER_PROTOCOL,
        "status": "BLOCKED",
        "reason_class": reason,
        "network_guard_attempts": attempts,
        "network_guard_scope": WORKER_NETWORK_GUARD_SCOPE,
        "elapsed_import_s": import_s,
        "elapsed_compute_s": compute_s,
        "attempt_count": 1,
    }


def _parse_worker_stdout(stdout: str, *, returncode: int) -> tuple[int, dict[str, object]]:
    lines = [line for line in (stdout or "").splitlines() if line.strip()]
    # Exactly one protocol line; any extra stdout is not ignored as success.
    if len(lines) != 1:
        return 2, _protocol_invalid()
    try:
        payload = json.loads(lines[0])
    except json.JSONDecodeError:
        return 2, _protocol_invalid()
    return validate_worker_payload(payload, returncode=returncode)


def spawn_materialized_worker(
    *,
    python_executable: str,
    input_dir: Path,
    cache_dir: Path,
    timeout_s: float = MATERIALIZED_WORKER_BUDGET_S,
    cleanup_budget_s: float = WORKER_CLEANUP_BUDGET_S,
    worker_source: str | None = None,
    proc_holder: dict[str, Any] | None = None,
) -> tuple[int, dict[str, object]]:
    env = _worker_env(cache_dir=cache_dir)
    for key in list(env):
        if key in {"HOME", "CODEX_HOME", "PYTHONPATH", "VIRTUAL_ENV", ROOT_ENV}:
            raise MetadataError("WORKER_ENV_INVALID")
    if any(
        name in env
        for name in (
            "GOOGLE_APPLICATION_CREDENTIALS",
            "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE",
            "TWELVE_DATA_API_KEY",
        )
    ):
        raise MetadataError("WORKER_ENV_INVALID")
    source = worker_source if worker_source is not None else _MATERIALIZED_WORKER_SOURCE
    proc = subprocess.Popen(
        [python_executable, "-I", "-c", source, str(input_dir)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        start_new_session=True,
        text=True,
    )
    try:
        pgid = os.getpgid(proc.pid)
    except ProcessLookupError:
        pgid = None
    if proc_holder is not None:
        proc_holder["proc"] = proc
        proc_holder["pgid"] = pgid
    timed_out = False
    cleanup_ok = True
    stdout = ""
    try:
        try:
            stdout, _stderr = proc.communicate(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            timed_out = True
            cleanup_ok = terminate_process_group(
                proc, pgid=pgid, budget_s=cleanup_budget_s
            )
            try:
                stdout, _stderr = proc.communicate(timeout=min(5.0, cleanup_budget_s))
            except subprocess.TimeoutExpired:
                cleanup_ok = False
                _close_proc_pipes(proc)
        # Leader may exit while TERM-ignoring descendants remain in the group.
        if cleanup_ok and pgid is not None and process_group_alive(pgid):
            cleanup_ok = terminate_process_group(
                proc, pgid=pgid, budget_s=cleanup_budget_s
            )
            try:
                if proc.poll() is not None and not (stdout or ""):
                    # Drain pipes after late group cleanup without unbounded wait.
                    stdout, _stderr = proc.communicate(timeout=min(1.0, cleanup_budget_s))
            except Exception:  # noqa: BLE001
                _close_proc_pipes(proc)
    except Exception:  # noqa: BLE001
        cleanup_ok = (
            terminate_process_group(proc, pgid=pgid, budget_s=cleanup_budget_s)
            and cleanup_ok
        )
        raise
    finally:
        if pgid is not None and process_group_alive(pgid):
            cleanup_ok = (
                terminate_process_group(proc, pgid=pgid, budget_s=cleanup_budget_s)
                and cleanup_ok
            )
        # Clear holder only after confirmed group absence; else outer finally remediates.
        if cleanup_ok and proc_holder is not None:
            proc_holder["proc"] = None
            proc_holder["pgid"] = None
    if not cleanup_ok:
        return 2, _blocked_worker_result("WORKER_CLEANUP_FAILED")
    if timed_out:
        return 2, _blocked_worker_result("WORKER_TIMEOUT")
    return _parse_worker_stdout(
        stdout or "",
        returncode=int(proc.returncode if proc.returncode is not None else 2),
    )


def run_materialized_identity_read(
    *,
    client: Any | None = None,
    clock: Callable[[], float] | None = None,
    root: str | None = None,
    budget_s: float = MATERIALIZED_ENTRY_BUDGET_S,
    read_budget_s: float = OVERALL_BUDGET_S,
    worker_budget_s: float = MATERIALIZED_WORKER_BUDGET_S,
    create_client: Callable[[], Any] | None = None,
    spawn_worker: Callable[..., tuple[int, dict[str, object]]] | None = None,
    preflight: Callable[[], dict[str, object]] | None = None,
    task_root: str | None = None,
) -> tuple[int, dict[str, object]]:
    started = (clock or time.monotonic)()
    deadline = started + budget_s
    mono = clock or time.monotonic
    result: dict[str, object] = {
        **_base_result(),
        "materialized_identity_matched": False,
        "runtime": None,
        "p1": None,
        "candidate": None,
        "worker": None,
    }
    input_dir: Path | None = None
    cleanup_ok = True
    worker_proc_holder: dict[str, Any] = {"proc": None, "pgid": None}

    def _on_term(_signum: int, _frame: object) -> None:
        proc = worker_proc_holder.get("proc")
        pgid = worker_proc_holder.get("pgid")
        if isinstance(proc, subprocess.Popen):
            terminate_process_group(
                proc, pgid=pgid if isinstance(pgid, int) else None
            )
        raise MetadataError("ENTRY_TERMINATED")

    previous_term = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, _on_term)
    try:
        try:
            _require_actions_identity()
            runtime = (preflight or preflight_materialized_runtime)()
            result["runtime"] = runtime
            root_uri = root if root is not None else os.environ.get(ROOT_ENV, "")
            task = Path(task_root or os.environ.get(TASK_ROOT_ENV, ""))
            if not isinstance(root_uri, str) or not root_uri:
                raise MetadataError("ROOT_MISMATCH")
            _parse_root(root_uri)
            _require_task_root(task)
            declared = os.environ.get(VENV_PYTHON_ENV)
            if declared is not None and declared != sys.executable:
                raise MetadataError("RUNTIME_PYTHON_MISMATCH")
            # Read budget starts before client construction.
            read_deadline = min(mono() + read_budget_s, deadline)
            _remaining(read_deadline, mono)
            if client is None:
                factory = create_client or _lazy_storage_client
                client = factory()
            _remaining(read_deadline, mono)
            bodies = read_p1_materialize_group(
                client,
                root_uri=root_uri,
                deadline=read_deadline,
                clock=mono,
            )
            result["p1"] = {
                key: {
                    "generation": int(P1_PINS[key]["generation"]),
                    "size_bytes": int(P1_PINS[key]["size_bytes"]),
                    "sha256": str(P1_PINS[key]["sha256"]),
                    "content_hash_verified": True,
                    "body_read": True,
                }
                for key in P1_KEYS
            }
            _remaining(deadline, mono)
            input_dir = _write_p1_input_dir(task, bodies)
            cache_dir = task / "worker-cache"
            cache_dir.mkdir(mode=0o700, exist_ok=True)
            _remaining(deadline, mono)
            worker_timeout = min(worker_budget_s, max(0.1, deadline - mono()))
            if spawn_worker is None:
                worker_code, raw_payload = spawn_materialized_worker(
                    python_executable=sys.executable,
                    input_dir=input_dir,
                    cache_dir=cache_dir,
                    timeout_s=worker_timeout,
                    proc_holder=worker_proc_holder,
                )
            else:
                worker_code, raw_payload = spawn_worker(
                    python_executable=sys.executable,
                    input_dir=input_dir,
                    cache_dir=cache_dir,
                    timeout_s=worker_timeout,
                )
            worker_code, worker_payload = validate_worker_payload(
                raw_payload, returncode=worker_code
            )
        except MetadataError as exc:
            result["reason_class"] = exc.reason_class
            return 2, result
        except Exception:  # noqa: BLE001
            result["reason_class"] = "MATERIALIZE_FAILED"
            return 2, result
        finally:
            if input_dir is not None:
                cleanup_ok = _cleanup_dir(input_dir)
                if not cleanup_ok and result.get("reason_class") is None:
                    # Defer final reason until after worker projection when needed.
                    pass

        result["worker"] = {
            "network_guard_scope": WORKER_NETWORK_GUARD_SCOPE,
            "network_guard_attempts": worker_payload.get("network_guard_attempts"),
            "elapsed_import_s": worker_payload.get("elapsed_import_s"),
            "elapsed_compute_s": worker_payload.get("elapsed_compute_s"),
            "attempt_count": 1,
        }
        if not cleanup_ok:
            result["reason_class"] = "INPUT_CLEANUP_FAILED"
            result["materialized_identity_matched"] = False
            return 2, result
        if worker_payload.get("status") != "OK" or worker_code != 0:
            reason = worker_payload.get("reason_class")
            result["reason_class"] = (
                reason if reason in WORKER_BLOCKED_REASONS else "WORKER_PROTOCOL_INVALID"
            )
            return 2, result
        if worker_payload.get("network_guard_attempts") != 0:
            result["reason_class"] = "NETWORK_ATTEMPT_REFUSED"
            return 2, result

        whole = worker_payload.get("candidate_whole_sha256")
        internal = worker_payload.get("internal_materialized_sha256")
        match = (
            whole == EXPECTED_MATERIALIZED_WHOLE_SHA256
            and worker_payload.get("internal_verified") is True
            and isinstance(internal, str)
            and _HEX64.fullmatch(internal) is not None
            and worker_payload.get("session_count") == EXPECTED_SESSION_COUNT
            and worker_payload.get("v7_config_sha256") == EXPECTED_V7_CONFIG_SHA256
            and worker_payload.get("p1_binding_sha256") == BINDING_SHA256
            and worker_payload.get("p1_manifest_sha256") == P1_MANIFEST_SHA256
        )
        result["candidate"] = {
            "expected_whole_sha256": EXPECTED_MATERIALIZED_WHOLE_SHA256,
            "observed_whole_sha256": whole if isinstance(whole, str) else None,
            "whole_sha_matched": whole == EXPECTED_MATERIALIZED_WHOLE_SHA256,
            "internal_materialized_sha256": internal if isinstance(internal, str) else None,
            "internal_verified": worker_payload.get("internal_verified") is True,
            "session_count": EXPECTED_SESSION_COUNT,
            "session_count_matched": worker_payload.get("session_count") == EXPECTED_SESSION_COUNT,
            "v7_config_sha256": EXPECTED_V7_CONFIG_SHA256,
            "v7_config_matched": worker_payload.get("v7_config_sha256") == EXPECTED_V7_CONFIG_SHA256,
            "p1_binding_sha256": BINDING_SHA256,
            "p1_manifest_sha256": P1_MANIFEST_SHA256,
        }
        try:
            _remaining(deadline, mono)
        except MetadataError as exc:
            result["reason_class"] = exc.reason_class
            return 2, result
        if not match:
            result["reason_class"] = "HASH_MISMATCH"
            return 2, result
        result["status"] = "MATERIALIZED_IDENTITY_READY"
        result["materialized_identity_matched"] = True
        return 0, result
    finally:
        signal.signal(signal.SIGTERM, previous_term)
        proc = worker_proc_holder.get("proc")
        pgid = worker_proc_holder.get("pgid")
        if isinstance(proc, subprocess.Popen):
            terminate_process_group(
                proc, pgid=pgid if isinstance(pgid, int) else None
            )
        elif isinstance(pgid, int) and process_group_alive(pgid):
            try:
                os.killpg(pgid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        if input_dir is not None and input_dir.exists():
            if not _cleanup_dir(input_dir):
                result["status"] = "BLOCKED"
                result["materialized_identity_matched"] = False
                result["reason_class"] = "INPUT_CLEANUP_FAILED"


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if argv == ["--raw-manifest-projection-only"]:
        code, payload = run_raw_manifest_projection_read()
        print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        return code
    if argv == ["--materialized-identity-only"]:
        code, payload = run_materialized_identity_read()
        print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        return code
    if argv == ["--contracts-only"]:
        code, payload = run_contracts_read()
        print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        return code
    if argv == ["--integrity-only"]:
        code, payload = run_integrity_read()
        print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        return code
    if argv:
        # Root/URI must come from the protected env, never argv.
        payload = {
            **_base_result(),
            "reason_class": "ARGV_REFUSED",
        }
        print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        return 2
    code, payload = run_metadata_read()
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
