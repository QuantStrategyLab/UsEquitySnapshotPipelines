"""GET-only research archive metadata reader for original RAW + R6 recovery pins.

Single-purpose entry: reload fixed RAW manifest metadata and the R6 completion
index (bounded download) plus four P1 member metadata receipts. No provider
calls, listing, uploads, body dumps, or research qualification claims.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sys
import time
from datetime import datetime
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
RAW_GENERATION = 1790338090501279
RAW_SIZE_BYTES = 7580
RAW_EXPECTED_SHA256 = "cb14a511083c824a748d137a271c93cfe0e8adf38f648905b26e37decf4c6182"

R6_COMPLETE_NAME = "complete.json"
R6_COMPLETE_MAX_BYTES = 64 * 1024
P1_MAX_BYTES = 16 * 1024 * 1024
P1_KEYS = ("binding.json", "manifest.json", "closes.json", "assurance.json")
P1_NAME_MAP = {key: f"p1/{key}" for key in P1_KEYS}
P1_MANIFEST_SHA256 = "86fa48cba3459ad9ff228671cd5b5e574e0e68c1637eaa40d7487c541ff36795"

STUDY_ID = "soxl_v7_twelve_basic_split_close_development_v1"
COMPLETION_SCHEMA = "soxl-v7-r6-twelve-single-completion.v1"
SIGNAL_CANDIDATE_ID = "soxl_soxx_core_only_p2_v7_longterm_compounding_cash_reserve"
PRODUCER_REVISION = "0ae8ac4eb886431f9f9695702d9dd60982919dae"
DATE_CUTOFF = "2026-08-25"
SOURCE_ASSURANCE = "single_source_structural_only_no_cross_provider_verification"

_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_SHA_NAME = re.compile(r"^(?:\.\./|/|gs:|https?:)", re.IGNORECASE)


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
    if size > R6_COMPLETE_MAX_BYTES:
        raise MetadataError("COMPLETE_OVERSIZE")
    timeout = _remaining(deadline, clock)
    object_name = prefix + R6_COMPLETE_NAME
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
        raise MetadataError("COMPLETE_DOWNLOAD_FAILED") from exc
    _remaining(deadline, clock)
    if not isinstance(content, (bytes, bytearray)) or len(content) != size:
        raise MetadataError("COMPLETE_DOWNLOAD_FAILED")
    return bytes(content)


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
        "status": "BLOCKED",
        "research_qualification": False,
        "trading_rights": False,
        "content_verified": False,
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


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if argv:
        # Root/URI must come from the protected env, never argv.
        payload = {
            "status": "BLOCKED",
            "reason_class": "ARGV_REFUSED",
            "research_qualification": False,
            "trading_rights": False,
            "content_verified": False,
        }
        print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        return 2
    code, payload = run_metadata_read()
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
