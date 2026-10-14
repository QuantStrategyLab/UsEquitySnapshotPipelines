"""Cloud-only RS-04 metadata inventory and create-only raw SIP archive.

The content manifest is deterministic for identical provider responses. A
separate receipt records retrieval time and GCS generations. Neither proves
historical point-in-time availability or effective live configuration parity.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import platform
import re
import subprocess
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

LEGACY_DIGEST = "b5a3dd40cb0d4260fb15492108bde7ab544ac0d191807943637b8216a6808482"
SYMBOLS = ("SOXL", "SOXX", "BOXX", "QQQ", "TQQQ", "QQQM")
MAX_BYTES = 128 * 1024 * 1024
MAX_PAGE = 4 * 1024 * 1024
MAX_REQUESTS = 60
MAX_OBJECTS = 2000
ACTION_TYPES = frozenset({
    "capital_gains_distributions", "cash_dividends", "cash_mergers", "forward_splits",
    "name_changes", "partial_calls", "redemptions", "reorganizations", "reverse_splits",
    "rights_distributions", "spin_offs", "stock_and_cash_mergers", "stock_dividends",
    "stock_mergers", "unit_splits", "worthless_removals",
})


class ArchiveError(ValueError):
    pass


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def require_cloud() -> None:
    branch = os.environ.get("GITHUB_DEFAULT_BRANCH", "")
    if not (platform.system() == "Linux" and os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("RUNNER_OS") == "Linux" and branch
            and os.environ.get("GITHUB_REF") == "refs/heads/" + branch
            and os.environ.get("GITHUB_RUN_ATTEMPT") == "1"):
        raise ArchiveError("CLOUD_DEFAULT_BRANCH_FIRST_ATTEMPT_REQUIRED")


def input_root(value: str) -> str:
    if not re.fullmatch(r"gs://[a-z0-9][a-z0-9._-]{2,221}/research/v2/input/?", value):
        raise ArchiveError("RESEARCH_INPUT_ROOT_REQUIRED")
    return value.rstrip("/") + "/"


def object_uri(value: str, root: str) -> str:
    if not value.startswith(root) or not re.fullmatch(r"[A-Za-z0-9._/-]+", value[len(root):]):
        raise ArchiveError("OBJECT_OUTSIDE_CONFIGURED_INPUT_ROOT")
    if any(part in ("", ".", "..") for part in value[len(root):].split("/")):
        raise ArchiveError("OBJECT_PATH_INVALID")
    return value


def inventory(root: str, command=subprocess.run) -> dict:
    """Metadata-only gcloud listing; auth's exported WIF is unsupported by gsutil."""
    proc = command(["gcloud", "storage", "ls", "--json", "--all-versions", root + "**", "--quiet"],
                   capture_output=True, text=True, timeout=90, check=False)
    if proc.returncode:
        if re.search(r"\b403\b|PERMISSION_DENIED", proc.stderr):
            raise ArchiveError("METADATA_LIST_FORBIDDEN")
        if re.search(r"\b401\b|UNAUTHENTICATED", proc.stderr):
            raise ArchiveError("METADATA_LIST_UNAUTHENTICATED")
        raise ArchiveError("METADATA_LIST_DENIED_OR_UNAVAILABLE")
    if len(proc.stdout) > 4 * 1024 * 1024:
        raise ArchiveError("METADATA_INVENTORY_LIMIT")
    try:
        records = json.loads(proc.stdout)
    except ValueError:
        raise ArchiveError("METADATA_LIST_SCHEMA_INVALID") from None
    if not isinstance(records, list) or len(records) > MAX_OBJECTS:
        raise ArchiveError("METADATA_INVENTORY_LIMIT")
    objects = []
    for record in records:
        if not isinstance(record, dict) or not isinstance(record.get("metadata"), dict):
            raise ArchiveError("METADATA_LIST_SCHEMA_INVALID")
        name = record.get("url", "").split("#", 1)[0]
        object_uri(name, root)
        meta = record["metadata"]
        generation, size = str(meta.get("generation", "")), str(meta.get("size", ""))
        if not generation.isdigit() or not size.isdigit():
            raise ArchiveError("METADATA_LIST_SCHEMA_INVALID")
        custom = meta.get("metadata", {})
        digest = custom.get("sha256") if isinstance(custom, dict) else None
        objects.append({"name": name[len(root):], "generation": generation, "bytes": size,
                        "sha256": digest if isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest) else None})
    matches = [o for o in objects if o["sha256"] == LEGACY_DIGEST]
    return {"status": "METADATA_INVENTORY_COMPLETE", "objects": objects,
            "legacy_digest": LEGACY_DIGEST, "legacy_metadata_matches": matches,
            "objects_without_sha256_metadata": sum(o["sha256"] is None for o in objects),
            "digest_absence_is_not_content_absence": True, "real_data_read": False}


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        return None


class Provider:
    def __init__(self, key: str, secret: str):
        if not key or not secret:
            raise ArchiveError("EXISTING_RESEARCH_CREDENTIALS_UNAVAILABLE")
        self.headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
        self.opener = build_opener(ProxyHandler({}), NoRedirect())
        self.requests = self.bytes = 0

    def get(self, path: str, params: dict) -> dict:
        if path not in {"/v1/corporate-actions", *(f"/v2/stocks/{s}/bars" for s in SYMBOLS)}:
            raise ArchiveError("PROVIDER_ENDPOINT_REJECTED")
        self.requests += 1
        if self.requests > MAX_REQUESTS or self.bytes >= MAX_BYTES:
            raise ArchiveError("PROVIDER_BUDGET_EXHAUSTED")
        request = Request("https://data.alpaca.markets" + path + "?" + urlencode(params),
                          headers=self.headers, method="GET")
        try:
            with self.opener.open(request, timeout=30) as response:
                body = response.read(min(MAX_PAGE, MAX_BYTES - self.bytes) + 1)
            self.bytes += len(body)
            if len(body) > MAX_PAGE or self.bytes > MAX_BYTES:
                raise ArchiveError("PROVIDER_BUDGET_EXHAUSTED")
            data = json.loads(body)
        except (HTTPError, URLError, OSError, ValueError):
            raise ArchiveError("PROVIDER_READ_FAILED_NO_RETRY") from None
        if not isinstance(data, dict):
            raise ArchiveError("PROVIDER_SCHEMA_INVALID")
        return data


class Store:
    def __init__(self, client, prefix: str):
        bucket, self.prefix = prefix[5:].split("/", 1)
        self.bucket = client.bucket(bucket)
        self.receipts: list[dict] = []

    def create(self, name: str, body: bytes) -> dict:
        blob = self.bucket.blob(self.prefix + name)
        digest = sha(body)
        blob.metadata = {"sha256": digest}
        try:
            blob.upload_from_string(body, content_type="application/json",
                                    if_generation_match=0, retry=None, timeout=30)
            blob.reload(retry=None, timeout=30)
            generation = int(blob.generation)
            readback = blob.download_as_bytes(if_generation_match=generation, retry=None, timeout=30)
        except Exception:
            raise ArchiveError("CREATE_OR_READBACK_UNKNOWN_NO_RETRY") from None
        if readback != body:
            raise ArchiveError("CREATE_READBACK_MISMATCH")
        record = {"name": name, "generation": str(generation), "bytes": len(body), "sha256": digest}
        self.receipts.append(record)
        return record

    def config_structure(self, uri: str) -> dict:
        bucket, name = uri[5:].split("/", 1)
        blob = self.bucket.client.bucket(bucket).blob(name)
        try:
            blob.reload(retry=None, timeout=30)
            if not 0 < int(blob.size) <= 1024 * 1024:
                raise ArchiveError("CONFIG_SIZE_REJECTED")
            body = blob.download_as_bytes(if_generation_match=int(blob.generation), retry=None, timeout=30)
            config = json.loads(body)
        except Exception:
            raise ArchiveError("CONFIG_READ_UNAVAILABLE") from None
        if not isinstance(config, dict):
            raise ArchiveError("CONFIG_OBJECT_REQUIRED")
        return {"status": "STRUCTURE_ONLY_NOT_REPLAY_BINDING", "generation": str(blob.generation),
                "structure": structure(config)}


def structure(value: Any) -> dict:
    """Retain key names and types only; never scalar values or their digests."""
    if isinstance(value, dict):
        return {"type": "object", "properties": {k: structure(v) for k, v in sorted(value.items())}}
    if isinstance(value, list):
        # Distinct shapes, without list length, values or private list identities.
        shapes = {canonical(structure(item)): structure(item) for item in value}
        return {"type": "array", "items": [shapes[key] for key in sorted(shapes)]}
    return {"type": "null" if value is None else "boolean" if isinstance(value, bool)
            else "number" if isinstance(value, (int, float)) else "string"}


def collect(provider, store, symbol: str, kind: str) -> dict:
    start = "2022-12-28" if symbol == "BOXX" else "2021-12-01"
    if kind == "bars":
        import exchange_calendars
        calendar = exchange_calendars.get_calendar("XNYS", start=start, end="2026-08-25")
        sessions = [s.date().isoformat() for s in calendar.sessions_in_range(start, "2026-08-25")]
        path = f"/v2/stocks/{symbol}/bars"
        params = {"timeframe": "1Day", "start": start + "T00:00:00-05:00",
                  "end": "2026-08-25T23:59:59-04:00", "asof": "2026-08-25",
                  "feed": "sip", "adjustment": "raw", "currency": "USD", "sort": "asc", "limit": "10000"}
    else:
        path = "/v1/corporate-actions"
        params = {"symbols": symbol, "region": "us", "start": start, "end": "2026-08-25",
                  "data_quality": "all", "sort": "asc", "limit": "1000"}
    pages, seen, rows, previous, count = [], set(), [], None, 0
    while True:
        data = provider.get(path, params)
        if kind == "bars":
            rows = data.get("bars")
            if not isinstance(rows, list) or data.get("symbol") not in (None, symbol):
                raise ArchiveError("BAR_SCHEMA_INVALID")
            for offset, row in enumerate(rows):
                try:
                    stamp = datetime.fromisoformat(row["t"].replace("Z", "+00:00"))
                    if stamp.tzinfo is None or not start <= stamp.date().isoformat() <= "2026-08-25":
                        raise ValueError
                    if count + offset >= len(sessions) or stamp.date().isoformat() != sessions[count + offset]:
                        raise ValueError
                    if previous is not None and stamp <= previous:
                        raise ValueError
                    for field in ("o", "h", "l", "c", "v"):
                        v = row[field]
                        if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
                            raise ValueError
                        if v < 0 or (field != "v" and v == 0):
                            raise ValueError
                    if row["l"] > min(row["o"], row["c"]) or row["h"] < max(row["o"], row["c"]):
                        raise ValueError
                    previous = stamp
                except (ValueError, KeyError, TypeError, AttributeError):
                    raise ArchiveError("BAR_VALIDATION_FAILED") from None
        else:
            actions = data.get("corporate_actions")
            if (not isinstance(actions, dict) or set(actions) - ACTION_TYPES
                    or any(not isinstance(v, list) for v in actions.values())):
                raise ArchiveError("ACTION_SCHEMA_INVALID")
            rows = [row for group in actions.values() for row in group]
            if any(not isinstance(row, dict) or row.get("symbol") not in (None, symbol) for row in rows):
                raise ArchiveError("ACTION_SYMBOL_INVALID")
        token = data.get("next_page_token")
        # Pagination token is operational, excluded from the deterministic data bytes.
        body = canonical({k: v for k, v in data.items() if k != "next_page_token"})
        name = f"{kind}/{symbol}/page-{len(pages) + 1:03d}.json"
        store.create(name, body)
        pages.append({"name": name, "bytes": len(body), "sha256": sha(body)})
        count += len(rows)
        if token in (None, ""):
            break
        if not rows or not isinstance(token, str) or len(token) > 2048 or token in seen or len(pages) >= 20:
            raise ArchiveError("PAGINATION_INVALID")
        seen.add(token)
        params["page_token"] = token
    if kind == "bars" and count != (917 if symbol == "BOXX" else 1187):
        raise ArchiveError("REGISTERED_SESSION_COVERAGE_MISMATCH")
    return {"symbol": symbol, "kind": kind, "count": count, "pages": pages, "complete_pagination": True}


def regenerate(provider, store, config_uri: str = "") -> dict:
    # Snapshot first: an unavailable supplied configuration must stop acquisition.
    config = store.config_structure(config_uri) if config_uri else {"status": "LIVE_CONFIG_URI_UNAVAILABLE"}
    store.create("effective-config-structure.json", canonical(config))
    inputs = [collect(provider, store, symbol, kind) for kind in ("bars", "actions") for symbol in SYMBOLS]
    manifest = {"schema_version": "qsl.research.raw_sip_input.v1", "inputs": inputs,
                "source": "alpaca.stocks.bars.v2_and_corporate_actions.v1", "feed": "sip",
                "price_adjustment": "raw", "calendar": "XNYS", "timezone": "America/New_York",
                "currency": "USD", "license": "user_attested_non_commercial_private_research",
                "window_start": "2021-12-01", "window_end": "2026-08-25",
                "supersedes_source_digest": LEGACY_DIGEST, "historical_pit_attested": False,
                "live_config_snapshot": config["status"], "historical_context": "NOT_BOUND",
                "no_order": True, "research_only": True}
    identity = store.create("manifest.json", canonical(manifest))
    receipt = {"schema_version": "rs04.archive-registration.v1", "manifest": identity,
               "members": list(store.receipts), "retrieved_at": datetime.now(timezone.utc).isoformat(),
               "producer_revision": os.environ.get("GITHUB_SHA"), "legacy_digest": LEGACY_DIGEST,
               "content_identity_is_not_legacy_identity": True, "no_order": True}
    # Final marker registers only a fully read-back archive; interrupted writes stay unregistered.
    registration = store.create("registration.json", canonical(receipt))
    return {"status": "ARCHIVE_REGISTERED", "manifest": identity, "registration": registration,
            "live_config_snapshot": config["status"], "historical_context": "NOT_BOUND",
            "legacy_digest_preserved": LEGACY_DIGEST, "no_order": True}


def main() -> int:
    try:
        require_cloud()
        root = input_root(os.environ.get("RS04_INPUT_ROOT", ""))
        mode = os.environ.get("RS04_MODE", "inventory")
        if mode == "inventory":
            result = inventory(root)
        elif mode == "regenerate":
            dataset = os.environ.get("RS04_DATASET_ID", "")
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,62}", dataset):
                raise ArchiveError("DATASET_ID_INVALID")
            config_uri = os.environ.get("RS04_EFFECTIVE_CONFIG_URI", "")
            if config_uri:
                object_uri(config_uri, root)
            provider = Provider(os.environ.get("ALPACA_API_KEY_ID", ""),
                                os.environ.get("ALPACA_API_SECRET_KEY", ""))
            from google.cloud import storage
            result = regenerate(provider, Store(storage.Client(), root + dataset + "/"), config_uri)
        else:
            raise ArchiveError("MODE_INVALID")
    except ArchiveError as exc:
        result = {"status": str(exc), "no_retry": True, "no_order": True}
    except Exception:
        result = {"status": "RUNTIME_FAILURE_SANITIZED", "no_retry": True, "no_order": True}
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] in ("METADATA_INVENTORY_COMPLETE", "ARCHIVE_REGISTERED") else 2


if __name__ == "__main__":
    raise SystemExit(main())
