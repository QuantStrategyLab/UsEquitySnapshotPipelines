#!/usr/bin/env python3
"""Manual cloud-only input acquisition and exact-byte evidence inventory.

This module has no publisher, broker, strategy replay, or promotion entry point.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import platform
import re
import tempfile
import time
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from xml.etree import ElementTree
from zipfile import ZipFile
from zoneinfo import ZoneInfo


class Stop(Exception):
    """A payload-free terminal diagnostic; calls are never retried."""


def sha(body):
    return hashlib.sha256(body).hexdigest()


def encode(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":")) + "\n").encode()


def now():
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def gcs_uri(uri):
    parsed = urlsplit(uri)
    if (parsed.scheme != "gs" or not parsed.netloc or not parsed.path.strip("/")
            or parsed.query or parsed.fragment or ".." in parsed.path or "//" in parsed.path):
        raise Stop("INVALID_PRIVATE_OBJECT")
    return parsed.netloc, parsed.path.lstrip("/")


def validate_plan(plan, root):
    bucket, prefix = gcs_uri(root)
    dataset = plan.get("dataset_id", "")
    if not root.endswith("/") or not re.fullmatch(r"[a-z0-9][a-z0-9_-]{2,80}", dataset):
        raise Stop("INVALID_RESEARCH_ROOT_OR_DATASET")
    if plan.get("scope") != "research_input_only" or plan.get("paid_data_usd") != 0:
        raise Stop("SCOPE_OR_PAID_DATA_REJECTED")
    if not 1 <= plan.get("retention_estimate_days", 0) <= 90:
        raise Stop("RETENTION_ESTIMATE_REQUIRED")
    if not 0 <= plan.get("estimated_cloud_usd", 11) <= plan.get("cloud_cap_usd", 0) <= 10:
        raise Stop("CLOUD_COST_CAP_REJECTED")
    end = date.fromisoformat(plan["end"])
    if datetime.combine(end + timedelta(days=2), datetime.min.time(), UTC) > datetime.now(UTC):
        raise Stop("DAILY_FINALITY_MARGIN_REQUIRED")
    instruments = plan.get("instruments", [])
    if len(instruments) > 20 or len({x["symbol"] for x in instruments}) != len(instruments):
        raise Stop("INSTRUMENT_SET_REJECTED")
    for item in instruments:
        if not re.fullmatch(r"[A-Z][A-Z0-9.]{0,9}", item["symbol"]):
            raise Stop("SYMBOL_REJECTED")
        if not date.fromisoformat(item["inception"]) <= date.fromisoformat(item["start"]) <= end:
            raise Stop("INSTRUMENT_WINDOW_REJECTED")
    objects = plan.get("inspect_objects", [])
    if len(objects) > 800:
        raise Stop("EVIDENCE_OBJECT_LIMIT")
    for obj in objects:
        gcs_uri(obj["uri"])
        if not str(obj["generation"]).isdigit() or not 0 <= int(obj["bytes"]) <= 12 * 1024**2:
            raise Stop("EVIDENCE_IDENTITY_REJECTED")
        if obj["role"] not in {"archive", "config_projection", "canonical_config", "risk_policy",
                              "plugin", "plugin_history", "decision", "execution", "oos"}:
            raise Stop("EVIDENCE_ROLE_REJECTED")
    return bucket, prefix + dataset + "/"


class Budget:
    """Hard resource bounds underneath the pre-run cost envelope."""
    def __init__(self):
        self.started = time.monotonic()
        self.http_requests = 0
        self.http_bytes = 0
        self.storage_operations = 0
        self.storage_transfer_bytes = 0
        self.retained_bytes = 0

    def check(self):
        if time.monotonic() - self.started > 1200:
            raise Stop("WALL_TIME_LIMIT")

    def http(self):
        self.check()
        if self.http_requests >= 120:
            raise Stop("HTTP_REQUEST_LIMIT")
        self.http_requests += 1

    def storage(self, transfer=0, retained=0):
        self.check()
        if (self.storage_operations >= 3000 or self.storage_transfer_bytes + transfer > 512 * 1024**2
                or self.retained_bytes + retained > 128 * 1024**2):
            raise Stop("STORAGE_BUDGET_LIMIT")
        self.storage_operations += 1
        self.storage_transfer_bytes += transfer
        self.retained_bytes += retained

    def summary(self):
        return {key: getattr(self, key) for key in ("http_requests", "http_bytes", "storage_operations",
                "storage_transfer_bytes", "retained_bytes")} | {
                    "elapsed_seconds": round(time.monotonic() - self.started, 3)}


class Store:
    def __init__(self, client, budget, bucket, prefix):
        self.client, self.budget, self.bucket, self.prefix = client, budget, bucket, prefix

    def read(self, obj):
        bucket, path = gcs_uri(obj["uri"])
        size, generation = int(obj["bytes"]), int(obj["generation"])
        self.budget.storage(transfer=size)
        try:
            body = self.client.bucket(bucket).blob(path, generation=generation).download_as_bytes(
                if_generation_match=generation, retry=None, timeout=30)
        except Exception:
            raise Stop("EXACT_OBJECT_READ_FAILED") from None
        if len(body) != size or (obj.get("sha256") and sha(body) != obj["sha256"]):
            raise Stop("EXACT_OBJECT_IDENTITY_MISMATCH")
        return body

    def create(self, relative, body):
        if not re.fullmatch(r"[A-Za-z0-9_./-]+", relative) or ".." in relative or relative.startswith("/"):
            raise Stop("OUTPUT_NAME_REJECTED")
        if len(body) > 96 * 1024**2:
            raise Stop("OUTPUT_OBJECT_TOO_LARGE")
        self.budget.storage(transfer=len(body), retained=len(body))
        blob = self.client.bucket(self.bucket).blob(self.prefix + relative)
        try:
            blob.upload_from_string(body, content_type="application/octet-stream", if_generation_match=0,
                                    retry=None, timeout=30)
        except Exception:
            raise Stop("CREATE_RESULT_UNKNOWN_STOP_NO_RETRY") from None
        if not blob.generation:
            raise Stop("WRITE_GENERATION_UNKNOWN")
        obj = {"uri": f"gs://{self.bucket}/{self.prefix}{relative}", "generation": str(blob.generation),
               "bytes": len(body), "sha256": sha(body)}
        if self.read(obj) != body:
            raise Stop("WRITE_READBACK_MISMATCH")
        return obj


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        return None


class Provider:
    def __init__(self, budget):
        self.budget = budget
        self.opener = build_opener(ProxyHandler({}), NoRedirect())

    def get(self, url, headers=None):
        parsed = urlsplit(url)
        if (parsed.scheme != "https" or parsed.username or parsed.password or parsed.port
                or parsed.hostname not in {"data.alpaca.markets", "www.federalreserve.gov",
                                           "markets.newyorkfed.org", "www.newyorkfed.org"}):
            raise Stop("PROVIDER_ENDPOINT_REJECTED")
        if headers and parsed.hostname != "data.alpaca.markets":
            raise Stop("AUTHENTICATED_ENDPOINT_REJECTED")
        self.budget.http()
        page_limit = 80 * 1024**2 if parsed.hostname == "www.federalreserve.gov" else 12 * 1024**2
        limit = min(page_limit, 96 * 1024**2 - self.budget.http_bytes)
        if limit < 1:
            raise Stop("HTTP_BYTE_LIMIT")
        try:
            with self.opener.open(Request(url, headers=headers or {}), timeout=40) as response:
                if response.status != 200:
                    raise Stop("HTTP_STATUS_REJECTED")
                body = response.read(limit + 1)
        except HTTPError as exc:
            raise Stop(f"HTTP_REJECTED_{exc.code}") from None
        except (URLError, TimeoutError, OSError):
            raise Stop("HTTP_TRANSPORT_FAILED") from None
        self.budget.http_bytes += len(body)
        if len(body) > limit:
            raise Stop("HTTP_BYTE_LIMIT")
        return body


def decimal_value(value):
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise Stop("NONFINITE_SOURCE_VALUE") from None
    if not number.is_finite() or isinstance(value, bool):
        raise Stop("NONFINITE_SOURCE_VALUE")
    return number


def bar_dates(data, start, end):
    dates = []
    rows = data.get("bars")
    if not isinstance(rows, list) or len(rows) > 10000:
        raise Stop("BAR_SCHEMA_INVALID")
    for row in rows:
        stamp = datetime.fromisoformat(row["t"].replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            raise Stop("NAIVE_PROVIDER_TIME")
        session = stamp.astimezone(ZoneInfo("America/New_York")).date().isoformat()
        if not start <= session <= end:
            raise Stop("BAR_OUT_OF_REQUEST_WINDOW")
        o, h, low, c = [decimal_value(row[k]) for k in ("o", "h", "l", "c")]
        if min(o, h, low, c) <= 0 or low > min(o, c) or h < max(o, c) or low > h:
            raise Stop("BAR_PRICE_INVALID")
        if decimal_value(row["v"]) < 0:
            raise Stop("BAR_VOLUME_INVALID")
        dates.append(session)
    return dates


def coverage(dates, start, end, expected):
    if dates != sorted(dates) or len(dates) != len(set(dates)):
        raise Stop("DUPLICATE_OR_UNORDERED_OBSERVATIONS")
    missing = sorted(set(expected) - set(dates))
    unexpected = sorted(set(dates) - set(expected))
    return {"requested_start": start, "requested_end": end, "first": dates[0] if dates else None,
            "last": dates[-1] if dates else None, "observations": len(dates),
            "expected_sessions": len(expected), "missing_sessions": len(missing),
            "missing_dates": missing, "unexpected_sessions": len(unexpected),
            "duplicates": 0, "coverage_complete": not missing and not unexpected and bool(dates)}


def acquire_alpaca(plan, provider, store):
    if not plan.get("instruments"):
        return []
    # Existing protected runner environment is consumed only by the approved client.
    key, secret = os.environ.get("ALPACA_API_KEY_ID"), os.environ.get("ALPACA_API_SECRET_KEY")
    if not key or not secret:
        raise Stop("EXISTING_ALPACA_CLIENT_NOT_CONFIGURED")
    headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
    import exchange_calendars as xcals
    from importlib.metadata import version
    calendar = xcals.get_calendar("XNYS", start="1990-01-01", end=plan["end"])
    result = []
    for item in plan["instruments"]:
        symbol, start, end = item["symbol"], item["start"], plan["end"]
        expected = [x.date().isoformat() for x in calendar.sessions_in_range(start, end)]
        schedule = [{"date": s, "close": calendar.session_close(s).isoformat()} for s in expected]
        for adjustment, basis in [("raw", "raw_unadjusted"), ("all", "split_dividend_adjusted")]:
            local_start = datetime.combine(date.fromisoformat(start), datetime.min.time(), ZoneInfo("America/New_York"))
            local_end = datetime.combine(date.fromisoformat(end), datetime.max.time(), ZoneInfo("America/New_York"))
            params = {"start": local_start.isoformat(), "end": local_end.isoformat(),
                      "timeframe": "1Day", "feed": "sip", "adjustment": adjustment,
                      "sort": "asc", "limit": "10000", "asof": end}
            pages, dates, seen = [], [], set()
            for page in range(1, 11):
                fetched = now()
                body = provider.get("https://data.alpaca.markets/v2/stocks/" + symbol + "/bars?"
                                    + urlencode(params), headers)
                data = json.loads(body)
                dates.extend(bar_dates(data, start, end))
                pages.append(store.create(f"alpaca/{symbol}/{adjustment}/page-{page:03}.json", body)
                             | {"fetched_at": fetched})
                token = data.get("next_page_token")
                if not token:
                    break
                if not isinstance(token, str) or len(token) > 2048 or token in seen:
                    raise Stop("PAGINATION_INVALID")
                seen.add(token)
                params["page_token"] = token
            else:
                raise Stop("PAGINATION_LIMIT")
            result.append({"symbol": symbol, "inception": item["inception"], "source_type": "real",
                           "provider": "alpaca", "feed": "sip", "price_basis": basis,
                           "currency": "USD", "instrument_type": "ETF", "listing_venue": "NOT_VERIFIED",
                           "vendor_adjustment": adjustment, "return_basis": "not_constructed",
                           "volume_basis": "provider_native_adjustment_not_independently_verified",
                           "session_scope": "provider_native", "close_kind": "provider_native",
                           "calendar_id": "XNYS", "calendar_version": version("exchange-calendars"),
                           "schedule_sha256": sha(encode(schedule)), "market_timezone": "America/New_York",
                           "coverage": coverage(dates, start, end, expected), "pages": pages,
                           "historical_available_at": None, "pit_status": "NOT_ESTABLISHED",
                           "finality": "cutoff_at_least_two_days_old_provider_revisions_possible",
                           "normalization": "not_performed_exact_provider_bytes_retained"})
        params = {"symbols": symbol, "start": start, "end": end, "limit": "1000", "sort": "asc"}
        pages, counts, seen = [], {}, set()
        for page in range(1, 11):
            body = provider.get("https://data.alpaca.markets/v1/corporate-actions?" + urlencode(params), headers)
            data = json.loads(body)
            actions = data.get("corporate_actions")
            if not isinstance(actions, dict):
                raise Stop("ACTION_SCHEMA_INVALID")
            for kind, rows in actions.items():
                if not re.fullmatch(r"[a-z_]{1,60}", kind) or not isinstance(rows, list):
                    raise Stop("ACTION_SCHEMA_INVALID")
                counts[kind] = counts.get(kind, 0) + len(rows)
            pages.append(store.create(f"alpaca/{symbol}/actions/page-{page:03}.json", body))
            token = data.get("next_page_token")
            if not token:
                break
            if not isinstance(token, str) or token in seen or len(token) > 2048:
                raise Stop("PAGINATION_INVALID")
            seen.add(token)
            params["page_token"] = token
        else:
            raise Stop("PAGINATION_LIMIT")
        result.append({"symbol": symbol, "type": "corporate_actions", "counts": counts, "pages": pages,
                       "completeness": "provider_returned_actions_not_independent_full_universe_assurance"})
    return result


def parse_board_csv(body, series, start, end):
    rows = list(csv.reader(io.StringIO(body.decode("utf-8-sig"))))
    header = next((i for i, row in enumerate(rows) if row and row[0].strip() == "Time Period"), None)
    if header is None or rows[header].count(series) != 1:
        raise Stop("BOARD_SERIES_HEADER_INVALID")
    col = rows[header].index(series)
    values = []
    for row in rows[header + 1:]:
        if len(row) <= col or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", row[0]):
            continue
        if start <= row[0] <= end and row[col].strip() not in {"", "ND", "NA"}:
            decimal_value(row[col])
            values.append({"date": row[0], "rate_percent": row[col]})
    if not values:
        raise Stop("BOARD_NO_OBSERVATIONS")
    return values


def parse_sofr(body, start, end):
    data = json.loads(body)
    rows = data.get("refRates")
    if not isinstance(rows, list):
        raise Stop("NYFED_SCHEMA_INVALID")
    values = []
    for row in rows:
        if row.get("type") != "SOFR":
            raise Stop("NYFED_RATE_IDENTITY_INVALID")
        stamp = row["effectiveDate"]
        if start <= stamp <= end:
            decimal_value(row["percentRate"])
            values.append({"date": stamp, "rate_percent": str(row["percentRate"])})
    values.sort(key=lambda row: row["date"])
    if not values or len({x["date"] for x in values}) != len(values):
        raise Stop("NYFED_EMPTY_OR_DUPLICATE")
    return values


def parse_board_zip(body, series, start, end):
    values = []
    with ZipFile(io.BytesIO(body)) as archive:
        members = [x for x in archive.infolist() if x.filename.lower().endswith("_data.xml")]
        if len(members) != 1 or members[0].file_size > 768 * 1024**2:
            raise Stop("BOARD_XML_ARCHIVE_INVALID")
        selected = False
        with archive.open(members[0]) as stream:
            for event, elem in ElementTree.iterparse(stream, events=("start", "end")):
                tag = elem.tag.rsplit("}", 1)[-1].lower()
                attrs = {k.upper(): v for k, v in elem.attrib.items()}
                if event == "start" and tag == "series":
                    selected = attrs.get("SERIES_NAME") == series
                elif event == "end" and tag == "obs":
                    stamp = attrs.get("TIME_PERIOD", "")
                    if selected and start <= stamp <= end and attrs.get("OBS_STATUS", "A") == "A":
                        decimal_value(attrs["OBS_VALUE"])
                        values.append({"date": stamp, "rate_percent": attrs["OBS_VALUE"]})
                    elem.clear()
                elif event == "end" and tag == "series":
                    selected = False
                    elem.clear()
    values.sort(key=lambda row: row["date"])
    if not values or len({row["date"] for row in values}) != len(values):
        raise Stop("BOARD_EMPTY_OR_DUPLICATE_SERIES")
    return values


def acquire_rates(plan, provider, store):
    result = []
    for request in plan.get("rates", []):
        kind, start, end = request["kind"], request["start"], plan["end"]
        url = request["url"]
        parsed = urlsplit(url)
        if kind == "board_h15" and parsed.hostname == "www.federalreserve.gov" and parsed.path == "/datadownload/Output.aspx":
            if parsed.query != "rel=H15&filetype=zip":
                raise Stop("BOARD_PACKAGE_ENDPOINT_REJECTED")
            def parser(body):
                return parse_board_zip(body, request["series"], start, end)
        elif kind == "nyfed_sofr" and parsed.hostname == "markets.newyorkfed.org" and parsed.path == "/api/rates/secured/sofr/search.json":
            def parser(body):
                return parse_sofr(body, start, end)
        else:
            raise Stop("RATE_ENDPOINT_REJECTED")
        notice_url = request["source_notice"]
        if notice_url not in {"https://www.federalreserve.gov/disclaimer.htm",
                              "https://www.newyorkfed.org/privacy/termsofuse"}:
            raise Stop("SOURCE_NOTICE_REJECTED")
        notice = store.create(f"rates/{kind}/source-notice.html", provider.get(notice_url))
        body = provider.get(url)
        values = parser(body)
        source = store.create(f"rates/{kind}/response.bin", body)
        normalized = store.create(f"rates/{kind}/selected-records.json", encode(values))
        result.append({"kind": kind, "series": request.get("series", "SOFR"), "first": values[0]["date"],
                       "last": values[-1]["date"], "observations": len(values), "source": source,
                       "selected_records": normalized, "rate_unit": "percent_per_annum",
                       "normalization": "selected_source_decimal_strings_json_v1",
                       "historical_available_at": None, "pit_status": "LATEST_VINTAGE_ONLY",
                       "source_notice": notice, "financing_spread": "NOT_PROVIDED",
                       "rate_regime_mapping": "NOT_APPLIED_NO_MODEL_RUN"})
    return result


def inspect_evidence(plan, store):
    from quant_platform_kit.strategy_lifecycle.release_readiness import _plugin_bundle_sha256
    import inspect
    from importlib.metadata import distribution
    qpk = distribution("quant-platform-kit")
    direct = json.loads(qpk.read_text("direct_url.json") or "{}")
    implementation = {"module": _plugin_bundle_sha256.__module__,
                      "source_file_sha256": sha(Path(inspect.getfile(_plugin_bundle_sha256)).read_bytes()),
                      "source_commit": direct.get("vcs_info", {}).get("commit_id")}
    private, plugin_paths = [], []
    with tempfile.TemporaryDirectory(prefix="input-evidence-") as temp:
        for i, obj in enumerate(plan.get("inspect_objects", [])):
            body = store.read(obj)
            entry = dict(obj) | {"sha256": sha(body), "status": "EXACT_BYTES_VERIFIED",
                                 "object_id": sha(obj["uri"].encode())}
            if obj["role"] == "plugin":
                path = Path(temp) / str(i)
                path.write_bytes(body)
                plugin_paths.append(path)
            if obj["uri"].endswith(".json"):
                try:
                    data = json.loads(body)
                except ValueError:
                    raise Stop("EVIDENCE_JSON_INVALID") from None
                if isinstance(data, dict):
                    # Structural presence only; values stay in the private cloud manifest.
                    keys = {k.lower() for k in data}
                    entry["structural_fields"] = sorted(keys & {
                        "schema_version", "config", "risk_policy", "positions", "orders", "fills",
                        "decisions", "decision", "executions", "fees", "cash", "input_digest",
                        "effective_config_sha256", "strategy_revision", "available_at", "generated_at",
                        "approval", "human_acceptance", "trial_ledger", "access_history"})
                    if obj["role"] == "config_projection":
                        entry["qualification"] = "PARTIAL_UNRESOLVED_NOT_CANONICAL"
                    if obj["role"] in {"execution", "decision"}:
                        entry["engineering_structure"] = engineering_structure(data)
            private.append(entry)
        findings = []
        digest = _plugin_bundle_sha256(plugin_paths, findings) if plugin_paths else None
    return {"objects": private, "candidate_plugin_digest": digest,
            "qpk_digest_implementation": implementation,
            "plugin_file_count": len(plugin_paths), "approved_plugin_set": False,
            "qualification": "INVENTORY_PENDING_OWNER_CONFIRMATION_NOT_RELEASE_EVIDENCE"}


def engineering_structure(data):
    """Report counts and field presence; never export account or execution values."""
    summary = data.get("summary", {})
    if not isinstance(summary, dict):
        summary = {}
    counts = {}
    for key in ("orders_submitted_count", "orders_pending_count", "order_events_count", "orders_previewed_count"):
        value = summary.get(key)
        if isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= 100000:
            counts[key] = value
    status_counts = summary.get("submitted_order_status_counts", {})
    counts["reported_filled_status_count"] = sum(
        value for key, value in status_counts.items()
        if key.lower() == "filled" and isinstance(value, int) and not isinstance(value, bool) and value >= 0
    ) if isinstance(status_counts, dict) else 0
    present = set()
    allowed = {"fills", "orders", "orderActivityCollection", "executionLegs", "fees", "commission",
               "input_digest", "input_sha256", "effective_config_sha256", "risk_policy_sha256",
               "plugin_bundle_sha256", "positions", "cash", "indicators", "decision", "decision_id",
               "order_id", "orderId", "fill_id", "filledQuantity", "fill_price"}

    def walk(value):
        if isinstance(value, dict):
            present.update(key for key in value if key in allowed)
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(data)
    run = data.get("run_id")
    return {"counts": counts, "fields_present": sorted(present),
            "run_join_sha256": sha(run.encode()) if isinstance(run, str) and run else None,
            "dry_run": data.get("dry_run") if isinstance(data.get("dry_run"), bool) else None,
            "fill_comparison_qualified": False}


def public_summary(manifest, budget):
    evidence = manifest["evidence_inventory"]
    return {"schema_version": "research_input_pilot_summary.v1", "status": "INPUT_PILOT_COMPLETED",
            "research_only": True, "release_status": "PARKED", "replay_executed": False,
            "promotion_eligible": False, "no_order": True,
            "coverage": [{k: v for k, v in entry.items() if k not in {"pages"}}
                         for entry in manifest["alpaca"]],
            "rates": [{k: v for k, v in entry.items() if k not in {"source", "selected_records", "source_notice"}}
                      for entry in manifest["rates"]],
            "evidence_objects": [{k: row[k] for k in ("object_id", "role", "generation", "bytes", "sha256", "status")}
                                 for row in evidence["objects"]],
            "engineering_records": [{"object_id": row["object_id"], **row["engineering_structure"]}
                                    for row in evidence["objects"] if "engineering_structure" in row],
            "candidate_plugin_digest": evidence["candidate_plugin_digest"],
            "qpk_digest_implementation": evidence.get("qpk_digest_implementation"),
            "plugin_file_count": evidence["plugin_file_count"], "approved_plugin_set": False,
            "resources": budget.summary()}


def require_cloud():
    # Refuse ordinary developer execution before loading credentials or data.
    if (platform.system() != "Linux" or os.environ.get("RUNNER_OS") != "Linux"
            or os.environ.get("GITHUB_ACTIONS") != "true"
            or os.environ.get("GITHUB_EVENT_NAME") != "workflow_dispatch"
            or os.environ.get("GITHUB_REF") != "refs/heads/main"
            or os.environ.get("GITHUB_RUN_ATTEMPT") != "1"):
        raise Stop("APPROVED_MANUAL_CLOUD_RUNNER_REQUIRED")


def main():
    require_cloud()
    from google.cloud import storage
    budget = Budget()
    root = os.environ["RESEARCH_INPUT_ROOT"]
    bucket, prefix = gcs_uri(root)
    plan_ref = {"uri": os.environ["PILOT_PLAN_URI"], "generation": os.environ["PILOT_PLAN_GENERATION"],
                "bytes": int(os.environ["PILOT_PLAN_BYTES"]), "sha256": os.environ["PILOT_PLAN_SHA256"]}
    if gcs_uri(plan_ref["uri"])[0] != bucket or not gcs_uri(plan_ref["uri"])[1].startswith(prefix):
        raise Stop("PLAN_OUTSIDE_APPROVED_RESEARCH_ROOT")
    client = storage.Client()
    reader = Store(client, budget, bucket, prefix)
    plan = json.loads(reader.read(plan_ref))
    bucket, output_prefix = validate_plan(plan, root)
    store = Store(client, budget, bucket, output_prefix)
    store.create("_write_probe.json", encode({"scope": "research_input_only", "no_order": True}))
    manifest = {"schema_version": "research_input_inventory.v1", "created_at": now(),
                "plan": plan_ref, "source_revision": os.environ.get("GITHUB_SHA"), "research_only": True,
                "retention_estimate_days": plan["retention_estimate_days"],
                "retention_action": "owner_review_required_no_automatic_deletion",
                "evidence_inventory": inspect_evidence(plan, store)}
    provider = Provider(budget)
    manifest["rates"] = acquire_rates(plan, provider, store)
    manifest["alpaca"] = acquire_alpaca(plan, provider, store)
    manifest["resources"] = budget.summary()
    receipt = store.create("input-inventory.json", encode(manifest))
    summary = public_summary(manifest, budget)
    summary["private_inventory_identity"] = {k: receipt[k] for k in ("generation", "sha256", "bytes")}
    # This exact allowlisted projection alone may leave the private research store.
    summary_receipt = store.create("coverage-summary.json", encode(summary))
    summary["summary_identity"] = {k: summary_receipt[k] for k in ("generation", "sha256", "bytes")}
    Path(os.environ["PILOT_SUMMARY_FILE"]).write_bytes(encode(summary))
    print("Research input pilot completed; release remains PARKED. See coverage summary.")


if __name__ == "__main__":
    try:
        main()
    except Stop as exc:
        print("STOP: " + str(exc))
        raise SystemExit(1) from None
    except Exception:
        # Provider/cloud errors may contain private URLs, payloads or credentials.
        print("STOP: UNCLASSIFIED_INPUT_FAILURE_NO_RETRY")
        raise SystemExit(1) from None
