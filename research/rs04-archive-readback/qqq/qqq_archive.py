"""Prepare an aggregate QQQ reconciliation from fixed archive metadata.

The caller supplies the existing generation-bound reader inside an authorized
protected job. This module cannot authenticate, publish, or write archive data.
"""
import csv
import json
import re
from datetime import date
from decimal import Decimal, InvalidOperation

from qqq_compare import Event, compare


START = date(2021, 12, 1)
END = date(2026, 8, 25)
ISSUER_URL = (
    "https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/46090E103/"
    "distribution?idType=cusip&productType=ETF"
)
PAGE_NAME = "actions/QQQ/page-001.json"
ARCHIVE_COUNT = 21
ISSUER_COUNT = 20


class ReconciliationError(ValueError):
    """An allowlisted stop reason without input content."""


def reference(row):
    generation, size, digest = row["generation"], row["bytes"], row["sha256"]
    if (not isinstance(generation, str) or not re.fullmatch(r"[1-9][0-9]*", generation)
            or type(size) is not int or not 0 < size <= 4 * 1024 * 1024
            or not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest)):
        raise ReconciliationError("OBJECT_REFERENCE_INVALID")
    return generation, size, digest


def cash_event(ex_date, payable_date, value):
    ex, pay = date.fromisoformat(ex_date), date.fromisoformat(payable_date)
    if type(value) not in (str, int, Decimal):
        raise ReconciliationError("CASH_EVENT_INVALID")
    amount = Decimal(value)
    if not START <= ex <= END or pay < ex or not amount.is_finite() or amount <= 0:
        raise ReconciliationError("CASH_EVENT_INVALID")
    # Provider cash_dividends and issuer cash distributions are not tax categories.
    return Event(ex, pay, amount)


def issuer_events(source):
    events = []
    for row in csv.DictReader(source):
        if row["symbol"] != "QQQ":
            continue
        if (row["event_kind"] != "cash_distribution" or row["source_url"] != ISSUER_URL
                or row["amount_basis"] != "issuer_reported_usd_per_share_unreconciled"):
            raise ReconciliationError("ISSUER_ROW_NOT_BOUND")
        events.append(cash_event(row["ex_or_split_date"], row["payable_date"], row["issuer_amount_or_ratio"]))
    if len(events) != ISSUER_COUNT or len({event.ex_date for event in events}) != ISSUER_COUNT:
        raise ReconciliationError("ISSUER_COUNT_MISMATCH")
    return events


def archive_events(data, read_exact):
    registration_ref = reference(data["registration"])
    manifest_ref = reference(data["manifest"])
    registration = json.loads(read_exact("registration.json", registration_ref))
    if (registration["schema_version"] != "rs04.archive-registration.v1"
            or registration["producer_revision"] != data["producer_revision"]
            or registration["no_order"] is not True
            or registration["manifest"]["name"] != "manifest.json"
            or reference(registration["manifest"]) != manifest_ref):
        raise ReconciliationError("REGISTRATION_BINDING_MISMATCH")

    manifest = json.loads(read_exact("manifest.json", manifest_ref))
    if (manifest["schema_version"] != "qsl.research.raw_sip_input.v1"
            or manifest["source"] != "alpaca.stocks.bars.v2_and_corporate_actions.v1"
            or manifest["window_start"] != START.isoformat()
            or manifest["window_end"] != END.isoformat() or manifest["no_order"] is not True
            or manifest["research_only"] is not True or manifest["currency"] != "USD"):
        raise ReconciliationError("MANIFEST_SCOPE_MISMATCH")
    inputs = [row for row in manifest["inputs"] if row["kind"] == "actions" and row["symbol"] == "QQQ"]
    if (len(inputs) != 1 or inputs[0]["complete_pagination"] is not True
            or inputs[0]["count"] != ARCHIVE_COUNT or len(inputs[0]["pages"]) != 1):
        raise ReconciliationError("QQQ_MEMBER_SET_MISMATCH")
    page = inputs[0]["pages"][0]
    members = [row for row in registration["members"] if row["name"] == PAGE_NAME]
    if len(members) != 1 or page["name"] != PAGE_NAME:
        raise ReconciliationError("QQQ_MEMBER_IDENTITY_MISMATCH")
    ref = reference(members[0])
    if (page["bytes"], page["sha256"]) != ref[1:]:
        raise ReconciliationError("QQQ_MEMBER_IDENTITY_MISMATCH")

    data = json.loads(read_exact(PAGE_NAME, ref), parse_float=Decimal)
    actions = data["corporate_actions"]
    cash = actions["cash_dividends"]
    if (data.get("next_page_token") is not None or not isinstance(cash, list)
            or len(cash) != ARCHIVE_COUNT or any(rows for kind, rows in actions.items() if kind != "cash_dividends")):
        raise ReconciliationError("QQQ_ACTION_SCOPE_MISMATCH")

    events, seen, dates_with_id = [], set(), {}
    revisions = ("cancelled", "canceled", "replaced_by", "replaces", "superseded_by", "replacement_id", "revision")
    for row in cash:
        if (row.get("symbol") not in (None, "QQQ") or row.get("currency", "USD") != "USD"
                or row.get("status", "active") != "active" or any(row.get(key) for key in revisions)):
            raise ReconciliationError("QQQ_ACTION_SCOPE_MISMATCH")
        identity = row.get("id")
        if identity is not None:
            if not isinstance(identity, str) or not 0 < len(identity) <= 256 or identity != identity.strip() or identity in seen:
                raise ReconciliationError("QQQ_ACTION_IDENTITY_INVALID")
            seen.add(identity)
        event = cash_event(row["ex_date"], row["payable_date"], row["rate"])
        if event.ex_date in dates_with_id and not (dates_with_id[event.ex_date] and identity):
            raise ReconciliationError("QQQ_SAME_DAY_IDENTITY_UNVERIFIED")
        dates_with_id[event.ex_date] = bool(identity)
        events.append(event)
    return events


def reconcile(data, read_exact, issuer_csv):
    """Return only fixed reasons or aggregate counts; keep records in memory."""
    try:
        issuer = issuer_events(issuer_csv)
        archive = archive_events(data, read_exact)
        return {"status": "QQQ_COMPARED", **compare(issuer, archive)}
    except ReconciliationError as exc:
        reason = str(exc)
    except ValueError as exc:
        reason = str(exc) if str(exc) in {"OBJECT_METADATA_MISMATCH", "OBJECT_BYTES_MISMATCH"} else "RECONCILIATION_INPUT_INVALID"
    except (KeyError, TypeError, AttributeError, InvalidOperation, csv.Error):
        reason = "RECONCILIATION_INPUT_INVALID"
    except Exception as exc:
        reason = {403: "GCS_OBJECT_FORBIDDEN", 404: "GCS_OBJECT_MISSING",
                  412: "GCS_GENERATION_PRECONDITION_FAILED"}.get(getattr(exc, "code", None), "READBACK_FAILED")
    return {"status": "PARKED", "reason_code": reason}
