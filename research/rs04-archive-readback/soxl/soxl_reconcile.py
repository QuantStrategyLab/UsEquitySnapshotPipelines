"""Reconcile three fixed action pages in memory using an injected exact reader."""
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from urllib.parse import urlsplit

SYMBOLS = ("SOXL", "SOXX", "BOXX")
COUNTS = {"SOXL": (16, 0), "SOXX": (20, 1), "BOXX": (1, 0)}
START, END = date(2021, 12, 1), date(2026, 8, 25)
BASIS = {"SOXL": "issuer_reported_usd_income_per_share_unreconciled",
         "SOXX": "issuer_reported_usd_as_paid_per_share_unreconciled",
         "BOXX": "issuer_reported_usd_capital_gain_per_share_unreconciled"}
HOST = {"SOXL": "www.direxion.com", "SOXX": "www.ishares.com", "BOXX": "funds.alphaarchitect.com"}


class Stop(ValueError):
    pass


def reference(row, limit=4 * 1024 * 1024):
    generation, size, digest = row["generation"], row["bytes"], row["sha256"]
    if (not isinstance(generation, str) or not re.fullmatch(r"[1-9][0-9]*", generation)
            or type(size) is not int or not 0 < size <= limit
            or not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest)):
        raise Stop("OBJECT_REFERENCE_INVALID")
    return generation, size, digest


def day(value):
    result = date.fromisoformat(value)
    if not START <= result <= END:
        raise Stop("EVENT_INVALID")
    return result


def number(value):
    if type(value) not in (str, int, Decimal):
        raise Stop("EVENT_INVALID")
    result = Decimal(value)
    if not result.is_finite() or result <= 0:
        raise Stop("EVENT_INVALID")
    return result


def issuer_events(source):
    events = {symbol: {"cash": [], "split": []} for symbol in SYMBOLS}
    for row in csv.DictReader(source):
        symbol = row["symbol"]
        if symbol not in SYMBOLS:
            raise Stop("ISSUER_SCOPE_MISMATCH")
        url = urlsplit(row["source_url"])
        if url.scheme != "https" or url.netloc != HOST[symbol]:
            raise Stop("ISSUER_ROW_NOT_BOUND")
        ex = day(row["ex_or_split_date"])
        value = number(row["issuer_amount_or_ratio"])
        if row["event_kind"] == "cash_distribution":
            if row["amount_basis"] != BASIS[symbol] or row["issuer_effective_date"]:
                raise Stop("ISSUER_ROW_NOT_BOUND")
            pay = date.fromisoformat(row["payable_date"])
            if pay < ex:
                raise Stop("EVENT_INVALID")
            events[symbol]["cash"].append((ex, pay, value))
        elif row["event_kind"] == "split":
            effective = day(row["issuer_effective_date"])
            if (symbol != "SOXX" or row["amount_basis"] != "shares_after_per_share_before"
                    or row["payable_date"] or effective > ex):
                raise Stop("ISSUER_ROW_NOT_BOUND")
            events[symbol]["split"].append((ex, Fraction(value)))
        else:
            raise Stop("ISSUER_ROW_NOT_BOUND")
    if any(tuple(len(events[s][k]) for k in ("cash", "split")) != COUNTS[s] for s in SYMBOLS):
        raise Stop("ISSUER_COUNT_MISMATCH")
    return events


def archive_events(data, read_exact, *, symbols=SYMBOLS):
    regref = reference(data["registration"], 128 * 1024)
    manref = reference(data["manifest"], 128 * 1024)
    reg = json.loads(read_exact("registration.json", regref))
    if (reg["schema_version"] != "rs04.archive-registration.v1"
            or reg["producer_revision"] != data["producer_revision"] or reg["no_order"] is not True
            or reg["manifest"]["name"] != "manifest.json" or reference(reg["manifest"], 128 * 1024) != manref):
        raise Stop("REGISTRATION_BINDING_MISMATCH")
    manifest = json.loads(read_exact("manifest.json", manref))
    if (manifest["schema_version"] != "qsl.research.raw_sip_input.v1"
            or manifest["source"] != "alpaca.stocks.bars.v2_and_corporate_actions.v1"
            or manifest["window_start"] != START.isoformat() or manifest["window_end"] != END.isoformat()
            or manifest["no_order"] is not True or manifest["research_only"] is not True
            or manifest["currency"] != "USD"):
        raise Stop("MANIFEST_SCOPE_MISMATCH")
    names = [member["name"] for member in reg["members"]]
    if len(names) != len(set(names)):
        raise Stop("DUPLICATE_REGISTERED_MEMBER")
    result = {}
    ids = set()
    for symbol in symbols:
        items = [x for x in manifest["inputs"] if x["kind"] == "actions" and x["symbol"] == symbol]
        if (len(items) != 1 or items[0]["complete_pagination"] is not True
                or items[0]["count"] != sum(COUNTS[symbol]) or len(items[0]["pages"]) != 1):
            raise Stop("ACTION_MEMBER_SET_MISMATCH")
        name = f"actions/{symbol}/page-001.json"
        page = items[0]["pages"][0]
        members = [x for x in reg["members"] if x["name"] == name]
        if len(members) != 1 or page["name"] != name:
            raise Stop("ACTION_MEMBER_IDENTITY_MISMATCH")
        ref = reference(members[0])
        if (page["bytes"], page["sha256"]) != ref[1:]:
            raise Stop("ACTION_MEMBER_IDENTITY_MISMATCH")
        body = json.loads(read_exact(name, ref), parse_float=Decimal)
        actions = body["corporate_actions"]
        if (not isinstance(actions, dict) or body.get("next_page_token") not in (None, "")
                or any(not isinstance(rows, list) for rows in actions.values())
                or any(rows for kind, rows in actions.items() if kind not in ("cash_dividends", "forward_splits"))):
            raise Stop("ACTION_SCOPE_MISMATCH")
        cash, splits = actions.get("cash_dividends", []), actions.get("forward_splits", [])
        if (len(cash), len(splits)) != COUNTS[symbol]:
            raise Stop("ACTION_SCOPE_MISMATCH")
        result[symbol] = {"cash": [], "split": []}
        for kind, rows in (("cash", cash), ("split", splits)):
            identities_by_date = {}
            for row in rows:
                if (row.get("symbol") not in (None, symbol) or row.get("currency", "USD") != "USD"
                        or row.get("status", "active") != "active" or any(row.get(k) for k in
                        ("cancelled", "canceled", "replaced_by", "replaces", "superseded_by", "replacement_id", "revision"))):
                    raise Stop("ACTION_SCOPE_MISMATCH")
                identity = row.get("id")
                if identity is not None:
                    if (not isinstance(identity, str) or not 0 < len(identity) <= 256
                            or identity != identity.strip() or identity in ids):
                        raise Stop("ACTION_IDENTITY_INVALID")
                    ids.add(identity)
                ex = day(row["ex_date"])
                if ex in identities_by_date and not (identity and identities_by_date[ex]):
                    raise Stop("ACTION_SAME_DAY_IDENTITY_UNVERIFIED")
                identities_by_date[ex] = bool(identity)
                if kind == "cash":
                    pay = date.fromisoformat(row["payable_date"])
                    if pay < ex:
                        raise Stop("EVENT_INVALID")
                    event = (ex, pay, number(row["rate"]))
                else:
                    event = (ex, Fraction(number(row["new_rate"])) / Fraction(number(row["old_rate"])))
                result[symbol][kind].append(event)
    return result


def compare(left, right, cash):
    a, b = Counter(left), Counter(right)
    exact = sum((a & b).values())
    a -= Counter(left) & Counter(right)
    b -= Counter(left) & Counter(right)
    la, lb = defaultdict(list), defaultdict(list)
    for event, count in a.items():
        la[event[0]].extend([event] * count)
    for event, count in b.items():
        lb[event[0]].extend([event] * count)
    out = {"exact_matches": exact, "residual_pairs": 0, "issuer_unmatched": 0,
           "archive_unmatched": 0, "ambiguous_groups": 0}
    if cash:
        out.update(payable_date_differences=0, amount_differences=0)
    else:
        out["ratio_differences"] = 0
    for key in la.keys() | lb.keys():
        x, y = la[key], lb[key]
        if len(x) == len(y) == 1:
            out["residual_pairs"] += 1
            if cash:
                out["payable_date_differences"] += x[0][1] != y[0][1]
                out["amount_differences"] += x[0][2] != y[0][2]
            else:
                out["ratio_differences"] += x[0][1] != y[0][1]
        else:
            out["issuer_unmatched"] += len(x)
            out["archive_unmatched"] += len(y)
            out["ambiguous_groups"] += bool(x and y)
    return out


def reconcile(data, read_exact, issuer_csv):
    try:
        issuer = issuer_events(issuer_csv)
        archive = archive_events(data, read_exact)
        return {"status": "SOXL_COMPARED", "symbols": {
            s: {k: compare(issuer[s][k], archive[s][k], k == "cash") for k in ("cash", "split")}
            for s in SYMBOLS}, "coverage_limitation": "ISSUER_ROWS_ONLY_NO_TAX_OR_PIT_INFERENCE"}
    except Stop as exc:
        reason = str(exc)
    except ValueError as exc:
        reason = str(exc) if str(exc) in ("OBJECT_METADATA_MISMATCH", "OBJECT_BYTES_MISMATCH", "SOXL_READ_SCOPE_EXCEEDED") else "RECONCILIATION_INPUT_INVALID"
    except (KeyError, TypeError, AttributeError, InvalidOperation, csv.Error, ZeroDivisionError):
        reason = "RECONCILIATION_INPUT_INVALID"
    except Exception as exc:
        reason = {403: "GCS_OBJECT_FORBIDDEN", 404: "GCS_OBJECT_MISSING", 412: "GCS_GENERATION_PRECONDITION_FAILED"}.get(getattr(exc, "code", None), "READBACK_FAILED")
    return {"status": "PARKED", "reason_code": reason}
