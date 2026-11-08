import csv
import hashlib
import io
import json
from datetime import date, timedelta
from decimal import Decimal

import unittest
from soxl_reconcile import COUNTS, SYMBOLS, compare, reconcile


def fixture():
    bodies, members, inputs, rows = {}, [], [], []
    for symbol in SYMBOLS:
        cash = []
        for i in range(COUNTS[symbol][0]):
            ex = date(2022, 1, 1) + timedelta(days=i * 10)
            pay = ex + timedelta(days=2)
            cash.append({"symbol": symbol, "ex_date": str(ex), "payable_date": str(pay), "rate": "1.25"})
            basis = {"SOXL": "income", "SOXX": "as_paid", "BOXX": "capital_gain"}[symbol]
            host = {"SOXL": "www.direxion.com", "SOXX": "www.ishares.com", "BOXX": "funds.alphaarchitect.com"}[symbol]
            rows.append([symbol, "cash_distribution", str(ex), str(pay), "1.25", f"issuer_reported_usd_{basis}_per_share_unreconciled", "", "https://" + host + "/"])
        splits = []
        if symbol == "SOXX":
            splits.append({"symbol": symbol, "ex_date": "2024-03-07", "new_rate": 3, "old_rate": 1})
            rows.append([symbol, "split", "2024-03-07", "", "3", "shares_after_per_share_before", "2024-03-06", "https://www.ishares.com/"])
        name = f"actions/{symbol}/page-001.json"
        bodies[name] = json.dumps({"corporate_actions": {"cash_dividends": cash, "forward_splits": splits}}).encode()
        ref = {"generation": "1", "bytes": len(bodies[name]), "sha256": hashlib.sha256(bodies[name]).hexdigest()}
        members.append({"name": name, **ref})
        inputs.append({"symbol": symbol, "kind": "actions", "count": len(cash) + len(splits), "complete_pagination": True, "pages": [{"name": name, "bytes": ref["bytes"], "sha256": ref["sha256"]}]})
    manifest = {"schema_version": "qsl.research.raw_sip_input.v1", "source": "alpaca.stocks.bars.v2_and_corporate_actions.v1", "window_start": "2021-12-01", "window_end": "2026-08-25", "no_order": True, "research_only": True, "currency": "USD", "inputs": inputs}
    bodies["manifest.json"] = json.dumps(manifest).encode()
    def ref(name):
        body = bodies[name]
        return {"generation": "1", "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
    registration = {"schema_version": "rs04.archive-registration.v1", "producer_revision": "synthetic", "no_order": True, "manifest": {"name": "manifest.json", **ref("manifest.json")}, "members": members}
    bodies["registration.json"] = json.dumps(registration).encode()
    data = {"producer_revision": "synthetic", "registration": ref("registration.json"), "manifest": ref("manifest.json")}
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["symbol", "event_kind", "ex_or_split_date", "payable_date", "issuer_amount_or_ratio", "amount_basis", "issuer_effective_date", "source_url"])
    writer.writerows(rows)
    calls = []
    def read(name, reference):
        calls.append(name)
        body = bodies[name]
        generation, size, digest = reference
        if generation != "1" or size != len(body):
            raise ValueError("OBJECT_METADATA_MISMATCH")
        if digest != hashlib.sha256(body).hexdigest():
            raise ValueError("OBJECT_BYTES_MISMATCH")
        return body
    return data, out.getvalue(), calls, read


class ReconciliationTests(unittest.TestCase):
    def test_fixed_reads_and_exact_comparison(self):
        data, issuer, calls, read = fixture()
        result = reconcile(data, read, io.StringIO(issuer))
        assert result["status"] == "SOXL_COMPARED"
        assert calls == ["registration.json", "manifest.json"] + [f"actions/{s}/page-001.json" for s in SYMBOLS]
        assert result["symbols"]["SOXX"]["split"]["exact_matches"] == 1
        assert result["symbols"]["SOXL"]["cash"]["exact_matches"] == 16


    def test_bad_reference_parked(self):
        for key, value, reason in [("generation", "2", "OBJECT_METADATA_MISMATCH"), ("sha256", "0" * 64, "OBJECT_BYTES_MISMATCH"), ("bytes", 131073, "OBJECT_REFERENCE_INVALID")]:
            with self.subTest(key=key):
                data, issuer, _, read = fixture()
                data["registration"][key] = value
                assert reconcile(data, read, io.StringIO(issuer)) == {"status": "PARKED", "reason_code": reason}


    def test_binding_and_invalid_input(self):
        data, issuer, _, read = fixture()
        data["producer_revision"] = "different"
        assert reconcile(data, read, io.StringIO(issuer))["reason_code"] == "REGISTRATION_BINDING_MISMATCH"
        assert reconcile(data, read, io.StringIO(issuer.replace("1.25", "NaN", 1)))["reason_code"] == "EVENT_INVALID"


    def test_duplicates_and_ambiguity_are_counted(self):
        d = date(2024, 1, 2)
        a, b, c = ((d, d, Decimal(str(x))) for x in (1, 2, 3))
        result = compare([a, a, b], [a, c, c], True)
        assert result["exact_matches"] == 1
        assert result["ambiguous_groups"] == 1
        assert result["issuer_unmatched"] == result["archive_unmatched"] == 2
        assert result["residual_pairs"] == 0


    def test_unique_residual_reports_exact_differences(self):
        d = date(2024, 1, 2)
        result = compare([(d, d, Decimal("1"))], [(d, d + timedelta(days=1), Decimal("2"))], True)
        assert result["amount_differences"] == result["payable_date_differences"] == 1


    def test_reader_scope_error_is_fixed(self):
        data, issuer, _, _ = fixture()
        def reject(name, ref):
            raise ValueError("SOXL_READ_SCOPE_EXCEEDED")
        assert reconcile(data, reject, io.StringIO(issuer))["reason_code"] == "SOXL_READ_SCOPE_EXCEEDED"


    def test_optional_symbol_empty_terminal_and_ambiguous_identity(self):
        data, issuer, _, read = fixture()
        def altered(name, ref):
            body = read(name, ref)
            if name == "actions/SOXL/page-001.json":
                page = json.loads(body)
                page["next_page_token"] = ""
                del page["corporate_actions"]["cash_dividends"][0]["symbol"]
                return json.dumps(page).encode()
            return body
        assert reconcile(data, altered, io.StringIO(issuer))["status"] == "SOXL_COMPARED"
        def duplicate(name, ref):
            body = altered(name, ref)
            if name == "actions/SOXL/page-001.json":
                page = json.loads(body)
                rows = page["corporate_actions"]["cash_dividends"]
                rows[1]["ex_date"] = rows[0]["ex_date"]
                return json.dumps(page).encode()
            return body
        assert reconcile(data, duplicate, io.StringIO(issuer))["reason_code"] == "ACTION_SAME_DAY_IDENTITY_UNVERIFIED"


    def test_wrong_action_and_non_list_category_park(self):
        data, issuer, _, read = fixture()
        def altered(name, ref):
            body = read(name, ref)
            if name == "actions/SOXL/page-001.json":
                page = json.loads(body)
                page["corporate_actions"]["cash_mergers"] = [{}]
                return json.dumps(page).encode()
            return body
        assert reconcile(data, altered, io.StringIO(issuer))["reason_code"] == "ACTION_SCOPE_MISMATCH"
