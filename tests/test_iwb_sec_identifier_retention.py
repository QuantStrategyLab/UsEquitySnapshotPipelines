"""Synthetic lexical evidence regressions; no source-security identifiers or network."""
from dataclasses import FrozenInstanceError, asdict, fields, replace
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
import socket
import unittest
from unittest.mock import patch

from us_equity_snapshot_pipelines import russell_1000_history as adapter


_WORKTREE_MODULE = Path(__file__).resolve().parents[1] / "src/us_equity_snapshot_pipelines/russell_1000_history.py"
assert Path(adapter.__file__).resolve() == _WORKTREE_MODULE.resolve()


_ISSUER = "Z" * 18 + "00"  # Lexical fixture, not a checksum-validity claim.
_OBSERVED = datetime(2026, 10, 4, 13, 49, 14, 780776, tzinfo=timezone.utc)
_INDEX = b"""<!DOCTYPE html><html><body>
<p>CIK: 0001100663</p><p>Accession Number: 0001004726-26-003726</p>
<p>Form: NPORT-P</p><p>Period of Report: 2026-03-31</p>
<p>Accepted: 2026-05-22 15:05:15</p></body></html>"""


def _holding(*, issuer=_ISSUER, title="Synthetic Common", other=None, isin=True,
             ticker=None, cusip="111111111", asset="EC", extra=""):
    identifiers = '<isin value="ZZ1111111111"/>' if isin else ""
    identifiers += other if other is not None else '<other otherDesc="Internal" value="synthetic-opaque"/>'
    if ticker is not None:
        identifiers += f'<ticker value="{ticker}"/>'
    return (f'<invstOrSec><name>Synthetic Issuer</name><cusip>{cusip}</cusip>'
            + (f'<lei>{issuer}</lei>' if issuer is not None else "")
            + (f'<title>{title}</title>' if title is not None else "")
            + f'<identifiers>{identifiers}</identifiers><assetCat>{asset}</assetCat>'
            + f'<issuerCat>CORP</issuerCat>{extra}</invstOrSec>')


def _xml(*holdings):
    return ('''<edgarSubmission xmlns="http://www.sec.gov/edgar/nport">
<headerData><submissionType>NPORT-P</submissionType><filerInfo>
<filer><issuerCredentials><cik>0001100663</cik></issuerCredentials></filer>
<seriesClassInfo><seriesId>S000004347</seriesId><classId>C000012077</classId></seriesClassInfo>
</filerInfo></headerData><formData><genInfo><regCik>0001100663</regCik>
<seriesId>S000004347</seriesId><repPdDate>2026-03-31</repPdDate></genInfo><invstOrSecs>'''
            + "".join(holdings) + '</invstOrSecs></formData></edgarSubmission>').encode()


def _parse(*holdings):
    return adapter.parse_iwb_sec_nport_xml_bytes(_xml(*holdings))[1]


def _bind(payload=None):
    return adapter.bind_iwb_sec_filing_input_version(
        index_html_bytes=_INDEX, nport_xml_bytes=payload or _xml(_holding()),
        observed_at=_OBSERVED, accepted_timezone="America/New_York",
        version_id="synthetic-retention", qualification="synthetic")


class IdentifierRetentionTests(unittest.TestCase):
    def setUp(self):
        def forbidden(*_args, **_kwargs):
            raise AssertionError("network is forbidden in lexical retention tests")
        for target in ("socket.socket", "socket.create_connection"):
            guard = patch(target, forbidden)
            guard.start()
            self.addCleanup(guard.stop)

    def test_retains_issuer_title_and_qualified_other_without_symbol(self):
        holding, = _parse(_holding())
        self.assertEqual(holding.issuer_identifier, _ISSUER)
        self.assertEqual(holding.security_title, "Synthetic Common")
        self.assertEqual([(o.description, o.value) for o in holding.other_identifiers],
                         [("Internal", "synthetic-opaque")])
        self.assertIsNone(holding.ticker)
        self.assertEqual(holding.status, "unresolved")
        self.assertIn("missing_ticker", holding.reasons)

    def test_preserves_source_order_case_and_repeated_qualified_identifiers(self):
        other = ('<other otherDesc="Internal" value="same"/>'
                 '<other otherDesc="INTERNAL" value="same"/>'
                 '<other otherDesc="Internal" value="same"/>')
        holding, = _parse(_holding(other=other))
        self.assertEqual([(o.description, o.value) for o in holding.other_identifiers],
                         [("Internal", "same"), ("INTERNAL", "same"), ("Internal", "same")])

    def test_opaque_other_never_becomes_ticker_even_when_label_says_ticker(self):
        holding, = _parse(_holding(other='<other otherDesc="ticker" value="FAKE.A"/>'))
        self.assertIsNone(holding.ticker)
        self.assertIn("missing_ticker", holding.reasons)

    def test_preserves_lexical_whitespace_without_identifier_normalization(self):
        holding, = _parse(_holding(title=" Synthetic Common ",
                                  other='<other otherDesc=" Internal " value=" opaque "/>'))
        self.assertEqual(holding.security_title, " Synthetic Common ")
        self.assertEqual(holding.other_identifiers[0].description, " Internal ")
        self.assertEqual(holding.other_identifiers[0].value, " opaque ")

    def test_whitespace_is_counted_in_lexical_lengths(self):
        for kwargs in ({"title": " " + "T" * 150},
                       {"other": f'<other otherDesc="D" value=" {"V" * 150}"/>'}):
            with self.subTest(field=next(iter(kwargs))):
                with self.assertRaisesRegex(adapter.IwbSecFilingAdapterError, "length"):
                    _parse(_holding(**kwargs))

    def test_unicode_length_limits_count_characters_not_utf8_bytes(self):
        holding, = _parse(_holding(title="界" * 150,
                                  other=f'<other otherDesc="界" value="{"界" * 150}"/>'))
        self.assertEqual(holding.security_title, "界" * 150)
        self.assertEqual(holding.other_identifiers[0].value, "界" * 150)

    def test_whitespace_only_new_evidence_rejects(self):
        for kwargs in ({"title": " "}, {"issuer": " "},
                       {"other": '<other otherDesc=" " value="V"/>'},
                       {"other": '<other otherDesc="Internal" value=" "/>'}):
            with self.subTest(field=next(iter(kwargs))):
                with self.assertRaises(adapter.IwbSecFilingAdapterError):
                    _parse(_holding(**kwargs))

    def test_issuer_field_preserves_na_rssd_and_lexical_lei_without_validation_claim(self):
        for issuer in ("N/A", "1234567890", _ISSUER):
            with self.subTest(shape=len(issuer)):
                holding, = _parse(_holding(issuer=issuer))
                self.assertEqual(holding.issuer_identifier, issuer)
                self.assertEqual(holding.status, "unresolved")

    def test_repeated_issuer_id_is_not_security_identity_conflict(self):
        a, b = _parse(_holding(ticker="FAKEA"),
                      _holding(ticker="FAKEB", cusip="222222222", isin=False))
        self.assertEqual(a.issuer_identifier, b.issuer_identifier)
        self.assertEqual((a.status, b.status), ("resolved_equity", "resolved_equity"))
        self.assertNotIn("identity_or_code_reuse_conflict", b.reasons)

    def test_existing_ticker_identity_conflict_rebuild_retains_evidence(self):
        a, b = _parse(_holding(ticker="FAKEA"), _holding(ticker="FAKEB"))
        for holding in (a, b):
            self.assertEqual(holding.status, "unresolved")
            self.assertIn("identity_or_code_reuse_conflict", holding.reasons)
            self.assertEqual(holding.issuer_identifier, _ISSUER)
            self.assertEqual(holding.security_title, "Synthetic Common")
            self.assertEqual(holding.other_identifiers[0].description, "Internal")
            self.assertEqual(holding.other_identifiers[0].value, "synthetic-opaque")

    def test_legacy_positional_constructor_hash_and_equality_stay_compatible(self):
        old = adapter.IwbSecHoldingRecord("Synthetic", None, None, None, "EC", "CORP",
                                        "unresolved", ("missing_ticker",), "USD", "1", "2", "NS", "3", None)
        evidence = replace(old, issuer_identifier=_ISSUER, security_title="Synthetic",
                           other_identifiers=(adapter.IwbSecOtherIdentifierRecord("Internal", "opaque"),))
        self.assertEqual(old, evidence)
        self.assertEqual(hash(old), hash(evidence))
        self.assertNotEqual(asdict(old), asdict(evidence))
        new_fields = fields(adapter.IwbSecHoldingRecord)[-3:]
        self.assertTrue(all(f.kw_only and not f.compare for f in new_fields))

    def test_asdict_has_explicit_additive_keys_and_qualified_pair(self):
        holding, = _parse(_holding())
        serialized = asdict(holding)
        self.assertEqual(set(serialized) - {f.name for f in fields(adapter.IwbSecHoldingRecord)[:-3]},
                         {"issuer_identifier", "security_title", "other_identifiers"})
        self.assertEqual(serialized["other_identifiers"],
                         ({"description": "Internal", "value": "synthetic-opaque"},))

    def test_evidence_records_are_immutable(self):
        holding, = _parse(_holding())
        self.assertIsInstance(holding.other_identifiers, tuple)
        with self.assertRaises(FrozenInstanceError):
            holding.other_identifiers[0].value = "changed"

    def test_missing_new_fields_remain_optional(self):
        holding, = _parse(_holding(issuer=None, title=None, other=""))
        self.assertIsNone(holding.issuer_identifier)
        self.assertIsNone(holding.security_title)
        self.assertEqual(holding.other_identifiers, ())
        self.assertEqual(holding.status, "unresolved")

    def test_new_evidence_does_not_change_existing_status_reasons(self):
        a, = _parse(_holding())
        b, = _parse(_holding(issuer=None, title=None, other=""))
        self.assertEqual((a.ticker, a.status, a.reasons), (b.ticker, b.status, b.reasons))

    def test_non_equity_remains_unsupported(self):
        holding, = _parse(_holding(ticker="SYNTHETIC_FUT", asset="DE"))
        self.assertEqual(holding.status, "unsupported")
        self.assertIn("non_equity_or_unsupported_asset_cat:DE", holding.reasons)

    def test_full_schema_lexical_maxima_are_accepted(self):
        holding, = _parse(_holding(title="T" * 150,
                                  other=f'<other otherDesc="{"D" * 150}" value="{"V" * 150}"/>'))
        self.assertEqual(len(holding.security_title), 150)
        self.assertEqual(len(holding.other_identifiers[0].value), 150)
        self.assertEqual(len(holding.other_identifiers[0].description), 150)

    def test_lexical_limits_reject_without_truncating(self):
        for kwargs in ({"issuer": "Z" * 21}, {"title": "T" * 151},
                       {"other": f'<other otherDesc="D" value="{"V" * 151}"/>'},
                       {"other": f'<other otherDesc="{"D" * 151}" value="V"/>'}):
            with self.subTest(field=next(iter(kwargs))):
                with self.assertRaisesRegex(adapter.IwbSecFilingAdapterError, "length"):
                    _parse(_holding(**kwargs))

    def test_identifier_budget_accepts100_and_rejects101_total_children(self):
        other = '<other otherDesc="Internal" value="opaque"/>' * 100
        holding, = _parse(_holding(other=other, isin=False))
        self.assertEqual(len(holding.other_identifiers), 100)
        with self.assertRaisesRegex(adapter.IwbSecFilingAdapterError, "identifier count"):
            _parse(_holding(other=other))  # One ISIN plus 100 other identifiers.

    def test_qualified_other_requires_both_attributes_and_attribute_only_structure(self):
        for other in ('<other otherDesc="Internal"/>', '<other value="V"/>',
                      '<other otherDesc="Internal" value=""/>', '<other otherDesc="" value="V"/>',
                      '<other otherDesc="Internal" value="V"><child/></other>',
                      '<other otherDesc="Internal" value="V">V</other>',
                      '<other otherDesc="Internal" value="V" unexpected="x"/>'):
            with self.subTest(structure=other):
                with self.assertRaises(adapter.IwbSecFilingAdapterError):
                    _parse(_holding(other=other))

    def test_duplicate_new_singletons_reject(self):
        for extra in ('<lei>N/A</lei>', '<title>Synthetic Second</title>'):
            with self.subTest(field=extra):
                with self.assertRaisesRegex(adapter.IwbSecFilingAdapterError, "duplicate"):
                    _parse(_holding(extra=extra))

    def test_new_consumed_fields_enforce_namespace(self):
        for field in ("lei", "title", "other"):
            payload = _xml(_holding()).replace(f'<{field}'.encode(),
                                               f'<x:{field} xmlns:x="urn:wrong"'.encode())
            payload = payload.replace(f'</{field}>'.encode(), f'</x:{field}>'.encode())
            with self.subTest(field=field):
                with self.assertRaisesRegex(adapter.IwbSecFilingAdapterError, "namespace mismatch"):
                    adapter.parse_iwb_sec_nport_xml_bytes(payload)

    def test_nested_other_is_not_accepted_as_qualified_direct_identifier(self):
        with self.assertRaisesRegex(adapter.IwbSecFilingAdapterError, "misplaced"):
            _parse(_holding(other='<container><other otherDesc="Internal" value="V"/></container>'))

    def test_nested_issuer_or_title_does_not_become_holding_evidence(self):
        holding, = _parse(_holding(issuer=None, title=None,
                                  extra='<derivativeInfo><lei>N/A</lei><title>Nested</title></derivativeInfo>'))
        self.assertIsNone(holding.issuer_identifier)
        self.assertIsNone(holding.security_title)

    def test_signature_title_in_official_common_namespace_remains_accepted(self):
        payload = _xml(_holding()).replace(
            b'</edgarSubmission>', b'<signature><c:title xmlns:c="http://www.sec.gov/edgar/nportcommon">'
            b'Synthetic Officer</c:title></signature></edgarSubmission>')
        _, holdings = adapter.parse_iwb_sec_nport_xml_bytes(payload)
        self.assertEqual(holdings[0].security_title, "Synthetic Common")

    def test_raw_hashes_and_current_capture_cutoff_stay_unchanged(self):
        payload = _xml(_holding())
        version = _bind(payload)
        self.assertEqual(version.xml_sha256, sha256(payload).hexdigest())
        self.assertEqual(version.index_sha256, sha256(_INDEX).hexdigest())
        self.assertEqual(version.raw_binding_sha256, sha256(_INDEX + b"\0" + payload).hexdigest())
        self.assertEqual(version.observed_at, _OBSERVED)
        for deadline in (version.accepted_at, datetime(2026, 3, 31, tzinfo=timezone.utc),
                         _OBSERVED - timedelta(microseconds=1)):
            with self.subTest(cutoff=deadline.isoformat()):
                with self.assertRaisesRegex(adapter.IwbSecFilingAdapterError, "no filing input version known"):
                    adapter.select_iwb_sec_filing_input_version_at_cutoff((version,), decision_at=deadline)
        self.assertIs(adapter.select_iwb_sec_filing_input_version_at_cutoff((version,), decision_at=_OBSERVED), version)

    def test_no_universe_or_trading_promotion_from_identifier_retention(self):
        version = _bind()
        self.assertFalse(version.trading_eligible)
        self.assertEqual(adapter.iwb_sec_research_candidate_symbols(version), ())
        with self.assertRaisesRegex(adapter.IwbSecFilingAdapterError, "canonical bridge rejected"):
            adapter.iwb_sec_universe_rows_for_ues(version)
        self.assertIn("event_evidence_not_supplied", version.incomplete_items[-1]["reasons"])
        self.assertEqual(set(version.incomplete_items[0]), {"kind", "name", "ticker", "status", "reasons"})

    def test_network_guard_is_active(self):
        with self.assertRaisesRegex(AssertionError, "network is forbidden"):
            socket.socket()
