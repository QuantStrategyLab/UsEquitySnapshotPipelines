"""Synthetic consumer-invariant tests, never real provider/mapping evidence.

Public dataclass reconstruction can change a status without authenticating the
record's raw binding. These tests repeat only the existing security-code
presence invariant; they do not claim protection against arbitrary forgery.
"""
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from us_equity_snapshot_pipelines import russell_1000_history as adapter
from test_iwb_sec_identifier_retention import _INDEX, _holding, _xml
from test_iwb_sec_placeholder_identity import _row


_EARLY = datetime(2040, 1, 2, 12, tzinfo=timezone.utc)
_LATE = _EARLY + timedelta(hours=1, microseconds=900000)


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("network is forbidden in identity bridge tests")
    monkeypatch.setattr("socket.socket", forbidden)
    monkeypatch.setattr("socket.create_connection", forbidden)


def _bind(*rows, observed_at=_EARLY, version_id="synthetic-bridge-gate"):
    return adapter.bind_iwb_sec_filing_input_version(
        index_html_bytes=_INDEX,
        nport_xml_bytes=_xml(*rows),
        observed_at=observed_at,
        accepted_timezone="America/New_York",
        version_id=version_id,
        qualification="synthetic",
    )


def _status_only_reconstruction(version, position=0):
    rows = list(version.holdings)
    rows[position] = replace(rows[position], status="resolved_equity")
    return replace(version, holdings=tuple(rows))


def _assert_full_bridge_rejects(version, consumer, monkeypatch):
    if consumer == "mss":
        def forbidden_constructor():
            raise AssertionError("invalid identity reached MSS constructor lookup")
        monkeypatch.setattr(adapter, "_iwb_sec_lazy_mss", forbidden_constructor)
        bridge = adapter.build_iwb_sec_point_in_time_universe_snapshot
    else:
        bridge = adapter.iwb_sec_universe_rows_for_ues
    with pytest.raises(adapter.IwbSecFilingAdapterError, match="canonical bridge rejected"):
        bridge(version)


@pytest.mark.parametrize("asset", ["EC", "EP"])
@pytest.mark.parametrize("codes", [(None, None), ("N/A", "N/A"), (None, "N/A"), ("N/A", None)])
@pytest.mark.parametrize("consumer", ["partial", "ues", "mss"])
def test_constructed_resolved_status_cannot_bypass_security_code_presence(asset, codes, consumer, monkeypatch):
    original = _bind(_row(asset=asset, cusip=codes[0], isin=codes[1]))
    assert original.holdings[0].status == "unresolved"
    assert "security_identifier_evidence_not_supplied" in original.holdings[0].reasons
    reconstructed = _status_only_reconstruction(original)
    assert reconstructed.raw_binding_sha256 == original.raw_binding_sha256
    assert reconstructed.observed_at == original.observed_at
    assert reconstructed.holdings[0].reasons == original.holdings[0].reasons
    assert not reconstructed.trading_eligible
    if consumer == "partial":
        assert adapter.iwb_sec_research_candidate_symbols(reconstructed) == ()
    else:
        _assert_full_bridge_rejects(reconstructed, consumer, monkeypatch)


@pytest.mark.parametrize("cusip,isin", [
    ("111111111", "N/A"), ("N/A", "ZZ1111111111"),
    ("0", None), ("None", None), ("n/a", None),
])
def test_existing_single_code_lexical_classification_and_ues_visibility_unchanged(cusip, isin):
    # The unusual values preserve lexical acceptance, not identifier validity.
    version = _bind(_row(cusip=cusip, isin=isin), observed_at=_LATE)
    holding, = version.holdings
    assert holding.status == "resolved_equity"
    assert adapter.iwb_sec_research_candidate_symbols(version) == ("FAKEA",)
    row, = adapter.iwb_sec_universe_rows_for_ues(version)
    assert dict(row) == {"symbol": "FAKEA", "visible_at": _LATE}
    assert holding.issuer_identifier is not None
    assert holding.security_title is not None
    assert "event_evidence_not_supplied" in holding.reasons
    assert "terminal_price_evidence_not_supplied" in holding.reasons
    assert not version.trading_eligible


@pytest.mark.parametrize("consumer", ["ues", "mss"])
def test_partial_helper_keeps_valid_row_while_complete_bridges_reject_missing_code(consumer, monkeypatch):
    original = _bind(_row(), _row(cusip="111111111", ticker="FAKEB"))
    reconstructed = _status_only_reconstruction(original)
    assert adapter.iwb_sec_research_candidate_symbols(reconstructed) == ("FAKEB",)
    _assert_full_bridge_rejects(reconstructed, consumer, monkeypatch)


def test_later_completed_synthetic_observation_never_becomes_earlier_availability():
    earlier = _bind(_holding(ticker=None), observed_at=_EARLY, version_id="synthetic-earlier")
    later = _bind(_holding(ticker="FAKEA"), observed_at=_LATE, version_id="synthetic-later")
    assert earlier.accepted_at == later.accepted_at
    assert earlier.report_period == later.report_period
    assert earlier.raw_binding_sha256 != later.raw_binding_sha256
    selected = adapter.select_iwb_sec_filing_input_version_at_cutoff(
        [later, earlier], decision_at=_LATE - timedelta(microseconds=1))
    assert selected is earlier
    assert adapter.iwb_sec_research_candidate_symbols(selected) == ()
    with pytest.raises(adapter.IwbSecFilingAdapterError, match="canonical bridge rejected"):
        adapter.iwb_sec_universe_rows_for_ues(selected)
    selected = adapter.select_iwb_sec_filing_input_version_at_cutoff([earlier, later], decision_at=_LATE)
    assert selected is later
    row, = adapter.iwb_sec_universe_rows_for_ues(selected)
    assert row["visible_at"] == _LATE
    assert not selected.trading_eligible


def test_known_observation_does_not_qualify_constructed_missing_code_record(monkeypatch):
    version = _status_only_reconstruction(_bind(_row(), observed_at=_LATE))
    with pytest.raises(adapter.IwbSecFilingAdapterError, match="no filing input version known"):
        adapter.select_iwb_sec_filing_input_version_at_cutoff(
            [version], decision_at=_LATE - timedelta(microseconds=1))
    selected = adapter.select_iwb_sec_filing_input_version_at_cutoff([version], decision_at=_LATE)
    assert selected is version
    assert adapter.iwb_sec_research_candidate_symbols(selected) == ()
    _assert_full_bridge_rejects(selected, "ues", monkeypatch)
