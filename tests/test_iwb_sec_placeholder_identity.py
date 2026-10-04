"""Synthetic exact-N/A identity regression and conservative equity evidence gate."""
import pytest

from us_equity_snapshot_pipelines import russell_1000_history as adapter
from test_iwb_sec_identifier_retention import _bind, _holding, _parse, _xml


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("network is forbidden in placeholder identity tests")
    monkeypatch.setattr("socket.socket", forbidden)
    monkeypatch.setattr("socket.create_connection", forbidden)


def _row(*, asset="EC", cusip="N/A", isin="N/A", ticker="FAKEA", other=""):
    identifiers = (f'<isin value="{isin}"/>' if isin is not None else "") + other
    row = _holding(asset=asset, cusip=cusip or "N/A", isin=False, other=identifiers, ticker=ticker)
    return row.replace("<cusip>N/A</cusip>", "") if cusip is None else row


def test_exact_na_does_not_coalesce_unsupported_derivative_identities():
    holdings = _parse(_row(asset="DE"), _row(asset="DE", ticker="FAKEB"))
    for h in holdings:
        assert h.cusip == h.isin == "N/A"
        assert h.status == "unsupported"
        assert "identity_or_code_reuse_conflict" not in h.reasons
        assert "non_equity_or_unsupported_asset_cat:DE" in h.reasons
        assert "event_evidence_not_supplied" in h.reasons
        assert "terminal_price_evidence_not_supplied" in h.reasons
    version = _bind(_xml(_row(asset="DE"), _row(asset="DE", ticker="FAKEB")))
    assert not version.trading_eligible
    assert adapter.iwb_sec_research_candidate_symbols(version) == ()
    with pytest.raises(adapter.IwbSecFilingAdapterError, match="canonical bridge rejected"):
        adapter.iwb_sec_universe_rows_for_ues(version)


@pytest.mark.parametrize("asset", ["EC", "EP"])
@pytest.mark.parametrize("codes", [("N/A", "N/A"), (None, None), ("N/A", None), (None, "N/A")])
def test_equities_without_nonplaceholder_security_code_stay_unresolved(asset, codes):
    rows = [_row(asset=asset, cusip=codes[0], isin=codes[1], ticker=s) for s in ("FAKEA", "FAKEB")]
    holdings = _parse(*rows)
    for h in holdings:
        assert h.status == "unresolved"
        assert "security_identifier_evidence_not_supplied" in h.reasons
        assert "identity_or_code_reuse_conflict" not in h.reasons
    version = _bind(_xml(*rows))
    assert not version.trading_eligible
    assert adapter.iwb_sec_research_candidate_symbols(version) == ()
    with pytest.raises(adapter.IwbSecFilingAdapterError, match="canonical bridge rejected"):
        adapter.iwb_sec_universe_rows_for_ues(version)


def test_one_cusip_code_suffices_for_existing_lexical_identity_classification():
    holdings = _parse(_row(cusip="111111111"), _row(cusip="222222222", ticker="FAKEB"))
    assert [h.status for h in holdings] == ["resolved_equity", "resolved_equity"]
    assert all("security_identifier_evidence_not_supplied" not in h.reasons for h in holdings)
    assert all("identity_or_code_reuse_conflict" not in h.reasons for h in holdings)


def test_one_isin_code_suffices_for_existing_lexical_identity_classification():
    holdings = _parse(_row(isin="ZZ1111111111"), _row(isin="ZZ2222222222", ticker="FAKEB"))
    assert [h.status for h in holdings] == ["resolved_equity", "resolved_equity"]
    assert all("identity_or_code_reuse_conflict" not in h.reasons for h in holdings)


@pytest.mark.parametrize("slot", ["cusip", "isin"])
@pytest.mark.parametrize("value", ["0", "000000000", "None", "NULL", "n/a", "NA"])
def test_only_exact_documented_na_is_excluded_not_guessed_sentinels(slot, value):
    kwargs = {slot: value}
    holdings = _parse(_row(**kwargs), _row(ticker="FAKEB", **kwargs))
    assert all("identity_or_code_reuse_conflict" in h.reasons for h in holdings)
    assert all("security_identifier_evidence_not_supplied" not in h.reasons for h in holdings)
    # This preserves opaque legacy behavior, not checksum or identifier validity.


@pytest.mark.parametrize("slot,value", [("cusip", "111111111"), ("isin", "ZZ1111111111")])
def test_real_lexical_code_collisions_remain_conflicts(slot, value):
    holdings = _parse(_row(**{slot: value}), _row(ticker="FAKEB", **{slot: value}))
    assert all(h.status == "unresolved" for h in holdings)
    assert all("identity_or_code_reuse_conflict" in h.reasons for h in holdings)
    assert all(h.issuer_identifier is not None and h.security_title is not None for h in holdings)


def test_same_ticker_with_two_nonplaceholder_security_codes_still_conflicts():
    holdings = _parse(_row(cusip="111111111"), _row(cusip="222222222"))
    assert all(h.status == "unresolved" for h in holdings)
    assert all("identity_or_code_reuse_conflict" in h.reasons for h in holdings)


def test_issuer_title_and_qualified_other_never_satisfy_security_code_gate():
    holding, = _parse(_row(other='<other otherDesc="CUSIP" value="111111111"/>'))
    assert holding.issuer_identifier is not None
    assert holding.security_title is not None
    assert holding.other_identifiers[0].description == "CUSIP"
    assert holding.status == "unresolved"
    assert "security_identifier_evidence_not_supplied" in holding.reasons


@pytest.mark.parametrize("asset,ticker,expected", [("STIV", None, "unresolved"),
                                                  ("STIV", "FAKEA", "unsupported"),
                                                  ("DE", None, "unresolved")])
def test_nonnequity_and_missing_ticker_blocks_are_preserved(asset, ticker, expected):
    holding, = _parse(_row(asset=asset, ticker=ticker))
    assert holding.status == expected
    assert "security_identifier_evidence_not_supplied" not in holding.reasons
    assert f"non_equity_or_unsupported_asset_cat:{asset}" in holding.reasons


def test_missing_ticker_with_real_code_remains_unresolved():
    holding, = _parse(_row(cusip="111111111", ticker=None))
    assert holding.status == "unresolved"
    assert "missing_ticker" in holding.reasons
    assert "security_identifier_evidence_not_supplied" not in holding.reasons
