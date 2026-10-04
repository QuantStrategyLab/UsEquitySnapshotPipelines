from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from us_equity_snapshot_pipelines.russell_1000_history import (
    IWB_SEC_FILING_CIK,
    IWB_SEC_FILING_CLASS_ID,
    IWB_SEC_FILING_SERIES_ID,
    IWB_SEC_FILING_TICKER,
    IwbSecFilingAdapterError,
    bind_iwb_sec_filing_input_version,
    build_iwb_sec_point_in_time_universe_snapshot,
    iwb_sec_research_candidate_symbols,
    iwb_sec_universe_rows_for_ues,
    parse_iwb_sec_filing_index_html,
    parse_iwb_sec_nport_xml_bytes,
    select_iwb_sec_filing_input_version_at_cutoff,
    validate_iwb_sec_universe_snapshot_for_decision,
)
from us_equity_snapshot_pipelines.russell_1000_proxy_long_history import (
    build_proxy_universe_history,
    main,
)

# Guard: tests must import the worktree package, not an older installed copy.
_WORKTREE_SRC = Path(__file__).resolve().parents[1] / "src" / "us_equity_snapshot_pipelines" / "russell_1000_history.py"
assert Path(bind_iwb_sec_filing_input_version.__code__.co_filename).resolve() == _WORKTREE_SRC.resolve()


def _price_rows(
    symbols: dict[str, dict[str, object]],
    *,
    start: str = "2020-01-02",
    periods: int = 70,
) -> pd.DataFrame:
    rows = []
    dates = pd.bdate_range(start, periods=periods)
    for idx, as_of in enumerate(dates):
        for symbol, config in symbols.items():
            market_value = config.get("market_value", 100.0)
            if callable(market_value):
                market_value = market_value(as_of, idx)
            rows.append(
                {
                    "symbol": symbol,
                    "sector": config.get("sector", "unknown"),
                    "as_of": as_of.date().isoformat(),
                    "close": float(config.get("close", 50.0)) + idx * 0.01,
                    "volume": int(config.get("volume", 1_000_000)),
                    "market_value": float(market_value),
                }
            )
    return pd.DataFrame(rows)


def test_proxy_universe_uses_point_in_time_market_value_without_future_leakage() -> None:
    prices = _price_rows(
        {
            "AAA": {"sector": "Technology", "market_value": 1_000.0},
            "BBB": {"sector": "Financials", "market_value": 800.0},
            "CCC": {
                "sector": "Health Care",
                "market_value": lambda as_of, _idx: 2_000.0 if as_of >= pd.Timestamp("2020-02-03") else 100.0,
            },
            "QQQ": {"sector": "benchmark", "market_value": 99_000.0},
            "SPY": {"sector": "benchmark", "market_value": 99_000.0},
            "BOXX": {"sector": "cash", "market_value": 99_000.0},
        },
        periods=70,
    )

    result = build_proxy_universe_history(
        prices,
        universe_size=2,
        min_price_usd=1.0,
        min_adv20_usd=0.0,
        min_history_days=5,
    )

    universe = result.universe_history
    first_start = universe["start_date"].min()
    first_active = universe.loc[universe["start_date"].eq(first_start), "symbol"].tolist()
    second_start = sorted(universe["start_date"].drop_duplicates())[1]
    second_active = universe.loc[universe["start_date"].eq(second_start), "symbol"].tolist()

    assert result.metadata["proxy_method"] == "point_in_time_market_value"
    assert first_start == pd.Timestamp("2020-02-03")
    assert first_active == ["AAA", "BBB"]
    assert second_active[0] == "CCC"
    assert {"QQQ", "SPY", "BOXX"}.isdisjoint(set(universe["symbol"]))
    assert (universe["start_date"] > universe["rank_as_of"]).all()


def test_proxy_universe_falls_back_to_adv20_when_market_value_is_missing() -> None:
    prices = _price_rows(
        {
            "HIGH": {"volume": 2_000_000},
            "MID": {"volume": 1_000_000},
            "LOW": {"volume": 100_000},
            "QQQ": {"volume": 5_000_000},
        },
        periods=35,
    ).drop(columns=["market_value"])

    result = build_proxy_universe_history(
        prices,
        universe_size=2,
        min_price_usd=1.0,
        min_adv20_usd=0.0,
        min_history_days=5,
        excluded_symbols=("QQQ",),
    )

    first_start = result.universe_history["start_date"].min()
    active = result.universe_history.loc[result.universe_history["start_date"].eq(first_start), "symbol"].tolist()

    assert result.metadata["proxy_method"] == "adv20_liquidity_proxy"
    assert result.metadata["ranking_column"] == "adv20_usd"
    assert active == ["HIGH", "MID"]


def test_proxy_research_cli_writes_proxy_outputs_without_validation(tmp_path) -> None:
    prices = _price_rows(
        {
            "AAA": {"market_value": 1_000.0},
            "BBB": {"market_value": 800.0},
            "QQQ": {"market_value": 99_000.0},
        },
        periods=35,
    )
    prices_path = tmp_path / "prices.csv"
    output_dir = tmp_path / "output"
    prices.to_csv(prices_path, index=False)

    exit_code = main(
        [
            "--prices",
            str(prices_path),
            "--output-dir",
            str(output_dir),
            "--skip-validation",
            "--universe-size",
            "2",
            "--min-price-usd",
            "1",
            "--min-adv20-usd",
            "0",
            "--min-history-days",
            "5",
        ]
    )

    assert exit_code == 0
    assert (output_dir / "russell_1000_proxy_universe_history.csv").exists()
    assert (output_dir / "russell_1000_proxy_metadata.csv").exists()


_SYNTHETIC_ACCESSION = "0001004726-26-003726"
_SYNTHETIC_ACCESSION_B = "0001004726-26-003800"
_NY = ZoneInfo("America/New_York")
_NS = "http://example.invalid/synthetic-nport-subset"


@pytest.fixture
def _mss_contract():
    # MSS is an optional bridge, not an installed UESP dependency. Core parser
    # and UES tests always run; only real MSS integration cases require it.
    return pytest.importorskip(
        "market_signal_sources.artifacts.point_in_time_universe",
        reason="optional MSS contract is not installed",
    )


@pytest.fixture
def _no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    import socket

    def _blocked(*_args, **_kwargs):
        raise AssertionError("network/socket use is forbidden in offline IWB SEC adapter tests")

    monkeypatch.setattr(socket, "socket", _blocked)
    monkeypatch.setattr(socket, "create_connection", _blocked)

_CLEAN_HOLDINGS = """
      <invstOrSec>
        <name>Synthetic Equity One</name>
        <identifiers>
          <cusip>037833100</cusip>
          <isin>US0378331005</isin>
          <tickers><ticker>AAPL</ticker></tickers>
        </identifiers>
        <assetCat>EC</assetCat>
        <issuerCat>CORP</issuerCat>
      </invstOrSec>
      <invstOrSec>
        <name>Synthetic Equity Two</name>
        <identifiers>
          <cusip>594918104</cusip>
          <tickers><ticker>MSFT</ticker></tickers>
        </identifiers>
        <assetCat>EC</assetCat>
        <issuerCat>CORP</issuerCat>
      </invstOrSec>
"""

_MIXED_HOLDINGS = (
    _CLEAN_HOLDINGS
    + """
      <invstOrSec>
        <name>Missing Ticker Corp</name>
        <identifiers><cusip>111111111</cusip></identifiers>
        <assetCat>EC</assetCat>
        <issuerCat>CORP</issuerCat>
      </invstOrSec>
      <invstOrSec>
        <name>Synthetic Derivative</name>
        <identifiers><tickers><ticker>DERX</ticker></tickers></identifiers>
        <assetCat>DER</assetCat>
        <issuerCat>CORP</issuerCat>
      </invstOrSec>
      <invstOrSec>
        <name>Cash Residual</name>
        <identifiers><tickers><ticker>CASHX</ticker></tickers></identifiers>
        <assetCat>CASH</assetCat>
        <issuerCat>CORP</issuerCat>
      </invstOrSec>
      <invstOrSec>
        <name>Contingent Consideration Claim</name>
        <identifiers><tickers><ticker>CVR1</ticker></tickers></identifiers>
        <assetCat>EC</assetCat>
        <issuerCat>CORP</issuerCat>
      </invstOrSec>
      <invstOrSec>
        <name>Spinoff Right</name>
        <identifiers><tickers><ticker>SPIN</ticker></tickers></identifiers>
        <assetCat>EC</assetCat>
        <issuerCat>CORP</issuerCat>
      </invstOrSec>
"""
)


def _synthetic_index_html_colon(
    *,
    cik: str = IWB_SEC_FILING_CIK,
    accession: str = _SYNTHETIC_ACCESSION,
    form_type: str = "NPORT-P",
    report_period: str = "2026-03-31",
    accepted: str = "2026-05-22 15:05:15",
) -> bytes:
    return f"""<!DOCTYPE html><html><body>
<p>CIK: {cik}</p>
<p>Accession Number: {accession}</p>
<p>Form: {form_type}</p>
<p>Period of Report: {report_period}</p>
<p>Accepted: {accepted}</p>
</body></html>""".encode("utf-8")


def _synthetic_index_html_sec_style(
    *,
    cik: str = IWB_SEC_FILING_CIK,
    accession: str = _SYNTHETIC_ACCESSION,
    form_type: str = "NPORT-P",
    primary_type: str | None = None,
    report_period: str = "2026-03-31",
    accepted: str = "2026-05-22 15:05:15",
    duplicate_cik: str | None = None,
    include_primary_table: bool = True,
    include_exhibit_row: bool = True,
) -> bytes:
    # Synthetic labeled-div/table subset; not a verified live SEC schema sample.
    extra_cik = ""
    if duplicate_cik is not None:
        extra_cik = f'<div class="infoHead">CIK</div><div class="info">{duplicate_cik}</div>'
    primary = form_type if primary_type is None else primary_type
    if include_primary_table:
        exhibit_row = (
            "<tr><td>2</td><td>Complete submission text file</td><td>TEXT</td></tr>"
            if include_exhibit_row
            else ""
        )
        table_html = f"""
<table class="tableFile">
  <tr><th>Seq</th><th>Description</th><th>Type</th></tr>
  <tr><td>1</td><td>Primary Document</td><td>{primary}</td></tr>
  {exhibit_row}
</table>"""
    else:
        table_html = '<table class="tableFile"><tr><th>Seq</th><th>Description</th><th>Type</th></tr></table>'
    return f"""<!DOCTYPE html><html><body>
<div class="formHeader">Form {form_type}</div>
<div class="companyInfo">Example Trust (CIK <a href="/cgi-bin/browse-edgar?CIK={cik}">{cik}</a>)</div>
{table_html}
<div class="formGrouping">
  <div class="infoHead">Accession Number</div><div class="info">{accession}</div>
  <div class="infoHead">Period of Report</div><div class="info">{report_period}</div>
  <div class="infoHead">Filing Date</div><div class="info">2026-05-22</div>
  <div class="infoHead">Accepted</div><div class="info">{accepted}</div>
  {extra_cik}
</div>
</body></html>""".encode("utf-8")


def _synthetic_nport_xml(
    *,
    cik: str = IWB_SEC_FILING_CIK,
    series_id: str = IWB_SEC_FILING_SERIES_ID,
    class_id: str = IWB_SEC_FILING_CLASS_ID,
    ticker: str = IWB_SEC_FILING_TICKER,
    report_period: str = "2026-03-31",
    accession: str | None = _SYNTHETIC_ACCESSION,
    holdings_xml: str | None = None,
    submission_type: str | None = "NPORT-P",
    extra_after_header: str = "",
) -> bytes:
    holdings_xml = _CLEAN_HOLDINGS if holdings_xml is None else holdings_xml
    accession_xml = f"<accessionNumber>{accession}</accessionNumber>" if accession else ""
    submission_xml = f"<submissionType>{submission_type}</submissionType>" if submission_type else ""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="{_NS}">
  <headerData>
    <filerInfo>
      <filer>
        <issuerCredentials>
          <cik>{cik}</cik>
        </issuerCredentials>
      </filer>
      {accession_xml}
      <seriesClassInfo>
        <seriesId>{series_id}</seriesId>
        <classId>{class_id}</classId>
        <ticker>{ticker}</ticker>
      </seriesClassInfo>
    </filerInfo>
  </headerData>
  {extra_after_header}
  <formData>
    {submission_xml}
    <genInfo>
      <repPdDate>{report_period}</repPdDate>
    </genInfo>
    <invstOrSecs>
      {holdings_xml}
    </invstOrSecs>
  </formData>
</edgarSubmission>
""".encode("utf-8")


def _bind(
    *,
    observed_at: datetime,
    version_id: str = "v1",
    index_html: bytes | None = None,
    xml: bytes | None = None,
    accepted_timezone: str | ZoneInfo | None = "America/New_York",
    qualification: str = "synthetic",
    holdings_xml: str | None = None,
):
    return bind_iwb_sec_filing_input_version(
        index_html_bytes=index_html or _synthetic_index_html_colon(),
        nport_xml_bytes=xml or _synthetic_nport_xml(holdings_xml=holdings_xml),
        observed_at=observed_at,
        version_id=version_id,
        accepted_timezone=accepted_timezone,
        qualification=qualification,
    )


def test_iwb_sec_offline_adapter_binds_identity_and_keeps_mixed_rows() -> None:
    observed = datetime(2026, 5, 22, 20, 0, 0, tzinfo=timezone.utc)
    version = _bind(observed_at=observed, holdings_xml=_MIXED_HOLDINGS)

    assert version.cik == IWB_SEC_FILING_CIK
    assert version.series_id == IWB_SEC_FILING_SERIES_ID
    assert version.class_id == IWB_SEC_FILING_CLASS_ID
    assert version.ticker == IWB_SEC_FILING_TICKER
    assert version.qualification == "synthetic"
    assert version.trading_eligible is False
    assert version.schema_claim == "synthetic_subset_not_verified_sec_sample"
    assert len(version.holdings) == 7
    assert any(holding.status == "unresolved" and "missing_ticker" in holding.reasons for holding in version.holdings)
    assert any("non_equity_or_unsupported_asset_cat:DER" in holding.reasons for holding in version.holdings)
    assert any("contingent_consideration_unresolved" in holding.reasons for holding in version.holdings)
    assert any("spinoff_unresolved" in holding.reasons for holding in version.holdings)
    assert "event_evidence_not_supplied" in version.incomplete_items[-1]["reasons"]
    assert iwb_sec_research_candidate_symbols(version) == ("AAPL", "MSFT")
    with pytest.raises(IwbSecFilingAdapterError, match="canonical bridge rejected"):
        build_iwb_sec_point_in_time_universe_snapshot(version)
    with pytest.raises(IwbSecFilingAdapterError, match="canonical bridge rejected"):
        iwb_sec_universe_rows_for_ues(version)
    with pytest.raises(TypeError):
        version.incomplete_items[-1]["reasons"] = ("mutated",)  # type: ignore[index]


def test_iwb_sec_canonical_bridge_requires_fully_resolved_equity_only() -> None:
    observed = datetime(2026, 5, 22, 20, 0, 0, tzinfo=timezone.utc)
    clean = _bind(observed_at=observed, holdings_xml=_CLEAN_HOLDINGS)
    assert {row["symbol"] for row in iwb_sec_universe_rows_for_ues(clean)} == {"AAPL", "MSFT"}
    assert clean.trading_eligible is False
    assert any(
        item["kind"] == "adapter_incomplete" and "event_evidence_not_supplied" in item["reasons"]
        for item in clean.incomplete_items
    )

    reuse = """
      <invstOrSec>
        <name>One</name>
        <identifiers><cusip>037833100</cusip><tickers><ticker>AAPL</ticker></tickers></identifiers>
        <assetCat>EC</assetCat>
      </invstOrSec>
      <invstOrSec>
        <name>Reuse</name>
        <identifiers><cusip>037833100</cusip><tickers><ticker>APPLX</ticker></tickers></identifiers>
        <assetCat>EC</assetCat>
      </invstOrSec>
"""
    conflicted = _bind(observed_at=observed, version_id="reuse", holdings_xml=reuse)
    assert any("identity_or_code_reuse_conflict" in holding.reasons for holding in conflicted.holdings)
    with pytest.raises(IwbSecFilingAdapterError, match="canonical bridge rejected"):
        build_iwb_sec_point_in_time_universe_snapshot(conflicted)


def test_iwb_sec_mss_bridge_resolved_membership(_mss_contract, _no_network) -> None:
    version = _bind(
        observed_at=datetime(2026, 5, 22, 20, 0, tzinfo=timezone.utc),
        holdings_xml=_CLEAN_HOLDINGS,
    )
    snapshot = build_iwb_sec_point_in_time_universe_snapshot(version)
    assert snapshot["constituents"] == ["AAPL", "MSFT"]
    assert _mss_contract.validate_point_in_time_universe_snapshot(snapshot) == snapshot
    validate_iwb_sec_universe_snapshot_for_decision(snapshot, decision_at=version.observed_at)
    assert version.trading_eligible is False


def test_iwb_sec_mss_missing_dependency_is_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    import builtins

    original_import = builtins.__import__

    def _without_mss(name, *args, **kwargs):
        if name.startswith("market_signal_sources"):
            raise ModuleNotFoundError("synthetic optional MSS unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _without_mss)
    version = _bind(
        observed_at=datetime(2026, 5, 22, 20, 0, tzinfo=timezone.utc),
        holdings_xml=_CLEAN_HOLDINGS,
    )
    with pytest.raises(IwbSecFilingAdapterError, match="MarketSignalSources.*not importable"):
        build_iwb_sec_point_in_time_universe_snapshot(version)


def test_iwb_sec_rejects_wrong_growth_series_and_index_xml_mismatch() -> None:
    observed = datetime(2026, 5, 22, 20, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(IwbSecFilingAdapterError, match="wrong growth"):
        _bind(observed_at=observed, xml=_synthetic_nport_xml(series_id="S000999999", ticker="IWF"))
    with pytest.raises(IwbSecFilingAdapterError, match="report period mismatch"):
        _bind(observed_at=observed, xml=_synthetic_nport_xml(report_period="2026-02-28"))


def test_iwb_sec_accepted_timezone_rules_and_eastern_dst() -> None:
    with pytest.raises(IwbSecFilingAdapterError, match="Accepted lacks timezone"):
        parse_iwb_sec_filing_index_html(_synthetic_index_html_colon())
    offset_index = parse_iwb_sec_filing_index_html(
        _synthetic_index_html_colon(accepted="2026-05-22 15:05:15-04:00")
    )
    assert offset_index.accepted_at.utcoffset() == timedelta(hours=-4)
    with pytest.raises(IwbSecFilingAdapterError, match="does not exist"):
        parse_iwb_sec_filing_index_html(
            _synthetic_index_html_colon(accepted="2026-03-08 02:30:00"),
            accepted_timezone="America/New_York",
        )
    with pytest.raises(IwbSecFilingAdapterError, match="ambiguous"):
        parse_iwb_sec_filing_index_html(
            _synthetic_index_html_colon(accepted="2026-11-01 01:30:00"),
            accepted_timezone="America/New_York",
        )


def test_iwb_sec_subsecond_decision_floor_prevents_lookahead(_mss_contract) -> None:
    observed = datetime(2026, 5, 22, 20, 0, 0, 900000, tzinfo=timezone.utc)
    version = _bind(observed_at=observed, holdings_xml=_CLEAN_HOLDINGS)
    snapshot = build_iwb_sec_point_in_time_universe_snapshot(version)
    assert snapshot["available_at"] == "2026-05-22T20:00:01Z"

    earlier_fractional_decision = datetime(2026, 5, 22, 20, 0, 0, 100000, tzinfo=timezone.utc)
    with pytest.raises(IwbSecFilingAdapterError, match="unavailable"):
        validate_iwb_sec_universe_snapshot_for_decision(snapshot, decision_at=earlier_fractional_decision)

    exact_second_observed = datetime(2026, 5, 22, 20, 0, 0, tzinfo=timezone.utc)
    exact_version = _bind(observed_at=exact_second_observed, version_id="exact", holdings_xml=_CLEAN_HOLDINGS)
    exact_snapshot = build_iwb_sec_point_in_time_universe_snapshot(exact_version)
    assert exact_snapshot["available_at"] == "2026-05-22T20:00:00Z"
    validate_iwb_sec_universe_snapshot_for_decision(exact_snapshot, decision_at=exact_second_observed)
    with pytest.raises(IwbSecFilingAdapterError, match="unavailable"):
        validate_iwb_sec_universe_snapshot_for_decision(
            exact_snapshot,
            decision_at=exact_second_observed - timedelta(seconds=1),
        )


def test_iwb_sec_observation_cutoff_revisions_and_accepted_ordering() -> None:
    accepted_newer = datetime(2026, 5, 22, 15, 5, 15, tzinfo=_NY)
    accepted_older = datetime(2026, 5, 20, 12, 0, 0, tzinfo=_NY)
    early = accepted_newer + timedelta(minutes=10)
    late = accepted_newer + timedelta(hours=5)

    v1 = _bind(observed_at=early, version_id="v1", holdings_xml=_CLEAN_HOLDINGS)
    revised = """
      <invstOrSec>
        <name>Synthetic Equity One Revised</name>
        <identifiers><cusip>037833100</cusip><tickers><ticker>AAPL</ticker></tickers></identifiers>
        <assetCat>EC</assetCat><issuerCat>CORP</issuerCat>
      </invstOrSec>
      <invstOrSec>
        <name>Synthetic Equity Three</name>
        <identifiers><cusip>023135106</cusip><tickers><ticker>AMZN</ticker></tickers></identifiers>
        <assetCat>EC</assetCat><issuerCat>CORP</issuerCat>
      </invstOrSec>
"""
    v2 = _bind(
        observed_at=late,
        version_id="v2",
        xml=_synthetic_nport_xml(holdings_xml=revised),
    )

    with pytest.raises(IwbSecFilingAdapterError, match="timezone-aware"):
        select_iwb_sec_filing_input_version_at_cutoff([v1], decision_at=datetime(2026, 5, 23))  # type: ignore[arg-type]
    before_observation = accepted_newer + timedelta(minutes=1)
    with pytest.raises(IwbSecFilingAdapterError, match="no filing input version known"):
        select_iwb_sec_filing_input_version_at_cutoff([v1, v2], decision_at=before_observation)

    selected_old = select_iwb_sec_filing_input_version_at_cutoff([v1, v2], decision_at=early)
    assert selected_old.version_id == "v1"
    assert {row["symbol"] for row in iwb_sec_universe_rows_for_ues(selected_old)} == {"AAPL", "MSFT"}
    selected_new = select_iwb_sec_filing_input_version_at_cutoff([v1, v2], decision_at=late)
    assert selected_new.version_id == "v2"
    assert {row["symbol"] for row in iwb_sec_universe_rows_for_ues(selected_new)} == {"AAPL", "AMZN"}

    conflict_same_time = _bind(
        observed_at=late,
        version_id="v2-conflict",
        xml=_synthetic_nport_xml(holdings_xml=revised),
    )
    with pytest.raises(IwbSecFilingAdapterError, match="equal-time conflicting"):
        select_iwb_sec_filing_input_version_at_cutoff([v2, conflict_same_time], decision_at=late)

    # Older report period arriving later is a mixed family and must reject.
    older_report_late = _bind(
        observed_at=datetime(2026, 6, 2, 12, 0, tzinfo=timezone.utc),
        version_id="march",
        index_html=_synthetic_index_html_colon(report_period="2026-03-31", accepted="2026-05-22 15:05:15"),
        xml=_synthetic_nport_xml(report_period="2026-03-31"),
    )
    newer_report_earlier_obs = _bind(
        observed_at=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
        version_id="april",
        index_html=_synthetic_index_html_colon(
            accession=_SYNTHETIC_ACCESSION_B,
            report_period="2026-04-30",
            accepted="2026-05-20 12:00:00",
        ),
        xml=_synthetic_nport_xml(
            accession=_SYNTHETIC_ACCESSION_B,
            report_period="2026-04-30",
        ),
    )
    with pytest.raises(IwbSecFilingAdapterError, match="mixed report-period"):
        select_iwb_sec_filing_input_version_at_cutoff(
            [older_report_late, newer_report_earlier_obs],
            decision_at=datetime(2026, 6, 3, tzinfo=timezone.utc),
        )

    # Same period: older Accepted filing observed later must not supersede newer Accepted.
    older_filing_late = _bind(
        observed_at=datetime(2026, 6, 2, 18, 0, tzinfo=timezone.utc),
        version_id="older-accepted",
        index_html=_synthetic_index_html_colon(
            accession=_SYNTHETIC_ACCESSION_B,
            accepted="2026-05-20 12:00:00",
        ),
        xml=_synthetic_nport_xml(accession=_SYNTHETIC_ACCESSION_B, holdings_xml=_CLEAN_HOLDINGS),
        accepted_timezone="America/New_York",
    )
    newer_filing_earlier_obs = _bind(
        observed_at=datetime(2026, 6, 1, 18, 0, tzinfo=timezone.utc),
        version_id="newer-accepted",
        index_html=_synthetic_index_html_colon(accepted="2026-05-22 15:05:15"),
        xml=_synthetic_nport_xml(holdings_xml=revised),
        accepted_timezone="America/New_York",
    )
    selected = select_iwb_sec_filing_input_version_at_cutoff(
        [older_filing_late, newer_filing_earlier_obs],
        decision_at=datetime(2026, 6, 3, tzinfo=timezone.utc),
    )
    assert selected.version_id == "newer-accepted"
    assert selected.accepted_at == accepted_newer
    assert {row["symbol"] for row in iwb_sec_universe_rows_for_ues(selected)} == {"AAPL", "AMZN"}
    assert older_filing_late.accepted_at == accepted_older


def test_iwb_sec_rejects_malformed_unsafe_encoding_and_structure() -> None:
    with pytest.raises(IwbSecFilingAdapterError, match="empty"):
        parse_iwb_sec_filing_index_html(b"")
    with pytest.raises(IwbSecFilingAdapterError, match="malformed|root"):
        parse_iwb_sec_nport_xml_bytes(b"<not-closed")
    with pytest.raises(IwbSecFilingAdapterError, match="DTD/entity"):
        parse_iwb_sec_nport_xml_bytes(
            b"""<?xml version="1.0" encoding="UTF-8"?>
            <!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
            <edgarSubmission>&xxe;</edgarSubmission>"""
        )
    utf16_entity = (
        '<?xml version="1.0" encoding="UTF-16"?>'
        '<!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>'
        "<edgarSubmission>&xxe;</edgarSubmission>"
    ).encode("utf-16")
    with pytest.raises(IwbSecFilingAdapterError, match="BOM|UTF-16|encoding|NUL"):
        parse_iwb_sec_nport_xml_bytes(utf16_entity)
    utf16le_internal = (
        '<?xml version="1.0"?>'
        '<!DOCTYPE foo [<!ENTITY inject "BAD">]>'
        "<edgarSubmission><headerData>&inject;</headerData></edgarSubmission>"
    ).encode("utf-16le")
    with pytest.raises(IwbSecFilingAdapterError, match="BOM|UTF-16|encoding|NUL|DTD/entity"):
        parse_iwb_sec_nport_xml_bytes(utf16le_internal)

    with pytest.raises(IwbSecFilingAdapterError, match="size limit"):
        parse_iwb_sec_filing_index_html(b"A" * (1_048_576 + 1))

    wrapped = b'<wrapper xmlns="' + _NS.encode() + b'">' + _synthetic_nport_xml()[len(b'<?xml version="1.0" encoding="UTF-8"?>') :] + b"</wrapper>"
    with pytest.raises(IwbSecFilingAdapterError, match="root must be edgarSubmission"):
        parse_iwb_sec_nport_xml_bytes(b'<?xml version="1.0" encoding="UTF-8"?>' + wrapped)

    duplicate_header = _synthetic_nport_xml(
        extra_after_header="""
  <headerData>
    <filerInfo><filer><issuerCredentials><cik>0000000001</cik></issuerCredentials></filer>
      <seriesClassInfo><seriesId>S000004347</seriesId><classId>C000012077</classId><ticker>IWB</ticker></seriesClassInfo>
    </filerInfo>
  </headerData>
"""
    )
    with pytest.raises(IwbSecFilingAdapterError, match="duplicate structural container: headerData"):
        parse_iwb_sec_nport_xml_bytes(duplicate_header)

    duplicate_series = _synthetic_nport_xml().replace(
        b"<seriesId>S000004347</seriesId>",
        b"<seriesId>S000004347</seriesId><seriesId>S000004347</seriesId>",
        1,
    )
    with pytest.raises(IwbSecFilingAdapterError, match="duplicate required singleton"):
        parse_iwb_sec_nport_xml_bytes(duplicate_series)

    blank_series = _synthetic_nport_xml().replace(b"<seriesId>S000004347</seriesId>", b"<seriesId>   </seriesId>", 1)
    with pytest.raises(IwbSecFilingAdapterError, match="blank N-PORT field: seriesId"):
        parse_iwb_sec_nport_xml_bytes(blank_series)

    with pytest.raises(IwbSecFilingAdapterError, match="invstOrSecs is empty"):
        parse_iwb_sec_nport_xml_bytes(_synthetic_nport_xml(holdings_xml=""))


def test_iwb_sec_rejects_unknown_or_nested_holdings_children() -> None:
    unknown_child = _CLEAN_HOLDINGS + """
      <unexpectedInvestment>
        <name>unmapped equity</name>
      </unexpectedInvestment>
"""
    with pytest.raises(IwbSecFilingAdapterError, match="unrecognized invstOrSecs child"):
        parse_iwb_sec_nport_xml_bytes(_synthetic_nport_xml(holdings_xml=unknown_child))

    nested = _CLEAN_HOLDINGS + """
      <wrapper>
        <invstOrSec>
          <name>Unresolved Stock</name>
          <assetCat>EC</assetCat>
        </invstOrSec>
      </wrapper>
"""
    with pytest.raises(IwbSecFilingAdapterError, match="unrecognized invstOrSecs child|malformed nested"):
        parse_iwb_sec_nport_xml_bytes(_synthetic_nport_xml(holdings_xml=nested))


def test_iwb_sec_rejects_truncated_index_html() -> None:
    colon_truncated = _synthetic_index_html_colon().split(b"</body>")[0]
    with pytest.raises(IwbSecFilingAdapterError, match="incomplete or truncated"):
        parse_iwb_sec_filing_index_html(colon_truncated, accepted_timezone=_NY)

    structured = _synthetic_index_html_sec_style()
    # Truncate after the last recognized Accepted info field value/openers.
    marker = b'<div class="infoHead">Accepted</div><div class="info">2026-05-22 15:05:15</div>'
    assert marker in structured
    structured_truncated = structured.split(marker, 1)[0] + marker
    assert b"</body>" not in structured_truncated
    with pytest.raises(IwbSecFilingAdapterError, match="incomplete or truncated"):
        parse_iwb_sec_filing_index_html(structured_truncated, accepted_timezone=_NY)


def test_iwb_sec_rejects_bad_form_and_cik_grammar() -> None:
    with pytest.raises(IwbSecFilingAdapterError, match="unsupported form type"):
        parse_iwb_sec_filing_index_html(_synthetic_index_html_colon(form_type="10-K"), accepted_timezone=_NY)
    with pytest.raises(IwbSecFilingAdapterError, match="invalid CIK grammar"):
        parse_iwb_sec_filing_index_html(
            _synthetic_index_html_colon(cik="BAD0001100663TRAILING"),
            accepted_timezone=_NY,
        )
    with pytest.raises(IwbSecFilingAdapterError, match="submissionType mismatch"):
        bind_iwb_sec_filing_input_version(
            index_html_bytes=_synthetic_index_html_colon(),
            nport_xml_bytes=_synthetic_nport_xml(submission_type="NPORT-P/A"),
            observed_at=datetime(2026, 5, 22, 20, 0, tzinfo=timezone.utc),
            version_id="mismatch-form",
            accepted_timezone=_NY,
        )


def test_iwb_sec_sec_style_html_fields_and_duplicate_identity() -> None:
    parsed = parse_iwb_sec_filing_index_html(
        _synthetic_index_html_sec_style(),
        accepted_timezone="America/New_York",
    )
    assert parsed.cik == IWB_SEC_FILING_CIK
    assert parsed.accession_number == _SYNTHETIC_ACCESSION
    assert parsed.form_type == "NPORT-P"
    assert parsed.report_period.isoformat() == "2026-03-31"
    assert parsed.accepted_at == datetime(2026, 5, 22, 15, 5, 15, tzinfo=_NY)

    with pytest.raises(IwbSecFilingAdapterError, match="duplicate conflicting filing index field: cik"):
        parse_iwb_sec_filing_index_html(
            _synthetic_index_html_sec_style(duplicate_cik="0000000001"),
            accepted_timezone="America/New_York",
        )

    mismatched = _synthetic_index_html_sec_style(form_type="NPORT-P", primary_type="10-K")
    with pytest.raises(
        IwbSecFilingAdapterError,
        match="header form disagrees with primary document type|unsupported primary document type",
    ):
        parse_iwb_sec_filing_index_html(mismatched, accepted_timezone="America/New_York")

    contradictory_headers = _synthetic_index_html_sec_style(primary_type="NPORT-P").replace(
        b'<div class="formHeader">Form NPORT-P</div>',
        b'<div class="formHeader">Form NPORT-P</div><div class="formHeader">Form 10-K</div>',
        1,
    )
    with pytest.raises(
        IwbSecFilingAdapterError,
        match="header form disagrees with primary document type|conflicting or unsupported header form",
    ):
        parse_iwb_sec_filing_index_html(contradictory_headers, accepted_timezone="America/New_York")

    with pytest.raises(IwbSecFilingAdapterError, match="primary document row is missing or malformed"):
        parse_iwb_sec_filing_index_html(
            _synthetic_index_html_sec_style(include_primary_table=False),
            accepted_timezone="America/New_York",
        )

    version = bind_iwb_sec_filing_input_version(
        index_html_bytes=_synthetic_index_html_sec_style(),
        nport_xml_bytes=_synthetic_nport_xml(),
        observed_at=datetime(2026, 5, 22, 20, 0, tzinfo=timezone.utc),
        version_id="sec-style",
        accepted_timezone="America/New_York",
    )
    assert version.raw_binding_sha256
    assert version.index_sha256 != version.xml_sha256


def test_iwb_sec_adapter_has_no_network_side_effects(_no_network) -> None:
    observed = datetime(2026, 5, 22, 20, 0, 0, tzinfo=timezone.utc)
    version = _bind(observed_at=observed, holdings_xml=_CLEAN_HOLDINGS)
    assert {row["symbol"] for row in iwb_sec_universe_rows_for_ues(version)} == {"AAPL", "MSFT"}
    assert select_iwb_sec_filing_input_version_at_cutoff([version], decision_at=observed) is version
    assert version.trading_eligible is False
    assert version.qualification == "synthetic"
