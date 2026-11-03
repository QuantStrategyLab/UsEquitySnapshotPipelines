"""Artificial archives only; no personal policy or downloaded market data."""
from copy import deepcopy
from importlib.metadata import version
import json
import socket

import pytest

from scripts.benchmark_reference_readback import prepare_reference
from scripts.research_input_pilot import Stop, sha


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError("synthetic archive validation prohibits network")
    monkeypatch.setattr(socket.socket, "connect", denied)
    monkeypatch.setattr(socket, "getaddrinfo", denied)


def fixture():
    prefix = "gs://example-private-research/artificial/"
    bodies = {}

    def put(name, data):
        body = json.dumps(data, sort_keys=True).encode()
        ref = {"uri": prefix + name, "generation": "7", "bytes": len(body), "sha256": sha(body)}
        bodies[ref["uri"]] = body
        return ref

    def inputs(closes=None, dates=None):
        closes = closes or [100, 80, 88, 84, 90]
        dates = dates or ["1999-12-31", "2000-01-03", "2000-01-04", "2000-01-05", "2000-01-06"]
        bars = [{"t": d + "T05:00:00Z", "o": c, "h": c, "l": c, "c": c, "v": 1}
                for d, c in zip(dates, closes)]
        page = put("alpaca/TEST/all/page-001.json", {"symbol": "TEST", "bars": bars, "next_page_token": None})
        page["fetched_at"] = "2000-01-07T01:00:00Z"
        entry = dict(symbol="TEST", source_type="real", provider="alpaca", feed="sip", currency="USD",
                     vendor_adjustment="all", price_basis="split_dividend_adjusted",
                     market_timezone="America/New_York", calendar_id="XNYS", calendar_version=version("exchange-calendars"),
                     coverage={"requested_start": "1999-12-30", "requested_end": "2000-01-06"}, pages=[page])
        manifest = {"schema_version": "research_input_inventory.v1", "research_only": True,
                    "created_at": "2000-01-07T12:00:00Z", "alpaca": [entry]}
        return manifest, entry

    manifest, entry = inputs()

    def run(*, changed=None, reader=None, available_at="2000-01-07T12:00:00Z", **kwargs):
        ref = put("input-inventory.json", changed or manifest)
        ref["available_at_utc"] = available_at
        parameters = dict(symbol="TEST", start="2000-01-03", end="2000-01-06",
                          frozen_at="2000-01-08T00:00:00Z", observed_at="2000-01-08T01:00:00Z")
        parameters.update(kwargs)
        return prepare_reference(ref, reader or (lambda r: bodies[r["uri"]]), **parameters)

    return run, manifest, entry, bodies, put


def test_first_loss_includes_pre_window_baseline_and_does_not_claim_account_net_returns():
    run, *_ = fixture()
    private, summary = run()
    assert summary["max_drawdown_loss_fraction"] == pytest.approx(.2)
    assert summary["baseline_session"] == "1999-12-31" and summary["observations"] == 4
    assert summary["source_objects_verified"] == 2
    assert len(private["returns"]) == 4 and private["returns"][0] == pytest.approx(-.2)
    assert "returns" not in summary and "dates" not in summary and "source" not in summary
    assert summary["return_basis"] == "PROVIDER_ADJUSTED_CLOSE_RATIO"
    for field in ("account_costs_included", "net_account_path_verified", "risk_policy_qualified",
                  "policy_selected", "settings_applied", "execution_authorized", "historical_pit_established"):
        assert summary[field] is False
    json.dumps(summary, allow_nan=False)


@pytest.mark.parametrize("fault", ["hash", "size", "symbol", "basis", "calendar_version", "scope", "page_time",
                                    "pagination", "gap", "baseline", "duplicate", "currency", "frozen", "source_type"])
def test_unverified_or_incomplete_source_is_rejected(fault):
    run, manifest, entry, bodies, put = fixture()
    page = entry["pages"][0]
    data = json.loads(bodies[page["uri"]])
    if fault == "hash":
        page["sha256"] = "0" * 64
    elif fault == "size":
        page["bytes"] += 1
    elif fault == "symbol":
        data["symbol"] = "OTHER"
    elif fault == "basis":
        entry["price_basis"] = "raw_unadjusted"
    elif fault == "calendar_version":
        entry["calendar_version"] = "0.0"
    elif fault == "scope":
        page["uri"] = "gs://another-bucket/private.json"
    elif fault == "page_time":
        page["fetched_at"] = "2000-01-09T00:00:00Z"
    elif fault == "pagination":
        data["next_page_token"] = "more"
    elif fault == "gap":
        del data["bars"][2]
    elif fault == "baseline":
        del data["bars"][0]
    elif fault == "duplicate":
        data["bars"][2] = deepcopy(data["bars"][1])
    elif fault == "currency":
        entry["currency"] = "HKD"
    elif fault == "frozen":
        manifest["created_at"] = "2000-01-09T12:00:00Z"
    else:
        entry["source_type"] = "unknown"
    if fault in {"symbol", "pagination", "gap", "baseline", "duplicate"}:
        replacement = put("alpaca/TEST/all/page-001.json", data)
        replacement["fetched_at"] = page["fetched_at"]
        entry["pages"] = [replacement]
    with pytest.raises(Stop):
        run()


def test_wrong_inventory_bytes_fail_before_any_page_read():
    run, *_ = fixture()
    calls = []

    def wrong(ref):
        calls.append(ref["uri"])
        return b"{}"

    with pytest.raises(Stop, match="REFERENCE_OBJECT_BYTES_MISMATCH"):
        run(reader=wrong)
    assert len(calls) == 1


@pytest.mark.parametrize("freeze", ["2000-01-09T00:00:00Z", "2000-01-08"])
def test_future_or_naive_freeze_is_not_accepted(freeze):
    run, *_ = fixture()
    with pytest.raises(Stop):
        run(frozen_at=freeze)


def test_non_session_reference_end_preserves_requested_period():
    run, manifest, entry, bodies, put = fixture()
    data = json.loads(bodies[entry["pages"][0]["uri"]])
    data["bars"].append({"t": "2000-01-07T05:00:00Z", "o": 92, "h": 92, "l": 92, "c": 92, "v": 1})
    page = put("alpaca/TEST/all/page-001.json", data)
    page["fetched_at"] = "2000-01-10T01:00:00Z"
    entry["pages"] = [page]
    entry["coverage"]["requested_end"] = "2000-01-09"
    manifest["created_at"] = "2000-01-10T12:00:00Z"
    _, summary = run(end="2000-01-09", available_at="2000-01-10T12:00:00Z",
                     frozen_at="2000-01-11T00:00:00Z", observed_at="2000-01-11T01:00:00Z")
    assert summary["requested_end"] == "2000-01-09" and summary["last_session"] == "2000-01-07"
    assert summary["observations"] == 5


def test_manifest_run_start_is_not_mislabeled_as_data_availability():
    run, manifest, *_ = fixture()
    manifest["created_at"] = "2000-01-07T00:00:00Z"
    _, summary = run()
    assert summary["source_available_at"] == "2000-01-07T12:00:00Z"
    assert summary["source_time_basis"] == "CALLER_SUPPLIED_EXACT_OBJECT_CREATION_METADATA"


def test_missing_exact_object_availability_is_not_inferred_from_run_start():
    run, *_ = fixture()
    with pytest.raises(Stop, match="REFERENCE_TIME_INVALID"):
        run(available_at=None)
