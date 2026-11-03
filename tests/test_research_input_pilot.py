"""Synthetic input recovery, price basis, budget and publication-boundary checks."""
import importlib.util
import io
import json
from pathlib import Path
from zipfile import ZipFile

import pytest

ROOT = Path(__file__).parents[1]
SPEC = importlib.util.spec_from_file_location("research_input_pilot", ROOT / "scripts/research_input_pilot.py")
pilot = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pilot)


def plan():
    return {"scope": "research_input_only", "dataset_id": "synthetic-test", "end": "2024-01-05",
            "paid_data_usd": 0, "cloud_cap_usd": 10, "estimated_cloud_usd": 1,
            "retention_estimate_days": 30, "instruments": [
                {"symbol": "TEST", "start": "2024-01-02", "inception": "2020-01-01"}]}


def test_plan_rejects_paid_cost_overrun_and_unsafe_path():
    assert pilot.validate_plan(plan(), "gs://synthetic-private/research/input/") == (
        "synthetic-private", "research/input/synthetic-test/")
    for field, value in [("paid_data_usd", 1), ("estimated_cloud_usd", 11),
                         ("dataset_id", "../production"), ("retention_estimate_days", 0)]:
        p = plan() | {field: value}
        with pytest.raises(pilot.Stop):
            pilot.validate_plan(p, "gs://synthetic-private/research/input/")


def test_coverage_never_hides_truncation_and_duplicates():
    result = pilot.coverage(["2024-01-03"], "2024-01-02", "2024-01-03", ["2024-01-02", "2024-01-03"])
    assert result["coverage_complete"] is False and result["missing_dates"] == ["2024-01-02"]
    with pytest.raises(pilot.Stop, match="DUPLICATE"):
        pilot.coverage(["2024-01-03"] * 2, "2024-01-02", "2024-01-03", [])


def test_bar_dates_require_finite_positive_prices_and_aware_times():
    bar = {"t": "2024-01-02T05:00:00Z", "o": 10, "h": 12, "l": 9, "c": 11, "v": 20}
    assert pilot.bar_dates({"bars": [bar]}, "2024-01-02", "2024-01-02") == ["2024-01-02"]
    for changes in [{"t": "2024-01-02T05:00:00"}, {"c": float("nan")}, {"v": -1}, {"l": 13}]:
        with pytest.raises(pilot.Stop):
            pilot.bar_dates({"bars": [bar | changes]}, "2024-01-02", "2024-01-02")


def test_budget_counts_attempts_and_refuses_before_limit():
    budget = pilot.Budget()
    budget.http_requests = 120
    with pytest.raises(pilot.Stop, match="HTTP_REQUEST_LIMIT"):
        budget.http()
    budget.retained_bytes = 128 * 1024**2
    with pytest.raises(pilot.Stop, match="STORAGE_BUDGET"):
        budget.storage(retained=1)


def test_direct_rate_parsers_keep_source_identity_and_reject_wrong_series():
    body = b"Description,synthetic\nTime Period,RIFSPFF_N.D\n1999-01-01,1.00\n1999-01-02,ND\n"
    values = pilot.parse_board_csv(body, "RIFSPFF_N.D", "1999-01-01", "1999-01-03")
    assert values == [{"date": "1999-01-01", "rate_percent": "1.00"}]
    with pytest.raises(pilot.Stop, match="SERIES"):
        pilot.parse_board_csv(body, "OTHER", "1999-01-01", "1999-01-03")
    body = pilot.encode({"refRates": [{"type": "SOFR", "effectiveDate": "2024-01-02", "percentRate": 1.25}]})
    assert pilot.parse_sofr(body, "2024-01-01", "2024-01-03")[0]["rate_percent"] == "1.25"


def test_board_xml_selects_only_exact_series_and_window():
    stream = io.BytesIO()
    with ZipFile(stream, "w") as archive:
        archive.writestr("H15_data.xml", '''<DataSet><Series SERIES_NAME="OTHER"><Obs TIME_PERIOD="1999-01-01" OBS_VALUE="9"/></Series>
        <Series SERIES_NAME="RIFSPFF_N.D"><Obs TIME_PERIOD="1998-01-01" OBS_VALUE="2"/>
        <Obs TIME_PERIOD="1999-01-01" OBS_VALUE="1.00" OBS_STATUS="A"/></Series></DataSet>''')
    assert pilot.parse_board_zip(stream.getvalue(), "RIFSPFF_N.D", "1999-01-01", "1999-01-02") == [
        {"date": "1999-01-01", "rate_percent": "1.00"}]


def test_cloud_guard_rejects_mac_and_reruns(monkeypatch):
    for key, value in {"GITHUB_ACTIONS": "true", "GITHUB_EVENT_NAME": "workflow_dispatch",
                       "RUNNER_OS": "Linux", "GITHUB_REF": "refs/heads/main", "GITHUB_RUN_ATTEMPT": "1"}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(pilot.platform, "system", lambda: "Darwin")
    with pytest.raises(pilot.Stop, match="CLOUD_RUNNER"):
        pilot.require_cloud()
    monkeypatch.setattr(pilot.platform, "system", lambda: "Linux")
    pilot.require_cloud()
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "2")
    with pytest.raises(pilot.Stop):
        pilot.require_cloud()


def test_engineering_summary_does_not_equate_status_with_qualified_fills():
    result = pilot.engineering_structure({"run_id": "synthetic-run", "account": "private-value",
        "summary": {"orders_submitted_count": 2, "submitted_order_status_counts": {"filled": 1}},
        "diagnostics": {"positions": [{"symbol": "PRIVATE", "cash": 100}], "fill_price": 42}})
    assert result["counts"]["reported_filled_status_count"] == 1
    assert result["fill_comparison_qualified"] is False
    assert "private-value" not in json.dumps(result)
    assert set(result) == {"counts", "fields_present", "run_join_sha256", "dry_run", "fill_comparison_qualified"}
    assert result["fields_present"] == ["cash", "fill_price", "positions"]


def test_store_create_only_and_exact_readback():
    class Blob:
        generation = 123

        def upload_from_string(self, body, **kwargs):
            assert kwargs["if_generation_match"] == 0 and kwargs["retry"] is None
            self.body = body

        def download_as_bytes(self, **kwargs):
            assert kwargs["if_generation_match"] == 123 and kwargs["retry"] is None
            return self.body

    class Client:
        obj = Blob()

        def bucket(self, _name):
            return self

        def blob(self, _path, **_kwargs):
            return self.obj

    store = pilot.Store(Client(), pilot.Budget(), "synthetic-private", "research/input/test/")
    receipt = store.create("test.json", b"synthetic")
    assert receipt["sha256"] == pilot.sha(b"synthetic")
    with pytest.raises(pilot.Stop):
        store.read(receipt | {"sha256": "0" * 64})


def test_acquisition_keeps_two_bases_and_all_action_pages(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY_ID", "synthetic-key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "synthetic-credential")
    calls, saved = [], []

    class Provider:
        def get(self, url, _headers):
            calls.append(url)
            if "corporate-actions" in url:
                return pilot.encode({"corporate_actions": {"cash_dividends": []}, "next_page_token": None})
            return pilot.encode({"bars": [{"t": "2024-01-02T05:00:00Z", "o": 10, "h": 12,
                                           "l": 9, "c": 11, "v": 20}], "next_page_token": None})

    class Store:
        def create(self, name, body):
            saved.append(name)
            return {"sha256": pilot.sha(body), "generation": "1", "bytes": len(body)}

    p = plan() | {"end": "2024-01-02"}
    result = pilot.acquire_alpaca(p, Provider(), Store())
    assert [row.get("price_basis") for row in result[:2]] == ["raw_unadjusted", "split_dividend_adjusted"]
    assert len(saved) == 3 and len(calls) == 3
    assert result[0]["coverage"]["coverage_complete"]
    assert "adjustment=raw" in calls[0] and "adjustment=all" in calls[1]


def test_summary_excludes_private_locators_and_response_bodies():
    manifest = {"alpaca": [], "rates": [], "evidence_inventory": {
        "objects": [{"object_id": "a" * 64, "role": "plugin", "uri": "gs://private/secret-location",
                     "generation": "1", "bytes": 3, "sha256": "b" * 64, "status": "EXACT_BYTES_VERIFIED"}],
        "candidate_plugin_digest": "c" * 64, "plugin_file_count": 1}}
    result = pilot.public_summary(manifest, pilot.Budget())
    assert "gs://" not in json.dumps(result)
    assert result["release_status"] == "PARKED" and result["approved_plugin_set"] is False


@pytest.mark.parametrize("start,end,dates", [
    ("2024-01-02", "2024-01-06", ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]),
    ("2024-01-12", "2024-01-15", ["2024-01-12"]),
])
def test_non_session_end_keeps_exact_provider_window(monkeypatch, start, end, dates):
    from urllib.parse import parse_qs, urlsplit

    monkeypatch.setenv("ALPACA_API_KEY_ID", "synthetic-key")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "synthetic-credential")

    class Provider:
        def get(self, url, _headers):
            query = parse_qs(urlsplit(url).query)
            assert query["end"][0].split("T")[0] == end
            if "corporate-actions" in url:
                return pilot.encode({"corporate_actions": {}, "next_page_token": None})
            bars = [{"t": d + "T05:00:00Z", "o": 10, "h": 10, "l": 10, "c": 10, "v": 1} for d in dates]
            return pilot.encode({"bars": bars, "next_page_token": None})

    class Store:
        def create(self, _name, body):
            return {"sha256": pilot.sha(body), "generation": "1", "bytes": len(body)}

    p = plan() | {"end": end, "instruments": [{"symbol": "TEST", "start": start, "inception": "2020-01-01"}]}
    result = pilot.acquire_alpaca(p, Provider(), Store())
    for row in result[:2]:
        assert row["coverage"]["requested_end"] == end
        assert row["coverage"]["observations"] == len(dates) and row["coverage"]["coverage_complete"]


def test_manual_workflow_has_no_publish_schedule_or_arbitrary_command():
    text = (ROOT / ".github/workflows/research-input-pilot.yml").read_text()
    assert "workflow_dispatch:" in text and "schedule:" not in text and "workflow_run:" not in text
    for forbidden in ["execute_publish", "gcloud run deploy", "gh variable", "gh secret", "curl ", "--admin"]:
        assert forbidden not in text
    assert "uv run --no-sync python scripts/research_input_pilot.py" in text
    assert "${{ inputs." not in text.split("run:")[-1]
    source = (ROOT / "scripts/research_input_pilot.py").read_text()
    assert '"raw_unadjusted"' in source and '"split_dividend_adjusted"' in source
