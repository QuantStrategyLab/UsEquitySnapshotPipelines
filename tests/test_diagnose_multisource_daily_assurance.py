from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from quant_platform_kit.data.multisource_assurance import (
    SOURCE_OBSERVATION_READY,
    DailyBar,
    DailyBarSourceObservation,
    DailyBarSourceSnapshot,
)

SCRIPT = Path("scripts/diagnose_multisource_daily_assurance.py")
SPEC = importlib.util.spec_from_file_location("diagnose_multisource_daily_assurance", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
diagnostic_cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(diagnostic_cli)


def _ready(source_id: str, *, symbol: str) -> DailyBarSourceObservation:
    snapshot = DailyBarSourceSnapshot(
        source_id=source_id,
        symbol=symbol,
        date_cutoff="2026-08-21",
        adjustment_basis="split_adjusted",
        source_artifact_sha256=("a" if source_id.startswith("twelve") else "b") * 64,
        bars=(DailyBar("2026-08-21", 100, 102, 99, 101, 1_000_000),),
    )
    return DailyBarSourceObservation(source_id, SOURCE_OBSERVATION_READY, snapshot)


def test_multisource_diagnostic_only_emits_redacted_assurance_reports(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        diagnostic_cli,
        "observe_twelve_data_adjusted_daily_bars",
        lambda *, symbol, **kwargs: _ready("twelve_data_1day_split_adjusted", symbol=symbol),
    )
    monkeypatch.setattr(
        diagnostic_cli,
        "observe_yahoo_finance_adjusted_daily_bars",
        lambda *, symbol, **kwargs: _ready("yahoo_finance_chart_1day_split_adjusted", symbol=symbol),
    )

    assert diagnostic_cli.main(["--date-cutoff", "2026-08-21"]) == 0

    output = capsys.readouterr().out.strip()
    assert output.startswith("MULTISOURCE_DAILY_ASSURANCE_DIAGNOSTIC=")
    payload = json.loads(output.removeprefix("MULTISOURCE_DAILY_ASSURANCE_DIAGNOSTIC="))
    assert payload["status"] == "MULTISOURCE_DAILY_ASSURANCE_VERIFIED"
    assert set(payload["reports"]) == {"BOXX", "SOXL", "SOXX"}
    assert all(report["can_publish_research_input"] is True for report in payload["reports"].values())
    for report in payload["reports"].values():
        coverage = report["session_coverage"]
        assert coverage["expected_session_count"] >= 1
        assert set(coverage["sources"]) == {
            "twelve_data_1day_split_adjusted",
            "yahoo_finance_chart_1day_split_adjusted",
        }
        assert all(source["missing_session_count"] >= 0 for source in coverage["sources"].values())
        assert report["price_agreement"] == {
            "status": "COMPARED",
            "price_relative_tolerance": 0.0001,
            "max_price_relative_delta": 0.0,
            "first_price_divergent_session": None,
            "price_divergent_fields": [],
        }
    assert '"open"' not in output
    assert '"volume"' not in output
