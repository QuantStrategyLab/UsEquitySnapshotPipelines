from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from quant_platform_kit.data.multisource_assurance import (
    SOURCE_OBSERVATION_READY,
    SOURCE_OBSERVATION_UNAVAILABLE,
    DailyBar,
    DailyBarSourceObservation,
    DailyBarSourceSnapshot,
)

SCRIPT = Path("scripts/diagnose_r3_v2_free_source_assurance.py")
SPEC = importlib.util.spec_from_file_location("diagnose_r3_v2_free_source_assurance", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
diagnostic_cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(diagnostic_cli)

_SYMBOLS = ("QQQ", "TQQQ", "SOXX", "SOXL")
_CUTOFF = "2026-08-21"


def _ready(source_id: str, *, symbol: str, volume: float = 1_000_000) -> DailyBarSourceObservation:
    snapshot = DailyBarSourceSnapshot(
        source_id=source_id,
        symbol=symbol,
        date_cutoff=_CUTOFF,
        adjustment_basis="split_adjusted",
        source_artifact_sha256=("a" if source_id.startswith("twelve") else "b") * 64,
        bars=(DailyBar(_CUTOFF, 100, 102, 99, 101, volume),),
    )
    return DailyBarSourceObservation(source_id, SOURCE_OBSERVATION_READY, snapshot)


def test_r3_v2_diagnostic_requires_full_ohlcv_volume_and_exact_universe(monkeypatch, capsys) -> None:
    policies = []
    original_assess = diagnostic_cli.assess_multisource_daily_bars

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
    monkeypatch.setattr(
        diagnostic_cli,
        "assess_multisource_daily_bars",
        lambda policy, observations: (policies.append(policy), original_assess(policy, observations))[1],
    )
    monkeypatch.setattr(
        diagnostic_cli,
        "_expected_xnys_sessions",
        lambda *, start_date, date_cutoff: (date_cutoff,),
    )

    assert diagnostic_cli.main(["--date-cutoff", _CUTOFF]) == 0

    output = capsys.readouterr().out.strip()
    assert output.startswith("R3_V2_FREE_SOURCE_ASSURANCE_DIAGNOSTIC=")
    payload = json.loads(output.removeprefix("R3_V2_FREE_SOURCE_ASSURANCE_DIAGNOSTIC="))
    assert payload["schema_version"] == "qsl.r3_v2_free_source_assurance_diagnostic.v1"
    assert payload["status"] == "VERIFIED"
    assert payload["can_promote"] is False
    assert payload["auto_promote"] is False
    assert set(payload["reports"]) == set(_SYMBOLS)
    assert all(report["can_publish_research_input"] is True for report in payload["reports"].values())
    assert all(
        policy.required_price_fields == ("open", "high", "low", "close") and policy.compare_volume
        for policy in policies
    )
    assert all(policy.adjustment_basis == "split_adjusted" for policy in policies)
    assert all(
        policy.required_source_ids
        == ("twelve_data_1day_split_adjusted", "yahoo_finance_chart_1day_split_adjusted")
        for policy in policies
    )
    for report in payload["reports"].values():
        coverage = report["session_coverage"]
        assert coverage["expected_session_count"] == 1
        assert coverage["calendar_id"] == "XNYS"
        assert coverage["coverage_complete"] is True
        assert report["ohlcv_agreement"]["compare_volume"] is True
        assert report["ohlcv_agreement"]["status"] == "COMPARED"
    assert '"open":' not in output
    assert '"high":' not in output
    assert '"low":' not in output
    assert '"close":' not in output
    assert '"volume":' not in output


def test_r3_v2_diagnostic_parks_when_sources_disagree_or_coverage_is_incomplete(monkeypatch, capsys) -> None:
    def observe_twelve(*, symbol, **kwargs):
        return _ready("twelve_data_1day_split_adjusted", symbol=symbol, volume=1_000_000)

    def observe_yahoo(*, symbol, **kwargs):
        if symbol == "QQQ":
            return DailyBarSourceObservation(
                "yahoo_finance_chart_1day_split_adjusted",
                SOURCE_OBSERVATION_UNAVAILABLE,
                reason_codes=("YAHOO_FINANCE_TRANSPORT_UNAVAILABLE",),
            )
        if symbol == "TQQQ":
            return _ready("yahoo_finance_chart_1day_split_adjusted", symbol=symbol, volume=2_000_000)
        return _ready("yahoo_finance_chart_1day_split_adjusted", symbol=symbol)

    monkeypatch.setattr(diagnostic_cli, "observe_twelve_data_adjusted_daily_bars", observe_twelve)
    monkeypatch.setattr(diagnostic_cli, "observe_yahoo_finance_adjusted_daily_bars", observe_yahoo)
    monkeypatch.setattr(
        diagnostic_cli,
        "_expected_xnys_sessions",
        lambda *, start_date, date_cutoff: ("2026-08-20", date_cutoff),
    )

    assert diagnostic_cli.main(["--date-cutoff", _CUTOFF]) == 0
    payload = json.loads(
        capsys.readouterr().out.strip().removeprefix("R3_V2_FREE_SOURCE_ASSURANCE_DIAGNOSTIC=")
    )
    assert payload["status"] == "NOT_VERIFIED"
    assert payload["can_promote"] is False
    assert payload["reports"]["QQQ"]["status"] in {"PARKED", "DEGRADED"}
    assert payload["reports"]["TQQQ"]["status"] == "DEGRADED"
    assert "daily_bar_volume_divergence" in payload["reports"]["TQQQ"]["findings"]
    assert payload["reports"]["SOXL"]["session_coverage"]["coverage_complete"] is False
    assert all(report["can_publish_research_input"] is False for report in payload["reports"].values())
