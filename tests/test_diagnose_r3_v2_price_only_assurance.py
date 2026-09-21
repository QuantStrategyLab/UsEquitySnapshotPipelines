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

SCRIPT = Path("scripts/diagnose_r3_v2_price_only_assurance.py")
SPEC = importlib.util.spec_from_file_location("diagnose_r3_v2_price_only_assurance", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
diagnostic_cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(diagnostic_cli)

_SYMBOLS = ("QQQ", "TQQQ", "SOXX", "SOXL")
_CUTOFF = "2026-08-21"


def _ready(
    source_id: str,
    *,
    symbol: str,
    open_: float = 100.0,
    high: float = 102.0,
    low: float = 99.0,
    close: float = 101.0,
    volume: float = 1_000_000,
) -> DailyBarSourceObservation:
    snapshot = DailyBarSourceSnapshot(
        source_id=source_id,
        symbol=symbol,
        date_cutoff=_CUTOFF,
        adjustment_basis="split_adjusted",
        source_artifact_sha256=("a" if source_id.startswith("twelve") else "b") * 64,
        bars=(DailyBar(_CUTOFF, open_, high, low, close, volume),),
    )
    return DailyBarSourceObservation(source_id, SOURCE_OBSERVATION_READY, snapshot)


def test_r3_v2_price_only_volume_delta_does_not_block_when_prices_agree(monkeypatch, capsys) -> None:
    policies = []
    original_assess = diagnostic_cli.assess_multisource_daily_bars

    monkeypatch.setattr(
        diagnostic_cli,
        "observe_twelve_data_adjusted_daily_bars",
        lambda *, symbol, **kwargs: _ready("twelve_data_1day_split_adjusted", symbol=symbol, volume=1_000_000),
    )
    monkeypatch.setattr(
        diagnostic_cli,
        "observe_yahoo_finance_adjusted_daily_bars",
        lambda *, symbol, **kwargs: _ready("yahoo_finance_chart_1day_split_adjusted", symbol=symbol, volume=9_000_000),
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
    assert output.startswith("R3_V2_PRICE_ONLY_ASSURANCE_DIAGNOSTIC=")
    payload = json.loads(output.removeprefix("R3_V2_PRICE_ONLY_ASSURANCE_DIAGNOSTIC="))
    assert payload["schema_version"] == "qsl.r3_v2_price_only_assurance_diagnostic.v1"
    assert payload["contract_id"] == "r3_v2_price_only"
    assert payload["status"] == "VERIFIED"
    assert payload["can_promote"] is False
    assert payload["auto_promote"] is False
    assert payload["compare_volume"] is False
    assert payload["volume_not_consumed"] is True
    assert payload["required_price_fields"] == ["open", "high", "low", "close"]
    assert set(payload["reports"]) == set(_SYMBOLS)
    assert all(report["can_publish_research_input"] is True for report in payload["reports"].values())
    assert all(
        policy.scope_id == f"r3_v2_price_only_{symbol.lower()}"
        and policy.required_price_fields == ("open", "high", "low", "close")
        and policy.compare_volume is False
        and policy.adjustment_basis == "split_adjusted"
        for symbol, policy in zip(_SYMBOLS, policies, strict=True)
    )
    for report in payload["reports"].values():
        assert "daily_bar_volume_divergence" not in report.get("findings", [])
        agreement = report["price_agreement"]
        assert agreement["compare_volume"] is False
        assert agreement["volume_not_consumed"] is True
        assert agreement["status"] == "COMPARED"
        assert report["session_coverage"]["coverage_complete"] is True
        assert report["session_coverage"]["calendar_id"] == "XNYS"
    assert '"open":' not in output
    assert '"high":' not in output
    assert '"low":' not in output
    assert '"close":' not in output
    assert '"volume":' not in output


def test_r3_v2_price_only_price_delta_still_blocks(monkeypatch, capsys) -> None:
    def observe_twelve(*, symbol, **kwargs):
        return _ready("twelve_data_1day_split_adjusted", symbol=symbol, close=101.0)

    def observe_yahoo(*, symbol, **kwargs):
        # Distinct price fields must remain FAIL-CLOSED even when volume is ignored.
        if symbol in {"SOXL", "SOXX", "TQQQ"}:
            return _ready(
                "yahoo_finance_chart_1day_split_adjusted",
                symbol=symbol,
                open_=100.0,
                high=106.0,
                low=99.0,
                close=105.0,
                volume=9_000_000,
            )
        return _ready("yahoo_finance_chart_1day_split_adjusted", symbol=symbol)

    monkeypatch.setattr(diagnostic_cli, "observe_twelve_data_adjusted_daily_bars", observe_twelve)
    monkeypatch.setattr(diagnostic_cli, "observe_yahoo_finance_adjusted_daily_bars", observe_yahoo)
    monkeypatch.setattr(
        diagnostic_cli,
        "_expected_xnys_sessions",
        lambda *, start_date, date_cutoff: (date_cutoff,),
    )

    assert diagnostic_cli.main(["--date-cutoff", _CUTOFF]) == 0
    payload = json.loads(capsys.readouterr().out.strip().removeprefix("R3_V2_PRICE_ONLY_ASSURANCE_DIAGNOSTIC="))
    assert payload["status"] == "NOT_VERIFIED"
    assert payload["volume_not_consumed"] is True
    assert payload["can_promote"] is False
    assert payload["auto_promote"] is False
    for symbol in ("SOXL", "SOXX", "TQQQ"):
        report = payload["reports"][symbol]
        assert report["status"] == "DEGRADED"
        assert "daily_bar_price_divergence" in report["findings"]
        assert "daily_bar_volume_divergence" not in report["findings"]
        assert report["can_publish_research_input"] is False
        assert "close" in report["price_agreement"]["price_divergent_fields"]
    assert payload["reports"]["QQQ"]["status"] == "VERIFIED"


def test_r3_v2_price_only_unavailable_or_incomplete_coverage_fail_closed(monkeypatch, capsys) -> None:
    def observe_twelve(*, symbol, **kwargs):
        return _ready("twelve_data_1day_split_adjusted", symbol=symbol)

    def observe_yahoo(*, symbol, **kwargs):
        if symbol == "QQQ":
            return DailyBarSourceObservation(
                "yahoo_finance_chart_1day_split_adjusted",
                SOURCE_OBSERVATION_UNAVAILABLE,
                reason_codes=("YAHOO_FINANCE_TRANSPORT_UNAVAILABLE",),
            )
        return _ready("yahoo_finance_chart_1day_split_adjusted", symbol=symbol)

    monkeypatch.setattr(diagnostic_cli, "observe_twelve_data_adjusted_daily_bars", observe_twelve)
    monkeypatch.setattr(diagnostic_cli, "observe_yahoo_finance_adjusted_daily_bars", observe_yahoo)
    monkeypatch.setattr(
        diagnostic_cli,
        "_expected_xnys_sessions",
        lambda *, start_date, date_cutoff: ("2026-08-20", date_cutoff),
    )

    assert diagnostic_cli.main(["--date-cutoff", _CUTOFF]) == 0
    payload = json.loads(capsys.readouterr().out.strip().removeprefix("R3_V2_PRICE_ONLY_ASSURANCE_DIAGNOSTIC="))
    assert payload["status"] == "NOT_VERIFIED"
    assert payload["contract_id"] == "r3_v2_price_only"
    assert payload["schema_version"] == "qsl.r3_v2_price_only_assurance_diagnostic.v1"
    assert payload["reports"]["QQQ"]["status"] in {"PARKED", "DEGRADED"}
    assert payload["reports"]["SOXL"]["session_coverage"]["coverage_complete"] is False
    assert "xnys_session_coverage_incomplete" in payload["reports"]["SOXL"]["findings"]
    assert all(report["can_publish_research_input"] is False for report in payload["reports"].values())
    assert all(
        report.get("price_agreement", {}).get("volume_not_consumed") is True for report in payload["reports"].values()
    )
