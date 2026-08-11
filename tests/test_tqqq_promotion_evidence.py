from __future__ import annotations

import copy
import hashlib
import json
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from quant_platform_kit.data.research_input import (
    canonical_research_input_manifest_bytes,
)
from quant_platform_kit.strategy_lifecycle.evidence_package_v2 import (
    read_evidence_package_v2_json,
    validate_evidence_package_v2,
)
from us_equity_strategies.production_parity.tqqq_contract import (
    evaluate_tqqq_research_contract,
)

from us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence import (
    TqqqPromotionEvidenceError,
    _Bar,
    _ImmutableReplayProducer,
    _ReplayState,
    run_tqqq_promotion_evidence,
)

RUNNER_REVISION = "1" * 40
MANDATE_RECEIPT_SHA256 = "6" * 64


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _sessions() -> list[date]:
    first_test = date(2019, 7, 30)
    warmup: list[date] = []
    cursor = first_test - timedelta(days=1)
    while len(warmup) < 260:
        if cursor.weekday() < 5:
            warmup.append(cursor)
        cursor -= timedelta(days=1)
    boundaries = {
        date(2018, 1, 2),
        date(2019, 7, 30),
        date(2019, 7, 31),
        date(2020, 1, 31),
        date(2020, 3, 2),
        date(2021, 10, 1),
        date(2021, 10, 4),
        date(2022, 3, 31),
        date(2022, 5, 2),
        date(2022, 12, 27),
        date(2022, 12, 28),
        date(2023, 12, 1),
        date(2023, 12, 4),
        date(2024, 5, 31),
        date(2024, 7, 1),
        date(2024, 7, 2),
        date(2025, 7, 1),
    }
    return sorted(set(warmup) | boundaries)


def _bar(symbol: str, session: date, index: int) -> dict[str, object]:
    base = 100.0 + index * (0.08 if symbol == "QQQ" else 0.12)
    if symbol == "BOXX":
        base = 100.0 + index * 0.01
    return {
        "date": session.isoformat(),
        "open": base,
        "high": base * 1.01,
        "low": base * 0.99,
        "close": base * 1.002,
        "volume": 1_000_000.0 + index,
    }


def _input_payload() -> dict[str, object]:
    sessions = _sessions()
    bars = {
        "schema_version": "tqqq_etf_only_private_bars.v1",
        "symbols": {
            "BOXX": [
                _bar("BOXX", session, index) for index, session in enumerate(sessions) if session >= date(2022, 12, 28)
            ],
            "QQQ": [_bar("QQQ", session, index) for index, session in enumerate(sessions)],
            "TQQQ": [_bar("TQQQ", session, index) for index, session in enumerate(sessions)],
        },
    }
    bars_bytes = _canonical(bars)
    observed_at = "2026-08-10T00:00:00Z"
    sources = []
    for symbol in ("BOXX", "QQQ", "TQQQ"):
        symbol_bytes = _canonical(bars["symbols"][symbol])
        sources.append(
            {
                "source_id": f"ibkr:{symbol}",
                "revision": "server-version-176",
                "observed_at": observed_at,
                "content_sha256": hashlib.sha256(symbol_bytes).hexdigest(),
            }
        )
    manifest = {
        "schema_version": "research_input_manifest.v1",
        "manifest_id": (
            "tqqq-ibkr-paper-single-acquisition-"
            f"{hashlib.sha256(bars_bytes).hexdigest()[:24]}"
        ),
        "research_input_contract_id": "tqqq_etf_only_ibkr_adjusted_last.v1",
        "domain": "us_equity",
        "profile": "tqqq_etf_only_single_strategy_research_v1",
        "artifact_type": "immutable_adjusted_ohlcv_etf_only",
        "observed_at": observed_at,
        "effective_at": observed_at,
        "as_of": observed_at,
        "producer": {
            "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
            "commit_sha": RUNNER_REVISION,
            "tree_sha": RUNNER_REVISION,
            "tool": "tqqq_ibkr_paper_single_acquisition",
            "tool_version": "v1",
        },
        "calendar": {
            "calendar_id": "XNYS",
            "timezone": "America/New_York",
            "session_date": "2025-07-01",
            "source": "exchange_calendars",
            "source_revision": "XNYS-2026-08-10",
        },
        "adjustment": {
            "policy": "total_return_adjusted",
            "source": "IBKR_ADJUSTED_LAST",
            "source_revision": "server-version-176",
        },
        "sources": sources,
        "members": [
            {
                "path": "bars.json",
                "media_type": "application/json",
                "size_bytes": len(bars_bytes),
                "sha256": hashlib.sha256(bars_bytes).hexdigest(),
            }
        ],
    }
    return {
        "provenance": {
            "evidence_class": "provider_observed",
            "real_producer": True,
            "provider": "IBKR Paper Gateway TWS API",
            "provider_revision": "server-version-176",
            "session_class": "paper",
            "license": "GFIS_API_NON_COMMERCIAL_PERSONAL_RESTRICTED_2026-02-04",
            "usage_scope": "PRIVATE_LOCAL_NONCOMMERCIAL_RESEARCH_NO_REDISTRIBUTION",
        },
        "input_manifest": manifest,
        "bars": bars,
    }


def _config() -> dict[str, object]:
    return {
        "schema_version": "tqqq_etf_only_replay_config.v1",
        "strategy_profile": "tqqq_etf_only_single_strategy_research_v1",
        "signal_model": "qqq_sma_200_close_t_open_t_plus_1",
        "signal_window_sessions": 200,
        "tqqq_nominal_cap": 0.15,
        "boxx_nominal_cap": 0.50,
        "risk_mandate_id": "tqqq_etf_only_research_v1",
        "risk_standard_id": "qpk.strategy_promotion_risk_standard.zh-CN.v2",
        "risk_standard_sha256": "2" * 64,
        "authority_receipt_sha256": "3" * 64,
        "platform_execution_revision": "4" * 40,
        "input_license": "GFIS_API_NON_COMMERCIAL_PERSONAL_RESTRICTED_2026-02-04",
        "input_usage_scope": "PRIVATE_LOCAL_NONCOMMERCIAL_RESEARCH_NO_REDISTRIBUTION",
        "session_class": "paper",
    }


def test_real_consumer_writes_valid_redacted_evidence_v2(tmp_path: Path) -> None:
    payload = _input_payload()
    assert payload["bars"]["symbols"]["BOXX"][0]["date"] == "2022-12-28"
    assert "2022-12-27" in {bar["date"] for bar in payload["bars"]["symbols"]["QQQ"]}

    with (
        patch(
            "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence._resolve_runner_revision",
            return_value=RUNNER_REVISION,
        ),
        patch(
            "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner._resolve_runner_revision",
            return_value=RUNNER_REVISION,
        ),
        patch(
            "us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence.evaluate_tqqq_research_contract",
            wraps=evaluate_tqqq_research_contract,
        ) as assess,
    ):
        result = run_tqqq_promotion_evidence(
            input_payload=_input_payload(),
            config_payload=_config(),
            output_dir=tmp_path,
            generated_at="2026-08-10T00:00:00Z",
            mandate_receipt_sha256=MANDATE_RECEIPT_SHA256,
        )

    evidence_path = tmp_path / "strategy-evidence-package.v2.json"
    evidence = read_evidence_package_v2_json(evidence_path)
    assert validate_evidence_package_v2(evidence, base_dir=tmp_path) == ()
    assert evidence["backtest"]["orchestrator"] == "BacktestOrchestrator"
    promotion_run = evidence["backtest"]["promotion_run"]
    assert {
        result["params"]["mandate_receipt_sha256"]
        for result in [*promotion_run["fold_results"], promotion_run["locked_oos_result"]]
    } == {MANDATE_RECEIPT_SHA256}
    assert evidence["backtest"]["promotion_run"]["source_revision"] == ("15df2a42df5d230cfb03a7cb655fd4b226956681")
    assert evidence["cost_stress"]["scenarios"] == [
        {"multiplier": 1, "total_cost_bps": 5.0},
        {"multiplier": 2, "total_cost_bps": 10.0},
        {"multiplier": 3, "total_cost_bps": 15.0},
    ]
    assert evidence["lifecycle_claims"] == {
        "learning_only": False,
        "promotion_eligible": False,
        "live_ready": False,
        "size_zero_required": True,
        "no_order": True,
    }
    assert result["evidence_sha256"] == hashlib.sha256(evidence_path.read_bytes()).hexdigest()
    risk = json.loads((tmp_path / "artifacts" / "risk.json").read_bytes())
    assert assess.call_count == sum(
        counts["assessments"] for counts in risk["scenario_counts"].values()
    )
    assert all(
        counts["assessments"] == counts["decisions"]
        for counts in risk["scenario_counts"].values()
    )
    assert (
        canonical_research_input_manifest_bytes(_input_payload()["input_manifest"])
        == (tmp_path / "artifacts" / "data-manifest.json").read_bytes()
    )
    packaged = b"".join(path.read_bytes() for path in tmp_path.rglob("*") if path.is_file())
    assert b'"open"' not in packaged
    assert b'"volume"' not in packaged


def test_input_tamper_and_nonempty_output_fail_closed_before_replay(
    tmp_path: Path,
) -> None:
    tampered = copy.deepcopy(_input_payload())
    tampered["bars"]["symbols"]["QQQ"][0]["close"] = 999.0

    with pytest.raises(TqqqPromotionEvidenceError, match="input identity"):
        run_tqqq_promotion_evidence(
            input_payload=tampered,
            config_payload=_config(),
            output_dir=tmp_path,
            mandate_receipt_sha256=MANDATE_RECEIPT_SHA256,
        )
    assert not any(tmp_path.iterdir())

    (tmp_path / "existing").write_text("x", encoding="utf-8")
    with pytest.raises(TqqqPromotionEvidenceError, match="empty"):
        run_tqqq_promotion_evidence(
            input_payload=_input_payload(),
            config_payload=_config(),
            output_dir=tmp_path,
            mandate_receipt_sha256=MANDATE_RECEIPT_SHA256,
        )


def test_provider_observed_contract_rejects_boxx_backfill(tmp_path: Path) -> None:
    payload = _input_payload()
    payload["bars"]["symbols"]["BOXX"].insert(0, _bar("BOXX", date(2022, 12, 23), 0))
    bars_bytes = _canonical(payload["bars"])
    payload["input_manifest"]["members"][0].update(
        size_bytes=len(bars_bytes), sha256=hashlib.sha256(bars_bytes).hexdigest()
    )
    boxx_bytes = _canonical(payload["bars"]["symbols"]["BOXX"])
    payload["input_manifest"]["sources"][0]["content_sha256"] = hashlib.sha256(boxx_bytes).hexdigest()

    with pytest.raises(TqqqPromotionEvidenceError, match="BOXX eligibility"):
        run_tqqq_promotion_evidence(
            input_payload=payload,
            config_payload=_config(),
            output_dir=tmp_path,
            mandate_receipt_sha256=MANDATE_RECEIPT_SHA256,
        )


def test_same_asset_drawdown_target_is_reduced_without_round_trip() -> None:
    session = date(2025, 1, 2)
    producer = object.__new__(_ImmutableReplayProducer)
    producer.tqqq = {
        session: _Bar(session, 100.0, 101.0, 99.0, 100.0, 1_000_000.0)
    }
    producer.boxx = {}
    producer._state = _ReplayState(
        cash=85_000.0,
        symbol="TQQQ",
        quantity=150.0,
        entry_price=100.0,
        stop_price=95.0,
        pending_symbol="TQQQ",
        pending_weight=0.10,
    )

    producer._trade_to_target(session, 5)

    assert producer._state.quantity == pytest.approx(100.0)
    assert producer._state.cash == pytest.approx(89_997.5)
    assert producer._state.turnover == pytest.approx(0.05)
    assert producer._state.trade_count == 1
