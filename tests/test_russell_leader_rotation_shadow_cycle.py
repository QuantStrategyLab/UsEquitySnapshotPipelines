from __future__ import annotations

import json
import socket
import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
USEQ_STRATEGIES_ROOT = PROJECT_ROOT.parent / "UsEquityStrategies" / "src"
QPK_ROOT = PROJECT_ROOT.parent / "QuantPlatformKit" / "src"
for path in (PROJECT_ROOT / "src", USEQ_STRATEGIES_ROOT, QPK_ROOT):
    text = str(path)
    if text not in sys.path:
        sys.path.insert(0, text)

pytest.importorskip("us_equity_strategies")

from us_equity_snapshot_pipelines.russell_leader_rotation_shadow_cycle import (  # noqa: E402
    run_russell_leader_rotation_shadow_cycle,
)
from us_equity_snapshot_pipelines.shadow_contract import (  # noqa: E402
    SHADOW_CYCLE_CONTRACT_SCHEMA_VERSION,
    validate_shadow_cycle_contract,
)


STAGING_SNAPSHOT = (
    PROJECT_ROOT
    / "data/output/russell_top50_leader_rotation_staging_20260628/russell_top50_leader_rotation_feature_snapshot_latest.csv"
)


def _write_synthetic_feature_snapshot(path: Path) -> None:
    rows = []
    for index in range(6):
        rows.append(
            {
                "as_of": "2026-04-30",
                "symbol": f"SYN{index + 1}",
                "sector": "technology",
                "close": 100.0 + index,
                "adv20_usd": 50_000_000.0,
                "history_days": 300,
                "mom_3m": 0.10 - index * 0.01,
                "mom_6m": 0.20 - index * 0.01,
                "mom_12_1": 0.30 - index * 0.01,
                "rel_mom_6m_vs_benchmark": 0.10 - index * 0.01,
                "rel_mom_6m_vs_broad_benchmark": 0.09 - index * 0.01,
                "high_252_gap": -0.01 - index * 0.005,
                "sma200_gap": 0.05 - index * 0.005,
                "vol_63": 0.20 + index * 0.01,
                "maxdd_126": -0.05 - index * 0.005,
            }
        )
    for symbol in ("QQQ", "SPY", "BOXX"):
        rows.append(
            {
                "as_of": "2026-04-30",
                "symbol": symbol,
                "sector": "benchmark",
                "close": 100.0,
                "adv20_usd": 50_000_000.0,
                "history_days": 300,
                "mom_3m": 0.05,
                "mom_6m": 0.05,
                "mom_12_1": 0.05,
                "rel_mom_6m_vs_benchmark": 0.0,
                "rel_mom_6m_vs_broad_benchmark": 0.0,
                "high_252_gap": 0.0,
                "sma200_gap": 0.05,
                "vol_63": 0.10,
                "maxdd_126": -0.02,
            }
        )
    pd.DataFrame(rows).to_csv(path, index=False)


def test_russell_shadow_contract_is_research_only() -> None:
    validate_shadow_cycle_contract({
        "shadow_contract": {
            "schema_version": SHADOW_CYCLE_CONTRACT_SCHEMA_VERSION,
            "mode": "research_only",
            "no_order": True,
            "broker_access": False,
        }
    })


@pytest.mark.skipif(not STAGING_SNAPSHOT.exists(), reason="local staging snapshot not available")
def test_run_russell_leader_rotation_shadow_cycle_on_staging_snapshot(tmp_path: Path) -> None:
    outputs = run_russell_leader_rotation_shadow_cycle(
        feature_snapshot_path=STAGING_SNAPSHOT,
        output_dir=tmp_path / "shadow_cycle",
        snapshot_as_of="2026-04-01",
    )

    assert outputs.diagnostics_json.exists()
    assert outputs.variant_comparison_json.exists()
    assert outputs.shadow_review_csv.exists()
    assert outputs.shadow_review_manifest.exists()

    diagnostics = json.loads(outputs.diagnostics_json.read_text(encoding="utf-8"))
    assert diagnostics["snapshot_as_of"] == "2026-04-01"
    assert diagnostics["diagnostics"]["leader_rotation_profile_variant"] == "blend_top2_50_top4_50"
    shadow_rows = diagnostics["diagnostics"]["leader_rotation_shadow_review_rows"]
    assert len(shadow_rows) == 3
    assert {row["shadow_variant"] for row in shadow_rows} == {
        "top4_baseline",
        "blend_top2_25_top4_75",
        "blend_top2_50_top4_50",
    }

    comparison = json.loads(outputs.variant_comparison_json.read_text(encoding="utf-8"))
    assert comparison["active_variant"] == "blend_top2_50_top4_50"
    assert len(comparison["variants"]) == 3

    review_rows = pd.read_csv(outputs.shadow_review_csv)
    assert len(review_rows) == 3
    active_row = review_rows.loc[review_rows["shadow_variant"].eq("blend_top2_50_top4_50")].iloc[0]
    assert float(active_row["turnover_delta_vs_active"]) == 0.0


@pytest.mark.skipif(not STAGING_SNAPSHOT.exists(), reason="local staging snapshot not available")
def test_shadow_cycle_emits_rebalance_trades(tmp_path: Path) -> None:
    """Verify the shadow cycle emits a rebalance_trades CSV with variant-level trade rows."""
    outputs = run_russell_leader_rotation_shadow_cycle(
        feature_snapshot_path=STAGING_SNAPSHOT,
        output_dir=tmp_path / "shadow_cycle",
        snapshot_as_of="2026-04-01",
    )
    assert outputs.rebalance_trades_csv is not None
    assert outputs.rebalance_trades_csv.exists()
    trades = pd.read_csv(outputs.rebalance_trades_csv)
    assert not trades.empty
    assert set(trades.columns).issuperset({
        "Date",
        "Run",
        "Variant Type",
        "Symbol",
        "Previous Weight",
        "Target Weight",
        "Trade Weight Delta",
        "Abs Trade Weight Delta",
        "Trade Side",
    })
    # Verify all 3 variant types are present
    assert trades["Variant Type"].nunique() == 3


def test_shadow_cycle_runs_end_to_end_on_synthetic_snapshot(tmp_path: Path, monkeypatch) -> None:
    def blocked(*_args, **_kwargs):
        raise AssertionError("network forbidden in synthetic Russell shadow-cycle test")

    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)
    monkeypatch.setattr(socket, "getaddrinfo", blocked)

    snapshot = tmp_path / "synthetic_feature_snapshot.csv"
    _write_synthetic_feature_snapshot(snapshot)

    outputs = run_russell_leader_rotation_shadow_cycle(
        feature_snapshot_path=snapshot,
        output_dir=tmp_path / "shadow_cycle",
        snapshot_as_of="2026-04-30",
        run_as_of="2026-05-01",
    )

    diagnostics = json.loads(outputs.diagnostics_json.read_text(encoding="utf-8"))
    assert diagnostics["diagnostics"]["diagnostic_summary"] == (
        "Russell Top50 当前仅比较目标仓位；该诊断没有回测收益证据，不能判断哪个变体更优。"
        "本次说明由程序生成，未调用 AI。"
    )
    assert diagnostics["shadow_contract"]["no_order"] is True
    assert outputs.shadow_review_csv.exists()
    assert outputs.shadow_review_manifest.exists()
    assert len(pd.read_csv(outputs.shadow_review_csv)) == 3


def test_shadow_cycle_rejects_invalid_snapshot_as_of(tmp_path: Path) -> None:
    snapshot = tmp_path / "invalid_feature_snapshot.csv"
    _write_synthetic_feature_snapshot(snapshot)
    frame = pd.read_csv(snapshot)
    frame.loc[0, "as_of"] = "not-a-date"
    frame.to_csv(snapshot, index=False)

    with pytest.raises(ValueError, match="invalid as_of values"):
        run_russell_leader_rotation_shadow_cycle(
            feature_snapshot_path=snapshot,
            output_dir=tmp_path / "shadow_cycle",
        )
