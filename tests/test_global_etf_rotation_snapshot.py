from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines import global_etf_rotation_snapshot as snapshot


def _prices(symbols: tuple[str, ...], *, periods: int = 280) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    dates = pd.bdate_range("2025-01-02", periods=periods)
    for index, symbol in enumerate(symbols):
        for offset, date in enumerate(dates):
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": date,
                    "close": 100.0 + offset * (1.0 + index / 10),
                    "volume": 1_000_000,
                }
            )
    return pd.DataFrame(rows)


def test_global_etf_snapshot_builds_universe_from_runtime_manifest() -> None:
    specs = snapshot.build_default_symbol_specs(
        {
            "ranking_pool": ("SMH", "GLD"),
            "canary_assets": ("SPY",),
            "safe_haven": "BIL",
        }
    )

    assert tuple(spec.symbol for spec in specs) == ("SMH", "GLD", "SPY", "BIL")
    assert tuple(spec.role for spec in specs) == ("ranking_pool_etf", "ranking_pool_etf", "canary_asset", "safe_haven")
    assert tuple(spec.eligible_for_trading for spec in specs) == (True, True, False, False)


def test_global_etf_snapshot_audit_ranks_only_tradeable_pool_members() -> None:
    result = snapshot.build_global_etf_rotation_audit(
        _prices(("SMH", "GLD", "SPY", "BIL")),
        config={"ranking_pool": ("SMH", "GLD"), "canary_assets": ("SPY",), "safe_haven": "BIL"},
    )

    assert set(result.candidate_snapshot["symbol"]) == {"SMH", "GLD", "SPY", "BIL"}
    assert set(result.ranking["symbol"]) == {"SMH", "GLD"}
    assert result.candidate_snapshot.set_index("symbol").loc["SPY", "research_action"] == "tracker_only"


def test_global_etf_feature_snapshot_exposes_runtime_contract_columns() -> None:
    feature_snapshot = snapshot.build_global_etf_rotation_feature_snapshot(
        _prices(("SMH", "GLD", "SPY", "BIL")),
        config={"ranking_pool": ("SMH", "GLD"), "canary_assets": ("SPY",), "safe_haven": "BIL"},
    )

    assert set(feature_snapshot.columns) >= {
        "as_of",
        "symbol",
        "role",
        "close",
        "momentum_13612w",
        "sma_pass",
        "eligible",
        "vol_126",
    }
    assert set(feature_snapshot["symbol"]) == {"SMH", "GLD", "SPY", "BIL"}
    assert set(feature_snapshot.loc[feature_snapshot["role"].eq("ranking_pool_etf"), "symbol"]) == {"SMH", "GLD"}
    assert feature_snapshot.set_index("symbol").loc["SPY", "role"] == "canary_asset"


def test_global_etf_snapshot_cli_writes_standard_artifacts(tmp_path: Path) -> None:
    prices_path = tmp_path / "prices.csv"
    output_dir = tmp_path / "output"
    default_symbols = tuple(spec.symbol for spec in snapshot.build_default_symbol_specs())
    _prices(default_symbols).to_csv(prices_path, index=False)

    exit_code = snapshot.main(["--prices", str(prices_path), "--output-dir", str(output_dir)])

    assert exit_code == 0
    for name in (
        "global_etf_rotation_feature_snapshot_latest.csv",
        "global_etf_rotation_feature_snapshot_latest.csv.manifest.json",
        "global_etf_rotation_ranking_latest.csv",
        "release_status_summary.json",
        "downloaded_price_history.csv",
        "candidate_snapshot.csv",
        "gate_results.csv",
        "ranking.csv",
        "promotion_decision.json",
        "run_manifest.json",
        "audit_report.md",
    ):
        assert (output_dir / name).exists()
    manifest = json.loads((output_dir / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["profile"] == "global_etf_rotation"
    assert manifest["artifact_type"] == "feature_snapshot_with_transparent_universe_audit"
    assert manifest["rule_spec"]["rule_id"] == "global_etf_rotation_universe_audit"
    feature_manifest = json.loads(
        (output_dir / "global_etf_rotation_feature_snapshot_latest.csv.manifest.json").read_text(encoding="utf-8")
    )
    assert feature_manifest["strategy_profile"] == "global_etf_rotation"
    assert feature_manifest["contract_version"] == "global_etf_rotation.feature_snapshot.v1"
