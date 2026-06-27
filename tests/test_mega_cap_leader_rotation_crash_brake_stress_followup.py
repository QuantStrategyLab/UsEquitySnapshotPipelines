from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_stress_followup import (
    build_crash_brake_stress_followup,
    main,
)


def _sample_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2020-01-02", periods=900)
    trends = {
        "SPY": 0.00035,
        "BOXX": 0.0001,
        "AAPL": 0.0009,
        "MSFT": 0.0010,
        "NVDA": 0.0012,
        "AMZN": 0.0008,
        "LLY": 0.0011,
        "XOM": 0.0007,
    }
    rows = []
    for idx, as_of in enumerate(dates):
        if idx < 500:
            qqq_multiplier = (1.0006) ** idx
        elif idx < 540:
            qqq_multiplier = (1.0006**500) * (1.0 - 0.18 * ((idx - 500) / 40.0))
        else:
            qqq_multiplier = (1.0006**500) * 0.82 * (1.003 ** (idx - 540))
        rows.append(
            {
                "symbol": "QQQ",
                "as_of": as_of.date().isoformat(),
                "close": 120.0 * qqq_multiplier,
                "volume": 3_000_000,
            }
        )
        for offset, (symbol, trend) in enumerate(trends.items()):
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": as_of.date().isoformat(),
                    "close": (85.0 + offset * 5.0) * ((1.0 + trend) ** idx),
                    "volume": 2_500_000,
                }
            )
    return pd.DataFrame(rows)


def _sample_dynamic_universe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "sector": sector,
                "start_date": "2021-01-29",
                "end_date": None,
                "mega_rank": rank,
            }
            for rank, (symbol, sector) in enumerate(
                [
                    ("NVDA", "Information Technology"),
                    ("MSFT", "Information Technology"),
                    ("AAPL", "Information Technology"),
                    ("LLY", "Health Care"),
                    ("AMZN", "Consumer Discretionary"),
                    ("XOM", "Energy"),
                ],
                start=1,
            )
        ]
    )


def test_build_crash_brake_stress_followup_runs_scenario_matrix() -> None:
    result = build_crash_brake_stress_followup(
        _sample_prices(),
        _sample_dynamic_universe(),
        start_date="2021-02-01",
        end_date="2023-05-31",
        turnover_cost_bps_values=(0.0, 5.0),
        universe_lag_days_values=(1, 2),
        min_adv20_usd_values=(1_000_000.0,),
        rolling_window_years=(1,),
        min_history_days=100,
        candidate_runs=("crash_brake_top2_50_floor25",),
        allowed_cagr_shortfall=0.20,
        allowed_drawdown_worse=0.20,
    )

    detail = result["crash_brake_stress_detail"]
    summary = result["crash_brake_stress_summary"]
    assert set(detail["Stress Turnover Cost Bps"]) == {0.0, 5.0}
    assert set(detail["Stress Universe Lag Trading Days"]) == {1, 2}
    assert set(summary["Run"]) == {"crash_brake_top2_50_floor25"}
    assert int(summary.iloc[0]["Stress Scenarios"]) == 4


def test_crash_brake_stress_followup_cli_writes_manifest(tmp_path: Path) -> None:
    prices_path = tmp_path / "prices.csv"
    universe_path = tmp_path / "universe.csv"
    research_manifest_path = tmp_path / "crash_brake_research_manifest.json"
    output_dir = tmp_path / "output"
    _sample_prices().to_csv(prices_path, index=False)
    _sample_dynamic_universe().to_csv(universe_path, index=False)
    research_manifest_path.write_text(
        json.dumps(
            {
                "manifest_type": "russell_top50_crash_brake_research",
                "artifact_schema_version": "russell_top50_crash_brake_research.v1",
                "experiment_profile": "panic_rebound_top2_sleeve_floor_v1",
            }
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "--prices",
            str(prices_path),
            "--universe",
            str(universe_path),
            "--research-manifest",
            str(research_manifest_path),
            "--output-dir",
            str(output_dir),
            "--start",
            "2021-02-01",
            "--end",
            "2023-05-31",
            "--turnover-cost-bps-values",
            "0,5",
            "--universe-lag-days-values",
            "1,2",
            "--min-adv20-usd-values",
            "1000000",
            "--rolling-window-years",
            "1",
            "--min-history-days",
            "100",
            "--allowed-cagr-shortfall",
            "0.20",
            "--allowed-drawdown-worse",
            "0.20",
        ]
    )

    assert exit_code == 0
    assert (output_dir / "crash_brake_stress_detail.csv").exists()
    assert (output_dir / "crash_brake_stress_summary.csv").exists()
    manifest = json.loads((output_dir / "crash_brake_stress_followup_manifest.json").read_text(encoding="utf-8"))
    assert manifest["manifest_type"] == "russell_top50_crash_brake_stress_followup"
    assert manifest["experiment_profile"] == "panic_rebound_top2_sleeve_floor_v1"
    assert manifest["row_counts"]["crash_brake_stress_summary"] > 0
