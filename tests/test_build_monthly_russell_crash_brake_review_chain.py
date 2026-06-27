from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.build_monthly_live_replacement_reviews import discover_replacement_review_inputs
from scripts.build_monthly_russell_crash_brake_review_chain import (
    build_crash_brake_review_chain,
    discover_crash_brake_research_runs,
    main,
)
from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_research import main as research_main


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
    rows: list[dict[str, object]] = []
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


def test_build_crash_brake_review_chain_from_research_outputs(tmp_path: Path) -> None:
    prices_path = tmp_path / "prices.csv"
    universe_path = tmp_path / "universe.csv"
    prices = _sample_prices()
    universe = _sample_dynamic_universe()
    prices.to_csv(prices_path, index=False)
    universe.to_csv(universe_path, index=False)

    research_dir = tmp_path / "russell_top50_crash_brake_research_20260624"
    assert (
        research_main(
            [
                "--prices",
                str(prices_path),
                "--universe",
                str(universe_path),
                "--output-dir",
                str(research_dir),
                "--start",
                "2021-02-01",
                "--end",
                "2023-05-31",
                "--universe-lag-days",
                "1",
                "--rolling-window-years",
                "1",
                "--min-history-days",
                "100",
                "--min-adv20-usd",
                "1000000",
                "--turnover-cost-bps",
                "0",
            ]
        )
        == 0
    )

    discovered = discover_crash_brake_research_runs(tmp_path)
    assert len(discovered) == 1

    outputs = build_crash_brake_review_chain(
        discovered[0],
        output_root=tmp_path / "generated",
        snapshot_as_of="2026-06-30",
    )

    assert (outputs["overfit_dir"] / "crash_brake_overfit_followup_manifest.json").exists()
    assert (outputs["stress_dir"] / "crash_brake_stress_followup_manifest.json").exists()
    assert (outputs["liquidity_dir"] / "crash_brake_liquidity_followup_manifest.json").exists()
    assert (outputs["promotion_dir"] / "crash_brake_promotion_review_manifest.json").exists()
    assert (outputs["shadow_dir"] / "russell_top50_leader_rotation_shadow_review_manifest.json").exists()
    assert (outputs["live_decay_dir"] / "live_decay_monitor_manifest.json").exists()

    groups = discover_replacement_review_inputs(tmp_path / "generated")
    assert len(groups) == 1
    assert groups[0]["group_type"] == "russell"
    assert groups[0]["shadow"] is not None
    assert groups[0]["live_decay"] is not None


def test_main_builds_crash_brake_review_chain(tmp_path: Path, monkeypatch, capsys) -> None:
    prices_path = tmp_path / "prices.csv"
    universe_path = tmp_path / "universe.csv"
    _sample_prices().to_csv(prices_path, index=False)
    _sample_dynamic_universe().to_csv(universe_path, index=False)
    research_dir = tmp_path / "russell_top50_crash_brake_research_20260624"
    assert (
        research_main(
            [
                "--prices",
                str(prices_path),
                "--universe",
                str(universe_path),
                "--output-dir",
                str(research_dir),
                "--start",
                "2021-02-01",
                "--end",
                "2023-05-31",
                "--universe-lag-days",
                "1",
                "--rolling-window-years",
                "1",
                "--min-history-days",
                "100",
                "--min-adv20-usd",
                "1000000",
                "--turnover-cost-bps",
                "0",
            ]
        )
        == 0
    )

    output_root = tmp_path / "monthly_chain"
    monkeypatch.setattr(
        "sys.argv",
        [
            "build_monthly_russell_crash_brake_review_chain.py",
            "--artifact-root",
            str(tmp_path),
            "--output-root",
            str(output_root),
            "--snapshot-as-of",
            "2026-06-30",
        ],
    )

    assert main() == 0
    captured = capsys.readouterr()
    assert "crash_brake_review_chain_count=1" in captured.out
    manifest = output_root / "russell_top50_crash_brake_research_20260624__crash_brake_promotion_review" / "crash_brake_promotion_review_manifest.json"
    assert manifest.exists()
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    assert payload["manifest_type"] == "russell_top50_crash_brake_promotion_review"
