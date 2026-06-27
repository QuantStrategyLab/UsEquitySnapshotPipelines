from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.build_monthly_live_replacement_reviews import (
    build_live_replacement_review_from_inputs,
    discover_replacement_review_inputs,
)
from scripts.run_monthly_report_bundle import build_bundle
from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_live_decay_followup import (
    main as live_decay_main,
)
from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_liquidity_followup import (
    main as liquidity_main,
)
from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_overfit_followup import (
    main as overfit_main,
)
from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_promotion_review import (
    main as promotion_review_main,
)
from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_research import (
    main as research_main,
)
from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_shadow_review import (
    main as shadow_review_main,
)
from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_stress_followup import (
    main as stress_main,
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


def _load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_crash_brake_artifact_chain_smoke(tmp_path: Path) -> None:
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

    research_manifest = research_dir / "crash_brake_research_manifest.json"
    summary_path = research_dir / "crash_brake_summary.csv"
    rolling_path = research_dir / "crash_brake_rolling_summary.csv"
    trades_path = research_dir / "crash_brake_rebalance_trades.csv"
    returns_path = research_dir / "crash_brake_daily_returns.csv"

    overfit_dir = tmp_path / "russell_top50_crash_brake_overfit_20260624"
    assert (
        overfit_main(
            [
                "--summary",
                str(summary_path),
                "--rolling",
                str(rolling_path),
                "--research-manifest",
                str(research_manifest),
                "--output-dir",
                str(overfit_dir),
            ]
        )
        == 0
    )

    stress_dir = tmp_path / "russell_top50_crash_brake_stress_20260624"
    assert (
        stress_main(
            [
                "--prices",
                str(prices_path),
                "--universe",
                str(universe_path),
                "--research-manifest",
                str(research_manifest),
                "--output-dir",
                str(stress_dir),
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
        == 0
    )

    liquidity_dir = tmp_path / "russell_top50_crash_brake_liquidity_20260624"
    assert (
        liquidity_main(
            [
                "--trades",
                str(trades_path),
                "--prices",
                str(prices_path),
                "--research-manifest",
                str(research_manifest),
                "--output-dir",
                str(liquidity_dir),
                "--portfolio-nav-values",
                "100000,500000",
            ]
        )
        == 0
    )

    promotion_dir = tmp_path / "russell_top50_crash_brake_promotion_review_20260624"
    assert (
        promotion_review_main(
            [
                "--summary",
                str(summary_path),
                "--research-manifest",
                str(research_manifest),
                "--overfit-promotion",
                str(overfit_dir / "overfit_promotion_gate_summary.csv"),
                "--stress-summary",
                str(stress_dir / "crash_brake_stress_summary.csv"),
                "--liquidity-summary",
                str(liquidity_dir / "liquidity_summary.csv"),
                "--output-dir",
                str(promotion_dir),
            ]
        )
        == 0
    )

    shadow_dir = tmp_path / "russell_top50_crash_brake_shadow_review_20260624"
    assert (
        shadow_review_main(
            [
                "--summary",
                str(summary_path),
                "--trades",
                str(trades_path),
                "--output-dir",
                str(shadow_dir),
                "--snapshot-as-of",
                "2026-06-30",
            ]
        )
        == 0
    )

    live_decay_dir = tmp_path / "live_decay_monitor_crash_brake_20260624"
    assert (
        live_decay_main(
            [
                "--returns",
                str(returns_path),
                "--research-manifest",
                str(research_manifest),
                "--candidate-runs",
                "crash_brake_top2_50_floor25",
                "--windows",
                "63",
                "--min-observations",
                "40",
                "--output-dir",
                str(live_decay_dir),
                "--expected-excess-cagr",
                "0.10",
                "--min-realized-expected-ratio",
                "0.50",
            ]
        )
        == 0
    )

    groups = discover_replacement_review_inputs(tmp_path)
    crash_brake_groups = [
        group for group in groups if group.get("group_type") == "russell" and "crash_brake" in str(group["promotion"]["review_path"])
    ]
    assert len(crash_brake_groups) == 1
    group = crash_brake_groups[0]
    assert group["shadow"] is not None
    assert group["live_decay"] is not None
    assert str(group["shadow"]["csv_path"]).endswith("russell_top50_leader_rotation_shadow_review_rows.csv")
    assert str(group["live_decay"]["summary_path"]).endswith("live_decay_strategy_summary.csv")

    live_replacement_dir = build_live_replacement_review_from_inputs(group, output_root=tmp_path)
    manifest = _load_json(live_replacement_dir / "live_replacement_manifest.json")
    review = pd.read_csv(live_replacement_dir / "live_replacement_review.csv")
    crash_brake_row = review.loc[review["candidate"].eq("crash_brake_top2_50_floor25")].iloc[0]
    assert manifest["manifest_type"] == "live_replacement_review"
    assert manifest["row_count"] >= 1
    assert manifest["inputs"]["russell_shadow_review"].endswith(
        "russell_top50_leader_rotation_shadow_review_rows.csv"
    )
    assert crash_brake_row["shadow_review_present"]
    assert crash_brake_row["live_decay_present"]
    assert bool(crash_brake_row["replace_live_now"]) is False
    assert "required_gates_failed" not in str(crash_brake_row["blocking_reason"])

    bundle = build_bundle(tmp_path, report_month="2026-06", ranking_preview_size=2)
    assert bundle["crash_brake_research_count"] == 1
    assert bundle["crash_brake_overfit_count"] == 1
    assert bundle["crash_brake_stress_count"] == 1
    assert bundle["crash_brake_liquidity_count"] == 1
    assert bundle["live_replacement_review_count"] >= 1
    live_replacement_reports = [
        item
        for item in bundle["live_replacement_reviews"]
        if "crash_brake" in str(item.get("manifest_path", ""))
    ]
    assert len(live_replacement_reports) == 1
    assert live_replacement_reports[0]["replace_live_now_count"] == 0
