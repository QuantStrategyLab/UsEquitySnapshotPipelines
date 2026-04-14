from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_backtest import EXPANDED_POOL
from us_equity_snapshot_pipelines.mega_cap_leader_rotation_robustness import (
    main,
    rank_robustness_summary,
    run_robustness_matrix,
)


def _sample_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2023-01-03", periods=520)
    trends = {symbol: 0.0005 + offset * 0.00012 for offset, symbol in enumerate(EXPANDED_POOL)}
    trends.update({"QQQ": 0.0010, "SPY": 0.0006, "BOXX": 0.0001})
    rows = []
    for idx, as_of in enumerate(dates):
        for offset, (symbol, trend) in enumerate(trends.items()):
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": as_of.date().isoformat(),
                    "close": (70.0 + offset * 4.0) * ((1.0 + trend) ** idx),
                    "volume": 1_000_000,
                }
            )
    return pd.DataFrame(rows)


def test_run_robustness_matrix_ranks_parameter_grid() -> None:
    summary = run_robustness_matrix(
        _sample_prices(),
        pools=("mag7", "expanded"),
        top_n_values=(3, 4),
        single_name_cap_values=(0.30,),
        defense_modes=("on", "off"),
        start_date="2024-06-03",
        end_date="2024-12-31",
        min_adv20_usd=1_000_000.0,
        turnover_cost_bps=0.0,
        portfolio_total_equity=5_000.0,
        min_position_value_usd=2_000.0,
    )
    ranked = rank_robustness_summary(summary)

    assert len(summary) == 8
    assert ranked["Rank"].tolist() == list(range(1, 9))
    assert {"mag7", "expanded"} == set(ranked["Pool"])
    assert {"on", "off"} == set(ranked["Defense Mode"])
    assert {3, 4} == set(ranked["Top N"])
    assert "Run" in ranked.columns
    assert set(ranked["Portfolio Total Equity"]) == {5_000.0}
    assert set(ranked["Min Position Value USD"]) == {2_000.0}


def test_robustness_cli_writes_matrix_outputs(tmp_path) -> None:
    prices_path = tmp_path / "prices.csv"
    output_dir = tmp_path / "output"
    _sample_prices().to_csv(prices_path, index=False)

    exit_code = main(
        [
            "--prices",
            str(prices_path),
            "--output-dir",
            str(output_dir),
            "--pools",
            "mag7",
            "--top-n-values",
            "3,4",
            "--single-name-cap-values",
            "0.30",
            "--defense-modes",
            "on",
            "--start",
            "2024-06-03",
            "--end",
            "2024-12-31",
            "--min-adv20-usd",
            "1000000",
            "--turnover-cost-bps",
            "0",
            "--portfolio-total-equity",
            "5000",
            "--min-position-value-usd",
            "2000",
        ]
    )

    assert exit_code == 0
    ranked = pd.read_csv(output_dir / "robustness_summary.csv")
    raw = pd.read_csv(output_dir / "robustness_summary_by_run.csv")
    assert len(ranked) == 2
    assert len(raw) == 2
    assert ranked["Rank"].tolist() == [1, 2]
