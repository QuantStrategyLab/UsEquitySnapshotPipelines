"""US Equity return matrix adapter — reads portfolio_and_tracker_returns.csv."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from quant_platform_kit.strategy_lifecycle.performance_metrics import normalize_return_matrix


def read_us_equity_returns(
    artifact_root: str | Path | None = None,
    *,
    date_column: str = "as_of",
) -> pd.DataFrame:
    """Read the US equity return matrix from pipeline artifacts.

    Args:
        artifact_root: Root directory for US equity snapshot pipeline output.
        date_column: Name of the date column.

    Returns:
        Normalized DataFrame with strategy return columns.
    """
    if artifact_root is None:
        from pathlib import Path
        artifact_root = Path(__file__).resolve().parents[3] / "data" / "output"

    root = Path(artifact_root)
    paths = sorted(root.rglob("portfolio_and_tracker_returns.csv"))
    if not paths:
        raise FileNotFoundError(f"No return matrix found under {root}")

    # Use the latest
    path = paths[-1]
    frame = pd.read_csv(str(path))
    return normalize_return_matrix(frame, date_column=date_column)


def list_us_equity_strategies(
    artifact_root: str | Path | None = None,
) -> list[str]:
    """List all strategy return columns in the US equity return matrix."""
    frame = read_us_equity_returns(artifact_root)
    ignored = {"as_of", "date"}
    strategies = []
    for col in frame.columns:
        col_str = str(col).strip()
        if col_str and col_str not in ignored and not col_str.startswith("buy_hold_"):
            strategies.append(col_str)
    return strategies
