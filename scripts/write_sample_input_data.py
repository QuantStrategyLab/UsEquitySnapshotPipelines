from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


TECH_COMMUNICATION_PULLBACK_PROFILE = "tech_communication_pullback_enhancement"
RUSSELL_1000_MULTI_FACTOR_DEFENSIVE_PROFILE = "russell_1000_multi_factor_defensive"
MEGA_CAP_LEADER_ROTATION_TOP50_BALANCED_PROFILE = "mega_cap_leader_rotation_top50_balanced"


def _price_rows(symbols: dict[str, tuple[float, float]], *, periods: int = 320) -> list[dict[str, object]]:
    dates = pd.bdate_range("2024-01-02", periods=periods)
    rows: list[dict[str, object]] = []
    for index, as_of in enumerate(dates):
        for symbol, (base_price, daily_slope) in symbols.items():
            close = base_price * (1.0 + index * daily_slope)
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": as_of.date().isoformat(),
                    "close": round(close, 4),
                    "volume": 1_000_000,
                }
            )
    return rows


def _tech_prices() -> pd.DataFrame:
    symbols = {
        "QQQ": (100.0, 0.0010),
        "SPY": (100.0, 0.0007),
        "BOXX": (100.0, 0.0002),
        "AAPL": (120.0, 0.0012),
        "MSFT": (110.0, 0.0011),
        "META": (90.0, 0.0013),
        "NVDA": (80.0, 0.0014),
        "JPM": (80.0, 0.0003),
    }
    return pd.DataFrame(_price_rows(symbols))


def _tech_universe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol": "AAPL", "sector": "Information Technology"},
            {"symbol": "MSFT", "sector": "Information Technology"},
            {"symbol": "META", "sector": "Communication Services"},
            {"symbol": "NVDA", "sector": "Information Technology"},
            {"symbol": "JPM", "sector": "Financials"},
        ]
    )


def _russell_prices() -> pd.DataFrame:
    symbols = {
        "SPY": (100.0, 0.0007),
        "BOXX": (100.0, 0.0002),
        "AAPL": (120.0, 0.0012),
        "MSFT": (110.0, 0.0011),
        "JPM": (80.0, 0.0003),
        "XOM": (75.0, 0.00025),
        "NVDA": (80.0, 0.0014),
    }
    return pd.DataFrame(_price_rows(symbols))


def _russell_universe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol": "AAPL", "sector": "Information Technology"},
            {"symbol": "MSFT", "sector": "Information Technology"},
            {"symbol": "JPM", "sector": "Financials"},
            {"symbol": "XOM", "sector": "Energy"},
            {"symbol": "NVDA", "sector": "Information Technology"},
        ]
    )


def _mega_prices() -> pd.DataFrame:
    symbols = {
        "QQQ": (100.0, 0.0010),
        "SPY": (100.0, 0.0007),
        "BOXX": (100.0, 0.0002),
        "AAPL": (120.0, 0.0010),
        "MSFT": (110.0, 0.0012),
        "NVDA": (80.0, 0.0018),
        "META": (90.0, 0.0014),
        "AMZN": (95.0, 0.0011),
        "TSLA": (75.0, 0.0001),
    }
    return pd.DataFrame(_price_rows(symbols))


def _mega_universe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol": "NVDA", "sector": "Information Technology", "mega_rank": 1},
            {"symbol": "MSFT", "sector": "Information Technology", "mega_rank": 2},
            {"symbol": "AAPL", "sector": "Information Technology", "mega_rank": 3},
            {"symbol": "META", "sector": "Communication Services", "mega_rank": 4},
            {"symbol": "AMZN", "sector": "Consumer Discretionary", "mega_rank": 5},
            {"symbol": "TSLA", "sector": "Consumer Discretionary", "mega_rank": 6},
        ]
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write synthetic input data for snapshot workflow smoke tests.")
    parser.add_argument("--profile", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    profile = str(args.profile).strip().lower().replace("-", "_")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if profile == TECH_COMMUNICATION_PULLBACK_PROFILE:
        prices = _tech_prices()
        universe = _tech_universe()
    elif profile == RUSSELL_1000_MULTI_FACTOR_DEFENSIVE_PROFILE:
        prices = _russell_prices()
        universe = _russell_universe()
    elif profile == MEGA_CAP_LEADER_ROTATION_TOP50_BALANCED_PROFILE:
        prices = _mega_prices()
        universe = _mega_universe()
    else:
        raise SystemExit(f"Unsupported profile: {args.profile}")

    prices_path = output_dir / "prices.csv"
    universe_path = output_dir / "universe.csv"
    prices.to_csv(prices_path, index=False)
    universe.to_csv(universe_path, index=False)
    print(f"wrote sample prices -> {prices_path}")
    print(f"wrote sample universe -> {universe_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
