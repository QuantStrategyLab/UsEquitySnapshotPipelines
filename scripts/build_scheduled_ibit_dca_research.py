#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from us_equity_snapshot_pipelines.ibit_smart_dca_research import (  # noqa: E402
    build_ibit_smart_dca_research,
    download_ibit_smart_dca_price_history,
    write_ibit_smart_dca_research_outputs,
)

DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "output" / "ibit_smart_dca" / "research" / "ibit_dca"
DEFAULT_PLUGIN_CONFIG: dict[str, object] = {
    "dynamic_lookback_days": 1460,
    "dynamic_min_periods": 365,
    "soft_exit_percentile": 0.95,
    "hard_exit_percentile": 0.985,
    "soft_exit_zscore_floor": 5.0,
    "hard_exit_zscore_floor": 7.0,
    "soft_exit_zscore_cap": 8.0,
    "hard_exit_zscore_cap": 10.0,
    "risk_reduced_ibit_exposure": 0.50,
    "risk_off_ibit_exposure": 0.25,
    "parking_symbol": "BOXX",
}


def _merged_plugin_config(overrides: Mapping[str, Any] | None, *, parking_symbol: str) -> dict[str, Any]:
    config = dict(DEFAULT_PLUGIN_CONFIG)
    config["parking_symbol"] = str(parking_symbol)
    config.update(dict(overrides or {}))
    return config


def build_scheduled_ibit_dca_research(
    *,
    zscore_metrics_path: str | Path,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    price_start: str = "2014-01-01",
    price_end: str | None = None,
    download_proxy: str | None = None,
    ibit_symbol: str = "IBIT",
    parking_symbol: str = "BOXX",
    primary_benchmark: str = "QQQ",
    secondary_benchmark: str = "SPY",
    btc_proxy_symbol: str = "BTC",
    initial_parking_value: float = 10_000.0,
    contribution_amount: float = 500.0,
    rebalance_frequency: str = "MS",
    turnover_cost_bps: float = 5.0,
    plugin_config: Mapping[str, Any] | None = None,
) -> Path:
    zscore_path = Path(zscore_metrics_path)
    if not zscore_path.exists():
        raise FileNotFoundError(f"IBIT zscore metrics CSV not found: {zscore_path}")

    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    prices = download_ibit_smart_dca_price_history(
        start=price_start,
        end=price_end,
        ibit_symbol=ibit_symbol,
        parking_symbol=parking_symbol,
        primary_benchmark=primary_benchmark,
        secondary_benchmark=secondary_benchmark,
        btc_proxy_symbol=btc_proxy_symbol,
        proxy=download_proxy,
    )
    prices.to_csv(output_root / "downloaded_price_history.csv", index=False)
    zscore_history = pd.read_csv(zscore_path)
    result = build_ibit_smart_dca_research(
        prices,
        zscore_history=zscore_history,
        ibit_symbol=ibit_symbol,
        parking_symbol=parking_symbol,
        primary_benchmark=primary_benchmark,
        secondary_benchmark=secondary_benchmark,
        btc_proxy_symbol=btc_proxy_symbol,
        initial_parking_value=float(initial_parking_value),
        contribution_amount=float(contribution_amount),
        rebalance_frequency=rebalance_frequency,
        turnover_cost_bps=float(turnover_cost_bps),
        plugin_enabled=True,
        plugin_config=_merged_plugin_config(plugin_config, parking_symbol=parking_symbol),
    )
    paths = write_ibit_smart_dca_research_outputs(result, output_root)
    return paths["manifest"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build scheduled research-only IBIT Smart DCA artifacts from zscore metrics and downloaded prices.",
    )
    parser.add_argument("--zscore-metrics", required=True, help="Normalized as_of,mvrv_zscore CSV path.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--price-start", default="2014-01-01")
    parser.add_argument("--price-end", default=None)
    parser.add_argument("--download-proxy", default=None)
    parser.add_argument("--ibit-symbol", default="IBIT")
    parser.add_argument("--parking-symbol", default="BOXX")
    parser.add_argument("--primary-benchmark", default="QQQ")
    parser.add_argument("--secondary-benchmark", default="SPY")
    parser.add_argument("--btc-proxy-symbol", default="BTC")
    parser.add_argument("--initial-parking-value", type=float, default=10_000.0)
    parser.add_argument("--contribution-amount", type=float, default=500.0)
    parser.add_argument("--rebalance-frequency", default="MS")
    parser.add_argument("--turnover-cost-bps", type=float, default=5.0)
    parser.add_argument("--plugin-config-json", default="{}")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    plugin_config = json.loads(args.plugin_config_json or "{}")
    manifest_path = build_scheduled_ibit_dca_research(
        zscore_metrics_path=args.zscore_metrics,
        output_dir=args.output_dir,
        price_start=args.price_start,
        price_end=args.price_end,
        download_proxy=args.download_proxy,
        ibit_symbol=args.ibit_symbol,
        parking_symbol=args.parking_symbol,
        primary_benchmark=args.primary_benchmark,
        secondary_benchmark=args.secondary_benchmark,
        btc_proxy_symbol=args.btc_proxy_symbol,
        initial_parking_value=args.initial_parking_value,
        contribution_amount=args.contribution_amount,
        rebalance_frequency=args.rebalance_frequency,
        turnover_cost_bps=args.turnover_cost_bps,
        plugin_config=plugin_config,
    )
    print(f"ibit_dca_research_manifest={manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
