from __future__ import annotations

import argparse
import json
from pathlib import Path

from us_equity_snapshot_pipelines.tecl_xlk_trend_income_research_inputs import (
    DEFAULT_LONG_HISTORY_START,
    build_tecl_long_history_inputs,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "data" / "output" / "tecl_xlk_long_history_inputs_20260628"


def main() -> int:
    parser = argparse.ArgumentParser(description="Build TECL/XLK long-history research price inputs.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--start", default=DEFAULT_LONG_HISTORY_START)
    parser.add_argument("--end", default=None)
    parser.add_argument("--synthesize-tecl-from-xlk", action="store_true")
    parser.add_argument("--boxx-proxy-symbol", default="BIL")
    args = parser.parse_args()

    prices, metadata = build_tecl_long_history_inputs(
        start=args.start,
        end=args.end,
        synthesize_tecl_from_xlk=bool(args.synthesize_tecl_from_xlk),
        boxx_proxy_symbol=str(args.boxx_proxy_symbol or "BIL"),
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    prices.to_csv(args.output_dir / "price_history.csv", index=False)
    (args.output_dir / "inputs_manifest.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(
        f"wrote {len(prices)} rows -> {args.output_dir / 'price_history.csv'} "
        f"(mode={metadata.get('inputs_mode')})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
