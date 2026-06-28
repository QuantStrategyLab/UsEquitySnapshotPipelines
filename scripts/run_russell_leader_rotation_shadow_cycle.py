#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
USEQ_STRATEGIES_ROOT = PROJECT_ROOT.parent / "UsEquityStrategies" / "src"
QPK_ROOT = PROJECT_ROOT.parent / "QuantPlatformKit" / "src"
for path in (SRC_ROOT, USEQ_STRATEGIES_ROOT, QPK_ROOT):
    text = str(path)
    if text not in sys.path:
        sys.path.insert(0, text)

from us_equity_snapshot_pipelines.russell_leader_rotation_shadow_cycle import main


if __name__ == "__main__":
    raise SystemExit(main())
