from __future__ import annotations

from .contracts import MEGA_CAP_LEADER_ROTATION_AGGRESSIVE_PROFILE
from .mega_cap_leader_rotation_dynamic_top20 import main as _main


def main(argv: list[str] | None = None) -> int:
    return _main(
        argv,
        profile=MEGA_CAP_LEADER_ROTATION_AGGRESSIVE_PROFILE,
        dynamic_universe_size_default=50,
        holdings_count_default=3,
        single_name_cap_default=0.35,
        soft_defense_exposure_default=1.0,
        hard_defense_exposure_default=1.0,
        soft_breadth_threshold_default=0.0,
        hard_breadth_threshold_default=0.0,
    )


if __name__ == "__main__":
    raise SystemExit(main())
