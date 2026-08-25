"""Run the isolated P2 v5 long-term SOXL research candidate.

The execution mechanics remain the frozen P3 core, but this wrapper installs
the v5 candidate identity before every validation and subprocess re-entry.
It is research-only and cannot load or execute a broker strategy.
"""

from __future__ import annotations

import importlib.util
from collections.abc import Sequence
from pathlib import Path

P2_UES_REVISION = "be692f75f64557e68edbff93786781e26c4f5893"
P2_QPK_REVISION = "f30e7b1910df8da22fdcedc347ab847df5adcd76"
P2_UES_UV_LOCK_SHA256 = "3ab6974ae8c2cece2fcff527828612eab6d4ab1baf5ab3b4a6f648c057ecc301"
P2_CANDIDATE_ID = "soxl_soxx_core_only_p2_v5_longterm_drawdown"
P2_CONFIG_SHA256 = "d1e9278400b1f94ebdf6bb43e796c50edc7f616fa8cb647a38b4eec32cb0f0ba"


def _core():
    """Load a process-private copy of the invariant isolated execution core."""
    legacy_path = Path(__file__).with_name("run_soxl_core_only_p3_isolated.py")
    spec = importlib.util.spec_from_file_location("qsl_soxl_core_only_p3_v5_runtime_core", legacy_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("isolated SOXL v5 runner unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.P2_UES_REVISION = P2_UES_REVISION
    module.P2_QPK_REVISION = P2_QPK_REVISION
    module.P2_UES_UV_LOCK_SHA256 = P2_UES_UV_LOCK_SHA256
    module.P2_CANDIDATE_ID = P2_CANDIDATE_ID
    module.P2_CONFIG_SHA256 = P2_CONFIG_SHA256
    module.ALLOWED_VOLATILITY_DELEVER_REDIRECT_SYMBOLS = frozenset({"SOXX", "BOXX", None})
    module.__file__ = str(Path(__file__).resolve())
    return module


def validate_p2_candidate(value: object) -> dict[str, object]:
    return _core().validate_p2_candidate(value)


def validate_ues_project(path: Path) -> dict[str, str]:
    return _core().validate_ues_project(path)


def run_isolated_source(
    *, ues_project: Path, input_path: Path, p2_candidate_path: Path
) -> dict[str, object]:
    return _core().run_isolated_source(
        ues_project=ues_project,
        input_path=input_path,
        p2_candidate_path=p2_candidate_path,
    )


def run_isolated_batch(
    *, ues_project: Path, input_path: Path, p2_candidate_path: Path
) -> dict[str, object]:
    return _core().run_isolated_batch(
        ues_project=ues_project,
        input_path=input_path,
        p2_candidate_path=p2_candidate_path,
    )


def run_isolated_replay(
    *, ues_project: Path, input_path: Path, p2_candidate_path: Path
) -> dict[str, object]:
    return _core().run_isolated_replay(
        ues_project=ues_project,
        input_path=input_path,
        p2_candidate_path=p2_candidate_path,
    )


def main(argv: Sequence[str] | None = None) -> int:
    return _core().main(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
