"""Run the isolated P2 v4 free-source SOXL research candidate.

The P3 execution mechanics are shared with the frozen v3 runner, but this
entry point loads a fresh private runtime core and replaces every candidate
identity before validation.  It never imports or mutates the v3 module in the
caller process, and its subprocesses re-enter this v4 wrapper.  Thus a v3
configuration, UES revision, QPK revision, or lockfile cannot pass as v4.
"""

from __future__ import annotations

import importlib.util
from collections.abc import Mapping, Sequence
from pathlib import Path

# This file executes inside the separately pinned UES environment, where the
# UESP package is intentionally unavailable.  These literals mirror the v4
# contract module and are cross-checked by the local wrapper tests.
P2_UES_REVISION = "be692f75f64557e68edbff93786781e26c4f5893"
P2_QPK_REVISION = "f30e7b1910df8da22fdcedc347ab847df5adcd76"
P2_UES_UV_LOCK_SHA256 = "3ab6974ae8c2cece2fcff527828612eab6d4ab1baf5ab3b4a6f648c057ecc301"
P2_CANDIDATE_ID = "soxl_soxx_core_only_p2_v4_free_split_close"
P2_CONFIG_SHA256 = "142fe512dd48d9e61c8fe302710dd00eeeb1b945a60892c95c5dd4a439fd0550"


def _core():
    """Load a process-private copy of the invariant isolated execution core."""
    legacy_path = Path(__file__).with_name("run_soxl_core_only_p3_isolated.py")
    spec = importlib.util.spec_from_file_location("qsl_soxl_core_only_p3_v4_runtime_core", legacy_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("isolated SOXL v4 runner unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.P2_UES_REVISION = P2_UES_REVISION
    module.P2_QPK_REVISION = P2_QPK_REVISION
    module.P2_UES_UV_LOCK_SHA256 = P2_UES_UV_LOCK_SHA256
    module.P2_CANDIDATE_ID = P2_CANDIDATE_ID
    module.P2_CONFIG_SHA256 = P2_CONFIG_SHA256
    # The outer execution core uses __file__ to launch the inner process.  It
    # must re-enter this wrapper so the same v4 constants are installed again.
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
