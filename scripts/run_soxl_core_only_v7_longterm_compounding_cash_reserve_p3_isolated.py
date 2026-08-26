"""Run the isolated P2 v7 SOXL research candidate without execution authority."""

from __future__ import annotations

import importlib.util
from collections.abc import Sequence
from pathlib import Path

P2_UES_REVISION = "07b164d95f2ab4d4c54fd993f6f2040bd207d664"
P2_QPK_REVISION = "f30e7b1910df8da22fdcedc347ab847df5adcd76"
P2_UES_UV_LOCK_SHA256 = "3ab6974ae8c2cece2fcff527828612eab6d4ab1baf5ab3b4a6f648c057ecc301"
P2_CANDIDATE_ID = "soxl_soxx_core_only_p2_v7_longterm_compounding_cash_reserve"
P2_CONFIG_SHA256 = "843ab4e93e81985c2b3becc61a2f0b971508ccf25afa59acf402e75f574514d1"


def _core():
    legacy_path = Path(__file__).with_name("run_soxl_core_only_p3_isolated.py")
    spec = importlib.util.spec_from_file_location("qsl_soxl_core_only_p3_v7_runtime_core", legacy_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("isolated SOXL v7 runner unavailable")
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


def run_isolated_source(*, ues_project: Path, input_path: Path, p2_candidate_path: Path) -> dict[str, object]:
    return _core().run_isolated_source(ues_project=ues_project, input_path=input_path, p2_candidate_path=p2_candidate_path)


def run_isolated_batch(*, ues_project: Path, input_path: Path, p2_candidate_path: Path) -> dict[str, object]:
    return _core().run_isolated_batch(ues_project=ues_project, input_path=input_path, p2_candidate_path=p2_candidate_path)


def run_isolated_replay(*, ues_project: Path, input_path: Path, p2_candidate_path: Path) -> dict[str, object]:
    return _core().run_isolated_replay(ues_project=ues_project, input_path=input_path, p2_candidate_path=p2_candidate_path)


def main(argv: Sequence[str] | None = None) -> int:
    return _core().main(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
