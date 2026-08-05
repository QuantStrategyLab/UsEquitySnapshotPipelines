from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


SCRIPT = Path(__file__).parents[1] / "scripts" / "run_soxl_promotion_research.py"


def _module():
    spec = importlib.util.spec_from_file_location("run_soxl_promotion_research", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_cli_requires_explicit_private_paths(monkeypatch) -> None:
    module = _module()
    monkeypatch.setattr(sys, "argv", [str(SCRIPT)])

    with pytest.raises(SystemExit) as exc:
        module.main()

    assert exc.value.code == 2


def test_cli_contains_no_provider_or_cloud_fallback() -> None:
    runner = (
        Path(__file__).parents[1]
        / "src/us_equity_snapshot_pipelines/lifecycle/soxl_promotion_runner.py"
    )
    source = (SCRIPT.read_text(encoding="utf-8") + runner.read_text(encoding="utf-8")).lower()

    assert "--input" in source
    assert "--config" in source
    assert "--output" in source
    for forbidden in ("yfinance", "tiingo", "ibkr", "download", "google.cloud", "boto"):
        assert forbidden not in source
