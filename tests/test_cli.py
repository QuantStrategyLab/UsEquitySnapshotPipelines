"""Unit tests for the unified `useq` CLI."""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path
from types import SimpleNamespace

import pytest

from us_equity_snapshot_pipelines import cli


def test_cli_help_lists_subcommands(capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli.main(["--help"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Unified CLI for UsEquitySnapshotPipelines." in out
    assert "publish-snapshot-artifacts" in out
    assert "backtest-global-etf-offensive-rotation" in out


def test_cli_no_args_prints_help(capsys: pytest.CaptureFixture[str]) -> None:
    rc = cli.main([])
    assert rc == 0
    assert "available subcommands:" in capsys.readouterr().out


def test_cli_unknown_subcommand_exits_2() -> None:
    with pytest.raises(SystemExit) as exc:
        cli.main(["nonexistent-subcommand"])
    assert exc.value.code == 2


def test_cli_dispatch_passes_args_to_target(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[list[str]] = []

    def fake_main(argv: list[str] | None = None) -> int:
        captured.append(list(argv or []))
        return 7

    monkeypatch.setattr(cli.importlib, "import_module", lambda _name: SimpleNamespace(main=fake_main))
    rc = cli.main(["publish-snapshot-artifacts", "--dry-run", "--date", "2026-01-01"])
    assert rc == 7
    assert captured == [["--dry-run", "--date", "2026-01-01"]]


def test_cli_dispatch_zero_arg_target_patches_sys_argv(monkeypatch: pytest.MonkeyPatch) -> None:
    observed_argv: list[list[str]] = []

    def zero_arg_main() -> int:
        observed_argv.append(list(sys.argv))
        return 0

    monkeypatch.setattr(cli.importlib, "import_module", lambda _name: SimpleNamespace(main=zero_arg_main))
    original_argv = list(sys.argv)
    rc = cli.main(["research-ibit-smart-dca", "--help"])
    assert rc == 0
    assert observed_argv == [["useq research-ibit-smart-dca", "--help"]]
    assert sys.argv == original_argv


def test_cli_commands_match_pyproject_scripts() -> None:
    pyproject_path = Path(__file__).resolve().parents[1] / "pyproject.toml"
    data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    scripts = data["project"]["scripts"]

    assert scripts["useq"] == "us_equity_snapshot_pipelines.cli:main"
    expected_from_scripts = {
        name.removeprefix("useq-"): target.split(":", 1)[0]
        for name, target in scripts.items()
        if name.startswith("useq-")
    }
    actual_from_cli = {name: spec.module for name, spec in cli.COMMANDS.items()}
    assert actual_from_cli == expected_from_scripts
