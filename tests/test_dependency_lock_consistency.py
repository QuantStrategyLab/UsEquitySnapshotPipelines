from __future__ import annotations

import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GIT_DEPENDENCIES = (
    "quant-platform-kit",
    "quant-strategy-plugins",
    "us-equity-strategies",
)


def _pyproject_git_revisions() -> dict[str, str]:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text())
    revisions: dict[str, str] = {}
    for dependency in pyproject["project"]["dependencies"]:
        for name in GIT_DEPENDENCIES:
            prefix = f"{name} @ git+"
            if dependency.startswith(prefix):
                revisions[name] = dependency.rsplit("@", 1)[1]
    return revisions


def test_uv_lock_matches_pyproject_git_dependency_refs():
    pyproject_revisions = _pyproject_git_revisions()
    lock_text = (ROOT / "uv.lock").read_text()

    assert set(pyproject_revisions) == set(GIT_DEPENDENCIES)
    for name, revision in pyproject_revisions.items():
        pattern = rf'name = "{re.escape(name)}"\n(?:.*\n){{0,8}}source = \{{ git = "[^"]*rev={re.escape(revision)}#'
        assert re.search(pattern, lock_text), f"uv.lock does not match pyproject ref for {name}"
