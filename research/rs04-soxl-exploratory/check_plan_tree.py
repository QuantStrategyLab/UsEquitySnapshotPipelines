"""Offline checks for the protected plan's frozen-build binding."""

from hashlib import sha256
import json

import pytest

from plan_tree import expected_tree


def test_plan_tree_binding():
    source, tree = "a" * 40, "b" * 40
    raw = json.dumps({"source_sha": source, "ues_tree": tree}).encode()
    digest = sha256(raw).hexdigest()
    assert expected_tree(raw, digest, source, source) == tree
    with pytest.raises(ValueError):
        expected_tree(raw, "0" * 64, source, source)
    with pytest.raises(ValueError):
        expected_tree(raw, digest, source, "c" * 40)
    with pytest.raises(ValueError):
        expected_tree(raw, digest, "c" * 40, "c" * 40)
    body = b"[]"
    with pytest.raises(ValueError):
        expected_tree(body, sha256(body).hexdigest(), source, source)
    for bad in ("short", "G" * 40, None):
        body = json.dumps({"source_sha": source, "ues_tree": bad}).encode()
        with pytest.raises(ValueError):
            expected_tree(body, sha256(body).hexdigest(), source, source)


def test_cli_does_not_emit_plan_on_failure(tmp_path):
    import os
    from pathlib import Path
    import subprocess
    import sys

    source, tree = "a" * 40, "b" * 40
    plan = json.dumps({"source_sha": source, "ues_tree": tree, "private": "sentinel"})
    output = tmp_path / "output"
    env = os.environ | {"RS04_SOXL_EXPLORATORY_PLAN": plan,
                        "RS04_SOXL_EXPLORATORY_PLAN_SHA256": "0" * 64,
                        "GITHUB_SHA": source, "RS04_SOXL_EXPLORATORY_EXPECTED_SHA": source,
                        "GITHUB_OUTPUT": str(output)}
    script = Path(__file__).with_name("plan_tree.py")
    result = subprocess.run([sys.executable, str(script)], env=env, capture_output=True, text=True)
    assert result.returncode == 2
    assert result.stdout == "SOXL_EXPLORATORY_BINDINGS_STOPPED\n"
    assert result.stderr == "" and not output.exists()
    env["RS04_SOXL_EXPLORATORY_PLAN_SHA256"] = sha256(plan.encode()).hexdigest()
    result = subprocess.run([sys.executable, str(script)], env=env, capture_output=True, text=True)
    assert result.returncode == 0 and result.stdout == f"::add-mask::{tree}\n"
    assert output.read_text() == f"ues_tree={tree}\n"
