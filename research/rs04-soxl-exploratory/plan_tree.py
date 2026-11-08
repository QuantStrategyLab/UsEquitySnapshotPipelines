"""Bind the frozen build tree to the protected plan before installing packages."""

from hashlib import sha256
import json
import os
import re


def expected_tree(plan_bytes, digest, source_sha, expected_sha):
    if (len(plan_bytes) > 48 * 1024
            or not re.fullmatch(r"[0-9a-f]{64}", digest)
            or sha256(plan_bytes).hexdigest() != digest
            or not re.fullmatch(r"[0-9a-f]{40}", source_sha)
            or source_sha != expected_sha):
        raise ValueError("PLAN_UNBOUND")
    plan = json.loads(plan_bytes)
    if not isinstance(plan, dict):
        raise ValueError("PLAN_UNBOUND")
    tree = plan.get("ues_tree")
    if plan.get("source_sha") != source_sha or not isinstance(tree, str) or not re.fullmatch(r"[0-9a-f]{40}", tree):
        raise ValueError("PLAN_UNBOUND")
    return tree


def main():
    try:
        tree = expected_tree(os.environ["RS04_SOXL_EXPLORATORY_PLAN"].encode(),
                             os.environ["RS04_SOXL_EXPLORATORY_PLAN_SHA256"],
                             os.environ["GITHUB_SHA"], os.environ["RS04_SOXL_EXPLORATORY_EXPECTED_SHA"])
        print(f"::add-mask::{tree}")
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
            output.write(f"ues_tree={tree}\n")
    except (KeyError, ValueError, TypeError, OSError):
        print("SOXL_EXPLORATORY_BINDINGS_STOPPED")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
