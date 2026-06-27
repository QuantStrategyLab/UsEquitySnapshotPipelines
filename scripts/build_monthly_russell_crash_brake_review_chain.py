#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_live_decay_followup import main as live_decay_main  # noqa: E402
from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_live_readiness_followup import main as live_readiness_main  # noqa: E402
from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_liquidity_followup import main as liquidity_main  # noqa: E402
from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_overfit_followup import main as overfit_main  # noqa: E402
from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_promotion_review import main as promotion_review_main  # noqa: E402
from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_shadow_review import main as shadow_review_main  # noqa: E402
from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_stress_followup import main as stress_main  # noqa: E402

DEFAULT_ARTIFACT_ROOT = PROJECT_ROOT / "data" / "output"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build monthly Russell crash-brake follow-up artifacts from discovered crash-brake research outputs."
    )
    parser.add_argument("--artifact-root", default=str(DEFAULT_ARTIFACT_ROOT))
    parser.add_argument("--output-root", default="")
    parser.add_argument("--snapshot-as-of", default="")
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_path(base_dir: Path, raw: Any) -> Path | None:
    if not raw:
        return None
    path = Path(str(raw))
    if path.is_absolute():
        return path
    if path.exists():
        return path.resolve()
    return (base_dir / path).resolve()


def discover_crash_brake_research_runs(artifact_root: Path) -> list[dict[str, Path]]:
    discovered: list[dict[str, Path]] = []
    for manifest_path in sorted(artifact_root.rglob("crash_brake_research_manifest.json")):
        try:
            payload = _load_json(manifest_path)
        except Exception:
            continue
        if str(payload.get("manifest_type", "")).strip() != "russell_top50_crash_brake_research":
            continue
        artifact_dir = manifest_path.parent
        inputs = payload.get("inputs") or {}
        prices_path = _resolve_path(artifact_dir, inputs.get("prices"))
        universe_path = _resolve_path(artifact_dir, inputs.get("universe"))
        summary_path = artifact_dir / "crash_brake_summary.csv"
        rolling_path = artifact_dir / "crash_brake_rolling_summary.csv"
        mode_path = artifact_dir / "crash_brake_mode_history.csv"
        trades_path = artifact_dir / "crash_brake_rebalance_trades.csv"
        returns_path = artifact_dir / "crash_brake_daily_returns.csv"
        required = [prices_path, universe_path, summary_path, rolling_path, trades_path, returns_path]
        if any(path is None or not path.exists() for path in required):
            continue
        discovered.append(
            {
                "manifest_path": manifest_path,
                "artifact_dir": artifact_dir,
                "prices_path": prices_path,
                "universe_path": universe_path,
                "summary_path": summary_path,
                "rolling_path": rolling_path,
                "mode_path": mode_path,
                "trades_path": trades_path,
                "returns_path": returns_path,
            }
        )
    return discovered


def _output_dir(base_root: Path, run_dir: Path, name: str, *, output_root_override: bool) -> Path:
    if not output_root_override:
        return base_root / name
    return base_root / f"{run_dir.name}__{name}"


def build_crash_brake_review_chain(
    run: dict[str, Path],
    *,
    output_root: Path | None = None,
    snapshot_as_of: str = "",
) -> dict[str, Path]:
    artifact_dir = Path(run["artifact_dir"])
    manifest_path = Path(run["manifest_path"])
    output_root_override = output_root is not None
    base_root = output_root if output_root is not None else artifact_dir

    overfit_dir = _output_dir(base_root, artifact_dir, "crash_brake_overfit_followup", output_root_override=output_root_override)
    stress_dir = _output_dir(base_root, artifact_dir, "crash_brake_stress_followup", output_root_override=output_root_override)
    liquidity_dir = _output_dir(base_root, artifact_dir, "crash_brake_liquidity_followup", output_root_override=output_root_override)
    live_readiness_dir = _output_dir(
        base_root, artifact_dir, "crash_brake_live_readiness_followup", output_root_override=output_root_override
    )
    promotion_dir = _output_dir(base_root, artifact_dir, "crash_brake_promotion_review", output_root_override=output_root_override)
    shadow_dir = _output_dir(base_root, artifact_dir, "crash_brake_shadow_review", output_root_override=output_root_override)
    live_decay_dir = _output_dir(base_root, artifact_dir, "live_decay_monitor_crash_brake", output_root_override=output_root_override)

    if overfit_main(
        [
            "--summary",
            str(run["summary_path"]),
            "--rolling",
            str(run["rolling_path"]),
            "--research-manifest",
            str(manifest_path),
            "--output-dir",
            str(overfit_dir),
        ]
    ):
        raise RuntimeError(f"failed to build crash-brake overfit follow-up for {artifact_dir}")

    if stress_main(
        [
            "--prices",
            str(run["prices_path"]),
            "--universe",
            str(run["universe_path"]),
            "--research-manifest",
            str(manifest_path),
            "--output-dir",
            str(stress_dir),
        ]
    ):
        raise RuntimeError(f"failed to build crash-brake stress follow-up for {artifact_dir}")

    if liquidity_main(
        [
            "--trades",
            str(run["trades_path"]),
            "--prices",
            str(run["prices_path"]),
            "--research-manifest",
            str(manifest_path),
            "--output-dir",
            str(liquidity_dir),
        ]
    ):
        raise RuntimeError(f"failed to build crash-brake liquidity follow-up for {artifact_dir}")

    live_readiness_args = [
        "--summary",
        str(run["summary_path"]),
        "--rolling",
        str(run["rolling_path"]),
        "--research-manifest",
        str(manifest_path),
        "--output-dir",
        str(live_readiness_dir),
    ]
    if run.get("mode_path") is not None and Path(run["mode_path"]).exists():
        live_readiness_args.extend(["--mode-history", str(run["mode_path"])])
    if live_readiness_main(live_readiness_args):
        raise RuntimeError(f"failed to build crash-brake live-readiness follow-up for {artifact_dir}")

    if promotion_review_main(
        [
            "--summary",
            str(run["summary_path"]),
            "--research-manifest",
            str(manifest_path),
            "--live-readiness",
            str(live_readiness_dir / "crash_brake_live_readiness_summary.csv"),
            "--overfit-promotion",
            str(overfit_dir / "overfit_promotion_gate_summary.csv"),
            "--stress-summary",
            str(stress_dir / "crash_brake_stress_summary.csv"),
            "--liquidity-summary",
            str(liquidity_dir / "liquidity_summary.csv"),
            "--output-dir",
            str(promotion_dir),
        ]
    ):
        raise RuntimeError(f"failed to build crash-brake promotion review for {artifact_dir}")

    shadow_args = [
        "--summary",
        str(run["summary_path"]),
        "--trades",
        str(run["trades_path"]),
        "--output-dir",
        str(shadow_dir),
    ]
    if snapshot_as_of:
        shadow_args.extend(["--snapshot-as-of", snapshot_as_of])
    if shadow_review_main(shadow_args):
        raise RuntimeError(f"failed to build crash-brake shadow review for {artifact_dir}")

    if live_decay_main(
        [
            "--returns",
            str(run["returns_path"]),
            "--research-manifest",
            str(manifest_path),
            "--output-dir",
            str(live_decay_dir),
        ]
    ):
        raise RuntimeError(f"failed to build crash-brake live decay follow-up for {artifact_dir}")

    return {
        "overfit_dir": overfit_dir,
        "stress_dir": stress_dir,
        "liquidity_dir": liquidity_dir,
        "live_readiness_dir": live_readiness_dir,
        "promotion_dir": promotion_dir,
        "shadow_dir": shadow_dir,
        "live_decay_dir": live_decay_dir,
    }


def main() -> int:
    args = parse_args()
    artifact_root = Path(args.artifact_root)
    output_root = Path(args.output_root) if args.output_root else None
    outputs: list[dict[str, Path]] = []
    for run in discover_crash_brake_research_runs(artifact_root):
        outputs.append(
            build_crash_brake_review_chain(
                run,
                output_root=output_root,
                snapshot_as_of=str(args.snapshot_as_of or ""),
            )
        )
    print(f"crash_brake_review_chain_count={len(outputs)}")
    for item in outputs:
        print(f"crash_brake_promotion_review_dir={item['promotion_dir']}")
        print(f"crash_brake_shadow_review_dir={item['shadow_dir']}")
        print(f"crash_brake_live_decay_dir={item['live_decay_dir']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
