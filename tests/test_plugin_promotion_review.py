from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from us_equity_snapshot_pipelines.ibit_smart_dca_research import (
    build_ibit_smart_dca_research,
    write_ibit_smart_dca_research_outputs,
)
from us_equity_snapshot_pipelines.plugin_promotion_review import (
    build_plugin_promotion_review_from_ibit_research_manifest,
    write_plugin_promotion_review_artifacts,
)


def _price_rows() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=80)
    rows: list[dict[str, object]] = []
    for idx, as_of in enumerate(dates):
        rows.append({"as_of": as_of, "symbol": "IBIT", "close": 100.0 + idx * 0.5})
        rows.append({"as_of": as_of, "symbol": "BOXX", "close": 100.0 + idx * 0.02})
        rows.append({"as_of": as_of, "symbol": "QQQ", "close": 100.0 + idx * 0.3})
        rows.append({"as_of": as_of, "symbol": "SPY", "close": 100.0 + idx * 0.2})
    return pd.DataFrame(rows)


def _zscore_history(*, sparse_tail: bool = False) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=80)
    rows = []
    for idx, as_of in enumerate(dates):
        payload: dict[str, object] = {"as_of": as_of}
        if sparse_tail and idx >= 70:
            payload["mvrv_zscore"] = None
        else:
            payload["mvrv_zscore"] = 9.5 if idx >= 60 else 2.0 + (idx % 10) * 0.05
        rows.append(payload)
    return pd.DataFrame(rows)


def _research_manifest(tmp_path: Path, *, sparse_tail: bool = False) -> Path:
    result = build_ibit_smart_dca_research(
        _price_rows(),
        zscore_history=_zscore_history(sparse_tail=sparse_tail),
        initial_parking_value=1_000.0,
        contribution_amount=100.0,
        plugin_enabled=True,
        plugin_config={"dynamic_min_periods": 5},
    )
    outputs = write_ibit_smart_dca_research_outputs(result, tmp_path / "ibit_research")
    return outputs["manifest"]


def test_plugin_promotion_review_maps_ibit_research_into_contract(tmp_path: Path) -> None:
    manifest_path = _research_manifest(tmp_path)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["review_summary"]["plugin_gate"] = "pass"
    payload["review_summary"]["zscore_coverage_gate"] = "pass"
    payload["review_summary"]["promotion_blockers"] = []
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    review = build_plugin_promotion_review_from_ibit_research_manifest(manifest_path)

    assert list(review["strategy"]) == ["ibit_smart_dca"]
    assert list(review["plugin"]) == ["ibit_zscore_exit"]
    row = review.iloc[0]
    assert bool(row["coverage_gate_passed"]) is True
    assert bool(row["research_gate_passed"]) is True
    assert bool(row["required_gates_passed"]) is True
    assert bool(row["notification_allowed"]) is True
    assert bool(row["position_control_allowed"]) is False
    assert row["plugin_role"] == "notification_only"
    assert bool(row["replace_live_component_now"]) is False
    assert "policy_still_notification_only" in str(row["blocking_reason"])
    assert row["recommended_action"] == "prepare_separate_promotion_artifact"


def test_plugin_promotion_review_preserves_research_blockers(tmp_path: Path) -> None:
    manifest_path = _research_manifest(tmp_path, sparse_tail=True)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["review_summary"]["plugin_gate"] = "pass"
    payload["review_summary"]["zscore_coverage_gate"] = "fail"
    payload["review_summary"]["promotion_blockers"] = ["zscore_coverage_below_minimum"]
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    review = build_plugin_promotion_review_from_ibit_research_manifest(manifest_path)

    row = review.iloc[0]
    assert bool(row["coverage_gate_passed"]) is False
    assert bool(row["required_gates_passed"]) is False
    assert "zscore_coverage_below_minimum" in str(row["blocking_reason"])
    assert row["recommended_action"] == "continue_research"


def test_write_plugin_promotion_review_artifacts_writes_csv_manifest_and_markdown(tmp_path: Path) -> None:
    manifest_path = _research_manifest(tmp_path)

    outputs = write_plugin_promotion_review_artifacts(
        source_manifest_path=manifest_path,
        output_dir=tmp_path / "plugin_promotion_review",
    )

    assert outputs["review_csv"].exists()
    assert outputs["review_md"].exists()
    assert outputs["manifest"].exists()

    manifest = json.loads(outputs["manifest"].read_text(encoding="utf-8"))
    assert manifest["manifest_type"] == "strategy_plugin_promotion_review"
    assert manifest["artifact_schema_version"] == "strategy_plugin_promotion_review.v1"
    assert manifest["strategy"] == "ibit_smart_dca"
    assert manifest["plugin"] == "ibit_zscore_exit"
    assert manifest["row_count"] == 1
    assert manifest["replace_live_component_now_count"] == 0
