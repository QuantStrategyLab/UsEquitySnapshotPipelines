from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.build_monthly_plugin_promotion_reviews import (
    discover_plugin_promotion_review_inputs,
    main,
)
from us_equity_snapshot_pipelines.ibit_smart_dca_research import (
    build_ibit_smart_dca_research,
    write_ibit_smart_dca_research_outputs,
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


def _zscore_history() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=80)
    return pd.DataFrame(
        {
            "as_of": dates,
            "mvrv_zscore": [9.5 if idx >= 60 else 2.0 + (idx % 10) * 0.05 for idx in range(len(dates))],
        }
    )


def _write_research_manifest(root: Path) -> Path:
    result = build_ibit_smart_dca_research(
        _price_rows(),
        zscore_history=_zscore_history(),
        initial_parking_value=1_000.0,
        contribution_amount=100.0,
        plugin_enabled=True,
        plugin_config={"dynamic_min_periods": 5},
    )
    outputs = write_ibit_smart_dca_research_outputs(result, root / "ibit_smart_dca_research_20260624")
    return outputs["manifest"]


def test_discover_plugin_promotion_review_inputs_finds_ibit_research_manifests(tmp_path: Path) -> None:
    manifest_path = _write_research_manifest(tmp_path)

    discovered = discover_plugin_promotion_review_inputs(tmp_path)

    assert len(discovered) == 1
    assert discovered[0] == manifest_path


def test_monthly_plugin_promotion_review_builder_writes_outputs(tmp_path: Path, capsys) -> None:
    _write_research_manifest(tmp_path)
    output_root = tmp_path / "monthly_plugin_reviews"

    exit_code = main(
        [
            "--artifact-root",
            str(tmp_path),
            "--output-root",
            str(output_root),
        ]
    )

    assert exit_code == 0
    review_manifests = sorted(output_root.rglob("plugin_promotion_review_manifest.json"))
    assert len(review_manifests) == 1
    manifest = json.loads(review_manifests[0].read_text(encoding="utf-8"))
    assert manifest["manifest_type"] == "strategy_plugin_promotion_review"
    assert manifest["plugin"] == "ibit_zscore_exit"
    assert manifest["strategy"] == "ibit_smart_dca"
    captured = capsys.readouterr()
    assert "plugin_promotion_review_count=1" in captured.out

