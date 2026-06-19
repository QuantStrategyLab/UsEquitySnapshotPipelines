from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd


SCRIPT_PATH = Path("scripts/download_ibit_zscore_metrics.py")


def _load_script_module():
    spec = importlib.util.spec_from_file_location("download_ibit_zscore_metrics", SCRIPT_PATH)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_normalize_ibit_zscore_metrics_accepts_common_aliases() -> None:
    module = _load_script_module()

    normalized = module.normalize_ibit_zscore_metrics(
        pd.DataFrame(
            {
                "date": ["2026-01-02", "2026-01-01", "bad"],
                "mvrv_z": ["2.7", "2.5", "bad"],
            }
        )
    )

    assert normalized.to_dict("records") == [
        {"as_of": "2026-01-01", "mvrv_zscore": 2.5},
        {"as_of": "2026-01-02", "mvrv_zscore": 2.7},
    ]


def test_frame_from_payload_accepts_nested_records_and_filters_dates() -> None:
    module = _load_script_module()

    frame = module.frame_from_payload(
        {
            "data": [
                {"timestamp": "2026-01-01", "z_score": 2.5},
                {"timestamp": "2026-01-02", "z_score": 2.7},
                {"timestamp": "2026-01-03", "z_score": 2.9},
            ]
        }
    )
    normalized = module.normalize_ibit_zscore_metrics(frame, start="2026-01-02", end="2026-01-02")

    assert normalized.to_dict("records") == [{"as_of": "2026-01-02", "mvrv_zscore": 2.7}]


def test_frame_from_payload_accepts_columns_and_rows_shape() -> None:
    module = _load_script_module()

    frame = module.frame_from_payload(
        {
            "columns": ["time", "mvrv_zscore"],
            "data": [["2026-01-01", 2.5], ["2026-01-02", 2.7]],
        }
    )
    normalized = module.normalize_ibit_zscore_metrics(frame)

    assert normalized.to_dict("records") == [
        {"as_of": "2026-01-01", "mvrv_zscore": 2.5},
        {"as_of": "2026-01-02", "mvrv_zscore": 2.7},
    ]
