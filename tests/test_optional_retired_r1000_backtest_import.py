import importlib


def test_package_import_does_not_require_retired_r1000_strategy():
    module = importlib.import_module("us_equity_snapshot_pipelines.global_etf_rotation_snapshot")

    assert hasattr(module, "build_global_etf_rotation_feature_snapshot")
