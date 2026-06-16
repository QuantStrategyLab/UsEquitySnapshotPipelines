from __future__ import annotations

import json

import pytest

from us_equity_snapshot_pipelines.artifacts import (
    normalize_strategy_plugin_gcs_prefix,
    write_strategy_plugin_release_manifest,
)


def test_write_strategy_plugin_release_manifest_creates_versioned_release(tmp_path) -> None:
    output_dir = tmp_path / "plugins" / "market_regime_control"
    output_dir.mkdir(parents=True)
    (output_dir / "latest_signal.json").write_text(
        json.dumps(
            {
                "schema_version": "market_regime_control.v1",
                "strategy": "tqqq_growth_income",
                "plugin": "market_regime_control",
                "mode": "shadow",
                "effective_mode": "shadow",
                "as_of": "2026-05-28",
                "canonical_route": "risk_reduced",
                "suggested_action": "delever",
            }
        ),
        encoding="utf-8",
    )
    (output_dir / "signal_history.csv").write_text("as_of,route\n2026-05-28,risk_reduced\n", encoding="utf-8")

    manifest_path = write_strategy_plugin_release_manifest(
        output_dir=output_dir,
        repository="QuantStrategyLab/UsEquitySnapshotPipelines",
        git_sha="abcdef1234567890",
        run_id="12345",
        run_attempt="2",
    )

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    release_dir = output_dir / "releases" / "2026-05-28-12345-attempt-2"

    assert manifest["manifest_type"] == "strategy_plugin_release"
    assert manifest["contract_version"] == "market_regime_control.v1"
    assert manifest["version"] == "2026-05-28-12345-attempt-2"
    assert manifest["producer"]["git_sha"] == "abcdef1234567890"
    assert manifest["release_dir"] == str(release_dir)
    assert (release_dir / "latest_signal.json").exists()
    assert (release_dir / "signal_history.csv").exists()
    assert (release_dir / "release_manifest.json").exists()
    assert manifest["release_artifacts"]["latest_signal.json"]["sha256"]
    assert manifest["current_artifacts"]["latest_signal.json"]["sha256"]


def test_normalize_strategy_plugin_gcs_prefix_accepts_known_artifact_root() -> None:
    assert (
        normalize_strategy_plugin_gcs_prefix(
            "gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/"
            "soxl_soxx_trend_income/plugins/market_regime_control/"
        )
        == "gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/"
        "soxl_soxx_trend_income/plugins/market_regime_control"
    )


@pytest.mark.parametrize(
    "prefix",
    [
        "",
        "gs://other-bucket/strategy-artifacts/us_equity/tqqq_growth_income/plugins/market_regime_control",
        "gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity",
        "gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/tqqq_growth_income/market_regime_control",
        "gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/tqqq_growth_income/plugins",
        "gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/tqqq_growth_income/../plugins/market_regime_control",
    ],
)
def test_normalize_strategy_plugin_gcs_prefix_rejects_out_of_policy_prefix(prefix: str) -> None:
    with pytest.raises(ValueError):
        normalize_strategy_plugin_gcs_prefix(prefix)
