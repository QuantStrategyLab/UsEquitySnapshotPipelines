from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from us_equity_snapshot_pipelines import tqqq_r1_snapshot as snapshot


def _fixture_prices() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"session": "2010-01-04", "symbol": "TQQQ", "adjusted_close": 10.5},
            {"session": "2010-01-04", "symbol": "QQQ", "adjusted_close": 45.25},
            {"session": "2010-01-05", "symbol": "TQQQ", "adjusted_close": 11.0},
            {"session": "2010-01-05", "symbol": "QQQ", "adjusted_close": 46.0},
        ]
    )


def test_materialize_writes_only_deterministic_immutable_artifacts(tmp_path: Path) -> None:
    output_dir = tmp_path / "snapshot"

    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), output_dir)

    assert tuple(sorted(path.name for path in output_dir.iterdir())) == (
        "manifest.json",
        "prices.csv",
        "sha256sums.json",
        "validation.json",
    )
    prices = (output_dir / "prices.csv").read_text(encoding="utf-8")
    assert prices == (
        "session,symbol,adjusted_close\n"
        "2010-01-04,QQQ,45.25\n"
        "2010-01-04,TQQQ,10.5\n"
        "2010-01-05,QQQ,46\n"
        "2010-01-05,TQQQ,11\n"
    )
    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    validation = json.loads((output_dir / "validation.json").read_text(encoding="utf-8"))
    sums = json.loads((output_dir / "sha256sums.json").read_text(encoding="utf-8"))
    assert result.manifest_sha256 == hashlib.sha256((output_dir / "manifest.json").read_bytes()).hexdigest()
    assert manifest == {
        "contract_version": "tqqq_r1_qqq_tqqq_immutable_snapshot.v1",
        "symbols": ["QQQ", "TQQQ"],
        "requested_lower_bound": "2010-01-01",
        "price_field": "adjusted_close",
        "plugin": "ABSENT_DISABLED",
        "mode": "core_only",
        "size": 0,
        "row_count": 4,
        "prices_sha256": sums["prices.csv"],
    }
    assert validation == {"valid": True, "row_count": 4, "symbols": ["QQQ", "TQQQ"]}
    assert set(sums) == {"prices.csv", "manifest.json", "validation.json"}


def test_materialize_preserves_adjusted_close_float_round_trip_precision(tmp_path: Path) -> None:
    prices = _fixture_prices()
    prices.loc[prices["symbol"].eq("QQQ") & prices["session"].eq("2010-01-04"), "adjusted_close"] = 1.0000000000000002

    result = snapshot.materialize_tqqq_r1_snapshot(prices, tmp_path / "snapshot")

    readback = pd.read_csv(result.output_dir / "prices.csv")
    actual = readback.loc[readback["symbol"].eq("QQQ"), "adjusted_close"].iloc[0]
    assert actual == 1.0000000000000002


def test_materialize_requires_qqq_and_tqqq_for_each_session(tmp_path: Path) -> None:
    prices = _fixture_prices().loc[lambda frame: ~((frame["session"] == "2010-01-05") & frame["symbol"].eq("TQQQ"))]

    with pytest.raises(snapshot.SnapshotValidationError, match="each session"):
        snapshot.materialize_tqqq_r1_snapshot(prices, tmp_path / "snapshot")


def test_materialize_rejects_timezone_aware_sessions(tmp_path: Path) -> None:
    prices = _fixture_prices().assign(session=lambda frame: pd.to_datetime(frame["session"], utc=True))

    with pytest.raises(snapshot.SnapshotValidationError, match="timezone-aware"):
        snapshot.materialize_tqqq_r1_snapshot(prices, tmp_path / "snapshot")


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (lambda frame: frame.assign(symbol="QQQ"), "missing required symbol"),
        (lambda frame: pd.concat([frame, frame.iloc[[0]]], ignore_index=True), "duplicate session"),
        (lambda frame: frame.assign(adjusted_close=0), "positive finite"),
        (lambda frame: frame.assign(session="2010-01-02"), "weekday"),
        (lambda frame: frame.assign(session="2009-12-31"), "lower bound"),
    ],
)
def test_materialize_fails_closed_for_invalid_prices(tmp_path: Path, mutator, message: str) -> None:
    with pytest.raises(snapshot.SnapshotValidationError, match=message):
        snapshot.materialize_tqqq_r1_snapshot(mutator(_fixture_prices()), tmp_path / "snapshot")


def test_materialize_rejects_non_core_mode_and_tampered_readback(tmp_path: Path) -> None:
    with pytest.raises(snapshot.SnapshotValidationError, match="core_only"):
        snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "non-core", mode="production")

    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")
    (tmp_path / "snapshot" / "prices.csv").write_text("tampered\n", encoding="utf-8")
    with pytest.raises(snapshot.SnapshotValidationError, match="hash mismatch"):
        snapshot.verify_tqqq_r1_snapshot(result.output_dir, expected_manifest_sha256=result.manifest_sha256)


def test_verify_requires_external_manifest_digest_and_rejects_coordinated_tampering(tmp_path: Path) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")
    output_dir = result.output_dir
    (output_dir / "prices.csv").write_text(
        "session,symbol,adjusted_close\n2010-01-04,QQQ,99\n2010-01-04,TQQQ,99\n",
        encoding="utf-8",
    )
    (output_dir / "manifest.json").write_text("{}\n", encoding="utf-8")
    (output_dir / "sha256sums.json").write_text(
        json.dumps(
            {
                "prices.csv": hashlib.sha256((output_dir / "prices.csv").read_bytes()).hexdigest(),
                "manifest.json": hashlib.sha256((output_dir / "manifest.json").read_bytes()).hexdigest(),
                "validation.json": hashlib.sha256((output_dir / "validation.json").read_bytes()).hexdigest(),
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(snapshot.SnapshotValidationError, match="trusted manifest hash mismatch"):
        snapshot.verify_tqqq_r1_snapshot(output_dir, expected_manifest_sha256=result.manifest_sha256)
