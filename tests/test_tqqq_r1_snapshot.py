from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
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


def _write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _refresh_trusted_metadata(output_dir: Path) -> str:
    manifest_path = output_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["prices_sha256"] = hashlib.sha256((output_dir / "prices.csv").read_bytes()).hexdigest()
    _write_json(manifest_path, manifest)
    _write_json(
        output_dir / "sha256sums.json",
        {
            name: hashlib.sha256((output_dir / name).read_bytes()).hexdigest()
            for name in ("prices.csv", "manifest.json", "validation.json")
        },
    )
    return hashlib.sha256(manifest_path.read_bytes()).hexdigest()


def test_materialize_writes_deterministic_immutable_artifacts(tmp_path: Path) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")

    assert tuple(sorted(path.name for path in result.output_dir.iterdir())) == (
        "manifest.json",
        "prices.csv",
        "sha256sums.json",
        "validation.json",
    )
    assert (result.output_dir / "prices.csv").read_text(encoding="utf-8") == (
        "session,symbol,adjusted_close\n"
        "2010-01-04,QQQ,45.25\n"
        "2010-01-04,TQQQ,10.5\n"
        "2010-01-05,QQQ,46\n"
        "2010-01-05,TQQQ,11\n"
    )
    assert json.loads((result.output_dir / "manifest.json").read_text(encoding="utf-8"))["contract_version"] == (
        "tqqq_r1_qqq_tqqq_immutable_snapshot.v2"
    )
    assert snapshot.verify_tqqq_r1_snapshot(
        result.output_dir, expected_manifest_sha256=result.manifest_sha256
    ) == result


def test_materialize_preserves_adjusted_close_float_round_trip_precision(tmp_path: Path) -> None:
    prices = _fixture_prices()
    prices.loc[prices["symbol"].eq("QQQ") & prices["session"].eq("2010-01-04"), "adjusted_close"] = 1.0000000000000002

    result = snapshot.materialize_tqqq_r1_snapshot(prices, tmp_path / "snapshot")

    actual = pd.read_csv(result.output_dir / "prices.csv").loc[lambda frame: frame["symbol"].eq("QQQ"), "adjusted_close"].iloc[0]
    assert actual == 1.0000000000000002


@pytest.mark.parametrize("column", ["session", "symbol", "adjusted_close"])
def test_materialize_rejects_duplicate_required_column_labels(tmp_path: Path, column: str) -> None:
    prices = _fixture_prices()
    prices = pd.concat([prices, prices[[column]]], axis=1)

    with pytest.raises(snapshot.SnapshotValidationError, match="required columns must appear exactly once"):
        snapshot.materialize_tqqq_r1_snapshot(prices, tmp_path / "snapshot")


@pytest.mark.parametrize(
    "value",
    [True, np.bool_(True)],
    ids=["native-bool", "numpy-bool"],
)
def test_materialize_rejects_native_and_numpy_boolean_adjusted_close(tmp_path: Path, value: object) -> None:
    prices = _fixture_prices().astype({"adjusted_close": object})
    prices.loc[0, "adjusted_close"] = value

    with pytest.raises(snapshot.SnapshotValidationError, match="boolean adjusted_close"):
        snapshot.materialize_tqqq_r1_snapshot(prices, tmp_path / "snapshot")


def test_materialize_rejects_mixed_boolean_adjusted_close(tmp_path: Path) -> None:
    prices = _fixture_prices().astype({"adjusted_close": object})
    prices.loc[0, "adjusted_close"] = np.bool_(True)

    with pytest.raises(snapshot.SnapshotValidationError, match="boolean adjusted_close"):
        snapshot.materialize_tqqq_r1_snapshot(prices, tmp_path / "snapshot")


@pytest.mark.parametrize(
    "prices",
    [
        _fixture_prices().assign(adjusted_close=[complex(10.5), complex(45.25), complex(11), complex(46)]),
        _fixture_prices().assign(
            adjusted_close=np.array([np.complex64(10.5), np.complex64(45.25), np.complex64(11), np.complex64(46)])
        ),
        _fixture_prices().assign(adjusted_close=pd.Series([10.5, 45.25, 11, 46], dtype="complex128")),
        _fixture_prices().assign(adjusted_close=pd.Series([complex(10.5), 45.25, 11, 46], dtype=object)),
    ],
    ids=["native-complex", "numpy-complex", "complex-dtype", "object-complex"],
)
def test_materialize_rejects_native_numpy_dtype_and_object_complex_adjusted_close(
    tmp_path: Path, prices: pd.DataFrame
) -> None:
    with pytest.raises(snapshot.SnapshotValidationError, match="complex adjusted_close"):
        snapshot.materialize_tqqq_r1_snapshot(prices, tmp_path / "snapshot")


@pytest.mark.parametrize(
    "sessions",
    [
        [
            datetime(2010, 1, 4),
            datetime(2010, 1, 4, tzinfo=timezone.utc),
            datetime(2010, 1, 5),
            datetime(2010, 1, 5, tzinfo=timezone.utc),
        ],
        ["2010-01-04T00:00:00+00:00", "2010-01-04T00:00:00-05:00", "2010-01-05T00:00:00+00:00", "2010-01-05T00:00:00-05:00"],
    ],
    ids=["mixed-naive-aware", "mixed-offsets"],
)
def test_materialize_rejects_mixed_timezone_session_inputs(tmp_path: Path, sessions: list[object]) -> None:
    prices = _fixture_prices().assign(session=sessions)

    with pytest.raises(snapshot.SnapshotValidationError, match="timezone-aware session"):
        snapshot.materialize_tqqq_r1_snapshot(prices, tmp_path / "snapshot")


def test_materialize_rejects_datetime_adjusted_close(tmp_path: Path) -> None:
    prices = _fixture_prices().assign(adjusted_close=pd.Timestamp("2026-07-25"))

    with pytest.raises(snapshot.SnapshotValidationError, match="datetime-like adjusted_close"):
        snapshot.materialize_tqqq_r1_snapshot(prices, tmp_path / "snapshot")


def test_verify_rejects_noncanonical_symbol_readback(tmp_path: Path) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")
    output_dir = result.output_dir
    (output_dir / "prices.csv").write_text(
        "session,symbol,adjusted_close\n"
        "2010-01-04, QQQ ,45.25\n"
        "2010-01-04,TQQQ,10.5\n"
        "2010-01-05,QQQ,46\n"
        "2010-01-05,TQQQ,11\n",
        encoding="utf-8",
    )

    with pytest.raises(snapshot.SnapshotValidationError, match="canonical symbol"):
        snapshot.verify_tqqq_r1_snapshot(output_dir, expected_manifest_sha256=_refresh_trusted_metadata(output_dir))


def test_verify_rejects_noncanonical_raw_session_encoding(tmp_path: Path) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")
    output_dir = result.output_dir
    (output_dir / "prices.csv").write_text(
        "session,symbol,adjusted_close\n"
        "2010-01-04T00:00:00,QQQ,45.25\n"
        "2010-01-04T00:00:00,TQQQ,10.5\n"
        "2010-01-05T00:00:00,QQQ,46\n"
        "2010-01-05T00:00:00,TQQQ,11\n",
        encoding="utf-8",
    )

    with pytest.raises(snapshot.SnapshotValidationError, match="canonical session"):
        snapshot.verify_tqqq_r1_snapshot(output_dir, expected_manifest_sha256=_refresh_trusted_metadata(output_dir))


def test_verify_rejects_boolean_readback(tmp_path: Path) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")
    output_dir = result.output_dir
    (output_dir / "prices.csv").write_text(
        "session,symbol,adjusted_close\n"
        "2010-01-04,QQQ,True\n"
        "2010-01-04,TQQQ,True\n"
        "2010-01-05,QQQ,True\n"
        "2010-01-05,TQQQ,True\n",
        encoding="utf-8",
    )

    with pytest.raises(snapshot.SnapshotValidationError, match="boolean adjusted_close"):
        snapshot.verify_tqqq_r1_snapshot(output_dir, expected_manifest_sha256=_refresh_trusted_metadata(output_dir))


def test_verify_rejects_mixed_offset_session_readback(tmp_path: Path) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")
    output_dir = result.output_dir
    (output_dir / "prices.csv").write_text(
        "session,symbol,adjusted_close\n"
        "2010-01-04T00:00:00+00:00,QQQ,45.25\n"
        "2010-01-04T00:00:00-05:00,TQQQ,10.5\n"
        "2010-01-05T00:00:00+00:00,QQQ,46\n"
        "2010-01-05T00:00:00-05:00,TQQQ,11\n",
        encoding="utf-8",
    )

    with pytest.raises(snapshot.SnapshotValidationError, match="timezone-aware session"):
        snapshot.verify_tqqq_r1_snapshot(output_dir, expected_manifest_sha256=_refresh_trusted_metadata(output_dir))


def test_verify_rejects_invalid_metadata_scalar_types(tmp_path: Path) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")
    output_dir = result.output_dir
    _write_json(output_dir / "validation.json", {"valid": 1, "row_count": 4.0, "symbols": ["QQQ", "TQQQ"]})

    with pytest.raises(snapshot.SnapshotValidationError, match="invalid validation"):
        snapshot.verify_tqqq_r1_snapshot(output_dir, expected_manifest_sha256=_refresh_trusted_metadata(output_dir))


def test_verify_normalizes_csv_parse_failures(tmp_path: Path) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")
    output_dir = result.output_dir
    (output_dir / "prices.csv").write_bytes(b"\xff")

    with pytest.raises(snapshot.SnapshotValidationError, match="invalid prices.csv"):
        snapshot.verify_tqqq_r1_snapshot(output_dir, expected_manifest_sha256=_refresh_trusted_metadata(output_dir))


def test_materialize_rejects_oversized_canonical_integer_before_numeric_conversion(tmp_path: Path) -> None:
    prices = _fixture_prices().astype({"adjusted_close": object})
    prices.loc[0, "adjusted_close"] = int("9" * 400)

    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.materialize_tqqq_r1_snapshot(prices, tmp_path / "snapshot")


def test_verify_rejects_oversized_canonical_integer_csv(tmp_path: Path) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")
    output_dir = result.output_dir
    (output_dir / "prices.csv").write_text(
        "session,symbol,adjusted_close\n"
        + "2010-01-04,QQQ," + "9" * 400 + "\n"
        + "2010-01-04,TQQQ,10.5\n"
        + "2010-01-05,QQQ,46\n"
        + "2010-01-05,TQQQ,11\n",
        encoding="utf-8",
    )

    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.verify_tqqq_r1_snapshot(output_dir, expected_manifest_sha256=_refresh_trusted_metadata(output_dir))
