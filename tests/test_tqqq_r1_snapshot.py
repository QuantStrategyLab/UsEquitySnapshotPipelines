from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
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


def _source_identity(calendar_sessions: list[str]) -> dict[str, object]:
    calendar_sha256 = hashlib.sha256(
        (json.dumps(calendar_sessions, separators=(",", ":")) + "\n").encode()
    ).hexdigest()
    return {
        "route": "R1_STATIC_RESEARCH",
        "provider": "external-control-plane-receipt",
        "retrieval_library": "local-fixture",
        "source_version": "fixture.v1",
        "symbols": ["QQQ", "TQQQ"],
        "price_field": "adjusted_close",
        "adjustment": "split-and-dividend-adjusted",
        "calendar_sha256": calendar_sha256,
        "calendar_sessions": calendar_sessions,
        "timezone": "America/New_York",
        "coverage_start": calendar_sessions[0],
        "coverage_end": calendar_sessions[-1],
        "as_of": calendar_sessions[-1],
        "payload_schema": "prices.csv.v1",
        "sort_order": "session,symbol",
        "missing_data_policy": "reject-missing-or-duplicate",
    }


SOURCE_IDENTITY = _source_identity(["2010-01-04", "2010-01-05"])


def _write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _external_manifest_sha256(prices: pd.DataFrame, source_identity: dict[str, object] = SOURCE_IDENTITY) -> str:
    normalized = prices.loc[:, ["session", "symbol", "adjusted_close"]].copy()
    normalized["session"] = pd.to_datetime(normalized["session"]).dt.strftime("%Y-%m-%d")
    normalized = normalized.sort_values(["session", "symbol"], kind="stable")
    prices_bytes = normalized.to_csv(index=False, lineterminator="\n", float_format="%.17g").encode()
    manifest = {
        "contract_version": snapshot.CONTRACT_VERSION,
        "symbols": list(snapshot.SYMBOLS),
        "requested_lower_bound": snapshot.REQUESTED_LOWER_BOUND,
        "price_field": snapshot.PRICE_FIELD,
        "plugin": snapshot.PLUGIN,
        "mode": snapshot.MODE,
        "size": 0,
        "row_count": len(normalized),
        "prices_sha256": hashlib.sha256(prices_bytes).hexdigest(),
        "source_identity": source_identity,
    }
    return hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode() + b"\n").hexdigest()


def _materialize(
    prices: pd.DataFrame,
    output_dir: Path,
    *,
    expected_manifest_sha256: str | None = None,
    source_identity: dict[str, object] = SOURCE_IDENTITY,
) -> snapshot.SnapshotResult:
    return snapshot.materialize_tqqq_r1_snapshot(
        prices,
        output_dir,
        expected_manifest_sha256=expected_manifest_sha256 or ("0" * 64),
        source_identity=source_identity,
    )


FIXTURE_MANIFEST_SHA256 = _external_manifest_sha256(_fixture_prices())


def _refresh_trusted_metadata(output_dir: Path) -> str:
    manifest_path = output_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["prices_sha256"] = hashlib.sha256((output_dir / "prices.csv").read_bytes()).hexdigest()
    _write_json(manifest_path, manifest)
    manifest_sha256 = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    validation_path = output_dir / "validation.json"
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    validation["snapshot_id"] = f"sha256-{manifest_sha256}"
    _write_json(validation_path, validation)
    _write_json(
        output_dir / "sha256sums.json",
        {
            name: hashlib.sha256((output_dir / name).read_bytes()).hexdigest()
            for name in ("prices.csv", "manifest.json", "validation.json")
        },
    )
    return manifest_sha256


def test_materialize_writes_deterministic_immutable_artifacts(tmp_path: Path) -> None:
    result = _materialize(_fixture_prices(), tmp_path / "snapshot", expected_manifest_sha256=FIXTURE_MANIFEST_SHA256)

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
        snapshot.CONTRACT_VERSION
    )
    assert snapshot.verify_tqqq_r1_snapshot(
        result.output_dir, expected_manifest_sha256=FIXTURE_MANIFEST_SHA256
    ) == result
    assert result.snapshot_id == f"sha256-{FIXTURE_MANIFEST_SHA256}"


def test_materialize_rejects_nonmatching_external_manifest_receipt(tmp_path: Path) -> None:
    with pytest.raises(snapshot.SnapshotValidationError, match="trusted manifest hash mismatch"):
        _materialize(_fixture_prices(), tmp_path / "snapshot")

    assert not (tmp_path / "snapshot").exists()


def test_legacy_v2_materialize_and_result_digest_remain_compatible(tmp_path: Path) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "legacy")

    assert result.manifest_sha256 == hashlib.sha256((result.output_dir / "manifest.json").read_bytes()).hexdigest()
    assert result.snapshot_id is None
    assert snapshot.verify_tqqq_r1_snapshot(
        result.output_dir, expected_manifest_sha256=result.manifest_sha256
    ) == result


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("route", "R2", "invalid R1 route"),
        ("retrieval_library", "", "invalid R1 source identity"),
        ("symbols", ["QQQ", "SPY"], "invalid R1 source identity"),
        ("adjustment", "raw-close", "invalid adjusted-price semantics"),
        ("calendar_sha256", "a" * 63, "invalid R1 calendar digest"),
        ("coverage_end", "2010-01-04", "R1 coverage identity does not match prices"),
    ],
)
def test_materialize_rejects_incomplete_or_inconsistent_r1_source_identity(
    tmp_path: Path, field: str, value: object, message: str
) -> None:
    source_identity = {**SOURCE_IDENTITY, field: value}

    with pytest.raises(snapshot.SnapshotValidationError, match=message):
        _materialize(_fixture_prices(), tmp_path / "snapshot", source_identity=source_identity)


def test_verify_rejects_snapshot_id_not_bound_to_external_manifest_receipt(tmp_path: Path) -> None:
    result = _materialize(_fixture_prices(), tmp_path / "snapshot", expected_manifest_sha256=FIXTURE_MANIFEST_SHA256)
    validation_path = result.output_dir / "validation.json"
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    validation["snapshot_id"] = "sha256-" + ("0" * 64)
    _write_json(validation_path, validation)
    _write_json(
        result.output_dir / "sha256sums.json",
        {
            name: hashlib.sha256((result.output_dir / name).read_bytes()).hexdigest()
            for name in ("prices.csv", "manifest.json", "validation.json")
        },
    )

    with pytest.raises(snapshot.SnapshotValidationError, match="snapshot id does not bind external manifest receipt"):
        snapshot.verify_tqqq_r1_snapshot(result.output_dir, expected_manifest_sha256=FIXTURE_MANIFEST_SHA256)


def test_verify_rejects_missing_referenced_calendar_session(tmp_path: Path) -> None:
    prices = pd.concat(
        [
            _fixture_prices(),
            pd.DataFrame(
                [
                    {"session": "2010-01-06", "symbol": "QQQ", "adjusted_close": 47.0},
                    {"session": "2010-01-06", "symbol": "TQQQ", "adjusted_close": 11.5},
                ]
            ),
        ],
        ignore_index=True,
    )
    source_identity = _source_identity(["2010-01-04", "2010-01-05", "2010-01-06"])
    result = _materialize(
        prices,
        tmp_path / "snapshot",
        expected_manifest_sha256=_external_manifest_sha256(prices, source_identity),
        source_identity=source_identity,
    )
    prices_path = result.output_dir / "prices.csv"
    prices_path.write_text(
        "\n".join(line for line in prices_path.read_text(encoding="utf-8").splitlines() if "2010-01-05" not in line) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(snapshot.SnapshotValidationError, match="referenced calendar sessions"):
        snapshot.verify_tqqq_r1_snapshot(
            result.output_dir, expected_manifest_sha256=_refresh_trusted_metadata(result.output_dir)
        )


def test_verify_rejects_raw_csv_outside_declared_sort_order(tmp_path: Path) -> None:
    result = _materialize(_fixture_prices(), tmp_path / "snapshot", expected_manifest_sha256=FIXTURE_MANIFEST_SHA256)
    prices_path = result.output_dir / "prices.csv"
    rows = prices_path.read_text(encoding="utf-8").splitlines()
    prices_path.write_text("\n".join([rows[0], rows[2], rows[1], *rows[3:]]) + "\n", encoding="utf-8")

    with pytest.raises(snapshot.SnapshotValidationError, match="declared sort order"):
        snapshot.verify_tqqq_r1_snapshot(
            result.output_dir, expected_manifest_sha256=_refresh_trusted_metadata(result.output_dir)
        )


def test_materialize_preserves_adjusted_close_float_round_trip_precision(tmp_path: Path) -> None:
    prices = _fixture_prices()
    prices.loc[prices["symbol"].eq("QQQ") & prices["session"].eq("2010-01-04"), "adjusted_close"] = 1.0000000000000002

    result = _materialize(
        prices, tmp_path / "snapshot", expected_manifest_sha256=_external_manifest_sha256(prices)
    )

    actual = (
        pd.read_csv(result.output_dir / "prices.csv")
        .loc[lambda frame: frame["symbol"].eq("QQQ"), "adjusted_close"]
        .iloc[0]
    )
    assert actual == 1.0000000000000002


@pytest.mark.parametrize("column", ["session", "symbol", "adjusted_close"])
def test_materialize_rejects_duplicate_required_column_labels(tmp_path: Path, column: str) -> None:
    prices = _fixture_prices()
    prices = pd.concat([prices, prices[[column]]], axis=1)

    with pytest.raises(snapshot.SnapshotValidationError, match="required columns must appear exactly once"):
        _materialize(prices, tmp_path / "snapshot")


@pytest.mark.parametrize(
    "value",
    [True, np.bool_(True)],
    ids=["native-bool", "numpy-bool"],
)
def test_materialize_rejects_native_and_numpy_boolean_adjusted_close(tmp_path: Path, value: object) -> None:
    prices = _fixture_prices().astype({"adjusted_close": object})
    prices.loc[0, "adjusted_close"] = value

    with pytest.raises(snapshot.SnapshotValidationError, match="boolean adjusted_close"):
        _materialize(prices, tmp_path / "snapshot")


def test_materialize_rejects_mixed_boolean_adjusted_close(tmp_path: Path) -> None:
    prices = _fixture_prices().astype({"adjusted_close": object})
    prices.loc[0, "adjusted_close"] = np.bool_(True)

    with pytest.raises(snapshot.SnapshotValidationError, match="boolean adjusted_close"):
        _materialize(prices, tmp_path / "snapshot")


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
        _materialize(prices, tmp_path / "snapshot")


@pytest.mark.parametrize(
    "sessions",
    [
        [
            datetime(2010, 1, 4),  # noqa: DTZ001
            datetime(2010, 1, 4, tzinfo=UTC),
            datetime(2010, 1, 5),  # noqa: DTZ001
            datetime(2010, 1, 5, tzinfo=UTC),
        ],
        [
            "2010-01-04T00:00:00+00:00",
            "2010-01-04T00:00:00-05:00",
            "2010-01-05T00:00:00+00:00",
            "2010-01-05T00:00:00-05:00",
        ],
    ],
    ids=["mixed-naive-aware", "mixed-offsets"],
)
def test_materialize_rejects_mixed_timezone_session_inputs(tmp_path: Path, sessions: list[object]) -> None:
    prices = _fixture_prices().assign(session=sessions)

    with pytest.raises(snapshot.SnapshotValidationError, match="timezone-aware session"):
        _materialize(prices, tmp_path / "snapshot")


def test_materialize_rejects_datetime_adjusted_close(tmp_path: Path) -> None:
    prices = _fixture_prices().assign(adjusted_close=pd.Timestamp("2026-07-25"))

    with pytest.raises(snapshot.SnapshotValidationError, match="datetime-like adjusted_close"):
        _materialize(prices, tmp_path / "snapshot")


def test_verify_rejects_noncanonical_symbol_readback(tmp_path: Path) -> None:
    result = _materialize(_fixture_prices(), tmp_path / "snapshot", expected_manifest_sha256=FIXTURE_MANIFEST_SHA256)
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
    result = _materialize(_fixture_prices(), tmp_path / "snapshot", expected_manifest_sha256=FIXTURE_MANIFEST_SHA256)
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
    result = _materialize(_fixture_prices(), tmp_path / "snapshot", expected_manifest_sha256=FIXTURE_MANIFEST_SHA256)
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
    result = _materialize(_fixture_prices(), tmp_path / "snapshot", expected_manifest_sha256=FIXTURE_MANIFEST_SHA256)
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
    result = _materialize(_fixture_prices(), tmp_path / "snapshot", expected_manifest_sha256=FIXTURE_MANIFEST_SHA256)
    output_dir = result.output_dir
    _write_json(output_dir / "validation.json", {"valid": 1, "row_count": 4.0, "symbols": ["QQQ", "TQQQ"]})

    with pytest.raises(snapshot.SnapshotValidationError, match="invalid validation"):
        snapshot.verify_tqqq_r1_snapshot(output_dir, expected_manifest_sha256=_refresh_trusted_metadata(output_dir))


def test_verify_normalizes_csv_parse_failures(tmp_path: Path) -> None:
    result = _materialize(_fixture_prices(), tmp_path / "snapshot", expected_manifest_sha256=FIXTURE_MANIFEST_SHA256)
    output_dir = result.output_dir
    (output_dir / "prices.csv").write_bytes(b"\xff")

    with pytest.raises(snapshot.SnapshotValidationError, match="invalid prices.csv"):
        snapshot.verify_tqqq_r1_snapshot(output_dir, expected_manifest_sha256=_refresh_trusted_metadata(output_dir))
