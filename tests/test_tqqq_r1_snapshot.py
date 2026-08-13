from __future__ import annotations

import hashlib
import json
import os
import socket
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from us_equity_snapshot_pipelines import tqqq_r1_snapshot as snapshot


def test_legacy_snapshot_is_not_comparable_without_runtime_or_authority(tmp_path: Path) -> None:
    root = tmp_path / "legacy"
    snapshot_dir = root / "snapshot"
    snapshot_dir.mkdir(parents=True)
    bars = b"{}"
    manifest = {
        "producer": {"repository": "QuantStrategyLab/UsEquitySnapshotPipelines", "tool": "tqqq_ibkr_paper_single_acquisition", "tool_version": "v1"},
        "members": [{"path": "bars.json", "media_type": "application/json", "size_bytes": len(bars), "sha256": hashlib.sha256(bars).hexdigest()}],
    }
    raw = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    snapshot_dir.joinpath("input-manifest.json").write_bytes(raw)
    snapshot_dir.joinpath("bars.json").write_bytes(bars)
    result = snapshot.assess_tqqq_r1_legacy_source(root, expected_manifest_sha256=hashlib.sha256(raw).hexdigest())
    assert result.comparison_status == "NOT_COMPARABLE"
    assert result.manifest_sha256 == hashlib.sha256(raw).hexdigest()


def test_legacy_snapshot_rejects_tampered_preserved_member(tmp_path: Path) -> None:
    root = tmp_path / "legacy"
    snapshot_dir = root / "snapshot"
    snapshot_dir.mkdir(parents=True)
    bars = b"{}"
    manifest = {
        "producer": {"repository": "QuantStrategyLab/UsEquitySnapshotPipelines", "tool": "tqqq_ibkr_paper_single_acquisition", "tool_version": "v1"},
        "members": [{"path": "bars.json", "media_type": "application/json", "size_bytes": len(bars), "sha256": hashlib.sha256(bars).hexdigest()}],
    }
    raw = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    snapshot_dir.joinpath("input-manifest.json").write_bytes(raw)
    snapshot_dir.joinpath("bars.json").write_bytes(b'{"tampered":true}')

    with pytest.raises(snapshot.SnapshotValidationError, match="legacy snapshot integrity mismatch"):
        snapshot.assess_tqqq_r1_legacy_source(root, expected_manifest_sha256=hashlib.sha256(raw).hexdigest())


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


def _rewrite_sha256sums(output_dir: Path) -> None:
    _write_json(
        output_dir / "sha256sums.json",
        {
            name: hashlib.sha256((output_dir / name).read_bytes()).hexdigest()
            for name in ("prices.csv", "manifest.json", "validation.json")
        },
    )


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


def test_materialize_rejects_python_int_beyond_digit_limit(tmp_path: Path) -> None:
    prices = _fixture_prices().astype({"adjusted_close": object})
    prices.loc[0, "adjusted_close"] = 10**5000 - 1

    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.materialize_tqqq_r1_snapshot(prices, tmp_path / "snapshot")


def test_verify_rejects_four_field_csv_row_without_pandas_parser(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")
    (result.output_dir / "prices.csv").write_text(
        "session,symbol,adjusted_close\n"
        "2010-01-04,QQQ,45.25,unexpected\n"
        "2010-01-04,TQQQ,10.5\n"
        "2010-01-05,QQQ,46\n"
        "2010-01-05,TQQQ,11\n",
        encoding="utf-8",
    )
    expected_manifest_sha256 = _refresh_trusted_metadata(result.output_dir)

    def pandas_parser_must_not_run(*args: object, **kwargs: object) -> pd.DataFrame:
        pytest.fail("trusted CSV verification must not use pandas.read_csv")

    monkeypatch.setattr(snapshot.pd, "read_csv", pandas_parser_must_not_run)

    with pytest.raises(snapshot.SnapshotValidationError, match="invalid prices.csv"):
        snapshot.verify_tqqq_r1_snapshot(result.output_dir, expected_manifest_sha256=expected_manifest_sha256)


def test_verify_fails_closed_without_posix_descriptor_capabilities(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")
    monkeypatch.setattr(snapshot.sys, "platform", "win32")

    with pytest.raises(snapshot.SnapshotValidationError, match="descriptor"):
        snapshot.verify_tqqq_r1_snapshot(result.output_dir, expected_manifest_sha256=result.manifest_sha256)


def test_verify_rejects_oversized_member_before_csv_parse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")
    (result.output_dir / "prices.csv").write_bytes(b"session,symbol,adjusted_close\n" + b"x" * (17 * 1024 * 1024))
    expected_manifest_sha256 = _refresh_trusted_metadata(result.output_dir)

    def pandas_parser_must_not_run(*args: object, **kwargs: object) -> pd.DataFrame:
        pytest.fail("oversized member reached CSV parser")

    monkeypatch.setattr(snapshot.pd, "read_csv", pandas_parser_must_not_run)

    with pytest.raises(snapshot.SnapshotValidationError, match="size limit"):
        snapshot.verify_tqqq_r1_snapshot(result.output_dir, expected_manifest_sha256=expected_manifest_sha256)


def test_verify_normalizes_deep_json_recursion(tmp_path: Path) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")
    (result.output_dir / "manifest.json").write_bytes(b'{"x":' * 2_000 + b"0" + b"}" * 2_000)
    manifest_sha256 = hashlib.sha256((result.output_dir / "manifest.json").read_bytes()).hexdigest()
    _write_json(
        result.output_dir / "sha256sums.json",
        {
            "prices.csv": hashlib.sha256((result.output_dir / "prices.csv").read_bytes()).hexdigest(),
            "manifest.json": manifest_sha256,
            "validation.json": hashlib.sha256((result.output_dir / "validation.json").read_bytes()).hexdigest(),
        },
    )

    with pytest.raises(snapshot.SnapshotValidationError, match="invalid manifest"):
        snapshot.verify_tqqq_r1_snapshot(
            result.output_dir,
            expected_manifest_sha256=manifest_sha256,
        )


def test_verify_authenticates_manifest_before_json_parse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")

    def json_parser_must_not_run(*args: object, **kwargs: object) -> dict[str, object]:
        pytest.fail("untrusted manifest reached a JSON parser")

    monkeypatch.setattr(snapshot, "_parse_json_object", json_parser_must_not_run)

    with pytest.raises(snapshot.SnapshotValidationError, match="trusted manifest hash mismatch"):
        snapshot.verify_tqqq_r1_snapshot(result.output_dir, expected_manifest_sha256="0" * 64)


def test_verify_rejects_fifth_entry_before_opening_members(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")
    (result.output_dir / "unexpected.txt").write_text("extra", encoding="utf-8")

    def member_reader_must_not_run(*args: object, **kwargs: object) -> bytes:
        pytest.fail("directory admission opened a member")

    monkeypatch.setattr(snapshot, "_read_member_from_root", member_reader_must_not_run)

    with pytest.raises(snapshot.SnapshotValidationError, match="unexpected output files"):
        snapshot.verify_tqqq_r1_snapshot(result.output_dir, expected_manifest_sha256=result.manifest_sha256)


def test_verify_rejects_member_symlink_by_descriptor(tmp_path: Path) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")
    replacement = tmp_path / "replacement.csv"
    replacement.write_text((result.output_dir / "prices.csv").read_text(encoding="utf-8"), encoding="utf-8")
    (result.output_dir / "prices.csv").unlink()
    (result.output_dir / "prices.csv").symlink_to(replacement)

    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.verify_tqqq_r1_snapshot(result.output_dir, expected_manifest_sha256=result.manifest_sha256)


def test_verify_rejects_root_symlink_by_descriptor(tmp_path: Path) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")
    alias = tmp_path / "snapshot-alias"
    alias.symlink_to(result.output_dir, target_is_directory=True)

    with pytest.raises(snapshot.SnapshotValidationError):
        snapshot.verify_tqqq_r1_snapshot(alias, expected_manifest_sha256=result.manifest_sha256)


def test_verify_rejects_member_identity_change_during_descriptor_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")
    original_fstat = snapshot.os.fstat
    regular_fstat_calls = 0

    def unstable_fstat(fd: int) -> os.stat_result | SimpleNamespace:
        nonlocal regular_fstat_calls
        observed = original_fstat(fd)
        if snapshot.stat.S_ISREG(observed.st_mode):
            regular_fstat_calls += 1
            if regular_fstat_calls == 2:
                return SimpleNamespace(
                    st_dev=observed.st_dev,
                    st_ino=observed.st_ino,
                    st_mode=observed.st_mode,
                    st_size=observed.st_size,
                    st_mtime_ns=observed.st_mtime_ns + 1,
                    st_ctime_ns=observed.st_ctime_ns,
                )
        return observed

    monkeypatch.setattr(snapshot.os, "fstat", unstable_fstat)

    with pytest.raises(snapshot.SnapshotValidationError, match="changed during read"):
        snapshot.verify_tqqq_r1_snapshot(result.output_dir, expected_manifest_sha256=result.manifest_sha256)


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="POSIX FIFO capability unavailable")
def test_verify_rejects_fifo_without_reading_it(tmp_path: Path) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")
    (result.output_dir / "prices.csv").unlink()
    os.mkfifo(result.output_dir / "prices.csv")

    with pytest.raises(snapshot.SnapshotValidationError, match="regular non-symlink"):
        snapshot.verify_tqqq_r1_snapshot(result.output_dir, expected_manifest_sha256=result.manifest_sha256)


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="Unix-domain socket capability unavailable")
def test_verify_rejects_socket_without_reading_it() -> None:
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="tq-") as directory:
        result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), Path(directory) / "snapshot")
        prices_path = result.output_dir / "prices.csv"
        prices_path.unlink()
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(str(prices_path))

            with pytest.raises(snapshot.SnapshotValidationError):
                snapshot.verify_tqqq_r1_snapshot(result.output_dir, expected_manifest_sha256=result.manifest_sha256)
        finally:
            listener.close()


def test_verify_rejects_total_member_limit_before_parse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")
    (result.output_dir / "prices.csv").write_bytes(b"p" * (16 * 1024 * 1024))
    (result.output_dir / "manifest.json").write_bytes(b"m" * (1024 * 1024))
    (result.output_dir / "validation.json").write_bytes(b"v" * (1024 * 1024))

    def json_parser_must_not_run(*args: object, **kwargs: object) -> dict[str, object]:
        pytest.fail("oversized aggregate reached a JSON parser")

    monkeypatch.setattr(snapshot, "_parse_json_object", json_parser_must_not_run)

    with pytest.raises(snapshot.SnapshotValidationError, match="total size limit"):
        snapshot.verify_tqqq_r1_snapshot(
            result.output_dir,
            expected_manifest_sha256=hashlib.sha256((result.output_dir / "manifest.json").read_bytes()).hexdigest(),
        )


@pytest.mark.parametrize(
    "manifest_bytes",
    [
        b'{"x":1,"x":2}',
        b'{"x":NaN}',
        b'{"x":Infinity}',
        b'{"x":1e10000}',
        b'{"x":' + b"9" * 129 + b"}",
    ],
    ids=["duplicate-key", "nan", "infinity", "overflow-float", "oversized-integer"],
)
def test_verify_normalizes_strict_json_rejections(tmp_path: Path, manifest_bytes: bytes) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")
    (result.output_dir / "manifest.json").write_bytes(manifest_bytes)
    _rewrite_sha256sums(result.output_dir)

    with pytest.raises(snapshot.SnapshotValidationError, match="invalid manifest"):
        snapshot.verify_tqqq_r1_snapshot(
            result.output_dir,
            expected_manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        )


def test_verify_accepts_legal_short_descriptor_reads(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")
    original_read = snapshot.os.read

    def short_read(fd: int, size: int) -> bytes:
        return original_read(fd, min(size, 7))

    monkeypatch.setattr(snapshot.os, "read", short_read)

    assert snapshot.verify_tqqq_r1_snapshot(
        result.output_dir, expected_manifest_sha256=result.manifest_sha256
    ) == result
