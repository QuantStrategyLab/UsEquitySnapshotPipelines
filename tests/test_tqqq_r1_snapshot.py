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


def _write_trust_context(tmp_path: Path) -> dict[str, object]:
    calendar = tmp_path / "calendar.ndjson"
    calendar.write_text(
        "\n".join(
            json.dumps(row, sort_keys=True, separators=(",", ":"))
            for row in (
                {
                    "schema": "qsl.r1.xnys.session.v1",
                    "session_date": "2010-01-04",
                    "open_utc": "2010-01-04T14:30:00Z",
                    "close_utc": "2010-01-04T21:00:00Z",
                    "early_close": False,
                },
                {
                    "schema": "qsl.r1.xnys.session.v1",
                    "session_date": "2010-02-11",
                    "open_utc": "2010-02-11T14:30:00Z",
                    "close_utc": "2010-02-11T21:00:00Z",
                    "early_close": False,
                },
                {
                    "schema": "qsl.r1.xnys.session.v1",
                    "session_date": "2010-02-12",
                    "open_utc": "2010-02-12T14:30:00Z",
                    "close_utc": "2010-02-12T21:00:00Z",
                    "early_close": False,
                },
            )
        )
        + "\n",
        encoding="utf-8",
    )
    runtime = tmp_path / "runtime-anchor.json"
    source_files = {
        "src/us_equity_snapshot_pipelines/tqqq_r1_snapshot.py": Path(snapshot.__file__),
        "src/us_equity_snapshot_pipelines/yfinance_prices.py": Path(snapshot.__file__).with_name("yfinance_prices.py"),
    }
    _write_json(
        runtime,
        {
            "schema": "qsl.tqqq.runtime-source-anchor.v1",
            "repo": "QuantStrategyLab/UsEquitySnapshotPipelines",
            "ref": "refs/heads/main",
            "commit": "a" * 40,
            "parent": "b" * 40,
            "tree": "c" * 40,
            "verified_at_utc": "2010-02-11T21:30:00Z",
            "files": {
                name: {"git_blob_sha1": "d" * 40, "content_sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "size_bytes": path.stat().st_size}
                for name, path in source_files.items()
            },
            "authority_event_id": "00000000-0000-4000-8000-000000000001",
        },
    )
    calendar_sha256 = hashlib.sha256(calendar.read_bytes()).hexdigest()
    runtime_sha256 = hashlib.sha256(runtime.read_bytes()).hexdigest()
    endpoint = tmp_path / "endpoint.json"
    _write_json(
        endpoint,
        {
            "schema": "qsl.tqqq.calendar-endpoint-packet.v1",
            "venue": "XNYS",
            "calendar_request_floor": "2010-01-01",
            "required_first_session": "2010-01-04",
            "required_last_completed_session": "2010-02-11",
            "required_last_completed_close_utc": "2010-02-11T21:00:00Z",
            "next_session": "2010-02-12",
            "next_session_close_utc": "2010-02-12T21:00:00Z",
            "endpoint_observed_at_utc": "2010-02-11T21:30:00Z",
            "completed_session_count": 2,
            "calendar_evidence_session_count": 3,
            "calendar_evidence_sha256": calendar_sha256,
            "runtime_anchor_sha256": runtime_sha256,
            "authority_event_id": "00000000-0000-4000-8000-000000000001",
        },
    )
    return {
        "calendar": calendar,
        "endpoint": endpoint,
        "runtime": runtime,
        "calendar_sha256": calendar_sha256,
        "endpoint_sha256": hashlib.sha256(endpoint.read_bytes()).hexdigest(),
        "runtime_sha256": runtime_sha256,
    }


def test_endpoint_trusted_snapshot_uses_real_schemas_and_digest_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    context = _write_trust_context(tmp_path)
    output_root = tmp_path / "private"
    output_root.mkdir(mode=0o700)
    monkeypatch.setattr(snapshot, "_utc_now", lambda: datetime(2010, 2, 11, 22, tzinfo=timezone.utc))
    prices = pd.DataFrame(
        [
            {"session": "2010-02-11", "symbol": "TQQQ", "adjusted_close": 10.0},
            {"session": "2010-01-04", "symbol": "QQQ", "adjusted_close": 45.0},
            {"session": "2010-02-11", "symbol": "QQQ", "adjusted_close": 46.0},
        ]
    )

    result = snapshot.materialize_tqqq_calendar_endpoint_trusted_snapshot(
        prices,
        output_root,
        calendar_path=context["calendar"],
        endpoint_packet_path=context["endpoint"],
        runtime_anchor_path=context["runtime"],
        expected_calendar_sha256=context["calendar_sha256"],
        expected_endpoint_packet_sha256=context["endpoint_sha256"],
        expected_runtime_source_identity_sha256=context["runtime_sha256"],
    )

    assert result.output_dir.name == f"sha256-{result.manifest_sha256}"
    assert {path.name for path in result.output_dir.iterdir()} == {"prices.csv", "manifest.json", "COMPLETE.json"}
    assert snapshot.verify_tqqq_calendar_endpoint_trusted_snapshot(output_root, expected_manifest_sha256=result.manifest_sha256) == result


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "fifo"])
def test_endpoint_trusted_snapshot_rejects_unsafe_calendar_before_read(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str) -> None:
    context = _write_trust_context(tmp_path)
    calendar = context["calendar"]
    replacement = tmp_path / f"{kind}-calendar"
    if kind == "symlink":
        replacement.symlink_to(calendar)
    elif kind == "hardlink":
        replacement.hardlink_to(calendar)
    else:
        import os

        os.mkfifo(replacement)
    output_root = tmp_path / "private"
    output_root.mkdir(mode=0o700)
    monkeypatch.setattr(snapshot, "_utc_now", lambda: datetime(2010, 2, 11, 22, tzinfo=timezone.utc))

    with pytest.raises(snapshot.SnapshotValidationError, match="CALENDAR_SCHEMA_INVALID"):
        snapshot.materialize_tqqq_calendar_endpoint_trusted_snapshot(
            _fixture_prices(), output_root, calendar_path=replacement, endpoint_packet_path=context["endpoint"], runtime_anchor_path=context["runtime"],
            expected_calendar_sha256=context["calendar_sha256"], expected_endpoint_packet_sha256=context["endpoint_sha256"],
            expected_runtime_source_identity_sha256=context["runtime_sha256"],
        )


def test_endpoint_trusted_snapshot_rejects_existing_digest_directory_and_tampered_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = _write_trust_context(tmp_path)
    output_root = tmp_path / "private"
    output_root.mkdir(mode=0o700)
    monkeypatch.setattr(snapshot, "_utc_now", lambda: datetime(2010, 2, 11, 22, tzinfo=timezone.utc))
    prices = pd.DataFrame(
        [
            {"session": "2010-01-04", "symbol": "QQQ", "adjusted_close": 45.0},
            {"session": "2010-02-11", "symbol": "QQQ", "adjusted_close": 46.0},
            {"session": "2010-02-11", "symbol": "TQQQ", "adjusted_close": 10.0},
        ]
    )
    kwargs = {
        "calendar_path": context["calendar"], "endpoint_packet_path": context["endpoint"], "runtime_anchor_path": context["runtime"],
        "expected_calendar_sha256": context["calendar_sha256"], "expected_endpoint_packet_sha256": context["endpoint_sha256"],
        "expected_runtime_source_identity_sha256": context["runtime_sha256"],
    }
    result = snapshot.materialize_tqqq_calendar_endpoint_trusted_snapshot(prices, output_root, **kwargs)

    with pytest.raises(snapshot.SnapshotValidationError, match="IMMUTABLE_CREATE_CONFLICT"):
        snapshot.materialize_tqqq_calendar_endpoint_trusted_snapshot(prices, output_root, **kwargs)
    _write_json(result.output_dir / "COMPLETE.json", {"schema": "qsl.tqqq.snapshot-completion.v1", "manifest_sha256": "0" * 64})
    with pytest.raises(snapshot.SnapshotValidationError, match="STRICT_READBACK_FAILED"):
        snapshot.verify_tqqq_calendar_endpoint_trusted_snapshot(output_root, expected_manifest_sha256=result.manifest_sha256)


@pytest.mark.parametrize(
    "mutation",
    [
        pytest.param("manifest", id="fixed-manifest-field"),
        pytest.param("prices", id="out-of-contract-prices"),
    ],
)
def test_endpoint_trusted_verifier_rejects_self_consistent_out_of_contract_members(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation: str
) -> None:
    context = _write_trust_context(tmp_path)
    output_root = tmp_path / "private"
    output_root.mkdir(mode=0o700)
    monkeypatch.setattr(snapshot, "_utc_now", lambda: datetime(2010, 2, 11, 22, tzinfo=timezone.utc))
    prices = pd.DataFrame(
        [
            {"session": "2010-01-04", "symbol": "QQQ", "adjusted_close": 45.0},
            {"session": "2010-02-11", "symbol": "QQQ", "adjusted_close": 46.0},
            {"session": "2010-02-11", "symbol": "TQQQ", "adjusted_close": 10.0},
        ]
    )
    result = snapshot.materialize_tqqq_calendar_endpoint_trusted_snapshot(
        prices,
        output_root,
        calendar_path=context["calendar"],
        endpoint_packet_path=context["endpoint"],
        runtime_anchor_path=context["runtime"],
        expected_calendar_sha256=context["calendar_sha256"],
        expected_endpoint_packet_sha256=context["endpoint_sha256"],
        expected_runtime_source_identity_sha256=context["runtime_sha256"],
    )
    manifest_path = result.output_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if mutation == "manifest":
        manifest["price_field"] = "close"
    else:
        (result.output_dir / "prices.csv").write_text(
            "session,symbol,adjusted_close\n2010-01-04,QQQ,45\n2010-02-11,QQQ,46\n2010-02-11,QQQ,10\n",
            encoding="utf-8",
        )
        manifest["prices_sha256"] = hashlib.sha256((result.output_dir / "prices.csv").read_bytes()).hexdigest()
    _write_json(manifest_path, manifest)
    replacement_sha256 = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    replacement_dir = output_root / f"sha256-{replacement_sha256}"
    result.output_dir.rename(replacement_dir)
    _write_json(replacement_dir / "COMPLETE.json", {"schema": "qsl.tqqq.snapshot-completion.v1", "manifest_sha256": replacement_sha256})

    with pytest.raises(snapshot.SnapshotValidationError, match="STRICT_READBACK_FAILED"):
        snapshot.verify_tqqq_calendar_endpoint_trusted_snapshot(output_root, expected_manifest_sha256=replacement_sha256)


def test_endpoint_trusted_snapshot_cleans_failed_publication_for_retry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    context = _write_trust_context(tmp_path)
    output_root = tmp_path / "private"
    output_root.mkdir(mode=0o700)
    monkeypatch.setattr(snapshot, "_utc_now", lambda: datetime(2010, 2, 11, 22, tzinfo=timezone.utc))
    prices = pd.DataFrame(
        [
            {"session": "2010-01-04", "symbol": "QQQ", "adjusted_close": 45.0},
            {"session": "2010-02-11", "symbol": "QQQ", "adjusted_close": 46.0},
            {"session": "2010-02-11", "symbol": "TQQQ", "adjusted_close": 10.0},
        ]
    )
    kwargs = {
        "calendar_path": context["calendar"], "endpoint_packet_path": context["endpoint"], "runtime_anchor_path": context["runtime"],
        "expected_calendar_sha256": context["calendar_sha256"], "expected_endpoint_packet_sha256": context["endpoint_sha256"],
        "expected_runtime_source_identity_sha256": context["runtime_sha256"],
    }
    original_write = snapshot._write_new_member

    def fail_manifest(directory_fd: int, name: str, raw: bytes) -> None:
        if name == "manifest.json":
            raise snapshot.SnapshotValidationError("injected publication failure")
        original_write(directory_fd, name, raw)

    monkeypatch.setattr(snapshot, "_write_new_member", fail_manifest)
    with pytest.raises(snapshot.SnapshotValidationError, match="injected publication failure"):
        snapshot.materialize_tqqq_calendar_endpoint_trusted_snapshot(prices, output_root, **kwargs)
    assert list(output_root.iterdir()) == []

    monkeypatch.setattr(snapshot, "_write_new_member", original_write)
    assert snapshot.materialize_tqqq_calendar_endpoint_trusted_snapshot(prices, output_root, **kwargs).output_dir.is_dir()
