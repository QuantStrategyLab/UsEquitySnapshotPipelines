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


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _trusted_fixture_inputs(tmp_path: Path) -> dict[str, object]:
    calendar = tmp_path / "fixture-calendar.ndjson"
    sessions = (
        ("2010-01-04", "2010-01-04T14:30:00Z", "2010-01-04T21:00:00Z"),
        ("2010-02-11", "2010-02-11T14:30:00Z", "2010-02-11T21:00:00Z"),
        ("2026-07-24", "2026-07-24T13:30:00Z", "2026-07-24T20:00:00Z"),
    )
    calendar.write_text(
        "".join(
            json.dumps(
                {
                    "schema": "qsl.r1.xnys.session.v1",
                    "session": session,
                    "open_utc": open_utc,
                    "close_utc": close_utc,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
            for session, open_utc, close_utc in sessions
        ),
        encoding="utf-8",
    )
    calendar_digest = _sha256_bytes(calendar.read_bytes())
    runtime_anchor = tmp_path / "fixture-runtime-source-identity.json"
    runtime_anchor.write_text(
        json.dumps(
            {
                "schema": "qsl.tqqq.fixture-runtime-source-identity.v1",
                "source_sha256": {
                    "tqqq_r1_snapshot.py": _sha256_bytes(Path(snapshot.__file__).read_bytes()),
                    "yfinance_prices.py": _sha256_bytes(Path(snapshot.__file__).with_name("yfinance_prices.py").read_bytes()),
                },
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    runtime_digest = _sha256_bytes(runtime_anchor.read_bytes())
    endpoint = tmp_path / "fixture-endpoint-packet.json"
    endpoint.write_text(
        json.dumps(
            {
                "schema": "qsl.tqqq.fixture-calendar-endpoint-packet.v1",
                "venue": "XNYS",
                "required_first_session": "2010-01-04",
                "required_last_completed_session": "2026-07-24",
                "required_last_completed_close_utc": "2026-07-24T20:00:00Z",
                "next_session": "2026-07-27",
                "next_session_close_utc": "2026-07-27T20:00:00Z",
                "endpoint_observed_at_utc": "2026-07-25T12:00:00Z",
                "expected_session_count": 3,
                "expected_calendar_sha256": calendar_digest,
                "expected_runtime_source_identity_sha256": runtime_digest,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    prices = pd.DataFrame(
        [
            {"session": "2010-01-04", "symbol": "QQQ", "adjusted_close": 45.25},
            {"session": "2010-02-11", "symbol": "QQQ", "adjusted_close": 44.0},
            {"session": "2010-02-11", "symbol": "TQQQ", "adjusted_close": 10.5},
            {"session": "2026-07-24", "symbol": "QQQ", "adjusted_close": 500.0},
            {"session": "2026-07-24", "symbol": "TQQQ", "adjusted_close": 80.0},
        ]
    )
    return {
        "prices": prices,
        "calendar_path": calendar,
        "endpoint_packet_path": endpoint,
        "runtime_anchor_path": runtime_anchor,
        "expected_calendar_sha256": calendar_digest,
        "expected_endpoint_packet_sha256": _sha256_bytes(endpoint.read_bytes()),
        "expected_runtime_source_identity_sha256": runtime_digest,
    }


def _trusted_materialize(tmp_path: Path, inputs: dict[str, object], **overrides: object) -> snapshot.SnapshotResult:
    kwargs = {**inputs, **overrides}
    prices = kwargs.pop("prices")
    return snapshot.materialize_tqqq_calendar_endpoint_trusted_snapshot(
        prices,
        tmp_path / "trusted-snapshot",
        **kwargs,
    )


@pytest.mark.parametrize(
    ("scenario", "expected_status"),
    [
        ("authority_missing", "CALENDAR_ENDPOINT_AUTHORITY_MISSING"),
        ("calendar_digest", "CALENDAR_DIGEST_MISMATCH"),
        ("endpoint_digest", "CALENDAR_ENDPOINT_PACKET_DIGEST_MISMATCH"),
        ("runtime_digest", "RUNTIME_SOURCE_IDENTITY_MISMATCH"),
        ("installed_source", "RUNTIME_SOURCE_IDENTITY_MISMATCH"),
        ("calendar_start", "CALENDAR_START_MISMATCH"),
        ("tqqq_session_gap", "EXACT_SESSION_SET_MISMATCH"),
        ("calendar_end", "CALENDAR_END_MISMATCH"),
        ("stale_observation", "CALENDAR_ENDPOINT_STALE_AT_OBSERVATION"),
        ("naive_or_future_clock", "CALENDAR_ENDPOINT_STALE_AT_OBSERVATION"),
        ("invalid_calendar_schema", "CALENDAR_SCHEMA_INVALID"),
        ("immutable_output", "IMMUTABLE_CREATE_FAILED"),
        ("strict_readback", "STRICT_READBACK_FAILED"),
    ],
)
def test_trusted_fixture_consumer_fails_closed_for_each_e1_red_class(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scenario: str, expected_status: str
) -> None:
    inputs = _trusted_fixture_inputs(tmp_path)
    if scenario == "authority_missing":
        inputs["expected_calendar_sha256"] = None
    elif scenario == "calendar_digest":
        inputs["expected_calendar_sha256"] = "0" * 64
    elif scenario == "endpoint_digest":
        inputs["expected_endpoint_packet_sha256"] = "0" * 64
    elif scenario == "runtime_digest":
        inputs["expected_runtime_source_identity_sha256"] = "0" * 64
    elif scenario == "installed_source":
        anchor = Path(inputs["runtime_anchor_path"])
        payload = json.loads(anchor.read_text(encoding="utf-8"))
        payload["source_sha256"]["tqqq_r1_snapshot.py"] = "0" * 64
        _write_json(anchor, payload)
        inputs["expected_runtime_source_identity_sha256"] = _sha256_bytes(anchor.read_bytes())
        endpoint = Path(inputs["endpoint_packet_path"])
        payload = json.loads(endpoint.read_text(encoding="utf-8"))
        payload["expected_runtime_source_identity_sha256"] = inputs["expected_runtime_source_identity_sha256"]
        _write_json(endpoint, payload)
        inputs["expected_endpoint_packet_sha256"] = _sha256_bytes(endpoint.read_bytes())
    elif scenario == "calendar_start":
        calendar = Path(inputs["calendar_path"])
        calendar.write_text(calendar.read_text(encoding="utf-8").replace("2010-01-04", "2010-01-05"), encoding="utf-8")
        inputs["expected_calendar_sha256"] = _sha256_bytes(calendar.read_bytes())
        endpoint = Path(inputs["endpoint_packet_path"])
        payload = json.loads(endpoint.read_text(encoding="utf-8"))
        payload["expected_calendar_sha256"] = inputs["expected_calendar_sha256"]
        _write_json(endpoint, payload)
        inputs["expected_endpoint_packet_sha256"] = _sha256_bytes(endpoint.read_bytes())
    elif scenario == "tqqq_session_gap":
        inputs["prices"] = inputs["prices"].loc[lambda frame: ~((frame.symbol == "TQQQ") & (frame.session == "2026-07-24"))]
    elif scenario == "calendar_end":
        endpoint = Path(inputs["endpoint_packet_path"])
        payload = json.loads(endpoint.read_text(encoding="utf-8"))
        payload["required_last_completed_session"] = "2026-07-23"
        _write_json(endpoint, payload)
        inputs["expected_endpoint_packet_sha256"] = _sha256_bytes(endpoint.read_bytes())
    elif scenario == "stale_observation":
        monkeypatch.setattr(snapshot, "_utc_now", lambda: datetime(2026, 7, 27, 20, 0, tzinfo=timezone.utc), raising=False)
    elif scenario == "naive_or_future_clock":
        monkeypatch.setattr(snapshot, "_utc_now", lambda: datetime(2026, 7, 27, 20, 0), raising=False)
    elif scenario == "invalid_calendar_schema":
        calendar = Path(inputs["calendar_path"])
        calendar.write_text(calendar.read_text(encoding="utf-8").replace('"schema":"qsl.r1.xnys.session.v1"', '"schema":"wrong"'), encoding="utf-8")
        inputs["expected_calendar_sha256"] = _sha256_bytes(calendar.read_bytes())
        endpoint = Path(inputs["endpoint_packet_path"])
        payload = json.loads(endpoint.read_text(encoding="utf-8"))
        payload["expected_calendar_sha256"] = inputs["expected_calendar_sha256"]
        _write_json(endpoint, payload)
        inputs["expected_endpoint_packet_sha256"] = _sha256_bytes(endpoint.read_bytes())
    elif scenario == "immutable_output":
        (tmp_path / "trusted-snapshot").mkdir()
    elif scenario == "strict_readback":
        monkeypatch.setattr(snapshot, "_trusted_readback_matches", lambda *_: False, raising=False)

    if scenario not in {"stale_observation", "naive_or_future_clock"}:
        monkeypatch.setattr(snapshot, "_utc_now", lambda: datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc))

    with pytest.raises(snapshot.SnapshotValidationError, match=expected_status) as error:
        _trusted_materialize(tmp_path, inputs)

    assert error.value.recommendation is None
    assert error.value.size_zero_required is True
    assert error.value.side_effects == {"provider": 0, "replay": 0, "order": 0}


def test_trusted_fixture_consumer_uses_private_utc_clock_and_exact_sessions(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    inputs = _trusted_fixture_inputs(tmp_path)
    monkeypatch.setattr(snapshot, "_utc_now", lambda: datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc), raising=False)

    result = _trusted_materialize(tmp_path, inputs)

    assert result.output_dir == tmp_path / "trusted-snapshot"
    assert snapshot.verify_tqqq_calendar_endpoint_trusted_snapshot(
        result.output_dir,
        expected_calendar_sha256=inputs["expected_calendar_sha256"],
        expected_endpoint_packet_sha256=inputs["expected_endpoint_packet_sha256"],
        expected_runtime_source_identity_sha256=inputs["expected_runtime_source_identity_sha256"],
    ) == result


def test_trusted_fixture_consumer_rejects_tampered_persisted_readback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inputs = _trusted_fixture_inputs(tmp_path)
    monkeypatch.setattr(snapshot, "_utc_now", lambda: datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc))
    result = _trusted_materialize(tmp_path, inputs)
    (result.output_dir / "prices.csv").write_text("tampered\n", encoding="utf-8")

    with pytest.raises(snapshot.SnapshotValidationError, match="STRICT_READBACK_FAILED"):
        snapshot.verify_tqqq_calendar_endpoint_trusted_snapshot(
            result.output_dir,
            expected_calendar_sha256=inputs["expected_calendar_sha256"],
            expected_endpoint_packet_sha256=inputs["expected_endpoint_packet_sha256"],
            expected_runtime_source_identity_sha256=inputs["expected_runtime_source_identity_sha256"],
        )
