from __future__ import annotations

import hashlib
import inspect
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from us_equity_snapshot_pipelines import tqqq_r1_snapshot as snapshot


_FIXTURE_SOURCE_IDENTITY = (
    b'{"commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","files":{"src/us_equity_snapshot_pipelines/tqqq_r1_snapshot.py":{"content_sha256":"77094cf8354bcde8f97955a74f26b943d8e606b6290ff9eaf679bff30ef0161c","git_blob_sha1":"dddddddddddddddddddddddddddddddddddddddd"},"src/us_equity_snapshot_pipelines/yfinance_prices.py":{"content_sha256":"d409c9dfe1548797e4e4d4ba304c310d18fd08260d0e9fa5f025c2a78b849323","git_blob_sha1":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}},"parent":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","ref":"refs/heads/main","repo":"QuantStrategyLab/UsEquitySnapshotPipelines","schema":"qsl.r1.uesp.source-identity-anchor.v1","tree":"cccccccccccccccccccccccccccccccccccccccc","verification":{"allowlisted_reads":0,"source":"fixture","writes":0}}\n'
)
_FIXTURE_SOURCE_IDENTITY_SHA256 = "3504652ab5c70670ad52e750fee65c8fd3b3b41cfc60b6d7b5ef3647cf2a59ee"
_EXECUTED_FILE_MISMATCH_SOURCE_IDENTITY = (
    b'{"commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","files":{"src/us_equity_snapshot_pipelines/tqqq_r1_snapshot.py":{"content_sha256":"0000000000000000000000000000000000000000000000000000000000000000","git_blob_sha1":"dddddddddddddddddddddddddddddddddddddddd"},"src/us_equity_snapshot_pipelines/yfinance_prices.py":{"content_sha256":"d409c9dfe1548797e4e4d4ba304c310d18fd08260d0e9fa5f025c2a78b849323","git_blob_sha1":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}},"parent":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","ref":"refs/heads/main","repo":"QuantStrategyLab/UsEquitySnapshotPipelines","schema":"qsl.r1.uesp.source-identity-anchor.v1","tree":"cccccccccccccccccccccccccccccccccccccccc","verification":{"allowlisted_reads":0,"source":"fixture","writes":0}}\n'
)
_EXECUTED_FILE_MISMATCH_SOURCE_IDENTITY_SHA256 = (
    "a99b01a774c17055d6054506e4951118ab9349873a2bda6d293d0725b84ecf85"
)
_FIXTURE_XNYS_CALENDAR = (
    b'{"close_utc":"2010-01-04T21:00:00Z","early_close":false,"open_utc":"2010-01-04T14:30:00Z","schema":"qsl.r1.xnys.session.v1","session_date":"2010-01-04"}\n'
    b'{"close_utc":"2010-01-05T21:00:00Z","early_close":false,"open_utc":"2010-01-05T14:30:00Z","schema":"qsl.r1.xnys.session.v1","session_date":"2010-01-05"}\n'
    b'{"close_utc":"2026-07-24T20:00:00Z","early_close":false,"open_utc":"2026-07-24T14:30:00Z","schema":"qsl.r1.xnys.session.v1","session_date":"2026-07-24"}\n'
)
_FIXTURE_XNYS_CALENDAR_SHA256 = "71f36e50f2fff906681b47d0d2f219d317993e967c12cbe2be5a034c126e8aa0"
_MISMATCHED_XNYS_CALENDAR = (
    b'{"close_utc":"2010-01-04T21:00:00Z","early_close":false,"open_utc":"2010-01-04T14:30:00Z","schema":"qsl.r1.xnys.session.v1","session_date":"2010-01-04"}\n'
    b'{"close_utc":"2010-01-05T21:00:00Z","early_close":false,"open_utc":"2010-01-05T14:30:00Z","schema":"qsl.r1.xnys.session.v1","session_date":"2010-01-05"}\n'
    b'{"close_utc":"2026-07-23T20:00:00Z","early_close":false,"open_utc":"2026-07-23T14:30:00Z","schema":"qsl.r1.xnys.session.v1","session_date":"2026-07-24"}\n'
)
_MISMATCHED_XNYS_CALENDAR_SHA256 = "d9e64d7c8d4617650749f4a9c0ddadc10759b3f5315e9a7b6da3695cd7530849"
_RETRIEVAL_ANCHOR_ARGUMENTS = (
    "source_identity",
    "calendar",
    "observed_coverage",
    "source_identity_path",
    "expected_source_identity_sha256",
    "calendar_path",
    "expected_calendar_sha256",
)


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


def _write_external_anchor_fixtures(
    tmp_path: Path,
    *,
    source_identity_bytes: bytes = _FIXTURE_SOURCE_IDENTITY,
    calendar_bytes: bytes = _FIXTURE_XNYS_CALENDAR,
) -> tuple[Path, Path]:
    source_identity = tmp_path / "source-identity.json"
    calendar = tmp_path / "xnys-sessions.ndjson"
    source_identity.write_bytes(source_identity_bytes)
    calendar.write_bytes(calendar_bytes)
    source_identity.chmod(0o444)
    calendar.chmod(0o444)
    return source_identity, calendar


def _fixture_downloaded_prices(*, extra_session: str | None = None) -> pd.DataFrame:
    sessions = ["2010-01-04", "2010-01-05"]
    if extra_session is not None:
        sessions.append(extra_session)
    return pd.DataFrame(
        [
            {"as_of": session, "symbol": symbol, "adjusted_close": 10.0}
            for symbol in ("QQQ", "TQQQ")
            for session in sessions
        ]
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


@pytest.mark.parametrize("anchor_argument", _RETRIEVAL_ANCHOR_ARGUMENTS)
def test_public_materializer_rejects_every_retrieval_anchor(
    tmp_path: Path, anchor_argument: str
) -> None:
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        snapshot.materialize_tqqq_r1_snapshot(
            _fixture_prices(),
            tmp_path / "snapshot",
            **{anchor_argument: object()},
        )


def test_retrieval_anchor_parameters_are_runner_only() -> None:
    materializer_parameters = set(inspect.signature(snapshot.materialize_tqqq_r1_snapshot).parameters)
    runner_parameters = set(inspect.signature(snapshot.run_tqqq_r1_snapshot).parameters)

    assert materializer_parameters == {"prices", "output_dir", "mode", "plugin", "size"}
    assert runner_parameters == {
        "output_dir",
        "source_identity_path",
        "expected_source_identity_sha256",
        "calendar_path",
        "expected_calendar_sha256",
    }


def test_runner_materializes_anchored_output_from_private_download_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_identity, calendar = _write_external_anchor_fixtures(tmp_path)
    monkeypatch.setattr(
        snapshot,
        "_utc_now",
        lambda: datetime(2026, 7, 24, 21, tzinfo=timezone.utc),
        raising=False,
    )
    monkeypatch.setattr(
        snapshot,
        "_download_price_history",
        lambda *_args, **_kwargs: _fixture_downloaded_prices(),
        raising=False,
    )

    result = snapshot.run_tqqq_r1_snapshot(
        tmp_path / "snapshot",
        source_identity_path=source_identity,
        expected_source_identity_sha256=_FIXTURE_SOURCE_IDENTITY_SHA256,
        calendar_path=calendar,
        expected_calendar_sha256=_FIXTURE_XNYS_CALENDAR_SHA256,
    )

    manifest = json.loads((result.output_dir / "manifest.json").read_text(encoding="utf-8"))
    receipt = json.loads((result.output_dir / "retrieval_receipt.json").read_text(encoding="utf-8"))
    assert manifest["source_identity"]["artifact_sha256"] == _FIXTURE_SOURCE_IDENTITY_SHA256
    assert receipt["calendar"]["artifact_sha256"] == _FIXTURE_XNYS_CALENDAR_SHA256
    assert receipt["calendar"]["coverage"] == {
        "first_session": "2010-01-04",
        "last_session": "2026-07-24",
        "session_count": 3,
    }
    assert (result.output_dir / "retrieval_receipt.json").is_file()
    assert snapshot.verify_tqqq_r1_snapshot(
        result.output_dir,
        expected_manifest_sha256=result.manifest_sha256,
        expected_source_identity_sha256=_FIXTURE_SOURCE_IDENTITY_SHA256,
        expected_calendar_sha256=_FIXTURE_XNYS_CALENDAR_SHA256,
    ) == result


def test_runner_uses_private_utc_captured_before_private_download(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_identity, calendar = _write_external_anchor_fixtures(tmp_path)
    events: list[str] = []

    def before_close() -> datetime:
        events.append("clock")
        return datetime(2026, 7, 24, 19, tzinfo=timezone.utc)

    def delayed_download(*_args: object, **_kwargs: object) -> pd.DataFrame:
        events.append("download")
        monkeypatch.setattr(
            snapshot,
            "_utc_now",
            lambda: datetime(2026, 7, 24, 21, tzinfo=timezone.utc),
        )
        return _fixture_downloaded_prices(extra_session="2026-07-24")

    monkeypatch.setattr(snapshot, "_utc_now", before_close, raising=False)
    monkeypatch.setattr(snapshot, "_download_price_history", delayed_download, raising=False)

    with pytest.raises(snapshot.SnapshotValidationError, match="unclosed trading session"):
        snapshot.run_tqqq_r1_snapshot(
            tmp_path / "snapshot",
            source_identity_path=source_identity,
            expected_source_identity_sha256=_FIXTURE_SOURCE_IDENTITY_SHA256,
            calendar_path=calendar,
            expected_calendar_sha256=_FIXTURE_XNYS_CALENDAR_SHA256,
        )

    assert events == ["clock", "download"]


def test_runner_rejects_wrong_external_source_anchor_before_download(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_identity, calendar = _write_external_anchor_fixtures(tmp_path)
    monkeypatch.setattr(
        snapshot,
        "_download_price_history",
        lambda *_args, **_kwargs: pytest.fail("download must not run"),
    )

    with pytest.raises(snapshot.SnapshotValidationError, match="source identity digest mismatch"):
        snapshot.run_tqqq_r1_snapshot(
            tmp_path / "snapshot",
            source_identity_path=source_identity,
            expected_source_identity_sha256="0" * 64,
            calendar_path=calendar,
            expected_calendar_sha256=_FIXTURE_XNYS_CALENDAR_SHA256,
        )


def test_runner_rejects_executed_source_file_mismatch_before_download(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_identity, calendar = _write_external_anchor_fixtures(
        tmp_path,
        source_identity_bytes=_EXECUTED_FILE_MISMATCH_SOURCE_IDENTITY,
    )
    monkeypatch.setattr(
        snapshot,
        "_download_price_history",
        lambda *_args, **_kwargs: pytest.fail("download must not run"),
    )

    with pytest.raises(snapshot.SnapshotValidationError, match="executed-file mismatch"):
        snapshot.run_tqqq_r1_snapshot(
            tmp_path / "snapshot",
            source_identity_path=source_identity,
            expected_source_identity_sha256=_EXECUTED_FILE_MISMATCH_SOURCE_IDENTITY_SHA256,
            calendar_path=calendar,
            expected_calendar_sha256=_FIXTURE_XNYS_CALENDAR_SHA256,
        )


def test_runner_rejects_observed_xnys_holiday_not_in_external_calendar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_identity, calendar = _write_external_anchor_fixtures(tmp_path)
    monkeypatch.setattr(
        snapshot,
        "_utc_now",
        lambda: datetime(2026, 12, 25, 21, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(
        snapshot,
        "_download_price_history",
        lambda *_args, **_kwargs: _fixture_downloaded_prices(extra_session="2026-12-25"),
    )

    with pytest.raises(snapshot.SnapshotValidationError, match="outside XNYS calendar"):
        snapshot.run_tqqq_r1_snapshot(
            tmp_path / "snapshot",
            source_identity_path=source_identity,
            expected_source_identity_sha256=_FIXTURE_SOURCE_IDENTITY_SHA256,
            calendar_path=calendar,
            expected_calendar_sha256=_FIXTURE_XNYS_CALENDAR_SHA256,
        )


def test_verify_rejects_legacy_snapshot_when_any_anchor_expectation_is_supplied(tmp_path: Path) -> None:
    result = snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot")

    with pytest.raises(snapshot.SnapshotValidationError, match="retrieval receipt is required"):
        snapshot.verify_tqqq_r1_snapshot(
            result.output_dir,
            expected_manifest_sha256=result.manifest_sha256,
            expected_source_identity_sha256=_FIXTURE_SOURCE_IDENTITY_SHA256,
        )


def test_runner_rejects_calendar_timestamps_from_another_declared_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_identity, calendar = _write_external_anchor_fixtures(
        tmp_path,
        calendar_bytes=_MISMATCHED_XNYS_CALENDAR,
    )
    monkeypatch.setattr(
        snapshot,
        "_download_price_history",
        lambda *_args, **_kwargs: pytest.fail("download must not run"),
    )

    with pytest.raises(snapshot.SnapshotValidationError, match="timestamps do not match declared session"):
        snapshot.run_tqqq_r1_snapshot(
            tmp_path / "snapshot",
            source_identity_path=source_identity,
            expected_source_identity_sha256=_FIXTURE_SOURCE_IDENTITY_SHA256,
            calendar_path=calendar,
            expected_calendar_sha256=_MISMATCHED_XNYS_CALENDAR_SHA256,
        )


def test_runner_rejects_timezone_naive_private_clock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_identity, calendar = _write_external_anchor_fixtures(tmp_path)
    monkeypatch.setattr(snapshot, "_utc_now", lambda: datetime(2026, 7, 24, 21))
    monkeypatch.setattr(
        snapshot,
        "_download_price_history",
        lambda *_args, **_kwargs: _fixture_downloaded_prices(),
    )

    with pytest.raises(snapshot.SnapshotValidationError, match="timezone-aware"):
        snapshot.run_tqqq_r1_snapshot(
            tmp_path / "snapshot",
            source_identity_path=source_identity,
            expected_source_identity_sha256=_FIXTURE_SOURCE_IDENTITY_SHA256,
            calendar_path=calendar,
            expected_calendar_sha256=_FIXTURE_XNYS_CALENDAR_SHA256,
        )


def test_runner_rejects_session_future_relative_to_private_clock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_identity, calendar = _write_external_anchor_fixtures(tmp_path)
    monkeypatch.setattr(
        snapshot,
        "_utc_now",
        lambda: datetime(2010, 1, 4, 22, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(
        snapshot,
        "_download_price_history",
        lambda *_args, **_kwargs: _fixture_downloaded_prices(),
    )

    with pytest.raises(snapshot.SnapshotValidationError, match="future observed session"):
        snapshot.run_tqqq_r1_snapshot(
            tmp_path / "snapshot",
            source_identity_path=source_identity,
            expected_source_identity_sha256=_FIXTURE_SOURCE_IDENTITY_SHA256,
            calendar_path=calendar,
            expected_calendar_sha256=_FIXTURE_XNYS_CALENDAR_SHA256,
        )
