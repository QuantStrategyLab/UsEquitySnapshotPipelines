from __future__ import annotations

import hashlib
import json
import subprocess
import sys
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


def _materialize(prices: pd.DataFrame, output_dir: Path, **kwargs: object) -> snapshot.SnapshotResult:
    normalized = snapshot._normalize_prices(prices)
    receipt = _receipt_for(normalized)
    return snapshot.materialize_tqqq_r1_snapshot(prices, output_dir, retrieval_receipt=receipt, **kwargs)


def _receipt_for(normalized: pd.DataFrame) -> dict[str, object]:
    return snapshot._build_receipt(
        coverage=snapshot._coverage(normalized),
        common_sessions=snapshot._common_session_coverage(normalized),
        source={"repository": "test/repository", "commit": "test-commit", "tree": "test-tree"},
        yfinance_version="test-yfinance",
        retrieval_utc="2026-07-25T00:00:00Z",
    )


def _verify(output_dir: Path, manifest_sha256: str, receipt_path: Path) -> snapshot.SnapshotResult:
    return snapshot.verify_tqqq_r1_snapshot(
        output_dir,
        expected_manifest_sha256=manifest_sha256,
        expected_receipt_path=receipt_path,
        expected_receipt_bytes=receipt_path.read_bytes(),
    )


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
    result = _materialize(_fixture_prices(), tmp_path / "snapshot")

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
        "tqqq_r1_qqq_tqqq_immutable_snapshot.v3"
    )
    assert _verify(result.output_dir, result.manifest_sha256, result.receipt_path) == result


def test_materialize_preserves_adjusted_close_float_round_trip_precision(tmp_path: Path) -> None:
    prices = _fixture_prices()
    prices.loc[prices["symbol"].eq("QQQ") & prices["session"].eq("2010-01-04"), "adjusted_close"] = 1.0000000000000002

    result = _materialize(prices, tmp_path / "snapshot")

    actual = pd.read_csv(result.output_dir / "prices.csv").loc[lambda frame: frame["symbol"].eq("QQQ"), "adjusted_close"].iloc[0]
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
        _materialize(prices, tmp_path / "snapshot")


def test_materialize_rejects_datetime_adjusted_close(tmp_path: Path) -> None:
    prices = _fixture_prices().assign(adjusted_close=pd.Timestamp("2026-07-25"))

    with pytest.raises(snapshot.SnapshotValidationError, match="datetime-like adjusted_close"):
        _materialize(prices, tmp_path / "snapshot")


def test_verify_rejects_noncanonical_symbol_readback(tmp_path: Path) -> None:
    result = _materialize(_fixture_prices(), tmp_path / "snapshot")
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
        _verify(output_dir, _refresh_trusted_metadata(output_dir), result.receipt_path)


def test_verify_rejects_noncanonical_raw_session_encoding(tmp_path: Path) -> None:
    result = _materialize(_fixture_prices(), tmp_path / "snapshot")
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
        _verify(output_dir, _refresh_trusted_metadata(output_dir), result.receipt_path)


def test_verify_rejects_boolean_readback(tmp_path: Path) -> None:
    result = _materialize(_fixture_prices(), tmp_path / "snapshot")
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
        _verify(output_dir, _refresh_trusted_metadata(output_dir), result.receipt_path)


def test_verify_rejects_mixed_offset_session_readback(tmp_path: Path) -> None:
    result = _materialize(_fixture_prices(), tmp_path / "snapshot")
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
        _verify(output_dir, _refresh_trusted_metadata(output_dir), result.receipt_path)


def test_verify_rejects_invalid_metadata_scalar_types(tmp_path: Path) -> None:
    result = _materialize(_fixture_prices(), tmp_path / "snapshot")
    output_dir = result.output_dir
    _write_json(output_dir / "validation.json", {"valid": 1, "row_count": 4.0, "symbols": ["QQQ", "TQQQ"]})

    with pytest.raises(snapshot.SnapshotValidationError, match="invalid validation"):
        _verify(output_dir, _refresh_trusted_metadata(output_dir), result.receipt_path)


def test_verify_normalizes_csv_parse_failures(tmp_path: Path) -> None:
    result = _materialize(_fixture_prices(), tmp_path / "snapshot")
    output_dir = result.output_dir
    (output_dir / "prices.csv").write_bytes(b"\xff")

    with pytest.raises(snapshot.SnapshotValidationError, match="invalid prices.csv"):
        _verify(output_dir, _refresh_trusted_metadata(output_dir), result.receipt_path)


def test_runner_invokes_exact_yfinance_download_contract(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[tuple[object, dict[str, object]]] = []

    def fake_download(*args: object, **kwargs: object) -> pd.DataFrame:
        calls.append((args, kwargs))
        return pd.DataFrame(
            [
                {"symbol": "QQQ", "as_of": "2010-01-04", "close": 45.25},
                {"symbol": "TQQQ", "as_of": "2010-01-04", "close": 10.5},
            ]
        )

    monkeypatch.setattr(snapshot, "download_price_history", fake_download)
    monkeypatch.setattr(snapshot, "_source_identity", lambda: {"repository": "test/repo", "commit": "abc", "tree": "def"})
    monkeypatch.setattr(snapshot, "_yfinance_runtime_version", lambda: "0.test")
    monkeypatch.setattr(snapshot, "_retrieval_utc", lambda: "2026-07-25T00:00:00Z")

    snapshot.run_tqqq_r1_snapshot(tmp_path / "snapshot")

    assert calls == [((['QQQ', 'TQQQ'],), {"start": "2010-01-01", "price_field": "adjusted_close"})]


def test_runner_rejects_proxy_configuration_before_download(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("YFINANCE_PROXY", "http://proxy.example")
    monkeypatch.setattr(snapshot, "download_price_history", lambda *_args, **_kwargs: pytest.fail("must not download"))

    with pytest.raises(snapshot.SnapshotValidationError, match="proxy is not allowed"):
        snapshot.run_tqqq_r1_snapshot(tmp_path / "snapshot")


def test_runner_records_raw_coverage_and_materializes_only_common_sessions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        snapshot,
        "download_price_history",
        lambda *_args, **_kwargs: pd.DataFrame(
            [
                {"symbol": "QQQ", "as_of": "2010-01-04", "close": 45.25},
                {"symbol": "QQQ", "as_of": "2010-01-05", "close": 46.0},
                {"symbol": "TQQQ", "as_of": "2010-01-05", "close": 10.5},
                {"symbol": "TQQQ", "as_of": "2010-01-06", "close": 11.0},
            ]
        ),
    )
    monkeypatch.setattr(snapshot, "_source_identity", lambda: {"repository": "test/repo", "commit": "abc", "tree": "def"})
    monkeypatch.setattr(snapshot, "_yfinance_runtime_version", lambda: "0.test")
    monkeypatch.setattr(snapshot, "_retrieval_utc", lambda: "2026-07-25T00:00:00Z")

    result = snapshot.run_tqqq_r1_snapshot(tmp_path / "snapshot")

    receipt = json.loads(result.receipt_path.read_text(encoding="utf-8"))
    manifest = json.loads((result.output_dir / "manifest.json").read_text(encoding="utf-8"))
    validation = json.loads((result.output_dir / "validation.json").read_text(encoding="utf-8"))
    assert receipt["observed_coverage"] == {
        "QQQ": {"first_session": "2010-01-04", "last_session": "2010-01-05", "row_count": 2},
        "TQQQ": {"first_session": "2010-01-05", "last_session": "2010-01-06", "row_count": 2},
    }
    assert receipt["common_session_coverage"] == {"first_session": "2010-01-05", "last_session": "2010-01-05", "row_count": 1}
    assert manifest["receipt_sha256"] == result.receipt_sha256
    assert manifest["source"] == receipt["source"]
    assert validation["strict_readback"] is True
    assert (result.output_dir / "prices.csv").read_text(encoding="utf-8").splitlines()[1:] == [
        "2010-01-05,QQQ,46",
        "2010-01-05,TQQQ,10.5",
    ]
    assert _verify(result.output_dir, result.manifest_sha256, result.receipt_path) == result


def test_verify_rejects_receipt_tamper_even_with_original_expected_bytes(tmp_path: Path) -> None:
    result = _materialize(_fixture_prices(), tmp_path / "snapshot")
    expected_receipt_bytes = result.receipt_path.read_bytes()
    receipt = json.loads(expected_receipt_bytes)
    receipt["route"] = "tampered"
    _write_json(result.receipt_path, receipt)

    with pytest.raises(snapshot.SnapshotValidationError, match="trusted receipt bytes mismatch"):
        snapshot.verify_tqqq_r1_snapshot(
            result.output_dir,
            expected_manifest_sha256=result.manifest_sha256,
            expected_receipt_path=result.receipt_path,
            expected_receipt_bytes=expected_receipt_bytes,
        )


def test_verify_rejects_manifest_receipt_hash_mismatch(tmp_path: Path) -> None:
    result = _materialize(_fixture_prices(), tmp_path / "snapshot")
    manifest_path = result.output_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["receipt_sha256"] = "0" * 64
    _write_json(manifest_path, manifest)

    with pytest.raises(snapshot.SnapshotValidationError, match="invalid manifest"):
        _verify(result.output_dir, _refresh_trusted_metadata(result.output_dir), result.receipt_path)


def test_verify_rejects_symlinked_receipt_path(tmp_path: Path) -> None:
    result = _materialize(_fixture_prices(), tmp_path / "snapshot")
    expected_receipt_bytes = result.receipt_path.read_bytes()
    copied_receipt = tmp_path / "receipt-copy.json"
    copied_receipt.write_bytes(expected_receipt_bytes)
    result.receipt_path.unlink()
    result.receipt_path.symlink_to(copied_receipt)

    with pytest.raises(snapshot.SnapshotValidationError, match="symlinked snapshot path"):
        snapshot.verify_tqqq_r1_snapshot(
            result.output_dir,
            expected_manifest_sha256=result.manifest_sha256,
            expected_receipt_path=result.receipt_path,
            expected_receipt_bytes=expected_receipt_bytes,
        )


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("source", "commit"), 1),
        (("retrieval_utc",), "not-a-dateZ"),
        (("yfinance_runtime_version",), 1),
        (("schema_version",), 1),
    ],
    ids=["source-type", "date-format", "runtime-type", "schema-type"],
)
def test_materialize_rejects_invalid_receipt_contract_types(
    tmp_path: Path, path: tuple[str, ...], value: object
) -> None:
    normalized = snapshot._normalize_prices(_fixture_prices())
    receipt = snapshot._build_receipt(
        coverage=snapshot._coverage(normalized),
        common_sessions=snapshot._common_session_coverage(normalized),
        source={"repository": "test/repository", "commit": "test-commit", "tree": "test-tree"},
        yfinance_version="test-yfinance",
        retrieval_utc="2026-07-25T00:00:00Z",
    )
    target: dict[str, object] = receipt
    for key in path[:-1]:
        target = target[key]  # type: ignore[assignment]
    target[path[-1]] = value

    with pytest.raises(snapshot.SnapshotValidationError, match="invalid receipt"):
        snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot", retrieval_receipt=receipt)


def test_materialize_rejects_missing_receipt_source_field(tmp_path: Path) -> None:
    normalized = snapshot._normalize_prices(_fixture_prices())
    receipt = snapshot._build_receipt(
        coverage=snapshot._coverage(normalized),
        common_sessions=snapshot._common_session_coverage(normalized),
        source={"repository": "test/repository", "commit": "test-commit", "tree": "test-tree"},
        yfinance_version="test-yfinance",
        retrieval_utc="2026-07-25T00:00:00Z",
    )
    del receipt["source"]["tree"]

    with pytest.raises(snapshot.SnapshotValidationError, match="invalid receipt"):
        snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot", retrieval_receipt=receipt)


def test_materialize_rejects_missing_receipt_schema_field(tmp_path: Path) -> None:
    normalized = snapshot._normalize_prices(_fixture_prices())
    receipt = snapshot._build_receipt(
        coverage=snapshot._coverage(normalized),
        common_sessions=snapshot._common_session_coverage(normalized),
        source={"repository": "test/repository", "commit": "test-commit", "tree": "test-tree"},
        yfinance_version="test-yfinance",
        retrieval_utc="2026-07-25T00:00:00Z",
    )
    del receipt["schema_version"]

    with pytest.raises(snapshot.SnapshotValidationError, match="invalid receipt"):
        snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot", retrieval_receipt=receipt)


def test_failed_stage_does_not_publish_destination_or_receipt(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(snapshot, "verify_tqqq_r1_snapshot", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("fail")))

    with pytest.raises(RuntimeError, match="fail"):
        _materialize(_fixture_prices(), tmp_path / "snapshot")

    assert not (tmp_path / "snapshot").exists()
    assert not (tmp_path / "snapshot.tqqq_r1_retrieval_receipt.v1.json").exists()


def _init_source_checkout(tmp_path: Path) -> Path:
    source_root = tmp_path / "source"
    package = source_root / "src" / "us_equity_snapshot_pipelines"
    package.mkdir(parents=True)
    module_path = Path(snapshot.__file__)
    (package / "tqqq_r1_snapshot.py").write_bytes(module_path.read_bytes())
    (package / "yfinance_prices.py").write_bytes((module_path.parent / "yfinance_prices.py").read_bytes())
    subprocess.run(["git", "init", "-q"], cwd=source_root, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=source_root, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=source_root, check=True)
    subprocess.run(["git", "add", "."], cwd=source_root, check=True)
    subprocess.run(["git", "commit", "-qm", "source"], cwd=source_root, check=True)
    subprocess.run(
        ["git", "remote", "add", "origin", "https://github.com/QuantStrategyLab/UsEquitySnapshotPipelines.git"],
        cwd=source_root,
        check=True,
    )
    return source_root


class _DirectUrlDistribution:
    def __init__(self, source_root: Path) -> None:
        self._source_root = source_root

    def read_text(self, name: str) -> str | None:
        return json.dumps({"url": self._source_root.as_uri()}) if name == "direct_url.json" else None


def test_source_identity_uses_verified_direct_url_checkout_for_non_editable_install(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source_root = _init_source_checkout(tmp_path)
    installed_module = tmp_path / "site-packages" / "us_equity_snapshot_pipelines" / "tqqq_r1_snapshot.py"
    installed_module.parent.mkdir(parents=True)
    installed_module.write_bytes((source_root / "src" / "us_equity_snapshot_pipelines" / "tqqq_r1_snapshot.py").read_bytes())
    installed_helper = installed_module.with_name("yfinance_prices.py")
    installed_helper.write_bytes((source_root / "src" / "us_equity_snapshot_pipelines" / "yfinance_prices.py").read_bytes())
    monkeypatch.setattr(snapshot, "__file__", str(installed_module))
    monkeypatch.setattr(sys.modules[snapshot.download_price_history.__module__], "__file__", str(installed_helper))
    monkeypatch.setattr(snapshot.importlib.metadata, "distribution", lambda _name: _DirectUrlDistribution(source_root))

    identity = snapshot._source_identity()

    assert identity["repository"] == "https://github.com/QuantStrategyLab/UsEquitySnapshotPipelines.git"
    assert identity["commit"] == subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=source_root, check=True, capture_output=True, text=True
    ).stdout.strip()


def test_source_identity_rejects_dirty_executed_download_helper(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    source_root = _init_source_checkout(tmp_path)
    installed_module = tmp_path / "site-packages" / "us_equity_snapshot_pipelines" / "tqqq_r1_snapshot.py"
    installed_module.parent.mkdir(parents=True)
    installed_module.write_bytes((source_root / "src" / "us_equity_snapshot_pipelines" / "tqqq_r1_snapshot.py").read_bytes())
    installed_helper = installed_module.with_name("yfinance_prices.py")
    installed_helper.write_bytes((source_root / "src" / "us_equity_snapshot_pipelines" / "yfinance_prices.py").read_bytes())
    (source_root / "src" / "us_equity_snapshot_pipelines" / "yfinance_prices.py").write_text("# dirty\n", encoding="utf-8")
    monkeypatch.setattr(snapshot, "__file__", str(installed_module))
    monkeypatch.setattr(sys.modules[snapshot.download_price_history.__module__], "__file__", str(installed_helper))
    monkeypatch.setattr(snapshot.importlib.metadata, "distribution", lambda _name: _DirectUrlDistribution(source_root))

    with pytest.raises(snapshot.SnapshotValidationError, match="dirty source"):
        snapshot._source_identity()


def test_source_identity_rejects_checkout_with_different_executed_helper(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source_root = _init_source_checkout(tmp_path)
    installed_module = tmp_path / "site-packages" / "us_equity_snapshot_pipelines" / "tqqq_r1_snapshot.py"
    installed_module.parent.mkdir(parents=True)
    installed_module.write_bytes((source_root / "src" / "us_equity_snapshot_pipelines" / "tqqq_r1_snapshot.py").read_bytes())
    installed_helper = installed_module.with_name("yfinance_prices.py")
    installed_helper.write_text("# different helper\n", encoding="utf-8")
    monkeypatch.setattr(snapshot, "__file__", str(installed_module))
    monkeypatch.setattr(sys.modules[snapshot.download_price_history.__module__], "__file__", str(installed_helper))
    monkeypatch.setattr(snapshot.importlib.metadata, "distribution", lambda _name: _DirectUrlDistribution(source_root))

    with pytest.raises(snapshot.SnapshotValidationError, match="unable to resolve verified source identity"):
        snapshot._source_identity()


def test_materialize_rejects_receipt_coverage_outside_common_session(tmp_path: Path) -> None:
    normalized = snapshot._normalize_prices(_fixture_prices())
    receipt = _receipt_for(normalized)
    receipt["observed_coverage"]["QQQ"]["first_session"] = "2010-01-05"

    with pytest.raises(snapshot.SnapshotValidationError, match="coverage"):
        snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot", retrieval_receipt=receipt)


def test_materialize_never_replaces_an_external_receipt(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    output_dir = tmp_path / "snapshot"
    receipt_path = tmp_path / "snapshot.tqqq_r1_retrieval_receipt.v1.json"
    original_replace = snapshot.os.replace

    def guarded_replace(source: str | Path, destination: str | Path) -> None:
        if Path(destination) == receipt_path:
            raise AssertionError("external receipt must be published without replacement")
        original_replace(source, destination)

    monkeypatch.setattr(snapshot.os, "replace", guarded_replace)

    result = _materialize(_fixture_prices(), output_dir)

    assert result.receipt_path == receipt_path
    assert _verify(result.output_dir, result.manifest_sha256, result.receipt_path) == result


def test_losing_receipt_publication_preserves_winner_receipt(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    output_dir = tmp_path / "snapshot"
    receipt_path = tmp_path / "snapshot.tqqq_r1_retrieval_receipt.v1.json"
    winner_receipt = b"winner receipt"

    def lose_link(_source: str | Path, destination: str | Path) -> None:
        Path(destination).write_bytes(winner_receipt)
        raise FileExistsError

    monkeypatch.setattr(snapshot.os, "link", lose_link)

    with pytest.raises(FileExistsError):
        _materialize(_fixture_prices(), output_dir)

    assert receipt_path.read_bytes() == winner_receipt
    assert not output_dir.exists()


def test_materialize_rejects_boolean_receipt_size(tmp_path: Path) -> None:
    normalized = snapshot._normalize_prices(_fixture_prices())
    receipt = _receipt_for(normalized)
    receipt["size"] = False

    with pytest.raises(snapshot.SnapshotValidationError, match="invalid receipt"):
        snapshot.materialize_tqqq_r1_snapshot(_fixture_prices(), tmp_path / "snapshot", retrieval_receipt=receipt)
