from __future__ import annotations

import math
import importlib.util
import hashlib
import json
import os
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location("r0a_evidence", Path(__file__).parents[1] / "src/us_equity_snapshot_pipelines/evidence.py")
assert _spec and _spec.loader
_evidence = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_evidence)
REQUIRED_ARTIFACT_FILES = _evidence.REQUIRED_ARTIFACT_FILES
EvidenceValidationError = _evidence.EvidenceValidationError
validate_evidence_manifest = _evidence.validate_evidence_manifest
validate_metrics_payload = _evidence.validate_metrics_payload
validate_evidence_bundle = getattr(_evidence, "validate_evidence_bundle", None)


def _manifest() -> dict[str, object]:
    return {
        "schema": "soxl_tqqq_research_evidence.v1",
        "profile": "soxl_soxx_trend_income",
        "run_id": "r0a.test",
        "code_sha": "a" * 40,
        "config_sha256": "b" * 64,
        "plugin_sha256": "c" * 64,
        "data_sha256": "d" * 64,
        "data_revision": "prices-2026.07",
        "calendar": "XNYS",
        "timezone": "America/New_York",
        "execution_timing": "next_open",
        "cost_model_id": "bps.5",
        "random_seed": 0,
        "as_of": "2026-07-13",
        "generated_at": "2026-07-13T18:15:03.123Z",
        "artifacts": {name: "e" * 64 for name in REQUIRED_ARTIFACT_FILES},
    }


def test_valid_manifest_needs_no_bundle_root() -> None:
    payload = _manifest()
    assert validate_evidence_manifest(payload) == payload


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p["artifacts"].pop(REQUIRED_ARTIFACT_FILES[0]),
        lambda p: p["artifacts"].update({"prices.parquet": "e" * 64}),
        lambda p: p["artifacts"].update({"prices.csv": "E" * 64}),
        lambda p: p["artifacts"].update({"prices.csv": "e" * 63}),
    ],
)
def test_manifest_rejects_non_exact_artifact_mapping(mutate) -> None:
    payload = _manifest()
    mutate(payload)
    with pytest.raises(EvidenceValidationError):
        validate_evidence_manifest(payload)


@pytest.mark.parametrize(
    "field,value",
    [
        ("as_of", "2026-02-30"),
        ("as_of", "2026-7-3"),
        ("generated_at", "2026-02-30T18:15:03Z"),
        ("generated_at", "2026-07-13T18:15:03+00:00"),
        ("generated_at", "2026-07-13T18:15:03+01:00"),
    ],
)
def test_manifest_rejects_annotation_only_dates(field, value) -> None:
    payload = _manifest()
    payload[field] = value
    with pytest.raises(EvidenceValidationError):
        validate_evidence_manifest(payload)


def test_metrics_require_numeric_finite_evidence() -> None:
    assert validate_metrics_payload({"metrics": {"sharpe": 1.2, "nested": {"mdd": -0.3}}})["metrics"]
    for value in (math.nan, math.inf, -math.inf):
        with pytest.raises(EvidenceValidationError):
            validate_metrics_payload({"metrics": {"risk": {"value": value}}})
    with pytest.raises(EvidenceValidationError):
        validate_metrics_payload({"metrics": {"note": "no numeric evidence", "ok": True}})

def _write_bundle(root: Path) -> None:
    root.mkdir(exist_ok=True)
    manifest = _manifest()
    fields = ("profile", "code_sha", "config_sha256", "plugin_sha256", "execution_timing", "timezone", "calendar", "as_of")
    identity = {**{field: manifest[field] for field in fields}, "schema": "champion_identity.v1"}
    content = {name: '{"ok":true}' for name in ("data_quality.json", "walk_forward.json", "robustness.json", "risk_sleeve.json")}
    content.update({name: "a,b\n1,2\n" for name in ("prices.csv", "daily_returns.csv", "targets.csv", "trades.csv", "costs.csv")})
    content.update({"champion_identity.json": json.dumps(identity), "metrics.json": '{"metrics":{"sharpe":1.2}}', "strategy_performance.v2.json": '{"returns":[0.1]}', "trial_ledger.jsonl": '{"trial":1}\n'})
    for name, data in content.items():
        (root / name).write_text(data, encoding="utf-8")
    checksums = "".join(f"{hashlib.sha256(content[name].encode()).hexdigest()}  {name}\n" for name in sorted(content))
    (root / "checksums.sha256").write_text(checksums, encoding="ascii")
    manifest["artifacts"] = {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in REQUIRED_ARTIFACT_FILES}
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


def _refresh(root: Path) -> None:
    names = sorted(set(REQUIRED_ARTIFACT_FILES) - {"checksums.sha256"})
    digests = {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in names}
    (root / "checksums.sha256").write_text("".join(f"{digests[n]}  {n}\n" for n in names), encoding="ascii")
    digests["checksums.sha256"] = hashlib.sha256((root / "checksums.sha256").read_bytes()).hexdigest()
    manifest = json.loads((root / "manifest.json").read_text())
    manifest["artifacts"] = digests
    (root / "manifest.json").write_text(json.dumps(manifest))


def _reject(root: Path, name: str, data: str | bytes, refresh: bool = True) -> None:
    path = root / name
    if isinstance(data, bytes):
        path.write_bytes(data)
    else:
        path.write_text(data, encoding="utf-8")
    if refresh:
        _refresh(root)
    with pytest.raises(EvidenceValidationError, match=name.replace(".", r"\.")):
        validate_evidence_bundle(root)


@pytest.fixture
def bundle(tmp_path: Path) -> Path:
    _write_bundle(tmp_path)
    return tmp_path


def test_valid_bundle_is_structure_only(bundle: Path) -> None:
    result = validate_evidence_bundle(bundle)
    assert result["bundle_integrity_valid"] is True and result["content_safety_status"] == "not_evaluated"
    assert set(result) == {"bundle_integrity_valid", "content_safety_status", "manifest"}
    assert "bundle_secret_free" not in result


def test_manifest_and_root_precedence(bundle: Path, tmp_path: Path) -> None:
    (bundle / "manifest.json").write_text("{}")
    (bundle / "prices.csv").write_text("bad\n")
    with pytest.raises(EvidenceValidationError, match="manifest"):
        validate_evidence_bundle(bundle)
    for kind in ("missing", "extra", "directory"):
        case = tmp_path / kind
        _write_bundle(case)
        if kind == "missing":
            (case / "prices.csv").unlink()
        elif kind == "extra":
            (case / "unexpected").write_text("x")
        else:
            (case / "unexpected").mkdir()
        with pytest.raises(EvidenceValidationError, match="bundle root"):
            validate_evidence_bundle(case)


def test_links_and_fifo(bundle: Path, tmp_path: Path) -> None:
    os.symlink(bundle, tmp_path / "link")
    with pytest.raises(EvidenceValidationError, match="bundle root"):
        validate_evidence_bundle(tmp_path / "link")
    artifact = tmp_path / "artifact"
    _write_bundle(artifact)
    target = artifact / "prices.csv"
    target.unlink()
    os.symlink("costs.csv", target)
    with pytest.raises(EvidenceValidationError, match="prices.csv"):
        validate_evidence_bundle(artifact)
    if hasattr(os, "mkfifo"):
        _write_bundle(tmp_path / "fifo")
        (tmp_path / "fifo" / "prices.csv").unlink()
        os.mkfifo(tmp_path / "fifo" / "prices.csv")
        with pytest.raises(EvidenceValidationError, match="prices.csv"):
            validate_evidence_bundle(tmp_path / "fifo")


@pytest.mark.parametrize("name", tuple(_evidence._FILE_MAX_BYTES))
def test_file_caps_and_bundle_total(bundle: Path, monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    assert set(_evidence._FILE_MAX_BYTES) == {"manifest.json", *REQUIRED_ARTIFACT_FILES}
    monkeypatch.setitem(_evidence._FILE_MAX_BYTES, name, (bundle / name).stat().st_size - 1)
    with pytest.raises(EvidenceValidationError, match=name.replace(".", r"\.")):
        validate_evidence_bundle(bundle)
    monkeypatch.setitem(_evidence._FILE_MAX_BYTES, name, 10**9)
    monkeypatch.setattr(_evidence, "_MAX_BUNDLE_BYTES", 1)
    with pytest.raises(EvidenceValidationError, match="bundle total"):
        validate_evidence_bundle(bundle)


def test_streaming_contract(bundle: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = Path(_evidence.__file__).read_text()
    assert all(x not in source for x in ("Path.read_text", "Path.read_bytes", ".read()"))
    assert "read(_HASH_CHUNK_BYTES)" in source
    monkeypatch.setattr(Path, "read_text", lambda *a, **k: (_ for _ in ()).throw(AssertionError()))
    monkeypatch.setattr(Path, "read_bytes", lambda *a, **k: (_ for _ in ()).throw(AssertionError()))
    validate_evidence_bundle(bundle)


@pytest.mark.parametrize(("name", "data"), [
    ("data_quality.json", '{"a":1,"\\u0061":2}'), ("data_quality.json", '{"a":{"x":1,"x":2}}'),
    ("data_quality.json", "not-json"), ("data_quality.json", "[]"), ("data_quality.json", "NaN"),
    ("data_quality.json", b"{\xff}"), ("trial_ledger.jsonl", b""), ("trial_ledger.jsonl", b"\n"),
    ("trial_ledger.jsonl", b"[]\n"), ("trial_ledger.jsonl", b'{"a":1}'),
    ("trial_ledger.jsonl", b'{"a":1,"\\u0061":2}\n'), ("trades.csv", ",b\n1,2\n"),
    ("trades.csv", "a,b\n"), ("trades.csv", "a,b\n\n"), ("trades.csv", "a,a\n1,2\n"),
    ("trades.csv", "a,b\n1\n"), ("trades.csv", ""), ("data_quality.json", "depth")])
def test_bounded_json_jsonl_csv(bundle: Path, name: str, data: str | bytes, monkeypatch: pytest.MonkeyPatch) -> None:
    if data == "depth":
        data = {}
        node = data
        for _ in range(65):
            node["x"] = {}
            node = node["x"]
        data = json.dumps(data)
    _reject(bundle, name, data)
    monkeypatch.setattr(_evidence.json, "loads", lambda *a, **k: (_ for _ in ()).throw(RecursionError))
    with pytest.raises(EvidenceValidationError, match="manifest"):
        validate_evidence_bundle(bundle)


def test_line_and_file_total_bounds(bundle: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(_evidence, "_MAX_LINE_BYTES", 4)
    _reject(bundle, "trial_ledger.jsonl", b'{"x":1}\n', False)
    monkeypatch.setattr(_evidence, "_MAX_LINE_BYTES", 12)
    case = tmp_path / "csv"
    _write_bundle(case)
    _reject(case, "trades.csv", "a,b\n1234567890,2\n", False)
    monkeypatch.setattr(_evidence, "_MAX_LINE_BYTES", 1_048_576)
    case = tmp_path / "total"
    _write_bundle(case)
    monkeypatch.setitem(_evidence._FILE_MAX_BYTES, "trades.csv", 1)
    _reject(case, "trades.csv", "a,b\n1,2\n", False)


def test_csv_rejects_unterminated_quote(bundle: Path) -> None:
    (bundle / "trades.csv").write_text('a,b\n"unterminated,2\n')
    _refresh(bundle)
    with pytest.raises(EvidenceValidationError, match="trades\\.csv CSV"):
        validate_evidence_bundle(bundle)


def test_checksum_set_and_digest_edges(bundle: Path, tmp_path: Path) -> None:
    valid = (bundle / "checksums.sha256").read_text().splitlines()
    cases = [valid[:-1], valid + [valid[0]], valid[:1] + valid[:1] + valid[1:], list(reversed(valid)),
             valid[:-1] + ["0" * 64 + "  " + valid[0].split("  ")[1]], valid + ["0" * 64 + "  checksums.sha256"],
             valid + ["0" * 64 + "  manifest.json"], [valid[0][:-1]], ["bad"]]
    for lines in cases:
        _write_bundle(tmp_path)
        (tmp_path / "checksums.sha256").write_text("\n".join(lines) + "\n")
        with pytest.raises(EvidenceValidationError, match="checksums\\.sha256"):
            validate_evidence_bundle(tmp_path)
    _write_bundle(tmp_path)
    _reject(tmp_path, "prices.csv", "a,b\n9,2\n", False)


@pytest.mark.parametrize("field", ["profile", "code_sha", "config_sha256", "plugin_sha256", "execution_timing", "timezone", "calendar", "as_of"])
def test_champion_consistency(bundle: Path, field: str) -> None:
    identity = json.loads((bundle / "champion_identity.json").read_text())
    identity[field] = "bad"
    (bundle / "champion_identity.json").write_text(json.dumps(identity))
    _refresh(bundle)
    with pytest.raises(EvidenceValidationError, match=f"champion identity {field}"):
        validate_evidence_bundle(bundle)


def test_champion_metrics_and_redaction(bundle: Path, tmp_path: Path) -> None:
    for action in ("unknown", "missing"):
        _write_bundle(tmp_path)
        identity = json.loads((tmp_path / "champion_identity.json").read_text())
        if action == "unknown":
            identity["unknown"] = 1
        else:
            identity.pop("calendar")
        (tmp_path / "champion_identity.json").write_text(json.dumps(identity))
        _refresh(tmp_path)
        with pytest.raises(EvidenceValidationError, match="champion identity fields"):
            validate_evidence_bundle(tmp_path)
    for data in ('{"metrics":{"note":"x","ok":true}}', '{"metrics":{"x":NaN}}'):
        _write_bundle(tmp_path)
        _reject(tmp_path, "metrics.json", data)
    _write_bundle(tmp_path)
    needle = "/absolute/user/path?token=SECRET/" + "0" * 64
    (tmp_path / "data_quality.json").write_text('{"x":"' + needle + '"')
    with pytest.raises(EvidenceValidationError) as error:
        validate_evidence_bundle(tmp_path)
    assert needle not in str(error.value) and str(tmp_path) not in str(error.value)
