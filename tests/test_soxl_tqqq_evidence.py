from __future__ import annotations

import math
import importlib.util
import hashlib
import json
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
validate_evidence_bundle = _evidence.validate_evidence_bundle


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


def _bundle(tmp_path: Path) -> Path:
    root = tmp_path / "bundle"
    root.mkdir(parents=True)
    manifest = _manifest()
    identity_fields = ("schema", "profile", "code_sha", "config_sha256", "plugin_sha256",
                       "execution_timing", "timezone", "calendar", "as_of")
    identity = {"schema": "champion_identity.v1", **{key: manifest[key] for key in identity_fields[1:]}}
    bodies = {
        "champion_identity.json": identity,
        "data_quality.json": {"quality": {"score": 1}},
        "metrics.json": {"metrics": {"sharpe": 1.0}},
        "walk_forward.json": {"windows": [{"return": 0.1}]},
        "robustness.json": {"checks": [{"passed": 1}]},
        "risk_sleeve.json": {"risk": {"mdd": -0.2}},
        "strategy_performance.v2.json": {"performance": {"return": 0.1}},
    }
    csv_bodies = {name: "symbol,value\nSOXL,1\n" for name in
                  ("prices.csv", "daily_returns.csv", "targets.csv", "trades.csv", "costs.csv")}
    for name, body in bodies.items():
        (root / name).write_text(json.dumps(body), encoding="utf-8")
    for name, body in csv_bodies.items():
        (root / name).write_text(body, encoding="utf-8")
    (root / "trial_ledger.jsonl").write_text('{"trial": 1}\n', encoding="utf-8")
    digests = {name: hashlib.sha256((root / name).read_bytes()).hexdigest()
               for name in REQUIRED_ARTIFACT_FILES if name != "checksums.sha256"}
    checksums = "".join(f"{digests[name]}  {name}\n" for name in sorted(digests))
    (root / "checksums.sha256").write_text(checksums, encoding="utf-8")
    manifest["artifacts"] = {**digests, "checksums.sha256": hashlib.sha256(checksums.encode()).hexdigest()}
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return root


def test_valid_bundle_is_secret_free_without_read_text_or_read_bytes(monkeypatch, tmp_path: Path) -> None:
    root = _bundle(tmp_path)
    monkeypatch.setattr(Path, "read_text", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError()))
    monkeypatch.setattr(Path, "read_bytes", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError()))
    assert validate_evidence_bundle(root)["bundle_secret_free"] is True


@pytest.mark.parametrize("name,body", [
    ("data_quality.json", '{"outer": [{"accountId": "redacted"}]}'),
    ("trial_ledger.jsonl", '{"nested": {"accessToken": "redacted"}}\n'),
    ("prices.csv", "account_id,value\nredacted,1\n"),
])
def test_bundle_rejects_nested_sensitive_keys_without_echo(tmp_path: Path, name: str, body: str) -> None:
    root = _bundle(tmp_path)
    (root / name).write_text(body, encoding="utf-8")
    with pytest.raises(EvidenceValidationError) as error:
        validate_evidence_bundle(root)
    assert "sensitive_key" in str(error.value)
    assert "redacted" not in str(error.value)


@pytest.mark.parametrize("name,body,bad", [
    ("data_quality.json", '{"path": "https://example.test/?token=do-not-log"}', "do-not-log"),
    ("trial_ledger.jsonl", '{"auth": "Bearer do-not-log"}\n', "do-not-log"),
    ("prices.csv", "label,value\nhttps://example.test/?api_key=do-not-log,1\n", "do-not-log"),
])
def test_bundle_rejects_sensitive_values_without_echo(tmp_path: Path, name: str, body: str, bad: str) -> None:
    root = _bundle(tmp_path)
    (root / name).write_text(body, encoding="utf-8")
    with pytest.raises(EvidenceValidationError) as error:
        validate_evidence_bundle(root)
    assert "sensitive_value" in str(error.value)
    assert bad not in str(error.value)


def test_bundle_rejects_bounded_json_and_jsonl_lines(tmp_path: Path) -> None:
    root = _bundle(tmp_path)
    (root / "data_quality.json").write_bytes(b"{" + b"a" * (_evidence.MAX_JSON_BYTES + 1) + b"}")
    with pytest.raises(EvidenceValidationError, match="json_size"):
        validate_evidence_bundle(root)
    root = _bundle(tmp_path / "second")
    (root / "trial_ledger.jsonl").write_bytes(b"x" * (_evidence.MAX_TEXT_LINE_BYTES + 1))
    with pytest.raises(EvidenceValidationError, match="text_line_size"):
        validate_evidence_bundle(root)


@pytest.mark.parametrize("bad", [
    "0" * 64 + "  manifest.json\n",
    "0" * 64 + "  checksums.sha256\n",
    "bad checksum line\n",
])
def test_checksums_are_non_self_referential_and_strict(tmp_path: Path, bad: str) -> None:
    root = _bundle(tmp_path)
    (root / "checksums.sha256").write_text(bad, encoding="utf-8")
    with pytest.raises(EvidenceValidationError):
        validate_evidence_bundle(root)


def test_champion_identity_must_match_manifest(tmp_path: Path) -> None:
    root = _bundle(tmp_path)
    (root / "champion_identity.json").write_text('{"schema":"champion_identity.v1","profile":"tqqq_growth_income"}', encoding="utf-8")
    with pytest.raises(EvidenceValidationError, match="champion_identity"):
        validate_evidence_bundle(root)


@pytest.mark.parametrize("bad", [
    "/private/path", "C:\\private\\path", "\\\\server\\share", "a/../b",
    "https://user:pass@example.test", "eyJhbGciOiJub25lIn0.eyJzdWIiOiIxIn0.signature",
    "ghp_" + "x" * 20,
])
def test_bundle_rejects_private_and_credential_values(tmp_path: Path, bad: str) -> None:
    root = _bundle(tmp_path)
    (root / "data_quality.json").write_text(json.dumps({"value": bad}), encoding="utf-8")
    with pytest.raises(EvidenceValidationError, match="(?:private_path|sensitive_value)"):
        validate_evidence_bundle(root)


@pytest.mark.parametrize("mutation", [
    lambda lines: lines[1:],
    lambda lines: lines + [lines[0]],
    lambda lines: list(reversed(lines)),
])
def test_checksums_require_exact_sorted_set(tmp_path: Path, mutation) -> None:
    root = _bundle(tmp_path)
    lines = (root / "checksums.sha256").read_text().splitlines(keepends=True)
    (root / "checksums.sha256").write_text("".join(mutation(lines)), encoding="utf-8")
    with pytest.raises(EvidenceValidationError, match="checksum_set"):
        validate_evidence_bundle(root)


@pytest.mark.parametrize("name,body,rule", [
    ("data_quality.json", "[]", "json_object"),
    ("trial_ledger.jsonl", "[]\n", "jsonl_object"),
    ("prices.csv", "symbol,value\nSOXL\n", "csv_width"),
    ("prices.csv", "symbol,symbol\nSOXL,1\n", "csv_header"),
])
def test_text_artifacts_require_structural_records(tmp_path: Path, name: str, body: str, rule: str) -> None:
    root = _bundle(tmp_path)
    (root / name).write_text(body, encoding="utf-8")
    with pytest.raises(EvidenceValidationError, match=rule):
        validate_evidence_bundle(root)


@pytest.mark.parametrize("key,accepted", [("api_key=redacted", False), ("https://user:pass@example.test", False), ("session_date", True)])
def test_mapping_keys_use_string_rules(tmp_path: Path, key: str, accepted: bool) -> None:
    if accepted:
        _evidence._inspect({key: 1}, "data_quality.json")
    else:
        with pytest.raises(EvidenceValidationError, match="sensitive_value"):
            _evidence._inspect({key: 1}, "data_quality.json")


def test_bundle_root_requires_exact_regular_set(tmp_path: Path) -> None:
    for extra in ("secret.txt", "subdir"):
        path = (root := _bundle(tmp_path / extra)) / extra
        path.mkdir() if extra == "subdir" else path.write_text("secret", encoding="utf-8")
        with pytest.raises(EvidenceValidationError, match="bundle_root_entries"):
            validate_evidence_bundle(root)


def test_bundle_rejects_deep_json_and_jsonl(tmp_path: Path) -> None:
    value: object = 1
    for _ in range(_evidence.MAX_NESTING_DEPTH + 1):
        value = {"nested": value}
    for suffix, body in (("json", json.dumps(value)), ("jsonl", json.dumps(value) + "\n")):
        root, name = _bundle(tmp_path / suffix), "data_quality.json" if suffix == "json" else "trial_ledger.jsonl"
        (root / name).write_text(body, encoding="utf-8")
        with pytest.raises(EvidenceValidationError, match="nesting_depth"):
            validate_evidence_bundle(root)
