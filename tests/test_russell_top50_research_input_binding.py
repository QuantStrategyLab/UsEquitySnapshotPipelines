from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import pandas as pd
import pytest
from quant_platform_kit.strategy_contracts import StrategyContext
from us_equity_strategies.entrypoints import russell_top50_leader_rotation_entrypoint

from us_equity_snapshot_pipelines.russell_top50_research_input_binding import (
    CONTRACT_VERSION,
    ResearchInputBindingError,
    bind_russell_top50_research_input,
    materialize_russell_top50_research_input,
    verify_russell_top50_research_input,
)
import us_equity_snapshot_pipelines.russell_top50_research_input_binding as binding


def _prices() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=320)
    symbols = {"QQQ": (100.0, .0010), "SPY": (100.0, .0007), "BOXX": (100.0, .0002), "NVDA": (80.0, .0018), "MSFT": (110.0, .0012), "AAPL": (120.0, .0010), "META": (90.0, .0014), "AMZN": (95.0, .0011), "TSLA": (75.0, .0001)}
    return pd.DataFrame({"symbol": symbol, "as_of": date.date().isoformat(), "close": base * (1 + index * slope), "volume": 1_000_000} for index, date in enumerate(dates) for symbol, (base, slope) in symbols.items())


def _universe() -> pd.DataFrame:
    return pd.DataFrame([
        {"symbol": "NVDA", "sector": "Information Technology", "mega_rank": 1}, {"symbol": "MSFT", "sector": "Information Technology", "mega_rank": 2},
        {"symbol": "AAPL", "sector": "Information Technology", "mega_rank": 3}, {"symbol": "META", "sector": "Communication Services", "mega_rank": 4},
        {"symbol": "AMZN", "sector": "Consumer Discretionary", "mega_rank": 5}, {"symbol": "TSLA", "sector": "Consumer Discretionary", "mega_rank": 6},
    ])


def _materialize(tmp_path: Path, name: str = "package"):
    return materialize_russell_top50_research_input(
        _prices(), _universe(), tmp_path / name,
        producer_commit_sha="b5306a972724105273efebbd22834afde1294e4e",
        producer_tree_sha="9c5c6bf08b7dcd70a7ff091b606fe09e4ee04b5c",
        observed_at=datetime(2025, 3, 24, tzinfo=timezone.utc),
        as_of=datetime(2025, 3, 24, tzinfo=timezone.utc),
        source_identity="repository_owned_frozen_synthetic_fixture",
    )


def test_deterministic_package_and_detached_readback(tmp_path: Path) -> None:
    result = _materialize(tmp_path)
    second = _materialize(tmp_path, "package-second")
    names = sorted(path.relative_to(result.output_dir).as_posix() for path in result.output_dir.rglob("*") if path.is_file())
    assert names == ["inputs/prices.csv", "inputs/universe.csv", "research-input-manifest.json", "snapshot/feature_snapshot.csv", "snapshot/feature_snapshot.manifest.json", "snapshot/ranking.csv"]
    assert verify_russell_top50_research_input(result.output_dir, expected_manifest_sha256=result.manifest_sha256) == result
    manifest = json.loads((result.output_dir / "research-input-manifest.json").read_text())
    assert manifest["research_input_contract_id"] == CONTRACT_VERSION
    assert manifest["producer"]["commit_sha"] == "b5306a972724105273efebbd22834afde1294e4e"
    assert result.manifest_sha256 == second.manifest_sha256
    assert result.members == second.members


@pytest.mark.parametrize("target", ["inputs/prices.csv", "snapshot/feature_snapshot.manifest.json"])
def test_rejects_tampered_members(tmp_path: Path, target: str) -> None:
    result = _materialize(tmp_path)
    path = result.output_dir / target
    path.write_bytes(path.read_bytes() + b"x")
    with pytest.raises(ResearchInputBindingError):
        verify_russell_top50_research_input(result.output_dir, expected_manifest_sha256=result.manifest_sha256)


def test_rejects_extra_member_and_no_clobber(tmp_path: Path) -> None:
    result = _materialize(tmp_path)
    (result.output_dir / "extra.csv").write_text("x")
    with pytest.raises(ResearchInputBindingError):
        verify_russell_top50_research_input(result.output_dir, expected_manifest_sha256=result.manifest_sha256)
    with pytest.raises(ResearchInputBindingError):
        _materialize(tmp_path)


@pytest.mark.parametrize("target", ["inputs/prices.csv", "snapshot/feature_snapshot.csv"])
def test_rejects_nonregular_or_oversized_members(tmp_path: Path, target: str) -> None:
    result = _materialize(tmp_path)
    path = result.output_dir / target
    path.unlink()
    if target.startswith("inputs"):
        path.symlink_to(result.output_dir / "inputs/universe.csv")
    else:
        path.mkdir()
    with pytest.raises(ResearchInputBindingError):
        verify_russell_top50_research_input(result.output_dir, expected_manifest_sha256=result.manifest_sha256)

    oversized = _materialize(tmp_path, "oversized")
    with (oversized.output_dir / "snapshot/ranking.csv").open("ab") as handle:
        handle.write(b"x" * (2 * 1024 * 1024))
    with pytest.raises(ResearchInputBindingError):
        verify_russell_top50_research_input(oversized.output_dir, expected_manifest_sha256=oversized.manifest_sha256)


def test_authenticates_outer_digest_before_parsing(tmp_path: Path, monkeypatch) -> None:
    result = _materialize(tmp_path)
    (result.output_dir / "research-input-manifest.json").write_bytes(b"not json")
    monkeypatch.setattr(binding, "read_research_input_manifest_json", lambda _: pytest.fail("parsed before digest"))
    with pytest.raises(ResearchInputBindingError):
        verify_russell_top50_research_input(result.output_dir, expected_manifest_sha256=result.manifest_sha256)


def test_rejects_untrusted_source_identity(tmp_path: Path) -> None:
    with pytest.raises(ResearchInputBindingError):
        materialize_russell_top50_research_input(_prices(), _universe(), tmp_path / "package", producer_commit_sha="b5306a972724105273efebbd22834afde1294e4e", producer_tree_sha="9c5c6bf08b7dcd70a7ff091b606fe09e4ee04b5c", observed_at=datetime(2025, 3, 24, tzinfo=timezone.utc), as_of=datetime(2025, 3, 24, tzinfo=timezone.utc), source_identity="provider_download")


def test_rejects_source_identity_str_subclass_without_comparison_hook(tmp_path: Path) -> None:
    calls: list[str] = []

    class ForgedIdentity(str):
        def __ne__(self, other: object) -> bool:
            calls.append("ne")
            return False

    with pytest.raises(ResearchInputBindingError):
        materialize_russell_top50_research_input(
            _prices(), _universe(), tmp_path / "package",
            producer_commit_sha="b5306a972724105273efebbd22834afde1294e4e",
            producer_tree_sha="9c5c6bf08b7dcd70a7ff091b606fe09e4ee04b5c",
            observed_at=datetime(2025, 3, 24, tzinfo=timezone.utc),
            as_of=datetime(2025, 3, 24, tzinfo=timezone.utc),
            source_identity=ForgedIdentity("provider_download"),
        )
    assert calls == []


def test_rejects_digest_str_subclass_before_manifest_authentication(tmp_path: Path, monkeypatch) -> None:
    result = _materialize(tmp_path)
    calls: list[str] = []

    class ForgedDigest(str):
        def __eq__(self, other: object) -> bool:
            calls.append("eq")
            return True

    monkeypatch.setattr(binding, "_read_package", lambda *_: pytest.fail("manifest authentication ran"))
    with pytest.raises(ResearchInputBindingError):
        verify_russell_top50_research_input(result.output_dir, expected_manifest_sha256=ForgedDigest("0" * 64))
    assert calls == []


def test_bound_entrypoint_matches_direct_call(tmp_path: Path) -> None:
    result = _materialize(tmp_path)
    context = StrategyContext(as_of="2025-03-24")
    snapshot = pd.read_csv(result.output_dir / "snapshot/feature_snapshot.csv")
    direct = russell_top50_leader_rotation_entrypoint.evaluate(
        StrategyContext(as_of=context.as_of, market_data={"feature_snapshot": snapshot})
    )
    bound = bind_russell_top50_research_input(result.output_dir, expected_manifest_sha256=result.manifest_sha256, context=context)
    assert bound == direct
    with pytest.raises(ResearchInputBindingError):
        bind_russell_top50_research_input(
            result.output_dir,
            expected_manifest_sha256=result.manifest_sha256,
            context=StrategyContext(as_of=context.as_of, capabilities={"broker": object()}),
        )


def test_module_has_no_provider_or_plugin_imports() -> None:
    source = Path(__file__).parents[1] / "src/us_equity_snapshot_pipelines/russell_top50_research_input_binding.py"
    assert not any(word in source.read_text() for word in ("requests", "yfinance", "google.cloud", "plugin", "broker"))


def test_rejects_modified_fixture_and_arbitrary_producer(tmp_path: Path) -> None:
    altered = _prices()
    altered.loc[0, "close"] += 1.0
    kwargs = dict(
        producer_commit_sha="b5306a972724105273efebbd22834afde1294e4e",
        producer_tree_sha="9c5c6bf08b7dcd70a7ff091b606fe09e4ee04b5c",
        observed_at=datetime(2025, 3, 24, tzinfo=timezone.utc),
        as_of=datetime(2025, 3, 24, tzinfo=timezone.utc),
        source_identity="repository_owned_frozen_synthetic_fixture",
    )
    with pytest.raises(ResearchInputBindingError):
        materialize_russell_top50_research_input(altered, _universe(), tmp_path / "altered", **kwargs)
    with pytest.raises(ResearchInputBindingError):
        materialize_russell_top50_research_input(_prices(), _universe(), tmp_path / "producer", producer_commit_sha="a" * 40, **{key: value for key, value in kwargs.items() if key != "producer_commit_sha"})

    result = _materialize(tmp_path, "repacked")
    prices_path = result.output_dir / "inputs/prices.csv"
    prices_path.write_bytes(prices_path.read_bytes().replace(b"100.0", b"101.0", 1))
    members = {name: (result.output_dir / name).read_bytes() for name in binding._MEMBERS}
    manifest = binding._outer_manifest(
        members,
        producer_commit_sha="b5306a972724105273efebbd22834afde1294e4e",
        producer_tree_sha="9c5c6bf08b7dcd70a7ff091b606fe09e4ee04b5c",
        observed_at="2025-03-24T00:00:00Z",
        as_of="2025-03-24T00:00:00Z",
    )
    digest = binding.research_input_manifest_sha256(manifest)
    (result.output_dir / "research-input-manifest.json").write_bytes(
        binding.canonical_research_input_manifest_bytes(manifest)
    )
    with pytest.raises(ResearchInputBindingError):
        verify_russell_top50_research_input(result.output_dir, expected_manifest_sha256=digest)


def test_bind_rejects_caller_controlled_context_and_reads_once(tmp_path: Path, monkeypatch) -> None:
    result = _materialize(tmp_path)
    called: list[str] = []
    context = StrategyContext(as_of="2025-03-24", runtime_config={"translator": lambda text: called.append(text)})
    with pytest.raises(ResearchInputBindingError):
        bind_russell_top50_research_input(result.output_dir, expected_manifest_sha256=result.manifest_sha256, context=context)
    assert not called

    reads = 0
    original = binding._read_package

    def count_reads(*args, **kwargs):
        nonlocal reads
        reads += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(binding, "_read_package", count_reads)
    bind_russell_top50_research_input(
        result.output_dir,
        expected_manifest_sha256=result.manifest_sha256,
        context=StrategyContext(as_of="2025-03-24"),
    )
    assert reads == 1


def test_requires_explicit_frozen_source_identity(tmp_path: Path) -> None:
    with pytest.raises(ResearchInputBindingError):
        materialize_russell_top50_research_input(
            _prices(), _universe(), tmp_path / "package",
            producer_commit_sha="b5306a972724105273efebbd22834afde1294e4e",
            producer_tree_sha="9c5c6bf08b7dcd70a7ff091b606fe09e4ee04b5c",
            observed_at=datetime(2025, 3, 24, tzinfo=timezone.utc),
            as_of=datetime(2025, 3, 24, tzinfo=timezone.utc),
        )


def test_binding_never_evaluates_caller_controlled_values(tmp_path: Path) -> None:
    result = _materialize(tmp_path)
    calls: list[str] = []

    class Explosive:
        def __bool__(self) -> bool:
            calls.append("bool")
            return False

    context = StrategyContext(as_of="2025-03-24", portfolio=Explosive())
    with pytest.raises(ResearchInputBindingError):
        bind_russell_top50_research_input(result.output_dir, expected_manifest_sha256=result.manifest_sha256, context=context)
    assert calls == []


def test_normalizes_missing_parent_and_no_clobber_race(tmp_path: Path, monkeypatch) -> None:
    kwargs = dict(
        producer_commit_sha="b5306a972724105273efebbd22834afde1294e4e",
        producer_tree_sha="9c5c6bf08b7dcd70a7ff091b606fe09e4ee04b5c",
        observed_at=datetime(2025, 3, 24, tzinfo=timezone.utc), as_of=datetime(2025, 3, 24, tzinfo=timezone.utc),
        source_identity="repository_owned_frozen_synthetic_fixture",
    )
    with pytest.raises(ResearchInputBindingError) as missing_parent:
        materialize_russell_top50_research_input(_prices(), _universe(), tmp_path / "missing" / "package", **kwargs)
    assert "missing" not in str(missing_parent.value)

    destination = tmp_path / "race"
    publish = binding._publish_noreplace

    def race(source: Path, target: Path) -> None:
        target.mkdir()
        publish(source, target)

    monkeypatch.setattr(binding, "_publish_noreplace", race)
    with pytest.raises(ResearchInputBindingError):
        materialize_russell_top50_research_input(_prices(), _universe(), destination, **kwargs)
    assert destination.is_dir()


def test_rejects_missing_member_change_during_read_and_resealed_inner_manifest(tmp_path: Path, monkeypatch) -> None:
    result = _materialize(tmp_path)
    (result.output_dir / "snapshot" / "ranking.csv").unlink()
    with pytest.raises(ResearchInputBindingError):
        verify_russell_top50_research_input(result.output_dir, expected_manifest_sha256=result.manifest_sha256)

    result = _materialize(tmp_path, "changed")
    read = binding.os.read
    changed = False

    def race_read(fd: int, count: int) -> bytes:
        nonlocal changed
        raw = read(fd, count)
        if not changed:
            changed = True
            (result.output_dir / "research-input-manifest.json").write_bytes(raw + b" ")
        return raw

    monkeypatch.setattr(binding.os, "read", race_read)
    with pytest.raises(ResearchInputBindingError):
        verify_russell_top50_research_input(result.output_dir, expected_manifest_sha256=result.manifest_sha256)

    monkeypatch.setattr(binding.os, "read", read)
    result = _materialize(tmp_path, "resealed")
    inner = result.output_dir / "snapshot" / "feature_snapshot.manifest.json"
    inner.write_bytes(inner.read_bytes().replace(b'"row_count": 9', b'"row_count": 8'))
    members = {name: (result.output_dir / name).read_bytes() for name in binding._MEMBERS}
    manifest = binding._outer_manifest(members, producer_commit_sha="b5306a972724105273efebbd22834afde1294e4e", producer_tree_sha="9c5c6bf08b7dcd70a7ff091b606fe09e4ee04b5c", observed_at="2025-03-24T00:00:00Z", as_of="2025-03-24T00:00:00Z")
    digest = binding.research_input_manifest_sha256(manifest)
    (result.output_dir / "research-input-manifest.json").write_bytes(binding.canonical_research_input_manifest_bytes(manifest))
    with pytest.raises(ResearchInputBindingError):
        verify_russell_top50_research_input(result.output_dir, expected_manifest_sha256=digest)
