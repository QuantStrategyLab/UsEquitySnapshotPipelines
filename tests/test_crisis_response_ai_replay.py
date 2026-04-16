from __future__ import annotations

import json
from pathlib import Path

from us_equity_snapshot_pipelines.crisis_response_ai_replay import (
    STATUS_BLOCKED_MISSING_API_KEY,
    STATUS_COMPLETED,
    STATUS_DRY_RUN,
    STATUS_WAITING_FOR_MIN_DAYS,
    build_replay_gate,
    load_shadow_signals,
    main,
    run_crisis_response_ai_replay,
)


def _write_shadow_signal(root: Path, day: int, *, would_trade: bool = False) -> None:
    as_of = f"2026-01-{day:02d}"
    payload = {
        "as_of": as_of,
        "mode": "shadow",
        "schema_version": "crisis_response_shadow.v1",
        "canonical_route": "true_crisis" if would_trade else "no_action",
        "suggested_action": "defend" if would_trade else "no_action",
        "would_trade_if_enabled": would_trade,
        "execution_controls": {
            "capital_impact": "none",
            "broker_order_allowed": False,
            "live_allocation_mutation_allowed": False,
        },
    }
    signal_dir = root / "signals"
    signal_dir.mkdir(parents=True, exist_ok=True)
    (signal_dir / f"{as_of}.json").write_text(json.dumps(payload), encoding="utf-8")
    (root / "latest_signal.json").write_text(json.dumps(payload), encoding="utf-8")
    audit_dir = root / "audit"
    audit_dir.mkdir(parents=True, exist_ok=True)
    (audit_dir / f"{as_of}_evidence.csv").write_text(
        "as_of,canonical_route\n" f"{as_of},{payload['canonical_route']}\n"
    )


def _write_shadow_days(root: Path, count: int, *, would_trade_on_last: bool = False) -> None:
    for day in range(1, count + 1):
        _write_shadow_signal(root, day, would_trade=would_trade_on_last and day == count)


def test_replay_gate_waits_until_twenty_shadow_days() -> None:
    signals = [{"as_of": f"2026-01-{day:02d}", "would_trade_if_enabled": False} for day in range(1, 20)]

    gate = build_replay_gate(signals)

    assert gate["status"] == STATUS_WAITING_FOR_MIN_DAYS
    assert gate["shadow_signal_days"] == 19
    assert gate["ai_replay_allowed"] is False
    assert gate["advisory_auto_enable_allowed"] is False


def test_replay_gate_allows_ai_after_twenty_but_not_advisory() -> None:
    signals = [{"as_of": f"2026-01-{day:02d}", "would_trade_if_enabled": False} for day in range(1, 21)]

    gate = build_replay_gate(signals)

    assert gate["ai_replay_allowed"] is True
    assert gate["advisory_review_eligible"] is False
    assert gate["advisory_auto_enable_allowed"] is False


def test_replay_gate_flags_advisory_review_eligibility_without_auto_enable() -> None:
    signals = [{"as_of": f"2026-02-{day:02d}", "would_trade_if_enabled": False} for day in range(1, 61)]

    gate = build_replay_gate(signals)

    assert gate["ai_replay_allowed"] is True
    assert gate["advisory_review_eligible"] is True
    assert gate["advisory_auto_enable_allowed"] is False


def test_ai_replay_waits_without_calling_provider_before_min_days(tmp_path) -> None:
    shadow_dir = tmp_path / "shadow"
    _write_shadow_days(shadow_dir, 5)
    calls = []

    payload = run_crisis_response_ai_replay(
        shadow_dir=shadow_dir,
        provider="openai",
        env={"OPENAI_API_KEY": "test-key"},
        client=lambda **kwargs: calls.append(kwargs) or "review",
    )

    assert payload["status"] == STATUS_WAITING_FOR_MIN_DAYS
    assert calls == []
    assert payload["execution_controls"]["broker_order_allowed"] is False
    assert (shadow_dir / "audit" / "2026-01-05_ai_replay_request.md").exists()


def test_ai_replay_blocks_missing_key_after_min_days(tmp_path) -> None:
    shadow_dir = tmp_path / "shadow"
    _write_shadow_days(shadow_dir, 20)

    payload = run_crisis_response_ai_replay(shadow_dir=shadow_dir, provider="openai", env={})

    assert payload["status"] == STATUS_BLOCKED_MISSING_API_KEY
    assert payload["gate"]["ai_replay_allowed"] is True
    assert "Missing API key" in payload["review_text"]


def test_ai_replay_calls_provider_after_twenty_days_when_key_exists(tmp_path) -> None:
    shadow_dir = tmp_path / "shadow"
    _write_shadow_days(shadow_dir, 20, would_trade_on_last=True)
    calls = []

    payload = run_crisis_response_ai_replay(
        shadow_dir=shadow_dir,
        provider="openai",
        env={"OPENAI_API_KEY": "test-key", "OPENAI_MODEL": "test-model"},
        client=lambda **kwargs: calls.append(kwargs) or '{"recommended_next_state":"keep_shadow"}',
    )

    assert payload["status"] == STATUS_COMPLETED
    assert payload["model"] == "test-model"
    assert len(calls) == 1
    assert "Recent shadow signals" in calls[0]["prompt"]
    assert payload["gate"]["latest_would_trade_if_enabled"] is True
    assert (shadow_dir / "audit" / "2026-01-20_ai_review.json").exists()
    assert (shadow_dir / "audit" / "2026-01-20_ai_review.md").exists()


def test_ai_replay_dry_run_after_min_days_writes_prompt_without_key(tmp_path) -> None:
    shadow_dir = tmp_path / "shadow"
    _write_shadow_days(shadow_dir, 20)

    payload = run_crisis_response_ai_replay(shadow_dir=shadow_dir, provider="none", dry_run=True, env={})

    assert payload["status"] == STATUS_DRY_RUN
    assert payload["gate"]["ai_replay_allowed"] is True


def test_load_shadow_signals_dedupes_and_sorts(tmp_path) -> None:
    shadow_dir = tmp_path / "shadow"
    _write_shadow_signal(shadow_dir, 3)
    _write_shadow_signal(shadow_dir, 1)
    _write_shadow_signal(shadow_dir, 2)

    signals = load_shadow_signals(shadow_dir)

    assert [signal["as_of"] for signal in signals] == ["2026-01-01", "2026-01-02", "2026-01-03"]


def test_ai_replay_cli_dry_run(tmp_path) -> None:
    shadow_dir = tmp_path / "shadow"
    _write_shadow_days(shadow_dir, 20)

    exit_code = main(["--shadow-dir", str(shadow_dir), "--provider", "none", "--dry-run"])

    assert exit_code == 0
    assert (shadow_dir / "audit" / "2026-01-20_ai_review.json").exists()
