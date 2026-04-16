from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import pandas as pd

from .artifacts import write_json

DEFAULT_SHADOW_DIR = "data/output/crisis_response_shadow"
DEFAULT_LOOKBACK_SIGNALS = 20
DEFAULT_MIN_AI_REPLAY_SHADOW_DAYS = 20
DEFAULT_ADVISORY_REVIEW_SHADOW_DAYS = 60
DEFAULT_MAX_OUTPUT_TOKENS = 3000
DEFAULT_OPENAI_MODEL = "gpt-5.4-mini"
DEFAULT_ANTHROPIC_MODEL = "claude-sonnet-4-6"
PROVIDER_OPENAI = "openai"
PROVIDER_ANTHROPIC = "anthropic"
PROVIDER_NONE = "none"
PROVIDERS = (PROVIDER_OPENAI, PROVIDER_ANTHROPIC, PROVIDER_NONE)

STATUS_WAITING_FOR_MIN_DAYS = "waiting_for_min_shadow_days"
STATUS_BLOCKED_MISSING_API_KEY = "blocked_missing_api_key"
STATUS_DRY_RUN = "dry_run"
STATUS_COMPLETED = "completed"
STATUS_NO_SIGNALS = "no_shadow_signals"


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _signal_sort_key(payload: Mapping[str, Any]) -> pd.Timestamp:
    return pd.Timestamp(payload.get("as_of", "1900-01-01")).normalize()


def load_shadow_signals(shadow_dir: str | Path) -> list[dict[str, Any]]:
    root = Path(shadow_dir)
    signal_dir = root / "signals"
    signals: list[dict[str, Any]] = []
    if signal_dir.exists():
        for path in sorted(signal_dir.glob("*.json")):
            try:
                payload = _load_json(path)
            except json.JSONDecodeError:
                continue
            if payload.get("mode") == "shadow" and payload.get("as_of"):
                signals.append(payload)
    if not signals:
        latest_path = root / "latest_signal.json"
        if latest_path.exists():
            latest = _load_json(latest_path)
            if latest.get("mode") == "shadow" and latest.get("as_of"):
                signals.append(latest)
    deduped = {str(signal["as_of"]): signal for signal in signals}
    return sorted(deduped.values(), key=_signal_sort_key)


def _latest_signal_date(signals: Sequence[Mapping[str, Any]]) -> str:
    if not signals:
        return datetime.now(timezone.utc).date().isoformat()
    return str(signals[-1]["as_of"])


def build_replay_gate(
    signals: Sequence[Mapping[str, Any]],
    *,
    min_ai_replay_shadow_days: int = DEFAULT_MIN_AI_REPLAY_SHADOW_DAYS,
    advisory_review_shadow_days: int = DEFAULT_ADVISORY_REVIEW_SHADOW_DAYS,
) -> dict[str, Any]:
    signal_count = len(signals)
    latest = signals[-1] if signals else {}
    would_trade_count = sum(bool(signal.get("would_trade_if_enabled", False)) for signal in signals)
    ai_replay_allowed = signal_count >= int(min_ai_replay_shadow_days)
    advisory_review_eligible = signal_count >= int(advisory_review_shadow_days)

    if not signals:
        status = STATUS_NO_SIGNALS
        reason = "No crisis_response_shadow signal files are available yet."
    elif ai_replay_allowed:
        status = "ready_for_ai_replay"
        reason = f"{signal_count} shadow signal days are available."
    else:
        status = STATUS_WAITING_FOR_MIN_DAYS
        reason = (
            f"{signal_count} shadow signal days are available; "
            f"{int(min_ai_replay_shadow_days)} are required before automatic AI replay."
        )

    return {
        "status": status,
        "reason": reason,
        "shadow_signal_days": signal_count,
        "min_ai_replay_shadow_days": int(min_ai_replay_shadow_days),
        "ai_replay_allowed": ai_replay_allowed,
        "advisory_review_shadow_days": int(advisory_review_shadow_days),
        "advisory_review_eligible": advisory_review_eligible,
        "advisory_auto_enable_allowed": False,
        "latest_as_of": latest.get("as_of"),
        "latest_would_trade_if_enabled": bool(latest.get("would_trade_if_enabled", False)),
        "would_trade_if_enabled_days": int(would_trade_count),
    }


def _read_evidence_text(shadow_dir: Path, as_of: str) -> str:
    path = shadow_dir / "audit" / f"{as_of}_evidence.csv"
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def build_ai_replay_prompt(
    signals: Sequence[Mapping[str, Any]],
    *,
    gate: Mapping[str, Any],
    evidence_text: str = "",
    lookback_signals: int = DEFAULT_LOOKBACK_SIGNALS,
) -> str:
    recent_signals = list(signals)[-int(lookback_signals) :]
    latest = recent_signals[-1] if recent_signals else {}
    latest_json = json.dumps(_json_safe(latest), ensure_ascii=False, indent=2, sort_keys=True)
    recent_json = json.dumps(_json_safe(recent_signals), ensure_ascii=False, indent=2, sort_keys=True)
    gate_json = json.dumps(_json_safe(gate), ensure_ascii=False, indent=2, sort_keys=True)

    sections = [
        "# Crisis Response Shadow AI Review",
        "",
        "You are auditing a shadow-only crisis response signal. Do not approve live trading, "
        "do not place orders, and do not change allocations.",
        "",
        "Gate state:",
        "",
        "```json",
        gate_json,
        "```",
        "",
        "Latest shadow signal:",
        "",
        "```json",
        latest_json,
        "```",
        "",
        f"Recent shadow signals, capped at {int(lookback_signals)}:",
        "",
        "```json",
        recent_json,
        "```",
        "",
    ]
    if evidence_text:
        sections.extend(
            [
                "Latest evidence CSV:",
                "",
                "```csv",
                evidence_text.strip(),
                "```",
                "",
            ]
        )
    sections.extend(
        [
            "Answer in concise JSON-compatible prose with these fields:",
            "",
            "- route_assessment",
            "- evidence_quality",
            "- false_positive_true_crisis_risk",
            "- false_negative_true_crisis_risk",
            "- historical_analogue",
            "- recommended_next_state",
            "- reviewer_notes",
            "",
            "The only allowed next states are `keep_shadow`, `human_review_advisory_candidate`, or `blocked`.",
            "Even when advisory_review_eligible is true, live or advisory enablement still requires explicit "
            "human approval.",
            "",
        ]
    )
    return "\n".join(sections)


def _provider_api_key(provider: str, env: Mapping[str, str] | None = None) -> str | None:
    env = env or os.environ
    if provider == PROVIDER_OPENAI:
        return env.get("OPENAI_API_KEY")
    if provider == PROVIDER_ANTHROPIC:
        return env.get("ANTHROPIC_API_KEY")
    return None


def _default_model(provider: str, env: Mapping[str, str] | None = None) -> str:
    env = env or os.environ
    if provider == PROVIDER_OPENAI:
        return env.get("OPENAI_MODEL", DEFAULT_OPENAI_MODEL)
    if provider == PROVIDER_ANTHROPIC:
        return env.get("ANTHROPIC_MODEL", DEFAULT_ANTHROPIC_MODEL)
    return "none"


def call_openai_responses_api(
    *,
    prompt: str,
    api_key: str,
    model: str,
    max_output_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS,
    timeout_seconds: int = 120,
) -> str:
    payload = json.dumps(
        {
            "model": model,
            "input": prompt,
            "max_output_tokens": int(max_output_tokens),
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        "https://api.openai.com/v1/responses",
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=int(timeout_seconds)) as response:
            data = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenAI API request failed: {exc.code} {detail}") from exc
    if "output_text" in data:
        return str(data["output_text"])
    chunks: list[str] = []
    for item in data.get("output", []):
        for content in item.get("content", []):
            if content.get("type") in {"output_text", "text"} and content.get("text"):
                chunks.append(str(content["text"]))
    return "\n".join(chunks).strip()


def call_anthropic_messages_api(
    *,
    prompt: str,
    api_key: str,
    model: str,
    max_output_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS,
    timeout_seconds: int = 120,
) -> str:
    payload = json.dumps(
        {
            "model": model,
            "max_tokens": int(max_output_tokens),
            "messages": [{"role": "user", "content": prompt}],
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=payload,
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=int(timeout_seconds)) as response:
            data = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Anthropic API request failed: {exc.code} {detail}") from exc
    chunks = [str(item.get("text", "")) for item in data.get("content", []) if item.get("type") == "text"]
    return "\n".join(chunk for chunk in chunks if chunk).strip()


ReplayClient = Callable[..., str]


def _call_provider(
    *,
    provider: str,
    prompt: str,
    api_key: str,
    model: str,
    max_output_tokens: int,
    client: ReplayClient | None = None,
) -> str:
    if client is not None:
        return client(
            prompt=prompt,
            api_key=api_key,
            model=model,
            max_output_tokens=max_output_tokens,
        )
    if provider == PROVIDER_OPENAI:
        return call_openai_responses_api(
            prompt=prompt,
            api_key=api_key,
            model=model,
            max_output_tokens=max_output_tokens,
        )
    if provider == PROVIDER_ANTHROPIC:
        return call_anthropic_messages_api(
            prompt=prompt,
            api_key=api_key,
            model=model,
            max_output_tokens=max_output_tokens,
        )
    raise ValueError(f"Unsupported provider for API call: {provider!r}")


def run_crisis_response_ai_replay(
    *,
    shadow_dir: str | Path = DEFAULT_SHADOW_DIR,
    output_dir: str | Path | None = None,
    provider: str = PROVIDER_OPENAI,
    model: str | None = None,
    dry_run: bool = False,
    lookback_signals: int = DEFAULT_LOOKBACK_SIGNALS,
    min_ai_replay_shadow_days: int = DEFAULT_MIN_AI_REPLAY_SHADOW_DAYS,
    advisory_review_shadow_days: int = DEFAULT_ADVISORY_REVIEW_SHADOW_DAYS,
    max_output_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS,
    env: Mapping[str, str] | None = None,
    client: ReplayClient | None = None,
) -> dict[str, Any]:
    provider = str(provider).strip().lower()
    if provider not in PROVIDERS:
        raise ValueError(f"Unsupported provider: {provider!r}")

    shadow_root = Path(shadow_dir)
    output_root = Path(output_dir) if output_dir is not None else shadow_root
    audit_dir = output_root / "audit"
    audit_dir.mkdir(parents=True, exist_ok=True)

    signals = load_shadow_signals(shadow_root)
    as_of = _latest_signal_date(signals)
    gate = build_replay_gate(
        signals,
        min_ai_replay_shadow_days=int(min_ai_replay_shadow_days),
        advisory_review_shadow_days=int(advisory_review_shadow_days),
    )
    evidence_text = _read_evidence_text(shadow_root, as_of)
    prompt = build_ai_replay_prompt(
        signals,
        gate=gate,
        evidence_text=evidence_text,
        lookback_signals=int(lookback_signals),
    )

    prompt_path = audit_dir / f"{as_of}_ai_replay_request.md"
    review_json_path = audit_dir / f"{as_of}_ai_review.json"
    review_md_path = audit_dir / f"{as_of}_ai_review.md"
    gate_path = audit_dir / f"{as_of}_ai_replay_gate.json"
    prompt_path.write_text(prompt, encoding="utf-8")
    write_json(gate_path, gate)

    api_key = _provider_api_key(provider, env)
    resolved_model = model or _default_model(provider, env)
    status = gate["status"]
    review_text = ""
    error = ""
    if not gate["ai_replay_allowed"]:
        status = gate["status"]
    elif dry_run or provider == PROVIDER_NONE:
        status = STATUS_DRY_RUN
        review_text = "Dry run only. Prompt was written; no AI provider was called."
    elif not api_key:
        status = STATUS_BLOCKED_MISSING_API_KEY
        review_text = f"Missing API key for provider={provider}; no AI provider was called."
    else:
        try:
            review_text = _call_provider(
                provider=provider,
                prompt=prompt,
                api_key=api_key,
                model=resolved_model,
                max_output_tokens=int(max_output_tokens),
                client=client,
            )
            status = STATUS_COMPLETED
        except Exception as exc:
            status = "api_error"
            error = str(exc)
            review_text = f"AI replay failed: {error}"

    payload = _json_safe(
        {
            "as_of": as_of,
            "status": status,
            "provider": provider,
            "model": resolved_model,
            "shadow_dir": str(shadow_root),
            "prompt_path": str(prompt_path),
            "review_md_path": str(review_md_path),
            "gate": gate,
            "review_text": review_text,
            "error": error,
            "execution_controls": {
                "capital_impact": "none",
                "broker_order_allowed": False,
                "live_allocation_mutation_allowed": False,
                "advisory_auto_enable_allowed": False,
            },
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    write_json(review_json_path, payload)
    review_md_path.write_text(_format_review_markdown(payload), encoding="utf-8")
    return payload


def _format_review_markdown(payload: Mapping[str, Any]) -> str:
    gate = payload.get("gate", {})
    lines = [
        "# Crisis Response AI Review",
        "",
        f"- as_of: {payload.get('as_of')}",
        f"- status: {payload.get('status')}",
        f"- provider: {payload.get('provider')}",
        f"- model: {payload.get('model')}",
        f"- shadow_signal_days: {gate.get('shadow_signal_days') if isinstance(gate, Mapping) else ''}",
        f"- ai_replay_allowed: {gate.get('ai_replay_allowed') if isinstance(gate, Mapping) else ''}",
        f"- advisory_review_eligible: {gate.get('advisory_review_eligible') if isinstance(gate, Mapping) else ''}",
        "- advisory_auto_enable_allowed: "
        f"{gate.get('advisory_auto_enable_allowed') if isinstance(gate, Mapping) else False}",
        "",
        "## Review",
        "",
        str(payload.get("review_text", "")).strip(),
        "",
    ]
    if payload.get("error"):
        lines.extend(["## Error", "", str(payload["error"]), ""])
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run gated AI replay over crisis response shadow logs.")
    parser.add_argument("--shadow-dir", default=DEFAULT_SHADOW_DIR)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--provider", choices=PROVIDERS, default=PROVIDER_OPENAI)
    parser.add_argument("--model", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--lookback-signals", type=int, default=DEFAULT_LOOKBACK_SIGNALS)
    parser.add_argument("--min-ai-replay-shadow-days", type=int, default=DEFAULT_MIN_AI_REPLAY_SHADOW_DAYS)
    parser.add_argument("--advisory-review-shadow-days", type=int, default=DEFAULT_ADVISORY_REVIEW_SHADOW_DAYS)
    parser.add_argument("--max-output-tokens", type=int, default=DEFAULT_MAX_OUTPUT_TOKENS)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_crisis_response_ai_replay(
        shadow_dir=args.shadow_dir,
        output_dir=args.output_dir,
        provider=args.provider,
        model=args.model,
        dry_run=bool(args.dry_run),
        lookback_signals=int(args.lookback_signals),
        min_ai_replay_shadow_days=int(args.min_ai_replay_shadow_days),
        advisory_review_shadow_days=int(args.advisory_review_shadow_days),
        max_output_tokens=int(args.max_output_tokens),
    )
    print(
        "wrote crisis response AI replay "
        f"{payload['as_of']} status={payload['status']} -> {payload['review_md_path']}"
    )
    return 0


__all__ = [
    "DEFAULT_ADVISORY_REVIEW_SHADOW_DAYS",
    "DEFAULT_MIN_AI_REPLAY_SHADOW_DAYS",
    "PROVIDER_ANTHROPIC",
    "PROVIDER_NONE",
    "PROVIDER_OPENAI",
    "STATUS_BLOCKED_MISSING_API_KEY",
    "STATUS_COMPLETED",
    "STATUS_DRY_RUN",
    "STATUS_WAITING_FOR_MIN_DAYS",
    "build_ai_replay_prompt",
    "build_replay_gate",
    "load_shadow_signals",
    "run_crisis_response_ai_replay",
]
