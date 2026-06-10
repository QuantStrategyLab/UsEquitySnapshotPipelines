from __future__ import annotations

import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "post_monthly_ai_review_issue.py"


def _load_script_module():
    spec = importlib.util.spec_from_file_location("post_monthly_ai_review_issue", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_github_request_uses_default_timeout(monkeypatch) -> None:
    module = _load_script_module()
    calls: list[tuple[str, str, float]] = []

    class FakeHeaders:
        def get_content_charset(self, default: str) -> str:
            return default

    class FakeResponse:
        headers = FakeHeaders()

        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def read(self) -> bytes:
            return b'{"ok": true}'

    def fake_urlopen(request, *, timeout: float):
        calls.append((request.get_method(), request.full_url, timeout))
        return FakeResponse()

    monkeypatch.setattr(module.urllib.request, "urlopen", fake_urlopen)

    result = module.github_request("GET", "https://api.github.com/repos/example/repo/issues", "token")

    assert result == {"ok": True}
    assert calls == [
        ("GET", "https://api.github.com/repos/example/repo/issues", module.DEFAULT_TIMEOUT_SECONDS),
    ]
