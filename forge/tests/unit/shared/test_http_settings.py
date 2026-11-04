"""HTTP configuration loading and validated operation overrides."""

from pathlib import Path

import httpx
import pytest
from pydantic import ValidationError

from forge.shared.config.loader import load_settings
from forge.shared.config.models import AppSettings, HttpSettings
from forge.shared.http.configured import (
    open_configured_http_client,
    resolve_http_settings,
)


def test_loads_http_settings_from_toml(tmp_path: Path) -> None:
    source = tmp_path / "marketforge.toml"
    source.write_text(
        """
version = 1

[http]
requests_per_second = 1.5
max_concurrent = 2
timeout_seconds = 15.0
max_attempts = 2
""",
        encoding="utf-8",
    )

    settings = load_settings(source)

    assert settings.http.requests_per_second == 1.5
    assert settings.http.max_concurrent == 2
    assert settings.http.timeout_seconds == 15.0
    assert settings.http.max_attempts == 2


def test_existing_configuration_uses_http_defaults() -> None:
    settings = AppSettings()

    assert settings.http == HttpSettings()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("requests_per_second", 0.0),
        ("requests_per_second", float("inf")),
        ("max_concurrent", 0),
        ("max_concurrent", True),
        ("timeout_seconds", -1.0),
        ("max_attempts", 11),
        ("unknown_option", True),
    ],
)
def test_rejects_invalid_http_settings(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        HttpSettings.model_validate({field: value})


def test_rate_override_preserves_original_settings() -> None:
    original = HttpSettings()

    resolved = resolve_http_settings(
        original,
        requests_per_second=0.5,
    )

    assert resolved.requests_per_second == 0.5
    assert original.requests_per_second == 2.0
    assert resolved.max_concurrent == original.max_concurrent


def test_rejects_invalid_rate_override() -> None:
    with pytest.raises(ValidationError):
        resolve_http_settings(
            HttpSettings(),
            requests_per_second=0.0,
        )


@pytest.mark.asyncio
async def test_configured_client_uses_attempt_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0
    delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr("forge.shared.http.client.sleep", fake_sleep)

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(503)

    settings = HttpSettings(max_attempts=1)

    async with open_configured_http_client(
        settings,
        transport=httpx.MockTransport(handler),
    ) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await client.get("https://exchange.test/instruments")

    assert attempts == 1
    assert delays == []
