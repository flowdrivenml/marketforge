"""Construct shared HTTP clients from validated application settings."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import httpx

from forge.shared.config.models import HttpSettings
from forge.shared.http.client import HttpClient, open_http_client
from forge.shared.http.rate_limit import RequestRateLimiter
from forge.shared.http.retry import RetryPolicy


def resolve_http_settings(
    settings: HttpSettings,
    *,
    requests_per_second: float | None = None,
) -> HttpSettings:
    """Apply and validate an optional operation-specific rate override."""
    if requests_per_second is None:
        return settings

    return HttpSettings.model_validate(
        {
            **settings.model_dump(),
            "requests_per_second": requests_per_second,
        }
    )


@asynccontextmanager
async def open_configured_http_client(
    settings: HttpSettings,
    *,
    requests_per_second: float | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
) -> AsyncGenerator[HttpClient, None]:
    """Own one upstream client and its rate budget for an operation.

    Reuse the yielded client across all tasks targeting that upstream.
    """
    resolved = resolve_http_settings(
        settings,
        requests_per_second=requests_per_second,
    )

    async with open_http_client(
        limiter=RequestRateLimiter(resolved.requests_per_second),
        max_concurrent=resolved.max_concurrent,
        timeout_seconds=resolved.timeout_seconds,
        retry_policy=RetryPolicy(max_attempts=resolved.max_attempts),
        transport=transport,
    ) as client:
        yield client
