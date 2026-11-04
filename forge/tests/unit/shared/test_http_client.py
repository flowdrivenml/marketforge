"""Tests for shared async HTTP behavior without external requests."""

import asyncio

import httpx
import pytest

from forge.shared.http.client import open_http_client
from forge.shared.http.rate_limit import RequestRateLimiter


class CountingLimiter(RequestRateLimiter):
    """Record admissions without introducing real delays."""

    def __init__(self) -> None:
        super().__init__(requests_per_second=1.0)
        self.admissions = 0

    async def wait(self) -> None:
        self.admissions += 1


@pytest.mark.asyncio
async def test_passes_request_parameters_and_uses_limiter() -> None:
    limiter = CountingLimiter()
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"available": True})

    async with open_http_client(
        limiter=limiter,
        transport=httpx.MockTransport(handler),
    ) as client:
        response = await client.get(
            "https://exchange.test/instruments",
            params={"symbol": "BTCUSDT"},
            headers={"X-Test": "marketforge"},
        )

    assert response.status_code == 200
    assert len(requests) == 1
    assert requests[0].url.params["symbol"] == "BTCUSDT"
    assert requests[0].headers["X-Test"] == "marketforge"
    assert limiter.admissions == 1


@pytest.mark.asyncio
async def test_limits_concurrent_requests() -> None:
    limiter = CountingLimiter()
    active = 0
    peak = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)

        try:
            await asyncio.sleep(0)
            return httpx.Response(200)
        finally:
            active -= 1

    async with open_http_client(
        limiter=limiter,
        max_concurrent=2,
        transport=httpx.MockTransport(handler),
    ) as client:
        responses = await asyncio.gather(
            *(client.get(f"https://exchange.test/{index}") for index in range(6))
        )

    assert len(responses) == 6
    assert peak == 2
    assert active == 0
    assert limiter.admissions == 6


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [302, 404, 429, 503])
async def test_rejects_unsuccessful_status_without_retrying(
    status_code: int,
) -> None:
    limiter = CountingLimiter()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code,
            headers={"Location": "https://other.test/archive"},
        )

    async with open_http_client(
        limiter=limiter,
        transport=httpx.MockTransport(handler),
    ) as client:
        with pytest.raises(httpx.HTTPStatusError) as exc:
            await client.get("https://exchange.test/archive")

    assert exc.value.response.status_code == status_code
    assert limiter.admissions == 1


@pytest.mark.asyncio
async def test_propagates_transport_timeout() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("Simulated timeout.", request=request)

    async with open_http_client(
        limiter=CountingLimiter(),
        transport=httpx.MockTransport(handler),
    ) as client:
        with pytest.raises(httpx.ReadTimeout):
            await client.get("https://exchange.test/instruments")


@pytest.mark.asyncio
async def test_cancellation_releases_concurrency_slot() -> None:
    started = asyncio.Event()
    block_first_request = True

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal block_first_request

        if block_first_request:
            block_first_request = False
            started.set()
            await asyncio.Event().wait()

        return httpx.Response(200)

    async with open_http_client(
        limiter=CountingLimiter(),
        max_concurrent=1,
        transport=httpx.MockTransport(handler),
    ) as client:
        task = asyncio.create_task(client.get("https://exchange.test/first"))

        try:
            await asyncio.wait_for(started.wait(), timeout=1.0)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        response = await asyncio.wait_for(
            client.get("https://exchange.test/second"),
            timeout=1.0,
        )

    assert response.status_code == 200
