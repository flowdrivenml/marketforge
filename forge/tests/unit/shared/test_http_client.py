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


@pytest.mark.asyncio
async def test_head_redirects_consume_rate_admissions() -> None:
    limiter = CountingLimiter()
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.host == "exchange.test":
            return httpx.Response(302, headers={"Location": "https://cdn.test/archive"})
        return httpx.Response(404)

    async with open_http_client(limiter=limiter, transport=httpx.MockTransport(handler)) as client:
        response = await client.head(
            "https://exchange.test/archive", allowed_statuses=frozenset({404})
        )
    assert response.status_code == 404
    assert [item.method for item in calls] == ["HEAD", "HEAD"]
    assert limiter.admissions == 2


@pytest.mark.asyncio
async def test_stream_redirect_does_not_forward_credentials() -> None:
    from forge.shared.http.client import HttpClient

    limiter = CountingLimiter()
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(302, headers={"Location": "https://cdn.test/archive"})
        assert "authorization" not in request.headers
        assert "cookie" not in request.headers
        return httpx.Response(200, stream=httpx.ByteStream(b"a,b\n1,2\n"))

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        headers={"Authorization": "secret", "Cookie": "private"},
        follow_redirects=True,
    ) as pool:
        client = HttpClient(pool, limiter, 1)
        async with client.stream("https://exchange.test/archive") as response:
            assert await response.aread() == b"a,b\n1,2\n"
    assert limiter.admissions == 2


@pytest.mark.asyncio
async def test_stream_holds_slot_and_cancellation_releases_it() -> None:
    limiter = CountingLimiter()
    calls = 0
    entered = asyncio.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, stream=httpx.ByteStream(b"a,b\n"))

    async with open_http_client(
        limiter=limiter, max_concurrent=1, transport=httpx.MockTransport(handler)
    ) as client:

        async def blocked() -> None:
            async with client.stream("https://exchange.test/first"):
                entered.set()
                await asyncio.Event().wait()

        first = asyncio.create_task(blocked())
        await asyncio.wait_for(entered.wait(), 1)
        second = asyncio.create_task(client.get("https://exchange.test/second"))
        await asyncio.sleep(0)
        assert calls == 1
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert (await asyncio.wait_for(second, 1)).status_code == 200
    assert calls == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["get", "post"])
async def test_discovery_metadata_size_is_bounded(method: str) -> None:
    async with open_http_client(
        limiter=CountingLimiter(),
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, stream=httpx.ByteStream(b"123456"))
        ),
    ) as client:
        with pytest.raises(httpx.RequestError, match="permitted size"):
            if method == "get":
                await client.get("https://exchange.test/discovery", max_response_bytes=5)
            else:
                await client.post("https://exchange.test/discovery", json={}, max_response_bytes=5)


@pytest.mark.asyncio
async def test_opted_in_buffered_get_redirect_consumes_admissions() -> None:
    limiter = CountingLimiter()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/first":
            return httpx.Response(302, headers={"Location": "https://exchange.test/final"})
        return httpx.Response(200, text="listing")

    async with open_http_client(limiter=limiter, transport=httpx.MockTransport(handler)) as client:
        response = await client.get(
            "https://exchange.test/first", follow_redirects=True, max_response_bytes=100
        )
    assert limiter.admissions == 2
    assert response.url.path == "/final"
    assert response.text == "listing"
