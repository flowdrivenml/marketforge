"""Retry behavior without external requests or real backoff delays."""

import asyncio
from datetime import UTC, datetime

import httpx
import pytest

import forge.shared.http.client as client_module
from forge.shared.http.client import open_http_client
from forge.shared.http.rate_limit import RequestRateLimiter
from forge.shared.http.retry import RetryPolicy, parse_retry_after


class CountingLimiter(RequestRateLimiter):
    """Count retry admissions without real pacing delays."""

    def __init__(self) -> None:
        super().__init__(1.0)
        self.admissions = 0

    async def wait(self) -> None:
        self.admissions += 1


@pytest.fixture
def delays(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    recorded: list[float] = []
    clock = 0.0

    def monotonic() -> float:
        return clock

    async def fake_sleep(delay: float) -> None:
        nonlocal clock
        recorded.append(delay)
        clock += delay

    monkeypatch.setattr(client_module, "sleep", fake_sleep)
    monkeypatch.setattr(client_module, "monotonic", monotonic)
    return recorded


def test_parses_retry_after_seconds_and_http_date() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)

    assert parse_retry_after("5", now=now) == 5.0
    assert (
        parse_retry_after(
            "Thu, 01 Jan 2026 00:00:05 GMT",
            now=now,
        )
        == 5.0
    )
    assert parse_retry_after("invalid", now=now) is None
    assert parse_retry_after("-1", now=now) is None


@pytest.mark.asyncio
async def test_retries_503_and_reuses_limiter(delays: list[float]) -> None:
    limiter = CountingLimiter()
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(503 if attempts == 1 else 200)

    async with open_http_client(
        limiter=limiter,
        retry_policy=RetryPolicy(max_attempts=3),
        transport=httpx.MockTransport(handler),
    ) as client:
        response = await client.get("https://exchange.test/instruments")

    assert response.status_code == 200
    assert attempts == 2
    assert limiter.admissions == 2
    assert delays == [0.5]


@pytest.mark.asyncio
async def test_respects_retry_after(delays: list[float]) -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1

        if attempts == 1:
            return httpx.Response(429, headers={"Retry-After": "5"})

        return httpx.Response(200)

    async with open_http_client(
        limiter=CountingLimiter(),
        retry_policy=RetryPolicy(max_attempts=3),
        transport=httpx.MockTransport(handler),
    ) as client:
        await client.get("https://exchange.test/instruments")

    assert attempts == 2
    assert delays == [5.0]


@pytest.mark.asyncio
async def test_does_not_shorten_excessive_retry_after(
    delays: list[float],
) -> None:
    limiter = CountingLimiter()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "120"})

    async with open_http_client(
        limiter=limiter,
        retry_policy=RetryPolicy(max_attempts=3),
        transport=httpx.MockTransport(handler),
    ) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await client.get("https://exchange.test/instruments")

    assert limiter.admissions == 1
    assert delays == []


@pytest.mark.asyncio
async def test_stops_after_attempt_limit(delays: list[float]) -> None:
    limiter = CountingLimiter()

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("Simulated timeout.", request=request)

    async with open_http_client(
        limiter=limiter,
        retry_policy=RetryPolicy(max_attempts=3),
        transport=httpx.MockTransport(handler),
    ) as client:
        with pytest.raises(httpx.ReadTimeout):
            await client.get("https://exchange.test/instruments")

    assert limiter.admissions == 3
    assert delays == [0.5, 1.0]


@pytest.mark.asyncio
async def test_does_not_retry_404(delays: list[float]) -> None:
    limiter = CountingLimiter()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    async with open_http_client(
        limiter=limiter,
        retry_policy=RetryPolicy(max_attempts=3),
        transport=httpx.MockTransport(handler),
    ) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await client.get("https://exchange.test/missing")

    assert limiter.admissions == 1
    assert delays == []


@pytest.mark.asyncio
async def test_cancellation_stops_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    limiter = CountingLimiter()

    async def cancelled_sleep(delay: float) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(client_module, "sleep", cancelled_sleep)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    async with open_http_client(
        limiter=limiter,
        retry_policy=RetryPolicy(max_attempts=3),
        transport=httpx.MockTransport(handler),
    ) as client:
        with pytest.raises(asyncio.CancelledError):
            await client.get("https://exchange.test/instruments")

    assert limiter.admissions == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 503])
async def test_shared_cooldown_survives_cancellation(
    monkeypatch: pytest.MonkeyPatch, status: int
) -> None:
    clock = 0.0
    sleeping = asyncio.Event()
    release = asyncio.Event()
    attempts: list[str] = []
    waits: list[float] = []

    async def sleep(delay: float) -> None:
        waits.append(delay)
        sleeping.set()
        await release.wait()

    monkeypatch.setattr(client_module, "monotonic", lambda: clock)
    monkeypatch.setattr(client_module, "sleep", sleep)

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(request.url.path)
        if len(attempts) == 1:
            return httpx.Response(status, headers={"Retry-After": "5"})
        return httpx.Response(200)

    limiter = CountingLimiter()
    async with open_http_client(
        limiter=limiter,
        max_concurrent=1,
        transport=httpx.MockTransport(handler),
    ) as client:
        # Even an exhausted retry policy must publish the cooldown.
        with pytest.raises(httpx.HTTPStatusError):
            await client.get("https://exchange.test/first")
        cancelled = asyncio.create_task(client.get("https://exchange.test/cancelled"))
        await asyncio.wait_for(sleeping.wait(), 1)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        sleeping.clear()
        survivor = asyncio.create_task(client.get("https://exchange.test/survivor"))
        await asyncio.wait_for(sleeping.wait(), 1)
        assert attempts == ["/first"]
        assert limiter.admissions == 1
        # Early wakeups must recheck the monotonic deadline.
        clock = 5.0
        release.set()
        assert (await asyncio.wait_for(survivor, 1)).status_code == 200
    assert attempts == ["/first", "/survivor"]
    assert waits == [5.0, 5.0]


@pytest.mark.asyncio
async def test_excessive_cooldown_rejects_queued_and_future_calls() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return httpx.Response(503, headers={"Retry-After": "120"})

    async with open_http_client(
        limiter=CountingLimiter(),
        max_concurrent=1,
        transport=httpx.MockTransport(handler),
    ) as client:
        first = asyncio.create_task(client.get("https://exchange.test/first"))
        await asyncio.wait_for(started.wait(), 1)
        queued = asyncio.create_task(client.get("https://exchange.test/queued"))
        await asyncio.sleep(0)
        release.set()
        for task in (first, queued):
            with pytest.raises(httpx.HTTPStatusError):
                await asyncio.wait_for(task, 1)
        with pytest.raises(httpx.HTTPStatusError):
            await client.get("https://exchange.test/future")
    assert calls == 1


@pytest.mark.asyncio
async def test_cooldown_rechecks_callers_already_waiting_for_limiter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = 0.0
    in_flight = asyncio.Event()
    respond = asyncio.Event()
    pacing = asyncio.Event()
    paced = asyncio.Event()
    cooling = asyncio.Event()
    cooled = asyncio.Event()
    paths: list[str] = []

    class BlockingLimiter(CountingLimiter):
        async def wait(self) -> None:
            await super().wait()
            if self.admissions == 2:
                pacing.set()
                await paced.wait()

    async def sleep(delay: float) -> None:
        cooling.set()
        await cooled.wait()

    async def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.url.path == "/first":
            in_flight.set()
            await respond.wait()
            return httpx.Response(429, headers={"Retry-After": "5"})
        return httpx.Response(200)

    monkeypatch.setattr(client_module, "monotonic", lambda: clock)
    monkeypatch.setattr(client_module, "sleep", sleep)
    limiter = BlockingLimiter()
    async with open_http_client(
        limiter=limiter, max_concurrent=2, transport=httpx.MockTransport(handler)
    ) as client:
        first = asyncio.create_task(client.get("https://exchange.test/first"))
        await asyncio.wait_for(in_flight.wait(), 1)
        second = asyncio.create_task(client.get("https://exchange.test/second"))
        await asyncio.wait_for(pacing.wait(), 1)
        respond.set()
        with pytest.raises(httpx.HTTPStatusError):
            await first
        paced.set()
        await asyncio.wait_for(cooling.wait(), 1)
        assert paths == ["/first"]
        clock = 5.0
        cooled.set()
        await asyncio.wait_for(second, 1)
    assert paths == ["/first", "/second"]
    assert limiter.admissions == 3


@pytest.mark.asyncio
async def test_retry_backoff_releases_slot(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeping = asyncio.Event()
    release = asyncio.Event()
    attempts = 0

    async def sleep(delay: float) -> None:
        sleeping.set()
        await release.wait()

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(500 if attempts == 1 else 200)

    monkeypatch.setattr(client_module, "sleep", sleep)
    limiter = CountingLimiter()
    async with open_http_client(
        limiter=limiter,
        max_concurrent=1,
        retry_policy=RetryPolicy(max_attempts=2),
        transport=httpx.MockTransport(handler),
    ) as client:
        first = asyncio.create_task(client.get("https://exchange.test/first"))
        await asyncio.wait_for(sleeping.wait(), 1)
        assert (await asyncio.wait_for(client.get("https://exchange.test/other"), 1)).is_success
        release.set()
        assert (await asyncio.wait_for(first, 1)).is_success
    assert limiter.admissions == 3
