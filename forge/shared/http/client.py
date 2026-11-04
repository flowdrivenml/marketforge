"""Shared async HTTP requests with pacing and concurrency limits."""

import math
from asyncio import Semaphore, sleep
from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from time import monotonic

import httpx

from forge.shared.http.rate_limit import RequestRateLimiter
from forge.shared.http.retry import RETRYABLE_STATUSES, RetryPolicy, parse_retry_after


class HttpClient:
    """HTTP operations using a shared connection pool and upstream rate budget.

    A 429/503 Retry-After exceeding max_delay_seconds permanently closes
    admissions for this instance. Queued and future calls raise HTTPStatusError
    from that response when they next check admission; create a new client to
    resume. In-flight requests may finish. Cancelling a caller never clears the
    shared cooldown, and cooldown/backoff sleeps do not occupy request slots.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        limiter: RequestRateLimiter,
        max_concurrent: int,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        if isinstance(max_concurrent, bool) or max_concurrent < 1:
            raise ValueError("max_concurrent must be a positive integer.")

        self._client = client
        self._limiter = limiter
        self._slots = Semaphore(max_concurrent)
        self._retry_policy = retry_policy or RetryPolicy()
        self._cooldown_until = 0.0
        self._cooldown_failure: httpx.Response | None = None

    def _check_cooldown_failure(self) -> None:
        if self._cooldown_failure is not None:
            self._cooldown_failure.raise_for_status()

    def _record_cooldown(self, response: httpx.Response) -> None:
        if response.status_code not in {429, 503}:
            return
        requested = parse_retry_after(response.headers.get("Retry-After"), now=datetime.now(UTC))
        if requested is None:
            return
        if requested > self._retry_policy.max_delay_seconds:
            self._cooldown_failure = response
        else:
            self._cooldown_until = max(self._cooldown_until, monotonic() + requested)

    async def _wait_for_cooldown(self) -> None:
        while True:
            self._check_cooldown_failure()
            remaining = self._cooldown_until - monotonic()
            if remaining <= 0:
                return
            await sleep(remaining)

    async def _get_once(
        self,
        url: str,
        *,
        params: Mapping[str, str] | None,
        headers: Mapping[str, str] | None,
    ) -> httpx.Response:
        """Perform one paced request while holding a concurrency slot."""
        while True:
            await self._wait_for_cooldown()
            async with self._slots:
                self._check_cooldown_failure()
                if self._cooldown_until > monotonic():
                    continue
                await self._limiter.wait()
                self._check_cooldown_failure()
                if self._cooldown_until > monotonic():
                    continue

                response = await self._client.get(url, params=params, headers=headers)
                # Publish before releasing the slot to queued callers.
                self._record_cooldown(response)
                return response

    async def get(
        self,
        url: str,
        *,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        """Fetch a buffered response with explicitly bounded retries."""
        attempt = 0

        while True:
            attempt += 1

            try:
                response = await self._get_once(
                    url,
                    params=params,
                    headers=headers,
                )
            except (
                httpx.TimeoutException,
                httpx.NetworkError,
                httpx.RemoteProtocolError,
            ):
                delay = self._retry_policy.delay(attempt)

                if delay is None:
                    raise

                await sleep(delay)
                continue

            if response.status_code in RETRYABLE_STATUSES:
                delay = self._retry_policy.delay(
                    attempt,
                    retry_after=response.headers.get("Retry-After"),
                )

                if delay is not None:
                    await response.aclose()
                    await sleep(delay)
                    continue

            response.raise_for_status()
            return response


@asynccontextmanager
async def open_http_client(
    *,
    limiter: RequestRateLimiter,
    max_concurrent: int = 4,
    timeout_seconds: float = 30.0,
    transport: httpx.AsyncBaseTransport | None = None,
    retry_policy: RetryPolicy | None = None,
) -> AsyncGenerator[HttpClient, None]:
    """Own and close an async connection pool for one upstream service.

    Share the yielded client across that service's concurrent tasks.
    Await those tasks before leaving this context.
    """
    if isinstance(max_concurrent, bool) or max_concurrent < 1:
        raise ValueError("max_concurrent must be a positive integer.")

    if (
        isinstance(timeout_seconds, bool)
        or not math.isfinite(timeout_seconds)
        or timeout_seconds <= 0
    ):
        raise ValueError("timeout_seconds must be finite and greater than zero.")

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(timeout_seconds),
        limits=httpx.Limits(
            max_connections=max_concurrent,
            max_keepalive_connections=max_concurrent,
        ),
        follow_redirects=False,
        transport=transport,
    ) as client:
        yield HttpClient(
            client=client,
            limiter=limiter,
            max_concurrent=max_concurrent,
            retry_policy=retry_policy,
        )
