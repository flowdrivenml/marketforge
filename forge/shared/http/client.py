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

    @asynccontextmanager
    async def _admission(self) -> AsyncGenerator[None, None]:
        while True:
            await self._wait_for_cooldown()
            await self._slots.acquire()
            try:
                self._check_cooldown_failure()
                if self._cooldown_until > monotonic():
                    continue
                await self._limiter.wait()
                self._check_cooldown_failure()
                if self._cooldown_until > monotonic():
                    continue
                yield
                return
            finally:
                self._slots.release()

    @asynccontextmanager
    async def _response(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        json: object = None,
        retry_safe: bool = True,
        streaming: bool = False,
        allowed_statuses: frozenset[int] = frozenset(),
        follow_redirects: bool = False,
    ) -> AsyncGenerator[httpx.Response, None]:
        attempt = 0
        redirects = 0
        while True:
            attempt += 1
            delay: float | None = None
            async with self._admission():
                try:
                    request = self._client.build_request(
                        method, url, params=params, headers=headers, json=json
                    )
                    if redirects:
                        for credential in ("authorization", "cookie", "proxy-authorization"):
                            request.headers.pop(credential, None)
                    response = await self._client.send(
                        request, stream=streaming, follow_redirects=False
                    )
                except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError):
                    delay = self._retry_policy.delay(attempt) if retry_safe else None
                    if delay is None:
                        raise
                else:
                    self._record_cooldown(response)
                    try:
                        if response.is_redirect and follow_redirects:
                            if redirects >= 5 or method == "POST":
                                raise httpx.TooManyRedirects(
                                    "Redirect limit reached", request=request
                                )
                            destination = response.next_request
                            if (
                                destination is None
                                or destination.url.scheme != "https"
                                or destination.url.username
                                or destination.url.password
                            ):
                                raise httpx.RequestError("Unsafe redirect", request=request)
                            # Public acquisition clients never forward credentials on redirects.
                            headers = {
                                key: value
                                for key, value in (headers or {}).items()
                                if key.lower()
                                not in {"authorization", "cookie", "proxy-authorization"}
                            }
                            url = str(destination.url)
                            params = None
                            redirects += 1
                            attempt = 0
                            continue
                        if response.status_code in RETRYABLE_STATUSES and retry_safe:
                            delay = self._retry_policy.delay(
                                attempt, retry_after=response.headers.get("Retry-After")
                            )
                        if delay is None:
                            if response.status_code not in allowed_statuses:
                                response.raise_for_status()
                            yield response
                            return
                    finally:
                        await response.aclose()
            await sleep(delay)

    async def _buffer_limited(self, response: httpx.Response, limit: int) -> httpx.Response:
        content = bytearray()
        async for chunk in response.aiter_bytes(chunk_size=65536):
            if len(content) + len(chunk) > limit:
                raise httpx.RequestError(
                    "HTTP metadata response exceeds the permitted size.", request=response.request
                )
            content.extend(chunk)
        headers = dict(response.headers)
        headers.pop("content-encoding", None)
        headers.pop("content-length", None)
        return httpx.Response(
            response.status_code, headers=headers, content=bytes(content), request=response.request
        )

    async def get(
        self,
        url: str,
        *,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        max_response_bytes: int | None = None,
        follow_redirects: bool = False,
    ) -> httpx.Response:
        """Fetch a buffered response, optionally bounding discovery metadata."""
        if max_response_bytes is not None and max_response_bytes < 1:
            raise ValueError("max_response_bytes must be positive.")
        async with self._response(
            "GET",
            url,
            params=params,
            headers=headers,
            streaming=max_response_bytes is not None,
            follow_redirects=follow_redirects,
        ) as response:
            if max_response_bytes is not None:
                return await self._buffer_limited(response, max_response_bytes)
            return response

    async def post(
        self,
        url: str,
        *,
        json: object,
        retry_safe: bool = False,
        headers: Mapping[str, str] | None = None,
        max_response_bytes: int | None = None,
    ) -> httpx.Response:
        """POST once unless the caller explicitly declares a read-only operation."""
        if max_response_bytes is not None and max_response_bytes < 1:
            raise ValueError("max_response_bytes must be positive.")
        async with self._response(
            "POST",
            url,
            json=json,
            headers=headers,
            retry_safe=retry_safe,
            streaming=max_response_bytes is not None,
        ) as response:
            if max_response_bytes is not None:
                return await self._buffer_limited(response, max_response_bytes)
            return response

    async def head(
        self,
        url: str,
        *,
        allowed_statuses: frozenset[int] = frozenset(),
    ) -> httpx.Response:
        """Probe existence with each redirect and retry paced separately."""
        async with self._response(
            "HEAD", url, allowed_statuses=allowed_statuses, follow_redirects=True
        ) as response:
            return response

    @asynccontextmanager
    async def stream(self, url: str) -> AsyncGenerator[httpx.Response, None]:
        """Stream GET while holding a slot; body failures propagate without replay.

        Only response acquisition is retried. A consumer must discard partial
        output after a body failure. Redirects consume normal rate admissions.
        """
        async with self._response(
            "GET",
            url,
            headers={"Accept-Encoding": "identity"},
            streaming=True,
            follow_redirects=True,
        ) as response:
            yield response


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
