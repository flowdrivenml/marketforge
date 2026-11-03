from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import requests

from marketforge.errors import AcquisitionError

RETRYABLE_STATUS_CODES = {
    429,
    500,
    502,
    503,
    504,
}


@dataclass(frozen=True)
class RequestPolicy:
    """Controls HTTP throttling and retry behavior."""

    request_interval: float = 0.0
    max_retries: int = 5
    backoff: float = 1.0


class HttpClient:
    """Shared HTTP execution layer.

    Exchange sources decide which RequestPolicy to use.
    HttpClient only enforces that policy.
    """

    def __init__(
        self,
        timeout: float = 30.0,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.timeout = timeout
        self.session = requests.Session()

        self._last_request_time: float | None = None

        self.session.headers.update(
            {
                "User-Agent": "MarketForge",
                "Accept": "*/*",
            }
        )

        if headers:
            self.session.headers.update(headers)

    def get(
        self,
        url: str,
        *,
        policy: RequestPolicy | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> requests.Response:
        return self.request(
            "GET",
            url,
            policy=policy,
            params=params,
            headers=headers,
            **kwargs,
        )

    def post(
        self,
        url: str,
        *,
        policy: RequestPolicy | None = None,
        json: Any = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> requests.Response:
        return self.request(
            "POST",
            url,
            policy=policy,
            json=json,
            params=params,
            headers=headers,
            **kwargs,
        )

    def head(
        self,
        url: str,
        *,
        policy: RequestPolicy | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> requests.Response:
        return self.request(
            "HEAD",
            url,
            policy=policy,
            params=params,
            headers=headers,
            **kwargs,
        )

    def request(
        self,
        method: str,
        url: str,
        *,
        policy: RequestPolicy | None = None,
        allowed_status_codes: set[int] | None = None,
        **kwargs: Any,
    ) -> requests.Response:
        """Execute an HTTP request using the supplied policy."""

        allowed_status_codes = allowed_status_codes or set()

        timeout = kwargs.pop(
            "timeout",
            self.timeout,
        )

        policy = policy or RequestPolicy()

        max_attempts = policy.max_retries + 1

        for attempt in range(max_attempts):
            self._wait_for_request_slot(policy)

            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    timeout=timeout,
                    **kwargs,
                )

                self._last_request_time = time.monotonic()

                # Normal success.
                if response.ok:
                    return response

                # Expected non-2xx status.
                if response.status_code in allowed_status_codes:
                    return response

                # Retryable failure.
                if response.status_code in RETRYABLE_STATUS_CODES:
                    if attempt < policy.max_retries:
                        self._sleep_before_retry(
                            policy=policy,
                            attempt=attempt,
                        )
                        continue

                response.raise_for_status()

            except (
                requests.ConnectionError,
                requests.Timeout,
            ) as exc:
                self._last_request_time = time.monotonic()

                if attempt < policy.max_retries:
                    self._sleep_before_retry(
                        policy=policy,
                        attempt=attempt,
                    )
                    continue

                raise AcquisitionError(
                    f"{method} request failed for {url}: {exc}"
                ) from exc

            except requests.RequestException as exc:
                self._last_request_time = time.monotonic()

                raise AcquisitionError(
                    f"{method} request failed for {url}: {exc}"
                ) from exc

        raise AcquisitionError(f"{method} request failed for {url}")

    def _wait_for_request_slot(
        self,
        policy: RequestPolicy,
    ) -> None:
        if self._last_request_time is None:
            return

        elapsed = time.monotonic() - self._last_request_time
        remaining = policy.request_interval - elapsed

        if remaining > 0:
            time.sleep(remaining)

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> HttpClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    @staticmethod
    def _sleep_before_retry(
        *,
        policy: RequestPolicy,
        attempt: int,
    ) -> None:
        """Sleep before retrying a transient request failure."""

        delay = policy.backoff * (2**attempt)

        if delay > 0:
            time.sleep(delay)
