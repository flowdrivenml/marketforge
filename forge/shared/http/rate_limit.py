"""Async pacing for outbound HTTP requests."""

import math
from asyncio import Lock, sleep
from time import monotonic


class RequestRateLimiter:
    """Space request admissions according to a maximum requests-per-second rate.

    Share one instance across requests using the same upstream rate budget.
    An instance belongs to one event loop.
    """

    def __init__(self, requests_per_second: float) -> None:
        if (
            isinstance(requests_per_second, bool)
            or not math.isfinite(requests_per_second)
            or requests_per_second <= 0
        ):
            raise ValueError("requests_per_second must be a finite number greater than zero.")

        self._interval = 1.0 / requests_per_second

        if not math.isfinite(self._interval):
            raise ValueError("requests_per_second is too small.")

        self._lock = Lock()
        self._last_admission: float | None = None

    async def wait(self) -> None:
        """Wait for permission to start one request.

        The first admission is immediate. Cancellation while waiting does not
        consume an admission or prevent subsequent callers from proceeding.
        """
        async with self._lock:
            while self._last_admission is not None:
                remaining = self._last_admission + self._interval - monotonic()

                if remaining <= 0:
                    break

                await sleep(remaining)

            self._last_admission = monotonic()
