"""Bounded retry policy for HTTP GET requests."""

import math
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Final

RETRYABLE_STATUSES: Final[frozenset[int]] = frozenset({408, 429, 500, 502, 503, 504})


def parse_retry_after(value: str | None, *, now: datetime) -> float | None:
    """Return the requested delay, or None for an invalid or absent header."""
    if value is None:
        return None

    value = value.strip()

    if value.isascii() and value.isdigit():
        try:
            return float(value)
        except OverflowError:
            return math.inf

    try:
        target = parsedate_to_datetime(value)
    except (ValueError, TypeError, OverflowError):
        return None

    if target.tzinfo is None:
        return None

    return max(0.0, (target - now).total_seconds())


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Attempt count includes the initial request."""

    max_attempts: int = 1
    base_delay_seconds: float = 0.5
    max_delay_seconds: float = 30.0

    def __post_init__(self) -> None:
        if isinstance(self.max_attempts, bool) or not 1 <= self.max_attempts <= 10:
            raise ValueError("max_attempts must be an integer from 1 to 10.")

        for name, value in (
            ("base_delay_seconds", self.base_delay_seconds),
            ("max_delay_seconds", self.max_delay_seconds),
        ):
            if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and greater than zero.")

        if self.base_delay_seconds > self.max_delay_seconds:
            raise ValueError("base_delay_seconds cannot exceed max_delay_seconds.")

    def delay(
        self,
        failed_attempt: int,
        *,
        retry_after: str | None = None,
    ) -> float | None:
        """Return a retry delay, or None when retrying is not permitted."""
        if not 1 <= failed_attempt < self.max_attempts:
            return None

        backoff = min(
            self.max_delay_seconds,
            self.base_delay_seconds * (2 ** (failed_attempt - 1)),
        )
        requested = parse_retry_after(
            retry_after,
            now=datetime.now(UTC),
        )

        if requested is not None:
            if requested > self.max_delay_seconds:
                return None
            return max(backoff, requested)

        return backoff
