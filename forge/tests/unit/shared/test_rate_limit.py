"""Tests for shared async request pacing."""

from asyncio import CancelledError

import pytest

import forge.shared.http.rate_limit as rate_limit
from forge.shared.http.rate_limit import RequestRateLimiter


class FakeClock:
    """Deterministic clock for pacing tests."""

    def __init__(self) -> None:
        self.now = 0.0
        self.delays: list[float] = []

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, delay: float) -> None:
        self.delays.append(delay)
        self.now += delay


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    clock = FakeClock()
    monkeypatch.setattr(rate_limit, "monotonic", clock.monotonic)
    monkeypatch.setattr(rate_limit, "sleep", clock.sleep)
    return clock


@pytest.mark.parametrize(
    "rate",
    [0.0, -1.0, float("inf"), float("-inf"), float("nan"), True],
)
def test_rejects_invalid_rates(rate: float) -> None:
    with pytest.raises(ValueError, match="requests_per_second"):
        RequestRateLimiter(rate)


@pytest.mark.asyncio
async def test_first_request_is_immediate(clock: FakeClock) -> None:
    limiter = RequestRateLimiter(2.0)

    await limiter.wait()

    assert clock.delays == []
    assert clock.now == 0.0


@pytest.mark.asyncio
async def test_spaces_request_admissions(clock: FakeClock) -> None:
    limiter = RequestRateLimiter(2.0)

    await limiter.wait()
    await limiter.wait()
    await limiter.wait()

    assert clock.delays == [0.5, 0.5]
    assert clock.now == 1.0


@pytest.mark.asyncio
async def test_elapsed_time_counts_toward_wait(clock: FakeClock) -> None:
    limiter = RequestRateLimiter(2.0)

    await limiter.wait()
    clock.now = 0.25

    await limiter.wait()

    assert clock.delays == [0.25]
    assert clock.now == 0.5


@pytest.mark.asyncio
async def test_idle_time_does_not_accumulate_burst_credit(
    clock: FakeClock,
) -> None:
    limiter = RequestRateLimiter(2.0)

    await limiter.wait()
    clock.now = 10.0

    await limiter.wait()
    await limiter.wait()

    assert clock.delays == [0.5]
    assert clock.now == 10.5


@pytest.mark.asyncio
async def test_cancelled_wait_releases_lock_without_consuming_admission(
    clock: FakeClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    limiter = RequestRateLimiter(2.0)
    await limiter.wait()

    async def cancelled_sleep(delay: float) -> None:
        raise CancelledError

    monkeypatch.setattr(rate_limit, "sleep", cancelled_sleep)

    with pytest.raises(CancelledError):
        await limiter.wait()

    monkeypatch.setattr(rate_limit, "sleep", clock.sleep)

    await limiter.wait()

    assert clock.delays == [0.5]
    assert clock.now == 0.5
