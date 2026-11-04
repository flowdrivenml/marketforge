"""Structured progress counts are deterministic and independent of rendering."""

import asyncio

import pytest

from forge.historical.archives.progress import ProgressEvent, ProgressState


def test_mapping_terminal_outcomes_and_subset_counts() -> None:
    state = ProgressState(5)
    for outcome, cached, count in [
        ("successful", False, 3),
        ("successful", True, 4),
        ("empty", True, 0),
        ("unsupported", False, 0),
        ("failed", False, 0),
    ]:
        state.apply(ProgressEvent("request_started", identity="bybit BTCUSDT trades"))
        # Literal values are parametrized below through typed event constructors.
        if outcome == "successful":
            event = ProgressEvent(
                "request_finished", request_outcome="successful", cached=cached, archives=count
            )
        elif outcome == "empty":
            event = ProgressEvent("request_finished", request_outcome="empty", cached=cached)
        elif outcome == "unsupported":
            event = ProgressEvent(
                "request_finished", request_outcome="unsupported", error="unsupported product"
            )
        else:
            event = ProgressEvent("request_finished", request_outcome="failed", error="page failed")
        snapshot = state.apply(event)
    assert snapshot.requests.finished == 5
    assert snapshot.requests.successful == 3
    assert snapshot.requests.cached == 2
    assert snapshot.requests.empty == 1
    assert snapshot.requests.failed == snapshot.requests.unsupported == 1
    assert snapshot.requests.archives == 7
    assert snapshot.files.queued == 0


@pytest.mark.asyncio
async def test_concurrent_worker_updates_and_provisional_total() -> None:
    state = ProgressState(1)
    gate = asyncio.Event()

    async def worker(index: int) -> None:
        state.apply(ProgressEvent("queued"))
        state.apply(
            ProgressEvent(
                "file_active",
                file_id=index,
                identity=f"bybit {index}.csv.gz",
                activity="Verifying local archive",
            )
        )
        await gate.wait()
        state.apply(
            ProgressEvent(
                "file_finished", file_id=index, file_outcome="completed" if index % 2 else "reused"
            )
        )

    tasks = [asyncio.create_task(worker(index)) for index in range(4)]
    # An explicit event turn lets all workers reach the controlled gate.
    ready = asyncio.Event()
    asyncio.get_running_loop().call_soon(ready.set)
    await ready.wait()
    assert state.files.queued == len(state.active) == 4
    gate.set()
    await asyncio.gather(*tasks)
    snapshot = state.apply(ProgressEvent("activity", activity="Discovering next request"))
    assert snapshot.provisional
    assert snapshot.files.resolved == snapshot.files.queued == 4
    assert not snapshot.active
    snapshot = state.apply(ProgressEvent("total_final"))
    assert not snapshot.provisional
    assert snapshot.files.completed == snapshot.files.reused == 2


def test_failures_cancellation_and_bounded_history() -> None:
    state = ProgressState(1000)
    state.apply(
        ProgressEvent(
            "file_finished", file_outcome="failed", identity="failed archive", error="HTTP 404"
        )
    )
    state.apply(
        ProgressEvent("file_finished", file_outcome="cancelled", identity="cancelled archive")
    )
    for index in range(1000):
        state.apply(
            ProgressEvent("request_finished", identity=str(index), request_outcome="successful")
        )
    snapshot = state.apply(ProgressEvent("finished", identity="run/report.json"))
    assert len(snapshot.recent) == 6
    assert "failed archive" in snapshot.failures[0]
    assert snapshot.files.failed == snapshot.files.cancelled == 1
    assert snapshot.report == "run/report.json"


def test_known_total_includes_discovered_files_not_yet_queued() -> None:
    state = ProgressState(2)
    state.apply(ProgressEvent("files_discovered", archives=70))
    snapshot = state.apply(ProgressEvent("queued"))
    assert snapshot.known_total == 70
    assert snapshot.files.queued == 1
    assert snapshot.provisional
    state.apply(ProgressEvent("files_discovered", archives=3))
    snapshot = state.apply(ProgressEvent("total_final"))
    assert snapshot.known_total == 73
    assert not snapshot.provisional
