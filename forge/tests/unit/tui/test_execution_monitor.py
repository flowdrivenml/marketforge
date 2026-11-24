"""Monitor hand-off observes the existing operation and forwards bounded requests."""

import asyncio
from pathlib import Path

import pytest

from forge.historical.pipelines.terminal import WindowUnavailable
from forge.tui import execution_monitor
from forge.tui.controller import Operation


class Desktop:
    def __init__(self, *, ready: bool) -> None:
        self.ready = ready
        self.exit = asyncio.Event()
        self.frame: Path | None = None
        self.launches = 0

    async def launch_module(
        self, module: str, request: Path, *, cwd: Path, title: str
    ) -> "Desktop":
        assert module == "forge.tui.execution_monitor"
        assert await asyncio.to_thread(request.read_text)
        self.frame = request
        self.launches += 1
        if self.ready:
            request.with_name("ready").touch()
        else:
            self.exit.set()
        return self

    async def wait(self) -> int:
        await self.exit.wait()
        return 0

    def terminate(self) -> None:
        self.exit.set()

    def kill(self) -> None:
        self.exit.set()


@pytest.mark.asyncio
@pytest.mark.parametrize("request_stop", [False, True])
async def test_observer_close_and_stop_are_distinct(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request_stop: bool
) -> None:
    desktop = Desktop(ready=True)
    monkeypatch.setattr(execution_monitor.Ghostty, "detect", lambda: desktop)
    op = Operation("local-view", "merge run", reference="fixture", run_id="merge-execute-100")
    requests: list[str] = []

    async def stop() -> None:
        requests.append("stop")
        desktop.exit.set()

    def opened() -> None:
        assert desktop.frame is not None
        assert "merge-execute-100" in desktop.frame.read_text()
        if request_stop:
            desktop.frame.with_name("stop").touch()
        else:
            desktop.exit.set()

    await execution_monitor.observe(op, "testing", tmp_path, stop, opened)
    assert desktop.launches == 1
    assert requests == (["stop"] if request_stop else [])
    assert op.run_id == "merge-execute-100"


@pytest.mark.asyncio
async def test_window_exit_before_handshake_is_an_explicit_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    desktop = Desktop(ready=False)
    monkeypatch.setattr(execution_monitor.Ghostty, "detect", lambda: desktop)

    async def stop() -> None:
        pytest.fail("A failed launch must leave execution ownership alone")

    with pytest.raises(WindowUnavailable, match="before the monitor was ready"):
        await execution_monitor.observe(
            Operation("view", "convert run"),
            "testing",
            tmp_path,
            stop,
            lambda: pytest.fail("Failed window was reported ready"),
        )
