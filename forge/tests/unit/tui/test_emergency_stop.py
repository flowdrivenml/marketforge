"""Exercise emergency stop using disposable subprocesses and the workspace pilot."""

import asyncio
import sys
from pathlib import Path

import pytest
from rich.console import Console
from textual.widgets import Button

from forge.historical.engine.process import supervise
from forge.shared.config.models import AppSettings
from forge.shared.paths import ProjectPaths
from forge.startup.context import ApplicationContext
from forge.tui.actions import Action, Values
from forge.tui.app import ForgeApp
from forge.tui.inspection import Inspection
from forge.tui.screens import Confirm
from forge.tui.workflows import Edit, Progress


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [(80, 24), (100, 30), (120, 32), (160, 48)])
async def test_stop_all_from_inspector(size: tuple[int, int], tmp_path: Path) -> None:
    context = ApplicationContext(
        AppSettings(), ProjectPaths(tmp_path, tmp_path / "data"), None, Console(), Console()
    )

    async def invoke(
        _key: str, _values: Values, _context: ApplicationContext, _progress: Progress, _edit: Edit
    ) -> object:
        code, interrupted = await supervise(
            [
                sys.executable,
                "-c",
                "import signal,time; from pathlib import Path; "
                "signal.signal(signal.SIGTERM,signal.SIG_IGN); "
                "print('ready',flush=True); Path('ready').touch(); time.sleep(60)",
            ],
            tmp_path,
            tmp_path,
        )
        return {"state": "interrupted" if interrupted else "completed", "exit_code": code}

    other = await asyncio.create_subprocess_exec(
        sys.executable, "-c", "import time; time.sleep(60)", start_new_session=True
    )
    app = ForgeApp(context, invoke=invoke)
    try:
        async with app.run_test(size=size) as pilot:
            assert app.screen.query_one("#emergency-stop", Button).region.x == 0
            task = app.controller.launch(Action("fixture", "Workflows"), {})
            async with asyncio.timeout(5):
                while not (tmp_path / "ready").exists():  # noqa: ASYNC110
                    await asyncio.sleep(0.02)
            app.push_screen(Inspection("Fixture", {"State": "Running"}))
            await pilot.pause()
            stop = app.screen.query_one("#emergency-stop", Button)
            assert stop.region.x == stop.region.y == 0
            assert await pilot.click(stop)
            await pilot.pause()
            assert isinstance(app.screen, Confirm)
            assert app.screen.focused is app.screen.query_one("#back")
            await pilot.press("ctrl+shift+x", "escape")
            assert isinstance(app.screen, Inspection)
            assert app.controller.active()
            await pilot.press("ctrl+shift+x")
            assert isinstance(app.screen, Confirm)
            assert await pilot.click("#confirm")
            op = await asyncio.wait_for(task, timeout=4)
            assert op.state == "interrupted"
            assert op.result == {"state": "interrupted", "exit_code": -9}
            assert len(app.controller.history) == 1
            assert other.returncode is None
        assert (tmp_path / "engine-stdout.log").read_text() == "ready\n"
    finally:
        if other.returncode is None:
            other.kill()
        await other.wait()


@pytest.mark.asyncio
async def test_idle_stop_exits_and_closes_dispatch(tmp_path: Path) -> None:
    context = ApplicationContext(
        AppSettings(), ProjectPaths(tmp_path, tmp_path / "data"), None, Console(), Console()
    )
    app = ForgeApp(context)
    async with app.run_test(size=(80, 24)) as pilot:
        await pilot.press("ctrl+shift+x")
        confirm = app.screen
        assert isinstance(confirm, Confirm)
        assert "Active operations: 0" in confirm.explanation
        assert "Observed managed processes: 0" in confirm.explanation
        await pilot.press("tab", "enter")
    assert app.controller.shutdown.emergency
    with pytest.raises(ValueError, match="Emergency shutdown"):
        app.controller.launch(Action("fixture", "Workflows"), {})


@pytest.mark.asyncio
async def test_owned_group_escalation_stops_multiple_workers(tmp_path: Path) -> None:
    from forge.historical.engine.process import Shutdown, shutdown

    # Pipe EOF after escalation also requires both workers to stop.
    worker = "import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(60)"
    coordinator = (
        "import signal,subprocess,sys,time; from pathlib import Path; "
        "signal.signal(signal.SIGTERM,signal.SIG_IGN); "
        "children=[subprocess.Popen([sys.executable,'-c',sys.argv[1]]) for _ in range(2)]; "
        "Path('group-ready').touch(); time.sleep(60)"
    )
    state = Shutdown()
    token = shutdown.set(state)
    try:
        execution = asyncio.create_task(
            supervise([sys.executable, "-c", coordinator, worker], tmp_path, tmp_path)
        )
    finally:
        shutdown.reset(token)
    async with asyncio.timeout(5):
        while not (tmp_path / "group-ready").exists():  # noqa: ASYNC110
            await asyncio.sleep(0.02)
    assert state.process_count() == 1
    state.emergency = True
    execution.cancel()
    assert await asyncio.wait_for(execution, timeout=3) == (-9, True)
    assert state.process_count() == 0
