"""Exercise authorization, terminal ownership and the CLI argument boundary."""

import asyncio
from pathlib import Path

import pytest
from rich.console import Console

from forge.cli.main import build_parser
from forge.historical.normalization.service import write_job
from forge.shared.config.models import AppSettings
from forge.shared.paths import ProjectPaths
from forge.shared.terminal.window import WindowUnavailable
from forge.startup.context import ApplicationContext
from forge.tui import execution_monitor
from forge.tui.actions import BY_KEY, EXECUTION_ACTIONS, Values
from forge.tui.controller import Controller


@pytest.mark.parametrize("key", sorted(EXECUTION_ACTIONS))
def test_native_execution_uses_cli_parser_and_reviewed_options(key: str) -> None:
    values: Values = {"reference": "my reviewed recipe"}
    if key.startswith("pipeline"):
        values["stage_terminal"] = "window"
        if key.endswith(" run"):
            values["stop_after"] = "normalize.prepare"
    args = execution_monitor.terminal_arguments(BY_KEY[key], values, testing=True)
    assert "my reviewed recipe" in args
    parsed = build_parser().parse_args(args)
    assert parsed.testing
    assert not parsed.as_json
    if key.startswith(("normalize", "merge", "convert", "pipeline")):
        assert parsed.tui
    if key.startswith("pipeline"):
        assert parsed.stage_terminal == "window"
    if key == "pipeline run":
        assert parsed.stop_after == "normalize.prepare"


class Desktop:
    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.exit = asyncio.Event()
        self.authorized = asyncio.Event()
        self.release = asyncio.Event()
        self.peer: asyncio.Task[None] | None = None
        self.instructions: list[bytes] = []
        self.launches = 0

    async def launch_command(
        self,
        argv: tuple[str, ...],
        *,
        cwd: Path,
        title: str,
        detached_lifetime: bool,
        hold: bool,
        diagnostics: Path,
    ) -> "Desktop":
        assert argv[1:3] == ("-m", "forge.tui.execution_worker")
        assert hold and detached_lifetime
        directory = Path(argv[-1])
        request = execution_monitor.read_object(directory / "request.json")
        assert request["root"] == str(cwd)
        assert request["registry"] == "isolated-registry"
        assert "fixture" in title
        self.launches += 1
        if self.mode == "startup":
            await asyncio.to_thread(diagnostics.write_text, "Fixture graphical startup error.")
            self.exit.set()
        else:
            self.peer = asyncio.create_task(self.worker(directory))
        return self

    async def worker(self, directory: Path) -> None:
        reader, writer = await asyncio.open_unix_connection(directory / "control")
        try:
            writer.write(b"R")
            await writer.drain()
            assert await reader.read(1) == b"G"
            write_job(
                directory / "run.json",
                {"run_id": "merge-fixture", "outcome": str(directory / "outcome.json")},
            )
            self.authorized.set()
            if self.mode == "lost":
                self.exit.set()
                return
            status = "completed"
            if self.mode in {"cancel", "close", "emergency"}:
                if self.mode == "close":
                    self.exit.set()
                self.instructions.append(await reader.read(1))
                status = "interrupted"
            else:
                await self.release.wait()
            write_job(directory / "result.json", {"status": status, "run_id": "merge-fixture"})
            writer.write(b"C")
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    async def wait(self) -> int:
        await self.exit.wait()
        return 23 if self.mode == "startup" else 0

    def terminate(self) -> None:
        self.exit.set()

    def kill(self) -> None:
        self.exit.set()


@pytest.fixture
def controller(tmp_path: Path) -> Controller:
    context = ApplicationContext(
        AppSettings(), ProjectPaths(tmp_path, tmp_path / "data"), None, Console(), Console(), True
    )

    async def editor(_: Path) -> None:
        pytest.fail("Execution must not open an editor")

    async def invoke(*_: object) -> dict[str, str]:
        return {"status": "fallback"}

    controller = Controller(context, editor, invoke)
    controller.registry_identity = "isolated-registry"
    return controller


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["complete", "close", "cancel", "emergency", "lost", "startup"])
async def test_dispatch_once_then_reconcile(
    controller: Controller, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    desktop = Desktop(mode)
    monkeypatch.setattr(execution_monitor.GhosttyWindow, "detect", lambda: desktop)
    task = controller.launch(BY_KEY["merge run"], {"reference": "fixture"}, desktop=True)
    try:
        if mode != "startup":
            await asyncio.wait_for(desktop.authorized.wait(), 3)
        if mode == "cancel":
            await controller.cancel()
        elif mode == "emergency":
            assert await controller.emergency_stop()
        elif mode == "complete":
            assert controller.active()
            desktop.release.set()
        result = await asyncio.wait_for(task, 3)
        assert desktop.launches == 1
        assert not controller.active()
        if mode == "startup":
            assert result.state == "fallback"
            assert any("Desktop terminal unavailable" in event for _, event in result.events)
            return
        assert result.run_id == "merge-fixture"
        assert result.state == {
            "complete": "completed",
            "lost": "completion unconfirmed",
        }.get(mode, "interrupted")
        if mode == "complete":
            assert not desktop.exit.is_set()
        if mode in {"cancel", "close", "emergency"}:
            assert desktop.instructions == [b"E" if mode == "emergency" else b"I"]
    finally:
        desktop.exit.set()
        if desktop.peer:
            await desktop.peer


@pytest.mark.asyncio
async def test_startup_failure_is_explicit(
    controller: Controller, monkeypatch: pytest.MonkeyPatch
) -> None:
    desktop = Desktop("startup")
    monkeypatch.setattr(execution_monitor.GhosttyWindow, "detect", lambda: desktop)

    def opened(_run: str | None, _phase: str) -> None:
        pytest.fail("Failed window was reported ready")

    with pytest.raises(
        WindowUnavailable, match="before the CLI worker connected.*Fixture graphical startup error"
    ):
        await execution_monitor.run_in_terminal(
            BY_KEY["convert run"],
            {"reference": "fixture"},
            controller.context,
            controller.registry_identity,
            None,
            opened,
        )


@pytest.mark.asyncio
async def test_workspace_stays_navigable_after_native_dispatch(
    controller: Controller, monkeypatch: pytest.MonkeyPatch
) -> None:
    from forge.tui.app import ForgeApp

    desktop = Desktop("complete")
    monkeypatch.setattr(execution_monitor.GhosttyWindow, "detect", lambda: desktop)
    app = ForgeApp(controller.context, invoke=controller.invoke)
    monkeypatch.setattr(app, "check_registry", lambda: None)
    app.controller.registry_identity = controller.registry_identity
    async with app.run_test(size=(100, 30)) as pilot:
        with monkeypatch.context() as native:
            native.setattr(ForgeApp, "is_headless", property(lambda _: False))
            task = asyncio.create_task(
                app.start_action(BY_KEY["merge run"], {"reference": "fixture"})
            )
            try:
                await asyncio.wait_for(desktop.authorized.wait(), 3)
                assert app.screen is app.root_screen and app.controller.active()
                await pilot.click("#area-1")
                assert app.screen is app.root_screen and app.controller.active()
                desktop.release.set()
                await asyncio.wait_for(task, 3)
                assert app.controller.current is not None
                assert app.controller.current.run_id == "merge-fixture"
                assert app.controller.current.state == "completed"
                assert not desktop.exit.is_set()
            finally:
                desktop.release.set()
                desktop.exit.set()
                await task
                if desktop.peer is not None:
                    await desktop.peer
