"""Desktop observer for an application-owned Forge operation."""

import asyncio
import json
import sys
from collections.abc import Awaitable, Callable
from contextlib import suppress
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory

from rich.console import Console
from rich.text import Text
from textual import on
from textual.app import App, ComposeResult
from textual.containers import Horizontal, VerticalScroll
from textual.widgets import Button, Static

from forge.historical.archives.storage.files import disk
from forge.historical.pipelines.terminal import Ghostty, WindowUnavailable, close_window
from forge.tui.controller import Operation, document
from forge.tui.operation_view import dashboard
from forge.tui.screens import Confirm


def publish(path: Path, op: Operation, environment: str) -> None:
    console = Console(file=StringIO(), record=True, width=96, color_system="truecolor")
    console.print(Text(f"{environment.upper()} · {op.reference}"))
    console.print(dashboard(op, 96, 42))
    evidence = document(op.result)
    if evidence.get("report_path"):
        console.print(Text(f"Retained report: {evidence['report_path']}"))
    temporary = path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(
            {
                "renderable": console.export_text(styles=True),
                "finished": op.finished_at is not None,
            }
        )
    )
    temporary.replace(path)


async def observe(
    op: Operation,
    environment: str,
    cwd: Path,
    stop: Callable[[], Awaitable[None]],
    opened: Callable[[], None],
    *,
    inspect: Callable[[str], None] = lambda _: None,
) -> None:
    launcher = await disk(Ghostty.detect)
    with TemporaryDirectory(prefix="mf-monitor-", dir="/tmp") as temporary:
        root = Path(temporary)
        frame = root / "frame.json"
        publish(frame, op, environment)
        window = await launcher.launch_module(
            "forge.tui.execution_monitor", frame, cwd=cwd, title=f"MarketForge · {op.title}"
        )
        waiting = asyncio.create_task(window.wait())
        try:
            async with asyncio.timeout(8):
                while not (root / "ready").exists():
                    if waiting.done():
                        raise WindowUnavailable("Ghostty closed before the monitor was ready.")
                    await asyncio.sleep(0.1)
            opened()
            while not waiting.done():
                request = root / "stop"
                if request.exists():
                    request.unlink(missing_ok=True)
                    if op.finished_at is None:
                        await stop()
                for verb in ("run", "logs"):
                    marker = root / verb
                    if marker.exists():
                        marker.unlink(missing_ok=True)
                        inspect(verb)
                publish(frame, op, environment)
                await asyncio.sleep(0.3)
            if await waiting and op.finished_at is None:
                raise WindowUnavailable("The desktop monitor exited unexpectedly.")
        except TimeoutError:
            raise WindowUnavailable("Ghostty did not acknowledge the monitor in time.") from None
        finally:
            if not waiting.done():
                await close_window(window, waiting, grace=0)


class Monitor(App[None]):
    TITLE = "MarketForge · Execution monitor"
    CSS_PATH = "workspace.tcss"
    BINDINGS = [("escape", "quit", "Close view")]

    def __init__(self, frame: Path) -> None:
        super().__init__()
        self.frame = frame

    def compose(self) -> ComposeResult:
        with VerticalScroll():
            yield Static("", id="monitor-frame")
        with Horizontal(classes="screen-footer"):
            yield Button("Open run in workspace", id="monitor-run")
            yield Button("Logs in workspace", id="monitor-logs")
            yield Button("Request stop", id="monitor-stop", variant="error")
            yield Button("Close view", id="monitor-close")

    def on_mount(self) -> None:
        self.refresh_frame()
        self.frame.with_name("ready").touch()
        self.set_interval(0.3, self.refresh_frame)

    def refresh_frame(self) -> None:
        try:
            frame = json.loads(self.frame.read_text())
        except FileNotFoundError:
            self.exit()
            return
        self.query_one("#monitor-frame", Static).update(Text.from_ansi(frame["renderable"]))
        self.query_one("#monitor-stop", Button).disabled = frame["finished"]

    @on(Button.Pressed, "#monitor-run, #monitor-logs")
    def inspect(self, event: Button.Pressed) -> None:
        if event.button.id:
            with suppress(FileNotFoundError):
                self.frame.with_name(event.button.id.removeprefix("monitor-")).touch()

    @on(Button.Pressed, "#monitor-close")
    def close(self) -> None:
        self.exit()

    @on(Button.Pressed, "#monitor-stop")
    def stop(self) -> None:
        def confirm(accepted: bool | None) -> None:
            if accepted:
                with suppress(FileNotFoundError):
                    self.frame.with_name("stop").touch()

        self.push_screen(
            Confirm("Request stop", "Forge will interrupt the run and retain its evidence."),
            confirm,
        )


if __name__ == "__main__":
    Monitor(Path(sys.argv[1])).run()
