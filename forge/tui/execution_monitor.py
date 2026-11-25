"""Dispatch one reviewed operation to a terminal, then watch its retained identity."""

import asyncio
import json
import sys
from collections.abc import Callable
from contextlib import suppress
from dataclasses import asdict, dataclass
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import cast

from forge.historical.engine.process import cancellation_grace, require_dispatch, shutdown
from forge.historical.normalization.service import write_job
from forge.historical.pipelines.terminal import close_window
from forge.shared.terminal.window import GhosttyWindow, Window, WindowUnavailable
from forge.startup.context import ApplicationContext
from forge.tui.actions import Action, Values


@dataclass(frozen=True)
class TerminalRequest:
    arguments: list[str]
    root: str
    storage: str
    settings_file: str | None
    settings: dict[str, object]
    testing: bool
    registry: str | None
    reference: str
    reviewed_file: str | None
    reviewed_sha256: str | None


def terminal_arguments(action: Action, values: Values, *, testing: bool) -> list[str]:
    if action.key.startswith("pipeline "):
        values = {**values, "stage_terminal": values.get("stage_terminal") or "inline"}
    args = action.arguments(values, testing=testing)
    if args[0] in {"normalize", "merge", "convert", "pipeline"}:
        args.append("--tui")
    return args


def read_object(path: Path) -> dict[str, object]:
    return cast(dict[str, object], json.loads(path.read_bytes()))


async def run_in_terminal(
    action: Action,
    values: Values,
    context: ApplicationContext,
    registry: str | None,
    reviewed: tuple[Path, bytes] | None,
    update: Callable[[str | None, str], None],
) -> dict[str, object]:
    """Allow fallback only before sending permission to execute."""
    launcher = await asyncio.to_thread(GhosttyWindow.detect)
    request = TerminalRequest(
        terminal_arguments(action, values, testing=context.testing),
        str(context.paths.root),
        str(context.paths.data_root),
        str(context.settings_file) if context.settings_file else None,
        context.settings.model_dump(mode="json"),
        context.testing,
        registry,
        str(values.get("reference", "")),
        str(reviewed[0]) if reviewed else None,
        sha256(reviewed[1]).hexdigest() if reviewed else None,
    )
    with TemporaryDirectory(prefix="mf-terminal-", dir="/tmp") as temporary:
        directory = Path(temporary)
        write_job(directory / "request.json", asdict(request))
        loop = asyncio.get_running_loop()
        connection: asyncio.Future[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = (
            loop.create_future()
        )

        def connect(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            if connection.done():
                writer.close()
            else:
                connection.set_result((reader, writer))

        server: asyncio.Server | None = None
        window: Window | None = None
        waiting: asyncio.Task[int] | None = None
        reader: asyncio.StreamReader | None = None
        writer: asyncio.StreamWriter | None = None
        authorized = False
        keep_window = False
        diagnostics = directory / "ghostty.stderr"
        finishing: asyncio.Task[bytes] | None = None
        run_id: str | None = None

        def result() -> dict[str, object]:
            if (directory / "result.json").is_file():
                saved_result = read_object(directory / "result.json")
                if saved_result.get("status") == "completion unconfirmed":
                    if (state := shutdown.get()) is not None:
                        state.unverified = True
                return saved_result
            identity = run_id
            if (directory / "run.json").is_file():
                evidence = read_object(directory / "run.json")
                identity = str(evidence["run_id"])
                outcome = Path(str(evidence["outcome"]))
                if outcome.is_file():
                    saved = read_object(outcome)
                    return {
                        **saved,
                        "terminal_warning": "CLI exit unconfirmed. Retained run outcome recovered.",
                    }
            if (state := shutdown.get()) is not None:
                state.unverified = True
            return {
                "status": "completion unconfirmed",
                "run_id": identity,
                "error": "The terminal disconnected before reporting completion. Inspect Runs before retrying.",
            }

        try:
            server = await asyncio.start_unix_server(connect, path=directory / "control")
            launch = asyncio.create_task(
                launcher.launch_command(
                    (sys.executable, "-m", "forge.tui.execution_worker", temporary),
                    cwd=context.paths.root,
                    title=f"MarketForge · {action.key} · {request.reference}",
                    detached_lifetime=True,
                    hold=True,
                    diagnostics=diagnostics,
                )
            )
            try:
                window = await asyncio.shield(launch)
            except asyncio.CancelledError:
                window = await launch
                raise
            assert window is not None
            waiting = asyncio.create_task(window.wait())
            async with asyncio.timeout(15):
                done, _ = await asyncio.wait(
                    {connection, waiting}, return_when=asyncio.FIRST_COMPLETED
                )
                if connection not in done:
                    diagnostic = ""
                    if diagnostics.is_file():
                        with diagnostics.open("rb") as stream:
                            stream.seek(max(0, diagnostics.stat().st_size - 2048))
                            diagnostic = stream.read(2048).decode(errors="replace").strip()
                    message = (
                        f"Ghostty exited ({waiting.result()}) before the CLI worker connected."
                    )
                    raise WindowUnavailable(f"{message} {diagnostic}".strip())
                reader, writer = connection.result()
                if await reader.read(1) != b"R":
                    if (directory / "result.json").is_file():
                        keep_window = True
                        return result()
                    raise WindowUnavailable("The CLI worker closed before it was ready.")
                require_dispatch()
                authorized = True
                writer.write(b"G")
                await writer.drain()
            # Stop accepting new peers while keeping the authorized connection open.
            server.close()
            update(None, "Running in Ghostty · Closing the window interrupts execution")
            finishing = asyncio.create_task(reader.read(1))
            while not finishing.done():
                if run_id is None and (directory / "run.json").is_file():
                    run_id = str(read_object(directory / "run.json")["run_id"])
                    update(run_id, "Running in Ghostty")
                if waiting.done():
                    with suppress(OSError):
                        writer.write(b"I")
                        await writer.drain()
                    await asyncio.wait({finishing}, timeout=cancellation_grace(20, 2))
                    break
                await asyncio.wait({finishing, waiting}, timeout=0.2)
            keep_window = finishing.done() and finishing.result() == b"C"
            return result()
        except asyncio.CancelledError:
            if authorized and writer is not None and reader is not None:
                with suppress(OSError):
                    state = shutdown.get()
                    writer.write(b"E" if state and state.emergency else b"I")
                    await writer.drain()
                if finishing is None:
                    finishing = asyncio.create_task(reader.read(1))
                done, _ = await asyncio.wait({finishing}, timeout=cancellation_grace(20, 2))
                if done or (directory / "result.json").is_file():
                    keep_window = bool(
                        done
                        and not finishing.cancelled()
                        and not finishing.exception()
                        and finishing.result() == b"C"
                    )
                return result()
            raise
        except (OSError, TimeoutError) as error:
            if authorized:
                return result()
            raise WindowUnavailable(f"Could not start the Ghostty CLI terminal: {error}") from error
        finally:
            if server is not None:
                server.close()
            if finishing is not None and not finishing.done():
                finishing.cancel()
            if writer is None and connection.done() and not connection.cancelled():
                _, writer = connection.result()
            connection.cancel()
            if writer is not None:
                writer.close()
                with suppress(OSError):
                    await writer.wait_closed()
            if server is not None:
                await server.wait_closed()
            if window is not None and not keep_window:
                if waiting is None:
                    waiting = asyncio.create_task(window.wait())
                await close_window(window, waiting, grace=0.25)
