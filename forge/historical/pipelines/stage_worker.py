"""Private entry point for a pipeline stage authorized over a local socket."""

from __future__ import annotations

import asyncio
import os
import signal
import socket
import sys
from contextlib import ExitStack, suppress
from datetime import datetime
from pathlib import Path
from uuid import UUID

from rich.text import Text

from forge.historical.engine.managed import checksum
from forge.historical.engine.process import Shutdown, joined_io, shutdown, supervise
from forge.historical.normalization.service import write_job
from forge.historical.pipelines import service
from forge.historical.pipelines.terminal import STAGE_LABELS, StageRequest
from forge.orchestration.registry import Execution, active
from forge.shared.config.models import AppSettings
from forge.shared.database.connection import (
    DatabaseError,
    database_dsn,
    database_environment,
    require_registry,
)
from forge.shared.errors import MarketForgeError
from forge.shared.identity import RunOperation
from forge.shared.paths import ProjectPaths
from forge.shared.terminal.console import create_console
from forge.startup.context import ApplicationContext


def verify_locks(paths: ProjectPaths, descriptors: list[int]) -> None:
    locks = tuple(
        dict.fromkeys((paths.root / ".marketforge.lock", paths.data_root / ".marketforge.lock"))
    )
    if len(descriptors) != len(locks):
        raise MarketForgeError("Stage worker did not receive both workspace ownership locks.")
    for path, fd in zip(locks, descriptors, strict=True):
        if not os.path.samestat(os.fstat(fd), path.stat(follow_symlinks=False)):
            raise MarketForgeError("Stage workspace ownership changed before execution.")


async def prepare(
    request: StageRequest, paths: ProjectPaths
) -> tuple[service.PipelineRun, Execution]:
    artifacts = service.artifacts_for(paths, request.run_id)
    if checksum(artifacts.report) != request.report_sha256:
        raise MarketForgeError("The pipeline checkpoint changed before the stage started.")
    report = service.status(paths, request.run_id)
    attempt = UUID(request.pipeline_attempt)
    intent_path = artifacts.root / "attempts" / str(attempt) / "intent.json"
    intent = service.read(intent_path)
    if (
        request.stage not in report["stages"]
        or report["run_id"] != request.run_id
        or report["state"] != "running"
        or report.get("terminal_dispatch") != request.identity()
        or report["environment"] != request.environment
        or intent["run_id"] != request.run_id
        or intent["operation"] != RunOperation.PIPELINE
        or intent["attempt_id"] != str(attempt)
        or intent["environment"] != request.environment
        or intent["registry_id"] != request.registry_id
        or Path(request.outcome)
        != intent_path.parent / "stage-windows" / f"{request.stage}-{request.dispatch_id}.json"
    ):
        raise MarketForgeError("Stage worker request does not match the retained pipeline attempt.")
    retained = service.retained(artifacts.resolved_plan, report["resolved_sha256"])
    settings = AppSettings.model_validate(service.read(retained)["settings"])
    context = ApplicationContext(
        settings,
        paths,
        None,
        create_console(settings.presentation),
        create_console(settings.presentation, stderr=True),
        testing=request.environment == "testing",
    )
    run = service.resume(context, request.run_id)
    registry = await service.registry_identity(paths)
    require_registry(report, registry)
    if request.registry_id != registry:
        raise DatabaseError("The stage worker requires the original verified registry.")
    execution = Execution(
        paths,
        request.run_id,
        RunOperation.PIPELINE,
        intent["action"],
        attempt,
        intent["number"],
        datetime.fromisoformat(intent["started_at"]),
        registry,
        request.environment,
        artifacts.root,
        database_dsn(paths.root),
    )
    return run, execution


def verify_engine_request(request: StageRequest, run: service.PipelineRun) -> None:
    engine = request.engine
    assert engine is not None
    directory = Path(engine.directory)
    binary = Path(engine.argv[0])
    intent_path = directory / "execution-intent.json"
    stage = run.stage(request.stage)
    if (
        request.stage not in {"normalize", "merge", "convert"}
        or not directory.resolve().is_relative_to(run.paths.runs_dir.resolve())
        or binary != directory / f"marketforge-{request.stage}"
        or checksum(intent_path) != engine.intent_sha256
        or stage["phase"] != "execute"
    ):
        raise MarketForgeError("Engine command does not match the retained stage execution.")
    intent = service.read(intent_path)
    submitted = intent.get("submission", {})
    input_path = Path(submitted.get("path") or intent["submission_path"])
    option = "--batch" if request.stage == "normalize" else "--job"
    if (
        intent["registry_id"] != request.registry_id
        or checksum(binary) != intent["executable_sha256"]
        or input_path.parent != directory
        or engine.argv[engine.argv.index(option) + 1] != str(input_path)
        or checksum(input_path) != (submitted.get("sha256") or intent["submitted_sha256"])
        or request.stage != "convert"
        and stage.get("execution_intent") != str(intent_path)
    ):
        raise MarketForgeError("Engine submission differs from the retained stage intent.")
    if runtime_digest := intent.get("configuration_sha256"):
        runtime = Path(engine.argv[engine.argv.index("--config") + 1])
        if runtime.parent != directory or checksum(runtime) != runtime_digest:
            raise MarketForgeError("Engine runtime differs from the retained stage intent.")


async def run_worker(
    request: StageRequest, channel: socket.socket, paths: ProjectPaths, startup_error: Path
) -> int:
    loop = asyncio.get_running_loop()
    current = asyncio.current_task()
    assert current is not None
    requested = False
    shutdown_state = Shutdown()

    def cancel() -> None:
        nonlocal requested
        if not requested:
            requested = True
            current.cancel()

    for number in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        loop.add_signal_handler(number, cancel)
    try:
        run, execution = await prepare(request, paths)
        engine = request.engine
        if engine is None and request.stage != "acquire":
            raise MarketForgeError("An Engine stage requires a pinned execution command.")
        if engine is not None:
            await joined_io(verify_engine_request, request, run)
    except (MarketForgeError, OSError, ValueError, KeyError, TypeError) as error:
        write_job(startup_error, {"error": str(error)})
        raise
    channel.setblocking(False)
    await loop.sock_sendall(channel, b"R")
    if await loop.sock_recv(channel, 1) != b"G":
        return 2
    await loop.sock_sendall(channel, b"S")

    async def watch_parent() -> None:
        try:
            shutdown_state.emergency = await loop.sock_recv(channel, 1) == b"E"
        finally:
            cancel()

    watcher = asyncio.create_task(watch_parent())
    state, message = "complete", None
    run.stage_window = True
    console = run.context.error_console
    console.print(
        Text(
            f"MarketForge · {STAGE_LABELS[request.stage]} · {run.report['name']} · {request.environment.upper()}"
        )
    )
    console.print(Text(f"Pipeline run {request.run_id}"))
    engine_result: tuple[int, bool] | None = None
    try:
        with active(execution):
            if engine is None:
                await run.acquire_with_display()
            else:
                shutdown.set(shutdown_state)
                engine_result = await supervise(
                    engine.argv,
                    paths.root,
                    Path(engine.directory),
                    tui=engine.tui,
                    foreground=True,
                )
                if engine_result[1]:
                    state, message = "interrupted", "Engine execution interrupted."
                elif engine_result[0]:
                    state, message = "failed", f"Engine exited with status {engine_result[0]}."
                else:
                    console.print(
                        Text("Engine completed. Forge is checking and recording the output.")
                    )

    except (asyncio.CancelledError, KeyboardInterrupt):
        state, message = (
            "interrupted",
            "Stage interrupted. Resume to reconcile any committed output.",
        )
    except (service.Blocked, service.ArchiveSelectionError, DatabaseError) as error:
        state, message = "blocked", str(error)
    except (MarketForgeError, OSError, ValueError, KeyError, TypeError) as error:
        state, message = "failed", str(error)
    finally:
        # Cancel the watcher after disabling its cancellation callback.
        requested = True
        watcher.cancel()
        with suppress(asyncio.CancelledError, OSError):
            await watcher
    if engine is None:
        run.save()
    stage = run.stage(request.stage)
    write_job(
        Path(request.outcome),
        {
            "identity": request.identity(),
            "state": state,
            "error": message,
            "report": str(run.artifacts.report),
            "report_sha256": checksum(run.artifacts.report)
            if engine is None
            else request.report_sha256,
            "exit_code": (
                engine_result or (130 if state == "interrupted" else 1, state == "interrupted")
            )[0],
            "interrupted": (engine_result or (1, state == "interrupted"))[1],
            "stage_attempt": stage.get("attempts", [{}])[-1].get("id")
            if stage.get("attempts")
            else None,
            "stage_run": stage.get("child_run_id"),
            "stage_report": stage.get("child_report") or stage.get("result", {}).get("report_path"),
        },
    )
    console.print(
        Text(
            f"{STAGE_LABELS[request.stage]} · {state.upper()} · {message or stage.get('summary', '')}"
        )
    )
    with suppress(OSError):
        await loop.sock_sendall(channel, b"C")
        if engine is not None and await loop.sock_recv(channel, 1) == b"F":
            final = service.status(paths, request.run_id)["stages"][request.stage]
            console.print(
                Text(
                    f"{STAGE_LABELS[request.stage]} · {final['state'].upper()} · {final.get('summary', '')}"
                )
            )
    return {"complete": 0, "blocked": 2, "failed": 1, "interrupted": 130}[state]


def main(path: Path) -> int:
    from forge.historical.pipelines.terminal import EngineRequest

    document = service.read(path)
    if document["engine"] is not None:
        document["engine"] = EngineRequest(**document["engine"])
    request = StageRequest(**document)
    if request.environment not in {"testing", "production"} or Path.cwd() != Path(
        request.working_directory
    ):
        raise MarketForgeError(
            "Stage worker environment or working directory does not match its request."
        )
    paths = ProjectPaths.resolve(
        root=Path(request.project_root), data_root=Path(request.storage_root)
    )
    with ExitStack() as stack:
        channel = stack.enter_context(socket.socket(socket.AF_UNIX, socket.SOCK_STREAM))
        channel.settimeout(15)
        channel.connect(request.channel)
        message, descriptors, flags, _ = socket.recv_fds(channel, 1, 2)
        for descriptor in descriptors:
            stack.callback(os.close, descriptor)
            os.set_inheritable(descriptor, False)
        if message != b"L" or flags & socket.MSG_CTRUNC:
            raise MarketForgeError("Invalid stage worker ownership message.")
        verify_locks(paths, descriptors)
        stack.enter_context(database_environment(testing=request.environment == "testing"))
        signal.signal(signal.SIGTSTP, signal.SIG_IGN)
        return asyncio.run(
            run_worker(request, channel, paths, path.with_name("startup-error.json"))
        )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(2)
    try:
        raise SystemExit(main(Path(sys.argv[1])))
    except (MarketForgeError, OSError, ValueError, KeyError, TypeError) as error:
        print(f"Stage worker: {error}", file=sys.stderr)
        raise SystemExit(2) from None
