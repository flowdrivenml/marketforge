"""Portable pipeline contracts, state transitions, rendering, and process ownership."""

import asyncio
import io
import os
import socket
import sys
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError
from rich.console import Console

from forge.historical.archives.progress import FileCounts, ProgressSnapshot, RequestCounts
from forge.historical.archives.selection import AcquiredArchive, ArchiveSelection
from forge.historical.engine.process import supervise
from forge.historical.normalization.service import write_job
from forge.historical.pipelines import dashboard, service
from forge.historical.pipelines.config import load_recipe
from forge.shared.config.models import AppSettings
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths
from forge.startup.context import ApplicationContext

EXAMPLE = Path(__file__).with_name("fixtures") / "bybit_pipeline.toml"


@pytest.fixture
def run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> service.PipelineRun:
    from collections.abc import Awaitable, Callable
    from types import SimpleNamespace

    async def execute(_: object, call: Callable[[], Awaitable[dict[str, Any]]]) -> dict[str, Any]:
        return await call()

    monkeypatch.setattr(service, "show_index", AsyncMock(return_value=None))
    monkeypatch.setattr(
        service, "begin", AsyncMock(return_value=SimpleNamespace(registry="testing"))
    )
    monkeypatch.setattr(service, "complete_call", execute)
    console = Console(file=io.StringIO())
    context = ApplicationContext(
        AppSettings(), ProjectPaths(tmp_path, tmp_path / "data"), None, console, console
    )
    return service.new(context, "example", EXAMPLE.read_bytes())


def test_example_compiles_to_exact_shared_requests() -> None:
    recipe = load_recipe(EXAMPLE.read_bytes())
    acquisition, normalization = recipe.acquisition(), recipe.normalization()
    assert acquisition.interval == normalization.interval == recipe.interval
    assert len(acquisition.requests) == 8
    assert {(x.scope.identifier, x.data_type) for x in acquisition.requests} == {
        (symbol, kind)
        for symbol in ("BTCUSDT", "BTCUSDC", "ETHUSDT", "ETHBTC")
        for kind in ("trades", "order_book_l2")
    }
    assert [x.model_dump() for x in acquisition.requests] == [
        x.model_dump(exclude={"source_key"}) for x in normalization.requests
    ]
    assert recipe.merging([2**60 + 9]).dataset_ids == [2**60 + 9]
    assert recipe.merging([7]).timeline_ordering == "causal"


@pytest.mark.parametrize(
    "before,after",
    [
        (b"version = 1", b"version = 4"),
        (b"version = 1", b"version = true"),
        (b"[merge]", b"[merge]\nmode = 'combined'"),
        (b'"causal"', b'"unrecognized"'),
        (b"[interval]", b"[interval]\nunknown = true"),
    ],
)
def test_invalid_pipeline_recipe(before: bytes, after: bytes) -> None:
    with pytest.raises(ValidationError):
        load_recipe(EXAMPLE.read_bytes().replace(before, after))


@pytest.mark.asyncio
async def test_partial_result_cannot_unlock_merge(
    run: service.PipelineRun, monkeypatch: pytest.MonkeyPatch
) -> None:
    request = run.recipe.requests[0]
    selection = ArchiveSelection(
        acquisition_report=run.artifacts.report,
        archives=[
            AcquiredArchive(
                request=request,
                local_archive_id=21,
                remote_archive_id=90,
                relative_path="archive.csv.gz",
                sha256="a" * 64,
                source_object_key="spot/sample",
            ),
        ],
    )
    load = AsyncMock(return_value=selection)
    prepare = AsyncMock(
        return_value={
            "status": "partial",
            "engine_ready": False,
            "report_path": str(run.artifacts.report),
        }
    )
    merge = AsyncMock(side_effect=AssertionError("Merge must remain pending"))
    monkeypatch.setattr(run, "acquire", load)
    monkeypatch.setattr(run, "freeze", lambda: run.report.update(engines={}, settings_files={}))

    def engine(_: str) -> Path:
        return run.artifacts.report

    monkeypatch.setattr(run, "engine", engine)
    monkeypatch.setattr(service, "registry_identity", AsyncMock(return_value="testing"))
    monkeypatch.setattr(service, "prepare_batch", prepare)
    monkeypatch.setattr(run, "merge", merge)
    report = await run.run()
    assert report["state"] == "blocked" and report["stages"]["merge"]["state"] == "pending"
    assert prepare.await_args is not None
    assert prepare.await_args.kwargs["pinned"] is selection
    assert prepare.await_args.args[1].interval == run.recipe.interval
    merge.assert_not_called()
    assert service.status(run.paths, report["run_id"]) == report


def test_failed_registration_and_partial_engine_result_are_distinct(
    run: service.PipelineRun,
) -> None:
    run.transition("normalize", "register", "running")
    with pytest.raises(service.Blocked, match="offline"):
        run.finish(
            "normalize",
            {
                "status": "failed",
                "verification": "verified",
                "registration": "pending",
                "registration_error": "offline",
            },
        )
    with pytest.raises(MarketForgeError, match="Processing"):
        run.finish(
            "normalize",
            {
                "status": "partial",
                "registration": "registered",
                "catalogue_dataset_ids": {"should-not-advance": 20},
            },
        )
    assert "primary_file_ids" not in run.stage("normalize")


def test_resume_holds_snapshot_and_rejects_tampering(run: service.PipelineRun) -> None:
    restored = service.resume(run.context, run.report["run_id"])
    assert restored.recipe == run.recipe
    assert restored.context.settings == run.context.settings
    run.artifacts.submitted_config.write_text("version = 9")
    with pytest.raises(service.Blocked, match="Retained artifact changed"):
        service.resume(run.context, run.report["run_id"])


@pytest.mark.asyncio
async def test_interrupted_validation_resumes_retained_job_and_ranking(
    run: service.PipelineRun,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selection = ArchiveSelection(acquisition_report=run.artifacts.report, archives=[])
    monkeypatch.setattr(service, "registry_identity", AsyncMock(return_value="testing"))
    monkeypatch.setattr(service.PipelineRun, "acquire", AsyncMock(return_value=selection))
    run.report.update(engines={}, settings_files={})

    def engine(self: service.PipelineRun, _: str) -> Path:
        return self.artifacts.report

    monkeypatch.setattr(service.PipelineRun, "engine", engine)
    candidate = run.artifacts.steps_dir / "candidate.json"
    job = run.artifacts.steps_dir / "normalize-batch.json"
    candidate_doc: dict[str, Any] = {
        "batch_path": str(job),
        "report_path": str(candidate),
        "ranking_snapshot": "old-snapshot",
        "status": "prepared",
        "engine_ready": False,
        "prepared_job_count": 1,
    }

    async def interrupted(*_: object, **options: Any) -> dict[str, Any]:
        write_job(job, {"ranking_snapshot": "old-snapshot", "job_id": 43})
        write_job(candidate, candidate_doc)
        options["on_candidate"](job, candidate)
        options["lifecycle"]("validate")
        raise asyncio.CancelledError

    planner = AsyncMock(side_effect=interrupted)
    monkeypatch.setattr(service, "prepare_batch", planner)
    first = await run.run()
    assert first["state"] == "interrupted"
    assert first["stages"]["normalize"]["phase"] == "validate"

    validated = run.artifacts.steps_dir / "prepare-report.json"

    async def validate(
        _: object, source: Path, report: dict[str, Any], *rest: object, **options: Any
    ) -> dict[str, Any]:
        assert source == job and report["ranking_snapshot"] == "old-snapshot"
        options["lifecycle"]("validate")
        final = dict(report, engine_ready=True, report_path=str(validated))
        write_job(validated, final)
        return final

    monkeypatch.setattr(service, "validate_normalization_candidate", validate)
    restored = service.resume(run.context, first["run_id"])
    stopped = await restored.run(stop=True)
    assert stopped["state"] == "stopped"
    assert stopped["stages"]["normalize"]["prepared"]["job"] == str(job)
    assert stopped["stages"]["merge"]["state"] == "pending"
    planner.assert_awaited_once()


@pytest.mark.parametrize("width,height", [(130, 34), (80, 24), (65, 16), (35, 9)])
def test_dashboard_renders_provisional_acquisition(
    run: service.PipelineRun, width: int, height: int
) -> None:
    run.report["state"] = "running"
    run.transition("acquire", "discover_and_acquire", "running")
    run.acquisition = ProgressSnapshot(
        RequestCounts(finished=1),
        FileCounts(completed=2, reused=1),
        8,
        True,
        "BTCUSDT",
        "discovering",
        (),
        (),
        (),
        False,
        "activity",
        known_total=3,
    )
    output = io.StringIO()
    console = Console(file=output, width=width, height=height, color_system=None)
    console.print(dashboard.render(run.snapshot(), width, height))
    rendered = output.getvalue()
    assert "RUNNING" in rendered and "1 / 8 requests finished" in rendered
    assert "discovery in progress" in rendered and "Ctrl+C" in rendered
    assert "\x1b" not in rendered
    if width >= 65:
        assert "Normalization" in rendered and "Merge" in rendered
    assert all(len(line) <= width for line in rendered.splitlines())


@pytest.mark.asyncio
async def test_presentation_fallback_preserves_authoritative_result(
    run: service.PipelineRun,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result: dict[str, Any] = {"state": "complete", "outputs": [125]}

    async def execute(*, stop: bool) -> dict[str, Any]:
        await asyncio.sleep(0)
        return result

    def broken(*_: object) -> None:
        raise RuntimeError("render failure")

    monkeypatch.setattr(run, "run", execute)
    monkeypatch.setattr(dashboard.Live, "start", broken)
    output = io.StringIO()
    terminal = Console(file=output, force_terminal=True)
    assert await dashboard.present(run, terminal, tui=True, stop=False) is result
    assert await dashboard.present(run, terminal, tui=False, stop=False) is result
    assert "Execution continues in plain mode" in output.getvalue()


@pytest.mark.asyncio
async def test_cancel_signals_child_and_joins_diagnostic_readers(tmp_path: Path) -> None:
    source = """
import os, signal, socket, sys
def stop(signum, frame):
    print("finished during cancellation", file=sys.stderr, flush=True)
    sys.exit(130)
signal.signal(signal.SIGTERM, stop)
print("ready", flush=True)
with socket.create_connection(("127.0.0.1", int(sys.argv[1]))) as ready:
    ready.sendall(str(os.getpid()).encode())
while True:
    signal.pause()
"""

    async def receive(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        async with asyncio.timeout(2):
            pid_data = await reader.read(128)
        pid_ready.set_result(int(pid_data))
        writer.close()
        await writer.wait_closed()

    pid_ready: asyncio.Future[int] = asyncio.get_running_loop().create_future()
    server = await asyncio.start_server(receive, "127.0.0.1", 0, family=socket.AF_INET)
    port = server.sockets[0].getsockname()[1]
    task = asyncio.create_task(
        supervise([sys.executable, "-c", source, str(port)], tmp_path, tmp_path, grace=1)
    )
    try:
        pid = await asyncio.wait_for(pid_ready, timeout=5)
        task.cancel()
        assert await task == (130, True)
        assert (tmp_path / "engine-stdout.log").read_text() == "ready\n"
        assert (tmp_path / "engine-stderr.log").read_text() == "finished during cancellation\n"
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
    finally:
        server.close()
        await server.wait_closed()
        if not task.done():
            task.cancel()
            await task
