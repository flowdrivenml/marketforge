"""Actual map/download CLI with fixture HTTP and the real testing database."""

import gzip
import json
from collections.abc import AsyncGenerator, Callable, Coroutine
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest

from forge.cli.main import main
from forge.shared.config.models import HttpSettings
from forge.shared.http.client import HttpClient, open_http_client
from forge.shared.http.rate_limit import RequestRateLimiter
from forge.shared.ownership import WorkspaceOwnership
from forge.startup.context import build_context

pytestmark = [pytest.mark.integration, pytest.mark.database]


class ImmediateLimiter(RequestRateLimiter):
    async def wait(self) -> None:
        pass


Handler = (
    Callable[[httpx.Request], httpx.Response]
    | Callable[[httpx.Request], Coroutine[None, None, httpx.Response]]
)


def install_http(monkeypatch: pytest.MonkeyPatch, handler: Handler) -> None:
    @asynccontextmanager
    async def open_fixture(
        settings: HttpSettings,
        *,
        requests_per_second: float | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> AsyncGenerator[HttpClient, None]:
        async with open_http_client(
            limiter=ImmediateLimiter(1),
            max_concurrent=settings.max_concurrent,
            transport=httpx.MockTransport(handler),
        ) as http:
            yield http

    monkeypatch.setattr(
        "forge.historical.archives.operation.open_configured_http_client", open_fixture
    )


def run(workspace: Path, action: str, *flags: str) -> int:
    return main(
        [
            "--project-root",
            str(workspace),
            "--json",
            "acquisition",
            action,
            "request",
            "--testing",
            *flags,
        ]
    )


@pytest.mark.parametrize("empty", [False, True])
def test_map_then_fresh_mapping_avoids_http(
    workspace: Path,
    isolated_acquisition: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    empty: bool,
) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        links = "" if empty else f'<a href="{isolated_acquisition}-2026-09-01.csv.gz">file</a>'
        return httpx.Response(200, text=f"<pre><a href=../>../</a>{links}</pre>")

    install_http(monkeypatch, handler)
    assert isolated_acquisition
    assert run(workspace, "map") == 0
    first = capsys.readouterr()
    assert first.err == ""
    document = json.loads(first.out)
    assert document["requests"][0]["archives_found"] == (0 if empty else 1)
    assert run(workspace, "map") == 0
    second = json.loads(capsys.readouterr().out)
    assert second["requests"][0]["cache"] == "reused"
    assert calls == 1
    assert run(workspace, "map", "--refresh") == 0
    capsys.readouterr()
    assert calls == 2


def test_bulk_mixed_failure_then_verified_reuse(
    workspace: Path,
    isolated_acquisition: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    body_calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/"):
            return httpx.Response(
                200,
                text=f'<pre><a href=../>../</a><a href="{isolated_acquisition}-2026-09-01.csv.gz">good</a><a href="{isolated_acquisition}-2026-09-02.csv.gz">bad</a></pre>',
            )
        body_calls.append(request.url.path)
        if "2026-09-02" in request.url.path:
            return httpx.Response(404)
        return httpx.Response(200, stream=httpx.ByteStream(gzip.compress(b"time,price\n1,2\n")))

    install_http(monkeypatch, handler)
    assert isolated_acquisition
    assert run(workspace, "download", "--silent") == 1
    captured = capsys.readouterr()
    assert captured.err == ""
    report = json.loads(captured.out)
    assert report["file_counts"]["completed"] == 1
    assert report["file_counts"]["failed"] == 1
    outcomes = Path(report["archive_outcomes"]).read_text()
    assert '"status": "failed"' in outcomes
    assert '"remote_archive_id"' in outcomes
    assert run(workspace, "download") == 1
    second = json.loads(capsys.readouterr().out)
    assert second["file_counts"]["reused"] == 1
    assert sum("2026-09-01" in path for path in body_calls) == 1
    assert "signature" not in Path(report["report"]).read_text()


def test_silent_human_results_and_ownership_conflict(
    workspace: Path,
    isolated_acquisition: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    install_http(monkeypatch, lambda _: httpx.Response(200, text="<pre><a href=../>../</a></pre>"))
    assert isolated_acquisition
    code = main(
        ["--project-root", str(workspace), "acquisition", "map", "request", "--testing", "--silent"]
    )
    output = capsys.readouterr()
    assert code == 0
    assert "Mapping complete" in output.out
    assert "Files queued" not in output.err
    with WorkspaceOwnership(build_context(project_root=workspace).paths):
        assert run(workspace, "map") == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "Another historical" in json.loads(captured.err)["error"]["message"]


def test_progress_goes_to_stderr(
    workspace: Path,
    isolated_acquisition: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert isolated_acquisition
    install_http(monkeypatch, lambda _: httpx.Response(200, text="<pre><a href=../>../</a></pre>"))
    assert (
        main(["--project-root", str(workspace), "acquisition", "map", "request", "--testing"]) == 0
    )
    captured = capsys.readouterr()
    assert "Mapping complete" in captured.out
    assert "empty:" in captured.err


@pytest.mark.asyncio
async def test_bulk_workers_are_concurrent_and_bounded(
    workspace: Path, isolated_acquisition: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import asyncio
    import os

    from forge.historical.archives.config import load_acquisition
    from forge.historical.archives.operation import run_acquisition

    started = asyncio.Event()
    totals_known = asyncio.Event()
    from forge.historical.archives.progress import ProgressSnapshot

    def observe(snapshot: ProgressSnapshot) -> None:
        if not snapshot.provisional and snapshot.active:
            totals_known.set()

    release = asyncio.Event()
    active = 0
    peak = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, peak
        if request.url.path.endswith("/"):
            return httpx.Response(
                200,
                text="<pre><a href=../>../</a>"
                + "".join(
                    f'<a href="{isolated_acquisition}-2026-09-01_part{index}.csv.gz">file</a>'
                    for index in range(12)
                )
                + "</pre>",
            )
        active += 1
        peak = max(peak, active)
        if active == 2:
            started.set()
        await release.wait()
        active -= 1
        return httpx.Response(200, stream=httpx.ByteStream(gzip.compress(b"a,b\n1,2\n")))

    install_http(monkeypatch, handler)
    context = build_context(project_root=workspace)
    config = load_acquisition(context.paths.acquisition_config_dir / "request.toml")
    assert isolated_acquisition
    with WorkspaceOwnership(context.paths):
        task = asyncio.create_task(
            run_acquisition(
                context,
                config,
                dsn=os.environ["MARKETFORGE_DATABASE_URL_TEST"],
                download=True,
                silent=True,
                observer=observe,
            )
        )
        try:
            await asyncio.wait_for(started.wait(), 2)
            await asyncio.wait_for(totals_known.wait(), 2)
        finally:
            release.set()
        result = await asyncio.wait_for(task, 3)
    assert peak == 2
    assert result["file_counts"]["completed"] == 12


def test_bulk_requests_report_each_outcome(
    workspace: Path,
    request_file: Path,
    isolated_acquisition: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert isolated_acquisition
    request_file.write_text(
        request_file.read_text()
        + """
[[requests]]
exchange = "binance"
instrument_type = "spot"
market_category = "spot"
data_type = "order_book_l2"
[requests.scope]
kind = "instrument"
identifier = "BTCUSDT"
"""
    )
    install_http(monkeypatch, lambda _: httpx.Response(200, text="<pre><a href=../>../</a></pre>"))
    assert run(workspace, "map") == 1
    outcomes = json.loads(capsys.readouterr().out)["requests"]
    assert [outcome["status"] for outcome in outcomes] == ["complete", "failed"]
    assert "trades only" in outcomes[1]["error"]


def test_published_registration_failure_recovers_on_next_cli_invocation(
    workspace: Path,
    isolated_acquisition: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from psycopg import AsyncConnection
    from psycopg.rows import TupleRow

    import forge.historical.archives.storage.acquire as storage

    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        if request.url.path.endswith("/"):
            return httpx.Response(
                200,
                text=f'<pre><a href=../>../</a><a href="{isolated_acquisition}-2026-09-01.csv.gz">file</a></pre>',
            )
        calls += 1
        return httpx.Response(200, stream=httpx.ByteStream(gzip.compress(b"a,b\n1,2\n")))

    original = storage.complete

    async def fail_registration(connection: AsyncConnection[TupleRow], local_id: int) -> None:
        raise RuntimeError("Injected final registration failure")

    install_http(monkeypatch, handler)
    monkeypatch.setattr(storage, "complete", fail_registration)
    assert isolated_acquisition
    assert run(workspace, "download") == 1
    capsys.readouterr()
    monkeypatch.setattr(storage, "complete", original)
    assert run(workspace, "download") == 0
    second = json.loads(capsys.readouterr().out)
    assert second["file_counts"]["reused"] == 1
    assert calls == 1


@pytest.mark.asyncio
async def test_operation_cancellation_records_and_recovers(
    workspace: Path, isolated_acquisition: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import asyncio
    import os
    from collections.abc import AsyncIterator

    from forge.historical.archives.config import load_acquisition
    from forge.historical.archives.operation import run_acquisition

    started = asyncio.Event()
    blocked = True

    class BlockedBody(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            started.set()
            yield b"partial"
            await asyncio.Event().wait()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/"):
            return httpx.Response(
                200,
                text=f'<pre><a href=../>../</a><a href="{isolated_acquisition}-2026-09-01.csv.gz">file</a></pre>',
            )
        return httpx.Response(
            200, stream=BlockedBody() if blocked else httpx.ByteStream(gzip.compress(b"a,b\n1,2\n"))
        )

    install_http(monkeypatch, handler)
    context = build_context(project_root=workspace)
    config = load_acquisition(context.paths.acquisition_config_dir / "request.toml")
    config = config.model_copy(update={"requests": config.requests * 2})
    assert isolated_acquisition
    with WorkspaceOwnership(context.paths):
        task = asyncio.create_task(
            run_acquisition(
                context,
                config,
                dsn=os.environ["MARKETFORGE_DATABASE_URL_TEST"],
                download=True,
                silent=True,
            )
        )
        await asyncio.wait_for(started.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        reports = await asyncio.to_thread(
            lambda: list(context.paths.runs_dir.glob("*/report.json"))
        )
        assert len(reports) == 1
        report = json.loads(reports[0].read_text())
        assert report["status"] == "cancelled"
        assert report["file_counts"]["cancelled"] == 2
        assert all(item["status"] == "cancelled" for item in report["requests"])
        assert report["request_counts"]["cancelled"] == 2
        assert report["request_counts"]["successful"] == 0
        assert '"status": "cancelled"' in await asyncio.to_thread(
            Path(report["archive_outcomes"]).read_text
        )
        blocked = False
        recovered = await run_acquisition(
            context,
            config,
            dsn=os.environ["MARKETFORGE_DATABASE_URL_TEST"],
            download=True,
            silent=True,
        )
    assert recovered["file_counts"]["completed"] == 1


def test_verified_reuse_never_refreshes_expired_url_and_renderer_cannot_fail_operation(
    workspace: Path,
    isolated_acquisition: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import asyncio
    import os

    from forge.shared.database.testing import testing_connection

    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if request.url.path.endswith("/"):
            return httpx.Response(
                200,
                text=f'<pre><a href=../>../</a><a href="{isolated_acquisition}-2026-09-01.csv.gz">file</a></pre>',
            )
        return httpx.Response(200, stream=httpx.ByteStream(gzip.compress(b"a,b\n1,2\n")))

    install_http(monkeypatch, handler)
    assert run(workspace, "download") == 0
    capsys.readouterr()
    before = calls

    async def expire() -> None:
        async with (
            testing_connection(os.environ["MARKETFORGE_DATABASE_URL_TEST"]) as connection,
            connection.transaction(),
        ):
            await connection.execute(
                "UPDATE historical.remote_archives SET url_expires_at = NOW() - INTERVAL '1 day' WHERE scope_json->>'scope_identifier' = %s",
                (isolated_acquisition,),
            )

    asyncio.run(expire())

    def broken_update(self: object, snapshot: object) -> None:
        raise OSError("terminal failed")

    monkeypatch.setattr(
        "forge.shared.terminal.acquisition.AcquisitionDisplay.update", broken_update
    )
    assert run(workspace, "download") == 0
    document = json.loads(capsys.readouterr().out)
    assert document["file_counts"]["reused"] == 1
    assert calls == before
    assert document["request_counts"]["successful"] == 1


@pytest.mark.asyncio
async def test_independent_requests_overlap_and_coalesce_duplicate_acquisitions(
    workspace: Path, isolated_acquisition: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import asyncio
    import os

    from forge.historical.archives.config import load_acquisition
    from forge.historical.archives.operation import run_acquisition
    from forge.historical.archives.progress import ProgressSnapshot

    context = build_context(project_root=workspace)
    config = load_acquisition(context.paths.acquisition_config_dir / "request.toml")
    second = config.requests[0].model_copy(update={"data_type": "order_book_l2"})
    config = config.model_copy(
        update={"requests": [config.requests[0], second, config.requests[0]]}
    )
    both = asyncio.Event()
    release = asyncio.Event()
    paths: set[str] = set()
    bodies = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal bodies
        if request.url.path.endswith("/"):
            paths.add(request.url.host or "")
            if len(paths) == 2:
                both.set()
            await release.wait()
            filename = (
                f"2026-09-01_{isolated_acquisition}_ob200.data.zip"
                if "orderbook" in request.url.path
                else f"{isolated_acquisition}-2026-09-01.csv.gz"
            )
            return httpx.Response(
                200, text=f'<pre><a href=../>../</a><a href="{filename}">file</a></pre>'
            )
        bodies += 1
        return httpx.Response(200, stream=httpx.ByteStream(gzip.compress(b"a,b\n1,2\n")))

    install_http(monkeypatch, handler)
    max_active = 0

    def observe(snapshot: ProgressSnapshot) -> None:
        nonlocal max_active
        max_active = max(max_active, len(snapshot.active_requests))
        if snapshot.requests.finished < 2:
            assert snapshot.provisional or len(paths) == 2

    task = asyncio.create_task(
        run_acquisition(
            context,
            config,
            dsn=os.environ["MARKETFORGE_DATABASE_URL_TEST"],
            download=True,
            silent=True,
            observer=observe,
        )
    )
    try:
        await asyncio.wait_for(both.wait(), 2)
    finally:
        release.set()
    report = await task
    assert max_active >= 2
    assert bodies == 2
    assert report["file_counts"]["reused"] == 1
    assert [r["index"] for r in report["requests"]] == [0, 1, 2]
