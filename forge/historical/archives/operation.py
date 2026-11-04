"""Bulk map/download orchestration, bounded workers, and durable run reports."""

import asyncio
import json
from collections import deque
from contextlib import AsyncExitStack
from dataclasses import asdict
from pathlib import Path
from typing import TypedDict, cast

import httpx
from psycopg import AsyncConnection
from psycopg.rows import TupleRow, class_row

from forge.historical.archives.config import AcquisitionConfig
from forge.historical.archives.discovery import DiscoveryResult, discover, scan_archives
from forge.historical.archives.models import archive_scope_key
from forge.historical.archives.progress import (
    ProgressEvent,
    ProgressObserver,
    ProgressState,
    RequestOutcome,
)
from forge.historical.archives.registry import CachedRemoteArchive
from forge.historical.archives.sources.base import (
    DiscoveryError,
    ProbeEvent,
    UnsupportedRequestError,
)
from forge.historical.archives.sources.exchanges import ExchangeSource
from forge.historical.archives.storage.acquire import acquire
from forge.historical.archives.storage.files import ArchiveFileError, disk
from forge.orchestration.artifacts import RunArtifacts
from forge.shared.async_work import bounded_map
from forge.shared.database.testing import TestingDatabaseError, testing_connection
from forge.shared.http.client import HttpClient
from forge.shared.http.configured import open_configured_http_client
from forge.shared.identity import new_run_id
from forge.startup.context import ApplicationContext


def failure_message(error: Exception) -> str:
    """Actionable diagnostics without echoing URLs, DB details, or response bodies."""
    if isinstance(error, ExceptionGroup):
        return failure_message(cast(ExceptionGroup[Exception], error).exceptions[0])
    if isinstance(error, httpx.HTTPStatusError):
        return f"HTTP {error.response.status_code}; verify upstream access or refresh archive locations."
    if isinstance(error, httpx.TransportError):
        return "HTTP transport failed; retry after checking connectivity."
    if isinstance(error, (DiscoveryError, ArchiveFileError, TestingDatabaseError)):
        return str(error)
    return f"{type(error).__name__}; inspect configuration, database access, and upstream compatibility."


class OperationReport(TypedDict):
    schema_version: int
    run_id: str
    status: str
    file_counts: dict[str, int]
    requests: list[dict[str, object]]
    request_counts: dict[str, int]
    files_known: int
    files_total_final: bool
    archive_outcomes: str
    report: str


class Reporter:
    def __init__(
        self, context: ApplicationContext, *, total_requests: int, observer: ProgressObserver | None
    ) -> None:
        self.context = context
        self.observer = observer
        self.progress = ProgressState(total_requests)
        self.run_id = new_run_id()
        self.artifacts = RunArtifacts.for_run(context.paths, self.run_id)
        self.counts = self.progress.files
        self.requests: list[dict[str, object]] = []
        self._lock = asyncio.Lock()

    def emit(self, event: ProgressEvent) -> None:
        snapshot = self.progress.apply(event)
        if self.observer is not None:
            try:
                self.observer(snapshot)
            except Exception:
                # Presentation must never determine registry or operation outcomes.
                self.observer = None

    async def event(self, document: dict[str, object]) -> None:
        async with self._lock:
            await disk(self._append, document)

    def _append(self, document: dict[str, object]) -> None:
        with self.artifacts.events.open("a", encoding="utf-8") as output:
            output.write(json.dumps(document) + "\n")
            output.flush()

    async def save(self, status: str) -> OperationReport:
        result: OperationReport = {
            "schema_version": 1,
            "run_id": str(self.run_id),
            "status": status,
            "file_counts": asdict(self.counts),
            "requests": self.requests,
            "request_counts": asdict(self.progress.requests),
            "files_known": self.progress.known_total,
            "files_total_final": not self.progress.provisional,
            "archive_outcomes": str(self.artifacts.events),
            "report": str(self.artifacts.report),
        }
        await disk(
            self.artifacts.report.write_text, json.dumps(result, indent=2) + "\n", encoding="utf-8"
        )
        return result


async def _download_scan(
    connection: AsyncConnection[TupleRow],
    dsn: str,
    http: HttpClient,
    configuration: AcquisitionConfig,
    scan: DiscoveryResult,
    reporter: Reporter,
    workers: int,
    request_index: int,
    source: ExchangeSource,
    transfer_slots: asyncio.Semaphore,
    archive_locks: tuple[asyncio.Lock, ...],
    scope_lock: asyncio.Lock,
) -> int:
    queue: asyncio.Queue[CachedRemoteArchive | None] = asyncio.Queue(maxsize=workers * 2)

    refresh_lock = asyncio.Lock()
    refreshed: DiscoveryResult | None = None
    failures = 0

    async def worker() -> None:
        nonlocal failures
        async with testing_connection(dsn) as local_connection:

            async def refresh_location(item: CachedRemoteArchive) -> CachedRemoteArchive:
                nonlocal refreshed
                async with refresh_lock, scope_lock:
                    if refreshed is None:
                        refreshed = await discover(
                            local_connection,
                            source,
                            configuration.requests[request_index],
                            configuration.interval,
                            refresh=True,
                        )
                        await reporter.event(
                            {
                                "type": "location_refresh",
                                "request_index": request_index,
                                "scan_id": refreshed.scan_id,
                                "remote_archive_id": item.id,
                            }
                        )
                    async with local_connection.cursor(
                        row_factory=class_row(CachedRemoteArchive)
                    ) as cursor:
                        await cursor.execute(
                            """SELECT a.id, a.source_object_key, a.filename,
                            a.start_at, a.end_at, a.download_url, a.url_expires_at,
                            a.source_etag, a.source_last_modified
                            FROM historical.remote_archives a
                            JOIN historical.discovery_scan_archives m ON m.remote_archive_id=a.id
                            WHERE m.scan_id=%s AND a.id=%s""",
                            (refreshed.scan_id, item.id),
                        )
                        updated = await cursor.fetchone()
                    if updated is None:
                        raise ArchiveFileError("Archive disappeared during location refresh.")
                    return updated

            while True:
                remote = await queue.get()
                try:
                    if remote is None:
                        return
                    try:
                        identity = (
                            f"{configuration.requests[request_index].exchange} {remote.filename}"
                        )

                        def activity(
                            value: str, file_id: int = remote.id, label: str = identity
                        ) -> None:
                            reporter.emit(
                                ProgressEvent(
                                    "file_active", file_id=file_id, identity=label, activity=value
                                )
                            )

                        async with archive_locks[remote.id % len(archive_locks)], transfer_slots:
                            result = await acquire(
                                local_connection,
                                http,
                                remote,
                                reporter.context.paths.raw_dir,
                                on_activity=activity,
                                refresh_location=refresh_location,
                            )
                    except asyncio.CancelledError:
                        reporter.emit(
                            ProgressEvent(
                                "file_finished",
                                file_id=remote.id,
                                identity=f"{configuration.requests[request_index].exchange} {remote.filename}",
                                file_outcome="cancelled",
                            )
                        )
                        await reporter.event(
                            {
                                "request_index": request_index,
                                "remote_archive_id": remote.id,
                                "source_object_key": remote.source_object_key,
                                "status": "cancelled",
                            }
                        )
                        raise
                    except Exception as error:
                        failures += 1
                        reporter.emit(
                            ProgressEvent(
                                "file_finished",
                                file_id=remote.id,
                                identity=f"{configuration.requests[request_index].exchange} {remote.filename}",
                                file_outcome="failed",
                                error=failure_message(error),
                            )
                        )
                        await reporter.event(
                            {
                                "request_index": request_index,
                                "remote_archive_id": remote.id,
                                "source_object_key": remote.source_object_key,
                                "status": "failed",
                                "error": failure_message(error),
                            }
                        )
                    else:
                        reporter.emit(
                            ProgressEvent(
                                "file_finished",
                                file_id=remote.id,
                                identity=identity,
                                file_outcome="reused" if result.status == "reused" else "completed",
                            )
                        )
                        await reporter.event(
                            {
                                "request_index": request_index,
                                "source_object_key": remote.source_object_key,
                                **asdict(result),
                            }
                        )
                finally:
                    queue.task_done()

    try:
        async with asyncio.TaskGroup() as group:
            for _ in range(workers):
                group.create_task(worker())
            after_id = 0
            while page := await scan_archives(
                connection,
                scan.scan_id,
                configuration.interval,
                after_id=after_id,
                allow_superseded=True,
            ):
                for remote in page:
                    await queue.put(remote)
                    reporter.emit(ProgressEvent("queued"))
                after_id = page[-1].id
            for _ in range(workers):
                await queue.put(None)
    except BaseException:
        while not queue.empty():
            remaining = queue.get_nowait()
            queue.task_done()
            if remaining is not None:
                reporter.emit(
                    ProgressEvent(
                        "file_finished",
                        file_id=remaining.id,
                        identity=remaining.filename,
                        file_outcome="cancelled",
                    )
                )
                await reporter.event(
                    {
                        "request_index": request_index,
                        "remote_archive_id": remaining.id,
                        "source_object_key": remaining.source_object_key,
                        "status": "cancelled",
                    }
                )
        raise

    return failures


async def run_acquisition(
    context: ApplicationContext,
    configuration: AcquisitionConfig,
    *,
    dsn: str,
    download: bool,
    refresh: bool = False,
    requests_per_second: float | None = None,
    silent: bool = False,
    source_file: Path | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    observer: ProgressObserver | None = None,
) -> OperationReport:
    """Execute one historical owner operation; caller holds WorkspaceOwnership."""
    reporter = Reporter(context, total_requests=len(configuration.requests), observer=observer)
    await disk(reporter.artifacts.root.mkdir, parents=True, exist_ok=False)
    if source_file is not None:
        submitted = await disk(source_file.read_bytes)
        await disk(reporter.artifacts.submitted_config.write_bytes, submitted)
    clients: dict[str, HttpClient] = {}
    scope_locks = tuple(asyncio.Lock() for _ in range(640))
    archive_locks = tuple(asyncio.Lock() for _ in range(640))
    completed_scopes: list[tuple[tuple[str, str, str], DiscoveryResult] | None] = [None] * 640
    transfer_slots = asyncio.Semaphore(context.settings.http.max_concurrent)
    discoveries_remaining = [len(configuration.requests)]
    reporter.requests = [
        {"index": i, "request": request.model_dump(mode="json")}
        for i, request in enumerate(configuration.requests)
    ]

    async def process(index: int) -> bool:
        async with testing_connection(dsn) as connection:
            await process_connected(index, connection)
        return True

    async def process_connected(index: int, connection: AsyncConnection[TupleRow]) -> None:
        request = configuration.requests[index]
        outcome = reporter.requests[index]
        identity = f"{request.exchange} {request.instrument_type}/{request.market_category} {request.scope.identifier} {request.data_type}"
        reporter.emit(ProgressEvent("request_started", identity=identity, request_id=index))
        terminal: RequestOutcome = "failed"
        archives_found = 0

        def activity(value: str) -> None:
            reporter.emit(ProgressEvent("activity", activity=value, request_id=index))

        diagnostics = {"checking": 0, "found": 0, "absent": 0, "failed": 0, "registered": 0}
        examples: deque[dict[str, str]] = deque(maxlen=8)

        def probe(event: ProbeEvent) -> None:
            diagnostics[event.status] += 1
            examples.append({"status": event.status, "filename": event.filename})

        def registered(count: int) -> None:
            diagnostics["registered"] += count
            activity(f"Archives registered: {diagnostics['registered']}")

        outcome["candidate_diagnostics"] = diagnostics
        download_failures = 0
        discovery_finished = False
        try:
            if request.exchange not in clients:
                raise UnsupportedRequestError("Unsupported historical exchange.")
            http = clients[request.exchange]
            source = ExchangeSource(request.exchange, http, on_activity=activity, on_probe=probe)
            key = (request.exchange, archive_scope_key(request), request.data_type)
            slot = (
                sorted(clients).index(request.exchange) * 128
                + (int(key[1][:8], 16) % 64) * 2
                + int(request.data_type == "order_book_l2")
            )
            async with scope_locks[slot]:
                previous = completed_scopes[slot]
                if previous is not None and previous[0] == key:
                    old = previous[1]
                    scan = DiscoveryResult(old.scan_id, True, old.archives_found)
                else:
                    scan = await discover(
                        connection,
                        source,
                        request,
                        configuration.interval,
                        refresh=refresh,
                        on_activity=activity,
                        on_registered=registered,
                    )
                    completed_scopes[slot] = (key, scan)
            archives_found = scan.archives_found
            cursor = await connection.execute(
                """SELECT min(a.start_at), max(a.end_at),
                COALESCE(tstzrange(%s, %s, '[)') <@ range_agg(tstzrange(a.start_at, a.end_at, '[)')), FALSE), count(*)
                FROM historical.remote_archives a
                JOIN historical.discovery_scan_archives m ON m.remote_archive_id=a.id
                WHERE m.scan_id=%s AND a.start_at < %s AND a.end_at > %s""",
                (
                    configuration.interval.start,
                    configuration.interval.end,
                    scan.scan_id,
                    configuration.interval.end,
                    configuration.interval.start,
                ),
            )
            bounds = await cursor.fetchone()
            if bounds is not None:
                outcome["observed_start_at"] = (
                    bounds[0].isoformat() if bounds[0] is not None else None
                )
                outcome["observed_end_at"] = (
                    bounds[1].isoformat() if bounds[1] is not None else None
                )
                outcome["archive_interval_coverage"] = (
                    "nominal intervals span request; content coverage unknown"
                    if bounds[2]
                    else "incomplete: observed archive intervals do not span request"
                )
            outcome.update(
                {
                    "scan_id": scan.scan_id,
                    "cache": "reused" if scan.cached else "refreshed",
                    "archives_found": scan.archives_found,
                }
            )
            discoveries_remaining[0] -= 1
            discovery_finished = True
            if discoveries_remaining[0] == 0:
                reporter.emit(ProgressEvent("total_final"))
            if download:
                reporter.emit(
                    ProgressEvent(
                        "files_discovered", archives=bounds[3] if bounds is not None else 0
                    )
                )
                download_failures = await _download_scan(
                    connection,
                    dsn,
                    http,
                    configuration,
                    scan,
                    reporter,
                    min(2, context.settings.http.max_concurrent),
                    index,
                    source,
                    transfer_slots,
                    archive_locks,
                    scope_locks[slot],
                )
        except Exception as error:
            if not discovery_finished:
                discoveries_remaining[0] -= 1
                if discoveries_remaining[0] == 0:
                    reporter.emit(ProgressEvent("total_final"))
            reporter.counts.cancelled = max(
                0,
                reporter.counts.queued
                - reporter.counts.completed
                - reporter.counts.reused
                - reporter.counts.failed,
            )
            terminal = "unsupported" if isinstance(error, UnsupportedRequestError) else "failed"
            outcome.update(
                {"status": "failed", "discovery": terminal, "error": failure_message(error)}
            )
        else:
            terminal = "empty" if scan.archives_found == 0 else "successful"
            outcome["discovery"] = "empty" if terminal == "empty" else "archives"
            outcome["coverage"] = (
                "unknown: archive discovery does not prove complete market-data coverage"
            )
            if request.exchange == "gateio":
                outcome["availability_note"] = (
                    "Candidate probes only; 404 means that object was not observed, not that the product is unsupported. Incremental orderbooks and orderbooks_slice snapshots are separate feeds."
                )
            outcome["status"] = "failed" if download_failures > 0 else "complete"
            if outcome["status"] == "failed":
                terminal = "failed"
                outcome["error"] = "One or more archive acquisitions failed; see archive outcomes."
        reporter.emit(
            ProgressEvent(
                "request_finished",
                identity=identity,
                request_outcome=terminal,
                request_id=index,
                cached=outcome.get("cache") == "reused",
                archives=archives_found,
                error=str(outcome.get("error", "")),
            )
        )
        outcome["recent_probe_diagnostics"] = list(examples)
        await reporter.event({"type": "request", **outcome})

    async def indices():
        for index in range(len(configuration.requests)):
            yield index

    try:
        # Preflight identity before any network work; clients are initialized once,
        # serially, then shared by all three request workers.
        async with testing_connection(dsn):
            pass
        async with AsyncExitStack() as stack:
            for exchange in sorted(
                {r.exchange for r in configuration.requests}
                & {"binance", "bybit", "okx", "gateio", "bitget"}
            ):
                cap = {"gateio": 1.0, "bitget": 0.5, "bybit": 1.0, "okx": 2.5}.get(exchange, 2.0)
                clients[exchange] = await stack.enter_async_context(
                    open_configured_http_client(
                        context.settings.http,
                        requests_per_second=min(
                            requests_per_second or context.settings.http.requests_per_second, cap
                        ),
                        transport=transport,
                    )
                )
            async for _ in bounded_map(indices(), process, workers=3):
                pass

    except BaseException as error:
        status = (
            "cancelled"
            if isinstance(error, (asyncio.CancelledError, KeyboardInterrupt))
            else "failed"
        )
        for item in reporter.requests:
            if "status" not in item:
                item["status"] = status
                reporter.emit(
                    ProgressEvent(
                        "request_finished",
                        identity=reporter.progress.current,
                        request_outcome="cancelled" if status == "cancelled" else "failed",
                    )
                )
        for index in range(len(reporter.requests), len(configuration.requests)):
            reporter.requests.append(
                {
                    "index": index,
                    "request": configuration.requests[index].model_dump(mode="json"),
                    "status": status,
                }
            )
            reporter.emit(
                ProgressEvent(
                    "request_finished",
                    identity=str(configuration.requests[index].scope.identifier),
                    request_outcome="cancelled" if status == "cancelled" else "failed",
                )
            )
        reporter.counts.cancelled = max(
            0,
            reporter.counts.queued
            - reporter.counts.completed
            - reporter.counts.reused
            - reporter.counts.failed,
        )
        reporter.emit(ProgressEvent("finished", identity=str(reporter.artifacts.report)))
        await reporter.save(status)
        raise
    reporter.emit(ProgressEvent("total_final"))
    reporter.emit(ProgressEvent("finished", identity=str(reporter.artifacts.report)))
    failed = any(item["status"] == "failed" for item in reporter.requests)
    return await reporter.save("failed" if failed else "complete")
