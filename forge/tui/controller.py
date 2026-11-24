"""Application-owned operations and authoritative result labels."""

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import cast
from uuid import uuid4

from forge.historical.archives.storage.files import disk
from forge.historical.engine.process import Shutdown, shutdown
from forge.orchestration.registry import current_execution
from forge.shared.database.connection import DatabaseError, database_environment, pinned_registry
from forge.shared.ownership import OwnershipConflictError, WorkspaceOwnership
from forge.startup.context import ApplicationContext
from forge.tui.actions import Action, Values, read_only
from forge.tui.services import execute
from forge.tui.workflows import PIPELINE_DESCRIPTORS, Edit, Progress

Invoke = Callable[[str, Values, ApplicationContext, Progress, Edit], Awaitable[object]]


def document(value: object) -> Mapping[str, object]:
    return cast(Mapping[str, object], value) if isinstance(value, dict) else {}


def outcome(value: object) -> str:
    result = document(value)
    for key in ("state", "status", "registration"):
        state = result.get(key)
        if isinstance(state, str):
            return "interrupted" if state == "cancelled" else state
    return "result available"


@dataclass(slots=True)
class Operation:
    id: str
    title: str
    state: str = "running"
    progress: object = None
    result: object = None
    error: str | None = None

    reference: str = ""
    started_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    started_tick: float = field(default_factory=monotonic)
    finished_tick: float | None = None
    finished_at: str | None = None
    run_id: str | None = None
    environment: str | None = None
    events: list[tuple[str, str]] = field(default_factory=list[tuple[str, str]])

    def record(self, phase: str) -> None:
        if self.events and self.events[-1][1] == phase:
            return
        self.events.append((datetime.now(UTC).isoformat(), phase))
        del self.events[:-20]

    def snapshot(self) -> object:
        if callable(self.progress):
            return cast(Callable[[], object], self.progress)()
        return self.progress


class Controller:
    def __init__(self, context: ApplicationContext, editor: Edit, invoke: Invoke = execute) -> None:
        self.context = context
        self.editor = editor
        self.invoke = invoke
        self.registry_identity: str | None = None
        self.task: asyncio.Task[Operation] | None = None
        self.current: Operation | None = None
        self.history: list[Operation] = []
        self.shutdown = Shutdown()

    def active(self) -> bool:
        return self.task is not None and not self.task.done()

    def launch(
        self,
        action: Action,
        values: Values,
        apply: Callable[[], Awaitable[object]] | None = None,
        *,
        reviewed: tuple[Path, bytes] | None = None,
    ) -> asyncio.Task[Operation]:
        if self.shutdown.emergency:
            raise ValueError("Emergency shutdown has started.")
        if self.active():
            raise ValueError("Work is already active. Inspect it in Runs or wait for cleanup.")
        op = Operation(
            uuid4().hex,
            action.key,
            reference=str(values.get("reference", "")),
            environment="testing" if self.context.testing else "production",
        )
        self.current = op
        self.task = asyncio.create_task(self._run(op, action, dict(values), apply, reviewed))
        return self.task

    async def _run(
        self,
        op: Operation,
        action: Action,
        values: Values,
        apply: Callable[[], Awaitable[object]] | None,
        reviewed: tuple[Path, bytes] | None,
    ) -> Operation:
        def progress(value: object) -> None:
            op.progress = value
            if isinstance(value, str):
                op.record(value)
            if execution := current_execution():
                op.run_id = str(execution.id)

        async def invoke() -> object:
            with (
                database_environment(testing=self.context.testing),
                pinned_registry(self.registry_identity),
            ):
                if reviewed is not None and await disk(reviewed[0].read_bytes) != reviewed[1]:
                    raise ValueError(
                        "The input changed after review. Reopen it and confirm the current version."
                    )
                if apply is not None:
                    return await apply()
                return await self.invoke(action.key, values, self.context, progress, self.editor)

        token_shutdown = shutdown.set(self.shutdown)
        try:
            if (
                not read_only(action, values)
                and action.effect in {"write", "engine", "network", "guarded"}
                and not (apply is not None and action.key.startswith("storage "))
            ):
                with WorkspaceOwnership(self.context.paths) as owner:
                    token = (
                        PIPELINE_DESCRIPTORS.set(owner.descriptors())
                        if action.key.startswith("pipeline ")
                        else None
                    )
                    try:
                        op.result = await invoke()
                    finally:
                        if token is not None:
                            PIPELINE_DESCRIPTORS.reset(token)
            else:
                op.result = await invoke()
            op.state = outcome(op.result)
        except asyncio.CancelledError:
            op.state = "interrupted"
        except (OwnershipConflictError, DatabaseError) as error:
            op.state = "blocked"
            op.error = str(error)
        except Exception as error:
            op.state = "failed"
            op.error = str(error)
        finally:
            shutdown.reset(token_shutdown)
            op.record(op.state)
            op.finished_tick = monotonic()
            op.finished_at = datetime.now(UTC).isoformat()
            self.history.append(op)
            del self.history[:-30]
        return op

    async def cancel(self) -> None:
        task = self.task
        if task is not None and not task.done():
            if self.current is not None and self.current.state != "interrupting":
                self.current.state = "interrupting"
                task.cancel()
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                if not task.cancelled():
                    raise
                if self.current is not None:
                    self.current.state = "interrupted"

    async def emergency_stop(self) -> bool:
        self.shutdown.emergency = True
        cancellation = asyncio.create_task(self.cancel())
        deadline = monotonic() + 4
        done, _ = await asyncio.wait({cancellation}, timeout=4)
        if cancellation not in done:
            cancellation.cancel()
            return False
        await cancellation
        while self.shutdown.process_count() and monotonic() < deadline:  # noqa: ASYNC110
            await asyncio.sleep(0.05)
        return not self.shutdown.unverified and self.shutdown.process_count() == 0
