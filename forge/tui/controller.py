"""Application-owned operations and authoritative result labels."""

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from time import monotonic
from typing import cast
from uuid import uuid4

from forge.shared.database.connection import DatabaseError, database_environment
from forge.shared.ownership import OwnershipConflictError, WorkspaceOwnership
from forge.startup.context import ApplicationContext
from forge.tui.actions import Action, Values, read_only
from forge.tui.services import execute
from forge.tui.workflows import Edit, Progress

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

    def snapshot(self) -> object:
        if callable(self.progress):
            return cast(Callable[[], object], self.progress)()
        return self.progress


class Controller:
    def __init__(self, context: ApplicationContext, editor: Edit, invoke: Invoke = execute) -> None:
        self.context = context
        self.editor = editor
        self.invoke = invoke
        self.task: asyncio.Task[Operation] | None = None
        self.current: Operation | None = None
        self.history: list[Operation] = []

    def active(self) -> bool:
        return self.task is not None and not self.task.done()

    def launch(
        self, action: Action, values: Values, apply: Callable[[], Awaitable[object]] | None = None
    ) -> asyncio.Task[Operation]:
        if self.active():
            raise ValueError("Work is already active. Inspect it in Runs or wait for cleanup.")
        op = Operation(uuid4().hex, action.key, reference=str(values.get("reference", "")))
        self.current = op
        self.task = asyncio.create_task(self._run(op, action, values, apply))
        return self.task

    async def _run(
        self,
        op: Operation,
        action: Action,
        values: Values,
        apply: Callable[[], Awaitable[object]] | None,
    ) -> Operation:
        def progress(value: object) -> None:
            op.progress = value

        async def invoke() -> object:
            with database_environment(testing=self.context.testing):
                if apply is not None:
                    return await apply()
                return await self.invoke(action.key, values, self.context, progress, self.editor)

        try:
            if (
                not read_only(action, values)
                and action.effect in {"write", "engine", "network", "guarded"}
                and not (apply is not None and action.key.startswith("storage "))
            ):
                with WorkspaceOwnership(self.context.paths):
                    op.result = await invoke()
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
