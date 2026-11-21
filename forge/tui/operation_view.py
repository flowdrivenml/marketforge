"""Operation dashboards rendered from producer snapshots and receipts."""

from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime

from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, VerticalScroll
from textual.screen import Screen
from textual.widgets import Button, Static

from forge.historical.archives.progress import ProgressSnapshot
from forge.historical.pipelines.progress import PipelineSnapshot
from forge.tui.controller import Operation, document
from forge.tui.inspection import ActionMenu, fields
from forge.tui.presentation import display, status
from forge.tui.screens import Detail, WorkspaceHost


def panel(title: str, body: Group | Table | Text) -> Panel:
    return Panel(body, title=title, title_align="left", padding=(0, 1), border_style="#565f89")


def duration(start: str | None, finished: str | None = None) -> str:
    if not start:
        return "—"
    try:
        start_at = datetime.fromisoformat(start)
        end = datetime.fromisoformat(finished) if finished else datetime.now(UTC)
        seconds = max(0, int((end - start_at).total_seconds()))
    except (ValueError, TypeError):
        return "—"
    return f"{seconds // 3600:02}:{seconds // 60 % 60:02}:{seconds % 60:02}"


def acquisition(value: ProgressSnapshot) -> Group:
    done, total = value.requests.finished, value.total_requests
    filled = 16 * min(done, total) // total if total else 0
    heading = Text("Requests  ", style="#7dcfff")
    if total:
        heading.append("━" * filled, style="#7dcfff")
        heading.append("─" * (16 - filled), style="#565f89")
        heading.append(f"  {done} / {total} requests")
    else:
        heading.append(f"{done} requests complete")
    count = f"{value.known_total} known files" + (
        " · discovery in progress" if value.provisional else ""
    )
    metrics = Text(
        f"{count}  ·  {value.files.completed} downloaded  ·  {value.files.reused} reused  ·  {value.files.failed} failed"
    )
    return Group(
        heading,
        metrics,
        Text(value.activity or value.current),
        *(Text(error, style="#e0af68") for error in value.failures[-2:]),
    )


def dashboard(op: Operation, width: int, height: int) -> Group:
    snapshot = op.snapshot()
    result = document(op.result)
    title = Text(op.title + "  ", style="bold #7aa2f7")
    title.append_text(status(op.state.upper()))
    parts: list[Panel | Text] = [title]
    if isinstance(snapshot, PipelineSnapshot):
        parts.append(
            Text(
                f"{snapshot.name}  ·  {snapshot.environment.upper()}  ·  {duration(snapshot.started_at, snapshot.finished_at)}"
            )
        )
        parts.append(
            Text(
                f"Requested UTC  {snapshot.interval[0].astimezone(UTC).isoformat()} → {snapshot.interval[1].astimezone(UTC).isoformat()} · end exclusive"
            )
        )
        table = Table.grid(padding=(0, 1), expand=True)
        for _ in range(5 if width >= 78 else 3):
            table.add_column()
        table.add_row("Stage", "Phase", "Outcome", *(("Duration", "Output") if width >= 78 else ()))
        for stage in snapshot.stages:
            table.add_row(
                Text(stage.name.capitalize(), style="#7aa2f7"),
                Text(stage.phase.replace("_", " ")),
                status(stage.state),
                *(
                    (duration(stage.started_at, stage.finished_at), Text(stage.summary or "—"))
                    if width >= 78
                    else ()
                ),
            )
        parts.append(panel("Pipeline stages", table))
        active = next(
            (stage for stage in snapshot.stages if stage.name == snapshot.active_stage), None
        )
        if active and active.name == "acquire" and snapshot.acquisition:
            parts.append(panel("Acquisition", acquisition(snapshot.acquisition)))
        elif active:
            meter = (
                f"{active.verified_jobs[0]} / {active.verified_jobs[1]} jobs verified"
                if active.verified_jobs
                else active.summary
            )
            parts.append(
                panel(
                    "Active stage",
                    Group(
                        Text(active.phase.replace("_", " ").capitalize(), style="#7dcfff"),
                        Text(meter or "Awaiting retained phase evidence"),
                    ),
                )
            )
        if snapshot.recent and height >= 24:
            parts.append(
                panel(
                    "Recent activity",
                    Group(
                        *(
                            Text(
                                f"{event.stage}  ·  {event.phase.replace('_', ' ')}  ·  {event.state}"
                            )
                            for event in snapshot.recent[-(5 if height >= 37 else 3) :]
                        )
                    ),
                )
            )
    elif isinstance(snapshot, ProgressSnapshot):
        parts.append(panel("Acquisition", acquisition(snapshot)))
    elif isinstance(snapshot, str):
        parts.append(panel("Current phase", Text(snapshot, style="#7dcfff")))
    else:
        parts.append(
            panel(
                "Activity",
                Text(
                    "Awaiting service evidence"
                    if op.state == "running"
                    else "See the retained result and available actions."
                ),
            )
        )
    output = {
        key: result[key]
        for key in (
            "execution",
            "verification",
            "registration",
            "path",
            "report_path",
            "job_path",
            "publication",
            "prepared_job_count",
            "registration_receipt",
        )
        if key in result and not isinstance(result[key], (dict, list))
    }
    if output:
        parts.append(panel("Retained outcome", fields(output)))
    error = (
        op.error
        or result.get("error")
        or (snapshot.error if isinstance(snapshot, PipelineSnapshot) else None)
    )
    if error:
        parts.append(panel("Action required", Text(display(error), style="#f7768e")))
    return Group(*parts)


class OperationView(Screen[None]):
    BINDINGS = [
        Binding("escape", "back", "Back"),
        Binding("f2", "actions", "Actions"),
        Binding("f3", "raw", "Evidence"),
    ]

    def __init__(
        self, operation: Operation, choices: Callable[[], dict[str, Callable[[], object]]]
    ) -> None:
        super().__init__()
        self.operation = operation
        self.choices = choices

    def compose(self) -> ComposeResult:
        yield Static("MarketForge / Operation", classes="screen-heading", markup=False)
        with VerticalScroll(id="operation-scroll"):
            yield Static(id="operation-dashboard")
        with Horizontal(classes="screen-footer"):
            yield Button("Esc Back", id="op-back", classes="compact")
            yield Button("F2 Actions", id="op-actions", classes="compact")
            yield Button("F3 Raw evidence", id="op-raw", classes="compact")

    def on_mount(self) -> None:
        self.refresh_dashboard()
        self.set_interval(0.3, self.refresh_dashboard)

    def refresh_dashboard(self) -> None:
        self.query_one("#operation-dashboard", Static).update(
            dashboard(self.operation, self.size.width, self.size.height)
        )
        button = self.query_one("#op-actions", Button)
        choices = self.choices()
        button.disabled = not choices
        button.label = f"F2 Actions ({len(choices)})" if choices else "F2 Actions"

    def action_back(self) -> None:
        self.dismiss(None)

    def action_actions(self) -> None:
        from typing import cast

        choices = self.choices()
        if choices:

            def choose(key: str | None) -> None:
                if key is not None:
                    choices[key]()

            cast(WorkspaceHost, self).app.push_screen(
                ActionMenu(self.operation.title, tuple(choices)), choose
            )

    def action_raw(self) -> None:
        from typing import cast

        snapshot = self.operation.snapshot()
        detail = (
            asdict(snapshot)
            if isinstance(snapshot, (PipelineSnapshot, ProgressSnapshot))
            else snapshot
        )
        cast(WorkspaceHost, self).app.push_screen(
            Detail("Raw operation evidence", {"activity": detail, "result": self.operation.result})
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "op-back":
            self.action_back()
        elif event.button.id == "op-actions":
            self.action_actions()
        elif event.button.id == "op-raw":
            self.action_raw()
