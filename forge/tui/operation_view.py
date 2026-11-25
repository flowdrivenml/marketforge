"""Operation dashboards rendered from producer snapshots and receipts."""

from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime
from typing import cast

from rich.box import ROUNDED
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
from forge.shared.presentation.normalization import PreparationView
from forge.shared.terminal.palette import BLUE, CYAN, MUTED, RED, YELLOW
from forge.tui.controller import Operation, document
from forge.tui.design import OUTLINE
from forge.tui.editor_inspection import OpenInspection, StructuredValue
from forge.tui.inspection import ActionMenu, RunEvidenceActions, fields
from forge.tui.presentation import display, status
from forge.tui.resources import records
from forge.tui.screens import Detail, WorkspaceHost


def panel(title: str, body: Group | Table | Text) -> Panel:
    return Panel(
        body,
        title=Text(title, style=f"bold {CYAN}"),
        title_align="left",
        padding=(0, 1),
        border_style=OUTLINE,
        box=ROUNDED,
    )


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
    heading = Text("Requests  ", style=CYAN)
    if total:
        heading.append("━" * filled, style=CYAN)
        heading.append("─" * (16 - filled), style=MUTED)
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
        *(Text(error, style=YELLOW) for error in value.failures[-2:]),
    )


def dashboard(op: Operation, width: int, height: int) -> Group:
    snapshot = op.snapshot()
    result = document(op.result)
    title = Text(op.title + "  ", style=f"bold {BLUE}")
    title.append_text(status(op.state.upper()))
    parts: list[Panel | Text] = [title]
    identity = (
        snapshot.run_id
        if isinstance(snapshot, PipelineSnapshot)
        else result.get("run_id") or result.get("execution_id") or op.run_id
    )
    parts.append(
        Text(
            " · ".join(
                filter(
                    None,
                    (
                        snapshot.name if isinstance(snapshot, PipelineSnapshot) else op.reference,
                        (
                            snapshot.environment
                            if isinstance(snapshot, PipelineSnapshot)
                            else op.environment or ""
                        ).upper(),
                        str(identity or "Awaiting run ID"),
                        duration(snapshot.started_at, snapshot.finished_at)
                        if isinstance(snapshot, PipelineSnapshot)
                        else duration(op.started_at, op.finished_at),
                    ),
                )
            )
        )
    )
    if isinstance(snapshot, PipelineSnapshot):
        parts.append(
            Text(
                f"Requested UTC  {snapshot.interval[0].astimezone(UTC).isoformat()} → {snapshot.interval[1].astimezone(UTC).isoformat()} · end exclusive"
            )
        )
        table = Table.grid(padding=(0, 1), expand=True)
        headings = ("Stage", "Phase", "Outcome", *(("Duration", "Output") if width >= 78 else ()))
        for heading in headings:
            table.add_column(justify="right" if heading == "Duration" else "left")
        table.add_row(*(Text(heading, f"bold {BLUE}") for heading in headings))
        for stage in snapshot.stages:
            table.add_row(
                Text(stage.name.capitalize()),
                Text(
                    stage.phase.replace("_", " "),
                    style=CYAN
                    if stage.name == snapshot.active_stage
                    else MUTED
                    if stage.state == "waiting"
                    else "",
                ),
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
                        Text(active.phase.replace("_", " ").capitalize(), style=CYAN),
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
                            Text.assemble(
                                f"{event.stage}  ·  {event.phase.replace('_', ' ')}  ·  ",
                                status(event.state),
                            )
                            for event in snapshot.recent[-(5 if height >= 37 else 3) :]
                        )
                    ),
                )
            )
    elif isinstance(snapshot, ProgressSnapshot):
        parts.append(panel("Acquisition", acquisition(snapshot)))
    elif isinstance(snapshot, str):
        parts.append(panel("Current phase", Text(snapshot, style=CYAN)))
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
    if op.events:
        events = (Text(f"{timestamp[11:19]} UTC · {phase}") for timestamp, phase in op.events[-5:])
        title = "Service activity" if isinstance(snapshot, PipelineSnapshot) else "Recent activity"
        parts.append(panel(title, Group(*events)))
    checks = {
        name: result[key]
        for key, name in (
            ("status", "Service outcome"),
            ("engine_ready", "Engine ready"),
            ("compatibility", "Engine compatibility"),
        )
        if key in result
        and ("compatibility" in result or "engine_ready" in result or result.get("blockers"))
    }
    if checks:
        parts.append(panel("Validation and readiness", fields(checks)))
    if result.get("kind") == "normalization_prepare_report":
        preparation = PreparationView.from_result(
            result,
            environment=op.environment or "",
            dry_run=result.get("dry_run") is True,
            recipe=op.reference,
        )
        if stage := result.get("failed_stage"):
            parts.append(panel("Failed stage", Text(display(stage), style=RED)))
        parts.append(
            panel("Preparation", fields(dict(preparation.identity + preparation.resolution)))
        )
        if preparation.examples:
            parts.append(
                panel(
                    "Preparation diagnostics",
                    Group(*(Text(reason, style=RED) for _, reason in preparation.examples)),
                )
            )
    blockers = [
        Text(display(item), style=RED)
        for key in ("blockers", "compatibility_blockers")
        if isinstance(items := result.get(key), list)
        for item in cast(list[object], items)
    ]
    if blockers:
        parts.append(panel("Blockers", Group(*blockers[:8])))
    resolutions = records(result, "reference_resolutions") or [
        resolution
        for job in records(result, "jobs")
        if (resolution := document(job.get("reference_resolution")))
    ]
    for resolution in resolutions:
        ranking = document(resolution.get("ranking"))
        evidence = document(ranking.get("snapshot"))
        summary: dict[str, object] = {
            "Mode": resolution.get("mode"),
            "Source publication": resolution.get("source_registry_publication_id"),
            "Target quote": resolution.get("target_quote"),
            "Ranking evidence": ranking.get("status"),
            "Validation": resolution.get("validation"),
        }
        if evidence:
            summary["Ranking snapshot"] = evidence.get("id")
        selected = [
            Text(f"{item['stream_id']} · {item['provider']} · Trades")
            for item in records(resolution, "selected")
        ]
        potential: list[Text] = []
        for quote, raw_detail in document(resolution.get("routes")).items():
            if isinstance(options := document(raw_detail).get("potential"), list):
                for route in cast(list[object], options):
                    if isinstance(route, (list, tuple)):
                        parts_of_route = cast(list[object] | tuple[object, ...], route)
                        if len(parts_of_route) != 2:
                            continue
                        currencies = cast(tuple[str, ...] | list[str], parts_of_route[0])
                        path = (
                            " → ".join(currencies)
                            if parts_of_route[1]
                            else f"{quote} → {quote} · Identity"
                        )
                        potential.append(Text(path))
        parts.append(
            panel(
                "Conversion reference resolution",
                Group(fields(summary), *selected[:8], *potential[:8]),
            )
        )
    routes: list[Text] = []
    for job in records(result, "jobs"):
        if job.get("engine_validation") != "passed":
            continue
        for stream in records(job, "planned_routes"):
            if not stream.get("selected") or not isinstance(paths := stream.get("routes"), list):
                continue
            for path in cast(list[object], paths):
                if isinstance(path, list):
                    route = " → ".join(map(str, cast(list[object], path)))
                    routes.append(Text(f"{stream.get('instrument')}  ·  {route}"))
    if routes:
        parts.append(panel("Engine-validated routes", Group(*routes[:8])))
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
            "indexing",
            "work_key",
            "files_written",
            "bytes_written",
        )
        if key in result and not isinstance(result[key], (dict, list))
    }
    if output:
        parts.append(panel("Retained outcome", fields(output)))
    error = (
        op.error
        or result.get("error")
        or result.get("validation_error")
        or (snapshot.error if isinstance(snapshot, PipelineSnapshot) else None)
    )
    if error:
        parts.append(panel("Action required", Text(display(error), style=RED)))
    return Group(*parts)


class OperationView(Screen[None]):
    BINDINGS = [
        Binding("escape", "back", "Back"),
        Binding("f2", "actions", "Actions"),
        Binding("f3", "raw", "Evidence"),
        Binding("f4", "errors", "Errors", show=False),
    ]

    def __init__(
        self,
        operation: Operation,
        choices: Callable[[], dict[str, Callable[[], object]]],
        stop: Callable[[], object] | None = None,
        *,
        errors: Callable[[], object] | None = None,
    ) -> None:
        super().__init__()
        self.operation = operation
        self.choices = choices
        self.stop = stop
        self.errors = errors

    def compose(self) -> ComposeResult:
        yield Static("MarketForge / Operation", classes="screen-heading", markup=False)
        yield Static("", classes="workspace-monitor", markup=False)
        if self.errors is not None:
            yield RunEvidenceActions(StructuredValue(self.evidence()))
        with VerticalScroll(id="operation-scroll"):
            yield Static(id="operation-dashboard")
        with Horizontal(classes="screen-footer"):
            yield Button("Close View", id="op-back", classes="compact")
            if self.stop is not None:
                yield Button("Request Stop", id="op-stop", variant="error")
            yield Button("F2 Actions", id="op-actions", classes="compact")
            yield Button("F3 Evidence", id="op-raw", classes="compact")

    def on_mount(self) -> None:
        self.refresh_dashboard()
        self.set_interval(0.3, self.refresh_dashboard)

    def refresh_dashboard(self) -> None:
        self.query_one("#operation-dashboard", Static).update(
            dashboard(self.operation, self.size.width, self.size.height)
        )
        if self.errors is not None:
            self.query_one("#inspect-open-json", OpenInspection).target = StructuredValue(
                self.evidence()
            )
        if self.stop is not None:
            self.query_one("#op-stop", Button).disabled = self.operation.finished_at is not None
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

    def evidence(self) -> dict[str, object]:
        snapshot = self.operation.snapshot()
        detail = (
            asdict(snapshot)
            if isinstance(snapshot, (PipelineSnapshot, ProgressSnapshot))
            else snapshot
        )
        return {"activity": detail, "result": self.operation.result}

    def action_raw(self) -> None:
        cast(WorkspaceHost, self).app.push_screen(Detail("Operation evidence", self.evidence()))

    def action_errors(self) -> None:
        if self.errors is not None:
            self.errors()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "op-stop" and self.stop is not None:
            self.stop()
        elif event.button.id == "op-back":
            self.action_back()
        elif event.button.id == "op-actions":
            self.action_actions()
        elif event.button.id == "inspect-errors":
            self.action_errors()
        elif event.button.id == "op-raw":
            self.action_raw()
