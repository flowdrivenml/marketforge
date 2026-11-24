"""Terminal rendering for typed Runs projections."""

from rich.console import Console
from rich.text import Text

from forge.shared.presentation.runs import (
    Document,
    Fields,
    LogView,
    MaintenanceView,
    ReportView,
    RunDetail,
    RunInventory,
    TableView,
    command,
    deletion,
    label,
    safe_text,
)
from forge.shared.terminal.presentation import (
    data_table,
    empty_state,
    header,
    section,
    status_label,
    status_style,
    summary,
)


def facts(console: Console, fields: Fields, title: str = "") -> None:
    if fields:
        if title:
            section(console, title)
        summary(console, ((name, safe_text(value)) for name, value in fields))


def actions(console: Console, commands: tuple[str, ...]) -> None:
    if commands:
        section(console, "Inspect")
        for value in commands:
            console.print(Text("  " + safe_text(value), style="mf.identifier"), soft_wrap=True)


def show_table(console: Console, view: TableView) -> None:
    if not view.records:
        return
    section(console, view.title)
    table = data_table()
    identifiers = {
        "Run ID",
        "Child run ID",
        "Attempt ID",
        "Engine job ID",
        "Engine publication ID",
        "Registry publication ID",
    }
    long = [i for i, name in enumerate(view.columns) if name in identifiers]
    if console.width < 112 and long and len(view.columns) > 3:
        index = long[0]
        table.add_column(view.columns[index], no_wrap=True)
        table.add_column("Details", overflow="fold")
        for row in view.records:
            description = "\n".join(
                f"{name}  {safe_text(row[i])}"
                for i, name in enumerate(view.columns)
                if i != index and i not in long and row[i] and row[i] != "—"
            )
            table.add_row(Text(safe_text(row[index])), Text(description))
            for nested in long[1:]:
                if row[nested] and row[nested] != "—":
                    table.add_row(
                        Text(safe_text(row[nested])), Text(view.columns[nested], "mf.secondary")
                    )
    else:
        for name in view.columns:
            table.add_column(name, overflow="fold", no_wrap=name in identifiers)
        for row in view.records:
            table.add_row(*(Text(safe_text(value)) for value in row))
    console.print(table)
    if len(view.records) < view.total:
        console.print(
            Text(
                f"Showing {len(view.records):,} of {view.total:,} returned records.",
                style="mf.secondary",
            )
        )


def render_inventory(console: Console, view: RunInventory, environment: str) -> None:
    header(console, "Runs", environment=environment)
    console.print(Text(view.ordering, style="mf.secondary"))
    finish = view.sort_field == "end"
    if view.filters:
        console.print(
            Text(
                "Filters · "
                + " · ".join(f"{key} {safe_text(value)}" for key, value in view.filters),
                style="mf.secondary",
            )
        )
    if not view.rows:
        empty_state(console, "No runs match these filters." if view.filters else "No indexed runs.")
    else:
        wide = console.width >= 132
        table = data_table()
        table.add_column("Run ID", no_wrap=True, style="mf.identifier")
        if wide:
            for title in (
                "Operation",
                "Action",
                "Status",
                "Finished UTC" if finish else "Started UTC",
                "Duration",
                "Recipe / outcome",
            ):
                table.add_column(title, min_width=16 if title == "Status" else None)
        else:
            table.add_column("Operation / status / timing")
        for row in view.rows:
            identity = Text(row.identity or "—")
            state = label(row.status) + ("\nliveness unknown" if row.liveness == "unknown" else "")
            state_text = Text(state, style=status_style(row.status))
            time = (row.finished if finish else row.started).removesuffix(" UTC")
            elapsed = (
                row.elapsed
                if "." not in row.elapsed
                else row.elapsed.split(".")[0] + "." + row.elapsed.split(".")[1][:3]
            )
            outcome = " · ".join(part for part in (row.recipe, row.outcome) if part)
            if wide:
                table.add_row(
                    identity,
                    Text(row.operation),
                    Text(row.action),
                    state_text,
                    Text(time),
                    Text(elapsed),
                    Text(outcome or "—"),
                )
            else:
                details = Text(f"{row.operation} · {row.action}\n")
                details.append_text(state_text)
                details.append(
                    f"\n{'Finished' if finish else 'Started'} {time} UTC"
                    if time != "—"
                    else "\nTime unavailable"
                )
                if row.elapsed != "—":
                    details.append(f" · {elapsed}")
                if outcome:
                    details.append("\n" + outcome)
                table.add_row(identity, details)
        console.print(table)
    console.print(
        Text(
            f"{len(view.rows):,} {'run' if len(view.rows) == 1 else 'runs'} shown · Offset {view.offset:,}"
        )
    )
    if view.next_command:
        console.print(
            Text("Next · " + safe_text(view.next_command), style="mf.identifier"), soft_wrap=True
        )


def render_detail(console: Console, view: RunDetail, environment: str) -> None:
    header(console, "Run Details", environment=environment)
    run = view.run
    identity = (
        ("Run ID", run.identity or "—"),
        ("Operation", run.operation),
        ("Action", run.action),
        *((("Recipe", run.recipe),) if run.recipe else ()),
        *view.identity,
        *((("Liveness", "Unknown"),) if run.liveness == "unknown" else ()),
    )
    summary(console, (*identity, ("Recorded status", status_label(run.status))))
    facts(console, view.timing, "Timing")
    facts(console, tuple((name, label(value)) for name, value in view.lifecycle), "Lifecycle")
    show_table(console, view.attempts)
    show_table(console, view.children)
    for group in view.resources:
        show_table(console, group)
    facts(console, view.artifacts, "Artifacts")
    show_table(console, view.attempt_evidence)
    for value in view.messages:
        console.print(Text(value, style="mf.secondary"))
    actions(console, view.commands)


def render_logs(console: Console, view: LogView, environment: str) -> None:
    header(console, "Run Logs", environment=environment)
    facts(console, (("Run ID", view.identity),))
    if view.history_removed:
        facts(console, (("History", "Removed · Evidence retained"),))
    if view.notice:
        console.print()
        empty_state(console, view.notice)
    if view.event_source:
        if view.events:
            show_table(
                console,
                TableView(
                    "Retained events",
                    ("Time (UTC)", "Level", "Stage", "Message"),
                    view.events,
                    len(view.events),
                ),
            )
            console.print(
                Text(f"{len(view.events):,} events shown from the start of the retained log.")
            )
        else:
            section(console, "Retained events")
            empty_state(console, "The retained event log is empty.")
        if view.truncated:
            console.print(
                Text(
                    "The event prefix reached the selected event or 1 MiB read limit.",
                    style="mf.warning",
                )
            )
    for stream in view.streams:
        section(console, stream.name)
        if stream.truncated:
            console.print(
                Text(
                    "Showing the last 12 KiB. The first displayed line may be partial.",
                    style="mf.warning",
                )
            )
        if stream.content:
            console.print(
                Text(
                    "\n".join("  " + line for line in stream.content.splitlines()), overflow="fold"
                )
            )
        else:
            empty_state(console, "The retained stream is empty.")
    actions(console, (command("report", view.identity, environment),))


def render_report(
    console: Console, view: ReportView, environment: str, selected: str = "all"
) -> None:
    header(console, view.title, environment=environment)
    presentation_state = {
        "report_missing_or_interrupted": "Report unavailable",
        "report_incomplete_or_invalid": "Invalid or incomplete report",
    }.get(view.status)
    summary(
        console, (*view.identity, ("Outcome", status_label(view.status, label=presentation_state)))
    )
    if selected in {"all", "summary"}:
        facts(console, view.lifecycle, "Lifecycle")
    if selected in {"all", "inputs"}:
        facts(console, view.inputs, "Selection and inputs")
    if selected in {"all", "outputs"}:
        facts(console, view.outputs, "Recorded outputs")
    table_sections = {
        "Discovery requests": "inputs",
        "Verified publications": "outputs",
        "Jobs": "outputs",
        "Conversion jobs": "outputs",
        "Conversion counters": "outputs",
        "Stages": "summary",
        "Stage reports": "artifacts",
    }
    for group in view.tables:
        if selected == "all" or selected == table_sections[group.title]:
            show_table(console, group)
    if selected in {"all", "diagnostics"} and (view.diagnostics or view.unknown_fields):
        section(console, "Diagnostics")
        for value in view.diagnostics:
            console.print(Text("  " + value))
        if view.unknown_fields:
            console.print(
                Text("Retained fields · " + ", ".join(view.unknown_fields), style="mf.secondary")
            )
    if selected in {"all", "artifacts"}:
        facts(console, view.artifacts, "Artifacts")
    actions(console, (view.machine_command,))


def render_maintenance(console: Console, view: MaintenanceView, environment: str) -> None:
    header(console, view.title, environment=environment)
    facts(console, view.summary)
    if view.results.records:
        show_table(console, view.results)
    else:
        console.print()
        empty_state(console, "No candidates were returned for this page.")


def render_deletion(console: Console, result: Document) -> None:
    view = deletion(result)
    header(console, "Run Deletion", environment=view.environment)
    status = {
        "preview": "Preview · No run data changed",
        "completed": "History removed · Evidence retained"
        if view.mode == "history_only"
        else "Completed",
        "already_deleted": "Already applied · Evidence retained"
        if view.mode == "history_only"
        else "Already deleted",
    }.get(view.status, label(view.status))
    facts(
        console,
        (
            ("Mode", "History only" if view.mode == "history_only" else "Full deletion"),
            ("Outcome", status),
            *view.summary,
        ),
    )
    show_table(console, view.targets)
    show_table(console, view.blockers)
    if view.retained:
        section(console, f"Retained artifact references ({len(view.retained):,})")
        for path in view.retained[:8]:
            console.print(Text(safe_text(path)))
    if view.missing:
        message = (
            "Reviewed artifact roots already absent"
            if view.status == "preview"
            else "Reviewed files already absent during retry"
        )
        console.print(Text(f"{message} · {view.missing:,}", style="mf.secondary"))
    if result.get("action") == "apply" and view.mode == "full":
        console.print(Text("Physical storage reclamation was not measured.", style="mf.secondary"))
    if view.command:
        section(console, "Apply reviewed plan" if view.status == "preview" else "Retry exact plan")
        console.print(Text(view.command), soft_wrap=True)


def render_deleted_run(
    console: Console, identity: str, removal: Document, environment: str
) -> None:
    header(console, "Run History", environment=environment)
    facts(
        console,
        (
            ("Run ID", identity),
            (
                "Outcome",
                "Run deleted"
                if removal["state"] == "removed"
                else "Deletion in progress or interrupted",
            ),
            ("Original status", label(removal.get("original_status"))),
            ("Receipt", str(removal["receipt_path"])),
        ),
    )
    console.print(Text("Ordinary run evidence is unavailable through this reference."))
