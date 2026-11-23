"""Render acquisition projections on a Rich console."""

import shlex
from collections.abc import Mapping
from itertools import groupby
from pathlib import Path
from typing import cast

from rich.cells import cell_len
from rich.console import Console
from rich.text import Text

from forge.historical.archives.sources.capabilities import SourceCapability
from forge.shared.documentation import ACQUISITION_CONFIGURATION, ACQUISITION_SOURCES
from forge.shared.presentation.acquisition import (
    DISPLAY_LIMIT,
    ConfigurationResult,
)
from forge.shared.presentation.acquisition import (
    AcquisitionResult as AcquisitionResult,
)
from forge.shared.terminal.presentation import (
    data_table,
    display_path,
    empty_state,
    header,
    reference,
    section,
    status_label,
    status_style,
    summary,
)


def render_sources(console: Console, capabilities: tuple[SourceCapability, ...]) -> None:
    header(console, "Historical Sources")
    table = data_table()
    for heading in ("Exchange", "Instrument", "Market", "Scope", "Trades", "L2 Depth"):
        table.add_column(heading)
    for key, rows in groupby(
        capabilities, key=lambda row: (row.exchange, row.instrument_type, row.market_category)
    ):
        group = tuple(rows)
        for scope in dict.fromkeys(row.scope for row in group):
            supported = {row.data_type for row in group if row.scope == scope}
            table.add_row(
                *key,
                scope,
                "Yes" if "trades" in supported else "—",
                "Yes" if "order_book_l2" in supported else "—",
            )
    console.print(table)
    console.print()
    console.print("Adapter support. Use map to discover archives for a specific scope and period.")
    reference(console, "Source reference", ACQUISITION_SOURCES)


def render_inventory(console: Console, items: tuple[ConfigurationResult, ...]) -> None:
    header(console, "Acquisition Configurations")
    if not items:
        empty_state(console, "No acquisition configurations found.")
        return
    table = data_table()
    table.add_column("Configuration", style="mf.identifier")
    table.add_column("Requests", justify="right")
    table.add_column("Period (UTC)")
    table.add_column("Structure")
    for item in items:
        if item.request_count is None:
            table.add_row(item.name, "—", "—", Text("Invalid", style="mf.failure"))
        else:
            table.add_row(
                item.name, str(item.request_count), item.period, Text("Valid", style="mf.success")
            )
    console.print(table)
    console.print()
    console.print(f"{len(items):,} configuration{'s' if len(items) != 1 else ''}")


def render_validation(console: Console, item: ConfigurationResult, *, root: Path) -> None:
    header(console, "Acquisition Validation")
    path = display_path(item.source, root)
    metadata = [("Configuration", item.name)]
    if item.request_count is not None:
        metadata.extend((("Requests", str(item.request_count)), ("Period", item.period)))
    if cell_len(path) + 15 <= console.width:
        summary(console, [*metadata, ("Source", path)])
    else:
        summary(console, metadata)
        reference(console, "Source", path)
    section(console, "Validation")
    if item.error:
        console.print(status_label("failed", label="Configuration structure invalid"))
        message = item.error.replace(str(item.source), path).removeprefix(f"{path}: ")
        console.print(Text(message, style="mf.failure"))
        reference(console, "Configuration reference", ACQUISITION_CONFIGURATION)
    else:
        console.print(status_label("valid", label="Configuration structure valid"))
    console.print()
    console.print("Source support and archive availability were not checked.", style="mf.secondary")


def render_result(console: Console, result: AcquisitionResult, *, root: Path | None = None) -> None:
    download = result.action == "download"
    title = "Download" if download else "Mapping"
    header(console, f"Acquisition {title}", environment=result.environment)
    summary(
        console,
        (
            ("Run", result.run_id),
            ("Recipe", result.recipe),
            ("Period", result.period),
            ("Status", status_label(result.status)),
        ),
    )
    section(console, "Request Results")
    table = data_table()
    for heading in ("Exchange", "Scope", "Market", "Data", "Discovery"):
        table.add_column(heading)
    if download:
        table.add_column("Result")
    else:
        table.add_column("Archives", justify="right")
    table.add_column("Source")
    for row in result.displayed_requests:
        discovery = "complete" if row.discovery == "archives" else row.discovery
        values = (
            Text(row.exchange),
            Text(row.scope, style="mf.identifier"),
            Text(row.market),
            Text(row.data),
            Text(discovery.capitalize(), style=status_style(discovery)),
        )
        if download:
            table.add_row(
                *values,
                Text(row.status.capitalize(), style=status_style(row.status)),
                Text(row.cache),
            )
        else:
            table.add_row(*values, Text(row.archives), Text(row.cache))
    if result.requests:
        console.print(table)
    else:
        empty_state(console, "No request outcomes recorded.")
    if len(result.requests) > DISPLAY_LIMIT:
        console.print(
            f"Showing {DISPLAY_LIMIT:,} of {len(result.requests):,} requests. See the retained report.",
            style="mf.secondary",
        )
    issues = result.issues
    if issues:
        section(console, "Issues")
        for row in issues[:5]:
            console.print(
                Text(
                    f"{row.exchange} · {row.scope} · {row.data}\n  {row.error}", style="mf.failure"
                )
            )
        if len(issues) > 5:
            console.print(f"{len(issues) - 5:,} more issues in the retained report.")
    section(console, "Download Summary" if download else "Discovery Summary")
    summary(console, result.counts, numeric=True)
    if result.registered_archives is not None:
        section(console, "Registration")
        summary(console, (("Distinct registered local archives", str(result.registered_archives)),))
    section(console, "Coverage")
    console.print(Text(result.coverage), style="mf.secondary")
    console.print("Market-data coverage has not been established.", style="mf.warning")
    if result.warning:
        console.print(Text(result.warning, style="mf.warning"))
    console.print()
    flags = " --testing" if result.environment == "testing" else ""
    reference(console, "Run details", f"mf runs report {shlex.quote(result.run_id)}{flags}")
    reference(console, "Report", display_path(Path(result.report), root or Path.cwd()))


def render_plan(
    console: Console, result: dict[str, object], *, name: str, environment: str
) -> None:
    from forge.orchestration.registry import rows
    from forge.orchestration.run_summary import mapping

    header(console, "Acquisition Plan", environment=environment)
    console.print("Cache-only inspection. Use map to discover archive availability.")
    console.print()
    requests = rows(result.get("requests"))
    archives = rows(result.get("sampled_archives"))
    summary(
        console,
        (
            ("Configuration", name),
            ("Requests", str(len(requests))),
            ("Sampled archives", str(len(archives))),
        ),
    )
    section(console, "Request Results")
    table = data_table()
    for heading in ("Exchange", "Scope", "Data", "Discovery", "Archives"):
        table.add_column(heading, justify="right" if heading == "Archives" else "left")
    for row in requests[:DISPLAY_LIMIT]:
        request = mapping(row.get("request"))
        scope = mapping(request.get("scope"))
        table.add_row(
            str(request.get("exchange", "")).title(),
            str(scope.get("identifier", "")),
            str(request.get("data_type", "")),
            "Cached" if "scan_id" in row else "Unresolved",
            str(row.get("archives_observed", "—")),
        )
    console.print(table)
    if len(requests) > DISPLAY_LIMIT:
        console.print(
            f"Showing {DISPLAY_LIMIT} of {len(requests)} requests. Use --json for all requests."
        )
    render_evidence(
        console,
        "Sampled Archives",
        archives,
        (
            ("File", "filename"),
            ("State", "state"),
            ("Local ID", "local_archive_id"),
        ),
    )
    console.print(Text(str(result.get("verification_scope", "")), style="mf.secondary"))
    console.print("Market-data coverage has not been established.", style="mf.warning")


def render_evidence(
    console: Console,
    title: str,
    records: list[Mapping[str, object]],
    columns: tuple[tuple[str, str], ...],
) -> None:
    if not records:
        return
    section(console, title)
    table = data_table()
    for label, _ in columns:
        table.add_column(label)
    for row in records[:DISPLAY_LIMIT]:
        table.add_row(
            *(Text(str(row[key]) if row.get(key) is not None else "—") for _, key in columns)
        )
    console.print(table)
    if len(records) > DISPLAY_LIMIT:
        console.print(
            f"Showing {DISPLAY_LIMIT} of {len(records)} entries. Use --json for all entries.",
            style="mf.secondary",
        )


def render_external(
    console: Console,
    document: Mapping[str, object],
    *,
    operation: str,
    action: str,
    name: str,
    root: Path,
) -> None:
    from forge.orchestration.registry import rows
    from forge.orchestration.run_summary import mapping

    header(
        console,
        f"Acquisition {operation} · {action.title()}",
        environment=str(document.get("environment", "")),
    )
    details = [("Configuration", name)]
    details.extend(
        (label, str(document[key]))
        for label, key in (
            ("Run", "run_id"),
            ("Status", "status"),
            ("Total inputs", "total_inputs"),
            ("Sampled inputs", "sampled_inputs"),
            ("Input count", "input_count"),
            ("Existing source objects", "existing_source_objects"),
        )
        if key in document
    )
    for key in ("path", "created"):
        if isinstance(destination := document.get(key), str):
            details.append(("Draft", display_path(Path(destination), root)))
    readiness = mapping(document.get("readiness"))
    if readiness.get("normalization_handoff"):
        details.append(("Normalization handoff", str(readiness["normalization_handoff"]).title()))
    summary(console, details)
    for key in ("validation", "normalization"):
        if isinstance(message := document.get(key), str):
            console.print(Text(message, style="mf.secondary"))
    unresolved = document.get("unresolved")
    if isinstance(unresolved, list):
        console.print(
            Text(
                "Required declarations: "
                + ", ".join(str(item) for item in cast(list[object], unresolved)),
                style="mf.warning",
            )
        )
    inputs = rows(document.get("inventory") or document.get("inputs"))
    if inputs:
        preview: list[Mapping[str, object]] = []
        for row in inputs[:DISPLAY_LIMIT]:
            sample = mapping(row.get("sample"))
            preview.append({**row, "format": sample.get("reason") or sample.get("layout")})
        render_evidence(
            console,
            "Inputs",
            preview,
            (
                ("Reference", "reference"),
                ("Bytes", "bytes" if action == "inspect" and operation == "Import" else "size"),
                *((("Sample", "format"),) if operation == "Import" and action == "inspect" else ()),
            ),
        )
        if len(inputs) > DISPLAY_LIMIT:
            console.print(
                f"Showing {DISPLAY_LIMIT} of {len(inputs)} inputs. Use --json for all inputs."
            )
    render_evidence(
        console,
        "Planned Actions",
        rows(document.get("intended_actions")),
        (
            ("Reference", "reference"),
            ("Action", "action"),
            ("Destination", "destination"),
        ),
    )
    render_evidence(
        console,
        "Registered Copies",
        rows(document.get("copies")),
        (
            ("Input", "input"),
            ("Status", "status"),
            ("Path", "relative_path"),
        ),
    )
    counts = mapping(document.get("counts"))
    if counts:
        section(console, "Import Summary")
        summary(
            console,
            ((key.replace("_", " ").title(), str(value)) for key, value in counts.items()),
            numeric=True,
        )
    for label, key in (("Report", "report"), ("Report", "report_path"), ("Events", "events")):
        if isinstance(path := document.get(key), str):
            reference(console, label, display_path(Path(path), root))
