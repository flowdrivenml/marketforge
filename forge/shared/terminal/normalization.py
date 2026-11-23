"""Normalization CLI views and the shared legacy execution entry point for merge."""

from pathlib import Path

from rich.console import Console
from rich.text import Text

from forge.markets.models import array, obj
from forge.shared.presentation.normalization import (
    LIMIT,
    NormalizationOutcome,
    PreparationView,
    RecipeView,
    binary_size,
    number,
)
from forge.shared.terminal.presentation import (
    data_table,
    display_path,
    empty_state,
    header,
    reference,
    section,
    status_label,
    summary,
)


def _table(
    console: Console,
    headings: tuple[str, ...],
    entries: tuple[tuple[str, ...], ...],
    *,
    numeric: tuple[str, ...] = (),
) -> None:
    table = data_table()
    for heading in headings:
        table.add_column(
            heading, overflow="fold", justify="right" if heading in numeric else "left"
        )
    for row in entries[:LIMIT]:
        table.add_row(*(Text(value) for value in row))
    console.print(table)
    if len(entries) > LIMIT:
        console.print(
            f"Showing {LIMIT} of {len(entries)} entries. Use --json for full details.",
            style="mf.secondary",
        )


def render_inventory(console: Console, recipes: tuple[RecipeView, ...]) -> None:
    header(console, "Normalization Recipes")
    if not recipes:
        empty_state(console, "No normalization recipes found.")
        return
    table = data_table()
    table.add_column("Recipe", style="mf.identifier", overflow="fold")
    table.add_column("Inputs", justify="right")
    table.add_column("Period (UTC)", overflow="fold")
    table.add_column("Status")
    for recipe in recipes:
        count = recipe.count
        table.add_row(
            recipe.name,
            str(count) if count is not None else "—",
            recipe.period(compact=True),
            Text(
                "Valid" if count is not None else "Invalid",
                style="mf.success" if count is not None else "mf.failure",
            ),
        )
    console.print(table)
    console.print()
    console.print(f"{len(recipes):,} recipe{'s' if len(recipes) != 1 else ''}")


def render_recipe(console: Console, item: RecipeView, *, action: str, root: Path) -> None:
    header(
        console,
        "Normalization Validation" if action in {"validate", "edit"} else "Normalization Recipe",
    )
    details: list[tuple[str, str | Text]] = [("Recipe", item.name)]
    if action == "new":
        details.append(("Status", status_label("complete", label="CREATED")))
    details.extend(item.detail(overrides=action == "show"))
    summary(console, details)
    reference(console, "Configuration", display_path(item.source, root))
    if action == "show" and item.configuration is not None:
        title, headings, entries = item.entries()
        section(console, title)
        _table(console, headings, entries)
        if (count := item.count) is not None and count > len(entries):
            console.print(
                f"Showing {len(entries)} of {count} entries. Use --json for full details.",
                style="mf.secondary",
            )
        return
    section(console, "Validation")
    if item.error:
        console.print(status_label("failed", label="Recipe structure invalid"))
        message = item.error.replace(str(item.source), display_path(item.source, root))
        console.print(Text(message, style="mf.failure"))
        console.print("Correct the reported issue and rerun validation.")
    else:
        console.print(status_label("valid", label="Recipe structure valid"))
        console.print(
            "Input availability, instrument resolution, and Engine validation were not checked.",
            style="mf.secondary",
        )
        if action == "new":
            reference(console, "Next", f"mf normalize prepare {item.name} --testing --dry-run")


def render_batch(console: Console, result: PreparationView, *, root: Path) -> None:
    header(console, "Normalization Preparation", environment=result.environment)
    if result.dry_run:
        console.print("DRY RUN", style="mf.warning")
    summary(console, (*result.identity, ("Status", status_label(result.status))))
    if result.error:
        console.print(Text(result.error, style="mf.failure"))
    if result.resolution:
        section(console, "Input Resolution")
        summary(console, result.resolution, numeric=True)
    for severity, title in (
        ("blocker", "Blockers"),
        ("warning", "Coverage and Metadata Warnings"),
        ("info", "Observations"),
    ):
        categories = [
            (category.replace("_", " ").capitalize(), count)
            for kind, category, count in result.categories
            if kind == severity
        ]
        examples = [example for kind, example in result.examples if kind == severity]
        if categories or examples:
            section(console, title)
            if categories:
                summary(console, categories[:8], numeric=True)
            for example in examples[:3]:
                console.print(
                    Text(
                        f"  {example}",
                        style="mf.failure" if severity == "blocker" else "mf.warning",
                    )
                )
            if len(categories) > 8 or len(examples) > 3:
                console.print("Use --json for complete diagnostics.", style="mf.secondary")
    if result.ranking:
        section(console, "Ranking Resolution")
        summary(console, result.ranking)
    if result.artifacts or result.compatibility:
        section(console, "Rust Validation")
        if result.engine_ready:
            console.print(status_label("valid", label="Prepared batch contract validated"))
            console.print("Preparation finished before execution.", style="mf.secondary")
        else:
            console.print(
                Text(
                    result.compatibility or "Prepared batch validation was not confirmed.",
                    style="mf.warning",
                )
            )
    if result.artifacts:
        section(console, "Planned Artifacts · Not Written" if result.dry_run else "Artifacts")
        for label, path in result.artifacts:
            reference(console, label, display_path(Path(path), root))
    elif result.dry_run:
        console.print("No batch or report was written.", style="mf.secondary")


def render_normalization_outcome(
    console: Console,
    result: NormalizationOutcome,
    *,
    root: Path,
    registering: bool,
    recovery: bool = False,
) -> None:
    title = "Normalization Registration" if registering else "Normalization"
    header(console, title, environment=result.environment)
    if recovery:
        console.print("Registration recovery", style="mf.identifier")
    details: list[tuple[str, str | Text]] = [
        *result.identity,
        (
            "Status",
            status_label(result.status, label="COMPLETED" if result.status == "complete" else None),
        ),
    ]
    if result.engine:
        details.append(("Engine", result.engine.replace("_", " ")))
    if result.verification:
        details.append(("Verification", status_label(result.verification)))
    if result.registration and not registering:
        details.append(("Registration", status_label(result.registration)))
    if result.duration:
        details.append(("Engine duration", result.duration))
    summary(console, details)
    if result.processing:
        section(console, "Processing Summary")
        summary(console, result.processing, numeric=True)
    if result.publications:
        section(console, "Published Datasets")
        table = data_table()
        for label in ("Type", "Engine Dataset ID", "Rows", "Files", "Size", "Integrity"):
            table.add_column(
                label,
                justify="right" if label in {"Rows", "Files", "Size"} else "left",
                overflow="fold",
            )
        for row in result.publications[:LIMIT]:
            table.add_row(
                Text(row.content_type),
                Text(row.engine_id, style="mf.identifier"),
                number(row.rows),
                number(row.files),
                binary_size(row.size_bytes),
                Text(row.integrity),
            )
        console.print(table)
        if len(result.publications) > LIMIT:
            console.print(
                f"Showing {LIMIT} of {number(len(result.publications))} datasets. "
                "See the retained report for all publications.",
                style="mf.secondary",
            )
    if result.registration_counts:
        section(console, "Registration")
        summary(console, result.registration_counts)
    if result.diagnostics or result.diagnostic_path:
        section(console, "Diagnostics")
        for issue in result.diagnostics[:8]:
            console.print(Text(issue, style="mf.warning"))
        if len(result.diagnostics) > 8:
            console.print("See the retained report for all issues.", style="mf.secondary")
        if result.diagnostic_path:
            reference(console, "Engine log", display_path(Path(result.diagnostic_path), root))
    if result.artifacts:
        section(console, "Artifacts")
        artifacts = [
            (
                label,
                display_path(Path(value), root)
                if label in {"Report", "Execution intent"}
                else value,
            )
            for label, value in result.artifacts
        ]
        label_width = max(len(label) for label, _ in artifacts)
        inline = [
            (label, value)
            for label, value in artifacts
            if label_width + 2 + len(value) <= console.width
        ]
        if inline:
            summary(console, inline)
        for label, value in artifacts:
            if label_width + 2 + len(value) > console.width:
                if len(value) + 2 > console.width and label in {"Report", "Execution intent"}:
                    parent, _, name = value.rpartition("/")
                    if parent:
                        value = f"{parent}/\n  {name}"
                reference(console, label, value)
    if result.next_action:
        console.print()
        reference(
            console,
            "Inspect or recover"
            if result.status not in {"complete", "completed", "registered", "partial"}
            else "Inspect results",
            result.next_action,
        )


def render_execution(console: Console, result: dict[str, object]) -> None:
    """Render the same durable outcome for foreground and TUI invocations."""
    operation = str(result.get("operation", "processing")).capitalize()
    status = str(result["status"])
    console.print(f"{operation} · {status}", style="mf.heading", markup=False)
    console.print(
        f"Run {result.get('run_id', result.get('execution_id'))}"
        f" · Configuration {result.get('recipe') or result.get('submitted_source') or result.get('submission_path')}",
        markup=False,
    )
    execution = result.get("normalization", result.get("execution", "unknown"))
    console.print(
        f"Execution {execution} · Verification {result.get('verification', 'not started')}"
        f" · Registration {result.get('registration', 'not started')}",
        markup=False,
    )
    counts = obj(result.get("counts", {}))
    if counts:
        console.print(" · ".join(f"{key.capitalize()} {value}" for key, value in counts.items()))
    jobs = array(result.get("jobs", []))
    if result.get("dataset"):
        jobs = [
            {
                **obj(result["dataset"]),
                "path": result.get("path"),
                "catalogue_dataset_id": result.get("catalogue_dataset_id"),
            }
        ]
    for value in jobs:
        job = obj(value)
        failure = job.get("failure")
        if failure:
            console.print(
                f"Job {job['job_id']} failed · {obj(failure).get('message')}",
                style="mf.warning",
                markup=False,
            )
            continue
        engine = job.get("dataset_id", job.get("engine_dataset_id"))
        console.print(
            f"Engine publication {engine} · {job.get('path')}",
            markup=False,
        )
        console.print(
            f"Rows {job.get('events_written')} · Files {job.get('files_written')}"
            f" · Bytes {job.get('bytes_written')}"
            f" · Observed ns {job.get('start_timestamp_ns')}–{job.get('end_timestamp_ns')}"
            f" · Integrity {job.get('integrity_status', 'see manifest')}",
            markup=False,
        )
    for path, dataset in obj(result.get("catalogue_dataset_ids", {})).items():
        console.print(f"Dataset {dataset} · {path}", markup=False)
    coverage = obj(result.get("coverage", {}))
    if coverage.get("observed_extent_gaps"):
        console.print(
            "The requested interval extends beyond some observed stream extents.",
            style="mf.warning",
        )
    for key in ("error", "registration_error", "report_path", "execution_receipt", "recovery"):
        if value := result.get(key):
            console.print(str(value), markup=False)
