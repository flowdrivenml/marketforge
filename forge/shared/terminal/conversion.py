"""Rich rendering for conversion recipe and execution evidence."""

import shlex
from collections.abc import Iterable
from pathlib import Path
from typing import Any, cast

from rich.console import Console
from rich.text import Text

from forge.historical.conversion.recipes import Source
from forge.shared.documentation import CONVERSION_RECIPE
from forge.shared.presentation.conversion import ConversionView, RecipeView
from forge.shared.presentation.merge import utc_ns
from forge.shared.presentation.normalization import binary_size
from forge.shared.terminal.presentation import (
    data_table,
    display_path,
    documentation_link,
    empty_state,
    header,
    reference,
    section,
    status_label,
    summary,
)


def table(console: Console, columns: tuple[str, ...], rows: Iterable[tuple[object, ...]]) -> None:
    grid = data_table()
    for column in columns:
        grid.add_column(column, overflow="fold")
    for row in rows:
        grid.add_row(*(Text(str(value)) for value in row))
    console.print(grid)


def interval(bounds: dict[str, Any]) -> str:
    if not bounds:
        return "Unavailable"
    return f"{utc_ns(bounds['start_ns'])} → {utc_ns(bounds['end_ns'])} (end exclusive)"


def render_inventory(console: Console, views: list[RecipeView], *, ordering: str = "") -> None:
    header(console, "Convert · Recipes")
    if ordering:
        console.print(Text(ordering, style="mf.secondary"))
    if not views:
        empty_state(
            console,
            "No conversion recipes. Use mf convert new NAME --from-merge SOURCE --target-quote USDT.",
        )
        return
    details = console.width >= 96
    table(
        console,
        ("Recipe", "Source", "Quote", *(("Interval and scope",) if details else ()), "Local state"),
        (
            (
                view.path.stem,
                view.source_label
                + (
                    "\n" + interval(view.publication.interval.model_dump()) + " · Whole publication"
                    if view.publication and not details
                    else ""
                ),
                view.recipe.conversion.target_quote if view.recipe else "—",
                *(
                    (
                        interval(view.recipe.interval.model_dump())
                        + (" · Whole publication" if view.publication else "")
                        if view.recipe
                        else "—",
                    )
                    if details
                    else ()
                ),
                "Declared · Registry unchecked" if view.publication else view.state,
            )
            for view in views
        ),
    )


def render_recipe(console: Console, view: RecipeView, root: Path, *, action: str) -> None:
    header(
        console,
        "Convert · "
        + {"new": "Recipe created", "edit": "Recipe edit", "show": "Recipe"}.get(action, "Recipe"),
    )
    summary(
        console,
        [
            ("Recipe", view.path.stem),
            ("Configuration", display_path(view.path, root)),
            ("Local state", status_label(view.state)),
        ],
    )
    if view.diagnostic:
        console.print(Text(view.diagnostic, style="mf.warning"))
    recipe = view.recipe
    if recipe:
        section(console, "Upstream Binding")
        source = recipe.source
        if isinstance(source, Source):
            summary(
                console,
                [
                    ("Source", view.source_label),
                    ("Bound path", source.path),
                    ("Fingerprint", "Matches" if view.state == "valid" else "Unverified"),
                    ("Requested interval", interval(recipe.interval.model_dump())),
                ],
            )
        else:
            summary(
                console,
                [
                    ("Source type", "Merged publication"),
                    ("Binding", "Bound declaration" if action == "new" else "Declared locally"),
                    ("Registry publication ID", str(source.registry_publication_id)),
                    ("Engine publication ID", source.engine_publication_id),
                    ("Bound environment", source.environment),
                    ("Registry identity", source.registry_id),
                    ("Committed interval", interval(source.interval.model_dump())),
                    ("Execution scope", "Whole publication"),
                ],
            )
        policy = recipe.conversion
        section(console, "Policy")
        summary(
            console,
            [
                ("Target quote", policy.target_quote),
                ("Price source", policy.price_source),
                ("Maximum age", age(policy.max_staleness_ns)),
                ("Max hops", str(policy.max_hops)),
                ("Path policy", policy.path_policy),
                ("Instrument kinds", listing(policy.instrument_kinds, "All")),
            ],
        )
        section(console, "Selection and References")
        selection = recipe.selection
        summary(
            console,
            [
                ("Registry publications", listing(selection.publication_ids)),
                ("Registered files", listing(selection.file_dataset_ids)),
                ("Expected Engine IDs", listing(selection.expected_engine_publication_ids)),
                (
                    "Whole-publication approval",
                    "Explicit"
                    if selection.whole_publication
                    else "Request scope checked at preparation",
                ),
                ("Trade reference streams", listing(recipe.reference_stream_ids)),
                ("External registry publication", str(recipe.reference_publication_id or "None")),
            ],
        )
        section(console, "Requested Resources")
        summary(console, resources(recipe.resources.model_dump()))
        if action == "new":
            summary(
                console,
                [
                    ("Reference configuration", "Review explicit reference trade selection"),
                    ("Execution readiness", "Assessed during preparation"),
                ],
            )
            environment = view.publication.environment if view.publication else "testing"
            reference(
                console,
                "Next",
                f"mf convert prepare {shlex.quote(view.path.stem)} --{environment} --dry-run",
            )
    documentation_link(console, CONVERSION_RECIPE)


def age(nanoseconds: int) -> str:
    seconds, remainder = divmod(nanoseconds, 1_000_000_000)
    if not seconds:
        return f"{nanoseconds:,} ns"
    exact = f"{seconds:,}.{remainder:09d}".rstrip("0").rstrip(".")
    return f"{exact} s ({nanoseconds:,} ns)"


def listing(values: Iterable[object] | None, default: str = "None configured") -> str:
    return ", ".join(map(str, values or ())) or default


def resources(values: dict[str, Any]) -> list[tuple[str, str]]:
    return [
        (label, f"{values[key]:,}{unit}")
        for key, label, unit in (
            ("workers", "Workers", ""),
            ("memory_limit_mb", "Memory budget", " MiB"),
            ("max_open_files", "Open files", ""),
        )
        if key in values
    ]


def render(console: Console, view: ConversionView, root: Path) -> None:
    report = view.result
    header(console, f"Convert · {view.action.title()}", environment=view.environment)
    overview: list[tuple[str, str | Text]] = [
        ("Reference", view.reference),
        (
            "Status",
            status_label(
                "prepared" if view.status == "ready to run" else view.status,
                label=view.status.upper(),
            ),
        ),
    ]
    if report.get("run_id"):
        overview.append(("Forge run", str(report["run_id"])))
    if view.action == "run":
        overview.append(("Elapsed", f"{view.elapsed:.2f} s"))
        overview.append(("Catalogue registration", "Unavailable for converted outputs"))
    if "compatibility" in report:
        overview.append(("Rust validation", report["compatibility"].replace("_", " ")))
    if report.get("dry_run"):
        overview.append(("Mode", "Dry run · paths are prospective"))
    summary(console, overview)
    if view.action in {"prepare", "run"} and report.get("run_id") and view.environment:
        reference(console, "Inspect run", f"mf runs report {report['run_id']} --{view.environment}")
    if report.get("requested_interval"):
        section(console, "Selection")
        summary(
            console,
            [
                ("Requested interval", interval(report["requested_interval"])),
                ("Execution scope", "Complete selected merge publications"),
                ("Candidate publications", str(len(report.get("candidates", ())))),
                ("Selected publications", str(len(report.get("sources", ())))),
                ("Job specifications", str(len(view.jobs))),
            ],
        )
        if report.get("sources"):
            table(
                console,
                (
                    "Registry publication",
                    "Engine publication",
                    "Content",
                    "Rows",
                    "Instruments",
                    "Streams",
                ),
                (
                    (
                        item["registry_publication_id"],
                        item["engine_publication_id"],
                        item["mode"],
                        f"{item['events']:,}",
                        len({stream["instrument_id"] for stream in item["streams"]}),
                        len(item["streams"]),
                    )
                    for item in report["sources"]
                ),
            )
        elif report.get("candidates"):
            table(
                console,
                ("Registry publication", "Engine publication", "Result"),
                (
                    (
                        item["registry_publication_id"],
                        item["engine_publication_id"],
                        item.get("reason")
                        or ("Eligible" if item.get("eligible") else "Unavailable"),
                    )
                    for item in report["candidates"]
                ),
            )
    if settings := report.get("settings"):
        section(console, "Conversion Settings")
        summary(
            console,
            [
                (
                    key.replace("_", " ").capitalize(),
                    age(value)
                    if key == "max_staleness_ns"
                    else listing(cast(list[object], value))
                    if isinstance(value, list)
                    else str(value),
                )
                for key, value in settings.items()
                if value is not None
            ],
        )
    if requested := report.get("resources"):
        section(console, "Resource Requests and Effective Limits")
        effective = dict(resources(report.get("effective_resource_limits", {})))
        summary(
            console,
            [
                (label, f"{value} requested · {effective.get(label, 'unavailable')} effective")
                for label, value in resources(requested)
            ],
        )
    if layers := report.get("validation_layers"):
        section(console, "Validation")
        summary(console, [(key.title(), value) for key, value in layers.items()])
    elif view.action == "validate" and view.status == "valid":
        summary(console, [("Checks", report["validation"])])
    if states := report.get("reuse_plan"):
        section(console, "Reuse Plan")
        summary(
            console,
            [
                (
                    str(job),
                    item["status"].replace("_", " ")
                    + (f" · {item['reason']}" if item.get("reason") else ""),
                )
                for job, item in states.items()
            ],
        )
    for job in view.jobs:
        render_job(console, job, root, view.environment)
    if view.action == "run":
        summary(
            console,
            [
                (
                    "Jobs executed",
                    str(
                        sum(
                            job.get("status") == "complete" and job.get("disposition") != "reused"
                            for job in view.jobs
                        )
                    ),
                ),
                ("Jobs reused", str(sum(job.get("disposition") == "reused" for job in view.jobs))),
                (
                    "Jobs failed or blocked",
                    str(sum(job.get("status") != "complete" for job in view.jobs)),
                ),
                (
                    "Jobs not attempted",
                    listing(report.get("not_attempted_job_ids"), "None reported"),
                ),
            ],
        )
    if pending := report.get("not_attempted_job_ids"):
        summary(console, [("Unattempted Engine jobs", listing(pending))])
    if view.diagnostics:
        section(console, "Diagnostics")
        for item in view.diagnostics:
            console.print(Text(item, style="mf.warning"))
    artifacts = [
        ("Recipe or job", report.get("path")),
        ("Forge collection", report.get("collection_path") or report.get("job_path")),
        ("Preparation report", report.get("report_path")),
        ("Runtime snapshot", report.get("artifacts", {}).get("runtime", {}).get("path")),
    ]
    if retained := [(label, display_path(Path(path), root)) for label, path in artifacts if path]:
        section(console, "Artifacts")
        summary(console, retained)


def render_job(console: Console, job: dict[str, Any], root: Path, environment: str) -> None:
    section(console, f"Engine Job {job['job_id']}")
    if job.get("engine_validation"):
        summary(
            console,
            [("Reference selection", f"{len(job.get('references', ())):,} explicit trade streams")],
        )
    if refs := job.get("references"):
        table(
            console,
            ("Market", "Provider", "Registry publication", "Trade stream"),
            (
                (
                    ref["identity"]["symbol"],
                    ref["provider"],
                    ref["merge_registry_publication_id"],
                    ref["stream_id"],
                )
                for ref in refs
            ),
        )
    if routes := job.get("planned_routes"):
        table(
            console,
            ("Instrument / stream", "Quote", "Planned routes"),
            (
                (
                    f"{row['instrument']} · {row['stream_id']}",
                    f"{row['source_quote']} → {row['target_quote']}",
                    listing(
                        (
                            kind + " · " + " → ".join(path)
                            for kind, path in zip(row["route_kinds"], row["routes"], strict=True)
                        ),
                        "No eligible route",
                    )
                    if row["selected"]
                    else "Outside conversion policy",
                )
                for row in routes
            ),
        )
        console.print(
            "Planned routes describe reachability. Observation freshness is evaluated during execution.",
            style="mf.secondary",
        )
    details = [
        (label, str(job[key]).replace("_", " "))
        for key, label in (
            ("status", "Outcome"),
            ("execution", "Execution"),
            ("indexing", "Reuse indexing"),
            ("engine_validation", "Rust validation"),
            ("source_registry_publication_id", "Source registry publication"),
        )
        if key in job
    ]
    if job.get("disposition") == "reused":
        console.print(Text(job["reuse_message"], style="mf.success"))
    if job.get("execution") == "complete" and not job.get("publication"):
        details.append(("Physical verification", "Failed"))
    if publication := job.get("publication"):
        manifest = Path(publication["manifest"])
        details.extend(
            [
                ("Engine publication", str(job["job_id"])),
                ("Publication", "Committed"),
                ("Physical verification", "Verified"),
                ("Output location", display_path(manifest.parent, root)),
                ("Manifest", display_path(manifest, root)),
            ]
        )
        if job.get("work_key") and job.get("status") == "complete":
            details.append(("Indexed output reference", job["work_key"]))
            if not job.get("indexing"):
                details.append(("Reuse indexing", "Available"))
        if "size_bytes" in publication:
            details.append(("Primary file size", binary_size(publication["size_bytes"])))
        metrics = publication.get("statistics", {})
        details.extend(
            (label, f"{metrics[key]:,}")
            for key, label in (
                ("events_written", "Events written"),
                ("identity", "Identity valuations"),
                ("converted", "Converted valuations"),
                ("unavailable", "Unavailable valuations"),
                ("skipped", "Skipped valuations"),
                ("reference_updates", "Reference updates"),
            )
            if key in metrics
        )
        depth: dict[str, Any] = metrics.get("depth") or {}
        details.extend(
            (label, f"{depth[key]:,}")
            for key, label in (
                ("levels_read", "Depth levels read"),
                ("levels_converted", "Depth levels converted"),
                ("levels_unavailable", "Depth levels unavailable"),
                ("levels_skipped", "Depth levels skipped"),
            )
            if key in depth
        )
        if "first_timestamp_ns" in publication.get("output", {}):
            output = publication["output"]
            details.extend(
                [
                    ("Content", str(output["content_type"])),
                    (
                        "Observed bounds",
                        f"{utc_ns(output['first_timestamp_ns'])} → {utc_ns(output['last_timestamp_ns'])}",
                    ),
                ]
            )
        if "materialized_partitions" in publication:
            details.append(
                ("Materialized partitions", f"{publication['materialized_partitions']:,}")
            )
        if "events_read" in publication:
            details.append(("Events read", f"{publication['events_read']:,}"))
        if "files_written" in publication.get("output", {}):
            details.append(("Primary files", f"{publication['output']['files_written']:,}"))
    for key, label in (
        ("path", "Rust job"),
        ("intended_job_path", "Planned Rust job"),
        ("result_path", "Execution report"),
        ("error", "Diagnostic"),
    ):
        if job.get(key):
            details.append((label, str(job[key])))
    summary(console, details)
    if (
        publication
        and job.get("status") == "complete"
        and job.get("work_key")
        and environment in {"testing", "production"}
    ):
        reference(
            console, "Inspect", f"mf datasets show --output-ref {job['work_key']} --{environment}"
        )
