"""Compact preparation summaries using the shared terminal palette."""

from rich.console import Console

from forge.markets.models import array, obj


def render_batch(console: Console, result: dict[str, object]) -> None:
    console.print("Normalization · prepare", style="mf.heading")
    console.print(f"Recipe: {result.get('recipe', 'unknown')}", markup=False)
    status = str(result["status"])
    console.print(
        f"Status: {status}" + (" · dry run" if result.get("dry_run") else ""),
        style="mf.warning" if status in {"partial", "failed"} else "mf.success",
    )
    console.print(
        f"Jobs: {result['prepared_job_count']} · Tasks: {result['prepared_task_count']}"
        f" · Streams: {result['prepared_stream_count']}",
    )
    summary = obj(result.get("diagnostic_summary", {}))
    if summary:
        console.print(
            f"Matched archives: {summary['matched_archive_count']} · Blocking archives: {summary['blocking_archive_count']}"
        )
        for value in array(summary.get("category_totals", [])):
            group = obj(value)
            label = (
                "Blocker"
                if group["severity"] == "blocker"
                else "Coverage warning (not a selection blocker)"
            )
            console.print(
                f"{label}: {group['category']} · {group['archive_count']} archives",
                style="mf.warning",
            )
        if summary.get("coverage_gap_count"):
            console.print(f"Coverage gaps: {summary['coverage_gap_count']}", style="mf.warning")
    if status == "partial":
        console.print(
            "Preparation is incomplete; input-resolution blockers and coverage gaps are recorded in the JSON diagnostics.",
            style="mf.warning",
        )
    if status == "partial" and result.get("dry_run"):
        console.print(
            "Use global --json for full per-archive and grouped diagnostics; no report was written.",
            style="mf.secondary",
        )
    ranking = obj(result.get("ranking_resolution", {}))
    if ranking.get("snapshot"):
        console.print(
            f"Ranking: {ranking['policy']} · {ranking['snapshot']} (persisted)",
            style="mf.secondary",
        )
    elif ranking.get("fallback"):
        console.print(
            f"Ranking: no published {ranking['policy']} snapshot; deterministic stream identity order.",
            style="mf.secondary",
        )
    compatibility = obj(result.get("engine_compatibility", {}))
    version = result.get("batch_schema_version", compatibility.get("current_rust_protocol_version"))
    if version is not None:
        console.print(
            f"Contract: Rust normalization batch schema v{version}.", style="mf.secondary"
        )
    if compatibility.get("direct_execution_supported") is False:
        console.print(
            "This batch contract is not supported by the configured Rust engine.",
            style="mf.warning",
        )
    elif result.get("engine_ready"):
        console.print(
            "Rust contract validation: passed; normalization has not been executed.",
            style="mf.secondary",
        )
    else:
        console.print(
            "Rust contract validation: not performed"
            + (" (dry run)." if result.get("dry_run") else "."),
            style="mf.secondary",
        )
    suffix = " (not written)" if result.get("dry_run") else ""
    for label, key in (("Batch", "batch_path"), ("Report", "report_path")):
        console.print(f"{label}: {result[key]}{suffix}", style="mf.identifier", markup=False)


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
