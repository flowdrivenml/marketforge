"""Rich-free run history, retained evidence and maintenance projections."""

import re
import shlex
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from forge.orchestration.inspection import MANAGED_RUN
from forge.orchestration.registry import rows, status_of
from forge.orchestration.registry_maintenance import MaintenanceEvidence
from forge.orchestration.run_summary import mapping, when
from forge.orchestration.runs import RunInspection
from forge.orchestration.sorting import RUNS
from forge.shared.identity import RunOperation
from forge.shared.presentation.merge import MergeView
from forge.shared.presentation.normalization import (
    NormalizationOutcome,
    PreparationView,
    binary_size,
)

Document = Mapping[str, object]
Fields = tuple[tuple[str, str], ...]
DISPLAY_LIMIT = 20

# Consume complete escape sequences first. Strip remaining control characters,
# including bidi overrides, so retained text cannot control terminal layout.
ESCAPES = re.compile(
    r"(?:\x1b\]|\x9d)[^\x07\x1b\x9c]*(?:\x07|\x1b\\|\x9c|$)"
    r"|(?:\x1b\[|\x9b)[0-?]*[ -/]*[@-~]"
)
CONTROLS = re.compile(r"[\x00-\x08\x0b-\x1f\x7f-\x9f\u202a-\u202e\u2066-\u2069]")
CREDENTIALS = re.compile(
    r"(?i)(\b(?:password|passwd|token|(?:api[-_ ]?)key|secret|signature|credential|"
    r"authorization|cookie)[\"']?\s*[:=]\s*)(?:bearer\s+|basic\s+)?"
    r"(?:\"[^\"\n]*\"|'[^'\n]*'|[^\s&,;}]+)"
)
USERINFO = re.compile(r"(://)[^/\s@]+@")
SIGNED_URL = re.compile(r"https?://[^\s<>\"']+\?[^\s<>\"']+", re.I)


def safe_text(value: str, *, multiline: bool = False) -> str:
    clean = CONTROLS.sub("", ESCAPES.sub("", value))
    clean = USERINFO.sub(r"\1[redacted]@", clean)
    clean = SIGNED_URL.sub(lambda match: match[0].split("?")[0] + "?[redacted]", clean)
    clean = CREDENTIALS.sub(r"\1[redacted]", clean)
    return clean if multiline else " ".join(clean.splitlines())


def scalar(value: object) -> str:
    if isinstance(value, str):
        return safe_text(value).strip()
    if type(value) is int:
        return str(value)
    return ""


def label(value: object) -> str:
    name = scalar(value)
    return name.replace("_", " ").capitalize() if name else "Unknown"


def path_label(value: object, root: Path) -> str:
    if not isinstance(value, str) or not value:
        return ""
    path = Path(value)
    return safe_text(str(path.relative_to(root) if path.is_relative_to(root) else path))


def timestamp(value: object) -> str:
    dt = when(value)
    return dt.isoformat(sep=" ").removesuffix("+00:00") + " UTC" if dt else "—"


def duration(start: object, finish: object) -> str:
    lo, hi = when(start), when(finish)
    if lo is None or hi is None or hi < lo:
        return "—"
    delta = hi - lo
    seconds = delta.days * 86400 + delta.seconds
    hours, remain = divmod(seconds, 3600)
    minutes, seconds = divmod(remain, 60)
    fraction = f".{delta.microseconds:06d}".rstrip("0") if delta.microseconds else ""
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}{fraction}"


def values(document: Document, keys: Fields) -> Fields:
    return tuple((name, text) for name, key in keys if (text := scalar(document.get(key))))


def counts(document: Document, keys: Fields) -> Fields:
    return tuple(
        (name, f"{count:,}")
        for name, key in keys
        if type(count := document.get(key)) is int and count >= 0
    )


def unique_fields(fields: Fields) -> Fields:
    seen: set[str] = set()
    result: list[tuple[str, str]] = []
    for name, text in fields:
        if text and text not in seen:
            seen.add(text)
            result.append((name, text))
    return tuple(result)


def artifacts(document: Document, root: Path, keys: Fields) -> Fields:
    return tuple(
        (name, text) for name, key in keys if (text := path_label(document.get(key), root))
    )


def lifecycle(document: Document) -> Fields:
    return values(
        document,
        (
            ("Engine execution", "execution"),
            ("Verification", "verification"),
            ("Registration", "registration"),
        ),
    )


def command(action: str, identity: str, environment: str, *, machine: bool = False) -> str:
    return shlex.join(
        ["mf", *(["--json"] if machine else []), "runs", action, identity, f"--{environment}"]
    )


@dataclass(frozen=True, slots=True)
class TableView:
    title: str
    columns: tuple[str, ...]
    records: tuple[tuple[str, ...], ...]
    total: int


def table(title: str, columns: tuple[str, ...], records: list[tuple[str, ...]]) -> TableView:
    return TableView(title, columns, tuple(records[:DISPLAY_LIMIT]), len(records))


@dataclass(frozen=True, slots=True)
class RunLine:
    identity: str
    operation: str
    action: str
    recipe: str
    status: str
    started: str
    finished: str
    elapsed: str
    outcome: str
    liveness: str


def run_line(row: Document) -> RunLine:
    saved = mapping(row.get("summary"))
    operation, action = scalar(row.get("operation")), scalar(row.get("action"))
    outcome: Fields = ()
    if operation == "normalize" and action in {"execute", "run"}:
        outcome = counts(
            mapping(saved.get("counts")),
            (("jobs successful", "successful"), ("primary files registered", "primary_files")),
        )
    elif operation == "acquisition" and action == "download":
        outcome = counts(
            mapping(saved.get("file_counts")),
            (("downloaded", "completed"), ("reused", "reused"), ("failed", "failed")),
        )
    elif operation == "convert":
        outcome = values(saved, (("Target quote", "target_quote"),))
    elif operation == "merge":
        outcome = values(saved, (("Registration", "registration"),))
    stage_values = mapping(saved.get("stages"))
    hint = " · ".join(
        f"{value} {key}" if value.replace(",", "").isdecimal() else f"{key} {label(value)}"
        for key, value in outcome
    )
    if operation == "convert" and outcome:
        hint = f"Target quote {outcome[0][1]}"
    if operation == "pipeline" and stage_values:
        hint = " · ".join(
            f"{title} {label(state)}"
            for name, title in PIPELINE_STAGES
            if (state := mapping(stage_values.get(name)).get("state"))
        )
    return RunLine(
        scalar(row.get("run_id")),
        label(operation),
        label(action),
        scalar(row.get("recipe")),
        scalar(row.get("status")) or "unknown",
        timestamp(row.get("started_at")),
        timestamp(row.get("finished_at")),
        duration(row.get("started_at"), row.get("finished_at")),
        hint,
        scalar(row.get("liveness")),
    )


@dataclass(frozen=True, slots=True)
class RunInventory:
    rows: tuple[RunLine, ...]
    offset: int
    filters: Fields
    next_command: str
    ordering: str
    sort_field: str


def inventory(
    payload: Document,
    filters: Fields,
    environment: str,
    limit: int,
    *,
    sort: str | None = None,
    order: str | None = None,
) -> RunInventory:
    ordering = RUNS.resolve(sort, order)
    offset = payload.get("offset")
    offset = offset if type(offset) is int else 0
    args = [
        "mf",
        "runs",
        "list",
        f"--{environment}",
        "--limit",
        str(limit),
        "--offset",
        str(offset + limit),
    ]
    for name, value in filters:
        args.extend((f"--{name}", value))
    if sort is not None:
        args.extend(("--sort", sort))
    if order is not None:
        args.extend(("--order", order))
    return RunInventory(
        tuple(run_line(row) for row in rows(payload.get("runs"))),
        offset,
        filters,
        shlex.join(args) if payload.get("more") else "",
        ordering.description,
        ordering.field,
    )


RESOURCE_LABELS: dict[str, tuple[str, str]] = {
    "discovery_scan": ("Inputs", "Discovery scan ID"),
    "remote_archive": ("Inputs", "Remote archive ID"),
    "local_archive": ("Archives", "Local archive ID"),
    "source_publication": ("Inputs", "Source Engine publication ID"),
    "registry_publication": ("Registry references", "Registry publication ID"),
    "engine_publication": ("Outputs", "Engine publication ID"),
    "primary_file_dataset": ("Outputs", "Registry primary-file ID"),
    "conversion_output": ("Outputs", "Converted output directory"),
    "conversion_output_ref": ("Outputs", "Indexed conversion output reference"),
    "prepared_batch": ("Prepared artifacts", "Prepared batch"),
    "prepared_job": ("Prepared artifacts", "Prepared job"),
    "engine_execution": ("Execution artifacts", "Engine execution reference"),
    "report": ("Execution artifacts", "Report"),
    "child_run": ("Child run references", "Retained child run ID"),
}
RESOURCE_PATHS = {"conversion_output", "prepared_batch", "prepared_job", "report"}


@dataclass(frozen=True, slots=True)
class RunDetail:
    run: RunLine
    identity: Fields
    timing: Fields
    lifecycle: Fields
    attempts: TableView
    attempt_evidence: TableView
    children: TableView
    resources: tuple[TableView, ...]
    artifacts: Fields
    messages: tuple[str, ...]
    commands: tuple[str, ...]


def detail(inspection: RunInspection, root: Path, environment: str) -> RunDetail:
    document = inspection.evidence
    line = run_line(document)
    attempts = rows(document.get("attempts"))
    children = rows(document.get("children"))
    child_ids = {scalar(child.get("run_id")) for child in children}
    saved = mapping(document.get("summary"))
    saved_paths = artifacts(
        document,
        root,
        (("Retained report pointer", "report_path"), ("Artifact directory", "artifact_root")),
    )
    seen_paths = {text for _, text in saved_paths}
    per_category: dict[str, list[tuple[str, ...]]] = {}
    for item in rows(document.get("resources")):
        kind, ref = scalar(item.get("kind")), scalar(item.get("reference"))
        if not kind or not ref or (kind == "child_run" and ref in child_ids):
            continue
        category, title = RESOURCE_LABELS.get(kind, ("Other registry resources", label(kind)))
        if kind == "prepared_job" and Path(ref).suffix != ".json":
            category, title = "Execution artifacts", "Recorded job path"
        if kind in RESOURCE_PATHS:
            ref = path_label(ref, root)
        stage = scalar(item.get("stage"))
        # A stage-labeled reference carries additional provenance.
        if kind == "report" and ref in seen_paths and not stage:
            continue
        per_category.setdefault(category, []).append(
            (scalar(item.get("number")) or "—", label(stage) if stage else "—", title, ref)
        )
    evidence_rows: list[tuple[str, ...]] = []
    for attempt in attempts[:DISPLAY_LIMIT]:
        for title, key in (("Outcome pointer", "outcome_path"), ("Report pointer", "report_path")):
            if (path := path_label(attempt.get(key), root)) and path not in seen_paths:
                seen_paths.add(path)
                evidence_rows.append((scalar(attempt.get("number")), title, path))
    notices = [
        text
        for condition, text in (
            (document.get("attempts_truncated"), "The registry returned the first 1,000 attempts."),
            (
                document.get("resources_truncated"),
                "The registry returned the first 4,096 resources.",
            ),
            (len(children) == 1000, "The child run list reached the registry limit of 1,000."),
            (
                not document.get("report_path"),
                "No report pointer is indexed. A retained attempt outcome may still be available.",
            ),
        )
        if condition
    ]
    if inspection.removal:
        notices.insert(0, "History removed · Evidence retained")
        saved_paths += artifacts(inspection.removal, root, (("Cleanup receipt", "receipt_path"),))
    error = scalar(document.get("error_summary"))
    if error:
        notices.insert(0, f"Recorded reason · {error}")
    links = (
        ("logs", "report")
        if line.status in {"failed", "interrupted", "cancelled"}
        else ("report", "logs")
    )
    return RunDetail(
        line,
        values(document, (("Parent run ID", "parent_run_id"),))
        + ((("Registry ID", str(document["database_id"])),) if document.get("database_id") else ()),
        (
            ("Started", line.started),
            ("Finished", timestamp(document.get("finished_at"))),
            ("Run duration", line.elapsed),
        ),
        lifecycle(saved),
        table(
            "Attempts",
            ("#", "Attempt ID", "Status", "Started UTC", "Duration", "Reason"),
            [
                (
                    scalar(row.get("number")),
                    safe_text(str(row.get("attempt_id") or "—")),
                    label(row.get("status")),
                    timestamp(row.get("started_at")).removesuffix(" UTC"),
                    duration(row.get("started_at"), row.get("finished_at")),
                    scalar(row.get("error_summary"))[:240],
                )
                for row in attempts
            ],
        ),
        table("Attempt artifacts", ("Attempt #", "Evidence", "Path"), evidence_rows),
        table(
            "Child runs",
            ("Operation", "Action", "Status", "Run ID"),
            [
                (
                    label(child.get("operation")),
                    label(child.get("action")),
                    label(child.get("status")),
                    scalar(child.get("run_id")),
                )
                for child in children
            ],
        ),
        tuple(
            table(name, ("Attempt #", "Stage", "Resource", "Reference"), records)
            for name, records in per_category.items()
        ),
        saved_paths,
        tuple(notices),
        tuple(command(action, inspection.name, environment) for action in links),
    )


@dataclass(frozen=True, slots=True)
class LogStream:
    name: str
    content: str
    truncated: bool


@dataclass(frozen=True, slots=True)
class LogView:
    identity: str
    events: tuple[tuple[str, str, str, str], ...]
    streams: tuple[LogStream, ...]
    event_source: bool
    truncated: bool
    notice: str
    history_removed: bool = False


def log_event(event: Document) -> tuple[str, str, str, str]:
    if not event:
        return "—", "—", "—", "Unrecognized event record"
    kind = scalar(event.get("type") or event.get("event"))
    state = scalar(event.get("status") or event.get("state"))
    message = scalar(event.get("message") or event.get("error"))
    if event.get("stage") and (phase := scalar(event.get("phase"))):
        message = " · ".join(filter(None, (label(phase), message)))
    resource = values(
        event,
        (
            ("Request", "request_index"),
            ("Scan ID", "scan_id"),
            ("Remote archive ID", "remote_archive_id"),
            ("Local archive ID", "local_archive_id"),
        ),
    )
    settings = mapping(event.get("request"))
    scope = mapping(settings.get("scope"))
    if name := scalar(scope.get("identifier")):
        resource += (("Scope", " ".join(filter(None, (scalar(settings.get("exchange")), name)))),)
    parts = [
        " · ".join(filter(None, (label(kind) if kind else "", label(state) if state else ""))),
        message,
        " · ".join(f"{key} {value}" for key, value in resource),
    ]
    if facts := counts(
        event, (("archives found", "archives_found"), ("bytes written", "bytes_written"))
    ):
        parts.append(" · ".join(f"{value} {key}" for key, value in facts))
    return (
        timestamp(event.get("timestamp") or event.get("at") or event.get("time")).removesuffix(
            " UTC"
        ),
        scalar(event.get("level") or event.get("severity")).upper() or "—",
        scalar(event.get("stage") or event.get("phase")) or "—",
        " · ".join(filter(None, parts))[:1000] or "Event without a retained message",
    )


def logs(inspection: RunInspection) -> LogView:
    document = inspection.evidence
    streams = tuple(
        LogStream(
            f"{prefix.title()} {suffix}",
            safe_text(content, multiline=True),
            name in inspection.truncated_streams,
        )
        for prefix in ("engine", "validation")
        for suffix in ("stdout", "stderr")
        if isinstance(content := document.get(name := f"{prefix}-{suffix}.log"), str)
    )
    if "engine-tui.log" in inspection.supplemental_logs:
        streams += (
            LogStream(
                "Engine TUI diagnostics",
                safe_text(inspection.supplemental_logs["engine-tui.log"], multiline=True),
                "engine-tui.log" in inspection.truncated_streams,
            ),
        )
    missing = document.get("status") == "events_missing"
    event_source = "events" in document and not missing
    return LogView(
        inspection.name,
        tuple(log_event(row) for row in rows(document.get("events"))),
        streams,
        event_source,
        bool(document.get("truncated")),
        "No retained events or Engine logs are available."
        if not streams and not event_source
        else "",
        bool(inspection.removal),
    )


PIPELINE_STAGES = (
    ("acquire", "Acquisition"),
    ("normalize", "Normalization"),
    ("merge", "Merge"),
    ("convert", "Conversion"),
)
REPORT_TITLES = {
    "acquisition": "Acquisition Report",
    "normalize": "Normalization Report",
    "merge": "Merge Report",
    "convert": "Conversion Report",
    "pipeline": "Pipeline Report",
}


@dataclass(frozen=True, slots=True)
class ReportView:
    title: str
    identity: Fields
    status: str
    lifecycle: Fields
    inputs: Fields
    outputs: Fields
    tables: tuple[TableView, ...]
    artifacts: Fields
    diagnostics: tuple[str, ...]
    unknown_fields: tuple[str, ...]
    machine_command: str


def issues(document: Document) -> tuple[str, ...]:
    messages = [
        text
        for key in (
            "error",
            "error_summary",
            "registration_error",
            "validation_error",
            "execution_registry_warning",
        )
        if (text := scalar(document.get(key)))
    ]
    for key in ("blockers", "compatibility_blockers"):
        entry = document.get(key)
        if isinstance(entry, list):
            for item in cast(list[object], entry)[:DISPLAY_LIMIT]:
                detail = mapping(item)
                value = scalar(item) or scalar(detail.get("reason") or detail.get("message"))
                if value:
                    messages.append(value)
    return tuple(dict.fromkeys(messages))


def id_preview(items: object) -> str:
    if not isinstance(items, list):
        return ""
    refs = list(dict.fromkeys(filter(None, (scalar(item) for item in cast(list[object], items)))))
    suffix = f" · {len(refs) - 8:,} more" if len(refs) > 8 else ""
    return ", ".join(refs[:8]) + suffix


def acquired(
    document: Document, action: str
) -> tuple[Fields, Fields, tuple[TableView, ...], tuple[str, ...]]:
    discovery = counts(
        mapping(document.get("request_counts")),
        (
            ("Requests successful", "successful"),
            ("Archives discovered", "archives"),
            ("Cached discoveries", "cached"),
            ("Empty discoveries", "empty"),
            ("Failed requests", "failed"),
            ("Unsupported requests", "unsupported"),
            ("Cancelled requests", "cancelled"),
        ),
    )
    transfers: Fields = ()
    if action == "download":
        transfers = counts(
            mapping(document.get("file_counts")),
            (
                ("Completed downloads", "completed"),
                ("Reused files", "reused"),
                ("Failed downloads", "failed"),
                ("Cancelled downloads", "cancelled"),
            ),
        )
        if type(document.get("bytes_written")) is int:
            transfers += (
                ("Bytes from completed downloads", binary_size(document["bytes_written"])),
            )
    if action == "register":
        registered = {
            value
            for row in rows(document.get("copies"))
            if (value := scalar(row.get("local_archive_id")))
        }
        if registered:
            transfers += (
                ("Distinct local archive IDs in retained copies", f"{len(registered):,}"),
            )
    request_table: list[tuple[str, ...]] = []
    for item in rows(document.get("requests")):
        request = mapping(item.get("request"))
        scope = mapping(request.get("scope"))
        selection = " ".join(
            filter(None, (scalar(request.get("exchange")), scalar(scope.get("identifier"))))
        )
        found = item.get("archives_found")
        request_table.append(
            (
                scalar(item.get("index")) or "—",
                selection or "—",
                label(item.get("status")),
                f"{found:,}" if type(found) is int else "—",
                scalar(item.get("error"))[:240] or "—",
            )
        )
    request_table.sort(key=lambda row: (row[4] == "—",))
    requests = table(
        "Discovery requests",
        ("Index", "Source", "Status", "Archives found", "Reason"),
        request_table,
    )
    return discovery, transfers, (requests,), ()


def normalized(
    document: Document,
    action: str,
    environment: str,
    recipe: str,
) -> tuple[Fields, Fields, Fields, tuple[TableView, ...], tuple[str, ...]]:
    if (
        action == "prepare"
        or "prepared_job_count" in document
        or "engine_compatibility" in document
    ):
        prep = PreparationView.from_result(
            document,
            environment=environment,
            dry_run=document.get("dry_run") is True,
            recipe=recipe,
        )
        return (
            (("Rust validation", prep.compatibility),) if prep.compatibility else (),
            tuple(item for item in prep.identity if item[0] != "Recipe")
            + prep.resolution
            + prep.ranking,
            (),
            (),
            tuple(filter(None, (prep.error, *(reason for _, reason in prep.examples[:10])))),
        )
    view = NormalizationOutcome.from_result(document, registering=action == "register")
    counts_doc = mapping(document.get("counts"))
    output = counts(
        counts_doc,
        (
            ("Jobs requested", "requested"),
            ("Jobs successful", "successful"),
            ("Jobs failed", "failed"),
        ),
    )
    output += tuple(dict.fromkeys(view.processing + view.registration_counts))
    facts = tuple(
        (name, value)
        for name, value in (
            ("Engine outcome", view.engine),
            ("Verification", view.verification),
            ("Registration", view.registration),
            ("Engine duration", view.duration),
        )
        if value and value != "—"
    )
    registrations = table(
        "Verified publications",
        ("Engine publication ID", "Content", "Stored rows", "Files", "Integrity"),
        [
            (
                pub.engine_id,
                pub.content_type,
                f"{pub.rows:,}" if pub.rows is not None else "—",
                f"{pub.files:,}" if pub.files is not None else "—",
                pub.integrity,
            )
            for pub in view.publications
        ],
    )
    jobs = table(
        "Jobs",
        ("Engine job ID", "State", "Disposition", "Registry publication ID"),
        [
            (
                scalar(job.get("job_id")) or "—",
                label(job.get("status")),
                label(job.get("disposition")),
                scalar(job.get("publication_id")) or "—",
            )
            for job in rows(document.get("jobs"))
        ],
    )
    return facts, (), output, (jobs, registrations), view.diagnostics


def merged(
    document: Document, action: str, reference: str
) -> tuple[Fields, Fields, Fields, tuple[str, ...]]:
    view = MergeView.from_result(dict(document), action=action, reference=reference)
    lifecycle_fields = tuple(
        (name, value)
        for name, value in view.details
        if name not in {"Recipe", "Reference", "Forge run ID"}
    )
    return lifecycle_fields, view.selection, view.output + view.registration, view.issues


def conversion(
    document: Document,
    saved: Document,
) -> tuple[Fields, Fields, tuple[TableView, ...], tuple[str, ...]]:
    settings = mapping(document.get("settings") or document.get("conversion"))
    target = scalar(
        settings.get("target_quote") or document.get("target_quote") or saved.get("target_quote")
    )

    def source_id(source: Document) -> str:
        return scalar(source.get("registry_publication_id"))

    collection_jobs = rows(document.get("jobs"))
    jobs = collection_jobs or ([document] if document.get("job_id") else [])
    sources = tuple(
        dict.fromkeys(
            filter(
                None,
                (
                    *(source_id(row) for row in rows(document.get("sources"))),
                    *(source_id(mapping(job.get("input"))) for job in jobs),
                ),
            )
        )
    )
    if not target:
        quotes = {
            value
            for job in jobs
            if (value := scalar(mapping(job.get("conversion")).get("target_quote")))
        }
        target = next(iter(quotes)) if len(quotes) == 1 else "Mixed" if quotes else ""
    inputs: Fields = (("Target quote", target),) if target else ()
    if sources:
        preview = id_preview(list(sources))
        inputs += (("Source merge registry publication IDs", preview),)
    if external := source_id(mapping(document.get("external_reference"))):
        inputs += (("External reference registry publication ID", external),)
    if isinstance(settings.get("references"), list):
        inputs += (
            ("Reference bindings recorded", str(len(cast(list[object], settings["references"])))),
        )
    inputs += values(
        settings,
        (
            ("Conversion route policy", "path_policy"),
            ("Reference price source", "price_source"),
            ("Maximum route hops", "max_hops"),
        ),
    )
    metrics = counts(
        mapping(document.get("statistics")),
        (
            ("Identity conversions", "identity"),
            ("Converted events", "converted"),
            ("Unavailable conversions", "unavailable"),
            ("Skipped events", "skipped"),
            ("Reference updates", "reference_updates"),
        ),
    )
    result_rows: list[tuple[str, ...]] = []
    diagnostics: list[str] = []
    for job in jobs:
        publication = mapping(job.get("publication"))
        verified = job.get("status") == "complete" and bool(
            mapping(publication.get("fingerprints")).get("manifest.json")
        )
        result_rows.append(
            (
                scalar(job.get("job_id")) or "—",
                label(job.get("status") or job.get("engine_validation")),
                label(job.get("execution")),
                label(job.get("disposition")),
                "Verified" if verified else label(job.get("verification")),
                label(job.get("registration")),
                f"{publication['events']:,}" if type(publication.get("events")) is int else "—",
                scalar(job.get("work_key")) or "—",
            )
        )
        diagnostics.extend(issues(job))
    counter_fields = (
        ("Identity", "identity"),
        ("Converted", "converted"),
        ("Unavailable", "unavailable"),
        ("Skipped", "skipped"),
        ("Reference updates", "reference_updates"),
    )
    counter_rows: list[tuple[str, ...]] = []
    for job in jobs:
        counters = dict(counts(mapping(job.get("statistics")), counter_fields))
        if counters:
            counter_rows.append(
                (
                    scalar(job.get("job_id")) or "—",
                    *(counters.get(name, "—") for name, _ in counter_fields),
                )
            )
    return (
        inputs,
        metrics,
        (
            table(
                "Conversion jobs",
                (
                    "Engine job ID",
                    "State",
                    "Execution",
                    "Disposition",
                    "Physical evidence",
                    "Registration",
                    "Stored rows",
                    "Work key",
                ),
                result_rows,
            ),
            table(
                "Conversion counters",
                ("Engine job ID", *(name for name, _ in counter_fields)),
                counter_rows,
            ),
        ),
        tuple(diagnostics),
    )


def pipeline(document: Document, root: Path) -> tuple[TableView, TableView]:
    stages = mapping(document.get("stages"))
    state_rows: list[tuple[str, ...]] = []
    paths: list[tuple[str, ...]] = []
    for key, title in PIPELINE_STAGES:
        stage = mapping(stages.get(key))
        if not stage:
            continue
        result = mapping(stage.get("result"))
        effect = values(
            result, (("Verification", "verification"), ("Registration", "registration"))
        )
        stage_result = scalar(stage.get("summary"))
        if not stage_result and isinstance(stage.get("primary_file_ids"), list):
            stage_result = (
                f"{len(cast(list[object], stage['primary_file_ids'])):,} registry primary files"
            )
        state_rows.append(
            (
                title,
                label(stage.get("state")),
                label(stage.get("phase")) if stage.get("phase") else "—",
                scalar(stage.get("child_run_id")) or "—",
                " · ".join(
                    filter(
                        None, (stage_result, *(f"{name} {label(value)}" for name, value in effect))
                    )
                )
                or "—",
            )
        )
        if path := path_label(stage.get("child_report"), root):
            paths.append((title, path))
        if path := path_label(stage.get("terminal_outcome"), root):
            paths.append((f"{title} terminal", path))
    return (
        table(
            "Stages", ("Stage", "State", "Recorded phase", "Child run ID", "Outcome"), state_rows
        ),
        table("Stage reports", ("Stage", "Retained report"), paths),
    )


ARTIFACT_FIELDS = (
    ("Retained report", "resolved_report"),
    ("Retained report", "report_path"),
    ("Retained report", "report"),
    ("Registration receipt", "registration_receipt"),
    ("Execution intent", "execution_receipt"),
    ("Engine result", "engine_result_path"),
    ("Diagnostics", "engine_diagnostics_path"),
    ("Prepared batch", "batch_path"),
    ("Prepared job or collection", "job_path"),
    ("Prepared collection", "collection_path"),
    ("Event log", "archive_outcomes"),
    ("Event log", "event_log"),
    ("Resolved plan", "resolved_plan"),
    ("Retained job result", "result_path"),
)


def report(inspection: RunInspection, root: Path, environment: str) -> ReportView:
    document = inspection.evidence
    record = inspection.record or {}
    saved = mapping(record.get("summary"))
    operation = scalar(document.get("operation") or record.get("operation"))
    if document.get("kind") == "historical_pipeline":
        operation = "pipeline"
    if not operation and (prefix := inspection.name.partition("-")[0]) in tuple(RunOperation):
        operation = prefix
    action = scalar(document.get("action") or record.get("action"))
    if not action and MANAGED_RUN.fullmatch(inspection.name):
        action = inspection.name.split("-")[1]
    recipe = scalar(document.get("recipe") or document.get("name") or record.get("recipe"))
    state = scalar(document.get("state") or document.get("status"))
    phase = scalar(document.get("active_stage"))
    identity: Fields = (("Run ID", inspection.name),)
    if inspection.removal:
        identity += (("History", "Removed · Evidence retained"),)
    identity += tuple(
        (name, value)
        for name, value in (
            ("Operation", label(operation) if operation else ""),
            ("Action", label(action) if action else ""),
            ("Recipe", recipe),
        )
        if value
    )
    timing = duration(document.get("started_at"), document.get("finished_at"))
    if timing != "—":
        identity += (("Recorded duration", timing),)
    inputs: Fields = ()
    outputs: Fields = ()
    tables: tuple[TableView, ...] = ()
    diagnostics = issues(document) + inspection.inspection_notes
    paths = artifacts(document, root, ARTIFACT_FIELDS) + artifacts(
        record, root, (("Indexed report pointer", "report_path"),)
    )
    states = lifecycle(document)
    unknown: tuple[str, ...] = ()
    title = REPORT_TITLES.get(operation, "Historical Run Report")
    recorded = scalar(record.get("status"))
    status = recorded if recorded and recorded != "unknown" else state or "unknown"
    if state in {"complete", "completed", "registered"} and document.get("registration") in {
        "failed",
        "pending",
    }:
        status = status_of(document, action)
    if state != status and not (state == "complete" and status == "completed"):
        identity += (("Retained evidence status", label(state)),) if state else ()
    if state in {"report_missing_or_interrupted", "report_incomplete_or_invalid"}:
        status = state
        if recorded:
            identity += (("Recorded run status", label(recorded)),)
    if state == "report_missing_or_interrupted":
        diagnostics += ("Retained report unavailable. This describes evidence availability.",)
    elif state == "report_incomplete_or_invalid":
        diagnostics += ("Retained report is incomplete or invalid.",)
    elif document.get("attempt_id") and "summary" in document:
        phase = "Retained attempt outcome"
        identity += values(document, (("Attempt ID", "attempt_id"), ("Attempt number", "number")))
        states = lifecycle(mapping(document.get("summary")))
        diagnostics += (
            "Detailed operation evidence is unavailable in this retained attempt outcome.",
        )
    else:
        try:
            if operation == "acquisition" and (
                "request_counts" in document or "copies" in document
            ):
                inputs, outputs, tables, notices = acquired(document, action)
                diagnostics += notices
            elif operation == "normalize" and (
                action in {"prepare", "register"}
                or {
                    "jobs",
                    "normalization",
                    "counts",
                    "engine_result",
                    "prepared_job_count",
                    "engine_compatibility",
                }
                & document.keys()
            ):
                view_facts, inputs, outputs, tables, notices = normalized(
                    document, action, environment, recipe
                )
                states = view_facts or states
                if (
                    action == "prepare"
                    or "prepared_job_count" in document
                    or "engine_compatibility" in document
                ):
                    phase = "Preparation"
                diagnostics += notices
            elif operation == "merge" and (
                {"compatibility", "execution_id", "dataset", "evidence"} & document.keys()
                or action == "register"
            ):
                view_facts, inputs, outputs, notices = merged(document, action, inspection.name)
                states = view_facts or states
                phase = (
                    "Registration recovery"
                    if action == "register"
                    else "Preparation"
                    if "execution_id" not in document
                    else "Execution"
                )
                diagnostics += notices
            elif (
                operation == "convert"
                and {"jobs", "settings", "job_id", "sources", "blockers"} & document.keys()
            ):
                if inspection.conversion_jobs:
                    source = {**document, "jobs": list(inspection.conversion_jobs)}
                    phase = "Execution"
                    for item in inspection.conversion_jobs:
                        paths += artifacts(item, root, (("Conversion job result", "result_path"),))
                else:
                    source = document
                    if "execution" not in document:
                        phase = "Preparation"
                inputs, outputs, tables, notices = conversion(source, saved)
                diagnostics += notices
            elif operation == "pipeline" and "stages" in document:
                tables = pipeline(document, root)
            elif (
                action == "validate" or "validation" in document or state in {"valid", "validated"}
            ):
                phase = "Validation"
                states += values(
                    document, (("Validation", "validation"), ("Compatibility", "compatibility"))
                )
            else:
                unknown = tuple(safe_text(str(name)) for name in list(document)[:DISPLAY_LIMIT])
        except (KeyError, TypeError, ValueError, AttributeError):
            unknown = tuple(safe_text(str(name)) for name in list(document)[:DISPLAY_LIMIT])
    if unknown:
        diagnostics += (
            "Some retained fields could not be interpreted. Full evidence is available in JSON.",
        )
    retained_environment = scalar(document.get("environment"))
    if retained_environment and (not record or retained_environment != environment):
        identity += (("Retained environment", label(retained_environment)),)
    if not record:
        identity += (("Registry association", "Unverified historical evidence"),)
    if phase:
        identity += (("Stage", label(phase)),)
    disposition = values(document, (("Disposition", "disposition"), ("Result", "reuse_message")))
    states += disposition
    if record.get("liveness") == "unknown":
        identity += (("Liveness", "Unknown"),)
    messages = tuple(dict.fromkeys(safe_text(message)[:1000] for message in diagnostics))
    diagnostics = tuple(
        message
        for message in messages
        if not any(other.startswith(message + " · ") for other in messages)
    )[:DISPLAY_LIMIT]
    return ReportView(
        title,
        identity,
        status,
        tuple(dict.fromkeys(states)),
        inputs,
        outputs,
        tables,
        unique_fields(paths),
        diagnostics,
        unknown,
        command("report", inspection.name, environment, machine=True),
    )


DECISIONS = {
    "history_removed": ("Skipped", "An explicit run history removal marker exists."),
    "unrecognized": ("Skipped", "Directory name is outside the supported run reference formats."),
    "canonical_run": ("Skipped", "Canonical run reference. Legacy indexing does not process it."),
    "indexed": ("Indexed", "The registry contains this matching run."),
    "newly_indexed": ("Newly indexed", "A run record was inserted from retained evidence."),
    "already_indexed": ("Already indexed", "The matching run was already in the registry."),
    "concurrent_insert": (
        "Review concurrent insert",
        "A run record appeared during apply. Review its registry identity.",
    ),
    "conflict": ("Blocked", "The indexed identity or artifact directory conflicts."),
    "invalid_evidence": ("Blocked", "Retained evidence could not be read safely."),
    "ambiguous_operation_or_identity": (
        "Review",
        "Retained operation or identity could not be established.",
    ),
    "unknown_environment": ("Review", "Retained environment is unavailable or unrecognized."),
    "different_or_unverified_registry": (
        "Blocked",
        "Retained evidence does not match the selected registry.",
    ),
    "missing_or_invalid_timing": (
        "Review",
        "Retained start or finish timing is missing or inconsistent.",
    ),
    "running_liveness_unknown": ("Review", "No conclusive outcome. Liveness is unknown."),
    "eligible": ("Eligible", "Retained evidence passed the indexing preview."),
    "conflicting_artifact_root": ("Blocked", "Artifact pointers conflict with the run directory."),
    "missing_outcome": ("Review", "The expected attempt outcome is unavailable."),
    "missing_or_unsafe_report": (
        "Blocked",
        "Report is missing, unsafe or disagrees with its fingerprint.",
    ),
    "retained_outcome": (
        "Recoverable",
        "A validated retained outcome can finalize registry metadata.",
    ),
    "already_reconciled": ("Already reconciled", "The indexed attempt matches retained evidence."),
    "reconciled": ("Reconciled", "Retained outcome was applied to the registry."),
    "invalid_or_conflicting_outcome": (
        "Blocked",
        "Outcome is invalid or conflicts with indexed evidence.",
    ),
    "unindexed_local_evidence_requires_review": (
        "Review",
        "Local evidence requires an indexed run before reconciliation.",
    ),
}


@dataclass(frozen=True, slots=True)
class MaintenanceView:
    title: str
    summary: Fields
    results: TableView


def maintenance(
    payload: Document,
    action: str,
    evidence: MaintenanceEvidence | None = None,
) -> MaintenanceView:
    legacy = action == "index-legacy"
    applying = payload.get("dry_run") is False
    outcomes = rows(payload.get("outcomes"))
    summary: Fields = (
        ("Mode", "Apply" if applying else "Preview"),
        ("Offset", scalar(payload.get("offset")) or "0"),
    )
    if legacy:
        summary += (("Candidates inspected on this page", f"{len(outcomes):,}"),)
    else:
        attempts = sum(bool(row.get("attempt_id")) for row in outcomes)
        summary += (
            ("Attempt results on this page", f"{attempts:,}"),
            ("Local evidence requiring review", f"{len(outcomes) - attempts:,}"),
        )
    decisions: Counter[str] = Counter()
    rendered: list[tuple[str, ...]] = []
    for item in outcomes:
        status = scalar(item.get("status"))
        if legacy and evidence is not None and status == "indexed":
            status = evidence.indexing.get(scalar(item.get("run_id")), status)
        decision, reason = DECISIONS.get(status, ("Review", "Unrecognized retained decision."))
        if status == "indexed":
            decision = "Indexed (new or existing)" if applying else "Already indexed"
        decisions[decision] += 1
        row = (scalar(item.get("run_id")) or "—", decision, scalar(item.get("reason")) or reason)
        if not legacy:
            attempt_id = scalar(item.get("attempt_id"))
            row = (row[0], attempt_id or "—", *row[1:])
            if evidence is not None:
                timing = evidence.attempts.get(attempt_id)
                row = (
                    row[0],
                    row[1],
                    str(timing[0]) if timing else "—",
                    label(timing[1]) if timing else "—",
                    *row[2:],
                )
        rendered.append(row)
    summary += tuple((name, f"{count:,}") for name, count in decisions.items())
    return MaintenanceView(
        "Historical Run Indexing" if legacy else "Run Reconciliation",
        summary,
        table(
            "Candidate results",
            ("Run ID", "Decision", "Reason")
            if legacy
            else ("Run ID", "Attempt ID", "Attempt #", "Recorded state", "Decision", "Reason")
            if evidence is not None
            else ("Run ID", "Attempt ID", "Decision", "Reason"),
            rendered,
        ),
    )


@dataclass(frozen=True, slots=True)
class DeletionView:
    environment: str
    mode: str
    status: str
    summary: Fields
    targets: TableView
    blockers: TableView
    retained: tuple[str, ...]
    missing: int
    command: str


def deletion(result: Document) -> DeletionView:
    environment = scalar(result.get("environment"))
    status = scalar(result.get("status"))
    mode = scalar(result.get("mode"))
    plan_path = scalar(result.get("plan_path"))
    preview = result.get("action") == "preview"
    targets = rows(result.get("targets"))
    if preview:
        facts = counts(
            mapping(result.get("estimated")),
            (
                ("Explicit runs", "selected_runs"),
                ("Additional descendants", "additional_descendants"),
                ("Affected attempts", "attempts"),
                ("Reviewed removable files", "removable_files"),
                ("Estimated file bytes", "removable_bytes"),
            ),
        ) + (("Blocked runs", str(sum(bool(target.get("blockers")) for target in targets))),)
        issues = [
            (scalar(target.get("run_id")), scalar(issue))
            for target in targets
            for issue in cast(list[object], target.get("blockers", []))
        ]
        missing = sum(len(cast(list[object], target.get("missing", []))) for target in targets)
        retained = {
            scalar(path)
            for target in targets
            for path in cast(list[object], target.get("retained", []))
        }
    else:
        facts = counts(
            result,
            (
                ("Runs hidden", "runs_hidden"),
                ("Run records removed", "run_records_removed"),
                ("Attempts removed", "attempt_records_removed"),
                ("Resource associations removed", "resource_records_removed"),
                ("Files removed", "files_removed"),
                ("Directories removed", "directories_removed"),
                ("Removed file bytes", "removed_file_bytes"),
                ("Storage reclaimed bytes", "storage_reclaimed_bytes"),
            ),
        )
        issues = [
            (scalar(row.get("run_id")), scalar(row.get("reason")))
            for row in rows(result.get("blockers"))
        ]
        missing = len(cast(list[object], result.get("missing_on_retry", [])))
        retained = {
            scalar(value) for value in cast(list[object], result.get("retained_artifacts", []))
        }
    facts += (("Plan", plan_path),) if plan_path else ()
    if receipt := scalar(result.get("receipt_path")):
        facts += (("Receipt", receipt),)
    can_apply = (preview and status == "preview") or (not preview and status == "partial")
    command_text = (
        shlex.join(["mf", "runs", "delete", f"--{environment}", "--apply", "--plan", plan_path])
        if plan_path and can_apply
        else ""
    )

    def bytes_for(target: Document) -> str:
        return f"{
            sum(
                size
                for tree in rows(target.get('trees'))
                for item in map(mapping, mapping(tree.get('files')).values())
                if isinstance(size := item.get('size'), int)
            ):,}"

    return DeletionView(
        environment,
        mode,
        status,
        facts,
        table(
            "Reviewed runs",
            ("Run ID", "Operation", "Outcome", "Attempts", "Removable bytes", "Decision"),
            [
                (
                    scalar(target.get("run_id")),
                    label(target.get("operation")),
                    label(target.get("status")),
                    scalar(target.get("attempt_count")),
                    bytes_for(target),
                    "Blocked" if target.get("blockers") else "Eligible",
                )
                for target in targets
            ],
        ),
        table("Blockers", ("Run ID", "Reason"), issues),
        tuple(sorted(retained - {""})),
        missing,
        command_text,
    )
