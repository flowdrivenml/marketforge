"""Rich-free views of normalization recipes and operation evidence."""

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from forge.historical.normalization.config import (
    ExactRecipe,
    NormalizationRecipe,
    PreparationRequest,
)
from forge.markets.models import array
from forge.orchestration.registry import rows, status_of
from forge.orchestration.run_summary import mapping
from forge.shared.presentation.acquisition import format_period

LIMIT = 20
Fields = tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class RecipeView:
    name: str
    source: Path
    configuration: NormalizationRecipe | PreparationRequest | ExactRecipe | None = None
    error: str = ""

    @property
    def count(self) -> int | None:
        if isinstance(self.configuration, NormalizationRecipe):
            return len(self.configuration.requests)
        if isinstance(self.configuration, ExactRecipe):
            return len(self.configuration.archive_pins)
        if self.configuration is not None:
            return len(self.configuration.inputs)
        return None

    def period(self, *, compact: bool = False) -> str:
        if isinstance(self.configuration, NormalizationRecipe):
            interval = self.configuration.interval
            return format_period(interval.start, interval.end, compact=compact)
        return "Explicit inputs" if self.configuration is not None else "—"

    @property
    def kind(self) -> str:
        return (
            "Source selection"
            if isinstance(self.configuration, NormalizationRecipe)
            else "Explicit inputs"
            if self.configuration is not None
            else ""
        )

    def detail(self, *, overrides: bool = False) -> Fields:
        config = self.configuration
        if config is None:
            return ()
        details = (("Type", self.kind), ("Ranking", config.ranking_policy))
        if isinstance(config, NormalizationRecipe):
            return (*details, ("Requests", str(len(config.requests))), ("Period", self.period()))
        if isinstance(config, ExactRecipe):
            return (*details, ("Registry", str(config.registry_id)), ("Archives", str(self.count)))
        details = (
            *details,
            ("Inputs", str(len(config.inputs))),
            ("Content type", config.content_type.title()),
            ("Job ID", str(config.job_id)),
            ("Dataset ID", str(config.dataset_id)),
        )
        if config.output and overrides:
            details = (
                *details,
                ("Staging override", config.output.staging_path),
                ("Dataset override", config.output.dataset_path),
            )
        return details

    def entries(self) -> tuple[str, tuple[str, ...], tuple[tuple[str, ...], ...]]:
        config = self.configuration
        if isinstance(config, NormalizationRecipe):
            return (
                "Configured Requests",
                ("Exchange", "Scope", "Market", "Data", "Source key"),
                tuple(
                    (
                        row.exchange.title(),
                        row.scope.identifier
                        if row.scope.kind == "instrument"
                        else f"{row.scope.identifier} ({row.scope.kind.replace('_', ' ')})",
                        f"{row.instrument_type.title()}/{row.market_category.title()}"
                        if row.instrument_type != row.market_category
                        else row.market_category.title(),
                        "Trades" if row.data_type == "trades" else "Depth",
                        row.source_key or "—",
                    )
                    for row in config.requests[:LIMIT]
                ),
            )
        if isinstance(config, ExactRecipe):
            return (
                "Exact archive selection",
                ("Archive ID", "Profile", "Archive evidence SHA-256"),
                tuple(
                    (str(pin.archive_id), pin.profile_id or "Automatic", pin.selection_sha256)
                    for pin in config.archive_pins[:LIMIT]
                ),
            )
        if config is not None:
            return (
                "Registered Archive References",
                ("Task ID", "Archive ID", "Instrument ID", "Member", "Profile"),
                tuple(
                    (
                        str(row.task_id),
                        str(row.archive_id),
                        str(row.instrument_id),
                        row.archive_member or "—",
                        row.format_profile or "—",
                    )
                    for row in config.inputs[:LIMIT]
                ),
            )
        return "", (), ()


def fields(document: Mapping[str, object], keys: tuple[tuple[str, str], ...]) -> Fields:
    return tuple(
        (label, str(document[key])) for label, key in keys if document.get(key) is not None
    )


@dataclass(frozen=True, slots=True)
class PreparationView:
    status: str
    environment: str
    dry_run: bool
    identity: Fields
    resolution: Fields
    categories: tuple[tuple[str, str, str], ...]
    examples: tuple[tuple[str, str], ...]
    compatibility: str
    engine_ready: bool
    ranking: Fields
    artifacts: Fields
    error: str

    @classmethod
    def from_result(
        cls, result: Mapping[str, object], *, environment: str, dry_run: bool, recipe: str
    ) -> "PreparationView":
        diagnostics = mapping(result.get("diagnostic_summary"))
        request_rows = rows(result.get("requests"))
        selected = {
            entry["local_id"]
            for request in request_rows
            for entry in rows(request.get("selected_archives"))
            if type(entry.get("local_id")) is int
        }
        resolution = fields(
            diagnostics,
            (
                ("Matched archives", "matched_archive_count"),
                ("Blocking archives", "blocking_archive_count"),
                ("Archives with warnings", "warning_archive_count"),
                ("Coverage gaps", "coverage_gap_count"),
            ),
        )
        work = Counter(
            str(entry.get("status"))
            for entry in rows(list(mapping(result.get("reuse_plan")).values()))
        )
        resolution = (
            *resolution,
            *(
                (label, f"{work[state]:,}")
                for state, label in (
                    ("would_reuse", "Would reuse"),
                    ("would_execute", "Would execute"),
                    ("needs_registration_recovery", "Needs registration recovery"),
                    ("in_progress", "Work in progress"),
                    ("blocked", "Blocked reuse"),
                )
                if work[state]
            ),
        )
        if request_rows and all("selected_archives" in request for request in request_rows):
            resolution = (*resolution, ("Distinct selected archives", str(len(selected))))
        categories = tuple(
            (str(group["severity"]), str(group["category"]), str(group["archive_count"]))
            for group in rows(diagnostics.get("category_totals"))
        )
        examples = tuple(
            dict.fromkeys(
                (str(group["severity"]), str(reason))
                for group in rows(diagnostics.get("groups"))
                for reason in array(group.get("reasons"))
            )
        )
        compatibility = mapping(result.get("engine_compatibility"))
        contract = (
            "Rust contract validation was not performed during this dry run."
            if dry_run
            else str(compatibility.get("validation") or "")
        )
        if compatibility.get("direct_execution_supported") is False:
            contract = "The configured Engine does not support this batch contract."
        return cls(
            str(result.get("status", "unknown")),
            environment,
            dry_run,
            (
                ("Recipe", recipe),
                *fields(
                    result,
                    (
                        ("Jobs", "prepared_job_count"),
                        ("Tasks", "prepared_task_count"),
                        ("Streams", "prepared_stream_count"),
                        ("Batch schema", "batch_schema_version"),
                    ),
                ),
            ),
            resolution,
            categories,
            examples,
            contract,
            result.get("engine_ready") is True,
            fields(
                mapping(result.get("ranking_resolution")),
                (
                    ("Policy", "policy"),
                    ("Snapshot", "snapshot"),
                    ("Resolution", "reason"),
                ),
            ),
            fields(result, (("Batch", "batch_path"), ("Report", "report_path"))),
            str(result.get("error") or result.get("validation_error") or ""),
        )


def number(value: object) -> str:
    return f"{value:,}" if type(value) is int else "—"


def binary_size(value: object) -> str:
    if type(value) is not int:
        return "—"
    scaled = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if scaled < 1024 or unit == "TiB":
            return f"{value:,} B" if unit == "B" else f"{scaled:,.1f} {unit}"
        scaled /= 1024
    return "—"


def _total(records: Sequence[Mapping[str, object]], key: str) -> int | None:
    values = [row.get(key) for row in records]
    return (
        sum(value for value in values if type(value) is int)
        if values and all(type(value) is int for value in values)
        else None
    )


def engine_duration(result: Mapping[str, object]) -> str:
    milliseconds = integer(result.get("engine_elapsed_ms"))
    if milliseconds is not None and milliseconds >= 0:
        duration_seconds = milliseconds // 1000
    else:
        start, end = result.get("started_at"), result.get("finished_at")
        if not isinstance(start, str) or not isinstance(end, str):
            return ""
        try:
            elapsed = datetime.fromisoformat(end) - datetime.fromisoformat(start)
        except (ValueError, TypeError):
            return ""
        if elapsed.days < 0:
            return ""
        duration_seconds = elapsed.days * 86400 + elapsed.seconds
    minutes, seconds = divmod(duration_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours:d}:{minutes:02d}:{seconds:02d}" if hours else f"{minutes:02d}:{seconds:02d}"


def registry_ids(values: Mapping[str, object]) -> str:
    ids = sorted({value for value in values.values() if type(value) is int})
    if not ids:
        return ""
    if len(ids) > 1 and ids[-1] - ids[0] + 1 == len(ids):
        return f"{ids[0]}–{ids[-1]}"
    if len(ids) > 8:
        return f"{len(ids):,} registered files (see report)"
    return ", ".join(map(str, ids))


@dataclass(frozen=True, slots=True)
class PublishedDataset:
    content_type: str
    engine_id: str
    rows: int | None
    files: int | None
    size_bytes: int | None
    integrity: str

    @classmethod
    def from_evidence(cls, row: Mapping[str, object]) -> "PublishedDataset":
        return cls(
            str(row.get("content_type") or "—").title(),
            str(row.get("dataset_id", row.get("engine_dataset_id", "—"))),
            integer(row.get("events_written")),
            integer(row.get("files_written")),
            integer(row.get("bytes_written")),
            str(row.get("integrity_status") or "—").replace("_", " ").capitalize(),
        )


def integer(value: object) -> int | None:
    return value if type(value) is int else None


def count_fields(counts: Mapping[str, object], keys: tuple[tuple[str, str], ...]) -> Fields:
    return tuple((label, number(counts[key])) for label, key in keys if key in counts)


@dataclass(frozen=True, slots=True)
class NormalizationOutcome:
    status: str
    environment: str
    identity: Fields
    engine: str
    duration: str
    processing: Fields
    publications: tuple[PublishedDataset, ...]
    registration_counts: Fields
    verification: str
    registration: str
    diagnostics: tuple[str, ...]
    diagnostic_path: str
    artifacts: Fields
    next_action: str

    @classmethod
    def from_result(
        cls,
        result: Mapping[str, object],
        *,
        registering: bool,
        manifests: Mapping[str, Mapping[str, object]] | None = None,
    ) -> "NormalizationOutcome":
        execution = mapping(result.get("execution"))
        logical_run = result.get("logical_run_id") or (
            result.get("run_id") if registering else None
        )
        environment = str(result.get("environment") or execution.get("environment") or "")
        flags = " --testing" if environment == "testing" else ""
        registration = str(result.get("registration") or "")
        verification = str(result.get("verification") or "")
        evidence = rows(result.get("evidence"))
        jobs = rows(result.get("jobs"))
        verified: list[Mapping[str, object]] = []
        for job in jobs:
            if job.get("verification") != "verified":
                continue
            manifest = (manifests or {}).get(str(job.get("dataset_id")), {})
            if manifest:
                counters = mapping(mapping(manifest.get("processing_metrics")).get("counters"))
                job = {**manifest, **job, "processing_counters": counters}
            verified.append(job)
        outputs = (
            [{**mapping(item.get("manifest")), **item} for item in evidence]
            if registering
            else verified
        )
        if registering and evidence and result.get("publication") == "committed":
            verification = verification or "verified"

        counts = mapping(result.get("counts"))
        engine_result = mapping(result.get("engine_result"))
        engine_summary = mapping(engine_result.get("summary"))
        requested = engine_summary.get("jobs_total", counts.get("requested"))
        completed = engine_summary.get("jobs_complete")
        failed = engine_summary.get("jobs_failed")
        if not engine_summary and jobs and len(jobs) == requested:
            completed = sum(job.get("status") == "complete" for job in jobs)
            failed = sum(job.get("status") == "failed" for job in jobs)
        engine = str(result.get("normalization") or "")
        if completed is not None and requested is not None and engine != "interrupted":
            engine = f"{number(completed)} / {number(requested)} jobs successful"
            if failed:
                engine += f" · {number(failed)} failed"

        processing: Fields = count_fields(
            counts, (("Reused jobs", "reused"), ("Engine jobs submitted", "executed"))
        )
        processed = [job for job in verified if job.get("disposition") != "reused"]
        if processed:
            metrics = [{**job, **mapping(job.get("processing_counters"))} for job in processed]
            for label, key in (
                ("Records read", "records_read"),
                ("Canonical rows", "events_written"),
                ("Rejected records", "records_rejected"),
            ):
                if (value := _total(metrics, key)) is not None:
                    processing += ((label, number(value)),)
            processing += count_fields(
                counts,
                (
                    ("Publications", "verified_publications"),
                    ("Primary files", "verified_primary_files"),
                ),
            )
        if verified and completed != len(verified):
            processing = (*processing, ("Scope", "Verified outputs"))
        elif verified and failed:
            processing = (*processing, ("Scope", "Successful jobs"))

        registration_counts = count_fields(
            counts,
            (
                ("Primary files registered", "primary_files"),
                ("New files", "new"),
                ("Reused files", "reused"),
            ),
        )
        if not registering and "verified_publications" in counts and requested is not None:
            registration_counts = (
                (
                    "Publications verified",
                    f"{number(counts['verified_publications'])} / {number(requested)}",
                ),
                *registration_counts,
            )
        if ids := registry_ids(mapping(result.get("catalogue_dataset_ids"))):
            registration_counts = (*registration_counts, ("Registry file IDs", ids))
        if withheld := sum(job.get("registration") == "withheld" for job in jobs):
            registration_counts = (
                *registration_counts,
                ("Publications withheld", number(withheld)),
            )
        if omitted := sum(job.get("registration") == "omitted" for job in jobs):
            registration_counts = (*registration_counts, ("Failed jobs omitted", number(omitted)))

        engine_error = mapping(engine_result.get("error"))
        issues = [
            str(result[key])
            for key in ("error", "registration_error", "execution_registry_warning")
            if result.get(key)
        ]
        if engine_error.get("message"):
            issues.append(str(engine_error["message"]))
            context = fields(
                mapping(engine_error.get("context")), (("Job", "job_id"), ("Stream", "stream_id"))
            )
            if context:
                issues.append(" · ".join(f"{label} {value}" for label, value in context))
        for job in jobs:
            if failure := mapping(job.get("failure")):
                issues.append(
                    f"Job {job.get('job_id', 'unknown')} · {failure.get('message', 'failed')}"
                )

        artifacts: Fields = ()
        if logical_run:
            artifacts += (("Run", str(logical_run)),)
        execution_id = result.get("execution_id") or execution.get("execution_id")
        if execution_id:
            artifacts += (("Execution", str(execution_id)),)
        if not registering and result.get("run_id") and result["run_id"] != logical_run:
            artifacts += (("Engine run", str(result["run_id"])),)
        report = result.get("report_path") or result.get("registration_receipt")
        if report:
            artifacts += (("Report", str(report)),)
        recovery = str(result.get("recovery") or "")
        if recovery:
            artifacts += fields(result, (("Execution intent", "execution_receipt"),))
        next_action = recovery
        if not next_action:
            if registration == "registered":
                next_action = f"mf datasets list{flags} --operation normalize"
            elif logical_run:
                next_action = f"mf runs show {logical_run}{flags}"
        status = registration if registering and registration else status_of(result, "execute")
        if status in {"failed", "pending"}:
            if engine_error or (engine == "failed" and verification in {"", "not_started"}):
                issues.insert(0, "Failed stage · Engine")
            elif verification == "failed":
                issues.insert(0, "Failed stage · Verification")
            elif registration in {"failed", "pending"}:
                issues.insert(0, "Failed stage · Registration")
            elif failed:
                issues.insert(0, "Failed stage · Engine")
        identity = fields(result, (("Recipe", "recipe"), ("Result", "reuse_message")))
        for original in rows(list(mapping(result.get("reused_outputs")).values())):
            path = original.get("path")
            if path:
                publication = mapping(mapping(original.get("publications")).get(str(path)))
                label = (
                    f"Reused publication {publication['publication_id']}"
                    if "publication_id" in publication
                    else "Reused output"
                )
                artifacts += ((label, str(path)),)

        if not identity and result.get("submitted_source"):
            identity = (("Batch", Path(str(result["submitted_source"])).name),)
        return cls(
            status,
            environment,
            identity,
            engine,
            engine_duration(result),
            processing,
            tuple(PublishedDataset.from_evidence(row) for row in outputs),
            registration_counts,
            verification,
            registration,
            tuple(dict.fromkeys(issues)),
            str(result.get("engine_diagnostics_path") or "")
            if status not in {"complete", "completed", "registered"}
            else "",
            artifacts,
            next_action,
        )
