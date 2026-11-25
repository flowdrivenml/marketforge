"""Project recorded diagnostic occurrences without assigning severity to prose."""

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal, cast

type Severity = Literal["error", "warning", "info"]
type Location = tuple[str | int, ...]

FAILURES = frozenset({"failed", "blocked", "error", "rejected", "unsupported", "invalid"})
INTERRUPTIONS = frozenset({"interrupted", "cancelled", "canceled"})
ERROR_FIELDS = frozenset(
    {
        "error",
        "error_message",
        "error_summary",
        "registration_error",
        "validation_error",
        "verification_error",
        "indexing_error",
        "exception",
        "exception_message",
        "traceback",
    }
)
WARNING_FIELDS = frozenset(
    {"warning", "warnings", "metadata_warnings", "execution_registry_warning"}
)
ERROR_GROUPS = frozenset({"blockers", "compatibility_blockers", "errors", "failures"})
METADATA = frozenset(
    {
        "exchange",
        "symbol",
        "scope",
        "scope_identifier",
        "instrument",
        "instrument_id",
        "instrument_type",
        "market_category",
        "data_type",
        "request_index",
        "discovery",
        "field",
        "field_path",
        "loc",
        "source_key",
        "source_object_key",
        "archive_id",
        "local_archive_id",
        "remote_archive_id",
        "external_archive_id",
        "local_id",
        "job_id",
        "task_id",
        "failed_task_id",
        "physical_source",
        "archive_member",
        "typed_cause",
        "limits",
        "stream_id",
        "profile",
        "profile_id",
        "format_profile",
        "registry_publication_id",
        "publication_id",
        "engine_publication_id",
        "dataset_id",
        "primary_file_id",
        "code",
        "error_code",
        "exception_type",
        "class",
        "path",
        "phase",
    }
)


def mapping(value: object) -> Mapping[str, object]:
    return cast(Mapping[str, object], value) if isinstance(value, dict) else {}


@dataclass(frozen=True, slots=True)
class EvidenceSource:
    path: Path
    run_id: str
    operation: str
    stage: str = ""
    attempt: str = ""
    status: str = ""
    sha256: str | None = None
    depth: int = 0


@dataclass(frozen=True, slots=True)
class Explanation:
    happened: str
    cause: tuple[str, ...] = ()
    affected: tuple[tuple[str, object], ...] = ()
    impact: str = ""
    resolution: str = ""
    evidence: EvidenceSource | None = None
    limitation: str = ""
    root_cause: bool = False
    mirror_identity: tuple[str, str] | None = None


@dataclass(frozen=True, slots=True)
class Diagnostic:
    severity: Severity
    message: str
    source: EvidenceSource
    location: Location = ()
    line: int | None = None
    timestamp: str = ""
    metadata: tuple[tuple[str, object], ...] = ()
    explanation: Explanation | None = None

    @property
    def pointer(self) -> str:
        return "".join(
            "/" + str(part).replace("~", "~0").replace("/", "~1") for part in self.location
        )


@dataclass(frozen=True, slots=True)
class Stage:
    name: str
    state: str
    child: str = ""
    phase: str = ""


def stage_evidence(document: Mapping[str, object]) -> Iterator[Stage]:
    for name, value in mapping(document.get("stages")).items():
        stage = mapping(value)
        if stage and isinstance(state := stage.get("state"), str):
            yield Stage(
                name, state, str(stage.get("child_run_id") or ""), str(stage.get("phase") or "")
            )


def diagnostics(
    value: object, source: EvidenceSource, *, line: int | None = None
) -> Iterator[Diagnostic]:
    def walk(
        item: object,
        location: Location,
        origin: EvidenceSource,
        context: dict[str, object],
        timestamp: str,
        severity: Severity | None = None,
    ) -> Iterator[Diagnostic]:
        if isinstance(item, list):
            for index, child in enumerate(cast(list[object], item)):
                yield from walk(child, (*location, index), origin, context, timestamp, severity)
            return
        if isinstance(item, str):
            if severity and item and item != "Inspect retained diagnostics.":
                yield Diagnostic(
                    severity, item, origin, location, line, timestamp, tuple(context.items())
                )
            return
        node = mapping(item)
        if not node:
            return
        context = {
            **context,
            **{key: val for key, val in node.items() if key in METADATA and val is not None},
        }
        for key in ("request", "identity", "source", "details", "context"):
            context.update(
                (field, val)
                for field, val in mapping(node.get(key)).items()
                if field in METADATA and val is not None
            )
        if isinstance(at := (node.get("timestamp") or node.get("time")), str):
            timestamp = at
        child_stage = (
            node.get("stage")
            or node.get("failed_stage")
            or mapping(node.get("context")).get("stage")
        )
        if child_stage and origin.stage and child_stage != origin.stage:
            context["recorded_stage"] = child_stage
        attempt = node.get("attempt_id")
        if len(location) >= 2 and location[-2] == "attempts":
            attempt = attempt or node.get("id")
        origin = replace(
            origin,
            stage=origin.stage or str(child_stage or ""),
            attempt=str(attempt or origin.attempt),
        )
        state = str(node.get("status") or node.get("state") or "").casefold()
        level = str(node.get("severity") or node.get("level") or "").casefold()
        kind = str(node.get("type") or "").casefold()
        if level in {"error", "fatal", "critical"} or kind in {"error", "fatal"}:
            severity = "error"
        elif level in {"warning", "warn"} or kind in {"warning", "warn"}:
            severity = "warning"
        elif state in INTERRUPTIONS:
            severity = "info"
        elif state in FAILURES and not level:
            severity = "error"

        for key, child in node.items():
            child_severity: Severity | None = None
            child_origin = origin
            if key == "stages":
                for stage, stage_node in mapping(child).items():
                    yield from walk(
                        stage_node,
                        (*location, key, stage),
                        replace(origin, stage=stage),
                        context,
                        timestamp,
                    )
                continue
            if key in WARNING_FIELDS:
                child_severity = "warning"
            elif key in ERROR_GROUPS:
                child_severity = "error"
            elif key in ERROR_FIELDS:
                child_severity = severity or "error"
                phase = key.removesuffix("_error")
                if phase in {"registration", "validation", "verification", "indexing"}:
                    child_origin = replace(origin, stage=origin.stage or phase)
            elif key in {"message", "reason", "summary", "detail", "details"}:
                child_severity = severity
            elif isinstance(child, (dict, list)):
                # Failure belongs to the declaring record, not its inputs/statistics.
                # A list of blockers or metadata warnings inherits its own contract.
                if (
                    severity is not None
                    and location
                    and (
                        location[-1] in WARNING_FIELDS | ERROR_GROUPS | ERROR_FIELDS
                        or (
                            isinstance(location[-1], int)
                            and len(location) >= 2
                            and location[-2] in WARNING_FIELDS | ERROR_GROUPS | ERROR_FIELDS
                        )
                    )
                ):
                    child_severity = severity
            if isinstance(child, (dict, list)) or child_severity:
                if key in {"recipe_source", "effective_configuration", "argv", "recent"}:
                    continue
                yield from walk(
                    cast(object, child),
                    (*location, key),
                    child_origin,
                    context,
                    timestamp,
                    child_severity,
                )

    yield from walk(value, (), source, {}, "")


def explain(entry: Diagnostic) -> Explanation:
    """Describe contracts that identify the failing phase, even without a cause."""
    if entry.explanation is not None:
        return entry.explanation
    context = dict(entry.metadata)
    if entry.source.operation in {"acquire", "acquisition"} and entry.severity == "error":
        if context.get("discovery") in ("failed", "unsupported"):
            return Explanation(
                "Acquisition could not discover archives for this request.",
                affected=entry.metadata,
                impact="Discovery stopped for the recorded request.",
                resolution=(
                    "Check the exchange, market category, and data kind against supported source combinations."
                    if context.get("discovery") == "unsupported"
                    else "Review the request identity and the original provider diagnostic."
                ),
            )
        if entry.line is not None and "remote_archive_id" in context:
            return Explanation(
                "Acquisition recorded a failure while downloading this archive.",
                affected=entry.metadata,
                resolution="Check the archive key and the recorded download diagnostic before retrying.",
            )
    if (
        entry.source.operation == "normalize"
        and dict(entry.metadata).get("error_code") == "source_open"
    ):
        return Explanation(
            "The Engine could not open a normalization source.",
            affected=entry.metadata,
            resolution="Check the retained archive or source path and the underlying open error.",
        )
    fields = {part for part in entry.location if isinstance(part, str)}
    for key, phase, action in (
        (
            "compatibility_blockers",
            "Engine compatibility validation",
            "Review the preparation report and validate the corrected inputs again.",
        ),
        (
            "validation_error",
            "Validation",
            "Review the retained validation evidence for the rejected input.",
        ),
        (
            "registration_error",
            "Registration",
            "Review the retained registration result and publication identity before retrying registration.",
        ),
        (
            "verification_error",
            "Output verification",
            "Review the retained verification result before using the output.",
        ),
        (
            "indexing_error",
            "Indexing",
            "Review the indexing result and verify the publication identity before retrying indexing.",
        ),
    ):
        if key in fields:
            return Explanation(
                f"{phase} recorded a failure.", affected=entry.metadata, resolution=action
            )
    if "blockers" in fields:
        return Explanation(
            "The report records a condition that blocked processing.",
            affected=entry.metadata,
            resolution="Review the blocked input and the original report before preparing again.",
        )
    if entry.severity == "warning":
        return Explanation("The retained evidence records a warning.", affected=entry.metadata)
    return Explanation(
        "The retained evidence records an error.",
        affected=entry.metadata,
        resolution="Inspect the original evidence for the failing input or processing step.",
    )


def conversion_route_failure(report: Mapping[str, object], message: str) -> Explanation | None:
    """Match a retained resolver or Engine route failure to its preparation evidence."""
    resolutions = report.get("reference_resolutions")
    if isinstance(resolutions, list):
        for item in cast(list[object], resolutions):
            resolution = mapping(item)
            if resolution.get("mode") != "automatic" or resolution.get("failure") != message:
                continue
            ranking = mapping(resolution.get("ranking"))
            return Explanation(
                "Automatic reference selection could not establish usable bindings.",
                cause=(message,),
                affected=tuple(
                    (key, value)
                    for key in (
                        "source_registry_publication_id",
                        "target_quote",
                        "sources",
                        "path_policy",
                    )
                    if (value := resolution.get(key)) is not None
                )
                + (("ranking_evidence", ranking.get("status") or "Unavailable"),),
                impact="Conversion stopped during preparation.",
                resolution=(
                    "Inspect the eligible trades and potential routes in the preparation report. "
                    "Select compatible references manually, pin a suitable ranking snapshot, "
                    "or bind a permitted external trade publication."
                ),
                root_cause=True,
            )
    jobs = report.get("jobs")
    if report.get("compatibility") != "not_validated" or not isinstance(jobs, list):
        return None
    for value in cast(list[object], jobs):
        job = mapping(value)
        path = job.get("path")
        # The report-level job_path is a collection. The validation error names a member.
        if job.get("engine_validation") != "failed" or not isinstance(path, str):
            continue
        if message != (
            f"Engine validation failed for {path}: Conversion failed: "
            "invalid configuration: trade conversion streams have no available path"
        ):
            continue
        affected = tuple(
            (key, item)
            for fields, keys in (
                (
                    mapping(report.get("upstream")),
                    ("registry_publication_id", "engine_publication_id"),
                ),
                (
                    mapping(report.get("settings")),
                    ("target_quote", "price_source", "max_staleness_ns", "max_hops", "path_policy"),
                ),
                (job, ("job_id", "path", "references")),
                (report, ("external_reference",)),
            )
            for key in keys
            if (item := fields.get(key)) is not None
        )
        return Explanation(
            "The Engine rejected the prepared job because at least one trade stream "
            "had no permitted conversion path to the target quote.",
            cause=(message,),
            affected=affected,
            impact="This job did not pass preparation validation.",
            resolution=(
                "Review the source quote currencies, reference stream selection, and routing policy "
                "for a path to the target quote. Correct the inputs or policy and validate preparation again."
            ),
            root_cause=True,
        )
    return None
