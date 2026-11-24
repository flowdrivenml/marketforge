"""Registry-backed run browsing with explicit historical filesystem access."""

import json
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from forge.orchestration.inspection import (
    MANAGED_RUN,
    read_engine_logs,
    read_events,
    read_logs,
    read_managed_logs,
    read_managed_report,
    read_report,
)
from forge.orchestration.registry import connection_for, show_index
from forge.orchestration.registry_maintenance import bounded_json
from forge.orchestration.run_summary import mapping
from forge.orchestration.run_visibility import hidden_names, marker
from forge.shared.database.connection import DatabaseError
from forge.shared.errors import MarketForgeError
from forge.shared.identity import parse_run_id
from forge.shared.paths import ProjectPaths


def safe_reference(reference: str) -> tuple[str, bool]:
    if MANAGED_RUN.fullmatch(reference):
        return reference, True
    try:
        parsed = parse_run_id(reference)
    except ValueError:
        raise MarketForgeError(
            "Expected a canonical run ID or a supported retained reference."
        ) from None
    return str(parsed), parsed.operation is None


def safe_report(path: Path, root: Path, *, exact: bool = False) -> dict[str, Any]:
    # The indexed path is a pointer. Read the original bytes, with the same inspection bounds.
    if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
        raise MarketForgeError("Unsafe indexed report path.")
    if not path.is_file():
        return {"status": "report_missing_or_interrupted"}
    with path.open("rb") as stream:
        data = stream.read(4 * 1024 * 1024 + 1)
    if len(data) > 4 * 1024 * 1024:
        if exact:
            return {"status": "report_exceeds_preview_limit"}
        raise MarketForgeError("Run report exceeds 4 MiB inspection limit.")
    try:
        payload: object = json.loads(data, parse_float=Decimal if exact else float)
    except (ValueError, UnicodeError):
        return {"status": "report_incomplete_or_invalid"}
    return (
        cast(dict[str, Any], payload)
        if isinstance(payload, dict)
        else {"status": "report_incomplete_or_invalid"}
    )


@dataclass(slots=True)
class RunInspection:
    """Original machine evidence with separately retained inspection context."""

    name: str
    evidence: dict[str, Any]
    record: dict[str, Any] | None
    truncated_streams: set[str] = field(default_factory=set[str])
    supplemental_logs: dict[str, str] = field(default_factory=dict[str, str])
    conversion_jobs: tuple[dict[str, Any], ...] = ()
    inspection_notes: tuple[str, ...] = ()
    removal: dict[str, Any] | None = None
    log_files: dict[str, Path] = field(default_factory=dict[str, Path])
    report_files: dict[str, Path] = field(default_factory=dict[str, Path])


async def inspect_run(
    paths: ProjectPaths, reference: str, *, action: str, limit: int
) -> dict[str, Any]:
    return (await inspect_run_evidence(paths, reference, action=action, limit=limit)).evidence


async def inspect_run_evidence(
    paths: ProjectPaths,
    reference: str,
    *,
    action: str,
    limit: int,
    human: bool = False,
    editor_targets: bool = False,
) -> RunInspection:
    name, legacy = safe_reference(reference)
    root = paths.runs_dir
    removal = None
    try:
        async with connection_for(paths) as (connection, _):
            removal = await marker(connection, name, include_alias=True, runs_root=root)
        if removal and removal["mode"] == "full":
            return RunInspection(
                name,
                {"run_id": name, "status": "run_deletion_recorded", "history_removal": removal},
                None,
                removal=removal,
            )
        record = await show_index(paths, name)
    except DatabaseError:
        if not legacy:
            raise
        if name in hidden_names(root):
            raise MarketForgeError(
                "History was removed. Select its original registry to inspect retained evidence."
            ) from None
        record = None
    truncated: set[str] = set()
    supplemental: dict[str, str] = {}
    extra = supplemental if human else None
    log_files: dict[str, Path] = {}
    sources = log_files if human and action == "logs" else None
    report_files: dict[str, Path] = {}
    report_sources = report_files if editor_targets and action != "logs" else None
    if record is None:
        if not legacy:
            raise MarketForgeError("Run is absent from the selected registry.")
        if action == "logs":
            evidence = (
                read_managed_logs(
                    root, name, truncated=truncated, supplemental=extra, sources=sources
                )
                if MANAGED_RUN.fullmatch(name)
                else read_logs(root, name, limit, sources=sources)
            )
        else:
            evidence = read_managed_report(root, name, sources=report_sources)
    elif action == "show":
        evidence = record
    else:
        evidence = retained_evidence(
            root,
            name,
            record,
            action=action,
            limit=limit,
            truncated=truncated,
            supplemental=extra,
            sources=sources,
            report_sources=report_sources,
        )
    jobs, notes = (
        conversion_receipts(root, record)
        if human and action == "report" and record and record.get("operation") == "convert"
        else ((), ())
    )
    if human and action == "report":
        if (
            not jobs
            and name.startswith("convert-execute-")
            and isinstance(evidence.get("publication"), dict)
        ):
            jobs = (evidence,)
        jobs = tuple(conversion_metadata(job, paths.datasets_dir / "converted") for job in jobs)
    if removal:
        evidence = {**evidence, "history_removal": removal}
    return RunInspection(
        name,
        evidence,
        record,
        truncated,
        supplemental,
        jobs,
        notes,
        removal,
        log_files,
        report_files,
    )


def conversion_metadata(receipt: dict[str, Any], root: Path) -> dict[str, Any]:
    """Read the pinned manifest's counters without opening or verifying primary data files."""
    from forge.datasets.converted import manifest_evidence

    reference = mapping(receipt.get("publication")).get("manifest")
    if not isinstance(reference, str):
        return receipt
    path = Path(reference)
    try:
        if not path.is_absolute() or path.parent.resolve().parent != root.resolve():
            return receipt
        _, manifest = manifest_evidence(receipt)
    except (OSError, KeyError, TypeError, ValueError, RuntimeError):
        return receipt
    if manifest.get("job_id") != receipt.get("job_id"):
        return receipt
    return {
        **receipt,
        "statistics": manifest.get("statistics"),
        "conversion": manifest.get("conversion"),
    }


def conversion_receipts(
    root: Path,
    record: dict[str, Any],
) -> tuple[tuple[dict[str, Any], ...], tuple[str, ...]]:
    """Read fixed per-job receipts referenced by the latest indexed conversion attempt."""
    if not record["attempts"] or record.get("attempts_truncated"):
        return (), ()
    number = record["attempts"][-1]["number"]
    references = {
        ref
        for row in record["resources"]
        if row["kind"] == "report"
        and row["number"] == number
        and isinstance(ref := row["reference"], str)
        and Path(ref).name == "execution-result.json"
        and Path(ref).parent.name.startswith("convert-execute-")
        and MANAGED_RUN.fullmatch(Path(ref).parent.name)
    }
    recovered: list[dict[str, Any]] = []
    unavailable = 0
    for reference in sorted(references)[:8]:
        path = Path(reference)
        if path.parent.is_symlink() or path.parent.resolve().parent != root.resolve():
            unavailable += 1
            continue
        try:
            receipt = safe_report(path, root)
        except (OSError, MarketForgeError):
            unavailable += 1
            continue
        if (
            receipt.get("registry_id") == str(record["database_id"])
            and type(receipt.get("job_id")) is int
            and isinstance(receipt.get("execution"), str)
        ):
            recovered.append({**receipt, "result_path": str(path)})
        else:
            unavailable += 1
    notes: list[str] = []
    if len(references) > 8:
        notes.append(
            f"Showing up to 8 of {len(references):,} indexed conversion receipt references. See run details for retained pointers."
        )
    if unavailable:
        notes.append(
            f"{unavailable:,} referenced conversion receipts were unavailable, invalid, or from a different registry."
        )
    return tuple(recovered), tuple(notes)


def retained_evidence(
    root: Path,
    name: str,
    record: dict[str, Any],
    *,
    action: str,
    limit: int,
    truncated: set[str] | None = None,
    supplemental: dict[str, str] | None = None,
    sources: dict[str, Path] | None = None,
    report_sources: dict[str, Path] | None = None,
) -> dict[str, Any]:
    if Path(record["artifact_root"]).resolve() != (root / name).resolve():
        raise MarketForgeError("Run evidence belongs to a different workspace.")
    report = record["report_path"]
    if action == "report":
        if report:
            if report_sources is not None:
                report_sources["Execution report"] = Path(report)
            return safe_report(Path(report), root, exact=report_sources is not None)
        if record["attempts"]:
            outcome = Path(record["attempts"][-1]["outcome_path"])
            if outcome.is_file():
                if report_sources is not None:
                    report_sources["Attempt outcome"] = outcome
                    return safe_report(outcome, root / name, exact=True)
                return bounded_json(outcome, root / name)
        return read_report(root, name, sources=report_sources)
    if record["operation"] == "acquisition" and report:
        path = Path(report).with_name("events.jsonl")
        if path.resolve().is_relative_to((root / name).resolve()) and path.is_file():
            return read_events(path, name, limit, sources=sources)
    events = (
        read_logs(root, name, limit, sources=sources) if not MANAGED_RUN.fullmatch(name) else None
    )
    if events is not None and events.get("status") != "events_missing":
        return events
    if MANAGED_RUN.fullmatch(name):
        return read_managed_logs(
            root, name, truncated=truncated, supplemental=supplemental, sources=sources
        )
    directory = Path(report).parent if report else None
    if directory is not None and record["operation"] == "normalize":
        retained = safe_report(Path(report), root, exact=True)
        if retained.get("kind") == "normalization_prepare_report" and isinstance(
            reference := retained.get("validation_logs"), str
        ):
            logs = Path(reference)
            if (
                logs.name.startswith("validation-")
                and directory in (logs, logs.parent)
                and logs.resolve().is_relative_to(root.resolve())
                and not logs.is_symlink()
            ):
                return read_engine_logs(logs, truncated=truncated, sources=sources)
    if (
        directory is not None
        and directory.resolve().parent == root.resolve()
        and MANAGED_RUN.fullmatch(directory.name)
    ):
        return read_managed_logs(
            root, directory.name, truncated=truncated, supplemental=supplemental, sources=sources
        )
    return events or {"run_id": name, "events": [], "status": "events_missing"}
