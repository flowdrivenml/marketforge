"""Presentation and bounded read-only sources for the workspace home."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from shutil import disk_usage
from time import monotonic
from typing import cast

from rich.text import Text

from forge.historical.archives.progress import ProgressSnapshot
from forge.historical.pipelines.progress import PipelineSnapshot
from forge.orchestration.inspection import MANAGED_RUN
from forge.shared.terminal.palette import BLUE, CYAN
from forge.tui.controller import Operation, document, outcome
from forge.tui.operation_view import duration
from forge.tui.presentation import Cell, entries, status
from forge.tui.resources import Row, value_at


@dataclass(frozen=True)
class Observation:
    text: str
    checked: datetime
    available: bool = True
    detail: str = ""
    targets: tuple[tuple[str, str], ...] = ()

    def label(self, now: datetime) -> str:
        return "stale · " + self.text if (now - self.checked).total_seconds() > 300 else self.text


def storage_free(path: Path) -> str:
    while not path.exists() and path != path.parent:
        path = path.parent
    free = disk_usage(path).free
    return f"{free / 1024**3:.1f} GiB free"


def text(value: object) -> str:
    return str(value) if value is not None else ""


def stages(report: Mapping[str, object]) -> tuple[Mapping[str, object], ...]:
    return tuple(document(item) for item in document(report.get("stages")).values())


def execution_key(report: Mapping[str, object]) -> tuple[str, str] | None:
    identity, fingerprint = report.get("execution_id"), report.get("submitted_sha256")
    if isinstance(identity, str) and isinstance(fingerprint, str) and identity and fingerprint:
        return identity, fingerprint
    return None


def registration_problem(report: Mapping[str, object]) -> bool:
    return (
        report.get("verification") == "verified"
        and report.get("registration") in ("failed", "pending")
        and bool(report.get("execution_receipt"))
    )


def pipeline_recovery(report: Mapping[str, object]) -> bool:
    return (
        report.get("kind") == "historical_pipeline"
        and report.get("state") in ("stopped", "interrupted", "blocked", "failed")
        and all(report.get(key) for key in ("run_id", "submitted_sha256", "resolved_sha256"))
    )


def publications(report: Mapping[str, object]) -> tuple[int, ...]:
    receipts = (report, *(document(stage.get("result")) for stage in stages(report)))
    return tuple(
        dict.fromkeys(
            identity
            for receipt in receipts
            for value in document(receipt.get("publications")).values()
            if isinstance(identity := document(value).get("publication_id"), int)
        )
    )


def result_label(report: Mapping[str, object], action: str = "") -> str:
    state = outcome(dict(report))
    candidates = (report, *(document(stage.get("result")) for stage in stages(report)))
    if any(registration_problem(item) for item in candidates):
        return (
            "Verified output · registration pending"
            if all(item.get("registration") != "failed" for item in candidates)
            else "Verified output · registration failed"
        )
    ids = publications(report)
    if ids:
        return f"{len(ids)} registered publication" + ("s" if len(ids) != 1 else "")
    files: set[str] = set()
    for stage in stages(report):
        selected = stage.get("primary_file_ids")
        if isinstance(selected, list):
            files.update(str(item) for item in cast(list[object], selected))
    if files:
        return f"{len(files)} registered files"
    if state in ("prepared", "stopped") or report.get("prepared_job_count"):
        return "Prepared jobs"
    if state in ("valid", "validated") or (
        action.endswith(" validate") and state == "result available"
    ):
        return "Validation report"
    if state in ("materialized", "unpublished"):
        return "Materialized · unpublished"
    if state == "registered" or report.get("registration") == "registered":
        return "Registered output"
    output = document(report.get("publication"))
    jobs = entries(report.get("jobs"))
    committed = sum(bool(document(job.get("publication")).get("manifest")) for job in jobs)
    if output.get("manifest") or committed:
        description = (
            f"{committed} committed publications" if committed > 1 else "Committed publication"
        )
        if report.get("registration") == "not_available" or any(
            job.get("registration") == "not_available" for job in jobs
        ):
            description += " · registration unavailable"
        return description
    if report.get("verification") == "verified":
        return "Verified output" + (
            " · registration unavailable" if report.get("registration") == "not_available" else ""
        )
    if state == "published":
        return "Committed publication"
    if state in ("failed", "blocked", "interrupted"):
        return "Failure report" if state == "failed" else "Retained report"
    if state in ("running", "pending", "interrupting"):
        return "Awaiting result"
    return "Report" if report else "—"


def started(value: object) -> str:
    if isinstance(value, str):
        try:
            stamp = datetime.fromisoformat(value)
            if stamp.tzinfo is not None:
                return stamp.astimezone(UTC).strftime("%m-%d %H:%M")
        except ValueError:
            pass
    return "—"


def operation_lines(op: Operation, active: bool, height: int, snapshot: object) -> Text:
    result = document(op.result)
    label = snapshot.name if isinstance(snapshot, PipelineSnapshot) else Path(op.reference).name
    elapsed = int(
        max(
            0, (op.finished_tick if op.finished_tick is not None else monotonic()) - op.started_tick
        )
    )
    output = Text(no_wrap=True, overflow="ellipsis")
    output.append(op.title.title(), style=f"bold {BLUE}")
    if label:
        output.append(f" · {label[:42]}")
    output.append(" · ")
    output.append_text(status("interrupting" if op.state == "interrupting" else op.state))
    output.append(f" · {elapsed // 3600:02}:{elapsed // 60 % 60:02}:{elapsed % 60:02}")
    phase = ""
    progress = ""
    activity = ""
    acquisition = snapshot if isinstance(snapshot, ProgressSnapshot) else None
    if isinstance(snapshot, PipelineSnapshot):
        stage = next(
            (stage for stage in snapshot.stages if stage.name == snapshot.active_stage), None
        )
        if stage:
            phase = f"{stage.name.capitalize()} / {stage.phase.replace('_', ' ')}"
            if stage.verified_jobs is not None:
                done, total = stage.verified_jobs
                progress = f"{done} / {total} jobs verified" if total else f"{done} jobs verified"
            activity = stage.summary
        if height >= 22:
            output.append(
                "\n" + "  ·  ".join(f"{step.name.title()} {step.state}" for step in snapshot.stages)
            )
        acquisition = snapshot.acquisition if snapshot.active_stage == "acquire" else None
        if snapshot.recent and not activity:
            last = snapshot.recent[-1]
            activity = f"{last.stage} / {last.phase.replace('_', ' ')} · {last.state}"
    if acquisition:
        phase = phase or "Acquisition"
        done, total = acquisition.requests.finished, acquisition.total_requests
        progress = f"{done} / {total} requests" if total else f"{done} requests complete"
        progress += (
            f" · {acquisition.files.completed} downloaded · {acquisition.files.reused} reused"
        )
        activity = acquisition.activity or acquisition.current
    if isinstance(snapshot, str):
        phase = snapshot
    if phase or progress:
        output.append("\n" + " · ".join(filter(None, (phase, progress))), style=CYAN)
    if active and activity and height >= 20:
        output.append("\n" + activity)
    if not active:
        output.append("\n" + result_label(result, op.title))
    return output


def run_duration(report: Mapping[str, object]) -> str:
    start, finish = text(report.get("started_at")), text(report.get("finished_at"))
    return duration(start, finish) if start and finish else "—"


def engine_summary(value: object) -> Observation:
    targets = entries(document(value).get("targets"))
    checked = datetime.now(UTC)
    if not targets:
        return Observation("status unavailable", checked, False)
    count = sum(item.get("status") == "verified" for item in targets)
    return Observation(
        f"{count}/{len(targets)} verified",
        checked,
        detail=", ".join(f"{item.get('target')}: {item.get('status')}" for item in targets),
        targets=tuple((text(item.get("target")), text(item.get("status"))) for item in targets),
    )


def run_columns(name: str, state: str, value: object, width: int) -> tuple[Cell, ...]:
    op = value if isinstance(value, Operation) else None
    evidence = document(op.result if op else value)
    start = op.started_at if op else evidence.get("started_at")
    range_evidence = (
        {"started_at": op.started_at, "finished_at": op.finished_at} if op else evidence
    )
    compact = width < 100
    wide = width >= 146
    return (
        Cell("Started", started(start), 11),
        Cell("Workflow / recipe", name, 17 if compact else 28 if wide else 22),
        Cell("State", state, 16, state=True),
        Cell("Result", result_label(evidence, op.title if op else ""), 38 if wide else 22),
        Cell("Duration", run_duration(range_evidence), 8, 1),
    )


def read_artifact(root: Path, path: Path) -> str:
    if not path.resolve().is_relative_to(root.resolve()):
        raise ValueError("Prepared artifact is outside the managed runs directory.")
    with path.open("rb") as source:
        data = source.read(256 * 1024 + 1)
    if len(data) > 256 * 1024:
        raise ValueError("Prepared artifact exceeds the 256 KiB preview limit.")
    return data.decode()


def recent_rows(retained: list[dict[str, object]], history: list[Operation]) -> dict[str, Row]:
    rows: dict[str, Row] = {}
    for item in retained:
        identity = value_at(item, "run_id")
        kind = value_at(item, "kind")
        name = next(
            (
                str(value)
                for key in ("name", "recipe", "operation")
                if isinstance(value := item.get(key), str) and value
            ),
            "Pipeline"
            if kind == "historical_pipeline"
            else identity.split("-")[0].capitalize()
            if MANAGED_RUN.fullmatch(identity)
            else "Run",
        )
        if kind == "historical_pipeline" and name != "Pipeline":
            name = f"Pipeline · {name}"
        state = outcome(item)
        if state in {"running", "pending", "interrupting"}:
            state = f"Recorded {state}"
        rows[identity] = Row(identity, name, "Retained run", state, item, identity)
    for op in reversed(history[-8:]):
        evidence = document(op.result)
        identity = str(evidence.get("run_id") or evidence.get("execution_id") or op.id)
        if identity in rows:
            continue
        rows[identity] = Row(
            identity,
            " · ".join(filter(None, (op.title.title(), op.reference))),
            "Owned run",
            op.state,
            op,
            identity,
        )
    return rows


def run_started(row: Row) -> str:
    return (
        row.value.started_at
        if isinstance(row.value, Operation)
        else text(document(row.value).get("started_at"))
    )
