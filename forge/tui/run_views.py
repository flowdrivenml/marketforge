"""Adapt shared semantic run projections to inspection sections."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from forge.orchestration.run_diagnostics import DiagnosticReader
from forge.orchestration.runs import RunInspection, safe_report
from forge.shared.errors import MarketForgeError
from forge.shared.presentation import runs
from forge.startup.context import ApplicationContext
from forge.tui.controller import document
from forge.tui.editor_inspection import RetainedFile
from forge.tui.overview import stages
from forge.tui.presentation import entries


@dataclass(frozen=True, slots=True)
class RunView:
    title: str
    evidence: dict[str, object]
    sections: dict[str, object]
    command: str = ""
    log_files: dict[str, Path] = field(default_factory=dict[str, Path])
    files: tuple[RetainedFile, ...] = ()
    diagnostics: DiagnosticReader | None = None


def project(evidence: RunInspection, context: ApplicationContext, action: str) -> RunView:
    environment = "testing" if context.testing else "production"
    reader = DiagnosticReader(context.paths, evidence, purpose=environment)
    files = retained_files(
        evidence.evidence,
        context.paths.runs_dir,
        record=evidence.record,
        sources={**evidence.report_files, **evidence.log_files},
        jobs=evidence.conversion_jobs,
        prepared_root=context.paths.normalization_jobs_dir,
    )
    if action == "logs":
        view = runs.logs(evidence)
        sections: dict[str, object] = {
            "Events": [
                dict(zip(("Time", "Level", "Stage", "Message"), event, strict=True))
                for event in view.events
            ],
            "Log sources": [
                {"Stream": stream.name, "Bounded tail": stream.truncated} for stream in view.streams
            ]
            or {"Notice": view.notice},
        }
        for stream in view.streams:
            sections[stream.name] = [{"Line": line} for line in stream.content.splitlines()]
        return RunView(
            f"Run logs · {view.identity}",
            evidence.evidence,
            sections,
            log_files=evidence.log_files,
            files=files,
            diagnostics=reader,
        )
    if action == "show" and evidence.record is not None:
        view = runs.detail(evidence, context.paths.root, environment)
        detail_sections: dict[str, object] = {
            "Summary": {
                "Run ID": view.run.identity,
                "Operation": view.run.operation,
                "Action": view.run.action,
                "Recipe": view.run.recipe,
                "Status": view.run.status,
                **dict(view.identity),
                **dict(view.timing),
                **dict(view.lifecycle),
            },
            "Artifacts": dict(view.artifacts),
            "Diagnostics": [{"Message": message} for message in view.messages],
        }
        configuration = {
            item.label: item.path.name
            for item in files
            if item.path.name
            in {
                "submitted.toml",
                "recipe.toml",
                "effective-configuration.json",
                "resolved-plan.json",
                "normalize.toml",
                "merge.toml",
                "convert.toml",
            }
        }
        if view.run.recipe and not any(
            item.path.name in {"submitted.toml", "recipe.toml"} for item in files
        ):
            configuration["Original recipe"] = "Original recipe snapshot unavailable."
        if configuration:
            detail_sections["Configuration"] = configuration
        detail_sections.update(
            table_sections((view.attempts, view.children, view.attempt_evidence, *view.resources))
        )
        return RunView(
            f"Run · {evidence.name}",
            evidence.evidence,
            detail_sections,
            files=files,
            diagnostics=reader,
        )
    report = runs.report(evidence, context.paths.root, environment)
    sections = {
        "Summary": {**dict(report.identity), "Status": report.status, **dict(report.lifecycle)},
        "Inputs": dict(report.inputs),
        "Outputs": dict(report.outputs),
        "Diagnostics": [{"Message": message} for message in report.diagnostics],
        "Artifacts": dict(report.artifacts),
    }
    sections.update(table_sections(report.tables))
    for key, label in (
        ("jobs", "Execution jobs"),
        ("stages", "Stage evidence"),
        ("registrations", "Registrations"),
        ("verification", "Verification evidence"),
        ("result", "Execution result"),
        ("effective_runtime", "Effective runtime"),
    ):
        if isinstance(value := evidence.evidence.get(key), (dict, list)):
            sections[label] = value
    return RunView(
        f"{report.title} · {evidence.name}",
        evidence.evidence,
        sections,
        report.machine_command,
        files=files,
        diagnostics=reader,
    )


def table_sections(tables: tuple[runs.TableView, ...]) -> dict[str, object]:
    return {
        table.title: [dict(zip(table.columns, row, strict=True)) for row in table.records]
        for table in tables
        if table.records
    }


REPORT_REFERENCES = {
    **{key: label for label, key in runs.ARTIFACT_FIELDS},
    "child_report": "Stage report",
    "submission_path": "Submitted job",
    "submitted_source": "Prepared source",
    "configuration_path": "Configuration snapshot",
    "submitted_config": "Submitted configuration",
    "recipe_snapshot": "Recipe snapshot",
    "effective_configuration": "Effective configuration",
}


def retained_files(
    evidence: Mapping[str, object],
    root: Path,
    *,
    record: Mapping[str, object] | None = None,
    sources: Mapping[str, Path] | None = None,
    jobs: tuple[dict[str, Any], ...] = (),
    prepared_root: Path | None = None,
) -> tuple[RetainedFile, ...]:
    """Collect known producer and index pointers, keeping raw paths separate from labels."""
    targets: dict[Path, RetainedFile] = {}
    permitted_directories: set[Path] = set()

    def add(
        label: str,
        reference: object,
        *,
        indexed: bool = False,
        prepared: bool = False,
        sha256: object = None,
    ) -> None:
        target = RetainedFile.reference(label, reference, root)
        if target is None or not target.path.is_absolute() or ".." in target.path.parts:
            return
        if target.path.is_relative_to(root):
            if not target.path.is_file():
                return
            if indexed and target.path.parent != root:
                permitted_directories.add(target.path.parent)
            if not indexed and not any(
                target.path.is_relative_to(parent) for parent in permitted_directories
            ):
                return
        elif prepared and prepared_root and target.path.is_relative_to(prepared_root):
            target = RetainedFile(label, target.path, prepared_root)
        else:
            return
        if isinstance(sha256, str):
            targets[target.path] = RetainedFile(label, target.path, target.scope, sha256)
        else:
            targets.setdefault(target.path, target)

    for label, path in (sources or {}).items():
        add(label, path, indexed=True)
    if record is not None:
        directory = record.get("artifact_root")
        if isinstance(directory, str):
            path = Path(directory)
            if path.parent == root and path.name == record.get("run_id"):
                permitted_directories.add(path)
        add("Execution report", record.get("report_path"), indexed=True)
        summary = document(record.get("summary"))
        metadata = document(summary.get("recipe_source"))
        add("Original recipe", summary.get("recipe_snapshot"), sha256=metadata.get("sha256"))
        add(
            "Effective configuration",
            metadata.get("effective_configuration"),
            sha256=metadata.get("effective_sha256"),
        )
        if isinstance(directory, str) and Path(directory) in permitted_directories:
            add("Original recipe", Path(directory) / "submitted.toml")
            add("Resolved configuration and inputs", Path(directory) / "resolved-plan.json")
        for attempt in entries(record.get("attempts")):
            number = attempt.get("number")
            add(f"Attempt {number} outcome", attempt.get("outcome_path"), indexed=True)
            add(f"Attempt {number} report", attempt.get("report_path"), indexed=True)
            outcome = attempt.get("outcome_path")
            if isinstance(outcome, str) and isinstance(directory, str):
                attempt_dir = Path(outcome).parent
                run_dir = Path(directory)
                if attempt_dir.parent == run_dir / "attempts" and run_dir in permitted_directories:
                    snapshot_metadata = attempt_dir / "recipe-source.json"
                    if snapshot_metadata.is_file():
                        try:
                            snapshot = safe_report(snapshot_metadata, run_dir)
                        except (MarketForgeError, OSError):
                            snapshot = {}
                        add(
                            f"Attempt {number} original recipe",
                            snapshot.get("snapshot", attempt_dir / "recipe.toml"),
                            sha256=snapshot.get("sha256"),
                        )
                        add(
                            f"Attempt {number} effective configuration",
                            snapshot.get("effective_configuration"),
                            sha256=snapshot.get("effective_sha256"),
                        )
                    elif (effective := attempt_dir / "effective-configuration.json").is_file():
                        add(f"Attempt {number} effective configuration", effective)
        for resource in entries(record.get("resources")):
            kind = resource.get("kind")
            if kind in ("report", "prepared_batch", "prepared_job"):
                label = str(kind).replace("_", " ").title()
                if name := resource.get("stage"):
                    label = f"{name} · {label}"
                add(label, resource.get("reference"), indexed=True, prepared=kind != "report")

    for value in (evidence, *stages(evidence), *entries(evidence.get("jobs")), *jobs):
        for key, label in REPORT_REFERENCES.items():
            add(
                label,
                value.get(key),
                prepared=key in {"batch_path", "job_path", "submitted_source"},
                sha256=value.get(
                    {
                        "submitted_config": "submitted_sha256",
                        "effective_configuration": "effective_sha256",
                        "resolved_plan": "resolved_sha256",
                        "configuration_path": "configuration_sha256",
                        "submission_path": "submitted_sha256",
                    }.get(key, "")
                )
                if key != "submission_path" or value.get("operation") != "convert"
                else None,
            )
        add("Submitted job", document(value.get("submission")).get("path"))
        pins = document(value.get("artifacts"))
        for key, label in (
            ("runtime", "Configuration snapshot"),
            ("recipe_snapshot", "Recipe snapshot"),
            ("catalog_snapshot", "Catalogue snapshot"),
            ("collection", "Prepared collection"),
        ):
            add(label, document(pins.get(key)).get("path"))
        prepared = document(value.get("prepared"))
        add("Prepared batch", prepared.get("batch"), prepared=True)
        add("Prepared job", prepared.get("job"), prepared=True)
    return tuple(targets.values())
