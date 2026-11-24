"""Adapt shared semantic run projections to inspection sections."""

from dataclasses import dataclass, field
from pathlib import Path

from forge.orchestration.runs import RunInspection
from forge.shared.presentation import runs
from forge.startup.context import ApplicationContext


@dataclass(frozen=True, slots=True)
class RunView:
    title: str
    evidence: dict[str, object]
    sections: dict[str, object]
    command: str = ""
    log_files: dict[str, Path] = field(default_factory=dict[str, Path])


def project(evidence: RunInspection, context: ApplicationContext, action: str) -> RunView:
    environment = "testing" if context.testing else "production"
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
            f"Run logs · {view.identity}", evidence.evidence, sections, log_files=evidence.log_files
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
        detail_sections.update(
            table_sections((view.attempts, view.children, view.attempt_evidence, *view.resources))
        )
        return RunView(f"Run · {evidence.name}", evidence.evidence, detail_sections)
    report = runs.report(evidence, context.paths.root, environment)
    sections = {
        "Summary": {**dict(report.identity), "Status": report.status, **dict(report.lifecycle)},
        "Inputs": dict(report.inputs),
        "Outputs": dict(report.outputs),
        "Diagnostics": [{"Message": message} for message in report.diagnostics],
        "Artifacts": dict(report.artifacts),
    }
    sections.update(table_sections(report.tables))
    return RunView(
        f"{report.title} · {evidence.name}", evidence.evidence, sections, report.machine_command
    )


def table_sections(tables: tuple[runs.TableView, ...]) -> dict[str, object]:
    return {
        table.title: [dict(zip(table.columns, row, strict=True)) for row in table.records]
        for table in tables
        if table.records
    }
