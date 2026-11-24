"""Validate a pipeline override draft with the same runtime composition as execution."""

from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from forge.historical.engine.managed import checksum, resolve
from forge.historical.engine.runtime import PIPELINE_STAGES, compose_pipeline
from forge.historical.engine.runtime_config import (
    RuntimeConfiguration,
    Snapshot,
    parse_document,
    read_snapshot,
    replace_entry,
)
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import WorkspaceOwnership, acquire_file_lock
from forge.shared.paths import ProjectPaths


@dataclass(frozen=True)
class PipelineChange:
    original: Snapshot
    candidate: bytes
    changes: list[dict[str, Any]]
    inputs: dict[str, str]

    @property
    def risk(self) -> bool:
        return any(field["risk"] == "integrity-critical" for field in self.changes)


class PipelineConfiguration:
    def __init__(self, paths: ProjectPaths) -> None:
        self.paths = paths
        self.path = paths.runtime_config("pipeline")
        self.stages = {stage: RuntimeConfiguration(paths, stage) for stage in PIPELINE_STAGES}

    def snapshot(self) -> Snapshot:
        return read_snapshot(self.path)

    def effective(self, data: bytes) -> tuple[dict[str, Any], dict[str, str]]:
        binaries = {stage: resolve(self.paths, stage) for stage in self.stages}
        hashes = {str(path): checksum(path) for path in binaries.values()}
        with TemporaryDirectory(prefix="marketforge-pipeline-settings-") as directory:
            draft = Path(directory) / "pipeline.toml"
            draft.write_bytes(data)
            report = compose_pipeline(self.paths, Path(directory), binaries, source=draft)
            sources = {
                name: value for name, value in report["sources"].items() if name != str(draft)
            }
            return report["effective"], {**sources, **hashes}

    def describe(self, data: bytes, effective: dict[str, Any]) -> list[dict[str, Any]]:
        configured = parse_document(data)
        return [
            {
                **service.describe(field, configured.get(stage, {}), effective[stage]),
                "stage": stage,
                "path": f"{stage}.{field['path']}",
            }
            for stage, service in self.stages.items()
            for field in service.schema()["fields"]
            if field["mutable"]
        ]

    def fields(self, snapshot: Snapshot) -> list[dict[str, Any]]:
        effective, _ = self.effective(snapshot.data)
        return self.describe(snapshot.data, effective)

    def field(self, path: str) -> dict[str, Any]:
        stage, name = path.split(".", 1)
        return self.stages[stage].field(name)

    def patch(self, data: bytes, path: str, raw: str | None) -> bytes:
        stage, name = path.split(".", 1)
        return self.stages[stage].patch(data, name, raw, stage=stage)

    def propose(self, original: Snapshot, candidate: bytes) -> PipelineChange:
        after, inputs = self.effective(candidate)
        before, previous_inputs = self.effective(original.data)
        if inputs != previous_inputs or any(
            checksum(Path(path)) != digest for path, digest in inputs.items()
        ):
            raise MarketForgeError("Pipeline runtime inputs changed. Preview again.")
        changes: list[dict[str, Any]] = []
        for old, new in zip(
            self.describe(original.data, before), self.describe(candidate, after), strict=True
        ):
            path = new["path"]
            if any(
                old[key] != new[key]
                for key in ("configured", "configured_path", "effective", "active")
            ):
                changes.append(
                    {
                        "path": path,
                        "risk": new["risk"],
                        "before": old["effective"],
                        "after": new["effective"],
                        "configured_override": new["configured"],
                        "previous_override": old["configured"],
                        "previously_explicit": old["configured_path"] is not None,
                        "explicit": new["configured_path"] is not None,
                        "active_before": old["active"],
                        "active_after": new["active"],
                    }
                )
        return PipelineChange(original, candidate, changes, inputs)

    def apply(self, change: PipelineChange, *, acknowledge_risk: bool) -> None:
        if change.risk and not acknowledge_risk:
            raise MarketForgeError("Integrity-critical pipeline overrides require acknowledgement.")
        with (
            WorkspaceOwnership(self.paths),
            acquire_file_lock(self.paths.root / ".marketforge-engine.lock"),
        ):
            _, current = self.effective(change.candidate)
            if current != change.inputs:
                raise MarketForgeError("Pipeline runtime inputs changed. Preview again.")
            if change.candidate != change.original.data:
                replace_entry(self.path, change.original, change.candidate)
