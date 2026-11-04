"""Locations of durable execution artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Self

from forge.shared.identity import RunId
from forge.shared.paths import ProjectPaths


@dataclass(frozen=True, slots=True)
class RunArtifacts:
    """Paths for one run; constructing these creates no directories."""

    root: Path

    @classmethod
    def for_run(cls, paths: ProjectPaths, run_id: RunId) -> Self:
        """Resolve artifact locations for an allocated or restored identity."""
        return cls(root=paths.runs_dir / str(run_id))

    @property
    def submitted_config(self) -> Path:
        return self.root / "submitted.toml"

    @property
    def resolved_plan(self) -> Path:
        return self.root / "resolved-plan.json"

    @property
    def report(self) -> Path:
        return self.root / "report.json"

    @property
    def events(self) -> Path:
        return self.root / "events.jsonl"

    @property
    def steps_dir(self) -> Path:
        return self.root / "steps"
