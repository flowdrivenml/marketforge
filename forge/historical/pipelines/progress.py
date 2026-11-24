"""Read-only pipeline presentation state."""

from dataclasses import dataclass
from datetime import datetime

from forge.historical.archives.progress import ProgressSnapshot


@dataclass(frozen=True, slots=True)
class StageSnapshot:
    name: str
    state: str
    phase: str
    started_at: str | None
    finished_at: str | None
    summary: str
    verified_jobs: tuple[int, int] | None
    terminal: str | None = None


@dataclass(frozen=True, slots=True)
class Activity:
    stage: str
    phase: str
    state: str


@dataclass(frozen=True, slots=True)
class PipelineSnapshot:
    name: str
    run_id: str
    environment: str
    interval: tuple[datetime, datetime]
    state: str
    started_at: str
    finished_at: str | None
    stages: tuple[StageSnapshot, ...]
    active_stage: str | None
    recent: tuple[Activity, ...]
    acquisition: ProgressSnapshot | None
    error: str | None
