"""Typed acquisition progress, independent of terminal rendering and scheduling."""

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Literal

RequestOutcome = Literal["successful", "empty", "unsupported", "failed", "cancelled"]
FileOutcome = Literal["completed", "reused", "failed", "cancelled"]


@dataclass(frozen=True, slots=True)
class ProgressEvent:
    kind: Literal[
        "request_started",
        "activity",
        "request_finished",
        "queued",
        "files_discovered",
        "file_active",
        "file_finished",
        "total_final",
        "finished",
    ]
    request_id: int = 0
    identity: str = ""
    activity: str = ""
    request_outcome: RequestOutcome | None = None
    file_outcome: FileOutcome | None = None
    cached: bool = False
    archives: int = 0
    file_id: int = 0
    error: str = ""


@dataclass(frozen=True, slots=True)
class RequestCounts:
    finished: int = 0
    successful: int = 0
    cached: int = 0
    empty: int = 0
    unsupported: int = 0
    failed: int = 0
    cancelled: int = 0
    archives: int = 0


@dataclass(slots=True)
class FileCounts:
    queued: int = 0
    completed: int = 0
    reused: int = 0
    failed: int = 0
    cancelled: int = 0

    @property
    def resolved(self) -> int:
        return self.completed + self.reused + self.failed + self.cancelled


@dataclass(frozen=True, slots=True)
class ProgressSnapshot:
    requests: RequestCounts
    files: FileCounts
    total_requests: int
    provisional: bool
    current: str
    activity: str
    active: tuple[tuple[int, str, str], ...]
    recent: tuple[str, ...]
    failures: tuple[str, ...]
    finished: bool
    transition: str
    report: str = ""
    known_total: int = 0
    active_requests: tuple[tuple[int, str, str], ...] = ()


@dataclass(slots=True)
class ProgressState:
    total_requests: int
    requests: RequestCounts = field(default_factory=RequestCounts)
    files: FileCounts = field(default_factory=FileCounts)
    provisional: bool = True
    current: str = ""
    activity: str = "Preparing operation"
    finished: bool = False
    report: str = ""
    known_total: int = 0
    active_requests: dict[int, tuple[str, str]] = field(
        default_factory=lambda: dict[int, tuple[str, str]]()
    )
    active: dict[int, tuple[str, str]] = field(default_factory=lambda: dict[int, tuple[str, str]]())
    recent: deque[str] = field(default_factory=lambda: deque(maxlen=6))
    failures: deque[str] = field(default_factory=lambda: deque(maxlen=4))

    def apply(self, event: ProgressEvent) -> ProgressSnapshot:
        transition = ""
        if event.kind == "request_started":
            self.current = event.identity
            self.activity = "Checking discovery cache"
            transition = "Starting " + event.identity
            self.active_requests[event.request_id] = (event.identity, self.activity)
        elif event.kind == "activity":
            self.activity = event.activity
            if event.request_id in self.active_requests:
                identity, _ = self.active_requests[event.request_id]
                self.active_requests[event.request_id] = (identity, event.activity)
        elif event.kind == "request_finished" and event.request_outcome is not None:
            self.active_requests.pop(event.request_id, None)
            counts = self.requests
            changes = {event.request_outcome: getattr(counts, event.request_outcome) + 1}
            successful = event.request_outcome in {"successful", "empty"}
            if event.request_outcome == "empty":
                changes["successful"] = counts.successful + 1
            self.requests = replace(
                counts,
                **changes,
                finished=counts.finished + 1,
                cached=counts.cached + int(event.cached and successful),
                archives=counts.archives + event.archives,
            )
            transition = f"{event.request_outcome}: {event.identity} · {event.archives} archives"
            if event.cached:
                transition += " (cached)"
            if event.error:
                transition += f" · {event.error}"
            self.recent.append(transition)
            if event.request_outcome in {"failed", "unsupported"}:
                self.failures.append(transition)
        elif event.kind == "files_discovered":
            self.known_total += event.archives
        elif event.kind == "queued":
            self.files.queued += 1
        elif event.kind == "file_active":
            self.active[event.file_id] = (event.identity, event.activity)
        elif event.kind == "file_finished" and event.file_outcome is not None:
            setattr(self.files, event.file_outcome, getattr(self.files, event.file_outcome) + 1)
            self.active.pop(event.file_id, None)
            transition = f"{event.file_outcome}: {event.identity}"
            if event.error:
                transition += f" · {event.error}"
            self.recent.append(transition)
            if event.file_outcome == "failed":
                self.failures.append(transition)
        elif event.kind == "total_final":
            self.provisional = False
        elif event.kind == "finished":
            self.finished = True
            self.report = event.identity
            self.active.clear()
            self.active_requests.clear()
        return ProgressSnapshot(
            self.requests,
            replace(self.files),
            self.total_requests,
            self.provisional,
            self.current,
            self.activity,
            tuple((key, *value) for key, value in self.active.items()),
            tuple(self.recent),
            tuple(self.failures),
            self.finished,
            transition,
            self.report,
            self.known_total,
            tuple((key, *value) for key, value in self.active_requests.items()),
        )


ProgressObserver = Callable[[ProgressSnapshot], None]
ActivityObserver = Callable[[str], None]
