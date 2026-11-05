"""Durable, reviewable cleanup plans and filesystem/registry recovery receipts."""

import hashlib
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from forge.historical.archives.storage.files import publish
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths
from forge.storage.managed_files import FileSnapshot


class Target(BaseModel):
    model_config = ConfigDict(extra="forbid")
    local_id: int = Field(gt=0)
    record: dict[str, object]
    record_digest: str
    relative_path: str
    partial_relative_path: str | None = None
    path: str | None = None
    final_file: FileSnapshot | None = None
    partial_file: FileSnapshot | None = None
    state: Literal["eligible", "blocked", "absent"]
    blockers: list[str] = Field(default_factory=lambda: list[str]())
    action: str
    actual_bytes: int | None = None
    reclaimable_bytes: int = 0


class CleanupPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1] = 1
    plan_id: str = Field(pattern=r"^[A-Za-z0-9_-]{1,80}$")
    operation: Literal["prune", "delete"]
    project_root: str
    data_root: str
    raw_root: str
    created_at: str
    filters: dict[str, object]
    overlap: bool
    selected_count: int
    review_complete: bool
    targets: list[Target]
    digest: str = ""

    @model_validator(mode="after")
    def consistent_targets(self) -> "CleanupPlan":
        if self.selected_count != len(self.targets) or len(
            {target.local_id for target in self.targets}
        ) != len(self.targets):
            raise ValueError("Preview target count/identifiers are inconsistent.")
        for target in self.targets:
            if (
                target.local_id != target.record.get("id")
                or target.relative_path != target.record.get("relative_path")
                or target.partial_relative_path != target.relative_path + ".part"
            ):
                raise ValueError("Preview path/identity differs from its frozen record.")
            if target.record_digest != digest(target.record):
                raise ValueError("Preview record digest is inconsistent.")
        return self


def canonical(value: object) -> str:
    def encode(item: object) -> str:
        if isinstance(item, datetime):
            return item.astimezone(UTC).isoformat()
        return str(item)

    return json.dumps(
        value,
        default=encode,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def plan_directory(paths: ProjectPaths) -> Path:
    directory = paths.data_root
    for part in (".jobs", "storage"):
        directory = directory / part
        if directory.is_symlink():
            raise MarketForgeError("Storage preview directory must not be a symlink.")
    return directory


def write_document(path: Path, value: object, *, replace: bool = False) -> None:
    # Only private plan/receipt paths call this, never archive or external paths.
    for parent in (path.parent.parent, path.parent):
        if parent.is_symlink():
            raise MarketForgeError("Storage preview directory must not be a symlink.")
        parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise MarketForgeError("Storage preview/receipt must not be a symlink.")
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, prefix=".cleanup-", delete=False
        ) as file:
            temporary = Path(file.name)
            file.write(canonical(value) + "\n")
            file.flush()
            os.fsync(file.fileno())
        if replace:
            os.replace(temporary, path)
            descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        else:
            publish(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def save_plan(paths: ProjectPaths, plan: CleanupPlan) -> Path:
    plan.digest = digest(plan.model_dump(exclude={"digest"}))
    destination = plan_directory(paths) / f"{plan.operation}-{plan.plan_id}.json"
    write_document(destination, plan.model_dump())
    return destination


def new_plan_id() -> str:
    return uuid4().hex


def load_plan(paths: ProjectPaths, path: Path, operation: str) -> tuple[Path, CleanupPlan]:
    directory = plan_directory(paths)
    source = path.expanduser().absolute()
    if (
        source.parent != directory
        or source.is_symlink()
        or not source.name.startswith(operation + "-")
    ):
        raise MarketForgeError("Use the exact managed preview path printed by this operation.")
    plan = CleanupPlan.model_validate_json(source.read_bytes())
    if (plan.operation, plan.project_root, plan.data_root, plan.raw_root) != (
        operation,
        str(paths.root),
        str(paths.data_root),
        str(paths.raw_dir),
    ):
        raise MarketForgeError("Preview belongs to another operation or configured storage root.")
    if source.name != f"{plan.operation}-{plan.plan_id}.json" or plan.digest != digest(
        plan.model_dump(exclude={"digest"})
    ):
        raise MarketForgeError("Preview content changed; create and review a new preview.")
    if not plan.review_complete:
        raise MarketForgeError(
            "Preview hid targets behind pagination. Run the preview again with --all or --json before applying."
        )
    return source, plan
