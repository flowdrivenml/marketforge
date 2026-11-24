"""Read-only editor targets and staging for structured inspection."""

import hashlib
import json
import os
import stat
from collections.abc import Iterable
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import date, time
from decimal import Decimal
from pathlib import Path
from typing import Protocol, cast
from uuid import UUID

from textual.widgets import Button

from forge.shared.terminal.editor import EditorError
from forge.tui.clipboard import contains_credentials, sensitive_field

FILETYPES = {".json": "json", ".jsonl": "json", ".toml": "toml", ".log": ""}
LARGE_INSPECTION = 32 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class RetainedFile:
    label: str
    path: Path
    scope: Path
    sha256: str | None = None

    @classmethod
    def reference(cls, label: str, reference: object, scope: Path) -> "RetainedFile | None":
        if isinstance(reference, (str, Path)):
            path = Path(reference)
            if path.suffix.lower() in FILETYPES:
                return cls(label, path, scope)
        return None


@dataclass(frozen=True, slots=True)
class StructuredValue:
    value: object


type InspectionTarget = RetainedFile | StructuredValue


def structured(value: object) -> StructuredValue | None:
    return StructuredValue(cast(object, value)) if isinstance(value, (dict, list, tuple)) else None


class EditorHost(Protocol):
    def inspect_in_editor(self, target: InspectionTarget) -> object: ...


class OpenInspection(Button):
    """A dedicated mouse and keyboard action for a single immutable target."""

    def __init__(self, target: InspectionTarget, label: str = "Open JSON", *, id: str) -> None:
        super().__init__(label, classes="compact open-inspection", id=id)
        self.target = target
        self.tooltip = (
            f"Open read-only in Neovim: {target.path}"
            if isinstance(target, RetainedFile)
            else "Open read-only in Neovim"
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        event.stop()
        cast(EditorHost, self.app).inspect_in_editor(self.target)  # pyright: ignore[reportUnknownMemberType]


def typed_json(value: object) -> str:
    # Configuration serialization also represents exact Decimal values as strings.
    if isinstance(value, (Decimal, date, time, UUID, Path)):
        return str(value) if not isinstance(value, (date, time)) else value.isoformat()
    raise TypeError(f"Unsupported JSON inspection value: {type(value).__name__}.")


def check_json_value(value: object) -> None:
    """Reject lossy key conversions and values the inspector redacts."""
    if isinstance(value, dict):
        for key, item in cast(dict[object, object], value).items():
            if not isinstance(key, str):
                raise TypeError("JSON inspection requires string object keys.")
            if sensitive_field(key) and item is not None and item != "":
                raise EditorError("This structured value contains a protected field.")
            check_json_value(item)
    elif isinstance(value, (list, tuple)):
        for item in cast(Iterable[object], value):
            check_json_value(item)
    elif isinstance(value, str) and contains_credentials(value):
        raise EditorError("This structured value contains protected content.")


def stage(target: InspectionTarget, temporary: Path) -> tuple[Path, str, int]:
    """Write a private, non-authoritative snapshot and return its editor format."""
    if isinstance(target, StructuredValue):
        path = temporary / "inspection.json"
        check_json_value(target.value)
        with path.open("x", encoding="utf-8") as stream:
            json.dump(
                target.value,
                stream,
                indent=2,
                ensure_ascii=False,
                allow_nan=False,
                default=typed_json,
            )
            stream.write("\n")
        return path, "json", path.stat().st_size

    path = target.path
    scope = target.scope
    if (
        not path.is_absolute()
        or not scope.is_absolute()
        or ".." in path.parts
        or ".." in scope.parts
        or not path.is_relative_to(scope)
        or path == scope
    ):
        raise EditorError(f"Artifact is outside its authorized inspection scope: {path}")
    suffix = path.suffix.lower()
    if suffix not in FILETYPES:
        raise EditorError(f"Unsupported inspection file type: {path.name}")

    # Pin each directory before opening its child. No symlink substitution between
    # validation and copying can redirect the open outside the authorized scope.
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    destination = temporary / path.name
    with ExitStack() as stack:
        directory = os.open(path.anchor, directory_flags)
        stack.callback(os.close, directory)
        for part in path.parts[1:-1]:
            child = os.open(part, directory_flags, dir_fd=directory)
            stack.callback(os.close, child)
            directory = child
        descriptor = os.open(
            path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory
        )
        stack.callback(os.close, descriptor)
        original = os.fstat(descriptor)
        if not stat.S_ISREG(original.st_mode):
            raise EditorError(f"Expected a regular inspection file: {path}")
        with os.fdopen(descriptor, "rb", closefd=False) as source, destination.open("xb") as output:
            # A live log may grow while copying. Bound this read to the size at open.
            remaining = original.st_size
            while remaining and (chunk := source.read(min(remaining, 1024 * 1024))):
                output.write(chunk)
                remaining -= len(chunk)
        current = os.stat(path.name, dir_fd=directory, follow_symlinks=False)
        if (
            remaining
            or not os.path.samestat(original, current)
            or original.st_size != current.st_size
            or original.st_mtime_ns != current.st_mtime_ns
        ):
            raise EditorError(f"Artifact changed while it was being copied: {path}")
    if target.sha256 is not None:
        with destination.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != target.sha256:
                raise EditorError(f"Artifact fingerprint differs from retained evidence: {path}")
    return destination, FILETYPES[suffix], destination.stat().st_size
