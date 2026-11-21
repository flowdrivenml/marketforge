"""Reviewed mutations of managed source recipes."""

from __future__ import annotations

import fcntl
import os
import stat
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import cast

import tomlkit
from tomlkit.items import AoT, Array, InlineTable, Table

from forge.historical.archives.config import AcquisitionConfig, AcquisitionInterval, ArchiveRequest
from forge.historical.archives.selection import request_key
from forge.historical.archives.sources.exchanges import validate_request
from forge.historical.archives.templates import ACQUISITION_TEMPLATE
from forge.historical.pipelines.config import PipelineRecipe
from forge.shared.config.library import resolve_config_file
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


@contextmanager
def recipe_writer(path: Path) -> Generator[None]:
    """Coordinate Forge writers and editor handoffs using a stable sidecar lock."""
    lock = path.with_name("." + path.name + ".forge.lock")
    with lock.open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise MarketForgeError(f"{path.name} is being edited by Forge.") from None
        yield


def create_pipeline(directory: Path, name: str) -> Path:
    return publish_new(
        resolve_config_file(directory, name),
        "# Pipeline draft. Review the interval and request before running.\n"
        + ACQUISITION_TEMPLATE,
    )


def publish_new(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".recipe-", dir=path.parent) as temporary:
        candidate = Path(temporary) / path.name
        with candidate.open("w", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(candidate, path)
        except FileExistsError:
            raise MarketForgeError(f"Recipe already exists: {path.name}") from None
    return path


def create_with_requests(
    directory: Path,
    name: str,
    requests: tuple[ArchiveRequest, ...],
    *,
    pipeline: bool,
    start: str,
    end: str,
) -> RecipeChange:
    interval = AcquisitionInterval.model_validate(
        {"start": datetime.fromisoformat(start), "end": datetime.fromisoformat(end)}
    )
    for request in requests:
        validate_request(request)
    payload = {
        "version": 1,
        "interval": interval.model_dump(),
        "requests": [request.model_dump(exclude_none=True) for request in requests],
    }
    model = PipelineRecipe if pipeline else AcquisitionConfig
    model.model_validate(payload)
    path = publish_new(resolve_config_file(directory, name), tomlkit.dumps(payload))
    return RecipeChange(path, len(requests), 0)


@dataclass(frozen=True)
class RecipeChange:
    path: Path
    added: int
    already_present: int


def add_requests(
    directory: Path,
    name: str,
    requests: tuple[ArchiveRequest, ...],
    *,
    pipeline: bool,
    expected: bytes,
) -> RecipeChange:
    path = resolve_config_file(directory, name)
    with recipe_writer(path):
        if path.read_bytes() != expected:
            raise MarketForgeError("Recipe changed since review. Reload it before applying.")
        model = PipelineRecipe if pipeline else AcquisitionConfig
        source = tomlkit.parse(expected.decode())
        existing = model.model_validate(source.unwrap())
        keys = {request_key(request) for request in existing.requests}
        additions: list[ArchiveRequest] = []
        for request in requests:
            validate_request(request)
            key = request_key(request)
            if key not in keys:
                keys.add(key)
                additions.append(request)
        if not additions:
            return RecipeChange(path, 0, len(requests))
        entries = source["requests"]
        if not isinstance(entries, (AoT, Array)):
            raise MarketForgeError("Recipe requests must be an array.")
        for request in additions:
            values = request.model_dump(exclude_none=True)
            if isinstance(entries, Array):
                item = tomlkit.inline_table()
                for key, value in values.items():
                    item[key] = value
            else:
                item = tomlkit.table()
                for key, value in values.items():
                    item[key] = value
            cast(list[object], entries).append(item)
        model.model_validate(source.unwrap())
        replace_reviewed(path, expected, tomlkit.dumps(source))
    return RecipeChange(path, len(additions), len(requests) - len(additions))


def replace_reviewed(path: Path, expected: bytes, text: str) -> None:
    mode = stat.S_IMODE(path.stat().st_mode)
    with tempfile.TemporaryDirectory(prefix=".recipe-", dir=path.parent) as temporary:
        candidate = Path(temporary) / path.name
        with candidate.open("w", encoding="utf-8") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        candidate.chmod(mode)
        if path.read_bytes() != expected:
            raise MarketForgeError(
                "Recipe changed during editing. External changes were preserved."
            )
        candidate.replace(path)


def replace_merge_source(paths: ProjectPaths, name: str, source: str, expected: bytes) -> Path:
    """Replace the one derived source while preserving its interval and options."""
    from forge.historical.merge.config import DerivedRecipe, normalization_source

    path = resolve_config_file(paths.config_dir / "historical/merge", name)
    with recipe_writer(path):
        if path.read_bytes() != expected:
            raise MarketForgeError("Merge recipe changed since review.")
        data = tomlkit.parse(expected.decode())
        current = DerivedRecipe.model_validate(data.unwrap())
        _, upstream, _ = normalization_source(paths, source)
        if current.selection.publication_ids:
            raise MarketForgeError("Choose a merge recipe without pinned publication IDs.")
        if (
            not upstream.interval.start
            <= current.interval.start
            < current.interval.end
            <= upstream.interval.end
        ):
            raise MarketForgeError("The replacement must cover the existing merge interval.")
        selection = data["selection"]
        if not isinstance(selection, (Table, InlineTable)):
            raise MarketForgeError("Merge recipe requires a selection table.")
        selection["source"] = source
        DerivedRecipe.model_validate(data.unwrap())
        if current.selection.source != source:
            replace_reviewed(path, expected, tomlkit.dumps(data))
    return path
