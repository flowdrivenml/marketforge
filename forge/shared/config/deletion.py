"""Preview, archive or restore a single managed recipe under workspace ownership."""

import hashlib
import os
import stat
from collections.abc import Callable
from contextlib import ExitStack, nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from uuid import uuid4

from forge.shared.config.library import config_filename, resolve_config_file
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.paths import ProjectPaths

RecipeFamily = Literal[
    "acquisition",
    "normalization",
    "merge",
    "conversion",
    "pipeline",
    "import",
    "registration",
    "ranking",
]


@dataclass(frozen=True, slots=True)
class RecipeDependencies:
    references: tuple[Path, ...] = ()
    unreadable: tuple[Path, ...] = ()
    executions: tuple[Path, ...] = ()


@dataclass(frozen=True, slots=True)
class RecipeDeletion:
    family: RecipeFamily
    name: str
    path: Path
    dependencies: RecipeDependencies
    status: Literal["ready_to_delete", "blocked", "deleted"]
    sha256: str
    archived_path: Path | None = None


def _directory(stack: ExitStack, path: Path) -> int:
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    fd = os.open(path.anchor, flags)
    stack.callback(os.close, fd)
    for part in path.parts[1:]:
        fd = os.open(part, flags, dir_fd=fd)
        stack.callback(os.close, fd)
    return fd


def _identity(info: os.stat_result) -> tuple[int, int, int, int]:
    return info.st_dev, info.st_ino, info.st_mode, info.st_ctime_ns


def recipe_directory(paths: ProjectPaths, family: RecipeFamily) -> Path:
    if family == "ranking":
        return paths.config_dir / "markets/rankings"
    if family == "registration":
        return paths.workflow_dir("import") / "registration"
    return paths.workflow_dir(family)


def delete_recipe(
    paths: ProjectPaths,
    family: RecipeFamily,
    name: str,
    *,
    apply: bool = False,
    force: bool = False,
    expected_sha256: str | None = None,
    expected_path: Path | None = None,
    dependencies: Callable[[Path], RecipeDependencies],
) -> RecipeDeletion:
    if force and not apply:
        raise MarketForgeError("--force requires --apply.")
    filename = config_filename(name)
    directory = recipe_directory(paths, family)
    source = directory / filename
    if expected_path is not None and expected_path != source:
        raise MarketForgeError("The selected file is outside its managed recipe library.")
    missing = f"{family.title()} recipe {source.stem!r} was not found."
    with WorkspaceOwnership(paths) if apply else nullcontext(), ExitStack() as stack:
        try:
            fd = _directory(stack, directory)
            original = os.stat(filename, dir_fd=fd, follow_symlinks=False)
        except FileNotFoundError:
            resolve_config_file(directory, name)  # Keep migration diagnostics for legacy recipes.
            raise MarketForgeError(missing) from None
        if not stat.S_ISREG(original.st_mode) or original.st_nlink != 1:
            raise MarketForgeError(f"Expected a regular recipe file with one link: {source}")
        opened = os.open(filename, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW, dir_fd=fd)
        stack.callback(os.close, opened)
        if resolve_config_file(directory, name) != source or _identity(
            os.fstat(opened)
        ) != _identity(original):
            raise MarketForgeError(f"Recipe changed while it was being inspected: {source}")
        with os.fdopen(os.dup(opened), "rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        if expected_sha256 is not None and digest != expected_sha256:
            raise MarketForgeError(f"Recipe changed since review: {source}")
        evidence = dependencies(source)
        blocked = bool(evidence.executions or (evidence.references and not force))
        if not apply or blocked:
            return RecipeDeletion(
                family,
                source.stem,
                source,
                evidence,
                "blocked" if blocked else "ready_to_delete",
                digest,
            )

        # Reopen the configured location and compare it with the pinned directory before
        # checking the entry again. unlinkat removes the entry itself without following it.
        try:
            current_directory = _directory(stack, directory)
            unchanged = os.path.samestat(os.fstat(fd), os.fstat(current_directory)) and (
                _identity(os.stat(filename, dir_fd=fd, follow_symlinks=False))
                == _identity(original)
                == _identity(os.fstat(opened))
            )
        except FileNotFoundError:
            unchanged = False
        if not unchanged:
            raise MarketForgeError(f"Recipe changed while it was being inspected: {source}")
        archive = directory / ".archive"
        archive.mkdir(exist_ok=True)
        archive_fd = _directory(stack, archive)
        slot = uuid4().hex
        os.mkdir(slot, dir_fd=archive_fd)
        slot_fd = _directory(stack, archive / slot)
        os.rename(filename, filename, src_dir_fd=fd, dst_dir_fd=slot_fd)
        for descriptor in (slot_fd, archive_fd, fd):
            os.fsync(descriptor)
        return RecipeDeletion(
            family, source.stem, source, evidence, "deleted", digest, archive / slot / filename
        )


def archived_recipes(paths: ProjectPaths, family: RecipeFamily) -> tuple[Path, ...]:
    directory = recipe_directory(paths, family) / ".archive"
    return tuple(sorted(path for path in directory.glob("*/*.toml") if path.is_file()))


def restore_recipe(paths: ProjectPaths, family: RecipeFamily, archived: Path) -> Path:
    directory = recipe_directory(paths, family)
    archive = directory / ".archive"
    if archived.parent.parent != archive or not (
        len(archived.parent.name) == 32
        and all(char in "0123456789abcdef" for char in archived.parent.name)
    ):
        raise MarketForgeError("Expected a managed archived recipe.")
    filename = config_filename(archived.name)
    with WorkspaceOwnership(paths), ExitStack() as stack:
        source = _directory(stack, archived.parent)
        target = _directory(stack, directory)
        if not stat.S_ISREG(os.stat(filename, dir_fd=source, follow_symlinks=False).st_mode):
            raise MarketForgeError("Expected a regular archived recipe.")
        try:
            os.link(filename, filename, src_dir_fd=source, dst_dir_fd=target, follow_symlinks=False)
        except FileExistsError as error:
            existing = os.stat(filename, dir_fd=target, follow_symlinks=False)
            original = os.stat(filename, dir_fd=source, follow_symlinks=False)
            if not os.path.samestat(existing, original):
                raise MarketForgeError(f"An active recipe already exists: {filename}") from error
        os.fsync(target)
        os.unlink(filename, dir_fd=source)
        os.fsync(source)
    return directory / filename
