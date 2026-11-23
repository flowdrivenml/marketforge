"""Preview or unlink a single managed recipe under workspace ownership."""

import os
import stat
from collections.abc import Callable
from contextlib import ExitStack, nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from forge.shared.config.library import config_filename, resolve_config_file
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.paths import ProjectPaths

RecipeFamily = Literal["acquisition", "normalization", "merge", "conversion"]


@dataclass(frozen=True, slots=True)
class RecipeDependencies:
    references: tuple[Path, ...] = ()
    unreadable: tuple[Path, ...] = ()


@dataclass(frozen=True, slots=True)
class RecipeDeletion:
    family: RecipeFamily
    name: str
    path: Path
    dependencies: RecipeDependencies
    status: Literal["ready_to_delete", "blocked", "deleted"]


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


def delete_recipe(
    paths: ProjectPaths,
    family: RecipeFamily,
    name: str,
    *,
    apply: bool = False,
    force: bool = False,
    dependencies: Callable[[Path], RecipeDependencies],
) -> RecipeDeletion:
    if force and not apply:
        raise MarketForgeError("--force requires --apply.")
    filename = config_filename(name)
    directory = paths.workflow_dir(family)
    source = directory / filename
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
        evidence = dependencies(source)
        blocked = bool(evidence.references) and not force
        if not apply or blocked:
            return RecipeDeletion(
                family, source.stem, source, evidence, "blocked" if blocked else "ready_to_delete"
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
        os.unlink(filename, dir_fd=fd)
        return RecipeDeletion(family, source.stem, source, evidence, "deleted")
