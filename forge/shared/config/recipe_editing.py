"""Validate and replace a recipe after editing a private draft."""

import os
import shutil
from collections.abc import Callable, Mapping
from pathlib import Path
from tempfile import mkdtemp
from typing import cast

from forge.historical.engine.runtime_config import read_snapshot, replace_entry
from forge.shared.config.toml import append_entry, configuration_item, inline_field
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.paths import ProjectPaths
from forge.shared.terminal.editor import edit_file


def edit_recipe(
    paths: ProjectPaths, source: Path, editor: str | None, validate: Callable[[Path], object]
) -> None:
    # External editors never hold workspace ownership. Validate the draft before replacing.
    source = Path(os.path.abspath(source))
    original = read_snapshot(source)
    directory = Path(mkdtemp(prefix="marketforge-recipe-")).resolve()
    draft = directory / source.name
    try:
        draft.write_bytes(original.data)
        edit_file(draft, editor=editor, fallback="nvim")
        candidate = read_snapshot(draft)
        validate(draft)
        if candidate.data != original.data:
            with WorkspaceOwnership(paths):
                replace_entry(source, original, candidate.data)
    except (MarketForgeError, OSError) as error:
        raise MarketForgeError(f"{error}\nRetained draft: {draft}") from error
    else:
        shutil.rmtree(directory)


def update_recipe(
    original: bytes, before: Mapping[str, object], after: Mapping[str, object]
) -> str:
    """Patch only changed values in the existing TOML document."""
    from collections.abc import MutableMapping, MutableSequence

    import tomlkit
    from tomlkit.items import AoT, Array, InlineTable

    def retained_indices(old: list[object], new: list[object]) -> list[int] | None:
        indices: list[int] = []
        for index, item in enumerate(old):
            if len(indices) < len(new) and item == new[len(indices)]:
                indices.append(index)
        return indices if len(indices) == len(new) else None

    def patch_array(
        array: AoT | Array, old: list[object], new: list[object], path: tuple[str, ...]
    ) -> bool:
        entries = cast(MutableSequence[object], array)
        if isinstance(array, AoT) and (
            not new or any(not isinstance(item, Mapping) for item in new)
        ):
            return False
        if (indices := retained_indices(old, new)) is not None:
            for index in reversed(range(len(entries))):
                if index not in indices:
                    del entries[index]
            return True
        if len(new) < len(old):
            return False
        # Form entries are edited in place, with new entries added at the end.
        # Keep existing table and array nodes so their comments survive edits.
        for index, (previous, item) in enumerate(zip(old, new, strict=False)):
            if previous == item:
                continue
            existing = entries[index]
            if (
                isinstance(existing, MutableMapping)
                and isinstance(previous, dict)
                and isinstance(item, dict)
            ):
                patch(
                    cast(MutableMapping[str, object], existing),
                    cast(dict[str, object], previous),
                    cast(dict[str, object], item),
                    path,
                )
            else:
                entries[index] = configuration_item(item, path, inline=True)
        for item in new[len(old) :]:
            append_entry(array, item, path)
        return True

    def patch(
        table: MutableMapping[str, object],
        old: Mapping[str, object],
        new: Mapping[str, object],
        path: tuple[str, ...] = (),
    ) -> None:
        inline_parent = isinstance(table, InlineTable)
        for key in old:
            if new.get(key) is None:
                del table[key]
        for key, item in new.items():
            previous = old.get(key)
            if item is None or previous == item:
                continue
            current = table.get(key)
            location = (*path, key)
            if (
                isinstance(current, MutableMapping)
                and isinstance(previous, dict)
                and isinstance(item, dict)
            ):
                patch(
                    cast(MutableMapping[str, object], current),
                    cast(dict[str, object], previous),
                    cast(dict[str, object], item),
                    location,
                )
            elif not (
                isinstance(current, (AoT, Array))
                and isinstance(previous, list)
                and isinstance(item, list)
                and patch_array(
                    current, cast(list[object], previous), cast(list[object], item), location
                )
            ):
                table[key] = configuration_item(
                    cast(object, item), location, inline=inline_parent or inline_field(location)
                )

    source = tomlkit.parse(original.decode())
    patch(cast(MutableMapping[str, object], source), before, after)
    return source.as_string()
