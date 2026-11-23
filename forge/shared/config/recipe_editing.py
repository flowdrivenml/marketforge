"""Validate and replace a recipe after editing a private draft."""

import os
import shutil
from collections.abc import Callable
from pathlib import Path
from tempfile import mkdtemp

from forge.historical.engine.runtime_config import read_snapshot, replace_entry
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
