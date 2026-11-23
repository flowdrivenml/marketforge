"""Registry tombstones and their workspace-local history visibility mirror."""

import json
import os
from pathlib import Path
from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import TupleRow, dict_row

from forge.shared.database.connection import database_purpose, registry_id
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths
from forge.storage.cleanup_plans import write_document


def cleanup_directory(paths: ProjectPaths) -> Path:
    parent = paths.data_root / ".jobs"
    directory = parent / "run-cleanup"
    if any(path.is_symlink() for path in (paths.data_root, parent, directory)):
        raise MarketForgeError("Run cleanup records require a non-symlink managed directory.")
    return directory


def hidden_names(runs_root: Path) -> set[str]:
    directory = runs_root.parent / ".jobs" / "run-cleanup" / "visibility"
    if any(path.is_symlink() for path in (directory, *list(directory.parents)[:2])):
        return set()
    if not directory.is_dir():
        return set()
    names: set[str] = set()
    with os.scandir(directory) as entries:
        for item in entries:
            if not item.name.endswith(".json") or not item.is_file(follow_symlinks=False):
                continue
            try:
                with open(item.path, "rb") as source:
                    data = source.read(256 * 1024 + 1)
                if len(data) > 256 * 1024:
                    continue
                document = json.loads(data)
                if document["runs_root"] == str(runs_root):
                    names.update(name for name in document["names"] if isinstance(name, str))
            except (OSError, KeyError, TypeError, ValueError):
                continue
    return names


def mirror(paths: ProjectPaths, plan_id: str, roots: list[Path]) -> None:
    by_root: dict[Path, set[str]] = {}
    for root in roots:
        by_root.setdefault(root.parent, set()).add(root.name)
    for root, names in by_root.items():
        directory = cleanup_directory(ProjectPaths(paths.root, root.parent)) / "visibility"
        write_document(
            directory / f"{plan_id}.json",
            {"runs_root": str(root), "names": sorted(names)},
            replace=True,
        )


async def marker(
    connection: AsyncConnection[TupleRow],
    reference: str,
    *,
    include_alias: bool = False,
    runs_root: Path | None = None,
) -> dict[str, Any] | None:
    registry = await registry_id(connection)
    alias = str(runs_root / reference) if include_alias and runs_root is not None else None
    async with connection.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            """SELECT run_id,operation,mode,state,original_status,receipt_path,parent_run_id
               FROM historical.execution_run_deletions
               WHERE database_id=%s AND database_purpose=%s
                 AND (run_id=%s OR %s=ANY(artifact_roots))
               ORDER BY (run_id=%s) DESC, created_at DESC LIMIT 1""",
            (registry, database_purpose(), reference, alias, reference),
        )
        return await cursor.fetchone()
