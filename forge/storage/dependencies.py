"""Concrete registry and managed prepared-job references; no completeness gate."""

import json
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast
from urllib.parse import unquote, urlsplit

from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.shared.paths import ProjectPaths

DEPENDENCY_CHECKS = (
    "Deletion checks registered dataset lineage, readable managed normalization/process jobs, "
    "saved normalization input selections, and managed operation ownership. "
    "Unregistered standalone readers or jobs elsewhere are not a blanket blocker."
)


@dataclass
class PreparedReferences:
    paths: dict[str, list[str]] = field(default_factory=lambda: dict[str, list[str]]())
    local_ids: dict[int, list[str]] = field(default_factory=lambda: dict[int, list[str]]())


def prepared_references(paths: ProjectPaths) -> PreparedReferences:
    result = PreparedReferences()
    directories = [paths.normalization_jobs_dir, paths.data_root / ".jobs" / "process"]
    directories.extend(paths.runs_dir.glob("*/jobs/normalize"))
    sources = [source for directory in directories for source in directory.glob("*.json")]
    sources.extend(paths.runs_dir.glob("*/normalize-batch.json"))
    for source in sorted(sources):
        try:
            document = cast(object, json.loads(source.read_text(encoding="utf-8")))
        except (OSError, UnicodeError, ValueError):
            continue  # An unreadable/invalid document is not a concrete reference.
        if not isinstance(document, dict):
            continue
        root = cast(dict[str, object], document)
        documents = [root]
        if root.get("kind") == "normalization_batch" and isinstance(root.get("jobs"), list):
            documents.extend(
                cast(dict[str, object], job)
                for job in cast(list[object], root["jobs"])
                if isinstance(job, dict)
            )
        for job in documents:
            tasks = job.get("tasks")
            if not isinstance(tasks, list):
                continue
            for task in cast(list[object], tasks):
                if not isinstance(task, dict):
                    continue
                input_path = cast(dict[str, object], task).get("input_path")
                if isinstance(input_path, str) and input_path:
                    resolved = (paths.root / input_path).resolve()
                    result.paths.setdefault(str(resolved), []).append(
                        f"Prepared job {source} references this file."
                    )
    for source in sorted(paths.normalization_config_dir.glob("*.toml")):
        try:
            with source.open("rb") as stream:
                document = cast(dict[str, object], tomllib.load(stream))
        except (OSError, UnicodeError, ValueError):
            continue
        inputs = document.get("inputs")
        if not isinstance(inputs, list):
            continue
        for selection in cast(list[object], inputs):
            if isinstance(selection, dict):
                identity = cast(dict[str, object], selection).get("archive_id")
                if type(identity) is int and identity > 0:
                    result.local_ids.setdefault(identity, []).append(
                        f"Preparation request {source} selects local:{identity}."
                    )
    return result


async def blockers(
    c: AsyncConnection[TupleRow],
    paths: ProjectPaths,
    row: dict[str, object],
    references: PreparedReferences,
) -> list[str]:
    identity = int(str(row["id"]))
    cursor = await c.execute(
        "SELECT dataset_id FROM historical.dataset_raw_inputs WHERE local_archive_id=%s ORDER BY dataset_id",
        (identity,),
    )
    found = [
        f"Registered dataset {item[0]} depends on local:{identity}."
        for item in await cursor.fetchall()
    ]
    found.extend(references.local_ids.get(identity, []))
    for relative in (str(row["relative_path"]), str(row["relative_path"]) + ".part"):
        found.extend(references.paths.get(str((paths.raw_dir / relative).resolve()), []))
    return sorted(set(found))


def is_external_original(paths: ProjectPaths, row: dict[str, object]) -> bool:
    root = row.get("external_source_root")
    key = row.get("external_object")
    if not isinstance(root, str) or not isinstance(key, str):
        return False
    parsed = urlsplit(root)
    if parsed.scheme not in {"", "file"} or (
        parsed.scheme == "file" and parsed.netloc not in {"", "localhost"}
    ):
        return False
    source_root = Path(unquote(parsed.path)) if parsed.scheme == "file" else Path(root)
    return (source_root / key).resolve() == (paths.raw_dir / str(row["relative_path"])).resolve()
