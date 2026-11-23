"""Durable claims and versioned identities for resolved historical work."""

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable, Iterable, Mapping
from pathlib import Path
from typing import Any

from psycopg.types.json import Jsonb

from forge.shared.database.connection import (
    database_connection,
    database_dsn,
    is_testing,
    pinned_registry,
)
from forge.shared.errors import MarketForgeError

VERSION = 1
READY_MESSAGE = "Already ready — reusing verified datasets."


def output_present(path: Path) -> bool:
    return path.exists() or path.is_symlink()


def excluding(value: Mapping[str, Any], names: set[str]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if key not in names}


def identity(operation: str, registry: str, executable: str, evidence: Mapping[str, Any]) -> str:
    payload = json.dumps(
        [VERSION, operation, registry, executable, evidence],
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def normalize_key(
    registry: str,
    executable: str,
    batch: dict[str, Any],
    job: dict[str, Any],
    raw: dict[str, str],
    runtime: dict[str, Any],
    profiles: list[dict[str, Any]],
) -> str:
    return identity(
        "normalize",
        registry,
        executable,
        {
            # Rust validates the request interval and uses the resolved task order and
            # ranks. It processes the selected inputs without an interval row filter.
            "batch": excluding(
                batch,
                {
                    "jobs",
                    "run_id",
                    "run_directory",
                    "batch_path",
                    "requested_interval",
                    "ranking_snapshot",
                    "ranking_policy",
                },
            ),
            "job": {
                **excluding(job, {"job_id", "dataset_id", "output"}),
                "output": excluding(job["output"], {"dataset_path", "staging_path"}),
            },
            "raw": raw,
            "runtime": excluding(runtime, {"diagnostics"}),
            "profiles": profiles,
        },
    )


def merge_key(
    registry: str,
    executable: str,
    job: dict[str, Any],
    runtime: dict[str, Any],
    inputs: list[dict[str, Any]],
) -> str:
    return identity(
        "merge",
        registry,
        executable,
        {
            "job": {
                **excluding(job, {"job_id", "output"}),
                "output": excluding(job["output"], {"dataset_path"}),
            },
            "runtime": runtime,
            "verified_inputs": inputs,
        },
    )


def convert_key(registry: str, executable: str, job: dict[str, Any]) -> str:
    return identity(
        "convert",
        registry,
        executable,
        {
            **excluding(job, {"job_id", "output"}),
            "output": excluding(job["output"], {"dataset_path"}),
        },
    )


class WorkIndex:
    """Rows with no result remain owned after a crash until proven recoverable."""

    def __init__(self, root: Path, registry: str, operation: str, owner: str) -> None:
        self.root = root
        self.registry = registry
        self.operation = operation
        self.owner = owner

    async def peek(
        self, keys: Iterable[str]
    ) -> dict[str, tuple[str, dict[str, Any], dict[str, Any] | None]]:
        with pinned_registry(self.registry):
            async with database_connection(database_dsn(self.root)) as connection:
                cursor = await connection.execute(
                    """SELECT work_key,owner,evidence,result FROM historical.resolved_work
                       WHERE registry_id=%s AND operation=%s AND work_key=ANY(%s)""",
                    (self.registry, self.operation, list(keys)),
                )
                return {
                    key: (owner, evidence, result)
                    for key, owner, evidence, result in await cursor.fetchall()
                }

    async def acquire(
        self, candidates: Mapping[str, dict[str, Any]]
    ) -> dict[str, tuple[str, dict[str, Any], dict[str, Any] | None]]:
        existing: dict[str, tuple[str, dict[str, Any], dict[str, Any] | None]] = {}
        with pinned_registry(self.registry):
            async with database_connection(database_dsn(self.root)) as connection:
                async with connection.transaction():
                    for key, evidence in sorted(candidates.items()):
                        inserted = await connection.execute(
                            """
                            INSERT INTO historical.resolved_work
                                (registry_id, operation, work_key, owner, evidence)
                            VALUES (%s,%s,%s,%s,%s)
                            ON CONFLICT DO NOTHING RETURNING owner
                            """,
                            (self.registry, self.operation, key, self.owner, Jsonb(evidence)),
                        )
                        if await inserted.fetchone() is not None:
                            if is_testing() and (path := evidence.get("path")):
                                retired = await connection.execute(
                                    """SELECT 1 FROM historical.publication_supersessions s
                                       JOIN historical.dataset_publications p ON p.id=s.publication_id
                                       WHERE s.canonical_id IS NULL AND p.storage_root=%s
                                       UNION ALL
                                       SELECT 1 FROM historical.converted_output_deletions
                                       WHERE registry_id=%s AND (
                                         evidence#>>'{target,path}'=%s OR EXISTS (
                                           SELECT 1 FROM jsonb_array_elements(
                                             COALESCE(evidence->'prior_deletions','[]'::jsonb)) prior
                                           WHERE prior#>>'{evidence,target,path}'=%s
                                         )
                                       ) LIMIT 1""",
                                    (path, self.registry, path, path),
                                )
                                if await retired.fetchone():
                                    raise MarketForgeError(
                                        f"Output path was explicitly retired: {path}. Prepare a new job."
                                    )
                            continue
                        cursor = await connection.execute(
                            """SELECT owner,evidence,result FROM historical.resolved_work
                               WHERE registry_id=%s AND operation=%s AND work_key=%s FOR UPDATE""",
                            (self.registry, self.operation, key),
                        )
                        row = await cursor.fetchone()
                        if row is None:
                            raise MarketForgeError("Equivalent work changed concurrently. Retry.")
                        owner, original, result = row
                        if result is None:
                            raise MarketForgeError(
                                f"Equivalent {self.operation} work is active or requires recovery. "
                                f"Inspect attempt {owner} and {original['report_path']}."
                            )
                        existing[key] = (owner, original, result)
        return existing

    async def recover_committed(
        self, candidates: Iterable[str], recover: Callable[[str, str, Path], Awaitable[None]]
    ) -> None:
        """Only resume from a retained intent and a visible committed destination."""
        for key, (owner, evidence, result) in (await self.peek(candidates)).items():
            if result is not None:
                continue
            intent = Path(evidence["report_path"]).with_name("execution-intent.json")
            if await asyncio.to_thread(intent.is_file) and await asyncio.to_thread(
                Path(evidence["path"]).is_dir
            ):
                await recover(key, owner, intent)

    async def complete(self, key: str, result: dict[str, Any], *, recovery: bool = False) -> None:
        with pinned_registry(self.registry):
            async with database_connection(database_dsn(self.root)) as connection:
                cursor = await connection.execute(
                    """UPDATE historical.resolved_work SET result=%s
                       WHERE registry_id=%s AND operation=%s AND work_key=%s
                         AND owner=%s AND result IS NULL""",
                    (Jsonb(result), self.registry, self.operation, key, self.owner),
                )
                if cursor.rowcount != 1 and not recovery:
                    raise MarketForgeError("Resolved work ownership changed before completion.")

    async def release(self, keys: Iterable[str]) -> None:
        """Release only work whose supervisor has stopped and cannot commit an output."""
        keys = list(keys)
        if not keys:
            return
        with pinned_registry(self.registry):
            async with database_connection(database_dsn(self.root)) as connection:
                await connection.execute(
                    """DELETE FROM historical.resolved_work
                       WHERE registry_id=%s AND operation=%s AND work_key=ANY(%s)
                         AND owner=%s AND result IS NULL""",
                    (self.registry, self.operation, keys, self.owner),
                )

    async def verify_registered(
        self, result: dict[str, Any], path: str, inventory: Mapping[str, str]
    ) -> None:
        publication = result["publications"][path]
        expected = publication["files"]
        with pinned_registry(self.registry):
            async with database_connection(database_dsn(self.root)) as connection:
                cursor = await connection.execute(
                    """SELECT d.storage_path,d.id,d.primary_file,d.file_sha256
                       FROM historical.datasets d
                       JOIN historical.dataset_publications p ON p.id=d.publication_id
                       WHERE p.id=%s AND p.storage_root=%s AND p.operation=%s
                         AND d.status='complete' AND d.primary_file IS NOT NULL""",
                    (publication["publication_id"], path, self.operation),
                )
                rows = await cursor.fetchall()
                if {location: file_id for location, file_id, _, _ in rows} != expected or any(
                    not digest or inventory.get(name) != digest for _, _, name, digest in rows
                ):
                    raise MarketForgeError("Canonical publication registration has changed.")
