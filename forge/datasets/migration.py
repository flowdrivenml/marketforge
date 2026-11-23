"""Migrate registration metadata without altering publication artifacts."""

from pathlib import Path
from typing import Any

from psycopg.rows import dict_row

from forge.datasets.registration import file_summaries, register_files
from forge.historical.archives.storage.acquire import root_key
from forge.historical.archives.storage.files import disk
from forge.historical.normalization.datasets import verify
from forge.shared.database.connection import database_connection, database_dsn
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


async def migrate(paths: ProjectPaths, *, dry_run: bool) -> dict[str, Any]:
    plans = []
    async with database_connection(database_dsn(paths.root)) as c:
        async with c.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                """SELECT d.id,d.storage_path,d.operation,d.operation_details,m.publication_id
                FROM historical.datasets d LEFT JOIN historical.dataset_registration_migrations m
                  ON m.legacy_dataset_id=d.id
                WHERE d.status='complete' AND d.publication_id IS NULL
                  AND d.operation IN ('normalize','merge') ORDER BY d.id"""
            )
            aggregates = await cursor.fetchall()
        for item in aggregates:
            ident = item["id"]
            plan: dict[str, Any] = {"legacy_dataset_id": ident, "path": item["storage_path"]}
            plans.append(plan)
            try:
                if item["operation"] != "normalize":
                    raise MarketForgeError(
                        "Legacy merge migration requires resolvable historical merge input evidence."
                    )
                evidence = await disk(verify, paths, Path(item["storage_path"]))
                details = item["operation_details"]
                for key in (
                    "manifest_sha256",
                    "processing_job_sha256",
                    "processing_runtime_sha256",
                    "engine_dataset_id",
                    "instrument_namespace",
                ):
                    if details.get(key) != evidence[key]:
                        raise MarketForgeError(
                            f"Legacy dataset {ident} disagrees with freshly verified {key}."
                        )
                for name, digest in details.get("inventory_sha256", {}).items():
                    if evidence["inventory_sha256"].get(name) != digest:
                        raise MarketForgeError(
                            f"Legacy dataset {ident} has changed inventory: {name}."
                        )
                cur = await c.execute(
                    """SELECT r.local_archive_id,r.input_sha256,l.storage_root_key,l.relative_path
                    FROM historical.dataset_raw_inputs r JOIN historical.local_archives l
                    ON l.id=r.local_archive_id WHERE r.dataset_id=%s""",
                    (ident,),
                )
                links = await cur.fetchall()
                raw = {(row[0], row[1]) for row in links}
                evidence["raw_links_by_path"] = {
                    str(paths.raw_dir / relative): (archive_id, digest)
                    for archive_id, digest, key, relative in links
                    if key == root_key(paths.raw_dir)
                }
                evidence["raw_sha256"] = {
                    path: link[1] for path, link in evidence["raw_links_by_path"].items()
                }
                cur = await c.execute(
                    """SELECT d.primary_file,d.id FROM historical.datasets d
                    JOIN historical.dataset_publications p ON p.id=d.publication_id
                    WHERE p.instrument_namespace=%s AND p.engine_dataset_id=%s
                    ORDER BY d.primary_file""",
                    (evidence["instrument_namespace"], evidence["engine_dataset_id"]),
                )
                plan.update(
                    engine_dataset_id=evidence["engine_dataset_id"],
                    instrument_namespace=evidence["instrument_namespace"],
                    primary_files=[s["path"] for s in file_summaries(evidence)],
                    matching_files=dict(await cur.fetchall()),
                    treatment="retain aggregate and its foreign keys, expose file replacements",
                    evidence={
                        "publication": "fresh production validation and SHA-256",
                        "raw_lineage": "reused registered links",
                    },
                )
                cur = await c.execute(
                    """SELECT dataset_id,input_dataset_id FROM historical.dataset_inputs
                    WHERE dataset_id=%s OR input_dataset_id=%s ORDER BY 1,2""",
                    (ident, ident),
                )
                plan["legacy_dataset_input_references"] = await cur.fetchall()
                if not dry_run:
                    async with c.transaction():
                        plan.update(await register_files(c, evidence, legacy_id=ident, raw=raw))
            except (MarketForgeError, OSError, ValueError, KeyError) as error:
                plan["conflict"] = str(error)
    return {
        "dry_run": dry_run,
        "publications": plans,
        "primary_file_count": sum(len(p.get("primary_files", [])) for p in plans),
        "conflict_count": sum("conflict" in p for p in plans),
    }
