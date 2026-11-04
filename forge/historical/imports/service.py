"""Stable-source copying with common acquisition verification/publication recovery."""

import hashlib
import json
import os
from collections.abc import AsyncIterator
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.archives.storage.acquire import complete, reuse_registered, root_key
from forge.historical.archives.storage.files import CHUNK_SIZE, disk, publish
from forge.historical.imports.models import FrozenInput, ImportConfig, ImportError, freeze_inputs
from forge.markets.repository import json_value
from forge.orchestration.artifacts import RunArtifacts
from forge.shared.async_work import bounded_map
from forge.shared.database.testing import testing_connection
from forge.shared.identity import new_run_id
from forge.shared.paths import ProjectPaths


async def _hash_source(item: FrozenInput) -> str:
    path = Path(item.path)
    if not item.matches(await disk(path.stat)):
        raise ImportError("Source changed after planning.")
    source = await disk(path.open, "rb")
    digest = hashlib.sha256()
    try:
        if not item.matches(await disk(os.fstat, source.fileno())):
            raise ImportError("Opened source no longer matches the frozen input.")
        while chunk := await disk(source.read, CHUNK_SIZE):
            digest.update(chunk)
        if not item.matches(await disk(os.fstat, source.fileno())) or not item.matches(
            await disk(path.stat)
        ):
            raise ImportError("Source changed while hashing.")
    finally:
        await disk(source.close)
    return digest.hexdigest()


async def import_file(
    c: AsyncConnection[TupleRow],
    config: ImportConfig,
    item: FrozenInput,
    raw_root: Path,
    run_id: str,
) -> dict[str, object]:
    checksum = await _hash_source(item)
    normalized_root = str(await disk(Path(config.root).resolve))
    identity = config.identity
    async with c.transaction():
        cur = await c.execute(
            """SELECT i.id FROM catalog.instruments i JOIN catalog.exchanges e ON e.id=i.exchange_id
            WHERE e.code=%s AND i.market_category=%s AND i.instrument_type=%s AND i.symbol=%s""",
            (
                identity.exchange,
                identity.market_category,
                identity.instrument_type,
                identity.symbol,
            ),
        )
        instrument = await cur.fetchone()
        if instrument is None and config.specifications is None:
            raise ImportError("Unknown instrument requires explicit historical specifications.")
        cur = await c.execute(
            """INSERT INTO historical.external_sources(source_key,source_kind,root_uri)
            VALUES(%s,%s,%s) ON CONFLICT(source_key) DO UPDATE SET source_key=EXCLUDED.source_key
            WHERE external_sources.root_uri=EXCLUDED.root_uri AND external_sources.source_kind=EXCLUDED.source_kind RETURNING id""",
            (config.source_key, config.source_kind, normalized_root),
        )
        row = await cur.fetchone()
        if row is None:
            raise ImportError("Source key already identifies a different root or origin kind.")
        source_id = row[0]
        readiness = (
            "requires_profile"
            if config.format_profile is None
            else "requires_historical_specs"
            if config.specifications is None
            else "pending_engine_validation"
        )
        declared = config.interval
        cur = await c.execute(
            """INSERT INTO historical.external_archives
            (external_source_id,source_object_key,sha256,bytes_expected,instrument_id,identity_json,specifications_json,data_type,
             declared_start_at,declared_end_at,format_profile,normalization_readiness,import_provenance)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(external_source_id,source_object_key,sha256) DO UPDATE SET source_object_key=EXCLUDED.source_object_key
            WHERE external_archives.identity_json=EXCLUDED.identity_json AND external_archives.data_type=EXCLUDED.data_type
              AND external_archives.declared_start_at IS NOT DISTINCT FROM EXCLUDED.declared_start_at
              AND external_archives.declared_end_at IS NOT DISTINCT FROM EXCLUDED.declared_end_at
              AND external_archives.specifications_json IS NOT DISTINCT FROM EXCLUDED.specifications_json
            RETURNING id,normalization_readiness""",
            (
                source_id,
                item.reference,
                checksum,
                item.size,
                instrument[0] if instrument else None,
                json_value(identity.model_dump()),
                json_value(config.specifications.model_dump(mode="json"))
                if config.specifications
                else None,
                config.data_type,
                declared.start if declared else None,
                declared.end if declared else None,
                config.format_profile,
                readiness,
                json_value(
                    {
                        "run_id": run_id,
                        "source_stat": asdict(item),
                        "check": "size,device,inode,mtime_ns,ctime_ns before/after hashing and copying",
                    }
                ),
            ),
        )
        revision = await cur.fetchone()
        if revision is None:
            raise ImportError(
                "Existing content identity has conflicting declared metadata; use a separate source identity."
            )
        external_id: int = revision[0]
        readiness = revision[1]
    existing = await reuse_registered(c, raw_root, external_archive_id=external_id)
    if existing is not None:
        return {**asdict(existing), "input": item.reference, "normalization_readiness": readiness}
    name = (
        "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in Path(item.path).name)[-160:]
        or "input"
    )
    relative = f"external/{external_id}/{uuid4().hex}/{name}"
    final = raw_root / relative
    partial = final.with_name(final.name + ".part")
    await disk(final.parent.mkdir, parents=True, exist_ok=False)
    async with c.transaction():
        cur = await c.execute(
            """INSERT INTO historical.local_archives(external_archive_id,storage_root_key,relative_path,status,bytes_written,sha256)
            VALUES(%s,%s,%s,'downloading',%s,%s) RETURNING id""",
            (external_id, root_key(raw_root), relative, item.size, checksum),
        )
        row = await cur.fetchone()
        if row is None:
            raise ImportError("Could not reserve managed copy.")
        local_id: int = row[0]
    published = False
    try:
        original = Path(item.path)
        if not item.matches(await disk(original.stat)):
            raise ImportError("Source changed before copying.")
        source = await disk(original.open, "rb")
        output = await disk(partial.open, "xb")
        digest = hashlib.sha256()
        written = 0
        try:
            while chunk := await disk(source.read, CHUNK_SIZE):
                await disk(output.write, chunk)
                digest.update(chunk)
                written += len(chunk)
            await disk(output.flush)

            if not item.matches(await disk(os.fstat, source.fileno())) or not item.matches(
                await disk(original.stat)
            ):
                raise ImportError("Source changed during copying; stable files are required.")
            await disk(os.fsync, output.fileno())
        finally:
            await disk(source.close)
            await disk(output.close)
        if written != item.size or digest.hexdigest() != checksum:
            raise ImportError("Source bytes changed during copying.")
        published = True
        await disk(publish, partial, final)
        await complete(c, local_id)
        return {
            "status": "completed",
            "input": item.reference,
            "external_archive_id": external_id,
            "local_archive_id": local_id,
            "relative_path": relative,
            "bytes_written": written,
            "sha256": checksum,
            "normalization_readiness": readiness,
        }
    except BaseException:
        if not published:
            # A hard crash may leave this reservation; common recovery verifies
            # its expected bytes before publication, never trusts filename alone.
            await disk(partial.unlink, missing_ok=True)
            await c.execute(
                "UPDATE historical.local_archives SET status='failed' WHERE id=%s", (local_id,)
            )
        raise


async def run_import(
    paths: ProjectPaths, dsn: str, config: ImportConfig, source_file: Path
) -> dict[str, object]:
    frozen = await disk(freeze_inputs, config)
    run_id = new_run_id()
    artifacts = RunArtifacts.for_run(paths, run_id)
    await disk(artifacts.root.mkdir, parents=True, exist_ok=False)
    await disk(artifacts.submitted_config.write_bytes, await disk(source_file.read_bytes))
    await disk(
        artifacts.resolved_plan.write_text,
        json.dumps(
            {
                "configuration": config.model_dump(mode="json"),
                "inputs": [asdict(i) for i in frozen],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    counts = {"completed": 0, "reused": 0, "failed": 0, "cancelled": 0}

    async def inputs() -> AsyncIterator[FrozenInput]:
        for item in frozen:
            yield item

    async def work(item: FrozenInput) -> dict[str, object]:
        try:
            async with testing_connection(dsn) as c:
                return await import_file(c, config, item, paths.raw_dir, str(run_id))
        except Exception as error:
            return {
                "input": item.reference,
                "status": "failed",
                "error": str(error)
                if isinstance(error, ImportError)
                else type(error).__name__ + "; verify source stability, storage and database.",
            }

    try:
        async for result in bounded_map(inputs(), work, workers=2):
            status = str(result["status"])
            counts[status] += 1

            def append(document: dict[str, object] = result) -> None:
                with artifacts.events.open("a", encoding="utf-8") as out:
                    out.write(json.dumps(document, default=str) + "\n")

            await disk(append)
    except BaseException:
        counts["cancelled"] = len(frozen) - sum(counts.values())
        await disk(
            artifacts.report.write_text,
            json.dumps({"run_id": str(run_id), "status": "cancelled", "counts": counts}),
            encoding="utf-8",
        )
        raise
    report: dict[str, object] = {
        "schema_version": 1,
        "run_id": str(run_id),
        "status": "failed" if counts["failed"] else "complete",
        "counts": counts,
        "report": str(artifacts.report),
        "events": str(artifacts.events),
        "plan": str(artifacts.resolved_plan),
    }
    await disk(artifacts.report.write_text, json.dumps(report, indent=2), encoding="utf-8")
    return report
