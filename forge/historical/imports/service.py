"""Stable-source copying with common acquisition verification/publication recovery."""

import hashlib
import json
import os
import tomllib
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
from forge.orchestration.registry import begin, complete_call, current_execution, retain_recipe
from forge.shared.async_work import bounded_map
from forge.shared.database.connection import database_connection
from forge.shared.identity import RunOperation, parse_run_id
from forge.shared.paths import ProjectPaths


async def hash_source(item: FrozenInput) -> str:
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
    *,
    planned_relative: str | None = None,
) -> dict[str, object]:
    checksum = await hash_source(item)
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
            """INSERT INTO historical.raw_sources(source_key,source_kind,root_uri)
            VALUES(%s,%s,%s) ON CONFLICT(source_key) DO UPDATE SET source_key=EXCLUDED.source_key
            WHERE raw_sources.root_uri=EXCLUDED.root_uri AND raw_sources.source_kind=EXCLUDED.source_kind RETURNING id""",
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
        object_cursor = await c.execute(
            """INSERT INTO historical.raw_objects(acquisition_method,raw_source_id,venue_code,scope_key,scope_json,data_type,source_object_key,filename,start_at,end_at,remote_status)
            VALUES('local_copy',%s,%s,%s,%s,%s,%s,%s,%s,%s,'unknown')
            ON CONFLICT(raw_source_id,source_object_key) WHERE acquisition_method='local_copy'
            DO UPDATE SET start_at=COALESCE(raw_objects.start_at,EXCLUDED.start_at),
                end_at=COALESCE(raw_objects.end_at,EXCLUDED.end_at)
            WHERE raw_objects.scope_json=EXCLUDED.scope_json AND raw_objects.data_type=EXCLUDED.data_type
              AND raw_objects.venue_code=EXCLUDED.venue_code
              AND (raw_objects.start_at IS NULL OR EXCLUDED.start_at IS NULL OR
                (raw_objects.start_at,raw_objects.end_at)=(EXCLUDED.start_at,EXCLUDED.end_at))
            RETURNING id""",
            (
                source_id,
                identity.exchange,
                f"local:{source_id}:{item.reference}",
                json_value(
                    {
                        "instrument_type": identity.instrument_type,
                        "market_category": identity.market_category,
                        "scope_kind": "instrument",
                        "scope_identifier": identity.symbol,
                    }
                ),
                config.data_type,
                item.reference,
                item.reference,
                declared.start if declared else None,
                declared.end if declared else None,
            ),
        )
        object_row = await object_cursor.fetchone()
        if object_row is None:
            raise ImportError("Source object conflicts with its registered identity/content.")
        raw_object_id = object_row[0]
        cur = await c.execute(
            """INSERT INTO historical.raw_interpretations
            (raw_object_id,external_source_id,source_object_key,sha256,bytes_expected,instrument_id,identity_json,specifications_json,data_type,
             declared_start_at,declared_end_at,format_profile,normalization_readiness,import_provenance,binding_fingerprint)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(external_source_id,source_object_key,sha256,binding_fingerprint) DO UPDATE SET source_object_key=EXCLUDED.source_object_key
            WHERE raw_interpretations.identity_json=EXCLUDED.identity_json AND raw_interpretations.data_type=EXCLUDED.data_type
              AND raw_interpretations.declared_start_at IS NOT DISTINCT FROM EXCLUDED.declared_start_at
              AND raw_interpretations.declared_end_at IS NOT DISTINCT FROM EXCLUDED.declared_end_at
              AND raw_interpretations.specifications_json IS NOT DISTINCT FROM EXCLUDED.specifications_json
            RETURNING id,normalization_readiness""",
            (
                raw_object_id,
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
                        "binding": config.binding,
                        "source_stat": asdict(item),
                        "check": "size,device,inode,mtime_ns,ctime_ns before/after hashing and copying",
                    }
                ),
                hashlib.sha256(
                    json.dumps(
                        config.binding
                        or {
                            "profile": config.format_profile,
                            "specifications": config.specifications.model_dump(mode="json")
                            if config.specifications
                            else None,
                        },
                        sort_keys=True,
                    ).encode()
                ).hexdigest(),
            ),
        )
        revision = await cur.fetchone()
        if revision is None:
            raise ImportError(
                "Existing content identity has conflicting declared metadata; use a separate source identity."
            )
        external_id: int = revision[0]
        readiness = revision[1]
    # A new interpretation can reuse immutable verified physical bytes from the
    # same source object. Bindings are appended; downstream evidence is untouched.
    cur = await c.execute(
        """SELECT external_archive_id FROM historical.local_archives
        WHERE raw_object_id=%s AND sha256=%s AND storage_root_key=%s AND status='complete' ORDER BY id DESC LIMIT 1""",
        (raw_object_id, checksum, root_key(raw_root)),
    )
    previous = await cur.fetchone()
    reuse_id = previous[0] if previous and previous[0] is not None else external_id
    existing = await reuse_registered(c, raw_root, external_archive_id=reuse_id)
    if existing is not None:
        async with c.transaction():
            await c.execute(
                "INSERT INTO historical.raw_bindings(local_archive_id,interpretation_id) VALUES(%s,%s) ON CONFLICT DO NOTHING",
                (existing.local_archive_id, external_id),
            )
        return {**asdict(existing), "input": item.reference, "normalization_readiness": readiness}
    name = (
        "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in Path(item.path).name)[-160:]
        or "input"
    )
    relative = planned_relative or f"external/{external_id}/{uuid4().hex}/{name}"
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
        await c.execute(
            "INSERT INTO historical.raw_bindings(local_archive_id,interpretation_id) VALUES(%s,%s) ON CONFLICT DO NOTHING",
            (local_id, external_id),
        )
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
    paths: ProjectPaths,
    dsn: str,
    config: ImportConfig,
    source_file: Path,
    *,
    source_bytes: bytes | None = None,
) -> dict[str, object]:
    execution = await begin(
        paths,
        RunOperation.ACQUISITION,
        "import",
        recipe=source_file.stem,
        source=source_file,
        dsn=dsn,
    )
    return await complete_call(
        execution, lambda: _run_import(paths, dsn, config, source_file, source_bytes=source_bytes)
    )


async def _run_import(
    paths: ProjectPaths,
    dsn: str,
    config: ImportConfig,
    source_file: Path,
    *,
    source_bytes: bytes | None = None,
) -> dict[str, object]:
    if config.binding is not None:
        raise ImportError(
            "Legacy import is raw-only. Use acquisition register for a verified binding."
        )
    submitted = source_bytes if source_bytes is not None else await disk(source_file.read_bytes)
    if ImportConfig.model_validate(tomllib.loads(submitted.decode())) != config:
        raise ImportError("The import recipe changed before execution.")
    execution = current_execution()
    assert execution is not None
    run_id = parse_run_id(execution.id)
    artifacts = RunArtifacts.for_run(paths, run_id)
    await disk(artifacts.root.mkdir, parents=True, exist_ok=True)
    await disk(
        retain_recipe,
        RunOperation.ACQUISITION,
        source_file,
        submitted,
        destination=artifacts.submitted_config,
        family="import",
    )
    frozen = await disk(freeze_inputs, config)
    from forge.orchestration.artifacts import retain_bytes

    resolved_sha256 = await disk(
        retain_bytes,
        artifacts.resolved_plan,
        json.dumps(
            {
                "configuration": config.model_dump(mode="json"),
                "inputs": [asdict(i) for i in frozen],
            },
            indent=2,
        ).encode(),
    )
    counts = {"completed": 0, "reused": 0, "failed": 0, "cancelled": 0}
    report: dict[str, object] = {
        "schema_version": 1,
        "run_id": str(run_id),
        "operation": "acquisition",
        "action": "import",
        "environment": execution.purpose,
        "registry_id": execution.registry,
        "counts": counts,
        "report": str(artifacts.report),
        "events": str(artifacts.events),
        "plan": str(artifacts.resolved_plan),
        "resolved_plan": str(artifacts.resolved_plan),
        "resolved_sha256": resolved_sha256,
    }

    async def inputs() -> AsyncIterator[FrozenInput]:
        for item in frozen:
            yield item

    async def work(item: FrozenInput) -> dict[str, object]:
        try:
            async with database_connection(dsn) as c:
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
            json.dumps({**report, "status": "cancelled"}),
            encoding="utf-8",
        )
        raise
    report["status"] = "failed" if counts["failed"] else "complete"
    await disk(artifacts.report.write_text, json.dumps(report, indent=2), encoding="utf-8")
    return report
