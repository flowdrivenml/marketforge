"""Browse registered primary data files without reading publication artifacts."""

import argparse
import re
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, LiteralString

from psycopg import AsyncConnection
from psycopg.pq import TransactionStatus
from psycopg.rows import TupleRow, dict_row
from psycopg.types.json import Jsonb

from forge.datasets import converted, parquet, timelines, views
from forge.datasets.inspection import contribution_counts, terminal_lineage
from forge.datasets.migration import migrate
from forge.datasets.registration import file_summaries
from forge.historical.archives.storage.files import disk
from forge.historical.engine.managed import checksum
from forge.shared.database.connection import database_connection, database_dsn
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths

COLUMNS = """d.id,d.operation,d.data_type,d.storage_path,d.primary_file,d.file_sha256,
d.status,d.storage_format,d.start_timestamp_ns,d.end_timestamp_ns,d.operation_details AS summary,
p.id AS publication_id,p.engine_dataset_id,p.instrument_namespace,p.storage_root,p.manifest_path,
p.evidence->>'manifest_sha256' AS manifest_sha256,
p.evidence#>>'{resolved_job,job_id}' AS engine_job_id"""
SEARCH_ACTIONS = {"list", "coverage", "find"}
DATA_TYPES = {
    "trade": "trade",
    "l2": "l2",
    "trade_l2": "trade_l2",
    "trades": "trade",
    "depth": "l2",
    "order_book_l2": "l2",
    "combined": "trade_l2",
}
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def timestamp(value: str) -> int:
    try:
        if re.fullmatch(r"-?\d+", value):
            result = int(value)
        else:
            fraction = re.search(r"\.(\d+)(?=Z|[+-]\d\d:\d\d$)", value)
            if fraction and len(fraction[1]) > 9:
                raise ValueError("at most nine fractional digits")
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError("timezone required")
            delta = parsed.replace(microsecond=0) - EPOCH
            result = (delta.days * 86400 + delta.seconds) * 10**9
            if fraction:
                result += int(fraction[1].ljust(9, "0"))
        if not -(2**63) <= result < 2**63:
            raise ValueError("outside signed 64-bit nanoseconds")
        return result
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            f"Use integer nanoseconds or a timezone-qualified ISO timestamp ({error})."
        ) from error


def utc(value: int) -> str:
    seconds, nanos = divmod(value, 10**9)
    return f"{datetime.fromtimestamp(seconds, UTC):%Y-%m-%dT%H:%M:%S}.{nanos:09d}Z"


def bounded(value: str) -> int:
    number = int(value)
    if not 1 <= number <= 1000:
        raise argparse.ArgumentTypeError("Use a number from 1 to 1000.")
    return number


def selected_path(row: dict[str, Any]) -> Path:
    file = Path(row["storage_path"])
    root = Path(row["storage_root"])
    if (
        file != root / row["primary_file"]
        or file.is_symlink()
        or file.resolve() != file
        or not file.is_relative_to(root)
        or not file.is_file()
    ):
        raise MarketForgeError("Registered primary file is missing or its path has changed.")
    return file


@dataclass(frozen=True, slots=True)
class DatasetRequest:
    dataset_action: str
    id: int | None = None
    operation: str | None = None
    data_type: str | None = None
    status: str | None = None
    exchange: str | None = None
    instrument: str | None = None
    instrument_type: str | None = None
    market_category: str | None = None
    publication: str | bool | None = None
    start: int | None = None
    end: int | None = None
    limit: int = 50
    offset: int = 0
    column: list[str] | None = None
    wide: bool = False
    precise: bool = False
    checksums: bool = False
    dry_run: bool = False
    view: str | None = None
    publication_id: int | None = None
    engine_publication: str | None = None
    output_ref: str | None = None
    target_quote: str | None = None
    search: str | None = None
    sort: str = "id"
    part: int | None = None
    timeline_reference: str | None = None


async def browse(
    paths: ProjectPaths, args: DatasetRequest, *, detailed: bool = True
) -> dict[str, Any]:
    action = args.dataset_action
    if not 1 <= args.limit <= 1000:
        raise MarketForgeError("Dataset limits must be between 1 and 1000.")
    if action not in SEARCH_ACTIONS | {
        "show",
        "streams",
        "lineage",
        "verify",
        "schema",
        "head",
        "migrate-registration",
        "refresh-summary",
        "timeline",
    }:
        raise MarketForgeError(f"Unsupported dataset action: {action}.")
    if action == "migrate-registration":
        return await migrate(paths, dry_run=args.dry_run)
    async with database_connection(database_dsn(paths.root)) as c:
        if (
            args.view is not None
            or args.publication_id is not None
            or args.output_ref is not None
            or args.timeline_reference is not None
        ):
            start_snapshot = c.info.transaction_status == TransactionStatus.IDLE
            async with c.transaction():
                if start_snapshot:
                    await c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                return await browse_projection(c, args, detailed=detailed)
        async with c.cursor(row_factory=dict_row) as cur:
            if action in SEARCH_ACTIONS:
                if args.offset < 0 or (
                    args.start is not None and args.end is not None and args.start >= args.end
                ):
                    raise MarketForgeError("Require nonnegative offset and start < end.")
                await cur.execute(
                    """SELECT """
                    + COLUMNS
                    + """ FROM historical.datasets d
                    JOIN historical.dataset_publications p ON d.publication_id=p.id
                    WHERE (%(operation)s::text IS NULL OR d.operation=%(operation)s)
                    AND (%(data_type)s::text IS NULL OR d.data_type=%(data_type)s)
                    AND (%(status)s::text IS NULL AND d.status<>'superseded' OR d.status=%(status)s)
                    AND (%(publication)s::text IS NOT NULL OR %(status)s::text IS NOT NULL
                        OR NOT EXISTS (SELECT 1 FROM historical.publication_supersessions s
                                       WHERE s.publication_id=p.id))
                    AND (%(publication)s::text IS NULL OR p.engine_dataset_id=%(publication)s)
                    AND (%(start)s::bigint IS NULL OR d.end_timestamp_ns >= %(start)s)
                    AND (%(end)s::bigint IS NULL OR d.start_timestamp_ns < %(end)s)
                    AND (
                       (%(exchange)s::text IS NULL AND %(instrument)s::text IS NULL
                        AND %(instrument_type)s::text IS NULL AND %(market_category)s::text IS NULL)
                       OR EXISTS (
                          SELECT 1 FROM jsonb_array_elements(d.operation_details->'memberships') AS s
                          WHERE (%(exchange)s::text IS NULL OR s->>'exchange'=%(exchange)s)
                            AND (%(instrument)s::text IS NULL OR lower(s->>'symbol')=lower(%(instrument)s)
                                 OR s->>'instrument_id'=%(instrument)s)
                            AND (%(instrument_type)s::text IS NULL OR s->>'instrument_type'=%(instrument_type)s)
                            AND (%(market_category)s::text IS NULL OR s->>'market_category'=%(market_category)s)
                            AND (%(start)s::bigint IS NULL OR (s->>'end_timestamp_ns')::bigint >= %(start)s)
                            AND (%(end)s::bigint IS NULL OR (s->>'start_timestamp_ns')::bigint < %(end)s)
                       )
                    ) ORDER BY d.id LIMIT %(limit)s OFFSET %(offset)s""",
                    {
                        **asdict(args),
                        "limit": args.limit if not detailed else args.limit + 1,
                        "data_type": DATA_TYPES.get(args.data_type) if args.data_type else None,
                    },
                )
                records = await cur.fetchall()
                presentation = {}
                if detailed:
                    presentation["has_more"] = len(records) > args.limit
                    records = records[: args.limit]
                for record in records:
                    record["first_observed_utc"] = (
                        utc(record["start_timestamp_ns"])
                        if record["start_timestamp_ns"] is not None
                        else None
                    )
                    record["last_observed_utc"] = (
                        utc(record["end_timestamp_ns"])
                        if record["end_timestamp_ns"] is not None
                        else None
                    )
                return {
                    **presentation,
                    "datasets": records,
                    "limit": args.limit,
                    "offset": args.offset,
                    "interval_semantics": "inclusive observed envelopes, query end exclusive; continuity not established",
                }
            if args.id is None or args.id <= 0:
                raise MarketForgeError("A positive registry primary-file ID is required.")
            await cur.execute(
                "SELECT " + COLUMNS + " FROM historical.datasets d "
                "JOIN historical.dataset_publications p ON d.publication_id=p.id WHERE d.id=%s",
                (args.id,),
            )
            row = await cur.fetchone()
            if row is None:
                await cur.execute(
                    """SELECT d.id,d.primary_file FROM historical.dataset_registration_migrations m
                    JOIN historical.datasets d ON d.publication_id=m.publication_id
                    WHERE m.legacy_dataset_id=%s ORDER BY d.primary_file""",
                    (args.id,),
                )
                replacements = await cur.fetchall()
                if replacements:
                    return {
                        "legacy_dataset_id": args.id,
                        "status": "superseded",
                        "replacement_files": replacements,
                    }
                raise MarketForgeError(
                    f"No active file dataset {args.id}. Aggregate IDs require migration."
                )
            if action == "lineage":
                if detailed:
                    return await terminal_lineage(
                        cur,
                        row,
                        publication=args.publication is True,
                        checksums=args.checksums,
                        limit=args.limit,
                    )
                await cur.execute(
                    """SELECT local_archive_id,input_sha256 FROM historical.dataset_raw_inputs
                    WHERE dataset_id=%s ORDER BY local_archive_id""",
                    (args.id,),
                )
                contributions = await cur.fetchall()
                await cur.execute(
                    "SELECT input_dataset_id FROM historical.dataset_inputs WHERE dataset_id=%s ORDER BY input_dataset_id",
                    (args.id,),
                )
                dataset_contributions = await cur.fetchall()
                await cur.execute(
                    """SELECT evidence->'selected_raw_inputs' AS raw_inputs,
                    evidence->'selected_dataset_inputs' AS dataset_inputs FROM historical.dataset_publications
                    WHERE id=%s""",
                    (row["publication_id"],),
                )
                return {
                    "id": args.id,
                    "file_contributions": {
                        "raw_inputs": contributions,
                        "dataset_inputs": dataset_contributions,
                    },
                    "publication_selection": await cur.fetchone(),
                }
            if action == "show":
                keys = (
                    "publication_id",
                    "engine_dataset_id",
                    "instrument_namespace",
                    "storage_root",
                    "manifest_path",
                    "manifest_sha256",
                    "engine_job_id",
                )
                return {
                    "file": {k: v for k, v in row.items() if k not in keys},
                    "publication": {k: row[k] for k in keys},
                    **({"counts": await contribution_counts(cur, args.id)} if detailed else {}),
                }
            if action == "streams":
                return {
                    "id": args.id,
                    "scope": row["summary"].get("membership_scope", "unresolved"),
                    "streams": row["summary"].get("memberships", []),
                    **({"file": row} if detailed else {}),
                }
            file = selected_path(row)
            if action == "verify":
                digest = await disk(checksum, file)
                manifest = Path(row["manifest_path"])
                shared = (
                    manifest == Path(row["storage_root"]) / "manifest.json"
                    and await disk(manifest.resolve) == manifest
                )
                if (
                    digest != row["file_sha256"]
                    or not shared
                    or await disk(checksum, manifest) != row["manifest_sha256"]
                ):
                    raise MarketForgeError(
                        "Selected file or shared manifest checksum disagrees with registration."
                    )
                return {
                    "id": args.id,
                    "status": "verified",
                    "scope": "primary file and shared manifest checksum",
                    "entire_publication_verified": False,
                    "file_sha256": digest,
                    **({"file": row} if detailed else {}),
                }
            if action == "schema":
                schema = await disk(parquet.schema, [file])
                return {
                    "id": args.id,
                    **({"file": row} if detailed else {}),
                    "columns": [
                        {"name": field.name, "type": str(field.type), "nullable": field.nullable}
                        for field in schema
                    ],
                }
            if action == "head":
                from itertools import islice

                def read() -> dict[str, Any]:
                    columns = args.column
                    if detailed and columns is None:
                        names = parquet.schema([file]).names
                        columns = names if args.wide else preview_columns(row, names)
                    batches = parquet.batches([file], columns=columns, batch_size=args.limit)
                    rows = [record for batch in islice(batches, 1) for record in batch.to_pylist()]
                    result: dict[str, Any] = {"id": args.id, "rows": rows}
                    if detailed:
                        result.update(
                            file=row,
                            projection=columns,
                            precise=args.precise,
                            compact=not args.wide and args.column is None,
                        )
                    return result

                return await disk(read)
            await cur.execute(
                "SELECT evidence FROM historical.dataset_publications WHERE id=%s",
                (row["publication_id"],),
            )
            saved = await cur.fetchone()
            assert saved is not None
            evidence = saved["evidence"]
            summary = next(s for s in file_summaries(evidence) if s["path"] == row["primary_file"])
            await cur.execute(
                "UPDATE historical.datasets SET operation_details=%s WHERE id=%s",
                (Jsonb(summary), args.id),
            )
            return {"id": args.id, "summary": summary}


async def browse_projection(
    c: AsyncConnection[TupleRow],
    args: DatasetRequest,
    *,
    detailed: bool,
) -> dict[str, Any]:
    action = args.dataset_action
    if action == "timeline" and args.timeline_reference is not None:
        ref = timelines.TimelineReference.decode(args.timeline_reference)
        selection = DatasetRequest(
            "list",
            view="timelines",
            operation=ref.operation,
            status=ref.status,
            start=ref.start,
            end=ref.end,
            search=ref.search,
            output_ref=ref.output,
            limit=1,
            **ref.membership_filters(),
        )
        result = await views.page(c, selection, timeline_ref=ref)
        return {
            "schema_version": 1,
            "environment": result["environment"],
            "timeline": result["timelines"][0],
        }
    if action in SEARCH_ACTIONS and args.view:
        if args.offset < 0 or (
            args.start is not None and args.end is not None and args.start >= args.end
        ):
            raise MarketForgeError("Require nonnegative offset and start < end.")
        return await views.page(c, args)
    if action not in SEARCH_ACTIONS and args.output_ref:
        outputs, capability, unavailable = await converted.indexed_outputs(c, args.output_ref)
        if capability == "unavailable":
            raise MarketForgeError("The selected registry has no conversion output index.")
        if unavailable:
            raise MarketForgeError("The indexed conversion manifest is unavailable or invalid.")
        if not outputs:
            raise MarketForgeError("No indexed conversion output matches this reference.")
        return await inspect_converted(outputs[0], args, detailed=detailed)
    if action in {"show", "streams"} and args.publication_id is not None:
        selection = replace(args, view="publications", limit=1)
        result = await views.page(c, selection, inspect=True)
        if not result["publications"]:
            raise MarketForgeError(f"No registry publication {args.publication_id}.")
        publication = result["publications"][0]
        if action == "streams":
            return {
                "file": publication,
                "scope": "primary-file membership across the publication",
                "streams": stream_memberships(publication.get("memberships") or []),
            }
        files = await views.page(c, replace(args, view="files"), inspect=True)
        return {"publication": publication, "files": files}
    raise MarketForgeError("An explicit dataset view or inspection reference is required.")


def stream_memberships(members: list[dict[str, Any]]) -> list[dict[str, Any]]:
    streams: dict[tuple[Any, ...], dict[str, Any]] = {}
    for member in members:
        identity = tuple(
            member.get(field)
            for field in (
                "stream_id",
                "stream_rank",
                "source_publication_id",
                "kind",
                "exchange",
                "instrument_id",
                "instrument_type",
                "market_category",
            )
        )
        stream = streams.setdefault(identity, dict(member))
        for field, combine in (("start_timestamp_ns", min), ("end_timestamp_ns", max)):
            if (bound := member.get(field)) is not None:
                prior = stream.get(field)
                stream[field] = bound if prior is None else combine(int(prior), int(bound))
    return [streams[key] for key in sorted(streams, key=str)]


async def inspect_converted(
    row: dict[str, Any], args: DatasetRequest, *, detailed: bool
) -> dict[str, Any]:
    action = args.dataset_action
    files = row["files"]
    part = args.part
    if part is not None and not 1 <= part <= len(files):
        raise MarketForgeError(f"Choose a converted file part from 1 to {len(files)}.")
    if action == "show":
        return {"output": row}
    if action == "streams":
        return {
            "file": row,
            "scope": "source publication",
            "streams": stream_memberships(row["summary"]["memberships"]),
        }
    if action == "lineage":
        return {"file": row, "publication_selection": row["indexed_result"]["input"]}
    if action == "verify":
        result = await disk(converted.verify_output, row, part)
        return {**result, **({"file": row} if detailed else {})}
    if part is None:
        raise MarketForgeError(
            "Specify --part using the file numbers from datasets show --output-ref."
        )
    file = await disk(converted.checked_path, Path(row["storage_root"]), files[part - 1]["path"])
    result: dict[str, Any] = {"output_ref": row["output_ref"], "part": part}
    if detailed:
        result["file"] = {**row, "summary": {**row["summary"], "rows": files[part - 1]["rows"]}}
    if action == "schema":
        schema = await disk(parquet.schema, [file])
        result["columns"] = [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in schema
        ]
    else:
        from itertools import islice

        def read() -> dict[str, Any]:
            columns = args.column
            if detailed and columns is None and not args.wide:
                names = parquet.schema([file]).names
                fields = (
                    "event_timestamp_ns",
                    "stream_id",
                    "event_kind",
                    "target_quote",
                    "price",
                    "price_target",
                    "quantity_quote",
                    "quantity_quote_target",
                    "trade",
                    "trade_target",
                    "changes",
                    "changes_target",
                    "depth",
                    "depth_target",
                    "conversion_status",
                )
                columns = [name for name in fields if name in names] or names[:5]
            batches = parquet.batches([file], columns=columns, batch_size=args.limit)
            rows = [record for batch in islice(batches, 1) for record in batch.to_pylist()]
            return {
                "rows": rows,
                **(
                    {"projection": columns, "precise": args.precise, "compact": False}
                    if detailed
                    else {}
                ),
            }

        result.update(await disk(read))
    return result


def preview_columns(row: dict[str, Any], names: list[str]) -> list[str]:
    memberships = row.get("summary", {}).get("memberships", [])
    identities = (
        ("exchange", "symbol", "instrument_id", "stream_id", "stream_rank")
        if len(memberships) != 1
        else ()
    )
    fields = {
        "trade": (
            "side",
            "price",
            "quantity_base",
            "quantity_quote",
            "quantity_contracts",
            "trade_id",
        ),
        "l2": (
            "side",
            "price",
            "quantity_base",
            "quantity_quote",
            "quantity_contracts",
            "order_count",
            "source_operation",
            "sequence_start",
            "sequence_end",
            "canonical_row_count",
        ),
        "trade_l2": ("event_kind", "trade", "canonical_row_count"),
    }.get(row["data_type"], ())
    available = [name for name in ("event_timestamp_ns", *identities, *fields) if name in names]
    return available or names[:5]


async def publications(
    paths: ProjectPaths,
    *,
    instrument: str | None = None,
    limit: int = 50,
    offset: int = 0,
    identity: int | None = None,
    file_offset: int = 0,
    filters: DatasetRequest | None = None,
    search: str = "",
    sort: str = "",
    descending: bool = True,
) -> dict[str, Any]:
    """List retained publication evidence once per registry publication."""
    if not 1 <= limit <= 100 or offset < 0 or file_offset < 0:
        raise MarketForgeError("Invalid publication page.")
    query = filters or DatasetRequest("list")
    orders: dict[str, LiteralString] = {
        "Publication": "CASE WHEN p.engine_dataset_id ~ '^[0-9]+$' THEN p.engine_dataset_id::numeric END",
        "Recorded size": "files.bytes",
        "Files": "files.primary_file_count",
        "Observed UTC": "files.latest",
        "Operation": "p.operation",
    }
    order = orders.get(sort, "p.id")
    direction: LiteralString = " DESC" if descending else " ASC"
    async with database_connection(database_dsn(paths.root)) as connection:
        await connection.execute("SET statement_timeout = '5s'")
        async with connection.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                """
                SELECT p.id AS registry_publication_id,p.engine_dataset_id,p.operation,
                    p.instrument_namespace,p.manifest_path,
                    p.evidence->>'manifest_sha256' AS manifest_sha256,
                    p.evidence->'manifest'->'streams' AS streams,
                    p.evidence->'manifest'->'time_range' AS requested_interval,
                    files.earliest AS first_observed_ns,
                    files.latest AS last_observed_ns,
                    p.evidence->'execution' AS execution,
                    p.evidence->'manifest'->'events_written' AS events,
                    files.bytes AS bytes_written,
                    files.primary_file_count,
                    files.first_file_id AS representative_file_id
                FROM historical.dataset_publications p
                JOIN LATERAL (
                    SELECT COUNT(*) AS primary_file_count, MIN(d.id) AS first_file_id,
                        MIN(d.start_timestamp_ns) AS earliest,MAX(d.end_timestamp_ns) AS latest,
                        SUM(CASE WHEN d.operation_details->>'size_bytes' ~ '^[0-9]+$' THEN
                            (d.operation_details->>'size_bytes')::numeric END) AS bytes
                    FROM historical.datasets d WHERE d.publication_id=p.id AND d.primary_file IS NOT NULL AND d.status='complete'
                ) files ON files.primary_file_count>0
                WHERE (%(identity)s::bigint IS NULL OR p.id=%(identity)s)
                    AND (%(operation)s::text IS NULL OR p.operation=%(operation)s)
                    AND (%(publication)s::text IS NULL OR p.engine_dataset_id=%(publication)s OR p.id::text=%(publication)s)
                    AND EXISTS (
                        SELECT 1 FROM historical.datasets d
                        WHERE d.publication_id=p.id AND d.primary_file IS NOT NULL AND d.status='complete'
                          AND (%(start)s::bigint IS NULL OR d.end_timestamp_ns >= %(start)s)
                          AND (%(end)s::bigint IS NULL OR d.start_timestamp_ns < %(end)s)
                          AND (
                            (p.engine_dataset_id ILIKE %(search)s OR p.id::text ILIKE %(search)s OR %(search)s='%%')
                            AND %(instrument)s::text IS NULL AND %(exchange)s::text IS NULL
                            AND %(type)s::text IS NULL AND %(market)s::text IS NULL
                            AND (%(data_type)s::text IS NULL OR d.data_type=%(data_type)s)
                            OR EXISTS (
                                SELECT 1 FROM jsonb_array_elements(COALESCE(d.operation_details->'memberships','[]')) m
                                WHERE (%(instrument)s::text IS NULL OR lower(m->>'symbol')=lower(%(instrument)s)
                                       OR m->>'instrument_id'=%(instrument)s)
                                  AND (%(exchange)s::text IS NULL OR m->>'exchange'=%(exchange)s)
                                  AND (%(data_type)s::text IS NULL OR d.data_type=%(data_type)s
                                       OR (%(data_type)s='trade' AND m->>'kind'='trade')
                                       OR (%(data_type)s='l2' AND m->>'kind'='depth'))
                                  AND (%(type)s::text IS NULL OR m->>'instrument_type'=%(type)s)
                                  AND (%(market)s::text IS NULL OR m->>'market_category'=%(market)s)
                                  AND (p.engine_dataset_id ILIKE %(search)s OR p.id::text ILIKE %(search)s
                                       OR m->>'symbol' ILIKE %(search)s OR m->>'exchange' ILIKE %(search)s)
                            )
                          )
                    )
                ORDER BY """
                + order
                + direction
                + " NULLS LAST,p.id LIMIT %(limit)s OFFSET %(offset)s",
                {
                    "identity": identity,
                    "instrument": query.instrument or instrument,
                    "operation": query.operation,
                    "publication": query.publication,
                    "data_type": query.data_type,
                    "start": query.start,
                    "end": query.end,
                    "exchange": query.exchange,
                    "type": query.instrument_type,
                    "market": query.market_category,
                    "search": "%" + search + "%",
                    "limit": limit + 1,
                    "offset": offset,
                },
            )
            rows = list(await cursor.fetchall())
            if identity is not None and rows:
                await cursor.execute(
                    """
                    SELECT id AS registry_primary_file_id,primary_file,file_sha256,data_type,
                           start_timestamp_ns AS first_observed_ns,end_timestamp_ns AS last_observed_ns
                    FROM historical.datasets WHERE publication_id=%s AND primary_file IS NOT NULL
                    ORDER BY id LIMIT 51 OFFSET %s
                """,
                    (identity, file_offset),
                )
                files = await cursor.fetchall()
                rows[0]["primary_files"] = list(files[:50])
                rows[0]["primary_files_truncated"] = len(files) > 50
    return {"publications": rows[:limit], "has_more": len(rows) > limit, "offset": offset}
