"""Browse registered primary data files without reading publication artifacts."""

import argparse
import asyncio
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from forge.datasets import parquet
from forge.datasets.migration import migrate
from forge.datasets.registration import file_summaries
from forge.historical.archives.storage.files import disk
from forge.historical.engine.managed import checksum
from forge.shared.database.testing import testing_connection, testing_dsn
from forge.shared.errors import MarketForgeError
from forge.shared.terminal.console import create_console
from forge.shared.terminal.datasets import render_coverage, render_datasets
from forge.startup.context import ApplicationContext

COLUMNS = """d.id,d.operation,d.data_type,d.storage_path,d.primary_file,d.file_sha256,
d.status,d.storage_format,d.start_timestamp_ns,d.end_timestamp_ns,d.operation_details AS summary,
p.id AS publication_id,p.engine_dataset_id,p.instrument_namespace,p.storage_root,p.manifest_path,
p.evidence->>'manifest_sha256' AS manifest_sha256,
p.evidence#>>'{resolved_job,job_id}' AS engine_job_id"""
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


def configure(parser: argparse.ArgumentParser) -> None:
    actions = parser.add_subparsers(dest="dataset_action", required=True)
    for name in (
        "list",
        "show",
        "streams",
        "lineage",
        "coverage",
        "verify",
        "schema",
        "head",
        "migrate-registration",
        "refresh-summary",
    ):
        command = actions.add_parser(name, allow_abbrev=False)
        command.add_argument("--testing", action="store_true", required=True)
        command.set_defaults(service_handler=run)
        if name not in {"list", "coverage", "migrate-registration"}:
            command.add_argument("id", type=int)
        if name == "list":
            command.add_argument("--wide", action="store_true")
            command.add_argument(
                "--precise", action="store_true", help="Display full nanosecond UTC timestamps."
            )
        if name in {"list", "coverage"}:
            command.add_argument("--operation", choices=("normalize", "merge"))
            command.add_argument(
                "--data-type", choices=("trade", "l2", "trade_l2", "trades", "depth", "combined")
            )
            for flag in (
                "status",
                "exchange",
                "instrument",
                "instrument-type",
                "market-category",
                "publication",
            ):
                command.add_argument(f"--{flag}")
            command.add_argument("--start", type=timestamp)
            command.add_argument("--end", type=timestamp)
            command.add_argument("--limit", type=bounded, default=50)
            command.add_argument("--offset", type=int, default=0)
        if name == "head":
            command.add_argument("--limit", type=bounded, default=10)
            command.add_argument("--column", action="append")
        if name == "migrate-registration":
            command.add_argument("--all", action="store_true", required=True)
            command.add_argument("--dry-run", action="store_true")


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


async def browse(args: argparse.Namespace, context: ApplicationContext) -> dict[str, Any]:
    paths = context.paths
    action = args.dataset_action
    if action == "migrate-registration":
        return await migrate(paths, dry_run=args.dry_run)
    async with testing_connection(testing_dsn(paths.root)) as c:
        async with c.cursor(row_factory=dict_row) as cur:
            if action in {"list", "coverage"}:
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
                    AND (%(status)s::text IS NULL OR d.status=%(status)s)
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
                        **vars(args),
                        "limit": args.limit if args.as_json else args.limit + 1,
                        "data_type": {"trades": "trade", "depth": "l2", "combined": "trade_l2"}.get(
                            args.data_type, args.data_type
                        ),
                    },
                )
                records = await cur.fetchall()
                presentation = {}
                if not args.as_json:
                    presentation["has_more"] = len(records) > args.limit
                    records = records[: args.limit]
                for record in records:
                    record["first_observed_utc"] = utc(record["start_timestamp_ns"])
                    record["last_observed_utc"] = utc(record["end_timestamp_ns"])
                return {
                    **presentation,
                    "datasets": records,
                    "limit": args.limit,
                    "offset": args.offset,
                    "interval_semantics": "inclusive observed envelopes, query end exclusive; continuity not established",
                }
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
                }
            if action == "streams":
                return {
                    "id": args.id,
                    "scope": row["summary"]["membership_scope"],
                    "streams": row["summary"]["memberships"],
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
                }
            if action == "schema":
                schema = await disk(parquet.schema, [file])
                return {
                    "id": args.id,
                    "columns": [
                        {"name": field.name, "type": str(field.type), "nullable": field.nullable}
                        for field in schema
                    ],
                }
            if action == "head":
                from itertools import islice

                def read() -> list[dict[str, Any]]:
                    batches = parquet.batches([file], columns=args.column, batch_size=args.limit)
                    return [row for batch in islice(batches, 1) for row in batch.to_pylist()]

                return {"id": args.id, "rows": await disk(read)}
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


def run(args: argparse.Namespace, context: ApplicationContext) -> int:
    try:
        result = asyncio.run(browse(args, context))
    except (ImportError, KeyError, ValueError) as error:
        raise MarketForgeError(str(error)) from error
    if args.as_json:
        print(json.dumps(result, default=str))
        return int(bool(result.get("conflict_count")))
    console = context.console
    if console.color_system is not None and (not console.file.isatty() or console.is_dumb_terminal):
        # Dataset output can be redirected independently of interactive color preferences.
        settings = context.settings.presentation.model_copy(update={"color": "never"})
        console = create_console(settings, stream=console.file)
    if args.dataset_action == "list":
        render_datasets(console, result, wide=args.wide, precise=args.precise)
    elif args.dataset_action == "coverage":
        render_coverage(console, result, filters=vars(args))
    else:
        console.print_json(json.dumps(result, default=str))
    return int(bool(result.get("conflict_count")))
