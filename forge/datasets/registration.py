"""File identities and shared evidence in the historical registry."""

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from psycopg.types.json import Jsonb

from forge.historical.archives.storage.files import disk
from forge.shared.errors import MarketForgeError


def file_summaries(evidence: dict[str, Any]) -> list[dict[str, Any]]:
    manifest = evidence["manifest"]
    normalize = evidence["operation"] == "normalize"
    tasks = {task["task_id"]: task for task in evidence["resolved_job"].get("tasks", [])}
    streams = {s["directory"]: s for s in manifest.get("streams", []) if "directory" in s}
    offsets: dict[str, int] = defaultdict(int)
    summaries = []
    for entry in sorted(manifest["files"], key=lambda item: item["path"]):
        name = entry["path"]
        directory = str(Path(name).parent)
        stream = streams.get(directory)
        start = offsets[directory]
        end = start + entry["rows"]
        offsets[directory] = end
        ids = set()
        if stream:
            for source in stream["sources"]:
                ranges = source.get("ranges") or [source]
                if any(r["start_row"] < end and r["end_row"] > start for r in ranges):
                    ids.add(source["task_id"])
        elif normalize and Path(name).parts[0] == "tasks":
            ids.add(int(Path(name).parts[1]))
        memberships = {}
        for task_id in sorted(ids):
            task = tasks.get(task_id)
            if task is None:
                raise MarketForgeError(f"Unknown source task {task_id} in {name}.")
            instrument = (
                task.get("raw_schema", {}).get("marketforge_source", {}).get("instrument") or {}
            )
            membership = {
                "stream_id": stream["stream_id"] if stream else task["stream_id"],
                "exchange": task["exchange"],
                "instrument_id": task["instrument_id"],
                "symbol": task["symbol"],
                "instrument_type": instrument.get("instrument_type"),
                "market_category": instrument.get("market_category"),
                "start_timestamp_ns": entry["start_timestamp_ns"],
                "end_timestamp_ns": entry["end_timestamp_ns"],
            }
            memberships[tuple(membership.values())] = membership
        file_members = evidence.get("file_memberships", {}).get(name, list(memberships.values()))
        kinds = {member.get("kind") for member in file_members}
        content = (
            {"trade": "trades", "depth": "depth"}.get(next(iter(kinds)))
            if len(kinds) == 1
            else None
        )
        summaries.append(
            {
                **entry,
                "summary_version": 1,
                "content_type": content
                or evidence.get("content_type", manifest.get("content_type")),
                "memberships": file_members,
                "membership_scope": "file" if file_members else "unresolved",
                "integrity": manifest.get("integrity_status", "physical_validation"),
                "integrity_scope": "publication",
                "contributing_dataset_ids": sorted(
                    {
                        member["input_file_dataset_id"]
                        for member in file_members
                        if member.get("input_file_dataset_id") is not None
                    }
                ),
                "source_paths": sorted({tasks[tid]["input_path"] for tid in ids}),
            }
        )
    return summaries


def merged_file_memberships(
    root: Path,
    manifest: dict[str, Any],
    inputs: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Read only provenance and time columns in bounded batches at registration."""
    from forge.datasets.parquet import batches

    source_members = {
        (item["engine_id"], member["stream_id"]): member
        for item in inputs
        for member in item["memberships"]
    }
    selected_files: dict[int, set[int]] = defaultdict(set)
    for item in inputs:
        selected_files[item["engine_id"]].add(item["database_id"])
    streams = {s["stream_rank"]: s for s in manifest["streams"]}
    results = {}
    for file in manifest["files"]:
        observed: dict[int, tuple[int, int]] = {}
        for batch in batches([root / file["path"]], columns=["stream_rank", "event_timestamp_ns"]):
            import pyarrow as pa

            grouped = (
                pa.Table.from_batches([batch])
                .group_by("stream_rank", use_threads=False)
                .aggregate([("event_timestamp_ns", "min"), ("event_timestamp_ns", "max")])
            )
            for rank, lo, hi in zip(
                *(column.to_pylist() for column in grouped.columns), strict=True
            ):
                start, end = observed.get(rank, (lo, hi))
                observed[rank] = (min(start, lo), max(end, hi))
        memberships = []
        for rank, (start, end) in sorted(observed.items()):
            s = streams[rank]
            identity = source_members.get((s["dataset_id"], s["stream_id"]), {})
            candidates = selected_files[s["dataset_id"]]
            # This rank proves contribution from the sole selected file, when
            # the publication was selected through exactly one primary part.
            contributor = next(iter(candidates)) if len(candidates) == 1 else None
            memberships.append(
                {
                    "stream_id": s["stream_id"],
                    "kind": s.get("kind"),
                    "input_file_dataset_id": contributor,
                    "exchange": identity.get("exchange", s["exchange"]),
                    "symbol": s["symbol"],
                    "instrument_id": s["instrument_id"],
                    "instrument_type": identity.get("instrument_type"),
                    "market_category": identity.get("market_category"),
                    "start_timestamp_ns": start,
                    "end_timestamp_ns": end,
                }
            )
        results[file["path"]] = memberships
    return results


async def register_files(
    c: AsyncConnection[TupleRow],
    evidence: dict[str, Any],
    *,
    execution: dict[str, Any] | None = None,
    legacy_id: int | None = None,
    raw: set[tuple[int, str]] | None = None,
    parents: list[int] | None = None,
) -> dict[str, Any]:
    namespace, engine = evidence["instrument_namespace"], evidence["engine_dataset_id"]
    root, operation = evidence["path"], evidence["operation"]
    raw = raw or set()
    await c.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (f"{namespace}:{engine}",)
    )
    shared = {
        **{
            k: v
            for k, v in evidence.items()
            if k not in {"selected_primary", "expected_raw_sha256"}
        },
        "execution": execution,
        "selected_raw_inputs": sorted(raw),
        "selected_dataset_inputs": parents or [],
    }
    cur = await c.execute(
        """INSERT INTO historical.dataset_publications
        (instrument_namespace,engine_dataset_id,operation,storage_root,manifest_path,evidence)
        VALUES(%s,%s,%s,%s,%s,%s)
        ON CONFLICT(instrument_namespace,engine_dataset_id) DO NOTHING RETURNING id""",
        (namespace, engine, operation, root, evidence["manifest_path"], Jsonb(shared)),
    )
    row = await cur.fetchone()
    if row:
        publication = row[0]
    else:
        cur = await c.execute(
            """SELECT id,evidence FROM historical.dataset_publications
            WHERE instrument_namespace=%s AND engine_dataset_id=%s""",
            (namespace, engine),
        )
        previous = await cur.fetchone()
        assert previous is not None
        publication = previous[0]
        for key, value in shared.items():
            if (
                key != "execution"
                and key in previous[1]
                and json.loads(json.dumps(value)) != previous[1][key]
            ):
                raise MarketForgeError(f"Publication {engine} conflicts on {key}.")
    if legacy_id is None:
        cur = await c.execute(
            """SELECT d.id FROM historical.datasets d
            LEFT JOIN historical.dataset_registration_migrations m ON m.legacy_dataset_id=d.id
            WHERE d.publication_id IS NULL AND m.legacy_dataset_id IS NULL AND
            d.operation_details->>'instrument_namespace'=%s AND
            d.operation_details->>'engine_dataset_id'=%s""",
            (namespace, engine),
        )
        if await cur.fetchone():
            raise MarketForgeError(
                "Migrate the existing aggregate registration before registering its files."
            )
    results = {}
    states = {}
    selected = evidence.get("selected_primary")
    for summary in file_summaries(evidence):
        relative = summary["path"]
        if selected is not None and relative != selected:
            continue
        digest = evidence["inventory_sha256"][relative]
        filepath = str(Path(root) / relative)
        cur = await c.execute(
            """INSERT INTO historical.datasets(data_type,start_timestamp_ns,end_timestamp_ns,
            storage_format,storage_path,status,completed_at,operation,storage_root_key,manifest_path,
            operation_details,publication_id,primary_file,file_sha256)
            VALUES(%s,%s,%s,'parquet',%s,'complete',statement_timestamp(),%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(publication_id,primary_file) WHERE primary_file IS NOT NULL
            DO NOTHING RETURNING id""",
            (
                {"trades": "trade", "depth": "l2", "combined": "trade_l2"}[
                    evidence["content_type"]
                ],
                summary["start_timestamp_ns"],
                summary["end_timestamp_ns"],
                filepath,
                operation,
                root,
                evidence["manifest_path"],
                Jsonb(summary),
                publication,
                relative,
                digest,
            ),
        )
        row = await cur.fetchone()
        if row:
            dataset = row[0]
            states[filepath] = "new"
            # Exact contributions only when manifest row ownership identifies the source.
            task_paths = {str(await disk(Path(path).resolve)) for path in summary["source_paths"]}
            raw_by_path = evidence.get("raw_links_by_path", {})
            for archive_id, sha in sorted(
                {tuple(raw_by_path[p]) for p in task_paths if p in raw_by_path}
            ):
                await c.execute(
                    """INSERT INTO historical.dataset_raw_inputs(dataset_id,local_archive_id,input_sha256)
                    VALUES(%s,%s,%s)""",
                    (dataset, archive_id, sha),
                )
            for parent in summary["contributing_dataset_ids"]:
                if parent not in (parents or []):
                    raise MarketForgeError(
                        "File contribution is missing from the publication selection."
                    )
                await c.execute(
                    "INSERT INTO historical.dataset_inputs(dataset_id,input_dataset_id) VALUES(%s,%s)",
                    (dataset, parent),
                )
        else:
            cur = await c.execute(
                """SELECT id,file_sha256,storage_path FROM historical.datasets
                WHERE publication_id=%s AND primary_file=%s""",
                (publication, relative),
            )
            row = await cur.fetchone()
            if row is None or row[1:] != (digest, filepath):
                raise MarketForgeError(
                    f"File identity conflicts with registered evidence: {relative}"
                )
            dataset = row[0]
            states[filepath] = "reused"
        results[filepath] = dataset
    if not results:
        raise MarketForgeError("Requested file is not a primary manifest member.")
    if legacy_id is not None:
        await c.execute(
            """INSERT INTO historical.dataset_registration_migrations VALUES(%s,%s)
            ON CONFLICT(legacy_dataset_id) DO NOTHING""",
            (legacy_id, publication),
        )
    return {"publication_id": publication, "files": results, "file_states": states}
