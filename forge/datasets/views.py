"""Set-based browsing over registry summaries and verified conversion metadata."""

from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING, Any, LiteralString, cast

from psycopg import AsyncConnection
from psycopg.rows import TupleRow, dict_row
from psycopg.types.json import Jsonb

from forge.datasets import timelines
from forge.datasets.converted import indexed_outputs
from forge.shared.database.connection import database_purpose, registry_id
from forge.shared.errors import MarketForgeError

if TYPE_CHECKING:
    from forge.datasets.query import DatasetRequest

# Each unit is a registered primary file or an indexed conversion output. Conversion
# membership is known at output scope, not at individual converted file scope.
BASE = """
, units AS MATERIALIZED (
    SELECT CASE WHEN %(timeline)s THEN jsonb_build_object('integrity',f.summary->'integrity')
                ELSE to_jsonb(f) END AS obj, f.id, f.publication_id, f.publication_id::text AS ref,
           f.engine_dataset_id, f.operation, f.instrument_namespace AS namespace,
           f.status, f.data_type, f.start_timestamp_ns AS lo, f.end_timestamp_ns AS hi,
           NULL::text AS target_quote,
           CASE WHEN f.summary->>'size_bytes' ~ '^[0-9]+$'
                THEN (f.summary->>'size_bytes')::numeric END AS bytes,
           1::bigint AS file_count,
           CASE WHEN f.summary->>'size_bytes' ~ '^[0-9]+$' THEN 0 ELSE 1 END AS unknown_sizes,
           f.summary->'memberships' AS members, f.parent_namespaces,
           f.representation, f.requested
    FROM registered f
    WHERE %(inspect)s OR %(status)s='superseded'
        OR (NOT %(timeline)s AND %(status)s::text IS NOT NULL)
        OR (f.status <> 'superseded' AND f.canonical_publication_id IS NULL)
    UNION ALL
    SELECT x, NULL, NULL, x->>'output_ref', x->>'engine_dataset_id', 'convert',
           x->>'instrument_namespace', x->>'status', x->>'data_type',
           (x->>'start_timestamp_ns')::bigint, (x->>'end_timestamp_ns')::bigint,
           x->>'target_quote', files.bytes, files.count, files.unknown_sizes,
           x#>'{summary,memberships}', NULL::jsonb, x->'representation', NULL::jsonb
    FROM jsonb_array_elements(%(converted)s) x
    CROSS JOIN LATERAL (
        SELECT count(*) AS count, sum((f->>'size_bytes')::numeric) AS bytes,
               count(*) - count(f->>'size_bytes') AS unknown_sizes
        FROM jsonb_array_elements(x->'files') f
    ) files
), eligible AS MATERIALIZED (
    SELECT * FROM units u WHERE (%(operation)s::text IS NULL OR operation=%(operation)s)
      AND (%(status)s::text IS NOT NULL AND status=%(status)s
           OR %(status)s::text IS NULL AND (%(inspect)s OR status='complete'))
      AND (%(publication_id)s::bigint IS NULL OR publication_id=%(publication_id)s)
      AND (%(engine_publication)s::text IS NULL OR engine_dataset_id=%(engine_publication)s)
      AND (%(output_ref)s::text IS NULL OR operation='convert' AND ref=%(output_ref)s)
      AND (%(target_quote)s::text IS NULL OR target_quote=%(target_quote)s)
      AND (%(start)s::bigint IS NULL OR hi >= %(start)s)
      AND (%(end)s::bigint IS NULL OR lo < %(end)s)
), matched AS MATERIALIZED (
    SELECT u.*, selected.members AS matched_members,
           coalesce(selected.trades,u.data_type IN ('trade','trade_l2')) AS matches_trades,
           coalesce(selected.depth,u.data_type IN ('l2','trade_l2')) AS matches_depth
    FROM eligible u
    CROSS JOIN LATERAL (
        SELECT jsonb_agg(entry.m || jsonb_build_object('kind', entry.kind)) AS members,
               bool_or(entry.kind='trade') AS trades, bool_or(entry.kind='l2') AS depth
        FROM (
            SELECT member.m,
                   CASE COALESCE(member.m->>'kind', member.m->>'data_type',u.data_type)
                       WHEN 'trade' THEN 'trade' WHEN 'trades' THEN 'trade'
                       WHEN 'l2' THEN 'l2' WHEN 'depth' THEN 'l2'
                       WHEN 'order_book_l2' THEN 'l2' END AS kind,
                   COALESCE(member.m->>'observed_start_ns',member.m->>'start_timestamp_ns') AS first_ns,
                   COALESCE(member.m->>'observed_end_ns',member.m->>'end_timestamp_ns') AS last_ns
            FROM jsonb_array_elements(
                CASE WHEN jsonb_typeof(u.members)='array' THEN u.members ELSE '[]'::jsonb END
            ) member(m)
        ) entry
        WHERE (%(exchange)s::text IS NULL OR lower(entry.m->>'exchange')=lower(%(exchange)s))
          AND (%(instrument)s::text IS NULL OR lower(entry.m->>'symbol')=lower(%(instrument)s)
               OR entry.m->>'instrument_id'=%(instrument)s)
          AND (%(instrument_type)s::text IS NULL OR entry.m->>'instrument_type'=%(instrument_type)s)
          AND (%(market_category)s::text IS NULL OR entry.m->>'market_category'=%(market_category)s)
          AND (%(kind)s::text IS NULL OR %(combined_publication)s OR entry.kind=%(kind)s
               OR %(kind)s='trade_l2' AND u.data_type='trade_l2')
          AND (%(start)s::bigint IS NULL OR entry.last_ns::bigint >= %(start)s)
          AND (%(end)s::bigint IS NULL OR entry.first_ns::bigint < %(end)s)
          AND (u.operation <> 'convert' OR
               (u.lo IS NULL OR entry.last_ns IS NULL OR entry.last_ns::bigint >= u.lo)
               AND (u.hi IS NULL OR entry.first_ns IS NULL OR entry.first_ns::bigint <= u.hi))
          AND (%(pattern)s::text IS NULL OR u.ref ILIKE %(pattern)s
               OR u.engine_dataset_id ILIKE %(pattern)s OR u.id::text ILIKE %(pattern)s
               OR entry.m->>'symbol' ILIKE %(pattern)s OR entry.m->>'exchange' ILIKE %(pattern)s
               OR entry.m->>'stream_id' ILIKE %(pattern)s OR u.target_quote ILIKE %(pattern)s)
    ) selected
    WHERE selected.members IS NOT NULL OR (
        (jsonb_typeof(u.members) IS DISTINCT FROM 'array' OR u.members='[]'::jsonb)
        AND %(exchange)s::text IS NULL AND %(instrument)s::text IS NULL
        AND %(instrument_type)s::text IS NULL AND %(market_category)s::text IS NULL
        AND (%(kind)s::text IS NULL OR %(combined_publication)s OR %(kind)s=u.data_type)
        AND (%(pattern)s::text IS NULL OR u.ref ILIKE %(pattern)s
             OR u.engine_dataset_id ILIKE %(pattern)s OR u.id::text ILIKE %(pattern)s
             OR u.target_quote ILIKE %(pattern)s)
    )
), output_matches AS (
    SELECT operation,ref,sum(file_count)::bigint AS matching_file_count
    FROM matched GROUP BY operation, ref
    HAVING NOT %(combined_publication)s OR bool_or(matches_trades) AND bool_or(matches_depth)
), stage_totals AS (
    SELECT operation,count(*) AS publications,sum(matching_file_count)::bigint AS files
    FROM output_matches GROUP BY operation
)
"""

PUBLICATIONS = """
, inventory AS (
    SELECT u.operation,u.ref, min(u.publication_id) AS publication_id,
           min(u.engine_dataset_id) AS engine_dataset_id,min(u.namespace) AS instrument_namespace,
           min(u.target_quote) AS target_quote, min(u.lo) AS start_timestamp_ns,
           max(u.hi) AS end_timestamp_ns, sum(u.bytes) AS known_bytes,
           CASE WHEN sum(u.unknown_sizes)=0 THEN sum(u.bytes) END AS size_bytes,
           sum(u.unknown_sizes)::bigint AS unknown_size_files,
           sum(u.file_count)::bigint AS file_count,
           CASE WHEN count(DISTINCT u.status)=1 THEN min(u.status) ELSE 'mixed' END AS status,
           CASE WHEN count(DISTINCT coalesce(u.obj#>>'{summary,integrity}',u.obj->>'integrity','unknown'))=1
                THEN min(coalesce(u.obj#>>'{summary,integrity}',u.obj->>'integrity','unknown'))
                ELSE 'mixed' END AS integrity,
           CASE WHEN bool_or(u.data_type IN ('trade','trade_l2'))
                  AND bool_or(u.data_type IN ('l2','trade_l2')) THEN 'trade_l2'
                WHEN count(DISTINCT u.data_type)=1 THEN min(u.data_type) END AS data_type,
           min(a.matching_file_count) AS matching_file_count,
           count(*) FILTER (WHERE jsonb_typeof(u.members) IS DISTINCT FROM 'array'
                            OR u.members='[]'::jsonb) AS units_without_membership,
           min(u.obj->>'manifest_path') AS manifest_path,
           min(u.obj->>'manifest_sha256') AS manifest_sha256,
           min(u.obj->>'storage_root') AS storage_root,
           min(u.obj->>'registration') AS registration,
           (jsonb_agg(u.obj->'engine_job_id')->0) AS engine_job_id,
           min(u.obj->>'verification') AS verification,
           min((u.obj->>'canonical_publication_id')::bigint) AS canonical_publication_id,
           min(u.obj->>'supersession_state') AS supersession_state
    FROM units u JOIN output_matches a USING(operation,ref)
    GROUP BY u.operation,u.ref
), selected_page AS (
    SELECT *, row_number() OVER (ORDER BY {order}) AS position
    FROM inventory ORDER BY {order} LIMIT %(limit)s OFFSET %(offset)s
), page AS (
    SELECT p.*, CASE WHEN p.operation='convert' THEN p.ref END AS output_ref,
           (SELECT jsonb_agg(DISTINCT member) FROM units u
                CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(u.members)='array'
                    THEN u.members ELSE '[]'::jsonb END) member
                WHERE (u.operation,u.ref)=(p.operation,p.ref)) AS memberships,
           (SELECT jsonb_agg(DISTINCT member) FROM matched m
                CROSS JOIN LATERAL jsonb_array_elements(m.matched_members) member
                WHERE (m.operation,m.ref)=(p.operation,p.ref)) AS matching_memberships
    FROM selected_page p
)
"""

FILES = """
, inventory AS (
    SELECT m.id,m.operation,m.ref,m.publication_id,m.obj,m.target_quote,m.lo AS start_timestamp_ns,
           m.bytes AS size_bytes,m.matched_members AS matching_memberships,NULL::integer AS part
    FROM matched m WHERE m.id IS NOT NULL
    UNION ALL
    SELECT NULL,m.operation,m.ref,NULL,
           (m.obj-'files') || jsonb_build_object('size_bytes',f->'size_bytes','rows',f->'rows',
               'part',f->'part','storage_path',f->'storage_path','primary_file',f->'path',
               'start_timestamp_ns',NULL,'end_timestamp_ns',NULL),
           m.target_quote,NULL::bigint,(f->>'size_bytes')::numeric,m.matched_members,(f->>'part')::integer
    FROM matched m CROSS JOIN LATERAL jsonb_array_elements(m.obj->'files') f
    WHERE m.operation='convert'
), page AS (
    SELECT *, row_number() OVER (ORDER BY {order}, part) AS position
    FROM inventory ORDER BY {order}, part LIMIT %(limit)s OFFSET %(offset)s
)
"""

OBSERVATIONS = """
, observations AS (
    SELECT u.operation,u.ref,u.id,u.publication_id,u.engine_dataset_id,u.status,
           coalesce(u.obj#>>'{summary,integrity}',u.obj->>'integrity','unknown') AS integrity,
           u.requested,u.representation,m->>'stream_id' AS stream_id,CASE WHEN u.operation='merge' THEN u.parent_namespaces->>(m->>'source_publication_id')
                ELSE u.namespace END AS instrument_namespace,
           CASE WHEN u.operation='merge' AND
                u.parent_namespaces->>(m->>'source_publication_id') IS NULL
                THEN u.ref END AS unresolved_namespace_output,
           u.target_quote,
           CASE WHEN u.operation='convert' THEN u.ref END AS variant,
           m->>'exchange' AS exchange,m->>'symbol' AS symbol,m->>'instrument_id' AS instrument_id,
           m->>'instrument_type' AS instrument_type,m->>'market_category' AS market_category,
           m->>'kind' AS data_type,
           CASE WHEN u.operation='convert'
                THEN CASE WHEN member_bounds.first_ns IS NOT NULL THEN greatest(member_bounds.first_ns,u.lo) END
                ELSE member_bounds.first_ns END AS lo,
           CASE WHEN u.operation='convert'
                THEN CASE WHEN member_bounds.last_ns IS NOT NULL THEN least(member_bounds.last_ns,u.hi) END
                ELSE member_bounds.last_ns END AS hi
    FROM matched u CROSS JOIN LATERAL jsonb_array_elements(u.matched_members) m
    CROSS JOIN LATERAL (
        SELECT coalesce(m->>'observed_start_ns',m->>'start_timestamp_ns')::bigint AS first_ns,
               coalesce(m->>'observed_end_ns',m->>'end_timestamp_ns')::bigint AS last_ns
    ) member_bounds
    WHERE m->>'kind' IS NOT NULL AND m->>'instrument_id' IS NOT NULL 
)
"""

COVERAGE = """
, inventory AS (
    SELECT operation,instrument_namespace,unresolved_namespace_output,target_quote,variant,
           exchange,instrument_id,max(symbol) AS symbol,instrument_type,market_category,data_type,
           min(lo) AS start_timestamp_ns,max(hi) AS end_timestamp_ns,
           count(DISTINCT id) AS supporting_files,count(DISTINCT ref) AS supporting_outputs
    FROM observations GROUP BY operation,instrument_namespace,unresolved_namespace_output,target_quote,variant,
        exchange,instrument_id,instrument_type,market_category,data_type
), page AS (
    SELECT *, row_number() OVER (ORDER BY {order}) AS position
    FROM inventory ORDER BY {order}
        LIMIT %(limit)s OFFSET %(offset)s
)
"""

ORDER: dict[str, LiteralString] = {
    "id": "id ASC NULLS LAST",
    "observed": "start_timestamp_ns DESC NULLS LAST",
    "size": "size_bytes DESC NULLS LAST",
    "stage": "CASE operation WHEN 'normalize' THEN 0 WHEN 'merge' THEN 1 ELSE 2 END",
}
COVERAGE_ORDER: LiteralString = (
    "operation,exchange,symbol,instrument_namespace,instrument_id,instrument_type,"
    "market_category,data_type,target_quote,variant,unresolved_namespace_output"
)


async def page(
    connection: AsyncConnection[TupleRow],
    args: DatasetRequest,
    *,
    inspect: bool = False,
    timeline_ref: timelines.TimelineReference | None = None,
) -> dict[str, Any]:
    from forge.datasets.query import COLUMNS, DATA_TYPES

    view = args.view or "publications"
    is_timeline = view == "timelines"
    registry = await registry_id(connection) if is_timeline else ""
    if timeline_ref and timeline_ref.registry != registry:
        raise MarketForgeError("This timeline reference belongs to a different registry.")
    include_converted = (
        args.operation in {None, "convert"}
        and args.publication_id is None
        and not args.publication
        and not args.engine_publication
    )
    converted, capability, unavailable = (
        await indexed_outputs(connection, args.output_ref)
        if include_converted and (timeline_ref is None or timeline_ref.output is not None)
        else ([], "outside_filter", [])
    )
    order: LiteralString
    fragment: LiteralString
    if is_timeline:
        order = timelines.SORTS[args.sort]
        fragment = OBSERVATIONS + timelines.SEGMENTS.replace(
            "{order}", order + ",identity,start_timestamp_ns NULLS LAST,segment"
        )
    elif view == "coverage":
        order = COVERAGE_ORDER
        fragment = OBSERVATIONS + COVERAGE.replace("{order}", order)
    elif view == "files":
        order = ORDER[args.sort]
        fragment = FILES.replace("{order}", order + ", operation, ref")
    else:
        order = ORDER[args.sort].replace("id ASC", "publication_id ASC")
        fragment = PUBLICATIONS.replace("{order}", order + ", operation, ref")
    pattern = (
        "%" + args.search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        if args.search
        else None
    )
    thin_columns: LiteralString = (
        "d.id,d.operation,d.data_type,d.status,d.start_timestamp_ns,d.end_timestamp_ns,"
        "jsonb_build_object('integrity',d.operation_details->'integrity',"
        "'memberships',d.operation_details->'memberships') AS summary,"
        "p.id AS publication_id,p.engine_dataset_id,p.instrument_namespace"
    )
    statistics: LiteralString = (
        ", (SELECT to_jsonb(t) FROM timeline_totals t) AS timeline_counts,"
        " (SELECT count(*) FROM contributing_outputs) AS matching_publications,"
        " (SELECT jsonb_object_agg(operation,n) FROM (SELECT operation,count(*) n"
        " FROM contributing_outputs GROUP BY operation) s) AS timeline_stages"
        if is_timeline
        else ""
    )
    async with connection.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            "WITH registered AS (SELECT "
            + (thin_columns if is_timeline else COLUMNS)
            + """,
                namespaces.items AS parent_namespaces,
                CASE WHEN %(timeline)s THEN jsonb_build_array(
                    p.evidence#>>'{manifest,protocol_version}',
                    p.evidence#>>'{manifest,physical_schema_version}',
                    p.evidence#>>'{manifest,timeline_ordering}',
                    p.evidence#>>'{manifest,processing_quality,timestamp_ordering}'
                ) END AS representation,
                CASE WHEN %(timeline)s AND d.operation='merge'
                     THEN p.evidence#>'{manifest,time_range}' END AS requested,
                supersession.canonical_id AS canonical_publication_id,
                supersession.state AS supersession_state
                FROM historical.datasets d JOIN historical.dataset_publications p
                ON d.publication_id=p.id
                LEFT JOIN historical.publication_supersessions supersession
                    ON supersession.publication_id=p.id
                LEFT JOIN LATERAL (
                    SELECT jsonb_object_agg(parent->>'engine_id',parent->>'namespace') AS items
                    FROM jsonb_array_elements(CASE WHEN d.operation='merge'
                        AND jsonb_typeof(p.evidence->'merge_inputs')='array'
                        THEN p.evidence->'merge_inputs' ELSE '[]'::jsonb END) parent
                    WHERE parent->>'engine_id' IS NOT NULL
                ) namespaces ON true
                WHERE d.primary_file IS NOT NULL
                  AND (%(operation)s::text IS NULL OR d.operation=%(operation)s)
                  AND (%(publication_id)s::bigint IS NULL OR p.id=%(publication_id)s)
                  AND (%(engine_publication)s::text IS NULL OR p.engine_dataset_id=%(engine_publication)s)
                  AND %(output_ref)s::text IS NULL AND %(target_quote)s::text IS NULL
                  AND (%(watermark)s::bigint IS NULL OR p.id<=%(watermark)s)
            )"""
            + BASE
            + fragment
            + """
            SELECT (SELECT count(*) FROM inventory) AS total,
                   (SELECT jsonb_agg(to_jsonb(p)-'position' ORDER BY position) FROM page p) AS items,
                   (SELECT jsonb_object_agg(operation,to_jsonb(s)-'operation') FROM stage_totals s)
                       AS stages
            """
            + statistics,
            {
                **asdict(args),
                "inspect": inspect,
                "converted": Jsonb(
                    [{k: v for k, v in item.items() if k != "indexed_result"} for item in converted]
                ),
                "kind": DATA_TYPES.get(args.data_type or ""),
                "combined_publication": view == "publications"
                and DATA_TYPES.get(args.data_type or "") == "trade_l2",
                "publication_id": None if is_timeline else args.publication_id,
                "engine_publication": None
                if is_timeline
                else args.engine_publication or args.publication,
                "timeline": is_timeline,
                "timeline_publication": args.publication_id,
                "timeline_engine": args.engine_publication or args.publication,
                "timeline_identity": timeline_ref.identity if timeline_ref else None,
                "timeline_lo": timeline_ref.lo if timeline_ref else None,
                "timeline_hi": timeline_ref.hi if timeline_ref else None,
                "watermark": timeline_ref.watermark if timeline_ref else None,
                "pattern": pattern,
            },
        )
        row = await cursor.fetchone()
    assert row is not None
    items: list[dict[str, Any]] = row["items"] or []
    if view == "files":
        items = [
            {
                **{
                    k: v
                    for k, v in cast(dict[str, Any], entry["obj"]).items()
                    if k != "parent_namespaces"
                },
                "matching_memberships": entry["matching_memberships"] or [],
            }
            for entry in items
        ]
    if is_timeline:
        for entry in items:
            fingerprint = timelines.publication_fingerprint(entry["publications"])
            if timeline_ref and timeline_ref.fingerprint != fingerprint:
                raise MarketForgeError(
                    "Timeline membership changed. Browse the current timelines for a new reference."
                )
            entry.update(zip(timelines.IDENTITY_FIELDS, entry.pop("identity"), strict=True))
            entry["coverage"] = (
                "unknown"
                if entry["segment"] == 0
                else "source_observed_extent"
                if entry["operation"] == "convert"
                else "observed_extent"
            )
            prior = entry.get("prior_observed_end_ns")
            start = entry["start_timestamp_ns"]
            entry["observed_separation"] = (
                {"after_ns": prior, "before_ns": start, "coverage": "unknown"}
                if prior is not None and start is not None and prior < start
                else None
            )
            entry["timeline_reference"] = (
                timeline_ref
                or timelines.TimelineReference(
                    registry,
                    entry["operation"],
                    entry["identity_key"],
                    entry["watermark"],
                    entry["start_timestamp_ns"],
                    entry["end_timestamp_ns"],
                    fingerprint,
                    args.status,
                    args.start,
                    args.end,
                    args.search,
                    entry["variant"],
                    [
                        args.exchange,
                        args.instrument,
                        args.instrument_type,
                        args.market_category,
                        args.data_type,
                    ],
                )
            ).encode()
        if timeline_ref and not items:
            raise MarketForgeError(
                "Timeline evidence is no longer selectable. Browse the current timelines for a new reference."
            )
    key = {
        "files": "datasets",
        "coverage": "coverage",
        "publications": "publications",
        "timelines": "timelines",
    }[view]
    return {
        "schema_version": 1,
        "view": view,
        "environment": database_purpose(),
        key: items,
        **(
            {
                "timeline_counts": row["timeline_counts"],
                "matching_publications": row["matching_publications"],
                "stage_publications": row["timeline_stages"] or {},
            }
            if is_timeline
            else {}
        ),
        "total": row["total"],
        "stages": row["stages"] or {},
        "limit": args.limit,
        "offset": args.offset,
        "has_more": args.offset + len(items) < row["total"],
        "conversion_index": capability,
        "unavailable_conversion_outputs": unavailable,
        "interval_semantics": "Recorded UTC bounds are inclusive. The filter end is exclusive. Continuity is unknown.",
    }
