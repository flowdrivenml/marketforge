"""Sort capabilities for the dataset inventory projections."""

from typing import LiteralString

from forge.shared.sorting import SortField, SortSpec

STAGE: LiteralString = "CASE operation WHEN 'normalize' THEN 0 WHEN 'merge' THEN 1 ELSE 2 END"
TIME = {
    "start": SortField("observed start", "desc", ("start_timestamp_ns",)),
    "end": SortField("observed end", "desc", ("end_timestamp_ns",)),
}
TIME_ALIASES = {"time": "start", "observed": "start"}
VIEWS = {
    "publications": SortSpec(
        {
            **TIME,
            "stage": SortField("stage", columns=(STAGE,)),
            "id": SortField("registry publication ID", columns=("publication_id",)),
            "size": SortField("recorded size", "desc", ("size_bytes",)),
            "files": SortField("primary file count", "desc", ("file_count",)),
        },
        "start",
        "publication_id ASC NULLS LAST, operation, ref",
        TIME_ALIASES,
    ),
    "files": SortSpec(
        {
            **TIME,
            "stage": SortField("stage", columns=(STAGE,)),
            "id": SortField("primary file ID", columns=("id",)),
            "size": SortField("recorded size", "desc", ("size_bytes",)),
        },
        "start",
        "id ASC NULLS LAST, operation, ref, part",
        TIME_ALIASES,
    ),
    "timelines": SortSpec(
        {
            **TIME,
            "stage": SortField("stage", columns=(STAGE,)),
            "id": SortField("timeline identity", columns=(STAGE, "exchange", "symbol")),
            "exchange": SortField("exchange", columns=("exchange", "symbol")),
            "instrument": SortField("instrument", columns=("symbol", "exchange")),
            "publications": SortField("publication count", "desc", ("publication_count",)),
        },
        "start",
        "identity, segment",
        TIME_ALIASES,
    ),
    "coverage": SortSpec(
        {
            **TIME,
            "stage": SortField("stage", columns=(STAGE,)),
            "identity": SortField("coverage identity", columns=("operation", "exchange", "symbol")),
            "exchange": SortField("exchange", columns=("exchange",)),
            "instrument": SortField("instrument", columns=("symbol",)),
            "files": SortField("supporting files", "desc", ("supporting_files",)),
            "publications": SortField("supporting outputs", "desc", ("supporting_outputs",)),
        },
        "identity",
        "operation,exchange,symbol,instrument_namespace,instrument_id,instrument_type,"
        "market_category,data_type,target_quote,variant,unresolved_namespace_output",
        TIME_ALIASES,
    ),
}
LEGACY_FILES = SortSpec(
    {
        **TIME,
        "stage": SortField(
            "stage",
            columns=("CASE d.operation WHEN 'normalize' THEN 0 WHEN 'merge' THEN 1 ELSE 2 END",),
        ),
        "id": SortField("primary file ID", columns=("d.id",)),
        "size": SortField(
            "recorded size",
            "desc",
            (
                "CASE WHEN d.operation_details->>'size_bytes' ~ '^[0-9]+$' THEN (d.operation_details->>'size_bytes')::numeric END",
            ),
        ),
    },
    "start",
    "d.id",
    TIME_ALIASES,
)

STREAMS = SortSpec(
    {
        "name": SortField("stream ID"),
        "rank": SortField("retained stream rank"),
        "start": SortField("observed start", "desc"),
        "end": SortField("observed end", "desc"),
    },
    "name",
    aliases={**TIME_ALIASES, "id": "name"},
)


REGISTERED_PUBLICATIONS = SortSpec(
    {
        "start": SortField("observed start", "desc", ("files.earliest",)),
        "end": SortField("observed end", "desc", ("files.latest",)),
        "id": SortField("registry publication ID", columns=("p.id",)),
        "engine": SortField(
            "Engine publication ID",
            columns=(
                "CASE WHEN p.engine_dataset_id ~ '^[0-9]+$' THEN p.engine_dataset_id::numeric END",
            ),
        ),
        "size": SortField("recorded size", "desc", ("files.complete_bytes",)),
        "files": SortField("primary file count", "desc", ("files.primary_file_count",)),
        "stage": SortField(
            "stage",
            columns=("CASE p.operation WHEN 'normalize' THEN 0 WHEN 'merge' THEN 1 ELSE 2 END",),
        ),
        "operation": SortField("operation", columns=("p.operation",)),
    },
    "start",
    "p.id",
    {
        **TIME_ALIASES,
        "Publication": "engine",
        "Recorded size": "size",
        "Files": "files",
        "Observed UTC": "end",
        "Operation": "operation",
    },
)
