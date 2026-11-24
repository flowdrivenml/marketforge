"""Raw archive and nominal coverage ordering."""

from forge.shared.sorting import SortField, SortSpec

GROUP_FIELDS = (
    "origin",
    "external_source_id",
    "external_source_key",
    "exchange",
    "instrument_type",
    "market_category",
    "scope_kind",
    "scope_identifier",
    "data_type",
    "feed",
)

ARCHIVES = SortSpec(
    {
        "start": SortField("nominal archive start", "desc", ("start_at",)),
        "end": SortField("nominal archive end", "desc", ("end_at",)),
        "id": SortField("local archive ID", columns=("id",)),
        "relative_path": SortField("relative path", columns=("relative_path",)),
        "bytes_written": SortField("recorded bytes", "desc", ("bytes_written",)),
        "status": SortField("recorded status", columns=("status",)),
    },
    "start",
    "id",
    {"time": "start", "start_at": "start", "end_at": "end", "size": "bytes_written"},
)
COVERAGE = SortSpec(
    {
        "start": SortField("earliest nominal archive start", "desc", ("earliest",)),
        "end": SortField("latest nominal archive end", "desc", ("latest",)),
        "identity": SortField("archive scope identity", columns=GROUP_FIELDS),
        "size": SortField("recorded bytes", "desc", ("recorded_bytes",)),
        "files": SortField("local archive count", "desc", ("file_count",)),
    },
    "start",
    ", ".join(GROUP_FIELDS),
    {"time": "start", "start_at": "start", "end_at": "end", "bytes_written": "size"},
)
