"""Indexed run history ordering."""

from forge.shared.sorting import SortField, SortSpec

RUNS = SortSpec(
    {
        "start": SortField("run start", "desc", ("started_at",)),
        "end": SortField("run finish", "desc", ("finished_at",)),
        "duration": SortField("recorded duration", "desc", ("finished_at - started_at",)),
        "id": SortField("run ID", columns=("run_id",)),
        "operation": SortField("operation", columns=("operation",)),
        "action": SortField("action", columns=("action",)),
        "status": SortField("recorded status", columns=("status",)),
        "recipe": SortField("recipe", columns=("recipe",)),
    },
    "start",
    "run_id",
    {"time": "start", "started_at": "start", "finished_at": "end"},
)
