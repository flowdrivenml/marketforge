"""Serialize prepared jobs using the production Rust schema-v4 batch contract."""

from pathlib import Path


def preparation_document(
    job_id: int,
    dataset_id: int,
    content_type: str,
    tasks: list[dict[str, object]],
    staging: Path,
    dataset: Path,
) -> dict[str, object]:
    """One prepared job; the enclosing batch owns its schema version."""
    return {
        "operation": "normalize",
        "job_id": job_id,
        "dataset_id": dataset_id,
        "tasks": tasks,
        "ordering": {"primary": "event_timestamp_ns", "tie_break": "stream_rank"},
        "output": {
            "content_type": content_type,
            "staging_path": str(staging),
            "dataset_path": str(dataset),
        },
    }
