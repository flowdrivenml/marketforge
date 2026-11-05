"""Protocol-v3 wire boundary, verified against current Rust job definitions.

No profile compilation or fingerprint algorithm is duplicated here. Rust compiles
profiles and validates runtime resources when the user later invokes the engine.
"""

from pathlib import Path

from forge.historical.normalization.config import PreparationRequest


def job_document(
    request: PreparationRequest,
    tasks: list[dict[str, object]],
    streams: list[dict[str, object]],
    staging: Path,
    dataset: Path,
) -> dict[str, object]:
    return {
        "protocol_version": 3,
        "operation": "normalize",
        "job_id": request.job_id,
        "dataset_id": request.dataset_id,
        "tasks": tasks,
        "streams": streams,
        "ordering": {"primary": "event_timestamp_ns", "tie_break": "stream_rank"},
        "output": {
            "content_type": request.content_type,
            "staging_path": str(staging),
            "dataset_path": str(dataset),
        },
    }
