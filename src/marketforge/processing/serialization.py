from __future__ import annotations

from pathlib import Path

from marketforge.processing.models import ProcessingJob


def write_processing_job(
    job: ProcessingJob,
    path: Path,
) -> Path:
    """Serialize an immutable processing job for the Rust engine."""

    path = path.resolve()

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    path.write_text(
        job.model_dump_json(
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    return path
