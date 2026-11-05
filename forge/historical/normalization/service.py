"""Generate deterministic JSON with read-only database access and atomic publication."""

import json
import os
import shlex
import tempfile
from pathlib import Path

from forge.historical.archives.storage.files import publish
from forge.historical.normalization.config import PreparationRequest
from forge.historical.normalization.contract import job_document
from forge.historical.normalization.resolution import require, resolve_input
from forge.shared.database.testing import testing_connection, testing_dsn
from forge.shared.paths import ProjectPaths


def output_paths(paths: ProjectPaths, request: PreparationRequest) -> tuple[Path, Path]:
    default = paths.data_root / "test" / "normalized"
    if request.output is None:
        staging = default / f".{request.dataset_id}.staging.{request.job_id}"
        dataset = default / str(request.dataset_id)
    else:
        staging = paths.root / request.output.staging_path
        dataset = paths.root / request.output.dataset_path
    staging, dataset = staging.resolve(), dataset.resolve()
    require(staging != dataset, "Staging and dataset paths must be distinct.")
    require(staging.parent == dataset.parent, "Outputs must share a parent for atomic publication.")
    for path in (staging, dataset):
        require(not path.exists() and not path.is_symlink(), f"Output already exists: {path}")
    return staging, dataset


def write_job(destination: Path, document: dict[str, object]) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=destination.parent, prefix=".prepare-", delete=False
        ) as source:
            temporary = Path(source.name)
            json.dump(document, source, indent=2, ensure_ascii=False, allow_nan=False)
            source.write("\n")
            source.flush()
            os.fsync(source.fileno())
        publish(temporary, destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


async def prepare(
    paths: ProjectPaths, request: PreparationRequest, *, dry_run: bool = False
) -> dict[str, object]:
    destination = paths.normalization_jobs_dir / f"{request.job_id}.json"
    require(
        not destination.exists() and not destination.is_symlink(),
        f"Job already exists: {destination}",
    )
    staging, dataset = output_paths(paths, request)
    tasks: list[dict[str, object]] = []
    ranks: dict[str, int | None] = {}
    async with testing_connection(testing_dsn(paths.root)) as connection:
        async with connection.transaction():
            # Enforce the preparation boundary in PostgreSQL, in addition to using
            # exclusively read-only resolution queries. Never allocate identifiers.
            await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            for selection in sorted(request.inputs, key=lambda i: i.task_id):
                task, stream = await resolve_input(
                    connection, paths, selection, request.content_type
                )
                if stream in ranks:
                    require(
                        ranks[stream] == selection.stream_rank,
                        "Tasks in a stream must agree on stream_rank.",
                    )
                ranks[stream] = selection.stream_rank
                tasks.append(task)
    if len(ranks) == 1:
        ranks = {stream: 0 if rank is None else rank for stream, rank in ranks.items()}
    require(
        all(rank is not None for rank in ranks.values()),
        "Multi-stream jobs require explicit stream_rank values.",
    )
    require(len(set(ranks.values())) == len(ranks), "Stream ranks must be unique.")
    streams: list[dict[str, object]] = [
        {"stream_id": stream, "stream_rank": ranks[stream]} for stream in sorted(ranks)
    ]
    document = job_document(request, tasks, streams, staging, dataset)
    # Recheck output collisions immediately before publishing the job.
    output_paths(paths, request)
    if not dry_run:
        write_job(destination, document)
    executable = paths.bin_dir / "marketforge-normalize"
    invocation = shlex.join(
        [
            str(executable),
            "--project-root",
            str(paths.root),
            "--config",
            str(paths.config_dir / "historical" / "normalize.toml"),
            "--job",
            str(destination),
        ]
    )
    return {
        "schema_version": 1,
        "status": "prepared",
        "dry_run": dry_run,
        "job_path": str(destination),
        "job_id": request.job_id,
        "dataset_id": request.dataset_id,
        "identifiers": "Unreserved test identifiers; no database allocation or registration performed.",
        "task_count": len(tasks),
        "stream_count": len(streams),
        "staging_path": str(staging),
        "dataset_path": str(dataset),
        "engine_ready": executable.is_file() and os.access(executable, os.X_OK),
        "engine_invocation": invocation,
        "execution": "Not executed. Rust profile compilation and real-engine integration remain deferred.",
    }
