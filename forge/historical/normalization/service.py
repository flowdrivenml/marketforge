"""Generate deterministic JSON with read-only database access and atomic publication."""

import json
import os
import tempfile
from pathlib import Path

from forge.historical.archives.storage.files import publish
from forge.historical.normalization.config import PreparationRequest
from forge.historical.normalization.resolution import require
from forge.shared.config.models import HttpSettings
from forge.shared.paths import ProjectPaths


def default_output_paths(paths: ProjectPaths, job_id: int, dataset_id: int) -> tuple[Path, Path]:
    default = paths.data_root / "test" / "normalized"
    staging = (default / f".{dataset_id}.staging.{job_id}").resolve()
    dataset = (default / str(dataset_id)).resolve()
    for path in (staging, dataset):
        require(not path.exists() and not path.is_symlink(), f"Output already exists: {path}")
    return staging, dataset


def output_paths(paths: ProjectPaths, request: PreparationRequest) -> tuple[Path, Path]:
    if request.output is None:
        staging, dataset = default_output_paths(paths, request.job_id, request.dataset_id)
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
    paths: ProjectPaths,
    request: PreparationRequest,
    *,
    dry_run: bool = False,
    http: HttpSettings | None = None,
) -> dict[str, object]:
    """Legacy configurations remain readable; all new artifacts use Forge batches."""
    from forge.historical.normalization.batch import prepare_batch

    return await prepare_batch(paths, request, dry_run=dry_run, http=http)
