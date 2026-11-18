"""Nonprocessing identity discovery, delegated to production Rust family resolution."""

import json
from pathlib import Path
from typing import Any

from forge.historical.archives.storage.files import contained
from forge.historical.engine.managed import checksum, resolve, supervised
from forge.historical.metadata.policy import NAMESPACE
from forge.historical.normalization.service import write_job
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


def discover(
    paths: ProjectPaths,
    row: dict[str, Any],
    reference: dict[str, Any],
    family_id: int,
    operation: Path,
) -> dict[str, Any]:
    path = contained(paths.raw_dir, row["relative_path"])
    before = path.stat()
    if (
        not path.is_file()
        or path.is_symlink()
        or before.st_size != row["bytes_written"]
        or before.st_size > 256 * 1024 * 1024
        or checksum(path) != row["sha256"]
    ):
        raise MarketForgeError(
            "Family identity discovery requires an unchanged verified managed copy <=256 MiB."
        )
    family = {
        "family_id": family_id,
        "exchange": row["exchange"],
        "instrument_type": row["instrument_type"],
        "market_category": row["market_category"],
        "identifier": row["scope_identifier"],
        "base_asset": reference["base_asset"],
        "quote_asset": reference["quote_asset"],
        "instrument": {
            "tick_size": str(reference["tick_size"]),
            "contract_value": str(reference["contract_value"])
            if reference.get("contract_value") is not None
            else None,
            "contract_value_asset": reference.get("contract_value_asset"),
        },
    }
    source = operation / f"identity-{row['id']}.json"
    batch: dict[str, object] = {
        "schema_version": 4,
        "kind": "normalization_batch",
        "status": "prepared",
        "run_id": operation.name,
        "run_directory": str(operation),
        "batch_path": str(source),
        "ranking_snapshot": None,
        "ranking_policy": "identity-discovery-only",
        "requested_interval": {
            "start": row["start_at"].isoformat(),
            "end": row["end_at"].isoformat(),
        },
        "instrument_namespace": NAMESPACE,
        "instruments": [],
        "families": [family],
        "jobs": [
            {
                "operation": "normalize",
                "job_id": 1,
                "dataset_id": 1,
                "tasks": [
                    {
                        "task_id": 1,
                        "input_path": str(path.resolve()),
                        "family_id": family_id,
                        "instrument_selection": {"mode": "all"},
                        "stream_rank": 1,
                    }
                ],
                "ordering": {"primary": "event_timestamp_ns", "tie_break": "stream_rank"},
                "output": {
                    "content_type": "trades" if row["data_type"] == "trades" else "depth",
                    "dataset_path": str(operation / f"uncreated-dataset-{row['id']}"),
                    "staging_path": str(operation / f"uncreated-staging-{row['id']}"),
                },
            }
        ],
    }
    write_job(source, batch)
    result = supervised(
        [str(resolve(paths)), "--resolve-batch", str(source), "--project-root", str(paths.root)],
        cwd=paths.root,
        timeout=180,
    )
    if result.returncode:
        raise MarketForgeError("Rust family identity resolution failed: " + result.stdout[-6000:])
    resolved = json.loads(result.stdout)
    if (
        resolved.get("kind") != "normalization_resolution"
        or resolved.get("normalization_executed") is not False
    ):
        raise MarketForgeError(
            "Installed engine lacks the nonprocessing family resolution contract."
        )
    after = path.stat()
    if any(
        getattr(after, field) != getattr(before, field)
        for field in ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
    ):
        raise MarketForgeError("Managed copy changed during identity discovery.")
    symbols = sorted({item["symbol"] for item in resolved["instruments"]})
    if not symbols:
        raise MarketForgeError("Archive has no resolved members of its declared family.")
    return {
        "symbols": symbols,
        "members": resolved["members"],
        "method": "archive_derived",
        "archive_population_complete": True,
        "historical_family_complete": False,
        "normalization_executed": False,
    }
