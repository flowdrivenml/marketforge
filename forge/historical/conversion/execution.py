"""Submit pinned per-job documents through the converter's single-job interface."""

from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import ValidationError

from forge.datasets.discovery import read_object
from forge.datasets.parquet import footer
from forge.historical.conversion.contract import Conversion, Job
from forge.historical.conversion.planning import output_available
from forge.historical.conversion.service import file_pin, retained_collection, validate_prepared_job
from forge.historical.engine.managed import checksum, inventory, resolve
from forge.historical.normalization.execution import run_process, sync_submission
from forge.historical.normalization.service import write_job
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


def read_pin(
    pin: dict[str, Any], directory: Path, *, expected: str | None = None
) -> tuple[Path, bytes]:
    path = Path(pin["path"])
    if (
        path.parent != directory
        or path.resolve() != path
        or expected is not None
        and path.name != expected
    ):
        raise MarketForgeError(f"Invalid retained artifact path: {path}.")
    data = path.read_bytes()
    if pin["sha256"] != file_pin(path, data)["sha256"]:
        raise MarketForgeError(f"Retained artifact changed: {path}.")
    return path, data


def verify_output(job: Job, pinned: dict[str, Any]) -> dict[str, Any]:
    output = Path(job.output.dataset_path)
    if output.resolve() != output or output.with_suffix(".staging").exists():
        raise MarketForgeError("Converter did not commit an unaliased output directory.")
    manifest_path = output / "manifest.json"
    manifest = read_object(manifest_path)
    receipt = read_object(output / "execution.json")
    effective = read_object(output / "conversion-runtime.json")
    if Job.model_validate(effective["job"]) != job or effective["runtime"] != {
        "workers": job.resources.workers,
        "parquet": job.parquet.model_dump(),
    }:
        raise MarketForgeError("Effective conversion job or runtime differs from the submission.")
    stats, out, integrity = (manifest[key] for key in ("statistics", "output", "integrity"))
    if (
        (manifest["protocol_version"], manifest["conversion_schema_version"], receipt["version"])
        != (1, 1, 1)
        or manifest["job_id"] != job.job_id
        or receipt["job_id"] != job.job_id
        or manifest["input"]
        != {
            "dataset_path": job.input_merge,
            "merge_job_id": int(pinned["engine_publication_id"]),
            "events": pinned["events"],
        }
        or Conversion.model_validate(manifest["conversion"]) != job.conversion
        or integrity["status"] != "pass"
        or not integrity["conversion_validation"]
        or not integrity["source_preservation"]
        or integrity["source_events_compared"] != pinned["events"]
        or out["content_type"] != pinned["mode"]
        or receipt["events_read"] != pinned["events"]
        or stats["events_written"] != pinned["events"]
        or receipt["events_written"] != stats["events_written"]
        or receipt["files_written"] != out["files_written"]
        or receipt["row_groups_written"] != out["row_groups"]
        or receipt["reference_updates"] != stats["reference_updates"]
        or any(
            receipt[f"{name}_events"] != stats[name]
            for name in ("identity", "converted", "unavailable")
        )
        or receipt.get("skipped_events", 0) != stats.get("skipped", 0)
    ):
        raise MarketForgeError(
            "Converted manifest and execution receipt disagree with the submitted job."
        )
    fingerprints = {
        "manifest.json": checksum(manifest_path),
        "execution.json": checksum(output / "execution.json"),
        "conversion-runtime.json": checksum(output / "conversion-runtime.json"),
    }
    total_rows = total_groups = 0
    for entry in out["files"]:
        relative = Path(entry["path"])
        path = output / relative
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or path.resolve() != path
            or str(relative) in fingerprints
            or path.suffix != ".parquet"
        ):
            raise MarketForgeError("Invalid primary path in converted manifest.")
        physical = footer(path)
        if (physical.size, physical.metadata.num_rows, physical.metadata.num_row_groups) != (
            entry["size_bytes"],
            entry["rows"],
            entry["row_groups"],
        ):
            raise MarketForgeError("Converted primary file disagrees with its manifest.")
        fingerprints[str(relative)] = checksum(path)
        total_rows += entry["rows"]
        total_groups += entry["row_groups"]
    if (len(out["files"]), total_rows, total_groups) != (
        out["files_written"],
        stats["events_written"],
        out["row_groups"],
    ):
        raise MarketForgeError("Converted output file totals disagree.")
    return {"manifest": str(manifest_path), "fingerprints": fingerprints, "events": total_rows}


async def execute(paths: ProjectPaths, source: Path, *, tui: bool = False) -> dict[str, Any]:
    collection, document, report = retained_collection(source)
    result = {
        "status": "blocked",
        "execution": "not_started",
        "collection_path": str(collection),
        "report_path": report["report_path"],
        "compatibility": "not_validated",
        "blockers": document["compatibility_blockers"],
    }
    entries: list[dict[str, Any]] = document["jobs"]
    if source != collection:
        entries = [entry for entry in entries if entry["path"] == str(source)]
        if len(entries) != 1:
            raise MarketForgeError("Per-job document is absent from the retained collection.")
    prepared: list[tuple[Job, bytes, dict[str, Any]]] = []
    sources = {item["registry_publication_id"]: item for item in document["sources"]}
    external = document["external_reference"]
    for entry in entries:
        _, submitted = read_pin(entry, collection.parent, expected=f"job-{entry['job_id']}.json")
        try:
            job = Job.model_validate_json(submitted)
        except ValidationError as error:
            raise MarketForgeError(
                f"Invalid prepared conversion job {entry['path']}; re-prepare with Forge: {error}"
            ) from error
        pinned = sources[entry["source_registry_publication_id"]]
        if (
            job.job_id != entry["job_id"]
            or job.input_merge != pinned["root"]
            or job.source.merge_job_id != int(pinned["engine_publication_id"])
            or job.output.dataset_path != entry["output"]
            or Path(job.output.dataset_path) != paths.datasets_dir / "converted" / str(job.job_id)
            or job.conversion.reference_merge != (external["root"] if external else None)
        ):
            raise MarketForgeError(
                "Submitted conversion job disagrees with its pinned inputs and destination."
            )
        output_available(Path(job.output.dataset_path))
        prepared.append((job, submitted, pinned))
    if result["blockers"]:
        return result

    if tui and not all(os.isatty(fd) for fd in (0, 1, 2)):
        raise MarketForgeError("--tui requires an interactive terminal.")
    pins = document["pins"]
    binary = resolve(paths, "convert")
    if pins["executable"] != {"path": str(binary), "sha256": checksum(binary)}:
        raise MarketForgeError("Managed converter changed since preparation. Prepare again.")
    if tui:
        installation = next(
            row for row in inventory(paths)["targets"] if row["target"] == "convert"
        )
        if "monitor-tui" not in installation["installation"].get("features", []):
            raise MarketForgeError("This managed converter was built without monitor-tui.")
    completed: list[dict[str, Any]] = []
    for job, submitted, pinned in prepared:
        directory = paths.runs_dir / f"convert-execute-{uuid4().hex}"
        directory.mkdir(parents=True, exist_ok=False)
        executable = directory / "marketforge-convert"
        shutil.copyfile(binary, executable)
        executable.chmod(0o755)
        if checksum(executable) != pins["executable"]["sha256"]:
            raise MarketForgeError("Managed converter changed while being pinned.")
        submission = directory / "submitted-job.json"
        submission.write_bytes(submitted)
        sync_submission(submission)
        argv = [str(executable), "--job", str(submission), "--run"]
        receipt: dict[str, Any] = {
            "status": "blocked",
            "execution": "not_started",
            "job_id": job.job_id,
            "submission": file_pin(submission, submitted),
            "input": pinned,
            "external_reference": external,
            "executable_sha256": pins["executable"]["sha256"],
            "collection_path": str(collection),
            "argv": [*argv, *(["--tui"] if tui else [])],
            "registration": "not_available",
            "result_path": str(directory / "execution-result.json"),
        }
        write_job(directory / "execution-intent.json", receipt)
        try:
            output_available(Path(job.output.dataset_path))
            if checksum(submission) != receipt["submission"]["sha256"]:
                raise MarketForgeError("Local submitted job changed before execution.")
            receipt["validation_output"] = await asyncio.to_thread(
                validate_prepared_job, executable, submission
            )
            receipt["compatibility"] = "validated"
            receipt.update(status="failed", execution="running")
            code, interrupted = run_process(argv, paths.root, directory, tui=tui)
            receipt.update(
                exit_code=code,
                execution="interrupted" if interrupted else "failed" if code else "complete",
            )
            if interrupted:
                receipt.update(status="interrupted")
            elif not code:
                receipt.update(status="complete", publication=verify_output(job, pinned))
            else:
                receipt["error"] = (
                    "Converter returned a nonzero exit code. Inspect retained engine logs."
                )
        except KeyboardInterrupt:
            receipt.update(status="interrupted", execution="interrupted")
        except (
            MarketForgeError,
            OSError,
            ValueError,
            KeyError,
            TypeError,
            subprocess.TimeoutExpired,
        ) as error:
            receipt["error"] = str(error)
        write_job(directory / "execution-result.json", receipt)
        completed.append(receipt)
        if receipt["status"] != "complete":
            break
    return {
        **result,
        "status": completed[-1]["status"],
        "execution": completed[-1]["execution"],
        "jobs": completed,
        "compatibility": completed[-1].get("compatibility", "not_validated"),
    }
