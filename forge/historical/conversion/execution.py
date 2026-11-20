"""Submit pinned per-job documents through the converter's single-job interface."""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any
from uuid import uuid4

from forge.datasets.discovery import read_object
from forge.datasets.parquet import footer
from forge.historical.conversion.contract import Conversion, Job
from forge.historical.conversion.evidence import verify_publication
from forge.historical.conversion.planning import output_available, static_blockers
from forge.historical.conversion.registry import parents, publications, raw_for_files
from forge.historical.conversion.service import Catalog, catalog_pin, file_pin, retained_collection
from forge.historical.engine.managed import checksum, inventory, resolve
from forge.historical.normalization.execution import run_process, sync_submission
from forge.historical.normalization.service import write_job
from forge.shared.database.testing import testing_connection, testing_dsn
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


async def verify_pins(paths: ProjectPaths, pins: list[dict[str, Any]]) -> None:
    ids = [pin["registry_publication_id"] for pin in pins]
    async with testing_connection(testing_dsn(paths.root)) as connection, connection.transaction():
        await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        found = {pub.id: pub for pub in await publications(connection, ids=ids)}
        for pin in pins:
            pub = found[pin["registry_publication_id"]]
            current_parents = await parents(connection, pub)
            now = await verify_publication(connection, paths, pub, current_parents)
            if now != {key: value for key, value in pin.items() if key != "lineage"}:
                raise MarketForgeError(
                    f"Pinned merge publication {pub.id} changed since preparation."
                )
            lineage: dict[str, Any] = pin.get("lineage", {})
            if matched := lineage.get("matched_parents"):
                parent_records = {parent["id"]: parent for parent in current_parents}
                raw = await raw_for_files(connection, paths, list(parent_records))
                for proof in matched:
                    file_id = proof["file_dataset_id"]
                    if proof["normalization_execution"] != parent_records[file_id]["evidence"].get(
                        "execution"
                    ):
                        raise MarketForgeError(
                            f"Normalization execution evidence changed for file {file_id}."
                        )
                    for archive in proof["raw_archives"]:
                        record = raw.get(file_id, {}).get(archive["path"])
                        if record is None or any(
                            (
                                record[key].isoformat()
                                if key in {"start_at", "end_at"}
                                else record[key]
                            )
                            != value
                            for key, value in archive.items()
                            if key != "path"
                        ):
                            raise MarketForgeError(
                                f"Raw input provenance changed for normalized file {file_id}."
                            )
            for artifact in ("merge_submission", "merge_prepare_report"):
                if (
                    artifact in lineage
                    and checksum(Path(lineage[artifact])) != lineage[f"{artifact}_sha256"]
                ):
                    raise MarketForgeError(
                        f"Pinned upstream evidence changed: {lineage[artifact]}."
                    )


def verify_output(job: Job, pinned: dict[str, Any]) -> dict[str, Any]:
    output = Path(job.output.dataset_path)
    if output.resolve() != output or output.with_suffix(".staging").exists():
        raise MarketForgeError("Converter did not commit an unaliased output directory.")
    manifest_path = output / "manifest.json"
    manifest = read_object(manifest_path)
    receipt = read_object(output / "execution.json")
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
        job = Job.model_validate_json(submitted)
        pinned = sources[entry["source_registry_publication_id"]]
        if (
            job.job_id != entry["job_id"]
            or job.input_merge != pinned["root"]
            or job.output.dataset_path != entry["output"]
            or Path(job.output.dataset_path) != paths.datasets_dir / "converted" / str(job.job_id)
            or job.conversion.reference_merge != (external["root"] if external else None)
        ):
            raise MarketForgeError(
                "Submitted conversion job disagrees with its pinned inputs and destination."
            )
        output_available(Path(job.output.dataset_path))
        result["blockers"] = list(
            dict.fromkeys(
                [
                    *result["blockers"],
                    *static_blockers(pinned, external, entry["references"], entry["routes"]),
                ]
            )
        )
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
    _, runtime = read_pin(pins["runtime"], collection.parent, expected="general.toml")
    _, data = read_pin(pins["catalog_snapshot"], collection.parent, expected="instruments.json")
    snapshot = Catalog.model_validate_json(data)
    await verify_pins(paths, [item[2] for item in prepared] + ([external] if external else []))
    completed: list[dict[str, Any]] = []
    for job, submitted, pinned in prepared:
        directory = paths.runs_dir / f"convert-execute-{uuid4().hex}"
        directory.mkdir(parents=True, exist_ok=False)
        executable = directory / "marketforge-convert"
        shutil.copyfile(binary, executable)
        executable.chmod(0o755)
        if checksum(executable) != pins["executable"]["sha256"]:
            raise MarketForgeError("Managed converter changed while being pinned.")
        submission, config = directory / "submitted-job.json", directory / "general.toml"
        submission.write_bytes(submitted)
        config.write_bytes(runtime)
        sync_submission(submission)
        argv = [
            str(executable),
            "--job",
            str(submission),
            "--project-root",
            str(paths.root),
            "--config",
            str(config),
            "--run",
        ]
        receipt: dict[str, Any] = {
            "status": "blocked",
            "execution": "not_started",
            "job_id": job.job_id,
            "submission": file_pin(submission, submitted),
            "runtime": file_pin(config, runtime),
            "input": pinned,
            "external_reference": external,
            "executable_sha256": pins["executable"]["sha256"],
            "fixed_catalog": pins["fixed_catalog"],
            "collection_path": str(collection),
            "argv": [*argv, *(["--tui"] if tui else [])],
            "registration": "not_available",
            "result_path": str(directory / "execution-result.json"),
        }
        write_job(directory / "execution-intent.json", receipt)
        try:
            if completed:
                await verify_pins(paths, [pinned, *([external] if external else [])])
            if catalog_pin(paths, snapshot) != pins["fixed_catalog"]:
                raise MarketForgeError("Fixed engine catalog changed since preparation.")
            output_available(Path(job.output.dataset_path))
            if (
                checksum(submission) != receipt["submission"]["sha256"]
                or checksum(config) != receipt["runtime"]["sha256"]
            ):
                raise MarketForgeError("Local submitted job or runtime changed before execution.")
            receipt.update(status="failed", execution="running")
            code, interrupted = run_process(argv, paths.root, directory, tui=tui)
            receipt.update(
                exit_code=code,
                execution="interrupted" if interrupted else "failed" if code else "complete",
            )
            if interrupted:
                receipt.update(status="interrupted")
            elif not code:
                if catalog_pin(paths, snapshot) != pins["fixed_catalog"]:
                    raise MarketForgeError("Fixed engine catalog changed during execution.")
                receipt.update(status="complete", publication=verify_output(job, pinned))
            else:
                receipt["error"] = (
                    "Converter returned a nonzero exit code. Inspect retained engine logs."
                )
        except KeyboardInterrupt:
            receipt.update(status="interrupted", execution="interrupted")
        except (MarketForgeError, OSError, ValueError, KeyError, TypeError) as error:
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
    }
