"""Submit pinned per-job documents through the converter's single-job interface."""

from __future__ import annotations

import asyncio
import json
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
from forge.historical.conversion.evidence import (
    check_source_environment,
    resolve_binding,
)
from forge.historical.conversion.planning import output_available
from forge.historical.conversion.service import (
    collection_publication_source,
    file_pin,
    retained_collection,
    validate_prepared_job,
)
from forge.historical.engine.managed import checksum, inventory, resolve
from forge.historical.engine.process import (
    EngineOutcomeUnknown,
    EngineRunner,
    Lifecycle,
    check,
    joined_io,
    supervise,
)
from forge.historical.engine.work import READY_MESSAGE, WorkIndex, convert_key, output_present
from forge.historical.normalization.execution import run_process, sync_submission
from forge.historical.normalization.service import write_job
from forge.shared.database.connection import (
    database_connection,
    database_dsn,
    pinned_registry,
    require_registry,
    selected_registry_id,
)
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
    partitions: set[str] = set()
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
        partitions.add(relative.parts[0])
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
    return {
        "manifest": str(manifest_path),
        "fingerprints": fingerprints,
        "events": total_rows,
        "statistics": stats,
        "output": {key: value for key, value in out.items() if key != "files"},
        "size_bytes": sum(entry["size_bytes"] for entry in out["files"]),
        "events_read": receipt["events_read"],
        **(
            {"materialized_partitions": len(partitions)}
            if partitions
            and all(
                folder.startswith("partition-") and folder.removeprefix("partition-").isdigit()
                for folder in partitions
            )
            else {}
        ),
    }


async def verify_reused_conversion(
    binary: Path,
    pinned: dict[str, Any],
    previous: dict[str, Any],
) -> dict[str, Any]:
    _, previous_submission = read_pin(
        previous["submission"],
        Path(previous["result_path"]).parent,
        expected="submitted-job.json",
    )
    await joined_io(validate_prepared_job, binary, Path(previous["submission"]["path"]))
    canonical = Job.model_validate_json(previous_submission)
    publication = await joined_io(verify_output, canonical, pinned)
    saved = previous["publication"]
    if any(saved[key] != publication[key] for key in ("manifest", "fingerprints", "events")) or any(
        publication.get(key) != value for key, value in saved.items()
    ):
        raise MarketForgeError("Canonical conversion evidence changed.")
    return publication


async def require_live_prepared_inputs(paths: ProjectPaths, registry: str, ids: set[int]) -> None:
    with pinned_registry(registry):
        async with database_connection(database_dsn(paths.root)) as connection:
            cursor = await connection.execute(
                """SELECT publication_id FROM historical.publication_invalidations
                   WHERE publication_id=ANY(%s)
                   UNION SELECT publication_id FROM historical.publication_supersessions
                   WHERE publication_id=ANY(%s) AND state IN ('retiring','removed')""",
                (sorted(ids), sorted(ids)),
            )
            invalid = [row[0] for row in await cursor.fetchall()]
    if invalid:
        raise MarketForgeError(
            f"Prepared conversion references retired testing publications: {sorted(invalid)}."
        )


async def execute(
    paths: ProjectPaths,
    source: Path,
    *,
    tui: bool = False,
    embedded: bool = False,
    executable: Path | None = None,
    engine_runner: EngineRunner | None = None,
    lifecycle: Lifecycle = lambda _: None,
) -> dict[str, Any]:
    collection, document, report = await joined_io(lambda: retained_collection(source))
    pins = document["pins"]
    direct = collection_publication_source(document)
    if direct is not None:
        check_source_environment(direct)
    require_registry(document, await selected_registry_id(paths.root))
    result = {
        "status": "blocked",
        "environment": document["environment"],
        "registry_id": document["registry_id"],
        "execution": "not_started",
        "collection_path": str(collection),
        "report_path": report["report_path"],
        "compatibility": "not_validated",
        "blockers": document["compatibility_blockers"],
    }
    if direct is not None:
        result["publication_source"] = direct.model_dump(mode="json")
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
        prepared.append((job, submitted, pinned))
    if result["blockers"]:
        return {**result, "not_attempted_job_ids": [job.job_id for job, _, _ in prepared]}
    input_ids = {pinned["registry_publication_id"] for _, _, pinned in prepared}
    if external:
        input_ids.add(external["registry_publication_id"])
    if direct is not None:
        with pinned_registry(document["registry_id"]):
            async with (
                database_connection(database_dsn(paths.root)) as connection,
                connection.transaction(),
            ):
                await connection.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
                )
                publication = await resolve_binding(connection, direct, check_files=True)
        if len(prepared) != 1 or any(
            pinned["registry_publication_id"] != publication.id
            or job.input_merge != str(publication.root)
            or job.source.merge_job_id != int(publication.engine_id)
            or job.source.manifest_sha256 != direct.manifest_sha256
            or job.source.publication_checksums_sha256 != direct.publication_checksums_sha256
            for job, _, pinned in prepared
        ):
            raise MarketForgeError("Prepared job disagrees with the bound registry publication.")
    await require_live_prepared_inputs(paths, document["registry_id"], input_ids)

    if tui and engine_runner is None and not all(os.isatty(fd) for fd in (0, 1, 2)):
        raise MarketForgeError("--tui requires an interactive terminal.")
    binary = executable or resolve(paths, "convert")
    if pins["executable"] != {"path": str(binary), "sha256": checksum(binary)}:
        raise MarketForgeError("Managed converter changed since preparation. Prepare again.")
    if tui and executable is None:
        installation = next(
            row for row in inventory(paths)["targets"] if row["target"] == "convert"
        )
        if "monitor-tui" not in installation["installation"].get("features", []):
            raise MarketForgeError(
                "This managed converter was built without monitor-tui. Run mf engine build convert."
            )
    if tui:
        help_result = await joined_io(
            lambda: subprocess.run(
                [str(binary), "--help"], capture_output=True, text=True, timeout=10
            )
        )
        if "--managed-output" not in help_result.stdout:
            raise MarketForgeError(
                "Managed converter cannot hand off the TUI result. Run mf engine build convert and prepare again."
            )
    completed: list[dict[str, Any]] = []
    for job, submitted, pinned in prepared:
        directory = paths.runs_dir / f"convert-execute-{uuid4().hex}"
        index = WorkIndex(paths.root, document["registry_id"], "convert", directory.name)
        key = convert_key(
            document["registry_id"], pins["executable"]["sha256"], job.model_dump(mode="json")
        )
        result_path = directory / "execution-result.json"

        async def recover_candidate(work_key: str, owner: str, intent: Path) -> None:
            retained = json.loads(await joined_io(intent.read_bytes))
            if (
                retained["work_key"] != work_key
                or owner != intent.parent.name
                or retained["executable_sha256"] != pins["executable"]["sha256"]
                or retained.get("registry_id") != document["registry_id"]
            ):
                raise MarketForgeError("Retained conversion intent disagrees with work ownership.")
            recovered_binary = intent.parent / "marketforge-convert"
            if checksum(recovered_binary) != retained["executable_sha256"]:
                raise MarketForgeError("Pinned conversion executable changed.")
            submission, payload = read_pin(
                retained["submission"], intent.parent, expected="submitted-job.json"
            )
            canonical = Job.model_validate_json(payload)
            if (
                convert_key(
                    document["registry_id"],
                    checksum(recovered_binary),
                    canonical.model_dump(mode="json"),
                )
                != work_key
            ):
                raise MarketForgeError("Retained conversion job disagrees with work key.")
            await joined_io(validate_prepared_job, recovered_binary, submission)
            publication = await joined_io(verify_output, canonical, retained["input"])
            retained.update(
                status="complete",
                execution="complete",
                compatibility="validated",
                disposition="recovered",
                publication=publication,
            )
            recovery_path = intent.with_name("publication-recovery.json")
            retained["result_path"] = str(recovery_path)
            write_job(recovery_path, retained)
            original = WorkIndex(paths.root, document["registry_id"], "convert", owner)
            await original.complete(work_key, retained, recovery=True)

        try:
            await index.recover_committed([key], recover_candidate)
            existing = await index.acquire(
                {
                    key: {
                        "report_path": str(result_path),
                        "path": job.output.dataset_path,
                    }
                }
            )
            if key in existing:
                _, _, previous = existing[key]
                assert previous is not None
                publication = await verify_reused_conversion(binary, pinned, previous)
                receipt = {
                    **previous,
                    "publication": publication,
                    "disposition": "reused",
                    "reuse_message": READY_MESSAGE,
                    "engine_execution_skipped": True,
                    "canonical_result_path": previous["result_path"],
                    "result_path": str(result_path),
                    "requested_output_path": job.output.dataset_path,
                }
                write_job(result_path, receipt)
                completed.append(receipt)
                continue
        except (
            MarketForgeError,
            OSError,
            ValueError,
            KeyError,
            subprocess.TimeoutExpired,
        ) as error:
            completed.append(
                {
                    "job_id": job.job_id,
                    "status": "blocked",
                    "execution": "not_started",
                    "error": str(error),
                }
            )
            break
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
        if tui:
            argv.append("--managed-output")
        receipt: dict[str, Any] = {
            "status": "blocked",
            "work_key": key,
            "registry_id": document["registry_id"],
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
        if direct is not None:
            receipt["publication_source"] = direct.model_dump(mode="json")
        write_job(directory / "execution-intent.json", receipt)
        try:
            output_available(Path(job.output.dataset_path))
            if checksum(submission) != receipt["submission"]["sha256"]:
                raise MarketForgeError("Local submitted job changed before execution.")
            lifecycle("validate")
            if embedded or engine_runner is not None:
                validation = await check(
                    [str(executable), "--job", str(submission), "--validate"],
                    paths.root,
                    directory,
                    "validation",
                )
                if validation.returncode:
                    raise MarketForgeError(validation.stdout)
                receipt["validation_output"] = validation.stdout
            else:
                receipt["validation_output"] = await joined_io(
                    validate_prepared_job, executable, submission
                )
            receipt["compatibility"] = "validated"
            receipt.update(status="failed", execution="running")
            lifecycle("execute")
            if engine_runner is not None:
                code, interrupted = await engine_runner(argv, paths.root, directory, tui)
            elif embedded:
                code, interrupted = await supervise(argv, paths.root, directory)
            else:
                code, interrupted = run_process(argv, paths.root, directory, tui=tui)
            receipt.update(
                exit_code=code,
                execution="interrupted" if interrupted else "failed" if code else "complete",
            )
            if interrupted:
                receipt.update(status="interrupted")
            elif not code:
                lifecycle("verify")
                publication = await joined_io(verify_output, job, pinned)
                receipt.update(status="complete", verification="verified", publication=publication)
            else:
                receipt["error"] = (
                    "Converter returned a nonzero exit code. Inspect retained engine logs."
                )
        except EngineOutcomeUnknown:
            raise
        except (KeyboardInterrupt, asyncio.CancelledError):
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
        if receipt["status"] == "complete":
            try:
                await index.complete(key, {**receipt, "indexing": "complete"})
            except (MarketForgeError, OSError) as error:
                receipt.update(status="failed", indexing="failed", error=str(error))
                write_job(directory / "execution-result.json", receipt, replace_existing=True)
            else:
                receipt["indexing"] = "complete"
                write_job(directory / "execution-result.json", receipt, replace_existing=True)
        elif not await joined_io(output_present, Path(job.output.dataset_path)):
            await index.release([key])
        completed.append(receipt)
        if receipt["status"] != "complete":
            break
    return {
        **result,
        "status": next(
            (item["status"] for item in completed if item["status"] != "complete"), "complete"
        ),
        "execution": completed[-1]["execution"],
        "jobs": completed,
        "not_attempted_job_ids": [job.job_id for job, _, _ in prepared[len(completed) :]],
        "disposition": "reused"
        if all(job.get("disposition") == "reused" for job in completed)
        else "new",
        "compatibility": "validated"
        if (
            len(completed) == len(prepared)
            and all(item.get("compatibility") == "validated" for item in completed)
        )
        else "not_validated",
    }
