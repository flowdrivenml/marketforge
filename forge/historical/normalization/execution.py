"""Execute immutable submitted JSON using the managed Rust engine, without a recipe."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import shlex
import shutil
import signal
import subprocess
import termios
import threading
import time
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from forge.historical.engine.managed import checksum, resolve, supervised
from forge.historical.engine.process import Lifecycle, check, joined_io, supervise
from forge.historical.engine.runtime import inspect_runtime
from forge.historical.engine.work import READY_MESSAGE, WorkIndex, normalize_key
from forge.historical.normalization.datasets import pin_raw_inputs, register_verified, verify
from forge.historical.normalization.service import write_job
from forge.historical.profiles.service import discover, discover_at
from forge.shared.database.connection import (
    database_flag,
    database_purpose,
    is_testing,
    pinned_registry,
    require_registry,
    selected_registry_id,
)
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


def _capture(stream: Any, path: Path, *, limit: int = 16 * 1024 * 1024) -> None:
    """Drain every byte while bounding retained logs and resident buffers."""
    with stream, contextlib.ExitStack() as stack:
        output = None
        with contextlib.suppress(OSError):
            output = stack.enter_context(path.open("wb", buffering=0))
        while chunk := stream.read(65536):
            if output is not None and limit:
                try:
                    limit -= output.write(chunk[:limit])
                except OSError:
                    # Continue draining if optional diagnostic storage fails.
                    output = None


def profile_inventory(paths: ProjectPaths, historical: Path | None) -> list[dict[str, Any]]:
    return discover(paths) if historical is None else discover_at(historical / "profiles")


def execute(
    paths: ProjectPaths,
    source: Path,
    *,
    tui: bool = False,
    allow_partial: bool = False,
    recipe: str | None = None,
) -> dict[str, Any]:
    return asyncio.run(
        execute_async(
            paths, source, tui=tui, allow_partial=allow_partial, recipe=recipe, standalone=True
        )
    )


async def execute_async(
    paths: ProjectPaths,
    source: Path,
    *,
    tui: bool = False,
    allow_partial: bool = False,
    recipe: str | None = None,
    standalone: bool = False,
    directory: Path | None = None,
    historical_root: Path | None = None,
    executable: Path | None = None,
    lifecycle: Lifecycle = lambda _: None,
    expected_raw: dict[str, tuple[int, str]] | None = None,
) -> dict[str, Any]:
    binary = executable or resolve(paths)
    if tui and not (os.isatty(0) and os.isatty(1) and os.isatty(2)):
        raise MarketForgeError("--tui requires an interactive terminal.")
    operation = directory or paths.runs_dir / f"normalize-execute-{uuid4().hex}"
    operation.mkdir(parents=True, exist_ok=False)
    expected_binary = checksum(binary)
    pinned_binary = operation / "marketforge-normalize"
    shutil.copyfile(binary, pinned_binary)
    pinned_binary.chmod(0o755)
    if checksum(pinned_binary) != expected_binary:
        raise MarketForgeError("Managed executable changed during execution preparation.")
    binary = pinned_binary
    protocol_argv = [str(binary), "--protocol"]
    protocol = (
        supervised(protocol_argv, cwd=paths.root)
        if standalone
        else await check(protocol_argv, paths.root, operation, "protocol")
    )
    capabilities: dict[str, Any] = json.loads(protocol.stdout) if protocol.returncode == 0 else {}
    if 4 not in capabilities.get("batch_schema_versions", []) or not capabilities.get(
        "result_file"
    ):
        raise MarketForgeError(
            "Run mf engine build normalize to install the managed result protocol."
        )
    if tui and not capabilities.get("managed_output"):
        raise MarketForgeError(
            "Run mf engine build normalize to install managed TUI output support."
        )
    submission = operation / "submitted-batch.json"
    # read_bytes preserves all edits and integer/decimal lexical representations.
    await joined_io(lambda: submission.write_bytes(source.read_bytes()))
    prepared = source.parent / "prepare-report.json"
    evidence: dict[str, Any] = json.loads(prepared.read_text()) if prepared.is_file() else {}
    origin = evidence.get("configuration_origin")
    if origin is not None:
        runtime = source.parent / str(evidence.get("configuration_filename", "general.toml"))
        historical_root = source.parent / "historical"
    else:
        runtime = paths.runtime_config("normalize", historical_root)
    config = operation / runtime.name
    shutil.copyfile(runtime, config)
    historical = operation / "historical"
    profile_evidence: list[dict[str, Any]] = []
    for item in profile_inventory(paths, historical_root):
        original = Path(item["location"])
        target = historical / "profiles" / item["id"].rsplit(".", 1)[1] / original.name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(original, target)
        if checksum(target) != item["sha256"]:
            raise MarketForgeError("Profile changed while freezing execution interpretation.")
        profile_evidence.append({"id": item["id"], "sha256": checksum(target)})
    try:
        parsed: object = json.loads(submission.read_bytes())
    except ValueError as error:
        raise MarketForgeError(f"Invalid prepared normalization batch: {error}") from error
    if not isinstance(parsed, dict):
        raise MarketForgeError("Expected a prepared normalization batch object.")
    document = cast(dict[str, Any], parsed)
    if document.get("status") != "prepared":
        raise MarketForgeError("Only fully prepared normalization requests can be executed.")
    registry = await selected_registry_id(paths.root)
    if evidence:
        if evidence.get("registry_id") is not None:
            require_registry(evidence, registry)
        elif not is_testing():
            raise MarketForgeError("Prepare this normalization batch again in production.")
        if (
            evidence.get("profile_evidence") != profile_evidence
            or evidence.get("configuration_sha256") != checksum(config)
            or evidence.get("executable_sha256", expected_binary) != expected_binary
            or evidence.get("submitted_sha256", checksum(submission)) != checksum(submission)
        ):
            raise MarketForgeError(
                "The batch, executable, profiles or runtime changed after preparation. Prepare the recipe again."
            )
    argv = [
        str(binary),
        "--batch",
        str(submission),
        "--project-root",
        str(paths.root),
        "--config",
        str(config),
        "--profiles-root",
        str(historical),
    ]
    validation_argv = [
        str(binary),
        "--validate-batch",
        str(submission),
        "--project-root",
        str(paths.root),
        "--config",
        str(config),
        "--profiles-root",
        str(historical),
    ]
    validation = (
        supervised(validation_argv, cwd=paths.root)
        if standalone
        else await check(validation_argv, paths.root, operation, "validation")
    )
    if validation.returncode:
        raise MarketForgeError(f"Production batch validation failed: {validation.stdout}")
    with pinned_registry(registry):
        pinned_raw = await pin_raw_inputs(paths, document["jobs"], expected=expected_raw)
    runtime_settings = await joined_io(inspect_runtime, paths, "normalize", config, binary)
    requested_sha = checksum(submission)
    requested: list[dict[str, Any]] = document["jobs"]
    keys = {
        str(job["job_id"]): normalize_key(
            registry,
            expected_binary,
            document,
            job,
            pinned_raw[str(job["job_id"])],
            runtime_settings,
            profile_evidence,
        )
        for job in requested
    }
    if len(set(keys.values())) != len(keys):
        raise MarketForgeError("The submitted batch repeats equivalent normalization jobs.")
    index = WorkIndex(paths.root, registry, "normalize", operation.name)

    async def recover_candidate(_key: str, _owner: str, intent: Path) -> None:
        await recover(paths, intent, require_all=True, lifecycle=lifecycle)

    await index.recover_committed(keys.values(), recover_candidate)
    existing = await index.acquire(
        {
            keys[str(job["job_id"])]: {
                "path": str(paths.root / job["output"]["dataset_path"]),
                "report_path": str(operation / "execution-result.json"),
            }
            for job in requested
        }
    )
    owned = [key for key in keys.values() if key not in existing]
    ready: dict[str, dict[str, Any]] = {}
    missing: list[dict[str, Any]] = []
    try:
        for job in requested:
            job_id = str(job["job_id"])
            if keys[job_id] in existing:
                _, _, original = existing[keys[job_id]]
                assert original is not None
                await verify_reuse(paths, index, binary, original)
                ready[job_id] = original
                continue
            for name in ("dataset_path", "staging_path"):
                destination = paths.root / job["output"][name]
                if destination.exists() or destination.is_symlink():
                    raise MarketForgeError(f"Execution output already exists: {destination}")
            missing.append(job)
    except BaseException:
        await index.release(owned)
        raise
    if len(missing) != len(requested):
        submission.rename(operation / "requested-batch.json")
        subset: dict[str, Any] = {**document, "jobs": missing}
        write_job(submission, subset)
        document = subset
    source_path = await joined_io(source.resolve)
    receipt: dict[str, Any] = {
        "schema_version": 1,
        "environment": database_purpose(),
        "registry_id": registry,
        "execution_id": operation.name,
        "operation": "normalize",
        "run_id": document["run_id"],
        "recipe": recipe,
        "allow_partial": allow_partial,
        "source_sha256_by_job": pinned_raw,
        "requested_sha256": requested_sha,
        "work_keys": keys,
        "reused_outputs": ready,
        "execution_receipt": str(operation / "execution-intent.json"),
        "report_path": str(operation / "execution-result.json"),
        "engine_result_path": str(operation / "engine-result.json"),
        "submission_path": str(submission),
        "submitted_source": str(source_path),
        "submitted_sha256": checksum(submission),
        "configuration_sha256": checksum(config),
        "configuration_origin": origin or str(runtime),
        "profiles": profile_evidence,
        "executable_sha256": checksum(binary),
        "started_at": datetime.now(UTC).isoformat(),
        "normalization": "running",
        "registrations": [],
    }
    sync_submission(submission)
    write_job(operation / "execution-intent.json", receipt)
    if not missing:
        receipt.update(
            status="complete",
            normalization="not_started",
            verification="verified",
            registration="registered",
            jobs=[],
            counts={"requested": 0, "successful": 0, "failed": 0},
        )
        add_reused(receipt, ready)
        write_job(operation / "execution-result.json", receipt)
        return receipt
    argv.extend(["--result-file", receipt["engine_result_path"]])
    if capabilities.get("managed_output"):
        argv.append("--managed-output")
    try:
        lifecycle("execute")
        engine_started = time.monotonic_ns()
        code, interrupted = (
            run_process(argv, paths.root, operation, tui=tui)
            if standalone
            else await supervise(argv, paths.root, operation)
        )
    except OSError as error:
        receipt.update(status="failed", normalization="failed", error=str(error))
        write_job(operation / "execution-result.json", receipt)
        await index.release(owned)
        return receipt
    receipt["engine_elapsed_ms"] = (time.monotonic_ns() - engine_started) // 1_000_000
    receipt["exit_code"] = code
    receipt["finished_at"] = datetime.now(UTC).isoformat()
    receipt["normalization"] = (
        "interrupted" if interrupted else "complete" if code == 0 else "failed"
    )
    diagnostic = operation / ("engine-tui.log" if tui else "engine-stderr.log")
    if diagnostic.is_file():
        receipt["engine_diagnostics_path"] = str(diagnostic)
    write_job(operation / "engine-outcome.json", receipt)
    try:
        await finish_async(paths, document, receipt, binary, operation, lifecycle=lifecycle)
    except (KeyboardInterrupt, asyncio.CancelledError):
        receipt.update(
            status="interrupted",
            recovery=recovery_command(operation / "execution-intent.json"),
        )
    add_reused(receipt, ready)
    write_job(operation / "execution-result.json", receipt)
    for job in missing:
        path = paths.root / job["output"]["dataset_path"]
        key = keys[str(job["job_id"])]
        if receipt.get("registration") == "registered" and str(path) in receipt.get(
            "publications", {}
        ):
            entry = next(item for item in receipt["jobs"] if item["path"] == str(path))
            await index.complete(
                key,
                {
                    "path": str(path),
                    "entry": entry,
                    "execution_id": operation.name,
                    "submission_path": str(submission),
                    "submitted_sha256": receipt["submitted_sha256"],
                    "publications": {str(path): receipt["publications"][str(path)]},
                },
            )
        elif not path.exists() and not path.is_symlink():
            await index.release([key])
    return receipt


async def verify_reuse(
    paths: ProjectPaths, index: WorkIndex, binary: Path, original: dict[str, Any]
) -> None:
    source = Path(original["submission_path"])
    data = await joined_io(source.read_bytes)
    if hashlib.sha256(data).hexdigest() != original["submitted_sha256"]:
        raise MarketForgeError("Canonical normalization submission changed.")
    batch = json.loads(data)
    job = next(
        (entry for entry in batch["jobs"] if entry["job_id"] == original["entry"]["job_id"]),
        None,
    )
    if job is None:
        raise MarketForgeError("Canonical job is absent from its submitted batch.")
    verified = await joined_io(
        committed_job,
        paths,
        binary,
        batch,
        job,
        original["submitted_sha256"],
    )
    if (
        verified["path"] != original["path"]
        or verified["manifest_sha256"] != original["entry"]["manifest_sha256"]
    ):
        raise MarketForgeError("Canonical normalization manifest changed.")
    await index.verify_registered(original, original["path"], verified["inventory_sha256"])


def add_reused(receipt: dict[str, Any], reused: dict[str, dict[str, Any]]) -> None:
    if not reused:
        return
    counts = receipt["counts"]
    counts.update(reused=len(reused), executed=counts["requested"])
    counts["requested"] += len(reused)
    counts["successful"] += len(reused)
    receipt["disposition"] = "mixed" if counts["executed"] else "reused"
    if not counts["executed"]:
        receipt["reuse_message"] = READY_MESSAGE
    for requested_id, original in reused.items():
        path = original["path"]
        receipt["jobs"].append(
            {
                **original["entry"],
                "disposition": "reused",
                "requested_job_id": int(requested_id),
                "canonical_execution_id": original["execution_id"],
            }
        )
        publication = original["publications"][path]
        receipt.setdefault("publications", {})[path] = publication
        receipt.setdefault("catalogue_dataset_ids", {}).update(publication["files"])
    counts["registered"] = len(receipt["catalogue_dataset_ids"])
    counts["reused_files"] = sum(
        len(item["publications"][item["path"]]["files"]) for item in reused.values()
    )


def sync_submission(submission: Path) -> None:
    with submission.open("rb") as artifact:
        os.fsync(artifact.fileno())
    parent = os.open(submission.parent.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(parent)
    finally:
        os.close(parent)


def run_process(argv: list[str], root: Path, operation: Path, *, tui: bool) -> tuple[int, bool]:
    if tui:
        argv.append("--tui")
    # Rust owns the live dashboard and restores it on normal exits.
    terminal = termios.tcgetattr(0) if tui and os.isatty(0) else None
    process = subprocess.Popen(
        argv,
        cwd=root,
        stdout=None if tui else subprocess.PIPE,
        stderr=None if tui else subprocess.PIPE,
        start_new_session=not tui,
    )
    readers: list[threading.Thread] = []
    if not tui:
        for stream, name in (
            (process.stdout, "engine-stdout.log"),
            (process.stderr, "engine-stderr.log"),
        ):
            reader = threading.Thread(target=_capture, args=(stream, operation / name), daemon=True)
            reader.start()
            readers.append(reader)
    interrupted = False
    try:
        code = process.wait()
    except KeyboardInterrupt:
        interrupted = True
        if not tui:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGTERM)
        else:
            process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            if not tui:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
            else:
                process.kill()
            process.wait()
        code = process.returncode
    finally:
        for reader in readers:
            reader.join(timeout=10)
    if terminal is not None and code < 0:
        # A signal can terminate the child before its terminal guard runs.
        with contextlib.suppress(termios.error):
            termios.tcsetattr(0, termios.TCSANOW, terminal)
        with contextlib.suppress(OSError):
            os.write(1, b"\x1b[?25h\x1b[?1049l")
    return code, interrupted or code in {
        130,
        -signal.SIGINT,
        -signal.SIGTERM,
        -signal.SIGKILL,
        -signal.SIGHUP,
    }


def committed_job(
    paths: ProjectPaths,
    binary: Path,
    document: dict[str, Any],
    job: dict[str, Any],
    submitted_sha256: str,
) -> dict[str, Any]:
    dataset = paths.root / job["output"]["dataset_path"]
    evidence = verify(paths, dataset, binary=binary)
    manifest = evidence["manifest"]
    resolved = evidence["resolved_job"]
    if (
        manifest["job_id"] != job["job_id"]
        or manifest["dataset_id"] != job["dataset_id"]
        or manifest["content_type"] != job["output"]["content_type"]
        or evidence["instrument_namespace"] != document["instrument_namespace"]
        or any(
            task.get("raw_schema", {}).get("marketforge_submission_sha256") != submitted_sha256
            for task in resolved["tasks"]
        )
        or Path(resolved["output"]["staging_path"]) != paths.root / job["output"]["staging_path"]
        or {Path(task["input_path"]) for task in resolved["tasks"]}
        != {paths.root / task["input_path"] for task in job["tasks"]}
    ):
        raise MarketForgeError(
            f"Committed dataset identity differs from submitted job {job['job_id']}."
        )
    return evidence


def finish(
    paths: ProjectPaths,
    document: dict[str, Any],
    receipt: dict[str, Any],
    binary: Path,
    directory: Path,
) -> None:
    asyncio.run(finish_async(paths, document, receipt, binary, directory))


async def finish_async(
    paths: ProjectPaths,
    document: dict[str, Any],
    receipt: dict[str, Any],
    binary: Path,
    directory: Path,
    *,
    lifecycle: Lifecycle = lambda _: None,
) -> None:
    expected = {job["job_id"]: job for job in document["jobs"]}
    receipt.update(
        status="failed",
        verification="not_started",
        registration="not_started",
        counts={"requested": len(expected), "successful": 0, "failed": 0, "registered": 0},
        jobs=[],
        recovery=recovery_command(directory / "execution-intent.json"),
    )
    engine_result = Path(receipt["engine_result_path"])
    if receipt["normalization"] == "interrupted":
        with contextlib.suppress(OSError, ValueError):
            receipt["engine_result"] = json.loads(await joined_io(engine_result.read_bytes))
        receipt["status"] = "interrupted"
        return
    lifecycle("verify")
    verified: list[dict[str, Any]] = []
    failures = 0
    try:
        result = json.loads(await joined_io(engine_result.read_bytes))
        receipt["engine_result"] = result
        if (
            result["kind"] != "normalization_result"
            or result["schema_version"] != 1
            or result["run_id"] != document["run_id"]
        ):
            raise MarketForgeError(
                "Engine did not report a definitive outcome for the submitted jobs."
            )
        if result["error"] is not None:
            raise MarketForgeError(result["error"]["message"])
        if len(result["jobs"]) != len(expected):
            raise MarketForgeError(
                "Engine did not report a definitive outcome for the submitted jobs."
            )
        for job in result["jobs"]:
            submitted = expected.pop(job["job_id"], None)
            if submitted is None or job["dataset_id"] != submitted["dataset_id"]:
                raise MarketForgeError("Unexpected engine job or dataset identity.")
            entry = dict(job, path=str(paths.root / submitted["output"]["dataset_path"]))
            receipt["jobs"].append(entry)
            if job["status"] == "failed" and job["failure"]:
                failures += 1
                entry["registration"] = "omitted"
                if job["failure"]["code"] in {"invalid_job", "output_failure"}:
                    raise MarketForgeError(
                        f"Job {job['job_id']} encountered a configuration or publication failure."
                    )
                continue
            if (
                job["status"] != "complete"
                or job["failure"]
                or (Path(job["manifest_path"]) != Path(entry["path"]) / "manifest.json")
            ):
                raise MarketForgeError("Invalid success evidence in engine job result.")
            evidence = await joined_io(
                partial(
                    committed_job, paths, binary, document, submitted, receipt["submitted_sha256"]
                )
            )
            evidence["expected_raw_sha256"] = receipt["source_sha256_by_job"][str(job["job_id"])]
            manifest = evidence["manifest"]
            for key in (
                "files_written",
                "events_written",
                "start_timestamp_ns",
                "end_timestamp_ns",
            ):
                if manifest[key] != job[key]:
                    raise MarketForgeError(
                        f"Engine result and committed manifest disagree on {key}."
                    )
            metrics = manifest.get("processing_metrics", {}).get("counters", {})
            entry.update(
                content_type=manifest.get("content_type"),
                processing_counters={
                    key: metrics[key]
                    for key in ("records_read", "records_rejected")
                    if key in metrics
                },
                verification="verified",
                instrument_namespace=evidence["instrument_namespace"],
                integrity_status=manifest["integrity_status"],
                manifest_sha256=evidence["manifest_sha256"],
                bytes_written=evidence["bytes_written"],
            )
            verified.append(evidence)
        if (receipt["exit_code"] == 0) != (failures == 0) or result["status"] != (
            "failed" if failures else "complete"
        ):
            raise MarketForgeError("Engine exit status and structured outcome disagree.")
    except (MarketForgeError, OSError, ValueError, KeyError, TypeError) as error:
        receipt.update(verification="failed", error=str(error))
        receipt["counts"].update(successful=len(verified), failed=failures)
        return
    receipt["counts"].update(successful=len(verified), failed=failures)
    receipt["counts"].update(
        verified_publications=len(verified),
        verified_primary_files=sum(item["manifest"]["files_written"] for item in verified),
    )
    receipt["verification"] = "verified" if verified else "no_successful_outputs"
    if not verified or (failures and not receipt["allow_partial"]):
        receipt["registration"] = "withheld"
        for job in receipt["jobs"]:
            if job.get("verification") == "verified":
                job["registration"] = "withheld"
        return
    lifecycle("register")
    registration = await register_verified(
        paths,
        verified,
        receipt_directory=directory,
        execution={
            key: receipt[key]
            for key in (
                "execution_id",
                "submitted_sha256",
                "allow_partial",
                "environment",
                "registry_id",
            )
        },
    )
    receipt["registration"] = registration["registration"]
    receipt["registration_receipt"] = registration["registration_receipt"]
    if registration["registration"] != "registered":
        receipt["registration_error"] = registration["error"]
        return
    ids = registration["catalogue_dataset_ids"]
    publications = registration["publications"]
    for job in receipt["jobs"]:
        if job["path"] in publications:
            job.update(registration="registered", **publications[job["path"]])
    receipt["counts"].update(registered=len(ids), **registration["counts"])
    receipt["catalogue_dataset_ids"] = ids
    receipt["publications"] = publications
    receipt["registrations"] = [
        {
            "registration": "registered",
            "catalogue_dataset_id": dataset_id,
            "path": filepath,
            "state": registration["file_states"][filepath],
        }
        for filepath, dataset_id in ids.items()
    ]
    receipt["status"] = "partial" if failures else "complete"
    receipt.pop("recovery")


def recovery_command(intent: Path) -> str:
    return f"mf normalize register {shlex.quote(str(intent))} {database_flag()}"


async def recover(
    paths: ProjectPaths,
    source: Path,
    *,
    require_all: bool = False,
    lifecycle: Lifecycle = lambda _: None,
) -> dict[str, Any]:
    intent = json.loads(await asyncio.to_thread(source.read_bytes))
    if intent.get("operation") != "normalize":
        raise MarketForgeError("Expected a managed normalization execution-intent.json receipt.")
    require_registry(intent, await selected_registry_id(paths.root))
    intent_path = await asyncio.to_thread(source.resolve)
    directory = intent_path.parent
    submission = directory / "submitted-batch.json"
    if checksum(submission) != intent["submitted_sha256"]:
        raise MarketForgeError("Recovery submission fingerprint differs from execution intent.")
    document = json.loads(submission.read_bytes())
    restored = directory / "marketforge-normalize"
    if require_all and (
        not restored.is_file() or checksum(restored) != intent["executable_sha256"]
    ):
        raise MarketForgeError("Pinned normalization executable changed or is unavailable.")
    binary = (
        restored
        if restored.is_file() and checksum(restored) == intent["executable_sha256"]
        else resolve(paths)
    )

    def recover_outputs() -> list[dict[str, Any]]:
        verified: list[dict[str, Any]] = []
        for job in document["jobs"]:
            output = paths.root / job["output"]["dataset_path"]
            if output.exists() or output.is_symlink():
                evidence = committed_job(paths, binary, document, job, intent["submitted_sha256"])
                evidence["expected_raw_sha256"] = intent["source_sha256_by_job"][str(job["job_id"])]
                verified.append(evidence)
        return verified

    lifecycle("verify")
    try:
        verified = await joined_io(recover_outputs)
    except (ValueError, KeyError, TypeError) as error:
        raise MarketForgeError(f"Invalid normalization recovery evidence: {error}") from error
    aliases = intent.get("reused_outputs", {})
    index = WorkIndex(paths.root, intent["registry_id"], "normalize", intent["execution_id"])
    for original in aliases.values():
        await verify_reuse(paths, index, binary, original)
    if (not verified and not aliases) or (require_all and len(verified) != len(document["jobs"])):
        raise MarketForgeError(
            "Required committed outputs are unavailable for registration recovery."
        )
    if not verified:
        return {
            "registration": "registered",
            "verification": "verified",
            "registration_receipt": str(directory / "execution-result.json"),
            "catalogue_dataset_ids": {
                path: file_id
                for item in aliases.values()
                for path, file_id in item["publications"][item["path"]]["files"].items()
            },
        }
    lifecycle("register")
    result = await register_verified(
        paths,
        verified,
        receipt_directory=paths.runs_dir / f"normalize-recovery-{uuid4().hex}",
        execution={
            key: intent[key]
            for key in (
                "execution_id",
                "submitted_sha256",
                "allow_partial",
                "environment",
                "registry_id",
            )
        },
    )

    if result["registration"] == "registered":
        for job in document["jobs"]:
            job_id = str(job["job_id"])
            key = intent.get("work_keys", {}).get(job_id)
            path = str(paths.root / job["output"]["dataset_path"])
            publication = result.get("publications", {}).get(path)
            if key and publication:
                evidence = next(item for item in verified if item["path"] == path)
                await index.complete(
                    key,
                    {
                        "path": path,
                        "execution_id": intent["execution_id"],
                        "submission_path": str(submission),
                        "submitted_sha256": intent["submitted_sha256"],
                        "entry": {
                            **evidence["manifest"],
                            "path": path,
                            "verification": "verified",
                            "status": "complete",
                            "registration": "registered",
                            "manifest_sha256": evidence["manifest_sha256"],
                            "bytes_written": evidence["bytes_written"],
                            **publication,
                        },
                        "publications": {path: publication},
                    },
                    recovery=True,
                )
        for original in aliases.values():
            result["catalogue_dataset_ids"].update(
                original["publications"][original["path"]]["files"]
            )
    if result["registration"] != "registered":
        result["execution_receipt"] = str(intent_path)
        result["recovery"] = recovery_command(intent_path)
    return result


async def preview_reuse(
    paths: ProjectPaths,
    document: dict[str, Any],
    registry: str,
    *,
    binary: Path,
    historical: Path | None,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Use the same key and readiness checks before materializing a prepared batch."""
    with pinned_registry(registry):
        raw = await pin_raw_inputs(paths, document["jobs"])
    config = paths.runtime_config("normalize", historical)
    settings = await joined_io(inspect_runtime, paths, "normalize", config, binary)
    profiles = [
        {"id": item["id"], "sha256": item["sha256"]}
        for item in profile_inventory(paths, historical)
    ]
    digest = await joined_io(checksum, binary)
    keys = {
        str(job["job_id"]): normalize_key(
            registry,
            digest,
            document,
            job,
            raw[str(job["job_id"])],
            settings,
            profiles,
        )
        for job in document["jobs"]
    }
    index = WorkIndex(paths.root, registry, "normalize", "preview")
    results = await index.peek(keys.values())
    states: dict[str, Any] = {}
    ready: dict[str, dict[str, Any]] = {}
    for job_id, key in keys.items():
        if key not in results:
            states[job_id] = {"work_key": key, "status": "would_execute"}
            continue
        owner, intent, result = results[key]
        if result is None:
            status = (
                "needs_registration_recovery"
                if await joined_io(Path(intent["path"]).is_dir)
                else "in_progress"
            )
            states[job_id] = {"work_key": key, "status": status, "attempt": owner}
            continue
        try:
            await verify_reuse(paths, index, binary, result)
        except (MarketForgeError, ValueError, OSError, KeyError, TypeError) as error:
            states[job_id] = {"work_key": key, "status": "blocked", "reason": str(error)}
        else:
            states[job_id] = {"work_key": key, "status": "would_reuse", "path": result["path"]}
            ready[job_id] = result
    return states, ready
