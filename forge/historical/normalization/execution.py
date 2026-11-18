"""Execute immutable submitted JSON using the managed Rust engine, without a recipe."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
import signal
import subprocess
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from forge.historical.engine.managed import checksum, resolve, supervised
from forge.historical.normalization.datasets import pin_raw_inputs, register_verified, verify
from forge.historical.normalization.service import write_job
from forge.historical.profiles.service import discover
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


def _capture(stream: Any, path: Path, *, limit: int = 16 * 1024 * 1024) -> None:
    """Drain every byte while bounding retained logs and resident buffers."""
    retained = 0
    with path.open("wb") as output:
        while chunk := stream.read(65536):
            remaining = max(0, limit - retained)
            output.write(chunk[:remaining])
            retained += min(len(chunk), remaining)
    stream.close()


def execute(
    paths: ProjectPaths,
    source: Path,
    *,
    tui: bool = False,
    allow_partial: bool = False,
    recipe: str | None = None,
) -> dict[str, Any]:
    binary = resolve(paths)
    protocol = supervised([str(binary), "--protocol"], cwd=paths.root)
    capabilities = json.loads(protocol.stdout) if protocol.returncode == 0 else {}
    if 4 not in capabilities.get("batch_schema_versions", []) or not capabilities.get(
        "result_file"
    ):
        raise MarketForgeError(
            "Run mf engine build normalize to install the managed result protocol."
        )
    if tui and not (os.isatty(0) and os.isatty(1) and os.isatty(2)):
        raise MarketForgeError("--tui requires an interactive terminal.")
    operation = paths.runs_dir / f"normalize-execute-{uuid4().hex}"
    operation.mkdir(parents=True, exist_ok=False)
    expected_binary = checksum(binary)
    pinned_binary = operation / "marketforge-normalize"
    shutil.copyfile(binary, pinned_binary)
    pinned_binary.chmod(0o755)
    if checksum(pinned_binary) != expected_binary:
        raise MarketForgeError("Managed executable changed during execution preparation.")
    binary = pinned_binary
    submission = operation / "submitted-batch.json"
    # read_bytes preserves all edits and integer/decimal lexical representations.
    submission.write_bytes(source.read_bytes())
    config = operation / "general.toml"
    shutil.copyfile(paths.config_dir / "historical/general.toml", config)
    historical = operation / "historical"
    profile_evidence: list[dict[str, Any]] = []
    for item in discover(paths):
        original = Path(item["location"])
        target = historical / "profiles" / item["id"].rsplit(".", 1)[1] / original.name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(original, target)
        if checksum(target) != item["sha256"]:
            raise MarketForgeError("Profile changed while freezing execution interpretation.")
        profile_evidence.append({"id": item["id"], "sha256": checksum(target)})
    try:
        document = json.loads(submission.read_bytes())
    except ValueError as error:
        raise MarketForgeError(f"Invalid prepared normalization batch: {error}") from error
    if not isinstance(document, dict) or document.get("status") != "prepared":
        raise MarketForgeError("Only fully prepared normalization requests can be executed.")
    prepared = source.parent / "prepare-report.json"
    if prepared.is_file():
        evidence = json.loads(prepared.read_text())
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
    validation = supervised(
        [
            str(binary),
            "--validate-batch",
            str(submission),
            "--project-root",
            str(paths.root),
            "--config",
            str(config),
            "--profiles-root",
            str(historical),
        ],
        cwd=paths.root,
    )
    if validation.returncode:
        raise MarketForgeError(f"Production batch validation failed: {validation.stdout}")
    for job in document["jobs"]:
        for key in ("dataset_path", "staging_path"):
            destination = Path(job["output"][key])
            if not destination.is_absolute():
                destination = paths.root / destination
            if destination.exists() or destination.is_symlink():
                raise MarketForgeError(f"Execution output already exists: {destination}")
    pinned_raw = asyncio.run(pin_raw_inputs(paths, document["jobs"]))
    receipt: dict[str, Any] = {
        "schema_version": 1,
        "environment": "testing",
        "execution_id": operation.name,
        "operation": "normalize",
        "run_id": document["run_id"],
        "recipe": recipe,
        "allow_partial": allow_partial,
        "source_sha256_by_job": pinned_raw,
        "execution_receipt": str(operation / "execution-intent.json"),
        "report_path": str(operation / "execution-result.json"),
        "engine_result_path": str(operation / "engine-result.json"),
        "submission_path": str(submission),
        "submitted_source": str(source.resolve()),
        "submitted_sha256": checksum(submission),
        "configuration_sha256": checksum(config),
        "profiles": profile_evidence,
        "executable_sha256": checksum(binary),
        "started_at": datetime.now(UTC).isoformat(),
        "normalization": "running",
        "registrations": [],
    }
    sync_submission(submission)
    write_job(operation / "execution-intent.json", receipt)
    argv.extend(["--result-file", receipt["engine_result_path"]])
    try:
        code, interrupted = run_process(argv, paths.root, operation, tui=tui)
    except OSError as error:
        receipt.update(status="failed", normalization="failed", error=str(error))
        write_job(operation / "execution-result.json", receipt)
        return receipt
    receipt["exit_code"] = code
    receipt["finished_at"] = datetime.now(UTC).isoformat()
    receipt["normalization"] = (
        "interrupted" if interrupted else "complete" if code == 0 else "failed"
    )
    write_job(operation / "engine-outcome.json", receipt)
    try:
        finish(paths, document, receipt, binary, operation)
    except KeyboardInterrupt:
        receipt.update(
            status="interrupted",
            recovery=f"mf normalize register {operation / 'execution-intent.json'} --testing",
        )
    write_job(operation / "execution-result.json", receipt)
    return receipt


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
    # Rust owns terminal rendering. TUI stdout must remain inherited; structured
    # publication is recovered from the exact submitted job destinations below.
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
    expected = {job["job_id"]: job for job in document["jobs"]}
    receipt.update(
        status="failed",
        verification="not_started",
        registration="not_started",
        counts={"requested": len(expected), "successful": 0, "failed": 0, "registered": 0},
        jobs=[],
        recovery=f"mf normalize register {directory / 'execution-intent.json'} --testing",
    )
    if receipt["normalization"] == "interrupted":
        receipt["status"] = "interrupted"
        return
    verified: list[dict[str, Any]] = []
    failures = 0
    try:
        result = json.loads(Path(receipt["engine_result_path"]).read_bytes())
        receipt["engine_result"] = result
        if (
            result["kind"] != "normalization_result"
            or result["schema_version"] != 1
            or result["run_id"] != document["run_id"]
            or result["error"] is not None
            or len(result["jobs"]) != len(expected)
        ):
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
            evidence = committed_job(
                paths, binary, document, submitted, receipt["submitted_sha256"]
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
            entry.update(
                verification="verified",
                instrument_namespace=evidence["instrument_namespace"],
                integrity_status=manifest["integrity_status"],
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
    registration = asyncio.run(
        register_verified(
            paths,
            verified,
            receipt_directory=directory,
            execution={
                key: receipt[key] for key in ("execution_id", "submitted_sha256", "allow_partial")
            },
        )
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


async def recover(paths: ProjectPaths, source: Path) -> dict[str, Any]:
    intent = json.loads(await asyncio.to_thread(source.read_bytes))
    if intent.get("operation") != "normalize" or intent.get("environment") != "testing":
        raise MarketForgeError("Expected a managed normalization execution-intent.json receipt.")
    directory = (await asyncio.to_thread(source.resolve)).parent
    submission = directory / "submitted-batch.json"
    if checksum(submission) != intent["submitted_sha256"]:
        raise MarketForgeError("Recovery submission fingerprint differs from execution intent.")
    document = json.loads(submission.read_bytes())
    restored = directory / "marketforge-normalize"
    binary = (
        restored
        if restored.is_file() and checksum(restored) == intent["executable_sha256"]
        else resolve(paths)
    )

    def recover_outputs() -> list[dict[str, Any]]:
        verified = []
        for job in document["jobs"]:
            output = paths.root / job["output"]["dataset_path"]
            if output.exists() or output.is_symlink():
                evidence = committed_job(paths, binary, document, job, intent["submitted_sha256"])
                evidence["expected_raw_sha256"] = intent["source_sha256_by_job"][str(job["job_id"])]
                verified.append(evidence)
        return verified

    try:
        verified = await asyncio.to_thread(recover_outputs)
    except (ValueError, KeyError, TypeError) as error:
        raise MarketForgeError(f"Invalid normalization recovery evidence: {error}") from error
    if not verified:
        raise MarketForgeError("No committed outputs are available for registration recovery.")
    return await register_verified(
        paths,
        verified,
        receipt_directory=paths.runs_dir / f"normalize-recovery-{uuid4().hex}",
        execution={
            key: intent[key] for key in ("execution_id", "submitted_sha256", "allow_partial")
        },
    )
