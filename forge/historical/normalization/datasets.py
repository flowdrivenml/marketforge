"""Verify committed Rust outputs and idempotently register the existing catalogue."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.datasets.registration import register_files
from forge.historical.archives.selection import ArchiveSelectionError
from forge.historical.archives.storage.acquire import root_key
from forge.historical.archives.storage.files import disk
from forge.historical.engine.managed import checksum, resolve, supervised
from forge.historical.normalization.service import write_job
from forge.shared.database.connection import (
    DatabaseError,
    database_connection,
    database_dsn,
    database_purpose,
    registry_id,
    require_registry,
)
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


def verify(
    paths: ProjectPaths,
    source: Path,
    *,
    binary: Path | None = None,
    selected_files: set[str] | None = None,
) -> dict[str, Any]:
    root = source.parent if source.name == "manifest.json" else source
    if root.is_symlink():
        raise MarketForgeError("Dataset root must not be a symlink.")
    root = root.resolve()
    manifest_path = root / "manifest.json"
    if selected_files is None:
        response = supervised(
            [str(binary or resolve(paths)), "--validate-dataset", str(root)], cwd=paths.root
        )
        if response.returncode:
            raise MarketForgeError(f"Rust committed-output validation failed: {response.stdout}")
    manifest = json.loads(manifest_path.read_bytes())
    if manifest.get("protocol_version") not in {3, 4, 5} or manifest.get(
        "integrity_status"
    ) not in {"clean", "degraded"}:
        raise MarketForgeError("Unsupported or failed dataset manifest semantics.")
    job = json.loads((root / "processing-job.json").read_bytes())
    if (
        Path(job["output"]["dataset_path"]).resolve() != root
        or Path(job["output"]["staging_path"]).resolve() == root
    ):
        raise MarketForgeError("Dataset is not at its committed resolved-job destination.")
    for name in ("processing-job", "processing-runtime"):
        digest = checksum(root / f"{name}.json")
        if (root / f"{name}.sha256").read_text() != f"{digest}  {name}.json\n":
            raise MarketForgeError(f"Invalid committed {name} provenance checksum.")
    files = manifest["files"]
    if (
        len(files) != manifest["files_written"]
        or sum(item["rows"] for item in files) != manifest["events_written"]
        or min((file["start_timestamp_ns"] for file in files), default=None)
        != manifest["start_timestamp_ns"]
        or max((file["end_timestamp_ns"] for file in files), default=None)
        != manifest["end_timestamp_ns"]
    ):
        raise MarketForgeError("Manifest inventory totals or coverage disagree.")
    auxiliary = manifest.get("auxiliary_files", [])
    if selected_files is not None:
        directories = {Path(name).parent for name in selected_files}
        auxiliary = [
            item
            for item in auxiliary
            if manifest["content_type"] == "depth" and Path(item["path"]).parent in directories
        ]
    inventory = verify_inventory(
        root,
        [file for file in files if selected_files is None or file["path"] in selected_files]
        + auxiliary,
        (
            "manifest.json",
            "processing-job.json",
            "processing-runtime.json",
        ),
        require_commit_checksums=binary is not None or selected_files is not None,
        selected_only=selected_files is not None,
    )
    if (
        job.get("operation") != "process"
        or manifest["content_type"] != job["output"]["content_type"]
        or manifest["start_timestamp_ns"] is None
        or manifest["end_timestamp_ns"] is None
        or {task["task_id"] for task in job["tasks"]} != set(manifest["source_tasks"])
    ):
        raise MarketForgeError("Committed normalization metadata disagrees with its manifest.")
    namespace = manifest.get("instrument_namespace")
    if not namespace or any(
        task.get("raw_schema", {}).get("marketforge_source", {}).get("instrument_namespace")
        != namespace
        for task in job["tasks"]
    ):
        raise MarketForgeError("Output lacks namespace-qualified identity evidence.")
    return {
        "operation": "normalize",
        "content_type": manifest["content_type"],
        "inventory_sha256": inventory,
        "bytes_written": sum(
            item["size_bytes"] for item in files + manifest.get("auxiliary_files", [])
        ),
        "path": str(root),
        "manifest_path": str(manifest_path),
        "manifest_sha256": checksum(manifest_path),
        "manifest": manifest,
        "resolved_job": job,
        "instrument_namespace": namespace,
        "engine_dataset_id": str(manifest["dataset_id"]),
        "processing_job_sha256": checksum(root / "processing-job.json"),
        "processing_runtime_sha256": checksum(root / "processing-runtime.json"),
    }


def verify_inventory(
    root: Path,
    entries: list[dict[str, Any]],
    metadata: tuple[str, ...],
    *,
    require_commit_checksums: bool,
    selected_only: bool = False,
) -> dict[str, str]:
    sizes: dict[str, int] = {item["path"]: item["size_bytes"] for item in entries}
    if len(sizes) != len(entries) or set(sizes).intersection(metadata):
        raise MarketForgeError("Duplicate manifest inventory path.")
    result: dict[str, str] = {}
    for name in (*metadata, *sizes):
        relative = Path(name)
        file = root / relative
        if (
            not relative.parts
            or relative.is_absolute()
            or ".." in relative.parts
            or str(relative) != name
            or file.is_symlink()
            or file.resolve() != file
            or not file.is_file()
            or (name in sizes and file.stat().st_size != sizes[name])
        ):
            raise MarketForgeError(f"Committed inventory is missing, unsafe or changed: {name}")
        result[name] = checksum(file)
    sidecar = root / "publication-checksums.json"
    if sidecar.is_symlink():
        raise MarketForgeError("Publication checksum sidecar must not be a symlink.")
    has_sidecar = sidecar.is_file()
    parsed: object = json.loads(sidecar.read_bytes()) if has_sidecar else None
    stored = cast(dict[str, object], parsed) if isinstance(parsed, dict) else None
    if has_sidecar:
        extras = set(stored) - result.keys() if stored is not None else set[str]()
        normalized_provenance = {"processing-job.sha256", "processing-runtime.sha256"}
        if (
            extras >= normalized_provenance if selected_only else extras == normalized_provenance
        ) and {"processing-job.json", "processing-runtime.json"} <= result.keys():
            for name in sorted(normalized_provenance):
                artifact = root / name
                if (
                    not artifact.is_file()
                    or artifact.resolve() != artifact
                    or artifact.is_symlink()
                ):
                    raise MarketForgeError(f"Unsafe checksum provenance: {name}")
                result[name] = checksum(artifact)
    matches = (
        stored is not None and all(stored.get(name) == digest for name, digest in result.items())
        if selected_only
        else stored == result
    )
    if (has_sidecar and not matches) or (require_commit_checksums and not has_sidecar):
        raise MarketForgeError("Publication checksums are missing or disagree with the output.")
    return result


async def raw_inputs(
    c: AsyncConnection[TupleRow], paths: ProjectPaths, evidence: dict[str, Any]
) -> set[tuple[int, str]]:
    inputs: set[tuple[int, str]] = set()
    raw_root = await disk(paths.raw_dir.resolve)
    for name, digest in evidence["raw_sha256"].items():
        path = Path(name)
        if not path.is_relative_to(raw_root):
            raise MarketForgeError(f"Required raw lineage is unavailable for {path}.")
        cur = await c.execute(
            """SELECT l.id,l.sha256 FROM historical.local_archives l
            WHERE l.status='complete' AND l.storage_root_key=%s AND l.relative_path=%s
            AND (l.external_archive_id IS NULL OR EXISTS (
                SELECT 1 FROM historical.raw_interpretations i
                WHERE i.sha256=l.sha256
                AND i.import_provenance #>> '{binding,readiness,normalization_handoff}'='ready'
                AND (i.id=l.external_archive_id OR EXISTS (
                    SELECT 1 FROM historical.raw_bindings b
                    WHERE b.local_archive_id=l.id AND b.interpretation_id=i.id
                ))
            ))
            ORDER BY l.id FOR SHARE OF l""",
            (root_key(paths.raw_dir), str(path.relative_to(raw_root))),
        )
        rows = await cur.fetchall()
        if len(rows) != 1 or rows[0][1] != digest:
            raise MarketForgeError(f"Required raw lineage is ambiguous or changed for {path}.")
        inputs.add(rows[0])
    return inputs


async def raw_link_map(
    c: AsyncConnection[TupleRow], paths: ProjectPaths, evidence: dict[str, Any]
) -> dict[str, tuple[int, str]]:
    return {
        name: next(iter(await raw_inputs(c, paths, {"raw_sha256": {name: digest}})))
        for name, digest in sorted(evidence["raw_sha256"].items())
    }


def primary_reference(source: Path) -> tuple[Path, str | None]:
    if source.is_symlink():
        raise MarketForgeError("A publication reference must not be a symlink.")
    if not source.is_file() or source.name == "manifest.json":
        return source, None
    absolute = source.absolute()
    if absolute.resolve() != absolute:
        raise MarketForgeError("A primary file reference must be canonical.")
    for root in absolute.parents:
        if (root / "manifest.json").is_file():
            return root, absolute.relative_to(root).as_posix()
    raise MarketForgeError("No owning publication manifest found for the selected file.")


def hash_inputs(paths: ProjectPaths, tasks: list[dict[str, Any]]) -> dict[str, str]:
    source_paths = sorted({(paths.root / task["input_path"]).resolve() for task in tasks})
    root = paths.raw_dir.resolve()
    for source in source_paths:
        if not source.is_relative_to(root) or not source.is_file():
            raise MarketForgeError(f"Required managed raw input is unavailable: {source}")
    return {str(source): checksum(source) for source in source_paths}


async def pin_raw_inputs(
    paths: ProjectPaths,
    jobs: list[dict[str, Any]],
    *,
    expected: dict[str, tuple[int, str]] | None = None,
) -> dict[str, dict[str, str]]:
    try:
        hashes = await disk(hash_inputs, paths, [task for job in jobs for task in job["tasks"]])
    except MarketForgeError as error:
        if expected is not None:
            raise ArchiveSelectionError(str(error)) from error
        raise
    async with database_connection(database_dsn(paths.root)) as c:
        async with c.transaction():
            if expected is None:
                await raw_inputs(c, paths, {"raw_sha256": hashes})
            elif {
                path: digest for path, (_, digest) in expected.items()
            } != hashes or await raw_link_map(c, paths, {"raw_sha256": hashes}) != expected:
                raise ArchiveSelectionError(
                    "Submitted jobs differ from the exact acquired archive selection."
                )
    return {
        str(job["job_id"]): {
            str((paths.root / task["input_path"]).resolve()): hashes[
                str((paths.root / task["input_path"]).resolve())
            ]
            for task in job["tasks"]
        }
        for job in jobs
    }


async def dataset_inputs(c: AsyncConnection[TupleRow], evidence: dict[str, Any]) -> list[int]:
    ids: list[int] = []
    for item in evidence["merge_inputs"]:
        cur = await c.execute(
            """SELECT d.id FROM historical.datasets d
            JOIN historical.dataset_publications p ON p.id=d.publication_id
            WHERE d.operation='normalize' AND d.status='complete' AND d.id=%s
              AND p.storage_root=%s AND p.instrument_namespace=%s AND p.engine_dataset_id=%s
              AND p.evidence->>'manifest_sha256'=%s AND d.primary_file=%s AND d.file_sha256=%s
            FOR SHARE OF d""",
            (
                item["database_id"],
                item["path"],
                item["namespace"],
                str(item["engine_id"]),
                item["manifest_sha256"],
                item["primary_file"],
                item["file_sha256"],
            ),
        )
        rows = await cur.fetchall()
        if len(rows) != 1:
            raise MarketForgeError(
                f"Required normalized lineage is unavailable for {item['path']}."
            )
        ids.append(rows[0][0])
    return ids


async def register_verified(
    paths: ProjectPaths,
    verified: list[dict[str, Any]],
    *,
    receipt_directory: Path,
    execution: dict[str, Any] | None = None,
) -> dict[str, Any]:
    await disk(receipt_directory.mkdir, parents=True, exist_ok=True)
    ordered = sorted(
        (dict(e) for e in verified),
        key=lambda e: (e["instrument_namespace"], e["engine_dataset_id"]),
    )
    receipt: dict[str, Any] = {
        "schema_version": 1,
        "environment": database_purpose(),
        "publication": "committed",
        "registration": "pending",
        "evidence": ordered,
        "execution": execution,
        "registration_receipt": str(receipt_directory / "registration-result.json"),
    }
    write_job(receipt_directory / "registration-intent.json", receipt)
    try:
        for evidence in ordered:
            if evidence["operation"] == "normalize":
                hashes = await disk(hash_inputs, paths, evidence["resolved_job"]["tasks"])
                expected = evidence.get("expected_raw_sha256")
                if expected is not None and hashes != expected:
                    raise MarketForgeError("Raw archives changed since the request was submitted.")
                evidence["raw_sha256"] = hashes
        database_ids: dict[str, int] = {}
        file_states: dict[str, str] = {}
        publications: dict[str, dict[str, Any]] = {}
        async with database_connection(database_dsn(paths.root)) as c:
            async with c.transaction():
                identity = await registry_id(c)
                if execution is not None:
                    require_registry(execution, identity)
                receipt["registry_id"] = identity
                for evidence in ordered:
                    await c.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                        (evidence["instrument_namespace"] + ":" + evidence["engine_dataset_id"],),
                    )
                for evidence in ordered:
                    if evidence["operation"] == "normalize":
                        links = await raw_link_map(c, paths, evidence)
                        evidence["raw_links_by_path"] = links
                        raw = set(links.values())
                        parents = []
                    else:
                        raw = set[tuple[int, str]]()
                        parents = await dataset_inputs(c, evidence)
                    publication = await register_files(
                        c, evidence, execution=execution, raw=raw, parents=parents
                    )
                    publications[evidence["path"]] = publication
                    database_ids.update(publication["files"])
                    file_states.update(publication["file_states"])
        receipt.update(
            registration="registered",
            catalogue_dataset_ids=database_ids,
            publications=publications,
            file_states=file_states,
            counts={
                "publications": len(publications),
                "primary_files": len(database_ids),
                "new": list(file_states.values()).count("new"),
                "reused": list(file_states.values()).count("reused"),
            },
        )
        if len(database_ids) == 1:
            receipt["catalogue_dataset_id"] = next(iter(database_ids.values()))
    except (MarketForgeError, OSError, ValueError) as error:
        receipt["registration"] = "pending" if isinstance(error, DatabaseError) else "failed"
        receipt["error"] = str(error)
    write_job(receipt_directory / "registration-result.json", receipt)
    return receipt


async def register(paths: ProjectPaths, source: Path) -> dict[str, Any]:
    try:
        root, selected = await disk(primary_reference, source)
        evidence = await disk(verify, paths, root)
        if selected is not None:
            evidence["selected_primary"] = selected
    except (ValueError, KeyError, TypeError) as error:
        raise MarketForgeError(f"Invalid normalization output evidence: {error}") from error
    return await register_verified(
        paths, [evidence], receipt_directory=paths.runs_dir / f"dataset-register-{uuid4().hex}"
    )
