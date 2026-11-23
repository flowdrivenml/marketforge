"""Retire reviewed publications and remove only their verified managed inventory."""

import asyncio
import json
import os
import re
import stat
import tomllib
from pathlib import Path
from typing import Any, cast
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from psycopg.types.json import Jsonb

from forge.historical.engine.managed import checksum
from forge.shared.database.connection import registry_id
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths
from forge.storage.managed_files import FileChanged, FileSnapshot, snapshot, unlink_reviewed


def reject_nested_mounts(root: Path) -> None:
    # Linux bind mounts may share a device and report false from ismount().
    table = Path("/proc/self/mountinfo")
    if not table.is_file():
        return
    for line in table.read_text().splitlines():
        fields = line.split()
        if len(fields) < 6:
            raise FileChanged("Could not verify the local mount inventory.")
        location = Path(re.sub(r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), fields[4]))
        if location.is_relative_to(root):
            raise FileChanged(f"Publication or a child is mounted: {location}.")


def scan_tree(root: Path) -> tuple[dict[str, FileSnapshot], dict[str, tuple[int, int]]]:
    reject_nested_mounts(root)
    if root.resolve() != root or root.is_symlink() or not root.is_dir() or os.path.ismount(root):
        raise FileChanged("Publication directory is missing, aliased or mounted.")
    info = root.stat()
    directories = {"": (info.st_dev, info.st_ino)}
    files: dict[str, FileSnapshot] = {}
    for parent, names, items in os.walk(root, followlinks=False, onerror=raise_walk_error):
        for name in names:
            path = Path(parent) / name
            child = path.lstat()
            if (
                not stat.S_ISDIR(child.st_mode)
                or child.st_dev != info.st_dev
                or os.path.ismount(path)
            ):
                raise FileChanged(f"Unsafe publication directory {path}.")
            directories[path.relative_to(root).as_posix()] = (child.st_dev, child.st_ino)
        for name in items:
            relative = (Path(parent) / name).relative_to(root).as_posix()
            evidence = snapshot(root, relative)
            if not evidence.exists or evidence.device != info.st_dev:
                raise FileChanged(f"Publication file changed: {relative}.")
            files[relative] = evidence
    return files, directories


def raise_walk_error(error: OSError) -> None:
    raise error


def include_normalization_sidecars(
    root: Path,
    expected: dict[str, str],
    names: set[str],
) -> None:
    """Account for verified historical provenance outside older registry inventories."""
    for name in ("processing-job", "processing-runtime"):
        marker = f"{name}.sha256"
        if marker in names and marker not in expected:
            fields = (root / marker).read_text().split()
            if fields != [expected.get(f"{name}.json"), f"{name}.json"]:
                raise FileChanged(f"Publication checksum pointer disagrees: {marker}.")
            expected[marker] = checksum(root / marker)
    if not any(name.endswith("/sources.json") and name not in expected for name in names):
        return
    if checksum(root / "manifest.json") != expected.get("manifest.json"):
        raise FileChanged("Publication manifest changed before reviewing stream provenance.")
    manifest: dict[str, Any] = json.loads((root / "manifest.json").read_bytes())
    for stream in manifest["streams"]:
        relative = Path(stream["directory"]) / "sources.json"
        if relative.as_posix() not in names or relative.as_posix() in expected:
            continue
        source = root / relative
        if json.loads(source.read_bytes()) != stream:
            raise FileChanged(
                f"Stream provenance disagrees with the publication manifest: {relative}."
            )
        expected[relative.as_posix()] = checksum(source)


def review_tree(paths: ProjectPaths, candidate: dict[str, Any]) -> dict[str, Any]:
    root = Path(candidate["path"])
    folder = {"normalize": "normalized", "merge": "merged", "convert": "converted"}
    if root.parent != paths.datasets_dir / folder[candidate["operation"]]:
        raise FileChanged("Publication is outside the selected managed dataset directory.")
    files, directories = scan_tree(root)
    expected = dict(candidate["inventory_sha256"])
    if candidate["operation"] != "convert":
        expected["publication-checksums.json"] = candidate["checksum_index_sha256"]
    if candidate["operation"] == "normalize":
        include_normalization_sidecars(root, expected, set(files))
    if set(files) != set(expected):
        raise FileChanged("The directory contains additional or missing files.")
    allowed = {""}
    for name in expected:
        allowed.update(str(parent) if parent != Path(".") else "" for parent in Path(name).parents)
    if set(directories) != allowed:
        raise FileChanged("The directory contains an unreviewed subdirectory.")
    for name, digest in expected.items():
        if checksum(root / name) != digest:
            raise FileChanged(f"Reviewed checksum changed: {name}.")
    return {
        "root": str(root),
        "files": {name: item.model_dump() for name, item in files.items()},
        "directories": {name: list(item) for name, item in directories.items()},
        "physical_bytes": sum((root / name).stat().st_blocks * 512 for name in files),
    }


def remove_reviewed_tree(review: dict[str, Any]) -> None:
    """Resume only the reviewed inodes still present after an interruption."""
    root = Path(review["root"])
    if not root.exists() and not root.is_symlink():
        return
    current, directories = scan_tree(root)
    expected = {name: FileSnapshot.model_validate(value) for name, value in review["files"].items()}
    if any(expected.get(name) != entry for name, entry in current.items()) or any(
        review["directories"].get(name) != list(entry) for name, entry in directories.items()
    ):
        raise FileChanged("Publication changed after retirement; remaining files were preserved.")
    for name in current:
        unlink_reviewed(root, name, expected[name])
    for name in sorted(directories, key=lambda part: (part.count("/"), len(part)), reverse=True):
        directory = root / name
        child = directory.lstat()
        if (child.st_dev, child.st_ino) != directories[name]:
            raise FileChanged("Publication directory changed during removal.")
        directory.rmdir()
        descriptor = os.open(directory.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


async def claim(
    connection: AsyncConnection[TupleRow],
    target: dict[str, Any],
    canonical: dict[str, Any] | None,
    cleanup_id: UUID,
    review: dict[str, Any] | None,
    blockers: list[str],
    *,
    force_plan: dict[str, Any] | None = None,
) -> bool:
    """Persist the canonical replacement and remove a ready target from selection."""
    explicit = bool(force_plan and force_plan.get("explicit_delete"))
    if canonical is None and not explicit:
        raise MarketForgeError("Deduplication requires a verified canonical publication.")
    target_id = target["publication_id"]
    survivor = canonical["publication_id"] if canonical else None
    async with connection.transaction():
        await connection.execute(
            "LOCK TABLE historical.dataset_publications, historical.datasets, historical.dataset_inputs, "
            "historical.execution_runs IN SHARE ROW EXCLUSIVE MODE"
        )
        retained = (force_plan or {}).get("retained", [])
        identities = [item["publication_id"] for item in retained]
        if target_id is not None:
            identities.append(target_id)
        if survivor is not None:
            identities.append(survivor)
        rows = await connection.execute(
            """SELECT p.id,p.storage_root,p.evidence->>'manifest_sha256',
                      (SELECT array_agg(d.id ORDER BY d.id) FROM historical.datasets d
                       WHERE d.publication_id=p.id AND d.status='complete')
               FROM historical.dataset_publications p WHERE p.id=ANY(%s) ORDER BY p.id""",
            (identities,),
        )
        actual = {row[0]: row[1:] for row in await rows.fetchall()}
        for item in (target, canonical, *retained):
            if item is None or item.get("output_ref"):
                continue
            if actual.get(item["publication_id"]) != (
                item["path"],
                item["manifest_sha256"],
                sorted(file["id"] for file in item["files"]),
            ):
                raise MarketForgeError("Publication registration changed after review.")
        if explicit and target.get("output_ref"):
            if not review or force_plan is None:
                raise MarketForgeError("Explicit deletion requires a reviewed physical target.")
            await claim_converted(connection, target, cleanup_id, review, force_plan)
            return True
        previous_aliases: list[dict[str, Any]] = []
        if force_plan:
            await connection.execute(
                "LOCK TABLE historical.resolved_work IN SHARE ROW EXCLUSIVE MODE"
            )
            if explicit:
                prior = await connection.execute(
                    """SELECT 1 FROM historical.publication_supersessions
                       WHERE publication_id=%s OR canonical_id=%s LIMIT 1""",
                    (target_id, target_id),
                )
                if await prior.fetchone():
                    raise MarketForgeError("Publication already participates in a supersession.")
                active_work = await connection.execute(
                    """SELECT 1 FROM historical.resolved_work
                       WHERE evidence->>'path'=%s AND result IS NULL""",
                    (target["path"],),
                )
                if await active_work.fetchone():
                    raise MarketForgeError("An existing work claim still owns the publication.")
                await connection.execute(
                    """DELETE FROM historical.resolved_work
                       WHERE registry_id=%s AND operation=%s AND result->>'path'=%s""",
                    (await registry_id(connection), target["operation"], target["path"]),
                )
            else:
                assert canonical is not None
                previous_aliases = await prepare_canonical_switch(
                    connection, target, canonical, force_plan["equivalent_ids"]
                )
        else:
            assert canonical is not None
            prior = await connection.execute(
                """SELECT publication_id,canonical_id,state FROM historical.publication_supersessions
                   WHERE publication_id=ANY(%s)""",
                ([target_id, survivor],),
            )
            prior_rows = await prior.fetchall()
            for publication, replacement, state in prior_rows:
                if publication == survivor or replacement != survivor or state != "retained":
                    raise MarketForgeError(
                        "A conflicting canonical selection or cleanup already exists."
                    )
            if not review and prior_rows:
                return False
            work = await connection.execute(
                """SELECT result->>'path' FROM historical.resolved_work
                   WHERE operation=%s AND work_key=%s FOR SHARE""",
                (target["operation"], target["work_key"]),
            )
            index = await work.fetchone()
            if index is not None and index[0] != canonical["path"]:
                raise MarketForgeError(
                    "The work index has an active owner or another canonical path."
                )
        if force_plan and not review:
            raise MarketForgeError("Force cascade requires a reviewed physical target.")
        if review:
            active = await connection.execute(
                "SELECT 1 FROM historical.execution_runs WHERE status IN ('pending','running') LIMIT 1"
            )
            if await active.fetchone():
                raise MarketForgeError("Selected registry has an active historical run.")
            if force_plan is not None:
                from forge.datasets.force_cleanup import (
                    dependency_graph,
                    graph_blockers,
                    require_testing,
                    revalidate_graph,
                )

                require_testing()
                assert target_id is not None
                graph = await dependency_graph(connection, target_id)
                if not await revalidate_graph(connection, graph, force_plan) or graph_blockers(
                    graph,
                    set(force_plan["protected_ids"] if explicit else force_plan["equivalent_ids"]),
                ):
                    raise MarketForgeError("Registered dependencies changed since force preview.")
                for node in graph["dependents"]:
                    await connection.execute(
                        """INSERT INTO historical.publication_invalidations
                           (publication_id,source_publication_id,cleanup_id,evidence)
                           VALUES(%s,%s,%s,%s)""",
                        (node["publication_id"], target_id, cleanup_id, Jsonb(node)),
                    )
                    await connection.execute(
                        """UPDATE historical.datasets SET status='superseded'
                           WHERE publication_id=%s AND status='complete'""",
                        (node["publication_id"],),
                    )
            else:
                deps = await connection.execute(
                    """SELECT 1 FROM historical.dataset_inputs WHERE input_dataset_id=ANY(%s)
                       UNION ALL SELECT 1 FROM historical.dataset_publications
                       WHERE evidence->'selected_dataset_inputs' ?| %s
                          OR evidence->'selected_dataset_inputs' @> ANY(%s::jsonb[])
                       LIMIT 1""",
                    (
                        [f["id"] for f in target["files"]],
                        [str(f["id"]) for f in target["files"]],
                        [Jsonb([f["id"]]) for f in target["files"]],
                    ),
                )
                if await deps.fetchone():
                    raise MarketForgeError("A registered dependency appeared after review.")
            await connection.execute(
                "UPDATE historical.datasets SET status='superseded' WHERE publication_id=%s AND status='complete'",
                (target_id,),
            )
        await connection.execute(
            """INSERT INTO historical.publication_supersessions
                (publication_id,canonical_id,state,cleanup_id,evidence) VALUES (%s,%s,%s,%s,%s)
               ON CONFLICT (publication_id) DO UPDATE
               SET canonical_id=EXCLUDED.canonical_id, state=EXCLUDED.state,
                   cleanup_id=EXCLUDED.cleanup_id, evidence=EXCLUDED.evidence
               WHERE publication_supersessions.state='retained'""",
            (
                target_id,
                survivor,
                "retiring" if review else "retained",
                cleanup_id,
                Jsonb(
                    {
                        "target": target,
                        "canonical": canonical,
                        "review": review,
                        "blockers": blockers,
                        "reason": (
                            "Explicit testing publication deletion"
                            if explicit
                            else "Verified equivalent historical work"
                        ),
                        "work_identity_version": 1,
                        **(
                            {"force_plan": force_plan, "previous_aliases": previous_aliases}
                            if force_plan
                            else {}
                        ),
                    }
                ),
            ),
        )

    return True


async def prepare_canonical_switch(
    connection: AsyncConnection[TupleRow],
    target: dict[str, Any],
    survivor: dict[str, Any],
    equivalent_ids: list[int],
) -> list[dict[str, Any]]:
    """Retarget mutable links and work results in the retirement transaction."""
    target_id, survivor_id = target["publication_id"], survivor["publication_id"]
    cursor = await connection.execute(
        """SELECT publication_id,canonical_id,state,to_jsonb(s)
           FROM historical.publication_supersessions s
           WHERE publication_id=ANY(%s) OR canonical_id=%s ORDER BY publication_id
           FOR UPDATE""",
        ([target_id, survivor_id], target_id),
    )
    previous: list[dict[str, Any]] = []
    for publication, canonical, state, evidence in await cursor.fetchall():
        original = evidence["evidence"].get("target", {})
        removed_alias = (
            state == "removed"
            and canonical == target_id
            and original.get("work_key") == survivor["work_key"]
            and original.get("contract_sha256") == survivor["contract_sha256"]
            and publication not in {target_id, survivor_id}
        )
        if not removed_alias and (
            state != "retained"
            or publication not in equivalent_ids
            or canonical not in equivalent_ids
            or (publication == survivor_id and canonical != target_id)
        ):
            raise MarketForgeError("A historical canonical link cannot be safely reassigned.")
        previous.append(evidence)
    await connection.execute(
        "DELETE FROM historical.publication_supersessions WHERE publication_id=%s AND canonical_id=%s",
        (survivor_id, target_id),
    )
    await connection.execute(
        "UPDATE historical.publication_supersessions SET canonical_id=%s WHERE canonical_id=%s",
        (survivor_id, target_id),
    )
    registry = await registry_id(connection)
    keys = {target["work_key"], survivor["work_key"]}
    for item in (target, survivor):
        if item.get("current_profile_order_key"):
            keys.add(item["current_profile_order_key"])
    for key in sorted(keys):
        cursor = await connection.execute(
            """SELECT result->>'path' FROM historical.resolved_work
               WHERE registry_id=%s AND operation=%s AND work_key=%s FOR UPDATE""",
            (registry, target["operation"], key),
        )
        old = await cursor.fetchone()
        if old is not None and old[0] not in {target["path"], survivor["path"]}:
            raise MarketForgeError(
                "The work index has an active owner or an unrelated canonical path."
            )
        if old == (survivor["path"],):
            continue
        result = survivor["reuse_result"]
        await connection.execute(
            """INSERT INTO historical.resolved_work(registry_id,operation,work_key,owner,evidence,result)
               VALUES (%s,%s,%s,%s,%s,%s)
               ON CONFLICT (registry_id,operation,work_key)
               DO UPDATE SET owner=excluded.owner,evidence=excluded.evidence,result=excluded.result""",
            (
                registry,
                target["operation"],
                key,
                result["execution_id"],
                Jsonb(
                    {
                        "path": survivor["path"],
                        "source": "force_canonical_selection",
                        "report_path": survivor["evidence_source"],
                    }
                ),
                Jsonb(result),
            ),
        )
    return previous


async def claim_converted(
    connection: AsyncConnection[TupleRow],
    target: dict[str, Any],
    cleanup_id: UUID,
    review: dict[str, Any],
    force_plan: dict[str, Any],
) -> None:
    """Keep the exact converted output receipt while retiring its mutable index entry."""
    from forge.datasets.force_cleanup import require_testing

    require_testing()
    await connection.execute("LOCK TABLE historical.resolved_work IN SHARE ROW EXCLUSIVE MODE")
    active = await connection.execute(
        "SELECT 1 FROM historical.execution_runs WHERE status IN ('pending','running') LIMIT 1"
    )
    if await active.fetchone():
        raise MarketForgeError("Selected registry has an active historical run.")
    identity = await registry_id(connection)
    ref = target["output_ref"]
    row = await connection.execute(
        """SELECT evidence->>'path',result FROM historical.resolved_work
           WHERE registry_id=%s AND operation='convert' AND work_key=%s""",
        (identity, ref),
    )
    if await row.fetchone() != (target["path"], target["indexed_result"]):
        raise MarketForgeError("Indexed conversion changed after review.")
    inserted = await connection.execute(
        """INSERT INTO historical.converted_output_deletions
           (registry_id,work_key,cleanup_id,state,evidence) VALUES (%s,%s,%s,'retiring',%s)
           ON CONFLICT (registry_id,work_key) DO UPDATE
           SET cleanup_id=EXCLUDED.cleanup_id,state='retiring',removed_at=NULL,
               created_at=clock_timestamp(),evidence=EXCLUDED.evidence || jsonb_build_object(
                   'prior_deletions',COALESCE(converted_output_deletions.evidence->'prior_deletions','[]'::jsonb)
                     || jsonb_build_array(to_jsonb(converted_output_deletions)-'evidence'
                        || jsonb_build_object('evidence',converted_output_deletions.evidence-'prior_deletions')))
           WHERE converted_output_deletions.state='removed'""",
        (
            identity,
            ref,
            cleanup_id,
            Jsonb(
                {
                    "target": target,
                    "canonical": None,
                    "review": review,
                    "force_plan": force_plan,
                    "reason": "Explicit testing conversion deletion",
                }
            ),
        ),
    )
    if inserted.rowcount != 1:
        raise MarketForgeError("A conversion deletion is already in progress.")
    await connection.execute(
        """DELETE FROM historical.resolved_work
           WHERE registry_id=%s AND operation='convert' AND work_key=%s""",
        (identity, ref),
    )


async def finish(connection: AsyncConnection[TupleRow], publication_id: int) -> None:
    await connection.execute(
        """UPDATE historical.publication_supersessions SET state='removed',removed_at=clock_timestamp()
           WHERE publication_id=%s AND state='retiring'""",
        (publication_id,),
    )


def local_artifact_dependencies(
    paths: ProjectPaths, target: dict[str, Any], *, covered: list[dict[str, Any]] | None = None
) -> list[str]:
    """Inspect executable preparations, retained recovery intents and recipe selections."""
    root = Path(target["path"])
    ids = {file["id"] for file in target["files"] if "id" in file}
    publication = target["publication_id"]
    runs = {paths.runs_dir, root.parent.parent.parent / "runs"}
    references: list[str] = []
    force_patterns = (
        (
            "*/submitted-merge.json",
            "*/steps/*/*/submitted-merge.json",
            "*/submitted-job.json",
            "*/convert.json",
        )
        if covered is not None
        else ()
    )
    documents = {
        file
        for directory in runs
        for pattern in (
            "*/merge-job.json",
            "*/job-*.json",
            "*/conversion-collection.json",
            "*/execution-intent.json",
            "*/steps/*/*/execution-intent.json",
            "*/jobs/merge/*.json",
            *force_patterns,
        )
        for file in directory.glob(pattern)
    }
    documents.update(
        file
        for storage in {paths.data_root, root.parent.parent.parent}
        for pattern in (
            ("*/execution.json", "*/conversion-runtime.json", "*/manifest.json")
            if covered is not None
            else ("*/execution.json",)
        )
        for file in (storage / "datasets" / "converted").glob(pattern)
    )
    execution: dict[str, Any] = target["execution"] or {}
    for file in sorted(documents):
        if target.get("output_ref") and file.is_relative_to(root):
            continue
        if file.resolve() != file or file.stat().st_size > 64 * 1024 * 1024:
            references.append(f"Cannot safely inspect {file}.")
            continue
        try:
            document: object = json.loads(file.read_bytes())
        except (OSError, ValueError):
            references.append(f"Cannot inspect {file}.")
            continue
        document_owner = intent_owner(document)
        if (
            file.name == "execution-intent.json"
            and execution.get("execution_id")
            and document_owner == execution["execution_id"]
        ):
            continue
        if target.get("output_ref") and conversion_creation_reference(file, document, target):
            continue
        # Compare exact identities, never a textual substring match.
        if contains_reference(document, root, ids, publication) and not covered_artifact(
            document, file, covered or []
        ):
            references.append(str(file))
    for directory in (paths.workflow_dir("merge"), paths.workflow_dir("convert")):
        for file in sorted(directory.glob("*.toml")):
            try:
                document = tomllib.loads(file.read_text())
            except (OSError, ValueError):
                references.append(f"Cannot inspect {file}.")
                continue
            if contains_reference(document, root, ids, publication):
                references.append(str(file))
    return references


def conversion_creation_reference(file: Path, document: object, target: dict[str, Any]) -> bool:
    """Recognize just the historical preparation that produced this exact output."""
    result = target["indexed_result"]
    pin: dict[str, Any] = result.get("submission") or {}
    if str(file) == pin.get("path"):
        return checksum(file) == pin.get("sha256")
    collection = result.get("collection_path")
    if not collection or file.parent != Path(collection).parent:
        return False
    from forge.historical.conversion.service import retained_collection

    try:
        _, prepared, _ = retained_collection(Path(collection))
        if prepared.get("registry_id") != result["registry_id"]:
            return False
        jobs = list(prepared["jobs"])
        own = [
            index
            for index, job in enumerate(jobs)
            if job["job_id"] == result["job_id"]
            and job["output"] == target["path"]
            and job["sha256"] == pin.get("sha256")
        ]
        if len(own) != 1:
            return False
        entry = jobs[own[0]]
        if str(file) == entry["path"]:
            return checksum(file) == entry["sha256"]
        if str(file) == collection and document == prepared:
            jobs[own[0]] = {**entry, "output": None}
            return not contains_reference(
                {**prepared, "jobs": jobs}, Path(target["path"]), set(), None
            )
    except (MarketForgeError, OSError, KeyError, TypeError, ValueError):
        return False
    return False


def intent_owner(document: object) -> object:
    if isinstance(document, dict):
        return cast(dict[str, object], document).get("execution_id")
    return None


def covered_artifact(document: object, file: Path, covered: list[dict[str, Any]]) -> bool:
    """Exempt exact registered jobs and their attributable completed submissions."""
    if not isinstance(document, dict):
        return False
    record = cast(dict[str, Any], document)
    for node in covered:
        if file.name != "execution-intent.json" and node["job"] and record == node["job"]:
            return True
        execution: dict[str, Any] = node["execution"] or {}
        owner = execution.get("execution_id")
        if (
            file.name == "execution-intent.json"
            and owner
            and file.parent.name == owner
            and record.get("execution_id") == owner
            and record.get("submitted_sha256") == execution.get("submitted_sha256")
            and record.get("path") == node["path"]
        ):
            return True
    return False


def cascade_artifact_dependencies(
    paths: ProjectPaths, target: dict[str, Any], dependents: list[dict[str, Any]]
) -> list[str]:
    blockers: set[str] = set()
    for item in [target, *dependents]:
        blockers.update(local_artifact_dependencies(paths, item, covered=dependents))
    return sorted(blockers)


def contains_reference(
    value: object, root: Path, ids: set[int], publication: int | None, field: str = ""
) -> bool:
    if isinstance(value, dict):
        return any(
            contains_reference(item, root, ids, publication, str(key))
            for key, item in cast(dict[object, object], value).items()
        )
    if isinstance(value, list):
        return any(
            contains_reference(item, root, ids, publication, field)
            for item in cast(list[object], value)
        )
    if isinstance(value, str):
        return value.startswith("/") and Path(value).is_relative_to(root)
    if type(value) is int:
        if field in {"publication_id", "publication_ids", "registry_publication_id"}:
            return value == publication
        return (
            field
            in {
                "database_id",
                "dataset_id",
                "dataset_ids",
                "input_dataset_id",
                "primary_file_ids",
                "selected_dataset_inputs",
                "selected_dataset_ids",
                "upstream_file_ids",
                "registry_primary_file_id",
            }
            and value in ids
        )
    return False


async def referenced_paths(
    connection: AsyncConnection[TupleRow],
    target: Path,
    excluded_id: int | None = None,
    *,
    include_lineage: bool = False,
    excluded_output_ref: str | None = None,
) -> list[str]:
    """Find overlapping registered paths, including filesystem aliases."""
    cursor = await connection.execute(
        """SELECT storage_root FROM historical.dataset_publications
           WHERE %s::bigint IS NULL OR id<>%s
           UNION SELECT d.storage_path FROM historical.datasets d
           WHERE d.publication_id IS NULL AND d.storage_path IS NOT NULL
           UNION SELECT evidence->>'path' FROM historical.resolved_work
           WHERE operation='convert' AND evidence ? 'path'
             AND (%s::text IS NULL OR work_key<>%s)""",
        (excluded_id, excluded_id, excluded_output_ref, excluded_output_ref),
    )
    overlaps: list[str] = []
    for (value,) in await cursor.fetchall():
        path = Path(value)
        if not path.is_absolute():
            if path.parts:
                overlaps.append(str(path))
            continue
        physical = await asyncio.to_thread(path.resolve)
        if physical.is_relative_to(target) or target.is_relative_to(physical):
            overlaps.append(str(path))
            continue
        try:
            if physical.samefile(target):
                overlaps.append(str(path))
        except FileNotFoundError:
            continue
    if include_lineage:
        cursor = await connection.execute(
            """SELECT id,storage_root,evidence->'resolved_job',evidence->'merge_inputs'
               FROM historical.dataset_publications WHERE operation='merge'"""
        )
        for identity, root, job, inputs in await cursor.fetchall():
            if identity != excluded_id and any(
                contains_reference(evidence, target, set(), -1) for evidence in (job, inputs)
            ):
                overlaps.append(root)
    return overlaps
