"""Verify historical testing storage roots and describe unguarded legacy catalogues."""

import asyncio
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

from psycopg import AsyncConnection, Error

from forge.shared.database.connection import database_dsn, expected_database, is_testing
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


def managed_storage(paths: ProjectPaths, candidate: dict[str, Any], registry: str) -> Path:
    root = Path(candidate["path"])
    folder = {"normalize": "normalized", "merge": "merged", "convert": "converted"}.get(
        candidate["operation"]
    )
    if not root.is_absolute() or root.resolve() != root or root.name in {"", ".", ".."}:
        raise MarketForgeError("Publication path is not canonical.")
    if folder and root.parent == paths.datasets_dir / folder:
        return paths.data_root
    storage = root.parent.parent.parent
    # This subtree is the historical integration harness' documented storage
    # layout. The name restricts the search; the registry-bound report supplies
    # evidence of the workspace and exact managed output.
    if (
        not is_testing()
        or not folder
        or root.parent != storage / "datasets" / folder
        or storage.parent != paths.data_root / "tests/historical/normalize"
        or not storage.name.startswith("pipeline-")
        or storage.resolve() != storage
    ):
        raise MarketForgeError("Publication is outside every verified managed dataset directory.")
    intent = Path(candidate.get("evidence_source", ""))
    try:
        parts = intent.relative_to(storage / "runs").parts
        if (
            len(parts) != 5
            or parts[1:3] != ("steps", candidate["operation"])
            or parts[-1] != "execution-intent.json"
        ):
            raise ValueError
        report = storage / "runs" / parts[0] / "report.json"
        if report.resolve() != report or intent.resolve() != intent:
            raise ValueError
        evidence = cast(dict[str, Any], json.loads(report.read_bytes()))
        stage = evidence["stages"][candidate["operation"]]
        if (
            evidence["environment"] != "testing"
            or evidence["registry_id"] != registry
            or evidence["report_path"] != str(report)
            or stage["state"] != "complete"
            or stage["execution_intent"] != str(intent)
            or not {file["id"] for file in candidate["files"]} <= set(stage["primary_file_ids"])
        ):
            raise ValueError
    except (OSError, ValueError, TypeError, KeyError):
        raise MarketForgeError(
            "Historical path lacks a registry-bound pipeline report for the exact files."
        ) from None
    return storage


def path_identity(path: Path) -> dict[str, Any]:
    resolved = path.resolve(strict=False)
    try:
        info = resolved.stat()
    except FileNotFoundError:
        return {"resolved_path": str(resolved), "identity": None}
    return {"resolved_path": str(resolved), "identity": [info.st_dev, info.st_ino]}


def classify_references(
    rows: list[dict[str, Any]], project_root: str | None
) -> list[dict[str, Any]]:
    """Anchor only the documented project-relative layout using declared metadata."""
    plausible = {
        str(path.parents[2])
        for row in rows
        if row["path"]
        for path in [Path(row["path"])]
        if path.is_absolute() and len(path.parts) > 3 and path.parts[-3:-1] == ("data", "processed")
    }
    result: list[dict[str, Any]] = []
    for row in rows:
        raw = row["path"]
        info: dict[str, Any] = {**row, "anchor": None}
        if not raw:
            info["classification"] = "unresolvable_legacy_reference"
        else:
            source = Path(raw)
            if source.is_absolute() or (
                project_root
                and source.parts[:2] == ("data", "processed")
                and ".." not in source.parts
            ):
                absolute = source.is_absolute()
                if not absolute:
                    assert project_root is not None
                    info["anchor"] = project_root
                    source = Path(project_root) / source
                info.update(path_identity(source))
                info["classification"] = (
                    "missing_physical_target"
                    if info["identity"] is None
                    else "verified_absolute_location"
                    if absolute
                    else "relative_with_verified_historical_anchor"
                )
            elif ".." not in source.parts and source.parts[:2] == ("data", "processed"):
                info["classification"] = (
                    "relative_with_plausible_unverified_anchors"
                    if plausible
                    else "unresolvable_legacy_reference"
                )
                info["unanchored_legacy_path"] = True
                info["plausible_locations"] = [
                    path_identity(Path(anchor) / source) for anchor in sorted(plausible)
                ]
            else:
                info["classification"] = "unresolvable_legacy_reference"
        result.append(info)
    return result


@asynccontextmanager
async def catalogue(paths: ProjectPaths, *, lock: bool = False) -> AsyncGenerator[dict[str, Any]]:
    """Inspect the supported legacy catalogue; apply holds shared locks without writing it."""
    dsn = database_dsn(paths.root, testing=False)
    expected = expected_database(dsn, testing=False)
    async with await AsyncConnection.connect(dsn, connect_timeout=5) as connection:
        if not lock:
            await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        else:
            await connection.execute("SET LOCAL lock_timeout='5s'")
        identity = await connection.execute(
            """SELECT current_database(), to_regclass('forge_meta.database_identity') IS NOT NULL,
                      to_regclass('historical.dataset_publications') IS NOT NULL,
                      to_regclass('catalog.datasets') IS NOT NULL"""
        )
        row = await identity.fetchone()
        if not row or row[0] != expected:
            raise MarketForgeError(
                "Connected database differs from the configured production database."
            )
        if row[1] or row[2] or not row[3]:
            raise MarketForgeError(
                "Production is neither a verified Forge registry nor a supported legacy catalogue."
            )
        if lock:
            await connection.execute(
                "LOCK TABLE catalog.datasets, catalog.schema_meta IN SHARE MODE"
            )
        cursor = await connection.execute(
            "SELECT key,value FROM catalog.schema_meta WHERE key IN ('schema_version','project_root')"
        )
        metadata = dict(await cursor.fetchall())
        if metadata.get("schema_version") != "9":
            raise MarketForgeError(
                "Production catalogue schema is unsupported for legacy clearance."
            )
        project_root = metadata.get("project_root")
        if project_root and not Path(project_root).is_absolute():
            raise MarketForgeError("Declared legacy production project root is not absolute.")
        cursor = await connection.execute(
            """SELECT id,storage_path,data_type,storage_format,status
               FROM catalog.datasets ORDER BY id"""
        )
        raw = [
            {"id": item, "path": path, "data_type": kind, "storage_format": fmt, "status": state}
            for item, path, kind, fmt, state in await cursor.fetchall()
        ]
        references = await asyncio.to_thread(classify_references, raw, project_root)
        yield {
            "state": "legacy_catalogue_without_verified_registry",
            "database": expected,
            "schema_version": "9",
            "verified_project_root": project_root,
            "references": references,
            "unanchored_references": sum("resolved_path" not in ref for ref in references),
        }


async def production_catalogue(paths: ProjectPaths) -> dict[str, Any]:
    """Read legacy storage evidence without establishing a Forge registry identity."""
    try:
        async with catalogue(paths) as evidence:
            return evidence
    except Error as error:
        return {"state": "unavailable", "sqlstate": error.sqlstate}
    except (MarketForgeError, OSError, ValueError, RuntimeError) as error:
        return {"state": "unavailable", "reason": str(error)}


def overlaps(reference: Path, target: Path) -> bool:
    if reference.is_relative_to(target) or target.is_relative_to(reference):
        return True
    try:
        ref = reference.stat()
        own = target.stat()
    except FileNotFoundError:
        return False
    # Prefixes miss bind-mounted directories and hard-linked files. Compare
    # endpoint identity against the other side's ancestors, not common ancestors.
    return any(
        (info.st_dev, info.st_ino) == endpoint
        for parents, endpoint in (
            ((target, *target.parents), (ref.st_dev, ref.st_ino)),
            ((reference, *reference.parents), (own.st_dev, own.st_ino)),
        )
        for info in (path.stat() for path in parents)
    )


def ownership(evidence: dict[str, Any], targets: list[Path]) -> dict[str, Any]:
    if evidence.get("state") != "legacy_catalogue_without_verified_registry":
        return {"state": "unavailable", "known_reference_ids": [], "unresolved_reference_ids": []}
    known: list[int] = []
    unresolved: list[int] = []
    guessed_overlap: list[int] = []
    unsupported: list[int] = []
    for ref in evidence["references"]:
        if resolved := ref.get("resolved_path"):
            if any(overlaps(Path(resolved), target) for target in targets):
                known.append(ref["id"])
        else:
            unresolved.append(ref["id"])
            if not ref.get("unanchored_legacy_path"):
                unsupported.append(ref["id"])
            if any(
                overlaps(Path(location["resolved_path"]), target)
                for location in ref.get("plausible_locations", [])
                for target in targets
                if location["identity"] is not None
            ):
                guessed_overlap.append(ref["id"])
    state = (
        "known_foreign_reference"
        if known
        else "possible_foreign_overlap"
        if guessed_overlap
        else "unsupported_legacy_reference"
        if unsupported
        else "unresolved_legacy_references"
        if unresolved
        else "verified_disjoint"
    )
    return {
        "state": state,
        "known_reference_ids": known,
        "unresolved_reference_ids": unresolved,
        "plausible_overlap_ids": guessed_overlap,
        "unsupported_reference_ids": unsupported,
    }


def cleanup_paths(target: dict[str, Any], graph: dict[str, Any]) -> list[Path]:
    paths = {
        Path(path)
        for item in [target, *graph["dependents"]]
        for path in [item["path"], *(file["path"] for file in item["files"])]
    }
    return sorted(paths)
