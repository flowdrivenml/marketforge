"""Reviewed deletion of run history and exclusively owned execution artifacts."""

from __future__ import annotations

import json
import os
from contextlib import ExitStack
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, LiteralString, cast
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import TupleRow, dict_row
from pydantic import BaseModel, ConfigDict, ValidationError, model_validator

from forge.datasets import legacy_storage
from forge.datasets.publication_cleanup import scan_tree
from forge.orchestration.inspection import MANAGED_RUN
from forge.orchestration.registry import connection_for
from forge.orchestration.run_visibility import cleanup_directory, mirror
from forge.orchestration.runs import safe_reference
from forge.shared.database.connection import (
    DatabaseError,
    database_environment,
    database_purpose,
    pinned_registry,
)
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import OwnershipError, WorkspaceOwnership, acquire_file_lock
from forge.shared.paths import ProjectPaths
from forge.storage.cleanup_plans import digest, new_plan_id, write_document
from forge.storage.managed_files import FileChanged, FileSnapshot, parent_directory, unlink_reviewed

ACTIVE = {"pending", "running"}
ARTIFACT_KINDS = {"prepared_batch", "prepared_job", "report", "engine_execution"}
PROTECTED_NAMES = {
    "manifest.json",
    "publication-checksums.json",
    "processing-job.json",
    "processing-runtime.json",
}
KNOWN_FILES = {
    "report.json",
    "events.jsonl",
    "intent.json",
    "outcome.json",
    "preparation.json",
    "recipe-source.json",
    "recipe.toml",
    "submitted.toml",
    "resolved-plan.json",
    "pipeline-report.json",
    "acquisition.json",
    "normalize-batch.json",
    "prepare-report.json",
    "execution-intent.json",
    "execution-result.json",
    "execution.json",
    "engine-outcome.json",
    "engine-result.json",
    "registration-result.json",
    "submitted-batch.json",
    "requested-batch.json",
    "submitted-merge.json",
    "submitted-job.json",
    "conversion-collection.json",
    "merge-job.json",
    "reuse-result.json",
    "normalize.toml",
    "merge.toml",
    "convert.toml",
    "general.toml",
    "conversion-runtime.json",
    "normalization-runtime.json",
    "merge-runtime.json",
    "marketforge-normalize",
    "marketforge-merge",
    "marketforge-convert",
}
CONSUMER_NAMES = {
    "report.json",
    "execution-intent.json",
    "execution-result.json",
    "prepare-report.json",
    "conversion-collection.json",
    "merge-job.json",
    "submitted-job.json",
    "submitted-merge.json",
    "reuse-result.json",
    "acquisition.json",
    "resolved-plan.json",
}
ID_FIELDS = {"execution_id", "engine_execution_id"}
MAX_RUNS = 1000
MAX_PLAN = 32 * 1024 * 1024


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Tree(Model):
    root: str
    files: dict[str, FileSnapshot]
    directories: dict[str, tuple[int, int]]


class Target(Model):
    run_id: str
    operation: str = ""
    status: str = ""
    parent: str | None = None
    attempt_count: int = 0
    resource_count: int = 0
    fingerprint: str = ""
    roots: list[str] = []
    trees: list[Tree] = []
    execution_owners: list[str] = []
    retained: list[str] = []
    missing: list[str] = []
    blockers: list[str] = []

    @property
    def bytes(self) -> int:
        return sum(item.size for tree in self.trees for item in tree.files.values())


class Plan(Model):
    schema_version: Literal[1] = 1
    operation: Literal["runs_delete"] = "runs_delete"
    plan_id: UUID
    environment: Literal["production", "testing"]
    registry_id: str
    workspace: str
    storage: str
    mode: Literal["full", "history_only"]
    requested: list[str]
    with_children: bool
    targets: list[Target]
    examined_other_registry: str | None = None
    created_at: str

    @model_validator(mode="after")
    def exact_selection(self) -> Plan:
        identities = {target.run_id for target in self.targets}
        if (
            not self.requested
            or not set(self.requested) <= identities
            or len(identities) != len(self.targets)
            or len(identities) > MAX_RUNS
            or any(
                set(tree.root for tree in target.trees) - set(target.roots)
                for target in self.targets
            )
            or (self.mode == "history_only" and any(target.trees for target in self.targets))
        ):
            raise ValueError("Inconsistent run deletion selection or artifact roots.")
        return self

    @property
    def blockers(self) -> bool:
        return any(target.blockers for target in self.targets)


def contained(path: Path, root: Path) -> bool:
    try:
        return path.is_absolute() and path.resolve() == path and path.is_relative_to(root)
    except (OSError, RuntimeError, ValueError):
        return False


def matches_path(value: str, roots: list[Path], paths: ProjectPaths) -> bool:
    if not value or "\n" in value:
        return False
    path = Path(value)
    if not path.is_absolute() and "/" not in value:
        return False
    candidates = (path,) if path.is_absolute() else (paths.root / path, paths.data_root / path)
    return any(
        os.path.commonpath((os.path.normpath(item), str(root))) == str(root)
        for root in roots
        for item in candidates
    )


def reference(value: object, roots: list[Path], paths: ProjectPaths, key: str = "") -> bool:
    if isinstance(value, str):
        return (key in ID_FIELDS and any(value == root.name for root in roots)) or matches_path(
            value, roots, paths
        )
    if isinstance(value, dict):
        return any(
            reference(item, roots, paths, str(name))
            for name, item in cast(dict[str, object], value).items()
        )
    if isinstance(value, (list, tuple)):
        return any(
            reference(item, roots, paths, key)
            for item in cast(list[object] | tuple[object, ...], value)
        )
    return False


def load_evidence(path: Path, root: Path, limit: int = 4 * 1024 * 1024) -> dict[str, Any]:
    if not contained(path, root):
        return {}
    try:
        with path.open("rb") as stream:
            raw = stream.read(limit + 1)
        if len(raw) > limit:
            return {}
        value = json.loads(raw)
        return cast(dict[str, Any], value) if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def owns(evidence: dict[str, Any], row: dict[str, Any], registry: str) -> bool:
    return (
        evidence.get("registry_id") == registry
        and evidence.get("environment") == row["database_purpose"]
        and (evidence.get("logical_run_id") or evidence.get("run_id")) == row["run_id"]
    )


def verified_root(
    paths: ProjectPaths, row: dict[str, Any], registry: str, *, resuming: bool
) -> Path:
    root = Path(row["artifact_root"])
    if root.name != row["run_id"] or not contained(root, paths.data_root):
        raise MarketForgeError(f"Indexed artifact root has unsafe or mismatched ownership: {root}")
    if root.parent == paths.runs_dir:
        return root
    storage = root.parent.parent
    historical = (
        row["database_purpose"] == "testing"
        and root.parent == storage / "runs"
        and storage.parent == paths.data_root / "tests/historical/normalize"
        and storage.name.startswith("pipeline-")
    )
    report = root / "report.json"
    if historical and (resuming or owns(load_evidence(report, root), row, registry)):
        return root
    raise MarketForgeError(
        f"Historical artifact root lacks registry-bound workspace evidence: {root}"
    )


async def gather(
    connection: AsyncConnection[TupleRow], query: LiteralString, args: tuple[Any, ...] = ()
) -> list[dict[str, Any]]:
    async with connection.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(query, args)
        return list(await cursor.fetchall())


async def expand(
    connection: AsyncConnection[TupleRow], selected: list[str], descend: bool
) -> tuple[list[str], set[str]]:
    # PostgreSQL UNION deduplicates overlapping selections and terminates even if old links form a cycle.
    if not descend:
        return sorted(set(selected)), set()
    entries = await gather(
        connection,
        """
        WITH RECURSIVE expanded(id) AS (
          SELECT unnest(%s::text[]) UNION
          SELECT r.run_id FROM historical.execution_runs r JOIN expanded e ON r.parent_run_id=e.id
        )
        SELECT e.id, r.parent_run_id FROM expanded e LEFT JOIN historical.execution_runs r ON r.run_id=e.id
        ORDER BY e.id LIMIT 1001
    """,
        (selected,),
    )
    if len(entries) > MAX_RUNS:
        raise MarketForgeError("The reviewed selection exceeds 1,000 runs.")
    parents = {row["id"]: row["parent_run_id"] for row in entries}
    cycles: set[str] = set()
    for identity in parents:
        visited: set[str] = set()
        node = identity
        while node in parents and node not in visited:
            visited.add(node)
            node = parents[node]
        if node in visited:
            cycles.add(identity)
    return list(parents), cycles


def inventory(root: Path, *, check_names: bool) -> Tree | None:
    if not root.exists() and not root.is_symlink():
        return None
    files, dirs = scan_tree(root)
    if check_names:
        for relative in files:
            path = Path(relative)
            if path.name in PROTECTED_NAMES or (
                path.name not in KNOWN_FILES and path.suffix not in {".log", ".jsonl"}
            ):
                raise MarketForgeError(f"No disposable run-artifact contract for {root / path}.")
    return Tree(root=str(root), files=files, directories=dirs)


async def dependencies(
    connection: AsyncConnection[TupleRow],
    paths: ProjectPaths,
    targets: list[Target],
    selected: list[str],
    *,
    foreign: bool = False,
) -> None:
    full_roots = [Path(tree.root) for target in targets for tree in target.trees]
    # Detached references also matter for liveness even when ownership of their
    # contents could not be proved and their contents will be retained.
    related = {
        target.run_id: [
            Path(value) for value in (*target.roots, *target.retained, *target.execution_owners)
        ]
        for target in targets
    }
    paths_reviewed = sorted({str(root) for roots in related.values() for root in roots})
    names = sorted({root.name for roots in related.values() for root in roots})
    if not paths_reviewed:
        return
    tokens = sorted(
        set(
            [
                *paths_reviewed,
                *(
                    str(Path(p).relative_to(paths.root))
                    for p in paths_reviewed
                    if Path(p).is_relative_to(paths.root)
                ),
                *(f"runs/{name}" for name in names),
                *names,
            ]
        )
    )
    # Work owners and registered execution IDs are operational lookup keys, unlike
    # a logical run ID in a descriptive label. Paths are rechecked structurally.
    work = await gather(
        connection,
        """
        SELECT operation,work_key,owner,evidence,result FROM historical.resolved_work w
        WHERE owner=ANY(%s) OR EXISTS (
          SELECT 1 FROM unnest(%s::text[]) AS token
          WHERE strpos(w.evidence::text,token)>0 OR strpos(w.result::text,token)>0
        ) LIMIT 10001
    """,
        (names, tokens),
    )
    if len(work) > 10000:
        raise MarketForgeError("Too many matching work references to review safely.")
    suffix = " in the other registry" if foreign else ""
    for target in targets:
        roots = related[target.run_id]
        owned = (
            [
                Path(value)
                for value in (*[tree.root for tree in target.trees], *target.execution_owners)
            ]
            if target.trees
            else []
        )
        for row in work:

            def linked(scope: list[Path], work_row: dict[str, Any] = row) -> bool:
                return bool(scope) and (
                    any(work_row["owner"] == root.name for root in scope)
                    or reference(work_row["evidence"], scope, paths)
                    or reference(work_row["result"], scope, paths)
                )

            if row["result"] is None and linked(roots):
                target.blockers.append(
                    f"Unfinished {row['operation']} work claim {row['work_key']}{suffix}, owner {row['owner']}."
                )
            elif linked(owned):
                target.blockers.append(
                    f"Completed-work reuse {row['operation']} {row['work_key']}{suffix} requires artifacts from {row['owner']}."
                )
    if foreign:
        foreign_owners = await gather(
            connection,
            "SELECT run_id,artifact_root FROM historical.execution_runs WHERE artifact_root=ANY(%s)",
            (paths_reviewed,),
        )
        for target in targets:
            for row in foreign_owners:
                if row["artifact_root"] in target.roots:
                    target.blockers.append(
                        f"Run {row['run_id']} in the other registry owns {row['artifact_root']}."
                    )
    if not full_roots:
        return
    publications = await gather(
        connection,
        """
        SELECT p.id,p.storage_root,p.manifest_path,p.evidence FROM historical.dataset_publications p
        WHERE EXISTS (SELECT 1 FROM unnest(%s::text[]) token WHERE
          strpos(p.evidence::text,token)>0 OR strpos(p.storage_root,token)>0
          OR strpos(p.manifest_path,token)>0) LIMIT 10001
    """,
        (tokens,),
    )
    if len(publications) > 10000:
        raise MarketForgeError("Too many matching publications to review safely.")
    legacy = await gather(
        connection,
        """
        SELECT d.id,d.operation_details,d.storage_path,d.manifest_path,d.result_report_path
        FROM historical.datasets d WHERE d.publication_id IS NULL AND EXISTS (
            SELECT 1 FROM unnest(%s::text[]) token WHERE strpos(d.operation_details::text,token)>0
              OR strpos(d.storage_path,token)>0 OR strpos(d.manifest_path,token)>0
              OR strpos(d.result_report_path,token)>0
        ) LIMIT 10001
    """,
        (tokens,),
    )
    if len(legacy) > 10000:
        raise MarketForgeError("Too many matching legacy dataset references to review safely.")
    retained_runs = await gather(
        connection,
        """
        WITH refs AS (
            SELECT run_id,artifact_root AS location FROM historical.execution_runs
            UNION ALL SELECT run_id,report_path FROM historical.execution_runs
            UNION ALL SELECT run_id,report_path FROM historical.execution_attempts
            UNION ALL SELECT run_id,outcome_path FROM historical.execution_attempts
            UNION ALL SELECT a.run_id,x.reference
              FROM historical.execution_resources x JOIN historical.execution_attempts a USING(attempt_id)
              WHERE x.kind=ANY(%s)
        )
        SELECT DISTINCT run_id,location FROM refs
        WHERE NOT (run_id=ANY(%s)) AND EXISTS (
            SELECT 1 FROM unnest(%s::text[]) token WHERE strpos(location,token)>0
        ) LIMIT 10001
    """,
        (sorted(ARTIFACT_KINDS), [] if foreign else selected, tokens),
    )
    if len(retained_runs) > 10000:
        raise MarketForgeError("Too many matching run references to review safely.")
    for target in targets:
        owned = (
            [
                Path(value)
                for value in (*[tree.root for tree in target.trees], *target.execution_owners)
            ]
            if target.trees
            else []
        )
        if not owned:
            continue
        for pub in publications:
            if reference(pub, owned, paths):
                target.blockers.append(
                    f"Registry publication {pub['id']}{suffix} requires retained registration/provenance evidence in {', '.join(map(str, owned))}."
                )
        for item in legacy:
            if reference(item, owned, paths):
                target.blockers.append(
                    f"Legacy dataset {item['id']}{suffix} references retained evidence in {', '.join(map(str, owned))}."
                )
        for row in retained_runs:
            if matches_path(row["location"], owned, paths) or any(
                row["location"] == p.name for p in owned
            ):
                target.blockers.append(
                    f"Surviving run {row['run_id']}{suffix} references {row['location']}."
                )


def local_dependencies(paths: ProjectPaths, targets: list[Target]) -> None:
    scopes = [
        (target, [Path(tree.root) for tree in target.trees]) for target in targets if target.trees
    ]
    if not scopes:
        return
    owned = {root for _, roots in scopes for root in roots}
    search = {
        paths.runs_dir,
        *(root.parent for root in owned),
        paths.normalization_jobs_dir,
        paths.data_root / ".jobs" / "process",
    }
    examined = 0
    for directory in sorted(search):
        if directory.is_symlink() or not directory.is_dir():
            continue
        for parent, children, files in os.walk(directory, followlinks=False):
            base = Path(parent)
            # Selected trees are not surviving consumers.
            children[:] = sorted(name for name in children if base / name not in owned)
            examined += len(children) + len(files)
            if examined > 100000:
                raise MarketForgeError("Retained dependency inventory exceeds the bounded search.")
            for name in sorted(set(files) & CONSUMER_NAMES):
                path = base / name
                if not contained(path, directory):
                    continue
                document = load_evidence(path, directory, 16 * 1024 * 1024)
                for target, roots in scopes:
                    if reference(document, roots, paths):
                        target.blockers.append(
                            f"Surviving preparation, recovery or pipeline evidence {path} references an owned artifact."
                        )


def owns_execution(file: Path, root: Path, row: dict[str, Any], registry: str) -> bool:
    document = load_evidence(file, root)
    return (
        document.get("execution_id") == file.parent.name
        and document.get("registry_id") == registry
        and document.get("environment") == row["database_purpose"]
    )


def review_owned_evidence(
    tree: Tree, target: Target, row: dict[str, Any], registry: str, paths: ProjectPaths
) -> None:
    root = Path(tree.root)
    for name in tree.files:
        file = root / name
        if file.name == "execution-intent.json":
            document = load_evidence(file, root)
            if owns_execution(file, root, row, registry):
                target.execution_owners.append(str(file.parent))
            # A physically committed but unregistered output may not yet appear
            # in dataset_publications. Keep its receipt and intent for recovery.
            result = load_evidence(file.with_name("execution-result.json"), root)
            if result.get("verification") == "verified" and result.get("registration") in {
                "failed",
                "pending",
                "not_started",
            }:
                target.blockers.append(
                    f"Registration recovery uses {file}; verification succeeded and registration is incomplete."
                )
            for key in ("output", "dataset_path", "output_path"):
                value = document.get(key)
                if isinstance(value, str):
                    output = Path(value)
                    if (
                        contained(output, paths.datasets_dir)
                        and (output / "manifest.json").is_file()
                    ):
                        target.blockers.append(
                            f"Committed publication at {output} requires execution evidence {file}."
                        )
        elif file.name == "reuse-result.json":
            document = load_evidence(file, root)
            if (
                document.get("operation") == "normalize"
                and document.get("disposition") == "reused"
                and document.get("verification") == "verified"
                and document.get("registration") == "registered"
                and document.get("recipe_source")
            ):
                target.blockers.append(
                    f"Normalization-source discovery uses the verified reuse mapping {file}."
                )
    target.execution_owners = sorted(set(target.execution_owners))


async def review(
    paths: ProjectPaths,
    connection: AsyncConnection[TupleRow],
    registry: str,
    requested: list[str],
    *,
    children: bool,
    mode: Literal["full", "history_only"],
    prior: Plan | None = None,
) -> Plan:
    ids, cycles = await expand(connection, requested, children)
    records = await gather(
        connection,
        """
        SELECT * FROM historical.execution_runs
        WHERE run_id=ANY(%s) AND database_id=%s AND database_purpose=%s
    """,
        (ids, registry, database_purpose()),
    )
    by_id = {row["run_id"]: row for row in records}
    direct_children = await gather(
        connection,
        """
        SELECT run_id,parent_run_id,database_id::text,database_purpose
        FROM historical.execution_runs WHERE parent_run_id=ANY(%s) ORDER BY run_id
    """,
        (ids,),
    )
    attempts = await gather(
        connection,
        """
        SELECT * FROM historical.execution_attempts WHERE run_id=ANY(%s) ORDER BY run_id,number
    """,
        (ids,),
    )
    resources = await gather(
        connection,
        """
        SELECT a.run_id,x.stage,x.kind,x.reference,x.attempt_id
        FROM historical.execution_resources x JOIN historical.execution_attempts a USING(attempt_id)
        WHERE a.run_id=ANY(%s) ORDER BY a.run_id,a.number,x.stage,x.kind,x.reference
    """,
        (ids,),
    )
    existing = await gather(
        connection,
        """
        SELECT run_id,mode,state,plan_id FROM historical.execution_run_deletions
        WHERE database_id=%s AND run_id=ANY(%s)
    """,
        (registry, ids),
    )
    marks = {row["run_id"]: row for row in existing}
    targets: list[Target] = []
    previous = {target.run_id: target for target in prior.targets} if prior else {}
    for identity in ids:
        target = Target(run_id=identity)
        targets.append(target)
        row = by_id.get(identity)
        if row is None:
            target.blockers.append("Run is absent from the selected registry.")
            continue
        own_attempts = [item for item in attempts if item["run_id"] == identity]
        own_resources = [item for item in resources if item["run_id"] == identity]
        related = [item for item in direct_children if item["parent_run_id"] == identity]
        target.operation, target.status, target.parent = (
            row["operation"],
            row["status"],
            row["parent_run_id"],
        )
        target.attempt_count, target.resource_count = len(own_attempts), len(own_resources)
        target.fingerprint = digest((row, own_attempts, own_resources, related))
        mark = marks.get(identity)
        if mark and (not prior or mark["plan_id"] != prior.plan_id):
            target.blockers.append(
                f"History already has a {mark['state']} marker. Inspect the original cleanup receipt."
            )
        if identity in cycles:
            target.blockers.append("Indexed parent–child relationships contain a cycle.")
        if any(
            (item["database_id"], item["database_purpose"]) != (registry, database_purpose())
            for item in related
        ):
            target.blockers.append("A child belongs to another registry or environment.")
        if mode == "full" and (
            survivors := [item["run_id"] for item in related if item["run_id"] not in ids]
        ):
            target.blockers.append(
                f"Surviving children {', '.join(survivors)} require this run; use --with-children or --history-only."
            )
        if row["status"] in ACTIVE or any(item["status"] == "running" for item in own_attempts):
            target.blockers.append(
                f"Nonterminal run or attempt. After work stops, inspect mf runs reconcile --{database_purpose()}."
            )
        try:
            root = verified_root(paths, row, registry, resuming=bool(prior))
        except MarketForgeError as error:
            target.blockers.append(str(error))
            continue
        target.roots = [str(root)]
        pointers = {
            value
            for value in [
                row["report_path"],
                *(a["report_path"] for a in own_attempts),
                *(x["reference"] for x in own_resources if x["kind"] in ARTIFACT_KINDS),
            ]
            if isinstance(value, str)
        }
        for pointer in sorted(pointers):
            location = Path(pointer)
            if not location.is_absolute() and MANAGED_RUN.fullmatch(pointer):
                location = root.parent / pointer
            if location.is_relative_to(root):
                continue
            if not contained(location, root.parent) or location == root.parent:
                # Output directories and foreign paths are never cleanup targets.
                if location.is_absolute():
                    target.retained.append(str(location))
                continue
            detached = root.parent / location.relative_to(root.parent).parts[0]
            if not MANAGED_RUN.fullmatch(detached.name) and not detached.name.startswith(
                "normalize-"
            ):
                target.retained.append(str(detached))
                continue
            evidence = any(
                owns(load_evidence(detached / name, detached), row, registry)
                for name in (
                    "execution-intent.json",
                    "execution-result.json",
                    "prepare-report.json",
                    "report.json",
                )
            )
            if evidence and detached.name not in by_id:
                target.roots.append(str(detached))
            else:
                target.retained.append(str(detached))
        target.roots = sorted(set(target.roots))
        target.retained = sorted(set(target.retained) - set(target.roots))
        if prior and identity in previous:
            # An interrupted full cleanup may already have removed ownership
            # files. Reuse only the roots verified in the committed plan.
            target.roots = previous[identity].roots
            target.retained = previous[identity].retained
        if mode == "history_only":
            target.retained = sorted(set([*target.retained, *target.roots]))
            for root_path in map(Path, target.roots):
                for file in [
                    root_path / "execution-intent.json",
                    *root_path.glob("steps/*/*/execution-intent.json"),
                ]:
                    if owns_execution(file, root_path, row, registry):
                        target.execution_owners.append(str(file.parent))
            target.execution_owners = sorted(set(target.execution_owners))
            continue
        if (
            row["operation"] == "pipeline"
            and row["status"] != "completed"
            and (root / "report.json").is_file()
        ):
            target.blockers.append(
                f"Pipeline resume uses {root / 'report.json'}; use --history-only."
            )
        if row["status"] == "prepared":
            target.blockers.append(
                f"Prepared run {identity} retains executable or selection evidence; use --history-only."
            )
        for owned_root in target.roots:
            location = Path(owned_root)
            try:
                tree = inventory(location, check_names=not prior)
                if tree is None:
                    target.missing.append(owned_root)
                else:
                    if not prior:
                        target.trees.append(tree)
                    review_owned_evidence(tree, target, row, registry, paths)
                # Current per-tree fingerprint validation is done for all roots
                # immediately before apply, including roots partly removed on retry.
            except (OSError, MarketForgeError) as error:
                target.blockers.append(f"{location}: {error}")
        if prior and identity in previous:
            target.trees = previous[identity].trees
            target.execution_owners = sorted(
                set(target.execution_owners) | set(previous[identity].execution_owners)
            )
    aliases = {
        root for target in targets for root in target.roots if Path(root).name != target.run_id
    }
    if aliases:
        separately_indexed = await gather(
            connection,
            """
            SELECT run_id,artifact_root FROM historical.execution_runs WHERE artifact_root=ANY(%s)
        """,
            (sorted(aliases),),
        )
        for row in separately_indexed:
            for target in targets:
                if row["artifact_root"] in target.roots:
                    target.blockers.append(
                        f"Detached directory is separately indexed by run {row['run_id']}: {row['artifact_root']}."
                    )
    owners: dict[str, list[Target]] = {}
    for target in targets:
        for root in target.roots:
            owners.setdefault(root, []).append(target)
    for root, bound in owners.items():
        if len(bound) > 1:
            for target in bound:
                target.blockers.append(f"Conflicting indexed ownership of {root}.")
    await dependencies(connection, paths, targets, ids)
    other_registry = None
    # Foreign registries can use the same workspace. When one is configured
    # and accessible, check only references to these reviewed artifacts.
    try:
        with database_environment(testing=database_purpose() != "testing"), pinned_registry(None):
            async with connection_for(paths) as (foreign_connection, other):
                other_registry = other
                await dependencies(foreign_connection, paths, targets, ids, foreign=True)
    except DatabaseError:
        if other_registry is not None:
            raise MarketForgeError(
                "The other registry could not complete its dependency check."
            ) from None
    if other_registry is None and database_purpose() == "testing" and mode == "full":
        catalogue = await legacy_storage.production_catalogue(paths)
        if catalogue.get("state") == "legacy_catalogue_without_verified_registry":
            for target in targets:
                owned = [Path(tree.root) for tree in target.trees]
                if not owned:
                    continue
                try:
                    ownership = legacy_storage.ownership(catalogue, owned)
                except OSError as error:
                    target.blockers.append(
                        f"Could not compare a legacy ownership reference: {error}"
                    )
                    continue
                for key in ("known_reference_ids", "plausible_overlap_ids"):
                    for identity in ownership[key]:
                        target.blockers.append(
                            f"Legacy production dataset {identity} may overlap the reviewed run artifacts."
                        )
    local_dependencies(paths, targets)
    for target in targets:
        target.blockers = sorted(set(target.blockers))
    return Plan(
        plan_id=prior.plan_id if prior else UUID(new_plan_id()),
        environment=cast(Literal["production", "testing"], database_purpose()),
        registry_id=registry,
        workspace=str(paths.root),
        storage=str(paths.data_root),
        mode=mode,
        requested=requested,
        with_children=children,
        targets=targets,
        examined_other_registry=other_registry,
        created_at=prior.created_at if prior else datetime.now(UTC).isoformat(),
    )


def plan_body(plan: Plan) -> dict[str, Any]:
    return plan.model_dump(mode="json")


def estimated(plan: Plan) -> dict[str, int]:
    return {
        "selected_runs": len(plan.requested),
        "additional_descendants": len(plan.targets) - len(plan.requested),
        "attempts": sum(target.attempt_count for target in plan.targets),
        "removable_files": sum(len(tree.files) for target in plan.targets for tree in target.trees),
        "removable_bytes": sum(target.bytes for target in plan.targets),
    }


async def preview(
    paths: ProjectPaths,
    references: list[str],
    *,
    children: bool = False,
    history_only: bool = False,
) -> dict[str, Any]:
    if not references or len(references) > MAX_RUNS:
        raise MarketForgeError("Select 1–1,000 explicit run references.")
    requested = sorted({safe_reference(value)[0] for value in references})
    mode: Literal["full", "history_only"] = "history_only" if history_only else "full"
    async with connection_for(paths) as (connection, registry):
        try:
            with WorkspaceOwnership(paths):
                async with connection.transaction():
                    await connection.execute(
                        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                    )
                    plan = await review(
                        paths, connection, registry, requested, children=children, mode=mode
                    )
        except OwnershipError as error:
            plan = Plan(
                plan_id=UUID(new_plan_id()),
                environment=cast(Literal["production", "testing"], database_purpose()),
                registry_id=registry,
                workspace=str(paths.root),
                storage=str(paths.data_root),
                mode=mode,
                requested=requested,
                with_children=children,
                targets=[Target(run_id=name, blockers=[str(error)]) for name in requested],
                created_at=datetime.now(UTC).isoformat(),
            )
    body = plan_body(plan)
    path = cleanup_directory(paths) / f"delete-{plan.plan_id.hex}.json"
    document = {**body, "plan_digest": digest(body)}
    from forge.storage.cleanup_plans import canonical

    if len(canonical(document).encode()) >= MAX_PLAN:
        raise MarketForgeError(
            "Run deletion plan exceeds the inspection bound. Review a smaller selection."
        )
    write_document(path, document)
    return {
        **document,
        "action": "preview",
        "status": "blocked" if plan.blockers else "preview",
        "plan_path": str(path),
        "estimated": estimated(plan),
    }


def load(paths: ProjectPaths, source: Path) -> tuple[Plan, str]:
    source = source.expanduser().absolute()
    if (
        source.parent != cleanup_directory(paths)
        or source.is_symlink()
        or not source.is_file()
        or not source.name.startswith("delete-")
    ):
        raise MarketForgeError("Use the exact managed run deletion plan printed by preview.")
    try:
        with source.open("rb") as file:
            raw = file.read(MAX_PLAN + 1)
        if len(raw) > MAX_PLAN:
            raise ValueError("plan exceeds inspection bound")
        body = json.loads(raw)
        reviewed_digest = body.pop("plan_digest")
        plan = Plan.model_validate(body)
    except (OSError, ValueError, TypeError, KeyError, AttributeError, ValidationError) as error:
        raise MarketForgeError("Run deletion plan is missing, invalid or unsafe.") from error
    if (
        source.name != f"delete-{plan.plan_id.hex}.json"
        or digest(plan_body(plan)) != reviewed_digest
        or (plan.workspace, plan.storage, plan.environment)
        != (str(paths.root), str(paths.data_root), database_purpose())
    ):
        raise MarketForgeError(
            "Run deletion plan differs from the reviewed workspace, content or environment."
        )
    return plan, reviewed_digest


async def locked(connection: AsyncConnection[TupleRow]) -> None:
    await connection.execute("SET LOCAL lock_timeout = '5s'")
    await connection.execute("""
        LOCK TABLE historical.execution_runs, historical.execution_attempts,
                   historical.execution_resources, historical.execution_run_deletions,
                   historical.resolved_work, historical.dataset_publications, historical.datasets
        IN SHARE ROW EXCLUSIVE MODE
    """)


async def journal_rows(connection: AsyncConnection[TupleRow], plan: Plan) -> list[dict[str, Any]]:
    return await gather(
        connection,
        """
        SELECT run_id,plan_id,plan_digest,state FROM historical.execution_run_deletions
        WHERE database_id=%s AND run_id=ANY(%s) ORDER BY run_id
    """,
        (plan.registry_id, [item.run_id for item in plan.targets]),
    )


def blockers(plan: Plan) -> list[dict[str, str]]:
    return [
        {"run_id": item.run_id, "reason": reason}
        for item in plan.targets
        for reason in item.blockers
    ]


def workspace_roots(plan: Plan) -> list[Path]:
    return sorted({Path(root) for item in plan.targets for root in item.roots})


def check_current_trees(plan: Plan, *, resuming: bool) -> list[dict[str, str]]:
    failures: list[dict[str, str]] = []
    if plan.mode != "full":
        return failures
    for target in plan.targets:
        reviewed = {tree.root: tree for tree in target.trees}
        for root in target.roots:
            try:
                current = inventory(Path(root), check_names=False)
                expected = reviewed.get(root)
                if current == expected:
                    continue
                if (
                    resuming
                    and expected is not None
                    and (
                        current is None
                        or (
                            all(
                                expected.files.get(name) == entry
                                for name, entry in current.files.items()
                            )
                            and all(
                                expected.directories.get(name) == entry
                                for name, entry in current.directories.items()
                            )
                        )
                    )
                ):
                    continue
                raise FileChanged(f"Artifact inventory differs from the preview: {root}")
            except (MarketForgeError, OSError) as error:
                failures.append({"run_id": target.run_id, "reason": str(error)})
    return failures


def remove_directory(root: Path, name: str, expected: tuple[int, int]) -> None:
    relative = root.name + (f"/{name}" if name else "")
    with parent_directory(root.parent, relative) as (parent, base):
        info = os.stat(base, dir_fd=parent, follow_symlinks=False)
        if (info.st_dev, info.st_ino) != expected:
            raise FileChanged(f"Reviewed directory changed: {root / name}")
        os.rmdir(base, dir_fd=parent)
        os.fsync(parent)


def clean(target: Target, result: dict[str, Any], receipt: Path) -> None:
    for tree in target.trees:
        root = Path(tree.root)
        current = inventory(root, check_names=False)
        available = current.files if current else {}
        directories = current.directories if current else {}
        if any(tree.files.get(name) != item for name, item in available.items()) or any(
            tree.directories.get(name) != item for name, item in directories.items()
        ):
            raise FileChanged(f"Reviewed tree changed during removal: {root}")
        for name, evidence in tree.files.items():
            exact = str(root / name)
            if name not in available:
                if exact not in result["removed_files"] and exact not in result["missing_on_retry"]:
                    result["missing_on_retry"].append(exact)
                    save_receipt(receipt, result)
                continue
            unlink_reviewed(root, name, evidence)
            result["removed_files"].append(exact)
            result["files_removed"] += 1
            result["removed_file_bytes"] += evidence.size
            save_receipt(receipt, result)
        for name in sorted(
            directories, key=lambda path: (path.count("/"), len(path)), reverse=True
        ):
            remove_directory(root, name, tree.directories[name])
            result["directories_removed"] += 1
            save_receipt(receipt, result)


def save_receipt(receipt: Path, result: dict[str, Any]) -> None:
    write_document(receipt, result, replace=receipt.exists())


def result_for(plan: Plan, receipt: Path, plan_path: Path, fingerprint: str) -> dict[str, Any]:
    if receipt.is_file() and not receipt.is_symlink():
        try:
            with receipt.open("rb") as stream:
                prior = json.loads(stream.read(MAX_PLAN + 1))
            if (
                prior.get("plan_digest") == fingerprint
                and prior.get("registry_id") == plan.registry_id
            ):
                return prior
        except (OSError, ValueError, AttributeError):
            pass
        raise MarketForgeError("Existing run deletion receipt could not be matched to this plan.")
    return {
        "schema_version": 1,
        "action": "apply",
        "status": "in_progress",
        "mode": plan.mode,
        "environment": plan.environment,
        "registry_id": plan.registry_id,
        "plan_id": str(plan.plan_id),
        "plan_digest": fingerprint,
        "plan_path": str(plan_path),
        "receipt_path": str(receipt),
        "blockers": [],
        "runs_hidden": 0,
        "run_records_removed": 0,
        "attempt_records_removed": 0,
        "resource_records_removed": 0,
        "files_removed": 0,
        "directories_removed": 0,
        "removed_file_bytes": 0,
        "storage_reclaimed_bytes": 0 if plan.mode == "history_only" else None,
        "removed_files": [],
        "missing_on_retry": [],
        "requested": plan.requested,
        "expanded_runs": [target.run_id for target in plan.targets],
        "retained_artifacts": sorted({path for item in plan.targets for path in item.retained}),
    }


def install_locks(paths: ProjectPaths, plan: Plan, locks: ExitStack) -> None:
    locks.enter_context(WorkspaceOwnership(paths))
    for storage in {root.parent.parent for root in workspace_roots(plan)} - {paths.data_root}:
        if (
            plan.environment != "testing"
            or not contained(storage, paths.data_root)
            or storage.parent != paths.data_root / "tests/historical/normalize"
            or not storage.name.startswith("pipeline-")
        ):
            raise MarketForgeError(f"Unsupported historical workspace in reviewed plan: {storage}")
        locks.enter_context(acquire_file_lock(storage / ".marketforge.lock"))


async def validate_plan(
    connection: AsyncConnection[TupleRow],
    paths: ProjectPaths,
    plan: Plan,
    *,
    resuming: bool,
) -> list[dict[str, str]]:
    fresh = await review(
        paths,
        connection,
        plan.registry_id,
        plan.requested,
        children=plan.with_children,
        mode=plan.mode,
        prior=plan if resuming else None,
    )
    failures = blockers(fresh)
    if fresh.examined_other_registry != plan.examined_other_registry:
        failures.append({"run_id": "", "reason": "Other registry identity changed since preview."})
    if [item.run_id for item in fresh.targets] != [item.run_id for item in plan.targets]:
        failures.append({"run_id": "", "reason": "Expanded run selection changed since preview."})
    by_id = {item.run_id: item for item in plan.targets}
    for current in fresh.targets:
        old = by_id.get(current.run_id)
        if old is None:
            continue
        if (
            current.fingerprint != old.fingerprint
            or current.roots != old.roots
            or current.retained != old.retained
            or current.execution_owners != old.execution_owners
            or (not resuming and current.trees != old.trees)
        ):
            failures.append(
                {
                    "run_id": current.run_id,
                    "reason": "Reviewed run, attempts, relationships or artifact ownership changed.",
                }
            )
    return [*failures, *check_current_trees(plan, resuming=resuming)]


async def apply(paths: ProjectPaths, source: Path) -> dict[str, Any]:
    plan, fingerprint = load(paths, source)
    receipt = cleanup_directory(paths) / f"receipt-{plan.plan_id.hex}.json"
    plan_path = cleanup_directory(paths) / f"delete-{plan.plan_id.hex}.json"
    result: dict[str, Any]

    def finish(state: str, failures: list[dict[str, str]]) -> dict[str, Any]:
        result.update(status=state, blockers=failures)
        save_receipt(receipt, result)
        return result

    targets = [item.run_id for item in plan.targets]
    with ExitStack() as locks:
        try:
            install_locks(paths, plan, locks)
        except (OSError, MarketForgeError) as error:
            # Another process may be writing this receipt under the workspace lock.
            return {
                **result_for(plan, receipt, plan_path, fingerprint),
                "status": "blocked",
                "blockers": [{"run_id": "", "reason": str(error)}],
            }
        result = result_for(plan, receipt, plan_path, fingerprint)
        if plan.blockers:
            return finish("blocked", blockers(plan))
        async with connection_for(paths) as (connection, registry):
            if registry != plan.registry_id:
                raise MarketForgeError("Run deletion plan belongs to a different registry.")
            async with connection.transaction():
                await locked(connection)
                entries = await journal_rows(connection, plan)
                if entries and (
                    len(entries) != len(targets)
                    or any(
                        entry["plan_id"] != plan.plan_id or entry["plan_digest"] != fingerprint
                        for entry in entries
                    )
                ):
                    return finish(
                        "blocked",
                        [{"run_id": "", "reason": "Selection overlaps another committed removal."}],
                    )
                completed = bool(entries) and all(
                    entry["state"] in {"hidden", "removed"} for entry in entries
                )
                if not completed:
                    failures = await validate_plan(connection, paths, plan, resuming=bool(entries))
                    if failures:
                        return finish("partial" if entries else "blocked", failures)
                if not entries:
                    result.update(status="in_progress", blockers=[])
                    save_receipt(receipt, result)
                    state = "hidden" if plan.mode == "history_only" else "retiring"
                    async with connection.cursor() as cursor:
                        await cursor.executemany(
                            """
                            INSERT INTO historical.execution_run_deletions
                                (run_id,database_id,database_purpose,mode,state,plan_id,plan_digest,
                                 operation,original_status,parent_run_id,receipt_path,artifact_roots)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                            [
                                (
                                    item.run_id,
                                    registry,
                                    plan.environment,
                                    plan.mode,
                                    state,
                                    plan.plan_id,
                                    fingerprint,
                                    item.operation,
                                    item.status,
                                    item.parent,
                                    str(receipt),
                                    item.roots,
                                )
                                for item in plan.targets
                            ],
                        )
            result["runs_hidden"] = len(targets)
            try:
                mirror(paths, plan.plan_id.hex, workspace_roots(plan))
                if plan.mode == "history_only":
                    return finish("already_deleted" if completed else "completed", [])
                if not completed:
                    async with connection.transaction():
                        await locked(connection)
                        failures = await validate_plan(connection, paths, plan, resuming=True)
                        if failures:
                            return finish("partial", failures)
                        for item in plan.targets:
                            try:
                                clean(item, result, receipt)
                            except (OSError, MarketForgeError) as error:
                                return finish(
                                    "partial", [{"run_id": item.run_id, "reason": str(error)}]
                                )
                        await connection.execute(
                            """
                            DELETE FROM historical.execution_resources WHERE attempt_id IN (
                                SELECT attempt_id FROM historical.execution_attempts WHERE run_id=ANY(%s)
                            )
                        """,
                            (targets,),
                        )
                        await connection.execute(
                            "DELETE FROM historical.execution_attempts WHERE run_id=ANY(%s)",
                            (targets,),
                        )
                        await connection.execute(
                            "DELETE FROM historical.execution_runs WHERE run_id=ANY(%s)", (targets,)
                        )
                        await connection.execute(
                            """
                            UPDATE historical.execution_run_deletions SET state='removed'
                            WHERE database_id=%s AND plan_id=%s
                        """,
                            (registry, plan.plan_id),
                        )
            except (OSError, MarketForgeError) as error:
                return finish("partial", [{"run_id": "", "reason": str(error)}])
            result.update(
                run_records_removed=len(targets),
                attempt_records_removed=sum(item.attempt_count for item in plan.targets),
                resource_records_removed=sum(item.resource_count for item in plan.targets),
            )
            return finish("already_deleted" if completed else "completed", [])
