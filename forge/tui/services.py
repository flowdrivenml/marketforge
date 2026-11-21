"""Explicit resource and maintenance requests for the Forge application."""

from dataclasses import asdict
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from pydantic import TypeAdapter

from forge.catalog import models as catalog_models
from forge.catalog import service as catalog
from forge.datasets.query import DatasetRequest, browse, timestamp
from forge.historical.archives.storage.files import disk
from forge.historical.drafts import write_new
from forge.historical.engine import managed
from forge.historical.engine.process import ProcessControl, controlled_thread
from forge.historical.metadata.service import enrich_testing
from forge.markets.models import Exchange, Market
from forge.markets.ranking import deletion, policy
from forge.markets.ranking import service as ranking
from forge.markets.ranking.coordination import ranking_lock
from forge.markets.ranking.workflow import refresh_and_publish
from forge.markets.repository import browse as market_browse
from forge.markets.service import collect
from forge.orchestration import inspection
from forge.shared.config.library import resolve_config_file
from forge.shared.database import backup, migrations, status
from forge.shared.database.testing import testing_connection, testing_dsn
from forge.shared.errors import MarketForgeError
from forge.startup.context import ApplicationContext
from forge.startup.diagnostics import doctor
from forge.storage import browsing, cleanup, cleanup_plans
from forge.storage.models import StorageFilters
from forge.storage.service import maintain
from forge.tui.actions import Values
from forge.tui.workflows import Edit, Progress, file, value, workflow


def words(v: Values, key: str) -> list[str]:
    return [entry.strip() for entry in value(v, key).split(",") if entry.strip()]


def optional(v: Values, key: str) -> str | None:
    return value(v, key) or None


def archive_identity(v: Values) -> int | None:
    ref = optional(v, "archive")
    if ref is None:
        return None
    identity = int(ref.removeprefix("local:"))
    if not 0 < identity < 2**63:
        raise MarketForgeError("Local archive ID must be a positive PostgreSQL bigint.")
    return identity


async def datasets(action: str, v: Values, context: ApplicationContext) -> object:
    kwargs: dict[str, Any] = {
        key: item
        for key, item in v.items()
        if item != "" and key in DatasetRequest.__dataclass_fields__
    }
    for name in ("start", "end"):
        if bound := optional(v, name):
            kwargs[name] = timestamp(bound)
    if v.get("column"):
        kwargs["column"] = words(v, "column")
    return await browse(context.paths, DatasetRequest(action, **kwargs))


async def storage(action: str, v: Values, context: ApplicationContext) -> object:
    paths = context.paths
    dsn = testing_dsn(paths.root)
    archive = archive_identity(v)
    selected = StorageFilters.model_validate({k: val for k, val in v.items() if val != ""})
    async with testing_connection(dsn) as connection:
        if action in {"verify", "reconcile"}:
            return await maintain(
                connection,
                root=paths.raw_dir,
                reconcile=action == "reconcile",
                limit=selected.limit,
                archive_id=archive,
                status=selected.status,
                origin=selected.origin,
            )
        async with connection.transaction():
            await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            if action == "coverage":
                return await browsing.coverage(
                    connection, paths.raw_dir, selected, include_gaps=bool(v.get("gaps"))
                )
            result = await browsing.list_archives(
                connection,
                paths.raw_dir,
                StorageFilters(limit=1) if action == "show" else selected,
                archive_id=archive,
            )
            if action == "show" and not result["archives"]:
                raise MarketForgeError("Local archive is unavailable in this raw root.")
            return result


async def review_cleanup(
    action: Literal["prune", "delete"], v: Values, context: ApplicationContext
) -> dict[str, object]:
    paths = context.paths
    selected = StorageFilters.model_validate({k: val for k, val in v.items() if val != ""})
    if action == "prune" and selected.status is None:
        selected = selected.model_copy(update={"status": "failed"})
    identity = archive_identity(v)
    overlap = bool(v.get("overlap"))
    cleanup.validate_selection(action, selected, identity, overlap)
    async with testing_connection(testing_dsn(paths.root)) as connection, connection.transaction():
        await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        plan = await cleanup.preview(
            connection,
            paths,
            selected,
            action,
            archive_id=identity,
            overlap=overlap,
            review_all=True,
        )
    counts = cleanup.totals(plan.targets)
    source = await disk(cleanup_plans.save_plan, paths, plan) if counts["eligible"] else None
    return {
        "status": "ready" if source else "no_eligible_targets",
        "preview_path": str(source) if source else None,
        "preview_digest": plan.digest,
        "review_complete": plan.review_complete,
        "totals": counts,
        "targets": [target.model_dump(mode="json") for target in plan.targets],
    }


async def apply_cleanup(
    action: Literal["prune", "delete"], reviewed: dict[str, object], context: ApplicationContext
) -> object:
    source, plan = await disk(
        cleanup_plans.load_plan, context.paths, Path(str(reviewed["preview_path"])), action
    )
    if plan.digest != reviewed["preview_digest"]:
        raise MarketForgeError("Saved cleanup preview changed; review again.")
    async with testing_connection(testing_dsn(context.paths.root)) as connection:
        return await cleanup.apply(connection, context.paths, source, plan)


async def markets(
    action: str, v: Values, context: ApplicationContext, progress: Progress
) -> object:
    dsn = testing_dsn(context.paths.root)
    exchanges, categories = words(v, "exchanges"), words(v, "markets")
    instrument = optional(v, "instrument")
    if action in {"sync", "refresh"}:
        return await collect(
            dsn,
            context.settings.http,
            metadata=action == "sync",
            exchanges=TypeAdapter(list[Exchange]).validate_python(exchanges) or None,
            markets=TypeAdapter(list[Market]).validate_python(categories) or None,
            symbol=instrument,
            progress=progress,
        )
    limit = int(v.get("limit", 50))
    async with testing_connection(dsn) as connection:
        rows = await market_browse(
            connection,
            exchanges=exchanges or None,
            markets=categories or None,
            symbol=instrument,
            limit=2 if action == "show" else limit + 1,
            offset=int(v.get("offset", 0)),
            sort=optional(v, "sort") or "identity",
            reference=optional(v, "reference"),
            base_asset=optional(v, "base_asset"),
            quote_asset=optional(v, "quote_asset"),
            match=optional(v, "match"),
        )
    if action == "show" and not rows:
        raise MarketForgeError("Instrument reference was not found.")
    return {
        "instruments": rows[:limit],
        "has_more": len(rows) > limit,
        "semantics": "Stored observations. Missing values are unknown; turnover retains native units.",
    }


async def ranks(
    action: str,
    v: Values,
    context: ApplicationContext,
    progress: Progress,
    reviewed: tuple[frozenset[str], frozenset[str]] | None = None,
) -> object:
    paths = context.paths
    directory = paths.config_dir / "markets/rankings"
    reference = optional(v, "reference")
    if action in {"new", "validate", "build"}:
        path = resolve_config_file(directory, value(v, "reference"))
        if action == "new":
            return {"status": "created", "path": str(await disk(write_new, path, policy.TEMPLATE))}
        config = await disk(policy.load_policy, path)
        if action == "validate":
            return {"valid": True, "policy": policy.policy_document(config)}
        async with (
            testing_connection(testing_dsn(paths.root)) as connection,
            ranking_lock(connection),
        ):
            return {
                "status": "published",
                "snapshot_id": str(await ranking.build(connection, path.stem, config)),
            }
    dsn = testing_dsn(paths.root)
    async with testing_connection(dsn) as connection:
        if action == "delete":
            return await deletion.delete_snapshots(
                connection,
                paths,
                reference,
                all_snapshots=bool(v.get("all")),
                apply=reviewed is not None,
                expected=reviewed[0] if reviewed else None,
                expected_eligible=reviewed[1] if reviewed else None,
            )
        if action == "update":
            async with ranking_lock(connection):
                return await refresh_and_publish(
                    dsn,
                    context.settings.http,
                    directory,
                    (reference,) if reference else ("turnover", "volume"),
                    progress=progress,
                )
        async with ranking_lock(connection, shared=True):
            limit = int(v.get("limit", 50))
            if action == "list":
                rows = await ranking.list_snapshots(
                    connection, limit + 1, offset=int(v.get("offset", 0)), search=value(v, "search")
                )
                return {"rankings": rows[:limit], "has_more": len(rows) > limit}
            try:
                identity = UUID(str(reference))
            except ValueError:
                identity = await ranking.latest_snapshot(
                    connection, resolve_config_file(directory, str(reference)).stem
                )
            result = await ranking.show(
                connection,
                identity,
                instrument_type=optional(v, "instrument_type"),
                base_asset=optional(v, "base_asset"),
                quote_asset=optional(v, "quote_asset"),
                match=optional(v, "match"),
            )
            from forge.markets.models import array, obj

            entries = [obj(entry) for entry in array(result["entries"])]
            if conversion := optional(v, "conversion"):
                selected = [
                    entry
                    for entry in entries
                    if conversion in {str(entry["position"]), entry["instrument_reference"]}
                ]
                if not selected:
                    raise MarketForgeError("No matching entry in the pinned snapshot.")
                result["selected_conversion"] = selected[0]
            shown = (
                entries
                if v.get("excluded")
                else [entry for entry in entries if entry["position"] is not None]
            )
            result["entries"] = shown if v.get("all") else shown[:limit]
            result["entries_truncated"] = not v.get("all") and len(shown) > limit
            return result


async def catalogue(model: str, action: str, v: Values, context: ApplicationContext) -> object:
    declaration = (
        await disk(catalog_models.load, model, file(v, "source", context))
        if action in {"validate", "add"}
        else None
    )
    if action == "validate":
        assert declaration is not None
        return {"valid": True, "declaration": declaration.model_dump(mode="json")}
    async with testing_connection(testing_dsn(context.paths.root)) as connection:
        return (
            await catalog.add(connection, declaration)
            if declaration
            else await catalog.browse(
                connection,
                model,
                optional(v, "reference"),
                limit=int(v.get("limit", 50)),
                offset=int(v.get("offset", 0)),
                search=value(v, "search"),
            )
        )


async def system(key: str, v: Values, context: ApplicationContext, progress: Progress) -> object:
    paths = context.paths
    reference = value(v, "reference")
    if key == "config show":
        return {
            "workspace": str(paths.root),
            "storage": str(paths.data_root),
            "settings_file": context.settings_file,
            "settings": context.settings.model_dump(mode="json"),
        }
    if key == "engine status":
        return await disk(managed.inventory, paths)
    if key == "engine build":
        builds: list[dict[str, Any]] = []
        building = not v.get("dry_run")
        offline = bool(v.get("offline"))
        jobs = int(v["jobs"]) if v.get("jobs") else None

        async def target_result(target: managed.Target) -> dict[str, Any]:
            if not building:
                return await disk(managed.plan, paths, target, offline=offline, jobs=jobs)

            def build_engine(control: ProcessControl) -> dict[str, Any]:
                return managed.build(paths, target, offline=offline, jobs=jobs, control=control)

            return await controlled_thread(build_engine)

        for target in managed.select(reference or None):
            progress(f"{'Build' if building else 'Plan'} {target.name}")
            builds.append(await target_result(target))
        return {"status": "built" if building else "planned", "targets": builds}
    if key == "database status":
        return asdict(await disk(status.inspect_testing_database, project_root=paths.root))
    if key.startswith("runs "):
        action = key.split()[-1]
        limit = int(v.get("limit", 50))
        if action == "list":
            return {"runs": await disk(inspection.list_runs, paths.runs_dir, limit)}
        if action == "logs":
            return await disk(inspection.read_logs, paths.runs_dir, reference, limit)
        return await disk(inspection.read_report, paths.runs_dir, reference)
    if key == "doctor":
        return await disk(doctor, paths)
    dsn = testing_dsn(paths.root)
    if key == "database migrate":
        return asdict(await disk(migrations.migrate_testing_database, dsn=dsn))
    if key == "database backup":
        return await controlled_thread(
            lambda control: backup.backup(
                dsn, paths, file(v, "reference", context), control=control
            )
        )
    if key == "database restore":
        return await controlled_thread(
            lambda control: backup.restore(
                dsn, file(v, "reference", context), value(v, "destination"), control=control
            )
        )
    if key == "init":
        await disk(paths.ensure_application_directories)
        migration = asdict(await disk(migrations.migrate_testing_database, dsn=dsn))
        progress("Collect catalogue")
        catalogue_result = await collect(
            dsn, context.settings.http, metadata=True, progress=progress
        )
        if catalogue_result.get("status") == "cancelled":
            return {"status": "cancelled", "catalogue": catalogue_result, "migrations": migration}
        progress("Enrich historical metadata")
        enrichment = await enrich_testing(dsn, paths)
        async with testing_connection(dsn) as connection, ranking_lock(connection):
            result = await refresh_and_publish(
                dsn, context.settings.http, paths.config_dir / "markets/rankings", progress=progress
            )
        if any(item.get("status") != "complete" for item in (catalogue_result, enrichment)):
            result["status"] = "partial"
        return {
            **result,
            "catalogue": catalogue_result,
            "migrations": migration,
            "historical_metadata": enrichment,
        }
    raise MarketForgeError(f"Unknown system action {key}.")


async def execute(
    key: str, v: Values, context: ApplicationContext, progress: Progress, edit: Edit
) -> object:
    first, *parts = key.split()
    if first == "datasets":
        return await datasets(parts[0], v, context)
    if first == "storage":
        if parts[0] == "delete":
            return await review_cleanup("delete", v, context)
        if parts[0] == "prune":
            return await review_cleanup("prune", v, context)
        return await storage(parts[0], v, context)
    if first == "markets":
        if parts[0] == "rank":
            return await ranks(parts[1], v, context, progress)
        return await markets(parts[0], v, context, progress)
    if first == "catalog":
        return await catalogue(parts[0], parts[1], v, context)
    if first in {"pipeline", "acquisition", "normalize", "merge", "convert"}:
        return await workflow(key, v, context, progress, edit)
    return await system(key, v, context, progress)
