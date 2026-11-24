"""UI workflow requests composed from the Forge domain services."""

from argparse import Namespace
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from forge.cli.commands.normalization_runtime import inspection
from forge.historical.archives.config import load_acquisition
from forge.historical.archives.operation import run_acquisition
from forge.historical.archives.planning import plan as acquisition_plan
from forge.historical.archives.sources.capabilities import source_capabilities
from forge.historical.archives.storage.files import disk
from forge.historical.archives.templates import write_acquisition_template
from forge.historical.authoring import create_pipeline, publish_new
from forge.historical.conversion import recipes as conversion_recipes
from forge.historical.conversion import service as conversion
from forge.historical.conversion.evidence import inspect_publication_source
from forge.historical.conversion.execution import execute as execute_conversion
from forge.historical.drafts import create_import, create_registration, write_new
from forge.historical.engine.runtime_config import RuntimeConfiguration
from forge.historical.imports import models as imports
from forge.historical.imports.formats import sample
from forge.historical.imports.service import run_import
from forge.historical.merge import service as merge
from forge.historical.merge.config import DerivedRecipe, normalization_source
from forge.historical.normalization import execution as normalize
from forge.historical.normalization.batch import prepare_batch
from forge.historical.normalization.config import ExactRecipe
from forge.historical.normalization.recipes import create_recipe, load_configuration, recipe_path
from forge.historical.pipelines import service as pipeline
from forge.historical.pipelines.config import load_recipe_file
from forge.historical.pipelines.planning import plan as pipeline_plan
from forge.historical.pipelines.terminal import Ghostty, StageWindows, WindowUnavailable
from forge.historical.profiles import service as profiles
from forge.historical.registration import models as registration
from forge.historical.registration import service as raw_registration
from forge.orchestration.registry import begin_operation, checkpoint, complete_call
from forge.shared.config.library import list_config_names, resolve_config_file, run_reference
from forge.shared.config.toml import dumps
from forge.shared.database.connection import database_connection, database_dsn, selected_registry_id
from forge.shared.errors import MarketForgeError
from forge.shared.identity import RunOperation
from forge.shared.presentation.conversion import inspect_recipe
from forge.startup.context import ApplicationContext
from forge.tui.actions import Values

Edit = Callable[[Path], Awaitable[None]]
Progress = Callable[[object], None]
PIPELINE_DESCRIPTORS: ContextVar[tuple[int, ...]] = ContextVar(
    "forge_tui_pipeline_descriptors", default=()
)


def value(values: Values, key: str) -> str:
    return str(values.get(key, ""))


def file(values: Values, key: str, context: ApplicationContext) -> Path:
    return (context.paths.root / Path(value(values, key)).expanduser()).resolve()


async def workflow(
    key: str, v: Values, context: ApplicationContext, progress: Progress, edit: Edit
) -> object:
    family, _, action = key.partition(" ")
    if (
        family in {"normalize", "merge", "convert"}
        and action in {"prepare", "execute", "run", "register"}
        and not v.get("dry_run")
    ):
        reference = value(v, "reference")
        execution = await begin_operation(context.paths, RunOperation(family), action, reference)

        async def call() -> dict[str, Any]:
            progress(f"{action} · managed execution")
            result = await workflow_operation(key, v, context, progress, edit)
            from typing import cast

            return cast(dict[str, Any], result)

        return await complete_call(execution, call)
    return await workflow_operation(key, v, context, progress, edit)


async def workflow_operation(
    key: str, v: Values, context: ApplicationContext, progress: Progress, edit: Edit
) -> object:
    paths = context.paths
    reference = value(v, "reference")
    family, *parts = key.split()
    action = parts[-1]
    dry = bool(v.get("dry_run"))
    engine_tui = bool(v.get("engine_tui"))
    if action == "list" and family in {"pipeline", "normalize", "merge"}:
        directory = {
            "pipeline": paths.pipeline_config_dir,
            "normalize": paths.normalization_recipe_dir,
            "merge": paths.workflow_dir("merge"),
        }[family]
        return {"configurations": await disk(list_config_names, directory)}
    if family == "pipeline":
        if action == "new":
            path = await disk(create_pipeline, paths.pipeline_config_dir, reference)
            return {"status": "draft_created", "path": str(path)}
        if action == "status":
            return await disk(pipeline.status, paths, reference)
        windows = None
        if v.get("stage_terminal") == "window":
            descriptors = PIPELINE_DESCRIPTORS.get()
            if not descriptors:
                raise MarketForgeError("Pipeline windows require owned workspace execution.")
            try:
                launcher = await disk(Ghostty.detect)
            except WindowUnavailable as error:
                progress(f"Stage windows unavailable. Continuing inline. {error}")
            else:
                windows = StageWindows(launcher, descriptors, True, progress)
        if action == "resume":
            run = await disk(pipeline.resume, context, reference)
        else:
            source = resolve_config_file(paths.pipeline_config_dir, reference)
            submitted = await disk(source.read_bytes)
            recipe = load_recipe_file(source, submitted)
            if action == "validate":
                return {"status": "valid", "recipe": recipe.model_dump(mode="json")}
            if action == "plan":
                return await pipeline_plan(paths, recipe, source)
            run = pipeline.new(context, reference, submitted, source=source)
        run.windows = windows
        progress(run.snapshot)
        return await run.run(stop=value(v, "stop_after") == "normalize.prepare")
    if family == "acquisition" and len(parts) == 1:
        directory = paths.acquisition_config_dir
        if action == "list":
            return {"configurations": await disk(list_config_names, directory)}
        if action == "sources":
            return {
                "Availability": "Use acquisition Map for archive discovery.",
                "sources": [asdict(entry) for entry in await disk(source_capabilities)],
            }
        source = resolve_config_file(directory, reference)
        if action == "new":
            await disk(write_acquisition_template, source)
            await edit(source)
            return {"status": "created", "path": str(source)}
        if action == "edit":
            await edit(source)
        config = await disk(load_acquisition, source)
        if action in {"validate", "edit"}:
            return {"status": "valid", "configuration": config.model_dump(mode="json")}
        dsn = database_dsn(paths.root)
        if action == "plan":
            async with database_connection(dsn) as connection:
                return await acquisition_plan(
                    connection,
                    config,
                    paths.raw_dir,
                    verify_local=bool(v.get("verify_local")),
                    limit=int(v.get("limit", 100)),
                )
        return await run_acquisition(
            context,
            config,
            dsn=dsn,
            download=action == "download",
            refresh=bool(v.get("refresh")),
            source_file=source,
            observer=progress,
            requests_per_second=float(v["rate"]) if v.get("rate") else None,
        )
    if key.startswith("acquisition import "):
        source = resolve_config_file(paths.workflow_dir("import"), reference)
        if action == "new":
            return {
                "status": "draft_created",
                "path": str(
                    await disk(create_import, source, reference, file(v, "source", context))
                ),
            }
        config = await disk(imports.load_import, source)
        if action == "run":
            return await run_import(paths, database_dsn(paths.root), config, source)
        if action == "plan":
            return await disk(imports.plan, config)
        return await disk(inspect_import, config, action)
    if key.startswith("acquisition register "):
        source = registration.recipe_path(paths, reference)
        if action == "new":
            await disk(create_registration, source, reference, file(v, "source", context))
            return {"status": "draft_created", "path": str(source)}
        if action in {"plan", "run"}:
            return await raw_registration.apply(paths, source, planning=action == "plan")
        if action == "edit":
            await edit(source)
        recipe = await disk(registration.load, source)
        return (
            await raw_registration.validate(recipe, paths)
            if action == "validate"
            else await disk(raw_registration.inspect, recipe, paths)
        )
    if key.startswith("normalize profiles "):
        if action == "list":
            return {
                "profiles": await disk(
                    lambda: profiles.discover(paths, **{k: str(val) for k, val in v.items() if val})
                )
            }
        if action == "new":
            draft = await disk(profiles.new_draft, paths, reference)
            return {"status": "draft_created", "path": str(draft), "profile_id": reference}
        if action == "sample":
            header = value(v, "header")
            return await disk(
                sample,
                file(v, "file", context),
                value(v, "member") or None,
                header == "present" if header else None,
            )
        if action in {"add", "register"}:
            return await profiles.register(paths, reference, file(v, "source", context))
        if bool(reference) == bool(v.get("file")):
            raise MarketForgeError("Choose one installed profile ID or a profile TOML file.")
        source = file(v, "file", context) if v.get("file") else profiles.installed(paths, reference)
        return (
            await disk(profiles.validate, paths, source)
            if action == "validate"
            else await disk(profiles.inspect, source)
        )
    if family == "normalize":
        if action == "register":
            source = file(v, "reference", context)
            return await (
                normalize.recover(paths, source)
                if source.name == "execution-intent.json"
                else raw_normalized(paths, source)
            )
        source = recipe_path(paths, reference)
        from_acquisition = value(v, "from_acquisition")
        if action == "new" or (action == "prepare" and from_acquisition):
            source = await disk(create_recipe, paths, reference, from_acquisition or None)
            await edit(source)
        if action == "run":
            source, submitted = await disk(run_reference, reference, source)
            if submitted:
                return await normalize.execute_async(
                    paths,
                    source,
                    tui=engine_tui,
                    allow_partial=bool(v.get("allow_partial")),
                    lifecycle=progress,
                )
        recipe = await disk(load_configuration, source)
        if action in {"new", "validate", "show"}:
            if isinstance(recipe, ExactRecipe) and action == "validate":
                review = await prepare_batch(paths, recipe, dry_run=True, recipe_source=source)
                if review["status"] == "prepared":
                    review["status"] = "valid"
                return review
            return {
                "status": "valid",
                "path": str(source),
                "recipe": recipe.model_dump(mode="json"),
            }
        result = await prepare_batch(
            paths,
            recipe,
            recipe_source=source,
            dry_run=dry,
            http=context.settings.http,
            supervised_validation=True,
            lifecycle=progress,
        )
        if action in {"run", "execute"} and not dry and result.get("status") == "prepared":
            checkpoint(result)
            return await normalize.execute_async(
                paths,
                Path(str(result["batch_path"])),
                tui=engine_tui,
                allow_partial=bool(v.get("allow_partial")),
                recipe=reference,
                lifecycle=progress,
            )
        return result
    if family == "merge":
        if action == "register":
            return await merge.register(paths, file(v, "reference", context), lifecycle=progress)
        directory = paths.workflow_dir("merge")
        if action == "new":
            if v.get("publication_ids") and not v.get("from_normalize"):
                publication_ids = [int(item) for item in value(v, "publication_ids").split(",")]
                recipe, evidence = await merge.publication_recipe(
                    paths,
                    publication_ids,
                    start_ns=int(bound) if (bound := v.get("start_ns")) not in (None, "") else None,
                    end_ns=int(bound) if (bound := v.get("end_ns")) not in (None, "") else None,
                    timeline_ordering=value(v, "timeline_ordering") or None,
                    stream_ranks=stream_ranks(v),
                )
                path = resolve_config_file(directory, reference)
                recipe_data = recipe.model_dump(mode="json", exclude_none=True)
                if v.get("preview_only"):
                    return {**evidence, "recipe": recipe_data, "path": str(path)}
                if v.get("selection_sha256") != recipe.selection_sha256:
                    raise MarketForgeError(
                        "Reviewed publication selection changed. Review it again."
                    )
                await disk(publish_new, path, dumps(recipe_data))
                return {**evidence, "status": "recipe_saved", "path": str(path)}
            return await disk(new_merge, context, reference, value(v, "from_normalize"), v)
        source, submitted = await disk(resolve_submission, directory, reference)
        if submitted:
            if action != "run":
                raise MarketForgeError("Choose a merge recipe TOML for this action.")
            return await merge.execute_async(paths, source, tui=engine_tui, lifecycle=progress)
        recipe = await disk(merge.load_recipe, source)
        if action == "show":
            return {"path": str(source), "recipe": recipe.model_dump(mode="json")}
        if action == "validate":
            await disk(merge.validate_recipe, paths, recipe)
            if isinstance(recipe, merge.Recipe) and recipe.registry_id is not None:
                registry = await selected_registry_id(paths.root)
                await merge.reviewed_inputs(paths, recipe, recipe.dataset_ids, registry)
            return {
                "status": "valid",
                "path": str(source),
                "recipe": recipe.model_dump(mode="json"),
            }
        if dry:
            return await disk(merge.prepare, paths, recipe, reference, dry_run=True)
        progress("prepare")
        result = await merge.prepare_async(paths, recipe, reference, lifecycle=progress)
        if action == "run" and result["status"] == "prepared":
            checkpoint(result)
        return (
            await merge.execute_async(
                paths, Path(result["job_path"]), tui=engine_tui, lifecycle=progress
            )
            if action == "run" and result["status"] == "prepared"
            else result
        )
    if family == "convert":
        if parts[0] == "config":
            service = RuntimeConfiguration(paths, "convert")
            args = Namespace(runtime_action=action, field=v.get("field") or None, all=False)
            return {
                "configuration_path": str(service.path),
                **await disk(inspection, service, args),
            }
        if action == "list":
            directory = conversion_recipes.library(paths)
            return {
                "schema_version": 1,
                "recipes": await disk(
                    lambda: [
                        inspect_recipe(paths, resolve_config_file(directory, name)).document()
                        for name in list_config_names(directory)
                    ]
                ),
            }
        if action == "new":
            route = value(v, "route")
            if route == "publication":
                from pydantic import TypeAdapter

                from forge.historical.merge.config import DatabaseID

                reference_id = value(v, "source")
                if not reference_id.isascii() or not reference_id.isdecimal():
                    raise MarketForgeError(
                        "Use the decimal registry publication ID for the merged source."
                    )
                source_id = TypeAdapter(DatabaseID).validate_python(int(reference_id))
                binding = await inspect_publication_source(paths, source_id)
                return await disk(
                    conversion_recipes.create_bound,
                    paths,
                    reference,
                    binding,
                    binding.interval,
                    value(v, "quote"),
                )
            if route not in {"merge", "normalize", "acquisition"}:
                raise MarketForgeError("Choose a conversion source route.")
            from typing import cast

            return await disk(
                conversion_recipes.create,
                paths,
                reference,
                cast(conversion_recipes.SourceKind, route),
                value(v, "source"),
                value(v, "quote"),
            )
        source, submitted = await disk(
            resolve_submission, conversion_recipes.library(paths), reference
        )
        if action == "show":
            if submitted:
                raise MarketForgeError("Choose a conversion recipe TOML for inspection.")
            return await disk(lambda: inspect_recipe(paths, source).document())
        if submitted:
            if action == "run":
                return await execute_conversion(
                    paths, source, tui=engine_tui, embedded=not engine_tui, lifecycle=progress
                )
            if action == "validate":
                return await disk(conversion.validate_reference, paths, source)
            raise MarketForgeError("Choose a conversion recipe TOML for preparation.")
        if action == "validate":
            recipe, upstream, _ = await disk(
                lambda: conversion_recipes.load_recipe(paths, source, retain=False)
            )
            if isinstance(upstream, conversion_recipes.PublicationSource):
                review = await conversion.prepare(paths, source, dry_run=True, embedded=True)
                if review["status"] == "generated":
                    review["status"] = "valid"
                return review
            return {
                "status": "valid",
                "path": str(source),
                "recipe": recipe.model_dump(mode="json"),
            }
        progress("prepare")
        result = await conversion.prepare(
            paths, source, dry_run=dry, embedded=True, lifecycle=progress
        )
        if action == "run" and result["status"] == "generated":
            checkpoint(result)
        return (
            await execute_conversion(
                paths,
                Path(result["job_path"]),
                tui=engine_tui,
                embedded=not engine_tui,
                lifecycle=progress,
            )
            if action == "run" and result["status"] == "generated"
            else result
        )
    raise MarketForgeError(f"Unknown workflow {key}.")


async def raw_normalized(paths: object, source: Path) -> object:
    from forge.historical.normalization.datasets import register
    from forge.shared.paths import ProjectPaths

    assert isinstance(paths, ProjectPaths)
    return await register(paths, source)


def resolve_submission(directory: Path, reference: str) -> tuple[Path, bool]:
    managed = resolve_config_file(directory, reference) if "/" not in reference else None
    return run_reference(reference, managed)


def inspect_import(config: imports.ImportConfig, action: str) -> object:
    inputs = imports.freeze_inputs(config)
    if action == "validate":
        return {"status": "valid", "input_count": len(inputs)}
    return {
        "inputs": [
            {"reference": i.reference, "bytes": i.size, "sample": sample(Path(i.path))}
            for i in inputs[:20]
        ],
        "total_inputs": len(inputs),
    }


def new_merge(
    context: ApplicationContext, name: str, upstream: str, values: Values | None = None
) -> object:
    paths = context.paths
    path = resolve_config_file(paths.workflow_dir("merge"), name)
    document = merge.TEMPLATE
    evidence: dict[str, Any] = {}
    if upstream:
        _, config, fingerprint = normalization_source(paths, upstream)
        fields = values or {}
        interval = config.interval.model_dump()
        for bound in ("start", "end"):
            if fields.get(bound):
                interval[bound] = datetime.fromisoformat(str(fields[bound]))
        recipe = DerivedRecipe.model_validate(
            {
                "version": 1,
                "interval": interval,
                "selection": {
                    "source": upstream,
                    "publication_ids": [
                        int(item) for item in value(fields, "publication_ids").split(",") if item
                    ]
                    or None,
                },
                "timeline_ordering": fields.get("timeline_ordering") or None,
                "stream_ranks": stream_ranks(fields),
            }
        )
        evidence = merge.source_selection(paths, recipe)
        if evidence["source"]["sha256"] != fingerprint:
            raise MarketForgeError("Normalization configuration changed during recipe creation.")
        document = dumps(recipe.model_dump(exclude_none=True))
    return {**evidence, "status": "template_created", "path": str(write_new(path, document))}


def stream_ranks(values: Values) -> dict[str, int]:
    return {
        stream.strip(): int(rank)
        for entry in value(values, "stream_ranks").split(",")
        if entry.strip()
        for stream, rank in [entry.rsplit("=", 1)]
    }
