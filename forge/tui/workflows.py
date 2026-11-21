"""UI workflow requests composed from the Forge domain services."""

import tomllib
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from forge.historical.archives.config import load_acquisition
from forge.historical.archives.operation import run_acquisition
from forge.historical.archives.planning import plan as acquisition_plan
from forge.historical.archives.storage.files import disk
from forge.historical.archives.templates import write_acquisition_template
from forge.historical.authoring import create_pipeline
from forge.historical.conversion import recipes as conversion_recipes
from forge.historical.conversion import service as conversion
from forge.historical.conversion.execution import execute as execute_conversion
from forge.historical.drafts import create_import, create_registration, write_new
from forge.historical.engine.managed import resolve
from forge.historical.imports import models as imports
from forge.historical.imports.formats import sample
from forge.historical.imports.service import run_import
from forge.historical.merge import service as merge
from forge.historical.merge.config import DerivedRecipe, Selection, normalization_source
from forge.historical.normalization import execution as normalize
from forge.historical.normalization.batch import prepare_batch
from forge.historical.normalization.recipes import create_recipe, load_configuration, recipe_path
from forge.historical.pipelines import service as pipeline
from forge.historical.pipelines.config import load_recipe
from forge.historical.pipelines.planning import plan as pipeline_plan
from forge.historical.profiles import service as profiles
from forge.historical.registration import models as registration
from forge.historical.registration import service as raw_registration
from forge.shared.config.library import list_config_names, resolve_config_file, run_reference
from forge.shared.config.toml import dumps
from forge.shared.database.testing import testing_connection, testing_dsn
from forge.shared.errors import MarketForgeError
from forge.startup.context import ApplicationContext
from forge.tui.actions import Values

Edit = Callable[[Path], Awaitable[None]]
Progress = Callable[[object], None]


def value(values: Values, key: str) -> str:
    return str(values.get(key, ""))


def file(values: Values, key: str, context: ApplicationContext) -> Path:
    return (context.paths.root / Path(value(values, key)).expanduser()).resolve()


async def workflow(
    key: str, v: Values, context: ApplicationContext, progress: Progress, edit: Edit
) -> object:
    paths = context.paths
    reference = value(v, "reference")
    family, *parts = key.split()
    action = parts[-1]
    dry = bool(v.get("dry_run"))
    if family == "pipeline":
        if action == "new":
            path = await disk(create_pipeline, paths.pipeline_config_dir, reference)
            return {"status": "draft_created", "path": str(path)}
        if action == "status":
            return await disk(pipeline.status, paths, reference)
        if action == "resume":
            run = await disk(pipeline.resume, context, reference)
        else:
            source = resolve_config_file(paths.pipeline_config_dir, reference)
            submitted = await disk(source.read_bytes)
            recipe = load_recipe(submitted)
            if action == "validate":
                return {"status": "valid", "recipe": recipe.model_dump(mode="json")}
            if action == "plan":
                return await pipeline_plan(paths, recipe, source)
            run = pipeline.new(context, reference, submitted)
        progress(run.snapshot)
        return await run.run(stop=value(v, "stop_after") == "normalize.prepare")
    if family == "acquisition" and len(parts) == 1:
        directory = paths.acquisition_config_dir
        if action == "list":
            return {"configurations": await disk(list_config_names, directory)}
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
        dsn = testing_dsn(paths.root)
        if action == "plan":
            async with testing_connection(dsn) as connection:
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
        source = resolve_config_file(paths.acquisition_config_dir / "import", reference)
        if action == "new":
            return {
                "status": "draft_created",
                "path": str(
                    await disk(create_import, source, reference, file(v, "source", context))
                ),
            }
        config = await disk(imports.load_import, source)
        if action == "run":
            return await run_import(paths, testing_dsn(paths.root), config, source)
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
    if key.startswith("acquisition profiles "):
        if action == "list":
            return {
                "profiles": await disk(
                    lambda: profiles.discover(paths, **{k: str(val) for k, val in v.items() if val})
                )
            }
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
        source = recipe_path(paths, reference)
        if action == "register":
            source = file(v, "reference", context)
            return await (
                normalize.recover(paths, source)
                if source.name == "execution-intent.json"
                else raw_normalized(paths, source)
            )
        from_acquisition = value(v, "from_acquisition")
        if action == "new" or (action == "prepare" and from_acquisition):
            source = await disk(create_recipe, paths, reference, from_acquisition or None)
            await edit(source)
        if action == "run":
            source, submitted = await disk(run_reference, reference, source)
            if submitted:
                return await normalize.execute_async(
                    paths, source, allow_partial=bool(v.get("allow_partial")), lifecycle=progress
                )
        recipe = await disk(load_configuration, source)
        if action in {"new", "validate"}:
            return {
                "status": "valid",
                "path": str(source),
                "recipe": recipe.model_dump(mode="json"),
            }
        result = await prepare_batch(
            paths,
            recipe,
            dry_run=dry,
            http=context.settings.http,
            supervised_validation=True,
            lifecycle=progress,
        )
        if action in {"run", "execute"} and not dry and result.get("status") == "prepared":
            return await normalize.execute_async(
                paths,
                Path(str(result["batch_path"])),
                allow_partial=bool(v.get("allow_partial")),
                recipe=reference,
                lifecycle=progress,
            )
        return result
    if family == "merge":
        if action == "register":
            return await merge.register(paths, file(v, "reference", context), lifecycle=progress)
        directory = paths.config_dir / "historical/merge"
        if action == "new":
            return await disk(new_merge, context, reference, value(v, "from_normalize"))
        source, submitted = await disk(resolve_submission, directory, reference)
        if submitted:
            if action != "run":
                raise MarketForgeError("Choose a merge recipe TOML for this action.")
            return await merge.execute_async(paths, source, lifecycle=progress)
        document = tomllib.loads(await disk(source.read_text))
        recipe = (
            DerivedRecipe
            if {"version", "interval", "selection"} & document.keys()
            else merge.Recipe
        ).model_validate(document)
        if action == "validate":
            return {
                "status": "valid",
                "path": str(source),
                "recipe": recipe.model_dump(mode="json"),
            }
        progress("prepare")
        result = await merge.prepare_async(paths, recipe, reference, lifecycle=progress)
        return (
            await merge.execute_async(paths, Path(result["job_path"]), lifecycle=progress)
            if action == "run" and result["status"] == "prepared"
            else result
        )
    if family == "convert":
        if action == "new":
            route = value(v, "route")
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
        if submitted:
            if action == "run":
                return await execute_conversion(paths, source, embedded=True, lifecycle=progress)
            if action == "validate":
                return {
                    "status": "valid",
                    "validation": await disk(
                        lambda: conversion.validate_prepared_job(resolve(paths, "convert"), source)
                    ),
                }
            raise MarketForgeError("Choose a conversion recipe TOML for preparation.")
        if action == "validate":
            recipe, _, _ = await disk(conversion_recipes.load_recipe, paths, source)
            return {
                "status": "valid",
                "path": str(source),
                "recipe": recipe.model_dump(mode="json"),
            }
        progress("prepare")
        result = await conversion.prepare(
            paths, source, dry_run=dry, embedded=True, lifecycle=progress
        )
        return (
            await execute_conversion(
                paths, Path(result["job_path"]), embedded=True, lifecycle=progress
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


def new_merge(context: ApplicationContext, name: str, upstream: str) -> object:
    paths = context.paths
    path = resolve_config_file(paths.config_dir / "historical/merge", name)
    document = merge.TEMPLATE
    evidence: dict[str, Any] = {}
    if upstream:
        _, config, fingerprint = normalization_source(paths, upstream)
        recipe = DerivedRecipe(
            version=1, interval=config.interval, selection=Selection(source=upstream)
        )
        evidence = merge.source_selection(paths, recipe)
        if evidence["source"]["sha256"] != fingerprint:
            raise MarketForgeError("Normalization configuration changed during recipe creation.")
        document = (
            "version = 1\n\n[interval]\n"
            + dumps(recipe.interval.model_dump())
            + "\n[selection]\n"
            + dumps(recipe.selection.model_dump(exclude_none=True))
        )
    return {**evidence, "status": "template_created", "path": str(write_new(path, document))}
