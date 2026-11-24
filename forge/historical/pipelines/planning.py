"""Read-only registry evidence for a pipeline request."""

from pathlib import Path
from typing import Any

from forge.historical.engine.runtime import runtime_report
from forge.historical.pipelines.config import PipelineRecipe
from forge.shared.database.connection import database_connection, database_dsn, database_purpose
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths
from forge.storage.models import StorageFilters
from forge.storage.registry import counts, parameters


async def plan(paths: ProjectPaths, recipe: PipelineRecipe, source: Path) -> dict[str, Any]:
    blockers: list[str] = []
    inventory: list[dict[str, Any]] = []
    stages = {
        "acquire": "Discovery and local verification deferred until run.",
        "normalize": "Exact archive selection, ranking and jobs deferred until acquisition.",
        "merge": "Primary-file identities and merge job deferred until normalization registration.",
    }
    if recipe.convert is not None:
        stages["convert"] = (
            "Exact merged publication and reference resolution deferred until merge registration."
        )
    runtime: dict[str, str] = {}
    for operation in ("pipeline", "convert") if recipe.convert is not None else ("pipeline",):
        try:
            runtime_report(paths, operation)
            runtime[operation] = "Available"
        except (MarketForgeError, OSError) as error:
            runtime[operation] = "Unavailable"
            blockers.append(str(error))
    try:
        async with (
            database_connection(database_dsn(paths.root)) as connection,
            connection.transaction(),
        ):
            await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            for request in recipe.requests:
                options = request.source_options
                filters = StorageFilters(
                    exchange=request.exchange,
                    market=request.market_category,
                    instrument_type=request.instrument_type,
                    instrument=request.scope.identifier,
                    scope_kind=request.scope.kind,
                    data_type=request.data_type,
                    status="complete",
                    origin="remote",
                    start=recipe.interval.start,
                    end=recipe.interval.end,
                    feed=(options.feed if options else "incremental")
                    if request.exchange == "gateio" and request.data_type == "order_book_l2"
                    else None,
                )
                evidence = await counts(connection, parameters(paths.raw_dir, filters))
                inventory.append(
                    {"request": request.model_dump(mode="json"), "registered_evidence": evidence}
                )
    except MarketForgeError as error:
        blockers.append(str(error))
    return {
        "schema_version": 1,
        "kind": "historical_pipeline_plan",
        "state": "blocked" if blockers else "planned",
        "environment": database_purpose(),
        "source": str(source),
        "recipe": recipe.model_dump(mode="json"),
        "stages": stages,
        "inventory": inventory,
        "blockers": blockers,
        "runtime_availability": runtime,
    }
