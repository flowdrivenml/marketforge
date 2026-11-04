"""Named TOML ranking policies and immutable snapshot commands."""

import argparse
import asyncio
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from forge.cli.commands.markets import emit
from forge.markets.ranking.policy import TEMPLATE, load_policy
from forge.markets.ranking.service import build, list_snapshots, show
from forge.shared.config.library import resolve_config_file
from forge.shared.database.testing import testing_connection, testing_dsn
from forge.startup.context import ApplicationContext


class Options(BaseModel):
    model_config = ConfigDict(extra="ignore")
    rank_action: str
    ranking_name: str | None = None
    ranking_id: UUID | None = None
    as_json: bool = False
    limit: int = 50


def configure(parser: argparse.ArgumentParser) -> None:
    actions = parser.add_subparsers(dest="rank_action", required=True)
    for action in ("new", "validate", "build", "list", "show"):
        child = actions.add_parser(action)
        child.set_defaults(service_handler=run)
        if action in {"new", "validate", "build"}:
            child.add_argument("ranking_name")
        if action == "show":
            child.add_argument("ranking_id", type=UUID)
        if action in {"build", "list", "show"}:
            child.add_argument("--testing", required=True, action="store_true")
        child.add_argument("--limit", type=int, default=50)


def run(arguments: argparse.Namespace, context: ApplicationContext) -> int:
    o = Options.model_validate(vars(arguments))
    directory = context.paths.config_dir / "markets" / "rankings"
    if o.rank_action in {"new", "validate", "build"}:
        if o.ranking_name is None:
            raise ValueError("Ranking name is required.")
        path = resolve_config_file(directory, o.ranking_name)
        if o.rank_action == "new":
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("x", encoding="utf-8") as out:
                out.write(TEMPLATE)
            emit(context, {"configuration": str(path), "created": True}, o.as_json)
            return 0
        policy = load_policy(path)
        if o.rank_action == "validate":
            emit(context, {"valid": True, "policy": policy.model_dump(mode="json")}, o.as_json)
            return 0

        async def generate() -> object:
            async with testing_connection(testing_dsn(context.paths.root)) as c:
                return {"snapshot_id": str(await build(c, path.stem, policy))}

        emit(context, asyncio.run(generate()), o.as_json)
        return 0

    async def browse() -> object:
        async with testing_connection(testing_dsn(context.paths.root)) as c:
            if o.rank_action == "list":
                return {"rankings": await list_snapshots(c, o.limit)}
            if o.ranking_id is None:
                raise ValueError("Ranking ID is required.")
            return await show(c, o.ranking_id)

    emit(context, asyncio.run(browse()), o.as_json)
    return 0
