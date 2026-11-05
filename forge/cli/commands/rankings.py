"""Named TOML ranking policies and immutable snapshot commands."""

import argparse
import asyncio
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from forge.cli.commands.markets import emit
from forge.cli.market_filters import configure_identity_filters
from forge.markets.models import MarketDataError, array, obj
from forge.markets.ranking.policy import TEMPLATE, load_policy, policy_document
from forge.markets.ranking.service import build, latest_snapshot, list_snapshots, show
from forge.markets.ranking.workflow import refresh_and_publish
from forge.shared.config.library import resolve_config_file
from forge.shared.database.testing import testing_connection, testing_dsn
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.terminal.markets import (
    MarketCollectionDisplay,
    render_ranking_result,
    render_workflow_result,
)
from forge.startup.context import ApplicationContext


class Options(BaseModel):
    model_config = ConfigDict(extra="ignore")
    rank_action: str
    ranking_name: str | None = None
    ranking_id: str | None = None
    instrument_type: Literal["spot", "perpetual", "future"] | None = None
    base_asset: str | None = Field(default=None, min_length=1)
    quote_asset: str | None = Field(default=None, min_length=1)
    match: str | None = None
    silent: bool = False
    as_json: bool = False
    limit: int = Field(default=50, ge=1, le=10000)
    all_entries: bool = False
    excluded: bool = False
    conversion: str | None = None


def configure(parser: argparse.ArgumentParser) -> None:
    actions = parser.add_subparsers(dest="rank_action", required=True)
    for action in ("new", "validate", "build", "list", "show", "update"):
        child = actions.add_parser(
            action,
            help={
                "update": "Refresh once and publish one or both standard policies.",
                "new": "Create a version-2 automatic spot-conversion policy.",
                "validate": "Validate TOML, conversion controls and manual provenance.",
                "build": "Build from retained observations only; no network calls.",
                "list": "List immutable published snapshots.",
                "show": "Inspect stored ranks and optional conversion evidence.",
            }[action],
        )
        child.set_defaults(service_handler=run)
        if action in {"new", "validate", "build"}:
            child.add_argument("ranking_name")
        if action == "show":
            child.add_argument("ranking_id", help="Policy name or published snapshot UUID.")
            configure_identity_filters(child)
            child.add_argument("--instrument-type", choices=("spot", "perpetual", "future"))
            child.add_argument(
                "--conversion",
                help="Inspect one entry's native amount, path, rate and source timestamps, by full instrument reference or rank number. JSON always includes complete evidence.",
            )
        if action == "update":
            child.add_argument("ranking_name", nargs="?", choices=("turnover", "volume"))
        if action in {"build", "list", "show", "update"}:
            child.add_argument("--testing", required=True, action="store_true")
        child.add_argument(
            "--silent",
            action="store_true",
            help="Retain final results; no animated ranking output.",
        )
        child.add_argument(
            "--limit",
            type=int,
            default=50,
            help="Human show: maximum ranked/excluded rows, without changing the snapshot or JSON. List: snapshot result limit.",
        )
        if action == "show":
            child.add_argument(
                "--all",
                dest="all_entries",
                action="store_true",
                help="Remove human row limits; exclusions require --excluded. JSON always contains all entries.",
            )
            child.add_argument(
                "--excluded",
                action="store_true",
                help="Also display individual excluded entries; default shows grouped reasons.",
            )


def run(arguments: argparse.Namespace, context: ApplicationContext) -> int:
    o = Options.model_validate(vars(arguments))

    def present(value: dict[str, object]) -> None:
        if o.as_json:
            emit(context, value, True)
        else:
            render_ranking_result(
                context.console,
                o.rank_action,
                value,
                limit=None if o.all_entries else o.limit,
                excluded=o.excluded,
                conversion=o.conversion,
            )

    directory = context.paths.config_dir / "markets" / "rankings"
    if o.rank_action == "update":
        display = MarketCollectionDisplay(context.error_console, silent=o.silent or o.as_json)
        with WorkspaceOwnership(context.paths), display:
            value = asyncio.run(
                refresh_and_publish(
                    testing_dsn(context.paths.root),
                    context.settings.http,
                    directory,
                    (o.ranking_name,) if o.ranking_name else ("turnover", "volume"),
                    progress=display.update,
                )
            )
        if o.as_json:
            emit(context, value, True)
        else:
            render_workflow_result(context.console, "Ranking update", value)
        return 130 if value["status"] == "cancelled" else 0 if value["status"] == "complete" else 1
    if o.rank_action in {"new", "validate", "build"}:
        if o.ranking_name is None:
            raise ValueError("Ranking name is required.")
        path = resolve_config_file(directory, o.ranking_name)
        if o.rank_action == "new":
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("x", encoding="utf-8") as out:
                out.write(TEMPLATE)
            present({"configuration": str(path), "created": True})
            return 0
        policy = load_policy(path)
        if o.rank_action == "validate":
            present({"valid": True, "policy": policy_document(policy)})
            return 0

        async def generate() -> dict[str, object]:
            async with testing_connection(testing_dsn(context.paths.root)) as c:
                return {"snapshot_id": str(await build(c, path.stem, policy))}

        with WorkspaceOwnership(context.paths):
            value = asyncio.run(generate())
        present(value)
        return 0

    async def browse() -> dict[str, object]:
        async with testing_connection(testing_dsn(context.paths.root)) as c:
            if o.rank_action == "list":
                rows = await list_snapshots(c, o.limit + 1)
                return {"rankings": rows[: o.limit], "truncated": len(rows) > o.limit}
            if o.ranking_id is None:
                raise ValueError("Ranking ID is required.")
            try:
                snapshot = UUID(o.ranking_id)
            except ValueError:
                name = resolve_config_file(directory, o.ranking_id).stem
                snapshot = await latest_snapshot(c, name)
            value = (
                await show(
                    c,
                    snapshot,
                    instrument_type=o.instrument_type,
                    base_asset=o.base_asset,
                    quote_asset=o.quote_asset,
                    match=o.match,
                )
                if o.quote_asset is not None or o.match is not None
                else await show(
                    c, snapshot, instrument_type=o.instrument_type, base_asset=o.base_asset
                )
                if o.base_asset
                else await show(c, snapshot, instrument_type=o.instrument_type)
                if o.instrument_type
                else await show(c, snapshot)
            )
            if o.conversion is not None and not any(
                str(obj(entry).get("position")) == o.conversion
                or obj(entry).get("instrument_reference") == o.conversion
                for entry in array(value["entries"])
            ):
                raise MarketDataError(
                    "Entry was not found in this pinned snapshot; use a stored instrument reference or rank number."
                )
            return value

    present(asyncio.run(browse()))
    return 0
