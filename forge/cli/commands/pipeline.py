"""Single-recipe historical pipeline commands."""

import argparse
import asyncio
import json
import signal
import sys
import threading
from typing import Any

from rich.text import Text

from forge.cli.environment import add_database_options
from forge.cli.sorting import add_sorting
from forge.historical.authoring import create_pipeline
from forge.historical.pipelines import service
from forge.historical.pipelines.config import load_recipe_file
from forge.historical.pipelines.dashboard import present
from forge.historical.pipelines.planning import plan
from forge.historical.pipelines.terminal import (
    Ghostty,
    StageWindows,
    WindowUnavailable,
)
from forge.shared.config.library import list_config_names, resolve_config_file
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.presentation import pipeline as views
from forge.shared.sorting import NAMES
from forge.shared.terminal import pipeline as terminal
from forge.shared.terminal.presentation import header, reference
from forge.startup.context import ApplicationContext


def configure(parser: argparse.ArgumentParser) -> None:
    commands = parser.add_subparsers(dest="pipeline_action", required=True)
    listing = commands.add_parser(
        "list", help="List managed pipeline recipes and local validation state."
    )
    listing.set_defaults(service_handler=command)
    add_sorting(listing, NAMES)
    for action in ("new", "validate", "plan", "run", "resume", "status"):
        child = commands.add_parser(action, allow_abbrev=False)
        child.add_argument(
            "reference",
            metavar="RUN_ID" if action in {"resume", "status"} else "NAME",
            help=(
                "Retained pipeline run ID."
                if action in {"resume", "status"}
                else "Managed pipeline recipe name."
            ),
        )
        if action in {"plan", "run", "resume"}:
            add_database_options(child)
        if action in {"run", "resume"}:
            child.add_argument(
                "--tui", action="store_true", help="Show interactive pipeline progress."
            )
            child.add_argument(
                "--stage-terminal",
                choices=("auto", "window", "inline"),
                default="auto",
                help="Stage display: automatic desktop windows, required windows, or inline (default: auto).",
            )
        if action == "run":
            child.add_argument(
                "--stop-after",
                choices=["normalize.prepare"],
                help="Retain prepared normalization jobs and stop before executing them.",
            )
        child.set_defaults(service_handler=command)


def display(
    context: ApplicationContext, result: dict[str, Any], *, action: str, as_json: bool
) -> None:
    if as_json:
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return
    index = (
        asyncio.run(service.execution_index(context.paths, result)) if result.get("run_id") else ""
    )
    if result.get("run_id") and (recipe := service.retained_recipe_metadata(context.paths, result)):
        result = {**result, "recipe": recipe}
    terminal.render(context.console, views.project(result, action, index_state=index))


async def run_pipeline(
    run: service.PipelineRun, context: ApplicationContext, *, tui: bool, stop: bool
) -> dict[str, Any]:
    if threading.current_thread() is not threading.main_thread():
        return await present(
            run, context.error_console, tui=tui or run.windows is not None, stop=stop
        )
    task = asyncio.current_task()
    assert task is not None
    loop = asyncio.get_running_loop()
    previous = signal.getsignal(signal.SIGINT)
    requested = False

    def interrupt() -> None:
        nonlocal requested
        if not requested:
            requested = True
            run.interrupting = True
            task.cancel()

    loop.add_signal_handler(signal.SIGINT, interrupt)
    try:
        return await present(
            run, context.error_console, tui=tui or run.windows is not None, stop=stop
        )
    finally:
        loop.remove_signal_handler(signal.SIGINT)
        signal.signal(signal.SIGINT, previous)


def stage_windows(
    args: argparse.Namespace, context: ApplicationContext, owner: WorkspaceOwnership
) -> StageWindows | None:
    mode = args.stage_terminal
    interactive = all(stream.isatty() for stream in (sys.stdin, sys.stdout, sys.stderr))
    if mode == "inline" or (mode == "auto" and (args.as_json or not interactive)):
        return None
    try:
        launcher = Ghostty.detect()
    except WindowUnavailable as error:
        if mode == "window":
            raise
        context.error_console.print(Text(f"{error} Continuing inline.", style="mf.warning"))
        return None

    def diagnostic(message: str) -> None:
        context.error_console.print(Text(message, style="mf.warning"))

    return StageWindows(launcher, owner.descriptors(), mode == "auto", diagnostic)


def command(args: argparse.Namespace, context: ApplicationContext) -> int:
    action = args.pipeline_action
    if action == "list":
        directory = context.paths.pipeline_config_dir
        names = list_config_names(directory, sort=args.sort, order=args.order)
        if args.as_json:
            print(json.dumps({"schema_version": 1, "recipes": list(names)}))
        else:
            terminal.recipes(
                context.console,
                tuple(views.library_entry(name, directory) for name in names),
                NAMES.resolve(args.sort, args.order).description,
            )
        return 0
    if action == "new":
        path = create_pipeline(context.paths.pipeline_config_dir, args.reference)
        if args.as_json:
            print(json.dumps({"status": "draft_created", "path": str(path)}))
        else:
            header(context.console, "Pipeline · New")
            reference(context.console, "Created recipe draft", str(path))
        return 0
    tui = getattr(args, "tui", False)
    if args.as_json and tui:
        raise MarketForgeError("--tui is incompatible with global --json.")
    tui = tui and sys.stdin.isatty() and sys.stderr.isatty()
    if action == "status":
        result = service.status(context.paths, args.reference)
        display(context, result, as_json=args.as_json, action=action)
        return 0
    if action == "resume":
        with WorkspaceOwnership(context.paths) as owner:
            run = service.resume(context, args.reference)
            if run.report.get("state") != "complete":
                run.windows = stage_windows(args, context, owner)
            result = asyncio.run(run_pipeline(run, context, tui=tui, stop=False))
    else:
        source = resolve_config_file(context.paths.pipeline_config_dir, args.reference)
        submitted = source.read_bytes()
        recipe = load_recipe_file(source, submitted)
        if action == "validate":
            result = {
                "schema_version": 1,
                "kind": "historical_pipeline_validation",
                "state": "valid",
                "source": str(source),
                "recipe": recipe.model_dump(mode="json"),
            }
        elif action == "plan":
            result = asyncio.run(plan(context.paths, recipe, source))
        else:
            with WorkspaceOwnership(context.paths) as owner:
                windows = stage_windows(args, context, owner)
                run = service.new(context, source.stem, submitted, source=source)
                run.windows = windows
                result = asyncio.run(
                    run_pipeline(run, context, tui=tui, stop=bool(args.stop_after))
                )
    display(context, result, as_json=args.as_json, action=action)
    return {"failed": 1, "blocked": 2, "interrupted": 130}.get(str(result["state"]), 0)
