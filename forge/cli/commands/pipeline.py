"""Single-recipe historical pipeline commands."""

import argparse
import asyncio
import json
import signal
import sys
import threading
from typing import Any

from rich.console import Console
from rich.text import Text

from forge.historical.authoring import create_pipeline
from forge.historical.pipelines import service
from forge.historical.pipelines.config import load_recipe
from forge.historical.pipelines.dashboard import present
from forge.historical.pipelines.planning import plan
from forge.shared.config.library import resolve_config_file
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import WorkspaceOwnership
from forge.startup.context import ApplicationContext


def configure(parser: argparse.ArgumentParser) -> None:
    commands = parser.add_subparsers(dest="pipeline_action", required=True)
    for action in ("new", "validate", "plan", "run", "resume", "status"):
        child = commands.add_parser(action, allow_abbrev=False)
        child.add_argument(
            "reference", metavar="RUN_ID" if action in {"resume", "status"} else "NAME"
        )
        if action in {"plan", "run", "resume"}:
            child.add_argument("--testing", required=True, action="store_true")
        if action in {"run", "resume"}:
            child.add_argument("--tui", action="store_true")
        if action == "run":
            child.add_argument("--stop-after", choices=["normalize.prepare"])
        child.set_defaults(service_handler=command)


def display(context: ApplicationContext, result: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return
    console = context.console if sys.stdout.isatty() else Console(color_system=None)
    console.print(Text(f"Pipeline {result.get('name', '')} · {result['state']}"))
    if "run_id" in result:
        console.print(Text(f"Run {result['run_id']}"))
    for name, stage in result.get("stages", {}).items():
        if isinstance(stage, str):
            console.print(Text(f"{name}: {stage}"))
        else:
            console.print(
                Text(
                    f"{name}: {stage['state']} · {stage.get('phase', 'pending')} "
                    f"{stage.get('summary', '')}".rstrip()
                )
            )
            if stage.get("primary_file_ids"):
                console.print(Text(f"  Registered primary files {stage['primary_file_ids']}"))
            phases = stage.get("phases", {})
            if phases:
                console.print(
                    Text(
                        "  "
                        + " · ".join(
                            f"{phase.replace('_', ' ')} {record['state']}"
                            for phase, record in phases.items()
                        )
                    )
                )
            outcome = stage.get("result", {})
            for location, publication in outcome.get("publications", {}).items():
                console.print(
                    Text(f"  Registry publication {publication['publication_id']} · {location}")
                )
            for job in outcome.get("jobs", []):
                console.print(Text(f"  Job {job['job_id']} · engine dataset {job['dataset_id']}"))
            if outcome.get("registration_receipt"):
                console.print(Text(f"  Registration {outcome['registration_receipt']}"))
            for key in ("child_report", "execution_intent"):
                if key in stage:
                    console.print(Text(f"  {stage[key]}"))
    if "recipe" in result:
        recipe = result["recipe"]
        console.print(
            Text(
                f"{len(recipe['requests'])} requested streams · "
                f"{recipe['interval']['start']} – {recipe['interval']['end']}"
            )
        )
        for request in recipe["requests"]:
            console.print(
                Text(
                    f"  {request['exchange']} {request['instrument_type']}/"
                    f"{request['market_category']} {request['scope']['identifier']} "
                    f"{request['data_type']}"
                )
            )
    for item in result.get("inventory", []):
        request, evidence = item["request"], item["registered_evidence"]
        console.print(
            Text(
                f"  {request['scope']['identifier']} {request['data_type']}: "
                f"{evidence['total']} matching registered archives"
            )
        )
    for message in [*result.get("blockers", []), result.get("error")]:
        if message:
            console.print(Text(message, style="mf.failure"))
    if "report_path" in result:
        console.print(Text(f"Report {result['report_path']}"))
    if result["state"] in {"stopped", "interrupted", "blocked", "failed"} and "run_id" in result:
        console.print(Text(f"Resume with mf pipeline resume {result['run_id']} --testing --tui"))


async def run_pipeline(
    run: service.PipelineRun, context: ApplicationContext, *, tui: bool, stop: bool
) -> dict[str, Any]:
    if threading.current_thread() is not threading.main_thread():
        return await present(run, context.error_console, tui=tui, stop=stop)
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
        return await present(run, context.error_console, tui=tui, stop=stop)
    finally:
        loop.remove_signal_handler(signal.SIGINT)
        signal.signal(signal.SIGINT, previous)


def command(args: argparse.Namespace, context: ApplicationContext) -> int:
    action = args.pipeline_action
    if action == "new":
        path = create_pipeline(context.paths.pipeline_config_dir, args.reference)
        if args.as_json:
            print(json.dumps({"status": "draft_created", "path": str(path)}))
        else:
            context.console.print(Text(f"Created pipeline draft {path}"))
        return 0
    tui = getattr(args, "tui", False)
    if args.as_json and tui:
        raise MarketForgeError("--tui is incompatible with global --json.")
    tui = tui and sys.stdin.isatty() and sys.stderr.isatty()
    if action == "status":
        result = service.status(context.paths, args.reference)
        display(context, result, as_json=args.as_json)
        return 0
    if action == "resume":
        with WorkspaceOwnership(context.paths):
            run = service.resume(context, args.reference)
            result = asyncio.run(run_pipeline(run, context, tui=tui, stop=False))
    else:
        source = resolve_config_file(context.paths.pipeline_config_dir, args.reference)
        submitted = source.read_bytes()
        recipe = load_recipe(submitted)
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
            with WorkspaceOwnership(context.paths):
                run = service.new(context, source.stem, submitted)
                result = asyncio.run(
                    run_pipeline(run, context, tui=tui, stop=bool(args.stop_after))
                )
    display(context, result, as_json=args.as_json)
    return {"failed": 1, "blocked": 2, "interrupted": 130}.get(str(result["state"]), 0)
