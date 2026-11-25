"""Managed external import configurations and non-executable format drafts."""

import argparse
import asyncio
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from forge.cli.commands.markets import emit
from forge.cli.environment import add_database_options
from forge.historical.drafts import create_import
from forge.historical.imports.formats import sample
from forge.historical.imports.models import ImportError, freeze_inputs, plan, read_import
from forge.historical.imports.service import run_import
from forge.shared.config.library import resolve_config_file
from forge.shared.database.connection import database_dsn
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.terminal.acquisition_result import render_external
from forge.startup.context import ApplicationContext


class Options(BaseModel):
    model_config = ConfigDict(extra="ignore")
    import_action: str
    import_name: str
    from_path: Path | None = None
    as_json: bool = False
    profile: str | None = None
    input_reference: str | None = None
    member: str | None = None
    header: bool | None = None
    overwrite: bool = False


def configure(parser: argparse.ArgumentParser) -> None:
    actions = parser.add_subparsers(dest="import_action", required=True)
    for action in ("new", "inspect", "validate", "plan", "run"):
        child = actions.add_parser(action)
        child.set_defaults(service_handler=run)
        child.add_argument(
            "--silent",
            action="store_true",
            help="Final result only; per-file outcomes remain in the run report/events.",
        )
        child.add_argument("import_name")
        if action == "new":
            child.add_argument("--from", dest="from_path", type=Path, required=True)
        if action == "run":
            add_database_options(child)
    formats = actions.add_parser("format").add_subparsers(dest="format_action", required=True)
    child = formats.add_parser(
        "new",
        help="Retired automatic drafting; use manually authored normalize profiles.",
    )
    child.set_defaults(service_handler=run)
    child.add_argument("import_name")
    child.add_argument("--profile", required=True)
    child.add_argument("--input", dest="input_reference", required=True)
    child.add_argument("--member", help="Exact member of a multi-member ZIP input.")
    child.add_argument(
        "--header",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Explicit CSV header evidence; otherwise left unresolved.",
    )
    child.add_argument("--overwrite", action="store_true")


def run(arguments: argparse.Namespace, context: ApplicationContext) -> int:
    o = Options.model_validate(vars(arguments))
    path = resolve_config_file(context.paths.workflow_dir("import"), o.import_name)
    if o.import_action == "new":
        if o.from_path is None:
            raise ImportError("--from is required.")
        source = o.from_path.expanduser().resolve()
        if not source.exists():
            raise ImportError("External input does not exist.")
        create_import(path, o.import_name, source)
        draft: dict[str, object] = {
            "created": str(path),
            "draft": True,
            "unresolved": [
                "identity",
                "historical specifications",
                "data_type",
                "optional interval and format profile",
            ],
        }
        _emit_import(context, o, draft)
        return 0
    config, submitted = read_import(path)
    result: dict[str, object]
    if o.import_action == "format":
        raise ImportError(
            "Automatic profile drafting is retired. Author a standalone schema-2 profile and use mf normalize profiles validate/add; migrate declarations with mf acquisition register."
        )
    elif o.import_action == "validate":
        inputs = freeze_inputs(config)
        result = {
            "valid": True,
            "input_count": len(inputs),
            "validation": "Configuration and stable-input plan structure only; no profile semantics or interval coverage inferred.",
        }
    elif o.import_action == "plan":
        result = plan(config)
    elif o.import_action == "inspect":
        inputs = freeze_inputs(config)
        result = {
            "inputs": [
                {"reference": i.reference, "bytes": i.size, "sample": sample(Path(i.path))}
                for i in inputs[:20]
            ],
            "sampled_inputs": min(20, len(inputs)),
            "total_inputs": len(inputs),
        }
    else:
        with WorkspaceOwnership(context.paths):
            result = asyncio.run(
                run_import(
                    context.paths,
                    database_dsn(context.paths.root),
                    config,
                    path,
                    source_bytes=submitted,
                )
            )
    _emit_import(context, o, result)
    return 1 if result.get("status") == "failed" else 0


def _emit_import(context: ApplicationContext, options: Options, result: dict[str, object]) -> None:
    if options.as_json:
        emit(context, result, True)
    else:
        render_external(
            context.console,
            result,
            operation="Import",
            action=options.import_action,
            name=options.import_name,
            root=context.paths.root,
        )
