"""Managed external import configurations and non-executable format drafts."""

import argparse
import asyncio
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from forge.cli.commands.markets import emit
from forge.historical.imports.formats import draft, sample
from forge.historical.imports.models import ImportError, freeze_inputs, load_import, plan
from forge.historical.imports.service import run_import
from forge.shared.config.library import resolve_config_file
from forge.shared.config.toml import dumps
from forge.shared.database.testing import testing_dsn
from forge.shared.ownership import WorkspaceOwnership
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
            child.add_argument("--testing", required=True, action="store_true")
    formats = actions.add_parser("format").add_subparsers(dest="format_action", required=True)
    child = formats.add_parser(
        "new",
        help="Create physical/profile TOML drafts outside the active Rust catalogue; mappings remain unapproved.",
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
    path = resolve_config_file(context.paths.acquisition_config_dir / "import", o.import_name)
    if o.import_action == "new":
        if o.from_path is None:
            raise ImportError("--from is required.")
        source = o.from_path.expanduser().resolve()
        if not source.exists():
            raise ImportError("External input does not exist.")
        document = {
            "version": 1,
            "source_key": o.import_name,
            "source_kind": "recording",
            "root": str(source if source.is_dir() else source.parent),
            "files": [source.name] if source.is_file() else [],
            "patterns": ["*"] if source.is_dir() else [],
            "max_files": 1000,
            "data_type": "trades",
            "identity": {
                "exchange": "external",
                "market_category": "spot",
                "instrument_type": "spot",
                "symbol": "UNSPECIFIED",
            },
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("x", encoding="utf-8") as out:
            out.write(
                "# Supply explicit identity/specifications before validating. No venue or units were inferred.\n"
                + dumps(document)
            )
        emit(
            context,
            {
                "created": str(path),
                "draft": True,
                "unresolved": [
                    "identity",
                    "historical specifications",
                    "data_type",
                    "optional interval and format profile",
                ],
            },
            o.as_json,
        )
        return 0
    config = load_import(path)
    if o.import_action == "format":
        if o.profile is None or o.input_reference is None:
            raise ImportError("Select --profile and --input.")
        result = draft(
            context.paths.root,
            config,
            o.profile,
            o.input_reference,
            member=o.member,
            header=o.header,
            overwrite=o.overwrite,
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
                run_import(context.paths, testing_dsn(context.paths.root), config, path)
            )
    emit(context, result, o.as_json)
    return 1 if result.get("status") == "failed" else 0
