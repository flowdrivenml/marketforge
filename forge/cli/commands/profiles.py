"""Namespaced profile management without database initialization."""

import argparse
import asyncio
import json
import shlex
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

from forge.cli.environment import add_database_options
from forge.historical.imports.formats import sample
from forge.historical.profiles.authoring import authoring_guide
from forge.historical.profiles.documentation import guide_url
from forge.historical.profiles.schema import load_schema
from forge.historical.profiles.service import (
    create_draft,
    discover,
    inspect,
    installed,
    new_draft,
    register,
    validate,
)
from forge.historical.profiles.source_sample import DEFAULT_ROWS, MAX_ROWS, inspect_source
from forge.shared.errors import MarketForgeError
from forge.shared.terminal.console import create_console
from forge.shared.terminal.editor import edit_file
from forge.shared.terminal.profiles import (
    render_documentation,
    render_inventory,
    render_profile,
    render_schema,
)
from forge.shared.terminal.source_sample import render_source_sample
from forge.startup.context import ApplicationContext


def configure(parser: argparse.ArgumentParser) -> None:
    actions = parser.add_subparsers(dest="profile_action", required=True)
    command = actions.add_parser("list")
    for name in ("type", "exchange", "instrument-type", "market-category"):
        command.add_argument(f"--{name}")
    command.set_defaults(service_handler=run)
    command = actions.add_parser(
        "sample", help="Inspect a bounded raw sample before authoring a profile."
    )
    command.add_argument("--file", type=Path, required=True)
    command.add_argument("--member")
    command.add_argument("--header", action=argparse.BooleanOptionalAction, default=None)
    command.add_argument(
        "--rows",
        type=int,
        choices=range(1, MAX_ROWS + 1),
        metavar="1–20",
        help="Maximum sampled data records. Human output defaults to five.",
    )
    command.add_argument(
        "--fields", action="store_true", help="Show source fields without the data preview."
    )
    command.set_defaults(service_handler=run)
    command = actions.add_parser("schema", help="Explore the Rust normalization profile reference.")
    command.add_argument("--type", choices=("trades", "depth"))
    selectors = command.add_mutually_exclusive_group()
    selectors.add_argument("--field", help="Explain one field, such as source.format.")
    selectors.add_argument("--section", help="Browse one section, such as normalization.")
    command.set_defaults(service_handler=run)
    command = actions.add_parser("guide", help="Walk through authoring a trade or depth profile.")
    command.add_argument("kind", choices=("trades", "depth"))
    command.set_defaults(service_handler=run)
    for action in ("inspect", "show", "new", "edit", "validate", "add", "register"):
        command = actions.add_parser(action)
        command.add_argument("profile_id", nargs="?" if action == "validate" else None)
        if action == "validate":
            command.add_argument("--file", type=Path)
        if action in {"add", "register"}:
            command.add_argument("--from", dest="source", type=Path, required=True)
            add_database_options(command)
        command.set_defaults(service_handler=run)


def interactive() -> bool:
    return sys.stdin.isatty() and sys.stdout.isatty() and sys.stderr.isatty()


def schema_output(args: argparse.Namespace, context: ApplicationContext) -> int:
    schema = load_schema()
    section = schema.section(args.section) if args.section is not None else None
    parameters = schema.parameters(section=section.id if section else None, profile_type=args.type)
    field = schema.field(args.field, profile_type=args.type) if args.field is not None else None
    if args.as_json:
        document = schema.model_dump(mode="json")
        if args.type or field or section:
            selected = (field,) if field else parameters
            sections = {item.section for item in selected}
            if section:
                sections.add(section.id)
            document["sections"] = [
                item.model_dump(mode="json") for item in schema.sections if item.id in sections
            ]
            document["fields"] = [item.model_dump(mode="json") for item in selected]
        print(json.dumps(document))
    else:
        render_schema(
            context.console, schema, field=field, selected=section, profile_type=args.type
        )
    return 0


def inventory(args: argparse.Namespace, context: ApplicationContext) -> int:
    filters: dict[str, str | None] = {
        name: getattr(args, name)
        for name in ("type", "exchange", "instrument_type", "market_category")
    }
    if args.as_json:
        print(json.dumps({"profiles": discover(context.paths, **filters)}))
    else:
        installed_profiles = discover(context.paths)
        keys = ("exchange", "instrument_type", "market_category", "type")
        matching = [
            entry
            for entry in installed_profiles
            if all(
                filters[key] in (None, token)
                for key, token in zip(keys, entry["id"].split("."), strict=True)
            )
        ]
        render_inventory(context.console, matching, len(installed_profiles), filters)
    return 0


def editor_action(args: argparse.Namespace, context: ApplicationContext) -> int:
    editing = args.profile_action == "edit"
    if args.as_json:
        raise MarketForgeError("Profile editing requires an interactive terminal without --json.")
    if not interactive():
        if editing:
            raise MarketForgeError("Profile editing requires an interactive terminal.")
        raise MarketForgeError(
            "Profile inspection requires an interactive terminal. Use "
            "mf normalize profiles show PROFILE_ID or "
            "mf --json normalize profiles inspect PROFILE_ID."
        )
    editor = context.settings.presentation.editor or "nvim"
    if not editing:
        source = installed(context.paths, args.profile_id)
        inspect(source)
        edit_file(source, editor=editor, readonly=True)
        render_documentation(context.console)
        return 0

    draft = create_draft(context.paths, args.profile_id)
    context.console.print(f"Draft: {draft}", style="mf.identifier")
    edit_file(draft, editor=editor)
    inspect(draft)
    context.console.print("Draft passed TOML and namespace checks.", style="mf.success")
    context.console.print(
        f"Compile it with: mf normalize profiles validate --file {shlex.quote(str(draft))}"
    )
    render_documentation(context.console)
    return 0


def run(args: argparse.Namespace, context: ApplicationContext) -> int:
    action = args.profile_action
    if action == "guide":
        guide = authoring_guide(args.kind)
        if args.as_json:
            print(json.dumps(asdict(guide)))
        else:
            label = "trade" if guide.profile_type == "trades" else "depth"
            context.console.print(f"MarketForge · Author a {label} profile", style="mf.heading")
            for number, step in enumerate(guide.steps, start=1):
                context.console.print(f"\n{number}. {step.title}", style="mf.section")
                context.console.print(step.detail)
                if step.command:
                    context.console.print(step.command, style="mf.warning")
            render_documentation(context.console)
        return 0
    if action == "new":
        draft = new_draft(context.paths, args.profile_id)
        documentation = guide_url(args.profile_id.rsplit(".", 1)[1])
        command = f"mf normalize profiles validate --file {shlex.quote(str(draft))}"
        if args.as_json:
            print(
                json.dumps(
                    {"draft": str(draft), "validation": "pending", "documentation": documentation}
                )
            )
        else:
            context.console.print(f"Draft  {draft}", style="mf.identifier")
            context.console.print(
                "Use nvim to resolve the format code and check the example mappings."
            )
            context.console.print(command)
            render_documentation(context.console)
        return 0
    if action == "list":
        return inventory(args, context)
    if action == "schema":
        return schema_output(args, context)
    if action == "edit" or (action == "inspect" and not args.as_json):
        return editor_action(args, context)
    if action == "sample":
        if args.as_json and args.rows is None:
            print(json.dumps(sample(args.file, args.member, args.header)))
        else:
            evidence = inspect_source(
                args.file, args.member, args.header, rows=args.rows or DEFAULT_ROWS
            )
            if args.as_json:
                print(evidence.evidence_json())
            else:
                render_source_sample(context.console, evidence, fields_only=args.fields)
        return 0
    result: dict[str, Any]
    if action in {"add", "register"}:
        result = asyncio.run(register(context.paths, args.profile_id, args.source))
    else:
        source = getattr(args, "file", None)
        if (source is None) == (args.profile_id is None):
            raise MarketForgeError("Supply exactly one profile ID or --file.")
        source = source or installed(context.paths, args.profile_id)
        if action == "validate":
            try:
                result = validate(context.paths, source)
            except MarketForgeError as error:
                if args.as_json:
                    raise
                console = create_console(context.settings.presentation, stderr=True)
                console.print(str(error), style="mf.failure")
                render_documentation(console)
                return 2
        else:
            result = inspect(source)
    if args.as_json:
        print(json.dumps(result))
    elif action == "show":
        render_profile(context.console, result)
    else:
        context.console.print_json(data=result)
    return 0
