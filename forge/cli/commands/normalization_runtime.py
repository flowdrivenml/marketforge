"""Manage the Rust Engine runtime separately from recipes and profiles."""

import argparse
import json
import os
import shlex
import shutil
import sys
from pathlib import Path
from tempfile import mkdtemp
from typing import Any

from forge.historical.engine.runtime_config import (
    RuntimeConfiguration,
    RuntimeOperation,
    parse_document,
    read_snapshot,
)
from forge.shared.errors import MarketForgeError
from forge.shared.terminal.editor import edit_file
from forge.shared.terminal.runtime_config import OVERVIEW_LABELS, render_runtime
from forge.startup.context import ApplicationContext


def configure(
    parser: argparse.ArgumentParser, *, operation: RuntimeOperation = "normalize"
) -> None:
    actions = parser.add_subparsers(dest="runtime_action", required=True)
    show = actions.add_parser(
        "show", help="Show configured and effective Engine settings.", allow_abbrev=False
    )
    scope = show.add_mutually_exclusive_group()
    scope.add_argument(
        "--all", action="store_true", help="List every supported field and its state."
    )
    scope.add_argument("--section", help="Canonical TOML section, such as resources.")
    get = actions.add_parser("get", help="Inspect one supported setting.", allow_abbrev=False)
    get.add_argument("field", help="Dotted runtime field path.")
    schema = actions.add_parser(
        "schema", help="Browse the Engine runtime schema.", allow_abbrev=False
    )
    schema.add_argument("--field", help="Explain one dotted runtime field path.")
    actions.add_parser(
        "validate", help="Validate the complete runtime through Rust.", allow_abbrev=False
    )
    for name in ("set", "unset"):
        child = actions.add_parser(
            name,
            help="Preview or apply a typed change."
            if name == "set"
            else "Preview or remove an optional field.",
            allow_abbrev=False,
        )
        child.add_argument("field", help="Dotted runtime field path.")
        if name == "set":
            child.add_argument(
                "value",
                help="Native value or exact binary size for a field that accepts size units.",
            )
        child.add_argument(
            "--apply",
            action="store_true",
            help="Replace the runtime after validation and conflict checks.",
        )
        child.add_argument(
            "--acknowledge-risk",
            action="store_true",
            help="Required with --apply for integrity-critical changes.",
        )
    actions.add_parser(
        "edit", help="Validate and review a private copy before saving.", allow_abbrev=False
    )
    parser.set_defaults(service_handler=run, runtime_operation=operation)


def inspection(service: RuntimeConfiguration, args: argparse.Namespace) -> dict[str, Any]:
    action = args.runtime_action
    if action == "validate":
        snapshot = service.snapshot()
        effective = service.validate(snapshot.data)
        return {
            "status": "valid",
            "runtime_schema_version": parse_document(snapshot.data)["schema_version"],
            "validation": {"toml": True, "rust": True},
            "effective": effective,
        }

    schema = service.schema()
    field_path = getattr(args, "field", None)
    fields = [service.field(field_path)] if field_path else schema["fields"]
    section = getattr(args, "section", None)
    if section:
        fields = [field for field in fields if field["path"].startswith(f"{section}.")]
        if not fields:
            raise MarketForgeError(f"Unsupported {service.label} runtime section: {section}")
    overview = action == "show" and not (args.all or section)
    if overview and service.operation == "normalize":
        fields = [service.field(path) for path in OVERVIEW_LABELS]
    configured: dict[str, Any] = {}
    effective: dict[str, Any] | None = None
    warning = None
    try:
        snapshot = service.snapshot()
        effective = service.validate(snapshot.data)
        configured = parse_document(snapshot.data)
    except MarketForgeError as error:
        if action in {"show", "get"}:
            raise
        warning = str(error)

    return {
        "status": "available" if warning else "valid",
        "runtime_schema_version": schema["runtime_schema_version"],
        "validation": {"rust": effective is not None},
        "fields": [service.describe(field, configured, effective) for field in fields],
        "view": "overview" if overview and service.operation == "normalize" else "detail",
        **({"warning": warning} if warning else {}),
    }


def edit(service: RuntimeConfiguration, context: ApplicationContext, result: dict[str, Any]) -> int:
    if not all(stream.isatty() for stream in (sys.stdin, sys.stdout, sys.stderr)):
        raise MarketForgeError(
            f"{service.label.capitalize()} runtime edit requires an interactive terminal."
        )
    original = service.snapshot()
    directory = Path(mkdtemp(prefix=f"marketforge-{service.operation}-edit-")).resolve()
    draft = directory / service.path.name
    saved = False
    try:
        with draft.open("xb") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(original.data)
        editor = (
            context.settings.presentation.editor
            or os.environ.get("VISUAL")
            or os.environ.get("EDITOR")
            or "nvim"
        )
        edit_file(draft, editor=editor)
        change = service.propose(original, read_snapshot(draft).data)
        if change.candidate == original.data:
            result.update(change.report(status="unchanged"))
            saved = True
            return 0
        preview = {**result, **change.report(status="ready_to_apply")}
        render_runtime(context.console, preview, root=context.paths.root, documentation=False)
        phrase = "apply and acknowledge risk" if change.requires_acknowledgement else "apply"
        context.console.print(f"Type {phrase!r} to replace the runtime configuration.")
        if input().strip() != phrase:
            result.update(change.report(status="cancelled"), draft_path=str(draft))
            return 1
        service.apply(change, acknowledge_risk=change.requires_acknowledgement)
        result.update(change.report(status="applied"))
        saved = True
        return 0
    finally:
        if saved:
            shutil.rmtree(directory)
        else:
            result["draft_path"] = str(draft)


def run(args: argparse.Namespace, context: ApplicationContext) -> int:
    service = RuntimeConfiguration(context.paths, args.runtime_operation)
    action = args.runtime_action
    report: dict[str, Any] = {
        "schema_version": 1,
        "kind": f"{service.label}_runtime_configuration",
        "action": action,
        "configuration_path": str(service.path),
    }
    code = 0
    try:
        if action == "edit":
            if args.as_json:
                raise MarketForgeError(
                    f"{service.label.capitalize()} runtime edit is incompatible with global --json."
                )
            code = edit(service, context, report)
        elif action in {"set", "unset"}:
            if args.acknowledge_risk and not args.apply:
                raise MarketForgeError("--acknowledge-risk requires --apply.")
            raw = args.value if action == "set" else None
            change = service.change_field(service.snapshot(), args.field, raw)
            changed = change.candidate != change.original.data
            report.update(change.report(status="ready_to_apply" if changed else "unchanged"))
            report["field"] = args.field
            if changed:
                command = ["mf"]
                if args.project_root is not None:
                    command.extend(["--project-root", str(context.paths.root)])
                command.extend([service.operation, "config", action, args.field])
                if raw is not None:
                    command.append(raw)
                command.append("--apply")
                if change.requires_acknowledgement:
                    command.append("--acknowledge-risk")
                report["apply_command"] = shlex.join(command)
            if args.apply and changed:
                service.apply(change, acknowledge_risk=args.acknowledge_risk)
                report["status"] = "applied"
                report.pop("apply_command", None)
        else:
            report.update(inspection(service, args))
    except (MarketForgeError, OSError, ValueError, EOFError) as error:
        report.update(status="failed", error={"message": str(error)})
        code = 1
    except KeyboardInterrupt:
        report.update(status="interrupted", error={"message": "Configuration editing interrupted."})
        code = 130
    if args.as_json:
        print(json.dumps(report, ensure_ascii=False, allow_nan=False))
    else:
        render_runtime(context.console, report, root=context.paths.root)
    return code
