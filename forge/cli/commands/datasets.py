"""Browse registered primary data files without reading publication artifacts."""

import argparse
import asyncio
import json
import re
from collections import Counter
from contextlib import nullcontext
from dataclasses import replace
from pathlib import Path
from typing import TextIO, cast

from rich.table import Table

from forge.cli.environment import add_database_options
from forge.cli.sorting import add_sorting
from forge.datasets import deduplication, explicit_delete, force_cleanup
from forge.datasets.query import (
    DATA_TYPES,
    SEARCH_ACTIONS,
    DatasetRequest,
    bounded,
    browse,
    timestamp,
)
from forge.datasets.sorting import STREAMS, VIEWS
from forge.shared.database.connection import database_purpose
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.presentation.normalization import binary_size
from forge.shared.terminal.console import create_console
from forge.shared.terminal.dataset_browse import render_browse, render_detail
from forge.shared.terminal.dataset_timelines import render_timeline, render_timelines
from forge.shared.terminal.presentation import header, summary
from forge.startup.context import ApplicationContext


def positive_id(value: str) -> int:
    number = int(value)
    if not 1 <= number < 2**63:
        raise argparse.ArgumentTypeError("Use a positive signed 64-bit integer.")
    return number


def output_reference(value: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{64}", value):
        raise argparse.ArgumentTypeError(
            "Use the full lowercase 64-digit indexed output reference."
        )
    return value


def configure(parser: argparse.ArgumentParser) -> None:
    actions = parser.add_subparsers(dest="dataset_action", required=True)
    deduplicate = actions.add_parser(
        "deduplicate", help="Preview or apply verified publication deduplication."
    )
    add_database_options(deduplicate)
    deduplicate.add_argument(
        "--operation", choices=("normalize", "merge", "convert", "all"), default="all"
    )
    deduplicate.add_argument(
        "--apply",
        action="store_true",
        help="Revalidate and apply eligible retirements and filesystem removals.",
    )
    deduplicate.add_argument(
        "--plan", type=Path, help="Apply a saved preview without adding targets."
    )
    deduplicate.add_argument(
        "--force",
        action="store_true",
        help="Testing only. Preview a cascade that invalidates reviewed downstream registry outputs.",
    )
    deduplicate.add_argument(
        "--accept-unverified-legacy-references",
        action="store_true",
        help="Acknowledge that applying this exact testing force plan could break unresolved legacy production references.",
    )
    deduplicate.set_defaults(service_handler=run_deduplication)
    delete = actions.add_parser(
        "delete", help="Review or apply exact testing publication deletion."
    )
    add_database_options(delete)
    delete.add_argument(
        "--publication-id",
        type=positive_id,
        action="append",
        default=[],
        help="Exact testing registry publication ID. Repeat to select multiple outputs.",
    )
    delete.add_argument(
        "--output-ref",
        type=output_reference,
        action="append",
        default=[],
        help="Exact testing indexed conversion reference.",
    )
    delete.add_argument(
        "--retain-publication-id",
        type=positive_id,
        action="append",
        default=[],
        help="Require a specific survivor to verify before deletion. Repeat as needed.",
    )
    delete.add_argument(
        "--cascade",
        action="store_true",
        help="Review invalidation of dependent testing publications. Their files remain.",
    )
    delete.add_argument("--apply", action="store_true", help="Apply an exact saved deletion plan.")
    delete.add_argument("--plan", type=Path, help="Path to the reviewed deletion plan.")
    delete.add_argument(
        "--accept-unverified-legacy-references",
        action="store_true",
        help="Acknowledge the risk of breaking unanchored legacy references for this exact plan.",
    )
    delete.set_defaults(service_handler=run_explicit_delete)
    supersession = actions.add_parser(
        "supersession", help="Inspect a superseded registry publication."
    )
    reference = supersession.add_mutually_exclusive_group(required=True)
    reference.add_argument(
        "publication_id", type=positive_id, nargs="?", help="Original registry publication ID."
    )
    reference.add_argument(
        "--output-ref", type=output_reference, help="Deleted indexed conversion reference."
    )
    add_database_options(supersession)
    supersession.set_defaults(service_handler=run_supersession)
    timeline = actions.add_parser("timeline", help="Inspect the publications behind a timeline.")
    timeline_show = timeline.add_subparsers(dest="timeline_action", required=True).add_parser(
        "show", help="Resolve an exact timeline reference."
    )
    timeline_show.add_argument(
        "timeline_reference",
        metavar="REFERENCE",
        help="Versioned reference from the timeline view.",
    )
    timeline_show.add_argument(
        "--precise", action="store_true", help="Display full nanosecond UTC timestamps."
    )
    add_database_options(timeline_show)
    timeline_show.set_defaults(service_handler=run)
    descriptions = {
        "list": "Browse historical timelines, publications or primary files.",
        "find": "Find processed outputs by market identity or reference.",
        "coverage": "Group observed bounds by stage, instrument and data kind.",
        "show": "Inspect a primary file, registry publication or indexed conversion.",
        "streams": "Inspect source membership.",
        "lineage": "Inspect selected inputs and recorded contributions.",
        "verify": "Verify the selected file or indexed conversion against retained fingerprints.",
        "schema": "Read a stored Arrow schema.",
        "partitions": "Inspect physical partitions, files and Parquet row groups.",
        "head": "Preview rows from a selected Parquet file.",
    }
    for name in (
        "list",
        "find",
        "show",
        "streams",
        "lineage",
        "coverage",
        "verify",
        "schema",
        "partitions",
        "head",
        "migrate-registration",
        "refresh-summary",
    ):
        command = actions.add_parser(
            name,
            allow_abbrev=False,
            help=descriptions.get(name),
            description=descriptions.get(name),
        )
        add_database_options(command)
        command.set_defaults(service_handler=run)
        if name not in SEARCH_ACTIONS | {"migrate-registration"}:
            if name in descriptions:
                references = command.add_mutually_exclusive_group(required=True)
                references.add_argument(
                    "id", type=int, nargs="?", help="Registered primary file ID."
                )
                references.add_argument(
                    "--output-ref",
                    type=output_reference,
                    help="Exact indexed conversion work reference.",
                )
                if name in {"show", "streams", "partitions", "head", "schema"}:
                    references.add_argument(
                        "--publication-id", type=positive_id, help="Registry publication ID."
                    )
                if name == "show":
                    command.add_argument(
                        "--wide", action="store_true", help="Expand retained file details."
                    )
                if name in {"head", "schema", "verify"}:
                    command.add_argument(
                        "--part", type=positive_id, help="One-based file number for --output-ref."
                    )
            else:
                command.add_argument("id", type=int, help="Registered primary file ID.")
        if name in {"list", "find"}:
            command.add_argument(
                "--wide",
                action="store_true",
                help=(
                    "Print each timeline inspection command or show physical-file paths."
                    if name == "list"
                    else "Show paths in the physical-file view."
                ),
            )
            command.add_argument(
                "--precise", action="store_true", help="Display full nanosecond UTC timestamps."
            )
        if name in SEARCH_ACTIONS:
            command.add_argument(
                "--operation",
                choices=("normalize", "merge", "convert"),
                help="Filter by processing stage.",
            )
            command.add_argument("--data-type", choices=DATA_TYPES)
            for flag in (
                "status",
                "exchange",
                "instrument",
                "instrument-type",
                "market-category",
            ):
                command.add_argument(f"--{flag}")
            references = command.add_mutually_exclusive_group()
            references.add_argument(
                "--publication",
                help=(
                    "Exact Engine publication ID. Timelines retain all contributors."
                    if name == "list"
                    else "Legacy exact Engine publication ID."
                ),
            )
            references.add_argument(
                "--engine-publication-id",
                dest="engine_publication",
                help="Exact Engine publication ID for registered outputs.",
            )
            references.add_argument(
                "--publication-id",
                type=positive_id,
                help=(
                    "Select timelines containing this registry publication and retain their other contributors."
                    if name == "list"
                    else "Exact registry publication ID."
                ),
            )
            references.add_argument(
                "--output-ref", type=output_reference, help="Exact indexed conversion reference."
            )
            views = {
                "list": ("timelines", "publications", "files"),
                "find": ("publications", "files"),
                "coverage": ("coverage", "files"),
            }[name]
            default_view = views[0]
            command.add_argument(
                "--view",
                choices=views,
                help=f"Human default: {default_view}. JSON without --view keeps the legacy file page.",
            )
            command.add_argument(
                "--target-quote", type=str.upper, help="Converted output target quote."
            )
            command.add_argument(
                "--search", help="Match a symbol, exchange, stream or output reference."
            )
            add_sorting(
                command,
                {view: VIEWS[view] for view in views},
                notes="time and observed mean observed start; end means observed end. Sorts apply globally within the selected view.",
            )
            command.add_argument("--start", type=timestamp)
            command.add_argument("--end", type=timestamp)
            command.add_argument("--limit", type=bounded, default=50)
            command.add_argument("--offset", type=int, default=0)
        if name == "streams":
            add_sorting(
                command, STREAMS, notes="time means the first observed timestamp for the stream."
            )
        if name in {"partitions", "schema", "head"}:
            command.add_argument("--partition", help="Physical locator from datasets partitions.")
        if name in {"schema", "head"}:
            command.add_argument(
                "--row-group",
                type=int,
                help="Zero-based row group within the selected partition file.",
            )
            command.add_argument(
                "--partition-file",
                type=positive_id,
                default=1,
                help="One-based file within the partition (default: 1).",
            )
        if name == "partitions":
            command.add_argument(
                "--row-groups",
                action="store_true",
                help="List footer groups for the selected partition.",
            )
            command.add_argument("--limit", type=bounded, default=50)
            command.add_argument("--offset", type=int, default=0)
        if name == "head":
            command.add_argument("--data-type", choices=("trades", "depth"))
            command.add_argument("--instrument-id", type=positive_id)
            command.add_argument("--stream-id")
            command.add_argument("--start", type=timestamp)
            command.add_argument("--end", type=timestamp)
            command.add_argument("--limit", type=bounded, default=10)
            command.add_argument(
                "--column",
                action="append",
                help="Repeat to select several stored columns.",
            )
            command.add_argument(
                "--wide", action="store_true", help="Show all stored columns by default."
            )
            command.add_argument(
                "--precise", action="store_true", help="Show exact nanosecond UTC timestamps."
            )
        if name == "lineage":
            command.add_argument(
                "--publication", action="store_true", help="Expand publication-level selection."
            )
            command.add_argument(
                "--checksums", action="store_true", help="Show retained raw input checksums."
            )
            command.add_argument(
                "--limit", type=bounded, default=25, help="Maximum rows per lineage section."
            )
        if name == "migrate-registration":
            command.add_argument("--all", action="store_true", required=True)
            command.add_argument("--dry-run", action="store_true")


def request_from_args(args: argparse.Namespace) -> DatasetRequest:
    return DatasetRequest(
        **{
            key: value
            for key, value in vars(args).items()
            if key in DatasetRequest.__dataclass_fields__
        }
    )


def run(args: argparse.Namespace, context: ApplicationContext) -> int:
    try:
        request = request_from_args(args)
        if request.publication_id is not None and request.publication_id <= 0:
            raise MarketForgeError("Use a positive registry publication ID.")
        if request.part is not None and request.output_ref is None:
            raise MarketForgeError("--part requires --output-ref.")
        if request.dataset_action in SEARCH_ACTIONS and request.view is None:
            if not args.as_json:
                request = replace(
                    request,
                    view={"coverage": "coverage", "list": "timelines", "find": "publications"}[
                        request.dataset_action
                    ],
                )
            elif (
                request.operation == "convert"
                or request.output_ref
                or request.publication_id is not None
                or request.engine_publication
                or request.target_quote
                or request.search
            ):
                raise MarketForgeError("Specify --view to use the new dataset filters in JSON.")
        result = asyncio.run(browse(context.paths, request, detailed=not args.as_json))
    except (ImportError, KeyError, OSError, ValueError) as error:
        raise MarketForgeError(str(error)) from error
    if args.as_json:
        print(json.dumps(result, default=str))
        return int(bool(result.get("conflict_count")))
    console = context.console
    if console.color_system is not None and (not console.file.isatty() or console.is_dumb_terminal):
        # Dataset output can be redirected independently of interactive color preferences.
        settings = context.settings.presentation.model_copy(update={"color": "never"})
        console = create_console(settings, stream=cast(TextIO, console.file))
    if request.dataset_action == "timeline":
        render_timeline(console, result, precise=request.precise)
    elif request.view == "timelines":
        render_timelines(console, result, request)
    elif request.dataset_action in SEARCH_ACTIONS:
        render_browse(console, result, request)
    elif request.dataset_action in {
        "show",
        "streams",
        "lineage",
        "schema",
        "head",
        "verify",
        "partitions",
    }:
        render_detail(console, request, result, environment=database_purpose())
    else:
        console.print_json(json.dumps(result, default=str))
    return int(bool(result.get("conflict_count")))


def run_deduplication(args: argparse.Namespace, context: ApplicationContext) -> int:
    if args.accept_unverified_legacy_references and not (
        getattr(args, "testing", False) and args.force and args.apply and args.plan
    ):
        raise MarketForgeError(
            "--accept-unverified-legacy-references requires --testing --force --apply --plan."
        )
    if args.force and not getattr(args, "testing", False):
        raise MarketForgeError("--force requires an explicit --testing selection.")
    if args.force and args.apply and not args.plan:
        raise MarketForgeError("Force apply requires --plan with a saved force preview.")
    if args.plan and not args.apply:
        raise MarketForgeError("--plan requires --apply.")
    with WorkspaceOwnership(context.paths) if args.apply else nullcontext():
        if args.apply:
            preview = deduplication.load_preview(context.paths, args.plan) if args.plan else None
            if args.force:
                assert preview is not None
                result = asyncio.run(
                    force_cleanup.apply(
                        context.paths,
                        args.operation,
                        preview,
                        accept_unverified_legacy_references=args.accept_unverified_legacy_references,
                    )
                )
            else:
                if preview and preview.get("force"):
                    raise MarketForgeError("A force preview requires --force when applying.")
                result = asyncio.run(
                    deduplication.apply_inventory(context.paths, args.operation, preview=preview)
                )
        else:
            result = asyncio.run(
                force_cleanup.preview(context.paths, args.operation)
                if args.force
                else deduplication.inventory(context.paths, args.operation, apply=False)
            )
            deduplication.save_preview(context.paths, result)
    if args.as_json:
        print(json.dumps(result))
        return int(result["status"] in {"partial", "blocked"})
    console = context.console
    header(
        console,
        "Force Dataset Cleanup" if args.force else "Dataset Deduplication",
        environment=result["environment"],
    )
    groups = result["groups"]
    counts = Counter(group["classification"] for group in groups)
    eligible = [
        target["review"] for group in groups for target in group["targets"] if target["review"]
    ]
    protected = sum(bool(target["blockers"]) for group in groups for target in group["targets"])
    summary(
        console,
        (
            ("Mode", ("Force " if args.force else "") + result["mode"].title()),
            ("Status", result["status"].upper()),
            ("Candidate groups", f"{len(groups):,}"),
            ("Proven equivalent", f"{counts['proven_equivalent']:,}"),
            (
                "Compatibility unproven",
                f"{counts['physically_identical_but_semantics_unproven']:,}",
            ),
            ("Insufficient evidence", f"{counts['insufficient_historical_evidence']:,}"),
            ("Protected publications", f"{protected:,}"),
            ("Unproven conversion outputs", f"{len(result['unproven_conversion_paths']):,}"),
            ("Eligible physical publications", f"{len(eligible):,}"),
            ("Eligible physical files", f"{sum(len(item['files']) for item in eligible):,}"),
            ("Reviewed allocation", binary_size(sum(item["physical_bytes"] for item in eligible))),
            ("Omitted from automatic selection", f"{len(result['omitted_from_selection']):,}"),
            ("Publications retired", f"{len(result['retired_publications']):,}"),
            ("Files physically removed", f"{result['physical_files_removed']:,}"),
            ("Removed file allocation", binary_size(result["removed_file_allocation_bytes"])),
            ("Report", result["report_path"]),
        ),
    )
    if args.force:
        import shlex

        plans = [item for group in groups for item in group["targets"]]
        edges = {
            (edge["parent"], edge["child"], edge["input_file_id"], edge["consumer_file_id"])
            for item in plans
            for edge in item["dependency_graph"]["edges"]
        }
        dependencies = {
            node["publication_id"]
            for item in plans
            for node in item["dependency_graph"]["dependents"]
        }
        direct = {
            edge["child"]
            for item in plans
            for edge in item["dependency_graph"]["edges"]
            if edge["parent"] == item["publication_id"] and edge["child"] is not None
        }
        optimized = result["canonical_optimization"]
        ownership_counts = Counter(item["ownership"]["state"] for item in plans)
        proposed = {
            node["publication_id"]
            for item in plans
            if item["review"]
            for node in item["dependency_graph"]["dependents"]
        }
        summary(
            console,
            (
                ("Redundant publications reviewed", str(len(plans))),
                ("Dependency edges", str(len(edges))),
                ("Affected downstream publications", str(len(dependencies))),
                ("Direct downstream publications", str(len(direct))),
                ("Additional transitive publications", str(len(dependencies - direct))),
                ("Downstream invalidation eligible", str(len(proposed))),
                ("Downstream invalidated", str(len(result["invalidated_downstream"]))),
                ("Primary files removed", str(result["primary_files_removed"])),
                ("Publication directories removed", str(result["publication_directories_removed"])),
                ("Ownership-blocked targets", str(len(set(result["ownership_blocked_targets"])))),
                ("Foreign paths verified disjoint", str(ownership_counts["verified_disjoint"])),
                ("Known foreign references", str(ownership_counts["known_foreign_reference"])),
                ("Possible foreign overlaps", str(ownership_counts["possible_foreign_overlap"])),
                (
                    "Unresolved legacy references",
                    str(ownership_counts["unresolved_legacy_references"]),
                ),
                ("Require legacy risk acknowledgement", str(len(result["legacy_risk_targets"]))),
                ("Projected downstream preserved · previous", str(optimized["baseline_preserved"])),
                (
                    "Projected downstream preserved · optimized",
                    str(optimized["optimized_preserved"]),
                ),
                ("Projected downstream affected", str(optimized["optimized_affected"])),
            ),
        )
        if not args.apply:
            location = shlex.quote(result["report_path"])
            console.print(
                f"mf datasets deduplicate --testing --operation {args.operation} --force --apply --plan {location}",
                markup=False,
            )
    if groups:
        table = Table(box=None, header_style="mf.heading")
        for label in ("Stage", "Registry publications", "Canonical", "Decision"):
            table.add_column(label)
        ordered = sorted(groups, key=lambda group: group["classification"] == "proven_equivalent")
        from rich.text import Text

        for group in ordered[:12]:
            ids = ", ".join(str(item["publication_id"]) for item in group["candidates"])
            decision = group["classification"].replace("_", " ")
            if group["targets"] and all(item["blockers"] for item in group["targets"]):
                decision = "Equivalent · physical copies protected"
            if args.force and (choice := group.get("canonical_decision")):
                decision += f" · {choice['downstream_preserved']} kept / {choice['downstream_affected']} affected"
            table.add_row(
                group["operation"].title(),
                Text(ids),
                str(group["canonical_publication_id"] or "—"),
                decision,
            )
        console.print(table)
        if len(groups) > 12:
            console.print(f"{len(groups) - 12:,} more groups in the retained report.", markup=False)
    blockers = Counter(
        reason for group in groups for target in group["targets"] for reason in target["blockers"]
    )
    if blockers:
        console.print("Physical cleanup blockers", style="mf.heading", markup=False)
        for reason, count in blockers.most_common(4):
            console.print(f"  {count:,} · {reason}", markup=False)
    if result.get("foreign_ownership_evidence", {}).get("schema_version"):
        legacy = result["foreign_ownership_evidence"]
        console.print(
            f"Legacy production catalogue v{legacy['schema_version']} · "
            f"{legacy['unanchored_references']} unanchored paths",
            style="mf.warning",
            markup=False,
        )
    elif result["other_registry_error"]:
        console.print(result["other_registry_error"], style="mf.warning", markup=False)
    if result.get("legacy_risk_targets"):
        console.print(
            "Applying the legacy-risk acknowledgement could break historical production references.",
            style="mf.warning",
            markup=False,
        )
        if args.force and not args.apply:
            console.print(
                "To acknowledge that risk for this saved plan, append "
                "--accept-unverified-legacy-references to the apply command.",
                markup=False,
            )
    actions = result["applied_actions"]
    if actions:
        flag = "--testing" if result["environment"] == "testing" else "--production"
        console.print(
            f"Inspect  mf datasets supersession {actions[0]['publication_id']} {flag} | cpout",
            markup=False,
        )
    return int(result["status"] in {"partial", "blocked"})


def run_explicit_delete(args: argparse.Namespace, context: ApplicationContext) -> int:
    if not getattr(args, "testing", False):
        raise MarketForgeError("Publication deletion requires explicit --testing selection.")
    if args.apply and not args.plan:
        raise MarketForgeError("Publication deletion apply requires --plan.")
    if args.apply and (
        args.publication_id or args.output_ref or args.retain_publication_id or args.cascade
    ):
        raise MarketForgeError(
            "Apply takes its exact targets, survivors and cascade policy from the reviewed plan."
        )
    if not args.apply and (args.plan or args.accept_unverified_legacy_references):
        raise MarketForgeError("A saved plan and legacy risk acknowledgement require --apply.")
    if args.apply:
        with WorkspaceOwnership(context.paths):
            reviewed = deduplication.load_preview(context.paths, args.plan)
            report = asyncio.run(
                explicit_delete.apply(
                    context.paths,
                    reviewed,
                    accept_legacy_risk=args.accept_unverified_legacy_references,
                )
            )
    else:
        report = asyncio.run(
            explicit_delete.preview(
                context.paths,
                args.publication_id,
                args.output_ref,
                args.retain_publication_id,
                cascade=args.cascade,
            )
        )
        deduplication.save_preview(context.paths, report)
    if args.as_json:
        print(json.dumps(report))
    else:
        console = context.console
        header(console, "Delete Publications", environment="testing")
        summary(
            console,
            (
                ("Mode", report["mode"].title()),
                ("Status", report["status"].upper()),
                ("Cascade reviewed", "Yes" if report["cascade"] else "No"),
                ("Plan" if not args.apply else "Report", report["report_path"]),
            ),
        )
        console.print(
            "Explicit deletion leaves historical identities intact and establishes no equivalence.",
            markup=False,
        )
        table = Table(box=None, header_style="mf.heading")
        for label in ("Publication or conversion reference", "Stage", "Content", "Action", "Files"):
            table.add_column(label)
        for item, action in [
            *((row, "Keep") for row in report["retained"]),
            *((plan["target"], "Selected") for plan in report["targets"]),
        ]:
            table.add_row(
                str(item.get("output_ref") or item["publication_id"]),
                item["operation"].title(),
                str(
                    item.get("entry", item.get("reuse_result", {}).get("entry", {})).get(
                        "content_type", "—"
                    )
                ),
                action,
                str(len(item["files"])),
            )
        console.print(table)
        impacted = {
            row["publication_id"]
            for plan in report["targets"]
            for row in plan["dependency_graph"]["dependents"]
        }
        blockers = Counter(reason for plan in report["targets"] for reason in plan["blockers"])
        summary(
            console,
            (
                ("Dependent publications", str(len(impacted))),
                (
                    "Eligible without acknowledgement",
                    str(sum(bool(plan["review"]) for plan in report["targets"])),
                ),
                ("Require legacy risk acknowledgement", str(len(report["legacy_risk_targets"]))),
                ("Directories removed", str(report.get("publication_directories_removed", 0))),
                ("Primary files removed", str(report.get("primary_files_removed", 0))),
            ),
        )
        if report["legacy_risk_targets"]:
            allocation = sum(
                plan["legacy_risk_review"]["physical_bytes"]
                for plan in report["targets"]
                if plan.get("legacy_risk_review")
            )
            summary(console, (("Allocation reviewed with legacy risk", binary_size(allocation)),))
            console.print(
                "The legacy-risk acknowledgement could break unresolved historical production references.",
                markup=False,
                style="mf.warning",
            )
        for reason, count in blockers.items():
            console.print(f"{count} · {reason}", markup=False)
        for blocked in report.get("blocked_actions", []):
            console.print(f"{blocked['target']} · {' · '.join(blocked['reasons'])}", markup=False)
    return int(args.apply and report["status"] != "complete")


def run_supersession(args: argparse.Namespace, context: ApplicationContext) -> int:
    record = asyncio.run(
        explicit_delete.inspect_conversion(context.paths, args.output_ref)
        if args.output_ref
        else deduplication.inspect_supersession(context.paths, args.publication_id)
    )
    if args.as_json:
        print(json.dumps(record))
    else:
        header(
            context.console, "Publication History", environment=record.get("environment", "testing")
        )
        if args.output_ref:
            summary(
                context.console,
                (
                    ("Original conversion reference", record["output_ref"]),
                    ("State", record["state"]),
                    ("Reason", record["evidence"]["reason"]),
                    ("Cleanup ID", record["cleanup_id"]),
                ),
            )
            return 0
        summary(
            context.console,
            (
                ("Original registry publication", str(record["publication_id"])),
                (
                    "Source retired by cleanup"
                    if record["state"] == "invalidated"
                    else "Canonical registry publication",
                    str(record.get("source_publication_id", record["canonical_id"]) or "—"),
                ),
                ("State", record["state"]),
                (
                    "Reason",
                    "Upstream testing publication retired"
                    if record["state"] == "invalidated"
                    else record["evidence"].get("reason", "Verified equivalent historical work"),
                ),
                ("Cleanup ID", record["cleanup_id"]),
            ),
        )
        for blocker in record["evidence"].get("blockers", []):
            context.console.print(blocker, markup=False)
    return 0
