"""Terminal rendering for the processed dataset inventory."""

import shlex
from pathlib import PurePosixPath
from typing import Any

from rich.console import Console
from rich.text import Text

from forge.datasets.query import DatasetRequest
from forge.shared.presentation.datasets import (
    KINDS,
    STAGES,
    count,
    file_summary,
    integrity,
    period,
    scope,
)
from forge.shared.terminal.datasets import render_inspection
from forge.shared.terminal.presentation import data_table, header, section, summary
from forge.shared.terminal.storage import size

Row = dict[str, Any]


def next_page(request: DatasetRequest, result: Row) -> str:
    options = [
        ("view", result["view"]),
        ("operation", request.operation),
        ("status", request.status),
        ("data-type", request.data_type),
        ("exchange", request.exchange),
        ("instrument", request.instrument),
        ("instrument-type", request.instrument_type),
        ("market-category", request.market_category),
        ("publication", request.publication),
        ("publication-id", request.publication_id),
        ("engine-publication-id", request.engine_publication),
        ("output-ref", request.output_ref),
        ("target-quote", request.target_quote),
        ("search", request.search),
        ("start", request.start),
        ("end", request.end),
        ("sort", request.sort if request.dataset_action != "coverage" else None),
        ("limit", request.limit),
        ("offset", request.offset + request.limit),
    ]
    command = ["mf", "datasets", request.dataset_action, f"--{result['environment']}"]
    for name, value in options:
        if value is not None:
            command.extend((f"--{name}", str(value)))
    for name in ("wide", "precise"):
        if getattr(request, name):
            command.append(f"--{name}")
    return shlex.join(command)


def reference(row: Row, view: str) -> str:
    if row.get("output_ref") or row.get("variant"):
        return str(row.get("output_ref") or row["variant"])
    if view == "publications":
        return str(row["publication_id"])
    return str(row.get("id") or "—")


def row_fields(row: Row, view: str, *, precise: bool = False) -> tuple[str, ...]:
    saved = file_summary(row)
    members: list[Row] = row.get("memberships") or saved.get("memberships") or []
    if view == "coverage":
        members = [row]
    identity, market = scope(members)
    if row.get("unresolved_namespace_output"):
        identity += f" · namespace unresolved (publication {row['unresolved_namespace_output']})"
    if not identity.startswith("Unknown") and row.get("units_without_membership"):
        identity += " · partial membership"
    if row.get("target_quote"):
        identity += f" → {row['target_quote']}"
    kind = KINDS.get(str(row.get("data_type")), "Unknown")
    if view == "coverage":
        metrics = (
            f"{count(row['supporting_files'])} registry {'file' if row['supporting_files'] == 1 else 'files'}"
            if row["operation"] != "convert"
            else f"{count(row['supporting_outputs'])} indexed {'output' if row['supporting_outputs'] == 1 else 'outputs'}"
        )
        state = ""
    else:
        byte_size = size(row.get("size_bytes", saved.get("size_bytes")))
        quantity = (
            f"{count(row.get('file_count'))} {'file' if row.get('file_count') == 1 else 'files'}"
            if view == "publications"
            else f"{count(row.get('rows', saved.get('rows')))} rows"
        )
        metrics = f"{quantity} · {byte_size}"
        state = f"{str(row.get('status') or 'unknown').title()} / {integrity(row.get('integrity', saved.get('integrity')))}"
    return (
        STAGES.get(row["operation"], row["operation"]),
        identity,
        market,
        kind,
        period(row, precise=precise),
        metrics,
        state,
    )


def render_rows(console: Console, rows: list[Row], view: str, request: DatasetRequest) -> None:
    narrow = console.width < 120 or request.precise
    table = data_table()
    ref_title = {"publications": "Publication ID", "files": "File ID", "coverage": "Variant"}[view]
    titles = (
        ("Stage", ref_title, "Scope / observed period (UTC)", "Data")
        if narrow
        else (
            "Stage",
            ref_title,
            "Scope",
            "Market",
            "Data",
            "Observed UTC",
            "Recorded",
            "State / integrity",
        )
    )
    for title in titles:
        table.add_column(
            title,
            no_wrap=title in {"Stage", ref_title, "Data", "Observed UTC"},
            min_width=19 if title == "Observed UTC" else None,
        )
    indexed: dict[str, int] = {}
    for row in rows:
        stage, identity, market, kind, observed, metrics, state = row_fields(
            row,
            view,
            precise=request.precise,
        )
        ref = reference(row, view)
        if row["operation"] == "convert":
            marker = indexed.setdefault(ref, len(indexed) + 1)
            part = row.get("part")
            ref = f"Key {marker} / {part}" if part else f"See key {marker}"
        match = row.get("matching_memberships")
        full = row.get("memberships") or file_summary(row).get("memberships")
        if match and full and scope(match) != scope(full):
            identity += f"\nMatched {scope(match)[0]}"
        requested_kind = {"trades": "trade", "depth": "l2", "order_book_l2": "l2"}.get(
            request.data_type or "", request.data_type
        )
        if (
            view != "coverage"
            and match
            and row.get("data_type") == "trade_l2"
            and requested_kind in {"trade", "l2"}
        ):
            kind += f"\nMatched {KINDS[requested_kind]}"
        if view == "publications" and row["matching_file_count"] != row["file_count"]:
            metrics += f" · {row['matching_file_count']} matching"
        bounds = observed.replace(" → ", "\n")
        if narrow:
            details = " · ".join(filter(None, (metrics, state)))
            facts = f"{identity} · {market}\n{bounds if request.precise else observed}\n{details}"
            cells = (stage, ref, facts, kind)
        else:
            cells = (stage, ref, identity, market, kind, bounds, metrics, state)
        styles = ("mf.heading", "mf.dataset_id", "mf.body", "mf.identifier_detail")
        table.add_row(
            *(
                Text(cell, styles[index] if index < len(styles) else "mf.body")
                for index, cell in enumerate(cells)
            )
        )
    if rows:
        console.print(table)
    for ref, index in indexed.items():
        console.print(Text(f"Key {index} · Indexed output reference", style="mf.secondary"))
        console.print(Text(ref, style="mf.identifier"))
    if request.wide and view == "files":
        for row in rows:
            if row.get("storage_path"):
                console.print(Text(f"{reference(row, view)} · {row['storage_path']}"))


def render_browse(console: Console, result: Row, request: DatasetRequest) -> None:
    header(console, "Datasets", environment=result["environment"])
    view = result["view"]
    units = {"publications": "publications", "files": "files", "coverage": "coverage groups"}
    unit = units[view]
    stages = result["stages"]
    tally_unit = "files" if view == "files" else "publications"
    totals: list[str] = []
    for stage, label in STAGES.items():
        if request.operation and stage != request.operation:
            continue
        if stage == "convert" and result["conversion_index"] == "outside_filter":
            continue
        if stage == "convert" and result["conversion_index"] == "unavailable":
            value = "index unavailable"
        else:
            value = count(stages.get(stage, {}).get(tally_unit, 0))
            if stage == "convert" and result["unavailable_conversion_outputs"]:
                value += f" available · {len(result['unavailable_conversion_outputs'])} unavailable"
        totals.append(f"{label} {value}")
    console.print(Text(f"Matching {tally_unit} · " + "  |  ".join(totals)))
    filters = [
        (name, value)
        for name, value in (
            ("stage", request.operation),
            ("exchange", request.exchange),
            ("instrument", request.instrument),
            ("market", request.market_category),
            ("instrument type", request.instrument_type),
            ("data", request.data_type),
            ("state", request.status),
            ("target quote", request.target_quote),
            ("publication ID", request.publication_id),
            ("Engine publication ID", request.engine_publication or request.publication),
            ("indexed output", request.output_ref),
            ("search", request.search),
            ("start (ns)", request.start),
            ("end (ns)", request.end),
        )
        if value is not None
    ]
    if filters:
        console.print(Text("Filters · " + " · ".join(f"{name} {value}" for name, value in filters)))
    console.print()
    rows = result[
        {"publications": "publications", "files": "datasets", "coverage": "coverage"}[view]
    ]
    render_rows(console, rows, view, request)
    label = unit.removesuffix("s") if len(rows) == 1 else unit
    console.print(
        Text(
            f"{len(rows):,} {label} shown of {result['total']:,} matching · offset {result['offset']:,}"
        )
    )
    if view in {"publications", "coverage"}:
        console.print(Text("Observed bounds do not establish continuity.", style="mf.secondary"))
    if any(row["operation"] == "convert" for row in rows):
        console.print(
            Text("Conversion membership describes its source publication.", style="mf.secondary")
        )
    if result["has_more"]:
        console.print(
            Text(f"Next · {next_page(request, result)}", style="mf.identifier"), soft_wrap=True
        )
    elif view == "publications" and rows:
        selector = (
            f"--output-ref {rows[0]['output_ref']}"
            if rows[0].get("output_ref")
            else f"--publication-id {rows[0]['publication_id']}"
        )
        console.print(
            Text(
                f"Inspect · mf datasets show --{result['environment']} {selector}",
                style="mf.identifier",
            )
        )


def render_detail(
    console: Console, request: DatasetRequest, result: Row, *, environment: str
) -> None:
    header(console, "Datasets", environment=environment)
    action = request.dataset_action
    if "legacy_dataset_id" in result:
        render_inspection(console, action, result, show_header=False)
        return
    item: Row = result.get("output") or result.get("file") or result.get("publication") or {}
    if item:
        identity, market = scope(
            item.get("memberships") or file_summary(item).get("memberships") or []
        )
        facts = [
            ("Stage", STAGES.get(str(item.get("operation")), "Unknown")),
            ("Scope", identity),
            ("Market", market),
            ("Data", KINDS.get(str(item.get("data_type")), "Unknown")),
        ]
        if request.output_ref:
            console.print(Text("Indexed output reference", style="mf.identifier"))
            console.print(Text(request.output_ref, style="mf.identifier"), soft_wrap=True)
            console.print()
            facts.append(("Target quote", str(item.get("target_quote") or "Unknown")))
            if request.part:
                facts.append(("File part", str(request.part)))
        elif request.publication_id is not None:
            facts.insert(1, ("Registry publication ID", str(request.publication_id)))
        elif request.id is not None:
            facts.insert(1, ("File ID", str(request.id)))
        summary(console, facts)
    if item and action == "show" and request.id is not None:
        saved, publication = file_summary(item), result["publication"]
        facts = [
            ("State", str(item["status"]).title()),
            ("Integrity", integrity(saved.get("integrity"))),
            ("Rows", count(saved.get("rows"))),
            ("Size", size(saved.get("size_bytes"))),
            ("Observed period (UTC)", period(item, precise=True)),
            ("Registry publication ID", str(publication["publication_id"])),
            ("Engine publication ID", str(publication["engine_dataset_id"])),
        ]
        if request.wide:
            facts += [
                ("Path", str(item.get("storage_path"))),
                ("SHA-256", str(item.get("file_sha256"))),
                ("Manifest", str(publication.get("manifest_path"))),
            ]
        summary(console, facts)
        console.print(Text(f"Streams · mf datasets streams {request.id} --{environment}"))
        return
    if action == "verify":
        section(console, "Verification")
        summary(console, (("Result", result["status"].title()), ("Checks", result["scope"])))
        return
    if action == "show" and (request.publication_id is not None or request.output_ref):
        files = result["files"]["datasets"] if "files" in result else item["files"]
        total = item.get("file_count", len(files))
        recorded_size = item.get("size_bytes")
        if request.output_ref and files and all(f.get("size_bytes") is not None for f in files):
            recorded_size = sum(f["size_bytes"] for f in files)
        facts = [
            ("Observed period (UTC)", period(item, precise=True)),
            ("Recorded files", count(total)),
            ("Recorded size", size(recorded_size)),
            ("State", str(item["status"]).title()),
            ("Integrity", integrity(item.get("integrity"))),
        ]
        if item.get("engine_dataset_id") and not request.output_ref:
            facts.append(("Engine publication ID", item["engine_dataset_id"]))
        if request.output_ref:
            facts += [
                ("Index", "Canonical work result"),
                ("Registration", "Unavailable"),
                ("Engine job ID", str(item.get("engine_job_id") or "Unknown")),
                ("Membership scope", "Source publication"),
                ("Verification", "Recorded in conversion evidence"),
            ]
        if item.get("canonical_publication_id") is not None:
            facts.append(("Supersession", str(item["supersession_state"]).title()))
            facts.append(("Canonical publication ID", str(item["canonical_publication_id"])))
        summary(console, facts)
        if item.get("canonical_publication_id") is not None:
            console.print(
                Text(
                    f"Canonical · mf datasets show --{environment} --publication-id {item['canonical_publication_id']}"
                ),
                soft_wrap=True,
            )
        section(console, "Primary files")
        table = data_table()
        for label in ("Part" if request.output_ref else "File ID", "Primary file", "Rows", "Size"):
            table.add_column(label)
        for file in files[: 50 if request.wide else 8]:
            saved = file_summary(file)
            path = (
                file["storage_path"]
                if request.wide
                else PurePosixPath(file.get("primary_file", file.get("path")) or "unknown").name
            )
            table.add_row(
                *(
                    Text(str(value))
                    for value in (
                        file.get("part", file.get("id")),
                        path,
                        count(file.get("rows", saved.get("rows"))),
                        size(file.get("size_bytes", saved.get("size_bytes"))),
                    )
                )
            )
        console.print(table)
        visible = min(len(files), 50 if request.wide else 8)
        if visible < total:
            console.print(Text(f"{visible} of {total} recorded files shown"))
        if request.publication_id is not None:
            console.print(
                Text(
                    f"Streams · mf datasets streams --{environment} --publication-id {request.publication_id}"
                ),
                soft_wrap=True,
            )
            console.print(
                Text(
                    f"Files · mf datasets list --{environment} --view files --publication-id {request.publication_id}"
                ),
                soft_wrap=True,
            )
        else:
            console.print(
                Text(
                    "Use the indexed output reference with streams, lineage, schema, head or verify."
                )
            )
        return
    if request.output_ref and action == "lineage":
        section(console, "Selected source publication")
        source = result["publication_selection"]
        summary(
            console,
            (
                ("Registry publication ID", str(source.get("registry_publication_id", "Unknown"))),
                ("Contribution scope", "Publication-level input selection"),
            ),
        )
        return
    display_result = {**result, "id": request.id or result.get("id")}
    render_inspection(console, action, display_result, show_header=not bool(item))
