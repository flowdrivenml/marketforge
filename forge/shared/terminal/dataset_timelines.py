"""Terminal views for historical segments and their exact publication references."""

from typing import Any

from rich.console import Console
from rich.text import Text

from forge.datasets.metadata import utc
from forge.datasets.query import DatasetRequest
from forge.datasets.sorting import VIEWS
from forge.shared.presentation.datasets import KINDS, STAGES, integrity, market, period
from forge.shared.terminal.dataset_browse import next_page
from forge.shared.terminal.presentation import data_table, header, section, summary

Row = dict[str, Any]
EVIDENCE = {
    "observed_extent": "Observed only",
    "source_observed_extent": "Source observed only",
    "unknown": "Unknown",
}


def inspect_command(row: Row, environment: str) -> str:
    return f"mf datasets timeline show {row['timeline_reference']} --{environment}"


def publication_command(publication: Row, environment: str) -> str:
    identity = publication["registry_publication_id"]
    if identity is not None:
        return f"mf datasets show --publication-id {identity} --{environment}"
    return f"mf datasets show --output-ref {publication['output_ref']} --{environment}"


def render_timelines(console: Console, result: Row, request: DatasetRequest) -> None:
    environment = result["environment"]
    header(console, "Datasets", environment=environment)
    console.print(
        Text(
            VIEWS[result["view"]].resolve(request.sort, request.order).description,
            style="mf.secondary",
        )
    )
    stats = result["timeline_counts"]
    console.print(
        Text(
            f"Historical timelines · {result['total']:,} segments · {stats['identities']:,} identities"
            f" · {result['matching_publications']:,} distinct outputs"
        )
    )
    stages: dict[str, int] = result["stage_publications"]
    labels = [
        f"{label} {stages.get(stage, 0):,}"
        for stage, label in STAGES.items()
        if stage != "convert" or result["conversion_index"] == "available"
    ]
    if result["conversion_index"] == "unavailable":
        labels.append("Conversion index unavailable")
    if result["unavailable_conversion_outputs"]:
        labels.append(
            f"Conversion metadata unavailable for {len(result['unavailable_conversion_outputs'])} outputs"
        )
    console.print(Text(" · ".join(labels)))
    console.print()
    rows: list[Row] = result["timelines"]
    compact = console.width < 120 or request.precise
    table = data_table()
    labels = (
        ("Stage", "Exchange / instrument", "Market / data", "Observed UTC / registry IDs")
        if compact
        else ("Stage", "Exchange", "Instrument", "Market", "Data", "Observed UTC", "Registry IDs")
    )
    for label in labels:
        table.add_column(label, no_wrap=label == "Stage", min_width=19 if "IDs" in label else None)
    indexed: list[Row] = []
    for row in rows:
        publications: list[Row] = row["publications"]
        converted = bool(row.get("variant"))
        if converted:
            indexed.append(row)
            refs = f"Indexed output {len(indexed)}"
        else:
            identities = [str(pub["registry_publication_id"]) for pub in publications]
            preview = 1 if compact or any(len(value) > 8 for value in identities[:3]) else 3
            refs = ", ".join(identities[:preview])
            if len(publications) > preview:
                refs += f"\n+{len(publications) - preview} more"
        stage = STAGES[row["operation"]]
        exchange = str(row["exchange"]).upper()
        symbol = str(row.get("symbol") or row["instrument_id"])
        if len(symbol) > 24:
            symbol = symbol[:23] + "…"
        market_label = market(row)
        kind = KINDS.get(str(row["data_type"]), "Unknown")
        if row.get("target_quote"):
            kind += f" → {row['target_quote']}"
        observed = (
            period(row, precise=request.precise).replace(" → ", "\n")
            if row["coverage"] != "unknown"
            else "Bounds unknown"
        )
        state = integrity(row["integrity"])
        if state != "Unknown" or row["status"] != "complete":
            observed += (
                f"\n{state}" if row["status"] == "complete" else f"\n{row['status']} / {state}"
            )
        if compact:
            cells = (
                stage,
                f"{exchange}\n{symbol}",
                f"{market_label}\n{kind}",
                f"{observed}\n{refs}",
            )
        else:
            cells = (stage, exchange, symbol, market_label, kind, observed, refs)
        table.add_row(*(Text(cell, "mf.heading" if i == 0 else "") for i, cell in enumerate(cells)))
    if rows:
        console.print(table)
    for index, row in enumerate(indexed, 1):
        console.print(Text(f"Indexed output {index} · exact reference"))
        console.print(Text(row["variant"], "mf.identifier"), soft_wrap=True)
    console.print(
        Text(
            f"{len(rows):,} timeline segments shown of {result['total']:,} · offset {result['offset']:,}"
        )
    )
    console.print(
        Text(
            "Bounds show observed extents. Separation between spans leaves coverage unknown.",
            "mf.secondary",
        )
    )
    if rows and (request.wide or len(rows) == 1):
        for row in rows:
            console.print(
                Text(f"Inspect timeline · {inspect_command(row, environment)}"), soft_wrap=True
            )
    elif rows:
        console.print(
            Text(
                "Timeline references · Add --wide for inspection commands or global --json for all IDs.",
                "mf.secondary",
            ),
            soft_wrap=True,
        )
    if rows:
        command = publication_command(rows[0]["publications"][0], environment)
        console.print(Text(f"Inspect output · {command}"), soft_wrap=True)
    if result["has_more"]:
        console.print(Text(f"Next · {next_page(request, result)}"), soft_wrap=True)


def render_timeline(console: Console, result: Row, *, precise: bool) -> None:
    environment = result["environment"]
    row: Row = result["timeline"]
    header(console, "Dataset Timeline", environment=environment)
    facts = [
        ("Stage", STAGES[row["operation"]]),
        ("Exchange", str(row["exchange"]).upper()),
        ("Instrument", str(row["symbol"] or row["instrument_id"])),
        ("Canonical instrument ID", str(row["instrument_id"])),
        ("Market", market(row)),
        ("Data", KINDS.get(str(row["data_type"]), "Unknown")),
        ("Observed UTC", period(row, precise=precise)),
        ("Coverage", EVIDENCE[row["coverage"]]),
        ("Contributing outputs", str(row["publication_count"])),
        ("Recorded state / integrity", f"{row['status'].title()} / {integrity(row['integrity'])}"),
    ]
    if row.get("target_quote"):
        facts.append(("Target quote", str(row["target_quote"])))
    else:
        facts.append(("Matching registry files", str(row["matching_primary_files"])))
    if row.get("stream_id"):
        facts.append(("Stream", row["stream_id"]))
    if separation := row.get("observed_separation"):
        facts.append(("Prior observed span ended", utc(separation["after_ns"])))
    publications: list[Row] = row["publications"]
    requested_intervals = {
        (interval["start_ns"], interval["end_ns"])
        for publication in publications
        if (interval := publication.get("requested_interval"))
    }
    if len(requested_intervals) == 1:
        start, end = next(iter(requested_intervals))
        facts.append(("Requested interval UTC", f"[{utc(start)}, {utc(end)})"))
    summary(console, facts)
    section(console, "Contributing publications")
    table = data_table()
    for label in ("Registry publication ID", "Observed UTC", "Matching files", "Integrity"):
        table.add_column(label)
    for publication in publications:
        table.add_row(
            Text(str(publication["registry_publication_id"] or "Indexed output")),
            Text(period(publication, precise=precise).replace(" → ", "\n")),
            Text(str(publication["matching_primary_files"] or "—")),
            Text(integrity(publication["integrity"])),
        )
    console.print(table)
    if publications:
        console.print(
            Text(f"Inspect output · {publication_command(publications[0], environment)}"),
            soft_wrap=True,
        )
