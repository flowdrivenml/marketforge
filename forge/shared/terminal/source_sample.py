"""Render bounded source evidence with the profile terminal theme."""

from rich.console import Console
from rich.text import Text

from forge.historical.profiles.source_sample import SourceSample, safe_text
from forge.shared.terminal.profiles import entries, grid, render_documentation, section

FORMAT = {
    "csv": "CSV",
    "jsonl": "JSONL",
    "unsupported": "Undetermined",
    "requires_member": "Undetermined",
}
CONTAINER = {"plain": "Plain", "zip": "ZIP", "gzip": "Gzip", "tar_gzip": "TAR.GZ"}


def compact(text: str, limit: int) -> str:
    return text if len(text) <= limit else f"{text[: limit - 1]}…"


def render_source_sample(console: Console, sample: SourceSample, *, fields_only: bool) -> None:
    console.print("MarketForge · Source Sample", style="mf.heading")
    is_csv = sample.layout == "csv"
    metadata = [
        ("File", safe_text(sample.filename)),
        ("Format", FORMAT[sample.layout]),
        ("Container", CONTAINER[sample.container]),
    ]
    if sample.member is not None:
        metadata.append(("Member", safe_text(sample.member)))
    if is_csv:
        header = (
            "Unknown (first row shown as data)"
            if sample.header is None
            else "Yes"
            if sample.header
            else "No"
        )
        metadata.extend((("Header", header), ("Columns", str(sample.columns))))
    elif sample.layout == "jsonl":
        metadata.append(("Fields shown", str(len(sample.fields))))
    if sample.layout in {"jsonl", "csv"}:
        count = len(sample.preview)
        unit = "row" if is_csv else "record"
        metadata.append(("Sampled", f"{count} {unit}{'' if count == 1 else 's'}"))
    entries(console, iter(metadata))

    if sample.reason:
        console.print(Text(sample.reason, style="mf.warning"))
    if sample.members:
        console.print(Text("Members: " + ", ".join(safe_text(name) for name in sample.members)))
    for warning in sample.warnings:
        console.print(Text(warning, style="mf.warning"))

    if sample.fields:
        section(console, "Source structure")
        headings = ("Column", "Pos.") if is_csv else ("Field",)
        table = grid(
            *headings,
            "Suggested type",
            "Observed values",
            "Missing",
            *(() if is_csv else ("Null",)),
        )
        table.columns[0].style = "mf.section"
        for column in table.columns:
            column.overflow = "fold"
        example_limit = max(18, console.width // 2 - 10)
        for field in sample.fields:
            name = safe_text(field.name) if field.name.strip() else "(empty header)"
            index = (str(field.position),) if is_csv else ()
            observed = compact(", ".join(field.examples), example_limit) if field.examples else "—"
            missing = (str(field.missing),) if is_csv else (str(field.missing), str(field.nulls))
            table.add_row(
                Text(compact(name, 72)), *index, field.representation, Text(observed), *missing
            )
        console.print(table)
        if not fields_only and sample.preview:
            render_preview(console, sample, is_csv)
        note = (
            "Use one-based CSV positions. Confirm timestamp encoding, quantity units and side meaning against the source."
            if is_csv
            else 'Quote dotted paths as TOML keys, for example [fields."data.ts"]. Confirm timestamp encoding, quantity units and event meaning against the source.'
        )
        console.print(Text(note, style="mf.secondary"))
    render_documentation(console)


def render_preview(console: Console, sample: SourceSample, is_csv: bool) -> None:
    section(console, "Data preview")
    names = [
        compact(safe_text(field.name), 72) if field.name.strip() else f"(empty #{field.position})"
        for field in sample.fields
    ]
    widths = [
        max(len(name), *(len(row[index] or "—") for row in sample.preview))
        for index, name in enumerate(names)
    ]
    if is_csv and len(names) <= 8 and sum(widths) + 2 * len(names) + 3 <= console.width:
        table = grid("#", *names)
        for number, row in enumerate(sample.preview, 1):
            table.add_row(
                str(number), *(Text(value if value is not None else "—") for value in row)
            )
        console.print(table)
    else:
        limit = 12
        if len(names) > limit:
            console.print(f"Showing the first {limit} fields per record.", style="mf.secondary")
        for number, row in enumerate(sample.preview, 1):
            console.print(f"#{number}", style="mf.section")
            entries(
                console,
                (
                    (
                        f"{index}. {name}" if is_csv else name,
                        compact(value, 120) if value is not None else "—",
                    )
                    for index, (name, value) in enumerate(
                        zip(names[:limit], row[:limit], strict=True), 1
                    )
                ),
            )
