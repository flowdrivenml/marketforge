"""Compact terminal presentations for normalize profiles."""

import json
from collections import Counter
from collections.abc import Iterator, Mapping
from typing import Any, cast

from rich.console import Console
from rich.style import Style
from rich.table import Table
from rich.text import Text

from forge.historical.profiles.documentation import PROFILE_REFERENCE
from forge.historical.profiles.schema import Field, ProfileSchema, Section, load_schema
from forge.shared.terminal.palette import CYAN


def render_documentation(console: Console) -> None:
    label = "Documentation"
    link = Text(PROFILE_REFERENCE, style=Style(link=PROFILE_REFERENCE))
    if console.width >= len(label) + 2 + len(PROFILE_REFERENCE):
        console.print(Text(f"{label}  ").append_text(link))
    else:
        console.print(label)
        console.print(link)


def section(console: Console, title: str) -> None:
    console.print()
    console.print(title, style=f"bold {CYAN}")


def grid(*headings: str) -> Table:
    return Table(
        *headings,
        box=None,
        header_style="mf.secondary",
        padding=(0, 1),
        pad_edge=False,
        show_header=bool(headings),
    )


def value_text(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        return ", ".join(value_text(item) for item in cast(list[object], value)) if value else "[]"
    return str(value)


def pairs(values: Mapping[str, object], prefix: str = "") -> Iterator[tuple[str, str]]:
    for key, value in values.items():
        label = json.dumps(key, ensure_ascii=False) if "." in key else key
        path = f"{prefix}.{label}" if prefix else label
        if isinstance(value, dict) and value:
            yield from pairs(cast(dict[str, object], value), path)
        elif isinstance(value, list) and any(
            isinstance(item, dict) for item in cast(list[object], value)
        ):
            for index, item in enumerate(cast(list[object], value), start=1):
                if isinstance(item, dict):
                    yield from pairs(cast(dict[str, object], item), f"{path}[{index}]")
                else:
                    yield f"{path}[{index}]", value_text(item)
        elif isinstance(value, dict):
            yield path, "{}"
        else:
            yield path, value_text(cast(object, value))


def entries(console: Console, rows: Iterator[tuple[str, str]]) -> None:
    table = grid()
    table.add_column(style="mf.warning", max_width=max(12, console.width // 2), overflow="fold")
    table.add_column(overflow="fold")
    for key, value in rows:
        table.add_row(Text(key), Text(value))
    console.print(table)


def render_inventory(
    console: Console, profiles: list[dict[str, Any]], installed: int, filters: dict[str, str | None]
) -> None:
    console.print("MarketForge · Acquisition Profiles", style="mf.heading")
    active = ", ".join(
        f"{key.replace('_', '-')}={value}" for key, value in filters.items() if value is not None
    )
    tokens = [entry["id"].split(".") for entry in profiles]
    types = Counter(parts[-1] for parts in tokens)
    exchanges = len({parts[0] for parts in tokens})
    label = (
        f"{len(profiles)} of {installed} installed profiles"
        if active
        else f"{installed} profiles installed"
    )
    venue_label = "Exchange" if exchanges == 1 else "Exchanges"
    console.print(
        f"{label}  ·  {types['trades']} Trades  ·  {types['depth']} Depth  ·  {exchanges} {venue_label}"
    )
    if active:
        console.print(f"Filters: {active}", style="mf.secondary")
    if not profiles:
        console.print("No matching profiles.", style="mf.secondary")
        render_documentation(console)
        return
    console.print()
    compact = console.width < 76
    table = grid(
        *(
            ("Exchange", "Data", "Profile")
            if compact
            else ("Exchange", "Instrument", "Market", "Data", "Format", "Profile")
        )
    )
    table.columns[-1].style = "mf.identifier"
    table.columns[-1].overflow = "fold"
    previous = None
    for profile, (exchange, instrument, market, kind) in zip(profiles, tokens, strict=True):
        if previous is not None and previous != exchange:
            table.add_row()
        source = profile["definition"].get("source")
        physical = (
            cast(dict[str, object], source).get("format") if isinstance(source, dict) else None
        )
        representation = (
            {"csv": "CSV", "jsonl": "JSONL", "xlsx": "XLSX"}.get(physical, "—")
            if isinstance(physical, str)
            else "—"
        )
        venue = {"okx": "OKX", "gateio": "Gate.io"}.get(exchange, exchange.title())
        middle = () if compact else (instrument.title(), market.title())
        tail = () if compact else (representation,)
        table.add_row(venue, *middle, kind.title(), *tail, Text(profile["id"]))
        previous = exchange
    console.print(table)
    console.print(
        "Inspect source: mf normalize profiles inspect PROFILE_ID\nRead settings: mf normalize profiles show PROFILE_ID",
        style="mf.secondary",
    )
    console.print(
        "Explore fields: mf normalize profiles schema\nCompile: mf normalize profiles validate PROFILE_ID",
        style="mf.secondary",
    )

    render_documentation(console)


def omitted_defaults(definition: Mapping[str, Any], kind: str) -> list[tuple[str, str]]:
    defaults: list[tuple[str, str]] = []
    for field in load_schema().parameters(profile_type=kind):
        if "<" in field.path or "[" in field.path:
            continue
        if field.path == "feeds" and (kind != "depth" or definition.get("exchange") != "gateio"):
            continue
        parts = field.path.split(".")
        table: object = definition
        for key in parts[:-1]:
            table = cast(dict[str, object], table).get(key) if isinstance(table, dict) else None
        if not isinstance(table, Mapping) or parts[-1] in table:
            continue
        # Positional indices may be derived from a raw level_format. Show
        # local container defaults only, without implying effective positions.
        default = field.variants[0].default
        if default in ({}, []):
            defaults.append((field.path, value_text(default)))
    return defaults


def render_profile(console: Console, profile: dict[str, Any]) -> None:
    console.print("MarketForge · Acquisition Profile", style="mf.heading")
    definition = profile["definition"]
    header = {
        key: value
        for key, value in definition.items()
        if key not in {"source", "fields", "normalization", "feeds"}
    }
    header["data"] = profile["id"].rsplit(".", 1)[1]
    entries(console, pairs(header))
    for title, key in (
        ("Source format", "source"),
        ("Field declarations", "fields"),
        ("Normalization", "normalization"),
        ("Feed variants", "feeds"),
    ):
        if key in definition:
            section(console, title)
            entries(console, pairs({key: definition[key]}))
    defaults = omitted_defaults(definition, header["data"])
    if defaults:
        section(console, "Omitted settings with schema defaults")
        entries(console, iter(defaults))
        console.print(
            "Rule compilation can supply values from other declarations.", style="mf.secondary"
        )
    section(console, "Definition")
    entries(console, iter([("File", profile["location"]), ("SHA-256", profile["sha256"])]))
    section(console, "Validation")
    console.print(profile["validation"])
    console.print(
        "Rust semantic validation has not run. Settings above are explicitly configured values."
    )
    console.print(f"Compile: mf normalize profiles validate {profile['id']}", style="mf.secondary")
    console.print(
        "Defaults and conditional settings: mf normalize profiles schema", style="mf.secondary"
    )

    render_documentation(console)


def render_field(console: Console, field: Field) -> None:
    section(console, field.path)
    console.print(f"{field.title}  ·  {field.section}")
    for variant in field.variants:
        section(
            console, f"{' / '.join(variant.profile_types)}  ·  {', '.join(variant.source_formats)}"
        )
        console.print(variant.description)
        entries(
            console,
            iter(
                [
                    ("Type", variant.type),
                    ("Required", variant.required),
                    (
                        "Default",
                        "None declared" if variant.default is None else value_text(variant.default),
                    ),
                ]
            ),
        )
        if variant.allowed_values:
            console.print("Accepted values", style="mf.section")
            entries(
                console,
                ((value_text(choice.value), choice.meaning) for choice in variant.allowed_values),
            )
        for title, values in (
            ("Conditions", variant.dependencies),
            ("Constraints and common mistakes", variant.constraints),
        ):
            if values:
                console.print(title, style="mf.section")
                for detail in values:
                    console.print(Text(f"  • {detail}"))
        if variant.related:
            console.print("Related  " + ", ".join(variant.related))
        if variant.examples:
            console.print("TOML", style="mf.section")
            for example in variant.examples:
                console.print(Text(f"  {example}"))
    if field.children:
        console.print("Nested fields  " + ", ".join(field.children))
    render_documentation(console)


def compact_path(path: str) -> bool:
    return not (
        path.startswith("feeds.<feed>.")
        and path.count(".") > 2
        or ".rules[]." in path
        and not path.endswith(".rules[].target")
    )


def render_schema(
    console: Console,
    schema: ProfileSchema,
    *,
    field: Field | None = None,
    selected: Section | None = None,
    profile_type: str | None = None,
) -> None:
    console.print("MarketForge · Normalization Profile Schema", style="mf.heading")
    console.print(
        f"Schema {schema.schema_version}  ·  {(profile_type or 'Trades and depth').capitalize()}  ·  {schema.authority}"
    )
    if field is not None:
        render_field(console, field)
        return

    groups = (selected,) if selected else schema.sections
    scope = f" --type {profile_type}" if profile_type else ""
    example: str | None = None
    overview = grid("Section", "Applies to", "Parameters")
    overview.columns[0].style = "mf.section"
    overview.columns[2].justify = "right"
    for group in groups:
        fields = schema.parameters(section=group.id, profile_type=profile_type)
        if not fields:
            if selected:
                console.print(
                    "This section does not apply to the selected type. Omit --type to broaden the selection."
                )
            continue
        if selected or profile_type:
            section(console, group.title)
            if selected:
                console.print(group.description)
            table = grid("Parameter", "Type", "Required")
            for column in table.columns:
                column.overflow = "fold"
            table.columns[0].style = "mf.warning"
            for item in fields:
                if not selected and not compact_path(item.path):
                    continue
                types = " / ".join(dict.fromkeys(variant.type for variant in item.variants))
                statuses = {variant.required for variant in item.variants}
                required = next(iter(statuses)) if len(statuses) == 1 else "varies"
                table.add_row(Text(item.path), types, required)
            console.print(table)
            if selected:
                example = next(
                    (item.path for item in fields if item.path.endswith(".format")), fields[0].path
                )
        else:
            applicability = {
                kind
                for item in fields
                for variant in item.variants
                for kind in variant.profile_types
            }
            labels = " / ".join(
                kind.capitalize() for kind in ("trades", "depth") if kind in applicability
            )
            overview.add_row(group.title, labels, str(len(fields)))
    if not (selected or profile_type):
        console.print(overview)

    section(console, "Explore further")
    if example:
        console.print(Text(f"Field details  mf normalize profiles schema{scope} --field {example}"))
    else:
        choice = "normalization" if profile_type else "source"
        console.print(f"Browse  mf normalize profiles schema{scope} --section {choice}")
    if profile_type:
        console.print(f"Authoring guide  mf normalize profiles guide {profile_type}")
    else:
        console.print("Authoring guides  mf normalize profiles guide --help")
    render_documentation(console)
