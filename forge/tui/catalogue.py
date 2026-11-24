"""Adapters between the shared catalogue schemas and workspace controls."""

from collections.abc import Mapping, MutableMapping
from hashlib import sha256
from pathlib import Path
from typing import Literal, cast, get_args, get_origin

import tomlkit
from pydantic import ValidationError
from tomlkit.exceptions import TOMLKitError

from forge.catalog.models import MODELS
from forge.historical.authoring import recipe_writer, replace_reviewed
from forge.historical.profiles import service as profiles
from forge.historical.profiles.schema import load_schema
from forge.shared.config.toml import value
from forge.shared.documentation import SITE_URL
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths
from forge.shared.terminal.profiles import pairs
from forge.tui.actions import Field, Value, Values
from forge.tui.controller import document
from forge.tui.presentation import Cell, interval
from forge.tui.recipe_forms import validation_issues
from forge.tui.resources import Row

LABELS = {
    "Exchanges": "Exchange",
    "Instruments": "Instrument",
    "Specifications": "Specification",
    "Profiles": "Profile",
}
DOCS = {
    "Exchanges": "workflows/catalogue/exchanges-and-instruments/",
    "Instruments": "workflows/catalogue/exchanges-and-instruments/",
    "Specifications": "workflows/catalogue/specifications/",
    "Profiles": "workflows/normalization/author-profile/",
}


def contract_fields(values: Mapping[str, Value]) -> bool:
    instrument = str(values.get("instrument", "")).split(".", 3)
    return len(instrument) == 4 and instrument[1] not in {"spot", "equity"}


def denomination_required(values: Mapping[str, Value]) -> bool:
    return bool(values.get("contract_value")) or values.get("quantity_type") == "contracts"


def specification_group(name: str) -> str:
    if name in {"as_of", "fetched_at", "valid_from", "valid_to", "provenance"}:
        return "Historical validity and provenance"
    if name in {"contract_value", "contract_value_asset", "quantity_type"}:
        return "Quantity and contract sizing"
    if name == "tick_size":
        return "Price increment"
    if name == "instrument":
        return "Registered instrument"
    return "Advanced Options"


def validate_declaration(kind: str, values: Values) -> dict[str, str]:
    try:
        MODELS[kind].model_validate({key: val for key, val in values.items() if val != ""})
    except ValidationError as error:
        return validation_issues(error, {(key,): key for key in values})
    return {}


def declaration_fields(kind: str, choices: tuple[str, ...] = ()) -> tuple[Field, ...]:
    fields: list[Field] = []
    for name, field in MODELS[kind].model_fields.items():
        options = (
            choices
            if name in {"exchange", "instrument"}
            else tuple(str(item) for item in get_args(field.annotation))
            if get_origin(field.annotation) is Literal
            else ()
        )
        label = {
            "as_of": "Observed at (timezone required)",
            "fetched_at": "Fetched at (timezone required)",
            "valid_from": "Valid from (inclusive, timezone required)",
            "valid_to": "Valid to (exclusive, timezone required)",
            "instrument": "Canonical instrument reference",
        }.get(name, name.replace("_", " ").capitalize())
        group = (
            specification_group(name)
            if kind == "specs"
            else "Trading venue"
            if name == "exchange"
            else "Market classification"
            if name in {"instrument_type", "market_category"}
            else "Currencies"
            if name.endswith("_asset")
            else "Identity"
        )
        sizing = name in {"contract_value", "contract_value_asset"}
        fields.append(
            Field(
                name,
                label,
                "choice" if options else "text",
                options,
                field.default if isinstance(field.default, str) else "",
                field.is_required() or (kind == "specs" and name in {"tick_size", "provenance"}),
                group=group,
                help=field.description
                or (
                    "Enter an ISO 8601 timestamp with a timezone. Supply both bounds for historical specifications, or a retrieval time for current specifications."
                    if name in {"as_of", "fetched_at", "valid_from", "valid_to"}
                    else ""
                ),
                visible_when=contract_fields if sizing else None,
                required_when=(
                    denomination_required
                    if name == "contract_value_asset"
                    else (
                        lambda values: denomination_required(
                            {**values, "contract_value": values.get("contract_value_asset", "")}
                        )
                    )
                    if name == "contract_value"
                    else None
                ),
            )
        )
    return tuple(fields)


def columns(view: str, row: Row, *, validated: bool = False) -> tuple[Cell, ...]:
    item = document(row.value)
    if view == "Profiles":
        return (
            Cell("Profile ID", row.reference, 34),
            Cell("Input", item.get("format"), 6),
            Cell("Data", item.get("data_kind"), 7),
            Cell("Validation", "Rust validated" if validated else row.state, 14, state=True),
            Cell("Exchange", item.get("exchange"), 8, 1),
            Cell("Market", item.get("market_category"), 9, 1),
        )
    if view == "Exchanges":
        return (
            Cell("Code", row.reference, 15),
            Cell("Name", item.get("name"), 27),
            Cell("State", "Registered", 10, state=True),
        )
    identity = (
        Cell("Exchange", item.get("exchange"), 8),
        Cell("Symbol", item.get("symbol"), 12),
        Cell("Type", item.get("instrument_type"), 10),
        Cell("Market", item.get("market_category"), 8),
    )
    if view == "Instruments":
        return (
            *identity,
            Cell("Base", item.get("base_asset"), 5),
            Cell("Quote", item.get("quote_asset"), 5),
            Cell("Specs", item.get("has_specifications"), 5),
            Cell("Canonical ID", item.get("historical_instrument_id"), 12, 1),
        )
    spec = document(item.get("definition")) or item
    return (
        *identity,
        Cell("Tick size", spec.get("tick_size"), 13),
        Cell("Qty step", spec.get("qty_step"), 12),
        Cell("Observed UTC", item.get("source_observed_at") or item.get("fetched_at"), 20, 1),
        Cell("Validity UTC", interval(spec.get("valid_from"), spec.get("valid_to")), 23, 1),
        Cell("Evidence", item.get("evidence_kind") or "Current", 19, 1),
    )


def draft_path(paths: ProjectPaths, profile_id: str) -> Path:
    profiles.identity(profile_id)
    return profiles.require_safe_path(
        paths, paths.engine_config_dir / "normalize/profile-drafts" / f"{profile_id}.toml"
    )


def clone_profile(paths: ProjectPaths, original: Path, profile_id: str) -> Path:
    exchange, instrument_type, market_category, kind = profiles.identity(profile_id).split(".")
    entry = profiles.inspect(original)
    if profile_id == entry["id"] or kind != str(entry["id"]).rsplit(".", 1)[1]:
        raise MarketForgeError("Use a new profile ID with the same trades or depth kind.")
    if profiles.installed(paths, profile_id).exists():
        raise MarketForgeError("A profile with that ID is already installed.")
    original_bytes = original.read_bytes()
    if sha256(original_bytes).hexdigest() != entry["sha256"]:
        raise MarketForgeError("Source profile changed while cloning.")
    draft = tomlkit.parse(original_bytes.decode())
    draft["id"] = profile_id
    draft["exchange"] = exchange
    draft["instrument_type"] = instrument_type
    draft["market_category"] = market_category
    return profiles.write_draft(paths, profile_id, tomlkit.dumps(draft).encode())


def profile_parameters(
    definition: Mapping[str, object], profile_id: str
) -> list[dict[str, object]]:
    current_format = document(definition.get("source")).get("format")
    kind = profile_id.rsplit(".", 1)[1]
    schema = load_schema()
    rows: list[dict[str, object]] = []
    for field in schema.parameters(
        profile_type=kind, source_format=str(current_format) if current_format else None
    ):
        if field.path in {
            "id",
            "exchange",
            "instrument_type",
            "market_category",
            "schema_version",
            "kind",
        }:
            continue
        current: object = definition
        for segment in field.path.split("."):
            current = document(current).get(segment)
        rows.append(
            {
                "Field": field.path,
                "Value": value(current) if current is not None else None,
                "Help": field.variants[0].description,
                "Documentation": SITE_URL
                + "reference/configuration/profile-fields/#"
                + field.documentation.partition("#")[2],
            }
        )
    return rows


def configured_mappings(definition: Mapping[str, object]) -> list[dict[str, object]]:
    return [
        {"Field": field, "Value": configured}
        for field, configured in pairs(
            {
                key: definition[key]
                for key in ("fields", "normalization", "feeds")
                if key in definition
            }
        )
    ]


def profile_choices(key: str, definition: Mapping[str, object]) -> tuple[str, ...]:
    kind = str(definition["id"]).rsplit(".", 1)[1]
    schema = load_schema()
    field = schema.field(key, profile_type=kind)
    return tuple(
        dict.fromkeys(
            str(choice.value)
            for variant in field.variants
            if variant.control == "select"
            for choice in variant.allowed_values
            if isinstance(choice.value, str)
        )
    )


def change_profile_field(source: bytes, key: str, text: str) -> str:
    if any(token in key for token in ("<", "[")):
        raise MarketForgeError("Edit repeated mappings and dynamic source field names in Neovim.")
    definition = tomlkit.parse(source.decode())
    parent = cast(MutableMapping[str, object], definition)
    *parents, leaf = key.split(".")
    for segment in parents:
        child = parent.get(segment)
        if not isinstance(child, MutableMapping):
            child = tomlkit.table()
            parent[segment] = child
        parent = cast(MutableMapping[str, object], child)
    if text:
        parent[leaf] = tomlkit.parse("value = " + text)["value"]
    elif leaf in parent:
        del parent[leaf]
    return tomlkit.dumps(definition)


def update_draft(path: Path, original: bytes, updated: str) -> None:
    with recipe_writer(path):
        replace_reviewed(path, original, updated)


def documentation_url(view: str) -> str:
    return SITE_URL + DOCS[view]


def review_text(values: Mapping[str, object]) -> str:
    lines: list[str] = []
    for key, item in values.items():
        label = key.replace("_", " ").capitalize()
        if isinstance(item, Mapping):
            lines.extend(("", label, review_text(cast(Mapping[str, object], item))))
        elif item is not None:
            lines.append(f"{label}  {item}")
    return "\n".join(lines)


def profile_form(
    definition: Mapping[str, object], profile_id: str
) -> tuple[tuple[Field, ...], Values]:
    """Expose concrete Rust schema fields and keep dynamic structures editable as TOML."""
    from functools import partial

    schema = load_schema()
    kind = profile_id.rsplit(".", 1)[1]
    fields: list[Field] = []
    preset: Values = {"profile": profile_id}
    fields.append(Field("profile", "Profile ID", group="Identity"))

    def applies(formats: frozenset[str], values: Mapping[str, Value]) -> bool:
        selected = values.get("source--format")
        return not selected or selected in formats

    for item in schema.parameters(profile_type=kind):
        path = item.path
        if path in {
            "id",
            "exchange",
            "instrument_type",
            "market_category",
            "schema_version",
            "kind",
        }:
            continue
        if (
            "<" in path
            or "[" in path
            or (item.children and path not in {"fields", "feeds", "normalization.rules"})
        ):
            continue
        variants = item.variants
        key = path.replace(".", "--")
        formats = frozenset(name for variant in variants for name in variant.source_formats)
        mandatory = frozenset(
            name
            for name in formats
            if all(
                variant.required == "required"
                for variant in variants
                if name in variant.source_formats
            )
        )
        options = (
            tuple(
                dict.fromkeys(
                    str(choice.value)
                    for variant in variants
                    for choice in variant.allowed_values
                    if isinstance(choice.value, str)
                )
            )
            if all(variant.control == "select" for variant in variants)
            else ()
        )
        current: object = definition
        for segment in path.split("."):
            current = document(current).get(segment)
        if options and isinstance(current, str) and current not in options:
            options = (*options, current)
        preset[key] = "" if current is None else str(current) if options else value(current)
        describe = tuple(
            dict.fromkeys(
                line
                for variant in variants
                for line in (
                    variant.description,
                    *(variant.constraints),
                    *(variant.dependencies),
                    *((f"Default: {variant.default}",) if variant.default is not None else ()),
                )
            )
        )
        group = (
            "Timestamp interpretation"
            if "timestamp" in path
            else "Normalization target"
            if path == "normalization.target"
            else "Trade interpretation"
            if item.section == "normalization" and kind == "trades"
            else "Depth interpretation"
            if item.section in {"normalization", "feeds"}
            else "Field mapping"
            if item.section == "fields"
            else "Record structure"
            if item.section == "source" and path != "source.format"
            else "Source format"
            if path == "source.format"
            else "Identity"
        )
        fields.append(
            Field(
                key,
                item.title,
                "choice" if options else "text",
                options,
                group=group,
                help="\n".join(describe),
                visible_when=partial(applies, formats) if path != "source.format" else None,
                required_when=partial(applies, mandatory) if mandatory else None,
                documentation_url=SITE_URL
                + "reference/configuration/profile-fields/#"
                + item.documentation.partition("#")[2],
                placeholder="TOML value" if not options else "",
            )
        )
    return tuple(fields), preset


def profile_form_candidate(
    original: bytes, fields: tuple[Field, ...], preset: Values, values: Values
) -> tuple[bytes, dict[str, str]]:
    updated = original
    issues: dict[str, str] = {}
    for field in fields:
        key = field.key
        if key == "profile" or key not in values or values[key] == preset[key]:
            continue
        current = values[key]
        try:
            updated = change_profile_field(
                updated,
                key.replace("--", "."),
                value(current) if field.choices and current else str(current),
            ).encode()
        except (ValueError, TOMLKitError) as error:
            issues[key] = str(error)
    return updated, issues
