#!/usr/bin/env python3
"""Build the Forge profile reference from Rust wire types and semantic annotations."""

import argparse
import hashlib
import json
import re
import tomllib
from functools import cache
from pathlib import Path

ENGINE = Path(__file__).resolve().parents[1]
SPEC = ENGINE / "specs/normalization-profile.toml"
OUTPUT = ENGINE.parent / "forge/historical/profiles/schema.json"
DOC = ENGINE.parent / "docs/content/reference/configuration/profile-fields.md"

SOURCES = {
    "wire": "src/historical/config/catalog/unified.rs",
    "catalogue": "src/historical/config/catalog.rs",
    "trade": "src/shared/formats/trades/spec.rs",
    "depth": "src/shared/formats/depth/spec.rs",
    "identity": "src/shared/formats/trades/native_identity.rs",
    "venue": "src/shared/domain/canonical/enums.rs",
    "instrument": "src/shared/domain/instrument/spec.rs",
    "container": "src/shared/io/container.rs",
    "normalization": "src/shared/normalization/config.rs",
    "trade_transforms": "src/shared/formats/trades/transforms.rs",
    "depth_scalar": "src/shared/formats/depth/scalar.rs",
    "depth_transform": "src/shared/formats/depth/transforms.rs",
}
ANNOTATIONS = tomllib.loads(SPEC.read_text(encoding="utf-8"))
RUST = {key: (ENGINE / value).read_text(encoding="utf-8") for key, value in SOURCES.items()}


@cache
def members(name):
    module, symbol = name.split(".")
    match = re.search(rf"(?ms)^\s*(?:pub )?struct {symbol} \{{\n(.*?)^\}}", RUST[module])
    if match is None:
        raise ValueError(f"Missing Rust type {name}")
    fields = {}
    attributes = ""
    for line in match[1].splitlines():
        line = line.strip()
        if not line or line.startswith("//"):
            continue
        if line.startswith("#[serde("):
            attributes += line
            continue
        field = re.fullmatch(r"(?:pub )?(\w+): (.+),", line)
        if not field:
            raise ValueError(f"Unrecognized Rust field in {name}: {line}")
        rename = re.search(r'rename = "(\w+)"', attributes)
        fields[rename[1] if rename else field[1]] = (field[2], "default" in attributes)
        attributes = ""
    return fields


@cache
def variants(name):
    module, symbol = name.split(".")
    match = re.search(
        rf'(?ms)#\[serde\(rename_all = "snake_case"\)\]\n(?:pub )?enum {symbol} \{{\n(.*?)^\}}',
        RUST[module],
    )
    if match is None:
        raise ValueError(f"Missing Rust enum {name}")
    values = []
    for line in match[1].splitlines():
        line = line.strip()
        if not line or line.startswith("//"):
            continue
        if re.fullmatch(r"[A-Z]\w*,", line) is None:
            raise ValueError(f"Unrecognized Rust enum variant: {line}")
        values.append(re.sub(r"(?<!^)(?=[A-Z])", "_", line[:-1]).lower())
    return values


def structural(rust_type, default):
    optional = rust_type.startswith("Option<")
    if optional:
        rust_type = rust_type[7:-1]
    if rust_type.startswith(("BTreeMap<", "HashMap<")):
        return (
            "table",
            not (optional or default),
            {} if default and not optional else None,
        )
    if rust_type.startswith("Vec<"):
        return f"array of {structural(rust_type[4:-1], False)[0]}", not optional, None
    if rust_type in ("u32", "u64", "usize"):
        return (
            "integer",
            not (optional or default),
            0 if default and not optional else None,
        )
    if rust_type in ("String", "bool", "Value"):
        return (
            {"String": "string", "bool": "boolean", "Value": "TOML value"}[rust_type],
            not optional,
            None,
        )
    return "table", not (optional or default), None


class Builder:
    def __init__(self):
        self.fields = {}

    def field(self, path, section, annotation, *, shape, profile_types):
        rust_type, serde_default = shape
        value_type, required, default = structural(rust_type, serde_default)
        enum = annotation.get("enum")
        values = variants(enum) if enum else annotation.get("values", [])
        meanings = annotation.get("meanings", {})
        if set(meanings) != {str(value).lower() for value in values}:
            raise ValueError(f"{path} must describe every allowed value  {values}")
        if enum:
            value_type = "string"
        if len(profile_types) > 1 and path.endswith((".target", ".format", ".record_format")):
            for profile_type in profile_types:
                self.field(path, section, annotation, shape=shape, profile_types=[profile_type])
            return
        if path.endswith(".target"):
            targets = ("trade",) if profile_types == ["trades"] else ("l2_snapshot", "l2_update")
            values = [value for value in values if value in targets]
        if path.endswith(".format") and profile_types == ["trades"]:
            values = [value for value in values if value != "xlsx"]
        if path.endswith(".record_format"):
            values = [
                value for value in values if (value == "rows") == (profile_types == ["trades"])
            ]
        formats = annotation.get("formats", ["csv", "jsonl", "xlsx"])
        if profile_types == ["trades"]:
            formats = [value for value in formats if value != "xlsx"]
        displayed_type = annotation.get("type", value_type)
        if values and displayed_type in ("string", "integer"):
            displayed_type = "literal" if len(values) == 1 and not enum else "enum"
        if path.endswith((".field", ".source")) and displayed_type == "string":
            displayed_type = "source field reference"
        examples = annotation.get("examples", [])
        if path.endswith((".target", ".record_format")):
            examples = [f"{path.rsplit('.', 1)[-1]} = {json.dumps(values[0])}"]
        if path.endswith((".bids.field", ".asks.field")):
            examples = [f'field = "{path.split(".")[-2]}"']
        if path.endswith(
            (
                ".price.field",
                ".trade_id.field",
                ".system_timestamp.field",
                ".sequence.field",
                ".sequence_start.field",
                ".sequence_count.field",
            )
        ):
            examples = [f'field = "{path.split(".")[-2]}"']
        if "." in path and section != "source" and required and "required" not in annotation:
            annotation = {
                **annotation,
                "required": "conditional",
                "dependencies": [
                    *annotation.get("dependencies", []),
                    "Required when the containing table is used.",
                ],
            }
        if examples:
            fragment = tomllib.loads("\n".join(examples))
            key = path.rsplit(".", 1)[-1]
            if values and key in fragment and fragment[key] not in values:
                raise ValueError(f"Example for {path} uses an unsupported value")
        row = {
            "type": displayed_type,
            "required": annotation.get("required", "required" if required else "optional"),
            "default": annotation.get("default", default),
            "description": annotation["description"],
            "allowed_values": [
                {"value": value, "meaning": meanings[str(value).lower()]} for value in values
            ],
            "constraints": annotation.get("constraints", []),
            "dependencies": annotation.get("dependencies", []),
            "related": annotation.get("related", []),
            "profile_types": profile_types,
            "source_formats": formats,
            "examples": examples,
        }
        row["control"] = (
            "select"
            if values and row["type"] in ("enum", "literal", "boolean")
            else {
                "integer": "integer",
                "boolean": "toggle",
                "string": "text",
                "identifier": "text",
                "source field reference": "text",
                "table": "group",
            }.get(row["type"], "structured")
        )
        data = self.fields.setdefault(
            path,
            {
                "path": path,
                "section": section,
                "title": path.rsplit(".", 1)[-1].replace("_", " ").capitalize(),
                "variants": [],
            },
        )
        if row not in data["variants"]:
            data["variants"].append(row)

    def block(self, name, prefix, section, profile_types, *, rule=False):
        source = ANNOTATIONS["annotations"][name]
        rust = members(name) if "." in name else {}
        edits = {"source": "field"} if name in ANNOTATIONS["mapping_types"] else {}
        if rust and {edits.get(key, key) for key in rust} != set(source):
            raise ValueError(f"Rust field inventory changed for {name}: {sorted(rust)}")
        for key, annotation in source.items():
            annotation = annotation.copy()
            rust_key = "source" if key == "field" and "source" in edits else key
            shape = rust.get(rust_key, ("Value", False))
            if (
                (rule and not shape[0].startswith("Option<"))
                or annotation.get("alias")
                or (
                    section in ("normalization", "feeds")
                    and prefix.endswith("normalization")
                    and name in ("trade.TradeSpec", "depth.DepthRules")
                    and not shape[0].startswith("Option<")
                )
            ) and annotation.get("required") != "optional":
                annotation["required"] = "conditional"
                if annotation.get("alias"):
                    condition = "Required when this mapping is used. The source alias is accepted."
                else:
                    condition = "Required in the effective target rule unless the enclosing mapping is optional."
                annotation["dependencies"] = [
                    *annotation.get("dependencies", []),
                    condition,
                ]
            path = f"{prefix}.{key}" if prefix else key
            selected_types = profile_types
            if path == "feeds":
                selected_types = ["depth"]
            if path.endswith((".trade_id_scope", ".trade_id_reset_domain")):
                selected_types = [kind for kind in profile_types if kind == "trades"]
            if not selected_types:
                continue
            if key == "target" and prefix.endswith("[]"):
                raise AssertionError("Rule target is generated separately")
            self.field(
                path,
                section if prefix else (key if key in SECTIONS else section),
                annotation,
                shape=shape,
                profile_types=selected_types,
            )
            if alias := annotation.get("alias"):
                self.field(
                    f"{prefix}.{alias}",
                    section,
                    {
                        "description": "Direct source-field spelling accepted by the compiled rule parser. Prefer field in authored profiles.",
                        "type": "string",
                        "required": "optional",
                        "related": [path],
                        "examples": [],
                    },
                    shape=("String", False),
                    profile_types=profile_types,
                )
            if child := annotation.get("children"):
                table = path + annotation.get("suffix", "")
                if child == "normalization":
                    self.normalization(
                        table, section if prefix else "normalization", selected_types
                    )
                else:
                    self.block(child, table, section if prefix else key, selected_types)

    def normalization(self, prefix, section, profile_types):
        self.block("normalization", prefix, section, profile_types)
        if "depth" in profile_types:
            for parent in (prefix, f"{prefix}.rules[]"):
                for side in ("bids", "asks"):
                    self.block("LevelFieldsMapping", f"{parent}.{side}", section, ["depth"])
        for kind, name in (
            ("trades", "trade.TradeSpec"),
            ("depth", "depth.DepthRules"),
        ):
            if kind not in profile_types:
                continue
            self.block(name, prefix, section, [kind])
            self.block(name, f"{prefix}.rules[]", section, [kind], rule=True)
            self.field(
                f"{prefix}.rules[].target",
                section,
                ANNOTATIONS["annotations"]["normalization"]["target"],
                shape=("RuleTarget", False),
                profile_types=[kind],
            )
            self.fields[f"{prefix}.rules[].target"]["variants"][-1]["required"] = "required"
        if "trades" in profile_types:
            self.block("trade_instrument", prefix, section, ["trades"])


SECTIONS = ("identity", "source", "fields", "normalization", "feeds")


def build():
    b = Builder()
    b.block("wire.Unified", "", "identity", ["trades", "depth"])
    for path, field in b.fields.items():
        if path.endswith(".book") or ".book." in path:
            for variant in field["variants"]:
                variant["profile_types"] = ["depth"]

    raw = (
        RUST["wire"].split("for key in field.keys()", 1)[1].split(".contains(&key.as_str())", 1)[0]
    )
    decoded = set(re.findall(r'"(\w+)"', raw))
    if decoded != set(ANNOTATIONS["annotations"]["RawField"]):
        raise ValueError(f"Rust raw field inventory changed: {sorted(decoded)}")
    order = {name: index for index, name in enumerate(SECTIONS)}
    fields = sorted(b.fields.values(), key=lambda item: (order[item["section"]], item["path"]))
    for field in fields:
        prefix = field["path"]
        parents = [prefix + ".", prefix + "[]."]
        field["children"] = [
            item["path"]
            for item in fields
            if any(
                item["path"].startswith(parent) and "." not in item["path"][len(parent) :]
                for parent in parents
            )
        ]
    return {
        "metadata_version": 1,
        "schema_version": 2,
        "title": "Normalization profile",
        "authority": "Rust Engine",
        "limitations": ANNOTATIONS["limitations"],
        "sections": [
            {
                "id": name,
                "title": name.capitalize(),
                "description": ANNOTATIONS["sections"][name],
            }
            for name in SECTIONS
        ],
        "fields": fields,
        "rust_sources": {
            path: hashlib.sha256((ENGINE / path).read_bytes()).hexdigest()
            for path in sorted(SOURCES.values())
        },
    }


REFERENCE_NOTES = {
    "identity": (
        "Use the filename `<id>.toml` under the matching `trades/` or `depth/` directory. "
        "Valid market pairs are spot/spot, perpetual or future with linear or inverse, "
        "option/option, and equity/equity. A valid identity does not establish reader support."
    ),
    "source": (
        "Read the format and layout rows together. Trades take CSV or JSONL with `rows`. "
        "Depth supports JSONL events and snapshots, specific positional CSV layouts, and a fixed XLSX snapshot layout. "
        "Archive members, headers and the selected mappings must match an implemented reader. "
        "For XLSX use one header sheet with exactly `timestamp`, `asks` and `bids`, Unix seconds, "
        'and common `input_ordering = { order = "ascending", sort_by = "timestamp" }`.'
    ),
    "fields": (
        'In executable mappings, `field = "size"` references the declared `[fields.size]` key. '
        'For nested JSON use a literal dotted key such as `[fields."data.ts"]` and `field = "data.ts"`. '
        "Where listed, `source` is a compatibility spelling. Prefer `field`, and use only `field` "
        "for the event timestamp and trade instrument bindings. CSV positions start at one. "
        "Level-array indices start at zero. Precision annotations describe the source and `encoding` selects the decoder."
    ),
    "normalization": (
        "Use one `target` or a nonempty `[[normalization.rules]]` array. Each item has its own target "
        "and inherits common mappings without overriding their keys. The tables below show the common paths. "
        "Use the same mapping keys within rules where applicable. Keep `quantity_encoding`, `book` "
        "and the trade instrument binding in common normalization. With depth, use an unsequenced "
        "path when both sequence mappings are absent, a consecutive policy with only `sequence_start`, or a "
        "ranged policy with both. All rules in a task must agree. See the bounded-observation exception below."
    ),
    "feeds": (
        "Each variant has its own complete depth [Source](#source), [Fields](#fields) and "
        "[Normalization](#normalization) tables. Keep the complete top-level definition as well. "
        'Set `feed = "snapshot"` or `feed = "incremental"` in acquisition request source options.'
    ),
}


def table_cell(text):
    return text.replace("|", r"\|").replace("\n", " ")


def field_group(section, path):
    if section == "normalization":
        parts = path.split(".")
        root = parts[1] if len(parts) > 1 else ""
        if root in {"target", "rules", "rules[]", "quantity_encoding"}:
            return "Targets and units"
        if root in {"snapshot", "update_semantics", "event_filter", "input_ordering"}:
            return "Depth operations"
        if root in {"bids", "asks"}:
            return "Depth level arrays"
        if root in {"book", "sequence_start", "sequence_count"}:
            return "Sequence and book metadata"
        return "Record mappings"
    return ""


def markdown(schema):
    lines = [
        "# Normalization profile field index",
        "",
        "<!-- Generated by engine/scripts/generate_profile_schema.py. -->",
        "",
        ("See the [authoring guide](../../workflows/normalization/author-profile.md) for working trade and depth examples. "
        "Angle brackets mark a key you supply. Empty brackets mark an array item. "
        "An optional key is unset when omitted unless a default is listed. A field marked "
        "conditional depends on its enclosing table, target or transform. Applicability labels "
        "distinguish trade and depth fields. The compiler and reader check the whole combination."),
        "",
    ]
    for section in schema["sections"]:
        name = section["id"]
        lines.extend(
            (f"## {section['title']}", "", section["description"], "", REFERENCE_NOTES[name], "")
        )
        groups = {}
        for field in schema["fields"]:
            path = field["path"]
            if (
                field["section"] != name
                or (path.startswith("feeds.<feed>.") and path.count(".") > 2)
                or (".rules[]." in path and not path.endswith(".rules[].target"))
            ):
                continue
            groups.setdefault(field_group(name, path), []).append(field)
        group_order = (
            "Targets and units",
            "Record mappings",
            "Depth operations",
            "Depth level arrays",
            "Sequence and book metadata",
        )
        for group in group_order if name == "normalization" else groups:
            fields = groups[group]
            if group:
                lines.extend((f"### {group}", ""))
            lines.extend(
                ("| Field | Type and presence | Meaning and constraints |", "| --- | --- | --- |")
            )
            for field in fields:
                path = field["path"]
                anchor = f"{name}_1" if path == name else re.sub(r"[^a-z0-9_]", "", path)
                for index, variant in enumerate(field["variants"]):
                    values = " ".join(
                        f"`{json.dumps(item['value'])}` — {item['meaning']}"
                        for item in variant["allowed_values"]
                    )
                    notes = [variant["description"], values]
                    notes.extend((*variant["constraints"], *variant["dependencies"]))
                    if variant["default"] is not None:
                        notes.append(f"Default `{json.dumps(variant['default'])}`.")
                    kinds = ", ".join(variant["profile_types"])
                    presence = f"{variant['type']} · {variant['required']} · {kinds}"
                    baseline = (
                        ["csv", "jsonl"]
                        if variant["profile_types"] == ["trades"]
                        else ["csv", "jsonl", "xlsx"]
                    )
                    if variant["source_formats"] != baseline:
                        presence += " · " + ", ".join(variant["source_formats"])
                    key = f'<a id="{anchor}"></a>' if index == 0 else ""
                    cells = (f"{key}`{path}`", presence, " ".join(filter(None, notes)))
                    lines.append("| " + " | ".join(map(table_cell, cells)) + " |")
            lines.append("")
            if group == "Depth level arrays":
                lines.extend(
                    (
                        ("For tuple levels such as `[[99, 2]]`, use "
                        '`level_format = ["price", "quantity"]` on the raw array field. '
                        'For object levels such as `[{"p":99,"q":2}]`, use '
                        '`level_fields = { price = "p", quantity = "q" }` instead. '
                        "Apply the mapping to each side's declared raw key."),
                        "",
                    )
                )
    return "\n".join(lines).rstrip() + "\n"


def artifact(schema):
    # Keep one generated line per field so changes remain reviewable.
    compact_fields = [json.dumps(field, ensure_ascii=False) for field in schema["fields"]]
    envelope = json.dumps({**schema, "fields": None}, indent=2, ensure_ascii=False)
    return (
        envelope.replace(
            '"fields": null',
            '"fields": [\n    ' + ",\n    ".join(compact_fields) + "\n  ]",
        )
        + "\n"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    schema = build()
    for path, content in (
        (OUTPUT, artifact(schema)),
        (DOC, markdown(schema)),
    ):
        if args.check:
            if not path.is_file() or path.read_text(encoding="utf-8") != content:
                raise SystemExit(
                    f"Stale profile reference: {path}. Regenerate with {Path(__file__).name}."
                )
        else:
            path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    main()
