"""Bounded, read-only source evidence for profile authoring interfaces."""

import csv
import io
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from itertools import islice
from pathlib import Path
from typing import cast

from forge.historical.imports.formats import SAMPLE_BYTES, read_sample

DEFAULT_ROWS = 5
MAX_ROWS = 20
MAX_FIELDS = 64
MAX_DEPTH = 8
INTEGER = re.compile(r"[+-]?[0-9]+")
DECIMAL = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")


@dataclass(frozen=True)
class FieldSample:
    name: str
    position: int | None
    representation: str
    examples: tuple[str, ...]
    missing: int
    nulls: int


@dataclass(frozen=True)
class SourceSample:
    filename: str
    container: str
    layout: str
    member: str | None = None
    header: bool | None = None
    fields: tuple[FieldSample, ...] = ()
    preview: tuple[tuple[str | None, ...], ...] = ()
    encoded_records: tuple[str, ...] = ()
    columns: int | None = None
    warnings: tuple[str, ...] = ()
    reason: str | None = None
    members: tuple[str, ...] = ()

    def evidence_json(self) -> str:
        """Return the existing evidence shape when a caller explicitly requests more rows."""
        metadata = {
            "container": self.container,
            "layout": self.layout,
            "member": self.member,
            "header": self.header,
            "fields": [field.name for field in self.fields],
            "sample_limit_bytes": SAMPLE_BYTES,
            "reason": self.reason,
            **({"members": self.members} if self.members else {}),
        }

        # Each JSONL entry is a checked JSON object. Retain its numeric tokens verbatim.
        return (
            json.dumps(metadata)[:-1]
            + ', "representative_rows": ['
            + ", ".join(self.encoded_records)
            + "]}"
        )


def safe_text(value: str) -> str:
    return "".join(
        character if character.isprintable() else ascii(character)[1:-1] for character in value
    )


def bounded_value(value: object, depth: int = 0) -> str:
    if isinstance(value, str):
        return safe_text(json.dumps(value, ensure_ascii=False))
    if isinstance(value, list):
        items = cast(list[object], value)
        if depth == 2:
            return f"[{len(items)} items]"
        shown = ", ".join(bounded_value(item, depth + 1) for item in items[:3])
        return f"[{shown}{', …' if len(items) > 3 else ''}]"
    if isinstance(value, dict):
        return f"{{{len(cast(dict[str, object], value))} keys}}"
    if value is None:
        return "null"
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def representation(value: object, *, csv_field: bool) -> str:
    if isinstance(value, str):
        if not value and csv_field:
            return "Empty"
        if INTEGER.fullmatch(value):
            return "Integer" if csv_field else "Numeric string"
        if DECIMAL.fullmatch(value):
            return "Decimal" if csv_field else "Decimal-like string"
        return "String"
    if isinstance(value, bool):
        return "Boolean"
    if isinstance(value, int):
        return "Integer"
    if isinstance(value, Decimal):
        return "Decimal"
    if isinstance(value, dict):
        return "Object"
    if isinstance(value, list):
        return "Array"
    return "Null"


def summarize(
    names: Sequence[str], values: Sequence[Sequence[object]], *, csv_fields: bool
) -> tuple[tuple[FieldSample, ...], tuple[tuple[str | None, ...], ...]]:
    columns: list[FieldSample] = []
    preview: list[list[str | None]] = [[] for _ in values]
    for index, name in enumerate(names):
        types: set[str] = set()
        examples: dict[str, None] = {}
        missing = nulls = 0
        for row, output in zip(values, preview, strict=True):
            if index >= len(row) or row[index] is MISSING:
                missing += 1
                output.append(None)
                continue
            value = row[index]
            nulls += value is None
            types.add(representation(value, csv_field=csv_fields))
            if csv_fields and isinstance(value, str):
                visible = safe_text(value) if value else '""'
            else:
                visible = bounded_value(value)
            output.append(visible)
            examples.setdefault(visible, None)
        suggested = "Mixed" if len(types) > 1 else next(iter(types), "Unknown")
        shown = tuple(islice(examples, 3))
        if len(examples) > len(shown):
            shown += ("…",)
        columns.append(
            FieldSample(name, index + 1 if csv_fields else None, suggested, shown, missing, nulls)
        )
    return tuple(columns), tuple(tuple(row) for row in preview)


MISSING = object()


def csv_records(text: str, limit: int) -> tuple[list[list[str]], str | None]:
    first_line = text.split("\n", 1)[0]
    try:
        delimiter = csv.Sniffer().sniff(first_line, delimiters=",\t;|").delimiter
    except csv.Error:
        delimiter = ","
    rows: list[list[str]] = []
    reader = csv.reader(io.StringIO(text, newline=""), delimiter=delimiter, strict=True)
    try:
        for row in reader:
            if row:
                rows.append(row)
                if len(rows) == limit:
                    break
    except csv.Error:
        return rows, "Malformed CSV record in the sample."
    return rows, None


def unique_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def reject_constant(token: str) -> None:
    raise ValueError(f"Invalid JSON constant: {token}")


def json_records(text: str, limit: int) -> tuple[list[str], list[dict[str, object]], str | None]:
    encoded: list[str] = []
    records: list[dict[str, object]] = []
    for line in islice((line for line in text.split("\n") if line.strip()), limit):
        try:
            value: object = json.loads(
                line,
                parse_float=Decimal,
                parse_constant=reject_constant,
                object_pairs_hook=unique_keys,
            )
            if not isinstance(value, dict):
                raise ValueError("expected object")
            encoded.append(line.strip())
            records.append(cast(dict[str, object], value))
        except (ValueError, RecursionError, ArithmeticError):
            return (
                encoded,
                records,
                "Malformed JSONL record, duplicate key, or excessive JSON nesting.",
            )
    return encoded, records, None


def flatten(record: dict[str, object], warnings: dict[str, None]) -> dict[str, object]:
    result: dict[str, object] = {}

    def visit(node: dict[str, object], parent: tuple[str, ...]) -> None:
        for key, value in node.items():
            if len(result) >= MAX_FIELDS:
                warnings.setdefault("Field inspection is limited to 64 paths.", None)
                break
            if not key or "." in key:
                warnings.setdefault(
                    "Literal dotted or empty JSON keys are ambiguous for profile paths and are omitted.",
                    None,
                )
                continue
            parts = (*parent, key)
            if isinstance(value, dict) and value and len(parts) < MAX_DEPTH:
                visit(cast(dict[str, object], value), parts)
            else:
                if isinstance(value, dict) and value:
                    warnings.setdefault(
                        "Objects at the path depth limit are shown without expanding their keys.",
                        None,
                    )
                result[".".join(parts)] = value

    visit(record, ())
    return result


def inspect_source(
    path: Path, member: str | None = None, header: bool | None = None, *, rows: int = DEFAULT_ROWS
) -> SourceSample:
    if not 1 <= rows <= MAX_ROWS:
        raise ValueError(f"Sample rows must be between 1 and {MAX_ROWS}.")
    metadata, content = read_sample(path, member)
    container = str(metadata["container"])
    selected = metadata.get("member")
    selected_member = selected if isinstance(selected, str) else None
    reason = metadata.get("reason")
    warnings: dict[str, None] = {}
    layout = "unsupported"
    names: list[str] = []
    raw: list[str] = []
    values: list[Sequence[object]] = []
    columns: int | None = None
    if reason is None:
        if len(content) == SAMPLE_BYTES:
            content = content[: content.rfind(b"\n") + 1]
            warnings["Only complete records within the 65,536-byte prefix are eligible."] = None
        try:
            text = content.decode("utf-8-sig")
            if "\x00" in text:
                raise ValueError("NUL in sample")
        except (UnicodeError, ValueError):
            reason = "Binary or unreadable text sample."
        else:
            first = text.lstrip()[:1]
            csv_name = (selected_member or path.name).lower().endswith((".csv", ".csv.gz"))
            jsonl_name = (
                (selected_member or path.name)
                .lower()
                .endswith((".jsonl", ".ndjson", ".jsonl.gz", ".ndjson.gz"))
            )
            if first and (jsonl_name or (first in "{[" and not csv_name)):
                raw, records, reason = json_records(text, rows)
                if records:
                    layout = "jsonl"
                    flat = [flatten(record, warnings) for record in records]
                    seen = dict.fromkeys(key for record in flat for key in record)
                    names = list(islice(seen, MAX_FIELDS))
                    if len(seen) > MAX_FIELDS:
                        warnings["Field inspection is limited to 64 paths."] = None
                    if any(
                        ".".join(parts[:length]) in seen
                        for path_key in seen
                        for parts in (path_key.split("."),)
                        for length in range(1, len(parts))
                    ):
                        warnings[
                            "Some JSON paths alternate between an object and a leaf across records."
                        ] = None
                    values = [[record.get(key, MISSING) for key in names] for record in flat]
            elif first:
                parsed, reason = csv_records(text, rows + int(header is True))
                if parsed and (len(parsed[0]) > 1 or csv_name):
                    layout = "csv"
                    columns = len(parsed[0])
                    names = (
                        parsed[0][:MAX_FIELDS]
                        if header is True
                        else [f"column_{index + 1}" for index in range(min(columns, MAX_FIELDS))]
                    )
                    raw = [json.dumps(row) for row in parsed]
                    values = list(parsed[int(header is True) :])
                    if not values and reason is None:
                        reason = "No data records after the CSV header."
                    if any(len(row) != columns for row in values):
                        warnings[
                            "CSV records have irregular widths. Extra cells without a column are omitted from the preview."
                        ] = None
                    if header is True and (
                        any(not name.strip() for name in parsed[0])
                        or len(set(parsed[0])) != columns
                    ):
                        warnings[
                            "CSV has duplicate or empty headers. Use positions to distinguish columns and resolve ambiguous mappings before authoring."
                        ] = None
                    if columns > MAX_FIELDS:
                        warnings["Field inspection is limited to 64 columns."] = None
            else:
                reason = (
                    "No complete records in the bounded prefix." if warnings else "Empty sample."
                )
        if layout == "unsupported" and reason is None:
            reason = "No supported CSV/JSONL text structure identified."
    else:
        layout = str(metadata["layout"])

    fields, preview = summarize(names, values, csv_fields=layout == "csv")
    members = metadata.get("members")
    return SourceSample(
        path.name,
        container,
        layout,
        selected_member,
        header,
        fields,
        preview,
        tuple(raw),
        columns,
        tuple(warnings),
        str(reason) if reason is not None else None,
        tuple(cast(list[str], members)) if isinstance(members, list) else (),
    )
