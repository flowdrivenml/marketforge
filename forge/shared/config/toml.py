"""Canonical TOML for new configuration and values inserted into existing documents."""

from collections.abc import Iterator, Mapping, MutableSequence
from copy import copy
from datetime import datetime, time, timedelta
from decimal import Decimal
from typing import cast
from uuid import UUID

import tomlkit
from tomlkit.items import AoT, Array, Item, Table

# Archive request models inherit fields from ArchiveSource. Keep identity together
# in the document, followed by the data kind and the compact selection objects.
_REQUEST_FIELDS = ("exchange", "instrument_type", "market_category", "data_type")
_REQUEST_INLINE = frozenset({"scope", "source_options"})


def _section(fields: Mapping[str, object], path: tuple[str, ...]) -> Table:
    table = tomlkit.table()
    _populate(table, fields, path)
    return table


def _populate(
    table: tomlkit.TOMLDocument | Table, fields: Mapping[str, object], path: tuple[str, ...]
) -> None:
    sections: list[tuple[str, Table | AoT]] = []
    for key, entry in _fields(fields, path):
        location = (*path, key)
        item = configuration_item(entry, location, inline=inline_field(location))
        if isinstance(item, (Table, AoT)):
            sections.append((key, item))
        else:
            table[key] = item
    for key, item in sections:
        table[key] = item


def _fields(fields: Mapping[str, object], path: tuple[str, ...]) -> Iterator[tuple[str, object]]:
    first = _REQUEST_FIELDS if path == ("requests",) else ()
    if not path:
        first = ("schema_version", "version")
    for key in dict.fromkeys((*first, *fields)):
        if (entry := fields.get(key)) is not None:
            yield key, entry


def inline_field(path: tuple[str, ...]) -> bool:
    return len(path) == 2 and path[0] == "requests" and path[1] in _REQUEST_INLINE


def append_entry(entries: AoT | Array, entry: object, path: tuple[str, ...]) -> None:
    """Append using the existing array representation."""
    if isinstance(entries, AoT):
        addition = _section(cast(Mapping[str, object], entry), path)
        addition.trivia.indent = "\n"
    else:
        addition = configuration_item(entry, path, inline=True)
    cast(MutableSequence[Item], entries).append(addition)


def configuration_item(entry: object, path: tuple[str, ...] = (), *, inline: bool = False) -> Item:
    """Build a value using the structural context and the schema's input order."""
    if isinstance(entry, Item):
        # Parsed items can carry precision beyond Python's native datetime.
        return copy(entry)
    if isinstance(entry, Mapping):
        fields = cast(Mapping[str, object], entry)
        if not inline:
            return _section(fields, path)
        table = tomlkit.inline_table()
        children = list(_fields(fields, path))
        if children:
            table.append(None, tomlkit.ws(" "))
            for key, child in children:
                table[key] = configuration_item(child, (*path, key), inline=True)
            table.append(None, tomlkit.ws(" "))
        return table
    if isinstance(entry, (list, tuple)):
        entries = cast("list[object] | tuple[object, ...]", entry)
        if not inline and entries and all(isinstance(child, Mapping) for child in entries):
            tables = tomlkit.aot()
            for child in entries:
                cast(MutableSequence[Table], tables).append(
                    _section(cast(Mapping[str, object], child), path)
                )
            return tables
        array = tomlkit.array()
        for child in entries:
            cast(MutableSequence[Item], array).append(configuration_item(child, path, inline=True))
        if not inline and len(array.as_string()) + len(path[-1] if path else "") + 3 > 88:
            array.multiline(True)
        return array
    if isinstance(entry, datetime):
        offset = entry.utcoffset()
        if offset is not None and offset % timedelta(minutes=1):
            raise ValueError("TOML datetime offsets must be whole minutes.")
    if isinstance(entry, time) and entry.tzinfo is not None:
        raise ValueError("TOML time values cannot carry a timezone.")
    if isinstance(entry, (Decimal, UUID)):
        entry = str(entry)
    return tomlkit.item(entry)


def value(entry: object) -> str:
    """Render a standalone value for a TOML input control."""
    return configuration_item(entry, inline=True).as_string()


def dumps(document: Mapping[str, object]) -> str:
    """Serialize configured fields in their supplied order, omitting absent fields."""
    result = tomlkit.document()
    _populate(result, document, ())
    return result.as_string()
