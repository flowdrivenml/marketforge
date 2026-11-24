"""Pydantic field discovery and TOML round-tripping for Forge settings."""

import tomlkit
from tomlkit.container import Container, OutOfOrderTableProxy
from tomlkit.items import InlineTable, Table

from forge.shared.config.models import AppSettings
from forge.shared.errors import MarketForgeError
from forge.tui.controller import document

UNITS = {"http.timeout_seconds": "seconds", "http.requests_per_second": "requests per second"}


def application_fields(settings: AppSettings) -> list[dict[str, object]]:
    current = settings.model_dump()
    defaults = AppSettings().model_dump()
    schema = AppSettings.model_json_schema()
    fields: list[dict[str, object]] = []

    def visit(properties: dict[str, object], prefix: str, values: object, initial: object) -> None:
        for name, raw in properties.items():
            entry = document(raw)
            path = prefix + name
            value, default = document(values)[name], document(initial)[name]
            if isinstance(reference := entry.get("$ref"), str):
                nested = schema["$defs"][reference.rsplit("/", 1)[1]]
                visit(nested["properties"], path + ".", value, default)
                continue
            fields.append(
                {
                    "path": path,
                    "current": value,
                    "default": default,
                    "units": UNITS.get(path),
                    "constraints": {
                        key: val for key, val in entry.items() if key not in {"default", "title"}
                    },
                }
            )

    visit(schema["properties"], "", current, defaults)
    return fields


def change_toml(data: bytes, path: str, value: object) -> bytes:
    configuration = tomlkit.parse(data.decode("utf-8"))
    node: Container | Table | InlineTable | OutOfOrderTableProxy = configuration
    members = path.split(".")
    for name in members[:-1]:
        if name not in node:
            if value is None:
                return data
            node[name] = tomlkit.table()
        child = node[name]
        if not isinstance(child, (Table, InlineTable, OutOfOrderTableProxy)):
            raise MarketForgeError(f"Expected a TOML table at {name}.")
        node = child
    key = members[-1]
    if value is None:
        if key in node:
            del node[key]
    else:
        node[key] = value
    return configuration.as_string().encode("utf-8")
