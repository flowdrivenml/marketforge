"""Adapt the existing recipe schemas to the shared Textual form."""

import tomllib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from typing import cast

from pydantic import BaseModel, ValidationError

from forge.historical.archives.config import AcquisitionConfig, ArchiveSource
from forge.historical.archives.sources.base import UnsupportedRequestError
from forge.historical.archives.sources.exchanges import HISTORICAL_EXCHANGES, validate_request
from forge.historical.conversion.recipes import Recipe as ConversionRecipe
from forge.historical.imports.models import ImportConfig
from forge.historical.merge.config import DerivedRecipe
from forge.historical.merge.service import Recipe as MergeRecipe
from forge.historical.normalization.config import (
    ExactRecipe,
    NormalizationRecipe,
    PreparationRequest,
)
from forge.historical.pipelines.config import PipelineRecipe
from forge.historical.registration.models import Recipe as RegistrationRecipe
from forge.shared.config.toml import value
from forge.tui.actions import Field, Value, Values
from forge.tui.controller import document

Payload = dict[str, object]
Path = tuple[str | int, ...]
GROUPS = {
    "interval": "Historical interval",
    "coverage": "Declared coverage",
    "identity": "Trading venue and instrument",
    "registration": "Registration",
    "source": "Source",
    "selection": "Source selection",
    "normalize": "Normalization · required stage",
    "merge": "Merge · required stage",
    "convert": "Conversion · optional stage",
    "conversion": "Conversion policy",
    "specifications": "Historical specifications",
    "resources": "Advanced Options",
    "version": "Advanced Options",
    "source_key": "Source provenance",
    "source_kind": "Source provenance",
    "files": "Source files",
    "root": "Source files",
    "patterns": "Source files",
    "max_files": "Advanced Options",
    "format_profile": "Source format",
    "binding": "Advanced Options",
    "reference_stream_ids": "Reference streams",
    "reference_publication_id": "Reference streams",
    "stream_ranks": "Advanced Options",
    "timeline_ordering": "Merge policy",
    "start_ns": "Historical interval",
    "end_ns": "Historical interval",
    "dataset_ids": "Physical files",
}


def model_for(family: str, data: Mapping[str, object]) -> type[BaseModel] | None:
    if family == "normalize":
        if "archive_pins" in data:
            return ExactRecipe
        if {"inputs", "job_id"} & data.keys():
            return PreparationRequest
        return NormalizationRecipe
    if family == "merge":
        return DerivedRecipe if "selection" in data else MergeRecipe
    return {
        "acquisition": AcquisitionConfig,
        "pipeline": PipelineRecipe,
        "convert": ConversionRecipe,
        "acquisition import": ImportConfig,
        "acquisition register": RegistrationRecipe,
    }.get(family)


def validation_issues(error: ValidationError, keys: Mapping[Path, str]) -> dict[str, str]:
    issues: dict[str, str] = {}
    for item in error.errors(include_url=False, include_input=False, include_context=False):
        path = item["loc"]
        key = keys.get(path)
        if key is None and path and path[-1] == "interval":
            key = keys.get((*path, "end"))
        if key is None:
            key = next(
                (
                    keys[path[:length]]
                    for length in range(len(path) - 1, 0, -1)
                    if path[:length] in keys
                ),
                "",
            )
        message = item["msg"].removeprefix("Value error, ")
        issues[key] = f"{issues[key]} · {message}" if key in issues else message
    return issues


@dataclass(frozen=True)
class Control:
    field: Field
    initial: Value
    read: str
    path: Path


@dataclass
class RecipeForm:
    model: type[BaseModel]
    original: Payload
    new: bool = False
    count: int = 0
    controls: list[Control] = field(default_factory=list[Control], init=False)
    schema: Payload = field(default_factory=dict[str, object], init=False)

    def __post_init__(self) -> None:
        self.schema = self.model.model_json_schema()
        for name in ("requests", "inputs"):
            entries = self.original.get(name)
            if isinstance(entries, list):
                self.count = len(cast(list[object], entries))

    @property
    def can_add(self) -> bool:
        return self.model in {
            AcquisitionConfig,
            PipelineRecipe,
            NormalizationRecipe,
            RegistrationRecipe,
            PreparationRequest,
        }

    def resolve(self, schema: Mapping[str, object]) -> Mapping[str, object]:
        ref = schema.get("$ref")
        if isinstance(ref, str):
            return document(document(self.schema.get("$defs")).get(ref.rsplit("/", 1)[1]))
        variants = schema.get("anyOf")
        if isinstance(variants, list):
            members = [document(item) for item in cast(list[object], variants)]
            concrete = [item for item in members if item.get("type") != "null"]
            if len(concrete) == 1:
                return {**schema, **self.resolve(concrete[0])}
        return schema

    def build(self, pending: Values | None = None) -> tuple[tuple[Field, ...], Values]:
        self.controls.clear()
        self.walk(self.schema, self.original)
        fields = tuple(control.field for control in self.controls)
        preset = {control.field.key: control.initial for control in self.controls}
        return fields, {**preset, **(pending or {})}

    def add(self, pending: Values) -> tuple[tuple[Field, ...], Values]:
        self.count = max(1, self.count) + 1
        fields, preset = self.build(pending)
        if self.new:
            return (
                Field("reference", "Recipe name", required=True, group="Recipe"),
                *fields,
            ), preset
        return fields, preset

    def walk(
        self,
        schema: Mapping[str, object],
        data: object,
        path: Path = (),
        group: str = "",
        required: bool = True,
        gates: tuple[str, ...] = (),
    ) -> None:
        schema = self.resolve(schema)
        name = str(path[-1]) if path else ""
        key = "--".join(map(str, path))
        if len(path) == 1 or name == "resources":
            group = GROUPS.get(name, "Required inputs" if required else "Processing options")
        props = document(schema.get("properties"))
        needed = schema.get("required", [])
        if props:
            if not required:
                self.controls.append(
                    Control(
                        Field(
                            key,
                            {
                                "source_options": "Configure archive feed",
                                "normalize": "Override normalization defaults",
                                "merge": "Override merge defaults",
                                "convert": "Enable conversion",
                            }.get(
                                name,
                                f"Enable {name.replace('_', ' ') if not name.isdecimal() else 'entry ' + str(int(name) + 1)}",
                            ),
                            "check",
                            group=group,
                            visible_when=partial(self.feed_options, key.removesuffix(name), gates)
                            if name == "source_options"
                            else self.visible(gates),
                        ),
                        data is not None,
                        "gate",
                        path,
                    )
                )
                gates = (*gates, key)
            if len(path) == 2 and path[0] == "requests":
                order = ("exchange", "instrument_type", "market_category", "scope", "data_type")
                props = {**{child: props[child] for child in order}, **props}
            for child, definition in props.items():
                self.walk(
                    document(definition),
                    document(data).get(child),
                    (*path, child),
                    group,
                    isinstance(needed, list) and child in needed,
                    gates,
                )
            return
        item = self.resolve(document(schema.get("items")))
        if (
            schema.get("type") == "array"
            and item.get("properties")
            and name in {"requests", "inputs"}
        ):
            entries = cast(list[object], data) if isinstance(data, list) else []
            for index in range(max(1, self.count)):
                self.walk(
                    item,
                    entries[index] if index < len(entries) else None,
                    (*path, index),
                    f"{'Request' if name == 'requests' else 'Input'} {index + 1}",
                    index == 0,
                    gates,
                )
                if index and index >= len(entries):
                    gate = next(
                        control for control in self.controls if control.path == (*path, index)
                    )
                    self.controls[self.controls.index(gate)] = Control(
                        gate.field, True, "gate", gate.path
                    )
            return
        choices: tuple[str, ...] = ()
        enums = schema.get("enum")
        if isinstance(enums, list) and all(
            isinstance(item, str) for item in cast(list[object], enums)
        ):
            choices = tuple(cast(list[str], enums))
        if self.model in {AcquisitionConfig, PipelineRecipe} and name == "exchange":
            choices = HISTORICAL_EXCHANGES
            if isinstance(data, str) and data not in choices:
                choices = (*choices, data)
        constant = schema.get("const")
        if self.new and data is None and required and type(constant) is int:
            data = constant
        if self.new and data is None and self.model is PipelineRecipe and path == ("version",):
            if schema.get("minimum") == schema.get("maximum"):
                data = schema.get("minimum")
        kind = schema.get("type")
        read = "toml" if kind not in {"string", "integer", "number"} and not choices else "scalar"
        if schema.get("format") == "date-time":
            read = "datetime"
        if self.model is MergeRecipe and path == ("dataset_ids",):
            read = "id-list"
        if name in {"tick_size", "qty_step", "min_qty", "max_qty", "min_notional", "max_notional"}:
            read = "scalar"
        label = {
            "start_ns": "Start (Unix nanoseconds, inclusive)",
            "end_ns": "End (Unix nanoseconds, exclusive)",
            "dataset_ids": "Registered primary-file IDs",
        }.get(name, name.replace("_", " ").capitalize())
        if schema.get("format") == "date-time":
            label = {"start": "Start UTC (inclusive)", "end": "End UTC (exclusive)"}.get(
                name, label
            )
        placeholder = ""
        help_parts: list[str] = []
        if schema.get("format") == "date-time":
            placeholder = "YYYY-MM-DDTHH:MM:SSZ"
            help_parts.append("Use an ISO 8601 timestamp with a timezone. Z denotes UTC.")
        elif read == "toml":
            help_parts.append("Enter a TOML value. Lists use brackets and quoted strings.")
        if "default" in schema and schema["default"] is not None:
            placeholder = f"Default: {schema['default']}"
            help_parts.append(placeholder)
        for constraint, wording in (
            ("minimum", "Minimum"),
            ("maximum", "Maximum"),
            ("minLength", "Minimum length"),
            ("minItems", "Minimum entries"),
            ("description", ""),
        ):
            if constraint in schema:
                help_parts.append(f"{wording} {schema[constraint]}".strip())
        initial = "" if data is None else value(data) if read == "toml" else str(data)
        if isinstance(data, int) and not isinstance(data, bool) and read == "scalar":
            initial = str(data)
        if read == "id-list":
            initial = (
                ", ".join(map(str, cast(list[object], data))) if isinstance(data, list) else ""
            )
            help_parts.append(
                "Use Find to add registered files. Separate manually entered IDs with commas."
            )
        conditional = None
        if self.model is NormalizationRecipe and len(path) == 3 and name == "source_key":
            conditional = partial(self.equals, key.removesuffix(name) + "instrument_type", "equity")
        elif name == "reference_stream_ids":
            conditional = partial(
                self.supplied, key.removesuffix(name) + "reference_publication_id"
            )
        self.controls.append(
            Control(
                Field(
                    key,
                    label,
                    "choice"
                    if choices
                    else "integer"
                    if kind == "integer"
                    else "number"
                    if kind == "number"
                    else "text",
                    choices,
                    required=required,
                    required_when=conditional,
                    group=group,
                    help="\n".join(help_parts),
                    placeholder=placeholder,
                    visible_when=partial(
                        self.feed_options, key.partition("source_options")[0], gates
                    )
                    if "source_options" in path
                    else self.visible(gates),
                ),
                initial,
                read,
                path,
            )
        )

    @staticmethod
    def feed_options(prefix: str, gates: tuple[str, ...], values: Mapping[str, Value]) -> bool:
        return all(
            values.get(gate) is True for gate in gates
        ) and ArchiveSource.supports_source_options(
            str(values.get(prefix + "exchange", "")), str(values.get(prefix + "data_type", ""))
        )

    @staticmethod
    def equals(key: str, expected: str, values: Mapping[str, Value]) -> bool:
        return values.get(key) == expected

    @staticmethod
    def supplied(key: str, values: Mapping[str, Value]) -> bool:
        return bool(values.get(key))

    @staticmethod
    def visible(gates: tuple[str, ...]) -> Callable[[Mapping[str, Value]], bool] | None:
        def active(values: Mapping[str, Value]) -> bool:
            return all(values.get(gate) is True for gate in gates)

        return active if gates else None

    def payload(self, values: Values) -> tuple[Payload, dict[str, str]]:
        result: Payload = {}
        issues: dict[str, str] = {}
        for control in self.controls:
            key, path = control.field.key, control.path
            if key not in values:
                continue
            raw = values[key]
            if raw == "" or (control.read == "gate" and raw is not True):
                continue
            parsed: object = raw
            if control.read == "gate":
                parsed = {}
            if control.read == "toml":
                try:
                    parsed = tomllib.loads(f"value = {raw}")["value"]
                except (ValueError, KeyError):
                    issues[key] = "Enter a valid TOML value."
                    continue
            if control.read == "id-list":
                try:
                    parsed = [int(item.strip()) for item in str(raw).split(",")]
                except ValueError:
                    issues[key] = "Select registered file IDs or enter comma-separated integers."
                    continue
            if control.read == "datetime":
                try:
                    parsed = datetime.fromisoformat(str(raw))
                except ValueError:
                    issues[key] = "Enter an ISO 8601 timestamp with a timezone."
                    continue
            if not isinstance(path[-1], int):
                self.put(result, path, parsed)
        return result, issues

    @staticmethod
    def put(result: Payload, path: Path, value: object) -> None:
        node: Payload = result
        for index, part in enumerate(path[:-1]):
            if isinstance(part, int):
                continue
            child = path[index + 1]
            if isinstance(child, int):
                entries = cast(list[Payload], node.setdefault(part, []))
                # Render order is also the authored array order. Disabled entries are omitted.
                if not entries or entries[-1].get("__form_index__") != child:
                    entries.append({"__form_index__": child})
                node = entries[-1]
            else:
                node = cast(Payload, node.setdefault(part, {}))
        last = path[-1]
        if isinstance(last, str):
            node[last] = value

    def candidate(self, values: Values) -> tuple[Payload, dict[str, str]]:
        data, issues = self.payload(values)
        for name in ("requests", "inputs"):
            if isinstance(entries := data.get(name), list):
                for item in cast(list[Payload], entries):
                    item.pop("__form_index__", None)
        if issues:
            return data, issues
        keys = {control.path: control.field.key for control in self.controls}
        try:
            model = self.model.model_validate(data)
        except ValidationError as error:
            return data, validation_issues(error, keys)
        if isinstance(model, AcquisitionConfig):
            for index, request in enumerate(model.requests):
                try:
                    validate_request(request)
                except UnsupportedRequestError as error:
                    issues[f"requests--{index}--exchange"] = str(error)
        return data, issues


def summarize(data: Payload, state: str, path: str) -> dict[str, object]:
    interval = document(data.get("interval") or data.get("coverage"))
    summary: dict[str, object] = {"Status": state or "Saved"}
    if "start" in interval and "end" in interval:
        summary["Requested period"] = f"{interval['start']} → {interval['end']} (end exclusive)"
    else:
        from forge.datasets.query import utc

        start_ns = interval.get("start_ns", data.get("start_ns"))
        end_ns = interval.get("end_ns", data.get("end_ns"))
        if type(start_ns) is int and type(end_ns) is int:
            summary["Requested period"] = f"{utc(start_ns)} → {utc(end_ns)} (end exclusive)"
    requests = data.get("requests")
    if isinstance(requests, list):
        labels: list[str] = []
        for item in cast(list[object], requests):
            request = document(item)
            scope = document(request.get("scope"))
            labels.append(
                " · ".join(
                    str(x)
                    for x in (
                        request.get("exchange"),
                        request.get("instrument_type"),
                        request.get("market_category"),
                        scope.get("identifier"),
                        {"trades": "Trades", "order_book_l2": "Depth"}.get(
                            str(request.get("data_type")), request.get("data_type")
                        ),
                    )
                    if x
                )
            )
        summary["Requests"] = "\n".join(labels)
    source = document(data.get("source") or data.get("identity") or data.get("selection"))
    if source:
        summary["Source"] = (
            " · ".join(
                str(source[key])
                for key in (
                    "kind",
                    "reference",
                    "source",
                    "exchange",
                    "symbol",
                    "provider",
                    "publication_id",
                    "registry_publication_id",
                    "engine_publication_id",
                )
                if key in source
            )
            or "See advanced details for source evidence"
        )
    if root := data.get("root") or source.get("root"):
        summary["Source file root"] = root
    for key in ("files", "patterns"):
        if isinstance(inputs := data.get(key), list) and inputs:
            summary[key.capitalize()] = "\n".join(map(str, cast(list[object], inputs)))
    policy = document(data.get("conversion") or data.get("convert"))
    if "target_quote" in policy:
        summary["Target quote"] = policy["target_quote"]
    for key in (
        "archive_pins",
        "dataset_ids",
        "publication_ids",
        "timeline_ordering",
        "ranking_policy",
    ):
        if key in data:
            val = data[key]
            if key == "archive_pins" and isinstance(val, list):
                val = [document(item).get("archive_id") for item in cast(list[object], val)]
            summary[key.replace("_", " ").capitalize()] = (
                ", ".join(map(str, cast(list[object], val))) if isinstance(val, list) else val
            )
    summary["Configuration"] = path
    return summary


def update_recipe(original: bytes, before: Payload, after: Payload) -> str:
    """Patch only changed values in the existing TOML document."""
    from collections.abc import MutableMapping

    import tomlkit

    def patch(
        table: MutableMapping[str, object], old: Mapping[str, object], new: Mapping[str, object]
    ) -> None:
        for key in old.keys() - new.keys():
            del table[key]
        for key, item in new.items():
            previous = old.get(key)
            if previous == item:
                continue
            current = table.get(key)
            if (
                isinstance(current, MutableMapping)
                and isinstance(previous, dict)
                and isinstance(item, dict)
            ):
                patch(
                    cast(MutableMapping[str, object], current),
                    cast(Payload, previous),
                    cast(Payload, item),
                )
            else:
                table[key] = item

    source = tomlkit.parse(original.decode())
    patch(cast(MutableMapping[str, object], source), before, after)
    return source.as_string()
