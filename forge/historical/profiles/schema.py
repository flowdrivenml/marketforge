"""Typed, local profile metadata for CLI, documentation and future editors."""

import re
from difflib import get_close_matches
from functools import cache
from importlib.resources import files
from typing import Literal

from pydantic import BaseModel, ConfigDict, JsonValue

from forge.historical.profiles.documentation import field_url
from forge.shared.errors import MarketForgeError


class Metadata(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class Choice(Metadata):
    value: JsonValue
    meaning: str


class FieldVariant(Metadata):
    type: str
    required: Literal["required", "optional", "conditional"]
    default: JsonValue
    description: str
    allowed_values: tuple[Choice, ...]
    constraints: tuple[str, ...]
    dependencies: tuple[str, ...]
    related: tuple[str, ...]
    profile_types: tuple[Literal["trades", "depth"], ...]
    source_formats: tuple[Literal["csv", "jsonl", "xlsx"], ...]
    examples: tuple[str, ...]
    control: Literal["select", "toggle", "text", "group", "integer", "structured"]


class Field(Metadata):
    path: str
    title: str
    section: str
    variants: tuple[FieldVariant, ...]
    children: tuple[str, ...]

    @property
    def documentation(self) -> str:
        return field_url(self.path, self.section)


class Section(Metadata):
    id: str
    title: str
    description: str


class ProfileSchema(Metadata):
    metadata_version: Literal[1]
    schema_version: Literal[2]
    title: str
    authority: str
    limitations: tuple[str, ...]
    sections: tuple[Section, ...]
    fields: tuple[Field, ...]
    rust_sources: dict[str, str]

    def field(self, path: str, *, profile_type: str | None = None) -> Field:
        fields = self.parameters(profile_type=profile_type)
        for field in fields:
            if path == field.path:
                return field
        array_path = re.sub(r"\[\d+\]", "[]", path)
        for field in fields:
            pattern = re.escape(field.path)
            pattern = pattern.replace(re.escape("<name>"), r'(?:[^.\s"]+|"[^"]+")')
            pattern = pattern.replace(re.escape("<feed>"), r"(?:snapshot|incremental)")
            if re.fullmatch(pattern, array_path):
                return field
        raise unknown("field", path, [field.path for field in fields])

    def section(self, name: str) -> Section:
        for section in self.sections:
            if name == section.id:
                return section
        raise unknown("section", name, [section.id for section in self.sections])

    def parameters(
        self,
        *,
        section: str | None = None,
        profile_type: str | None = None,
        source_format: str | None = None,
    ) -> tuple[Field, ...]:
        """Return fields with only the variants applicable to the requested context."""
        if section is not None:
            self.section(section)
        if profile_type == "trades" and source_format == "xlsx":
            return ()
        result: list[Field] = []
        for field in self.fields:
            if section is not None and field.section != section:
                continue
            variants = tuple(
                variant
                for variant in field.variants
                if (profile_type is None or profile_type in variant.profile_types)
                and (source_format is None or source_format in variant.source_formats)
            )
            if profile_type or source_format:
                variants = tuple(
                    variant.model_copy(
                        update={
                            "profile_types": (profile_type,)
                            if profile_type
                            else variant.profile_types,
                            "source_formats": (
                                tuple(
                                    item for item in variant.source_formats if item == source_format
                                )
                                if source_format
                                else tuple(
                                    item
                                    for item in variant.source_formats
                                    if profile_type != "trades" or item != "xlsx"
                                )
                            ),
                        }
                    )
                    for variant in variants
                )
            if variants:
                result.append(field.model_copy(update={"variants": variants}))
        return tuple(result)


def unknown(label: str, value: str, candidates: list[str]) -> MarketForgeError:
    nearby = get_close_matches(value, candidates, n=3, cutoff=0.38)
    suggestion = f" Did you mean {', '.join(nearby)}?" if nearby else ""
    return MarketForgeError(
        f"Unknown profile schema {label} {value!r}.{suggestion} "
        "Run mf normalize profiles schema to browse supported parameters."
    )


@cache
def load_schema() -> ProfileSchema:
    """The generated artifact is shipped with Forge. No Engine or database is opened."""
    source = files("forge.historical.profiles").joinpath("schema.json")
    return ProfileSchema.model_validate_json(source.read_text(encoding="utf-8"))
