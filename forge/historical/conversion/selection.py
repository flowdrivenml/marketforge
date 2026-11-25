"""Reference selection configuration shared by historical recipes."""

from typing import Annotated, Any, Literal, Self
from uuid import UUID

from pydantic import Field, SerializerFunctionWrapHandler, model_serializer, model_validator

from forge.historical.merge.config import DatabaseID
from forge.shared.config.models import SettingsModel


class ReferenceSelection(SettingsModel, frozen=True):
    mode: Literal["automatic", "manual"] = "automatic"
    ranking_policy: str | None = Field(default=None, pattern=r"^[a-zA-Z0-9_-]+$")
    ranking_snapshot: Annotated[UUID, Field(strict=False)] | None = None
    temporal_policy: Literal["historical", "ex_post"] = "historical"

    @model_validator(mode="after")
    def compatible_settings(self) -> Self:
        if self.mode == "manual" and (
            self.ranking_policy or self.ranking_snapshot or self.temporal_policy == "ex_post"
        ):
            raise ValueError("Ranking settings require automatic reference selection.")
        if self.temporal_policy == "ex_post" and self.ranking_snapshot is None:
            raise ValueError("Ex-post reference selection requires an exact ranking snapshot ID.")
        return self


class ReferenceOptions(SettingsModel, frozen=True):
    reference_stream_ids: list[str] = Field(default_factory=list)
    reference_publication_id: DatabaseID | None = None
    reference_selection: ReferenceSelection | None = None

    @model_serializer(mode="wrap")
    def serialize_options(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        document: dict[str, Any] = handler(self)
        if "reference_stream_ids" not in self.model_fields_set:
            document.pop("reference_stream_ids", None)
        if self.reference_selection is None:
            document.pop("reference_selection", None)
        return document

    def selection_policy(self, version: int) -> ReferenceSelection:
        if self.reference_selection is not None:
            return self.reference_selection
        if version == 1 or "reference_stream_ids" in self.model_fields_set:
            return ReferenceSelection(mode="manual")
        return ReferenceSelection()

    def validate_reference_options(self, version: int) -> None:
        policy = self.selection_policy(version)
        ids = self.reference_stream_ids
        if any(not item.strip() for item in ids) or len(ids) != len(set(ids)):
            raise ValueError("Reference stream IDs must be nonblank and unique.")
        if policy.mode == "automatic" and "reference_stream_ids" in self.model_fields_set:
            raise ValueError("Remove reference_stream_ids or select manual reference mode.")
        if policy.mode == "manual" and self.reference_publication_id is not None and not ids:
            raise ValueError(
                "An external reference publication requires explicit trade stream IDs in manual mode."
            )
