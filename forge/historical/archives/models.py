"""Validated records produced by historical archive discovery."""

import hashlib
import json
from typing import Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from forge.historical.archives.config import AcquisitionInterval, ArchiveRequest


def archive_scope_document(request: ArchiveRequest) -> dict[str, str]:
    """Describe archive scope independently of dates and download URLs."""
    document = {
        "instrument_type": request.instrument_type,
        "market_category": request.market_category,
        "scope_kind": request.scope.kind,
        "scope_identifier": request.scope.identifier,
    }

    # Preserve existing incremental identities. Snapshots get a distinct scope,
    # used by both remote archive registration and discovery caching.
    if (
        request.exchange == "gateio"
        and request.data_type == "order_book_l2"
        and request.source_options is not None
        and request.source_options.feed == "snapshot"
    ):
        document["source_feed"] = "snapshot"

    return document


def archive_scope_key(request: ArchiveRequest) -> str:
    """Produce a deterministic registry key for a market scope."""
    canonical = json.dumps(
        archive_scope_document(request),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class DiscoveredArchive(BaseModel, frozen=True):
    """One archive observed by an exchange adapter.

    request describes the archive's actual content scope, not necessarily the
    broader selection that caused discovery to inspect it.

    interval describes the archive's coverage, using an exclusive end.

    source_object_key must remain stable when a signed download URL changes.
    It should come from the upstream object identity or a deterministic adapter
    key, never from temporary authentication parameters.
    """

    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        validate_default=True,
    )

    request: ArchiveRequest
    source_object_key: str = Field(min_length=1, pattern=r"\S")
    filename: str = Field(min_length=1, pattern=r"\S")
    interval: AcquisitionInterval

    download_url: str | None = Field(
        default=None,
        min_length=1,
        pattern=r"\S",
    )
    url_expires_at: AwareDatetime | None = None

    source_etag: str | None = None
    source_last_modified: AwareDatetime | None = None

    @model_validator(mode="after")
    def validate_download_location(self) -> Self:
        if self.url_expires_at is not None and self.download_url is None:
            raise ValueError("url_expires_at requires download_url.")

        return self

    @property
    def scope_key(self) -> str:
        """Return the market scope component of the registry identity."""
        return archive_scope_key(self.request)

    @property
    def identity(self) -> tuple[str, str, str, str]:
        """Return the application equivalent of the registry's unique key."""
        return (
            self.request.exchange,
            self.scope_key,
            self.request.data_type,
            self.source_object_key,
        )
