"""Exact acquisition inputs, keyed by shared request semantics."""

from datetime import UTC, datetime
from pathlib import Path

from pydantic import Field

from forge.historical.archives.config import ArchiveRequest, ArchiveSource, ArchiveSourceOptions
from forge.shared.config.models import SettingsModel
from forge.shared.errors import MarketForgeError


class ArchiveSelectionError(MarketForgeError):
    """Pinned acquisition evidence is unavailable or inconsistent."""


def revision_time(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat() if value is not None else None


def request_key(request: ArchiveSource) -> str:
    """Normalize request identity independent of service indexing."""
    document = request.model_dump(include=set(ArchiveRequest.model_fields))
    if request.exchange == "gateio" and request.data_type == "order_book_l2":
        document["source_options"] = (request.source_options or ArchiveSourceOptions()).model_dump()
    return ArchiveRequest.model_validate(document).model_dump_json()


class AcquiredArchive(SettingsModel, frozen=True):
    request: ArchiveRequest
    local_archive_id: int = Field(gt=0)
    remote_archive_id: int = Field(gt=0)
    relative_path: str
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    source_object_key: str
    source_etag: str | None = None
    source_last_modified: str | None = None


class ArchiveSelection(SettingsModel, frozen=True):
    acquisition_report: Path
    archives: list[AcquiredArchive]
