"""Strict discovery helpers shared by asynchronous exchange adapters."""

import re
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol, cast
from urllib.parse import parse_qs, urlsplit

from forge.historical.archives.config import AcquisitionInterval, ArchiveRequest
from forge.historical.archives.models import DiscoveredArchive
from forge.shared.errors import MarketForgeError


class DiscoveryError(MarketForgeError):
    """An upstream search failed; its coverage must not be cached."""


class UnsupportedRequestError(DiscoveryError):
    """The requested exchange/product/scope cannot be interpreted by this adapter."""


@dataclass(frozen=True, slots=True)
class ProbeEvent:
    status: Literal["checking", "found", "absent", "failed"]
    filename: str


class ArchiveSource(Protocol):
    version: str

    def validate(self, request: ArchiveRequest) -> None: ...

    def discover(
        self, request: ArchiveRequest, interval: AcquisitionInterval
    ) -> AsyncIterator[DiscoveredArchive]: ...


def mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise DiscoveryError("Expected an upstream JSON object.")
    return cast(dict[str, object], value)


def sequence(value: object) -> list[object]:
    if not isinstance(value, list):
        raise DiscoveryError("Expected an upstream JSON array.")
    return cast(list[object], value)


def text(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DiscoveryError("Missing or invalid upstream string field.")
    return value


def check_code(body: dict[str, object], key: str, expected: str) -> None:
    if str(body.get(key)) != expected:
        # Do not echo server messages: they can include signed URLs or credentials.
        raise DiscoveryError(
            "Upstream rejected discovery; check access and endpoint compatibility."
        )


def records(body: dict[str, object], key: str, alternatives: tuple[str, ...]) -> list[object]:
    _check_collection_complete(body)
    value = body.get(key)
    if isinstance(value, dict):
        document = cast(dict[str, object], value)
        _check_collection_complete(document)
        for name in alternatives:
            if name in document:
                return sequence(document[name])
        raise DiscoveryError("Missing upstream result collection.")
    return sequence(value)


def daily(filename: str) -> datetime:
    match = re.search(r"(?<!\d)(\d{4}[-_]?\d{2}[-_]?\d{2})(?!\d)", filename)
    if match is None:
        raise DiscoveryError("Archive filename has no supported daily date.")
    try:
        return datetime.strptime(match[1].replace("-", "").replace("_", ""), "%Y%m%d").replace(
            tzinfo=UTC
        )
    except ValueError:
        raise DiscoveryError("Invalid upstream archive date.") from None


def milliseconds(value: object) -> datetime:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise DiscoveryError("Invalid upstream timestamp.")
    try:
        return datetime.fromtimestamp(float(value) / 1000, UTC)
    except (ValueError, OverflowError, OSError):
        raise DiscoveryError("Invalid upstream timestamp.") from None


def windows(interval: AcquisitionInterval, days: int) -> Iterator[AcquisitionInterval]:
    current = interval.start
    while current < interval.end:
        end = min(current + timedelta(days=days), interval.end)
        yield AcquisitionInterval(start=current, end=end)
        current = end


def archive(
    request: ArchiveRequest,
    filename: str,
    url: str,
    start: datetime,
    *,
    end: datetime | None = None,
    key: str | None = None,
) -> DiscoveredArchive:
    location = urlsplit(url)
    if (
        location.scheme != "https"
        or not location.hostname
        or location.username
        or location.password
    ):
        raise DiscoveryError("Archive URL must be public HTTPS without user credentials.")
    # Hostnames and signatures may change; the source-native path identifies the object.
    expires_at = url_expiration(url)
    return DiscoveredArchive(
        request=request,
        filename=filename,
        source_object_key=key or location.path,
        download_url=url,
        url_expires_at=expires_at,
        interval=AcquisitionInterval(start=start, end=end or start + timedelta(days=1)),
    )


def overlaps(item: DiscoveredArchive, interval: AcquisitionInterval) -> bool:
    return item.interval.start < interval.end and item.interval.end > interval.start


def url_expiration(url: str) -> datetime | None:
    """Recognize common S3/OSS signed URL expiration formats without storing secrets."""
    query = {
        key.lower(): values[0] for key, values in parse_qs(urlsplit(url).query).items() if values
    }
    try:
        if "x-amz-date" in query and "x-amz-expires" in query:
            issued = datetime.strptime(query["x-amz-date"], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
            return issued + timedelta(seconds=int(query["x-amz-expires"]))
        if "expires" in query:
            return datetime.fromtimestamp(int(query["expires"]), UTC)
    except (ValueError, OverflowError, OSError):
        raise DiscoveryError("Malformed signed archive URL expiration.") from None
    return None


def _check_collection_complete(document: dict[str, object]) -> None:
    for key in ("nextPageCursor", "nextCursor", "nextPage", "hasMore", "hasNext"):
        continuation = document.get(key)
        if (
            continuation is not None
            and continuation != ""
            and continuation is not False
            and continuation != 0
        ):
            raise DiscoveryError("Unexpected upstream pagination; refusing incomplete coverage.")
