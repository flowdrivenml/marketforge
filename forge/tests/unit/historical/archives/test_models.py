"""Stable identities for discovered historical archives."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from forge.historical.archives.config import AcquisitionInterval, ArchiveRequest
from forge.historical.archives.models import DiscoveredArchive


@pytest.fixture
def archive() -> DiscoveredArchive:
    request = ArchiveRequest.model_validate(
        {
            "exchange": "bybit",
            "instrument_type": "spot",
            "market_category": "spot",
            "data_type": "trades",
            "scope": {
                "kind": "instrument",
                "identifier": "BTCUSDT",
            },
        }
    )

    return DiscoveredArchive(
        request=request,
        source_object_key="spot/BTCUSDT/2026-09-01.csv.gz",
        filename="BTCUSDT-2026-09-01.csv.gz",
        interval=AcquisitionInterval(
            start=datetime(2026, 9, 1, tzinfo=UTC),
            end=datetime(2026, 9, 2, tzinfo=UTC),
        ),
        download_url="https://exchange.test/archive?signature=first",
    )


def test_url_refresh_preserves_archive_identity(
    archive: DiscoveredArchive,
) -> None:
    payload = archive.model_dump()
    payload["download_url"] = "https://exchange.test/archive?signature=second"
    payload["url_expires_at"] = datetime(2026, 9, 3, tzinfo=UTC)

    refreshed = DiscoveredArchive.model_validate(payload)

    assert refreshed.identity == archive.identity
    assert refreshed.download_url != archive.download_url


def test_different_objects_have_different_identities(
    archive: DiscoveredArchive,
) -> None:
    payload = archive.model_dump()
    payload["source_object_key"] = "spot/BTCUSDT/2026-09-02.csv.gz"

    other = DiscoveredArchive.model_validate(payload)

    assert other.scope_key == archive.scope_key
    assert other.identity != archive.identity


def test_archive_dates_do_not_change_market_scope(
    archive: DiscoveredArchive,
) -> None:
    payload = archive.model_dump()
    payload["interval"] = {
        "start": datetime(2026, 10, 1, tzinfo=UTC),
        "end": datetime(2026, 10, 2, tzinfo=UTC),
    }

    other = DiscoveredArchive.model_validate(payload)

    assert other.scope_key == archive.scope_key


def test_market_scope_distinguishes_instruments(
    archive: DiscoveredArchive,
) -> None:
    request_payload = archive.request.model_dump()
    request_payload["scope"] = {
        "kind": "instrument",
        "identifier": "ETHUSDT",
    }

    payload = archive.model_dump()
    payload["request"] = request_payload

    other = DiscoveredArchive.model_validate(payload)

    assert other.scope_key != archive.scope_key


def test_data_type_is_a_separate_identity_component(
    archive: DiscoveredArchive,
) -> None:
    request_payload = archive.request.model_dump()
    request_payload["data_type"] = "order_book_l2"

    payload = archive.model_dump()
    payload["request"] = request_payload

    other = DiscoveredArchive.model_validate(payload)

    assert other.scope_key == archive.scope_key
    assert other.identity != archive.identity


def test_expiry_requires_download_url(archive: DiscoveredArchive) -> None:
    payload = archive.model_dump()
    payload["download_url"] = None
    payload["url_expires_at"] = datetime(2026, 9, 3, tzinfo=UTC)

    with pytest.raises(ValidationError, match="requires download_url"):
        DiscoveredArchive.model_validate(payload)


def test_rejects_naive_expiry(archive: DiscoveredArchive) -> None:
    payload = archive.model_dump()
    payload["url_expires_at"] = datetime(2026, 9, 3)

    with pytest.raises(ValidationError):
        DiscoveredArchive.model_validate(payload)
