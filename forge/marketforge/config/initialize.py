from __future__ import annotations

from psycopg import Connection

from marketforge.config.defaults import DEFAULT_PROCESSING_PROFILES
from marketforge.config.repository import ConfigRepository
from marketforge.config.services import ConfigService


def ensure_default_processing_profiles(
    conn: Connection,
) -> int:
    """
    Ensure MarketForge's default processing profiles exist.

    Existing profiles are never modified.

    Returns the number of profiles newly created.
    """

    repository = ConfigRepository(conn)
    service = ConfigService(repository)

    created = 0

    for profile in DEFAULT_PROCESSING_PROFILES:
        existing = repository.get_processing_profile(profile.name)

        if existing is not None:
            continue

        service.create_processing_profile(profile)

        created += 1

    return created
