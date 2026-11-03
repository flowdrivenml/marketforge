from __future__ import annotations

from marketforge.config.models import ProcessingProfile
from marketforge.config.repository import ConfigRepository


class ConfigService:
    """Manage persistent MarketForge processing profiles."""

    def __init__(
        self,
        repository: ConfigRepository,
    ) -> None:
        self.repository = repository

    def list_processing_profiles(
        self,
    ) -> list[ProcessingProfile]:
        return self.repository.list_processing_profiles()

    def get_processing_profile(
        self,
        name: str,
    ) -> ProcessingProfile:
        profile = self.repository.get_processing_profile(name)

        if profile is None:
            raise KeyError(f"Processing profile not found: {name}")

        return profile

    def create_processing_profile(
        self,
        profile: ProcessingProfile,
    ) -> ProcessingProfile:
        existing = self.repository.get_processing_profile(profile.name)

        if existing is not None:
            raise ValueError("Processing profile already exists: " f"{profile.name}")

        return self.repository.create_processing_profile(profile)

    def update_processing_profile(
        self,
        profile: ProcessingProfile,
    ) -> ProcessingProfile:
        existing = self.repository.get_processing_profile(profile.name)

        if existing is None:
            raise KeyError(f"Processing profile not found: " f"{profile.name}")

        return self.repository.update_processing_profile(profile)

    def delete_processing_profile(
        self,
        name: str,
    ) -> None:
        deleted = self.repository.delete_processing_profile(name)

        if not deleted:
            raise KeyError(f"Processing profile not found: {name}")

    def ensure_processing_profile(
        self,
        profile: ProcessingProfile,
    ) -> ProcessingProfile:
        """
        Ensure a processing profile exists.

        Existing profiles are returned unchanged. Defaults never
        overwrite user-modified configuration.
        """

        existing = self.repository.get_processing_profile(profile.name)

        if existing is not None:
            return existing

        return self.repository.create_processing_profile(profile)
