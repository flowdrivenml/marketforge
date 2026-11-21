"""One historical request, compiled into the existing service inputs."""

import tomllib
from typing import Self

from pydantic import Field, model_validator

from forge.historical.archives.config import AcquisitionConfig, AcquisitionInterval, ArchiveRequest
from forge.historical.archives.selection import request_key
from forge.historical.archives.sources.base import UnsupportedRequestError
from forge.historical.archives.sources.exchanges import validate_request
from forge.historical.merge.config import TimelineOrdering, unix_ns
from forge.historical.merge.service import Recipe
from forge.historical.normalization.config import NormalizationRecipe, NormalizationSource
from forge.shared.config.models import SettingsModel


class NormalizeOptions(SettingsModel, frozen=True):
    ranking_policy: str = Field(default="turnover", pattern=r"^[a-zA-Z0-9_-]+$")


class MergeOptions(SettingsModel, frozen=True):
    timeline_ordering: TimelineOrdering | None = None


class PipelineRecipe(SettingsModel, frozen=True):
    version: int = Field(ge=1, le=1)
    interval: AcquisitionInterval
    requests: list[ArchiveRequest] = Field(min_length=1)
    normalize: NormalizeOptions = Field(default_factory=NormalizeOptions)
    merge: MergeOptions = Field(default_factory=MergeOptions)

    @model_validator(mode="after")
    def consistent(self) -> Self:
        if len({request_key(request) for request in self.requests}) != len(self.requests):
            raise ValueError("Pipeline requests must be unique.")
        for bound in (self.interval.start, self.interval.end):
            if not -(2**63) <= unix_ns(bound) < 2**63:
                raise ValueError(
                    "Pipeline timestamps must fit the merge engine's nanosecond range."
                )
        for request in self.requests:
            try:
                validate_request(request)
            except UnsupportedRequestError as error:
                raise ValueError(
                    f"{request.exchange} {request.scope.identifier}: {error}"
                ) from None
        self.normalization()
        return self

    def acquisition(self) -> AcquisitionConfig:
        return AcquisitionConfig(interval=self.interval, requests=self.requests)

    def normalization(self) -> NormalizationRecipe:
        return NormalizationRecipe(
            interval=self.interval,
            requests=[
                NormalizationSource.model_validate(request.model_dump())
                for request in self.requests
            ],
            ranking_policy=self.normalize.ranking_policy,
        )

    def merging(self, ids: list[int]) -> Recipe:
        return Recipe(
            dataset_ids=ids,
            start_ns=unix_ns(self.interval.start),
            end_ns=unix_ns(self.interval.end),
            timeline_ordering=self.merge.timeline_ordering,
        )


def load_recipe(submitted: bytes) -> PipelineRecipe:
    return PipelineRecipe.model_validate(tomllib.loads(submitted.decode("utf-8")))
