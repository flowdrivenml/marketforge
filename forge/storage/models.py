"""Validated read-only storage selections and interval diagnostics."""

from datetime import UTC, datetime
from typing import Literal, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator


class StorageFilters(BaseModel):
    model_config = ConfigDict(extra="ignore")
    exchange: str | None = None
    market: Literal["spot", "linear", "inverse", "option"] | None = None
    instrument_type: Literal["spot", "perpetual", "future", "option"] | None = None
    instrument: str | None = None
    scope_kind: Literal["instrument", "family", "base_coin"] | None = None
    match: str | None = None
    base_asset: str | None = None
    quote_asset: str | None = None
    data_type: Literal["trades", "order_book_l2"] | None = None
    feed: Literal["snapshot", "incremental", "unknown"] | None = None
    origin: Literal["remote", "external"] | None = None
    status: str | None = None
    start: AwareDatetime | None = None
    end: AwareDatetime | None = None
    limit: int = Field(default=100, ge=1, le=10000)
    offset: int = Field(default=0, ge=0, le=2**63 - 1)

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if self.start is not None and self.end is not None and self.start >= self.end:
            raise ValueError("start must precede exclusive end.")
        if self.start is not None:
            self.start = self.start.astimezone(UTC)
        if self.end is not None:
            self.end = self.end.astimezone(UTC)
        return self


def interval(start: object, end: object) -> tuple[tuple[datetime, datetime] | None, str | None]:
    if start is None or end is None:
        return None, "unknown interval: start or end is missing"
    if (
        not isinstance(start, datetime)
        or not isinstance(end, datetime)
        or start.tzinfo is None
        or end.tzinfo is None
    ):
        return None, "invalid interval: timezone-aware timestamps required"
    if start >= end:
        return None, "invalid interval: start must precede end"
    return (start.astimezone(UTC), end.astimezone(UTC)), None
