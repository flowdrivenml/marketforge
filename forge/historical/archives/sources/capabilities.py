"""Enumerate historical combinations using the archive adapter's validator."""

from dataclasses import dataclass
from itertools import product
from typing import get_args

from forge.historical.archives.config import ArchiveRequest, ArchiveScope
from forge.historical.archives.sources.base import UnsupportedRequestError
from forge.historical.archives.sources.exchanges import HISTORICAL_EXCHANGES, validate_request
from forge.shared.sorting import SortField, SortSpec, ordered


@dataclass(frozen=True, slots=True)
class SourceCapability:
    exchange: str
    instrument_type: str
    market_category: str
    data_type: str
    scope: str


CAPABILITY_FIELDS = ("exchange", "instrument_type", "market_category", "data_type", "scope")
CAPABILITIES = SortSpec(
    {
        "identity": SortField("source capability identity"),
        **{field: SortField(field.replace("_", " ")) for field in CAPABILITY_FIELDS},
    },
    "identity",
)


def capability_identity(row: SourceCapability) -> tuple[str, ...]:
    return row.exchange, row.instrument_type, row.market_category, row.data_type, row.scope


def source_capabilities(
    *, sort: str | None = None, order: str | None = None
) -> tuple[SourceCapability, ...]:
    """Return accepted combinations without checking availability."""
    ordering = CAPABILITIES.resolve(sort, order)
    fields = ArchiveRequest.model_fields
    combinations = product(
        HISTORICAL_EXCHANGES,
        get_args(fields["instrument_type"].annotation),
        get_args(fields["market_category"].annotation),
        get_args(fields["data_type"].annotation),
        get_args(ArchiveScope.model_fields["kind"].annotation),
    )
    supported: list[SourceCapability] = []
    for exchange, instrument, market, data, scope in combinations:
        request = ArchiveRequest.model_validate(
            {
                "exchange": exchange,
                "instrument_type": instrument,
                "market_category": market,
                "data_type": data,
                "scope": {"kind": scope, "identifier": "BTC"},
            }
        )
        try:
            validate_request(request)
        except UnsupportedRequestError:
            continue
        supported.append(SourceCapability(exchange, instrument, market, data, scope))
    field = ordering.field
    if field == "identity":
        return tuple(ordered(supported, ordering, capability_identity, capability_identity))
    index = CAPABILITY_FIELDS.index(field)
    return tuple(
        ordered(
            supported, ordering, lambda row: capability_identity(row)[index], capability_identity
        )
    )
