"""Ordering for stored observations and ranking snapshots."""

from forge.shared.sorting import SortField, SortSpec

MARKETS = SortSpec(
    {
        "identity": SortField(
            "market identity",
            columns=("e.code", "i.market_category", "i.instrument_type", "i.symbol"),
        ),
        "symbol": SortField("symbol", columns=("i.symbol",)),
        "exchange": SortField("exchange", columns=("e.code",)),
        "measured": SortField("recorded metric window end", "desc", ("m.measured_at",)),
        "observed": SortField("source observation time", "desc", ("o.source_observed_at",)),
        "fetched": SortField("fetch time", "desc", ("o.fetched_at",)),
        "turnover": SortField(
            "turnover within native denomination",
            "desc",
            ("m.turnover_24h",),
            "m.turnover_denomination ASC NULLS LAST",
        ),
    },
    "identity",
    "e.code,i.market_category,i.instrument_type,i.symbol,i.id",
    {"freshness": "measured", "time": "observed"},
)
SNAPSHOTS = SortSpec(
    {
        "created": SortField("snapshot creation time", "desc", ("created_at",)),
        "policy": SortField("policy", columns=("policy_name",)),
        "currency": SortField("comparison currency", columns=("comparison_currency",)),
        "ranked": SortField("ranked entry count", "desc", ("included_count",)),
        "universe": SortField("universe count", "desc", ("universe_count",)),
        "id": SortField("snapshot ID", columns=("id",)),
    },
    "created",
    "id",
    {
        "time": "created",
        "Policy": "policy",
        "Currency": "currency",
        "Ranked": "ranked",
        "Published UTC": "created",
        "Universe": "universe",
    },
)
ENTRIES = SortSpec(
    {
        "rank": SortField("stored rank", columns=("group_rank", "position")),
        "symbol": SortField("symbol", columns=("symbol",)),
        "reference": SortField("instrument reference", columns=("instrument_reference",)),
        "observed": SortField("retained source observation time", "desc", ("source_observed_at",)),
    },
    "rank",
    "position ASC NULLS LAST, instrument_reference",
    {"time": "observed"},
)
