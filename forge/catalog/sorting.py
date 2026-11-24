"""Ordering for catalogue identities and historical specification evidence."""

from forge.shared.sorting import SortField, SortSpec

EVIDENCE_FIELDS = {
    "id": SortField("evidence ID", columns=("s.id",)),
    "observed": SortField("source observation time", "desc", ("s.source_observed_at",)),
    "kind": SortField("evidence kind", columns=("s.evidence_kind",)),
}
SPEC_SHOW = SortSpec(EVIDENCE_FIELDS, "id", "s.id", {"time": "observed"})
CATALOG = {
    "exchange": SortSpec(
        {
            "code": SortField("exchange code", columns=("code",)),
            "name": SortField("exchange name", columns=("name",)),
            "id": SortField("exchange ID", columns=("id",)),
        },
        "code",
        "id",
    ),
    "instrument": SortSpec(
        {
            "identity": SortField(
                "instrument identity",
                columns=("e.code", "i.instrument_type", "i.market_category", "i.symbol"),
            ),
            "symbol": SortField("symbol", columns=("i.symbol",)),
            "exchange": SortField("exchange", columns=("e.code",)),
            "type": SortField("instrument type", columns=("i.instrument_type",)),
            "status": SortField("status", columns=("i.status",)),
            "id": SortField("instrument ID", columns=("i.id",)),
        },
        "identity",
        "e.code,i.instrument_type,i.market_category,i.symbol,i.id",
        {"Declaration": "symbol", "Exchange": "exchange", "Type": "type", "State": "status"},
    ),
    "specs": SortSpec(
        {
            "identity": SortField("instrument identity", columns=("a.identity_key",)),
            **EVIDENCE_FIELDS,
        },
        "identity",
        "a.identity_key,s.id",
        {"time": "observed"},
    ),
}
