from __future__ import annotations

SNAPSHOT_RULES = {
    "event_timestamp": {
        "source": "timestamp",
    },
    "bids": {
        "source": "bids",
        "decode": "json",
        "price_index": 0,
        "quantity_index": 1,
    },
    "asks": {
        "source": "asks",
        "decode": "json",
        "price_index": 0,
        "quantity_index": 1,
    },
    "snapshot": {
        "type": "single_row",
    },
    "input_ordering": {
        "sort_by": "timestamp",
        "order": "ascending",
    },
}


NORMALIZATION_RULES = [
    # ------------------------------------------------------------------
    # Trades
    # ------------------------------------------------------------------
    {
        "format_code": "BITGET-T1",
        "dataset": "trade",
        "instrument_type": "spot",
        "market_category": "spot",
        "target_schema": "trade",
        "rules": {
            "event_timestamp": {
                "source": "timestamp",
            },
            "trade_id": {
                "source": "trade_id",
            },
            "side": {
                "source": "side",
                "transform": "casefold",
            },
            "price": {
                "source": "price",
            },
            "quantity": {
                "source": "size(base)",
            },
        },
        "notes": (
            "Spot size(base) is the authoritative raw quantity. "
            "volume(quote) is redundant with canonical quantity "
            "normalization and is not independently retained."
        ),
    },
    {
        "format_code": "BITGET-T1",
        "dataset": "trade",
        "instrument_type": "perpetual",
        "market_category": "linear",
        "target_schema": "trade",
        "rules": {
            "event_timestamp": {
                "source": "timestamp",
            },
            "trade_id": {
                "source": "trade_id",
            },
            "side": {
                "source": "side",
                "transform": "casefold",
            },
            "price": {
                "source": "price",
            },
            "quantity": {
                "source": "size(base)",
            },
        },
        "notes": (
            "Linear perpetual trade files use the same physical format "
            "as Spot. size(base) is used as the authoritative quantity "
            "source and normalized according to instrument_specs. "
            "volume(quote) is not independently retained."
        ),
    },
    # ------------------------------------------------------------------
    # Spot L2
    # ------------------------------------------------------------------
    {
        "format_code": "BITGET-B1",
        "dataset": "l2",
        "instrument_type": "spot",
        "market_category": "spot",
        "target_schema": "l2_snapshot",
        "rules": {
            **SNAPSHOT_RULES,
        },
        "notes": (
            "Each raw XLSX row is an independent full snapshot. "
            "Raw rows are not chronologically ordered and must be "
            "sorted by timestamp before canonical emission. "
            "No L2Update records are generated."
        ),
    },
    # ------------------------------------------------------------------
    # Linear Perpetual L2
    # ------------------------------------------------------------------
    {
        "format_code": "BITGET-B1",
        "dataset": "l2",
        "instrument_type": "perpetual",
        "market_category": "linear",
        "target_schema": "l2_snapshot",
        "rules": {
            **SNAPSHOT_RULES,
        },
        "notes": (
            "Each raw row is an independent full snapshot. "
            "Raw rows must be sorted by timestamp. Level quantities "
            "are normalized using derivative instrument_specs. "
            "No L2Update records are generated."
        ),
    },
    # ------------------------------------------------------------------
    # Inverse Perpetual L2
    # ------------------------------------------------------------------
    {
        "format_code": "BITGET-B1",
        "dataset": "l2",
        "instrument_type": "perpetual",
        "market_category": "inverse",
        "target_schema": "l2_snapshot",
        "rules": {
            **SNAPSHOT_RULES,
        },
        "notes": (
            "Each raw row is an independent snapshot. "
            "Raw rows must be sorted by timestamp. Snapshot depth "
            "may vary. Level quantities are normalized using inverse "
            "instrument_specs. No L2Update records are generated."
        ),
    },
]
