from __future__ import annotations

TRADE_COMMON = {
    "event_timestamp": {
        "source": "created_time",
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
        "source": "size",
    },
    "optional": {
        "is_rpi": {
            "source": "source",
            "transform": "equals",
            "value": 1,
        },
    },
}


L2_COMMON = {
    "event_timestamp": {
        "source": "ts",
    },
    "instrument": {
        "source": "instId",
        "lookup": "catalog",
    },
    "bids": {
        "source": "bids",
        "price_index": 0,
        "quantity_index": 1,
        "order_count_index": 2,
    },
    "asks": {
        "source": "asks",
        "price_index": 0,
        "quantity_index": 1,
        "order_count_index": 2,
    },
}


ABSOLUTE_L2_UPDATE = {
    "type": "absolute_level",
    "zero_quantity": "delete",
    "nonzero_quantity": "set",
}


NORMALIZATION_RULES = [
    # ------------------------------------------------------------------
    # Trades
    # ------------------------------------------------------------------
    {
        "format_code": "OKX-T1",
        "dataset": "trade",
        "instrument_type": "spot",
        "market_category": "spot",
        "target_schema": "trade",
        "rules": {
            **TRADE_COMMON,
        },
        "notes": (
            "Spot size is base-asset quantity. "
            "source=1 identifies RPI trades and is normalized to is_rpi."
        ),
    },
    {
        "format_code": "OKX-T1",
        "dataset": "trade",
        "instrument_type": "perpetual",
        "market_category": "linear",
        "target_schema": "trade",
        "rules": {
            **TRADE_COMMON,
        },
        "notes": (
            "Linear perpetual size is contract quantity and is normalized "
            "using instrument_specs. source=1 is retained as is_rpi."
        ),
    },
    {
        "format_code": "OKX-T1",
        "dataset": "trade",
        "instrument_type": "perpetual",
        "market_category": "inverse",
        "target_schema": "trade",
        "rules": {
            **TRADE_COMMON,
        },
        "notes": (
            "Inverse perpetual size is contract quantity and is normalized "
            "using instrument_specs. source=1 is retained as is_rpi."
        ),
    },
    {
        "format_code": "OKX-T1",
        "dataset": "trade",
        "instrument_type": "option",
        "market_category": "option",
        "target_schema": "trade",
        "rules": {
            **TRADE_COMMON,
            "instrument": {
                "source": "instrument_name",
                "lookup": "catalog",
            },
            "identity": {
                "fields": [
                    "instrument_name",
                    "trade_id",
                ],
            },
            "input_ordering": {
                "sort_by": "created_time",
                "order": "ascending",
            },
        },
        "notes": (
            "Option archives contain trades from multiple instruments. "
            "instrument_name is resolved through the catalog. trade_id "
            "is scoped to instrument, so raw identity uses "
            "(instrument_name, trade_id). Raw rows are not globally "
            "chronological and are sorted by created_time before "
            "canonical emission."
        ),
    },
    # ------------------------------------------------------------------
    # Spot L2
    # ------------------------------------------------------------------
    {
        "format_code": "OKX-B1",
        "dataset": "l2",
        "instrument_type": "spot",
        "market_category": "spot",
        "target_schema": "l2_snapshot",
        "rules": {
            **L2_COMMON,
            "event_filter": {
                "source": "action",
                "equals": "snapshot",
            },
            "snapshot": {
                "type": "single_event",
            },
        },
        "notes": (
            "Each snapshot event replaces the complete known book. "
            "Level quantities are Spot base quantities."
        ),
    },
    {
        "format_code": "OKX-B1",
        "dataset": "l2",
        "instrument_type": "spot",
        "market_category": "spot",
        "target_schema": "l2_update",
        "rules": {
            **L2_COMMON,
            "event_filter": {
                "source": "action",
                "equals": "update",
            },
            "update_semantics": ABSOLUTE_L2_UPDATE,
        },
        "notes": (
            "Each update level supplies the resulting absolute quantity. "
            "Zero quantity deletes the level; non-zero quantity sets it."
        ),
    },
    # ------------------------------------------------------------------
    # Linear Perpetual L2
    # ------------------------------------------------------------------
    {
        "format_code": "OKX-B1",
        "dataset": "l2",
        "instrument_type": "perpetual",
        "market_category": "linear",
        "target_schema": "l2_snapshot",
        "rules": {
            **L2_COMMON,
            "event_filter": {
                "source": "action",
                "equals": "snapshot",
            },
            "snapshot": {
                "type": "single_event",
            },
        },
        "notes": (
            "Snapshot level quantities represent derivative contracts "
            "and are normalized using instrument_specs."
        ),
    },
    {
        "format_code": "OKX-B1",
        "dataset": "l2",
        "instrument_type": "perpetual",
        "market_category": "linear",
        "target_schema": "l2_update",
        "rules": {
            **L2_COMMON,
            "event_filter": {
                "source": "action",
                "equals": "update",
            },
            "update_semantics": ABSOLUTE_L2_UPDATE,
        },
        "notes": (
            "Update quantities are absolute contract quantities. "
            "Zero quantity deletes the level; non-zero quantity sets it."
        ),
    },
    # ------------------------------------------------------------------
    # Inverse Perpetual L2
    # ------------------------------------------------------------------
    {
        "format_code": "OKX-B1",
        "dataset": "l2",
        "instrument_type": "perpetual",
        "market_category": "inverse",
        "target_schema": "l2_snapshot",
        "rules": {
            **L2_COMMON,
            "event_filter": {
                "source": "action",
                "equals": "snapshot",
            },
            "snapshot": {
                "type": "single_event",
            },
        },
        "notes": (
            "Snapshot level quantities represent inverse contracts "
            "and are normalized using instrument_specs."
        ),
    },
    {
        "format_code": "OKX-B1",
        "dataset": "l2",
        "instrument_type": "perpetual",
        "market_category": "inverse",
        "target_schema": "l2_update",
        "rules": {
            **L2_COMMON,
            "event_filter": {
                "source": "action",
                "equals": "update",
            },
            "update_semantics": ABSOLUTE_L2_UPDATE,
        },
        "notes": (
            "Update quantities are absolute inverse contract quantities. "
            "Zero quantity deletes the level; non-zero quantity sets it."
        ),
    },
    # ------------------------------------------------------------------
    # Option L2
    # ------------------------------------------------------------------
    {
        "format_code": "OKX-B2",
        "dataset": "l2",
        "instrument_type": "option",
        "market_category": "option",
        "target_schema": "l2_snapshot",
        "rules": {
            **L2_COMMON,
            "event_filter": {
                "source": "action",
                "equals": "snapshot",
            },
            "snapshot": {
                "type": "single_event",
            },
            "archive_scope": {
                "type": "per_member_instrument",
            },
        },
        "notes": (
            "Option archive members represent separate instruments. "
            "Each contract is normalized independently. Actual populated "
            "depth may be substantially below the format maximum."
        ),
    },
    {
        "format_code": "OKX-B2",
        "dataset": "l2",
        "instrument_type": "option",
        "market_category": "option",
        "target_schema": "l2_update",
        "rules": {
            **L2_COMMON,
            "event_filter": {
                "source": "action",
                "equals": "update",
            },
            "update_semantics": ABSOLUTE_L2_UPDATE,
            "archive_scope": {
                "type": "per_member_instrument",
            },
        },
        "notes": (
            "Each option contract maintains independent book state. "
            "Zero quantity deletes a level and non-zero quantity sets "
            "the resulting absolute quantity."
        ),
    },
]
