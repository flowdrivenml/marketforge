from __future__ import annotations

STANDARD_L2_RULES = {
    "event_timestamp": {
        "source": "cts",
    },
    "system_timestamp": {
        "source": "ts",
    },
    "instrument": {
        "source": "data.s",
        "lookup": "catalog",
    },
    "bids": {
        "source": "data.b",
        "price_index": 0,
        "quantity_index": 1,
    },
    "asks": {
        "source": "data.a",
        "price_index": 0,
        "quantity_index": 1,
    },
    "sequence_start": {
        "source": "data.u",
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
        "format_code": "BYBIT-T1",
        "dataset": "trade",
        "instrument_type": "spot",
        "market_category": "spot",
        "target_schema": "trade",
        "rules": {
            "event_timestamp": {
                "source": "timestamp",
            },
            "trade_id": {
                "source": "id",
            },
            "side": {
                "source": "side",
                "transform": "casefold",
            },
            "price": {
                "source": "price",
            },
            "quantity": {
                "source": "volume",
            },
            "optional": {
                "is_rpi": {
                    "source": "rpi",
                    "transform": "bool",
                },
            },
        },
        "notes": (
            "Spot volume is base-asset quantity. "
            "Raw side is taker/aggressor side. "
            "RPI is retained as canonical is_rpi."
        ),
    },
    {
        "format_code": "BYBIT-T2",
        "dataset": "trade",
        "instrument_type": "perpetual",
        "market_category": "linear",
        "target_schema": "trade",
        "rules": {
            "event_timestamp": {
                "source": "timestamp",
            },
            "trade_id": {
                "source": "trdMatchID",
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
                    "source": "RPI",
                    "transform": "bool",
                },
            },
        },
        "notes": (
            "Linear perpetual size is normalized using instrument_specs. "
            "homeNotional, foreignNotional, and grossValue are not used "
            "as authoritative canonical quantity fields."
        ),
    },
    {
        "format_code": "BYBIT-T2",
        "dataset": "trade",
        "instrument_type": "perpetual",
        "market_category": "inverse",
        "target_schema": "trade",
        "rules": {
            "event_timestamp": {
                "source": "timestamp",
            },
            "trade_id": {
                "source": "trdMatchID",
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
                    "source": "RPI",
                    "transform": "bool",
                },
            },
        },
        "notes": (
            "Inverse perpetual size is normalized using instrument_specs. "
            "Contract interpretation must not be inferred from "
            "homeNotional, foreignNotional, or hardcoded contract values."
        ),
    },
    {
        "format_code": "BYBIT-T3",
        "dataset": "trade",
        "instrument_type": "option",
        "market_category": "option",
        "target_schema": "trade",
        "rules": {
            "event_timestamp": {
                "source": "timestamp",
            },
            "instrument": {
                "source": "instrument_name",
                "lookup": "catalog",
            },
            "trade_id": {
                "source": "trade_id",
            },
            "sequence": {
                "source": "trade_seq",
            },
            "side": {
                "source": "direction",
                "transform": "casefold",
            },
            "price": {
                "source": "price",
            },
            "quantity": {
                "source": "amount",
            },
            "optional": {
                "trade_iv": {
                    "source": "iv",
                },
                "mark_iv": {
                    "source": "mark_iv",
                },
                "index_price": {
                    "source": "index_price",
                },
                "mark_price": {
                    "source": "mark_price",
                },
            },
        },
        "notes": (
            "Option archives contain multiple instruments, so "
            "instrument_name is resolved through the catalog. "
            "trade_id is the trade identifier and trade_seq is retained "
            "separately as canonical sequence."
        ),
    },
    # ------------------------------------------------------------------
    # Spot L2
    # ------------------------------------------------------------------
    {
        "format_code": "BYBIT-B1",
        "dataset": "l2",
        "instrument_type": "spot",
        "market_category": "spot",
        "target_schema": "l2_snapshot",
        "rules": {
            **STANDARD_L2_RULES,
            "event_filter": {
                "source": "type",
                "equals": "snapshot",
            },
            "snapshot": {
                "type": "single_event",
            },
        },
        "notes": (
            "A raw snapshot event directly produces one canonical "
            "L2Snapshot. Bid and ask quantities are normalized using "
            "instrument_specs."
        ),
    },
    {
        "format_code": "BYBIT-B1",
        "dataset": "l2",
        "instrument_type": "spot",
        "market_category": "spot",
        "target_schema": "l2_update",
        "rules": {
            **STANDARD_L2_RULES,
            "event_filter": {
                "source": "type",
                "equals": "delta",
            },
            "update_semantics": ABSOLUTE_L2_UPDATE,
        },
        "notes": (
            "Each raw delta level is normalized independently. "
            "Non-zero quantity produces canonical set; zero quantity "
            "produces canonical delete."
        ),
    },
    # ------------------------------------------------------------------
    # Linear Perpetual L2
    # ------------------------------------------------------------------
    {
        "format_code": "BYBIT-B1",
        "dataset": "l2",
        "instrument_type": "perpetual",
        "market_category": "linear",
        "target_schema": "l2_snapshot",
        "rules": {
            **STANDARD_L2_RULES,
            "event_filter": {
                "source": "type",
                "equals": "snapshot",
            },
            "snapshot": {
                "type": "single_event",
            },
        },
        "notes": (
            "Same physical snapshot representation as BYBIT-B1 Spot. "
            "Level quantities are interpreted using derivative "
            "instrument_specs."
        ),
    },
    {
        "format_code": "BYBIT-B1",
        "dataset": "l2",
        "instrument_type": "perpetual",
        "market_category": "linear",
        "target_schema": "l2_update",
        "rules": {
            **STANDARD_L2_RULES,
            "event_filter": {
                "source": "type",
                "equals": "delta",
            },
            "update_semantics": ABSOLUTE_L2_UPDATE,
        },
        "notes": (
            "Non-zero quantities set the resulting level quantity. "
            "Zero quantity deletes the level."
        ),
    },
    # ------------------------------------------------------------------
    # Inverse Perpetual L2
    # ------------------------------------------------------------------
    {
        "format_code": "BYBIT-B1",
        "dataset": "l2",
        "instrument_type": "perpetual",
        "market_category": "inverse",
        "target_schema": "l2_snapshot",
        "rules": {
            **STANDARD_L2_RULES,
            "event_filter": {
                "source": "type",
                "equals": "snapshot",
            },
            "snapshot": {
                "type": "single_event",
            },
        },
        "notes": (
            "Same physical snapshot representation as other BYBIT-B1 "
            "markets. Inverse quantity conversion is resolved through "
            "instrument_specs."
        ),
    },
    {
        "format_code": "BYBIT-B1",
        "dataset": "l2",
        "instrument_type": "perpetual",
        "market_category": "inverse",
        "target_schema": "l2_update",
        "rules": {
            **STANDARD_L2_RULES,
            "event_filter": {
                "source": "type",
                "equals": "delta",
            },
            "update_semantics": ABSOLUTE_L2_UPDATE,
        },
        "notes": (
            "Non-zero quantities set the resulting level quantity. "
            "Zero quantity deletes the level. Inverse quantity "
            "conversion uses instrument_specs."
        ),
    },
    # ------------------------------------------------------------------
    # Option L2
    # ------------------------------------------------------------------
    {
        "format_code": "BYBIT-B2",
        "dataset": "l2",
        "instrument_type": "option",
        "market_category": "option",
        "target_schema": "l2_snapshot",
        "rules": {
            **STANDARD_L2_RULES,
            "event_filter": {
                "source": "type",
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
            "Each option contract is reconstructed independently. "
            "A snapshot event produces one canonical L2Snapshot for "
            "that option instrument."
        ),
    },
    {
        "format_code": "BYBIT-B2",
        "dataset": "l2",
        "instrument_type": "option",
        "market_category": "option",
        "target_schema": "l2_update",
        "rules": {
            **STANDARD_L2_RULES,
            "event_filter": {
                "source": "type",
                "equals": "delta",
            },
            "update_semantics": ABSOLUTE_L2_UPDATE,
            "archive_scope": {
                "type": "per_member_instrument",
            },
        },
        "notes": (
            "Each option contract maintains independent book state. "
            "Non-zero quantity produces canonical set and zero quantity "
            "produces canonical delete."
        ),
    },
]
