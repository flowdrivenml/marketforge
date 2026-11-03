from __future__ import annotations

SPOT_L2_COMMON = {
    "event_timestamp": {
        "source": "timestamp",
    },
    "side": {
        "source": "side",
        "transform": "map",
        "values": {
            0: "bid",
            1: "ask",
        },
    },
    "price": {
        "source": "price",
    },
    "quantity": {
        "source": "amount",
    },
    "sequence_start": {
        "source": "begin_id",
    },
    "sequence_count": {
        "source": "merged_count",
    },
}


PERPETUAL_L2_COMMON = {
    "event_timestamp": {
        "source": "timestamp",
    },
    "side": {
        "source": "size",
        "transform": "sign_to_book_side",
        "positive": "bid",
        "negative": "ask",
    },
    "price": {
        "source": "price",
    },
    "quantity": {
        "source": "size",
        "transform": "abs",
    },
    "sequence_start": {
        "source": "begin_id",
    },
    "sequence_count": {
        "source": "merged_count",
    },
}


NORMALIZATION_RULES = [
    # ------------------------------------------------------------------
    # Trades
    # ------------------------------------------------------------------
    {
        "format_code": "GATE-T1",
        "dataset": "trade",
        "instrument_type": "spot",
        "market_category": "spot",
        "target_schema": "trade",
        "rules": {
            "event_timestamp": {
                "source": "timestamp",
            },
            "trade_id": {
                "source": "dealid",
            },
            "side": {
                "source": "side",
                "transform": "map",
                "values": {
                    1: "sell",
                    2: "buy",
                },
            },
            "price": {
                "source": "price",
            },
            "quantity": {
                "source": "amount",
            },
        },
        "notes": (
            "Spot amount is base-asset quantity. "
            "Gate side encoding 1=sell and 2=buy is converted to "
            "canonical aggressor side."
        ),
    },
    {
        "format_code": "GATE-T2",
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
                "source": "size",
                "transform": "sign_to_side",
                "positive": "buy",
                "negative": "sell",
            },
            "price": {
                "source": "price",
            },
            "quantity": {
                "source": "size",
                "transform": "abs",
            },
        },
        "notes": (
            "Signed size encodes both direction and quantity. "
            "Absolute size is normalized through instrument_specs."
        ),
    },
    {
        "format_code": "GATE-T2",
        "dataset": "trade",
        "instrument_type": "perpetual",
        "market_category": "inverse",
        "target_schema": "trade",
        "rules": {
            "event_timestamp": {
                "source": "timestamp",
            },
            "trade_id": {
                "source": "trade_id",
            },
            "side": {
                "source": "size",
                "transform": "sign_to_side",
                "positive": "buy",
                "negative": "sell",
            },
            "price": {
                "source": "price",
            },
            "quantity": {
                "source": "size",
                "transform": "abs",
            },
        },
        "notes": (
            "Signed size encodes both direction and quantity. "
            "Absolute contract quantity is normalized using inverse "
            "instrument_specs."
        ),
    },
    # ------------------------------------------------------------------
    # Spot L2 Snapshot
    # ------------------------------------------------------------------
    {
        "format_code": "GATE-B1",
        "dataset": "l2",
        "instrument_type": "spot",
        "market_category": "spot",
        "target_schema": "l2_snapshot",
        "rules": {
            **SPOT_L2_COMMON,
            "event_filter": {
                "source": "action",
                "equals": "set",
            },
            "snapshot": {
                "type": "group_rows",
                "group_by": "timestamp",
            },
        },
        "notes": (
            "Initial set rows collectively form one canonical snapshot. "
            "The raw snapshot is represented by many physical rows."
        ),
    },
    # ------------------------------------------------------------------
    # Spot L2 Updates
    # ------------------------------------------------------------------
    {
        "format_code": "GATE-B1",
        "dataset": "l2",
        "instrument_type": "spot",
        "market_category": "spot",
        "target_schema": "l2_update",
        "rules": {
            **SPOT_L2_COMMON,
            "event_filter": {
                "source": "action",
                "in": [
                    "make",
                    "take",
                ],
            },
            "update_semantics": {
                "type": "relative_level",
                "action_source": "action",
                "add_action": "make",
                "subtract_action": "take",
                "emit": "resulting_absolute_level",
                "zero_result": "delete",
                "nonzero_result": "set",
            },
        },
        "notes": (
            "Gate make/take updates are interpreted against maintained "
            "book state. Canonical output describes resulting absolute "
            "level state rather than preserving make/take operations."
        ),
    },
    # ------------------------------------------------------------------
    # Linear Perpetual L2 Snapshot
    # ------------------------------------------------------------------
    {
        "format_code": "GATE-B2",
        "dataset": "l2",
        "instrument_type": "perpetual",
        "market_category": "linear",
        "target_schema": "l2_snapshot",
        "rules": {
            **PERPETUAL_L2_COMMON,
            "event_filter": {
                "source": "action",
                "equals": "set",
            },
            "snapshot": {
                "type": "group_rows",
                "group_by": "timestamp",
            },
        },
        "notes": (
            "Initial set rows collectively form one canonical snapshot. "
            "Book side is derived from the sign of size and canonical "
            "quantity uses abs(size)."
        ),
    },
    # ------------------------------------------------------------------
    # Linear Perpetual L2 Updates
    # ------------------------------------------------------------------
    {
        "format_code": "GATE-B2",
        "dataset": "l2",
        "instrument_type": "perpetual",
        "market_category": "linear",
        "target_schema": "l2_update",
        "rules": {
            **PERPETUAL_L2_COMMON,
            "event_filter": {
                "source": "action",
                "in": [
                    "make",
                    "take",
                ],
            },
            "update_semantics": {
                "type": "relative_level",
                "action_source": "action",
                "add_action": "make",
                "subtract_action": "take",
                "emit": "resulting_absolute_level",
                "zero_result": "delete",
                "nonzero_result": "set",
            },
        },
        "notes": (
            "make/take operations are applied to maintained book state. "
            "The emitted canonical update contains resulting absolute "
            "quantity and set/delete action."
        ),
    },
    # ------------------------------------------------------------------
    # Inverse Perpetual L2 Snapshot
    # ------------------------------------------------------------------
    {
        "format_code": "GATE-B2",
        "dataset": "l2",
        "instrument_type": "perpetual",
        "market_category": "inverse",
        "target_schema": "l2_snapshot",
        "rules": {
            **PERPETUAL_L2_COMMON,
            "event_filter": {
                "source": "action",
                "equals": "set",
            },
            "snapshot": {
                "type": "group_rows",
                "group_by": "timestamp",
            },
        },
        "notes": (
            "Initial set rows collectively form one canonical snapshot. "
            "Signed size determines book side; absolute quantity is "
            "normalized using inverse instrument_specs."
        ),
    },
    # ------------------------------------------------------------------
    # Inverse Perpetual L2 Updates
    # ------------------------------------------------------------------
    {
        "format_code": "GATE-B2",
        "dataset": "l2",
        "instrument_type": "perpetual",
        "market_category": "inverse",
        "target_schema": "l2_update",
        "rules": {
            **PERPETUAL_L2_COMMON,
            "event_filter": {
                "source": "action",
                "in": [
                    "make",
                    "take",
                ],
            },
            "update_semantics": {
                "type": "relative_level",
                "action_source": "action",
                "add_action": "make",
                "subtract_action": "take",
                "emit": "resulting_absolute_level",
                "zero_result": "delete",
                "nonzero_result": "set",
            },
        },
        "notes": (
            "make/take operations are applied to maintained book state. "
            "Canonical output contains resulting absolute quantity rather "
            "than Gate-specific relative update semantics."
        ),
    },
]
