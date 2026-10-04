from __future__ import annotations

NORMALIZATION_RULES = [
    {
        "format_code": "BINANCE-T1",
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
                "source": "is_buyer_maker",
                "transform": "map",
                "values": {
                    False: "buy",
                    True: "sell",
                },
            },
            "price": {
                "source": "price",
            },
            "quantity": {
                "source": "quantity",
            },
        },
        "notes": (
            "Spot quantity is base-asset quantity. "
            "Quote quantity exists in the raw format but canonical "
            "quantity normalization is driven by instrument_specs. "
            "is_best_match is not retained in the canonical Trade schema."
        ),
    },
    {
        "format_code": "BINANCE-T2",
        "dataset": "trade",
        "instrument_type": "perpetual",
        "market_category": "linear",
        "target_schema": "trade",
        "rules": {
            "event_timestamp": {
                "source": "time",
            },
            "trade_id": {
                "source": "id",
            },
            "side": {
                "source": "is_buyer_maker",
                "transform": "map",
                "values": {
                    False: "buy",
                    True: "sell",
                },
            },
            "price": {
                "source": "price",
            },
            "quantity": {
                "source": "qty",
            },
        },
        "notes": (
            "Linear perpetual qty is normalized according to "
            "instrument_specs. quote_qty is redundant with canonical "
            "quantity conversion and is not independently retained."
        ),
    },
    {
        "format_code": "BINANCE-T3",
        "dataset": "trade",
        "instrument_type": "perpetual",
        "market_category": "inverse",
        "target_schema": "trade",
        "rules": {
            "event_timestamp": {
                "source": "time",
            },
            "trade_id": {
                "source": "id",
            },
            "side": {
                "source": "is_buyer_maker",
                "transform": "map",
                "values": {
                    False: "buy",
                    True: "sell",
                },
            },
            "price": {
                "source": "price",
            },
            "quantity": {
                "source": "qty",
            },
        },
        "notes": (
            "Inverse perpetual qty is contract quantity. "
            "Contract value and contract-value asset are obtained from "
            "instrument_specs rather than hardcoded. Raw base_qty is "
            "redundant with canonical quantity conversion and is not "
            "independently retained."
        ),
    },
]
