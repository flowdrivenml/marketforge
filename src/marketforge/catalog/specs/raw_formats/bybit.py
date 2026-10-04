from __future__ import annotations

PERPETUAL_TRADE_SCHEMA = {
    "header": True,
    "granularity": "daily",
    "ordering": "chronological",
    "fields": [
        {
            "name": "timestamp",
            "type": "decimal",
            "representation": "unix_epoch",
            "unit": "seconds",
            "precision": "up_to_4_fractional_digits",
        },
        {
            "name": "symbol",
            "type": "string",
        },
        {
            "name": "side",
            "type": "string",
        },
        {
            "name": "size",
            "type": "decimal",
        },
        {
            "name": "price",
            "type": "decimal",
        },
        {
            "name": "tickDirection",
            "type": "string",
        },
        {
            "name": "trdMatchID",
            "type": "uuid/string",
        },
        {
            "name": "grossValue",
            "type": "decimal",
        },
        {
            "name": "homeNotional",
            "type": "decimal",
        },
        {
            "name": "foreignNotional",
            "type": "decimal",
        },
        {
            "name": "RPI",
            "type": "integer/bool",
        },
    ],
}


STANDARD_L2_SCHEMA = {
    "granularity": "daily",
    "ordering": "chronological",
    "maximum_depth_per_side": 200,
    "fields": [
        {
            "name": "topic",
            "type": "string",
        },
        {
            "name": "ts",
            "type": "integer",
            "representation": "unix_epoch",
            "unit": "milliseconds",
            "precision": "millisecond",
        },
        {
            "name": "type",
            "type": "string",
            "values": [
                "snapshot",
                "delta",
            ],
        },
        {
            "name": "data.s",
            "type": "string",
        },
        {
            "name": "data.b",
            "type": "array",
            "level_schema": [
                "price",
                "quantity",
            ],
        },
        {
            "name": "data.a",
            "type": "array",
            "level_schema": [
                "price",
                "quantity",
            ],
        },
        {
            "name": "data.u",
            "type": "integer",
        },
        {
            "name": "data.seq",
            "type": "integer",
        },
        {
            "name": "cts",
            "type": "integer",
            "representation": "unix_epoch",
            "unit": "milliseconds",
            "precision": "millisecond",
        },
    ],
    "order_book": {
        "event_model": "snapshot_and_incremental_delta",
        "snapshot_depth": {
            "bids": 200,
            "asks": 200,
        },
        "update_id": "data.u",
        "cross_sequence": "data.seq",
        "delta_delete_quantity": "0",
        "reconstruction_required": True,
    },
}


RAW_FORMATS = [
    {
        "format_code": "BYBIT-T1",
        "dataset": "trade",
        "instrument_type": "spot",
        "market_category": "spot",
        "container_format": "csv",
        "compression": "gzip",
        "record_format": "rows",
        "schema": {
            "header": True,
            "granularity": "daily",
            "ordering": "chronological",
            "fields": [
                {
                    "name": "id",
                    "type": "integer",
                },
                {
                    "name": "timestamp",
                    "type": "integer",
                    "representation": "unix_epoch",
                    "unit": "milliseconds",
                    "precision": "millisecond",
                },
                {
                    "name": "price",
                    "type": "decimal",
                },
                {
                    "name": "volume",
                    "type": "decimal",
                },
                {
                    "name": "side",
                    "type": "string",
                },
                {
                    "name": "rpi",
                    "type": "integer/bool",
                },
            ],
        },
        "notes": (
            "Daily GZIP CSV. id is a sequential row/trade identifier "
            "within the archive. side represents taker side."
        ),
    },
    {
        "format_code": "BYBIT-T2",
        "dataset": "trade",
        "instrument_type": "perpetual",
        "market_category": "linear",
        "container_format": "csv",
        "compression": "gzip",
        "record_format": "rows",
        "schema": PERPETUAL_TRADE_SCHEMA,
        "notes": (
            "Daily GZIP CSV. Physical parser is shared with Bybit "
            "inverse perpetual trades. Linear and inverse notional "
            "semantics differ and must be handled during normalization."
        ),
    },
    {
        "format_code": "BYBIT-T2",
        "dataset": "trade",
        "instrument_type": "perpetual",
        "market_category": "inverse",
        "container_format": "csv",
        "compression": "gzip",
        "record_format": "rows",
        "schema": PERPETUAL_TRADE_SCHEMA,
        "notes": (
            "Daily GZIP CSV. Physical parser is shared with Bybit "
            "linear perpetual trades. Linear and inverse notional "
            "semantics differ and must be handled during normalization."
        ),
    },
    {
        "format_code": "BYBIT-T3",
        "dataset": "trade",
        "instrument_type": "option",
        "market_category": "option",
        "container_format": "csv",
        "compression": "zip",
        "record_format": "rows",
        "schema": {
            "header": True,
            "granularity": "daily",
            "ordering": "chronological",
            "fields": [
                {
                    "name": "trade_id",
                    "type": "uuid/string",
                },
                {
                    "name": "trade_seq",
                    "type": "integer",
                },
                {
                    "name": "timestamp",
                    "type": "integer",
                    "representation": "unix_epoch",
                    "unit": "milliseconds",
                    "precision": "millisecond",
                },
                {
                    "name": "instrument_name",
                    "type": "string",
                },
                {
                    "name": "direction",
                    "type": "string",
                },
                {
                    "name": "price",
                    "type": "decimal",
                },
                {
                    "name": "amount",
                    "type": "decimal",
                },
                {
                    "name": "iv",
                    "type": "decimal",
                },
                {
                    "name": "index_price",
                    "type": "decimal",
                },
                {
                    "name": "mark_price",
                    "type": "decimal",
                },
                {
                    "name": "mark_iv",
                    "type": "decimal",
                },
            ],
        },
        "notes": (
            "Daily ZIP containing option trades. trade_seq may be "
            "shared by multiple trades; trade_id is the unique trade "
            "identifier. Includes trade IV, mark IV, index price, and "
            "mark price."
        ),
    },
    {
        "format_code": "BYBIT-B1",
        "dataset": "l2",
        "instrument_type": "spot",
        "market_category": "spot",
        "container_format": "jsonl",
        "compression": "zip",
        "record_format": "events",
        "schema": STANDARD_L2_SCHEMA,
        "notes": (
            "Daily ZIP containing JSON Lines. Snapshot plus incremental "
            "delta stream with 200 levels per side. Start from a snapshot "
            "and apply deltas using update ID data.u."
        ),
    },
    {
        "format_code": "BYBIT-B1",
        "dataset": "l2",
        "instrument_type": "perpetual",
        "market_category": "linear",
        "container_format": "jsonl",
        "compression": "zip",
        "record_format": "events",
        "schema": STANDARD_L2_SCHEMA,
        "notes": (
            "Same physical L2 representation and reconstruction model "
            "as Bybit Spot BYBIT-B1."
        ),
    },
    {
        "format_code": "BYBIT-B1",
        "dataset": "l2",
        "instrument_type": "perpetual",
        "market_category": "inverse",
        "container_format": "jsonl",
        "compression": "zip",
        "record_format": "events",
        "schema": STANDARD_L2_SCHEMA,
        "notes": (
            "Same physical L2 representation and reconstruction model "
            "as Bybit Spot and Linear Perpetual BYBIT-B1."
        ),
    },
    {
        "format_code": "BYBIT-B2",
        "dataset": "l2",
        "instrument_type": "option",
        "market_category": "option",
        "container_format": "jsonl",
        "compression": "zip",
        "record_format": "events",
        "schema": {
            "granularity": "daily",
            "ordering": "chronological",
            "archive_members": "multiple_option_contracts",
            "maximum_depth_per_side": 25,
            "fields": [
                {
                    "name": "topic",
                    "type": "string",
                },
                {
                    "name": "ts",
                    "type": "integer",
                    "representation": "unix_epoch",
                    "unit": "milliseconds",
                    "precision": "millisecond",
                },
                {
                    "name": "type",
                    "type": "string",
                    "values": [
                        "snapshot",
                        "delta",
                    ],
                },
                {
                    "name": "id",
                    "type": "string",
                },
                {
                    "name": "data.s",
                    "type": "string",
                },
                {
                    "name": "data.b",
                    "type": "array",
                    "level_schema": [
                        "price",
                        "quantity",
                    ],
                },
                {
                    "name": "data.a",
                    "type": "array",
                    "level_schema": [
                        "price",
                        "quantity",
                    ],
                },
                {
                    "name": "data.u",
                    "type": "integer",
                },
                {
                    "name": "data.seq",
                    "type": "integer",
                },
                {
                    "name": "cts",
                    "type": "integer",
                    "representation": "unix_epoch",
                    "unit": "milliseconds",
                    "precision": "millisecond",
                },
            ],
            "order_book": {
                "event_model": "snapshot_and_incremental_delta",
                "maximum_depth_per_side": 25,
                "update_id": "data.u",
                "cross_sequence": "data.seq",
                "event_id": "id",
                "delta_delete_quantity": "0",
                "reconstruction_required": True,
                "reconstruction_scope": "per_option_contract",
            },
        },
        "notes": (
            "Daily ZIP contains independent members for many option "
            "contracts. Each contract is reconstructed independently. "
            "Depth 25 is a maximum; snapshots may contain fewer populated "
            "levels. Event representation resembles BYBIT-B1 but packaging, "
            "depth, and top-level event id differ."
        ),
    },
]
