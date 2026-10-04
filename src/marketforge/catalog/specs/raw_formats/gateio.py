from __future__ import annotations

PERPETUAL_TRADE_SCHEMA = {
    "header": False,
    "granularity": "monthly",
    "ordering": "chronological",
    "fields": [
        {
            "position": 1,
            "name": "timestamp",
            "type": "decimal",
            "representation": "unix_epoch",
            "unit": "seconds",
            "precision": "microsecond",
            "fractional_digits": 6,
        },
        {
            "position": 2,
            "name": "trade_id",
            "type": "integer",
        },
        {
            "position": 3,
            "name": "price",
            "type": "decimal",
        },
        {
            "position": 4,
            "name": "size",
            "type": "signed_integer",
        },
    ],
}


PERPETUAL_L2_SCHEMA = {
    "header": False,
    "granularity": "hourly",
    "fields": [
        {
            "position": 1,
            "name": "timestamp",
            "type": "decimal",
            "representation": "unix_epoch",
            "unit": "seconds",
            "snapshot_precision": "second",
            "update_precision": "100ms",
        },
        {
            "position": 2,
            "name": "action",
            "type": "string",
            "values": [
                "set",
                "make",
                "take",
            ],
        },
        {
            "position": 3,
            "name": "price",
            "type": "decimal",
        },
        {
            "position": 4,
            "name": "size",
            "type": "signed_decimal",
        },
        {
            "position": 5,
            "name": "begin_id",
            "type": "integer",
        },
        {
            "position": 6,
            "name": "merged_count",
            "type": "integer",
        },
    ],
    "order_book": {
        "event_model": "row_snapshot_and_incremental_updates",
        "snapshot_action": "set",
        "update_actions": [
            "make",
            "take",
        ],
        "side_encoding": "size_sign",
        "sequence": {
            "begin_id": "begin_id",
            "merged_count": "merged_count",
            "continuity_rule": (
                "next.begin_id = " "current.begin_id + current.merged_count"
            ),
        },
        "reconstruction_required": True,
    },
}


RAW_FORMATS = [
    {
        "format_code": "GATE-T1",
        "dataset": "trade",
        "instrument_type": "spot",
        "market_category": "spot",
        "container_format": "csv",
        "compression": "gzip",
        "record_format": "rows",
        "schema": {
            "header": False,
            "granularity": "monthly",
            "ordering": "chronological",
            "fields": [
                {
                    "position": 1,
                    "name": "timestamp",
                    "type": "decimal",
                    "representation": "unix_epoch",
                    "unit": "seconds",
                    "precision": "microsecond",
                    "fractional_digits": 6,
                },
                {
                    "position": 2,
                    "name": "dealid",
                    "type": "integer",
                },
                {
                    "position": 3,
                    "name": "price",
                    "type": "decimal",
                },
                {
                    "position": 4,
                    "name": "amount",
                    "type": "decimal",
                },
                {
                    "position": 5,
                    "name": "side",
                    "type": "integer",
                },
            ],
        },
        "notes": (
            "Monthly GZIP containing headerless CSV. "
            "Timestamps are Unix seconds with six fractional digits. "
            "The raw side field uses Gate-specific integer encoding."
        ),
    },
    {
        "format_code": "GATE-T2",
        "dataset": "trade",
        "instrument_type": "perpetual",
        "market_category": "linear",
        "container_format": "csv",
        "compression": "gzip",
        "record_format": "rows",
        "schema": PERPETUAL_TRADE_SCHEMA,
        "notes": (
            "Monthly GZIP containing headerless CSV. "
            "size is signed and combines quantity magnitude with trade "
            "direction. Physical parser is shared with Gate inverse "
            "perpetual trades. Contract interpretation belongs to "
            "normalization and instrument metadata."
        ),
    },
    {
        "format_code": "GATE-T2",
        "dataset": "trade",
        "instrument_type": "perpetual",
        "market_category": "inverse",
        "container_format": "csv",
        "compression": "gzip",
        "record_format": "rows",
        "schema": PERPETUAL_TRADE_SCHEMA,
        "notes": (
            "Monthly GZIP containing headerless CSV. "
            "size is signed and combines quantity magnitude with trade "
            "direction. Physical parser is shared with Gate linear "
            "perpetual trades. Contract interpretation belongs to "
            "normalization and instrument metadata."
        ),
    },
    {
        "format_code": "GATE-B1",
        "dataset": "l2",
        "instrument_type": "spot",
        "market_category": "spot",
        "container_format": "csv",
        "compression": "gzip",
        "record_format": "book_rows",
        "schema": {
            "header": False,
            "granularity": "hourly",
            "ordering": "chronological",
            "fields": [
                {
                    "position": 1,
                    "name": "timestamp",
                    "type": "decimal",
                    "representation": "unix_epoch",
                    "unit": "seconds",
                    "snapshot_precision": "second",
                    "update_precision": "100ms",
                },
                {
                    "position": 2,
                    "name": "side",
                    "type": "integer",
                },
                {
                    "position": 3,
                    "name": "action",
                    "type": "string",
                    "values": [
                        "set",
                        "make",
                        "take",
                    ],
                },
                {
                    "position": 4,
                    "name": "price",
                    "type": "decimal",
                },
                {
                    "position": 5,
                    "name": "amount",
                    "type": "decimal",
                },
                {
                    "position": 6,
                    "name": "begin_id",
                    "type": "integer",
                },
                {
                    "position": 7,
                    "name": "merged_count",
                    "type": "integer",
                },
            ],
            "order_book": {
                "event_model": ("row_snapshot_and_incremental_updates"),
                "snapshot_action": "set",
                "update_actions": [
                    "make",
                    "take",
                ],
                "side_field": "side",
                "sequence": {
                    "begin_id": "begin_id",
                    "merged_count": "merged_count",
                    "continuity_rule": (
                        "next.begin_id = " "current.begin_id + current.merged_count"
                    ),
                },
                "reconstruction_required": True,
            },
        },
        "notes": (
            "Hourly GZIP containing headerless 7-column CSV. "
            "Each file begins with a complete set snapshot represented "
            "across many physical rows, followed by make/take updates. "
            "Updates use approximately 100 ms timestamp resolution."
        ),
    },
    {
        "format_code": "GATE-B2",
        "dataset": "l2",
        "instrument_type": "perpetual",
        "market_category": "linear",
        "container_format": "csv",
        "compression": "gzip",
        "record_format": "book_rows",
        "schema": PERPETUAL_L2_SCHEMA,
        "notes": (
            "Hourly GZIP containing headerless 6-column CSV. "
            "Initial set rows form one logical full snapshot, followed "
            "by make/take updates. Book side is encoded by the sign of "
            "size. Physical parser is shared with inverse perpetual L2."
        ),
    },
    {
        "format_code": "GATE-B2",
        "dataset": "l2",
        "instrument_type": "perpetual",
        "market_category": "inverse",
        "container_format": "csv",
        "compression": "gzip",
        "record_format": "book_rows",
        "schema": PERPETUAL_L2_SCHEMA,
        "notes": (
            "Hourly GZIP containing headerless 6-column CSV. "
            "Initial set rows form one logical full snapshot, followed "
            "by make/take updates. Book side is encoded by the sign of "
            "size. Physical parser is shared with linear perpetual L2."
        ),
    },
]
