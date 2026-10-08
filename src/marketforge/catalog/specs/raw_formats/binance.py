from __future__ import annotations

RAW_FORMATS = [
    {
        "format_code": "BINANCE-T1",
        "dataset": "trade",
        "instrument_type": "spot",
        "market_category": "spot",
        "container_format": "csv",
        "compression": "zip",
        "record_format": "rows",
        "schema": {
            "header": False,
            "granularity": "daily",
            "ordering": "chronological",
            "fields": [
                {
                    "position": 1,
                    "name": "trade_id",
                    "type": "integer",
                },
                {
                    "position": 2,
                    "name": "price",
                    "type": "decimal",
                },
                {
                    "position": 3,
                    "name": "quantity",
                    "type": "decimal",
                },
                {
                    "position": 4,
                    "name": "quote_quantity",
                    "type": "decimal",
                },
                {
                    "position": 5,
                    "name": "timestamp",
                    "type": "integer",
                    "representation": "unix_epoch",
                    "unit": "microseconds",
                    "precision": "microsecond",
                },
                {
                    "position": 6,
                    "name": "is_buyer_maker",
                    "type": "boolean",
                },
                {
                    "position": 7,
                    "name": "is_best_match",
                    "type": "boolean",
                },
            ],
        },
        "notes": (
            "Daily ZIP/deflate archive containing headerless CSV. "
            "Trade IDs were consecutive in the inspected fixture. "
            "Quote quantity matched price multiplied by quantity. "
            "is_best_match was true for every observed record."
        ),
    },
    {
        "format_code": "BINANCE-T2",
        "dataset": "trade",
        "instrument_type": "perpetual",
        "market_category": "linear",
        "container_format": "csv",
        "compression": "zip",
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
                    "name": "price",
                    "type": "decimal",
                },
                {
                    "name": "qty",
                    "type": "decimal",
                },
                {
                    "name": "quote_qty",
                    "type": "decimal",
                },
                {
                    "name": "time",
                    "type": "integer",
                    "representation": "unix_epoch",
                    "unit": "milliseconds",
                    "precision": "millisecond",
                },
                {
                    "name": "is_buyer_maker",
                    "type": "boolean",
                },
            ],
        },
        "notes": (
            "Daily ZIP/deflate archive containing headered CSV. "
            "Trade IDs were unique but not contiguous in the inspected "
            "fixture. Quote quantity matched price multiplied by quantity."
        ),
    },
    {
        "format_code": "BINANCE-T3",
        "dataset": "trade",
        "instrument_type": "perpetual",
        "market_category": "inverse",
        "container_format": "csv",
        "compression": "zip",
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
                    "name": "price",
                    "type": "decimal",
                },
                {
                    "name": "qty",
                    "type": "decimal",
                },
                {
                    "name": "base_qty",
                    "type": "decimal",
                },
                {
                    "name": "time",
                    "type": "integer",
                    "representation": "unix_epoch",
                    "unit": "milliseconds",
                    "precision": "millisecond",
                },
                {
                    "name": "is_buyer_maker",
                    "type": "boolean",
                },
            ],
        },
        "notes": (
            "Daily ZIP/deflate archive containing headered CSV. "
            "qty is contract quantity and base_qty is base-asset quantity. "
            "The inspected BTCUSD_PERP fixture satisfied "
            "base_qty = qty * 100 / price, but the 100 contract value "
            "must not be assumed universal and belongs to instrument metadata."
        ),
    },
    {
        "format_code": "BINANCE-T2",
        "dataset": "trade",
        "instrument_type": "future",
        "market_category": "linear",
        "container_format": "csv",
        "compression": "zip",
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
                    "name": "price",
                    "type": "decimal",
                },
                {
                    "name": "qty",
                    "type": "decimal",
                },
                {
                    "name": "quote_qty",
                    "type": "decimal",
                },
                {
                    "name": "time",
                    "type": "integer",
                    "representation": "unix_epoch",
                    "unit": "milliseconds",
                    "precision": "millisecond",
                },
                {
                    "name": "is_buyer_maker",
                    "type": "boolean",
                },
            ],
        },
        "notes": (
            "Daily ZIP/deflate archive containing headered CSV. "
            "Linear dated futures use the same physical trade format "
            "as linear perpetual contracts."
        ),
    },
    {
        "format_code": "BINANCE-T3",
        "dataset": "trade",
        "instrument_type": "future",
        "market_category": "inverse",
        "container_format": "csv",
        "compression": "zip",
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
                    "name": "price",
                    "type": "decimal",
                },
                {
                    "name": "qty",
                    "type": "decimal",
                },
                {
                    "name": "base_qty",
                    "type": "decimal",
                },
                {
                    "name": "time",
                    "type": "integer",
                    "representation": "unix_epoch",
                    "unit": "milliseconds",
                    "precision": "millisecond",
                },
                {
                    "name": "is_buyer_maker",
                    "type": "boolean",
                },
            ],
        },
        "notes": (
            "Daily ZIP/deflate archive containing headered CSV. "
            "Inverse dated futures use the same physical trade format "
            "as inverse perpetual contracts. Contract value belongs "
            "to instrument metadata."
        ),
    },
]
