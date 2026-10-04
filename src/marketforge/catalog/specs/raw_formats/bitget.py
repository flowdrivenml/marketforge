from __future__ import annotations

TRADE_SCHEMA = {
    "header": True,
    "granularity": "daily",
    "archive_day_boundary": "UTC+8",
    "daily_packaging": "numbered_zip_chunks",
    "ordering": "chronological",
    "fields": [
        {
            "name": "trade_id",
            "type": "integer/string",
        },
        {
            "name": "timestamp",
            "type": "integer",
            "representation": "unix_epoch",
            "unit": "milliseconds",
            "observed_effective_precision": "second",
        },
        {
            "name": "price",
            "type": "decimal",
        },
        {
            "name": "side",
            "type": "string",
        },
        {
            "name": "volume(quote)",
            "type": "decimal",
        },
        {
            "name": "size(base)",
            "type": "decimal",
        },
    ],
}


L2_SCHEMA = {
    "header": True,
    "granularity": "daily",
    "archive_day_boundary": "UTC+8",
    "workbook_sheets": 1,
    "ordering": "not_chronological",
    "sorting_required": True,
    "fields": [
        {
            "name": "timestamp",
            "type": "integer",
            "representation": "unix_epoch",
            "unit": "seconds",
            "precision": "second",
        },
        {
            "name": "asks",
            "type": "string",
            "encoding": "json",
            "level_schema": [
                "price",
                "quantity",
            ],
            "ordering": "price_ascending",
        },
        {
            "name": "bids",
            "type": "string",
            "encoding": "json",
            "level_schema": [
                "price",
                "quantity",
            ],
            "ordering": "price_descending",
        },
    ],
    "order_book": {
        "event_model": "standalone_snapshot",
        "delta_updates": False,
        "sequence_id": False,
        "update_id": False,
        "reconstruction_required": False,
    },
}


RAW_FORMATS = [
    {
        "format_code": "BITGET-T1",
        "dataset": "trade",
        "instrument_type": "spot",
        "market_category": "spot",
        "container_format": "csv",
        "compression": "zip",
        "record_format": "rows",
        "schema": TRADE_SCHEMA,
        "notes": (
            "Daily UTC+8 archive packaged as one or more numbered ZIP "
            "chunks. Each archive contains one headered CSV. "
            "Observed timestamps were encoded as milliseconds but had "
            "effective one-second precision."
        ),
    },
    {
        "format_code": "BITGET-T1",
        "dataset": "trade",
        "instrument_type": "perpetual",
        "market_category": "linear",
        "container_format": "csv",
        "compression": "zip",
        "record_format": "rows",
        "schema": TRADE_SCHEMA,
        "notes": (
            "Same physical trade format as Bitget Spot. "
            "Daily UTC+8 archive may be split across multiple numbered "
            "ZIP chunks. Observed timestamps were encoded as milliseconds "
            "but had effective one-second precision."
        ),
    },
    {
        "format_code": "BITGET-B1",
        "dataset": "l2",
        "instrument_type": "spot",
        "market_category": "spot",
        "container_format": "xlsx",
        "compression": "zip",
        "record_format": "snapshot_rows",
        "schema": {
            **L2_SCHEMA,
            "order_book": {
                **L2_SCHEMA["order_book"],
                "snapshot_depth": {
                    "asks": 500,
                    "bids": 500,
                },
            },
        },
        "notes": (
            "ZIP contains an XLSX workbook with one sheet. "
            "Rows are independent full snapshots and raw workbook order "
            "is not chronological. Snapshots must be sorted by timestamp. "
            "Observed Spot snapshots contained exactly 500 asks and "
            "500 bids."
        ),
    },
    {
        "format_code": "BITGET-B1",
        "dataset": "l2",
        "instrument_type": "perpetual",
        "market_category": "linear",
        "container_format": "xlsx",
        "compression": "zip",
        "record_format": "snapshot_rows",
        "schema": {
            **L2_SCHEMA,
            "order_book": {
                **L2_SCHEMA["order_book"],
                "snapshot_depth": {
                    "asks": 500,
                    "bids": 500,
                },
            },
        },
        "notes": (
            "ZIP contains an XLSX workbook with one sheet. "
            "Rows are independent full snapshots and raw workbook order "
            "is not chronological. Snapshots must be sorted by timestamp. "
            "Observed Linear snapshots contained exactly 500 asks and "
            "500 bids."
        ),
    },
    {
        "format_code": "BITGET-B1",
        "dataset": "l2",
        "instrument_type": "perpetual",
        "market_category": "inverse",
        "container_format": "xlsx",
        "compression": "zip",
        "record_format": "snapshot_rows",
        "schema": {
            **L2_SCHEMA,
            "order_book": {
                **L2_SCHEMA["order_book"],
                "snapshot_depth": "variable",
            },
        },
        "notes": (
            "ZIP contains an XLSX workbook with one sheet. "
            "Rows are independent snapshots and raw workbook order is "
            "not chronological. Snapshots must be sorted by timestamp. "
            "Observed inverse snapshot depth was variable."
        ),
    },
]
